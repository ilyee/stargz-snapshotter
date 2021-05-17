/*
   Copyright The containerd Authors.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

/*
   Copyright 2019 The Go Authors. All rights reserved.
   Use of this source code is governed by a BSD-style
   license that can be found in the NOTICE.md file.
*/

package fusemanager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/containerd/containerd/log"
	"github.com/moby/sys/mountinfo"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"

	pb "github.com/containerd/stargz-snapshotter/fusemanager/api"
	"github.com/containerd/stargz-snapshotter/service"
	"github.com/containerd/stargz-snapshotter/snapshot"
)

const (
	fuseManagerReady = iota
	fuseManagerNotReady
)

type FuseManagerServer struct {
	pb.UnimplementedFileSystemServiceServer

	lock  sync.RWMutex
	ready int32

	// root is the latest root passed from containerd-stargz-grpc
	root string
	// config is the latest config passed from containerd-stargz-grpc
	config *service.Config
	// fsMap stores all filesystems from various configs
	fsMap sync.Map
	// curFs is filesystem created by latest config
	curFs snapshot.FileSystem
	ms    *bolt.DB
}

func NewFuseManager(ctx context.Context, fuseStoreAddr string) (*FuseManagerServer, error) {
	if err := os.MkdirAll(filepath.Dir(fuseStoreAddr), 0700); err != nil {
		return nil, errors.Wrapf(err, "failed to create directory %q", filepath.Dir(fuseStoreAddr))
	}

	db, err := bolt.Open(fuseStoreAddr, 0666, &bolt.Options{Timeout: 10 * time.Second, ReadOnly: false})
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure fusestore")
	}

	fm := &FuseManagerServer{
		ready: fuseManagerNotReady,
		lock:  sync.RWMutex{},
		fsMap: sync.Map{},
		ms:    db,
	}

	return fm, nil
}

func (fm *FuseManagerServer) Init(ctx context.Context, req *pb.InitRequest) (*pb.Response, error) {
	fm.lock.Lock()
	fm.ready = fuseManagerNotReady
	defer func() {
		fm.ready = fuseManagerReady
		fm.lock.Unlock()
	}()

	ctx = log.WithLogger(ctx, log.G(ctx))

	config := &service.Config{}
	err := json.Unmarshal(req.Config, config)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to get config")
		return &pb.Response{}, err
	}
	fm.root = req.Root
	fm.config = config

	fs, err := service.NewFileSystem(ctx, fm.root, fm.config)
	if err != nil {
		return &pb.Response{}, err
	}
	fm.curFs = fs

	err = fm.restoreFuseInfo(ctx)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to restore fuse info")
		return &pb.Response{}, err
	}

	return &pb.Response{}, nil
}

func (fm *FuseManagerServer) Mount(ctx context.Context, req *pb.MountRequest) (*pb.Response, error) {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	if fm.ready != fuseManagerReady {
		return &pb.Response{}, fmt.Errorf("fuse manager not ready")
	}

	ctx = log.WithLogger(ctx, log.G(ctx).WithField("mountpoint", req.Mountpoint))

	err := fm.mount(ctx, req.Mountpoint, req.Labels)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to mount stargz")
		return &pb.Response{}, err
	}

	fm.storeFuseInfo(&fuseInfo{
		Root:       fm.root,
		Mountpoint: req.Mountpoint,
		Labels:     req.Labels,
		Config:     *fm.config,
	})

	return &pb.Response{}, nil
}

func (fm *FuseManagerServer) Check(ctx context.Context, req *pb.CheckRequest) (*pb.Response, error) {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	if fm.ready != fuseManagerReady {
		return &pb.Response{}, fmt.Errorf("fuse manager not ready")
	}

	ctx = log.WithLogger(ctx, log.G(ctx).WithField("mountpoint", req.Mountpoint))

	obj, found := fm.fsMap.Load(req.Mountpoint)
	if !found {
		err := fmt.Errorf("failed to find filesystem of mountpoint %s", req.Mountpoint)
		log.G(ctx).WithError(err).Errorf("failed to check filesystem")
		return &pb.Response{}, err
	}

	fs := obj.(snapshot.FileSystem)
	err := fs.Check(ctx, req.Mountpoint, req.Labels)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to check filesystem")
		return &pb.Response{}, err
	}

	return &pb.Response{}, nil
}

func (fm *FuseManagerServer) Unmount(ctx context.Context, req *pb.UnmountRequest) (*pb.Response, error) {
	fm.lock.RLock()
	defer fm.lock.RUnlock()
	if fm.ready != fuseManagerReady {
		return &pb.Response{}, fmt.Errorf("fuse manager not ready")
	}

	ctx = log.WithLogger(ctx, log.G(ctx).WithField("mountpoint", req.Mountpoint))

	obj, found := fm.fsMap.Load(req.Mountpoint)
	if !found {
		// check whether already unmounted
		mounts, err := mountinfo.GetMounts(func(info *mountinfo.Info) (skip, stop bool) {
			if info.Mountpoint == req.Mountpoint {
				return false, true
			}
			return true, false
		})
		if err != nil {
			log.G(ctx).WithError(err).Errorf("failed to get mount info")
			return &pb.Response{}, err
		}

		if len(mounts) <= 0 {
			return &pb.Response{}, nil
		}
		err = fmt.Errorf("failed to find filesystem of mountpoint %s", req.Mountpoint)
		log.G(ctx).WithError(err).Errorf("failed to unmount filesystem")
		return &pb.Response{}, err
	}

	fs := obj.(snapshot.FileSystem)
	err := fs.Unmount(ctx, req.Mountpoint)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to unmount filesystem")
		return &pb.Response{}, err
	}

	fm.fsMap.Delete(req.Mountpoint)
	fm.removeFuseInfo(&fuseInfo{
		Mountpoint: req.Mountpoint,
	})

	return &pb.Response{}, nil
}

func (fm *FuseManagerServer) Close(ctx context.Context) error {
	fm.lock.Lock()
	defer fm.lock.Unlock()

	mounts, err := mountinfo.GetMounts(nil)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to get mount info")
		return err
	}

	for _, m := range mounts {
		if strings.HasPrefix(m.Mountpoint, filepath.Join(fm.root, "snapshots")) {
			if err := syscall.Unmount(m.Mountpoint, syscall.MNT_FORCE); err != nil {
				return err
			}
		}
	}

	err = fm.ms.Close()
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to close fusestore")
		return err
	}

	return nil
}

func (fm *FuseManagerServer) mount(ctx context.Context, mountpoint string, labels map[string]string) error {
	if _, found := fm.fsMap.Load(mountpoint); found {
		return nil
	}

	err := fm.curFs.Mount(ctx, mountpoint, labels)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to mount stargz")
		return err
	}

	fm.fsMap.Store(mountpoint, fm.curFs)
	return nil
}
