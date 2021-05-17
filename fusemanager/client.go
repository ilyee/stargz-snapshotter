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

	"github.com/containerd/containerd/log"
	"google.golang.org/grpc"

	pb "github.com/containerd/stargz-snapshotter/fusemanager/api"
	"github.com/containerd/stargz-snapshotter/service"
	"github.com/containerd/stargz-snapshotter/snapshot"
)

type ManagerClient struct {
	client pb.FileSystemServiceClient
}

func NewManagerClient(ctx context.Context, root, socket string, config *service.Config) (snapshot.FileSystem, error) {
	conn, err := grpc.Dial(socket, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}

	cli := &ManagerClient{
		client: pb.NewFileSystemServiceClient(conn),
	}

	err = cli.Init(ctx, root, config)
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func (cli *ManagerClient) Init(ctx context.Context, root string, config *service.Config) error {
	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}

	req := &pb.InitRequest{
		Root:   root,
		Config: configBytes,
	}

	_, err = cli.client.Init(ctx, req)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to call Init")
		return err
	}

	return nil
}

func (cli *ManagerClient) Mount(ctx context.Context, mountpoint string, labels map[string]string) error {
	req := &pb.MountRequest{
		Mountpoint: mountpoint,
		Labels:     labels,
	}

	_, err := cli.client.Mount(ctx, req)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to call Mount")
		return err
	}

	return nil
}

func (cli *ManagerClient) Check(ctx context.Context, mountpoint string, labels map[string]string) error {
	req := &pb.CheckRequest{
		Mountpoint: mountpoint,
		Labels:     labels,
	}

	_, err := cli.client.Check(ctx, req)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to call Check")
		return err
	}

	return nil
}

func (cli *ManagerClient) Unmount(ctx context.Context, mountpoint string) error {
	req := &pb.UnmountRequest{
		Mountpoint: mountpoint,
	}

	_, err := cli.client.Unmount(ctx, req)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("failed to call Unmount")
		return err
	}

	return nil
}
