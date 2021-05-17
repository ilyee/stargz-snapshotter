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

	bolt "go.etcd.io/bbolt"

	"github.com/containerd/stargz-snapshotter/service"
)

var (
	fuseInfoBucket = []byte("fuse-info-bucket")
)

type fuseInfo struct {
	Root       string
	Mountpoint string
	Labels     map[string]string
	Config     service.Config
}

func (fm *FuseManagerServer) storeFuseInfo(fuseInfo *fuseInfo) error {
	return fm.ms.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(fuseInfoBucket)
		if err != nil {
			return err
		}

		key := []byte(fuseInfo.Mountpoint)

		val, err := json.Marshal(fuseInfo)
		if err != nil {
			return err
		}

		err = bucket.Put(key, val)
		if err != nil {
			return err
		}

		return nil
	})
}

func (fm *FuseManagerServer) removeFuseInfo(fuseInfo *fuseInfo) error {
	return fm.ms.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(fuseInfoBucket)
		if err != nil {
			return err
		}

		key := []byte(fuseInfo.Mountpoint)

		err = bucket.Delete(key)
		if err != nil {
			return err
		}

		return nil
	})
}

func (fm *FuseManagerServer) restoreFuseInfo(ctx context.Context) error {
	return fm.ms.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(fuseInfoBucket)
		if bucket == nil {
			return nil
		}

		return bucket.ForEach(func(_, v []byte) error {
			mi := &fuseInfo{}
			err := json.Unmarshal(v, mi)
			if err != nil {
				return err
			}

			return fm.mount(ctx, mi.Mountpoint, mi.Labels)
		})
	})
}
