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

package source

import (
	"context"
	"fmt"
	"strings"

	"github.com/containerd/containerd/images"
	"github.com/containerd/containerd/labels"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/stargz-snapshotter/fs/config"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// GetSources is a function for converting snapshot labels into typed blob sources
// information. This package defines a default converter which provides source
// information based on some labels but implementations aren't required to use labels.
// Implementations are allowed to return several sources (registry config + image refs)
// about the blob.
type GetSources func(labels map[string]string) (source []Source, err error)

// Source is a typed blob source information. This contains information about
// a blob stored in registries and some contexts of the blob.
type Source struct {

	// Hosts is a registry configuration where this blob is stored.
	Hosts docker.RegistryHosts

	// Name is an image reference which contains this blob.
	Name reference.Spec

	// Target is a descriptor of this blob.
	Target ocispec.Descriptor

	// Manifest is an image manifest which contains the blob. This will
	// be used by the filesystem to pre-resolve some layers contained in
	// the manifest.
	// Currently, only layer digests (Manifest.Layers.Digest) will be used.
	Manifest ocispec.Manifest
}

const (
	// targetRefLabel is a label which contains image reference.
	targetRefLabel = "containerd.io/snapshot/remote/stargz.reference"

	// targetDigestLabel is a label which contains layer digest.
	targetDigestLabel = "containerd.io/snapshot/remote/stargz.digest"

	// targetImageLayersLabel is a label which contains layer digests contained in
	// the target image.
	targetImageLayersLabel = "containerd.io/snapshot/remote/stargz.layers"
)

// FromDefaultLabels returns a function for converting snapshot labels to
// source information based on labels.
func FromDefaultLabels(hosts docker.RegistryHosts) GetSources {
	return func(labels map[string]string) ([]Source, error) {
		refStr, ok := labels[targetRefLabel]
		if !ok {
			return nil, fmt.Errorf("reference hasn't been passed")
		}
		refspec, err := reference.Parse(refStr)
		if err != nil {
			return nil, err
		}

		digestStr, ok := labels[targetDigestLabel]
		if !ok {
			return nil, fmt.Errorf("digest hasn't been passed")
		}
		target, err := digest.Parse(digestStr)
		if err != nil {
			return nil, err
		}

		var layersDgst []digest.Digest
		if l, ok := labels[targetImageLayersLabel]; ok {
			layersStr := strings.Split(l, ",")
			for _, l := range layersStr {
				d, err := digest.Parse(l)
				if err != nil {
					return nil, err
				}
				if d.String() != target.String() {
					layersDgst = append(layersDgst, d)
				}
			}
		}

		var layers []ocispec.Descriptor
		for _, dgst := range append([]digest.Digest{target}, layersDgst...) {
			layers = append(layers, ocispec.Descriptor{Digest: dgst})
		}

		return []Source{
			{
				Hosts:    hosts,
				Name:     refspec,
				Target:   ocispec.Descriptor{Digest: target},
				Manifest: ocispec.Manifest{Layers: layers},
			},
		}, nil
	}
}

// AppendDefaultLabelsHandlerWrapper makes a handler which appends image's basic
// information to each layer descriptor as annotations during unpack. These
// annotations will be passed to this remote snapshotter as labels and used to
// construct source information.
func AppendDefaultLabelsHandlerWrapper(ref string, prefetchSize int64) func(f images.Handler) images.Handler {
	return func(f images.Handler) images.Handler {
		return images.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
			children, err := f.Handle(ctx, desc)
			if err != nil {
				return nil, err
			}
			switch desc.MediaType {
			case ocispec.MediaTypeImageManifest, images.MediaTypeDockerSchema2Manifest:
				for i := range children {
					c := &children[i]
					if images.IsLayerType(c.MediaType) {
						if c.Annotations == nil {
							c.Annotations = make(map[string]string)
						}
						c.Annotations[targetRefLabel] = ref
						c.Annotations[targetDigestLabel] = c.Digest.String()
						var layers string
						for _, l := range children[i:] {
							if images.IsLayerType(l.MediaType) {
								ls := fmt.Sprintf("%s,", l.Digest.String())
								// This avoids the label hits the size limitation.
								// Skipping layers is allowed here and only affects performance.
								if err := labels.Validate(targetImageLayersLabel, layers+ls); err != nil {
									break
								}
								layers += ls
							}
						}
						c.Annotations[targetImageLayersLabel] = strings.TrimSuffix(layers, ",")
						c.Annotations[config.TargetPrefetchSizeLabel] = fmt.Sprintf("%d", prefetchSize)
					}
				}
			}
			return children, nil
		})
	}
}
