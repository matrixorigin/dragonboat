// Copyright 2012 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.
//
// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux
// +build linux

package tan

import (
	"errors"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"os"
	"syscall"
	"testing"
)

type nativePreallocationFS struct {
	vfs.FS
	t    *testing.T
	log  vfs.File
	name string
}

func (f *nativePreallocationFS) Create(name string) (vfs.File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	kind, _, ok := parseFilename(f.FS, name)
	if ok && kind == fileTypeLog {
		f.log, f.name = file, name
		f.t.Cleanup(func() {
			if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
				_ = file.Close()
			}
		})
	}
	// Preserve the native *os.File: wrapping it would bypass prealloc entirely.
	return file, nil
}
func TestNativePreallocationFailureRetiresCandidate(t *testing.T) {
	fs := &nativePreallocationFS{FS: vfs.Default, t: t}
	// The negative allocation length deterministically exercises native EINVAL
	// without disk pressure or changing process-wide resource limits.
	owner, err := open(1, 1, "owner", t.TempDir(), &Options{FS: fs, MaxLogFileSize: -indexBlockSize - 1})
	if owner != nil {
		t.Cleanup(func() { _ = owner.close() })
	}
	require.ErrorIs(t, err, syscall.EINVAL)
	require.Nil(t, owner)
	require.NotNil(t, fs.log)
	_, native := fs.log.(*os.File)
	require.True(t, native, "test must reach the native preallocation path")
	_, statErr := fs.log.Stat()
	require.ErrorIs(t, statErr, os.ErrClosed, "candidate descriptor must retire before cleanup rescue")
	_, pathErr := os.Stat(fs.name)
	require.ErrorIs(t, pathErr, os.ErrNotExist, "unpublished candidate path must be removed")
}
