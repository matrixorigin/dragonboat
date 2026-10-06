// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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

package tee

import (
	"errors"
	"io"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/config"
	internaltee "github.com/lni/dragonboat/v4/internal/logdb/tee"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type acquiredTeeHandle struct {
	io.Closer
	fs     *teeAcquisitionFS
	closes int
}

func (h *acquiredTeeHandle) Close() error {
	h.closes++
	err := h.Closer.Close()
	if h.fs.failed && h.fs.mode == "error with close diagnostic" {
		return errors.Join(err, h.fs.diagnostic)
	}
	return err
}

type acquiredTeeFile struct {
	vfs.File
	handle *acquiredTeeHandle
}

func (f *acquiredTeeFile) Close() error { return f.handle.Close() }

type teeAcquisitionFS struct {
	vfs.FS
	mode              string
	cause, diagnostic error
	failed            bool
	handles           []*acquiredTeeHandle
}

func (f *teeAcquisitionFS) MkdirAll(name string, mode os.FileMode) error {
	if strings.Contains(name, "tee-pebble") {
		f.failed = true
		switch f.mode {
		case "panic":
			panic(f.cause)
		case "Goexit":
			runtime.Goexit()
		default:
			return f.cause
		}
	}
	return f.FS.MkdirAll(name, mode)
}
func (f *teeAcquisitionFS) OpenDir(name string) (vfs.File, error) {
	file, err := f.FS.OpenDir(name)
	if err != nil {
		return file, err
	}
	h := &acquiredTeeHandle{Closer: file, fs: f}
	f.handles = append(f.handles, h)
	return &acquiredTeeFile{File: file, handle: h}, nil
}
func (f *teeAcquisitionFS) Lock(name string) (io.Closer, error) {
	lock, err := f.FS.Lock(name)
	if err != nil {
		return lock, err
	}
	h := &acquiredTeeHandle{Closer: lock, fs: f}
	f.handles = append(f.handles, h)
	return h, nil
}
func TestTeeConstructorsRetireFirstChildOnSecondFailure(t *testing.T) {
	entries := []struct {
		name   string
		create func(config.NodeHostConfig, config.LogDBCallback, []string, []string) (raftio.ILogDB, error)
	}{
		{"internal", internaltee.NewTeeLogDB}, {"plugin", CreateTanPebbleLogDB},
	}
	for _, entry := range entries {
		t.Run(entry.name, func(t *testing.T) {
			for _, mode := range []string{"error", "panic", "Goexit", "error with close diagnostic"} {
				t.Run(mode, func(t *testing.T) {
					mem := vfs.NewStrictMem()
					t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
					fs := &teeAcquisitionFS{FS: mem, mode: mode, cause: errors.New("second child fault"), diagnostic: errors.New("first child close diagnostic")}
					t.Cleanup(func() {
						for _, h := range fs.handles {
							if h.closes == 0 {
								_ = h.Close()
							}
						}
					})
					cfg := config.NodeHostConfig{}
					cfg.Expert.FS = fs
					cfg.Expert.LogDB = config.GetTinyMemLogDBConfig()
					cfg.Expert.LogDB.Shards = 1
					cfg.Expert.Engine.ExecShards = 1
					cfg.Expert.LogDB.KVWriteBufferSize = 64 * 1024
					cfg.Expert.LogDB.KVLRUCacheSize = 64 * 1024
					done := make(chan struct{})
					var owner raftio.ILogDB
					var err error
					var recovered any
					returned := false
					go func() {
						defer close(done)
						defer func() { recovered = recover() }()
						owner, err = entry.create(cfg, nil, []string{"/tee-owner"}, nil)
						returned = true
					}()
					select {
					case <-done:
					case <-time.After(time.Second):
						t.Fatal("constructor unwind did not complete")
					}
					if owner != nil {
						t.Cleanup(func() { _ = owner.Close() })
					}
					require.True(t, fs.failed, "second actual constructor reached")
					require.Nil(t, owner)
					require.GreaterOrEqual(t, len(fs.handles), 3, "first native child directories and lock acquired")
					for _, h := range fs.handles {
						require.Equal(t, 1, h.closes, "first child native handle retires exactly once before constructor exits")
					}
					switch mode {
					case "panic":
						require.Same(t, fs.cause, recovered)
						require.False(t, returned)
					case "Goexit":
						require.Nil(t, recovered)
						require.False(t, returned)
					default:
						require.Nil(t, recovered)
						require.True(t, returned)
						require.ErrorIs(t, err, fs.cause)
						if mode == "error with close diagnostic" {
							require.ErrorIs(t, err, fs.diagnostic)
						}
					}
				})
			}
		})
	}
}
