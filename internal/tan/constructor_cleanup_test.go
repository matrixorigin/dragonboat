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

package tan

import (
	"errors"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"io"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

type observedConstructorFile struct {
	vfs.File
	closes atomic.Int32
}

func (f *observedConstructorFile) Close() error { f.closes.Add(1); return f.File.Close() }

type constructorFailureFS struct {
	vfs.FS
	t         *testing.T
	bootstrap string
	phase     string
	cause     error
	armed     bool
	files     []*observedConstructorFile
}

func (f *constructorFailureFS) List(name string) ([]string, error) {
	names, err := f.FS.List(name)
	if name == f.bootstrap && err == nil {
		f.armed = true
	}
	return names, err
}
func (f *constructorFailureFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	if f.armed && name == f.bootstrap && f.phase == "second directory" {
		return nil, f.cause
	}
	file, err := f.FS.Open(name, opts...)
	if err != nil || !f.armed {
		return file, err
	}
	owned := &observedConstructorFile{File: file}
	f.files = append(f.files, owned)
	f.t.Cleanup(func() {
		if owned.closes.Load() == 0 {
			_ = owned.Close()
		}
	})
	return owned, nil
}
func (f *constructorFailureFS) Lock(name string) (io.Closer, error) {
	if f.armed {
		if f.phase == "lock Goexit" {
			runtime.Goexit()
		}
		if f.phase == "lock error" {
			return nil, f.cause
		}
	}
	return f.FS.Lock(name)
}
func TestConstructorReleasesEarlierDirectories(t *testing.T) {
	for _, tc := range []struct {
		phase   string
		handles int
	}{{"second directory", 1}, {"lock error", 2}, {"lock Goexit", 2}} {
		t.Run(tc.phase, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			fs := &constructorFailureFS{FS: mem, t: t, bootstrap: "/constructor/tandb/bootstrap", phase: tc.phase, cause: errors.New("directory acquisition failure")}
			cfg := config.NodeHostConfig{Expert: config.ExpertConfig{FS: fs, LogDB: config.GetTinyMemLogDBConfig()}}
			cfg.Expert.LogDB.KVWriteBufferSize = 4096
			done := make(chan struct{})
			var result error
			var returned bool
			go func() {
				defer close(done)
				owner, err := CreateTan(cfg, nil, []string{"/constructor"}, nil)
				result = err
				returned = true
				if owner != nil {
					t.Cleanup(func() { _ = owner.Close() })
				}
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("constructor did not unwind")
			}
			require.Len(t, fs.files, tc.handles, "fault must occur after the intended real acquisitions")
			for _, file := range fs.files {
				require.Equal(t, int32(1), file.closes.Load())
			}
			if tc.phase == "lock Goexit" {
				require.False(t, returned)
				require.Nil(t, result)
			} else {
				require.True(t, returned)
				require.ErrorIs(t, result, fs.cause)
			}
		})
	}
}

type observedNativeCloser struct {
	io.Closer
	closes int
}

func (c *observedNativeCloser) Close() error { c.closes++; return c.Closer.Close() }

func TestLogDBCloseRetiresAllOwnersAfterChildInterruption(t *testing.T) {
	for _, mode := range []string{"clean", "error", "panic", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			cfg := config.NodeHostConfig{Expert: config.ExpertConfig{FS: mem, LogDB: config.GetTinyMemLogDBConfig()}}
			cfg.Expert.LogDB.KVWriteBufferSize = 4096
			cfg.Expert.LogDB.DisablePrealloc = true
			cfg.Expert.LogDB.MaxLogFileSize = 64 * 1024
			require.NoError(t, cfg.Prepare())
			parent, err := CreateTan(cfg, nil, []string{"/parent-close"}, nil)
			require.NoError(t, err)
			dir := &observedConstructorFile{File: parent.dir}
			bootstrap := &observedConstructorFile{File: parent.bsDir}
			lock := &observedNativeCloser{Closer: parent.fileLock}
			parent.dir, parent.bsDir, parent.fileLock = dir, bootstrap, lock
			t.Cleanup(func() {
				if lock.closes == 0 {
					require.NoError(t, lock.Close())
				}
				if bootstrap.closes.Load() == 0 {
					require.NoError(t, bootstrap.Close())
				}
				if dir.closes.Load() == 0 {
					require.NoError(t, dir.Close())
				}
			})
			cause := errors.New("child persistence interruption")
			var files []*interruptedPersistenceFile
			for _, shard := range []uint64{1, 2} {
				child, err := parent.getDB(shard, 1)
				require.NoError(t, err)
				file := &interruptedPersistenceFile{File: child.mu.logFile, mode: mode, stage: "Sync", cause: cause}
				if mode == "clean" {
					file.stage = ""
				}
				child.mu.logFile = file
				files = append(files, file)
				t.Cleanup(func() {
					if child.closed.Load() == nil {
						file.stage = ""
						_ = child.close()
					}
					if file.closes == 0 {
						require.NoError(t, file.Close())
					}
				})
			}
			var recovered any
			returned := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				err = parent.Close()
				returned = true
			}()
			<-done
			for _, file := range files {
				require.Equal(t, 1, file.closes, "every child's native descriptor retires before rescue")
			}
			require.Equal(t, int32(1), dir.closes.Load())
			require.Equal(t, int32(1), bootstrap.closes.Load())
			require.Equal(t, 1, lock.closes)
			if mode == "panic" {
				require.False(t, returned)
				require.Same(t, cause, recovered)
			} else if mode == "Goexit" {
				require.False(t, returned)
				require.Nil(t, recovered)
			} else {
				require.True(t, returned)
				require.Nil(t, recovered)
				if mode == "error" {
					require.ErrorIs(t, err, cause)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}
