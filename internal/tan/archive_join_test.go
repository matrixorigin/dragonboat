//go:build go1.25

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
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

type joinedArchiveDelete struct {
	config.ArchiveIO
	entered, release, exited chan struct{}
	active                   atomic.Int32
}

func (a *joinedArchiveDelete) Delete(context.Context, string, ...string) error {
	a.active.Store(1)
	close(a.entered)
	<-a.release
	a.active.Store(0)
	close(a.exited)
	return nil
}

type archiveRetirementFS struct {
	vfs.FS
	backend *joinedArchiveDelete
	early   atomic.Bool
	mu      sync.Mutex
	handles []*archiveRetirementFile
}
type archiveRetirementFile struct {
	vfs.File
	fs     *archiveRetirementFS
	closes atomic.Int32
}

func (f *archiveRetirementFile) Close() error {
	if f.fs.backend.active.Load() != 0 {
		f.fs.early.Store(true)
	}
	f.closes.Add(1)
	return f.File.Close()
}
func (f *archiveRetirementFS) track(file vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return file, err
	}
	h := &archiveRetirementFile{File: file, fs: f}
	f.mu.Lock()
	f.handles = append(f.handles, h)
	f.mu.Unlock()
	return h, nil
}
func (f *archiveRetirementFS) Create(name string) (vfs.File, error) {
	file, err := f.FS.Create(name)
	return f.track(file, err)
}
func (f *archiveRetirementFS) OpenDir(name string) (vfs.File, error) {
	file, err := f.FS.OpenDir(name)
	return f.track(file, err)
}
func TestDatabaseCloseJoinsArchiveDelete(t *testing.T) {
	for _, mode := range []string{"GC", "retry ticker"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mem := vfs.NewStrictMem()
				t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
				require.NoError(t, mem.MkdirAll("/archive-join", 0700))
				aio := &joinedArchiveDelete{entered: make(chan struct{}), release: make(chan struct{}), exited: make(chan struct{})}
				fs := &archiveRetirementFS{FS: mem, backend: aio}
				owner, err := open(1, 1, "owner", "/archive-join", &Options{FS: fs, archiveIO: aio, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
				require.NoError(t, err)
				var releaseOnce, closeOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(aio.release) }) }
				closed := make(chan struct{})
				var closeErr error
				startClose := func() { closeOnce.Do(func() { go func() { defer close(closed); closeErr = owner.close() }() }) }
				t.Cleanup(func() { release(); startClose(); <-closed; synctest.Wait() })
				synctest.Wait()
				now := time.Now()
				require.NoError(t, owner.archiver.recorder.append(config.RecordItem{FileNum: 99, TS: now.Add(2 * time.Hour), FirstLsn: 99}))
				if mode == "GC" {
					owner.gcArchive(now.Add(time.Hour))
				} else {
					owner.archiver.mu.Lock()
					owner.archiver.mu.gcFailedQueue = []string{"retry.log"}
					owner.archiver.mu.Unlock()
					// Advance virtual time to the production retry ticker, not a scheduling delay.
					time.Sleep(gcFailRetryInterval)
				}
				synctest.Wait()
				select {
				case <-aio.entered:
				default:
					t.Fatal("native archive Delete not reached")
				}
				require.Equal(t, int32(1), aio.active.Load())
				require.NoError(t, owner.archiver.recorder.append(config.RecordItem{FileNum: 100, TS: now.Add(3 * time.Hour), FirstLsn: 100}), "remote Delete must not hold recorder lock")
				fs.mu.Lock()
				var owned []*archiveRetirementFile
				for _, h := range fs.handles {
					if h.closes.Load() == 0 {
						owned = append(owned, h)
					}
				}
				fs.mu.Unlock()
				require.GreaterOrEqual(t, len(owned), 3, "real directory, manifest and log ownership reached")
				// GC temporary handles also close while Delete is inactive; reset observation at shutdown boundary.
				fs.early.Store(false)
				startClose()
				synctest.Wait()
				require.ErrorIs(t, owner.ctx.Err(), context.Canceled)
				select {
				case <-owner.stopper.ShouldStop():
				default:
					t.Fatal("shutdown admission not sealed")
				}
				select {
				case <-closed:
					t.Fatal("Close returned before Delete completed")
				default:
				}
				for _, h := range owned {
					require.Zero(t, h.closes.Load(), "owned native handle retired before Delete completed")
				}
				require.False(t, fs.early.Load())
				release()
				synctest.Wait()
				select {
				case <-closed:
				default:
					t.Fatal("Close did not join after Delete release")
				}
				select {
				case <-aio.exited:
				default:
					t.Fatal("Delete completion missing")
				}
				require.Zero(t, aio.active.Load())
				require.NoError(t, closeErr)
				require.False(t, fs.early.Load())
				for _, h := range owned {
					require.Equal(t, int32(1), h.closes.Load())
				}
			})
		})
	}
}

type initialArchiveDelete struct {
	config.ArchiveIO
	mode  string
	calls int
	files []string
}

func (a *initialArchiveDelete) Delete(_ context.Context, _ string, files ...string) error {
	a.calls++
	a.files = append(a.files, files...)
	switch a.mode {
	case "Goexit":
		runtime.Goexit()
	case "error":
		return errors.New("remote deletion fault")
	}
	return nil
}
func TestInitialGCRetainsUncompletedBatch(t *testing.T) {
	for _, mode := range []string{"success", "error", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				mem := vfs.NewStrictMem()
				t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
				require.NoError(t, mem.MkdirAll("/initial-gc", 0700))
				aio := &initialArchiveDelete{mode: mode}
				owner, err := open(1, 1, "owner", "/initial-gc", &Options{FS: mem, archiveIO: aio, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
				require.NoError(t, err)
				var closeOnce sync.Once
				closeOwner := func() { closeOnce.Do(func() { require.NoError(t, owner.close()) }) }
				t.Cleanup(closeOwner)
				synctest.Wait()
				// Replace constructor metadata with the two records needed by this GC contract.
				f, err := mem.Create("/initial-gc/ARCHIVE")
				require.NoError(t, err)
				require.NoError(t, f.Close())
				now := time.Now()
				require.NoError(t, owner.archiver.recorder.append(config.RecordItem{FileNum: 98, TS: now, FirstLsn: 98}))
				require.NoError(t, owner.archiver.recorder.append(config.RecordItem{FileNum: 99, TS: now.Add(2 * time.Hour), FirstLsn: 99}))
				read := func() []byte {
					f, err := mem.Open("/initial-gc/ARCHIVE")
					require.NoError(t, err)
					defer func() { require.NoError(t, f.Close()) }()
					b, err := io.ReadAll(f)
					require.NoError(t, err)
					return b
				}
				before := read()
				require.Len(t, before, 48)
				owner.gcArchive(now.Add(time.Hour))
				synctest.Wait()
				closeOwner()
				require.Equal(t, before[24:], read(), "metadata publication committed")
				expected := []string{"/initial-gc/000098.log", "/initial-gc/000098.index"}
				require.Equal(t, 1, aio.calls)
				require.Equal(t, expected, aio.files)
				if mode == "success" {
					require.Empty(t, owner.archiver.mu.gcFailedQueue)
				} else {
					require.Equal(t, expected, owner.archiver.mu.gcFailedQueue, "uncompleted committed batch remains owned exactly once")
				}
			})
		})
	}
}
