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
	"bytes"
	"errors"
	"github.com/lni/dragonboat/v4/config"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

type failedIndexRenameFS struct {
	vfs.FS
	cause   error
	fail    bool
	renames int
}

func (f *failedIndexRenameFS) Rename(old, new string) error {
	f.renames++
	if f.fail {
		return f.cause
	}
	return f.FS.Rename(old, new)
}
func TestFailedIndexPublicationRetainsEntriesForRetry(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "healthy"
		if fail {
			name = "rename failure then retry"
		}
		t.Run(name, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/index-retry", 0700))
			dir, err := mem.OpenDir("/index-retry")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, dir.Close()) })
			fs := &failedIndexRenameFS{FS: mem, cause: errors.New("index publication fault"), fail: fail}
			states := newNodeStates()
			want := []indexEntry{{start: 10, end: 12, fileNum: 7, pos: 0, length: 30}}
			states.getIndex(1, 1).currEntries.update(want[0])
			err = states.save("/index-retry", dir, 7, fs)
			if fail {
				require.ErrorIs(t, err, fs.cause)
				fs.fail = false
				require.NoError(t, states.save("/index-retry", dir, 7, fs))
			} else {
				require.NoError(t, err)
			}
			file, err := mem.Open("/index-retry/000007.index")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, file.Close()) })
			loaded := newNodeStates()
			require.NoError(t, loaded.load(file))
			require.Equal(t, want, loaded.getIndex(1, 1).entries.entries, "successful retry must publish original logical entries")
		})
	}
}

func TestFailedIndexPublicationCloseReopenPreservesEntries(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "healthy"
		if fail {
			name = "failed publication then Close"
		}
		t.Run(name, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/index-reopen", 0700))
			fs := &failedIndexRenameFS{FS: mem, cause: errors.New("index rename failure")}
			owner, err := open(1, 1, "owner", "/index-reopen", &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
			require.NoError(t, err)
			var once sync.Once
			var expectedCloseErr error
			closeOwner := func() {
				once.Do(func() {
					closeErr := owner.close()
					if expectedCloseErr != nil {
						require.ErrorIs(t, closeErr, expectedCloseErr)
					} else {
						require.NoError(t, closeErr)
					}
				})
			}
			t.Cleanup(closeOwner)
			expected := []pb.Entry{{Term: 1, Index: 10, Cmd: []byte("retained")}}
			_, err = owner.write(pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 1, Commit: 10}, EntriesToSave: expected}, nil)
			require.NoError(t, err)
			require.NoError(t, owner.sync())
			if fail {
				fs.fail = true
				expectedCloseErr = fs.cause
				owner.mu.Lock()
				err = owner.saveIndex()
				owner.mu.Unlock()
				require.ErrorIs(t, err, fs.cause)
				fs.fail = false
				_, admissionErr := owner.write(pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 1, Commit: 10}}, nil)
				require.ErrorIs(t, admissionErr, fs.cause)
			}
			renames := fs.renames
			closeOwner()
			if fail {
				require.Equal(t, renames, fs.renames, "terminal Close cannot publish another index")
			}
			reopened, err := open(1, 1, "reopened", "/index-reopen", &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.close()) })
			got, _, err := reopened.getEntries(1, 1, nil, 0, 10, 11, ^uint64(0))
			require.NoError(t, err, "fsynced entry must remain readable after index publication failure and normal Close/reopen")
			require.Equal(t, expected, got)
		})
	}
}

type interruptedPersistenceFile struct {
	vfs.File
	mode          string
	stage         string
	wrote         bool
	cause         error
	closes        int
	writes, syncs int
	attempted     []byte
}

func (f *interruptedPersistenceFile) Sync() error {
	f.syncs++
	if f.stage != "Sync" {
		return f.File.Sync()
	}
	switch f.mode {
	case "panic":
		panic(f.cause)
	case "Goexit":
		runtime.Goexit()
	}
	return f.cause
}
func (f *interruptedPersistenceFile) Write(p []byte) (int, error) {
	f.writes++
	if f.stage != "Write" || f.wrote {
		return f.File.Write(p)
	}
	f.wrote = true
	f.attempted = append([]byte(nil), p...)
	switch f.mode {
	case "panic":
		panic(f.cause)
	case "Goexit":
		runtime.Goexit()
	}
	return 0, f.cause
}
func (f *interruptedPersistenceFile) Close() error { f.closes++; return f.File.Close() }

type interruptedPersistenceFS struct {
	vfs.FS
	mode    string
	stage   string
	cause   error
	file    *interruptedPersistenceFile
	renames int
	creates int
	target  string
	t       *testing.T
	opened  []*interruptedPersistenceFile
}

func (f *interruptedPersistenceFS) Create(name string) (vfs.File, error) {
	f.creates++
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	f.file = &interruptedPersistenceFile{File: file, mode: f.mode, stage: f.stage, cause: f.cause}
	owned := f.file
	if f.t != nil {
		f.t.Cleanup(func() {
			if owned.closes == 0 {
				_ = owned.Close()
			}
		})
	}
	return f.file, nil
}
func (f *interruptedPersistenceFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.Open(name, opts...)
	if err != nil || name != f.target {
		return file, err
	}
	owned := &interruptedPersistenceFile{File: file, mode: f.mode, stage: f.stage, cause: f.cause}
	f.opened = append(f.opened, owned)
	f.t.Cleanup(func() {
		if owned.closes == 0 {
			require.NoError(f.t, owned.Close())
		}
	})
	return owned, nil
}

func (f *interruptedPersistenceFS) Rename(old, new string) error {
	f.renames++
	return f.FS.Rename(old, new)
}

func TestMetadataPublicationRequiresCompletedPersistence(t *testing.T) {
	for _, publisher := range []string{"index", "CURRENT"} {
		t.Run(publisher, func(t *testing.T) {
			for _, stage := range []string{"Write", "Sync"} {
				t.Run(stage, func(t *testing.T) {
					for _, mode := range []string{"error", "panic", "Goexit"} {
						t.Run(mode, func(t *testing.T) {
							mem := vfs.NewStrictMem()
							t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
							require.NoError(t, mem.MkdirAll("/metadata-sync", 0700))
							dir, err := mem.OpenDir("/metadata-sync")
							require.NoError(t, err)
							t.Cleanup(func() { require.NoError(t, dir.Close()) })
							fs := &interruptedPersistenceFS{FS: mem, mode: mode, stage: stage, cause: errors.New("metadata temporary persistence fault")}
							done := make(chan struct{})
							t.Cleanup(func() {
								<-done
								if fs.file != nil && fs.file.closes == 0 {
									require.NoError(t, fs.file.Close())
								}
							})
							var recovered any
							returned := false
							go func() {
								defer close(done)
								defer func() { recovered = recover() }()
								if publisher == "CURRENT" {
									err = setCurrentFile("/metadata-sync", fs, 7)
								} else {
									states := newNodeStates()
									states.getIndex(1, 1).currEntries.update(indexEntry{start: 10, end: 12, fileNum: 7, pos: 0, length: 30})
									err = states.save("/metadata-sync", dir, 7, fs)
								}
								returned = true
							}()
							select {
							case <-done:
							case <-time.After(time.Second):
								t.Fatal("metadata save unwind did not complete")
							}
							if mode == "error" {
								require.True(t, returned)
								require.Nil(t, recovered)
								require.ErrorIs(t, err, fs.cause)
							} else {
								require.False(t, returned)
								if mode == "panic" {
									require.Same(t, fs.cause, recovered)
								} else {
									require.Nil(t, recovered)
								}
							}
							require.NotNil(t, fs.file, "temporary native file acquired")
							require.Zero(t, fs.renames, "failed or interrupted persistence must never publish metadata")
							require.Equal(t, 1, fs.file.closes, "persistence interruption must not bypass native Close")
						})
					}
				})
			}
		})
	}
}

func TestReplaySyncFailureRetiresSourceBeforeIndexPublication(t *testing.T) {
	for _, tc := range []struct {
		name, mode, stage string
		historical        bool
	}{
		{"error", "error", "Sync", false}, {"panic", "panic", "Sync", false}, {"Goexit", "Goexit", "Sync", false}, {"historical error", "error", "Sync", true},
		{"historical save error", "error", "Write", true}, {"historical save panic", "panic", "Write", true}, {"historical save Goexit", "Goexit", "Write", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode := tc.mode
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			require.NoError(t, mem.MkdirAll("/replay-sync", 0700))
			opts := &Options{FS: mem, DisablePrealloc: true, MaxLogFileSize: 64 * 1024}
			seed, err := open(1, 1, "seed", "/replay-sync", opts)
			require.NoError(t, err)
			var once sync.Once
			closeSeed := func() { once.Do(func() { require.NoError(t, seed.close()) }) }
			t.Cleanup(closeSeed)
			_, err = seed.write(pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 5, Commit: 10}, Snapshot: pb.Snapshot{Index: 3, Term: 5}, EntriesToSave: []pb.Entry{{Term: 5, Index: 10, Cmd: []byte("replayed")}}}, nil)
			require.NoError(t, err)
			logNum := seed.mu.logNum
			if tc.historical {
				func() { seed.mu.Lock(); defer seed.mu.Unlock(); require.NoError(t, seed.switchToNewLog()) }()
			}
			closeSeed()
			require.NoError(t, mem.Remove(makeFilename(mem, "/replay-sync", fileTypeIndex, logNum)))
			fs := &interruptedPersistenceFS{FS: mem, t: t, target: makeFilename(mem, "/replay-sync", fileTypeLog, logNum), mode: mode, stage: tc.stage, cause: errors.New("recovered source Sync fault")}
			var recovered any
			var owner *db
			returned := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				owner, err = open(1, 1, "replay", "/replay-sync", &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
				returned = true
			}()
			<-done
			if owner != nil {
				t.Cleanup(func() { require.NoError(t, owner.close()) })
			}
			require.Nil(t, owner, "failed recovered-log Sync cannot publish an owner")
			require.Len(t, fs.opened, 2, "actual replay read and subsequent source-Sync borrows reached")
			for _, file := range fs.opened {
				require.Equal(t, 1, file.closes, "each native source borrow retires once before rescue")
			}
			if tc.stage == "Sync" {
				require.Zero(t, fs.creates, "failed source Sync must suppress index and fresh-log creation")
			} else {
				require.Equal(t, 1, fs.creates, "only interrupted index candidate may be created")
				require.Equal(t, 1, fs.file.closes)
				require.Equal(t, 1, fs.file.writes)
				decoded := newNodeStates()
				require.NoError(t, decoded.load(io.NopCloser(bytes.NewReader(fs.file.attempted))))
				node := decoded.getIndex(1, 1)
				require.Len(t, node.entries.entries, 1)
				require.Equal(t, uint64(10), node.entries.entries[0].start)
				require.Equal(t, uint64(10), node.entries.entries[0].end)
				require.Equal(t, logNum, node.entries.entries[0].fileNum)
				require.Equal(t, uint64(3), node.snapshot.start)
				require.Equal(t, uint64(10), node.state.start)
				_, statErr := mem.Stat(makeFilename(mem, "/replay-sync", fileTypeIndex, logNum))
				require.ErrorIs(t, statErr, os.ErrNotExist, "partial accumulator cannot publish a derived index")
			}
			require.Zero(t, fs.renames)
			if mode == "error" {
				require.True(t, returned)
				require.Nil(t, recovered)
				require.ErrorIs(t, err, fs.cause)
			} else {
				require.False(t, returned)
				if mode == "panic" {
					require.Same(t, fs.cause, recovered)
				} else {
					require.Nil(t, recovered)
				}
			}
		})
	}
}

func TestPersistenceInterruptionRejectsFurtherMutationAndPublication(t *testing.T) {
	for _, stage := range []string{"Write", "Sync"} {
		t.Run(stage, func(t *testing.T) {
			for _, mode := range []string{"error", "panic", "Goexit"} {
				t.Run(mode, func(t *testing.T) {
					mem := vfs.NewStrictMem()
					t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
					require.NoError(t, mem.MkdirAll("/persistence-owner", 0700))
					fs := &interruptedPersistenceFS{FS: mem}
					owner, err := open(1, 1, "owner", "/persistence-owner", &Options{FS: fs, DisablePrealloc: true, MaxLogFileSize: 64 * 1024})
					require.NoError(t, err)
					closed := false
					t.Cleanup(func() {
						if !closed {
							closed = true
							_ = owner.close()
						}
					})
					file := owner.mu.logFile.(*interruptedPersistenceFile)
					file.stage, file.mode, file.cause = stage, mode, errors.New("native persistence fault")
					update := pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 5, Commit: 10}, EntriesToSave: []pb.Entry{{Term: 5, Index: 10, Cmd: []byte("record")}}}
					var state pb.State
					if stage == "Sync" {
						_, err = owner.write(update, nil)
						require.NoError(t, err)
						state = update.State
					}
					done := make(chan struct{})
					var recovered any
					returned := false
					go func() {
						defer close(done)
						defer func() { recovered = recover() }()
						if stage == "Write" {
							_, err = owner.write(update, nil)
						} else {
							err = owner.sync()
						}
						returned = true
					}()
					<-done
					if mode == "error" {
						require.True(t, returned)
						require.Nil(t, recovered)
						require.ErrorIs(t, err, file.cause)
					} else {
						require.False(t, returned)
						if mode == "panic" {
							require.Same(t, file.cause, recovered)
						} else {
							require.Nil(t, recovered)
						}
					}
					// Remove the backend fault; refusal must come from the owner contract.
					file.stage = ""
					writes, syncs, creates := file.writes, file.syncs, fs.creates
					_, terminal := owner.write(pb.Update{ShardID: 1, ReplicaID: 1, State: state}, nil)
					require.Error(t, terminal, "even an equal-state write must reject uncertain storage")
					if mode == "error" {
						require.ErrorIs(t, terminal, file.cause)
					}
					require.ErrorIs(t, owner.sync(), terminal)
					require.ErrorIs(t, owner.removeAll(1, 1), terminal)
					closed = true
					require.ErrorIs(t, owner.close(), terminal)
					require.Equal(t, writes, file.writes, "Close must not complete uncertain record emission")
					require.Equal(t, syncs, file.syncs, "Close must not retry uncertain data durability")
					require.Equal(t, creates, fs.creates, "Close must not publish an index after interrupted persistence")
					require.Equal(t, 1, file.closes)
				})
			}
		})
	}
}

func TestRegularBatchDoesNotAcknowledgeInterruptedSync(t *testing.T) {
	for _, mode := range []string{"clean", "error", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			mem := vfs.NewStrictMem()
			t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
			fs := &interruptedPersistenceFS{FS: mem}
			cfg := config.NodeHostConfig{Expert: config.ExpertConfig{FS: fs, LogDB: config.GetTinyMemLogDBConfig()}}
			cfg.Expert.LogDB.KVWriteBufferSize = 4096
			cfg.Expert.LogDB.DisablePrealloc = true
			cfg.Expert.LogDB.MaxLogFileSize = 64 * 1024
			require.NoError(t, cfg.Prepare())
			owner, err := CreateTan(cfg, nil, []string{"/regular-outcome"}, nil)
			require.NoError(t, err)
			t.Cleanup(func() { _ = owner.Close() })
			physical, err := owner.getDB(1, 1)
			require.NoError(t, err)
			file := physical.mu.logFile.(*interruptedPersistenceFile)
			if mode != "clean" {
				file.stage, file.mode, file.cause = "Sync", mode, errors.New("worker Sync fault")
			}
			update := pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 1, Commit: 10}, EntriesToSave: []pb.Entry{{Term: 1, Index: 10, Cmd: []byte("one record")}}}
			err = owner.SaveRaftState([]pb.Update{update}, 1)
			if mode == "clean" {
				require.NoError(t, err)
			} else {
				require.Error(t, err, "unsuccessful worker Sync must never acknowledge successful batch persistence")
				if mode == "error" {
					require.ErrorIs(t, err, file.cause)
				}
			}
			require.Equal(t, 2, file.syncs, "candidate preparation and actual final worker Sync both reached")
		})
	}
}
