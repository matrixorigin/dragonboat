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

package tan

import (
	"bytes"
	"errors"
	"sort"
	"testing"

	"github.com/lni/dragonboat/v4/config"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/vfs"
	"github.com/stretchr/testify/require"
)

func TestMultiplexedBatchSurvivesPowerLossAcrossSegments(t *testing.T) {
	mem := vfs.NewStrictMem()
	t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
	cfg := config.NodeHostConfig{Expert: config.ExpertConfig{FS: mem, LogDB: config.GetTinyMemLogDBConfig()}}
	cfg.Expert.LogDB.KVWriteBufferSize = 4096
	cfg.Expert.LogDB.MaxLogFileSize = 128
	cfg.Expert.LogDB.DisablePrealloc = true
	require.NoError(t, cfg.Prepare())
	owner, err := CreateLogMultiplexedTan(cfg, nil, []string{"/batch-durable"}, nil)
	require.NoError(t, err)
	closed := false
	closeOwner := func() {
		if !closed {
			closed = true
			require.NoError(t, owner.Close())
		}
	}
	t.Cleanup(closeOwner)
	updates := []pb.Update{
		{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 5, Commit: 10}, Snapshot: pb.Snapshot{Term: 5, Index: 9}, EntriesToSave: []pb.Entry{{Term: 5, Index: 10, Cmd: bytes.Repeat([]byte("a"), 200)}}},
		{ShardID: 17, ReplicaID: 1, State: pb.State{Term: 6, Commit: 20}, Snapshot: pb.Snapshot{Term: 6, Index: 19}, EntriesToSave: []pb.Entry{{Term: 6, Index: 20, Cmd: bytes.Repeat([]byte("b"), 200)}}},
		{ShardID: 33, ReplicaID: 1, State: pb.State{Term: 7, Commit: 30}, Snapshot: pb.Snapshot{Term: 7, Index: 29}, EntriesToSave: []pb.Entry{{Term: 7, Index: 30, Cmd: bytes.Repeat([]byte("c"), 200)}}},
	}
	require.NoError(t, owner.SaveRaftState(updates, 1))
	physical, err := owner.getDB(1, 1)
	require.NoError(t, err)
	var logNumbers []fileNum
	func() {
		physical.mu.Lock()
		defer physical.mu.Unlock()
		for number := range physical.mu.versions.currentVersion().files {
			logNumbers = append(logNumbers, number)
		}
	}()
	sort.Slice(logNumbers, func(i, j int) bool { return logNumbers[i] < logNumbers[j] })
	require.Len(t, logNumbers, 3, "one acknowledged batch must cross three physical log segments")
	// Inspect the two sealed indexes before teardown can publish another one.
	// Each segment contains one literal update; historical entries must not be
	// copied into later segment indexes even if aggregate replay still works.
	for i, number := range logNumbers[:2] {
		func() {
			file, err := mem.Open(makeFilename(mem, physical.dirname, fileTypeIndex, number))
			require.NoError(t, err)
			defer func() { require.NoError(t, file.Close()) }()
			decoded := newNodeStates()
			require.NoError(t, decoded.load(file))
			entryCount := 0
			for _, node := range decoded.indexes {
				entryCount += len(node.entries.entries)
			}
			require.Equal(t, 1, entryCount, "sealed index must contain only its segment's entry")
			node := decoded.getIndex(updates[i].ShardID, 1)
			require.Len(t, node.entries.entries, 1)
			entry := node.entries.entries[0]
			require.Equal(t, number, entry.fileNum)
			require.Equal(t, updates[i].EntriesToSave[0].Index, entry.start)
			require.Equal(t, updates[i].EntriesToSave[0].Index, entry.end)
		}()
	}
	// Teardown is forbidden from repairing the acknowledged durability boundary.
	mem.SetIgnoreSyncs(true)
	closeOwner()
	mem.ResetToSyncedState()
	mem.SetIgnoreSyncs(false)
	reopened, err := CreateLogMultiplexedTan(cfg, nil, []string{"/batch-durable"}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	requireDurableRaftUpdates(t, reopened, updates)
}

func requireDurableRaftUpdates(t *testing.T, reopened *LogDB, updates []pb.Update) {
	t.Helper()
	for _, update := range updates {
		index := update.EntriesToSave[0].Index
		entries, _, err := reopened.IterateEntries(nil, 0, update.ShardID, 1, index, index+1, ^uint64(0))
		require.NoError(t, err)
		require.Equal(t, update.EntriesToSave, entries, "exact entries from every old and current segment survive")
		state, err := reopened.ReadRaftState(update.ShardID, 1, index-1)
		require.NoError(t, err)
		require.Equal(t, update.State, state.State)
		snapshot, err := reopened.GetSnapshot(update.ShardID, 1)
		require.NoError(t, err)
		require.Equal(t, update.Snapshot, snapshot)
	}
}

type unsyncedSuffixFS struct {
	vfs.FS
	fail                   bool
	cause                  error
	syncs, writes, creates int
}

type unsyncedSuffixFile struct {
	vfs.File
	fs *unsyncedSuffixFS
}

func (f *unsyncedSuffixFile) Sync() error {
	f.fs.syncs++
	if f.fs.fail {
		return f.fs.cause
	}
	return f.File.Sync()
}

func (f *unsyncedSuffixFile) Write(p []byte) (int, error) {
	f.fs.writes++
	return f.File.Write(p)
}

func (f *unsyncedSuffixFS) Create(name string) (vfs.File, error) {
	f.creates++
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	kind, _, ok := parseFilename(f.FS, name)
	if ok && kind == fileTypeLog {
		return &unsyncedSuffixFile{File: file, fs: f}, nil
	}
	return file, nil
}

func TestReplayMakesCompleteUnsyncedSuffixDurableBeforeIndex(t *testing.T) {
	mem := vfs.NewStrictMem()
	t.Cleanup(func() { vfs.ReportLeakedFD(mem, t) })
	fs := &unsyncedSuffixFS{FS: mem, cause: errors.New("final log Sync failed")}
	cfg := config.NodeHostConfig{Expert: config.ExpertConfig{FS: fs, LogDB: config.GetTinyMemLogDBConfig()}}
	cfg.Expert.LogDB.KVWriteBufferSize = 4096
	cfg.Expert.LogDB.MaxLogFileSize = 128
	cfg.Expert.LogDB.DisablePrealloc = true
	require.NoError(t, cfg.Prepare())
	owner, err := CreateLogMultiplexedTan(cfg, nil, []string{"/suffix-durable"}, nil)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			closed = true
			_ = owner.Close()
		}
	})
	// Acquire the initial candidate before injecting the final data-Sync fault.
	_, err = owner.getDB(1, 1)
	require.NoError(t, err)
	old := pb.Update{ShardID: 1, ReplicaID: 1, State: pb.State{Term: 5, Commit: 10}, Snapshot: pb.Snapshot{Term: 5, Index: 9}, EntriesToSave: []pb.Entry{{Term: 5, Index: 10, Cmd: []byte("complete unsynced suffix")}}}
	fs.fail = true
	require.ErrorIs(t, owner.SaveRaftState([]pb.Update{old}, 1), fs.cause)
	fs.fail = false
	physical, err := owner.getDB(1, 1)
	require.NoError(t, err)
	writes, syncs, creates := fs.writes, fs.syncs, fs.creates
	// A healthy backend must not make an uncertain owner writable again.
	equalState := pb.Update{ShardID: 1, ReplicaID: 1, State: old.State}
	require.ErrorIs(t, owner.SaveRaftState([]pb.Update{equalState}, 1), fs.cause)
	require.ErrorIs(t, physical.sync(), fs.cause)
	require.ErrorIs(t, physical.removeAll(1, 1), fs.cause)
	closed = true
	require.ErrorIs(t, owner.Close(), fs.cause)
	require.Equal(t, writes, fs.writes, "terminal admission and Close must not finish uncertain emission")
	require.Equal(t, syncs, fs.syncs, "terminal admission and Close must not repair uncertain durability")
	require.Equal(t, creates, fs.creates, "terminal Close must not publish a new index")
	require.ErrorIs(t, physical.sync(), ErrClosed)
	require.Equal(t, syncs, fs.syncs, "closed owner rejects Sync before borrowing retired descriptor")
	// Reopen while complete unsynced bytes are still visible, not after reset.
	replayed, err := CreateLogMultiplexedTan(cfg, nil, []string{"/suffix-durable"}, nil)
	require.NoError(t, err)
	replayClosed := false
	closeReplay := func() {
		if !replayClosed {
			replayClosed = true
			require.NoError(t, replayed.Close())
		}
	}
	t.Cleanup(closeReplay)
	fresh := pb.Update{ShardID: 17, ReplicaID: 1, State: pb.State{Term: 6, Commit: 20}, Snapshot: pb.Snapshot{Term: 6, Index: 19}, EntriesToSave: []pb.Entry{{Term: 6, Index: 20, Cmd: []byte("fresh segment")}}}
	require.NoError(t, replayed.SaveRaftState([]pb.Update{fresh}, 1))
	requireDurableRaftUpdates(t, replayed, []pb.Update{old, fresh})
	mem.SetIgnoreSyncs(true)
	closeReplay()
	mem.ResetToSyncedState()
	mem.SetIgnoreSyncs(false)
	recovered, err := CreateLogMultiplexedTan(cfg, nil, []string{"/suffix-durable"}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, recovered.Close()) })
	requireDurableRaftUpdates(t, recovered, []pb.Update{old, fresh})
}
