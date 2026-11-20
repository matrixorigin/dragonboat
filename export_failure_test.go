// Copyright 2026 Matrix Origin
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

package dragonboat

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/internal/vfs"
	pb "github.com/lni/dragonboat/v4/raftpb"
)

type failExportRenameFS struct {
	vfs.IFS
	fail atomic.Bool
}

type failExportCleanupFS struct {
	vfs.IFS
	fail atomic.Bool
}

func (fs *failExportCleanupFS) RemoveAll(path string) error {
	if strings.HasSuffix(path, ".generating") && fs.fail.CompareAndSwap(true, false) {
		return syscall.EIO
	}
	return fs.IFS.RemoveAll(path)
}

func (fs *failExportRenameFS) Rename(oldname, newname string) error {
	if strings.HasSuffix(oldname, ".generating") &&
		strings.Contains(newname, "exported-snapshot-error") && fs.fail.CompareAndSwap(true, false) {
		return syscall.EIO
	}
	return fs.IFS.Rename(oldname, newname)
}

func TestExportCommitErrorReturnsWithoutStoppingNodeHost(t *testing.T) {
	baseFS := vfs.GetTestFS()
	fs := &failExportRenameFS{IFS: baseFS}
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			n, ok := nh.getShard(1)
			if !ok {
				t.Fatal("shard missing")
			}
			originalFS := n.snapshotter.fs
			n.snapshotter.fs = fs
			defer func() { n.snapshotter.fs = originalFS }()
			path := "exported-snapshot-error"
			if err := fs.MkdirAll(path, 0755); err != nil {
				t.Fatal(err)
			}
			defer fs.RemoveAll(path)
			propose := func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := nh.SyncPropose(ctx, nh.GetNoOPSession(1), []byte("record")); err != nil {
					t.Fatal(err)
				}
			}
			propose()
			fs.fail.Store(true)
			opt := SnapshotOption{Exported: true, ExportPath: path}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := nh.SyncRequestSnapshot(ctx, 1, opt)
			cancel()
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("expected export EIO, got %v", err)
			}
			var exportErr *SnapshotExportError
			if !errors.As(err, &exportErr) {
				t.Fatalf("expected completed export failure, got %T", err)
			}
			if fs.fail.Load() {
				t.Fatal("export rename injection was not reached")
			}
			ss, err := nh.mu.logdb.GetSnapshot(1, 1)
			if err != nil || !pb.IsEmptySnapshot(ss) {
				t.Fatalf("export changed internal snapshot: %v, %v", ss, err)
			}
			// The failed exported task must release the save slot so the same
			// quiescent shard can export at the same index on retry.
			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			_, err = nh.SyncRequestSnapshot(ctx, 1, opt)
			cancel()
			if err != nil {
				t.Fatalf("retry export failed: %v", err)
			}
		},
	}
	runNodeHostTest(t, to, baseFS)
}

func TestExportAbortCleanupErrorReturnsWithoutStoppingNodeHost(t *testing.T) {
	baseFS := vfs.GetTestFS()
	fs := &failExportCleanupFS{IFS: baseFS}
	to := &testOption{
		defaultTestNode: true,
		tf: func(nh *NodeHost) {
			n, ok := nh.getShard(1)
			if !ok {
				t.Fatal("shard missing")
			}
			originalFS := n.snapshotter.fs
			n.snapshotter.fs = fs
			defer func() { n.snapshotter.fs = originalFS }()
			path := "exported-snapshot-abort-cleanup"
			if err := fs.MkdirAll(path, 0755); err != nil {
				t.Fatal(err)
			}
			defer fs.RemoveAll(path)
			propose := func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if _, err := nh.SyncPropose(ctx, nh.GetNoOPSession(1), []byte("record")); err != nil {
					t.Fatal(err)
				}
			}
			propose()
			opt := SnapshotOption{Exported: true, ExportPath: path}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, err := nh.SyncRequestSnapshot(ctx, 1, opt)
			cancel()
			if err != nil {
				t.Fatalf("initial export failed: %v", err)
			}
			// A second export at the same index aborts because the final
			// directory exists. Failure to remove its temporary directory
			// must reach the caller without panicking the snapshot worker.
			fs.fail.Store(true)
			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			_, err = nh.SyncRequestSnapshot(ctx, 1, opt)
			cancel()
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("expected cleanup EIO, got %v", err)
			}
			var exportErr *SnapshotExportError
			if !errors.As(err, &exportErr) {
				t.Fatalf("expected export failure, got %T", err)
			}
			if fs.fail.Load() {
				t.Fatal("export cleanup injection was not reached")
			}
			propose()
		},
	}
	runNodeHostTest(t, to, baseFS)
}
