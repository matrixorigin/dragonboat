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

package transport

import (
	"errors"
	"io"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/registry"
	"github.com/lni/dragonboat/v4/internal/rsm"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/settings"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	pb "github.com/lni/dragonboat/v4/raftpb"
	"github.com/lni/goutils/leaktest"
)

type constructorTransport struct {
	raftio.ITransport
	handle       io.Closer
	nameFailure  func()
	start        func() error
	beforeClose  func()
	chunkHandler raftio.ChunkHandler
	closeErr     error
	closes       int32
}

func (c *constructorTransport) Name() string {
	if c.nameFailure != nil {
		c.nameFailure()
	}
	return c.ITransport.Name()
}
func (c *constructorTransport) Start() error { return c.start() }
func (c *constructorTransport) Close() error {
	atomic.AddInt32(&c.closes, 1)
	if c.beforeClose != nil {
		c.beforeClose()
	}
	if err := c.handle.Close(); err != nil {
		return err
	}
	if err := c.ITransport.Close(); err != nil {
		return err
	}
	return c.closeErr
}

type constructorTransportFactory struct{ c *constructorTransport }

func (f constructorTransportFactory) Validate(string) bool { return true }
func (f constructorTransportFactory) Create(c config.NodeHostConfig, h raftio.MessageHandler, ch raftio.ChunkHandler) raftio.ITransport {
	f.c.ITransport = NewNOOPTransport(c, h, ch)
	f.c.chunkHandler = ch
	return f.c
}

func TestTransportCloseDrainsSnapshotReceiver(t *testing.T) {
	defer leaktest.AfterTest(t)()
	primary := errors.New("start failed after receiver admission")
	secondary := errors.New("secondary transport close error")
	for _, startErr := range []error{nil, primary} {
		name := "published_close"
		if startErr != nil {
			name = "start_error"
		}
		t.Run(name, func(t *testing.T) {
			for _, closeErr := range []error{nil, secondary} {
				name := "close_ok"
				if closeErr != nil {
					name = "close_error"
				}
				t.Run(name, func(t *testing.T) {
					fs := vfs.NewMemFS()
					dirs := newTestSnapshotDir(fs)
					handle, err := fs.Create("/transport-handle")
					if err != nil {
						t.Fatal(err)
					}
					child := &constructorTransport{handle: handle, closeErr: closeErr}
					var owner *Transport
					started := false
					t.Cleanup(func() {
						if atomic.LoadInt32(&child.closes) == 0 {
							if owner != nil {
								_ = owner.Close()
							} else if started {
								_ = child.Close()
							} else {
								_ = handle.Close()
							}
						}
						dirs.cleanup()
						vfs.ReportLeakedFD(fs, t)
					})
					// Use a real snapshot header and one-byte payload with validation on.
					writer, err := rsm.NewSnapshotWriter("/source-snapshot", pb.NoCompression, fs)
					if err != nil {
						t.Fatal(err)
					}
					func() {
						defer func() {
							if err := writer.Close(); err != nil {
								t.Error(err)
							}
						}()
						if _, err := writer.Write([]byte{1}); err != nil {
							t.Fatal(err)
						}
					}()
					file, err := fs.Open("/source-snapshot")
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = file.Close() })
					raw, err := io.ReadAll(file)
					if err != nil {
						t.Fatal(err)
					}
					chunk := pb.Chunk{DeploymentId: settings.UnmanagedDeploymentID, BinVer: raftio.TransportBinVersion,
						ShardID: 100, ReplicaID: 2, From: 12, Index: 1, Term: 1, Filepath: "snapshot.gbsnap",
						ChunkCount: 2, FileChunkCount: 2, FileSize: uint64(len(raw)),
						Data: raw[:rsm.HeaderSize+1], ChunkSize: rsm.HeaderSize + 1}
					root := dirs.GetSnapshotRootDir(chunk.ShardID, chunk.ReplicaID)
					if err := fs.MkdirAll(root, 0755); err != nil {
						t.Fatal(err)
					}
					drain, done := make(chan struct{}), make(chan struct{})
					accepted := false
					child.start = func() error {
						started = true
						go func() {
							defer close(done)
							<-drain
							accepted = child.chunkHandler(chunk)
						}()
						return startErr
					}
					child.beforeClose = func() { close(drain); <-done }
					cfg := config.NodeHostConfig{RaftAddress: "127.0.0.1:1"}
					cfg.Expert.TransportFactory = constructorTransportFactory{c: child}
					env, err := server.NewEnv(cfg, fs)
					if err != nil {
						t.Fatal(err)
					}
					nodes := registry.NewNodeRegistry(settings.Soft.StreamConnections, nil)
					owner, err = NewTransport(cfg, newTestMessageHandler(), env, nodes, dirs.GetSnapshotRootDir, &dummyTransportEvent{}, fs)
					if startErr != nil {
						if owner != nil || err != startErr {
							t.Fatalf("failed construction: owner=%v error=%v", owner, err)
						}
					} else {
						if owner == nil || err != nil || atomic.LoadInt32(&child.closes) != 0 {
							t.Fatalf("successful handoff: owner=%v error=%v", owner, err)
						}
						if err := owner.Close(); err != closeErr {
							t.Errorf("Close error=%v want %v", err, closeErr)
						}
					}
					if !accepted || atomic.LoadInt32(&child.closes) != 1 {
						t.Error("receiver must deliver its last valid chunk and close once")
					}
					entries, err := fs.List(root)
					if err != nil {
						t.Fatal(err)
					}
					if len(entries) != 0 {
						t.Errorf("snapshot temp storage survived terminal Close: %v", entries)
					}
				})
			}
		})
	}
}

func TestTransportConstructorUnwind(t *testing.T) {
	defer leaktest.AfterTest(t)()
	primary := errors.New("transport acquisition failed")
	secondary := errors.New("secondary transport close error")
	cases := []struct {
		name        string
		nameFailure func()
		start       func() error
		panicValue  any
		returned    bool
		success     bool
	}{
		{name: "start_error", start: func() error { return primary }, returned: true},
		{name: "name_panic", nameFailure: func() { panic(primary) }, start: func() error { return nil }, panicValue: primary},
		{name: "start_goexit", start: func() error { runtime.Goexit(); return nil }},
		{name: "success", start: func() error { return nil }, returned: true, success: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, closeErr := range []error{nil, secondary} {
				name := "close_ok"
				if closeErr != nil {
					name = "close_error"
				}
				t.Run(name, func(t *testing.T) {
					fs := vfs.NewMemFS()
					handle, err := fs.Create("/transport-handle")
					if err != nil {
						t.Fatal(err)
					}
					child := &constructorTransport{handle: handle, nameFailure: tc.nameFailure, start: tc.start, closeErr: closeErr}
					t.Cleanup(func() {
						// Release only an unclosed handle after the ownership oracle.
						if atomic.LoadInt32(&child.closes) == 0 {
							_ = handle.Close()
						}
						vfs.ReportLeakedFD(fs, t)
					})
					cfg := config.NodeHostConfig{RaftAddress: "127.0.0.1:1"}
					cfg.Expert.TransportFactory = constructorTransportFactory{c: child}
					env, err := server.NewEnv(cfg, fs)
					if err != nil {
						t.Fatal(err)
					}
					nodes := registry.NewNodeRegistry(settings.Soft.StreamConnections, nil)
					dirs := newTestSnapshotDir(fs)
					var owner *Transport
					var gotErr error
					var gotPanic any
					returned := false
					done := make(chan struct{})
					go func() {
						defer close(done)
						defer func() { gotPanic = recover() }()
						owner, gotErr = NewTransport(cfg, newTestMessageHandler(), env, nodes, dirs.GetSnapshotRootDir, &dummyTransportEvent{}, fs)
						returned = true
					}()
					<-done
					if returned != tc.returned || gotPanic != tc.panicValue {
						t.Errorf("returned=%t panic=%#v; want returned=%t panic=%#v", returned, gotPanic, tc.returned, tc.panicValue)
					}
					if tc.success {
						if owner == nil {
							t.Fatalf("success returned nil: %v", gotErr)
						}
						if gotErr != nil || owner.trans != child || atomic.LoadInt32(&child.closes) != 0 {
							t.Error("successful construction did not transfer ownership intact")
						}
						if err := owner.Close(); err != closeErr {
							t.Errorf("public Close error=%v want %v", err, closeErr)
						}
					} else {
						if owner != nil {
							_ = owner.Close()
							t.Fatal("failed construction published transport")
						}
						if tc.returned && gotErr != primary {
							t.Errorf("original error replaced: %v", gotErr)
						}
					}
					if n := atomic.LoadInt32(&child.closes); n != 1 {
						t.Errorf("child Close calls=%d want1", n)
					}
				})
			}
		})
	}
}
