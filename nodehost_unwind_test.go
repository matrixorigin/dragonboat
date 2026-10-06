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

package dragonboat

import (
	"io"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/internal/server"
	"github.com/lni/dragonboat/v4/internal/transport"
	"github.com/lni/dragonboat/v4/internal/vfs"
	"github.com/lni/dragonboat/v4/raftio"
	"github.com/lni/goutils/leaktest"
)

type constructorLockFS struct {
	vfs.IFS
	locks    []*constructorLock
	closeErr error
}

type constructorLock struct {
	io.Closer
	closes   int32
	closeErr error
}

func (f *constructorLockFS) Lock(name string) (io.Closer, error) {
	l, err := f.IFS.Lock(name)
	if err != nil {
		return nil, err
	}
	c := &constructorLock{Closer: l, closeErr: f.closeErr}
	f.locks = append(f.locks, c)
	return c, nil
}

func (c *constructorLock) Close() error {
	atomic.AddInt32(&c.closes, 1)
	if err := c.Closer.Close(); err != nil {
		return err
	}
	return c.closeErr
}

func TestNodeHostConstructorUnwind(t *testing.T) {
	defer leaktest.AfterTest(t)()
	failure := errors.New("logdb acquisition failed")
	modes := []struct {
		name       string
		run        func() error
		panicValue interface{}
		returned   bool
	}{
		{"error", func() error { return failure }, nil, true},
		{"string_panic", func() error { panic("constructor unwind") }, "constructor unwind", false},
		{"error_panic", func() error { panic(failure) }, failure, false},
		{"goexit", func() error { runtime.Goexit(); return nil }, nil, false},
	}
	for _, m := range modes {
		t.Run(m.name, func(t *testing.T) {
			for _, badClose := range []bool{false, true} {
				name := "close_ok"
				if badClose {
					name = "close_error"
				}
				t.Run(name, func(t *testing.T) {
					fs := &constructorLockFS{IFS: vfs.NewMemFS()}
					if badClose {
						fs.closeErr = errors.New("secondary lock close error")
					}
					// A fallback releases only leaked locks after the ownership assertion, so a
					// failing regression does not leak its own fixture or conceal missing Close.
					t.Cleanup(func() {
						for _, l := range fs.locks {
							if atomic.LoadInt32(&l.closes) == 0 {
								_ = l.Closer.Close()
							}
						}
						vfs.ReportLeakedFD(fs.IFS, t)
					})
					listener := &testSysEventListener{}
					cfg := config.NodeHostConfig{NodeHostDir: "/constructor", RTTMillisecond: 10, RaftAddress: "127.0.0.1:1", SystemEventListener: listener}
					cfg.Expert.FS = fs
					called, returned := false, false
					var gotPanic interface{}
					var gotErr error
					var owner *NodeHost
					cfg.Expert.LogDBFactory = &testLogDBFactory2{name: "unwind", f: func(config.NodeHostConfig, config.LogDBCallback, []string, []string) (raftio.ILogDB, error) {
						called = true
						return nil, m.run()
					}}
					done := make(chan struct{})
					go func() {
						defer close(done)
						defer func() { gotPanic = recover() }()
						owner, gotErr = NewNodeHost(cfg)
						returned = true
					}()
					<-done
					if owner != nil {
						owner.Close()
						t.Fatal("failed construction published a host")
					}
					if !called || returned != m.returned || gotPanic != m.panicValue {
						t.Errorf("called=%t returned=%t panic=%#v; want returned=%t panic=%#v", called, returned, gotPanic, m.returned, m.panicValue)
					}
					if m.returned && gotErr != failure {
						t.Errorf("error identity lost: got %v", gotErr)
					}
					if len(fs.locks) != 1 {
						t.Fatalf("expected one directory lock, got %d", len(fs.locks))
					}
					if n := atomic.LoadInt32(&fs.locks[0].closes); n != 1 {
						t.Errorf("lock Close calls=%d, want 1", n)
					}
					// Both the directory lock and listener worker are released before unwind
					// completes; no transport is created by this fixture.
					listener.mu.Lock()
					shutdowns := listener.nodeHostShuttingdown
					listener.mu.Unlock()
					if shutdowns != 1 {
						t.Errorf("shutdown notifications=%d, want 1", shutdowns)
					}
				})
			}
		})
	}
}

func TestNodeHostCloseBeforeWorkersInitialized(t *testing.T) {
	fs := vfs.NewMemFS()
	cfg := config.NodeHostConfig{NodeHostDir: "/partial"}
	cfg.Expert.FS = fs
	env, err := server.NewEnv(cfg, fs)
	if err != nil {
		t.Fatal(err)
	}
	defer vfs.ReportLeakedFD(fs, t)
	if err = fs.MkdirAll(cfg.NodeHostDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err = env.LockNodeHostDir(); err != nil {
		t.Fatal(err)
	}
	nh := &NodeHost{env: env, nhConfig: cfg, fs: fs}
	nh.Close()
	if atomic.LoadInt32(&nh.closed) != 1 {
		t.Fatal("partial host was not closed")
	}
}

func TestNodeHostRejectsUnsafeWorkerCountBeforeAcquisition(t *testing.T) {
	fs := &constructorLockFS{IFS: vfs.NewMemFS()}
	defer vfs.ReportLeakedFD(fs.IFS, t)
	called := false
	cfg := config.NodeHostConfig{NodeHostDir: "/invalid-engine", RTTMillisecond: 10, RaftAddress: "127.0.0.1:1"}
	cfg.Expert.FS = fs
	cfg.Expert.Engine = config.GetDefaultEngineConfig()
	cfg.Expert.Engine.CloseShards = ^uint64(0)
	cfg.Expert.LogDBFactory = &testLogDBFactory2{f: func(config.NodeHostConfig, config.LogDBCallback, []string, []string) (raftio.ILogDB, error) {
		called = true
		return nil, errors.New("factory must not run")
	}}
	owner, err := NewNodeHost(cfg)
	if owner != nil {
		owner.Close()
		t.Fatal("invalid configuration published a host")
	}
	if err == nil || called || len(fs.locks) != 0 {
		t.Fatalf("unsafe engine must fail before acquisition: error=%v factory=%t locks=%d", err, called, len(fs.locks))
	}
}

type unwindNodeHostTransport struct {
	raftio.ITransport
	handle      io.Closer
	nameFailure func()
	goexitStart bool
	closes      int32
}

func (c *unwindNodeHostTransport) Name() string {
	if c.nameFailure != nil {
		c.nameFailure()
	}
	return c.ITransport.Name()
}
func (c *unwindNodeHostTransport) Start() error {
	if c.goexitStart {
		runtime.Goexit()
	}
	return c.ITransport.Start()
}
func (c *unwindNodeHostTransport) Close() error {
	atomic.AddInt32(&c.closes, 1)
	if err := c.handle.Close(); err != nil {
		return err
	}
	if err := c.ITransport.Close(); err != nil {
		return err
	}
	return errors.New("secondary transport close error")
}

type unwindNodeHostTransportFactory struct{ child *unwindNodeHostTransport }

func (f unwindNodeHostTransportFactory) Validate(string) bool { return true }
func (f unwindNodeHostTransportFactory) Create(cfg config.NodeHostConfig, h raftio.MessageHandler, ch raftio.ChunkHandler) raftio.ITransport {
	handle, err := cfg.Expert.FS.Create(cfg.Expert.FS.PathJoin(cfg.NodeHostDir, "transport-handle"))
	if err != nil {
		panic(err)
	}
	f.child.handle = handle
	f.child.ITransport = transport.NewNOOPTransport(cfg, h, ch)
	return f.child
}

type unwindNodeHostLogDB struct {
	noopLogDB
	closes int32
}

func (l *unwindNodeHostLogDB) Close() error { atomic.AddInt32(&l.closes, 1); return nil }

func TestNodeHostTransportConstructorUnwind(t *testing.T) {
	defer leaktest.AfterTest(t)()
	failure := errors.New("transport name failed")
	for _, goexit := range []bool{false, true} {
		name := "name_panic"
		if goexit {
			name = "start_goexit"
		}
		t.Run(name, func(t *testing.T) {
			fs := &constructorLockFS{IFS: vfs.NewMemFS()}
			child := &unwindNodeHostTransport{goexitStart: goexit}
			if !goexit {
				child.nameFailure = func() { panic(failure) }
			}
			t.Cleanup(func() {
				if child.handle != nil && atomic.LoadInt32(&child.closes) == 0 {
					_ = child.handle.Close()
				}
				for _, l := range fs.locks {
					if atomic.LoadInt32(&l.closes) == 0 {
						_ = l.Closer.Close()
					}
				}
				vfs.ReportLeakedFD(fs.IFS, t)
			})
			ldb := &unwindNodeHostLogDB{}
			listener := &testSysEventListener{}
			cfg := config.NodeHostConfig{NodeHostDir: "/constructor", WALDir: "/wal", RTTMillisecond: 10, RaftAddress: "127.0.0.1:1", SystemEventListener: listener}
			cfg.Expert.FS = fs
			cfg.Expert.Engine = config.EngineConfig{ExecShards: 1, CommitShards: 1, ApplyShards: 1, SnapshotShards: 1, CloseShards: 1}
			cfg.Expert.LogDBFactory = &testLogDBFactory{ldb: ldb}
			cfg.Expert.TransportFactory = unwindNodeHostTransportFactory{child: child}
			var owner *NodeHost
			var got any
			returned := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				defer func() { got = recover() }()
				owner, _ = NewNodeHost(cfg)
				returned = true
			}()
			<-done
			if owner != nil {
				owner.Close()
				t.Fatal("failed constructor published host")
			}
			if returned {
				t.Error("abrupt constructor returned normally")
			}
			if (!goexit && got != failure) || (goexit && got != nil) {
				t.Errorf("original unwind lost: %#v", got)
			}
			if n := atomic.LoadInt32(&child.closes); n != 1 {
				t.Errorf("transport closes=%d want1", n)
			}
			if n := atomic.LoadInt32(&ldb.closes); n != 1 {
				t.Errorf("logdb closes=%d want1", n)
			}
			if len(fs.locks) != 2 {
				t.Fatalf("locks=%d want2", len(fs.locks))
			}
			for _, l := range fs.locks {
				if n := atomic.LoadInt32(&l.closes); n != 1 {
					t.Errorf("lock closes=%d want1", n)
				}
			}
			listener.mu.Lock()
			shutdowns := listener.nodeHostShuttingdown
			listener.mu.Unlock()
			if shutdowns != 1 {
				t.Errorf("shutdowns=%d want1", shutdowns)
			}
		})
	}
}
