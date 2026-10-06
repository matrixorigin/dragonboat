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
