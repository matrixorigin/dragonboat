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
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/stretchr/testify/require"
)

type interruptedRetryIO struct {
	config.ArchiveIO
	mode             string
	cause            error
	attempts         []string
	entered, release chan struct{}
}

func (a *interruptedRetryIO) Delete(_ context.Context, _ string, files ...string) error {
	a.attempts = append(a.attempts, files...)
	if files[0] == "failed" {
		return a.cause
	}
	if files[0] == "interrupted" {
		close(a.entered)
		<-a.release
		switch a.mode {
		case "panic":
			panic(a.cause)
		case "Goexit":
			runtime.Goexit()
		default:
			return a.cause
		}
	}
	return nil
}
func TestRetryRestoresOnlyUncompletedTail(t *testing.T) {
	for _, mode := range []string{"error", "panic", "Goexit"} {
		t.Run(mode, func(t *testing.T) {
			cause := errors.New("retry interruption")
			aio := &interruptedRetryIO{mode: mode, cause: cause, entered: make(chan struct{}), release: make(chan struct{})}
			a := &archiver{ArchiveIO: aio, ctx: context.Background()}
			a.mu.gcFailedQueue = []string{"successful", "failed", "interrupted", "unattempted"}
			done := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(aio.release) }) }
			t.Cleanup(func() {
				release()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("retry worker did not join during cleanup")
				}
			})
			var recovered any
			returned := false
			go func() {
				defer close(done)
				defer func() { recovered = recover() }()
				a.gcFailedRetry()
				returned = true
			}()
			select {
			case <-aio.entered:
			case <-time.After(time.Second):
				t.Fatal("interruption boundary not reached")
			}
			a.mu.Lock()
			a.mu.gcFailedQueue = append(a.mu.gcFailedQueue, "new-one", "new-two")
			a.mu.Unlock()
			release()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("retry exit did not join")
			}
			if mode == "error" {
				require.Nil(t, recovered)
				require.True(t, returned)
				require.Equal(t, []string{"successful", "failed", "interrupted", "unattempted"}, aio.attempts)
				require.Equal(t, []string{"failed", "new-one", "new-two", "interrupted"}, a.mu.gcFailedQueue)
			} else {
				require.False(t, returned)
				require.Equal(t, []string{"successful", "failed", "interrupted"}, aio.attempts)
				require.Equal(t, []string{"failed", "new-one", "new-two", "interrupted", "unattempted"}, a.mu.gcFailedQueue, "successful items retired, incomplete tail restored")
				if mode == "panic" {
					require.Same(t, cause, recovered)
				} else {
					require.Nil(t, recovered)
				}
			}
		})
	}
}
