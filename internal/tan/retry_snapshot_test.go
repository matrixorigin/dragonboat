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
	"sync"
	"testing"
	"time"

	"github.com/lni/dragonboat/v4/config"
	"github.com/stretchr/testify/require"
)

type retrySnapshotIO struct {
	config.ArchiveIO
	entered, release chan struct{}
	attempts         []string
}

func (a *retrySnapshotIO) Delete(_ context.Context, _ string, files ...string) error {
	a.attempts = append(a.attempts, files...)
	if len(a.attempts) == 1 {
		close(a.entered)
		<-a.release
	}
	return nil
}
func TestRetryOwnsItsQueueSnapshot(t *testing.T) {
	aio := &retrySnapshotIO{entered: make(chan struct{}), release: make(chan struct{})}
	a := &archiver{ArchiveIO: aio, ctx: context.Background()}
	a.mu.gcFailedQueue = []string{"old-one", "old-two"}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(aio.release) }) }
	done := make(chan struct{})
	go func() { defer close(done); a.gcFailedRetry() }()
	t.Cleanup(func() {
		release()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("retry worker did not join")
		}
	})
	select {
	case <-aio.entered:
	case <-time.After(time.Second):
		t.Fatal("original retry admission not reached")
	}
	a.mu.Lock()
	a.mu.gcFailedQueue = append(a.mu.gcFailedQueue, "new-one", "new-two")
	a.mu.Unlock()
	release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retry did not finish")
	}
	require.Equal(t, []string{"old-one", "old-two"}, aio.attempts, "in-flight retry must own its original snapshot")
	require.Equal(t, []string{"new-one", "new-two"}, a.mu.gcFailedQueue, "new failures remain queued for the next retry")
}
