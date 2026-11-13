//go:build !plan9 && !solaris

package iclouddrive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rclone/rclone/backend/iclouddrive/api"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func stopDownloads(b *downloadBatcher) { b.cancel(); b.wg.Wait() }

func TestDownloadBatcherConcurrent(t *testing.T) {
	var calls, active, peak atomic.Int32
	b := newDownloadBatcher(context.Background(), 25, 4, func(ctx context.Context, ids []string) ([]api.DownloadURL, *http.Response, error) {
		calls.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		assert.LessOrEqual(t, len(ids), 25)
		for _, id := range ids {
			assert.Equal(t, api.ZoneFromDriveID(ids[0]), api.ZoneFromDriveID(id))
		}
		time.Sleep(5 * time.Millisecond)
		out := make([]api.DownloadURL, len(ids))
		for i, id := range ids {
			out[i].URL = id
		}
		return out, nil, nil
	})
	defer stopDownloads(b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 300 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := fmt.Sprintf("FILE::zone%d::%d", i%2, i)
			url, _, err := b.get(ctx, id)
			assert.NoError(t, err)
			assert.Equal(t, id, url)
		}()
	}
	wg.Wait()
	assert.Less(t, calls.Load(), int32(300))
	assert.Greater(t, peak.Load(), int32(1))
	assert.LessOrEqual(t, peak.Load(), int32(4))
}

func TestDownloadBatcherCancellation(t *testing.T) {
	entered := make(chan struct{})
	var once sync.Once
	b := newDownloadBatcher(context.Background(), 25, 1, func(ctx context.Context, ids []string) ([]api.DownloadURL, *http.Response, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return nil, nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, _, err := b.get(ctx, "FILE::zone::a"); done <- err }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("batch did not flush")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("cancel blocked")
	}
	stopDownloads(b)
	_, _, err := b.get(context.Background(), "FILE::zone::b")
	require.ErrorIs(t, err, context.Canceled)
}

func TestDownloadBatcherErrors(t *testing.T) {
	failure := errors.New("missing file")
	b := newDownloadBatcher(context.Background(), 25, 1, func(ctx context.Context, ids []string) ([]api.DownloadURL, *http.Response, error) {
		out := make([]api.DownloadURL, len(ids))
		for i := range out {
			out[i].Err = failure
		}
		return out, &http.Response{StatusCode: 200}, nil
	})
	defer stopDownloads(b)
	_, resp, err := b.get(context.Background(), "FILE::zone::a")
	require.ErrorIs(t, err, failure)
	require.NotNil(t, resp)
	assert.Equal(t, 200, resp.StatusCode)
}

func TestDownloadBatcherOutlivesCreator(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := newDownloadBatcher(ctx, 25, 1, func(ctx context.Context, ids []string) ([]api.DownloadURL, *http.Response, error) {
		return []api.DownloadURL{{URL: "ok"}}, nil, ctx.Err()
	})
	defer stopDownloads(b)
	cancel()
	url, _, err := b.get(context.Background(), "FILE::zone::a")
	require.NoError(t, err)
	assert.Equal(t, "ok", url)
}

func TestOpenDownloadRetries(t *testing.T) {
	ctx, ci := fs.AddConfig(context.Background())
	ci.MaxConnections, ci.LowLevelRetries = 1, 3
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var metadataCalls, contentCalls atomic.Int32
	var endpoint string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/download/batch") {
			if metadataCalls.Add(1) == 1 {
				w.WriteHeader(503)
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{"document_id": "a", "data_token": map[string]string{"url": endpoint + "/file"}}})
		} else if r.URL.Path == "/file" {
			if contentCalls.Add(1) == 1 {
				w.WriteHeader(503)
				return
			}
			assert.Equal(t, "bytes=1-2", r.Header.Get("Range"))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("bc"))
		} else {
			t.Errorf("unexpected path %q", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	endpoint = server.URL
	client, err := api.New("", "", "", "", nil, nil, "test", api.WsDrive)
	require.NoError(t, err)
	info, err := json.Marshal(map[string]any{"webservices": map[string]any{"drivews": map[string]string{"url": endpoint}, "docws": map[string]string{"url": endpoint}}})
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(info, &client.Session.AccountInfo))
	service, err := client.DriveService()
	require.NoError(t, err)
	f := &Fs{service: service, pacer: fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(time.Millisecond)))}
	f.downloads = newDownloadBatcher(ctx, 25, 4, f.getDownloadURLs)
	defer func() { require.NoError(t, f.Shutdown(ctx)) }()
	o := &Object{fs: f, size: 3, driveID: "FILE::com.apple.CloudDocs::a"}
	reader, err := o.Open(ctx, &fs.RangeOption{Start: 1, End: 2})
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "bc", string(data))
	assert.EqualValues(t, 2, metadataCalls.Load())
	assert.EqualValues(t, 2, contentCalls.Load())
}
