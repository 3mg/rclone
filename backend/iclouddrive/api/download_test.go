package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadURLs(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       []string
		failed     []bool
		err        bool
	}{
		{"reordered", `[{"document_id":"b","package_token":{"url":"https://example.test/b"}},{"document_id":"a","data_token":{"url":"https://example.test/a"}}]`, []string{"https://example.test/a", "https://example.test/b", "https://example.test/a"}, []bool{false, false, false}, false},
		{"missing", `[{"document_id":"a","data_token":{"url":"https://example.test/a"}}]`, []string{"https://example.test/a", "", "https://example.test/a"}, []bool{false, true, false}, false},
		{"no tokens", `[{"document_id":"a"},{"document_id":"b","data_token":{}}]`, []string{"", "", ""}, []bool{true, true, true}, false},
		{"unexpected", `[{"document_id":"unknown"}]`, nil, nil, true},
		{"duplicate", `[{"document_id":"a"},{"document_id":"a"}]`, nil, nil, true},
		{"null entry", `[null]`, nil, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "POST", r.Method)
				assert.Equal(t, "/ws/com.apple.CloudDocs/download/batch", r.URL.Path)
				var body []map[string]string
				require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
				assert.Equal(t, []map[string]string{{"document_id": "a"}, {"document_id": "b"}}, body)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			d := &DriveService{icloud: &Client{Session: NewSession()}, docsEndpoint: server.URL}
			got, resp, err := d.GetDownloadURLsByDriveID(context.Background(), []string{"FILE::com.apple.CloudDocs::a", "FILE::com.apple.CloudDocs::b", "FILE::com.apple.CloudDocs::a"})
			require.NotNil(t, resp)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, got, 3)
			for i, v := range got {
				assert.Equal(t, tc.want[i], v.URL)
				assert.Equal(t, tc.failed[i], v.Err != nil)
			}
		})
	}
}

func TestDownloadURLsInvalidInput(t *testing.T) {
	d := &DriveService{}
	for _, ids := range [][]string{nil, make([]string, 101), {"FILE::one::a", "FILE::two::b"}, {"FILE::one::"}} {
		_, resp, err := d.GetDownloadURLsByDriveID(context.Background(), ids)
		require.Error(t, err)
		assert.Nil(t, resp)
	}
}

func TestDownloadURLsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }))
	defer server.Close()
	d := &DriveService{icloud: &Client{Session: NewSession()}, docsEndpoint: server.URL}
	_, resp, err := d.GetDownloadURLsByDriveID(context.Background(), []string{"FILE::com.apple.CloudDocs::a"})
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, 503, resp.StatusCode)
}

func TestDownloadURLMissingToken(t *testing.T) {
	for _, body := range []string{"null", "{}", `{"data_token":{}}`} {
		t.Run(strings.ReplaceAll(body, "/", "_"), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer server.Close()
			d := &DriveService{icloud: &Client{Session: NewSession()}, docsEndpoint: server.URL}
			url, _, err := d.GetDownloadURLByDriveID(context.Background(), "FILE::com.apple.CloudDocs::a")
			require.Error(t, err)
			assert.Empty(t, url)
		})
	}
}
