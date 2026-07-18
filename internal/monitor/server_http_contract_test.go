package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestIndexRequiresExactPathAndReadMethod(t *testing.T) {
	server := &Server{}

	notFound := httptest.NewRecorder()
	server.handleIndex(notFound, httptest.NewRequest(http.MethodGet, "/missing", nil))
	if notFound.Code != http.StatusNotFound {
		t.Fatalf("missing path status=%d, want 404", notFound.Code)
	}

	method := httptest.NewRecorder()
	server.handleIndex(method, httptest.NewRequest(http.MethodPost, "/", nil))
	if method.Code != http.StatusMethodNotAllowed || method.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("POST / status=%d allow=%q", method.Code, method.Header().Get("Allow"))
	}

	head := httptest.NewRecorder()
	server.handleIndex(head, httptest.NewRequest(http.MethodHead, "/", nil))
	if head.Code != http.StatusOK || head.Body.Len() != 0 {
		t.Fatalf("HEAD / status=%d body=%q", head.Code, head.Body.String())
	}
}

func TestLogsRequireGETAndReturnJSONErrors(t *testing.T) {
	server := &Server{}
	recorder := httptest.NewRecorder()
	server.handleLogs(recorder, httptest.NewRequest(http.MethodPost, "/api/logs", nil))

	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("Allow=%q", got)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type=%q", got)
	}
}

func TestTrafficPreservesNDJSONRecordsAcrossReads(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("upstream method=%s", r.Method)
		}
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte(`{"up":1`))
		flusher.Flush()
		_, _ = w.Write([]byte("}\n{\"down\":2}\n"))
	}))
	defer upstream.Close()

	server := &Server{trafficHTTPClient: upstream.Client(), trafficURL: upstream.URL}
	recorder := httptest.NewRecorder()
	server.handleTraffic(recorder, httptest.NewRequest(http.MethodGet, "/api/traffic", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("Content-Type=%q", got)
	}
	if got, want := recorder.Body.String(), "data: {\"up\":1}\n\ndata: {\"down\":2}\n\n"; got != want {
		t.Fatalf("SSE body=%q, want %q", got, want)
	}
}

func TestTrafficRejectsMethodsAndMapsUnsupportedUpstream(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.NotFound(w, nil)
	}))
	defer upstream.Close()
	server := &Server{trafficHTTPClient: upstream.Client(), trafficURL: upstream.URL}

	method := httptest.NewRecorder()
	server.handleTraffic(method, httptest.NewRequest(http.MethodPost, "/api/traffic", nil))
	if method.Code != http.StatusMethodNotAllowed || calls.Load() != 0 {
		t.Fatalf("method status=%d upstream calls=%d", method.Code, calls.Load())
	}

	unsupported := httptest.NewRecorder()
	server.handleTraffic(unsupported, httptest.NewRequest(http.MethodGet, "/api/traffic", nil))
	if unsupported.Code != http.StatusNotImplemented {
		t.Fatalf("unsupported status=%d body=%s", unsupported.Code, unsupported.Body.String())
	}
	if got := unsupported.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type=%q", got)
	}
}

func TestTrafficCancelsUpstreamWhenClientDisconnects(t *testing.T) {
	started := make(chan struct{})
	canceled := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(canceled)
	}))
	defer upstream.Close()

	server := &Server{trafficHTTPClient: upstream.Client(), trafficURL: upstream.URL}
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/api/traffic", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() {
		server.handleTraffic(httptest.NewRecorder(), request)
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request did not start")
	}
	cancel()
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("upstream request was not canceled")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("traffic handler did not return")
	}
}

func TestSettingsPUTRequiresNodeManager(t *testing.T) {
	server := &Server{}
	recorder := httptest.NewRecorder()
	server.handleSettings(recorder, httptest.NewRequest(http.MethodPut, "/api/settings", strings.NewReader(`{}`)))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type=%q", got)
	}
}

func TestNodeErrorsUseSpecificStatusAndJSON(t *testing.T) {
	server := &Server{}
	tests := []struct {
		name string
		err  error
		want int
	}{
		{name: "missing", err: ErrNodeNotFound, want: http.StatusNotFound},
		{name: "conflict", err: ErrNodeConflict, want: http.StatusConflict},
		{name: "invalid", err: ErrInvalidNode, want: http.StatusBadRequest},
		{name: "canceled", err: context.Canceled, want: http.StatusRequestTimeout},
		{name: "deadline", err: context.DeadlineExceeded, want: http.StatusGatewayTimeout},
		{name: "internal", err: errors.New("failure"), want: http.StatusInternalServerError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			server.respondNodeError(recorder, test.err)
			if recorder.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", recorder.Code, test.want, recorder.Body.String())
			}
			if recorder.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("Content-Type=%q", recorder.Header().Get("Content-Type"))
			}
			var payload map[string]any
			if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil || payload["error"] == "" {
				t.Fatalf("invalid JSON error body=%q err=%v", recorder.Body.String(), err)
			}
		})
	}
}
