package geoip

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLookupCloseCancelsAndWaitsForManualUpdate(t *testing.T) {
	lookup := newLookup(nil, "unused.mmdb", 0)
	started := make(chan struct{})
	cancelObserved := make(chan struct{})
	release := make(chan struct{})
	var startOnce sync.Once
	var cancelOnce sync.Once
	var calls atomic.Int32
	var callbacks atomic.Int32
	lookup.updateFn = func(ctx context.Context) error {
		calls.Add(1)
		startOnce.Do(func() { close(started) })
		<-ctx.Done()
		cancelOnce.Do(func() { close(cancelObserved) })
		<-release
		return nil
	}
	lookup.SetUpdateCallback(func() { callbacks.Add(1) })

	updateDone := make(chan error, 1)
	go func() { updateDone <- lookup.Update() }()
	waitForGeoIPTestSignal(t, started, "manual update did not start")

	closeDone := make(chan error, 1)
	go func() { closeDone <- lookup.Close() }()
	waitForGeoIPTestSignal(t, cancelObserved, "close did not cancel the update")
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before the active update exited: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)

	if err := waitForGeoIPTestResult(t, updateDone, "manual update did not exit"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Update error = %v, want context.Canceled", err)
	}
	if err := waitForGeoIPTestResult(t, closeDone, "Close did not finish"); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	if got := callbacks.Load(); got != 0 {
		t.Fatalf("canceled update invoked %d callbacks", got)
	}
	if err := lookup.Update(); !errors.Is(err, ErrLookupClosed) {
		t.Fatalf("Update after Close error = %v, want ErrLookupClosed", err)
	}
	lookup.SetUpdateCallback(func() { callbacks.Add(1) })
	lookup.notifyUpdate()
	if got := callbacks.Load(); got != 0 {
		t.Fatalf("closed lookup invoked %d callbacks", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("update calls = %d, want 1", got)
	}
	if err := lookup.Close(); err != nil {
		t.Fatalf("second Close error: %v", err)
	}
}

func TestLookupCloseStopsAutoUpdateAndWaitsForItsActiveRun(t *testing.T) {
	lookup := newLookup(nil, "unused.mmdb", time.Millisecond)
	started := make(chan struct{})
	cancelObserved := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	lookup.updateFn = func(ctx context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
		close(cancelObserved)
		<-release
		return nil
	}
	lookup.startAutoUpdate()
	waitForGeoIPTestSignal(t, started, "automatic update did not start")

	closeDone := make(chan error, 1)
	go func() { closeDone <- lookup.Close() }()
	waitForGeoIPTestSignal(t, cancelObserved, "Close did not cancel automatic update")
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while automatic update was active: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := waitForGeoIPTestResult(t, closeDone, "Close did not wait for automatic update"); err != nil {
		t.Fatalf("Close error: %v", err)
	}
	time.Sleep(5 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("automatic update restarted after Close: calls=%d", got)
	}
}

func TestGeoIPDownloadPathsRejectOversizedContentLength(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", strconv.FormatInt(maxGeoIPDatabaseSize+1, 10))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	previousURL := geoIPDatabaseURL
	geoIPDatabaseURL = server.URL
	t.Cleanup(func() { geoIPDatabaseURL = previousURL })

	for _, test := range []struct {
		name string
		run  func(string) error
	}{
		{name: "initial database", run: EnsureDatabase},
		{name: "periodic update", run: func(path string) error {
			return downloadDatabaseContext(context.Background(), path)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.run(filepath.Join(t.TempDir(), "GeoLite2-Country.mmdb"))
			if err == nil || !strings.Contains(err.Error(), "exceeds") {
				t.Fatalf("oversized Content-Length error = %v", err)
			}
		})
	}
}

func TestCopyGeoIPDatabaseBoundsUnknownLengthBody(t *testing.T) {
	const limit int64 = 32
	response := &http.Response{
		Body:          io.NopCloser(bytes.NewReader(bytes.Repeat([]byte{'x'}, int(limit+1)))),
		ContentLength: -1,
	}
	var destination bytes.Buffer
	written, err := copyGeoIPDatabase(&destination, response, limit)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("unknown-length oversized body error = %v", err)
	}
	if written != limit+1 || int64(destination.Len()) != limit+1 {
		t.Fatalf("bounded copy wrote %d/%d bytes, want %d", written, destination.Len(), limit+1)
	}
}

func waitForGeoIPTestSignal(t *testing.T, signal <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal(failure)
	}
}

func waitForGeoIPTestResult(t *testing.T, result <-chan error, failure string) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal(failure)
		return nil
	}
}
