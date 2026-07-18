package monitor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"easy_proxies/internal/config"
)

type blockingAuthBody struct {
	started chan struct{}
	unblock chan struct{}
	once    sync.Once
	body    []byte
}

func (b *blockingAuthBody) Read(destination []byte) (int, error) {
	b.once.Do(func() { close(b.started) })
	<-b.unblock
	if len(b.body) == 0 {
		return 0, io.EOF
	}
	n := copy(destination, b.body)
	b.body = b.body[n:]
	return n, nil
}

func (*blockingAuthBody) Close() error { return nil }

func TestSetConfigHotUpdatesPasswordAndInvalidatesSessions(t *testing.T) {
	manager, err := NewManager(Config{ProbeTarget: "example.com:80"})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()

	server := &Server{
		cfg:        Config{Listen: "127.0.0.1:9091", Password: "old-password"},
		mgr:        manager,
		sessions:   map[string]*Session{"old-token": {Token: "old-token", ExpiresAt: time.Now().Add(time.Hour)}},
		sessionTTL: time.Hour,
	}
	server.SetConfig(&config.Config{Management: config.ManagementConfig{
		Listen:      "127.0.0.1:9091",
		Password:    "new-password",
		ProbeTarget: "example.com:80",
	}})
	if got := server.managementPassword(); got != "new-password" {
		t.Fatalf("runtime password=%q", got)
	}
	if server.validateSession("old-token") {
		t.Fatal("old session survived password change")
	}

	login := httptest.NewRecorder()
	server.handleAuth(login, httptest.NewRequest(http.MethodPost, "/api/auth", strings.NewReader(`{"password":"new-password"}`)))
	if login.Code != http.StatusOK {
		t.Fatalf("new password login status=%d body=%s", login.Code, login.Body.String())
	}
}

func TestPasswordRotationRejectsInFlightOldPasswordLogin(t *testing.T) {
	server := &Server{
		cfg:        Config{Listen: "127.0.0.1:9091", Password: "old-password"},
		sessions:   make(map[string]*Session),
		sessionTTL: time.Hour,
	}
	body := &blockingAuthBody{
		started: make(chan struct{}),
		unblock: make(chan struct{}),
		body:    []byte(`{"password":"old-password"}`),
	}
	request := httptest.NewRequest(http.MethodPost, "/api/auth", body)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		server.handleAuth(recorder, request)
		close(done)
	}()

	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("login request did not block while reading its body")
	}
	server.SetConfig(&config.Config{Management: config.ManagementConfig{
		Listen:   "127.0.0.1:9091",
		Password: "new-password",
	}})
	close(body.unblock)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("login request did not finish")
	}

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("stale login status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	server.sessionMu.RLock()
	defer server.sessionMu.RUnlock()
	if len(server.sessions) != 0 {
		t.Fatal("old-password request recreated a session after password rotation")
	}
}

func TestSetConfigDefersPasswordChangeWhenManagementListenRequiresRestart(t *testing.T) {
	server := &Server{
		cfg: Config{Listen: "0.0.0.0:9091", Password: "old-password"},
		sessions: map[string]*Session{
			"old-token": {Token: "old-token", ExpiresAt: time.Now().Add(time.Hour)},
		},
	}
	server.SetConfig(&config.Config{Management: config.ManagementConfig{
		Listen:      "127.0.0.1:9091",
		Password:    "",
		ProbeTarget: "example.com:80",
	}})

	if got := server.managementPassword(); got != "old-password" {
		t.Fatalf("runtime password changed before listener restart: %q", got)
	}
	if server.cfgSrc == nil || server.cfgSrc.Management.Password != "" {
		t.Fatal("persisted next-start password was not retained")
	}
	if !server.validateSession("old-token") {
		t.Fatal("current-listener session was invalidated by a deferred password change")
	}
}

func TestTLSConfiguredLoginCookieIsSecure(t *testing.T) {
	server := &Server{
		cfg: Config{
			Password:    "secret",
			TLSCertFile: "management.crt",
			TLSKeyFile:  "management.key",
		},
		sessions:   make(map[string]*Session),
		sessionTTL: time.Hour,
	}
	recorder := httptest.NewRecorder()
	server.handleAuth(recorder, httptest.NewRequest(http.MethodPost, "/api/auth", strings.NewReader(`{"password":"secret"}`)))
	if recorder.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	cookies := recorder.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].Secure {
		t.Fatalf("TLS login cookie is not Secure: %#v", cookies)
	}
}

func TestWriteJSONDisablesCaching(t *testing.T) {
	recorder := httptest.NewRecorder()
	writeJSON(recorder, map[string]string{"status": "ok"})
	if recorder.Header().Get("Cache-Control") != "no-store, max-age=0" || recorder.Header().Get("Pragma") != "no-cache" {
		t.Fatalf("cache prevention headers missing: %#v", recorder.Header())
	}
}

func TestAuthRejectsUnknownFieldsAndOversizedBodies(t *testing.T) {
	server := &Server{
		cfg:        Config{Password: "secret"},
		sessions:   make(map[string]*Session),
		sessionTTL: time.Hour,
	}
	for _, test := range []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "unknown field", body: `{"password":"secret","extra":true}`, wantStatus: http.StatusBadRequest},
		{name: "trailing JSON", body: `{"password":"secret"}{}`, wantStatus: http.StatusBadRequest},
		{name: "oversized", body: strings.Repeat(" ", int(maxAuthBodyBytes)+1), wantStatus: http.StatusRequestEntityTooLarge},
	} {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			server.handleAuth(recorder, httptest.NewRequest(http.MethodPost, "/api/auth", strings.NewReader(test.body)))
			if recorder.Code != test.wantStatus {
				t.Fatalf("status=%d, want=%d; body=%s", recorder.Code, test.wantStatus, recorder.Body.String())
			}
			if len(server.sessions) != 0 {
				t.Fatal("invalid login created a session")
			}
		})
	}
}

func TestPasswordlessManagementRejectsCrossSiteAndDNSRebindingRequests(t *testing.T) {
	called := false
	server := &Server{}
	handler := server.withAuth(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(http.StatusNoContent)
	})

	for _, test := range []struct {
		name   string
		host   string
		remote string
		origin string
		site   string
		want   int
	}{
		{name: "local native client", host: "127.0.0.1:9091", remote: "127.0.0.1:51000", want: http.StatusNoContent},
		{name: "local same origin", host: "localhost:9091", remote: "[::1]:51000", origin: "http://localhost:9091", want: http.StatusNoContent},
		{name: "spoofed local host from remote", host: "127.0.0.1:9091", remote: "198.51.100.8:51000", want: http.StatusForbidden},
		{name: "cross site form", host: "127.0.0.1:9091", remote: "127.0.0.1:51000", origin: "https://attacker.example", want: http.StatusForbidden},
		{name: "fetch metadata cross site", host: "127.0.0.1:9091", remote: "127.0.0.1:51000", site: "cross-site", want: http.StatusForbidden},
		{name: "dns rebinding host", host: "attacker.example:9091", remote: "127.0.0.1:51000", origin: "http://attacker.example:9091", want: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			called = false
			request := httptest.NewRequest(http.MethodPost, "http://"+test.host+"/api/reload", nil)
			request.Host = test.host
			request.RemoteAddr = test.remote
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			if test.site != "" {
				request.Header.Set("Sec-Fetch-Site", test.site)
			}
			recorder := httptest.NewRecorder()
			handler(recorder, request)
			if recorder.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", recorder.Code, test.want, recorder.Body.String())
			}
			if called != (test.want == http.StatusNoContent) {
				t.Fatalf("handler called=%v", called)
			}
		})
	}
}

func TestWithAuthRequiresBearerSchemeForHeaderToken(t *testing.T) {
	server := &Server{
		cfg: Config{Password: "secret"},
		sessions: map[string]*Session{
			"valid-token": {Token: "valid-token", ExpiresAt: time.Now().Add(time.Hour)},
		},
	}
	handler := server.withAuth(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	for _, test := range []struct {
		name   string
		header string
		want   int
	}{
		{name: "bearer", header: "Bearer valid-token", want: http.StatusNoContent},
		{name: "case insensitive scheme", header: "bearer valid-token", want: http.StatusNoContent},
		{name: "raw token", header: "valid-token", want: http.StatusUnauthorized},
		{name: "wrong scheme", header: "Basic valid-token", want: http.StatusUnauthorized},
		{name: "extra field", header: "Bearer valid-token trailing", want: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/api/nodes", nil)
			request.Header.Set("Authorization", test.header)
			recorder := httptest.NewRecorder()
			handler(recorder, request)
			if recorder.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", recorder.Code, test.want, recorder.Body.String())
			}
		})
	}
}

func TestCookieAuthenticationRequiresSameOriginForUnsafeMethods(t *testing.T) {
	server := &Server{
		cfg: Config{Password: "secret"},
		sessions: map[string]*Session{
			"valid-token": {Token: "valid-token", ExpiresAt: time.Now().Add(time.Hour)},
		},
	}
	handler := server.withAuth(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	for _, test := range []struct {
		name   string
		method string
		host   string
		origin string
		want   int
	}{
		{name: "same origin POST", method: http.MethodPost, host: "127.0.0.1:9091", origin: "http://127.0.0.1:9091", want: http.StatusNoContent},
		{name: "default HTTP port", method: http.MethodPost, host: "example.test", origin: "http://example.test:80", want: http.StatusNoContent},
		{name: "safe GET without origin", method: http.MethodGet, host: "127.0.0.1:9091", want: http.StatusNoContent},
		{name: "missing origin", method: http.MethodPost, host: "127.0.0.1:9091", want: http.StatusForbidden},
		{name: "different port", method: http.MethodPost, host: "127.0.0.1:9091", origin: "http://127.0.0.1:3000", want: http.StatusForbidden},
		{name: "different host", method: http.MethodDelete, host: "admin.example.test", origin: "http://evil.example.test", want: http.StatusForbidden},
		{name: "different scheme", method: http.MethodPut, host: "admin.example.test", origin: "https://admin.example.test", want: http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "http://"+test.host+"/api/reload", nil)
			request.Host = test.host
			request.AddCookie(&http.Cookie{Name: "session_token", Value: "valid-token"})
			if test.origin != "" {
				request.Header.Set("Origin", test.origin)
			}
			recorder := httptest.NewRecorder()
			handler(recorder, request)
			if recorder.Code != test.want {
				t.Fatalf("status=%d want=%d body=%s", recorder.Code, test.want, recorder.Body.String())
			}
		})
	}
}

func TestBearerAuthenticationBypassesCookieOriginCheck(t *testing.T) {
	server := &Server{
		cfg: Config{Password: "secret"},
		sessions: map[string]*Session{
			"valid-token": {Token: "valid-token", ExpiresAt: time.Now().Add(time.Hour)},
		},
	}
	handler := server.withAuth(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:9091/api/reload", nil)
	request.Header.Set("Authorization", "Bearer valid-token")
	request.Header.Set("Origin", "http://127.0.0.1:3000")
	request.AddCookie(&http.Cookie{Name: "session_token", Value: "valid-token"})
	recorder := httptest.NewRecorder()

	handler(recorder, request)

	if recorder.Code != http.StatusNoContent {
		t.Fatalf("bearer request status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestAuthRateLimitReturns429ForBruteForce(t *testing.T) {
	server := &Server{
		cfg:        Config{Password: "secret"},
		sessions:   make(map[string]*Session),
		sessionTTL: time.Hour,
	}
	server.authGuard.burst = 2
	server.authGuard.refill = time.Hour

	for attempt := 0; attempt < 3; attempt++ {
		request := httptest.NewRequest(http.MethodPost, "/api/auth", strings.NewReader(`{"password":"wrong"}`))
		request.RemoteAddr = "198.51.100.20:41000"
		recorder := httptest.NewRecorder()
		server.handleAuth(recorder, request)
		if attempt < 2 && recorder.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d status=%d body=%s", attempt+1, recorder.Code, recorder.Body.String())
		}
		if attempt == 2 {
			if recorder.Code != http.StatusTooManyRequests {
				t.Fatalf("rate-limited status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if recorder.Header().Get("Retry-After") == "" {
				t.Fatal("rate-limited response is missing Retry-After")
			}
		}
	}
}

func TestAuthGlobalConcurrencyLimitRunsBeforeBodyDecode(t *testing.T) {
	server := &Server{
		cfg:        Config{Password: "secret"},
		sessions:   make(map[string]*Session),
		sessionTTL: time.Hour,
	}
	server.authGuard.maxConcurrent = 1
	body := &blockingAuthBody{
		started: make(chan struct{}),
		unblock: make(chan struct{}),
		body:    []byte(`{"password":"secret"}`),
	}
	firstRequest := httptest.NewRequest(http.MethodPost, "/api/auth", nil)
	firstRequest.RemoteAddr = "198.51.100.30:42000"
	firstRequest.Body = body
	firstRecorder := httptest.NewRecorder()
	firstDone := make(chan struct{})
	go func() {
		defer close(firstDone)
		server.handleAuth(firstRecorder, firstRequest)
	}()

	select {
	case <-body.started:
	case <-time.After(time.Second):
		t.Fatal("first auth request did not reach body decoding")
	}
	secondRequest := httptest.NewRequest(http.MethodPost, "/api/auth", strings.NewReader(`{"password":"secret"}`))
	secondRequest.RemoteAddr = "203.0.113.40:43000"
	secondRecorder := httptest.NewRecorder()
	server.handleAuth(secondRecorder, secondRequest)
	if secondRecorder.Code != http.StatusTooManyRequests {
		t.Fatalf("concurrent status=%d body=%s", secondRecorder.Code, secondRecorder.Body.String())
	}
	close(body.unblock)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first auth request did not finish")
	}
	if firstRecorder.Code != http.StatusOK {
		t.Fatalf("first auth status=%d body=%s", firstRecorder.Code, firstRecorder.Body.String())
	}
}

func TestAuthRateTableIsBoundedAndEntriesExpire(t *testing.T) {
	guard := authRequestGuard{
		maxEntries:    2,
		burst:         1,
		refill:        time.Hour,
		entryTTL:      time.Minute,
		maxConcurrent: 4,
	}
	now := time.Unix(1_700_000_000, 0)
	for index, remote := range []string{"192.0.2.1:1", "192.0.2.2:2", "192.0.2.3:3"} {
		release, _, allowed := guard.begin(remote, now.Add(time.Duration(index)*time.Second))
		if !allowed {
			t.Fatalf("new remote %s was unexpectedly denied", remote)
		}
		release()
	}
	guard.mu.Lock()
	if len(guard.clients) != 2 {
		t.Fatalf("client table size=%d want=2", len(guard.clients))
	}
	if _, retained := guard.clients["192.0.2.1"]; retained {
		t.Fatal("oldest client was not evicted")
	}
	guard.mu.Unlock()

	if release, _, allowed := guard.begin("192.0.2.3:9", now.Add(30*time.Second)); allowed {
		release()
		t.Fatal("exhausted entry was allowed before expiry")
	}
	release, _, allowed := guard.begin("192.0.2.3:9", now.Add(2*time.Minute))
	if !allowed {
		t.Fatal("expired entry did not receive a fresh bucket")
	}
	release()
}

func TestAuthRateKeyNormalizesIPv4MappedIPv6(t *testing.T) {
	guard := authRequestGuard{
		maxEntries:    4,
		burst:         1,
		refill:        time.Hour,
		entryTTL:      time.Minute,
		maxConcurrent: 4,
	}
	now := time.Unix(1_700_000_000, 0)
	release, _, allowed := guard.begin("[::ffff:192.0.2.55]:1234", now)
	if !allowed {
		t.Fatal("first mapped IPv6 attempt was denied")
	}
	release()
	if release, _, allowed := guard.begin("192.0.2.55:5678", now); allowed {
		release()
		t.Fatal("mapped IPv6 and IPv4 did not share a rate-limit bucket")
	}
}

func TestSuccessfulAuthResetsClientBucket(t *testing.T) {
	server := &Server{
		cfg:        Config{Password: "secret"},
		sessions:   make(map[string]*Session),
		sessionTTL: time.Hour,
	}
	server.authGuard.burst = 1
	server.authGuard.refill = time.Hour
	for attempt := 0; attempt < 2; attempt++ {
		request := httptest.NewRequest(http.MethodPost, "/api/auth", strings.NewReader(`{"password":"secret"}`))
		request.RemoteAddr = "192.0.2.80:44000"
		recorder := httptest.NewRecorder()
		server.handleAuth(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("successful attempt %d status=%d body=%s", attempt+1, recorder.Code, recorder.Body.String())
		}
	}
}

func TestManagementServerUsesDefensiveReadTimeouts(t *testing.T) {
	manager, err := NewManager(Config{ProbeTarget: "example.com:80"})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Stop()
	server := NewServer(Config{Enabled: true, Listen: "127.0.0.1:0"}, manager, nil)
	if server == nil || server.srv == nil {
		t.Fatal("management server was not constructed")
	}
	defer server.Shutdown(context.Background())
	if server.srv.ReadHeaderTimeout != managementReadHeaderTimeout ||
		server.srv.ReadTimeout != managementReadTimeout ||
		server.srv.IdleTimeout != managementIdleTimeout ||
		server.srv.MaxHeaderBytes != managementMaxHeaderBytes {
		t.Fatalf("unexpected server limits: %#v", server.srv)
	}
	if server.srv.WriteTimeout != 0 {
		t.Fatalf("streaming endpoints require WriteTimeout=0, got %s", server.srv.WriteTimeout)
	}
}
