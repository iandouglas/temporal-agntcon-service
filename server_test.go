package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newTestHandler returns a handler serving /taco, already synced to an
// in-process Redis, plus the Redis handle so tests can break or inspect it.
func newTestHandler(t *testing.T) (*handler, *Store, *miniredis.Miniredis) {
	t.Helper()

	st, mr, _ := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st.Run(ctx)
	waitFor(t, "ready", st.Ready)

	cfg := &Config{Path: "/taco", Name: "taco", StateKey: "toggle:test"}
	return newHandler(st, cfg, discardLogger()), st, mr
}

// do issues a request against the handler and returns the recorder.
func do(h *handler, method, path string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, nil))
	return rec
}

func TestEndpointServesPayloadWhenOn(t *testing.T) {
	h, _, _ := newTestHandler(t)

	rec := do(h, http.MethodGet, "/taco")

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"taco":true}` {
		t.Errorf("body = %q, want {\"taco\":true}", got)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
}

func TestEndpointServesBareFiveHundredWhenOff(t *testing.T) {
	h, st, _ := newTestHandler(t)

	if err := st.Set(context.Background(), false); err != nil {
		t.Fatalf("Set: %v", err)
	}

	rec := do(h, http.MethodGet, "/taco")

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	// Callers retry this every 250ms while off, so the failure path stays
	// as cheap as the success path: no body at all.
	if got := rec.Body.Len(); got != 0 {
		t.Errorf("body length = %d, want 0 (%q)", got, rec.Body.String())
	}
}

func TestEndpointAcceptsAnyMethod(t *testing.T) {
	// This endpoint mutates nothing, so rejecting a method would only give
	// a caller a way to fail for the wrong reason.
	h, _, _ := newTestHandler(t)

	for _, m := range []string{http.MethodGet, http.MethodHead, http.MethodPost} {
		if rec := do(h, m, "/taco"); rec.Code != http.StatusOK {
			t.Errorf("%s /taco = %d, want 200", m, rec.Code)
		}
	}
}

func TestToggleEndpointsRequirePost(t *testing.T) {
	h, st, _ := newTestHandler(t)

	for _, path := range []string{"/on", "/off"} {
		for _, m := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
			rec := do(h, m, path)

			if rec.Code != http.StatusMethodNotAllowed {
				t.Errorf("%s %s = %d, want 405", m, path, rec.Code)
			}
			if allow := rec.Header().Get("Allow"); allow != "POST" {
				t.Errorf("%s %s Allow = %q, want POST", m, path, allow)
			}
		}
	}

	// A rejected method must not have touched the state.
	if !st.On() {
		t.Error("a 405 changed the state")
	}
}

func TestTogglePostsFlipState(t *testing.T) {
	h, st, _ := newTestHandler(t)

	rec := do(h, http.MethodPost, "/off")
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /off = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"on":false}` {
		t.Errorf("body = %q, want {\"on\":false}", got)
	}
	if st.On() {
		t.Error("POST /off did not turn the service off")
	}

	rec = do(h, http.MethodPost, "/on")
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /on = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"on":true}` {
		t.Errorf("body = %q, want {\"on\":true}", got)
	}
	if !st.On() {
		t.Error("POST /on did not turn the service on")
	}
}

func TestToggleIsIdempotent(t *testing.T) {
	// The control system may retry, and repeated calls must be harmless.
	h, st, _ := newTestHandler(t)

	for range 3 {
		if rec := do(h, http.MethodPost, "/off"); rec.Code != http.StatusOK {
			t.Fatalf("POST /off = %d, want 200", rec.Code)
		}
	}
	if st.On() {
		t.Error("state should be off")
	}
}

func TestToggleReportsFailureWhenRedisIsDown(t *testing.T) {
	h, st, mr := newTestHandler(t)
	mr.Close()

	rec := do(h, http.MethodPost, "/off")

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}
	if !st.On() {
		t.Error("state changed locally despite the Redis write failing")
	}
}

func TestHealthIsAlwaysTwoHundred(t *testing.T) {
	// Railway restarts a container whose health check returns non-2xx. If
	// /health tracked the toggle, calling /off would restart every replica
	// and wipe the state that was just set.
	h, st, mr := newTestHandler(t)

	cases := []struct {
		name string
		set  func()
		want string
	}{
		{"on", func() {}, `{"on":true}`},
		{"off", func() {
			if err := st.Set(context.Background(), false); err != nil {
				t.Fatalf("Set: %v", err)
			}
		}, `{"on":false}`},
		{"redis down", func() { mr.Close() }, `{"on":false,"stale":true}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.set()
			rec := do(h, http.MethodGet, "/health")

			if rec.Code != http.StatusOK {
				t.Errorf("status = %d, want 200 always", rec.Code)
			}
			if got := rec.Body.String(); got != tc.want {
				t.Errorf("body = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHealthReadsRedisNotLocalState(t *testing.T) {
	// All replicas must agree so a polling client never sees flicker from
	// landing on one that has not yet received a broadcast. Writing the
	// key without publishing simulates exactly that window.
	h, st, mr := newTestHandler(t)

	mr.Set("toggle:test", redisOff)

	// Assert before the reconcile loop can catch up: local still says on,
	// but /health must report Redis's answer.
	if !st.On() {
		t.Skip("reconcile already ran; timing-dependent assertion skipped")
	}
	if got := do(h, http.MethodGet, "/health").Body.String(); got != `{"on":false}` {
		t.Errorf("body = %q, want {\"on\":false} from Redis", got)
	}
}

func TestReadyGatesOnFirstRedisRead(t *testing.T) {
	// This gate is what makes booting to a default state safe: Railway
	// routes no traffic until the real state is known.
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	st := NewStore(rdb, "toggle:test", discardLogger())
	cfg := &Config{Path: "/taco", Name: "taco", StateKey: "toggle:test"}
	h := newHandler(st, cfg, discardLogger())

	rec := do(h, http.MethodGet, "/ready")
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("before sync: status = %d, want 503", rec.Code)
	}
	if got := rec.Body.String(); got != `{"ready":false}` {
		t.Errorf("before sync: body = %q", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st.Run(ctx)
	waitFor(t, "ready", st.Ready)

	rec = do(h, http.MethodGet, "/ready")
	if rec.Code != http.StatusOK {
		t.Errorf("after sync: status = %d, want 200", rec.Code)
	}
	if got := rec.Body.String(); got != `{"ready":true}` {
		t.Errorf("after sync: body = %q", got)
	}
}

func TestUnknownPathIsNotFound(t *testing.T) {
	h, _, _ := newTestHandler(t)

	for _, path := range []string{"/", "/coffee", "/taco/extra", "/metrics"} {
		if rec := do(h, http.MethodGet, path); rec.Code != http.StatusNotFound {
			t.Errorf("GET %s = %d, want 404", path, rec.Code)
		}
	}
}

// BenchmarkEndpointOn measures the hot path. It must show zero allocations:
// the response body and every header value are built once at startup.
func BenchmarkEndpointOn(b *testing.B) {
	st := NewStore(nil, "toggle:test", discardLogger())
	st.on.Store(true)
	cfg := &Config{Path: "/taco", Name: "taco", StateKey: "toggle:test"}
	h := newHandler(st, cfg, discardLogger())

	req := httptest.NewRequest(http.MethodGet, "/taco", nil)
	w := &nullResponseWriter{header: make(http.Header)}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		h.ServeHTTP(w, req)
	}
}

// BenchmarkEndpointOff measures the sustained-load path, since callers retry
// aggressively precisely while the service is off.
func BenchmarkEndpointOff(b *testing.B) {
	st := NewStore(nil, "toggle:test", discardLogger())
	st.on.Store(false)
	cfg := &Config{Path: "/taco", Name: "taco", StateKey: "toggle:test"}
	h := newHandler(st, cfg, discardLogger())

	req := httptest.NewRequest(http.MethodGet, "/taco", nil)
	w := &nullResponseWriter{header: make(http.Header)}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		h.ServeHTTP(w, req)
	}
}

// nullResponseWriter discards output so the benchmark measures the handler
// rather than httptest.ResponseRecorder's buffer growth.
type nullResponseWriter struct{ header http.Header }

func (n *nullResponseWriter) Header() http.Header         { return n.header }
func (n *nullResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (n *nullResponseWriter) WriteHeader(int)             {}
