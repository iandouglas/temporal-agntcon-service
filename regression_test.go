package main

// Regression tests. Each one pins an invariant that has either already broken
// or would break the demo in a way that is hard to diagnose live. They are
// deliberately named for the failure they prevent rather than the code they
// exercise, so a failure here tells you what will go wrong in production, not
// just which function changed.

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// storeIn returns a synced Store in the requested state, plus its Redis.
func storeIn(t *testing.T, on bool) (*Store, *miniredis.Miniredis) {
	t.Helper()

	st, mr, _ := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	st.Run(ctx)
	waitFor(t, "ready", st.Ready)

	if err := st.Set(ctx, on); err != nil {
		t.Fatalf("Set(%v): %v", on, err)
	}
	return st, mr
}

// handlerIn returns a handler serving /taco in the requested state.
func handlerIn(t *testing.T, on bool) (*handler, *Store, *miniredis.Miniredis) {
	t.Helper()

	st, mr := storeIn(t, on)
	cfg := &Config{Path: "/taco", Name: "taco", StateKey: "toggle:test"}
	return newHandler(st, cfg, discardLogger()), st, mr
}

// TestRegressionHealthNeverReturnsNonSuccess guards the single most damaging
// mistake available in this service.
//
// Railway restarts any container whose healthcheckPath returns non-2xx. If
// /health ever tracked the toggle, calling /off would fail every replica's
// health check at once, Railway would restart all of them, and the restart
// would wipe the state that was just set -- an unrecoverable loop, on stage,
// triggered by the demo working as intended.
//
// The matrix is exhaustive on purpose: on and off, Redis up and down.
func TestRegressionHealthNeverReturnsNonSuccess(t *testing.T) {
	for _, on := range []bool{true, false} {
		for _, redisDown := range []bool{false, true} {
			name := map[bool]string{true: "on", false: "off"}[on] +
				map[bool]string{true: "/redis-down", false: "/redis-up"}[redisDown]

			t.Run(name, func(t *testing.T) {
				h, _, mr := handlerIn(t, on)
				if redisDown {
					mr.Close()
				}

				rec := do(h, http.MethodGet, "/health")

				if rec.Code != http.StatusOK {
					t.Fatalf("/health returned %d; it must always be 200, "+
						"or Railway will restart the container", rec.Code)
				}
			})
		}
	}
}

// TestRegressionReadyIsSeparateFromHealth pins the split that makes the above
// safe. /ready must reflect fitness to serve, and must not follow the toggle:
// if it did, /health being always-200 would not help, because Railway probes
// /ready.
func TestRegressionReadyIsSeparateFromHealth(t *testing.T) {
	for _, on := range []bool{true, false} {
		h, _, _ := handlerIn(t, on)

		rec := do(h, http.MethodGet, "/ready")
		if rec.Code != http.StatusOK {
			t.Errorf("with on=%v, /ready returned %d; a synced replica is "+
				"ready regardless of the toggle", on, rec.Code)
		}
	}
}

// TestRegressionReseedPreservesLastKnownState pins the bug that shipped and
// was fixed.
//
// A Redis restart without persistence, or an eviction, leaves the key absent
// while Redis itself is healthy. Nothing re-created it, so the whole fleet
// reported stale indefinitely. The dangerous half of the fix is which value
// comes back: reseeding from the boot default would silently turn a
// deliberately-off demo back on.
func TestRegressionReseedPreservesLastKnownState(t *testing.T) {
	st, mr := storeIn(t, false)

	if bootDefault == false {
		t.Fatal("this test is only meaningful while bootDefault is true")
	}

	mr.Del("toggle:test")

	waitFor(t, "key to be reseeded", func() bool {
		_, err := mr.Get("toggle:test")
		return err == nil
	})

	got, _ := mr.Get("toggle:test")
	if got != redisOff {
		t.Fatalf("reseeded to %q, want %q; a wiped Redis must not flip the "+
			"fleet back to the boot default", got, redisOff)
	}
	if st.On() {
		t.Error("local state flipped on after the key was wiped")
	}
}

// TestRegressionStrandedReplicaRecoversWithinOneRetryCycle bounds the recovery
// window rather than merely proving recovery happens.
//
// A keep-alive HTTP client resolves <service>.railway.internal once and pins
// to a single replica, so a replica that misses a broadcast stays broken for
// that caller indefinitely -- while /health, which reads Redis, reports
// healthy. The reconcile loop is the only thing that repairs this, and it has
// to do so inside roughly one caller retry cycle to stay invisible.
func TestRegressionStrandedReplicaRecoversWithinOneRetryCycle(t *testing.T) {
	st, mr := storeIn(t, true)

	// Writing the key without publishing is exactly a lost broadcast.
	mr.Set("toggle:test", redisOff)

	// Allow a little scheduling slack over the interval itself; the point
	// is that recovery is bounded and fast, not that it is instant.
	bound := 4 * reconcileInterval
	deadline := time.Now().Add(bound)

	for time.Now().Before(deadline) {
		if !st.On() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("replica still stranded after %v; callers pinned to it by "+
		"keep-alive would stay broken with /health reporting healthy", bound)
}

// TestRegressionFailedToggleDoesNotDiverge pins fail-closed behaviour on
// writes. Applying a toggle locally when the Redis write failed would leave
// this replica disagreeing with its siblings, and the reconcile loop would
// silently revert it moments later -- so the operator would see the toggle
// appear to work, then undo itself.
func TestRegressionFailedToggleDoesNotDiverge(t *testing.T) {
	h, st, mr := handlerIn(t, true)
	mr.Close()

	rec := do(h, http.MethodPost, "/off")

	if rec.Code == http.StatusOK {
		t.Error("a toggle that could not reach Redis reported success")
	}
	if !st.On() {
		t.Error("local state changed despite the Redis write failing; " +
			"reconcile would revert this and the toggle would appear to undo itself")
	}
}

// TestRegressionOffPathStaysCheap pins the shape of the failure response.
//
// Callers retry hardest precisely while the service is off, so the off path is
// the sustained-load path. A body added here would be served thousands of
// times per off-window.
func TestRegressionOffPathStaysCheap(t *testing.T) {
	h, _, _ := handlerIn(t, false)

	rec := do(h, http.MethodGet, "/taco")

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("off path grew a body (%q); it is the sustained-load path",
			rec.Body.String())
	}
}

// TestRegressionHotPathIsAllocationFree fails if the request path starts
// allocating.
//
// Response bodies and header values are built once at startup for this
// reason. The usual way this regresses is innocuous-looking: a fmt.Sprintf in
// a handler, a json.Marshal instead of a fixed byte slice, or switching a
// direct header-map assignment to Header().Set. A benchmark would report the
// change silently; this fails the build.
func TestRegressionHotPathIsAllocationFree(t *testing.T) {
	cfg := &Config{Path: "/taco", Name: "taco", StateKey: "toggle:test"}

	for _, tc := range []struct {
		name string
		on   bool
	}{{"on", true}, {"off", false}} {
		t.Run(tc.name, func(t *testing.T) {
			st := NewStore(nil, "toggle:test", discardLogger())
			st.on.Store(tc.on)
			h := newHandler(st, cfg, discardLogger())

			req := httptest.NewRequest(http.MethodGet, "/taco", nil)
			w := &nullResponseWriter{header: make(http.Header)}

			// Warm the header map so its buckets exist; the first
			// write to a fresh map allocates and would mask the
			// steady-state result.
			h.ServeHTTP(w, req)

			if got := testing.AllocsPerRun(200, func() {
				h.ServeHTTP(w, req)
			}); got != 0 {
				t.Errorf("hot path allocates %.1f times per request, want 0", got)
			}
		})
	}
}

// TestRegressionListenerAcceptsIPv6 pins the Railway private-network
// invariant.
//
// Railway's private network is IPv6-only, so a listener that does not accept
// IPv6 produces a service that passes its health check and answers on its
// public domain while being unreachable at <service>.railway.internal. Every
// caller fails and the service looks fine, which is why this is worth pinning
// rather than trusting to review.
//
// Verified to fail if listen switches to network "tcp4", or to a literal host
// such as 127.0.0.1. Note it does not fail for "0.0.0.0:port", because Go
// treats that as the unspecified address and still opens a dual-stack socket
// -- unlike Node or Python, where the same bind really is IPv4-only.
func TestRegressionListenerAcceptsIPv6(t *testing.T) {
	if probe, err := net.Listen("tcp", "[::1]:0"); err != nil {
		t.Skip("no IPv6 loopback on this machine")
	} else {
		probe.Close()
	}

	ln, err := listen("0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", ln.Addr(), err)
	}

	for _, host := range []string{"127.0.0.1", "[::1]"} {
		conn, err := net.DialTimeout("tcp", host+":"+port, 2*time.Second)
		if err != nil {
			t.Errorf("cannot reach the listener over %s: %v; "+
				"Railway's private network is IPv6-only", host, err)
			continue
		}
		conn.Close()
	}
}

// TestRegressionConfigRefusesToBootMisconfigured pins fail-fast startup.
//
// Every case here would otherwise produce a replica that starts happily and
// then shares state with nothing, or shadows a control route. Both are far
// harder to diagnose than a container that refuses to start.
func TestRegressionConfigRefusesToBootMisconfigured(t *testing.T) {
	const redisURL = "redis://localhost:6379"

	cases := map[string]map[string]string{
		"endpoint shadows /health": {"ENDPOINT": "/health", "REDIS_URL": redisURL},
		"endpoint shadows /ready":  {"ENDPOINT": "/ready", "REDIS_URL": redisURL},
		"endpoint shadows /on":     {"ENDPOINT": "/on", "REDIS_URL": redisURL},
		"endpoint shadows /off":    {"ENDPOINT": "/off", "REDIS_URL": redisURL},
		"endpoint is multipath":    {"ENDPOINT": "/api/taco", "REDIS_URL": redisURL},
		"endpoint missing":         {"REDIS_URL": redisURL},
		"redis url missing":        {"ENDPOINT": "/taco"},
	}

	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			if cfg, err := LoadConfig(envFunc(env)); err == nil {
				t.Fatalf("booted with %+v; misconfiguration must be fatal", cfg)
			}
		})
	}
}

// TestRegressionReservedPathsMatchRoutes catches the drift where a route is
// added to the handler but not to the reserved set, letting ENDPOINT shadow it
// and silently disable a control endpoint.
func TestRegressionReservedPathsMatchRoutes(t *testing.T) {
	h, _, _ := handlerIn(t, true)

	for path := range reservedPaths {
		if path == "/" {
			continue // deliberately reserved without being a route
		}
		if rec := do(h, http.MethodGet, path); rec.Code == http.StatusNotFound {
			t.Errorf("%s is reserved but is not a route; either drop the "+
				"reservation or restore the route", path)
		}
	}
}
