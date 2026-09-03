package main

// End-to-end tests. These drive a fleet of replicas over real HTTP against a
// shared Redis, so they exercise the actual serving stack -- net/http, status
// codes, headers, connection reuse -- rather than calling handlers directly.
// This is the layer that would catch a routing or wiring mistake that unit
// tests on the handler cannot see.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// replica is one running service instance, addressable over HTTP.
type replica struct {
	srv *httptest.Server
	st  *Store
}

// fleet spins up n replicas of /taco sharing one Redis, mirroring a Railway
// service with numReplicas: n.
func fleet(t *testing.T, n int) ([]*replica, *miniredis.Miniredis) {
	t.Helper()

	mr := miniredis.RunT(t)
	cfg := &Config{Path: "/taco", Name: "taco", StateKey: "toggle:test"}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	replicas := make([]*replica, 0, n)
	for range n {
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		st := NewStore(rdb, cfg.StateKey, discardLogger())
		srv := httptest.NewServer(newHandler(st, cfg, discardLogger()))

		t.Cleanup(func() {
			srv.Close()
			rdb.Close()
		})

		st.Run(ctx)
		waitFor(t, "replica ready", st.Ready)
		replicas = append(replicas, &replica{srv: srv, st: st})
	}
	return replicas, mr
}

// get issues a real HTTP GET and returns the status and body.
func get(t *testing.T, c *http.Client, url string) (int, string) {
	t.Helper()

	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", url, err)
	}
	return resp.StatusCode, string(body)
}

// post issues a real HTTP POST with no body and returns the status.
func post(t *testing.T, c *http.Client, url string) int {
	t.Helper()

	resp, err := c.Post(url, "", nil)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

// waitForFleet blocks until every replica's endpoint returns want, which is
// necessary because propagation is asynchronous.
func waitForFleet(t *testing.T, c *http.Client, replicas []*replica, want int) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		agreed := true
		for _, r := range replicas {
			if code, _ := get(t, c, r.srv.URL+"/taco"); code != want {
				agreed = false
				break
			}
		}
		if agreed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	for i, r := range replicas {
		code, body := get(t, c, r.srv.URL+"/taco")
		t.Logf("  replica %d: %d %s", i, code, body)
	}
	t.Fatalf("fleet did not converge on %d", want)
}

// TestE2EToggleReachesEveryReplica is the headline guarantee: Railway
// load-balances across replicas behind one domain, so a toggle delivered to
// whichever replica happened to answer must reach the others.
func TestE2EToggleReachesEveryReplica(t *testing.T) {
	replicas, _ := fleet(t, 3)
	c := &http.Client{Timeout: 2 * time.Second}

	waitForFleet(t, c, replicas, http.StatusOK)

	// Deliver /off to the first replica only.
	if code := post(t, c, replicas[0].srv.URL+"/off"); code != http.StatusOK {
		t.Fatalf("POST /off = %d, want 200", code)
	}
	waitForFleet(t, c, replicas, http.StatusInternalServerError)

	// Recover via a different replica, proving the direction is not special.
	if code := post(t, c, replicas[2].srv.URL+"/on"); code != http.StatusOK {
		t.Fatalf("POST /on = %d, want 200", code)
	}
	waitForFleet(t, c, replicas, http.StatusOK)

	for i, r := range replicas {
		code, body := get(t, c, r.srv.URL+"/taco")
		if code != http.StatusOK || body != `{"taco":true}` {
			t.Errorf("replica %d: %d %q", i, code, body)
		}
	}
}

// TestE2EKeepAlivePinnedClientSeesChanges reproduces the real caller shape.
//
// A Go HTTP client with keep-alive resolves the service name once and reuses
// one connection, so every retry lands on the same replica. That is the
// scenario in which a replica that missed a broadcast would stay broken
// forever for that caller -- so a pinned client must still observe a toggle
// delivered elsewhere.
func TestE2EKeepAlivePinnedClientSeesChanges(t *testing.T) {
	replicas, _ := fleet(t, 3)

	// A client that reuses one connection to replica 0 and nothing else.
	pinned := &http.Client{
		Timeout: 2 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1,
			DisableKeepAlives:   false,
		},
	}
	target := replicas[0].srv.URL + "/taco"

	if code, _ := get(t, pinned, target); code != http.StatusOK {
		t.Fatalf("precondition: expected 200")
	}

	// Toggle via a different replica entirely.
	control := &http.Client{Timeout: 2 * time.Second}
	if code := post(t, control, replicas[1].srv.URL+"/off"); code != http.StatusOK {
		t.Fatalf("POST /off = %d, want 200", code)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if code, _ := get(t, pinned, target); code == http.StatusInternalServerError {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("a keep-alive client pinned to one replica never observed the toggle")
}

// TestE2EHealthAgreesAcrossReplicas pins the reason /health reads Redis rather
// than the local atomic: the control system polls one domain, lands on a
// random replica each time, and must not see the state flicker.
func TestE2EHealthAgreesAcrossReplicas(t *testing.T) {
	replicas, _ := fleet(t, 3)
	c := &http.Client{Timeout: 2 * time.Second}

	for _, want := range []bool{false, true} {
		path := map[bool]string{true: "/on", false: "/off"}[want]
		if code := post(t, c, replicas[0].srv.URL+path); code != http.StatusOK {
			t.Fatalf("POST %s = %d, want 200", path, code)
		}

		// No wait: /health is authoritative, so every replica must agree
		// immediately, even before pub/sub has propagated.
		for i, r := range replicas {
			code, body := get(t, c, r.srv.URL+"/health")
			if code != http.StatusOK {
				t.Errorf("replica %d /health = %d, want 200 always", i, code)
			}

			var got struct {
				On    bool `json:"on"`
				Stale bool `json:"stale"`
			}
			if err := json.Unmarshal([]byte(body), &got); err != nil {
				t.Fatalf("replica %d /health body %q: %v", i, body, err)
			}
			if got.On != want {
				t.Errorf("replica %d /health on = %v, want %v (flicker)", i, got.On, want)
			}
			if got.Stale {
				t.Errorf("replica %d reported stale with Redis up", i)
			}
		}
	}
}

// TestE2EFleetSurvivesRedisOutage pins the payoff of caching state locally:
// the endpoint keeps serving through a Redis outage instead of failing with
// it, and says so on /health.
func TestE2EFleetSurvivesRedisOutage(t *testing.T) {
	replicas, mr := fleet(t, 3)
	c := &http.Client{Timeout: 2 * time.Second}

	waitForFleet(t, c, replicas, http.StatusOK)
	mr.Close()

	// Several reconcile ticks must fail without changing behaviour.
	time.Sleep(3 * reconcileInterval)

	for i, r := range replicas {
		if code, body := get(t, c, r.srv.URL+"/taco"); code != http.StatusOK {
			t.Errorf("replica %d stopped serving during a Redis outage: %d %q",
				i, code, body)
		}

		code, body := get(t, c, r.srv.URL+"/health")
		if code != http.StatusOK {
			t.Errorf("replica %d /health = %d, want 200 always", i, code)
		}
		if body != `{"on":true,"stale":true}` {
			t.Errorf("replica %d /health = %q, want the stale marker", i, body)
		}

		// Writes must fail closed while the source of truth is gone.
		if code := post(t, c, r.srv.URL+"/off"); code != http.StatusServiceUnavailable {
			t.Errorf("replica %d POST /off = %d, want 503", i, code)
		}
	}
}

// TestE2EContract pins the exact wire format every consumer depends on: the
// control system parses /health and /on, and Temporal keys its retries on the
// endpoint's status code. A change here breaks callers silently.
func TestE2EContract(t *testing.T) {
	replicas, _ := fleet(t, 1)
	base := replicas[0].srv.URL
	c := &http.Client{Timeout: 2 * time.Second}

	steps := []struct {
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{http.MethodGet, "/taco", http.StatusOK, `{"taco":true}`},
		{http.MethodGet, "/health", http.StatusOK, `{"on":true}`},
		{http.MethodGet, "/ready", http.StatusOK, `{"ready":true}`},
		{http.MethodPost, "/off", http.StatusOK, `{"on":false}`},
		{http.MethodGet, "/taco", http.StatusInternalServerError, ``},
		{http.MethodGet, "/health", http.StatusOK, `{"on":false}`},
		{http.MethodGet, "/ready", http.StatusOK, `{"ready":true}`},
		{http.MethodPost, "/on", http.StatusOK, `{"on":true}`},
		{http.MethodGet, "/taco", http.StatusOK, `{"taco":true}`},
		{http.MethodGet, "/off", http.StatusMethodNotAllowed, ``},
		{http.MethodGet, "/on", http.StatusMethodNotAllowed, ``},
		{http.MethodGet, "/nope", http.StatusNotFound, "404 page not found\n"},
	}

	for _, s := range steps {
		req, err := http.NewRequest(s.method, base+s.path, nil)
		if err != nil {
			t.Fatalf("building %s %s: %v", s.method, s.path, err)
		}
		resp, err := c.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", s.method, s.path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode != s.wantStatus {
			t.Errorf("%s %s status = %d, want %d",
				s.method, s.path, resp.StatusCode, s.wantStatus)
		}
		if string(body) != s.wantBody {
			t.Errorf("%s %s body = %q, want %q",
				s.method, s.path, string(body), s.wantBody)
		}
	}
}
