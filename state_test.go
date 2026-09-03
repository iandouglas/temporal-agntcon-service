package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// discardLogger keeps test output readable; assertions are on state, not logs.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// newTestStore returns a Store wired to an in-process Redis, plus the server
// handle so tests can inspect or break it.
func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis, *redis.Client) {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })

	return NewStore(rdb, "toggle:test", discardLogger()), mr, rdb
}

// waitFor polls cond until it holds or the deadline passes. Propagation is
// asynchronous, so tests must not assert immediately after a write.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestStoreBootsOnAndNotReady(t *testing.T) {
	st, _, _ := newTestStore(t)

	if !st.On() {
		t.Error("a fresh replica should boot on")
	}
	if st.Ready() {
		t.Error("a replica must not report ready before its first Redis read")
	}
}

func TestStoreInitialSyncSeedsAbsentKey(t *testing.T) {
	st, mr, _ := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st.Run(ctx)

	waitFor(t, "ready", st.Ready)

	// Seeding matters because /health reads Redis directly: without a
	// value present, the very first health poll would fail rather than
	// report the boot state.
	got, err := mr.Get("toggle:test")
	if err != nil {
		t.Fatalf("key was not seeded: %v", err)
	}
	if got != redisOn {
		t.Errorf("seeded value = %q, want %q", got, redisOn)
	}
}

func TestStoreInitialSyncAdoptsExistingOffState(t *testing.T) {
	st, mr, _ := newTestStore(t)

	// A replica restarting into a deliberately-off demo must adopt that
	// state, not clobber it with its boot default.
	mr.Set("toggle:test", redisOff)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st.Run(ctx)

	waitFor(t, "ready", st.Ready)
	if st.On() {
		t.Error("replica overwrote an existing off state with its boot default")
	}
}

func TestStoreSetPersistsAndAppliesLocally(t *testing.T) {
	st, mr, _ := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st.Run(ctx)
	waitFor(t, "ready", st.Ready)

	if err := st.Set(ctx, false); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if st.On() {
		t.Error("local state should be off immediately after a successful Set")
	}
	if got, _ := mr.Get("toggle:test"); got != redisOff {
		t.Errorf("Redis value = %q, want %q", got, redisOff)
	}
}

func TestStorePropagatesToOtherReplicaViaPubSub(t *testing.T) {
	// This is the core guarantee: flipping one replica must reach the
	// others, since Railway load-balances across them behind one domain.
	mr := miniredis.RunT(t)
	log := discardLogger()

	newReplica := func() *Store {
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { rdb.Close() })
		return NewStore(rdb, "toggle:test", log)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, b, c := newReplica(), newReplica(), newReplica()
	for _, st := range []*Store{a, b, c} {
		st.Run(ctx)
		waitFor(t, "replica ready", st.Ready)
	}

	if err := a.Set(ctx, false); err != nil {
		t.Fatalf("Set on replica a: %v", err)
	}

	waitFor(t, "replica b to observe off", func() bool { return !b.On() })
	waitFor(t, "replica c to observe off", func() bool { return !c.On() })

	if err := a.Set(ctx, true); err != nil {
		t.Fatalf("Set on replica a: %v", err)
	}
	waitFor(t, "replica b to observe on", b.On)
	waitFor(t, "replica c to observe on", c.On)
}

func TestStoreReconcileRepairsAStrandedReplica(t *testing.T) {
	// Simulates a lost broadcast by writing the key behind the store's
	// back, with no PUBLISH. Only the reconcile loop can recover this, and
	// it must, because a keep-alive client pins to a single replica.
	st, mr, _ := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st.Run(ctx)
	waitFor(t, "ready", st.Ready)

	if !st.On() {
		t.Fatal("precondition: expected replica to be on")
	}

	mr.Set("toggle:test", redisOff)

	waitFor(t, "reconcile to repair local state", func() bool { return !st.On() })
}

func TestStoreSetFailsClosedWhenRedisIsDown(t *testing.T) {
	// Applying optimistically would create a divergence that the reconcile
	// loop reverts moments later, so a failed write must not change local
	// state and must surface as an error.
	st, mr, _ := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st.Run(ctx)
	waitFor(t, "ready", st.Ready)

	mr.Close()

	if err := st.Set(ctx, false); err == nil {
		t.Fatal("expected Set to fail while Redis is unreachable")
	}
	if !st.On() {
		t.Error("local state changed despite the Redis write failing")
	}
}

func TestStoreServesLastKnownStateWhenRedisDies(t *testing.T) {
	// The whole point of caching in an atomic: the endpoint keeps serving
	// through a Redis outage instead of failing with it.
	st, mr, _ := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st.Run(ctx)
	waitFor(t, "ready", st.Ready)

	mr.Close()

	// Long enough for several reconcile ticks to fail.
	time.Sleep(3 * reconcileInterval)

	if !st.On() {
		t.Error("replica abandoned its last-known state after losing Redis")
	}
	if _, err := st.Fetch(ctx); err == nil {
		t.Error("Fetch should report an error while Redis is unreachable")
	}
}

func TestStoreWithdrawClearsReady(t *testing.T) {
	st, _, _ := newTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st.Run(ctx)
	waitFor(t, "ready", st.Ready)

	st.Withdraw()
	if st.Ready() {
		t.Error("Withdraw should make /ready report 503")
	}
}

func TestDecodeAcceptsCommonEncodings(t *testing.T) {
	// Tolerant on read so a value set by hand with redis-cli still works.
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{redisOn, true}, {"true", true}, {"on", true},
		{redisOff, false}, {"false", false}, {"off", false},
	} {
		got, err := decode(tc.in)
		if err != nil {
			t.Errorf("decode(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("decode(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}

	if _, err := decode("maybe"); err == nil {
		t.Error("decode should reject an unrecognised value")
	}
}
