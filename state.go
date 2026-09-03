package main

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	redisOn  = "1"
	redisOff = "0"

	// bootDefault is the state a replica assumes before its first
	// successful Redis read. "On" is the steady state of the demo, so
	// booting on avoids a blip on every deploy. The /ready gate keeps
	// traffic away until the first read lands, so this default is never
	// actually served.
	bootDefault = true

	// reconcileInterval bounds how long a replica can hold a wrong state
	// after missing a pub/sub message. A keep-alive HTTP client pins to a
	// single replica, so without this floor one stranded replica would
	// stay broken indefinitely while /health reported healthy.
	reconcileInterval = 250 * time.Millisecond

	// initialBackoffMax caps the retry delay while waiting for Redis at
	// boot. Railway's private DNS can take a few seconds to resolve on a
	// cold start, so a failed first dial is expected, not fatal.
	initialBackoffMax = 2 * time.Second

	// redisOpTimeout bounds any single Redis call made while serving a
	// request, so a wedged Redis cannot hold a connection open.
	redisOpTimeout = 250 * time.Millisecond
)

// ErrNotConnected is returned by Fetch before the Redis client is available.
var ErrNotConnected = errors.New("redis client not initialised")

// Store owns this replica's view of the on/off state.
//
// Reads on the hot path hit only the atomic, costing a single relaxed load
// with no syscall, allocation, or lock. Writes go to Redis and are broadcast
// over pub/sub; every replica applies inbound broadcasts to its own atomic,
// and a slow reconcile loop re-reads Redis as a backstop against a lost
// message. Redis is the source of truth; the atomic is a cache of it.
type Store struct {
	on    atomic.Bool
	ready atomic.Bool

	rdb *redis.Client
	key string
	log *slog.Logger
}

// NewStore returns a Store seeded with the boot default and marked not-ready.
// It performs no I/O; call Run to connect and begin syncing.
func NewStore(rdb *redis.Client, key string, log *slog.Logger) *Store {
	s := &Store{rdb: rdb, key: key, log: log}
	s.on.Store(bootDefault)
	return s
}

// On reports whether the toggleable endpoint should serve its payload. This
// is the hot path: one atomic load, nothing else.
func (s *Store) On() bool { return s.on.Load() }

// Ready reports whether this replica has completed at least one successful
// Redis read. Railway's health check is pointed at this so no traffic is
// routed to a replica still serving its boot default.
func (s *Store) Ready() bool { return s.ready.Load() }

// Set writes the state to Redis and broadcasts it to every replica.
//
// The local atomic is updated only after Redis accepts the write. Applying it
// optimistically on failure would create a divergence that the reconcile loop
// would silently revert moments later, which is worse than a clear error.
func (s *Store) Set(ctx context.Context, on bool) error {
	if s.rdb == nil {
		return ErrNotConnected
	}
	val := encode(on)

	// One round trip for both commands: the durable write that new and
	// reconciling replicas will read, and the broadcast that gets the
	// change to live replicas in about a millisecond.
	pipe := s.rdb.Pipeline()
	pipe.Set(ctx, s.key, val, 0)
	pipe.Publish(ctx, s.key, val)
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}

	s.apply(on, "http")
	return nil
}

// Fetch reads the authoritative state straight from Redis. /health uses this
// so that every replica returns the same answer and a polling client never
// sees flicker from landing on a replica mid-propagation.
func (s *Store) Fetch(ctx context.Context) (bool, error) {
	if s.rdb == nil {
		return false, ErrNotConnected
	}
	val, err := s.rdb.Get(ctx, s.key).Result()
	if err != nil {
		return false, err
	}
	return decode(val)
}

// Run drives state synchronisation until ctx is cancelled. It blocks until
// the initial sync succeeds or ctx ends, then leaves the subscriber and
// reconcile loops running in the background.
func (s *Store) Run(ctx context.Context) {
	if !s.initialSync(ctx) {
		return
	}
	go s.subscribe(ctx)
	go s.reconcile(ctx)
}

// initialSync seeds the key if absent, then reads it, retrying with backoff.
// It reports whether the sync completed before ctx was cancelled.
func (s *Store) initialSync(ctx context.Context) bool {
	backoff := 50 * time.Millisecond
	for {
		// SetNX seeds a cold Redis without clobbering a state someone
		// deliberately set, so /health always has a value to report.
		if err := s.rdb.SetNX(ctx, s.key, encode(bootDefault), 0).Err(); err != nil {
			s.log.Warn("redis seed failed, retrying",
				"key", s.key, "backoff", backoff, "err", err)
		} else if on, err := s.Fetch(ctx); err != nil {
			s.log.Warn("redis initial read failed, retrying",
				"key", s.key, "backoff", backoff, "err", err)
		} else {
			s.apply(on, "initial")
			s.ready.Store(true)
			s.log.Info("redis synced", "key", s.key, "on", on)
			return true
		}

		select {
		case <-ctx.Done():
			return false
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > initialBackoffMax {
			backoff = initialBackoffMax
		}
	}
}

// subscribe applies state broadcasts as they arrive. go-redis reconnects the
// subscription internally; any state missed during a reconnect is repaired by
// the reconcile loop rather than handled here.
func (s *Store) subscribe(ctx context.Context) {
	sub := s.rdb.Subscribe(ctx, s.key)
	defer sub.Close()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, open := <-ch:
			if !open {
				return
			}
			on, err := decode(msg.Payload)
			if err != nil {
				s.log.Warn("ignoring malformed broadcast",
					"payload", msg.Payload, "err", err)
				continue
			}
			s.apply(on, "pubsub")
		}
	}
}

// reconcile re-reads Redis on a fixed interval. This is the backstop that
// bounds how long a replica can disagree with the source of truth.
func (s *Store) reconcile(ctx context.Context) {
	t := time.NewTicker(reconcileInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			readCtx, cancel := context.WithTimeout(ctx, redisOpTimeout)
			on, err := s.Fetch(readCtx)
			cancel()
			if err != nil {
				// Deliberately not logged: a Redis outage would
				// emit four lines per second per replica and
				// bury the state timeline. Staleness surfaces
				// on /health instead.
				continue
			}
			s.apply(on, "reconcile")
		}
	}
}

// apply stores the state locally, logging only genuine transitions. Because
// every path funnels through here, the log becomes a clean timeline of state
// changes rather than a record of redundant writes -- a replica receiving the
// broadcast of a change it just made itself logs nothing.
func (s *Store) apply(on bool, src string) {
	if s.on.Swap(on) != on {
		s.log.Info("state changed", "on", on, "src", src)
	}
}

func encode(on bool) string {
	if on {
		return redisOn
	}
	return redisOff
}

func decode(val string) (bool, error) {
	switch val {
	case redisOn, "true", "on":
		return true, nil
	case redisOff, "false", "off":
		return false, nil
	default:
		return false, errors.New("unrecognised state value: " + val)
	}
}

// Withdraw marks this replica not-ready so /ready reports 503. Called at the
// start of shutdown so anything polling this replica observes it leaving
// rotation before its connections are closed.
func (s *Store) Withdraw() { s.ready.Store(false) }
