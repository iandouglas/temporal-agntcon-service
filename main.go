package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	readTimeout  = 5 * time.Second
	writeTimeout = 5 * time.Second
	idleTimeout  = 90 * time.Second

	// shutdownGrace bounds connection draining on SIGTERM. Railway sends
	// SIGTERM and follows with SIGKILL, so this stays well inside that
	// window.
	shutdownGrace = 10 * time.Second
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// listen binds the service's TCP listener on both address families.
//
// Railway's private network is IPv6-only, so a listener that is not reachable
// over IPv6 leaves the service unreachable at <service>.railway.internal
// while still answering fine on the public domain -- a deploy that looks
// healthy with every caller broken.
//
// The network must stay "tcp" and the host must stay empty. Go opens an
// AF_INET6 socket with V6ONLY off whenever the network is "tcp" and the host
// is the unspecified address, which covers both families. Passing "tcp4", or
// any literal host such as 127.0.0.1, drops IPv6 and causes exactly that
// failure. ("0.0.0.0" happens to be safe here, since Go also treats it as
// unspecified, but the empty host says what is meant.)
func listen(port string) (net.Listener, error) {
	return net.Listen("tcp", ":"+port)
}

func run(log *slog.Logger) error {
	cfg, err := LoadConfig(os.Getenv)
	if err != nil {
		return err
	}

	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return errors.New("invalid REDIS_URL: " + err.Error())
	}
	rdb := redis.NewClient(opts)
	defer rdb.Close()

	st := NewStore(rdb, cfg.StateKey, log)

	ln, err := listen(cfg.Port)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Handler:      newHandler(st, cfg, log),
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		IdleTimeout:  idleTimeout,
		// The stdlib logs each accept/TLS error via the standard
		// logger by default; route it through slog so every line in
		// Railway's log stream is structured.
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	// Cancelled on SIGTERM (Railway's stop signal) or SIGINT (local Ctrl-C).
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Serve before Redis is reachable so /ready can answer 503 while the
	// initial sync retries. Railway's private DNS can take a few seconds
	// to resolve on a cold start, and a container that refused to listen
	// during that window would fail its own health check.
	serveErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil &&
			!errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	go st.Run(ctx)

	log.Info("listening",
		"addr", ln.Addr().String(),
		"endpoint", cfg.Path,
		"json_key", cfg.Name,
		"state_key", cfg.StateKey,
	)

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	return shutdown(srv, st, log)
}

// shutdown drains in-flight connections. /ready is flipped to 503 first so
// that anything still polling this replica sees it leaving rotation before
// its connections are closed.
func shutdown(srv *http.Server, st *Store, log *slog.Logger) error {
	start := time.Now()
	st.Withdraw()
	log.Info("shutting down", "grace", shutdownGrace)

	ctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Warn("drain incomplete", "elapsed", time.Since(start), "err", err)
		return err
	}
	log.Info("drained", "elapsed", time.Since(start))
	return nil
}
