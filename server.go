package main

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
)

// Header values are pre-built as slices so handlers can assign them directly
// into the header map. Going through Header().Set would re-run MIME
// canonicalisation of the key on every request for no benefit.
var (
	hdrJSON      = []string{"application/json"}
	hdrAllowPost = []string{"POST"}
)

// Response bodies are fixed at startup, so serving one is a single write of a
// pre-existing byte slice with no marshalling and no allocation.
var (
	bodyHealthOn       = []byte(`{"on":true}`)
	bodyHealthOff      = []byte(`{"on":false}`)
	bodyHealthOnStale  = []byte(`{"on":true,"stale":true}`)
	bodyHealthOffStale = []byte(`{"on":false,"stale":true}`)
	bodyReady          = []byte(`{"ready":true}`)
	bodyNotReady       = []byte(`{"ready":false}`)
)

// handler serves every route. It replaces http.ServeMux deliberately: with
// five fixed paths, a switch on the raw path beats a map lookup and avoids
// the mux's redirect and cleanup logic entirely.
type handler struct {
	st   *Store
	path string // the toggleable endpoint, e.g. "/taco"
	log  *slog.Logger

	onBody []byte   // e.g. {"taco":true}
	onLen  []string // Content-Length for onBody
}

func newHandler(st *Store, cfg *Config, log *slog.Logger) *handler {
	body := []byte(`{"` + cfg.Name + `":true}`)
	return &handler{
		st:     st,
		path:   cfg.Path,
		log:    log,
		onBody: body,
		onLen:  []string{strconv.Itoa(len(body))},
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case h.path:
		h.serveToggleable(w)
	case "/health":
		h.serveHealth(w, r)
	case "/ready":
		h.serveReady(w)
	case "/on":
		h.serveSet(w, r, true)
	case "/off":
		h.serveSet(w, r, false)
	default:
		http.NotFound(w, r)
	}
}

// serveToggleable is the hot path. Callers retry this aggressively while the
// service is off, so the failure branch is kept as cheap as the success one:
// no body to write, and nothing logged.
//
// Any method is accepted here. Unlike /on and /off this endpoint mutates
// nothing, so rejecting a HEAD or POST would only create a way for a caller
// to fail for the wrong reason mid-demo.
func (h *handler) serveToggleable(w http.ResponseWriter) {
	if !h.st.On() {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	hdr := w.Header()
	hdr["Content-Type"] = hdrJSON
	hdr["Content-Length"] = h.onLen
	w.Write(h.onBody)
}

// serveHealth reports the authoritative state and always returns 200.
//
// The status code must never track the toggle: Railway restarts a container
// whose health check returns non-2xx, so an "off" service that answered 503
// here would be restarted on every poll, wiping the state that was just set.
//
// It reads Redis rather than the local atomic so that all replicas agree, and
// a polling client never sees flicker from landing on a replica that has not
// yet received a broadcast. A read failure falls back to the local value and
// says so.
func (h *handler) serveHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), redisOpTimeout)
	defer cancel()

	var body []byte
	if on, err := h.st.Fetch(ctx); err == nil {
		body = bodyHealthOff
		if on {
			body = bodyHealthOn
		}
	} else if h.st.On() {
		body = bodyHealthOnStale
	} else {
		body = bodyHealthOffStale
	}

	hdr := w.Header()
	hdr["Content-Type"] = hdrJSON
	hdr["Content-Length"] = []string{strconv.Itoa(len(body))}
	w.Write(body)
}

// serveReady is Railway's health check target. It reports 503 until this
// replica has read Redis at least once, which is what makes booting to a
// default state safe: no traffic arrives before the real state is known.
func (h *handler) serveReady(w http.ResponseWriter) {
	body, status := bodyNotReady, http.StatusServiceUnavailable
	if h.st.Ready() {
		body, status = bodyReady, http.StatusOK
	}
	hdr := w.Header()
	hdr["Content-Type"] = hdrJSON
	hdr["Content-Length"] = []string{strconv.Itoa(len(body))}
	w.WriteHeader(status)
	w.Write(body)
}

// serveSet handles /on and /off. POST only: these mutate shared state that
// every replica of every service instance observes, and a 405 on GET makes
// that explicit rather than leaving the endpoint reachable by anything that
// happens to follow a URL.
func (h *handler) serveSet(w http.ResponseWriter, r *http.Request, on bool) {
	if r.Method != http.MethodPost {
		hdr := w.Header()
		hdr["Allow"] = hdrAllowPost
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), redisOpTimeout)
	defer cancel()

	if err := h.st.Set(ctx, on); err != nil {
		// Worth logging: a toggle that did not take is the one failure
		// that would leave an operator staring at an unchanged service
		// with no explanation.
		h.log.Error("toggle failed", "requested", on, "err", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	body := bodyHealthOff
	if on {
		body = bodyHealthOn
	}
	hdr := w.Header()
	hdr["Content-Type"] = hdrJSON
	hdr["Content-Length"] = []string{strconv.Itoa(len(body))}
	w.Write(body)
}
