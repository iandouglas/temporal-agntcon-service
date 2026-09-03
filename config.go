package main

import (
	"fmt"
	"strings"
)

// Reserved paths cannot be used as the toggleable ENDPOINT, since doing so
// would shadow a control or probe route.
var reservedPaths = map[string]struct{}{
	"/on":     {},
	"/off":    {},
	"/health": {},
	"/ready":  {},
	"/":       {},
}

const (
	defaultPort      = "8080"
	defaultKeyPrefix = "toggle"
)

// Config is the fully-resolved runtime configuration. Every value is derived
// once at startup so that request handling never reads the environment or
// allocates a derived string.
type Config struct {
	// Port is the TCP port to listen on. Railway injects PORT.
	Port string

	// Path is the URL path of the toggleable endpoint, e.g. "/taco". It
	// always has exactly one leading slash and no trailing slash.
	Path string

	// Name is Path without its leading slash, used as the JSON response
	// key, e.g. "taco".
	Name string

	// StateKey is the Redis key holding this service's on/off state. It
	// doubles as the pub/sub channel name; Redis keeps the keyspace and
	// the pub/sub namespace separate, so there is no collision.
	StateKey string

	// RedisURL is the connection URL for the shared Redis instance.
	RedisURL string
}

// LoadConfig reads and validates configuration from the environment.
//
// ENDPOINT is the only variable that must be set per service; the JSON key
// and the Redis key are derived from it so they cannot drift out of sync. Set
// STATE_KEY to override the derived Redis key outright, or KEY_PREFIX to
// change only its namespace.
//
// Errors are returned rather than defaulted: a misconfigured replica that
// starts anyway would silently share state with nothing, which is far harder
// to spot than a container that refuses to boot.
func LoadConfig(getenv func(string) string) (*Config, error) {
	endpoint := strings.TrimSpace(getenv("ENDPOINT"))
	if endpoint == "" {
		return nil, fmt.Errorf("ENDPOINT is required (e.g. ENDPOINT=/taco)")
	}

	name := strings.Trim(endpoint, "/")
	if name == "" {
		return nil, fmt.Errorf("ENDPOINT %q has no path segment", endpoint)
	}
	if strings.Contains(name, "/") {
		return nil, fmt.Errorf("ENDPOINT %q must be a single path segment; "+
			"it becomes the JSON response key", endpoint)
	}

	path := "/" + name
	if _, reserved := reservedPaths[path]; reserved {
		return nil, fmt.Errorf("ENDPOINT %q is reserved", path)
	}

	// Railway's Redis addon exposes both a public proxy URL and a private
	// network URL. Prefer whatever the operator wired to REDIS_URL, then
	// fall back to the private URL so a bare Railway variable reference
	// also works.
	redisURL := strings.TrimSpace(getenv("REDIS_URL"))
	if redisURL == "" {
		redisURL = strings.TrimSpace(getenv("REDIS_PRIVATE_URL"))
	}
	if redisURL == "" {
		return nil, fmt.Errorf("REDIS_URL is required " +
			"(use ${{Redis.REDIS_PRIVATE_URL}} on Railway)")
	}

	stateKey := strings.TrimSpace(getenv("STATE_KEY"))
	if stateKey == "" {
		prefix := strings.TrimSpace(getenv("KEY_PREFIX"))
		if prefix == "" {
			prefix = defaultKeyPrefix
		}
		stateKey = prefix + ":" + name
	}

	port := strings.TrimSpace(getenv("PORT"))
	if port == "" {
		port = defaultPort
	}

	return &Config{
		Port:     port,
		Path:     path,
		Name:     name,
		StateKey: stateKey,
		RedisURL: redisURL,
	}, nil
}
