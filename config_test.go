package main

import "testing"

// envFunc builds a getenv stand-in from a map so LoadConfig can be tested
// without mutating the process environment.
func envFunc(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func TestLoadConfigDerivesKeysFromEndpoint(t *testing.T) {
	cfg, err := LoadConfig(envFunc(map[string]string{
		"ENDPOINT":  "/taco",
		"REDIS_URL": "redis://localhost:6379",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	if cfg.Path != "/taco" {
		t.Errorf("Path = %q, want /taco", cfg.Path)
	}
	if cfg.Name != "taco" {
		t.Errorf("Name = %q, want taco", cfg.Name)
	}
	if cfg.StateKey != "toggle:taco" {
		t.Errorf("StateKey = %q, want toggle:taco", cfg.StateKey)
	}
	if cfg.Port != defaultPort {
		t.Errorf("Port = %q, want %q", cfg.Port, defaultPort)
	}
}

func TestLoadConfigNormalisesEndpoint(t *testing.T) {
	// The endpoint is copied by hand into a Railway dashboard, so accept
	// the shapes a human is likely to type.
	for _, in := range []string{"taco", "/taco", "/taco/", "  /taco  "} {
		cfg, err := LoadConfig(envFunc(map[string]string{
			"ENDPOINT":  in,
			"REDIS_URL": "redis://localhost:6379",
		}))
		if err != nil {
			t.Fatalf("ENDPOINT=%q: %v", in, err)
		}
		if cfg.Path != "/taco" || cfg.Name != "taco" {
			t.Errorf("ENDPOINT=%q gave Path=%q Name=%q", in, cfg.Path, cfg.Name)
		}
	}
}

func TestLoadConfigOverrides(t *testing.T) {
	t.Run("STATE_KEY replaces the derived key", func(t *testing.T) {
		cfg, err := LoadConfig(envFunc(map[string]string{
			"ENDPOINT":  "/taco",
			"REDIS_URL": "redis://localhost:6379",
			"STATE_KEY": "toggle:shared",
		}))
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if cfg.StateKey != "toggle:shared" {
			t.Errorf("StateKey = %q, want toggle:shared", cfg.StateKey)
		}
		if cfg.Name != "taco" {
			t.Errorf("STATE_KEY must not affect the JSON key, got %q", cfg.Name)
		}
	})

	t.Run("KEY_PREFIX replaces only the namespace", func(t *testing.T) {
		cfg, err := LoadConfig(envFunc(map[string]string{
			"ENDPOINT":   "/coffee",
			"REDIS_URL":  "redis://localhost:6379",
			"KEY_PREFIX": "agentcon2026",
		}))
		if err != nil {
			t.Fatalf("LoadConfig: %v", err)
		}
		if cfg.StateKey != "agentcon2026:coffee" {
			t.Errorf("StateKey = %q, want agentcon2026:coffee", cfg.StateKey)
		}
	})
}

func TestLoadConfigFallsBackToPrivateRedisURL(t *testing.T) {
	// A bare ${{Redis.REDIS_PRIVATE_URL}} reference should work without
	// also having to set REDIS_URL.
	cfg, err := LoadConfig(envFunc(map[string]string{
		"ENDPOINT":          "/taco",
		"REDIS_PRIVATE_URL": "redis://redis.railway.internal:6379",
	}))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.RedisURL != "redis://redis.railway.internal:6379" {
		t.Errorf("RedisURL = %q", cfg.RedisURL)
	}
}

func TestLoadConfigRejectsBadInput(t *testing.T) {
	// Each of these would otherwise produce a replica that boots happily
	// and then shares state with nothing, which is far harder to diagnose
	// than a container that refuses to start.
	cases := map[string]map[string]string{
		"missing ENDPOINT":  {"REDIS_URL": "redis://localhost:6379"},
		"empty ENDPOINT":    {"ENDPOINT": "  ", "REDIS_URL": "redis://localhost:6379"},
		"slash only":        {"ENDPOINT": "/", "REDIS_URL": "redis://localhost:6379"},
		"multi segment":     {"ENDPOINT": "/api/taco", "REDIS_URL": "redis://localhost:6379"},
		"reserved /health":  {"ENDPOINT": "/health", "REDIS_URL": "redis://localhost:6379"},
		"reserved /on":      {"ENDPOINT": "/on", "REDIS_URL": "redis://localhost:6379"},
		"reserved /ready":   {"ENDPOINT": "/ready", "REDIS_URL": "redis://localhost:6379"},
		"missing REDIS_URL": {"ENDPOINT": "/taco"},
	}

	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadConfig(envFunc(env)); err == nil {
				t.Fatal("expected an error, got nil")
			}
		})
	}
}
