# Toggle Service

A small Go service whose main endpoint can be switched on and off at runtime.
When it is on, that endpoint returns a tiny JSON payload; when it is off, it
returns a bare `500`. The switch is shared across every replica through Redis,
so flipping one replica flips them all.

Built for a conference demo: an audience-facing control system calls `/on` and
`/off`, while Temporal workflows hammer the main endpoint and retry through the
failures.

## Endpoints

| Method | Path | Response |
|---|---|---|
| any | `/<ENDPOINT>` | `200` `{"<name>":true}` when on; bare `500`, no body, when off |
| `POST` | `/on` | `200` `{"on":true}` — turns the fleet on |
| `POST` | `/off` | `200` `{"on":false}` — turns the fleet off |
| any | `/health` | **always** `200` `{"on":<bool>}`; adds `"stale":true` if Redis is unreachable |
| any | `/ready` | `200` `{"ready":true}` once synced with Redis; `503` `{"ready":false}` before that |

`/on` and `/off` are `POST`-only; any other method returns `405` with
`Allow: POST`. The main endpoint accepts any method, since it mutates nothing.

If a toggle cannot be written to Redis, the endpoint returns `503` and the
local state is left **unchanged** — applying it optimistically would create a
divergence that the reconcile loop would silently revert moments later.

## Configuration

| Variable | Required | Default | Notes |
|---|---|---|---|
| `ENDPOINT` | **yes** | — | The toggleable path, e.g. `/taco`. A single path segment. |
| `REDIS_URL` | **yes** | — | Redis connection URL. Falls back to `REDIS_PRIVATE_URL` if unset. |
| `PORT` | no | `8080` | Injected by Railway. |
| `KEY_PREFIX` | no | `toggle` | Namespace for the derived Redis key. |
| `STATE_KEY` | no | derived | Overrides the Redis key outright. |

### How the keys are derived

`ENDPOINT` is the only per-service variable you need. Everything else follows
from it, so the JSON key and the Redis key cannot drift out of sync:

```
ENDPOINT=/taco
  ├─ route            /taco
  ├─ JSON key         {"taco":true}
  └─ Redis key        toggle:taco     ← KEY_PREFIX + ":" + name
```

`ENDPOINT` is normalised, so `taco`, `/taco`, `/taco/`, and `  /taco  ` are all
equivalent. Multi-segment paths (`/api/taco`) are **rejected** at startup,
because the segment becomes a JSON key. So are `/on`, `/off`, `/health`,
`/ready`, and `/`.

Set `STATE_KEY` only if you want two different services to share one switch:

```
# Both services flip together
taco:    ENDPOINT=/taco    STATE_KEY=toggle:shared
coffee:  ENDPOINT=/coffee  STATE_KEY=toggle:shared
```

A misconfiguration is a **startup failure**, never a default. A replica that
booted anyway would share state with nothing, which is far harder to spot than
a container that refuses to start.

## Redis setup

**Nothing to configure.** No `notify-keyspace-events`, no modules, no ACL
changes, no schema. The service uses only `GET`, `SET`, `SETNX`, `PUBLISH`, and
`SUBSCRIBE`, all available on a default Redis.

On Railway:

1. Add a Redis service to the project (**+ New → Database → Redis**).
2. In each toggle service's **Variables**, reference the private URL:

   ```
   REDIS_URL=${{Redis.REDIS_PRIVATE_URL}}
   ```

   Use `REDIS_PRIVATE_URL`, not `REDIS_URL`. The private one keeps traffic on
   Railway's internal network — no TLS, no egress, sub-millisecond. The public
   one routes through a TCP proxy and would also require CA certificates, which
   the `scratch` image deliberately does not carry.

3. One Redis instance serves every toggle service. `KEY_PREFIX` keeps them
   namespaced, so nothing collides.

### Keys written

One key per service, e.g. `toggle:taco`, holding `"1"` or `"0"` with **no
expiry**. The same name is used as the pub/sub channel; Redis keeps the keyspace
and the pub/sub namespace separate, so there is no conflict.

Inspect or drive it by hand:

```bash
redis-cli GET  toggle:taco          # read the authoritative state
redis-cli SET  toggle:taco 0        # off, propagates within 250ms via reconcile
redis-cli PUBLISH toggle:taco 0     # off, propagates within ~1ms
```

Reads are tolerant: `1`/`true`/`on` and `0`/`false`/`off` are all accepted, so
setting a value by hand works.

Wipe all demo state between sessions:

```bash
redis-cli --scan --pattern 'toggle:*' | xargs redis-cli DEL
```

Replicas notice the key is gone within 250ms and **reseed it from their
last-known state** — so a wipe does not silently turn a deliberately-off demo
back on. To reset to a known state, set the value rather than deleting it.

## Railway deployment

`railway.json` pins the deployment shape, so it lives in git rather than being
clicked into a dashboard once per service:

```json
{
  "deploy": {
    "numReplicas": 3,
    "healthcheckPath": "/ready"
  }
}
```

Deploy this repo once per flavour, changing only `ENDPOINT`:

| Railway service | `ENDPOINT` | Redis key | Replicas |
|---|---|---|---|
| `taco` | `/taco` | `toggle:taco` | 3 |
| `coffee` | `/coffee` | `toggle:coffee` | 3 |

### Two things that will bite you

**`healthcheckPath` must be `/ready`, never `/health`.** Railway restarts a
container whose health check returns non-2xx. `/health` always returns `200` for
exactly this reason — but if you pointed Railway at an endpoint that tracked the
toggle, calling `/off` would fail every replica's health check, restart all of
them, and wipe the state you just set. `/health` reports business state;
`/ready` reports whether the replica is fit to serve.

**Callers must use the private network.** Have Temporal workers hit
`http://taco.railway.internal:8080/taco`, not the public domain. The public
domain adds a TLS handshake and an edge-proxy hop — tens of milliseconds — which
completely dominates a request path that is otherwise ~17ns of work. The service
binds `:$PORT` (dual-stack) rather than `0.0.0.0:$PORT`, because Railway's
private network is IPv6-only and binding the IPv4 wildcard would leave the
service unreachable internally while still looking fine publicly.

## How state stays consistent

Redis is the source of truth; each replica keeps a local `atomic.Bool` cache of
it. The hot path never touches the network.

```
POST /on  →  replica 1
               ├─ SET toggle:taco 1       durable, for new/reconciling replicas
               └─ PUBLISH toggle:taco 1   broadcast
                        ↓ ~1ms
               replica 2, 3: atomic.Store(true)

GET /taco  →  atomic.Load()               ~17ns, no I/O
GET /health → GET toggle:taco             authoritative, so all replicas agree
```

Three mechanisms, in order of speed:

1. **Pub/sub** — normal propagation, about a millisecond.
2. **Reconcile poll** — every replica re-reads Redis every **250ms**. This is
   the backstop that bounds how long a replica can hold a wrong state after a
   missed broadcast. It matters more than it looks: a keep-alive HTTP client
   resolves `taco.railway.internal` once and **pins to a single replica**, so
   without this floor one stranded replica would stay broken indefinitely while
   `/health` cheerfully reported healthy. Worst case is now one Temporal retry
   cycle. Costs ~12 `GET`/s per service.
3. **Startup sync** — a booting replica seeds the key if absent (`SETNX`, so it
   never clobbers a deliberate state) and reads it before reporting ready.

### Behaviour when Redis is unavailable

| | Behaviour |
|---|---|
| Main endpoint | Keeps serving its last-known state. The cache is the whole point. |
| `/health` | `200` with `"stale":true`, reporting the local value. |
| `/ready` | `200` if it synced at least once; `503` if it never has. |
| `/on`, `/off` | `503`, local state unchanged. |
| Boot | Retries with backoff up to 2s intervals; serves nothing until `/ready`. |

A replica boots to **on**, but `/ready` returns `503` until its first Redis read
lands, so Railway routes no traffic to it before the real state is known. That
gate is what makes the boot default safe — it is never actually served.

Railway's private DNS can take a few seconds to resolve on a cold start, so the
first Redis dial failing is expected, not fatal.

## Performance

The hot path is one atomic load and one write of a pre-built byte slice.
Response bodies and header values are constructed at startup, so no request
marshals JSON or allocates. `http.ServeMux` is replaced with a switch on the
raw path — with five fixed routes that beats a map lookup and skips the mux's
redirect and cleanup logic.

```
BenchmarkEndpointOn-14     71101342    16.57 ns/op    0 B/op    0 allocs/op
BenchmarkEndpointOff-14   316478508     3.73 ns/op    0 B/op    0 allocs/op
```

The off path is the fast one, and deliberately so: it is the sustained-load path,
since callers retry hardest precisely while the service is broken.

Go needs no `async`/`await` here. `net/http` already serves every request on its
own goroutine, and a handler that performs no I/O is the fastest thing you can
write.

On `GOMAXPROCS`: Go 1.25+ sizes the scheduler from the container's cgroup CPU
quota rather than the host's core count, so no `automaxprocs` dependency is
needed. This is the container knob that actually affects throughput — the base
image does not.

## Local development

```bash
redis-server --port 6379 --save '' --daemonize yes

REDIS_URL=redis://localhost:6379 ENDPOINT=/taco PORT=8080 go run .
```

```bash
curl -s localhost:8080/taco             # {"taco":true}
curl -s -X POST localhost:8080/off      # {"on":false}
curl -si localhost:8080/taco | head -1  # HTTP/1.1 500 Internal Server Error
curl -s localhost:8080/health           # {"on":false}
```

Run several replicas against one Redis to watch propagation:

```bash
for p in 9101 9102 9103; do
  REDIS_URL=redis://localhost:6379 ENDPOINT=/taco PORT=$p go run . &
done

curl -s -X POST localhost:9101/off      # flip one
curl -s localhost:9103/health           # all three agree
```

## Tests

```bash
go test ./...                       # unit and integration
go test -race ./...                 # race detector
go test -run '^$' -bench . -benchmem # benchmarks
```

Tests use `miniredis`, so the pub/sub loop, the reconcile backstop, and the
Redis-outage paths are all exercised without a live Redis.

## Logging

Structured JSON on stdout. State transitions are logged; the main endpoint,
`/health`, and `/ready` log **nothing**. At 250ms retries across several
workflows, per-request logging would be thousands of lines per off-window, would
burn Railway's log quota, and would make `stdout` the slowest thing in the
request path.

The result is a readable timeline of the demo:

```
listening       endpoint=/taco json_key=taco state_key=toggle:taco
redis synced    key=toggle:taco on=true
state changed   on=false src=http
state changed   on=true  src=pubsub
shutting down   grace=10s
drained         elapsed=144µs
```

`src` tells you how a change arrived: `http` (this replica handled the request),
`pubsub` (broadcast from a sibling), `reconcile` (repaired by the poll), or
`initial` (startup sync). A replica receiving the broadcast of a change it made
itself logs nothing, since only genuine transitions are recorded.
