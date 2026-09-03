# syntax=docker/dockerfile:1

# ---- build ----
FROM golang:1.27-alpine AS build

WORKDIR /src

# Copy the module files first so dependency download is cached independently
# of source changes.
COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./

# CGO_ENABLED=0 produces a fully static binary, which is what allows the
# final stage to be scratch. -s -w strips the symbol table and DWARF data;
# -trimpath removes local filesystem paths from the binary.
RUN CGO_ENABLED=0 GOOS=linux go build \
      -ldflags="-s -w" \
      -trimpath \
      -o /svc .

# ---- run ----
# scratch keeps the image around 6MB, so Railway pulls and boots it in about
# a second. No ca-certificates are needed: Redis and all callers are reached
# over Railway's private network, so nothing here speaks TLS.
FROM scratch

COPY --from=build /svc /svc

# Documentation only. Railway injects PORT at runtime and the service binds
# whatever it is given.
EXPOSE 8080

ENTRYPOINT ["/svc"]
