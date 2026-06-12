# syntax=docker/dockerfile:1.7
#
# Two-stage build:
#   1. golang:1.25-bookworm builds the static binary (CGO_ENABLED=0 because
#      modernc.org/sqlite is pure-Go — no libsqlite/libc dependency).
#   2. gcr.io/distroless/static-debian12 ships only the binary + ca-certs.
#
# Final image is ~25MB.

FROM golang:1.26 AS build
# Target Intel Haswell+ / AMD Excavator+ for FMA/AVX2 wins. Override
# with --build-arg GOAMD64=v1 for older CPUs. No-op for arm64 builds.
ARG GOAMD64=v3
ENV GOAMD64=${GOAMD64}
WORKDIR /src

# Bypass the public proxy for our private agent-kit-go module so go mod download
# hits GitHub via git directly (the credential helper supplies the token over
# HTTPS). git ships in the golang image — no extra packages needed.
ENV GOPRIVATE=github.com/Daniel-dev22/*

# Fetch the private agent-kit-go module using the GitHub App token the build-agent
# provides as the `ghtoken` secret — a credential helper reads it from the tmpfs
# mount at fetch time so the token value never lands in .gitconfig or any layer
# (the buildcache is pushed to the registry). required=true so a build without
# the token fails loudly rather than silently attempting an anonymous fetch.
COPY go.mod go.sum* ./
RUN --mount=type=secret,id=ghtoken,required=true \
    git config --global credential.helper '!f() { echo username=x-access-token; echo "password=$(cat /run/secrets/ghtoken)"; }; f' && \
    go mod download

# Source.
COPY cmd/ ./cmd/
COPY internal/ ./internal/

# Build static binary. Trimpath strips absolute paths from stack traces and
# embedded metadata so the binary is reproducible across build hosts.
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux \
    go build \
      -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/gdrive-agent \
      ./cmd/gdrive-agent

# alpine matches duplicacy-agent-api: adds ca-certificates (HTTPS to
# googleapis.com) + wget for the docker-compose healthcheck. Runs as root
# so the bind-mounted mode-0600 root-owned bearer-token file is readable
# without chmod'ing it world-readable on the host.
FROM alpine:3.19
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/gdrive-agent /gdrive-agent
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget -q --spider http://localhost:8080/health/ready || exit 1
ENTRYPOINT ["/gdrive-agent"]
