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

# Bypass the public proxy for our private agent-kit-go module so go mod
# download hits GitHub via git+SSH. The build invocation must pass
# `--ssh default` so the host's SSH agent is mounted only during the
# `RUN --mount=type=ssh` below — the key never lands in the image.
# Ansible playbook handles this.
RUN apt-get update && apt-get install -y --no-install-recommends openssh-client && rm -rf /var/lib/apt/lists/*
ENV GOPRIVATE=github.com/Daniel-dev22/*
RUN git config --global url."git@github.com:".insteadOf "https://github.com/" && \
    mkdir -p /root/.ssh && \
    ssh-keyscan -t rsa,ecdsa,ed25519 github.com >> /root/.ssh/known_hosts

# Pre-download deps so the layer caches across source-only changes.
COPY go.mod go.sum* ./
RUN --mount=type=ssh go mod download

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
