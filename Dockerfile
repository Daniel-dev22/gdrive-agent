# syntax=docker/dockerfile:1.7
#
# Two-stage build:
#   1. golang:1.26 builds a static binary. CGO_ENABLED=0 is free here because
#      modernc.org/sqlite is pure Go — there is no libsqlite or libc to link.
#   2. alpine:3.21 ships the binary plus ca-certificates (HTTPS to
#      googleapis.com) and wget (the HEALTHCHECK below).
#
# The runtime stage runs as ROOT — deliberately, so a root-owned mode-0600
# bearer-token file can be bind-mounted in without being made world-readable
# on the host. That is the only reason; see README.md.
#
# Building this image needs a GitHub token with read access to the private
# agent-kit-go module, passed as a buildx secret:
#
#   docker buildx build --secret id=ghtoken,env=GITHUB_TOKEN -t gdrive-agent .

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

# Fetch the private agent-kit-go module using a GitHub token supplied as the
# `ghtoken` secret — an inline credential helper reads it from the tmpfs mount
# at fetch time, so the token value never lands in .gitconfig or in any layer
# (build caches get pushed to a registry). required=true so a build without the
# token fails loudly rather than silently attempting an anonymous fetch.
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

# Runtime stage — kept in step with duplicacy-agent-api. See the root note
# above for why this runs as root rather than a nonroot uid.
FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget
COPY --from=build /out/gdrive-agent /gdrive-agent
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD wget -q --spider http://localhost:8080/health/ready || exit 1
ENTRYPOINT ["/gdrive-agent"]
