# syntax=docker/dockerfile:1.7
#
# Two-stage build:
#   1. golang:1.25-bookworm builds the static binary (CGO_ENABLED=0 because
#      modernc.org/sqlite is pure-Go — no libsqlite/libc dependency).
#   2. gcr.io/distroless/static-debian12 ships only the binary + ca-certs.
#
# Final image is ~25MB.

FROM golang:1.25 AS build
WORKDIR /src

# Pre-download deps so the layer caches across source-only changes.
COPY go.mod go.sum* ./
RUN go mod download

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

# Distroless static is the smallest base that still ships ca-certificates
# (needed for HTTPS to googleapis.com). Runs as nonroot:nonroot by tag.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gdrive-agent /gdrive-agent
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/gdrive-agent"]
