# Copyright 2026 Beacon Contributors
# SPDX-License-Identifier: AGPL-3.0-or-later

# Keep both stages immutable. Renovation is intentional: update the tag and its
# matching Docker Hub manifest digest together, then let CI rebuild the image.
ARG GO_IMAGE=docker.io/library/golang:1.26.5-alpine@sha256:0178a641fbb4858c5f1b48e34bdaabe0350a330a1b1149aabd498d0699ff5fb2
ARG RUNTIME_IMAGE=docker.io/library/alpine:3.23@sha256:fd791d74b68913cbb027c6546007b3f0d3bc45125f797758156952bc2d6daf40

FROM ${GO_IMAGE} AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

ARG VERSION=dev
ARG COMMIT_SHA=unknown
ARG BUILD_TIME=unknown
ARG BUILD_DIRTY=false

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -buildvcs=false \
    -ldflags="-s -w \
      -X main.version=${VERSION} \
      -X main.commitSHA=${COMMIT_SHA} \
      -X main.buildTime=${BUILD_TIME} \
      -X main.buildDirty=${BUILD_DIRTY}" \
    -o /out/beacon ./cmd/beacon

FROM ${RUNTIME_IMAGE} AS runtime

ARG VERSION=dev
ARG COMMIT_SHA=unknown
ARG BUILD_TIME=unknown

LABEL org.opencontainers.image.title="Beacon API" \
      org.opencontainers.image.description="MeshCore Beacon observation API" \
      org.opencontainers.image.source="https://github.com/n30nex/beacon-server" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT_SHA}" \
      org.opencontainers.image.created="${BUILD_TIME}"

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S -g 65532 beacon \
    && adduser -S -D -H -u 65532 -G beacon beacon \
    && command -v wget >/dev/null

COPY --from=builder --chown=65532:65532 /out/beacon /usr/local/bin/beacon

USER 65532:65532
WORKDIR /app
EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=4 \
  CMD wget -q -O /dev/null http://127.0.0.1:8080/healthz || exit 1

ENTRYPOINT ["/usr/local/bin/beacon"]
