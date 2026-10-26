FROM golang:1.26.10-alpine@sha256:27f2d3b83cfc5803c8078f8e730af0f7c431bf067afaec8f60a36fb763187cba AS build
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /work
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY testdata ./testdata
COPY tools ./tools
COPY docs ./docs
COPY README.md LICENSE THIRD_PARTY.md Dockerfile .dockerignore ./
COPY deploy ./deploy
RUN go mod vendor
# Vendor contains the actual third-party source and licenses needed for an
# offline rebuild. Include Loki's per-directory license exception declaration
# as well as the AGPL text; go vendor does not retain LICENSING.md by default.
RUN cp /go/pkg/mod/github.com/grafana/loki/v3@v3.7.9/LICENSING.md vendor/github.com/grafana/loki/v3/LICENSING.md && \
    cp vendor/github.com/grafana/loki/v3/LICENSE LICENSE && \
    cp /usr/local/go/LICENSE GO_LICENSE && \
    find vendor -type f \( -iname '*license*' -o -iname '*notice*' -o -iname '*copying*' -o -iname '*copyright*' \) | sort > LICENSE_INVENTORY.txt && \
    tar -czf /source.tar.gz go.mod go.sum cmd internal testdata tools docs deploy vendor README.md LICENSE THIRD_PARTY.md GO_LICENSE LICENSE_INVENTORY.txt Dockerfile .dockerignore
ARG VERSION=dev
RUN go build -mod=vendor -trimpath -buildvcs=false -ldflags="-s -w -X main.version=${VERSION}" -o /adapter ./cmd/adapter

FROM scratch
COPY --from=build /adapter /adapter
COPY --from=build /source.tar.gz /source.tar.gz
COPY --from=build /work/LICENSE /LICENSE
USER 65532:65532
ENV ADAPTER_LISTEN=:3100 SOURCE_PATH=/source.tar.gz
EXPOSE 3100
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/adapter", "healthcheck"]
ENTRYPOINT ["/adapter"]
