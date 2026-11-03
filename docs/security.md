# Dependency and runtime security checks

The first Go 1.25.12 / Loki v3.5.7 candidate was not approved for deployment.
govulncheck reported 28 called-symbol advisories, including standard HTTP/TLS
issues and a text-normalization loop reachable from label processing. Some
module-wide findings concerned Loki/Prometheus server endpoints that this
adapter never serves, but that is not a reason to retain avoidable old pins.

The application now uses Go 1.26.10 and Loki v3.7.9 with its updated dependency
graph. The pinned dskit memberlist fork is explicitly recorded in go.mod and
its MPL-2.0 license/source retained in the source bundle. No Loki main branch
or unpinned toolchain is used.

```sh
GOTOOLCHAIN=go1.26.10 go run golang.org/x/vuln/cmd/govulncheck@v1.1.4 ./...
```

The source scan last ran on 2026-10-26, after adding Loki's Drain package and
gorilla/websocket. It exited successfully with no called-symbol findings. The
remaining module-only GO-2026-5932 concerns the unmaintained OpenPGP packages
within x/crypto; none of those packages is imported by this binary.
This is not a guarantee of security. Repeat scanning when dependencies or
toolchains change, and verify the produced binary/source archive before
deployment.

The adapter serves no ingestion, Prometheus remote-read/write, Loki ruler,
configuration mutation, arbitrary SQL or multi-tenant routing API.
Unsupported paths/queries return errors. Access is through authenticated
Grafana on a private Docker network, with no published production adapter port.

The read-only tail websocket accepts handshakes without an `Origin` header
(server clients such as Grafana's backend), from the request's own host, or
from origins listed in `ADAPTER_ALLOWED_ORIGINS`. Production lists only
Grafana's public origin, which Explore uses through Grafana's authenticated
data-source proxy. Other browser origins are refused before upgrading.
The separate ClickHouse user is SELECT-only on the logs table, `readonly=1`,
and subject to server-enforced query/memory/read limits. Containers run
non-root with a read-only filesystem and dropped capabilities.

Query values, SQL, production log bodies, tokens and environment dumps are not
written to application logs or source artifacts. Runtime credentials stay in
the host's owner-only environment file and are sent only to ClickHouse.
