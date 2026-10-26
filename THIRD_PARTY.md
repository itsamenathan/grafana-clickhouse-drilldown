# Third-party dependencies

The application uses Grafana Loki's LogQL parser from
`github.com/grafana/loki/v3` v3.7.9, under AGPL-3.0-only. Upstream source,
license text, and exceptions:

- https://github.com/grafana/loki/tree/v3.7.9
- https://github.com/grafana/loki/blob/v3.7.9/LICENSE
- https://github.com/grafana/loki/blob/v3.7.9/LICENSING.md

The database client is `github.com/ClickHouse/clickhouse-go/v2` v2.40.3,
under Apache-2.0. Its `LICENSE` accompanies packaging. Source headers refer to
a NOTICE, but no separate NOTICE file is present in the pinned module release:

- https://github.com/ClickHouse/clickhouse-go/blob/v2.40.3/LICENSE

The user approved AGPL-3.0-only for this adapter on 2026-10-22. The Docker build
vendors the actual imported dependency source, preserves its legal notices,
adds Loki's licensing-exception declaration and the Go standard-library
license, and packages all of it with application source and build instructions
in `/source.tar.gz`. The image's root LICENSE contains the full AGPL text.

The source offer is available at `/source`, advertised by a Link response
header and build information with the archive's SHA-256. Deployment sets its
publicly usable URL through Grafana's authenticated data-source proxy; this
does not expose a new public adapter port or create a hosted Git repository.

The patched bundled-source audit found 201 license/notice-related files. The
recognized license families were AGPL-3.0, Apache-2.0, MIT, BSD, MPL-2.0 and
ISC. HashiCorp's imported API/client packages retain their MPL notices and
source; no BUSL license appeared in this bundled graph. `go-spew`'s ISC text
was reviewed separately from the automated license-family scan. The security
upgrade also pins Grafana's required memberlist fork under MPL-2.0. Its actual
license and source, the Go license, and Loki's exceptions are bundled. The
source download's SHA-256 matched build information and unimported OpenPGP
packages were absent. This records an engineering
dependency review, not legal advice or a security audit.

After changing dependencies, rebuild and inspect the source archive and
`LICENSE_INVENTORY.txt` again. Do not remove the source offer or notices from
a distributed or network-served build.
