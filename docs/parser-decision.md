# Parser decision checkpoint

Status: user approved Loki parser adoption and AGPL-3.0-only on 2026-10-22.
The runtime parser boundary is under `internal/query`; the bounded adapter is
deployed privately on observability with separate reader/source identities.

## Confirmed target

Read-only authenticated Grafana API checks returned Grafana 13.2.4 and enabled
Logs Drilldown 3.0.0 and Loki frontend 13.2.2. The old experimental adapter was
retired on 2026-10-23 UTC after fixture, live API, browser and load gates passed.
Existing SQL/PromQL sources and ingestion remain unchanged.

## Current decision after security review

The application and isolated probe now pin Loki v3.7.9, release commit
`d3b4a2f76bfc5625abee7ceb00b8896fa3c31549`, module checksum
`h1:AI9auwgBKmh43/U3U1TUOZmlPgtxP7mgNCAXb5DSs8I=`. It requires Go 1.26.10,
which is the selected patched build/test toolchain. Go downloads that toolchain
without replacing the workstation's globally installed Go. The Docker builder
uses the same version and a pinned image digest.

The initial candidate below was rejected for deployment after govulncheck
reported standard-library and dependency advisories, including a reachable
text-normalization loop. The patched application scan exits successfully with
no called-symbol vulnerabilities. Its remaining module-only finding,
GO-2026-5932, concerns x/crypto/openpgp, which the adapter does not import.

Loki v3.7.9's actual LICENSE and LICENSING.md retain the same AGPL-3.0-only
parser licensing and Apache exceptions. Its dskit requires the exact
`github.com/grafana/memberlist` fork pinned in Loki's own module:
`v0.3.1-0.20251126142931-6f9f62ab6f86`, under MPL-2.0. Both local modules use
that explicit replacement because dependency replace directives are not
inherited. No unrelated upstream replacements were copied blindly.

The updated probe reports 895 packages, 241 standard-library packages and
162 external modules in its compiled test graph. A warm-cache test build took
1.55 seconds with peak child RSS 681,684 KiB. These are workstation build
measurements, not runtime memory or clean-cache benchmarks. Runtime limits and
load acceptance must still be measured independently.

## Initial candidate, superseded

- Module: `github.com/grafana/loki/v3`, version `v3.5.7`.
- Release commit: `d5b382b90738505d8bf7217ff3a1a8c90fc09b7e`.
- Module checksum: `h1:NLQgb+x78Q1lEccCJV+VMwlvdpWiEwFTib9udX4Nq9w=`.
- Parser import: `github.com/grafana/loki/v3/pkg/logql/syntax`.
- Upstream requires Go 1.24.0 and suggests toolchain Go 1.24.8. The evaluation
  uses installed Go 1.25.12 with `GOTOOLCHAIN=local`, not a downloaded toolchain.
- This is a fixed released candidate, not Loki main or a claim that this is the
  latest release. Security and dependency review remain release gates.

The isolated module under `tools/parser-probe` records the resolved graph and
executable syntax checks. The current pins are recorded above.

## Measured cost and behavior

The first `go test -count=1 -v ./...` after dependency resolution took 12.53
seconds and reported peak child RSS of 606,948 KiB using GNU time. This is a
workstation build measurement with a pre-existing Go cache, not a clean-cache
benchmark, aggregate parallel compiler memory, or adapter runtime memory.

`go list -deps -test -json ./...` reported 661 packages, including 230 standard
library packages, and 118 external modules in the compiled test dependency
graph. This is much larger than a standalone grammar package. It includes
Consul, etcd, Redis, and Prometheus packages. Importing the parser does not mean
deploying any of those services. Dependency modules' replace directives do
not automatically apply to our module; this probe uses the resolved graph
recorded in its own `go.mod` and `go.sum` without copying Loki's replacements.

The initial probe exposed a semantic-validation gap: `ParseExpr` accepts a
line filter with invalid regex `"["`. Loki rejects it when constructing the
pipeline. A dedicated test records both behaviors. Our adapter must validate
every regex itself before sending SQL, in addition to checking every AST node.
The probe must not be described as an adapter unsupported-syntax test suite.

## License finding

Inspected the downloaded release's root `LICENSE`, `README.md`, `LICENSING.md`,
and parser sources. Loki identifies its default as AGPL-3.0-only. Its Apache-2.0
exceptions include `pkg/logqlmodel`, `pkg/logproto`, and `pkg/util`, but not
`pkg/logql/syntax` or all the query packages it imports. The parser's lack of
a license header does not make it permissively licensed.

References:

- https://github.com/grafana/loki/blob/v3.5.7/LICENSING.md
- https://github.com/grafana/loki/blob/v3.5.7/LICENSE
- https://github.com/grafana/loki/tree/v3.5.7/pkg/logql/syntax

Do not treat a statically linked adapter as independently permissive merely
because it imports only the parser. The conservative integration choice is an
AGPL-3.0-only adapter, with corresponding source and build/install material
available to recipients and an appropriate network source offer to users.
Private use does not inherently require creating a public GitHub repository.
Retain applicable copyright/license notices and review the actual transitive
dependencies and any NOTICE requirements before distribution. This checkpoint
is not a completed legal or third-party notice audit.

The user approved AGPL-3.0-only and parser adoption after reviewing this choice.
Do not substitute another strategy silently or extract queries with regular
expressions.

## Remaining first-step gates

- Capture and sanitize requests across the required installed UI workflows.
- Freeze the endpoint/method/parameter and supported-query matrix.
- Extend the parser/compiler subset as required by the captured workflows.

The first implementation-plan checkbox remains unchecked until those gates pass.

## Work completed after approval

The application imports the pinned parser through `internal/query`, validates
every expression and pipeline AST node, and rejects unsupported operations.
JSON/logfmt, field predicates, error-label drop stages, compound line filters
and required sliding-window aggregates are now implemented. The shared
`internal/sqlgen` compiler produces parameterized SQL plus request-local,
bounded residual stages using Loki's actual implementations. It does not
approximate those parsers with ClickHouse functions of different semantics.
All endpoints use the same complete selection plan. See the compatibility
matrix for tested forms and deliberate limitations.

The official `clickhouse-go/v2` client is pinned to v2.40.3. It requires Go
1.24.0, works with the selected local toolchain, and is Apache-2.0 licensed.
The storage skeleton has bounded connections, query admission and deadlines,
and a real table SELECT for readiness. Local ClickHouse integration tests now
pass with the race detector after the user enabled host CPU passthrough on
linux-code. The exact pinned image and restricted reader are unchanged.

Testing exposed the native driver's automatic deadline-to-setting rewrite,
which fails under `readonly=1`. The driver-only context adapter preserves
cancellation while preventing that rewrite. Tests verify the server execution
limit stays at 15 seconds, the account stays read-only, an in-flight query is
interrupted at its application deadline, and readiness works afterward.
See the fixture README for the tested scope. Production configuration is
unchanged. Differential tests pass against the patched Loki 3.7.9 reference;
the restricted-database race suite and fixture/live browser gates also pass.
