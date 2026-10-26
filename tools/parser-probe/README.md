# Parser dependency evaluation

This isolated module measures Loki's parser dependency separately from the
adapter implementation. It pins Loki v3.7.9, as does the application. The user
approved an AGPL-3.0-only adapter. The initial v3.5.7 candidate was superseded
after vulnerability scanning; it must not be used for deployment.

Run with the pinned patched Go toolchain, downloaded by Go if not cached:

```sh
cd tools/parser-probe
GOTOOLCHAIN=go1.26.10 go test -count=1 -v ./...
GOTOOLCHAIN=go1.26.10 go list -deps -test ./...
```

The tests cover synthetic query forms from the implementation plan and trial
checks. They are not a new capture of every installed Drilldown workflow.
They check parser acceptance and malformed-input rejection only. They do not
prove SQL semantics, supported-stage validation, or runtime compatibility.
In particular, Loki accepts operations that our adapter must reject until
implemented. The last test makes that distinction explicit.

The dependency is under AGPL-3.0-only. Loki's `LICENSING.md` does not include
`pkg/logql/syntax` in its Apache-2.0 exceptions. See
`../../docs/parser-decision.md` before adopting it in application code.
