---
name: bonsai
description: Build/test/lint targets for bonsai as a bashy dag pipeline (agent-first; the Makefile is generated from this file and only forwards here)
type: dag
---

# bonsai — DAG task file

The Makefile in this directory is GENERATED from this file (`bashy dag --makefile`)
and only forwards to the targets below — do not edit it. This DAG file is the
agent-first task list, runnable with `bashy dag`:

```bash
bashy dag --list            # available targets
bashy dag build             # compile the single `bonsai` binary
bashy dag test              # run unit + e2e tests
bashy dag --json test       # machine-readable envelope for an agent
```

The version baked into the binary comes from the `VERSION` environment variable
(or a dag override, `bashy dag build VERSION=v1.2.3`), else `git describe`, else
`dev` — the old Makefile's rule. Extra go flags ride go's own `GOFLAGS`
environment variable, as they would for any `go` invocation.

## Tasks

### build
Compile the single `bonsai` binary (subcommands inside) into ./bonsai, with
the version ldflags.
Sources: cmd/, pkg/, go.mod, go.sum
Generates: bonsai
Effects: exec,net,write

```bash
set -e
version="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
go build -ldflags="-s -w -X main.version=$version" -o bonsai ./cmd/bonsai
```

### install
`go install` into $GOBIN (or $GOPATH/bin) with the same ldflags as build.
Effects: exec,net,write

```bash
set -e
version="${VERSION:-$(git describe --tags --always --dirty 2>/dev/null || echo dev)}"
install_dir="$(go env GOBIN)"
[ -n "$install_dir" ] || install_dir="$(go env GOPATH)/bin"
echo "→ installing bonsai $version into $install_dir"
go install -ldflags="-s -w -X main.version=$version" ./cmd/bonsai
echo "  done — run \`bonsai help\` to verify ($install_dir must be on \$PATH)"
```

### test
Run unit + e2e tests (the bonsai-authored package set the old Makefile ran).
Effects: exec,net,write

```bash
go test -count=1 ./pkg/bonsai/... ./pkg/bonsai/graphalgo/... ./pkg/graphql/... ./pkg/audit/... ./pkg/ui/... ./cmd/bonsai/server/... ./cmd/bonsai/tools/...
```

### vet
go vet over the packages bonsai wrote or substantially rewrote. The whole tree
is deliberately not vetted: it inherits pre-existing `copylocks` warnings from
upstream proto-generated types, unrelated to the bonsai rewrite.
Effects: exec,net,write

```bash
go vet ./pkg/bonsai/... ./cmd/bonsai/... ./worker/...
```

### all
Build + test + vet, as one gate.
Requires: vet, build, test

```bash
:
```

### smoke-as-third-party
Simulate an external Go program importing pkg/bonsai. If this stops compiling
the v1 API contract (see pkg/bonsai/doc.go) is broken. Expects the example to
print exactly `{"q":[{"name":"Alice"}]}`.
Effects: destroy,exec,net,write

```bash
set -e
echo "→ third-party smoke: build + run testdata/third-party-example"
go build -o /tmp/bonsai-smoke ./testdata/third-party-example
got=$(/tmp/bonsai-smoke)
want='{"q":[{"name":"Alice"}]}'
if [ "$got" = "$want" ]; then
  echo "  OK: $got"
else
  echo "  FAIL: got $got, want $want"; exit 1
fi
rm -f /tmp/bonsai-smoke
```

### clean
Remove the built binary.
Effects: destroy

```bash
rm -f bonsai
```

### fmtcheck
The gofmt gate (reports unformatted files, never rewrites them), plus a check
that the generated Makefile still matches this file — regenerate with
`bashy dag --makefile` instead of editing it.
Effects: exec,read

```bash
set -e
./scripts/fmtcheck.sh
bashy dag --makefile --check
```

### hooks
Install the pre-push formatting gate.
Effects: exec,write

```bash
git config core.hooksPath scripts/hooks
echo "hooks installed: core.hooksPath=scripts/hooks"
```
