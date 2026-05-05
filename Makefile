# Bonsai — lightweight, local-only fork of upstream Dgraph.
#
# Targets:
#   make build  — compile the single `bonsai` binary (subcommands inside)
#   make test   — run unit + e2e tests
#   make all    — build + test + vet
#   make clean  — remove the built binary

GOFLAGS  ?=
# Version baked into the binary via -ldflags. Override with `make build VERSION=v1.2.3`
# or via the VERSION env var. Falls back to `git describe` if available, else "dev".
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(VERSION)

.PHONY: build test all clean vet

build:
	go build $(GOFLAGS) -ldflags="$(LDFLAGS)" -o bonsai ./cmd/bonsai

test:
	go test $(GOFLAGS) -count=1 ./pkg/bonsai/... ./pkg/graphql/... ./pkg/audit/... ./pkg/ui/... ./cmd/bonsai/server/...

# go vet on the whole tree flags many pre-existing `copylocks` warnings in
# upstream code (the proto-generated types embed sync.Mutex via MessageState
# in google.golang.org/protobuf/runtime). Those are inherited from priorart
# and unrelated to the bonsai rewrite. The vet target below only checks
# packages bonsai wrote or substantially rewrote.
vet:
	go vet ./pkg/bonsai/... ./cmd/bonsai/... ./worker/...

all: vet build test

# smoke-as-third-party simulates an external Go program importing
# pkg/bonsai. If this stops compiling we've broken our v1 API contract
# (see pkg/bonsai/doc.go). Run from a clean state, expects the example
# to print exactly the line `{"q":[{"name":"Alice"}]}`.
.PHONY: smoke-as-third-party
smoke-as-third-party:
	@echo "→ third-party smoke: build + run testdata/third-party-example"
	@go build -o /tmp/bonsai-smoke ./testdata/third-party-example
	@got=$$(/tmp/bonsai-smoke); \
	  want='{"q":[{"name":"Alice"}]}'; \
	  if [ "$$got" = "$$want" ]; then \
	    echo "  OK: $$got"; \
	  else \
	    echo "  FAIL: got $$got, want $$want"; exit 1; \
	  fi
	@rm -f /tmp/bonsai-smoke

clean:
	rm -f bonsai
