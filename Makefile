# dgraph2 — lightweight, local-only fork of upstream Dgraph.
#
# Targets:
#   make build  — compile pkg/dgraph2 + cmd/dgraph2-server
#   make test   — run unit + e2e tests for pkg/dgraph2 and cmd/dgraph2-server
#   make all    — build + test
#   make clean  — remove the built binary

GOFLAGS  ?=
# Version baked into the binary via -ldflags. Override with `make build VERSION=v1.2.3`
# or via the VERSION env var. Falls back to `git describe` if available, else "dev".
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(VERSION)
PKGS     := ./pkg/dgraph2/... ./cmd/dgraph2-server/... ./worker/... ./schema/... ./posting/... ./query/... ./dql/... ./types/... ./tok/... ./algo/... ./codec/... ./lex/... ./x/...

.PHONY: build test all clean vet

build:
	go build $(GOFLAGS) -ldflags="$(LDFLAGS)" ./cmd/dgraph2-server ./cmd/dgraph2-cli

test:
	go test $(GOFLAGS) -count=1 ./pkg/dgraph2/... ./pkg/graphql/... ./pkg/audit/... ./cmd/dgraph2-server/...

# go vet on the whole tree flags many pre-existing `copylocks` warnings in
# upstream code (the proto-generated types embed sync.Mutex via MessageState
# in google.golang.org/protobuf/runtime). Those are inherited from priorart
# and unrelated to the dgraph2 rewrite. The vet target below only checks
# packages dgraph2 wrote or substantially rewrote.
vet:
	go vet ./pkg/dgraph2/... ./cmd/dgraph2-server/... ./worker/...

all: vet build test

clean:
	rm -f dgraph2-server
