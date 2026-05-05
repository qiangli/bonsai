# Bonsai — lightweight, local-only fork of upstream Dgraph.
#
# Targets:
#   make build  — compile pkg/bonsai + cmd/bonsai-server
#   make test   — run unit + e2e tests for pkg/bonsai and cmd/bonsai-server
#   make all    — build + test
#   make clean  — remove the built binary

GOFLAGS  ?=
# Version baked into the binary via -ldflags. Override with `make build VERSION=v1.2.3`
# or via the VERSION env var. Falls back to `git describe` if available, else "dev".
VERSION  ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS  := -s -w -X main.version=$(VERSION)
PKGS     := ./pkg/bonsai/... ./cmd/bonsai-server/... ./worker/... ./schema/... ./posting/... ./query/... ./dql/... ./types/... ./tok/... ./algo/... ./codec/... ./lex/... ./x/...

.PHONY: build test all clean vet

build:
	go build $(GOFLAGS) -ldflags="$(LDFLAGS)" ./cmd/bonsai-server ./cmd/bonsai-cli

test:
	go test $(GOFLAGS) -count=1 ./pkg/bonsai/... ./pkg/graphql/... ./pkg/audit/... ./pkg/ui/... ./cmd/bonsai-server/...

# go vet on the whole tree flags many pre-existing `copylocks` warnings in
# upstream code (the proto-generated types embed sync.Mutex via MessageState
# in google.golang.org/protobuf/runtime). Those are inherited from priorart
# and unrelated to the bonsai rewrite. The vet target below only checks
# packages bonsai wrote or substantially rewrote.
vet:
	go vet ./pkg/bonsai/... ./cmd/bonsai-server/... ./worker/...

all: vet build test

clean:
	rm -f bonsai-server
