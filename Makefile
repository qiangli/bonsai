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

clean:
	rm -f bonsai
