# dgraph2 — lightweight, local-only fork of upstream Dgraph.
#
# Targets:
#   make build  — compile pkg/dgraph2 + cmd/dgraph2-server
#   make test   — run unit + e2e tests for pkg/dgraph2 and cmd/dgraph2-server
#   make all    — build + test
#   make clean  — remove the built binary

GOFLAGS ?=
PKGS    := ./pkg/dgraph2/... ./cmd/dgraph2-server/... ./worker/... ./schema/... ./posting/... ./query/... ./dql/... ./types/... ./tok/... ./algo/... ./codec/... ./lex/... ./x/...

.PHONY: build test all clean vet

build:
	go build $(GOFLAGS) ./cmd/dgraph2-server

test:
	go test $(GOFLAGS) -count=1 ./pkg/dgraph2/... ./cmd/dgraph2-server/...

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
