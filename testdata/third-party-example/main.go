/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Smoke test that mimics a third-party consumer of pkg/bonsai. If this
 * file ever fails to compile, we've broken the v1 API contract declared
 * in pkg/bonsai/doc.go. Triggered by `make smoke-as-third-party`.
 */
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/bonsai/pkg/bonsai"
)

func main() {
	dir, err := os.MkdirTemp("", "bonsai-smoke-*")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	// Open + write + read — the absolute minimum a consumer cares about.
	db, err := bonsai.Open(bonsai.Options{Dir: dir})
	if err != nil {
		log.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	if err := db.Alter(ctx, "name: string @index(exact) ."); err != nil {
		log.Fatalf("Alter: %v", err)
	}
	if _, err := db.Mutate(ctx, &apiproto.Mutation{
		SetNquads: []byte(`_:a <name> "Alice" .`),
	}); err != nil {
		log.Fatalf("Mutate: %v", err)
	}
	resp, err := db.Query(ctx, `{ q(func: eq(name, "Alice")) { name } }`)
	if err != nil {
		log.Fatalf("Query: %v", err)
	}
	if string(resp.GetJson()) != `{"q":[{"name":"Alice"}]}` {
		log.Fatalf("unexpected query result: %s", resp.GetJson())
	}

	// Freeze + OpenFrozen — the build-once / query-many path.
	if err := db.Close(); err != nil {
		log.Fatalf("Close: %v", err)
	}
	artifact := dir + "/graph.bonsai"
	if err := bonsai.Freeze(dir, artifact); err != nil {
		log.Fatalf("Freeze: %v", err)
	}
	frozen, err := bonsai.OpenFrozen(artifact)
	if err != nil {
		log.Fatalf("OpenFrozen: %v", err)
	}
	defer frozen.Close()
	if !frozen.ReadOnly() {
		log.Fatalf("frozen DB should be ReadOnly")
	}

	// Confirm the same row reads back.
	resp, err = frozen.Query(ctx, `{ q(func: has(name)) { name } }`)
	if err != nil {
		log.Fatalf("Query (frozen): %v", err)
	}
	fmt.Println(string(resp.GetJson()))
}
