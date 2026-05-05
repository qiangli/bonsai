/*
 * SPDX-FileCopyrightText: dgraph2 contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * dgraph2-cli — minimal command-line client wrapping the gRPC api.DgraphServer.
 *
 * Subcommands:
 *   alter <schemaText>          — apply schema
 *   query <dql>                 — run a DQL query, print JSON
 *   mutate <rdf>                — set N-Quads via SetNquads
 *   drop-all                    — wipe the database
 *   drop-data                   — wipe data, keep schema
 *
 * The client is stateless and reconnects per invocation; intended for
 * scripts and ad-hoc poking, not high-throughput workloads (use dgo for that).
 */
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9080", "gRPC server address")
	timeout := flag.Duration("timeout", 30*time.Second, "request timeout")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer func() { _ = conn.Close() }()

	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch args[0] {
	case "alter":
		if len(args) < 2 {
			log.Fatalf("alter: schema text required")
		}
		if err := dg.Alter(ctx, &api.Operation{Schema: args[1]}); err != nil {
			log.Fatalf("alter: %v", err)
		}
		fmt.Println("ok")

	case "query":
		body := args[1:]
		if len(body) == 0 {
			b, _ := io.ReadAll(os.Stdin)
			body = []string{string(b)}
		}
		txn := dg.NewReadOnlyTxn()
		resp, err := txn.Query(ctx, body[0])
		if err != nil {
			log.Fatalf("query: %v", err)
		}
		_, _ = os.Stdout.Write(resp.Json)
		fmt.Println()

	case "mutate":
		body := args[1:]
		if len(body) == 0 {
			b, _ := io.ReadAll(os.Stdin)
			body = []string{string(b)}
		}
		txn := dg.NewTxn()
		resp, err := txn.Mutate(ctx, &api.Mutation{SetNquads: []byte(body[0])})
		if err != nil {
			log.Fatalf("mutate: %v", err)
		}
		if err := txn.Commit(ctx); err != nil {
			log.Fatalf("commit: %v", err)
		}
		fmt.Printf("uids: %v\n", resp.Uids)

	case "drop-all":
		if err := dg.Alter(ctx, &api.Operation{DropAll: true}); err != nil {
			log.Fatalf("drop-all: %v", err)
		}
		fmt.Println("ok")

	case "drop-data":
		if err := dg.Alter(ctx, &api.Operation{DropOp: api.Operation_DATA}); err != nil {
			log.Fatalf("drop-data: %v", err)
		}
		fmt.Println("ok")

	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: dgraph2-cli [--addr 127.0.0.1:9080] <command> [args]

commands:
  alter <schema>      apply a DQL schema
  query <dql>         run a DQL query (or read from stdin)
  mutate <rdf>        apply RDF triples (SetNquads, or read from stdin)
  drop-all            wipe the database
  drop-data           wipe data only (keep schema)`)
	os.Exit(2)
}
