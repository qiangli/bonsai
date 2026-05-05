/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * bonsai-cli — minimal command-line client.
 *
 * Subcommands using the gRPC api.DgraphServer:
 *   alter <schemaText>          — apply schema
 *   query <dql>                 — run a DQL query, print JSON
 *   mutate <rdf>                — set N-Quads via SetNquads
 *   drop-all                    — wipe the database
 *   drop-data                   — wipe data, keep schema
 *
 * Subcommands using the HTTP /admin/* surface (admin ops are HTTP-only):
 *   backup <dst>                — POST /admin/backup?path=<dst>
 *   restore <src>               — POST /admin/restore?path=<src>
 *   export <fmt> <dst>          — POST /admin/export?format=<rdf|json>&path=<dst>
 *
 * The client is stateless and reconnects per invocation.
 */
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9080", "gRPC server address")
	httpAddr := flag.String("http", "http://127.0.0.1:8080", "HTTP base URL (used for backup/restore/export)")
	timeout := flag.Duration("timeout", 30*time.Second, "request timeout")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		usage()
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	switch args[0] {
	case "backup", "restore", "export", "import", "download":
		runHTTP(ctx, *httpAddr, args)
	default:
		runGRPC(ctx, *addr, args)
	}
}

func runGRPC(ctx context.Context, addr string, args []string) {
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", addr, err)
	}
	defer func() { _ = conn.Close() }()

	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))

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

func runHTTP(ctx context.Context, base string, args []string) {
	switch args[0] {
	case "import":
		// import <format> <file>
		if len(args) < 3 {
			log.Fatalf("import: format (rdf|json) and file required")
		}
		f, err := os.Open(args[2])
		if err != nil {
			log.Fatalf("open %s: %v", args[2], err)
		}
		defer func() { _ = f.Close() }()
		postAdminBody(ctx, base, "/admin/import",
			url.Values{"format": {args[1]}}, f, "application/octet-stream")

	case "download":
		// download <format> [out-file]   stream /admin/export?download=true
		if len(args) < 2 {
			log.Fatalf("download: format (rdf|json) required")
		}
		var out *os.File = os.Stdout
		if len(args) >= 3 {
			f, err := os.Create(args[2])
			if err != nil {
				log.Fatalf("create %s: %v", args[2], err)
			}
			defer func() { _ = f.Close() }()
			out = f
		}
		getAdminToWriter(ctx, base, "/admin/export",
			url.Values{"format": {args[1]}, "download": {"true"}}, out)

	case "backup":
		// Forms:
		//   backup <dst>                       single-file stream
		//   backup manifest <dir>              upstream multi-file format (full)
		//   backup manifest <dir> incremental  incremental on top of prior chain
		if len(args) < 2 {
			log.Fatalf("backup: destination path required")
		}
		q := url.Values{}
		switch args[1] {
		case "manifest":
			if len(args) < 3 {
				log.Fatalf("backup manifest: destination directory required")
			}
			q.Set("path", args[2])
			q.Set("format", "manifest")
			if len(args) >= 4 {
				q.Set("type", args[3])
			}
		default:
			q.Set("path", args[1])
		}
		postAdmin(ctx, base, "/admin/backup", q)

	case "restore":
		if len(args) < 2 {
			log.Fatalf("restore: source path required")
		}
		postAdmin(ctx, base, "/admin/restore", url.Values{"path": {args[1]}})

	case "export":
		if len(args) < 3 {
			log.Fatalf("export: format and destination path required (e.g. export rdf /tmp/out.rdf)")
		}
		postAdmin(ctx, base, "/admin/export", url.Values{
			"format": {args[1]},
			"path":   {args[2]},
		})
	}
}

func postAdmin(ctx context.Context, base, path string, q url.Values) {
	postAdminBody(ctx, base, path, q, nil, "")
}

// postAdminBody is postAdmin with an optional request body, for endpoints
// that accept a file upload (e.g. /admin/import).
func postAdminBody(ctx context.Context, base, path string, q url.Values, body io.Reader, contentType string) {
	endpoint := base + path + "?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, body)
	if err != nil {
		log.Fatalf("build request: %v", err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("%s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	rb, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 400 {
		log.Fatalf("%s: %s: %s", path, resp.Status, string(rb))
	}
	_, _ = os.Stdout.Write(rb)
	if len(rb) > 0 && rb[len(rb)-1] != '\n' {
		fmt.Println()
	}
}

// getAdminToWriter pulls a streaming admin endpoint into out (typically a
// file or stdout). Used for /admin/export?download=true.
func getAdminToWriter(ctx context.Context, base, path string, q url.Values, out io.Writer) {
	endpoint := base + path + "?" + q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		log.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("%s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		log.Fatalf("%s: %s: %s", path, resp.Status, string(body))
	}
	if _, err := io.Copy(out, resp.Body); err != nil {
		log.Fatalf("%s: copy: %v", path, err)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: bonsai-cli [--addr 127.0.0.1:9080] [--http http://127.0.0.1:8080] <command> [args]

gRPC commands:
  alter <schema>           apply a DQL schema
  query <dql>              run a DQL query (or read from stdin)
  mutate <rdf>             apply RDF triples (SetNquads, or read from stdin)
  drop-all                 wipe the database
  drop-data                wipe data only (keep schema)

HTTP /admin commands:
  backup <dst>             write a single-file Badger-stream backup
  backup manifest <dir>    write an upstream-compatible multi-file backup
  backup manifest <dir> incremental
                           write an incremental backup on top of prior chain
  restore <src>            restore from <src>; if <src> is a directory it is
                           treated as a multi-file manifest backup, otherwise
                           as a Badger-stream backup
  export <fmt> <dst>       export to a server-local path (rdf|json)
  import <fmt> <file>      upload <file> contents and ingest as RDF or JSON
  download <fmt> [out]     stream the export back to <out> (default stdout)`)
	os.Exit(2)
}
