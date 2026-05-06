/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The bonsai binary. Dispatches on os.Args[1] to one of four sub-binaries
 * that used to live as separate cmd/bonsai-{server,cli,bulk,live} mains.
 *
 *   bonsai server                 run the HTTP + gRPC server
 *   bonsai bulk                   offline batch loader (opens DB directly)
 *   bonsai live                   streaming loader (talks to a server)
 *   bonsai version                print the bonsai version
 *   bonsai <cli-op>               operate against a running server
 *
 * Subcommand-specific flags follow the subcommand:
 *
 *   bonsai server --dir ./data --http :8080 --grpc :9080
 *   bonsai bulk --dir ./data --rdfs goldendata.rdf
 *   bonsai live --addr 127.0.0.1:9080 --rdfs data.rdf
 *
 * Each sub still parses its own flags (stdlib `flag` for cli/bulk/live,
 * pflag for server). The dispatcher resets both global FlagSets before
 * handing off so the sub gets a clean slate regardless of which library
 * it picked.
 */
package main

import (
	stdflag "flag"
	"fmt"
	"os"
	"strings"

	pflag "github.com/spf13/pflag"

	"github.com/qiangli/bonsai/cmd/bonsai/bulk"
	"github.com/qiangli/bonsai/cmd/bonsai/cli"
	"github.com/qiangli/bonsai/cmd/bonsai/live"
	"github.com/qiangli/bonsai/cmd/bonsai/server"
	"github.com/qiangli/bonsai/cmd/bonsai/tools"
)

// version is overridden at build time via -ldflags "-X main.version=...".
// It's pushed into server.Version for the gRPC CheckVersion RPC and the
// OTel resource attribute.
var version = "dev"

// cliSubs is the set of operations that route to the cli package. They
// preserve the subcommand as the first positional arg so cli.Main's
// existing dispatch (a switch over flag.Args()[0]) keeps working.
var cliSubs = map[string]bool{
	"alter":     true,
	"query":     true,
	"mutate":    true,
	"drop-all":  true,
	"drop-data": true,
	"backup":    true,
	"restore":   true,
	"export":    true,
	"import":    true,
	"download":  true,
}

func main() {
	if len(os.Args) < 2 {
		usage(os.Stderr)
		os.Exit(2)
	}

	// If the first token after the binary name starts with `-`, the user
	// is passing global flags before the subcommand — typical for the
	// client ops (`bonsai --addr X alter ...`). Route the whole tail to
	// cli, whose flag parsing handles addr/http/timeout before the
	// sub-subcommand, then walks `flag.Args()` for the operation.
	if strings.HasPrefix(os.Args[1], "-") {
		switch os.Args[1] {
		case "-h", "--help":
			usage(os.Stdout)
			return
		case "-v", "--version":
			fmt.Println("bonsai", version)
			return
		}
		runWithArgv("bonsai", os.Args[1:], cli.Main)
		return
	}

	sub := os.Args[1]
	rest := os.Args[2:]

	switch sub {
	case "help":
		usage(os.Stdout)
		return
	case "version":
		fmt.Println("bonsai", version)
		return
	}

	switch {
	case sub == "server":
		server.Version = version
		runWithArgv("bonsai server", rest, server.Main)
	case sub == "bulk":
		runWithArgv("bonsai bulk", rest, bulk.Main)
	case sub == "live":
		runWithArgv("bonsai live", rest, live.Main)
	case sub == "diff":
		// `bonsai diff <a.ntx> <b.ntx>` — local-only utility, no server.
		runWithArgv("bonsai diff", rest, tools.DiffMain)
	case sub == "freeze":
		// `bonsai freeze <data-dir> -o <out.bonsai>` — produce a
		// single-file read-only artifact. Local-only.
		runWithArgv("bonsai freeze", rest, tools.FreezeMain)
	case sub == "import-gfy":
		// `bonsai import-gfy <repo-or-graph.json>` — one-shot ingest of
		// a gfy code-knowledge graph. Local-only (opens DB directly).
		runWithArgv("bonsai import-gfy", rest, tools.ImportGfyMain)
	case sub == "explore":
		// `bonsai explore <path>` — link gfy in-process, build the
		// code-knowledge graph for <path>, ingest into a bonsai DB,
		// and start the Explorer server. End-to-end "open this repo
		// in Bonsai" in one command.
		runWithArgv("bonsai explore", rest, tools.ExploreMain)
	case cliSubs[sub]:
		// Re-include the subcommand so cli's own dispatch sees it as
		// flag.Args()[0].
		runWithArgv("bonsai", append([]string{sub}, rest...), cli.Main)
	default:
		fmt.Fprintf(os.Stderr, "bonsai: unknown subcommand %q\n\n", sub)
		usage(os.Stderr)
		os.Exit(2)
	}
}

// runWithArgv installs a clean argv and resets the global FlagSets used by
// the stdlib `flag` and `github.com/spf13/pflag` packages so the called
// Main() sees only the flags it expects.
func runWithArgv(progName string, args []string, fn func()) {
	os.Args = append([]string{progName}, args...)
	stdflag.CommandLine = stdflag.NewFlagSet(progName, stdflag.ExitOnError)
	pflag.CommandLine = pflag.NewFlagSet(progName, pflag.ExitOnError)
	fn()
}

func usage(w *os.File) {
	fmt.Fprintln(w, `bonsai — single-node, embeddable graph database.

usage: bonsai <subcommand> [flags]

server / loader subcommands:
  server                       run the HTTP + gRPC server
  bulk                         offline batch loader (opens the DB directly)
  live                         streaming loader (talks to a running server)

client subcommands (operate against a running server):
  alter <schema>               apply a DQL schema
  query <dql>                  run a DQL query (or read from stdin)
  mutate <rdf>                 apply RDF triples (SetNquads)
  drop-all                     wipe the database
  drop-data                    wipe data only (keep schema)
  backup <dst>                 single-file Badger-stream backup
  backup manifest <dir> [incremental]
                               upstream-compatible multi-file backup
  restore <src>                restore (file → stream, dir → manifest)
  export <fmt> <dst>           export to a server-local path (rdf|json|ntx)
  import <fmt> <file>          upload <file> contents and ingest
  download <fmt> [out]         stream the export back to <out> or stdout

local utilities:
  diff <a.ntx> <b.ntx>         line-diff two NTX exports (deterministic
                               canonical N-Quads). Exit 0 if equal, 1 if
                               not, 2 on read error.
  freeze <data-dir> -o <file>  compact + tarball a build-once data dir
                               into a single .bonsai artifact. Open it
                               read-only with bonsai server --frozen
                               or, in Go, bonsai.OpenFrozen(file).
  import-gfy <repo>            one-shot ingest of a gfy code-knowledge
                               graph (.gfy-out/graph.json). Detects the
                               format, ingests, enriches the inferred
                               schema with @reverse / @index where
                               predicate names match common conventions.
  explore <path>               build the code-knowledge graph for <path>
                               with gfy (linked in-process), ingest into
                               a bonsai data dir, and start the Explorer
                               UI. End-to-end "open this codebase in
                               Bonsai" in one command.

other:
  version                      print the bonsai version
  help                         show this help

Run 'bonsai <subcommand> --help' for subcommand-specific flags.`)
}
