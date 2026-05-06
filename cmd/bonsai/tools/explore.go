/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * `bonsai explore <path>` — one-command "open this codebase in Bonsai":
 * imports gfy as a Go library, runs source detection + AST extraction,
 * builds a code-knowledge graph, ingests it into a Bonsai DB, then starts
 * the HTTP+gRPC server so the user can query/visualize via the Explorer.
 *
 * No shelling out: gfy's pkg/{detect,extract,build,graph} are linked in.
 * The graph round-trips through NetworkX node-link JSON because that's
 * what bonsai.ImportStream already auto-detects — saves us reimplementing
 * the predicate inference here.
 */

package tools

import (
	"bytes"
	"context"
	stdflag "flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/qiangli/gfy/pkg/build"
	"github.com/qiangli/gfy/pkg/detect"
	"github.com/qiangli/gfy/pkg/extract"
	"github.com/qiangli/gfy/pkg/types"

	"github.com/qiangli/bonsai/cmd/bonsai/server"
	"github.com/qiangli/bonsai/pkg/bonsai"
)

// ExploreMain is invoked from cmd/bonsai/main.go for `bonsai explore`.
//
//	bonsai explore <path> [-o <data-dir>] [-http :8080] [-grpc :9080] [--no-serve]
//
// Steps: discover code files via gfy's detect, run extract.Extract +
// build.BuildFromResult, serialize to NetworkX JSON, ingest via
// bonsai.ImportStream, run schema enrichment, then start the server with
// the Explorer UI.
func ExploreMain() {
	fs := stdflag.NewFlagSet("bonsai explore", stdflag.ExitOnError)
	out := fs.String("o", "", "data directory (default <path>/.bonsai-data)")
	force := fs.Bool("f", false, "overwrite an existing non-empty data directory")
	httpAddr := fs.String("http", ":8080", "HTTP listen address for the Explorer UI")
	grpcAddr := fs.String("grpc", ":9080", "gRPC listen address")
	noServe := fs.Bool("no-serve", false, "ingest only; don't start the server afterward")
	// Stdlib flag stops parsing at the first non-flag, so `bonsai explore
	// <path> --no-serve` would silently drop --no-serve. Reorder argv to
	// put flag-tokens (and their values) first, positionals last.
	_ = fs.Parse(reorderFlags(os.Args[1:], map[string]bool{"o": true, "http": true, "grpc": true}))
	args := fs.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: bonsai explore [flags] <path>\n  -o <data-dir>   default <path>/.bonsai-data\n  -http :8080     HTTP/UI listen\n  -grpc :9080     gRPC listen\n  -f              overwrite existing data dir\n  --no-serve      ingest only")
		os.Exit(2)
	}

	abs, err := filepath.Abs(args[0])
	if err != nil {
		fmt.Fprintln(os.Stderr, "explore:", err)
		os.Exit(2)
	}
	fi, err := os.Stat(abs)
	if err != nil {
		fmt.Fprintln(os.Stderr, "explore:", err)
		os.Exit(2)
	}
	if !fi.IsDir() {
		fmt.Fprintf(os.Stderr, "explore: %s is not a directory (git-URL/archive support is TODO)\n", abs)
		os.Exit(2)
	}

	dataDir := *out
	if dataDir == "" {
		dataDir = filepath.Join(abs, ".bonsai-data")
	}
	if !*force {
		if entries, err := os.ReadDir(dataDir); err == nil && len(entries) > 0 {
			fmt.Fprintf(os.Stderr, "explore: %s is not empty (use -f to overwrite)\n", dataDir)
			os.Exit(2)
		}
	} else {
		_ = os.RemoveAll(dataDir)
	}

	t0 := time.Now()
	fmt.Printf("→ scanning %s\n", abs)

	// Step 1: detect files. Returns DetectionResult.Files keyed by FileType.
	detection := detect.Detect(abs, false)
	codeFiles := detection.Files[types.Code]
	fmt.Printf("  found %d files (%d code)\n", detection.TotalFiles, len(codeFiles))
	if len(codeFiles) == 0 {
		fmt.Fprintln(os.Stderr, "explore: no code files found — nothing to graph")
		os.Exit(1)
	}

	// Step 2: extract AST nodes/edges.
	fmt.Println("→ extracting AST")
	extraction := extract.Extract(codeFiles, abs)
	fmt.Printf("  extracted %d nodes, %d edges\n", len(extraction.Nodes), len(extraction.Edges))

	// Step 3: assemble graph + serialize to NetworkX node-link JSON.
	g := build.BuildFromResult(extraction, true)
	graphJSON, err := g.ToJSON()
	if err != nil {
		fmt.Fprintln(os.Stderr, "explore: serialize graph:", err)
		os.Exit(1)
	}

	// Step 4: open Bonsai DB and ingest.
	db, err := bonsai.Open(bonsai.Options{Dir: dataDir, CompactOnClose: true})
	if err != nil {
		fmt.Fprintln(os.Stderr, "explore: open db:", err)
		os.Exit(1)
	}
	ctx := context.Background()
	fmt.Println("→ ingesting graph into bonsai")
	summary, err := bonsai.ImportStream(ctx, db, "json", bytes.NewReader(graphJSON), 1000)
	if err != nil {
		_ = db.Close()
		fmt.Fprintln(os.Stderr, "explore: ingest:", err)
		os.Exit(1)
	}

	// Step 5: enrich schema with @reverse / @index where the inferred
	// predicate names match common conventions. Uses the same helper as
	// `bonsai import-gfy` so the two paths stay consistent.
	enrich, err := buildEnrichment(ctx, db)
	if err != nil {
		_ = db.Close()
		fmt.Fprintln(os.Stderr, "explore: read schema:", err)
		os.Exit(1)
	}
	if enrich != "" {
		if err := db.Alter(ctx, enrich); err != nil {
			_ = db.Close()
			fmt.Fprintln(os.Stderr, "explore: enrich schema:", err)
			os.Exit(1)
		}
	}

	if err := db.Close(); err != nil {
		fmt.Fprintln(os.Stderr, "explore: close:", err)
		os.Exit(1)
	}

	fmt.Printf("→ done in %s\n", time.Since(t0).Round(time.Millisecond))
	if summary != nil {
		fmt.Printf("  detected:        %s\n", summary.Detected)
		fmt.Printf("  nquads ingested: %d\n", summary.Nquads)
	}

	if *noServe {
		fmt.Println()
		fmt.Println("Next:")
		fmt.Printf("  bonsai server --dir %s\n", dataDir)
		return
	}

	url := "http://localhost" + *httpAddr
	fmt.Println()
	fmt.Printf("→ starting Bonsai Explorer at %s\n", url)
	fmt.Println("  press Ctrl-C to stop")
	go openBrowser(url)

	// Hand off to the server. Reset argv as the dispatcher does so server.Main
	// sees a clean flag set.
	os.Args = []string{"bonsai server",
		"--dir", dataDir,
		"--http", *httpAddr,
		"--grpc", *grpcAddr,
	}
	server.Main()
}

// openBrowser tries to open the given URL in the user's default browser.
// Best-effort; failure is silent. Sleeps briefly so the server has a chance
// to bind its listener before we trigger the browser.
func openBrowser(url string) {
	time.Sleep(800 * time.Millisecond)
	var cmd *exec.Cmd
	switch {
	case fileExists("/usr/bin/open"):
		cmd = exec.Command("open", url) // macOS
	case fileExists("/usr/bin/xdg-open"):
		cmd = exec.Command("xdg-open", url) // most Linux
	default:
		return
	}
	_ = cmd.Start()
}

func fileExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// reorderFlags moves flag-tokens (and the values of flags listed in
// valueFlags) ahead of positional args. Stdlib `flag.Parse` stops at the
// first non-flag, so without reordering `cmd <pos> -flag` ignores -flag.
func reorderFlags(in []string, valueFlags map[string]bool) []string {
	var flags, positionals []string
	for i := 0; i < len(in); i++ {
		a := in[i]
		if !strings.HasPrefix(a, "-") || a == "-" {
			positionals = append(positionals, a)
			continue
		}
		flags = append(flags, a)
		// `-name=val` already carries its value.
		if strings.Contains(a, "=") {
			continue
		}
		// `-name val` for flags that take a value: consume the next token.
		name := strings.TrimLeft(a, "-")
		if valueFlags[name] && i+1 < len(in) {
			i++
			flags = append(flags, in[i])
		}
	}
	return append(flags, positionals...)
}
