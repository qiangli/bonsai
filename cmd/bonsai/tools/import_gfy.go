/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * `bonsai import-gfy <repo>` — one-command ingest of a gfy code-graph
 * (https://github.com/qiangli/gfy).
 *
 * Design constraint: must work for *any* repo's gfy output, not just the
 * ones with the predicate set we happen to know. So we let the data
 * inform the schema and only post-apply *enrichments* — turn every uid
 * edge into a @reverse edge, add @index(term) to label-like string
 * fields, add @index(exact) to id/path-like fields. Anything else stays
 * as the auto-detect import inferred.
 */

package tools

import (
	"context"
	stdflag "flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/qiangli/bonsai/pkg/bonsai"
)

// labelLike predicates get @index(exact, term) so callers can do
// `anyofterms(<pred>, "...")` and `eq(<pred>, "...")`.
var labelLike = map[string]bool{
	"label": true, "name": true, "title": true,
	"comment": true, "summary": true, "description": true,
}

// pathLike predicates get @index(exact) so callers can pivot by exact
// match (e.g. `eq(source_file, "foo.go")`).
var pathLike = map[string]bool{
	"source_file": true, "file": true, "path": true,
	"uri": true, "url": true,
	"file_type": true, "kind": true, "type": true,
	"language": true,
	"gid":       true, // bonsai's normalised id from the auto-detect path
}

// listLike predicates we expect to be `[string]` and want term-indexed.
var listLike = map[string]bool{
	"tags": true, "labels": true, "keywords": true,
	"log_messages": true, "throw_messages": true,
}

// ImportGfyMain is invoked from cmd/bonsai/main.go for `bonsai import-gfy`.
//
//	bonsai import-gfy <path> [-o <data-dir>] [-f]
//
// Discovers <path>/.gfy-out/graph.json (or <path> directly if it points
// at a graph.json), runs the auto-detect import, then enriches the
// generated schema with @reverse / @index where the inferred predicates
// match common naming conventions.
func ImportGfyMain() {
	fs := stdflag.NewFlagSet("bonsai import-gfy", stdflag.ExitOnError)
	out := fs.String("o", "", "data directory (default <path>/.bonsai-data)")
	force := fs.Bool("f", false, "overwrite an existing non-empty data directory")
	_ = fs.Parse(os.Args[1:])
	args := fs.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: bonsai import-gfy <path-to-repo-or-graph.json> [-o <data-dir>] [-f]")
		os.Exit(2)
	}
	target := args[0]

	// Resolve <target> into the actual graph.json path. Accept either
	// a repo root (look under .gfy-out/graph.json) or a direct file
	// path. Both are common in the wild.
	graphJSON, repoRoot, err := resolveGfyInput(target)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	dataDir := *out
	if dataDir == "" {
		dataDir = filepath.Join(repoRoot, ".bonsai-data")
	}
	if !*force {
		if entries, err := os.ReadDir(dataDir); err == nil && len(entries) > 0 {
			fmt.Fprintf(os.Stderr, "import-gfy: %s is not empty (use -f to overwrite)\n", dataDir)
			os.Exit(2)
		}
	} else {
		_ = os.RemoveAll(dataDir)
	}

	fmt.Printf("→ ingesting %s\n", graphJSON)
	t0 := time.Now()

	db, err := bonsai.Open(bonsai.Options{Dir: dataDir, CompactOnClose: true})
	if err != nil {
		fmt.Fprintf(os.Stderr, "import-gfy: open %s: %v\n", dataDir, err)
		os.Exit(1)
	}
	ctx := context.Background()

	f, err := os.Open(graphJSON)
	if err != nil {
		_ = db.Close()
		fmt.Fprintf(os.Stderr, "import-gfy: open %s: %v\n", graphJSON, err)
		os.Exit(1)
	}
	defer func() { _ = f.Close() }()

	// Phase 1: ingest. ImportStream auto-detects NetworkX/Cytoscape
	// shapes, applies a basic inferred schema, and bulk-loads the data.
	summary, err := bonsai.ImportStream(ctx, db, "json", f, 1000)
	if err != nil {
		_ = db.Close()
		fmt.Fprintf(os.Stderr, "import-gfy: ingest: %v\n", err)
		os.Exit(1)
	}

	// Phase 2: enrich. Read the schema bonsai inferred, decide which
	// predicates deserve indexes and reverses based on naming
	// conventions, and apply the deltas as a single Alter. Re-Altering
	// a predicate to add an index triggers IndexRebuild over the
	// already-loaded data; OK at typical code-graph scale.
	enrich, err := buildEnrichment(ctx, db)
	if err != nil {
		_ = db.Close()
		fmt.Fprintf(os.Stderr, "import-gfy: read schema: %v\n", err)
		os.Exit(1)
	}
	if enrich != "" {
		if err := db.Alter(ctx, enrich); err != nil {
			_ = db.Close()
			fmt.Fprintf(os.Stderr, "import-gfy: enrich schema: %v\n", err)
			os.Exit(1)
		}
	}

	if err := db.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "import-gfy: close: %v\n", err)
		os.Exit(1)
	}

	// Sanity-print a count by re-opening read-only.
	var nodeCount string
	if rdb, err := bonsai.Open(bonsai.Options{Dir: dataDir, ReadOnly: true}); err == nil {
		if r, err := rdb.Query(ctx, `{ q(func: has(gid)) { count(uid) } }`); err == nil {
			nodeCount = string(r.GetJson())
		}
		_ = rdb.Close()
	}

	fmt.Printf("→ done in %s\n", time.Since(t0).Round(time.Millisecond))
	if summary != nil {
		fmt.Printf("  detected:        %s\n", summary.Detected)
		fmt.Printf("  nquads ingested: %d\n", summary.Nquads)
		if summary.Errors > 0 {
			fmt.Printf("  batch errors:    %d\n", summary.Errors)
		}
	}
	if enrich != "" {
		fmt.Printf("  enrichments:     %d predicate(s) gained @reverse / @index\n",
			strings.Count(enrich, "\n"))
	}
	if nodeCount != "" {
		fmt.Printf("  nodes:           %s\n", nodeCount)
	}
	fmt.Println()
	fmt.Println("Next:")
	fmt.Printf("  bonsai server --dir %s\n", dataDir)
	fmt.Println("  open http://localhost:8080/   (Explorer UI)")
	fmt.Println()
	fmt.Println("Sample queries (DQL):")
	fmt.Println(`  { q(func: anyofterms(label, "ProcessQuery")) { gid label source_file } }`)
	fmt.Println(`  { q(func: eq(gid, "<id>")) { label callers: ~calls { label } } }`)
}

// resolveGfyInput accepts either a repo root or a direct .json path and
// returns (graphJSON, repoRoot).
func resolveGfyInput(target string) (string, string, error) {
	abs, err := filepath.Abs(target)
	if err != nil {
		return "", "", fmt.Errorf("import-gfy: %w", err)
	}
	fi, err := os.Stat(abs)
	if err != nil {
		return "", "", fmt.Errorf("import-gfy: %s: %w", abs, err)
	}
	if !fi.IsDir() {
		// Direct file path. Use its parent's parent as the repo root —
		// .gfy-out/graph.json → repo root is two levels up. If the
		// caller pointed at a graph.json elsewhere, the repo root just
		// becomes the file's parent dir.
		if filepath.Base(filepath.Dir(abs)) == ".gfy-out" {
			return abs, filepath.Dir(filepath.Dir(abs)), nil
		}
		return abs, filepath.Dir(abs), nil
	}
	// Repo root — look for .gfy-out/graph.json.
	candidate := filepath.Join(abs, ".gfy-out", "graph.json")
	if _, err := os.Stat(candidate); err != nil {
		return "", "", fmt.Errorf(
			"import-gfy: %s/.gfy-out/graph.json not found.\n"+
				"  Run gfy first: see https://github.com/qiangli/gfy", abs)
	}
	return candidate, abs, nil
}

// buildEnrichment reads the post-ingest schema text, parses out one line
// per declared predicate, and emits an Alter delta that adds @reverse to
// uid edges and @index() to predicates whose names match common
// conventions. Predicates already carrying the right index are left
// alone.
func buildEnrichment(ctx context.Context, db *bonsai.DB) (string, error) {
	current, err := db.SchemaText(ctx)
	if err != nil {
		return "", err
	}
	var deltas []string
	for _, line := range strings.Split(current, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "type ") {
			continue
		}
		// "<name>: <type-and-directives> ."
		colon := strings.Index(line, ":")
		if colon <= 0 {
			continue
		}
		name := strings.TrimSpace(line[:colon])
		body := strings.TrimSpace(strings.TrimSuffix(line[colon+1:], "."))
		if strings.HasPrefix(name, "dgraph.") {
			continue // reserved
		}
		switch {
		case strings.Contains(body, "[uid]"):
			if !strings.Contains(body, "@reverse") {
				deltas = append(deltas, fmt.Sprintf("%s: [uid] @reverse .", name))
			}
		case strings.HasPrefix(body, "[string]"):
			if listLike[name] && !strings.Contains(body, "@index") {
				deltas = append(deltas, fmt.Sprintf("%s: [string] @index(term) .", name))
			}
		case strings.HasPrefix(body, "string"):
			switch {
			case labelLike[name] && !strings.Contains(body, "@index"):
				deltas = append(deltas, fmt.Sprintf("%s: string @index(exact, term) .", name))
			case pathLike[name] && !strings.Contains(body, "@index"):
				deltas = append(deltas, fmt.Sprintf("%s: string @index(exact) .", name))
			}
		}
	}
	if len(deltas) == 0 {
		return "", nil
	}
	return strings.Join(deltas, "\n") + "\n", nil
}
