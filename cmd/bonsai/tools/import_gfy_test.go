/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/qiangli/bonsai/pkg/bonsai"
)

// TestBuildEnrichment_Conventions exercises the schema-enrichment logic
// in isolation: given a schema text mimicking what auto-detect produces
// (no indexes, no @reverse), the enrichment should add @reverse to uid
// edges, @index(term) to list-typed predicates that match labelLike /
// listLike conventions, and @index(exact) to id/path-like predicates.
func TestBuildEnrichment_Conventions(t *testing.T) {
	src := newSeededDB(t, `
		gid:         string .
		label:       string .
		source_file: string .
		source_line: string .
		random_pred: string .
		tags:        [string] .
		extra_list:  [string] .
		calls:       [uid] .
		references:  [uid] @reverse .
		random_uid:  [uid] .
	`)
	defer src.Close()

	got, err := buildEnrichment(context.Background(), src)
	if err != nil {
		t.Fatalf("buildEnrichment: %v", err)
	}

	// Expected enrichments:
	// - gid:         string → @index(exact)            (pathLike)
	// - label:       string → @index(exact, term)      (labelLike)
	// - source_file: string → @index(exact)            (pathLike)
	// - source_line: untouched (not in any list)
	// - random_pred: untouched
	// - tags:        [string] → @index(term)           (listLike)
	// - extra_list:  untouched (unknown name)
	// - calls:       [uid] → @reverse                  (every uid edge)
	// - references:  untouched (already @reverse)
	// - random_uid:  [uid] → @reverse                  (every uid edge)
	for _, want := range []string{
		"gid: string @index(exact) .",
		"label: string @index(exact, term) .",
		"source_file: string @index(exact) .",
		"tags: [string] @index(term) .",
		"calls: [uid] @reverse .",
		"random_uid: [uid] @reverse .",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("enrichment missing %q:\n%s", want, got)
		}
	}
	for _, doNotWant := range []string{
		"source_line:", "random_pred:", "extra_list:", "references:",
	} {
		if strings.Contains(got, doNotWant) {
			t.Errorf("enrichment unexpectedly touched %q:\n%s", doNotWant, got)
		}
	}
}

// TestImportGfy_SmallSynthetic runs the full import-gfy flow against a
// hand-crafted 6-node NetworkX-shape JSON. Exercises everything except
// the post-enrichment IndexRebuild on a large dataset (which we leave
// to the manual `bonsai import-gfy .` smoke against the repo's own
// .gfy-out fixture — too heavy for a unit-test budget and the
// IndexRebuild has its own coverage in TestAlterRebuildsIndex).
func TestImportGfy_SmallSynthetic(t *testing.T) {
	tmp := t.TempDir()
	repoDir := filepath.Join(tmp, "repo")
	gfyDir := filepath.Join(repoDir, ".gfy-out")
	if err := os.MkdirAll(gfyDir, 0o755); err != nil {
		t.Fatal(err)
	}
	graphJSON := filepath.Join(gfyDir, "graph.json")
	if err := os.WriteFile(graphJSON, []byte(`{
	  "directed": true,
	  "multigraph": false,
	  "graph": {},
	  "nodes": [
	    {"id":"a","label":"funcA","source_file":"a.go","tags":["exported"]},
	    {"id":"b","label":"funcB","source_file":"b.go"},
	    {"id":"c","label":"funcC","source_file":"c.go"}
	  ],
	  "links": [
	    {"source":"a","target":"b","relation":"calls"},
	    {"source":"a","target":"c","relation":"calls"},
	    {"source":"b","target":"c","relation":"calls"}
	  ]
	}`), 0o644); err != nil {
		t.Fatal(err)
	}

	dataDir := filepath.Join(tmp, "data")
	db, err := bonsai.Open(bonsai.Options{Dir: dataDir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	f, err := os.Open(graphJSON)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	if _, err := bonsai.ImportStream(ctx, db, "json", f, 1000); err != nil {
		t.Fatalf("ImportStream: %v", err)
	}
	enrich, err := buildEnrichment(ctx, db)
	if err != nil {
		t.Fatalf("buildEnrichment: %v", err)
	}
	// `calls` already gained @reverse from the auto-detect pass, so it's
	// NOT in the enrichment delta. The enrichment adds the index
	// directives auto-detect doesn't infer.
	if !strings.Contains(enrich, "label: string @index(exact, term) .") {
		t.Errorf("enrichment should add @index(exact, term) to label, got:\n%s", enrich)
	}
	if !strings.Contains(enrich, "source_file: string @index(exact) .") {
		t.Errorf("enrichment should add @index(exact) to source_file, got:\n%s", enrich)
	}
	if err := db.Alter(ctx, enrich); err != nil {
		t.Fatalf("Alter enrichment: %v", err)
	}
	// Confirm the union-of-(auto-detect + enrichment) schema is what we
	// expect by reading SchemaText back.
	final, _ := db.SchemaText(ctx)
	for _, want := range []string{
		"calls: [uid] @reverse .", // from auto-detect
		"label: string",            // body checked below for @index
	} {
		if !strings.Contains(final, want) {
			t.Errorf("post-Alter schema missing %q:\n%s", want, final)
		}
	}

	// Reverse walk from c → a, b.
	resp, err := db.Query(ctx, `{
		q(func: eq(gid, "c")) {
			gid
			callers: ~calls { gid }
		}
	}`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	body := string(resp.GetJson())
	for _, want := range []string{`"gid":"c"`, `"gid":"a"`, `"gid":"b"`} {
		if !strings.Contains(body, want) {
			t.Errorf("query missing %s: %s", want, body)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// newSeededDB opens a fresh DB at a temp dir, applies the given schema,
// and returns the open DB (registering Close via t.Cleanup).
func newSeededDB(t *testing.T, schema string) *bonsai.DB {
	t.Helper()
	db, err := bonsai.Open(bonsai.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Alter(context.Background(), schema); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	return db
}

