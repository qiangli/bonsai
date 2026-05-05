/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Unit tests for the auto-detect / converter pipeline. These exercise
 * the pure functions (sniffGraphJSON, prepareGraphJSON) without going
 * through ImportStream or db.Mutate, so they're fast and don't share the
 * process-global state that the e2e tests do.
 */

package bonsai

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stubAlloc is a deterministic uid allocator for tests: returns 1, 1+n,
// 1+n+m, ... so callers see a predictable mapping.
func stubAlloc() func(uint64) (uint64, error) {
	var next uint64
	return func(n uint64) (uint64, error) {
		start := next + 1
		next += n
		return start, nil
	}
}

// TestSniffGraphJSON_NetworkX_NodeLink covers the sniff outcomes for the
// NetworkX node-link shape (default `links` key and the alternate
// `edges` spelling).
func TestSniffGraphJSON_NetworkX_NodeLink(t *testing.T) {
	cases := []struct {
		name string
		body string
		want graphJSONKind
	}{
		{"default-links", `{"directed":true,"nodes":[],"links":[]}`, networkxNodeLink},
		{"alt-edges", `{"directed":false,"nodes":[],"edges":[]}`, networkxNodeLink},
		{"with-graph-meta", `{"graph":{"name":"x"},"nodes":[],"links":[]}`, networkxNodeLink},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got, err := sniffGraphJSON(strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("sniff: %v", err)
			}
			if got != tc.want {
				t.Errorf("got=%s want=%s", got, tc.want)
			}
		})
	}
}

// TestSniffGraphJSON_Cytoscape covers the Cytoscape elements shape.
func TestSniffGraphJSON_Cytoscape(t *testing.T) {
	body := `{"elements":{"nodes":[{"data":{"id":"a"}}],"edges":[]}}`
	_, got, err := sniffGraphJSON(strings.NewReader(body))
	if err != nil {
		t.Fatalf("sniff: %v", err)
	}
	if got != cytoscapeElements {
		t.Errorf("got=%s want=cytoscape-elements", got)
	}
}

// TestSniffGraphJSON_DgraphPassthrough confirms ordinary Dgraph-flavored
// JSON (an array of objects) is left alone.
func TestSniffGraphJSON_DgraphPassthrough(t *testing.T) {
	for _, body := range []string{
		`[{"name":"Alice"},{"name":"Bob"}]`,
		`{"name":"Alice","age":30}`, // single object — not a graph shape
		`   [   ]`,                  // leading whitespace + empty array
	} {
		_, got, err := sniffGraphJSON(strings.NewReader(body))
		if err != nil {
			t.Fatalf("sniff %q: %v", body, err)
		}
		if got != dgraphJSON {
			t.Errorf("body %q: got=%s want=dgraph-json", body, got)
		}
	}
}

// TestSniffGraphJSON_PassesThroughBytes ensures the returned reader
// replays everything we peeked at — important since the caller hands the
// reader to a downstream parser and we must not have consumed bytes.
func TestSniffGraphJSON_PassesThroughBytes(t *testing.T) {
	body := `{"directed":true,"nodes":[{"id":"a"}],"links":[]}`
	r, _, _ := sniffGraphJSON(strings.NewReader(body))
	got, err := readAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != body {
		t.Errorf("byte mismatch:\nwant: %q\ngot:  %q", body, got)
	}
}

// TestPrepareGraphJSON_Minimal exercises the conversion of a small
// hand-crafted graph and asserts on every element of the output.
func TestPrepareGraphJSON_Minimal(t *testing.T) {
	body := `{
	  "directed": true,
	  "multigraph": false,
	  "graph": {},
	  "nodes": [
	    {"id": "a", "label": "Alice", "tags": ["person", "vip"]},
	    {"id": "b", "label": "Bob", "tags": ["person"]},
	    {"id": "c", "label": "Carol"}
	  ],
	  "links": [
	    {"source": "a", "target": "b", "relation": "knows"},
	    {"source": "a", "target": "c", "relation": "knows"}
	  ]
	}`
	conv, err := prepareGraphJSON(strings.NewReader(body), networkxNodeLink, stubAlloc())
	if err != nil {
		t.Fatalf("prepareGraphJSON: %v", err)
	}

	// Schema: gid + label + tags ([string]) + knows ([uid]).
	wantSchemaSubstrings := []string{
		"gid: string @index(exact) .",
		"label: string .",
		"tags: [string] .",
		"knows: [uid] @reverse .",
	}
	for _, want := range wantSchemaSubstrings {
		if !strings.Contains(conv.Schema, want) {
			t.Errorf("schema missing %q\n--- schema ---\n%s", want, conv.Schema)
		}
	}

	// UID range: 3 nodes, allocator returned 1 → uids [1,2,3].
	if conv.UidStart != 1 || conv.UidCount != 3 {
		t.Errorf("uid range: start=%d count=%d, want 1/3", conv.UidStart, conv.UidCount)
	}

	rdf := string(conv.RDF)
	// Each node carries its gid, label, and tags. Hex UIDs are deterministic.
	wantRDFLines := []string{
		`<0x1> <gid> "a" .`,
		`<0x1> <label> "Alice" .`,
		`<0x1> <tags> "person" .`,
		`<0x1> <tags> "vip" .`,
		`<0x2> <gid> "b" .`,
		`<0x3> <gid> "c" .`,
		`<0x1> <knows> <0x2> .`,
		`<0x1> <knows> <0x3> .`,
	}
	for _, want := range wantRDFLines {
		if !strings.Contains(rdf, want) {
			t.Errorf("RDF missing line %q\n--- RDF ---\n%s", want, rdf)
		}
	}
	// Carol has no `tags` predicate; make sure we didn't emit one.
	if strings.Contains(rdf, `<0x3> <tags>`) {
		t.Errorf("RDF emitted tags for Carol who had none:\n%s", rdf)
	}
}

// TestPrepareGraphJSON_PredicatePromotion ensures a predicate that ever
// appears as a list across the document is typed `[string]` even on
// nodes where it appeared as a scalar.
func TestPrepareGraphJSON_PredicatePromotion(t *testing.T) {
	body := `{
	  "nodes": [
	    {"id": "a", "tag": "x"},
	    {"id": "b", "tag": ["y", "z"]}
	  ],
	  "links": []
	}`
	conv, err := prepareGraphJSON(strings.NewReader(body), networkxNodeLink, stubAlloc())
	if err != nil {
		t.Fatalf("prepareGraphJSON: %v", err)
	}
	if !strings.Contains(conv.Schema, "tag: [string] .") {
		t.Errorf("expected tag promoted to [string], schema=\n%s", conv.Schema)
	}
	if strings.Contains(conv.Schema, "tag: string .") {
		t.Errorf("expected tag NOT to be a scalar string, schema=\n%s", conv.Schema)
	}
}

// TestPrepareGraphJSON_PredicateSanitization confirms keys with chars
// the schema parser doesn't accept get rewritten before they hit the
// schema.
func TestPrepareGraphJSON_PredicateSanitization(t *testing.T) {
	body := `{
	  "nodes": [{"id":"a","my-pred":"v","_priv":"v"}],
	  "links": []
	}`
	conv, err := prepareGraphJSON(strings.NewReader(body), networkxNodeLink, stubAlloc())
	if err != nil {
		t.Fatalf("prepareGraphJSON: %v", err)
	}
	// `my-pred` → `my_pred`; `_priv` → `priv` (leading underscores stripped).
	if !strings.Contains(conv.Schema, "my_pred: string .") {
		t.Errorf("expected my_pred predicate, schema=\n%s", conv.Schema)
	}
	if !strings.Contains(conv.Schema, "priv: string .") {
		t.Errorf("expected priv predicate, schema=\n%s", conv.Schema)
	}
}

// TestPrepareGraphJSON_DroppedDanglingEdges confirms edges whose
// endpoints aren't in the node table are silently skipped (rather than
// failing the whole import).
func TestPrepareGraphJSON_DroppedDanglingEdges(t *testing.T) {
	body := `{
	  "nodes": [{"id":"a"}],
	  "links": [
	    {"source":"a","target":"b","relation":"r"},
	    {"source":"b","target":"a","relation":"r"}
	  ]
	}`
	conv, err := prepareGraphJSON(strings.NewReader(body), networkxNodeLink, stubAlloc())
	if err != nil {
		t.Fatalf("prepareGraphJSON: %v", err)
	}
	if strings.Contains(string(conv.RDF), "<r>") {
		t.Errorf("unexpected r edge with dangling endpoint:\n%s", string(conv.RDF))
	}
}

// TestPrepareGraphJSON_GfyFixture exercises the conversion against the
// real .gfy-out/graph.json fixture in the repo. We don't ingest it here
// — the unit test cares only about converter shape and counts. The
// smaller end-to-end test (TestFeatureAutoDetectNetworkXImport) covers
// the ingest path.
func TestPrepareGraphJSON_GfyFixture(t *testing.T) {
	path := findRepoFile(t, ".gfy-out/graph.json")
	if path == "" {
		t.Skip(".gfy-out/graph.json not found at the repo root; skipping fixture test")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}

	// Sniff first; the fixture must be detected as node-link.
	_, kind, err := sniffGraphJSON(bytes.NewReader(body))
	if err != nil || kind != networkxNodeLink {
		t.Fatalf("sniff(.gfy-out/graph.json): kind=%s err=%v", kind, err)
	}

	conv, err := prepareGraphJSON(bytes.NewReader(body), kind, stubAlloc())
	if err != nil {
		t.Fatalf("prepareGraphJSON(.gfy-out): %v", err)
	}

	// Headline shape: 4429 nodes, with `gid`, `label`, etc. and
	// uid-edge predicates `calls`, `contains`, `method`.
	if conv.UidCount != 4429 {
		t.Errorf("UidCount=%d, want 4429", conv.UidCount)
	}
	for _, want := range []string{
		"gid: string @index(exact) .",
		"label: string .",
		"file_type: string .",
		"source_file: string .",
		"calls: [uid] @reverse .",
		"contains: [uid] @reverse .",
		"method: [uid] @reverse .",
	} {
		if !strings.Contains(conv.Schema, want) {
			t.Errorf("schema missing %q\nschema:\n%s", want, conv.Schema)
		}
	}

	// At least one `gid`-line per node and at least one edge of each kind.
	rdf := string(conv.RDF)
	for _, want := range []string{
		`<gid> "algo_cm_sketch_go"`,
		`<calls>`,
		`<contains>`,
		`<method>`,
	} {
		if !strings.Contains(rdf, want) {
			t.Errorf("RDF missing %q (truncated head: %.200s)", want, rdf)
		}
	}

	// Ballpark the line count: at least 4429 (one gid per node) plus one
	// per-edge triple. Real fixture is ~37K triples.
	gotLines := strings.Count(rdf, "\n")
	if gotLines < 4429*2 {
		t.Errorf("RDF line count %d looks too low; expected ≫ 8K", gotLines)
	}
}

// findRepoFile climbs from the test's CWD until it finds a sibling named
// `name`. Returns "" if none of the parents have it. Used so the test
// works regardless of go test's chosen working directory.
func findRepoFile(t *testing.T, name string) string {
	t.Helper()
	wd, _ := os.Getwd()
	for {
		try := filepath.Join(wd, name)
		if _, err := os.Stat(try); err == nil {
			return try
		}
		parent := filepath.Dir(wd)
		if parent == wd {
			return ""
		}
		wd = parent
	}
}

// readAll drains an io.Reader into a string. Tiny helper so the test
// file doesn't need its own io import path repeated everywhere.
func readAll(r interface {
	Read(p []byte) (n int, err error)
}) (string, error) {
	var buf bytes.Buffer
	chunk := make([]byte, 4096)
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			buf.Write(chunk[:n])
		}
		if err != nil {
			if err.Error() == "EOF" {
				return buf.String(), nil
			}
			return buf.String(), err
		}
	}
}
