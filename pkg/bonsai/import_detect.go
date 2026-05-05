/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Automatic detection + conversion for popular graph-export JSON shapes
 * that aren't Dgraph-flavored (NetworkX node-link, Cytoscape elements).
 *
 * The sniffer reads enough of the JSON head to identify the shape, then
 * either hands the original bytes back (for Dgraph JSON, which the chunker
 * handles natively) or transcodes to RDF N-Quads on the fly so the rest of
 * the import pipeline doesn't have to know the difference.
 *
 * Supported "alien" shapes:
 *
 *   NetworkX node-link:
 *     {"directed":..., "multigraph":..., "graph":{...},
 *      "nodes":[{"id":..., ...attrs}], "links":[{"source":..., "target":..., ...attrs}]}
 *     (also accepts "edges" instead of "links")
 *
 *   Cytoscape.js elements:
 *     {"elements": {"nodes":[{"data":{"id":..., ...}}],
 *                   "edges":[{"data":{"source":..., "target":..., ...}}]}}
 *
 * For any other JSON, the sniffer returns kind=DgraphJSON and the original
 * bytes are passed through to the chunker's existing JSON parser.
 */

package bonsai

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
)

// graphJSONKind tags what shape the input matched.
type graphJSONKind int

const (
	dgraphJSON graphJSONKind = iota
	networkxNodeLink
	cytoscapeElements
)

func (k graphJSONKind) String() string {
	switch k {
	case networkxNodeLink:
		return "networkx-node-link"
	case cytoscapeElements:
		return "cytoscape-elements"
	default:
		return "dgraph-json"
	}
}

// sniffGraphJSON peeks at the head of r to decide which shape it is.
// Returns a new reader that replays the peeked bytes followed by the rest
// of the input — callers should use the returned reader, not r.
//
// The peek window is bounded (1 MB) — typical graph-export JSONs put the
// distinguishing top-level keys in the first KB or two, so a 1 MB peek
// covers documents with very long header values.
func sniffGraphJSON(r io.Reader) (io.Reader, graphJSONKind, error) {
	const peekLimit = 1 << 20
	br := bufio.NewReaderSize(r, peekLimit)
	head, _ := br.Peek(peekLimit)
	trim := bytes.TrimLeft(head, " \t\r\n")
	if len(trim) == 0 {
		return br, dgraphJSON, nil
	}
	// Dgraph JSON is typically an array — `[...]` — pass through.
	if trim[0] == '[' {
		return br, dgraphJSON, nil
	}
	// Object form: try to decode just the top-level keys. RawMessage
	// avoids parsing nested values, which keeps the work bounded by the
	// number of top-level keys (typically <10 for graph exports).
	var top map[string]json.RawMessage
	if err := json.Unmarshal(trim, &top); err != nil {
		// The peek may have truncated the document mid-token. Try again
		// with json.Decoder, which can stream the head and stop early
		// once we've seen enough to decide.
		if kind, ok := sniffPartial(trim); ok {
			return br, kind, nil
		}
		return br, dgraphJSON, nil
	}
	switch {
	case hasKey(top, "nodes") && (hasKey(top, "links") || hasKey(top, "edges")):
		return br, networkxNodeLink, nil
	case hasKey(top, "elements"):
		return br, cytoscapeElements, nil
	case hasKey(top, "nodes") && (hasKey(top, "directed") || hasKey(top, "multigraph") || hasKey(top, "graph")):
		// NetworkX exports always include at least one of these
		// metadata keys alongside `nodes`, even if `links` was off the
		// peek window. Dgraph JSON, by contrast, is an array — never
		// an object with a top-level `nodes` key.
		return br, networkxNodeLink, nil
	}
	return br, dgraphJSON, nil
}

func hasKey(m map[string]json.RawMessage, k string) bool {
	_, ok := m[k]
	return ok
}

// sniffPartial scans a possibly-truncated JSON object for top-level keys
// using json.Decoder. It tracks brace/bracket depth so that strings
// nested inside values aren't confused with keys at depth 1.
func sniffPartial(b []byte) (graphJSONKind, bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return dgraphJSON, false
	}
	keys := map[string]bool{}
	depth := 1
	expectKey := true
	for {
		t, err := dec.Token()
		if err != nil {
			break
		}
		switch v := t.(type) {
		case json.Delim:
			switch v {
			case '{', '[':
				depth++
				expectKey = false
			case '}', ']':
				depth--
				expectKey = (depth == 1) // back at top level → next string is a key
			}
		case string:
			if depth == 1 && expectKey {
				keys[v] = true
				expectKey = false // next token is the value
			} else if depth == 1 {
				expectKey = true // value finished, next string is a key
			}
		default:
			if depth == 1 {
				expectKey = true // primitive value at top level finished
			}
		}
	}
	switch {
	case keys["nodes"] && (keys["links"] || keys["edges"]):
		return networkxNodeLink, true
	case keys["elements"]:
		return cytoscapeElements, true
	case keys["nodes"] && (keys["directed"] || keys["multigraph"] || keys["graph"]):
		return networkxNodeLink, true
	}
	return dgraphJSON, false
}

// graphConversion is the result of preparing a node-link / cytoscape
// document for ingest: the generated schema (one line per predicate, the
// types inferred from observed values) and the converted RDF body. The
// RDF uses explicit hex UIDs (`<0xN>`) rather than blank nodes so that
// the import can be split across multiple Mutate batches without
// fragmenting blank-node aliases.
type graphConversion struct {
	Schema   string
	RDF      []byte
	UidStart uint64 // first UID used; the converter requests a contiguous range
	UidCount uint64 // number of UIDs allocated (== node count)
}

// prepareGraphJSON parses a NetworkX-style node-link or Cytoscape
// document, infers a permissive schema (string / [string] / [uid]) for
// every observed predicate, and emits the matching RDF triples with
// explicit hex UIDs.
//
// allocUid must allocate a contiguous range of `count` UIDs and return
// the first UID in that range; it's typically (*DB).AssignUid. We allocate
// up-front so the converted RDF can be split across multiple Mutate
// batches without losing coherence (blank-node aliases are not stable
// across separate Mutate calls).
//
// The caller applies Schema via db.Alter before feeding RDF to the chunker.
//
// We deliberately buffer the converted RDF in memory rather than streaming
// it: the conversion needs to learn the predicate set from a full pass
// over the document anyway. Graph-export JSONs in the wild are typically
// well under 100 MB; if you have a 1 GB NetworkX dump, generate the RDF
// out-of-band and use bonsai-bulk --rdfs.
func prepareGraphJSON(r io.Reader, kind graphJSONKind, allocUid func(count uint64) (uint64, error)) (*graphConversion, error) {
	var doc struct {
		Nodes []map[string]any `json:"nodes"`
		Links []map[string]any `json:"links"`
		Edges []map[string]any `json:"edges"`
		Elements *struct {
			Nodes []struct {
				Data map[string]any `json:"data"`
			} `json:"nodes"`
			Edges []struct {
				Data map[string]any `json:"data"`
			} `json:"edges"`
		} `json:"elements"`
	}
	if err := json.NewDecoder(r).Decode(&doc); err != nil {
		return nil, fmt.Errorf("graph-json decode: %w", err)
	}

	if kind == cytoscapeElements {
		if doc.Elements == nil {
			return nil, fmt.Errorf("graph-json: cytoscape detected but elements{} missing")
		}
		doc.Nodes = nil
		doc.Links = nil
		for _, n := range doc.Elements.Nodes {
			doc.Nodes = append(doc.Nodes, n.Data)
		}
		for _, e := range doc.Elements.Edges {
			doc.Links = append(doc.Links, e.Data)
		}
	} else if len(doc.Links) == 0 && len(doc.Edges) > 0 {
		doc.Links = doc.Edges
	}

	// First pass: collect ids and learn which scalar predicates ever
	// appeared as a JSON list (so we type them `[string]` instead of
	// `string`). Edge relations always become `[uid]` predicates.
	scalarPreds := map[string]bool{}
	listPreds := map[string]bool{}
	for i, n := range doc.Nodes {
		if _, ok := stringField(n, "id"); !ok {
			return nil, fmt.Errorf("graph-json: node[%d] has no id", i)
		}
		for k, v := range n {
			if k == "id" {
				continue
			}
			pred := sanitizePredicate(k)
			if pred == "" {
				continue
			}
			if _, isList := v.([]any); isList {
				listPreds[pred] = true
			} else {
				scalarPreds[pred] = true
			}
		}
	}
	// A predicate that ever appears as a list must always be declared
	// list-typed — otherwise mutations with multiple values for the same
	// (subject, predicate) get rejected.
	for p := range listPreds {
		delete(scalarPreds, p)
	}
	uidPreds := map[string]bool{}
	for _, l := range doc.Links {
		rel, _ := stringField(l, "relation")
		rel = sanitizePredicate(rel)
		if rel == "" {
			rel = "edge"
		}
		uidPreds[rel] = true
	}

	// Build the schema. `gid` is always present (the node's stable
	// external id); we index it as exact so callers can do
	// `eq(gid, "...")` lookups.
	var schema strings.Builder
	schema.WriteString("gid: string @index(exact) .\n")
	preds := append(sortedKeys(scalarPreds), sortedKeys(listPreds)...)
	for _, p := range sortedKeys(scalarPreds) {
		fmt.Fprintf(&schema, "%s: string .\n", p)
	}
	for _, p := range sortedKeys(listPreds) {
		fmt.Fprintf(&schema, "%s: [string] .\n", p)
	}
	for _, p := range sortedKeys(uidPreds) {
		fmt.Fprintf(&schema, "%s: [uid] @reverse .\n", p)
	}
	_ = preds // silence unused; preserved for diagnostics if we ever surface it

	// Allocate a contiguous UID range up-front so each node gets a stable
	// hex UID we can reference across batches.
	count := uint64(len(doc.Nodes))
	startUid, err := allocUid(count)
	if err != nil {
		return nil, fmt.Errorf("graph-json: allocate uids: %w", err)
	}
	idToUid := make(map[string]uint64, count)
	for i, n := range doc.Nodes {
		id, _ := stringField(n, "id")
		idToUid[id] = startUid + uint64(i)
	}

	// Second pass: emit RDF using `<0xN>` subject/object refs.
	var buf bytes.Buffer
	for i, n := range doc.Nodes {
		uid := startUid + uint64(i)
		subj := fmt.Sprintf("<%#x>", uid)
		id, _ := stringField(n, "id")
		_ = writeTriple(&buf, subj, "gid", quoteRDF(id))
		for k, v := range n {
			if k == "id" {
				continue
			}
			pred := sanitizePredicate(k)
			if pred == "" {
				continue
			}
			if err := emitAttr(&buf, subj, pred, v); err != nil {
				return nil, err
			}
		}
	}
	for _, l := range doc.Links {
		s, _ := stringField(l, "source")
		if s == "" {
			s, _ = stringField(l, "_src")
		}
		t, _ := stringField(l, "target")
		if t == "" {
			t, _ = stringField(l, "_tgt")
		}
		su, sok := idToUid[s]
		tu, tok := idToUid[t]
		if !sok || !tok {
			continue
		}
		rel, _ := stringField(l, "relation")
		rel = sanitizePredicate(rel)
		if rel == "" {
			rel = "edge"
		}
		_ = writeTriple(&buf,
			fmt.Sprintf("<%#x>", su), rel, fmt.Sprintf("<%#x>", tu))
	}
	return &graphConversion{
		Schema:   schema.String(),
		RDF:      buf.Bytes(),
		UidStart: startUid,
		UidCount: count,
	}, nil
}

// sortedKeys returns the keys of a string-set in deterministic order so
// the generated schema is stable.
func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// convertNodeLinkJSONtoRDF is a thin wrapper around prepareGraphJSON that
// writes only the RDF body to w (discarding the inferred schema). Used
// only by tests, so it allocates UIDs starting from 1 — production
// callers always go through ImportStream and pass db.AssignUid.
func convertNodeLinkJSONtoRDF(r io.Reader, w io.Writer, kind graphJSONKind) error {
	var nextUid uint64
	alloc := func(n uint64) (uint64, error) {
		start := nextUid + 1
		nextUid += n
		return start, nil
	}
	conv, err := prepareGraphJSON(r, kind, alloc)
	if err != nil {
		return err
	}
	_, err = w.Write(conv.RDF)
	return err
}

// emitAttr writes one triple per scalar value, or one per list element if
// the value is a JSON array. Nested objects are JSON-encoded into a
// string — they're rare in graph exports and the alternative (deep
// flattening) usually does the wrong thing.
func emitAttr(w io.Writer, subject, pred string, v any) error {
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		return writeTriple(w, subject, pred, quoteRDF(x))
	case bool:
		return writeTriple(w, subject, pred, fmt.Sprintf(`%q^^<xs:boolean>`, strconv.FormatBool(x)))
	case float64:
		// JSON numbers come through as float64. Use int form when the
		// value is an integer so the schema infers the right type.
		if x == float64(int64(x)) {
			return writeTriple(w, subject, pred, fmt.Sprintf(`%q^^<xs:int>`, strconv.FormatInt(int64(x), 10)))
		}
		return writeTriple(w, subject, pred, fmt.Sprintf(`%q^^<xs:float>`, strconv.FormatFloat(x, 'g', -1, 64)))
	case []any:
		for _, item := range x {
			if err := emitAttr(w, subject, pred, item); err != nil {
				return err
			}
		}
		return nil
	default:
		// map[string]any or other: JSON-encode and store as a string.
		buf, err := json.Marshal(x)
		if err != nil {
			return err
		}
		return writeTriple(w, subject, pred, quoteRDF(string(buf)))
	}
}

// stringField returns the named field as a string, coercing numeric ids
// (e.g. integer node ids in some exports) into their decimal form.
func stringField(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", false
	}
	switch x := v.(type) {
	case string:
		return x, true
	case float64:
		if x == float64(int64(x)) {
			return strconv.FormatInt(int64(x), 10), true
		}
		return strconv.FormatFloat(x, 'g', -1, 64), true
	default:
		return fmt.Sprintf("%v", x), true
	}
}

// sanitizePredicate normalises a key into something the DQL schema
// accepts. Replaces non-letter/digit/underscore runes with `_`, then
// trims leading underscores and digits so the result is a valid
// identifier. Empty input returns empty.
func sanitizePredicate(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '_':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	out := strings.TrimLeft(b.String(), "_0123456789")
	if out == "" {
		return "p_" + b.String()
	}
	return out
}

func writeTriple(w io.Writer, subject, predicate, object string) error {
	_, err := fmt.Fprintf(w, "%s <%s> %s .\n", subject, predicate, object)
	return err
}

func quoteRDF(s string) string {
	// strconv.Quote handles escaping for control characters, quotes, and
	// backslashes — it's a superset of what RDF N-Quads needs and Bonsai's
	// chunker accepts the result.
	return strconv.Quote(s)
}
