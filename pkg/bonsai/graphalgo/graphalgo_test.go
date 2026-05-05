/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package graphalgo_test

import (
	"context"
	"testing"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/bonsai/pkg/bonsai"
	"github.com/qiangli/bonsai/pkg/bonsai/graphalgo"
)

// helper: open a fresh DB, apply schema, ingest RDF, return open DB.
func newGraphDB(t *testing.T, schema, rdf string) *bonsai.DB {
	t.Helper()
	db, err := bonsai.Open(bonsai.Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Alter(context.Background(), schema); err != nil {
		t.Fatalf("Alter: %v", err)
	}
	if rdf != "" {
		if _, err := db.Mutate(context.Background(), &apiproto.Mutation{
			SetNquads: []byte(rdf),
		}); err != nil {
			t.Fatalf("Mutate: %v", err)
		}
	}
	return db
}

// TestLoadAdjacency_Basic confirms LoadAdjacency walks every uid edge
// and returns a coherent map.
func TestLoadAdjacency_Basic(t *testing.T) {
	db := newGraphDB(t, "name: string @index(exact) .\nfollows: [uid] .\n", `
		_:a <name>    "Alice" .
		_:b <name>    "Bob"   .
		_:c <name>    "Carol" .
		_:a <follows> _:b .
		_:a <follows> _:c .
		_:b <follows> _:c .
	`)

	adj, err := graphalgo.LoadAdjacency(context.Background(), db, "follows")
	if err != nil {
		t.Fatalf("LoadAdjacency: %v", err)
	}

	// Every node should appear in adj, and Alice has out-degree 2.
	if got := len(adj); got != 2 {
		t.Errorf("adj has %d sources, want 2 (alice + bob)", got)
	}
	for src, dsts := range adj {
		if src == 0 {
			t.Errorf("zero uid in adj: %v", adj)
		}
		if len(dsts) == 0 {
			t.Errorf("source %#x has no destinations", src)
		}
	}

	if got := len(adj.Nodes()); got != 3 {
		t.Errorf("Nodes() = %d, want 3 (a + b + c)", got)
	}
}

// TestTopInDegree_Order asserts the highest-fan-in node bubbles up.
func TestTopInDegree_Order(t *testing.T) {
	db := newGraphDB(t, "name: string @index(exact) .\ncalls: [uid] .\n", `
		_:a <name>  "A" .
		_:b <name>  "B" .
		_:c <name>  "C" .
		_:d <name>  "D" .
		_:a <calls> _:c .
		_:b <calls> _:c .
		_:d <calls> _:c .
		_:a <calls> _:b .
	`)
	adj, _ := graphalgo.LoadAdjacency(context.Background(), db, "calls")

	top := graphalgo.TopInDegree(adj, 2)
	if len(top) != 2 {
		t.Fatalf("TopInDegree len=%d, want 2", len(top))
	}
	// C has in-degree 3 (most-called), B has 1.
	if top[0].Degree != 3 {
		t.Errorf("top[0].Degree = %d, want 3 (C is the god node)", top[0].Degree)
	}
}

// TestPageRank_Sanity confirms the score distribution is a probability
// (sums to ~1) and that a hub node scores higher than a leaf.
func TestPageRank_Sanity(t *testing.T) {
	db := newGraphDB(t, "name: string @index(exact) .\nlinks: [uid] .\n", `
		_:a <name>  "A" .
		_:b <name>  "B" .
		_:c <name>  "C" .
		_:d <name>  "D" .
		_:b <links> _:a .
		_:c <links> _:a .
		_:d <links> _:a .
	`)
	adj, _ := graphalgo.LoadAdjacency(context.Background(), db, "links")
	scores := graphalgo.PageRank(adj, graphalgo.PageRankParams{Iterations: 30})

	if len(scores) != 4 {
		t.Errorf("scores has %d nodes, want 4", len(scores))
	}
	var sum float64
	for _, s := range scores {
		sum += s
	}
	if sum < 0.99 || sum > 1.01 {
		t.Errorf("PageRank sum = %f, expected ~1.0", sum)
	}
	// A has three incoming links; PageRank should rank A first. Verify
	// by finding the top-scoring node and asserting its in-degree is 3.
	var topUid uint64
	var topScore float64
	for u, s := range scores {
		if s > topScore {
			topScore = s
			topUid = u
		}
	}
	indeg := 0
	for _, dsts := range adj {
		for _, d := range dsts {
			if d == topUid {
				indeg++
			}
		}
	}
	if indeg != 3 {
		t.Errorf("top-scoring uid %#x has in-degree %d, want 3 (A)", topUid, indeg)
	}
}

// TestConnectedComponents partitions a graph with two disconnected
// components and checks their membership.
func TestConnectedComponents(t *testing.T) {
	db := newGraphDB(t, "name: string @index(exact) .\nlink: [uid] .\n", `
		_:a <name> "A" .
		_:b <name> "B" .
		_:c <name> "C" .
		_:d <name> "D" .
		_:a <link> _:b .
		_:c <link> _:d .
	`)
	adj, _ := graphalgo.LoadAdjacency(context.Background(), db, "link")
	cc := graphalgo.ConnectedComponents(adj)
	if len(cc) != 2 {
		t.Fatalf("got %d components, want 2: %v", len(cc), cc)
	}
	for _, c := range cc {
		if len(c) != 2 {
			t.Errorf("component %v has %d nodes, want 2", c, len(c))
		}
	}
}
