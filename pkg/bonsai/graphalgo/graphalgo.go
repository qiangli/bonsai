/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Built-in graph algorithms operating against a live bonsai DB. Designed
 * for the build-once / query-many workload where you've ingested a graph
 * (e.g. a code-knowledge graph from gfy) and want to answer the standard
 * "which nodes are central / which clusters exist / who's the most-called
 * function" questions without implementing the algorithm yourself.
 *
 * All algorithms run over an in-memory adjacency view loaded by
 * LoadAdjacency. For single-node embedded workloads (millions of edges,
 * hundreds of MB) this is fast enough; for huge graphs, reach for a
 * dedicated graph processing framework.
 *
 * Stability: this package is part of the v1 surface; see
 * pkg/bonsai/doc.go for the semver contract.
 */

package graphalgo

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/qiangli/bonsai/pkg/bonsai"
)

// Adjacency is a directed-graph adjacency map keyed by source UID.
// Each entry holds the list of destination UIDs reachable in one hop
// along the loaded predicate.
type Adjacency map[uint64][]uint64

// Nodes returns the set of UIDs that appear as a source or destination.
func (a Adjacency) Nodes() []uint64 {
	seen := make(map[uint64]struct{}, len(a))
	for src, dsts := range a {
		seen[src] = struct{}{}
		for _, d := range dsts {
			seen[d] = struct{}{}
		}
	}
	out := make([]uint64, 0, len(seen))
	for u := range seen {
		out = append(out, u)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// LoadAdjacency walks the entire predicate via DQL and returns its
// adjacency. The predicate must be uid-typed (`[uid]` in the schema).
//
// The query is `{ q(func: has(<predicate>)) { uid <predicate> { uid } } }`,
// which fans out one query and returns every edge in one shot. For very
// large graphs this is cheaper than per-node lookups but does buffer the
// full result in memory.
func LoadAdjacency(ctx context.Context, db *bonsai.DB, predicate string) (Adjacency, error) {
	q := fmt.Sprintf(`{ q(func: has(%s)) { uid %s { uid } } }`, predicate, predicate)
	resp, err := db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("graphalgo: load %s: %w", predicate, err)
	}
	// Decode the freeform JSON: "q":[{"uid":"0x1","<pred>":[{"uid":"0x2"}, ...]}, ...].
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(resp.GetJson(), &raw); err != nil {
		return nil, err
	}
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(raw["q"], &rows); err != nil {
		return nil, err
	}
	adj := make(Adjacency, len(rows))
	for _, row := range rows {
		srcStr := unquote(row["uid"])
		src, err := parseHexUid(srcStr)
		if err != nil {
			return nil, err
		}
		var children []map[string]json.RawMessage
		if err := json.Unmarshal(row[predicate], &children); err != nil {
			// No children for this row — has(predicate) matched but the
			// list is empty. Skip gracefully.
			continue
		}
		dsts := make([]uint64, 0, len(children))
		for _, c := range children {
			dst, err := parseHexUid(unquote(c["uid"]))
			if err == nil {
				dsts = append(dsts, dst)
			}
		}
		adj[src] = dsts
	}
	return adj, nil
}

// NodeDegree pairs a UID with its in- or out-degree. Returned by TopN.
type NodeDegree struct {
	Uid    uint64
	Degree int
}

// TopOutDegree returns the n nodes with the highest out-degree along the
// given adjacency (most callers, most-published authors, etc.).
func TopOutDegree(adj Adjacency, n int) []NodeDegree {
	out := make([]NodeDegree, 0, len(adj))
	for u, dsts := range adj {
		out = append(out, NodeDegree{Uid: u, Degree: len(dsts)})
	}
	return topN(out, n)
}

// TopInDegree returns the n nodes with the highest in-degree (most-called,
// most-cited, etc.). Computed by inverting the adjacency once.
func TopInDegree(adj Adjacency, n int) []NodeDegree {
	in := make(map[uint64]int, len(adj))
	for _, dsts := range adj {
		for _, d := range dsts {
			in[d]++
		}
	}
	out := make([]NodeDegree, 0, len(in))
	for u, c := range in {
		out = append(out, NodeDegree{Uid: u, Degree: c})
	}
	return topN(out, n)
}

func topN(s []NodeDegree, n int) []NodeDegree {
	sort.Slice(s, func(i, j int) bool {
		if s[i].Degree != s[j].Degree {
			return s[i].Degree > s[j].Degree
		}
		return s[i].Uid < s[j].Uid // tiebreak deterministically
	})
	if n > 0 && len(s) > n {
		s = s[:n]
	}
	return s
}

// PageRank returns the PageRank score for every node reachable in the
// adjacency. Standard algorithm:
//
//	r[v] = (1-d)/N + d * Σ r[u] / outdegree(u)  for each u → v
//
// Defaults: damping=0.85, iterations=20. Set them via PageRankParams.
type PageRankParams struct {
	Damping    float64
	Iterations int
}

// PageRank applies PageRankParams; pass {} for defaults.
func PageRank(adj Adjacency, opts PageRankParams) map[uint64]float64 {
	if opts.Damping == 0 {
		opts.Damping = 0.85
	}
	if opts.Iterations == 0 {
		opts.Iterations = 20
	}
	nodes := adj.Nodes()
	N := float64(len(nodes))
	if N == 0 {
		return map[uint64]float64{}
	}
	scores := make(map[uint64]float64, len(nodes))
	for _, u := range nodes {
		scores[u] = 1.0 / N
	}
	// Build reverse adjacency (incoming edges) once for the inner loop.
	rev := make(map[uint64][]uint64, len(nodes))
	for src, dsts := range adj {
		for _, d := range dsts {
			rev[d] = append(rev[d], src)
		}
	}
	out := make(map[uint64]int, len(adj))
	for src, dsts := range adj {
		out[src] = len(dsts)
	}

	base := (1 - opts.Damping) / N
	for i := 0; i < opts.Iterations; i++ {
		next := make(map[uint64]float64, len(nodes))
		// Dangling-node mass (nodes with no outgoing edges) is
		// distributed evenly to all nodes; standard PageRank fix.
		var dangling float64
		for _, u := range nodes {
			if out[u] == 0 {
				dangling += scores[u]
			}
		}
		danglingShare := opts.Damping * dangling / N
		for _, v := range nodes {
			s := base + danglingShare
			for _, u := range rev[v] {
				if out[u] > 0 {
					s += opts.Damping * scores[u] / float64(out[u])
				}
			}
			next[v] = s
		}
		scores = next
	}
	return scores
}

// ConnectedComponents partitions the nodes by undirected connectivity.
// Treats edges as bidirectional regardless of how they were stored.
// Returns a slice of components, each a sorted UID list. The slice
// itself is sorted so the smallest-min-UID component comes first —
// determinism matters for diff-friendly outputs.
func ConnectedComponents(adj Adjacency) [][]uint64 {
	nodes := adj.Nodes()
	parent := make(map[uint64]uint64, len(nodes))
	for _, u := range nodes {
		parent[u] = u
	}
	var find func(uint64) uint64
	find = func(u uint64) uint64 {
		if parent[u] == u {
			return u
		}
		r := find(parent[u])
		parent[u] = r
		return r
	}
	union := func(a, b uint64) {
		ra, rb := find(a), find(b)
		if ra != rb {
			parent[ra] = rb
		}
	}
	for src, dsts := range adj {
		for _, d := range dsts {
			union(src, d)
		}
	}
	groups := make(map[uint64][]uint64)
	for _, u := range nodes {
		r := find(u)
		groups[r] = append(groups[r], u)
	}
	out := make([][]uint64, 0, len(groups))
	for _, g := range groups {
		sort.Slice(g, func(i, j int) bool { return g[i] < g[j] })
		out = append(out, g)
	}
	sort.Slice(out, func(i, j int) bool {
		if len(out[i]) == 0 {
			return false
		}
		if len(out[j]) == 0 {
			return true
		}
		return out[i][0] < out[j][0]
	})
	return out
}

// helpers

func unquote(b json.RawMessage) string {
	if len(b) >= 2 && b[0] == '"' && b[len(b)-1] == '"' {
		return string(b[1 : len(b)-1])
	}
	return string(b)
}

func parseHexUid(s string) (uint64, error) {
	var v uint64
	if _, err := fmt.Sscanf(s, "0x%x", &v); err != nil {
		return 0, fmt.Errorf("parseHexUid %q: %w", s, err)
	}
	return v, nil
}
