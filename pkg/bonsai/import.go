/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * Streaming ingest helper. Same chunker pipeline that bonsai-bulk uses,
 * exposed as a library function so HTTP /admin/import and CLI tools can
 * share it without code duplication.
 */

package bonsai

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/bonsai/chunker"
)

// ImportSummary is the JSON shape returned from ImportStream / /admin/import.
type ImportSummary struct {
	Format     string `json:"format"`
	Detected   string `json:"detected,omitempty"` // populated when auto-detection picked a non-default shape
	Nquads     uint64 `json:"nquads"`
	Batches    uint64 `json:"batches"`
	DurationMs int64  `json:"duration_ms"`
	Errors     int    `json:"errors,omitempty"`
}

// ImportStream parses RDF or JSON from r in chunks of batchSize nquads and
// applies each batch as a Mutate call. Returns a summary even on partial
// failure (Errors counts batches that did not commit).
//
// Format must be "rdf" or "json".
func ImportStream(ctx context.Context, db *DB, format string, r io.Reader, batchSize int) (*ImportSummary, error) {
	if db == nil {
		return nil, fmt.Errorf("ImportStream: db is nil")
	}
	if r == nil {
		return nil, fmt.Errorf("ImportStream: reader is nil")
	}
	if batchSize <= 0 {
		batchSize = 1000
	}

	var inputFormat chunker.InputFormat
	var detected string
	switch format {
	case "rdf":
		inputFormat = chunker.RdfFormat
	case "json":
		// JSON could be Dgraph-flavored (the chunker handles it natively)
		// or one of the popular graph-export shapes (NetworkX node-link,
		// Cytoscape elements). Sniff the head to decide.
		sniffed, kind, err := sniffGraphJSON(r)
		if err != nil {
			return nil, fmt.Errorf("ImportStream: sniff: %w", err)
		}
		if kind == networkxNodeLink || kind == cytoscapeElements {
			// Parse the whole document, infer a permissive schema for
			// the predicates we observe, apply it, then feed the
			// generated RDF triples through the chunker. Buffering the
			// converted RDF in memory is fine for typical graph-export
			// JSONs (well under 100 MB); for huge inputs, convert
			// out-of-band and use --rdfs.
			//
			// The converter pre-allocates a contiguous UID range via
			// db.AssignUid so the emitted RDF references each node by
			// explicit hex UID — necessary because blank-node aliases
			// don't survive splitting across multiple Mutate batches.
			alloc := func(n uint64) (uint64, error) {
				start, _, err := db.AssignUid(ctx, n)
				return start, err
			}
			conv, err := prepareGraphJSON(sniffed, kind, alloc)
			if err != nil {
				return nil, fmt.Errorf("ImportStream: convert %s: %w", kind, err)
			}
			if err := db.Alter(ctx, conv.Schema); err != nil {
				return nil, fmt.Errorf("ImportStream: apply inferred schema: %w", err)
			}
			r = bytes.NewReader(conv.RDF)
			inputFormat = chunker.RdfFormat
			detected = kind.String()
			// The whole document is already in memory after conversion, and
			// per-batch overhead with @reverse-edge maintenance scales
			// poorly past a few thousand nodes (37K nquads × 37 batches
			// hangs >60s; same nquads in a single batch lands in ~150 ms).
			// One big batch is the right shape for an auto-detected
			// graph import.
			if int64(len(conv.RDF)) > int64(batchSize) {
				batchSize = len(conv.RDF) // chunker batches by nquad count, not bytes; this is a safe upper bound
			}
		} else {
			r = sniffed
			inputFormat = chunker.JsonFormat
		}
	default:
		return nil, fmt.Errorf("ImportStream: unknown format %q (want rdf or json)", format)
	}

	start := time.Now()
	var (
		nquads  atomic.Uint64
		batches atomic.Uint64
		errs    atomic.Int32
	)

	ch := chunker.NewChunker(inputFormat, batchSize)

	// Producer reads chunks of input and pushes parsed nquads onto the
	// chunker's internal channel.
	prodErr := make(chan error, 1)
	go func() {
		defer close(prodErr)
		br := bufio.NewReaderSize(r, 1<<20)
		for {
			cb, err := ch.Chunk(br)
			if cb != nil && cb.Len() > 0 {
				if perr := ch.Parse(cb); perr != nil {
					prodErr <- fmt.Errorf("parse: %w", perr)
					return
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				prodErr <- fmt.Errorf("chunk: %w", err)
				return
			}
		}
		ch.NQuads().Flush()
	}()

	// Consumer applies batches. We use one consumer because db.Mutate
	// already serialises behind a process-wide lock; running multiple
	// consumers just adds contention without parallelism.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for nqs := range ch.NQuads().Ch() {
			if len(nqs) == 0 {
				continue
			}
			if _, err := db.Mutate(ctx, &apiproto.Mutation{Set: nqs}); err != nil {
				errs.Add(1)
				continue
			}
			nquads.Add(uint64(len(nqs)))
			batches.Add(1)
		}
	}()

	pe := <-prodErr
	wg.Wait()

	summary := &ImportSummary{
		Format:     format,
		Detected:   detected,
		Nquads:     nquads.Load(),
		Batches:    batches.Load(),
		DurationMs: time.Since(start).Milliseconds(),
		Errors:     int(errs.Load()),
	}
	if pe != nil {
		return summary, pe
	}
	return summary, nil
}
