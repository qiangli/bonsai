/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * bonsai-bulk — offline batch loader for the embedded bonsai DB.
 *
 * The upstream `dgraph bulk` command predates this fork; it shards the
 * input across multiple groups, builds Badger LSM levels in parallel,
 * and emits ready-made on-disk shards. bonsai is single-node, so the
 * sharding pipeline is unnecessary. This tool opens the DB directly,
 * streams RDF or JSON through the same chunker the live loader uses,
 * and writes batched mutations to the local posting store.
 *
 * Typical use:
 *
 *	bonsai-bulk --dir ./data --schema schema.dql --rdfs goldendata.rdf.gz
 *	bonsai-bulk --dir ./data --json data.json --batch 1000
 *
 * The bonsai-server must NOT be running on the same --dir at the same
 * time; bulk holds an exclusive Badger lock.
 */
package bulk

import (
	"bufio"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apiproto "github.com/dgraph-io/dgo/v250/protos/api"

	"github.com/qiangli/bonsai/chunker"
	"github.com/qiangli/bonsai/pkg/bonsai"
)

func Main() {
	dir := flag.String("dir", "./bonsai-data", "data directory (must not be in use by bonsai-server)")
	schema := flag.String("schema", "", "DQL schema file to apply before ingest (optional)")
	rdfs := flag.String("rdfs", "", "input RDF file (.rdf or .rdf.gz)")
	jsonInput := flag.String("json", "", "input JSON file (.json or .json.gz)")
	batch := flag.Int("batch", 1000, "nquads per mutation")
	concurrency := flag.Int("concurrency", runtime.NumCPU(), "parallel mutators")
	flag.Parse()

	if *rdfs == "" && *jsonInput == "" {
		log.Fatal("--rdfs or --json is required")
	}
	if *rdfs != "" && *jsonInput != "" {
		log.Fatal("pass exactly one of --rdfs / --json")
	}

	db, err := bonsai.Open(bonsai.Options{Dir: *dir})
	if err != nil {
		log.Fatalf("Open %s: %v", *dir, err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Close: %v", err)
		}
	}()

	ctx := context.Background()

	if *schema != "" {
		schemaBytes, err := os.ReadFile(*schema)
		if err != nil {
			log.Fatalf("read schema %s: %v", *schema, err)
		}
		if err := db.Alter(ctx, string(schemaBytes)); err != nil {
			log.Fatalf("Alter schema: %v", err)
		}
		log.Printf("schema applied")
	}

	src := *rdfs
	format := chunker.RdfFormat
	if *jsonInput != "" {
		src = *jsonInput
		format = chunker.JsonFormat
	}

	r, closer, err := openInput(src)
	if err != nil {
		log.Fatalf("open %s: %v", src, err)
	}
	defer closer()

	start := time.Now()
	var (
		nqsCount  atomic.Uint64
		batchDone atomic.Uint64
	)

	ch := chunker.NewChunker(format, *batch)

	// Producer: read chunks, parse, push nquads onto buffer's channel.
	var prodErr error
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		br := bufio.NewReaderSize(r, 1<<20)
		for {
			cb, err := ch.Chunk(br)
			if cb != nil && cb.Len() > 0 {
				if perr := ch.Parse(cb); perr != nil {
					prodErr = fmt.Errorf("parse: %w", perr)
					return
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				prodErr = fmt.Errorf("chunk: %w", err)
				return
			}
		}
		ch.NQuads().Flush()
	}()

	// Consumers: pull nquad batches and apply as mutations.
	var wg sync.WaitGroup
	wg.Add(*concurrency)
	for w := 0; w < *concurrency; w++ {
		go func() {
			defer wg.Done()
			for nqs := range ch.NQuads().Ch() {
				if len(nqs) == 0 {
					continue
				}
				if _, err := db.Mutate(ctx, &apiproto.Mutation{Set: nqs}); err != nil {
					log.Printf("mutate batch (%d nquads): %v", len(nqs), err)
					continue
				}
				nqsCount.Add(uint64(len(nqs)))
				batchDone.Add(1)
			}
		}()
	}

	// Periodic progress.
	progDone := make(chan struct{})
	go func() {
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-progDone:
				return
			case <-t.C:
				n := nqsCount.Load()
				rate := float64(n) / time.Since(start).Seconds()
				log.Printf("progress: %d nquads, %d batches, %.0f nq/s", n, batchDone.Load(), rate)
			}
		}
	}()

	<-prodDone
	wg.Wait()
	close(progDone)

	if prodErr != nil {
		log.Fatalf("ingest: %v", prodErr)
	}
	log.Printf("done: %d nquads in %s (%.0f nq/s)",
		nqsCount.Load(), time.Since(start).Round(time.Millisecond),
		float64(nqsCount.Load())/time.Since(start).Seconds())
}

// openInput returns a reader for an .rdf, .rdf.gz, .json, or .json.gz file.
// The closer must be called to release file handles.
func openInput(path string) (io.Reader, func(), error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	closer := func() { _ = f.Close() }
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			closer()
			return nil, nil, err
		}
		return gz, func() { _ = gz.Close(); closer() }, nil
	}
	return f, closer, nil
}
