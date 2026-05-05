/*
 * SPDX-FileCopyrightText: bonsai contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * bonsai-live — streaming RDF/JSON ingest against a running bonsai-server.
 *
 * Differences from bonsai-bulk:
 *   - bonsai-live talks to a running bonsai-server over gRPC; bulk opens
 *     the embedded DB directly.
 *   - Use live when the server must stay up (e.g. to serve queries during
 *     the load); use bulk when raw ingest speed matters and downtime is
 *     acceptable.
 *
 * Typical use:
 *
 *	bonsai-live --addr 127.0.0.1:9080 --schema schema.dql --rdfs data.rdf.gz
 *	bonsai-live --addr remote:9080 --json data.json --batch 1000
 */
package main

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

	"github.com/dgraph-io/dgo/v250"
	"github.com/dgraph-io/dgo/v250/protos/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/qiangli/bonsai/chunker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9080", "bonsai-server gRPC address")
	schema := flag.String("schema", "", "DQL schema file to apply before ingest (optional)")
	rdfs := flag.String("rdfs", "", "input RDF file (.rdf or .rdf.gz)")
	jsonInput := flag.String("json", "", "input JSON file (.json or .json.gz)")
	batch := flag.Int("batch", 1000, "nquads per mutation")
	concurrency := flag.Int("concurrency", runtime.NumCPU(), "parallel mutators")
	timeout := flag.Duration("timeout", 5*time.Minute, "overall ingest timeout")
	flag.Parse()

	if *rdfs == "" && *jsonInput == "" {
		log.Fatal("--rdfs or --json is required")
	}
	if *rdfs != "" && *jsonInput != "" {
		log.Fatal("pass exactly one of --rdfs / --json")
	}

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer func() { _ = conn.Close() }()
	dg := dgo.NewDgraphClient(api.NewDgraphClient(conn))

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	if *schema != "" {
		schemaBytes, err := os.ReadFile(*schema)
		if err != nil {
			log.Fatalf("read schema %s: %v", *schema, err)
		}
		if err := dg.Alter(ctx, &api.Operation{Schema: string(schemaBytes)}); err != nil {
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

	var wg sync.WaitGroup
	wg.Add(*concurrency)
	for w := 0; w < *concurrency; w++ {
		go func() {
			defer wg.Done()
			for nqs := range ch.NQuads().Ch() {
				if len(nqs) == 0 {
					continue
				}
				txn := dg.NewTxn()
				if _, err := txn.Mutate(ctx, &api.Mutation{Set: nqs, CommitNow: true}); err != nil {
					log.Printf("mutate batch (%d nquads): %v", len(nqs), err)
					_ = txn.Discard(ctx)
					continue
				}
				nqsCount.Add(uint64(len(nqs)))
				batchDone.Add(1)
			}
		}()
	}

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
