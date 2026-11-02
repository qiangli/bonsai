---
id: a8cb2c49033e
kind: bug
title: S557 prevent concurrent RDF export writes from corrupting roundtrip data
seq: 6
status: assigned
priority: p1
created: 2026-11-02T01:48:59.576393Z
assignee: s557-test-readiness
sprint: 557
sprint_id: f5ccf563-f5d1-5b94-8be4-9388e51ef86d
sprint_title: 'Bashy v1.0.0 preparation: split private/public development and retire qiangli/dhnt'
---

Frozen candidate58a2442e Windows workspace native41370643706 job125040178482 failed TestFeatureExportImportRoundtrip: Alice and Bob/Carol edges absent after import. Bonsai pin48195f21. Only1 of25 native entrypoints failed. Read-only source identifies exportRDF parallel Badger KeyToList callbacks writing directly to shared io.Writer, including multiple writes per scalar triple; roundtrip destination is bytes.Buffer. Establish bounded race/deterministic reproduction before repair, serialize complete postings without weakening roundtrip assertions, preserve error reporting and parallel traversal. Full suites hosted; parent controls integration/push.

## Delivery evidence

The existing `TestFeatureExportImportRoundtrip` reproduces the defect under the
race detector: concurrent Badger `KeyToList` callbacks call `emitRDFPosting` and
write to the same `bytes.Buffer`. Badger explicitly documents concurrent callbacks;
scalar RDF records take multiple writes. This can interleave records or lose data
before import. The original bounded race run failed in 1.510s.

The repair serializes each complete RDF posting at the writer boundary while
retaining parallel traversal. Original Alice/Bob/Carol query assertions remain;
the test additionally verifies all six records committed in one error-free batch.
The change does not alter RDF formatting or silently accept partial imports.
The pre-existing lack of writer-error propagation in `emitRDFPosting` is unchanged
and is outside this concurrency repair.

Validation: `GOWORK=off GOMAXPROCS=2 GOFLAGS=-p=2 go test -race ./pkg/bonsai
-run '^TestFeature(ExportImportRoundtrip|ExportNTXDeterministic|ImportStreamProducerErrors)$'
-count=3 -timeout=60s` PASS (3.643s). Original Windows workspace failure remains
recorded until the corrected candidate is independently tested on hosted Windows.
No full local suite or product build was run.
