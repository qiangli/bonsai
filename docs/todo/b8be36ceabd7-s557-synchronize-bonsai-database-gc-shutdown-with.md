---
id: b8be36ceabd7
kind: bug
title: S557 synchronize Bonsai database GC shutdown with Close
seq: 4
status: assigned
priority: p1
created: 2026-11-01T22:02:22.710915Z
assignee: s557-test-readiness
sprint: 557
sprint_id: f5ccf563-f5d1-5b94-8be4-9388e51ef86d
sprint_title: 'Bashy v1.0.0 preparation: split private/public development and retire qiangli/dhnt'
---

Root independent race gate for bf567b2 detects DB.Close db.go:362 write versus valueLogGCLoop db.go:266 read in GraphQL subscription tests. Diagnose and fix lifecycle safely; do not disable race or suppress GC. Repro GOWORK=off GOMAXPROCS=2 GOFLAGS=-p=2 go test -race -count=3 -timeout=3m -run '^TestSubscription(MutationDuringInitialSend|Lifecycle|ConnectionAck)$' ./pkg/graphql in /Users/qiangli/.bashy/weave/bonsai-19397c96/workspaces/issue-1. Worker submitted, noactivewrites. Workisolatedclone/worktreefrombf567b2 preservingexternalpatch. AddmeaningfuldeterministicClose/GCtest ifneeded, nofullsuiteslocal. CommitnamedfileswithSprint557trailers, nopush.

## Implementation

Based on external subscription fix bf567b2, preserving its source and tests.
The existing Close path closed gcStop and then assigned nil while the GC
worker concurrently read that field. It also did not wait for the worker
before tearing down the posting/Badger store. Keep both GC lifecycle channels
immutable after Open, acknowledge worker exit with gcDone, and wait for that
acknowledgment before teardown. Check shutdown between value-log rewrites so
a successful rewrite does not indefinitely defer a pending stop request.

New focused lifecycle cases cover close before the first tick, frequent ticks,
disabled GC, repeated close, and a shutdown signal delivered before worker
startup. No GC disabling, race suppression, threshold changes, or subscription
patch changes are introduced.

Validation passed with GOWORK=off GOMAXPROCS=2 GOFLAGS=-p=2:

```
go test -race -count=3 -timeout=3m \
  -run '^(TestSubscription(MutationDuringInitialSend|Lifecycle|ConnectionAck)|TestCloseJoinsValueLogGC|TestValueLogGCObservesStopBeforeStart|TestOpenClose)$' \
  ./pkg/graphql ./pkg/bonsai
```

GraphQL passed in 5.013s and Bonsai lifecycle checks passed in 2.873s.
Full suites and canonical integration remain the manager's responsibility.
