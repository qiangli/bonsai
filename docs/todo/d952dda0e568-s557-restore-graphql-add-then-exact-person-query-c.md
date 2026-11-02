---
id: d952dda0e568
kind: bug
title: S557 restore GraphQL add then exact-person query contract
seq: 5
status: wontfix
priority: p1
labels:
    - superseded
created: 2026-11-01T23:54:32.694774Z
assignee: s557-manager
sprint: 557
sprint_id: f5ccf563-f5d1-5b94-8be4-9388e51ef86d
sprint_title: 'Bashy v1.0.0 preparation: split private/public development and retire qiangli/dhnt'
closed: 2026-11-02T05:32:10.621905Z
---

Native41359794189 allcompletedOS modes TestGraphQLAddAndQuery handler_test.go79 getPersonmissingAliceemptyresults. Exactcandidate8baad698. Diagnose realpostaddID/query semantics andtestisolation, preserve assertions no retries/skips. Logs /tmp/s557-native-125007549282.log and Windows125007549238. Boundedsmalltestonly, nofullsuite local.

Delivery: the test queried hard-coded UID 0x1 even though worker UID allocation
is process-wide. The original test fails in a two-repetition focused run after
the first database has consumed that UID. Validate the mutation's returned UID
as a nonzero hexadecimal identifier and query that exact identity. Retain all
Alice/name/age and error assertions; no production behavior changes.

Independent bounded verification: original count=2 reproduces the empty result;
updated add/query plus subscription regression tests pass with race detection,
count=3, timeout=60s (4.820s). Full hosted native acceptance remains pending.

## Superseded by consolidation — 2026-11-02

Operator requested consolidation of implementation repairs into acceptance owners. Scope, original acceptance, unresolved work and evidence transfer to public umbrella #6, story `bf7624a5c2f2`. This record is closed by decision with label `superseded`; it is NOT a delivery acceptance, test pass, waiver or feature retirement. The parent remains open. Continue corrections under that parent rather than creating another repair story. Original content above is retained unchanged.
