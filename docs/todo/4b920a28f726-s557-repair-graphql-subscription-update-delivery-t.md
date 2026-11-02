---
id: 4b920a28f726
kind: bug
title: S557 repair GraphQL subscription update delivery timeout
seq: 3
status: wontfix
priority: p1
labels:
    - superseded
created: 2026-11-01T19:27:07.209207Z
weave: 1
assignee: codex-gpt6-sol
sprint: 557
sprint_id: f5ccf563-f5d1-5b94-8be4-9388e51ef86d
sprint_title: 'Bashy v1.0.0 preparation: split private/public development and retire qiangli/dhnt'
closed: 2026-11-02T05:32:10.773065Z
---

Sprint557 migration repair. Work only in assigned isolated workspace. Read local CLAUDE/AGENTS. Preserve all test assertions, coverage, thresholds and deadlines; diagnose before modifying. No release/push/parent pin changes. No heavy/full local builds/suites: bounded targeted tests only GOMAXPROCS=2 GOFLAGS=-p=2, full validation hosted by manager. Commit named files with Sprint: #557 and correct Story/Story-ID trailers and injected BASHY_AGENT. Report exact evidence and limitations. Native run41332670393 Ubuntu workspace TestSubscriptionLifecycle pkg/graphql/subscription_test.go gets connection_ack and Alice, then times out3s awaiting Bob after mutation. Diagnose polling/update/ownership race, preserve3s deadline and assertions. Add deterministic focused regression if needed; no external paid subscription involved.

## Superseded by consolidation — 2026-11-02

Operator requested consolidation of implementation repairs into acceptance owners. Scope, original acceptance, unresolved work and evidence transfer to public umbrella #6, story `bf7624a5c2f2`. This record is closed by decision with label `superseded`; it is NOT a delivery acceptance, test pass, waiver or feature retirement. The parent remains open. Continue corrections under that parent rather than creating another repair story. Original content above is retained unchanged.
