---
id: 4b920a28f726
kind: bug
title: S557 repair GraphQL subscription update delivery timeout
seq: 3
status: assigned
priority: p1
created: 2026-11-01T19:27:07.209207Z
weave: 1
assignee: codex-gpt6-sol
sprint: 557
sprint_id: f5ccf563-f5d1-5b94-8be4-9388e51ef86d
sprint_title: 'Bashy v1.0.0 preparation: split private/public development and retire qiangli/dhnt'
---

Sprint557 migration repair. Work only in assigned isolated workspace. Read local CLAUDE/AGENTS. Preserve all test assertions, coverage, thresholds and deadlines; diagnose before modifying. No release/push/parent pin changes. No heavy/full local builds/suites: bounded targeted tests only GOMAXPROCS=2 GOFLAGS=-p=2, full validation hosted by manager. Commit named files with Sprint: #557 and correct Story/Story-ID trailers and injected BASHY_AGENT. Report exact evidence and limitations. Native run41332670393 Ubuntu workspace TestSubscriptionLifecycle pkg/graphql/subscription_test.go gets connection_ack and Alice, then times out3s awaiting Bob after mutation. Diagnose polling/update/ownership race, preserve3s deadline and assertions. Add deterministic focused regression if needed; no external paid subscription involved.
