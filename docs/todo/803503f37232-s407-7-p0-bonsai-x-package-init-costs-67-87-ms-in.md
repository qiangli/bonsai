---
id: 803503f37232
kind: bug
title: 'S407.7 P0: bonsai/x package init costs 67-87 ms in every bashy process (metrics registration + health goroutine + zap logger at init); make it lazy'
seq: 1
status: done
priority: p0
labels:
    - performance
created: 2026-10-25T04:05:57.021821Z
weave: 2
assignee: claude-sonnet5.5
sprint: 407
sprint_id: 90546935-fbc2-5d09-94b0-19c380846681
sprint_title: bashy 1.0.0 release gates pending external dependencies (follow-up to Sprint 379)
closed: 2026-10-25T04:36:43.329663Z
resolution: fixed
closed_by: claude-sonnet5.5-b
---

Found 2026-10-25 (operator: fix the Darwin startup cost too). Measured with GODEBUG=inittrace=1 on macOS arm64:
- released v1.0.0-rc.14-dev bashy-darwin-arm64: total package init 96.5 ms over 927 packages; github.com/qiangli/bonsai/x = 67 ms (go-runewidth 14 ms is handled separately by S407.6 621f736b).
- local dev build of bashy 19ae35e3: total 123.8 ms; bonsai/x = 87 ms. 'bashy -c true' 0.11-0.15 s real vs /bin/sh ~0.
- Linux cert builds (tag bashy_cert_base, 335 pkgs) do not link bonsai, but regular Linux/macOS/Windows releases (927 pkgs) do, so every bashy exec on those pays it.
Suspects in bonsai/x: metrics.go init() (expvar map, MetricsContext, and a goroutine with a 5 s health ticker started in EVERY importing process), package-level opencensus/otel measure + view vars, logger.go init() building a zap production logger, plus any package-level regexp/table vars.

Do (in the bonsai repo):
1. Red first: a test that runs a tiny program importing bonsai/x with GODEBUG=inittrace=1 and fails if bonsai/x init > 2 ms; and a test that no goroutine is started by import alone (runtime.NumGoroutine before/after a blank import program).
2. Make everything lazy: move metric/view registration, the health ticker goroutine and the slow-operation logger behind sync.Once accessors called from the server/DB paths that actually need them. No behaviour change for the graph DB itself; its existing tests stay green (go test -short ./...).
3. Find any other bonsai or gfy package in bashy's import graph with init > 1 ms and apply the same treatment.
4. Report before/after inittrace numbers for bonsai/x and for a bashy build that replaces bonsai with your commit (GOWORK=off with a local replace is fine for measurement; do not push bashy).
Deliver: pushed bonsai commit SHA(s) on bonsai main with Sprint/Story/Story-ID trailers; the conductor bumps pins in consumers (yoke/bashy) after announcing. Do not edit bashy, yoke or ycode.
