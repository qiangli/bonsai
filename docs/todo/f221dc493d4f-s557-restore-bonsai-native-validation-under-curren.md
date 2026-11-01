---
id: f221dc493d4f
kind: bug
title: S557 restore Bonsai native validation under current Go toolchain
seq: 2
status: assigned
priority: p1
created: 2026-11-01T17:00:37.578413Z
assignee: s557-manager
sprint: 557
sprint_id: f5ccf563-f5d1-5b94-8be4-9388e51ef86d
sprint_title: 'Bashy v1.0.0 preparation: split private/public development and retire qiangli/dhnt'
---

Remote native validation exposed three nonconstant EventLog.Printf format calls in schema and an export/import roundtrip timeout. Repair literal logging and diagnose fixture or runtime deadlock without dropping tests; verify bounded targeted checks and remote native suite. Preserve percent characters in formatted messages.

Implementation: EventLog messages use literal %s formatting. Import producer
always flushes/closes its NQuad stream, including parse/read failures, so the
consumer exits and the original error returns. Targeted export/import and both
producer error regressions pass; schema compilation/vet passes. Hosted complete
native suite remains the acceptance gate.

Windows fixture follow-up: initialization probes use the .exe suffix on Windows;
file discovery expectations use filepath.Join. Log-writer tests use separate
TempDir roots, check initialization/reader/scanner errors, close all readers,
and wait for concurrent writers with a WaitGroup instead of sleeping. The
10000-line assertion and initialization budget are unchanged.
Validation: targeted four log/file tests pass (0.548s); x tests compile for
Windows/amd64. Full native suites remain the manager's hosted acceptance gate.
