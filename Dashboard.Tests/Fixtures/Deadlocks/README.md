# Deadlock graph parser/layout test fixtures

Inputs for `DeadlockGraphParserTests` / `DeadlockGraphLayoutTests` (both exercise the shared
`PerformanceMonitor.Common` deadlock types). Each file is a bare `<deadlock>` element — exactly what
both apps hand the parser at runtime (Lite `deadlock_graph_xml` and Dashboard
`collect.deadlocks.deadlock_graph` are both `evt.query('.../value/deadlock')`) — except the Azure
fixture, which keeps its `<event>` wrapper on purpose (see below).

## Real samples — captured from sql2025 `PerformanceMonitor.collect.deadlock_xml` (2026-06-21 HammerDB TPC-C)

The `<stackFrames>` symbol-address dumps were stripped (the parser never reads them); everything the
parser does read — processes, `inputbuf`, `executionStack/frame@procname`, `resource-list`,
`victim-list` — is byte-for-byte the original.

- `deadlock_2proc_real_sql2025.xml` — id 46574. 2 processes (spid 85, 103), 1 victim (103). One
  2-cycle: 103 →(new_order) 85, 85 →(orders) 103.
- `deadlock_5proc_real_sql2025.xml` — id 46559. 5 processes (80,88,97,103,108), 2 victims (88,108).
  **Two disjoint cycles**: a 3-cycle {80→103→108→80} and a 2-cycle {88→97→88}. One victim per cycle.
- `deadlock_8proc_multivictim_real_sql2025.xml` — id 75970. 8 processes
  (105,107,114,122,123,125,135,139), 4 victims (107,114,122,135). **Four disjoint 2-cycles**:
  {105,122}, {107,123}, {114,125}, {135,139}. One victim per cycle.

Key real-world finding that shaped the layout: in this workload a single "N-process deadlock" is a
**bundle of independent small cycles** (mostly 2- and 3-cycles), NOT one big N-cycle. Every 5-proc
deadlock sampled was a 3-cycle + 2-cycle; the 8-proc was four 2-cycles. So the layout detects
connected components and lays each out as its own ring, tiled — a single global ring would render the
disjoint cycles as meaningless chords.

## Synthetic samples — for cases this workload never produced

- `deadlock_parallel_selfedge_synthetic.xml` — intra-query parallel deadlock (two ECIDs of spid 55).
  `exchangeEvent` resources with no `objectname`/`mode`; includes one exchangeEvent whose owner and
  waiter are the **same** process (a self-edge). Exercises `IsParallel`, the "Parallelism" label
  fallback, empty modes, and the layout's self-loop tolerance.
- `deadlock_azure_database_report_synthetic.xml` — Azure SQL DB `database_xml_deadlock_report` event
  wrapper (the only cloud difference is the XE event *name*; the inner `<deadlock>` is identical).
  Proves the parser is wrapper-agnostic (anchors on `Descendants("deadlock")`). Its `keylock`s carry
  **no `objectname`** → exercises the `KEY <hobtid>` label fallback.
- `deadlock_crossproduct_synthetic.xml` — one `objectlock` with 2 owners (201,202) and 2 waiters
  (203,204) → the parser emits the 2×2 = 4 cross-product waiter→owner edges.
- `deadlock_single5cycle_synthetic.xml` — a true single 5-process simple cycle
  (301→302→303→304→305→301). Exercises the single-ring layout path that the bundled real samples
  don't.
