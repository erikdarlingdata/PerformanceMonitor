/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The single-sourced cap on how large ONE row's captured cached-plan XML (query_stats /
/// procedure_stats, via <c>sys.dm_exec_text_query_plan</c>) is allowed to be before the collector
/// ships NULL for that row instead of the XML.
///
/// <para><b>Why this exists:</b> both collectors already bound row COUNT with <c>TOP (200)</c>, but
/// nothing bounded the SIZE of any one row's plan XML — and <c>sys.dm_exec_text_query_plan</c> can
/// return a document that is multiple megabytes for a sufficiently complex plan. Measured on
/// <c>ayr-01</c> (the fleet's outlier for this collector's tail): a cycle's <c>open_ms</c> — the
/// server-side compile-and-first-row cost — was 1,168ms, while <c>drain_ms</c> — reading the
/// remaining rows off the wire — was 120,096ms for the SAME 200-row cap other servers clear in low
/// single-digit seconds. That split points at PAYLOAD SIZE, not row count or server-side execution:
/// the query already returns fast, the client then spends two minutes reading it back. A handful of
/// oversized plan-XML rows in a 200-row batch is enough to produce exactly this shape, and #2673's
/// wall-clock budget (<see cref="QueryStatsCollector.PerItemWallClockBudget"/>) already accepts that
/// framing — it exists because "the 60s command timeout bounds only execution, not the drain of a
/// large ... result".
///
/// <para>The cap is a per-ROW gate, not a running-total budget like
/// <see cref="QueryStoreCollector.MaxTextBytesPerDatabase"/> — that collector has no row cap of its
/// own, so it needs a budget that decides how many whole items fit. Here the row count is already
/// bounded at 200, so a flat per-row ceiling is the simpler instrument and does not depend on the
/// byte-size ordering of an otherwise duration-ranked result set. It does NOT assume the cost is one
/// or two catastrophic outliers, though — see the measurement below, which found something flatter.</para>
///
/// <para>NULL, not a placeholder string, for the same reason <see cref="QueryStorePlanMap"/>'s
/// content-less marker rows are NULL rather than a synthesized value (#2312): an omitted-for-size
/// plan must read as "not captured", never as real content, or a reader downstream cannot tell the
/// two apart. The row's own numeric columns (CPU, reads, execution count, …) are still captured in
/// full — only the XML projection is gated — so an oversized plan costs its plan-view fetch, not its
/// place in the ranking or its resource accounting.</para>
///
/// <para>This NULL is permanent, not "not yet": a plan over the cap ships NULL every cycle it recurs,
/// with no path back to its content today. #3392 proposes eventually collecting these — a small backlog
/// table plus a low-frequency, deliberately one-plan-at-a-time sweep, keyed on the measured finding that
/// this fleet's over-cap plans are long-lived cache residents (13.6 hours to 144.6 days observed on
/// ayr-01, all `Proc`-grain, multi-million execution counts) rather than the volatile handles a naive
/// reading of <c>sys.dm_exec_query_stats.plan_handle</c> would assume — unbuilt as of this cap shipping.</para>
///
/// <para><b>512 KB is a measured choice, not a guess — a first attempt at 2 MB was.</b> Live on
/// ayr-01 (2026-09-12), the actual 200-row candidate set for one cycle carried 27.08 MB of plan XML.
/// The distribution is heavy-tailed, not one or two catastrophic outliers: only 2 of 200 rows exceed
/// 2 MB (24% of the total bytes), but the top 10 of 200 (5% of rows) carry 56% of the total, median
/// plan size is 27,982 bytes, and a 2 MB cap left 76% of the payload — and correspondingly little of
/// the drain time — untouched. Cap sweep on that same captured distribution: 2 MB catches 24% of
/// bytes (2 rows), 1 MB catches 47% (6 rows), 512 KB catches 56% (10 rows), 256 KB catches 67% (18
/// rows), 128 KB catches 82% (39 rows, 20% of all rows — the point past which the cap starts costing
/// visibility into plans that are large but not pathological, not just the tail). 512 KB is the knee:
/// it takes more than half the bytes while still touching only 5% of rows, each individually well
/// above the ~28 KB median.
///
/// <para>Timed end to end against that same server, real query, four trials each, guard on vs off:
/// unguarded 1.48-2.68s (mean 1.98s, ~13.5 MB shipped) against guarded-at-512KB 1.12-1.40s (mean
/// 1.26s, ~5.9 MB shipped) — every guarded trial faster than every unguarded trial, a ~36% wall-clock
/// reduction tracking the ~56% byte reduction (the gap is the fixed per-query cost — connection,
/// compile — that does not shrink with the payload). That is from a client on the public internet,
/// not the collector's own same-region path, so the absolute seconds do not transfer; the relative
/// reduction, driven by bytes moved rather than by that path, should.</para>
///
/// <para><b>Why the cap's win is bigger than "fewer bytes on the wire" — the client, not the network
/// or the target, is where the time actually goes.</b> ayr-01's own production history (78 real
/// <c>query_stats</c>/<c>procedure_stats</c> stall-probe samples, 2026-09-11 through 2026-09-13, all
/// firing per <see cref="QueryStatsCollector.PerItemWallClockBudget"/> at ~30s in) shows every single
/// stall with a QUIET target: <c>runnable_tasks</c> in the single digits, <c>work_queue_length: 0</c>,
/// <c>pending_disk_io</c> 0 on all but two samples — no scheduler pressure, no IO queue, no lock wait
/// leading the sample. CPU utilization across three of these events (2026-09-13, ayr-01) stayed flat in
/// the same 10-35% band the server runs at all the time, no spike aligned to any of them, even though
/// bytes read for those three events were statistically IDENTICAL (27.09-28.09 MB) to a same-night
/// 26-second run of the same query on the same server. Same bytes, same idle target, 3-5x duration
/// spread: the difference is not in what the target does or how much data crosses the wire, it is in
/// how fast the CLIENT drains what already arrived. <c>DarlingCollectorRunner</c> opens every
/// reader with <c>CommandBehavior.Default</c> — no
/// <c>CommandBehavior.SequentialAccess</c> — and this collector's <c>ReadAsync</c> calls
/// <c>reader.GetString</c> on the plan-XML column, which always materializes the ENTIRE value as one
/// managed string regardless of behavior; at pre-cap sizes (single rows up to several MB, 200 of them
/// per cycle) that is a steady stream of Large-Object-Heap allocations, and a stop-the-world Gen2/LOH
/// collection pauses every managed thread in the process — not just the one reading this row. That
/// last part is independently corroborated: #2880's own out-of-band-watchdog investigation found "the
/// four cheapest collectors' open_ms ran 3x to 152x their own baselines in the body before each
/// abandoned run" — i.e. OTHER collectors' brand-new, unrelated <c>ExecuteReaderAsync</c> calls stalled
/// in the same window, on different connections, different servers in the same sweep. A per-connection
/// network or target-side problem cannot do that; a process-wide GC pause can. From the target's side
/// this reads as exactly what the stall probes show: the client stops calling <c>Read()</c>, SQL
/// Server's send buffer fills, the session waits on <c>ASYNC_NETWORK_IO</c> (the #2 wait by total time
/// fleet-wide the same night), and nothing about the target's own scheduler, IO or locks ever moves.
///
/// <para>This is why the fix here is a BYTE cap and not <c>CommandBehavior.SequentialAccess</c>: adding
/// sequential access alone would not help — it changes how many columns' raw buffers the provider must
/// hold at once, not what a bare <c>GetString</c> call does with the column it is actually reading, so
/// the large single-string LOH allocation this collector is exposed to would be unchanged. What
/// actually caps LOH allocation size is capping the VALUE, which is what <see
/// cref="MaxCapturedPlanXmlBytes"/> already does — this paragraph documents why that is the right lever
/// and not merely a byte-transfer optimization that happened to also help. A true fix for the
/// allocation itself (streaming the XML via <c>GetChars</c>/<c>GetStream</c> instead of materializing a
/// string) would remove the LOH pressure at any cap size, but changes the store-write path's contract
/// (a <c>string?</c> today) and is unbuilt — the per-row cap plus <see
/// cref="QueryStatsCollector.PerItemWallClockBudget"/>'s abandon-and-retry-next-cycle safety net are
/// what is shipped and measured today.</para>
/// </summary>
public static class QueryPlanXmlCaptureLimits
{
    public const int MaxCapturedPlanXmlBytes = 512 * 1024;
}
