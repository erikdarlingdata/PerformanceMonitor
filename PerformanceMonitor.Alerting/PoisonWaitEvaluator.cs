/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// The Poison Wait alert's threshold shape, shared by both engines (#3539 A4), plus the SQL Server
/// evaluation that consumes it. <see cref="PostgresAlertEvaluator.EvaluatePoisonWait"/> is the PostgreSQL
/// half; its three constants alias the ones here so the two engines cannot drift.
///
/// <para><b>Why one shape.</b> "Poison Wait" is ONE alert name on both engines — one
/// <see cref="IAlertEngineSettings.PoisonWaitEnabled"/> switch, one mute key, one history filter — and
/// until #3539 it meant two different things: PostgreSQL fired on accumulated wait time over a ten-minute
/// window, graded Warning/Critical (#2711), while SQL Server fired presence-flat CRITICAL when ONE
/// collector delta row's avg-ms-per-wait crossed 500 ms, with no volume floor. The SQL Server shape had
/// both failure modes the accumulation shape was designed against: a single 600 ms wait paged CRITICAL,
/// and a THREADPOOL storm of thousands of short waits — 500 seconds of starvation per minute — never moved
/// the average and slept. A mute rule written against one meaning silently governed the other.</para>
///
/// <para><b>Measured on the SQL Server side before the port</b> (one production store class, 43 primaries,
/// 4 days of wait_stats, 24,757 server-ten-minute buckets): the worst bucket ANYWHERE accumulated 5,795 ms
/// of THREADPOOL — 0.0097 average waiters against this class's 1.0 Warning bar, so the bar sits ~100x
/// above the worst healthy bucket on that population, the same order as the ~160x margin the #2711
/// research measured for the PostgreSQL events. RESOURCE_SEMAPHORE accrued zero. The old avg &gt;= 500 ms
/// trigger fired on no row in those four days, but its near-misses show both wrong shapes: single
/// THREADPOOL rows at 247.5 ms over 2 tasks and 163 ms over 1 task (one slow wait, which a 600 ms sibling
/// would have paged CRITICAL), and the largest row at 703 tasks averaging 8.2 ms (the storm shape an
/// average cannot see). Rows with <c>delta_wait_time_ms &gt; 0</c> and <c>delta_waiting_tasks = 0</c> — a
/// task still waiting across the interval boundary — were dropped by the old read's task filter and are
/// summed by this one.</para>
///
/// <para><b>No engine-specific reason to diverge was found</b>, so the constants are shared rather than
/// merely equal. THREADPOOL, RESOURCE_SEMAPHORE and RESOURCE_SEMAPHORE_QUERY_COMPILE share the trait the
/// PostgreSQL pair was chosen for — near-zero in healthy operation — and "one task continuously starved
/// for the whole window" is the same categorical statement about a SQL Server as "one backend continuously
/// stuck" is about a PostgreSQL server. If a future measurement shows a SQL Server wait type that needs its
/// own bar, it gets its own documented constant here, beside these, not a silent local number.</para>
/// </summary>
public static class PoisonWaitEvaluator
{
    /// <summary>
    /// The evaluation window on both engines. The read side sums every collector delta whose
    /// collection_time falls inside it; the evaluator divides by the window's WALL CLOCK, so partial
    /// coverage (service just started, collector gap) undercounts and therefore under-fires — the correct
    /// failure direction for an alert that pages. Ten minutes was the SQL Server read's own recency window
    /// before #3539, and the PostgreSQL twin adopted it in #2711 so the two engines answer over one horizon.
    /// </summary>
    public const int WindowMinutes = 10;

    /// <summary>The window as milliseconds — the denominator every "average waiters" figure divides by.</summary>
    public const double WindowMs = WindowMinutes * 60_000.0;

    /// <summary>
    /// Warning fires when accumulated wait time averages one task (SQL Server) or backend (PostgreSQL)
    /// continuously stuck across the whole window — 600 seconds of wait per 10 minutes.
    /// <para>PostgreSQL calibration (#2711): the WORST server observed averaged ~0.006 concurrently-waiting
    /// backends on IPC:BtreePage over 24h (538,850 ms / 86,400 s), so the bar sits ~160x above the worst
    /// healthy baseline seen anywhere on that fleet.</para>
    /// <para>SQL Server calibration (#3539): the worst ten-minute bucket across 43 servers and 4 days held
    /// 5,795 ms of THREADPOOL — 0.0097 average waiters — so the same bar sits ~100x above the worst healthy
    /// bucket on that population. Nothing measured on either engine would have fired it, which is the
    /// point: these waits are near-zero when healthy, and a full task pinned for ten straight minutes is
    /// categorically not that.</para>
    /// </summary>
    public const double WarningAvgWaiters = 1.0;

    /// <summary>
    /// Critical at ten tasks/backends continuously stuck on average — an active contention collapse,
    /// where the pile-up itself is throttling throughput rather than merely taxing it. Same figure on both
    /// engines for the reason the class comment gives.
    /// </summary>
    public const double CriticalAvgWaiters = 10.0;

    /// <summary>
    /// The wait types the SQL Server alert watches, in the store's spelling. All three share one defining
    /// trait — near-zero in healthy operation, so any sustained accumulation is inherently abnormal — which
    /// is what makes one accumulation bar meaningful across them.
    /// </summary>
    public static readonly IReadOnlyList<string> SqlServerWaitTypes = new[]
    {
        "THREADPOOL",
        "RESOURCE_SEMAPHORE",
        "RESOURCE_SEMAPHORE_QUERY_COMPILE",
    };

    /// <summary>Accumulated wait normalized to the window: "how many were continuously stuck, on average".</summary>
    public static double AvgWaiters(long accumulatedWaitMs) => accumulatedWaitMs / WindowMs;

    /// <summary>
    /// The shared grade: null under the Warning bar, Warning at or above it, Critical at or above
    /// <see cref="CriticalAvgWaiters"/>. Exactly-at fires; one millisecond below does not — the PostgreSQL
    /// evaluator's boundary rule, pinned there and here against the same figures.
    /// </summary>
    public static AlertSeverityLevel? Grade(long accumulatedWaitMs)
    {
        var avgWaiters = AvgWaiters(accumulatedWaitMs);
        if (avgWaiters < WarningAvgWaiters)
        {
            return null;
        }

        return avgWaiters >= CriticalAvgWaiters ? AlertSeverityLevel.Critical : AlertSeverityLevel.Warning;
    }

    /// <summary>
    /// One SQL Server poison wait type over the bar, with the figures and prose the engine fires with.
    /// The strings live here rather than in the engine so the two engines' "Poison Wait" alerts read
    /// alike — same clauses, same order, same units — with only the engine's noun differing (a SQL Server
    /// "task" waits for a worker or a memory grant; a PostgreSQL "backend" waits on a page).
    /// </summary>
    /// <param name="WaitType">The wait type, as the mute-rule and history dimension.</param>
    /// <param name="Severity">Warning or Critical per <see cref="Grade"/>.</param>
    /// <param name="AccumulatedWaitMs">The window's summed wait, the numeric history value (ms — the
    /// history formatter's existing unit for this metric).</param>
    /// <param name="AccumulatedWaits">Waits completed inside the window, display only.</param>
    /// <param name="AvgWaiters">The normalized figure the bars are stated in.</param>
    /// <param name="BreachedAvgWaiters">Whichever bar was crossed, so the threshold text names it.</param>
    /// <param name="NewestCollectionTime">For the #2704 guard — the collector's clock.</param>
    public sealed record SqlServerFinding(
        string WaitType,
        AlertSeverityLevel Severity,
        long AccumulatedWaitMs,
        long AccumulatedWaits,
        double AvgWaiters,
        double BreachedAvgWaiters,
        DateTime NewestCollectionTime)
    {
        /// <summary>Whole seconds of wait — the unit the prose speaks in.</summary>
        public long AccumulatedSeconds => AccumulatedWaitMs / 1000;

        /// <summary>The per-type clause of the alert's CurrentValue: "THREADPOOL (612s in 10m)".</summary>
        public string CurrentValueClause => string.Create(CultureInfo.InvariantCulture,
            $"{WaitType} ({AccumulatedSeconds:N0}s in {WindowMinutes}m)");

        /// <summary>The bar that was crossed, in the PostgreSQL twin's exact clause order.</summary>
        public string ThresholdValue => string.Create(CultureInfo.InvariantCulture,
            $"{BreachedAvgWaiters * WindowMinutes * 60:N0}s accumulated over {WindowMinutes}m "
            + $"(an average of {BreachedAvgWaiters:0.#} task(s) continuously waiting)");

        /// <summary>The numeric twin of <see cref="ThresholdValue"/>: the breached bar as milliseconds.</summary>
        public double NumericThresholdValue => BreachedAvgWaiters * WindowMs;

        /// <summary>
        /// The toast/one-line body, server-name prefix excluded per the engine's contract. The remedy is
        /// deliberately NOT appended here, unlike the PostgreSQL finding's ShortMessage: Lite renders this
        /// string as a tray balloon, which truncates, so the remedy rides in the detail item instead.
        /// </summary>
        public string ShortMessage => string.Create(CultureInfo.InvariantCulture,
            $"[{WaitType}] {AccumulatedSeconds:N0}s of wait accumulated in the last {WindowMinutes} minutes "
            + $"across {AccumulatedWaits:N0} waits — on average {AvgWaiters:N1} task(s) continuously stuck");
    }

    /// <summary>
    /// The SQL Server Poison Wait evaluation: one finding per wait type over the bar, worst-first (severity,
    /// then accumulated wait). A pure function of the rows — no I/O, no state — exactly like
    /// <see cref="PostgresAlertEvaluator.EvaluatePoisonWaits"/>; the engine owns the active flag, the
    /// cooldown and the #2704 guard.
    /// <para>PER WAIT TYPE, not summed across the poison set: THREADPOOL (worker starvation),
    /// RESOURCE_SEMAPHORE (memory-grant queueing) and RESOURCE_SEMAPHORE_QUERY_COMPILE (compile-memory
    /// queueing) are different incidents with different remedies. The conservative consequence — two types
    /// each just under the bar do not fire — is accepted for the reason the PostgreSQL twin gives: an alert
    /// neither earns is worse than one arriving a window later.</para>
    /// <para>An empty input is the healthy case AND the collector-silent case; telling them apart is the
    /// engine's job (it has the list, and <see cref="PoisonWaitAccumulation.ObservedIntervals"/>), not this
    /// function's, which only says what is over the bar.</para>
    /// </summary>
    public static List<SqlServerFinding> EvaluateSqlServer(IReadOnlyList<PoisonWaitAccumulation>? rows)
    {
        var findings = new List<SqlServerFinding>();
        if (rows is null)
        {
            return findings;
        }

        foreach (var row in rows)
        {
            var finding = EvaluateSqlServer(row);
            if (finding is not null)
            {
                findings.Add(finding);
            }
        }

        findings.Sort((a, b) =>
        {
            var bySeverity = b.Severity.CompareTo(a.Severity);
            return bySeverity != 0 ? bySeverity : b.AccumulatedWaitMs.CompareTo(a.AccumulatedWaitMs);
        });
        return findings;
    }

    /// <summary>One wait type against the shared bars; null when under the Warning bar.</summary>
    public static SqlServerFinding? EvaluateSqlServer(PoisonWaitAccumulation row)
    {
        ArgumentNullException.ThrowIfNull(row);

        var severity = Grade(row.AccumulatedWaitMs);
        if (severity is null)
        {
            return null;
        }

        return new SqlServerFinding(
            row.WaitType,
            severity.Value,
            row.AccumulatedWaitMs,
            row.AccumulatedWaits,
            AvgWaiters(row.AccumulatedWaitMs),
            severity == AlertSeverityLevel.Critical ? CriticalAvgWaiters : WarningAvgWaiters,
            row.NewestCollectionTime);
    }

    /// <summary>
    /// The SQL Server sibling of <see cref="PostgresAlertEvaluator.PoisonWaitRemedyFor"/>: the three poison
    /// types present identically as "everything got slow at once" and have completely different fixes, so
    /// the alert carries the fix. The descriptions are the README's own one-line characterizations of each
    /// type (worker thread exhaustion / memory grant pressure / compilation memory pressure), expanded to
    /// what to look at. Matched case-insensitively; the store spells them upper-case but a mute rule or a
    /// hand-typed filter may not.
    /// </summary>
    public static string SqlServerRemedyFor(string? waitType) => waitType?.ToUpperInvariant() switch
    {
        "THREADPOOL" => "Tasks are queueing for a worker thread — the server has run out of workers, usually "
            + "because blocking or parallelism is holding the ones it has. Look at the blocking and active-query "
            + "snapshots for this window before touching max worker threads; adding workers to a blocked pile "
            + "adds to the pile.",
        "RESOURCE_SEMAPHORE" => "Queries are queueing for memory grants — the grants in flight have consumed the "
            + "query memory budget and new queries wait for one to finish. Look at the memory-grant snapshots "
            + "for the largest requested grants and the plans that asked for them (sorts, hashes, "
            + "over-estimated row counts).",
        "RESOURCE_SEMAPHORE_QUERY_COMPILE" => "Queries are queueing for compile memory — too many large plans are "
            + "compiling at once, typically a burst of non-parameterized or freshly-evicted statements. Look at "
            + "the compile and recompile rates and at the statement shapes that are compiling in this window.",
        _ => "Sample the active-query snapshots for the sessions in this wait type.",
    };
}
