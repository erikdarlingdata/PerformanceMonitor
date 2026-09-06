/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// What the client knows about a server-scoped read that is still in flight, sampled by
/// <see cref="StallProbeArm"/> to decide whether the target is worth asking about (#2880).
/// </summary>
/// <param name="ElapsedMs">
/// Milliseconds since <c>ExecuteReaderAsync</c> was issued — the OPEN and the DRAIN together, on one clock.
/// Deliberately not the drain alone: the #2880 forensics found the four cheapest collectors' <c>open_ms</c>
/// degraded 3x to 152x in the sweep body before each abandoned run, so a stall can present in either phase
/// and a trigger scoped to the drain would miss the one that presents at execute.
/// </param>
/// <param name="RowsRead">
/// Rows the counting reader has handed the collector so far, or <c>-1</c> before the reader exists (the read
/// is still inside <c>ExecuteReaderAsync</c>). Never <c>rows_collected</c>, which is rows STORED and is 0 on
/// any abandoned cycle by definition — the misreading the whole of #2880's second half is about.
/// </param>
/// <param name="BytesRead">
/// String/binary payload bytes delivered so far (<see cref="DrainCountingDataReader.PayloadBytes"/>), or
/// <c>-1</c> before the reader exists. UTF-16 off the string getters, not wire size, for that property's
/// documented reason.
/// </param>
/// <param name="LastReadMs">
/// The drain clock at the last successful read, or <c>-1</c> when no row has arrived.
/// <para><b>Recorded, and deliberately NOT part of the firing decision.</b> See
/// <see cref="StallWaitProbePolicy.Decide"/> — the measured failure mode has 0-3 ms of terminal silence on 8
/// of 8 abandoned runs, so any condition keyed on this being LARGE would never fire on the actual defect. It
/// rides along because the row that records a probe should also record what the client believed at the
/// moment it fired.</para>
/// </param>
public readonly record struct StallProbeObservation(long ElapsedMs, long RowsRead, long BytesRead, long LastReadMs)
{
    /// <summary>
    /// Milliseconds the reader has sat with nothing arriving, or <c>-1</c> when no row has arrived at all so
    /// the question has no answer yet. <c>drain_ms - last_read_ms</c>, the V109 discriminator, computed live.
    /// Reported on the probe row; never gated on.
    /// </summary>
    public long TerminalSilenceMs => LastReadMs < 0 ? -1 : Math.Max(0, ElapsedMs - LastReadMs);

    /// <summary>
    /// Delivered bytes per second over the whole in-flight read, or <c>-1</c> when nothing has been measured
    /// yet (no reader, or no elapsed time to divide by). This is the rate the #2880 measurement separates the
    /// two populations on: 0.21-0.24 MB/s on abandoned runs against 11.3-14.0 MB/s on successful runs of the
    /// same collectors carrying the same payload volume.
    /// </summary>
    public double BytesPerSecond =>
        BytesRead < 0 || ElapsedMs <= 0 ? -1 : BytesRead * 1000.0 / ElapsedMs;
}

/// <summary>
/// Whether to spend an out-of-band probe, and the reason — one value so a caller cannot read the verdict
/// without the reason it can log, and cannot log a reason that disagrees with the verdict.
/// </summary>
public readonly record struct StallProbeDecision(bool Fire, string Reason);

/// <summary>One wait type's share of a server-wide sample.</summary>
public readonly record struct StallWaitRow(string WaitType, long WaitingTasks, long TotalWaitMs, long MaxWaitMs);

/// <summary>
/// One out-of-band, server-wide wait sample: what the whole monitored instance was waiting on and how loaded
/// its schedulers were, at one instant inside a collector stall.
/// </summary>
/// <param name="WaitingTaskCount">
/// Every waiting task on the instance except the probe's own, summed before the top-N cut, so the headline
/// count is not the top five's subtotal.
/// </param>
/// <param name="DistinctWaitTypes">How many distinct wait types those tasks span, again before the cut.</param>
/// <param name="TopWaitType">
/// The wait type holding the most total wait time, or <c>null</c> when nothing on the instance was waiting.
/// A NULL here beside a positive <paramref name="SchedulerCount"/> is a real finding — an instance answering
/// a trivial query in milliseconds, producing rows 50x slowly, and waiting on nothing.
/// </param>
/// <param name="SchedulerCount">
/// <c>VISIBLE ONLINE</c> schedulers. <b>This is the sample's own denominator</b>, and it is why an empty wait
/// list is readable. Every live SQL Server instance has at least one, so zero cannot mean "quiet" — it means
/// the result set did not describe an instance, and <see cref="StallWaitProbePolicy.OutcomeNoSample"/> says so
/// rather than storing a row of zeros that reads as a measured all-clear.
/// </param>
public sealed record StallWaitSample(
    long WaitingTaskCount,
    int DistinctWaitTypes,
    string? TopWaitType,
    long TopWaitTotalMs,
    long TopWaitMaxMs,
    string? WaitSummary,
    int SchedulerCount,
    long RunnableTasks,
    long WorkQueueLength,
    long PendingDiskIo,
    int MaxRunnableTasks);

/// <summary>
/// The out-of-band server-wide wait sample #2880 asks for: the bounds it runs under, the condition that fires
/// it, the query it runs, and the vocabulary its outcomes are recorded in. Pure — no clock, no connection, no
/// store — so every one of those is pinnable without a host.
///
/// <para><b>Why anything out-of-band is needed at all.</b> Collectors run strictly sequentially per server, so
/// a stalled collector holds the sequence <c>waiting_tasks</c>, <c>dmv_blocking_snapshot</c> and
/// <c>query_snapshots</c> would run in. Measured on one real stall: nothing was observed for four minutes,
/// <c>waiting_tasks</c> ran 2.3 s after it cleared and returned zero rows, and about 20 collectors then
/// completed inside 20 seconds as the backlog drained. The instrument stops sampling exactly when the thing
/// being measured happens, and no amount of client-side recording fixes that.</para>
///
/// <para><b>Why SERVER-WIDE and not the stalled session.</b> #2880 was written around
/// <c>collection_log.target_session_id</c> as a join key, and that key is not populated in production: across
/// 7,929 rows carrying the column, 2,076 are <c>0</c> and 5,853 are <c>null</c>, none positive. A server-wide
/// sample needs no session id — and the evidence says it is the better question anyway. The degradation is
/// producer-side and payload-independent: in the body before each abandoned run the four cheapest collectors
/// (sub-5 KB payloads, <c>drain_ms: 0</c>) showed <c>open_ms</c> degraded 3x to 152x against their own
/// baselines, 9 of 9 across both affected servers. Everything on the instance is slow to produce rows, so our
/// session is one victim among many and its own wait would most likely read <c>ASYNC_NETWORK_IO</c>, which a
/// stalled drain generates by definition and which therefore cannot establish direction.</para>
/// </summary>
public static class StallWaitProbePolicy
{
    /// <summary>
    /// How far into a collector's own wall-clock budget the probe fires, as a fraction of that budget.
    ///
    /// <para><b>A fraction rather than a fixed number of seconds</b>, because the shipped budgets differ by 5x
    /// (120 s on <c>procedure_stats</c> / <c>query_stats</c> / <c>plan_correction</c>, 600 s on
    /// <c>query_store</c>) and a constant tuned against one of them either fires on the other's healthy runs
    /// or never fires at all inside its stalls.</para>
    ///
    /// <para><b>Derived two-sided against the measurement.</b> Above: on a 120 s budget this is 30 s, and the
    /// healthy runs of the two collectors that actually abandon complete in about 2.5 s (median 28.2 MB at
    /// 11.26 MB/s, and 14.0 MB at 13.98 MB/s) — an order of magnitude of headroom, so an ordinary run is over
    /// long before the timer. Below: the observed stall is a roughly 240-second steady state, not a spike, so
    /// firing 30 s in lands well inside it with 90 s of budget still to run, which is what leaves room for
    /// <see cref="HardBudget"/> to fit underneath (see <see cref="FitsUnderBudget"/>).</para>
    /// </summary>
    public const double TriggerFractionOfBudget = 0.25;

    /// <summary>
    /// The delivered-throughput floor, in bytes per second, under which an in-flight read counts as the
    /// measured signature rather than as a big query making honest progress.
    ///
    /// <para><b>Two-sided, from the #2880 throughput table.</b> Above every abandoned rate measured — 0.21 MB/s
    /// median on <c>procedure_stats</c> and 0.24 MB/s on <c>query_stats</c>, so this sits about 4x clear of
    /// the band it must catch. Below every successful median by about 11x — 11.26 MB/s and 13.98 MB/s on the
    /// same two collectors, on payloads of the same size.</para>
    ///
    /// <para><b>Throughput alone is NOT sufficient and this constant does not pretend to be.</b> The two
    /// populations overlap: successful runs reach down to 0.15 MB/s, below the abandoned median. What separates
    /// them is that a slow successful run is also a SHORT one, which is why <see cref="Decide"/> requires the
    /// elapsed floor as well and why neither half is load-bearing on its own.</para>
    /// </summary>
    public const long ThroughputFloorBytesPerSecond = 1_048_576;

    /// <summary>
    /// The probe's whole-operation ceiling: connect, execute, read, and give up. One shot gets this long in
    /// total and is then abandoned as <see cref="OutcomeConnectTimedOut"/> or
    /// <see cref="OutcomeQueryTimedOut"/> depending on which phase it died in.
    ///
    /// <para><b>Sized so the watchdog cannot outlive the stall it exists to explain</b>, which is #2880's own
    /// constraint. On the 120 s budget that actually abandons, firing at 30 s leaves 90 s, so 10 s is a ninth
    /// of the remaining window and a twelfth of the budget being diagnosed.
    /// <see cref="FitsUnderBudget"/> states that as arithmetic rather than as a comment, so raising this past
    /// its headroom is a failing test and not a judgement call.</para>
    ///
    /// <para><b>Generous against what the probe actually has to do.</b> The same forensics measured the four
    /// cheapest collectors completing in 128-448 ms INSIDE a degraded body, and the one open measured on a
    /// stalled connection was 104 ms. Two DMV scans on a fresh connection is that shape of work, so 10 s is
    /// roughly 20x the observed cost of a comparable query on a comparably degraded instance — not a tight
    /// bound the probe is expected to brush against, a ceiling for the case where the instance has stopped
    /// answering at all.</para>
    /// </summary>
    public static readonly TimeSpan HardBudget = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Command timeout for the probe query, in seconds — half of <see cref="HardBudget"/>, so the query has
    /// its own ceiling even where a provider honours its timeout in preference to a cancellation token, and
    /// there is still budget left for the connect phase either way. The token is the guarantee; this is what
    /// keeps a well-behaved provider from needing it.
    ///
    /// <para>There is deliberately no matching connect override. The probe runs on the monitored server's
    /// OWN connection string — the same one the collectors use, so it draws on the same pool rather than
    /// standing up a second one whose idle connections would outlive every probe. Its connect is bounded by
    /// <see cref="HardBudget"/>'s token rather than by a string setting, and what the recorded
    /// <c>connect_ms</c> therefore measures is time-to-usable-connection, which is not the same claim as a
    /// fresh login: under the sequential model the pool's one connection is the one stalling, so in practice
    /// SqlClient opens a new physical connection here, but the probe does not assert that and the column is
    /// not evidence of a login.</para>
    /// </summary>
    public const int CommandTimeoutSeconds = 5;

    /// <summary>How many wait types the sample carries in detail. The rest are counted, never named.</summary>
    public const int TopWaitTypeCount = 5;

    /// <summary>
    /// Character ceiling on <see cref="StallWaitSample.WaitSummary"/>. The summary is a human-read
    /// convenience beside the queryable columns, never the only home of an answer — hence a cap rather than a
    /// widened column.
    /// </summary>
    public const int WaitSummaryMaxLength = 512;

    /// <summary>A server-wide sample was taken and describes a live instance.</summary>
    public const string OutcomeSampled = "SAMPLED";

    /// <summary>
    /// The probe ran and the target answered, but the answer did not describe an instance — no row, or zero
    /// <c>VISIBLE ONLINE</c> schedulers, which no live SQL Server reports. Distinct from
    /// <see cref="OutcomeSampled"/> with an empty wait list, which is a genuine and interesting all-clear.
    /// </summary>
    public const string OutcomeNoSample = "NO_SAMPLE";

    /// <summary>
    /// The probe could not get a usable connection, for a reason that was not its own deadline: a refused
    /// login, an exhausted pool, a network error.
    ///
    /// <para><b>An expected outcome, not an error.</b> Whether a connection can be obtained mid-stall has
    /// never been tested — the one open in evidence (<c>open:104ms</c>) is the stalled collector's OWN open,
    /// taken before the stall — so a probe that cannot connect is a measurement of the thing #2880 says is
    /// unknown, and it is stored as such rather than logged as a fault.</para>
    /// </summary>
    public const string OutcomeConnectFailed = "CONNECT_FAILED";

    /// <summary>The probe's own <see cref="HardBudget"/> expired before the connection was usable.</summary>
    public const string OutcomeConnectTimedOut = "CONNECT_TIMED_OUT";

    /// <summary>The connection opened and the query failed for a reason that was not the probe's deadline.</summary>
    public const string OutcomeQueryFailed = "QUERY_FAILED";

    /// <summary>The connection opened and <see cref="HardBudget"/> expired inside the query.</summary>
    public const string OutcomeQueryTimedOut = "QUERY_TIMED_OUT";

    /// <summary>
    /// Every outcome this probe can record. Named as a set so a read can bucket by explicit list instead of
    /// by complement, the <c>ABANDONED</c> reasoning: a value added later must join no bucket rather than
    /// silently join the wrong one.
    /// </summary>
    public static readonly IReadOnlyList<string> Outcomes = new[]
    {
        OutcomeSampled,
        OutcomeNoSample,
        OutcomeConnectFailed,
        OutcomeConnectTimedOut,
        OutcomeQueryFailed,
        OutcomeQueryTimedOut,
    };

    /// <summary>
    /// When the probe fires, relative to the start of the in-flight read, or <c>null</c> for a collector that
    /// declares no wall-clock budget — which is every collector but the four, and is the reason an unbudgeted
    /// collector is never armed: there is no budget to take a fraction of, so there is no defensible moment.
    /// </summary>
    public static TimeSpan? TriggerElapsedFor(TimeSpan? wallClockBudget) =>
        wallClockBudget is { } budget && budget > TimeSpan.Zero
            ? TimeSpan.FromMilliseconds(budget.TotalMilliseconds * TriggerFractionOfBudget)
            : null;

    /// <summary>
    /// The constraint #2880 states as prose, as arithmetic: a probe fired at
    /// <see cref="TriggerElapsedFor"/> and running its full <see cref="HardBudget"/> must still finish inside
    /// the budget it is diagnosing. False is a design regression — a watchdog outliving its own stall — and it
    /// is asserted for every shipped budget rather than reasoned about per rung.
    /// </summary>
    public static bool FitsUnderBudget(TimeSpan wallClockBudget) =>
        TriggerElapsedFor(wallClockBudget) is { } trigger && trigger + HardBudget <= wallClockBudget;

    /// <summary>
    /// Whether an in-flight server-scoped read has earned one out-of-band sample.
    ///
    /// <para><b>The condition is elapsed-past-the-floor AND delivering below the floor rate.</b> Both halves,
    /// because neither works alone: every abandoned run is long, but so is every legitimately large one
    /// (a measured healthy body carried 71,977 ms for 12,557 rows beside peers at 1 ms), and every abandoned
    /// run is slow, but successful runs reach 0.15 MB/s too — they are just short. The conjunction is what
    /// separates them, and <see cref="TriggerFractionOfBudget"/> and
    /// <see cref="ThroughputFloorBytesPerSecond"/> each carry their own two-sided derivation.</para>
    ///
    /// <para><b>Terminal silence is not consulted, and that is the point.</b>
    /// <see cref="StallProbeObservation.LastReadMs"/> and
    /// <see cref="StallProbeObservation.TerminalSilenceMs"/> are on the observation and are stored on the
    /// row, but they cannot change this answer. The failure mode #2880 measured is streaming SLOWLY, with
    /// <c>drain_ms - last_read_ms</c> at 0-3 ms on 8 of 8 abandoned runs — the last row lands at the instant
    /// the budget fires — so a watchdog gated on the reader having gone quiet would never fire on the actual
    /// defect, only on the delivered-and-hung failure that has never once been observed. A delivered-and-hung
    /// stream still fires here, because it is also slow and also past the floor; it simply is not required
    /// to.</para>
    ///
    /// <para><b>A read that has delivered nothing measurable fires.</b> <c>-1</c> bytes means the reader does
    /// not exist yet, i.e. the read is still inside <c>ExecuteReaderAsync</c> — the phase the cheap-collector
    /// <c>open_ms</c> degradation showed up in — and a read still executing 30 s into a 120 s budget is
    /// exactly what is worth a sample.</para>
    /// </summary>
    /// <param name="engine">
    /// The target's engine. PostgreSQL never fires: <see cref="QueryText"/> is T-SQL and reads
    /// <c>sys.dm_os_*</c>, so dispatching it at a PostgreSQL target would fail in the parser. Gated here
    /// rather than at the call site so the engine seam is pinnable — the #2213 lesson, where every defect was
    /// a call site that never learned engines exist.
    /// </param>
    public static StallProbeDecision Decide(
        CollectorTargetEngine engine, TimeSpan? wallClockBudget, StallProbeObservation observation)
    {
        if (engine != CollectorTargetEngine.SqlServer)
        {
            return new StallProbeDecision(false, $"target engine is {engine}; the probe query is T-SQL");
        }

        if (TriggerElapsedFor(wallClockBudget) is not { } trigger)
        {
            return new StallProbeDecision(false, "collector declares no wall-clock budget");
        }

        /* The constraint-2 invariant, enforced rather than merely asserted: a collector whose budget is too
           short for a fired probe to finish underneath it is not armed at all. That makes
           FitsUnderBudget total — the probe never runs where it would not fit — instead of a property that
           happens to hold for today's two budgets and would quietly stop holding for a future collector
           declaring a tight one. */
        if (!FitsUnderBudget(wallClockBudget!.Value))
        {
            return new StallProbeDecision(
                false,
                string.Format(
                    CultureInfo.InvariantCulture,
                    "a {0:F0} ms trigger plus the {1:F0} ms probe budget does not fit under this collector's {2:F0} ms budget",
                    trigger.TotalMilliseconds,
                    HardBudget.TotalMilliseconds,
                    wallClockBudget.Value.TotalMilliseconds));
        }

        if (observation.ElapsedMs < trigger.TotalMilliseconds)
        {
            return new StallProbeDecision(
                false,
                string.Format(
                    CultureInfo.InvariantCulture,
                    "read has run {0} ms, under the {1:F0} ms trigger",
                    observation.ElapsedMs,
                    trigger.TotalMilliseconds));
        }

        var rate = observation.BytesPerSecond;
        if (rate >= ThroughputFloorBytesPerSecond)
        {
            return new StallProbeDecision(
                false,
                string.Format(
                    CultureInfo.InvariantCulture,
                    "delivering {0:F2} MB/s, at or above the {1:F2} MB/s floor",
                    rate / 1024 / 1024,
                    ThroughputFloorBytesPerSecond / 1024d / 1024d));
        }

        return new StallProbeDecision(
            true,
            string.Format(
                CultureInfo.InvariantCulture,
                "read has run {0} ms past the {1:F0} ms trigger delivering {2:F2} MB/s, under the {3:F2} MB/s floor",
                observation.ElapsedMs,
                trigger.TotalMilliseconds,
                rate < 0 ? 0 : rate / 1024 / 1024,
                ThroughputFloorBytesPerSecond / 1024d / 1024d));
    }

    /// <summary>
    /// The server-wide sample, as one result set of at most <see cref="TopWaitTypeCount"/> rows.
    ///
    /// <para><b>Two DMV scans and no temp table, deliberately.</b> Staging the wait aggregate first would give
    /// one consistent snapshot instead of two scans milliseconds apart, and that is the house pattern for a
    /// heavy query — but this one runs against an instance that may be stalled ON tempdb or on IO, and
    /// creating an object there to serve a diagnostic is exactly how a watchdog becomes the second problem.
    /// <c>sys.dm_os_waiting_tasks</c> reads in-memory structures, so the cost of reading it twice is
    /// negligible and the skew between the totals and the top-N cut is milliseconds on a headline sample.</para>
    ///
    /// <para><b>No ignored-wait filter, unlike <see cref="WaitingTasksCollector"/>.</b> That collector feeds a
    /// trend surface an operator reads all day, so <c>IgnoredWaitDefaults</c> earns its place there. This is a
    /// forensic sample of an instance that has gone 50x slow at producing rows, and the candidate causes —
    /// scheduler starvation above all — live squarely among the wait types a trend surface calls benign.
    /// Filtering here would delete the answer.</para>
    ///
    /// <para><b>The instance's own sessions are all included; only the probe's is excluded.</b> There is no
    /// session id to exclude the stalled collector by (see the type comment), and it should not be excluded
    /// anyway — a server-wide sample that omitted the most interesting session would answer a different
    /// question.</para>
    ///
    /// <para><b><c>OUTER APPLY</c> so the result set has a row even when nothing is waiting.</b> An empty
    /// result and an idle instance would otherwise be the same absence of rows, and "answering trivial
    /// queries in milliseconds while producing rows 50x slowly, waiting on nothing" is a finding this probe
    /// has to be able to return rather than lose.</para>
    /// </summary>
    public const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorDarling stall probe */
    wait_type = w.wait_type,
    waiting_tasks = w.waiting_tasks,
    total_wait_ms = w.total_wait_ms,
    max_wait_ms = w.max_wait_ms,
    all_waiting_tasks = t.all_waiting_tasks,
    distinct_wait_types = t.distinct_wait_types,
    scheduler_count = s.scheduler_count,
    runnable_tasks = s.runnable_tasks,
    work_queue_length = s.work_queue_length,
    pending_disk_io = s.pending_disk_io,
    max_runnable_tasks = s.max_runnable_tasks
FROM
(
    SELECT
        scheduler_count = COUNT(*),
        runnable_tasks = ISNULL(SUM(CONVERT(bigint, os.runnable_tasks_count)), 0),
        work_queue_length = ISNULL(SUM(CONVERT(bigint, os.work_queue_count)), 0),
        pending_disk_io = ISNULL(SUM(CONVERT(bigint, os.pending_disk_io_count)), 0),
        max_runnable_tasks = ISNULL(MAX(os.runnable_tasks_count), 0)
    FROM sys.dm_os_schedulers AS os
    WHERE os.status = N'VISIBLE ONLINE'
) AS s
CROSS JOIN
(
    SELECT
        all_waiting_tasks = COUNT_BIG(*),
        distinct_wait_types = COUNT(DISTINCT owt.wait_type)
    FROM sys.dm_os_waiting_tasks AS owt
    WHERE owt.wait_type IS NOT NULL
    AND   owt.session_id <> @@SPID
) AS t
OUTER APPLY
(
    SELECT TOP (5)
        wait_type = owt.wait_type,
        waiting_tasks = COUNT_BIG(*),
        total_wait_ms = SUM(CONVERT(bigint, owt.wait_duration_ms)),
        max_wait_ms = MAX(CONVERT(bigint, owt.wait_duration_ms))
    FROM sys.dm_os_waiting_tasks AS owt
    WHERE owt.wait_type IS NOT NULL
    AND   owt.session_id <> @@SPID
    GROUP BY owt.wait_type
    ORDER BY SUM(CONVERT(bigint, owt.wait_duration_ms)) DESC, owt.wait_type ASC
) AS w
ORDER BY w.total_wait_ms DESC, w.wait_type ASC
OPTION(RECOMPILE);";

    /// <summary>
    /// Folds <see cref="QueryText"/>'s result set into one <see cref="StallWaitSample"/>, or <c>null</c> when
    /// what came back does not describe a live instance — no row at all, or zero <c>VISIBLE ONLINE</c>
    /// schedulers. The caller records that as <see cref="OutcomeNoSample"/>.
    ///
    /// <para>The scheduler and total figures repeat on every row (they are cross-joined onto the top-N cut),
    /// so they are taken from the first row and the rest of it is read for the wait detail only.</para>
    /// </summary>
    public static async ValueTask<StallWaitSample?> ReadAsync(DbDataReader reader, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var waits = new List<StallWaitRow>(TopWaitTypeCount);
        var haveHeader = false;
        long allWaitingTasks = 0;
        var distinctWaitTypes = 0;
        var schedulerCount = 0;
        long runnableTasks = 0;
        long workQueueLength = 0;
        long pendingDiskIo = 0;
        var maxRunnableTasks = 0;

        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!haveHeader)
            {
                allWaitingTasks = reader.IsDBNull(4) ? 0 : reader.GetInt64(4);
                distinctWaitTypes = reader.IsDBNull(5) ? 0 : reader.GetInt32(5);
                schedulerCount = reader.IsDBNull(6) ? 0 : reader.GetInt32(6);
                runnableTasks = reader.IsDBNull(7) ? 0 : reader.GetInt64(7);
                workQueueLength = reader.IsDBNull(8) ? 0 : reader.GetInt64(8);
                pendingDiskIo = reader.IsDBNull(9) ? 0 : reader.GetInt64(9);
                maxRunnableTasks = reader.IsDBNull(10) ? 0 : reader.GetInt32(10);
                haveHeader = true;
            }

            /* NULL across the OUTER APPLY's columns is the guaranteed row an instance with nothing waiting
               produces. It carries the header and no wait detail — an all-clear, not a missing sample. */
            if (reader.IsDBNull(0))
            {
                continue;
            }

            waits.Add(new StallWaitRow(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3)));
        }

        if (!haveHeader || schedulerCount <= 0)
        {
            return null;
        }

        var top = waits.Count > 0 ? waits[0] : default;

        return new StallWaitSample(
            allWaitingTasks,
            distinctWaitTypes,
            waits.Count > 0 ? top.WaitType : null,
            waits.Count > 0 ? top.TotalWaitMs : 0,
            waits.Count > 0 ? top.MaxWaitMs : 0,
            waits.Count > 0 ? RenderWaitSummary(waits) : null,
            schedulerCount,
            runnableTasks,
            workQueueLength,
            pendingDiskIo,
            maxRunnableTasks);
    }

    /// <summary>
    /// Renders the sampled wait types as one line — <c>TYPE:Nx/Mms</c> per entry, semicolon separated, capped
    /// at <see cref="WaitSummaryMaxLength"/>.
    ///
    /// <para>A rendered string beside the queryable columns rather than instead of them: the discriminators an
    /// operator or a read filters on (top wait type, its totals, the scheduler aggregates) each have their own
    /// column, and this carries the BREADTH those cannot — five types in one glance without five more columns.
    /// It is summary only, and nothing keys on parsing it.</para>
    /// </summary>
    public static string RenderWaitSummary(IReadOnlyList<StallWaitRow> waits)
    {
        ArgumentNullException.ThrowIfNull(waits);

        var summary = new StringBuilder();

        foreach (var wait in waits)
        {
            var entry = string.Format(
                CultureInfo.InvariantCulture,
                "{0}:{1}x/{2}ms",
                wait.WaitType,
                wait.WaitingTasks,
                wait.TotalWaitMs);

            /* Whole entries only. A summary truncated mid-figure would read as a real, smaller number. */
            if (summary.Length + entry.Length + (summary.Length > 0 ? 2 : 0) > WaitSummaryMaxLength)
            {
                break;
            }

            if (summary.Length > 0)
            {
                summary.Append("; ");
            }

            summary.Append(entry);
        }

        return summary.ToString();
    }
}
