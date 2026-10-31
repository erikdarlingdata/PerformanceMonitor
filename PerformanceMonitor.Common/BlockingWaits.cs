/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// #5644 W1: what ONE blocking event is, defined once for the alert engine, the fleet "Blocking N" tile and the
/// analysis BLOCKING_EVENTS fact, in Darling and in Lite.
///
/// <para>SQL Server writes a blocked-process report for a still-blocked session every
/// <c>blocked process threshold</c> seconds (5 at the lowest), so one one-minute wait is about eleven report rows.
/// Counting rows counted that wait eleven times. Erik's ruling: one event per blocked session, blocker and wait
/// start, reporting the longest wait time.</para>
///
/// <para><b>The wait start is derived</b>: the report's time minus its <c>wait_time_ms</c>. Every report of one
/// wait derives the same instant, give or take the milliseconds between the event timestamp and the wait time
/// being read, so two reports of one (blocked, blocker) pair belong to the same wait when their derived starts are
/// within <see cref="WaitStartToleranceMs"/> of the previous one (reports sorted by derived start). A different
/// wait of the same pair that was reported at all lasted at least the threshold (5 s) before its first report,
/// so its start is at least that far from the previous wait's start; the tolerance sits well below it.</para>
///
/// <para><b>Why that is not enough for DMV snapshots (#5743).</b> A DMV blocking snapshot catches a wait of any
/// length, so two separate short waits of one pair can start less than 2 s apart (wait A seen at T with wait
/// 1,500 ms, the pair blocks again 200 ms later and is seen at T + 10 s with wait 9,800 ms: starts 1.7 s apart).
/// A blocked session waits on one thing at a time, so a new wait of a pair cannot start before the previous wait
/// was last seen alive, and a row's event time is a moment its wait was observed alive. So a row also starts a
/// new wait when its derived start is more than <see cref="WaitStartJitterMs"/> after the LATEST event time of all
/// the pair's preceding rows (same start order). One rule for every source: for blocked-process reports the
/// first report comes at least the threshold after the wait began, so every report of one wait derives a start at
/// least 5 s BEFORE any earlier report's event time and the second condition never splits a wait there (waits of
/// different starts are already split by the first condition).</para>
///
/// <para>The same rule is written twice because the counts live in two places: <see cref="Collapse"/> for rows
/// already in memory (the alert engine's read), and <see cref="WaitsCteSql"/> for the counts that run in the
/// store (the fleet tile and the analysis fact). A test runs both over the same rows and checks they agree.</para>
/// </summary>
public static class BlockingWaits
{
    /// <summary>
    /// #6645: the SQL predicate that is true for a row that is a real wait and false for a parallel query's thread
    /// waiting on its own session (the same non-zero spid on both sides). The one definition every read surface
    /// aliases, in DuckDB and PostgreSQL. It is <see cref="Collapse{T}"/>'s rule: a (0, 0) row is a report that
    /// named no session (the collector stores 0, never NULL), which stays a blocking event, so the equal-spid test
    /// carries <c>blocked_spid &lt;&gt; 0</c>; a NULL spid makes the test NULL, which COALESCE turns to FALSE and keeps
    /// the row. Column names are unqualified: every use has one source relation in scope.
    /// </summary>
    public const string NotSelfWaitSql = "NOT COALESCE(blocked_spid = blocking_spid AND blocked_spid <> 0, FALSE)";

    /// <summary>How far apart (milliseconds) two reports' derived wait starts may be and still be one wait. The
    /// threshold is at least 5 s, so 2 s cannot merge two blocked-process-report waits and is far above the few
    /// milliseconds of timestamp jitter between reports of one wait. It alone is not enough for DMV snapshots: see <see cref="WaitStartJitterMs"/>.</summary>
    public const long WaitStartToleranceMs = 2000;

    /// <summary>
    /// #5743: how far (milliseconds) a row's derived wait start may sit after the latest event time of the pair's
    /// preceding rows and still be the same wait. It is the timestamp jitter between a row's event time and its
    /// wait time. Evidence: a DMV snapshot's <c>event_time</c> is the collector's own stamp
    /// (<c>CollectionTimeClock.NextStrictlyAfter(..., DateTime.UtcNow)</c>), taken at the start of the run, before
    /// the store connection and the SQL Server connection open and before the query reads
    /// <c>wait_duration_ms</c>; the derived start is therefore the true start minus that run's lead time, which
    /// varies from run to run (not measured). PostgreSQL and DuckDB keep microseconds, so the store adds none.
    /// 150 ms stays under the 200 ms re-block gap #5743 reports (the ceiling: a larger value would merge that case
    /// again). A continuing wait splits by mistake only when two of its rows' lead times differ by more than
    /// the wait time of the row with the longer lead plus this value. The collector keeps only waits of at least
    /// 500 ms (page latches; 1 s, 2 s and 5 s for the other wait types it reads), so that takes a lead
    /// difference above 650 ms.
    /// </summary>
    public const long WaitStartJitterMs = 150;

    /// <summary>The wait start of one report in epoch milliseconds: event time minus its wait time.</summary>
    public static double WaitStartEpochMs(DateTime eventTime, long waitTimeMs) =>
        (eventTime - DateTime.UnixEpoch).TotalMilliseconds - waitTimeMs;

    /// <summary>
    /// Collapses reports to one row per wait (a report whose blocked and blocking spid are the same, and not 0, is a
    /// parallel query waiting on itself, #6645, and is dropped): the report with the longest wait time (the latest one on a tie).
    /// <paramref name="partition"/> keeps rows that must never merge apart (the report source, for one). A row with
    /// no event time or no wait time has no derived start and cannot be placed in a wait, so it stays a wait of
    /// its own (the SQL form does the same). The result is ordered by the
    /// representative's event time, newest first, as the alert reads are.
    /// </summary>
    public static List<T> Collapse<T>(
        IEnumerable<T> reports,
        Func<T, string> partition,
        Func<T, int> blockedSpid,
        Func<T, int> blockingSpid,
        Func<T, DateTime?> eventTime,
        Func<T, long?> waitTimeMs)
    {
        var result = new List<T>();
        var placeable = new List<(T Row, double Start, DateTime Time, long Wait)>();
        foreach (var row in reports)
        {
            /* #6645: a parallel query's threads report waits on their own session (same spid, different ecid).
               That is the query waiting on itself, not blocking. Spid 0 is a report that named no session (the
               collector stores 0, not NULL, when a report has no spid: BlockedProcessReportCollector), which is
               still a blocking event: NotSelfWaitSql keeps (0, 0) rows too, by the same `<> 0` test. */
            if (blockedSpid(row) == blockingSpid(row) && blockedSpid(row) != 0)
            {
                continue;
            }

            var time = eventTime(row);
            var wait = waitTimeMs(row);
            if (time is null || wait is null)
            {
                result.Add(row);
                continue;
            }

            placeable.Add((row, WaitStartEpochMs(time.Value, wait.Value), time.Value, wait.Value));
        }

        foreach (var pair in placeable.GroupBy(p => (partition(p.Row), blockedSpid(p.Row), blockingSpid(p.Row))))
        {
            var ordered = pair.OrderBy(p => p.Start).ThenBy(p => p.Time).ToList();
            var best = ordered[0];
            var previousStart = ordered[0].Start;
            var latestTime = ordered[0].Time;
            for (var i = 1; i < ordered.Count; i++)
            {
                var current = ordered[i];
                /* #5743: a new wait cannot start before the previous one was last seen alive. latestTime is the
                   latest event time over ALL preceding rows of the pair (not only the current group), in the
                   same start order the SQL form's window uses. */
                var latestEpochMs = (latestTime - DateTime.UnixEpoch).TotalMilliseconds;
                if (current.Start - previousStart > WaitStartToleranceMs
                    || current.Start - latestEpochMs > WaitStartJitterMs)
                {
                    result.Add(best.Row);
                    best = current;
                }
                else if (current.Wait > best.Wait || (current.Wait == best.Wait && current.Time >= best.Time))
                {
                    best = current;
                }

                previousStart = current.Start;
                if (current.Time > latestTime)
                {
                    latestTime = current.Time;
                }
            }

            result.Add(best.Row);
        }

        return result
            .OrderByDescending(r => eventTime(r) ?? DateTime.MinValue)
            .ToList();
    }

    /// <summary>
    /// #7028: ONE DMV snapshot's total blocked wait in milliseconds, the measure the <c>Blocking Wait Time</c> alert
    /// reads (<c>blocking.wait_threshold_seconds</c>) and the blocking band's snapshot arm reads. An aggregate
    /// expression over a relation that has <c>wait_time_ms</c>, <c>blocked_spid</c> and <c>blocking_spid</c>
    /// (PostgreSQL): the summed <c>wait_time_ms</c> of the rows that are real waits (<see cref="NotSelfWaitSql"/>),
    /// 0 when none, as a bigint. The alert's one-snapshot read and <see cref="SnapshotTotalsSql"/> both aggregate it,
    /// so there is one SUM expression.
    /// </summary>
    public const string SnapshotTotalWaitMsSql =
        "CAST(COALESCE(SUM(wait_time_ms) FILTER (WHERE " + NotSelfWaitSql + "), 0) AS bigint)";

    /// <summary>
    /// #7028: a CTE named <paramref name="name"/>, one row per DMV snapshot, with the partition columns, the
    /// <c>collection_time</c> that identifies the snapshot (the alert's observation identity), and
    /// <c>total_wait_ms</c> = <see cref="SnapshotTotalWaitMsSql"/> over the snapshot's rows. <paramref name="source"/>
    /// is a SELECT that returns <c>collection_time</c>, <c>wait_time_ms</c>, <c>blocked_spid</c>, <c>blocking_spid</c>
    /// and every column in <paramref name="partitionColumns"/>. The text ends with the closing parenthesis, no
    /// trailing comma. PostgreSQL only.
    /// </summary>
    public static string SnapshotTotalsSql(
        string name, string source, IReadOnlyList<string>? partitionColumns = null)
    {
        var cols = PartitionList(partitionColumns, "s.");
        return $@"
{name} AS (
    SELECT {cols}s.collection_time, {SnapshotTotalWaitMsSql} AS total_wait_ms
    FROM ({source}) AS s
    GROUP BY {cols}s.collection_time
)";
    }

    /// <summary>
    /// #7028: the blocking band's snapshot arm value, a CTE named <paramref name="name"/> with the partition columns
    /// and <c>peak_snapshot_wait_ms</c> (double precision, milliseconds), one row per partition that has a snapshot,
    /// built from a <see cref="SnapshotTotalsSql"/> CTE named <paramref name="totals"/>. It mirrors the gate that
    /// decides whether <c>Blocking Wait Time</c> pages, so a snapshot that would not page does not colour the band:
    /// for each snapshot take the larger of (a) the smallest total across that snapshot and the
    /// <paramref name="breachSamples"/> - 1 snapshots before it, counted only while the snapshots are adjacent
    /// (a gap of more than <paramref name="episodeGapSeconds"/> starts a new episode, as the alert restarts a
    /// streak), 0 when fewer than <paramref name="breachSamples"/> adjacent snapshots exist, and (b) the total divided
    /// by <paramref name="singleSnapshotMultiplier"/> (one snapshot at that multiple of the bar pages alone). The
    /// value is the largest of those over the rows <paramref name="totals"/> holds. A tier then compares as
    /// <c>value &gt;= tier</c>. Callers pass the alert's own constants (<c>AlertEngine.BlockingWaitBreachSamples</c>,
    /// <c>BlockingWaitSingleSnapshotMultiplier</c> and the episode gap factor times the collector cadence). The text
    /// ends with the closing parenthesis, no trailing comma. PostgreSQL only.
    /// </summary>
    public static string GatedSnapshotWaitSql(
        string name, string totals, int breachSamples, int singleSnapshotMultiplier, double episodeGapSeconds,
        IReadOnlyList<string>? partitionColumns = null)
    {
        var inv = System.Globalization.CultureInfo.InvariantCulture;
        var cols = PartitionList(partitionColumns, string.Empty);
        var partition = partitionColumns is { Count: > 0 }
            ? "PARTITION BY " + string.Join(", ", partitionColumns)
            : string.Empty;
        var episodePartition = partitionColumns is { Count: > 0 }
            ? partition + ", episode"
            : "PARTITION BY episode";
        var samples = Math.Max(1, breachSamples);
        var groupBy = string.IsNullOrEmpty(cols) ? string.Empty : "GROUP BY " + cols.Substring(0, cols.Length - 2);
        return $@"
{name}_gaps AS (
    SELECT t.*, t.collection_time - LAG(t.collection_time) OVER ({partition} ORDER BY t.collection_time) AS since_prev
    FROM {totals} AS t
),
{name}_episodes AS (
    SELECT g.*,
        SUM(CASE WHEN g.since_prev IS NULL OR g.since_prev > interval '{episodeGapSeconds.ToString("R", inv)} seconds' THEN 1 ELSE 0 END)
            OVER ({partition} ORDER BY g.collection_time) AS episode
    FROM {name}_gaps AS g
),
{name}_framed AS (
    SELECT e.*,
        MIN(e.total_wait_ms) OVER w AS streak_min,
        COUNT(*) OVER w AS streak_len
    FROM {name}_episodes AS e
    WINDOW w AS ({episodePartition} ORDER BY e.collection_time ROWS BETWEEN {(samples - 1).ToString(inv)} PRECEDING AND CURRENT ROW)
),
{name} AS (
    SELECT {cols}MAX(GREATEST(
        CASE WHEN f.streak_len >= {samples.ToString(inv)} THEN CAST(f.streak_min AS double precision) ELSE 0 END,
        CAST(f.total_wait_ms AS double precision) / {Math.Max(1, singleSnapshotMultiplier).ToString(inv)})) AS peak_snapshot_wait_ms
    FROM {name}_framed AS f
    {groupBy}
)";
    }

    /// <summary>
    /// #7028: the per-hour source choice the blocking alerts make for their rolling hour, applied to every hour of a
    /// longer window. A CTE named <paramref name="name"/> with the partition columns, <c>bucket</c> (the hour the
    /// wait falls in), <c>event_time</c>, <c>wait_time_ms</c> and <c>from_xe</c>: one row per WAIT of the chosen
    /// source. In each (partition, hour) the XE waits are the chosen rows if the hour has any, else the DMV waits
    /// (the alert's XE-preferred rule, <c>AlertContextBuilders.BlockingWaitRows</c>). A wait belongs to the hour of
    /// its representative report (the longest of its reports). <paramref name="xeWaits"/> and
    /// <paramref name="dmvWaits"/> name two <see cref="WaitsCteSql"/> CTEs (already free of self-waits), each with
    /// <paramref name="xeTimeColumn"/> / <paramref name="dmvTimeColumn"/> and <c>wait_time_ms</c> and the partition
    /// columns. Hours are <c>date_bin('1 hour', time, origin)</c>, <paramref name="originSql"/> a SQL timestamp
    /// expression (the window end for a rolling window, so the newest hour is the alert's own, or the day start for
    /// a calendar day). With <paramref name="singleBucket"/> (a window of one hour or less) there is one bucket, the
    /// alert's rolling hour itself. A caller sums or counts this CTE: the window count is the sum of the chosen
    /// waits, the longest wait is their MAX. The text ends with the closing parenthesis, no trailing comma.
    /// PostgreSQL only.
    /// </summary>
    public static string HourlyChosenWaitsSql(
        string name, string xeWaits, string dmvWaits, string originSql, bool singleBucket = false,
        IReadOnlyList<string>? partitionColumns = null, string xeTimeColumn = "event_time", string dmvTimeColumn = "event_time")
    {
        var cols = PartitionList(partitionColumns, string.Empty);
        string Bucket(string alias, string timeColumn) => singleBucket
            ? $"CAST({originSql} AS timestamp)"
            : $"date_bin('1 hour', {alias}.{timeColumn}, {originSql})";
        var match = string.Concat((partitionColumns ?? Array.Empty<string>()).Select(c => $"x.{c} = d.{c} AND "));
        var dcols = PartitionList(partitionColumns, "d.");
        return $@"
{name}_xe AS (
    SELECT {PartitionList(partitionColumns, "x.")}{Bucket("x", xeTimeColumn)} AS bucket, x.{xeTimeColumn} AS event_time, x.wait_time_ms
    FROM {xeWaits} AS x
),
{name}_dmv AS (
    SELECT {dcols}{Bucket("d", dmvTimeColumn)} AS bucket, d.{dmvTimeColumn} AS event_time, d.wait_time_ms
    FROM {dmvWaits} AS d
),
{name} AS (
    SELECT {cols}bucket, event_time, wait_time_ms, TRUE AS from_xe
    FROM {name}_xe
    UNION ALL
    SELECT {dcols}d.bucket, d.event_time, d.wait_time_ms, FALSE
    FROM {name}_dmv AS d
    WHERE NOT EXISTS (SELECT 1 FROM {name}_xe AS x WHERE {match}x.bucket = d.bucket)
)";
    }

    /// <summary>"a, b, " for the columns with the alias prefix, or empty: a SELECT list prefix.</summary>
    private static string PartitionList(IReadOnlyList<string>? columns, string alias) =>
        columns is { Count: > 0 } ? string.Concat(columns.Select(c => alias + c + ", ")) : string.Empty;

    /// <summary>Which store's SQL dialect <see cref="WaitsCteSql"/> writes the epoch conversion in.</summary>
    public enum SqlDialect
    {
        /// <summary>PostgreSQL (Darling's store).</summary>
        Postgres,

        /// <summary>DuckDB (Lite's store).</summary>
        DuckDb,
    }

    /// <summary>A timestamp column as epoch milliseconds, in the SQL of the given store.</summary>
    private static string EpochMsSql(SqlDialect dialect, string column) =>
        dialect == SqlDialect.DuckDb
            ? $"(CAST(epoch_us({column}) AS DOUBLE) / 1000.0)"
            : $"(date_part('epoch', {column}) * 1000.0)";

    /// <summary>
    /// The CTE chain that turns a report relation into one row per wait, written for both stores from this one
    /// text. <paramref name="source"/> is a SELECT that must return <c>blocked_spid</c>, <c>blocking_spid</c>,
    /// <c>event_time</c> and <c>wait_time_ms</c>, plus whatever else the caller reads (kept as the representative
    /// report's values), plus any column named in <paramref name="partitionColumns"/> (for example
    /// <c>server_id</c>). The chain ends in a CTE named <paramref name="name"/>, one row per wait, whose
    /// <c>wait_time_ms</c> is the longest wait of that wait's reports and whose other columns are that
    /// report's. The caller continues the WITH list after it (the text ends with the closing parenthesis, no
    /// trailing comma).
    ///
    /// <para><paramref name="reportCount"/> adds a <c>wait_reports</c> column to the chain's last CTE: how many source
    /// rows (blocked-process reports, or DMV snapshot rows) the wait collapsed. It is the count a health band
    /// whose tiers were measured in REPORTS per server-hour reads (#5644), while the displayed count stays one per
    /// wait. Off by default so every other caller's CTE keeps its shape.</para>
    /// </summary>
    public static string WaitsCteSql(
        SqlDialect dialect, string name, string source, IReadOnlyList<string>? partitionColumns = null,
        bool reportCount = false)
    {
        var partition = string.Join(
            ", ", (partitionColumns ?? Array.Empty<string>()).Concat(new[] { "blocked_spid", "blocking_spid" }));
        var reportColumn = reportCount
            ? $", COUNT(*) OVER (PARTITION BY {partition}, n.wait_no) AS wait_reports"
            : string.Empty;
        var epochMs = EpochMsSql(dialect, "s.event_time");
        var epochMsM = EpochMsSql(dialect, "m.event_time");
        var tolerance = WaitStartToleranceMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var jitter = WaitStartJitterMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return $@"
{name}_marked AS (
    SELECT s.*, {epochMs} - s.wait_time_ms AS wait_start_ms
    FROM ({source}) AS s
    /* #6645: a parallel query's threads write reports with blocked_spid = blocking_spid (different ecid). That is
       the query waiting on itself, not blocking: it is no wait, no head blocker and no report. NotSelfWaitSql
       keeps a report that names no session (spids 0/0, or NULL). */
    WHERE {NotSelfWaitSql}
),
{name}_gapped AS (
    SELECT m.*,
        CASE
            WHEN m.wait_start_ms IS NULL THEN 1
            WHEN LAG(m.wait_start_ms) OVER (PARTITION BY {partition} ORDER BY m.wait_start_ms NULLS LAST, m.event_time NULLS LAST) IS NULL
              OR m.wait_start_ms - LAG(m.wait_start_ms) OVER (PARTITION BY {partition} ORDER BY m.wait_start_ms NULLS LAST, m.event_time NULLS LAST) > {tolerance}
              /* #5743: a new wait cannot start before the previous one was last seen alive. The latest event
                 time over ALL preceding rows of the pair; NULL (no preceding row) puts no constraint. */
              OR m.wait_start_ms - MAX({epochMsM}) OVER (PARTITION BY {partition} ORDER BY m.wait_start_ms NULLS LAST, m.event_time NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) > {jitter}
            THEN 1 ELSE 0
        END AS starts_wait
    FROM {name}_marked AS m
),
{name}_numbered AS (
    SELECT g.*,
        SUM(g.starts_wait) OVER (PARTITION BY {partition} ORDER BY g.wait_start_ms NULLS LAST, g.event_time NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS wait_no
    FROM {name}_gapped AS g
),
{name}_ranked AS (
    SELECT n.*,
        ROW_NUMBER() OVER (PARTITION BY {partition}, n.wait_no ORDER BY n.wait_time_ms DESC NULLS LAST, n.event_time DESC NULLS LAST) AS wait_rank{reportColumn}
    FROM {name}_numbered AS n
),
{name} AS (
    SELECT r.*
    FROM {name}_ranked AS r
    WHERE r.wait_rank = 1
)";
    }
}
