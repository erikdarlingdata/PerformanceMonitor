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
/// so its start is at least that far from the previous wait's start; the tolerance sits well below it. That
/// holds for blocked-process reports only. A DMV blocking snapshot catches a wait of any length, so two separate
/// short waits of one pair can start less than 2 s apart and count as one wait; issue #5743 tracks the fix.</para>
///
/// <para>The same rule is written twice because the counts live in two places: <see cref="Collapse"/> for rows
/// already in memory (the alert engine's read), and <see cref="WaitsCteSql"/> for the counts that run in the
/// store (the fleet tile and the analysis fact). A test runs both over the same rows and checks they agree.</para>
/// </summary>
public static class BlockingWaits
{
    /// <summary>How far apart (milliseconds) two reports' derived wait starts may be and still be one wait. The
    /// threshold is at least 5 s, so 2 s cannot merge two waits and is far above the few milliseconds of
    /// timestamp jitter between reports of one wait (and the whole-second rounding of a store that keeps only
    /// seconds).</summary>
    public const long WaitStartToleranceMs = 2000;

    /// <summary>The wait start of one report in epoch milliseconds: event time minus its wait time.</summary>
    public static double WaitStartEpochMs(DateTime eventTime, long waitTimeMs) =>
        (eventTime - DateTime.UnixEpoch).TotalMilliseconds - waitTimeMs;

    /// <summary>
    /// Collapses reports to one row per wait: the report with the longest wait time (the latest one on a tie).
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
            for (var i = 1; i < ordered.Count; i++)
            {
                var current = ordered[i];
                if (current.Start - previousStart > WaitStartToleranceMs)
                {
                    result.Add(best.Row);
                    best = current;
                }
                else if (current.Wait > best.Wait || (current.Wait == best.Wait && current.Time >= best.Time))
                {
                    best = current;
                }

                previousStart = current.Start;
            }

            result.Add(best.Row);
        }

        return result
            .OrderByDescending(r => eventTime(r) ?? DateTime.MinValue)
            .ToList();
    }

    /// <summary>Which store's SQL dialect <see cref="WaitsCteSql"/> writes the epoch conversion in.</summary>
    public enum SqlDialect
    {
        /// <summary>PostgreSQL (Darling's store).</summary>
        Postgres,

        /// <summary>DuckDB (Lite's store).</summary>
        DuckDb,
    }

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
        var epochMs = dialect == SqlDialect.DuckDb
            ? "(CAST(epoch_us(s.event_time) AS DOUBLE) / 1000.0)"
            : "(date_part('epoch', s.event_time) * 1000.0)";
        var tolerance = WaitStartToleranceMs.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return $@"
{name}_marked AS (
    SELECT s.*, {epochMs} - s.wait_time_ms AS wait_start_ms
    FROM ({source}) AS s
),
{name}_gapped AS (
    SELECT m.*,
        CASE
            WHEN m.wait_start_ms IS NULL THEN 1
            WHEN LAG(m.wait_start_ms) OVER (PARTITION BY {partition} ORDER BY m.wait_start_ms NULLS LAST, m.event_time NULLS LAST) IS NULL
              OR m.wait_start_ms - LAG(m.wait_start_ms) OVER (PARTITION BY {partition} ORDER BY m.wait_start_ms NULLS LAST, m.event_time NULLS LAST) > {tolerance}
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
