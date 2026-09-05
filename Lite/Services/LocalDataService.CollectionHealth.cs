/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// #1591: how many DISTINCT collectors were permission-denied in the last 7 days — the badge count for the
    /// Collection Health tab header.
    ///
    /// <para>Deliberately its own narrow COUNT rather than reusing <c>GetCollectionHealthAsync</c>: that one is
    /// per-collector and only runs when its tab is selected, which is exactly why a permission problem stayed
    /// invisible until someone thought to look. This runs on every refresh alongside the alert-count badge, so a
    /// denied collector is discoverable from any tab. Counts collectors, not rows, so one collector failing every
    /// cycle for a week reads as "1" rather than a meaningless four-figure number.</para>
    /// </summary>
    public async Task<int> GetPermissionDeniedCollectorCountAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT COUNT(DISTINCT collector_name)
FROM v_collection_log
WHERE server_id = $1
AND   collection_time >= $2
AND   status = 'PERMISSIONS'";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-7) });

        var scalar = await command.ExecuteScalarAsync();
        return scalar is null or DBNull ? 0 : Convert.ToInt32(scalar);
    }

    /// <summary>
    /// The 7-day per-collector aggregate behind the Collection Health grid. A named constant rather
    /// than an inline literal so <c>Lite.Tests</c> can pin its SHAPE by reference, the way its four
    /// Darling twins are pinned - #2926 needs every one of the five banding reads asserted to count
    /// abandonment the same way, and a pin that greps this file instead cannot tell the SQL from the
    /// prose around it. Byte-parity with Darling's <c>CollectionHealthSql</c> is the standing contract:
    /// both MCP surfaces read this result set POSITIONALLY.
    /// </summary>
    internal const string CollectionHealthSql = $@"
SELECT
    collector_name,
    COUNT(*) AS total_runs,
    -- #2926: SUCCESS excludes an abandonment that predates #2803, so the Success column beside
    -- Abandoned cannot count the same run twice. Post-#2803 rows need no exclusion - ABANDONED
    -- is not SUCCESS - and an ordinary empty run stays counted, which is what the COALESCE in
    -- the shared predicate is for: NULL under this NOT would have dropped it.
    SUM(CASE WHEN status = 'SUCCESS'
              AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}
             THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
    AVG(duration_ms) AS avg_duration_ms,
    -- #2460: the mean above describes a collector whose runs all cost about the same, and says
    -- nothing true about one whose runs come in two sizes. query_store on a dense shard reported a
    -- 13,834 ms average over 1,155 runs where 958 of them yielded nothing and cost ~36 ms, which
    -- puts the other 197 at ~80,900 ms EACH — each one on its own larger than the whole 60,000 ms
    -- sweep budget. duration_ms has been written per run since the table existed; nothing had ever
    -- read it as anything but a mean.
    --
    -- p95 rather than the max for the number a decision is made from: a max is one run, so a single
    -- pathological cycle would make a collector look permanently terrible for the rest of the
    -- window. p95 also scales itself to the sample — over 3,500 runs it discards the one bad cycle,
    -- and over the six runs a daily collector gets in a week it lands on the max, which is right,
    -- because with six samples there is no outlier anyone can afford to throw away. DISC rather
    -- than CONT so the answer is a duration some run actually took instead of an interpolation
    -- between the two modes, which would be a number describing no run at all — the exact defect
    -- this column exists to end. Both engines ignore NULL duration_ms here, as AVG already does.
    MAX(duration_ms) AS max_duration_ms,
    PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_duration_ms,
    -- SKIPPED counts as a healthy run (dedup / version-gated collectors no-op without being stale)
    MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
    MAX(collection_time) AS last_run_time,
    -- #1855: the message from the NEWEST failing run, not MAX()'s lexicographically greatest one. The
    -- status re-check is load-bearing rather than belt-and-braces: when no failing run in the window
    -- carried text, error_rank = 1 falls through to the newest row of ANY class, and without it a
    -- SUCCESS row's note could surface here as a fake last error.
    MAX(CASE WHEN error_rank = 1 AND status IN ('ERROR', 'PERMISSIONS') THEN error_message END) AS last_error,
    -- The newest failure OUTRIGHT, text or not: when did this last FAIL is about the run, not the
    -- message. It can only name a different row than last_error if a failure was written with no text.
    MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS') THEN collection_time END) AS last_error_time,
    SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
    -- YIELDED = the 1s LOCK_TIMEOUT guard fired (#1805): deliberate, benign for collection,
    -- counted apart from errors because clustering here is a signal about the TARGET's lock
    -- contention rather than a monitoring fault.
    SUM(CASE WHEN status = 'YIELDED' THEN 1 ELSE 0 END) AS yield_count,
    -- #1837: the note a SUCCEEDING run can leave behind (an enumeration that yielded 0 items, items
    -- whose enumeration probe failed). Gated on SUCCESS specifically, rather than on every non-failure status:
    -- the runners attach a note only to the SUCCESS write, and the looser complement would drag
    -- SESSION_MISSING and CANCELLED messages into a column whose whole claim is that it is NOT an
    -- error. Display text only: no band, no count, no threshold reads it, and a legitimately empty
    -- target (no user databases, no AGs) stays HEALTHY exactly as before.
    MAX(CASE WHEN note_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS last_note,
    -- How many of the window's runs carried one. note_count = total_runs is the persistently-empty
    -- signal the operator is actually looking for: EVERY run this week came back with nothing.
    COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count,
    -- #1852: the one thing that makes a persistently-empty enumeration interesting — does this target
    -- actually HAVE user databases? Zero items on a server with none is legitimate and stays quiet;
    -- zero items on a server that HAS them is a login that cannot enter any, or a filter that
    -- swallowed everything. database_size_stats rather than database_config for three reasons: it runs
    -- on the scheduled loop (60 min) where database_config is on-load and can age past this window on
    -- a long-running install; it is indexed on (server_id, collection_time) where database_config has
    -- no index at all; and it reads sys.master_files, so it still sees databases the monitoring login
    -- cannot ENTER — which is exactly the case being diagnosed. database_id > 4 excludes the system
    -- databases, tempdb included: the size collector takes every ONLINE database, so a bare row check
    -- would be true on every server alive.
    --
    -- The inventory window is the health read's OWN ($2) — no second parameter, and an inventory that
    -- aged out says nothing rather than something stale. Uncorrelated, so both engines evaluate it
    -- once per query (a Postgres InitPlan) instead of per row, and it needs no GROUP BY entry and no
    -- join. Feeds display text only, through the shared formatter; the banding never sees it.
    CASE
        WHEN EXISTS
             (
                 SELECT 1
                 FROM v_database_size_stats
                 WHERE server_id = $1
                 AND   collection_time >= $2
                 AND   database_id > 4
             )
        THEN 1
        ELSE 0
    END AS has_user_databases,
    -- #2472: the per-database fan-out, described for ONE run — the dearest one in the window. Five
    -- collectors run once per database and the run writes a single blended duration_ms, so eight
    -- databases at 10.1s and one at 62s beside seven at 2.7s are the same 80,900 ms and want
    -- opposite fixes. (No double quotes anywhere in this string: it is a verbatim literal, where a
    -- lone quote ends it.) The four parts compose into slowest_item_ms * fanout_items /
    -- slowest_run_duration_ms, which is 1.0 for an even fan-out and 6.1 for the dominated example.
    -- All four come from the SAME row via slowest_rank: parts taken from different runs would
    -- compose into a ratio describing no run that ever happened. Twins Darling's read.
    MAX(CASE WHEN slowest_rank = 1 THEN fanout_item_count END) AS fanout_items,
    MAX(CASE WHEN slowest_rank = 1 THEN slowest_item END) AS slowest_item,
    MAX(CASE WHEN slowest_rank = 1 THEN slowest_item_ms END) AS slowest_item_ms,
    MAX(CASE WHEN slowest_rank = 1 THEN duration_ms END) AS slowest_run_duration_ms,
    -- #2804: runs the #2673 wall-clock budget abandoned. APPENDED, never inserted — this result set
    -- is read positionally and Darling's CollectionHealthSql mirrors these ordinals, so a mid-list
    -- insert would silently re-map every later column in whichever surface was not edited with it.
    --
    -- #2926: keyed on the ROW, not on the status alone. collection_log is append-only, so a
    -- window can still hold cycles written before #2803 gave abandonment its own status:
    -- status = 'SUCCESS' beside rows_collected = 0 and the budget note. Counted by status
    -- alone this read 0 for them, and the collector banded HEALTHY while losing cycles - a
    -- filter correct against current writes and silently wrong against older ones, failing in
    -- the reassuring direction. The pattern is one LIKE because the budget is INTERPOLATED and
    -- the shipped values differ (120 s for procedure_stats/query_stats/plan_correction, 600 s
    -- for query_store), so equality against one rendered sentence matches one collector.
    SUM(CASE WHEN {EnumeratedCollectorDriver.AbandonedRunPredicateSql}
             THEN 1 ELSE 0 END) AS abandoned_count
FROM
(
    -- #1855: rank each class of message newest-first so the two exemplar columns above can take the
    -- LATEST one instead of the lexicographically greatest. Ordering on whether the class's CASE came
    -- back empty puts every row that carries such a message ahead of every row that does not, so rank 1
    -- is the newest one that has text — and a later clean run no longer blanks a note the window still
    -- holds. MAX() was never wrong about WHICH rows to consider, only about which of them wins, and
    -- text does not sort like the number #1837's probe note carries: 12 item(s) sorts below 9 item(s).
    -- collection_time settles it; error_message DESC only breaks an exact-timestamp tie, and breaks it
    -- identically on DuckDB and Postgres, which binary-vs-locale collation would not.
    SELECT
        collector_name,
        collection_time,
        duration_ms,
        status,
        error_message,
        -- #2926: the abandonment predicate above reads it. Projected here for the same reason
        -- #2472's three columns are: this subquery ENUMERATES its columns, so an aggregate
        -- outside naming one it does not carry fails at the STORE and nowhere earlier.
        rows_collected,
        -- #2472: projected here because this subquery enumerates its columns rather than SELECT *-ing
        -- them, so an aggregate outside that names a column the inner query does not carry fails at the
        -- store and nowhere earlier.
        fanout_item_count,
        slowest_item,
        slowest_item_ms,
        ROW_NUMBER() OVER
        (
            PARTITION BY collector_name
            ORDER BY (CASE WHEN status = 'SUCCESS' THEN error_message END) IS NULL,
                     collection_time DESC,
                     error_message DESC
        ) AS note_rank,
        ROW_NUMBER() OVER
        (
            PARTITION BY collector_name
            ORDER BY (CASE WHEN status IN ('ERROR', 'PERMISSIONS') THEN error_message END) IS NULL,
                     collection_time DESC,
                     error_message DESC
        ) AS error_rank,
        -- #2472: the window's dearest single ITEM, and with it the run that carried it. Ranked on
        -- slowest_item_ms rather than duration_ms because the question is which database is expensive,
        -- not which cycle was.
        ROW_NUMBER() OVER
        (
            PARTITION BY collector_name
            ORDER BY slowest_item_ms IS NULL,
                     slowest_item_ms DESC,
                     collection_time DESC
        ) AS slowest_rank
    FROM v_collection_log
    WHERE server_id = $1
    AND   collection_time >= $2
) runs
GROUP BY collector_name
ORDER BY collector_name";

    /// <summary>
    /// Gets collection health summary for all collectors on a server.
    /// </summary>
    public async Task<List<CollectorHealthRow>> GetCollectionHealthAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = CollectionHealthSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-7) });

        var items = new List<CollectorHealthRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new CollectorHealthRow
            {
                CollectorName = reader.GetString(0),
                TotalRuns = reader.IsDBNull(1) ? 0 : ToInt64(reader.GetValue(1)),
                SuccessCount = reader.IsDBNull(2) ? 0 : ToInt64(reader.GetValue(2)),
                ErrorCount = reader.IsDBNull(3) ? 0 : ToInt64(reader.GetValue(3)),
                AvgDurationMs = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                MaxDurationMs = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                P95DurationMs = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                LastSuccessTime = reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                LastRunTime = reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                LastError = reader.IsDBNull(9) ? null : reader.GetString(9),
                LastErrorTime = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                PermissionDeniedCount = reader.IsDBNull(11) ? 0 : ToInt64(reader.GetValue(11)),
                YieldCount = reader.IsDBNull(12) ? 0 : ToInt64(reader.GetValue(12)),
                LastNote = reader.IsDBNull(13) ? null : reader.GetString(13),
                NoteCount = reader.IsDBNull(14) ? 0 : ToInt64(reader.GetValue(14)),
                TargetHasUserDatabases = !reader.IsDBNull(15) && ToInt64(reader.GetValue(15)) != 0,
                /* Ordinals are positional and these four were APPENDED (#2472) — never inserted. */
                FanoutItems = reader.IsDBNull(16) ? null : Convert.ToInt32(reader.GetValue(16)),
                SlowestItem = reader.IsDBNull(17) ? null : reader.GetString(17),
                SlowestItemMs = reader.IsDBNull(18) ? null : Convert.ToInt32(reader.GetValue(18)),
                SlowestRunDurationMs = reader.IsDBNull(19) ? null : Convert.ToInt32(reader.GetValue(19)),
                /* Appended (#2804), for the same reason the four above were. */
                AbandonedCount = reader.IsDBNull(20) ? 0 : ToInt64(reader.GetValue(20))
            });
        }

        return items;
    }

    /// <summary>
    /// Whether this server has EVER recorded a collector run, ignoring any window.
    /// <para>Lets an empty log read say WHICH kind of nothing it found. "No runs in the last N hours" is
    /// true both of a quiet window and of a server that has never collected, and those want opposite
    /// responses from the caller -- widen the window, versus go find out why collection is not running.
    /// Darling's twin is <c>DarlingDataReader.HasAnyCollectionLogAsync</c>; the two must stay in step so a
    /// user moving between the SKUs is not told a different story about the same state.</para>
    /// </summary>
    public async Task<bool> HasAnyCollectionLogAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_collection_log
WHERE server_id = $1
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Gets recent collection log entries for a server, most recent first, bounded to the tab's
    /// settable window. A preset ends "now" (<paramref name="hoursBack"/> from now); a custom range
    /// (<paramref name="fromDate"/>/<paramref name="toDate"/>, both already server-time) bounds
    /// <c>collection_time</c> on BOTH sides EXACTLY via <see cref="GetTimeRange"/> — mirroring how
    /// <see cref="GetWaitStatsAsync"/> windows its read. The old single now-relative lower bound ignored
    /// the custom To, rounding a custom range to a hours-back-from-now span.
    /// </summary>
    public async Task<List<CollectionLogRow>> GetRecentCollectionLogAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null, int maxRows = 500, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
SELECT
    collector_name,
    collection_time,
    duration_ms,
    sql_duration_ms,
    duckdb_duration_ms,
    rows_collected,
    status,
    error_message,
    server_name
FROM v_collection_log
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY collection_time DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = maxRows });

        var items = new List<CollectionLogRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new CollectionLogRow
            {
                CollectorName = reader.GetString(0),
                CollectionTime = reader.GetDateTime(1),
                DurationMs = reader.IsDBNull(2) ? null : (int?)Convert.ToInt32(reader.GetValue(2)),
                SqlDurationMs = reader.IsDBNull(3) ? null : (int?)Convert.ToInt32(reader.GetValue(3)),
                DuckDbDurationMs = reader.IsDBNull(4) ? null : (int?)Convert.ToInt32(reader.GetValue(4)),
                RowsCollected = reader.IsDBNull(5) ? null : (int?)Convert.ToInt32(reader.GetValue(5)),
                Status = reader.GetString(6),
                ErrorMessage = reader.IsDBNull(7) ? null : reader.GetString(7),
                ServerName = reader.IsDBNull(8) ? null : reader.GetString(8)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets collection log entries for a specific collector on a server.
    /// </summary>
    public async Task<List<CollectionLogRow>> GetCollectionLogByCollectorAsync(int serverId, string collectorName, int hoursBack = 168)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    collector_name,
    collection_time,
    duration_ms,
    sql_duration_ms,
    duckdb_duration_ms,
    rows_collected,
    status,
    error_message,
    server_name
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = $2
AND   collection_time >= $3
ORDER BY collection_time DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = collectorName });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-hoursBack) });

        var items = new List<CollectionLogRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new CollectionLogRow
            {
                CollectorName = reader.GetString(0),
                CollectionTime = reader.GetDateTime(1),
                DurationMs = reader.IsDBNull(2) ? null : (int?)Convert.ToInt32(reader.GetValue(2)),
                SqlDurationMs = reader.IsDBNull(3) ? null : (int?)Convert.ToInt32(reader.GetValue(3)),
                DuckDbDurationMs = reader.IsDBNull(4) ? null : (int?)Convert.ToInt32(reader.GetValue(4)),
                RowsCollected = reader.IsDBNull(5) ? null : (int?)Convert.ToInt32(reader.GetValue(5)),
                Status = reader.GetString(6),
                ErrorMessage = reader.IsDBNull(7) ? null : reader.GetString(7),
                ServerName = reader.IsDBNull(8) ? null : reader.GetString(8)
            });
        }

        return items;
    }
}

public class CollectionLogRow
{
    public string CollectorName { get; set; } = "";
    public string? ServerName { get; set; }
    public DateTime CollectionTime { get; set; }
    public int? DurationMs { get; set; }
    public int? SqlDurationMs { get; set; }
    public int? DuckDbDurationMs { get; set; }
    public int? RowsCollected { get; set; }
    public string Status { get; set; } = "";
    public string? ErrorMessage { get; set; }

    public string CollectionTimeFormatted => CollectionTime.ToLocalTime().ToString("g");

    public string DurationFormatted => DurationMs.HasValue
        ? (DurationMs.Value < 1000 ? $"{DurationMs.Value} ms" : $"{DurationMs.Value / 1000.0:F1} s")
        : "";

    public string SqlDurationFormatted => SqlDurationMs.HasValue ? $"{SqlDurationMs.Value} ms" : "";

    public string DuckDbDurationFormatted => DuckDbDurationMs.HasValue ? $"{DuckDbDurationMs.Value} ms" : "";
}

/// <summary>
/// One Collection Health grid row — a collector's 7-day roll-up with its health band.
/// <see cref="HealthStatus"/> delegates to the shared <see cref="CollectorHealthClassifier"/> in
/// PerformanceMonitor.Common (#1573), so Lite, the Darling viewer, and the service band identically and
/// cannot drift; it resolves the collector's cadence from the shared <see cref="CollectorScheduleDefaults"/>
/// so a healthy DAILY collector is no longer flagged stale/failing on the frequent-collector thresholds.
/// </summary>
public class CollectorHealthRow
{
    public string CollectorName { get; set; } = "";
    public long TotalRuns { get; set; }
    public long SuccessCount { get; set; }
    public long ErrorCount { get; set; }
    public double AvgDurationMs { get; set; }

    /// <summary>
    /// The single worst run in the window (#2460). A FACT, never a decision input: one pathological
    /// cycle would otherwise make a collector read as permanently terrible for seven days. Its job is
    /// to sit beside <see cref="P95DurationMs"/> — when the two agree the tail is routine, and when the
    /// max towers over the p95 the max was a one-off.
    /// </summary>
    public double MaxDurationMs { get; set; }

    /// <summary>
    /// The 95th-percentile run in the window (#2460) — what a HEAVY run of this collector costs, as
    /// opposed to what its runs cost on average. The number the sweep's peak-cycle arithmetic is built
    /// from (via <see cref="PerformanceMonitor.Common.SweepPressureClassifier.PeakRunMs"/>), because a
    /// mean over a bimodal collector describes neither of its populations.
    /// </summary>
    public double P95DurationMs { get; set; }

    public DateTime? LastSuccessTime { get; set; }
    public DateTime? LastRunTime { get; set; }
    public string? LastError { get; set; }
    public DateTime? LastErrorTime { get; set; }
    public long PermissionDeniedCount { get; set; }
    /// <summary>1s lock-timeout yields (#1805) — deliberate, benign, counted apart from errors.</summary>
    public long YieldCount { get; set; }

    /// <summary>Runs the #2673 whole-server wall-clock budget gave up on (#2804). Counted apart from
    /// errors for the same reason <see cref="YieldCount"/> is — a guard firing is not a fault — but unlike
    /// a yield it is data LOSS: the cycle stored nothing and advanced no watermark.</summary>
    public long AbandonedCount { get; set; }

    /// <summary>
    /// The note a non-failing run left behind (#1837): an enumeration that yielded 0 items, items whose
    /// enumeration probe failed. Null for the ordinary run, which is why the column reads blank for a
    /// plainly healthy collector. Informational only — see <see cref="NoteFormatted"/>.
    /// </summary>
    public string? LastNote { get; set; }

    /// <summary>How many of <see cref="TotalRuns"/> carried a <see cref="LastNote"/>.</summary>
    public long NoteCount { get; set; }

    /// <summary>
    /// #1852: whether the store saw user databases on this target inside the health window
    /// (<c>has_user_databases</c>) — what tells a legitimately empty server apart from one that is
    /// enumerating nothing despite having databases. False also covers "no inventory to go on", which
    /// deliberately reads the same as "nothing to say". Informational, like <see cref="LastNote"/>:
    /// <see cref="HealthStatus"/> never sees it.
    /// </summary>
    public bool TargetHasUserDatabases { get; set; }

    /* ── The per-database fan-out rollup (#2472), twinning Darling's CollectorHealth ─────────────────
       Four parts of ONE run — the window's dearest single item and the run that carried it. NULL on
       every collector that does not fan out. The tail statistics above cannot answer this: they
       aggregate over RUNS, and each run is one blended row however many databases it covered. */

    /// <summary>How many items that run fanned out over, or null when it did not fan out.</summary>
    public int? FanoutItems { get; set; }

    /// <summary>The dearest item in the window — a database name, for every fan-out that exists today.</summary>
    public string? SlowestItem { get; set; }

    /// <summary>What that item cost, SQL plus storage.</summary>
    public int? SlowestItemMs { get; set; }

    /// <summary>The whole run that item came from.</summary>
    public int? SlowestRunDurationMs { get; set; }

    /// <summary>The answer, as one number: 1.0 is a perfectly even fan-out and it rises with
    /// concentration. Near 1.0 the cost is the fan-out's WIDTH; around 2.0 or above one database
    /// dominates and a per-database override or a stagger is the lever (#2468). Null when the collector
    /// does not fan out, or when the run's duration is zero — a ratio against nothing is a wrong
    /// answer rather than a smaller one.</summary>
    public double? FanoutDominance =>
        FanoutItems is > 0 && SlowestItemMs.HasValue && SlowestRunDurationMs is > 0
            ? (double)SlowestItemMs.Value * FanoutItems.Value / SlowestRunDurationMs.Value
            : null;

    public double FailureRatePercent => TotalRuns > 0 ? (double)ErrorCount / TotalRuns * 100 : 0;

    /// <summary>Share of runs the #2673 budget abandoned (#2804) — its own rate, not folded into
    /// <see cref="FailureRatePercent"/>, because the two carry very different thresholds and merging
    /// them would report a 2%-abandoning collector as a 2%-erroring one.</summary>
    public double AbandonRatePercent => TotalRuns > 0 ? (double)AbandonedCount / TotalRuns * 100 : 0;
    public double HoursSinceLastSuccess => LastSuccessTime.HasValue
        ? (DateTime.UtcNow - LastSuccessTime.Value).TotalHours
        : 999;

    /// <summary>Hours since the newest run of ANY status — the input <see cref="CollectorHealthClassifier"/>'s
    /// STOPPED band reads. Distinct from <see cref="HoursSinceLastSuccess"/>: a collector that keeps being
    /// invoked and keeps failing has a small value here even while its success clock runs out; a collector
    /// whose gate flipped off and stopped being invoked entirely has a large value here too, which is what
    /// tells the two apart. Falls back to <see cref="HoursSinceLastSuccess"/> rather than the bare 999
    /// sentinel when the column is unset: a run can never be MORE certain than a known success, so absent
    /// better information this must not read more dormant than the success clock alone already says.</summary>
    public double HoursSinceLastRun => LastRunTime.HasValue
        ? (DateTime.UtcNow - LastRunTime.Value).TotalHours
        : HoursSinceLastSuccess;

    /// <summary>The collector's default cadence from the shared <see cref="CollectorScheduleDefaults"/>
    /// (0 for an on-load or unknown collector — both fall to the floor thresholds). The banding uses the
    /// shipped default, not the per-install ScheduleManager override, so all three surfaces stay in parity.
    /// Internal since #2296: the tool's sweep-pressure roll-up amortizes each collector's average
    /// duration by this same cadence, so both readers of it share one resolution.</summary>
    internal int FrequencyMinutes =>
        CollectorScheduleDefaults.All.TryGetValue(CollectorName, out var schedule) ? schedule.FrequencyMinutes : 0;

    public string HealthStatus => CollectorHealthClassifier.Classify(
        TotalRuns, SuccessCount, ErrorCount, PermissionDeniedCount, AbandonedCount,
        HoursSinceLastSuccess, HoursSinceLastRun, FrequencyMinutes, CollectorHealthClassifier.IsOnLoadCollector(CollectorName));

    public string AvgDurationFormatted => AvgDurationMs < 1000
        ? $"{AvgDurationMs:F0} ms"
        : $"{AvgDurationMs / 1000:F1} s";

    public string LastSuccessFormatted => LastSuccessTime.HasValue
        ? LastSuccessTime.Value.ToLocalTime().ToString("g")
        : "Never";

    public string LastRunFormatted => LastRunTime.HasValue
        ? LastRunTime.Value.ToLocalTime().ToString("g")
        : "Never";

    public string LastErrorFormatted => LastErrorTime.HasValue
        ? LastErrorTime.Value.ToLocalTime().ToString("g")
        : "";

    /// <summary>
    /// The informational note plus its "all N runs" / "N of M runs" qualifier (#1837) and, when a
    /// persistently empty enumeration lands on a target the store has seen user databases on, #1852's
    /// "target has user databases" — or blank. Shared with the Darling Viewer through
    /// <see cref="CollectorHealthClassifier.FormatCollectionNote"/> so the two apps' health grids read
    /// identically. Never feeds <see cref="HealthStatus"/>.
    /// </summary>
    public string NoteFormatted =>
        CollectorHealthClassifier.FormatCollectionNote(LastNote, NoteCount, TotalRuns, CollectorName, TargetHasUserDatabases);
}

