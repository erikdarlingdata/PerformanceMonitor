/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Collection Health tab's three ported reads (W1i), copied from Lite's
/// <c>LocalDataService.CollectionHealth.cs</c> and run on the <c>v_collection_log</c> passthrough view.
/// This REPLACES the shell's placeholder Collection Health read (the DISTINCT-ON latest-run-per-collector
/// query and its simple <c>CollectorHealthRow</c> record, both removed from <c>ViewerDataService.cs</c>)
/// with Lite's rich 7-day aggregate: per-collector run/success/error counts, average duration, last
/// success / last run / last error timestamps, and the <see cref="CollectorHealthRow.HealthStatus"/>
/// banding. The SQL is byte-portable between DuckDB and Postgres (positional params, plain aggregates),
/// so only the parameter binding differs: window <see cref="DateTime"/>s go in with
/// <c>DateTimeKind.Unspecified</c> (the naive-UTC store convention), and the SUM/COUNT/AVG results are
/// read type-agnostically (Postgres returns <c>bigint</c> for the counts and <c>numeric</c> for AVG,
/// where DuckDB returned HUGEINT/DECIMAL) — the same intent as Lite's <c>ToInt64</c>/<c>ToDouble</c>
/// helpers, minus the DuckDB BigInteger case Postgres never produces.
/// <para>
/// The <b>Health Summary</b> aggregate keeps Lite's fixed 7-day horizon (its staleness banding needs a
/// stable window regardless of the toolbar). The <b>Collection Log</b> read (feeding the log grid + the
/// Duration Trends chart) diverges from Lite in ONE deliberate way: it bounds <c>collection_time</c> on
/// BOTH sides so the per-server toolbar's custom From/To is honored EXACTLY — Lite (and this file's first
/// port) took a single now-relative <c>hoursBack</c> lower bound, which rounded a custom range to a
/// hours-back-from-now span.
/// </para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Lite's 7-day per-collector health aggregate (<c>GetCollectionHealthAsync</c>) verbatim: one row
    /// per collector over the trailing window, with the SKIPPED-counts-as-a-healthy-run rule baked into
    /// the last-success MAX and the PERMISSIONS bucket split out for the NO_PERMISSIONS banding. $1
    /// server_id, $2 window start (naive UTC).
    /// </summary>
    public const string CollectionHealthSql = """
        SELECT
            collector_name,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
            AVG(duration_ms) AS avg_duration_ms,
            -- SKIPPED counts as a healthy run (dedup / version-gated collectors no-op without being stale)
            MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
            MAX(collection_time) AS last_run_time,
            -- #1855: the message from the NEWEST failing run, not MAX()'s lexicographically greatest
            -- one. The status re-check is load-bearing rather than belt-and-braces: when no failing run
            -- in the window carried text, error_rank = 1 falls through to the newest row of ANY class,
            -- and without it a SUCCESS row's note could surface here as a fake last error.
            MAX(CASE WHEN error_rank = 1 AND status IN ('ERROR', 'PERMISSIONS') THEN error_message END) AS last_error,
            -- The newest failure OUTRIGHT, text or not — "when did this last fail" means the run, not
            -- the message. It can only name a different row than last_error if a failure was written
            -- with no text.
            MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS') THEN collection_time END) AS last_error_time,
            SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
            -- YIELDED = the 1s LOCK_TIMEOUT guard fired (#1805): deliberate, benign for collection,
            -- counted apart from errors because clustering here is a signal about the TARGET's lock
            -- contention rather than a monitoring fault.
            SUM(CASE WHEN status = 'YIELDED' THEN 1 ELSE 0 END) AS yield_count,
            -- #1837: the note a SUCCEEDING run can leave behind (an enumeration that yielded 0 items,
            -- items whose enumeration probe failed). Gated on SUCCESS specifically, rather than on
            -- every non-failure status: the runners attach a note only to the SUCCESS write, and the looser
            -- complement would drag SESSION_MISSING and CANCELLED messages into a column whose whole
            -- claim is that it is NOT an error. Display text only: no band, no count, no threshold
            -- reads it, and a legitimately empty target stays HEALTHY exactly as before.
            MAX(CASE WHEN note_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS last_note,
            -- How many of the window's runs carried one. note_count = total_runs is the
            -- persistently-empty signal: EVERY run this week came back with nothing.
            COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count,
            -- #1852: the one thing that makes a persistently-empty enumeration interesting — does this
            -- target actually HAVE user databases? Zero items on a server with none is legitimate and
            -- stays quiet; zero items on a server that HAS them is a login that cannot enter any, or a
            -- filter that swallowed everything. database_size_stats rather than database_config for
            -- three reasons: it runs on the scheduled loop (60 min) where database_config is on-load
            -- and can age past this window on a long-running service; it is indexed on
            -- (server_id, collection_time) where database_config has no index at all; and it reads
            -- sys.master_files, so it still sees databases the monitoring login cannot ENTER — exactly
            -- the case being diagnosed. database_id > 4 excludes the system databases, tempdb
            -- included: the size collector takes every ONLINE database, so a bare row check
            -- would be true on every server alive.
            --
            -- The inventory window is the health read's OWN ($2) — no second parameter, and an
            -- inventory that aged out says nothing rather than something stale. Uncorrelated, so both
            -- engines evaluate it once per query (a Postgres InitPlan) instead of per row, and it
            -- needs no GROUP BY entry and no join. Feeds display text only, through the shared
            -- formatter; the banding never sees it.
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
            -- #2804: runs the #2673 wall-clock budget abandoned. Appended last — this result set is
            -- read positionally by one shared mapper serving BOTH the per-server and fleet reads.
            SUM(CASE WHEN status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned_count
        FROM
        (
            -- #1855: rank each class of message newest-first so the two exemplar columns above can take
            -- the LATEST one instead of the lexicographically greatest. Ordering on whether the class's
            -- CASE came back empty puts every row that carries such a message ahead of every row that
            -- does not, so rank 1 is the newest one that has text — and a later clean run no longer
            -- blanks a note the window still holds. MAX() was never wrong about WHICH rows to consider,
            -- only about which of them wins, and text does not sort like the number #1837's probe note
            -- carries: 12 item(s) sorts below 9 item(s). collection_time settles it; error_message DESC
            -- only breaks an exact-timestamp tie, and breaks it identically here and in Lite's DuckDB
            -- twin, which binary-vs-locale collation would not.
            SELECT
                collector_name,
                collection_time,
                duration_ms,
                status,
                error_message,
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
                ) AS error_rank
            FROM v_collection_log
            WHERE server_id = $1
            AND   collection_time >= $2
        ) runs
        GROUP BY collector_name
        ORDER BY collector_name
        """;

    /// <summary>
    /// #1591: how many DISTINCT collectors were permission-denied in the window — the badge count for the
    /// Collection Health tab header. Lite's twin is
    /// <c>LocalDataService.GetPermissionDeniedCollectorCountAsync</c>.
    ///
    /// <para>Its own narrow COUNT rather than a reuse of <see cref="CollectionHealthSql"/>: that one is
    /// per-collector and only runs when its tab is selected, which is exactly why a permission problem stayed
    /// invisible until someone thought to look. Counts collectors, not rows, so one collector failing every cycle
    /// for a week reads as "1" rather than a meaningless four-figure number.</para>
    /// </summary>
    public const string PermissionDeniedCollectorCountSql = """
        SELECT COUNT(DISTINCT collector_name)
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   status = 'PERMISSIONS'
        """;

    /// <summary>Runs <see cref="PermissionDeniedCollectorCountSql"/> over the same 7-day window the health grid uses.</summary>
    public async Task<int> GetPermissionDeniedCollectorCountAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(PermissionDeniedCollectorCountSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified) });

        var scalar = await command.ExecuteScalarAsync(cancellationToken);
        return scalar is null or DBNull ? 0 : Convert.ToInt32(scalar);
    }

    /// <summary>
    /// The fleet-cumulative variant of <see cref="CollectionHealthSql"/>: the same 15-column per-collector
    /// aggregate but across ALL enabled monitored servers (GROUP BY server_id, collector_name — one row per
    /// server/collector pair), for the status bar's aggregate-view total (mirrors Lite's cumulative
    /// GetHealthSummary(null)). Scoped to enabled servers so a removed server's aged-out rows don't read as
    /// erroring. $1 window start (naive UTC).
    /// <para>
    /// The two exemplar MESSAGE columns are deliberately NULL here rather than ranked as the per-server
    /// read ranks them (#1855). This query's only caller is <c>UpdateCollectorHealthTextAsync</c>, which
    /// reads <see cref="CollectorHealthRow.HealthStatus"/> and the collector NAME — no surface renders a
    /// fleet row's message, and the per-server read behind the Collection Health grid is where the note
    /// and the last error are actually shown. Ranking them costs more than the whole rest of the query:
    /// PostgreSQL cannot parallelize above a WindowAgg, so adding the ranks turned this from a parallel
    /// hash aggregate into a serial sort of every row in the window — 0.84s to 13.9s over a 200-server /
    /// 4M-row store, measured on PG 18.4, on a status-bar refresh that would display none of it. NULL,
    /// not the old lexicographic MAX: a fleet rollup carries band INPUTS, exactly like the service-side
    /// <c>DarlingFleetReader.FleetCollectionHealthSql</c>, which projects no message columns at all. The
    /// columns stay in the projection because both reads share <see cref="MapHealthRow"/> and its
    /// ordinals; a surface that ever needs fleet-wide exemplars needs a different read, not this one.
    /// </para>
    /// <para>
    /// #1852's <c>has_user_databases</c> is NULL here for the same two reasons and one more. It qualifies
    /// a DISPLAYED note, and this read displays none — with <c>last_note</c> already NULL the shared
    /// formatter returns blank whatever the flag says, so a real value could not change one rendered
    /// character. And the flag is per-SERVER while this read groups server_id INTO the result, so there is
    /// no single $1 to probe the inventory for: a truthful fleet version would be a second cross-collector
    /// join across every enabled server, on a query the status bar re-runs on every aggregate-tab refresh
    /// — precisely the cost #1855 measured and declined. The column exists to hold the ordinal.
    /// </para>
    /// </summary>
    public const string FleetCollectionHealthSql = """
        SELECT
            collector_name,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
            AVG(duration_ms) AS avg_duration_ms,
            MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
            MAX(collection_time) AS last_run_time,
            CAST(NULL AS text) AS last_error,
            MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS') THEN collection_time END) AS last_error_time,
            SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
            SUM(CASE WHEN status = 'YIELDED' THEN 1 ELSE 0 END) AS yield_count,
            CAST(NULL AS text) AS last_note,
            COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count,
            CAST(NULL AS integer) AS has_user_databases,
            -- #2804: runs the #2673 wall-clock budget abandoned. Appended last — this result set is
            -- read positionally by one shared mapper serving BOTH the per-server and fleet reads.
            SUM(CASE WHEN status = 'ABANDONED' THEN 1 ELSE 0 END) AS abandoned_count
        FROM v_collection_log
        WHERE collection_time >= $1
        AND   server_id IN (SELECT server_id FROM config_monitored_servers WHERE is_enabled)
        GROUP BY server_id, collector_name
        """;

    /// <summary>
    /// The Collection Log sub-tab's recent-log read, adapted from Lite's <c>GetRecentCollectionLogAsync</c>
    /// to honor the per-server toolbar's settable window EXACTLY: the collection_log rows for one server
    /// between the window's start and end bounds, newest first, capped. Where Lite (and the shell's first
    /// port) passed a single now-relative <c>hoursBack</c> lower bound — so a custom From/To rounded to a
    /// hours-back-from-now span — this bounds <c>collection_time</c> on BOTH sides, matching how the Wait
    /// Stats / Blocking tabs window their reads. $1 server_id, $2 window start, $3 window end (all naive
    /// UTC), $4 row cap.
    /// </summary>
    public const string RecentCollectionLogSql = """
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
        LIMIT $4
        """;

    /// <summary>
    /// Lite's per-collector drill read (<c>GetCollectionLogByCollectorAsync</c>) verbatim: every
    /// collection_log row for one collector on one server since the window start, newest first. Feeds
    /// the CollectionLogWindow the Health Summary grid opens on double-click. $1 server_id, $2
    /// collector_name, $3 window start (naive UTC).
    /// </summary>
    public const string CollectionLogByCollectorSql = """
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
        ORDER BY collection_time DESC
        """;

    /// <summary>
    /// Per-collector health summary for one server over the trailing 7 days. Copied from Lite's
    /// <c>GetCollectionHealthAsync</c> — same window, same columns, HealthStatus computed on the row.
    /// </summary>
    public async Task<List<CollectorHealthRow>> GetCollectionHealthAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var items = new List<CollectorHealthRow>();

        await using var command = _dataSource.CreateCommand(CollectionHealthSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified),
        });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(MapHealthRow(reader));
        }

        return items;
    }

    /// <summary>
    /// Fleet-cumulative per-collector health across ALL enabled monitored servers over the trailing 7 days —
    /// the status bar's aggregate-view total (Overview / Alert History / FinOps / Recommendations), where Lite
    /// shows a cumulative count rather than one server's. ONE query (<see cref="FleetCollectionHealthSql"/>)
    /// regardless of fleet size; each row is a (server, collector) pair carrying its own
    /// <see cref="CollectorHealthRow.HealthStatus"/>, so the caller counts total + FAILING exactly as per-server.
    /// </summary>
    public async Task<List<CollectorHealthRow>> GetFleetCollectionHealthAsync(CancellationToken cancellationToken = default)
    {
        var items = new List<CollectorHealthRow>();

        await using var command = _dataSource.CreateCommand(FleetCollectionHealthSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified),
        });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(MapHealthRow(reader));
        }

        return items;
    }

    /// <summary>Maps one row of the shared 15-column health projection (per-server or fleet, ordinals 0-14) to a
    /// <see cref="CollectorHealthRow"/>. The count is load-bearing: both projections are read POSITIONALLY
    /// through this one mapper, so it must match them exactly (15 since #2804 appended abandoned_count at
    /// ordinal 14).</summary>
    private static CollectorHealthRow MapHealthRow(NpgsqlDataReader reader) => new()
    {
        CollectorName = reader.GetString(0),
        TotalRuns = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
        SuccessCount = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
        ErrorCount = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
        AvgDurationMs = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
        LastSuccessTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
        LastRunTime = reader.IsDBNull(6) ? null : reader.GetDateTime(6),
        LastError = reader.IsDBNull(7) ? null : reader.GetString(7),
        LastErrorTime = reader.IsDBNull(8) ? null : reader.GetDateTime(8),
        PermissionDeniedCount = reader.IsDBNull(9) ? 0 : Convert.ToInt64(reader.GetValue(9)),
        YieldCount = reader.IsDBNull(10) ? 0 : Convert.ToInt64(reader.GetValue(10)),
        LastNote = reader.IsDBNull(11) ? null : reader.GetString(11),
        NoteCount = reader.IsDBNull(12) ? 0 : Convert.ToInt64(reader.GetValue(12)),
        /* #1852: NULL on the fleet read (see FleetCollectionHealthSql) reads as "no inventory to go on",
           which is the same silence an install with no size stats gets. */
        TargetHasUserDatabases = !reader.IsDBNull(13) && Convert.ToInt64(reader.GetValue(13)) != 0,
        /* Appended (#2804). Both reads above compute it, so unlike has_user_databases it is never a
           NULL placeholder on the fleet side — an abandoned cycle is data loss on either surface. */
        AbandonedCount = reader.IsDBNull(14) ? 0 : Convert.ToInt64(reader.GetValue(14)),
    };

    /// <summary>
    /// Collection_log entries for one server between the window's naive-UTC start/end bounds, most recent
    /// first (500-row cap). Feeds the Collection Log sub-tab grid and the Duration Trends chart. The window
    /// is the per-server toolbar's settable range (<c>GetWindowUtc</c>): a preset ends "now", a custom
    /// From/To bounds EXACTLY — unlike the old hours-back read, a custom range no longer rounds to a
    /// hours-back-from-now span. Mirrors how <see cref="GetDistinctWaitTypesAsync"/> windows its read.
    /// </summary>
    public async Task<List<CollectionLogRow>> GetRecentCollectionLogAsync(int serverId, DateTime startUtc, DateTime endUtc, int maxRows = 500, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(RecentCollectionLogSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = maxRows });

        return await ReadCollectionLogAsync(command, cancellationToken);
    }

    /// <summary>
    /// Collection_log entries for a specific collector on one server, most recent first. Copied from
    /// Lite's <c>GetCollectionLogByCollectorAsync</c> (default 168-hour / 7-day window). Feeds the
    /// CollectionLogWindow drill.
    /// </summary>
    public async Task<List<CollectionLogRow>> GetCollectionLogByCollectorAsync(int serverId, string collectorName, int hoursBack = 168, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(CollectionLogByCollectorSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = collectorName });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-hoursBack), DateTimeKind.Unspecified),
        });

        return await ReadCollectionLogAsync(command, cancellationToken);
    }

    /// <summary>Shared reader for the two collection-log projections (identical column list).</summary>
    private static async Task<List<CollectionLogRow>> ReadCollectionLogAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var items = new List<CollectionLogRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new CollectionLogRow
            {
                CollectorName = reader.GetString(0),
                CollectionTime = reader.GetDateTime(1),
                DurationMs = reader.IsDBNull(2) ? null : Convert.ToInt32(reader.GetValue(2)),
                SqlDurationMs = reader.IsDBNull(3) ? null : Convert.ToInt32(reader.GetValue(3)),
                DuckDbDurationMs = reader.IsDBNull(4) ? null : Convert.ToInt32(reader.GetValue(4)),
                RowsCollected = reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5)),
                Status = reader.GetString(6),
                ErrorMessage = reader.IsDBNull(7) ? null : reader.GetString(7),
                ServerName = reader.IsDBNull(8) ? null : reader.GetString(8),
            });
        }

        return items;
    }
}

/// <summary>
/// One row of the Collection Log grid / drill window — a single collector run's outcome. Copied
/// VERBATIM from Lite's <c>CollectionLogRow</c> (LocalDataService.CollectionHealth.cs): every display
/// property is a pure format of stored values, and <see cref="CollectionTimeFormatted"/> routes the
/// store's naive-UTC collection_time through <see cref="ViewerTimeHelper.ForDisplay"/> — the viewer's
/// mode-aware Server/Local/UTC conversion every other Darling timestamp also uses.
/// <see cref="DuckDbDurationMs"/> keeps its store column name (<c>duckdb_duration_ms</c>)
/// but in the Darling store that column records the POSTGRES write phase — the Collection Log grid
/// labels it "Store (ms)".
/// </summary>
public class CollectionLogRow
{
    public string CollectorName { get; set; } = "";
    public string? ServerName { get; set; }
    public DateTime CollectionTime { get; set; }
    public int? DurationMs { get; set; }
    public int? SqlDurationMs { get; set; }

    /// <summary>Stored under the legacy <c>duckdb_duration_ms</c> column name; in the Darling store it
    /// carries the Postgres storage-phase milliseconds. Surfaced as the grid's "Store (ms)" column.</summary>
    public int? DuckDbDurationMs { get; set; }
    public int? RowsCollected { get; set; }
    public string Status { get; set; } = "";
    public string? ErrorMessage { get; set; }

    public string CollectionTimeFormatted => ViewerTimeHelper.ForDisplay(CollectionTime).ToString("g");

    public string DurationFormatted => DurationMs.HasValue
        ? (DurationMs.Value < 1000 ? $"{DurationMs.Value} ms" : $"{DurationMs.Value / 1000.0:F1} s")
        : "";

    public string SqlDurationFormatted => SqlDurationMs.HasValue ? $"{SqlDurationMs.Value} ms" : "";

    public string DuckDbDurationFormatted => DuckDbDurationMs.HasValue ? $"{DuckDbDurationMs.Value} ms" : "";
}

/// <summary>
/// One Collection Health "Health Summary" grid row — a collector's 7-day roll-up with its health band.
/// Copied VERBATIM from Lite's rich <c>CollectorHealthRow</c> (LocalDataService.CollectionHealth.cs);
/// it REPLACES the shell's placeholder <c>CollectorHealthRow</c> record (a single latest-run snapshot).
/// Every property is a pure computation over the aggregate — <see cref="HealthStatus"/> delegates to the
/// shared <see cref="CollectorHealthClassifier"/> in PerformanceMonitor.Common (#1573), so Lite, this
/// viewer, and the service band identically and cannot drift; it resolves the collector's cadence from the
/// shared <see cref="CollectorScheduleDefaults"/> so a healthy DAILY collector is no longer flagged
/// stale/failing on the frequent-collector thresholds. The <see cref="DateTime.UtcNow"/> arithmetic in
/// <see cref="HoursSinceLastSuccess"/> is correct against the store's naive-UTC timestamps because both
/// sides are UTC instants (tick subtraction ignores Kind), matching Lite.
/// </summary>
public class CollectorHealthRow
{
    public string CollectorName { get; set; } = "";
    public long TotalRuns { get; set; }
    public long SuccessCount { get; set; }
    public long ErrorCount { get; set; }
    public double AvgDurationMs { get; set; }
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
    /// enumerating nothing despite having databases. False also covers "no inventory to go on" (the fleet
    /// read's NULL, an install without size stats), which deliberately reads the same as "nothing to say".
    /// Informational, like <see cref="LastNote"/>: <see cref="HealthStatus"/> never sees it.
    /// </summary>
    public bool TargetHasUserDatabases { get; set; }

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
    /// shipped default, not any per-install override: the viewer has no cheap per-collector effective
    /// frequency at the row level, and using the same default across all three surfaces keeps them in parity.</summary>
    private int FrequencyMinutes =>
        CollectorScheduleDefaults.All.TryGetValue(CollectorName, out var schedule) ? schedule.FrequencyMinutes : 0;

    public string HealthStatus => CollectorHealthClassifier.Classify(
        TotalRuns, SuccessCount, ErrorCount, PermissionDeniedCount, AbandonedCount,
        HoursSinceLastSuccess, HoursSinceLastRun, FrequencyMinutes, CollectorHealthClassifier.IsOnLoadCollector(CollectorName));

    public string AvgDurationFormatted => AvgDurationMs < 1000
        ? $"{AvgDurationMs:F0} ms"
        : $"{AvgDurationMs / 1000:F1} s";

    public string LastSuccessFormatted => LastSuccessTime.HasValue
        ? ViewerTimeHelper.ForDisplay(LastSuccessTime.Value).ToString("g")
        : "Never";

    public string LastRunFormatted => LastRunTime.HasValue
        ? ViewerTimeHelper.ForDisplay(LastRunTime.Value).ToString("g")
        : "Never";

    public string LastErrorFormatted => LastErrorTime.HasValue
        ? ViewerTimeHelper.ForDisplay(LastErrorTime.Value).ToString("g")
        : "";

    /// <summary>
    /// The informational note plus its "all N runs" / "N of M runs" qualifier (#1837) and, when a
    /// persistently empty enumeration lands on a target the store has seen user databases on, #1852's
    /// "target has user databases" — or blank. Shared with Lite through
    /// <see cref="CollectorHealthClassifier.FormatCollectionNote"/> so the two apps' health grids read
    /// identically. Never feeds <see cref="HealthStatus"/>.
    /// </summary>
    public string NoteFormatted =>
        CollectorHealthClassifier.FormatCollectionNote(LastNote, NoteCount, TotalRuns, CollectorName, TargetHasUserDatabases);
}
