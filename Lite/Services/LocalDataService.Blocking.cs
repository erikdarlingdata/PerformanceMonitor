/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using static PerformanceMonitor.Common.DeadlockGraphProcessParser;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The CURRENT total blocked wait time (#1839): the newest <c>dmv_blocking_snapshots</c> snapshot's
    /// summed <c>wait_time_ms</c> and distinct blocked-SPID count, or null when the store holds no
    /// snapshot for the server. Freshness is judged by the caller (the alert adapter, which knows the
    /// server's effective cadence) — this read reports the snapshot's time, not a verdict on it.
    /// <para>
    /// Scoped to ONE snapshot by <c>collection_time = MAX(collection_time)</c>, never a time window:
    /// summing a window would add up blocking that has already ended, and the alert could then never
    /// clear while the window still held it. Reads the archive-aware view for the same reason
    /// <see cref="GetLatestRunningJobsSnapshotTimeAsync"/> does — after a 512 MB reset the newest
    /// snapshot can live only in parquet, and it must read as its true (possibly stale) age rather than
    /// as absent.
    /// </para>
    /// </summary>
    public async Task<(DateTime SnapshotTime, long TotalWaitMs, int BlockedSessionCount)?> GetCurrentBlockingWaitAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        /* The two CASTs are load-bearing, not decoration: DuckDB widens SUM(BIGINT) to HUGEINT, which
           DuckDB.NET materializes as System.Numerics.BigInteger — a type Convert.ToInt64 cannot convert
           (BigInteger does not implement IConvertible), so the read threw at alert-check time without
           them. COUNT is cast for the same reason its result is read as an int. */
        command.CommandText = @"
SELECT
    collection_time,
    CAST(COALESCE(SUM(wait_time_ms), 0) AS BIGINT) AS total_wait_ms,
    CAST(COUNT(DISTINCT blocked_spid) AS INTEGER) AS blocked_sessions
FROM v_dmv_blocking_snapshots
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM v_dmv_blocking_snapshots
    WHERE server_id = $1
)
GROUP BY collection_time";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return (
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? 0L : reader.GetInt64(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt32(2));
    }

    /// <summary>The Deadlocks grid's row cap — the default <paramref name="limit"/> of
    /// <see cref="GetRecentDeadlocksAsync"/>, so every grid caller reads exactly what it always read.</summary>
    public const int DeadlockGridCap = 50;

    /// <summary>
    /// Gets recent deadlock events for a server, newest first, capped at <paramref name="limit"/>.
    ///
    /// <para>The cap is a PARAMETER with the grid's value as its default (#3541 A3). It was <c>LIMIT 50</c>
    /// under an MCP tool that advertised <c>limit</c> and applied it with <c>Take(limit)</c>, so a caller
    /// asking for 100 deadlocks silently got 50 and a payload that called them <c>total_deadlocks</c>. The
    /// MCP tools now pass <c>limit + 1</c> and read the extra row as the truncation signal; the grids pass
    /// nothing and keep their 50.</para>
    ///
    /// <para><paramref name="graphOnly"/> restricts the read to rows that CARRY a graph, in SQL, so
    /// <c>get_deadlock_detail</c>'s <c>limit</c> counts graphs rather than rows it would have to discard —
    /// filtering for XML in C# after a capped fetch was the shape of the defect, where a run of graph-less rows
    /// at the newest end read as "no XML in the window" while older graphs sat behind the cap.</para>
    /// </summary>
    public async Task<List<DeadlockRow>> GetRecentDeadlocksAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null, int limit = DeadlockGridCap, bool graphOnly = false, bool windowOnCollectionTime = false)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        var graphClause = graphOnly
            ? @"
AND   deadlock_graph_xml IS NOT NULL
AND   deadlock_graph_xml <> ''"
            : string.Empty;

        /* The grid answers "what deadlocked in this window", so it windows on deadlock_time. The alert engine
           passes windowOnCollectionTime: its read is a delivery cursor, and on the event time a deadlock collected
           late (seconds, or hours after an outage) would fall out of the window before it ever alerted. */
        var windowCol = windowOnCollectionTime ? "collection_time" : "deadlock_time";
        command.CommandText = @"
SELECT
    collection_time,
    deadlock_time,
    victim_process_id,
    victim_sql_text,
    deadlock_graph_xml,
    database_name
FROM " + StoredEventCopies.Deadlocks(
            windowOnCollectionTime
                ? "server_id = $1 AND collection_time <= $3" + graphClause
                : "server_id = $1 AND deadlock_time >= $2 AND deadlock_time <= $3" + graphClause,
            windowOnCollectionTime ? "$2" : null) + @" AS dl
ORDER BY deadlock_time DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = limit });

        var items = new List<DeadlockRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DeadlockRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeadlockTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                VictimProcessId = reader.IsDBNull(2) ? "" : reader.GetString(2),
                VictimSqlText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                DeadlockGraphXml = reader.IsDBNull(4) ? "" : reader.GetString(4),
                DatabaseName = reader.IsDBNull(5) ? null : reader.GetString(5)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets hourly-bucketed metrics from query snapshots for the time-range slicer.
    /// The metric column is determined by the caller's sort preference.
    /// </summary>
    public async Task<List<TimeSliceBucket>> GetActiveQuerySlicerDataAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
SELECT
    date_trunc('hour', collection_time) AS bucket,
    COUNT(*) AS session_count,
    COALESCE(SUM(cpu_time_ms), 0) AS total_cpu,
    COALESCE(SUM(total_elapsed_time_ms), 0) AS total_elapsed,
    COALESCE(SUM(reads), 0) AS total_reads,
    COALESCE(SUM(logical_reads), 0) AS total_logical_reads,
    COALESCE(SUM(writes), 0) AS total_writes
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
GROUP BY date_trunc('hour', collection_time)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<TimeSliceBucket>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                TotalCpu = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                TotalElapsed = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalLogicalReads = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TotalWrites = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                Value = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)), // default: session count
            });
        }

        return items;
    }

    /// <summary>
    /// Gets query snapshots (currently running queries) for a server.
    /// </summary>
    public async Task<List<QuerySnapshotRow>> GetLatestQuerySnapshotsAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetLatestQuerySnapshotsAsync", "v_query_snapshots latest");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
SELECT
    session_id,
    database_name,
    elapsed_time_formatted,
    query_text,
    status,
    blocking_session_id,
    wait_type,
    wait_time_ms,
    wait_resource,
    cpu_time_ms,
    total_elapsed_time_ms,
    reads,
    writes,
    logical_reads,
    granted_query_memory_gb,
    transaction_isolation_level,
    dop,
    parallel_worker_count,
    query_plan IS NOT NULL AS has_query_plan,
    live_query_plan IS NOT NULL AS has_live_query_plan,
    collection_time,
    login_name,
    host_name,
    program_name,
    open_transaction_count,
    percent_complete,
    query_hash,
    request_id
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
AND   query_text NOT LIKE 'WAITFOR%'
ORDER BY collection_time DESC, cpu_time_ms DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QuerySnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QuerySnapshotRow
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? "" : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Status = reader.IsDBNull(4) ? "" : reader.GetString(4),
                BlockingSessionId = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                WaitType = reader.IsDBNull(6) ? "" : reader.GetString(6),
                WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                CpuTimeMs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalElapsedTimeMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                Reads = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                Writes = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                LogicalReads = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                GrantedQueryMemoryGb = reader.IsDBNull(14) ? 0 : ToDouble(reader.GetValue(14)),
                TransactionIsolationLevel = reader.IsDBNull(15) ? "" : reader.GetString(15),
                Dop = reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
                ParallelWorkerCount = reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                HasQueryPlan = reader.IsDBNull(18) ? false : reader.GetBoolean(18),
                HasLiveQueryPlan = reader.IsDBNull(19) ? false : reader.GetBoolean(19),
                CollectionTime = reader.IsDBNull(20) ? DateTime.MinValue : reader.GetDateTime(20),
                LoginName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                HostName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                ProgramName = reader.IsDBNull(23) ? "" : reader.GetString(23),
                OpenTransactionCount = reader.IsDBNull(24) ? 0 : reader.GetInt32(24),
                PercentComplete = reader.IsDBNull(25) ? 0m : Convert.ToDecimal(reader.GetValue(25)),
                QueryHash = reader.IsDBNull(26) ? "" : reader.GetString(26),
                RequestId = reader.IsDBNull(27) ? 0 : reader.GetInt32(27)
            });
        }

        return items;
    }

    /// <summary>Selects <c>query_plan</c> on <c>false</c>, <c>live_query_plan</c> on <c>true</c> — SQL can't
    /// parameterize a column name, so <see cref="GetSnapshotPlanTextAsync"/> picks the text at call time.</summary>
    private static string SnapshotPlanColumn(bool live) => live ? "live_query_plan" : "query_plan";

    /// <summary>
    /// On-demand fetch of ONE snapshot's plan XML, by its capture key (#4239). The bulk reads
    /// (<see cref="GetLatestQuerySnapshotsAsync"/>, <see cref="GetQuerySnapshotsByWaitTypeAsync"/>,
    /// <see cref="GetAllQuerySnapshotsInRangeAsync"/>) stopped selecting this payload for every row in the
    /// window — on a busy server it was megabytes of plan XML for grid rows nobody clicks. The plan buttons
    /// call this instead, scoped to the one row the user picked.
    ///
    /// <para>Reads <c>v_query_snapshots</c> — the SAME archive-aware view the three bulk reads use — not the
    /// bare <c>query_snapshots</c> table. A snapshot old enough to have been archived to parquet is still
    /// shown by those reads (and its plan is still in the parquet copy), so a fetcher scoped to the live
    /// table alone would silently regress every archived row to "no plan available".</para>
    ///
    /// <para><c>(server_id, collection_time, session_id, request_id)</c> is unique by construction — the SQL
    /// Server collector query is provably unique per (session_id, request_id) per collection tick (the only
    /// join that could fan out is wrapped in an aggregate with no GROUP BY, so it always collapses to one
    /// row: see QuerySnapshotsCollector.cs). It is NOT enforced by a constraint — query_snapshots is
    /// bulk-appended with no PK, same as its Postgres counterpart — so LIMIT 1 is a defensive guard against a
    /// freak duplicate, not a real expectation. <c>request_id</c> reads back NULL for rows collected before
    /// schema v34 added the column (DuckDbInitializer ~1066) or archived before that migration, via parquet's
    /// union-by-name; every reader above already defaults a null request_id to 0, so the match does too.</para>
    ///
    /// <para>The codebase's usual tie-break idiom for "no PK, need one deterministic row"
    /// (<c>QueryStoreSliceRepairService</c>'s <c>ORDER BY ... , rowid DESC</c>) does not reach here:
    /// <c>v_query_snapshots</c> is a UNION ALL of a live table and <c>read_parquet()</c> (query_snapshots
    /// has no dedup key in <c>ArchiveViewDedupKeys</c>, so it is a plain union with no QUALIFY, as <c>v_deadlocks</c> is too; a deadlock's copies are dropped per read, not in the view), and DuckDB does
    /// not propagate the <c>rowid</c> pseudocolumn through a UNION or a <c>SELECT *</c> view. Unlike
    /// <c>config_alert_log</c>, this view carries no 'live'/'archive' <c>source</c> literal to break a tie on
    /// either — there is nothing left to order by beyond the WHERE match itself.</para>
    ///
    /// <para><c>AND {column} IS NOT NULL</c> (#4297) is that missing tie-break for the ONE thing that
    /// matters: on a freak duplicate sharing this key, <c>LIMIT 1</c> alone can land on the row whose plan is
    /// NULL while a sibling matching row carries the real one — silently reporting "no plan available" for a
    /// row that has one. The guard drops the NULL-plan candidate first, so <c>LIMIT 1</c> only ever breaks a
    /// tie among plan-bearing rows (the same captured plan either way). Mirrors Darling's
    /// <c>QuerySnapshotEstimatedPlanSql</c> / <c>QuerySnapshotLivePlanSql</c>
    /// (<c>ViewerDataService.QuerySnapshots.cs</c>), which guards this same read the same way.</para>
    /// </summary>
    public async Task<string?> GetSnapshotPlanTextAsync(int serverId, DateTime collectionTime, int sessionId, int requestId, bool live)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var column = SnapshotPlanColumn(live);

        command.CommandText = $@"
SELECT {column}
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time = $2
AND   session_id = $3
AND   COALESCE(request_id, 0) = $4
AND   {column} IS NOT NULL
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        command.Parameters.Add(new DuckDBParameter { Value = sessionId });
        command.Parameters.Add(new DuckDBParameter { Value = requestId });

        var result = await command.ExecuteScalarAsync();
        return result is null or DBNull ? null : (string)result;
    }

    /// <summary>Estimated plan for a grid row: the in-row <see cref="QuerySnapshotRow.QueryPlan"/> when a
    /// caller (the Live Snapshot handler) already populated it, else a store fetch gated on
    /// <see cref="QuerySnapshotRow.HasQueryPlan"/> so a row that never had a plan never reaches the store.</summary>
    public Task<string?> ResolveSnapshotEstimatedPlanAsync(int serverId, QuerySnapshotRow row)
    {
        if (row.QueryPlan != null)
            return Task.FromResult<string?>(row.QueryPlan);
        if (!row.HasQueryPlan)
            return Task.FromResult<string?>(null);
        return GetSnapshotPlanTextAsync(serverId, row.CollectionTime, row.SessionId, row.RequestId, live: false);
    }

    /// <summary>The actual/live-captured plan counterpart of <see cref="ResolveSnapshotEstimatedPlanAsync"/>.</summary>
    public Task<string?> ResolveSnapshotLivePlanAsync(int serverId, QuerySnapshotRow row)
    {
        if (row.LiveQueryPlan != null)
            return Task.FromResult<string?>(row.LiveQueryPlan);
        if (!row.HasLiveQueryPlan)
            return Task.FromResult<string?>(null);
        return GetSnapshotPlanTextAsync(serverId, row.CollectionTime, row.SessionId, row.RequestId, live: true);
    }

    /// <summary>
    /// The get_active_queries MCP read (#3541 A13): the newest <paramref name="cap"/> snapshot rows over the
    /// window that pass the caller's filters, plus the FILTERED population's size from the same statement.
    /// A sibling of <see cref="GetLatestQuerySnapshotsAsync"/> rather than a change to it: that read is the
    /// grids' whole-window snapshot (unfiltered, unbounded — the Active Queries grid wants every row and
    /// filters in the UI), and the two questions are different enough that one signature serving both would
    /// carry a page cap the grid must remember to disable.
    ///
    /// <para><b>Every filter is part of the query.</b> The tool used to read the whole window, filter
    /// <c>database_name</c> and <c>blocking_only</c> in C#, take <c>limit</c>, and publish the PRE-filter row
    /// count as <c>total_snapshots</c> beside the page — a total of a different population from the rows, and a
    /// page that could be empty while the window held matches. Here the filters are predicates, the population
    /// count is <c>COUNT(*) OVER ()</c> on the filtered rows above the cap (Darling's #3613 idiom), and the cap
    /// is the caller's, fetched at <c>limit + 1</c> so truncation is observed rather than inferred.</para>
    ///
    /// <para><b>Head blockers are never stripped.</b> The WAITFOR trim exists to drop idle monitoring shells,
    /// but the classic head blocker IS a session sitting in <c>WAITFOR</c> with an open transaction, and it was
    /// dropped while its victims' <c>blocking_session_id</c> pointed at a session no longer on the page. A row
    /// is kept whatever its text when a row in the SAME capture names it as its blocker — same capture, not
    /// same window, because session ids are reused and the old C# arm matched across the whole window. The two
    /// flags tell a victim's story when its blocker is absent: <c>blocker_in_capture</c> false is the idle
    /// open-transaction blocker sys.dm_exec_requests never lists; <c>blocker_in_population</c> false is a
    /// blocker the caller's own database filter excluded.</para>
    ///
    /// <para><b>The wait_type filter (#5235)</b> is one more population predicate, so the count and the cap see it
    /// and it ANDs with the other two. It matches the request's own wait at the capture by exact name with the case
    /// ignored (one equality, so a <c>%</c> or <c>_</c> in the input is literal), and a NULL wait never matches. The
    /// stored value is right-trimmed first because an older collector wrote a trailing space. Darling's twin is
    /// <c>upper(w.wait_type) IN (upper($7), upper($7) || ' ')</c>.</para>
    ///
    /// <para>The predicates are composed as SQL text from the booleans and the optional wait filter (a database
    /// name and the wait value are still bound), the <see cref="BuildDbInClause"/> way, because DuckDB cannot infer a
    /// type for a bare <c>$N IS NULL</c> parameter the way PostgreSQL's <c>$N::text</c> cast lets Darling's twin do
    /// it. The wait value's index follows the database values, so it is <c>5 + dbValues.Count</c>.</para>
    /// </summary>
    public async Task<(List<QuerySnapshotRow> Rows, long PopulationCount)> GetActiveQueriesPageAsync(
        int serverId, int hoursBack, int cap, string? databaseName = null, bool blockingOnly = false, string? waitType = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetActiveQueriesPageAsync", "v_query_snapshots filtered page (MCP)");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, null, null, asOfUtc);
        var dbClause = BuildDbInClause(
            string.IsNullOrWhiteSpace(databaseName) ? null : new[] { databaseName.Trim() }, "w.database_name", 5, out var dbValues);
        var blockingClause = blockingOnly ? " AND (w.blocking_session_id > 0 OR h.session_id IS NOT NULL)" : "";
        var waitFilter = string.IsNullOrWhiteSpace(waitType) ? null : waitType.Trim();
        var waitClause = waitFilter == null ? "" : $" AND upper(rtrim(w.wait_type)) = upper(${5 + dbValues.Count})";

        command.CommandText = @"
WITH window_rows AS (
    SELECT
        session_id,
        database_name,
        elapsed_time_formatted,
        query_text,
        status,
        blocking_session_id,
        wait_type,
        wait_time_ms,
        wait_resource,
        cpu_time_ms,
        total_elapsed_time_ms,
        reads,
        writes,
        logical_reads,
        granted_query_memory_gb,
        transaction_isolation_level,
        dop,
        parallel_worker_count,
        collection_time,
        login_name,
        host_name,
        program_name,
        open_transaction_count,
        percent_complete,
        query_hash
    FROM v_query_snapshots
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
heads AS (
    /* (capture, session) pairs some victim in the SAME capture points at. */
    SELECT DISTINCT collection_time, blocking_session_id AS session_id
    FROM window_rows
    WHERE blocking_session_id > 0
),
population AS (
    SELECT
        w.*,
        (h.session_id IS NOT NULL) AS is_head_blocker,
        EXISTS (
            SELECT 1
            FROM window_rows b
            WHERE b.collection_time = w.collection_time
            AND   b.session_id = w.blocking_session_id
        ) AS blocker_in_capture
    FROM window_rows w
    LEFT JOIN heads h
      ON  h.collection_time = w.collection_time
      AND h.session_id = w.session_id
    WHERE (w.query_text NOT LIKE 'WAITFOR%' OR h.session_id IS NOT NULL)" + dbClause + blockingClause + waitClause + @"
)
SELECT
    p.session_id,
    p.database_name,
    p.elapsed_time_formatted,
    p.query_text,
    p.status,
    p.blocking_session_id,
    p.wait_type,
    p.wait_time_ms,
    p.wait_resource,
    p.cpu_time_ms,
    p.total_elapsed_time_ms,
    p.reads,
    p.writes,
    p.logical_reads,
    p.granted_query_memory_gb,
    p.transaction_isolation_level,
    p.dop,
    p.parallel_worker_count,
    p.collection_time,
    p.login_name,
    p.host_name,
    p.program_name,
    p.open_transaction_count,
    p.percent_complete,
    p.query_hash,
    p.is_head_blocker,
    p.blocker_in_capture,
    EXISTS (
        SELECT 1
        FROM population q
        WHERE q.collection_time = p.collection_time
        AND   q.session_id = p.blocking_session_id
    ) AS blocker_in_population,
    COUNT(*) OVER () AS population_count
FROM population p
ORDER BY p.collection_time DESC, p.cpu_time_ms DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = cap });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });
        if (waitFilter != null)
            command.Parameters.Add(new DuckDBParameter { Value = waitFilter });

        var items = new List<QuerySnapshotRow>();
        long populationCount = 0;
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QuerySnapshotRow
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? "" : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Status = reader.IsDBNull(4) ? "" : reader.GetString(4),
                BlockingSessionId = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                WaitType = reader.IsDBNull(6) ? "" : reader.GetString(6),
                WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                CpuTimeMs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalElapsedTimeMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                Reads = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                Writes = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                LogicalReads = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                GrantedQueryMemoryGb = reader.IsDBNull(14) ? 0 : ToDouble(reader.GetValue(14)),
                TransactionIsolationLevel = reader.IsDBNull(15) ? "" : reader.GetString(15),
                Dop = reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
                ParallelWorkerCount = reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                CollectionTime = reader.IsDBNull(18) ? DateTime.MinValue : reader.GetDateTime(18),
                LoginName = reader.IsDBNull(19) ? "" : reader.GetString(19),
                HostName = reader.IsDBNull(20) ? "" : reader.GetString(20),
                ProgramName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                OpenTransactionCount = reader.IsDBNull(22) ? 0 : reader.GetInt32(22),
                PercentComplete = reader.IsDBNull(23) ? 0m : Convert.ToDecimal(reader.GetValue(23)),
                QueryHash = reader.IsDBNull(24) ? "" : reader.GetString(24),
                IsHeadBlocker = !reader.IsDBNull(25) && reader.GetBoolean(25),
                BlockerInCapture = !reader.IsDBNull(26) && reader.GetBoolean(26),
                BlockerInPopulation = !reader.IsDBNull(27) && reader.GetBoolean(27),
            });
            populationCount = Convert.ToInt64(reader.GetValue(28));
        }

        return (items, populationCount);
    }

    /// <summary>
    /// Gets lightweight blocking + deadlock counts and latest event time for alert badge updates.
    /// Much cheaper than fetching full rows with XML — just COUNT(*) and MAX(time).
    /// </summary>
    /// <remarks>
    /// <paramref name="fromDate"/>/<paramref name="toDate"/> are the tab's custom range as naive-UTC instants
    /// (#4766), so this read needs no clock. It is the one on the badge path, which runs on every tab's own timer
    /// whether or not that tab is visible, so the server it names and the server the desktop is showing are
    /// routinely different ones; it once had to be handed the clock of the right one, and a clock from the wrong
    /// one left the window an offset off in every display mode.
    /// <para>A DISPLAY read (the server tab's badge), not an alert-engine read: it windows on the event time, as
    /// the grids do, and the alert engine never calls it.</para>
    /// </remarks>
    public async Task<(int blockingCount, int deadlockCount, DateTime? latestEventTime)> GetAlertCountsAsync(int serverId, int hoursBack, DateTime? fromDate, DateTime? toDate)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null);

        /* blocking_count prefers the blocked-process-report; falls back to the always-on DMV snapshot when
           BPR captured nothing (AWS RDS). latest_event_time includes DMV blocking recency too. */
        command.CommandText = @"
SELECT
    COALESCE(NULLIF((SELECT COUNT(*) FROM " + StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3") + @" AS ev), 0),
     (SELECT COUNT(*) FROM v_dmv_blocking_snapshots
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3)) AS blocking_count,
    (SELECT " + StoredEventCopies.DeadlockDistinctCount + @" FROM v_deadlocks AS dl WHERE server_id = $1 AND deadlock_time >= $2 AND deadlock_time <= $3) AS deadlock_count,
    (SELECT MAX(t) FROM (
        SELECT MAX(event_time) AS t FROM " + StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3") + @" AS ev
        UNION ALL
        SELECT MAX(event_time) AS t FROM v_dmv_blocking_snapshots
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
        UNION ALL
        SELECT MAX(deadlock_time) AS t FROM v_deadlocks AS dl WHERE server_id = $1 AND deadlock_time >= $2 AND deadlock_time <= $3
    )) AS latest_event_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return (0, 0, null);

        var blockingCount = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
        var deadlockCount = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
        var latestEventTime = reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2);

        return (blockingCount, deadlockCount, latestEventTime);
    }

    /// <summary>
    /// Gets recent blocked process reports from the XE-based collector plus the always-on DMV fallback,
    /// newest first, merged and re-capped at <paramref name="limit"/>.
    ///
    /// <para>The cap is a PARAMETER with the grid's value as its default (#3541 A3): it was <c>LIMIT 200</c> on
    /// both arms and in the merge, under an MCP tool that advertised <c>limit</c> and took that many off the
    /// top. The MCP tools pass <c>limit + 1</c> and read the extra row as truncation; the grids pass nothing.
    /// The two arms are fetched so a merged result larger than the cap stays OBSERVABLE: the XE arm fetches
    /// the cap, the DMV arm fetches the cap PLUS the XE rows in hand, because the merge drops one DMV row per
    /// (pair, minute) an XE row already covers — fetched at the bare cap, a surplus made of XE-covered rows
    /// would vanish in the merge and a full page would read as complete. Darling's
    /// <c>DarlingBlockingReader</c> does the same, for the same reason.</para>
    ///
    /// <para><paramref name="xmlOnly"/> restricts the read to XE rows that CARRY a report, in SQL, and skips the
    /// DMV arm entirely (a DMV snapshot never has one) — the population <c>get_blocked_process_xml</c> pages
    /// over, so its <c>limit</c> counts reports rather than rows it would have to discard.</para>
    /// </summary>
    public async Task<List<BlockedProcessReportRow>> GetRecentBlockedProcessReportsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null, int limit = BlockedProcessReportGridCap, bool xmlOnly = false, bool windowOnCollectionTime = false) =>
        (await ReadRecentBlockedProcessReportsAsync(serverId, hoursBack, fromDate, toDate, databaseNames, asOfUtc, limit, xmlOnly, windowOnCollectionTime)).Rows;

    /// <summary>The Blocked Process Reports grid's row cap: the default <c>limit</c> of
    /// <see cref="GetRecentBlockedProcessReportsAsync"/> and <see cref="ReadRecentBlockedProcessReportsAsync"/>, and the
    /// cap the grid's "Showing since" notice judges the page by (#4966), so the read's <c>LIMIT</c> and the notice
    /// cannot drift apart. The same 200 as the Darling viewer's grid.</summary>
    public const int BlockedProcessReportGridCap = BlockedProcessReportMerge.DefaultCap;

    /// <summary>
    /// <see cref="GetRecentBlockedProcessReportsAsync"/>'s rows and where a read that filled its own cap stops the grid being
    /// complete (#4966): the Lite twin of the Darling viewer's <c>ReadRecentBlockedProcessReportsAsync</c>. The grid is fed by
    /// two reads, the XE reports (cap = <paramref name="limit"/>) and the always-on DMV snapshots (cap =
    /// <paramref name="limit"/> plus the XE rows in hand), and the merge then drops the DMV rows an XE report already covers and
    /// the DMV rows that repeat a pair within a minute, so the merged list can hold FEWER than <paramref name="limit"/> rows
    /// while the DMV read stopped at its LIMIT and left older snapshots out: a DMV read of 200 + <c>n</c> rows made of
    /// repeated pairs merges to a handful. The count of the merged list cannot show that, so each read's own count is checked
    /// before the merge (<see cref="FilledPageStart"/>) and <see cref="BlockedProcessReportsRead.CappedSourceStartUtc"/> names
    /// the oldest event time of the read that filled its page (the later one when both did).
    /// </summary>
    public async Task<BlockedProcessReportsRead> ReadRecentBlockedProcessReportsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null, int limit = BlockedProcessReportGridCap, bool xmlOnly = false, bool windowOnCollectionTime = false)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        /* $4 is the row cap, so the optional database list starts at $5. */
        var dbClause = BuildDbInClause(databaseNames, "database_name", 5, out var dbValues);
        var xmlClause = xmlOnly
            ? @"
AND   blocked_process_report_xml IS NOT NULL
AND   blocked_process_report_xml <> ''"
            : string.Empty;

        /* The XE arm windows on event_time; the DMV arm stays on collection_time because
           dmv_blocking_snapshots.event_time IS its collection time. The alert engine opts out (see GetRecentDeadlocksAsync),
           and only that collection_time window needs the look-back collectedFrom adds. */
        var xeRows = windowOnCollectionTime
            ? StoredEventCopies.BlockedProcessReports("server_id = $1 AND collection_time <= $3" + xmlClause + dbClause, collectedFrom: "$2")
            : StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3" + xmlClause + dbClause);

        command.CommandText = @"
SELECT
    collection_time,
    event_time,
    database_name,
    blocked_spid,
    blocked_ecid,
    blocking_spid,
    blocking_ecid,
    wait_time_ms,
    wait_resource,
    lock_mode,
    blocked_status,
    blocked_isolation_level,
    blocked_log_used,
    blocked_transaction_count,
    blocked_client_app,
    blocked_host_name,
    blocked_login_name,
    blocked_sql_text,
    blocking_status,
    blocking_isolation_level,
    blocking_client_app,
    blocking_host_name,
    blocking_login_name,
    blocking_sql_text,
    blocked_process_report_xml,
    blocked_transaction_name,
    blocking_transaction_name,
    blocked_last_tran_started,
    blocking_last_tran_started,
    blocked_last_batch_started,
    blocking_last_batch_started,
    blocked_last_batch_completed,
    blocking_last_batch_completed,
    blocked_priority,
    blocking_priority,
    contentious_object,
    monitor_loop
FROM " + xeRows + @" AS ev
ORDER BY event_time DESC
LIMIT $4";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = limit });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<BlockedProcessReportRow>();
        using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                items.Add(new BlockedProcessReportRow
                {
                    CollectionTime = reader.GetDateTime(0),
                    EventTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    DatabaseName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    BlockedSpid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    BlockedEcid = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                    BlockingSpid = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                    BlockingEcid = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                    WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                    LockMode = reader.IsDBNull(9) ? "" : reader.GetString(9),
                    BlockedStatus = reader.IsDBNull(10) ? "" : reader.GetString(10),
                    BlockedIsolationLevel = reader.IsDBNull(11) ? "" : reader.GetString(11),
                    BlockedLogUsed = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                    BlockedTransactionCount = reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                    BlockedClientApp = reader.IsDBNull(14) ? "" : reader.GetString(14),
                    BlockedHostName = reader.IsDBNull(15) ? "" : reader.GetString(15),
                    BlockedLoginName = reader.IsDBNull(16) ? "" : reader.GetString(16),
                    BlockedSqlText = reader.IsDBNull(17) ? "" : reader.GetString(17),
                    BlockingStatus = reader.IsDBNull(18) ? "" : reader.GetString(18),
                    BlockingIsolationLevel = reader.IsDBNull(19) ? "" : reader.GetString(19),
                    BlockingClientApp = reader.IsDBNull(20) ? "" : reader.GetString(20),
                    BlockingHostName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                    BlockingLoginName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                    BlockingSqlText = reader.IsDBNull(23) ? "" : reader.GetString(23),
                    BlockedProcessReportXml = reader.IsDBNull(24) ? "" : reader.GetString(24),
                    BlockedTransactionName = reader.IsDBNull(25) ? "" : reader.GetString(25),
                    BlockingTransactionName = reader.IsDBNull(26) ? "" : reader.GetString(26),
                    BlockedLastTranStarted = reader.IsDBNull(27) ? null : reader.GetDateTime(27),
                    BlockingLastTranStarted = reader.IsDBNull(28) ? null : reader.GetDateTime(28),
                    BlockedLastBatchStarted = reader.IsDBNull(29) ? null : reader.GetDateTime(29),
                    BlockingLastBatchStarted = reader.IsDBNull(30) ? null : reader.GetDateTime(30),
                    BlockedLastBatchCompleted = reader.IsDBNull(31) ? null : reader.GetDateTime(31),
                    BlockingLastBatchCompleted = reader.IsDBNull(32) ? null : reader.GetDateTime(32),
                    BlockedPriority = reader.IsDBNull(33) ? 0 : reader.GetInt32(33),
                    BlockingPriority = reader.IsDBNull(34) ? 0 : reader.GetInt32(34),
                    ContentiousObject = reader.IsDBNull(35) ? "" : reader.GetString(35),
                    MonitorLoop = reader.IsDBNull(36) ? (int?)null : reader.GetInt32(36)
                });
            }
        }

        /* An XE read that returned a whole page (its LIMIT) left older reports out, whatever the merge below keeps. */
        var xeFilledPageStart = FilledPageStart(items, limit);

        // Always-on DMV blocking snapshot: surface its rows in the grid too, so the block-chain viewer is
        // reachable when the blocked-process-report XE captured nothing (AWS RDS). Same connection/lock.
        // Skipped under xmlOnly: a DMV snapshot never carries a report, so it has nothing to add to that page.
        DateTime? dmvFilledPageStart = null;
        if (!xmlOnly)
            dmvFilledPageStart = await AppendDmvBlockedProcessGridRowsAsync(connection.CreateCommand, items, serverId, startTime, endTime, databaseNames, limit);

        return new BlockedProcessReportsRead(items, LaterOf(xeFilledPageStart, dmvFilledPageStart));
    }

    /// <summary>
    /// The oldest event time of a newest-first <paramref name="page"/> that is as long as the <paramref name="fetched"/> rows its
    /// read asked for (its LIMIT), or null when it is shorter: a page under its LIMIT holds everything the store has in the
    /// window. A row with no event time never names a start. See <see cref="ReadRecentBlockedProcessReportsAsync"/>.
    /// </summary>
    internal static DateTime? FilledPageStart(IReadOnlyCollection<BlockedProcessReportRow> page, int fetched)
    {
        if (fetched <= 0 || page.Count < fetched)
        {
            return null;
        }

        DateTime? oldest = null;
        foreach (var row in page)
        {
            if (row.EventTime is DateTime time && (oldest is null || time < oldest))
            {
                oldest = time;
            }
        }

        return oldest;
    }

    private static DateTime? LaterOf(DateTime? first, DateTime? second) =>
        first is DateTime a && second is DateTime b ? (a >= b ? a : b) : first ?? second;

    /// <summary>
    /// Fetches always-on DMV blocking-snapshot rows for the blocked-process grid and merges them into the
    /// BPR list — BPR preferred (dedup by blocked/blocker SPID within a minute), re-capped to the newest
    /// <paramref name="cap"/>. Runs on the caller's connection/lock (the read lock is non-recursive, so a
    /// second connection can't be opened). v_dmv_blocking_snapshots is created by DuckDbInitializer, so it
    /// always exists. The DMV fetch is <paramref name="cap"/> plus the XE rows already in
    /// <paramref name="items"/> — see <see cref="GetRecentBlockedProcessReportsAsync"/> for why.
    /// </summary>
    private static async Task<DateTime?> AppendDmvBlockedProcessGridRowsAsync(
        Func<DuckDBCommand> createCommand, List<BlockedProcessReportRow> items, int serverId, DateTime startTime, DateTime endTime, IReadOnlyList<string>? databaseNames, int cap)
    {
        var dmvItems = new List<BlockedProcessReportRow>();
        /* The DMV read's own cap: the grid's cap plus the XE rows in hand. */
        var dmvFetch = cap + items.Count;
        var dbClause = BuildDbInClause(databaseNames, "database_name", 5, out var dbValues);
        using (var command = createCommand())
        {
            command.CommandText = @"
SELECT
    collection_time, event_time, database_name,
    blocked_spid, blocked_ecid, blocking_spid, blocking_ecid,
    wait_time_ms, lock_mode, blocking_status, contentious_object,
    blocked_sql_text, blocking_sql_text,
    blocked_login_name, blocked_host_name, blocked_client_app,
    blocked_last_tran_started, blocking_last_tran_started, monitor_loop,
    blocking_login_name, blocking_host_name, blocking_client_app
FROM v_dmv_blocking_snapshots
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3" + dbClause + @"
ORDER BY event_time DESC
LIMIT $4";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = endTime });
            command.Parameters.Add(new DuckDBParameter { Value = dmvFetch });
            foreach (var db in dbValues)
                command.Parameters.Add(new DuckDBParameter { Value = db });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                dmvItems.Add(new BlockedProcessReportRow
                {
                    CollectionTime = reader.GetDateTime(0),
                    EventTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    DatabaseName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    BlockedSpid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    BlockedEcid = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                    BlockingSpid = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                    BlockingEcid = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                    WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    LockMode = reader.IsDBNull(8) ? "" : reader.GetString(8),
                    BlockingStatus = reader.IsDBNull(9) ? "" : reader.GetString(9),
                    ContentiousObject = reader.IsDBNull(10) ? "" : reader.GetString(10),
                    BlockedSqlText = reader.IsDBNull(11) ? "" : reader.GetString(11),
                    BlockingSqlText = reader.IsDBNull(12) ? "" : reader.GetString(12),
                    BlockedLoginName = reader.IsDBNull(13) ? "" : reader.GetString(13),
                    BlockedHostName = reader.IsDBNull(14) ? "" : reader.GetString(14),
                    BlockedClientApp = reader.IsDBNull(15) ? "" : reader.GetString(15),
                    BlockedLastTranStarted = reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                    BlockingLastTranStarted = reader.IsDBNull(17) ? null : reader.GetDateTime(17),
                    MonitorLoop = reader.IsDBNull(18) ? (int?)null : reader.GetInt32(18),
                    /* Blocking-side identity — the DMV snapshot is the only blocking source on AWS RDS / when the
                       BPR threshold is unset, so surface it here too (the XE path already sets these). */
                    BlockingLoginName = reader.IsDBNull(19) ? "" : reader.GetString(19),
                    BlockingHostName = reader.IsDBNull(20) ? "" : reader.GetString(20),
                    BlockingClientApp = reader.IsDBNull(21) ? "" : reader.GetString(21),
                    Source = BlockedProcessAlertRow.DmvSnapshotSource
                });
            }
        }

        /* Dedup + re-cap moved verbatim to the shared BlockedProcessReportMerge (Phase-5 slice B)
           so the Darling Postgres adapter reproduces EXACTLY these XE-preferred fallback semantics. */
        var dmvFilledPageStart = FilledPageStart(dmvItems, dmvFetch);
        BlockedProcessReportMerge.AppendDmvFallbackRows(items, dmvItems, cap);
        return dmvFilledPageStart;
    }

    /// <summary>
    /// Fetches the blocked/blocker pair rows for a window, for the block-chain viewer to feed
    /// <see cref="BlockingChainReconstructor"/>. Internal (not public): <see cref="BlockingPairRow"/> is
    /// internal to the analysis assembly — a public method returning it would be CS0050.
    /// <see cref="OpenConnectionAsync"/> already takes the DuckDB read lock, so do NOT acquire it again
    /// (the lock is NoRecursion). Uses <see cref="Analysis.BlockingPairRowQuery.SpidFilter"/> so it agrees
    /// with the drill-down + fact collectors on the apex.
    /// </summary>
    internal async Task<List<BlockingPairRow>> GetBlockingPairRowsAsync(int serverId, DateTime start, DateTime end)
    {
        var rows = new List<BlockingPairRow>();

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT
    {PerformanceMonitorLite.Analysis.BlockingPairRowQuery.LeadingColumns},
    blocked_sql_text, blocking_sql_text,
    {PerformanceMonitorLite.Analysis.BlockingPairRowQuery.IdentityColumns},
    contentious_object,
    {PerformanceMonitorLite.Analysis.BlockingPairRowQuery.TrailingIdentityColumns}
FROM {StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3 " + PerformanceMonitorLite.Analysis.BlockingPairRowQuery.SpidFilter)} AS ev
ORDER BY event_time DESC
LIMIT 5000";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = start });
        command.Parameters.Add(new DuckDBParameter { Value = end });

        using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                rows.Add(PerformanceMonitorLite.Analysis.BlockingPairRowQuery.Read(reader));
        }

        // Always-on DMV blocking snapshot: merge in the fallback rows so the viewer works even when the
        // blocked-process-report XE captured nothing (threshold unset / AWS RDS). Same connection/lock.
        /* #2443: CancellationToken.None because the viewer genuinely has no pass to abandon — this
           read serves a person waiting at a grid, not an analysis pass with a budget or a service
           stop. Stated here rather than defaulted in the shared method, so the exception belongs to
           the caller that actually has it. */
        await PerformanceMonitorLite.Analysis.BlockingPairRowQuery.AppendDmvSnapshotRowsAsync(
            connection.CreateCommand, rows, serverId, start, end, System.Threading.CancellationToken.None);

        return rows;
    }

    /// <summary>
    /// Gets hourly-bucketed metrics from blocked process reports for the time-range slicer.
    /// </summary>
    public async Task<List<TimeSliceBucket>> GetBlockingSlicerDataAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        /* BPR buckets, falling back to the always-on DMV snapshot only when BPR has no buckets in the
           window (AWS RDS) — so a server with both sources never double-counts. */
        command.CommandText = @"
WITH bpr AS (
    SELECT
        date_trunc('hour', event_time) AS bucket,
        COUNT(*) AS event_count,
        COALESCE(SUM(wait_time_ms), 0) / 1000.0 AS total_wait_sec,
        COUNT(DISTINCT blocking_spid) AS distinct_blockers,
        COUNT(DISTINCT blocked_spid) AS distinct_blocked,
        COUNT(DISTINCT database_name) AS distinct_databases
    FROM " + StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3" + dbClause) + @" AS ev
    GROUP BY date_trunc('hour', event_time)
),
dmv AS (
    SELECT
        date_trunc('hour', collection_time) AS bucket,
        COUNT(*) AS event_count,
        COALESCE(SUM(wait_time_ms), 0) / 1000.0 AS total_wait_sec,
        COUNT(DISTINCT blocking_spid) AS distinct_blockers,
        COUNT(DISTINCT blocked_spid) AS distinct_blocked,
        COUNT(DISTINCT database_name) AS distinct_databases
    FROM v_dmv_blocking_snapshots
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3" + dbClause + @"
    GROUP BY date_trunc('hour', collection_time)
)
SELECT bucket, event_count, total_wait_sec, distinct_blockers, distinct_blocked, distinct_databases FROM bpr
UNION ALL
SELECT bucket, event_count, total_wait_sec, distinct_blockers, distinct_blocked, distinct_databases FROM dmv
WHERE NOT EXISTS (SELECT 1 FROM bpr)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<TimeSliceBucket>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var eventCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1));
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = eventCount,
                TotalCpu = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                TotalElapsed = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalLogicalReads = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                Value = eventCount,
            });
        }

        return items;
    }

    /// <summary>
    /// Gets hourly-bucketed metrics from deadlocks for the time-range slicer.
    /// </summary>
    public async Task<List<TimeSliceBucket>> GetDeadlockSlicerDataAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null);

        command.CommandText = @"
SELECT
    date_trunc('hour', deadlock_time) AS bucket,
    " + StoredEventCopies.DeadlockDistinctCount + @" AS deadlock_count
FROM v_deadlocks AS dl
WHERE server_id = $1 AND deadlock_time >= $2 AND deadlock_time <= $3
GROUP BY date_trunc('hour', deadlock_time)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<TimeSliceBucket>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var count = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1));
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = count,
                Value = count,
            });
        }

        return items;
    }

    /// <summary>
    /// #5098: whether the XE blocked process reports have a row in the window, with the SAME predicate (and database filter) as the
    /// <c>bpr</c> arm of <see cref="GetBlockingTrendAsync"/> and <see cref="GetBlockingDurationStatsAsync"/>. When true those reads drew
    /// the XE rows alone (the DMV arm is skipped), so the note must name the XE start, not a DMV-covered one.
    /// </summary>
    public async Task<bool> HasBlockedProcessReportsInWindowAsync(int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        command.CommandText = "SELECT 1 FROM " + StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3" + dbClause) + " AS ev LIMIT 1";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        using var reader = await command.ExecuteReaderAsync();
        return await reader.ReadAsync();
    }

    /// <summary>
    /// #5098: the earliest XE blocked process report in the window, with the SAME predicate and database filter as
    /// <see cref="HasBlockedProcessReportsInWindowAsync"/>. Null when there is none. Opens its own connection.
    /// </summary>
    public async Task<DateTime?> GetEarliestBlockedProcessReportInWindowAsync(int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        command.CommandText = "SELECT MIN(ev.event_time) FROM " + StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3" + dbClause) + " AS ev";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        return await command.ExecuteScalarAsync() is DateTime first ? first : null;
    }

    /// <summary>
    /// #5098: the blocked process threshold's history over the window, the same three outputs as the Darling viewer's
    /// <c>BlockedProcessThresholdOnSql</c>: on at the window's start (the newest snapshot at or before it is above zero), the
    /// earliest snapshot inside (start, end] that is above zero, and whether a snapshot read zero before the threshold was seen on (the newest one at or before the start, or one in the window dated before that first nonzero one).
    /// <c>capture_time</c> is UTC, like every Lite store column. No snapshots reads as (false, null, false), which
    /// <see cref="PerformanceMonitor.Common.BlockingThresholdCoverage.Combine"/> leaves unchanged. Opens its own connection.
    /// </summary>
    public async Task<(bool OnAtWindowStart, DateTime? FirstOnInWindow, bool SawZeroSnapshot)> GetBlockedProcessThresholdOnAsync(int serverId, DateTime startUtc, DateTime endUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
WITH before AS (
    SELECT c.value_in_use
    FROM v_server_config AS c
    WHERE c.server_id = $1 AND c.configuration_name = 'blocked process threshold (s)' AND c.capture_time <= $2
    ORDER BY c.capture_time DESC
    LIMIT 1),
inside AS (
    SELECT c.capture_time, c.value_in_use
    FROM v_server_config AS c
    WHERE c.server_id = $1 AND c.configuration_name = 'blocked process threshold (s)' AND c.capture_time > $2 AND c.capture_time <= $3),
first_on AS (
    SELECT MIN(i.capture_time) AS capture_time FROM inside AS i WHERE i.value_in_use > 0)
SELECT
    COALESCE((SELECT b.value_in_use > 0 FROM before AS b), FALSE),
    (SELECT f.capture_time FROM first_on AS f),
    EXISTS (SELECT 1 FROM before AS b WHERE b.value_in_use = 0)
        OR EXISTS (SELECT 1 FROM inside AS i CROSS JOIN first_on AS f
                   WHERE i.value_in_use = 0 AND (f.capture_time IS NULL OR i.capture_time < f.capture_time))";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });

        using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        return (reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1), reader.GetBoolean(2));
    }

    /// <summary>
    /// #5098: where the XE blocked process report data starts: the collector's coverage floor (<paramref name="collectorFloorOf"/>,
    /// the <c>includeAlsoCovered: false</c> probe), the earliest report, and the threshold's history, through
    /// <see cref="PerformanceMonitor.Common.BlockingThresholdCoverage.Combine"/> (the rule the Darling viewer uses). The three reads run
    /// one after the other, each on its own connection; a throw from any of them is the caller's, so a failed probe costs only the note.
    /// </summary>
    public static async Task<DateTime?> CombineBlockingXeStartAsync(
        Func<Task<DateTime?>> collectorFloorOf, Func<Task<DateTime?>> earliestReportOf,
        Func<Task<(bool OnAtWindowStart, DateTime? FirstOnInWindow, bool SawZeroSnapshot)>> thresholdOf)
    {
        var collector = await collectorFloorOf();
        var report = await earliestReportOf();
        var threshold = await thresholdOf();
        return PerformanceMonitor.Common.BlockingThresholdCoverage.Combine(
            collector, report, threshold.OnAtWindowStart, threshold.FirstOnInWindow, threshold.SawZeroSnapshot);
    }

    /// <summary>#5098: the Blocking tab's XE start for a window: <see cref="CombineBlockingXeStartAsync"/> over this store.</summary>
    public Task<DateTime?> GetBlockingXeDataStartAsync(int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null) =>
        CombineBlockingXeStartAsync(
            () => GetQueryWindowFloorAsync(QueryWindowRelation.BlockedProcessReports, serverId, startUtc, endUtc, includeAlsoCovered: false),
            () => GetEarliestBlockedProcessReportInWindowAsync(serverId, startUtc, endUtc, databaseNames),
            () => GetBlockedProcessThresholdOnAsync(serverId, startUtc, endUtc));

    /// <summary>
    /// Gets blocking incident trend (count of distinct blocking events per time bucket).
    /// Uses blocked_process_reports from Extended Events for more reliable detection.
    /// Falls back to blocking_snapshots if no XE data available.
    /// </summary>
    public async Task<List<TrendPoint>> GetBlockingTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        /* Use blocked_process_reports from XE session - more reliable than point-in-time snapshots
           Group by event_time (when blocking actually occurred) rather than collection_time */
        /* BPR per-minute buckets, falling back to the always-on DMV snapshot only when BPR has none in the
           window (AWS RDS) — so a server with both sources never double-counts. */
        command.CommandText = @"
WITH bpr AS (
    SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
    FROM " + StoredEventCopies.BlockedProcessReports("server_id = $1 AND event_time >= $2 AND event_time <= $3" + dbClause) + @" AS ev
    GROUP BY DATE_TRUNC('minute', event_time)
),
dmv AS (
    SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
    FROM v_dmv_blocking_snapshots
    WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3" + dbClause + @"
    GROUP BY DATE_TRUNC('minute', event_time)
)
SELECT bucket, incident_count FROM bpr
UNION ALL
SELECT bucket, incident_count FROM dmv WHERE NOT EXISTS (SELECT 1 FROM bpr)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<TrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TrendPoint
            {
                Time = reader.GetDateTime(0),
                Count = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets deadlock trend (count of deadlocks per minute bucket).
    /// </summary>
    public async Task<List<TrendPoint>> GetDeadlockTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
SELECT
    bucket,
    deadlock_count
FROM (
    SELECT
        DATE_TRUNC('minute', deadlock_time) AS bucket,
        " + StoredEventCopies.DeadlockDistinctCount + @" AS deadlock_count
    FROM v_deadlocks AS dl
    WHERE server_id = $1 AND deadlock_time >= $2 AND deadlock_time <= $3
    GROUP BY DATE_TRUNC('minute', deadlock_time)
) sub
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<TrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TrendPoint
            {
                Time = reader.GetDateTime(0),
                Count = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1))
            });
        }
        return items;
    }

    /* ───────────────── the denominator an empty trend needs (#2485) ───────────────── */

    /// <summary>
    /// Successful runs per blocking collector inside the window, with the first and last of them.
    /// <para>The blocking trend reads EDGE tables — rows exist only where an event happened — so an
    /// absence of rows is a capture that found nothing and a capture that never ran, wearing the same
    /// face. <c>collection_log</c> can tell them apart, because a collector that ran and stored nothing
    /// still records a SUCCESS with zero rows.</para>
    /// <para>BOTH capture paths are counted, deliberately: <see cref="GetBlockingTrendAsync"/> unions
    /// <c>v_blocked_process_reports</c> with <c>v_dmv_blocking_snapshots</c>, so counting one of them
    /// would report "never captured" for a server capturing perfectly well through the other. Only
    /// SUCCESS counts as having looked — a PERMISSIONS or ERROR row is a collector that did not see the
    /// window either. Darling's twin is <c>DarlingBlockingTrendReader.BlockingCaptureCountsSql</c>; the
    /// two must stay in step so a user moving between the SKUs is not told a different story about the
    /// same state.</para>
    /// </summary>
    public Task<List<CollectorCaptureCount>> GetBlockingCaptureCountsAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null) =>
        GetCaptureCountsAsync(serverId, hoursBack, "collector_name IN ('blocked_process_report', 'dmv_blocking_snapshot')", fromDate, toDate, asOfUtc);

    /// <summary>
    /// Successful runs of the deadlock collector inside the window. One capture path here, not two —
    /// deadlocks come only from the <c>deadlocks</c> collector's system_health read, and there is no DMV
    /// fallback to count.
    /// </summary>
    public Task<List<CollectorCaptureCount>> GetDeadlockCaptureCountsAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null) =>
        GetCaptureCountsAsync(serverId, hoursBack, "collector_name = 'deadlocks'", fromDate, toDate, asOfUtc);

    /// <summary>
    /// Whether either blocking collector has EVER run successfully for this server, ignoring any window.
    /// <para>Asked ONLY when the window count came back zero, and only to pick which sentence is true: a
    /// server whose collectors have run before has a GAP in this window, while one that has never run
    /// them is not collecting blocking at all. Both are "not an all-clear" and they want different next
    /// moves. LIMIT 1, so it stops at the first row.</para>
    /// <para>NOT the same question as the neighbouring <c>HasAnyBlockingCaptureAsync</c> in
    /// <c>LocalDataService.BlockingStats.cs</c>, which asks whether an EVENT was ever captured. That one is
    /// right for get_blocking_stats, whose verdict is about severity; it is wrong here, because a server
    /// collected perfectly for months that simply never blocked has no event rows and would be reported as
    /// never captured — the reassuring-answer failure this read exists to prevent, inverted. Hence
    /// "collector run", not "capture": the denominator is whether we LOOKED, not whether we found
    /// something. Darling's twin is <c>DarlingBlockingTrendReader.HasAnyBlockingCollectorRunAsync</c>.</para>
    /// </summary>
    public Task<bool> HasAnyBlockingCollectorRunAsync(int serverId) =>
        HasAnyCaptureAsync(serverId, "collector_name IN ('blocked_process_report', 'dmv_blocking_snapshot')");

    /// <summary>Whether the deadlock collector has EVER run successfully for this server. See
    /// <see cref="HasAnyBlockingCollectorRunAsync"/> for why the question is asked at all.</summary>
    public Task<bool> HasAnyDeadlockCollectorRunAsync(int serverId) =>
        HasAnyCaptureAsync(serverId, "collector_name = 'deadlocks'");

    /// <summary>
    /// The shared body behind the two capture-count reads. The collector predicate is a COMPILE-TIME
    /// literal from the two call sites above — never caller input — so it is concatenated rather than
    /// bound; the server id and window still bind as parameters.
    /// </summary>
    /* fromDate/toDate let a CALLER pin one instant across the trend read and this one. Resolving now
       independently in each means the two can straddle a row that arrives between them, and the whole
       point of asking both is that their answers are compared. Darling's twin takes explicit bounds
       for the same reason. */
    private async Task<List<CollectorCaptureCount>> GetCaptureCountsAsync(
        int serverId, int hoursBack, string collectorPredicate, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
SELECT
    collector_name,
    COUNT(*),
    MIN(collection_time),
    MAX(collection_time)
FROM v_collection_log
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   status = 'SUCCESS'
AND   " + collectorPredicate + @"
GROUP BY collector_name
ORDER BY collector_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<CollectorCaptureCount>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new CollectorCaptureCount(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetDateTime(3)));
        }
        return items;
    }

    /// <summary>The shared body behind the two existence probes. Same literal-predicate contract as
    /// <see cref="GetCaptureCountsAsync"/>.</summary>
    private async Task<bool> HasAnyCaptureAsync(int serverId, string collectorPredicate)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_collection_log
WHERE server_id = $1
AND   status = 'SUCCESS'
AND   " + collectorPredicate + @"
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// The bucketed statement text (#4349, matching #4234/#4340's shape), pulled out of
    /// <see cref="GetLockWaitTrendAsync"/> so its shape is checkable without a live DuckDB.
    /// </summary>
    internal static readonly string LockWaitTrendSql = $@"
WITH raw AS
(
    SELECT
        collection_time,
        wait_type,
        delta_wait_time_ms,
        /* #3540: the STORED interval where the row has one; 0 (no delta knowable) becomes NULL through NULLIF
           and the reader drops the row rather than reading 0.00. NULL (a pre-v60 row) falls back to the LAG. */
        CASE WHEN sample_interval_seconds IS NULL
             THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time))))
             ELSE NULLIF(sample_interval_seconds, 0)
        END AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   wait_type LIKE 'LCK%'
    AND   collection_time >= $2
    AND   collection_time <= $3
),
rated AS
(
    SELECT
        collection_time,
        wait_type,
        CASE WHEN interval_seconds > 0 AND delta_wait_time_ms >= 0 THEN delta_wait_time_ms END AS rated_wait_ms,
        CASE WHEN interval_seconds > 0 AND delta_wait_time_ms >= 0 THEN interval_seconds END AS rated_seconds
    FROM raw
)
SELECT
    wait_type,
    GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
    SUM(rated_wait_ms) / SUM(rated_seconds) AS wait_time_ms_per_second,
    MIN(collection_time) AS first_collection_time,
    COUNT(*) AS collection_count
FROM rated
GROUP BY wait_type, 2
HAVING COUNT(rated_seconds) > 0
ORDER BY wait_type, 2";

    /// <summary>
    /// Gets lock wait stats trend data (LCK% wait types) for the blocking trends chart.
    /// Returns per-second rates grouped by wait type.
    ///
    /// <para>#2484: takes <paramref name="asOfUtc"/> so the MCP twin (get_lock_wait_trend) can anchor the
    /// window at a past incident. Threaded as the anchor rather than as fromDate/toDate because those two
    /// are a custom range's UTC bounds (#4766) and the caller here has one instant, the end of an hours-back
    /// window: the anchor states that end once and the window's length comes from hoursBack. collection_time
    /// is stored in UTC, so this read windows on the UTC bounds.</para>
    /// <para>#4349: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES (wait type), matching
    /// #4234/#4340's shape — <c>seriesCount</c> is always 1 into <see cref="TrendBuckets.AutoMinutes"/>. When
    /// every bucket the call returns holds exactly one physical collection, every point is stamped at its own
    /// raw collection time instead of the <c>time_bucket</c> grid line.</para>
    /// </summary>
    public async Task<List<LockWaitTrendPoint>> GetLockWaitTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = LockWaitTrendSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(string WaitType, DateTime BucketStart, DateTime FirstCollectionTime, double Rate)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (Convert.ToInt64(reader.GetValue(4)) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2))));
        }

        var items = new List<LockWaitTrendPoint>();
        foreach (var row in rows)
        {
            items.Add(new LockWaitTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                WaitType = row.WaitType,
                WaitTimeMsPerSecond = row.Rate
            });
        }
        return items;
    }
}

/// <summary>
/// One collector's SUCCESSFUL run count inside a window, with the first and last of those runs — the
/// denominator an empty blocking or deadlock trend needs to be interpretable (#2485). Darling's twin is
/// <c>DarlingBlockingTrendReader.CaptureCount</c>.
/// </summary>
public sealed record CollectorCaptureCount(string CollectorName, long Runs, DateTime? FirstRunAt, DateTime? LastRunAt);

public class LockWaitTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string WaitType { get; set; } = string.Empty;
    public double WaitTimeMsPerSecond { get; set; }
}

public class TrendPoint
{
    public DateTime Time { get; set; }
    public int Count { get; set; }
}

/// <summary>
/// Lite's deadlock grid row. The alert-consumed members (victim process/SQL, graph XML, the parsed
/// <c>ProcessSummary</c>) live on the shared <see cref="DeadlockAlertRow"/> base (Phase-5 slice B)
/// so store reads flow into the shared alert builders without a mapping copy; only the
/// grid-display extras stay here.
/// </summary>
public class DeadlockRow : DeadlockAlertRow
{
    public DateTime CollectionTime { get; set; }
    public DateTime? DeadlockTime { get; set; }

    /// <summary>The database the deadlock was captured for. On an Azure SQL Database <c>master</c> target
    /// this is the user database whose deadlock it is.</summary>
    public string? DatabaseName { get; set; }
}

/// <summary>
/// Lite's Deadlocks-grid row — one parsed process per deadlock graph. The raw parsed fields and the
/// sp_BlitzLock-style graph walk now live once on the shared <see cref="DeadlockProcessInfo"/> /
/// <see cref="DeadlockGraphProcessParser"/> (Common); only Lite's per-server display getters
/// (<see cref="ServerTimeHelper"/>) stay here.
///
/// <para>The two timestamps are in DIFFERENT frames and take different renderers.
/// <c>DeadlockTime</c> is <c>deadlocks.deadlock_time</c>, the XE <c>@timestamp</c>, so it is naive UTC
/// and converts through <see cref="ServerTimeHelper.FormatServerTime"/>. <c>LastTranStarted</c> is the
/// deadlock graph's <c>lasttranstarted</c> attribute, walked out of the stored XML at READ time by
/// <see cref="DeadlockGraphProcessParser"/>, and SQL Server writes that attribute in its own local
/// clock — so it renders through <see cref="ServerTimeHelper.FormatServerClock"/>. No
/// <c>CollectorColumn</c> declares it, so the catalog-derived clock-frame census cannot reach this
/// pair and the frames are stated here instead.</para>
/// </summary>
public class DeadlockProcessDetail : DeadlockProcessInfo
{
    public string DeadlockTimeLocal => ServerTimeHelper.FormatServerTime(DeadlockTime);
    public string VictimDisplay => IsVictim ? "Victim" : "";
    public string WaitTimeFormatted => WaitTime > 0 ? $"{WaitTime:N0} ms" : "";
    public string LastTranStartedLocal => ServerTimeHelper.FormatServerClock(LastTranStarted);

    /// <summary>
    /// Parses a list of <see cref="DeadlockRow"/> into per-process detail rows via the shared
    /// <see cref="DeadlockGraphProcessParser"/> (the sp_BlitzLock-style graph walk, de-duplicated with the
    /// Darling viewer). Lite never captures a victim plan, so the plan input is always null.
    /// </summary>
    public static List<DeadlockProcessDetail> ParseFromRows(List<DeadlockRow> rows)
        => DeadlockGraphProcessParser.Parse<DeadlockProcessDetail>(
            rows.Select(r => new DeadlockGraphInput(r.DeadlockGraphXml, r.DeadlockTime, r.VictimSqlText, null))).ToList();
}

/// <summary>
/// The Blocked Process Reports grid's rows and where a read that filled its own cap stops the grid being complete
/// (<see cref="LocalDataService.ReadRecentBlockedProcessReportsAsync"/>, #4966).
/// </summary>
/// <param name="Rows">The merged rows, newest first, at most <see cref="LocalDataService.BlockedProcessReportGridCap"/> of them
/// for a grid read.</param>
/// <param name="CappedSourceStartUtc">The oldest event time the XE read or the DMV read returned, for the one that returned a
/// full page (the later, when both did): the older reports of that read are not in the grid, whatever the merged count is.
/// Null when neither read filled its cap.</param>
public sealed record BlockedProcessReportsRead(List<BlockedProcessReportRow> Rows, DateTime? CappedSourceStartUtc);

/// <summary>
/// Lite's blocked-process grid row. The alert-consumed members (event time, database, SPID pair,
/// wait/lock, the query pair, report XML, contentious object, Source) live on the shared
/// <see cref="BlockedProcessAlertRow"/> base (Phase-5 slice B) so store reads flow into the shared
/// alert builders and the XE→DMV fallback merge without a mapping copy; only the grid-display
/// extras stay here.
/// </summary>
public class BlockedProcessReportRow : BlockedProcessAlertRow
{
    public DateTime CollectionTime { get; set; }
    public int BlockedEcid { get; set; }
    public int BlockingEcid { get; set; }
    public int? MonitorLoop { get; set; }

    public string WaitResource { get; set; } = "";
    public string BlockedStatus { get; set; } = "";
    public string BlockedIsolationLevel { get; set; } = "";
    public long BlockedLogUsed { get; set; }
    public int BlockedTransactionCount { get; set; }
    public string BlockedClientApp { get; set; } = "";
    public string BlockedHostName { get; set; } = "";
    public string BlockedLoginName { get; set; } = "";
    public string BlockingStatus { get; set; } = "";
    public string BlockingIsolationLevel { get; set; } = "";
    public string BlockingClientApp { get; set; } = "";
    public string BlockingHostName { get; set; } = "";
    public string BlockingLoginName { get; set; } = "";
    public string BlockedTransactionName { get; set; } = "";
    public string BlockingTransactionName { get; set; } = "";
    public DateTime? BlockedLastTranStarted { get; set; }
    public DateTime? BlockingLastTranStarted { get; set; }
    public DateTime? BlockedLastBatchStarted { get; set; }
    public DateTime? BlockingLastBatchStarted { get; set; }
    public DateTime? BlockedLastBatchCompleted { get; set; }
    public DateTime? BlockingLastBatchCompleted { get; set; }
    public int BlockedPriority { get; set; }
    public int BlockingPriority { get; set; }

    public string EventTimeLocal => ServerTimeHelper.FormatServerTime(EventTime);
    public string WaitTimeFormatted => WaitTimeMs < 1000 ? $"{WaitTimeMs} ms" : $"{WaitTimeMs / 1000.0:F1} sec";
    public bool IsLongBlock => WaitTimeMs > 30000;
    /* EventTime above is the XE @timestamp and is naive UTC. These three are attributes of the
       blocked-process report XML, which SQL Server writes in the monitored server's own clock, so
       they take the server-clock renderer instead: one row, two frames, two renderers. */
    public string BlockedLastTranStartedLocal => ServerTimeHelper.FormatServerClock(BlockedLastTranStarted);
    public string BlockedLastBatchStartedLocal => ServerTimeHelper.FormatServerClock(BlockedLastBatchStarted);
    public string BlockedLastBatchCompletedLocal => ServerTimeHelper.FormatServerClock(BlockedLastBatchCompleted);
}

public class QuerySnapshotRow
{
    /// <summary>Gates "Get Actual Plan (re-run)" in the query grids' plan menu — the re-run executes this row's
    /// query text (ServerTab.Plans.cs). Row types the re-run handler has no case for simply omit this property,
    /// so the menu item's binding falls back to Collapsed and the verb is hidden rather than silently no-op.</summary>
    public bool CanGetActualPlan => !string.IsNullOrEmpty(QueryText);

    public int SessionId { get; set; }
    public string DatabaseName { get; set; } = "";
    public string ElapsedTimeFormatted { get; set; } = "";
    public string QueryText { get; set; } = "";
    public string Status { get; set; } = "";
    public int BlockingSessionId { get; set; }
    public string WaitType { get; set; } = "";
    public long WaitTimeMs { get; set; }
    public long CpuTimeMs { get; set; }
    public long TotalElapsedTimeMs { get; set; }
    public long Reads { get; set; }
    public long Writes { get; set; }
    public long LogicalReads { get; set; }
    public double GrantedQueryMemoryGb { get; set; }
    public string TransactionIsolationLevel { get; set; } = "";
    public int Dop { get; set; }
    public int ParallelWorkerCount { get; set; }
    public string WaitResource { get; set; } = "";
    public DateTime CollectionTime { get; set; }
    public string? QueryPlan { get; set; }
    public string? LiveQueryPlan { get; set; }
    public string LoginName { get; set; } = "";
    public string HostName { get; set; } = "";
    public string ProgramName { get; set; } = "";
    public int OpenTransactionCount { get; set; }
    public decimal PercentComplete { get; set; }
    public string QueryHash { get; set; } = "";

    /// <summary>Some row in the SAME capture names this session as its blocker (#3541 A13) — the reason a
    /// WAITFOR row can be on the MCP page. Set by <see cref="LocalDataService.GetActiveQueriesPageAsync"/> only.</summary>
    public bool IsHeadBlocker { get; set; }

    /// <summary>For a victim: its blocker had a row in the same capture at all. False is the idle
    /// open-transaction head blocker sys.dm_exec_requests never lists. MCP read only.</summary>
    public bool BlockerInCapture { get; set; }

    /// <summary>For a victim: its blocker also passes the caller's filters, so it is in the population the
    /// page is drawn from (it may still be past the page — the tool checks that). MCP read only.</summary>
    public bool BlockerInPopulation { get; set; }

    /// <summary>
    /// Whether a plan exists for this capture (#4239). Independent of <see cref="QueryPlan"/>: the three
    /// snapshot reads (<see cref="LocalDataService.GetLatestQuerySnapshotsAsync"/>,
    /// <see cref="LocalDataService.GetQuerySnapshotsByWaitTypeAsync"/>,
    /// <see cref="LocalDataService.GetAllQuerySnapshotsInRangeAsync"/>) set this from
    /// <c>query_plan IS NOT NULL</c> without selecting the payload, leaving <see cref="QueryPlan"/> null on
    /// the row; the plan buttons fetch it on click via <see cref="LocalDataService.ResolveSnapshotEstimatedPlanAsync"/>.
    /// The one path that still builds a row with the payload already in hand — <c>ServerTab.xaml.cs</c>'s
    /// Live Snapshot handler, whose rows are never in the store — sets this explicitly alongside
    /// <see cref="QueryPlan"/> instead of relying on a read.
    /// </summary>
    public bool HasQueryPlan { get; set; }
    /// <summary>See <see cref="HasQueryPlan"/> — the same independence, for <see cref="LiveQueryPlan"/>.</summary>
    public bool HasLiveQueryPlan { get; set; }
    public string CollectionTimeLocal => CollectionTime == DateTime.MinValue ? "" : ServerTimeHelper.FormatServerTime(CollectionTime);

    // Sessions this session is blocking at the same collection_time (SQL-derived in the
    // snapshot queries; Chain mode overwrites it with the chain walker's transitive count)
    public int BlockedSessionCount { get; set; }

    // Wait-drilldown triage columns collected from dm_exec_query_memory_grants,
    // session/task tempdb space usage, and dm_tran_* (schema v34)
    public double RequestedMemoryMb { get; set; }
    public double UsedMemoryMb { get; set; }
    public double MaxUsedMemoryMb { get; set; }
    public double TempdbCurrentMb { get; set; }
    public double TempdbAllocationsMb { get; set; }
    public double TranLogUsedMb { get; set; }
    public DateTime? TranStartTime { get; set; }
    public int RequestId { get; set; }
    /// <summary>The transaction begin time, empty when the request has no open transaction.
    /// <c>query_snapshots.tran_start_time</c> is <c>MIN(transaction_begin_time)</c> off
    /// <c>sys.dm_tran_active_transactions</c> — the server's own wall clock, not naive UTC — so unlike
    /// <see cref="CollectionTimeLocal"/> beside it, it renders through
    /// <see cref="ServerTimeHelper.FormatServerClock"/>.</summary>
    public string TranStartTimeLocal => ServerTimeHelper.FormatServerClock(TranStartTime);

    // Chain mode — set by WaitDrillDownWindow when showing head blockers
    public string ChainBlockingPath { get; set; } = "";
}
