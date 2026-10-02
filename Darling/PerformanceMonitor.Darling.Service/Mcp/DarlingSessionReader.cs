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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the session MCP tools (<see cref="DarlingMcpSessionTools"/>) — the SAME
/// collected data Lite's <c>McpSessionTools</c> / <c>McpWaitTools</c> and the viewer read, adapted here
/// so the MCP host never references the WPF viewer project. Each is a STORED read (no live
/// monitored-server hit) keyed by <c>server_id</c>.
///
/// <para>
/// get_session_stats reads the LATEST per-application snapshot from <c>v_session_stats</c> (Lite's
/// <c>GetLatestSessionStatsAsync</c> — the store-faithful per-<c>program_name</c> shape, NOT the
/// Dashboard's server-wide <c>session_summary_stats</c> summary; where the two diverge slices 1+2 follow
/// Lite). get_active_queries reads the <c>query_snapshots</c> base table over the window (the viewer's
/// <c>LatestQuerySnapshotsSql</c>). get_waiting_tasks reads the <c>waiting_tasks</c> BASE table (there is
/// no <c>v_waiting_tasks</c> passthrough view — the base table has existed since V1, and the viewer's
/// trend reads hit it directly the same way); <c>resource_description</c> is stored but the collector
/// writes it NULL ("no longer collected"). Every SQL string is a public const so Darling.Tests can pin the
/// dialect + columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingSessionReader
{
    /* ─────────────────────────── result rows ─────────────────────────── */

    /// <summary>One (program_name) group at the latest session_stats snapshot.</summary>
    public sealed record SessionStatRow(
        DateTime CollectionTime, string ProgramName, long ConnectionCount, int RunningCount, int SleepingCount,
        int DormantCount, long? TotalCpuTimeMs, long? TotalReads, long? TotalWrites, long? TotalLogicalReads);

    /// <summary>One captured query snapshot — the columns Lite's get_active_queries surfaces.</summary>
    public sealed record ActiveQueryRow(
        DateTime CollectionTime, int SessionId, string? DatabaseName, string? Status, long CpuTimeMs,
        long TotalElapsedTimeMs, string? ElapsedTimeFormatted, long LogicalReads, long Reads, long Writes,
        string? WaitType, long WaitTimeMs, int BlockingSessionId, int Dop, int ParallelWorkerCount,
        double GrantedQueryMemoryGb, string? TransactionIsolationLevel, int OpenTransactionCount,
        string? LoginName, string? HostName, string? ProgramName, string? QueryText)
    {
        /// <summary>Some row in the SAME capture names this session as its blocker (#3541 A13) — the reason a
        /// WAITFOR row can be on the page.</summary>
        public bool IsHeadBlocker { get; init; }

        /// <summary>For a victim: its blocker had a row in the same capture at all. False is the idle
        /// open-transaction head blocker sys.dm_exec_requests never lists.</summary>
        public bool BlockerInCapture { get; init; }

        /// <summary>For a victim: its blocker also passes the caller's filters, so it is in the population the
        /// page is drawn from (it may still be past the page — the tool checks that).</summary>
        public bool BlockerInPopulation { get; init; }
    }

    /// <summary>One waiting-task snapshot row.</summary>
    public sealed record WaitingTaskRow(
        DateTime CollectionTime, int SessionId, string? WaitType, long WaitDurationMs,
        int? BlockingSessionId, string? ResourceDescription, string? DatabaseName);

    /* ─────────────────────────── session stats (latest snapshot, per application) ─────────────────────────── */

    /// <summary>
    /// The latest per-application session_stats snapshot — Lite's <c>GetLatestSessionStatsAsync</c>: every
    /// program_name at the newest collection, most connections first. connection_count is bigint; the
    /// running/sleeping/dormant counts are integer; the cumulative totals are nullable bigint. $1 server_id.
    /// </summary>
    public const string LatestSessionStatsSql = """
        SELECT
            collection_time,
            program_name,
            connection_count,
            running_count,
            sleeping_count,
            dormant_count,
            total_cpu_time_ms,
            total_reads,
            total_writes,
            total_logical_reads
        FROM v_session_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_session_stats WHERE server_id = $1)
        ORDER BY connection_count DESC
        """;

    public static async Task<List<SessionStatRow>> GetLatestSessionStatsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<SessionStatRow>();
        await using var command = postgres.CreateCommand(LatestSessionStatsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new SessionStatRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9)));
        }

        return rows;
    }

    /* ─────────────────────────── active queries (query snapshots over the window) ─────────────────────────── */

    /// <summary>
    /// The captured query snapshots over the window — the viewer's <c>LatestQuerySnapshotsSql</c> projected
    /// to the columns Lite's get_active_queries surfaces, from the base <c>query_snapshots</c> table (the
    /// viewer reads base here too). granted_query_memory_gb is <c>numeric(18,2)</c> → double precision.
    /// $1 server_id, $2/$3 window (naive UTC), $4 row cap, $5 database filter (NULL = all), $6 blocking_only.
    ///
    /// <para><b>Every filter is part of the query (#3541 A13).</b> This read used to return the whole window
    /// unfiltered and unbounded; the tool then applied <c>database_name</c> and <c>blocking_only</c> in C#,
    /// took <c>limit</c>, and published the pre-filter row count as <c>total_snapshots</c> beside the page —
    /// so the "total" was of a different population from the rows, and a page could be empty while the
    /// window held matches. Now the filters are predicates on the same statement, the population count is
    /// <c>COUNT(*) OVER ()</c> on the FILTERED rows above the parameterised <c>LIMIT</c> (the #3613 idiom), and
    /// the cap is the caller's, fetched at <c>limit + 1</c> so truncation is observed rather than inferred.</para>
    ///
    /// <para><b>Head blockers are never stripped (#3541 A13).</b> The WAITFOR trim exists to drop the idle
    /// shells a monitoring session leaves in dm_exec_requests, but the classic head blocker IS a session
    /// sitting in <c>WAITFOR</c> with an open transaction — and this read dropped it while its victims'
    /// <c>blocking_session_id</c> pointed at the session that was no longer on the page. A row is kept
    /// whatever its text when some row in the SAME capture names it as its blocker. Same capture, not same
    /// window: session ids are reused, so "any row in the window points at this session id" would resurrect
    /// unrelated sessions from other snapshots, which is what the old C# arm did.</para>
    ///
    /// <para><b>The two blocker-presence flags.</b> <c>blocker_in_capture</c>: the victim's blocker had a row
    /// in the same capture at all — FALSE is the other classic head blocker, a session idle in an open
    /// transaction, which sys.dm_exec_requests never lists and so was never captured; the tool says so on the
    /// row rather than leaving a dangling id. <c>blocker_in_population</c>: the blocker also passes the
    /// caller's own filters (a head blocker in another database under a <c>database_name</c> filter does
    /// not) — the caller asked for that database, so the row is honoured and the victim says its blocker was
    /// filtered. A blocker that passes both but falls past the page is detected by the tool, which has the
    /// page.</para>
    /// </summary>
    public const string ActiveQueriesSql = """
        WITH window_rows AS (
            SELECT
                collection_time,
                session_id,
                database_name,
                status,
                cpu_time_ms,
                total_elapsed_time_ms,
                elapsed_time_formatted,
                logical_reads,
                reads,
                writes,
                wait_type,
                wait_time_ms,
                blocking_session_id,
                dop,
                parallel_worker_count,
                CAST(granted_query_memory_gb AS double precision) AS granted_query_memory_gb,
                transaction_isolation_level,
                open_transaction_count,
                login_name,
                host_name,
                program_name,
                query_text
            FROM query_snapshots
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
            FROM window_rows AS w
            LEFT JOIN heads AS h
              ON  h.collection_time = w.collection_time
              AND h.session_id = w.session_id
            WHERE (w.query_text NOT LIKE 'WAITFOR%' OR h.session_id IS NOT NULL)
            AND   ($5::text IS NULL OR w.database_name = $5)
            AND   (NOT $6::boolean OR w.blocking_session_id > 0 OR h.session_id IS NOT NULL)
        )
        SELECT
            p.collection_time,
            p.session_id,
            p.database_name,
            p.status,
            p.cpu_time_ms,
            p.total_elapsed_time_ms,
            p.elapsed_time_formatted,
            p.logical_reads,
            p.reads,
            p.writes,
            p.wait_type,
            p.wait_time_ms,
            p.blocking_session_id,
            p.dop,
            p.parallel_worker_count,
            p.granted_query_memory_gb,
            p.transaction_isolation_level,
            p.open_transaction_count,
            p.login_name,
            p.host_name,
            p.program_name,
            p.query_text,
            p.is_head_blocker,
            p.blocker_in_capture,
            EXISTS (
                SELECT 1
                FROM population q
                WHERE q.collection_time = p.collection_time
                AND   q.session_id = p.blocking_session_id
            ) AS blocker_in_population,
            COUNT(*) OVER () AS population_count
        FROM population AS p
        ORDER BY p.collection_time DESC, p.cpu_time_ms DESC
        LIMIT $4
        """;

    /// <summary>The filtered page plus the filtered population's size, from one statement.</summary>
    public sealed record ActiveQueriesPage(List<ActiveQueryRow> Rows, long PopulationCount);

    /// <summary>
    /// The newest <paramref name="cap"/> snapshots over the window that pass the filters, with the filtered
    /// population's count (#3541 A13). Callers detecting truncation pass <c>limit + 1</c> and read the extra
    /// row as the signal. <paramref name="databaseName"/> null = every database; <paramref name="blockingOnly"/>
    /// keeps victims and the head blockers of victims in the same capture.
    /// </summary>
    public static async Task<ActiveQueriesPage> GetActiveQueriesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int cap,
        string? databaseName = null, bool blockingOnly = false, CancellationToken cancellationToken = default)
    {
        var rows = new List<ActiveQueryRow>();
        long populationCount = 0;
        await using var command = postgres.CreateCommand(ActiveQueriesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, cap);
        DarlingMcpReadParameters.AddNullableText(command, databaseName);
        DarlingMcpReadParameters.AddBoolean(command, blockingOnly);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ActiveQueryRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? null : reader.GetString(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                reader.IsDBNull(12) ? 0 : reader.GetInt32(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                reader.IsDBNull(14) ? 0 : reader.GetInt32(14),
                reader.IsDBNull(15) ? 0 : reader.GetDouble(15),
                reader.IsDBNull(16) ? null : reader.GetString(16),
                reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                reader.IsDBNull(18) ? null : reader.GetString(18),
                reader.IsDBNull(19) ? null : reader.GetString(19),
                reader.IsDBNull(20) ? null : reader.GetString(20),
                reader.IsDBNull(21) ? null : reader.GetString(21))
            {
                IsHeadBlocker = !reader.IsDBNull(22) && reader.GetBoolean(22),
                BlockerInCapture = !reader.IsDBNull(23) && reader.GetBoolean(23),
                BlockerInPopulation = !reader.IsDBNull(24) && reader.GetBoolean(24),
            });
            populationCount = reader.GetInt64(25);
        }

        return new ActiveQueriesPage(rows, populationCount);
    }

    /* ─────────────────────────── waiting tasks (base table over the window) ─────────────────────────── */

    /// <summary>
    /// The recently-captured waiting tasks over the window — the base <c>waiting_tasks</c> table (there is no
    /// <c>v_waiting_tasks</c> view), newest first then longest wait. resource_description is stored but always
    /// NULL (the collector no longer collects it). $1 server_id, $2/$3 window (naive UTC), $4 row cap.
    ///
    /// <para>The cap is a PARAMETER, not a literal (#3541 A3). It was <c>LIMIT 500</c> under a tool that
    /// advertised <c>limit</c>, applied it with <c>Take(limit)</c>, and then published a bare envelope with no
    /// window, no count and no bound — so a caller could not tell thirty tasks from thirty of five thousand.</para>
    /// </summary>
    public const string WaitingTasksSql = """
        SELECT
            collection_time,
            session_id,
            wait_type,
            wait_duration_ms,
            blocking_session_id,
            resource_description,
            database_name
        FROM waiting_tasks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   wait_type IS NOT NULL
        ORDER BY collection_time DESC, wait_duration_ms DESC
        LIMIT $4
        """;

    /// <summary>The newest <paramref name="cap"/> waiting tasks over the window. Callers detecting truncation
    /// pass <c>limit + 1</c> and read the extra row as the signal.</summary>
    public static async Task<List<WaitingTaskRow>> GetWaitingTasksAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int cap, CancellationToken cancellationToken = default)
    {
        var rows = new List<WaitingTaskRow>();
        await using var command = postgres.CreateCommand(WaitingTasksSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, cap);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new WaitingTaskRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6)));
        }

        return rows;
    }
}
