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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The store reads behind the "who drove it" tools (#6072): <c>get_cpu_by_database</c>, <c>get_load_by_caller</c> and <c>get_query_pileups</c>.
/// All three are STORE-ONLY and read-only: they read what the collectors already wrote and never connect to the monitored server. Every command
/// carries <see cref="McpCommandDeadlines.ReadSeconds"/>. Windows are naive UTC, bound through <see cref="DarlingMcpReadParameters"/>.
/// </summary>
internal static class DarlingWhoDroveItReader
{
    /// <summary>One database's CPU from the statement stats, with its heaviest statement. Microseconds, as the collectors store them.</summary>
    internal sealed record DatabaseCpuRow(
        string DatabaseName, double CpuUs, double ElapsedUs, long Executions, long Statements, double ServerCpuUs,
        string? TopQueryHash, double? TopCpuUs, long? TopExecutions);

    /// <summary>One database's CPU from the procedure stats. It is the SAME work the statement stats count (a procedure's statements are in both), so it is shown beside the statement figure and never added to it.</summary>
    internal sealed record DatabaseProcedureRow(string DatabaseName, double CpuUs, long Executions, string? TopProcedure, double? TopProcedureCpuUs);

    /// <summary>
    /// CPU per database over the statement stats, grouped (database, query_hash) first and summed second, so a statement that ran
    /// under several plans is one statement. $1 server_id, $2/$3 window (naive UTC), $4 cap on databases, $5 the hourly rollup's
    /// materialisation ceiling (only when <paramref name="ceiling"/>). The server total is a window function over every database
    /// BEFORE the cap, so a share is a share of the server and not of the page.
    /// </summary>
    internal static string StatementCpuSql(bool hourly, bool ceiling)
    {
        var table = hourly ? "collect.query_stats_io_hourly" : "collect.query_stats";
        var time = hourly ? "bucket" : "collection_time";
        var cpu = hourly ? "worker_time_sum" : "delta_worker_time";
        var elapsed = hourly ? "elapsed_time_sum" : "delta_elapsed_time";
        var executions = hourly ? "execution_count_sum" : "delta_execution_count";
        var upper = hourly ? "<" : "<=";
        var tail = hourly
            ? (ceiling ? " AND q.bucket < $5" : string.Empty)
            : $" AND q.{TimescaleSupport.IntervalHonestSourceFilter}";
        return $"""
            WITH per_hash AS (
                SELECT COALESCE(q.database_name, '') AS database_name,
                       q.query_hash AS query_hash,
                       SUM(q.{cpu}) AS cpu_us,
                       SUM(q.{elapsed}) AS elapsed_us,
                       SUM(q.{executions}) AS executions
                FROM {table} AS q
                WHERE q.server_id = $1
                AND   q.{time} >= $2
                AND   q.{time} {upper} $3{tail}
                GROUP BY COALESCE(q.database_name, ''), q.query_hash
            ),
            per_db AS (
                SELECT database_name,
                       SUM(cpu_us) AS cpu_us,
                       SUM(elapsed_us) AS elapsed_us,
                       SUM(executions) AS executions,
                       COUNT(*) AS statements
                FROM per_hash
                GROUP BY database_name
            ),
            top_hash AS (
                SELECT DISTINCT ON (database_name) database_name, query_hash, cpu_us, executions
                FROM per_hash
                ORDER BY database_name, cpu_us DESC NULLS LAST, query_hash
            )
            SELECT d.database_name,
                   CAST(COALESCE(d.cpu_us, 0) AS double precision) AS cpu_us,
                   CAST(COALESCE(d.elapsed_us, 0) AS double precision) AS elapsed_us,
                   CAST(COALESCE(d.executions, 0) AS bigint) AS executions,
                   CAST(d.statements AS bigint) AS statements,
                   CAST(COALESCE(SUM(d.cpu_us) OVER (), 0) AS double precision) AS server_cpu_us,
                   t.query_hash AS top_query_hash,
                   CAST(t.cpu_us AS double precision) AS top_cpu_us,
                   CAST(t.executions AS bigint) AS top_executions
            FROM per_db AS d
            LEFT JOIN top_hash AS t ON t.database_name = d.database_name
            ORDER BY d.cpu_us DESC NULLS LAST, d.database_name
            LIMIT $4
            """;
    }

    /// <summary>
    /// CPU per database over the procedure stats, for the databases on the page ($5, <c>text[]</c>; '' is a NULL database), with each
    /// database's heaviest procedure. $1..$4 as <see cref="StatementCpuSql"/> ($4 is unused here and kept so one binder serves both), $6 ceiling.
    /// </summary>
    internal static string ProcedureCpuSql(bool hourly, bool ceiling)
    {
        var table = hourly ? "collect.procedure_stats_io_hourly" : "collect.procedure_stats";
        var time = hourly ? "bucket" : "collection_time";
        var cpu = hourly ? "worker_time_sum" : "delta_worker_time";
        var executions = hourly ? "execution_count_sum" : "delta_execution_count";
        var upper = hourly ? "<" : "<=";
        var tail = hourly
            ? (ceiling ? " AND q.bucket < $6" : string.Empty)
            : $" AND q.{TimescaleSupport.IntervalHonestSourceFilter}";
        return $"""
            WITH per_proc AS (
                SELECT COALESCE(q.database_name, '') AS database_name,
                       COALESCE(q.schema_name, '') || '.' || COALESCE(q.object_name, '') AS procedure_name,
                       SUM(q.{cpu}) AS cpu_us,
                       SUM(q.{executions}) AS executions
                FROM {table} AS q
                WHERE q.server_id = $1
                AND   q.{time} >= $2
                AND   q.{time} {upper} $3{tail}
                AND   COALESCE(q.database_name, '') = ANY($5)
                GROUP BY COALESCE(q.database_name, ''), COALESCE(q.schema_name, '') || '.' || COALESCE(q.object_name, '')
            ),
            per_db AS (
                SELECT database_name, SUM(cpu_us) AS cpu_us, SUM(executions) AS executions
                FROM per_proc
                GROUP BY database_name
            ),
            top_proc AS (
                SELECT DISTINCT ON (database_name) database_name, procedure_name, cpu_us
                FROM per_proc
                ORDER BY database_name, cpu_us DESC NULLS LAST, procedure_name
            )
            SELECT d.database_name,
                   CAST(COALESCE(d.cpu_us, 0) AS double precision) AS cpu_us,
                   CAST(COALESCE(d.executions, 0) AS bigint) AS executions,
                   t.procedure_name AS top_procedure,
                   CAST(t.cpu_us AS double precision) AS top_procedure_cpu_us
            FROM per_db AS d
            LEFT JOIN top_proc AS t ON t.database_name = d.database_name
            """;
    }

    /// <summary>The newest raw text of each statement on the page, by hash (index <c>idx_query_stats_server_hash_time</c>). $1 server_id, $2/$3 window, $4 hashes.</summary>
    internal const string StatementTextSql = """
        SELECT DISTINCT ON (q.database_name, q.query_hash)
               COALESCE(q.database_name, '') AS database_name,
               q.query_hash AS query_hash,
               COALESCE(q.query_text, d.query_text) AS query_text
        FROM collect.query_stats AS q
        LEFT JOIN query_text_dim AS d ON d.digest = q.query_text_digest
        WHERE q.server_id = $1
        AND   q.query_hash = ANY($4)
        AND   q.collection_time >= $2
        AND   q.collection_time <= $3
        AND   (q.query_text IS NOT NULL OR d.query_text IS NOT NULL)
        ORDER BY q.database_name, q.query_hash, q.collection_time DESC, q.collection_id DESC
        """;

    private static NpgsqlCommand WindowCommand(NpgsqlDataSource postgres, string sql, int serverId, DateTime startUtc, DateTime endUtc)
    {
        var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        return command;
    }

    private static double Dbl(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? 0 : Convert.ToDouble(reader.GetValue(ordinal), CultureInfo.InvariantCulture);

    /// <summary>The databases ranked by statement CPU, the first <paramref name="limit"/> of them (pass the cap plus one to observe a cut).</summary>
    internal static async Task<List<DatabaseCpuRow>> ReadStatementCpuAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit, bool hourly, DateTime? ceiling, CancellationToken cancellationToken)
    {
        var useCeiling = hourly && ceiling is not null;
        await using var command = WindowCommand(postgres, StatementCpuSql(hourly, useCeiling), serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, limit);
        if (useCeiling)
        {
            DarlingMcpReadParameters.AddTimestamp(command, ceiling!.Value);
        }

        var rows = new List<DatabaseCpuRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DatabaseCpuRow(
                reader.GetString(0), Dbl(reader, 1), Dbl(reader, 2), reader.GetInt64(3), reader.GetInt64(4), Dbl(reader, 5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : Dbl(reader, 7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8)));
        }

        return rows;
    }

    /// <summary>Procedure CPU for the named databases ('' = the NULL database).</summary>
    internal static async Task<Dictionary<string, DatabaseProcedureRow>> ReadProcedureCpuAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, string[] databases, bool hourly, DateTime? ceiling, CancellationToken cancellationToken)
    {
        var useCeiling = hourly && ceiling is not null;
        await using var command = WindowCommand(postgres, ProcedureCpuSql(hourly, useCeiling), serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, databases.Length);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = databases });
        if (useCeiling)
        {
            DarlingMcpReadParameters.AddTimestamp(command, ceiling!.Value);
        }

        var rows = new Dictionary<string, DatabaseProcedureRow>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var name = reader.GetString(0);
            rows[name] = new DatabaseProcedureRow(
                name, Dbl(reader, 1), reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : Dbl(reader, 4));
        }

        return rows;
    }

    /// <summary>The newest raw statement text per (database, hash), for the hashes asked; a hash the raw tier no longer holds has none.</summary>
    internal static async Task<Dictionary<(string Database, string Hash), string>> ReadStatementTextAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, string[] hashes, CancellationToken cancellationToken)
    {
        var texts = new Dictionary<(string, string), string>();
        if (hashes.Length == 0)
        {
            return texts;
        }

        await using var command = WindowCommand(postgres, StatementTextSql, serverId, startUtc, endUtc);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = hashes });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(2))
            {
                texts[(reader.GetString(0), reader.GetString(1))] = reader.GetString(2);
            }
        }

        return texts;
    }

    /* ───────────────────────── get_load_by_caller ───────────────────────── */

    /// <summary>One caller group (host, program, login) over the active-query snapshots, with its heaviest statements.</summary>
    internal sealed record CallerGroup(
        string HostName, string ProgramName, string LoginName, long Samples, long Captures, double CpuMs, double ElapsedMs,
        double TotalCpuMs, double TotalElapsedMs, long TotalSamples, DateTime FirstSeen, DateTime LastSeen,
        List<CallerStatement> Statements);

    internal sealed record CallerStatement(string QueryHash, long Samples, double CpuMs, string? QueryText);

    /// <summary>
    /// The active-query snapshots of the window grouped by (host_name, program_name, login_name), ranked by summed cpu_time_ms. $1 server_id,
    /// $2/$3 window (naive UTC), $4 groups on the page, $5 statements per group. A sampled request carries its CUMULATIVE cpu_time_ms at
    /// each capture, so the sums count a long request once per capture it appears in: they weigh a caller by how much and how long it was
    /// running, not by completed work. The WAITFOR shells get_active_queries drops are dropped here too. The totals are window functions over every
    /// group BEFORE the cap, so a share is a share of the server. Statement text is fetched in the same pass, for the top statements only.
    /// Reads query_snapshots by (server_id, collection_time) range; no index is added.
    /// </summary>
    internal const string LoadByCallerSql = """
        WITH snap AS MATERIALIZED (
            SELECT COALESCE(host_name, '') AS host_name,
                   COALESCE(program_name, '') AS program_name,
                   COALESCE(login_name, '') AS login_name,
                   query_hash,
                   collection_time,
                   COALESCE(cpu_time_ms, 0) AS cpu_ms,
                   COALESCE(total_elapsed_time_ms, 0) AS elapsed_ms,
                   query_text
            FROM query_snapshots
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   (query_text IS NULL OR query_text NOT LIKE 'WAITFOR%')
        ),
        grp AS (
            SELECT host_name, program_name, login_name,
                   COUNT(*) AS samples,
                   COUNT(DISTINCT collection_time) AS captures,
                   SUM(cpu_ms) AS cpu_ms,
                   SUM(elapsed_ms) AS elapsed_ms,
                   MIN(collection_time) AS first_seen,
                   MAX(collection_time) AS last_seen
            FROM snap
            GROUP BY host_name, program_name, login_name
        ),
        ranked AS (
            SELECT g.*,
                   SUM(cpu_ms) OVER () AS total_cpu_ms,
                   SUM(elapsed_ms) OVER () AS total_elapsed_ms,
                   SUM(samples) OVER () AS total_samples,
                   ROW_NUMBER() OVER (ORDER BY cpu_ms DESC, samples DESC, host_name, program_name, login_name) AS rk
            FROM grp AS g
        ),
        page AS (SELECT * FROM ranked WHERE rk <= $4),
        per_hash AS (
            SELECT s.host_name, s.program_name, s.login_name, s.query_hash,
                   COUNT(*) AS samples, SUM(s.cpu_ms) AS cpu_ms
            FROM snap AS s
            JOIN page AS p USING (host_name, program_name, login_name)
            WHERE s.query_hash IS NOT NULL
            GROUP BY s.host_name, s.program_name, s.login_name, s.query_hash
        ),
        top_hash AS (
            SELECT * FROM (
                SELECT h.*, ROW_NUMBER() OVER (PARTITION BY host_name, program_name, login_name ORDER BY cpu_ms DESC, samples DESC, query_hash) AS hr
                FROM per_hash AS h
            ) AS x
            WHERE hr <= $5
        ),
        txt AS (
            SELECT DISTINCT ON (s.query_hash) s.query_hash, s.query_text
            FROM snap AS s
            WHERE s.query_text IS NOT NULL
            AND   s.query_hash IN (SELECT query_hash FROM top_hash)
            ORDER BY s.query_hash, s.collection_time DESC
        )
        SELECT p.host_name, p.program_name, p.login_name,
               CAST(p.samples AS bigint) AS samples, CAST(p.captures AS bigint) AS captures,
               CAST(p.cpu_ms AS double precision) AS cpu_ms, CAST(p.elapsed_ms AS double precision) AS elapsed_ms,
               CAST(p.total_cpu_ms AS double precision) AS total_cpu_ms, CAST(p.total_elapsed_ms AS double precision) AS total_elapsed_ms,
               CAST(p.total_samples AS bigint) AS total_samples,
               p.first_seen, p.last_seen, p.rk,
               t.query_hash, CAST(t.samples AS bigint) AS statement_samples, CAST(t.cpu_ms AS double precision) AS statement_cpu_ms,
               x.query_text
        FROM page AS p
        LEFT JOIN top_hash AS t
               ON t.host_name = p.host_name AND t.program_name = p.program_name AND t.login_name = p.login_name
        LEFT JOIN txt AS x ON x.query_hash = t.query_hash
        ORDER BY p.rk, t.hr
        """;

    internal static async Task<List<CallerGroup>> ReadLoadByCallerAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int groups, int statements, CancellationToken cancellationToken)
    {
        await using var command = WindowCommand(postgres, LoadByCallerSql, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, groups);
        DarlingMcpReadParameters.AddInt(command, statements);
        var result = new List<CallerGroup>();
        long lastRank = -1;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var rank = reader.GetInt64(12);
            if (rank != lastRank)
            {
                lastRank = rank;
                result.Add(new CallerGroup(
                    reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt64(3), reader.GetInt64(4),
                    Dbl(reader, 5), Dbl(reader, 6), Dbl(reader, 7), Dbl(reader, 8), reader.GetInt64(9),
                    reader.GetDateTime(10), reader.GetDateTime(11), []));
            }

            if (!reader.IsDBNull(13))
            {
                result[^1].Statements.Add(new CallerStatement(
                    reader.GetString(13), reader.GetInt64(14), Dbl(reader, 15), reader.IsDBNull(16) ? null : reader.GetString(16)));
            }
        }

        return result;
    }

    /* ───────────────────────── get_query_pileups ───────────────────────── */

    /// <summary>One capture time where several sessions ran one query_hash.</summary>
    internal sealed record Pileup(
        DateTime CollectionTime, string QueryHash, long Sessions, long BlockedSessions, long MaxWaitMs, string? QueryText,
        List<(string WaitType, long Sessions)> Waits);

    /// <summary>
    /// The capture times where at least $4 distinct sessions ran the same non-NULL query_hash at once, most sessions first, newest first among
    /// equals, $5 of them. $1 server_id, $2/$3 window. The waits of each pile-up come from the same rows, so the counts add up to its sessions
    /// (a session with no wait has a NULL wait_type row). Reads query_snapshots by (server_id, collection_time) range; no index is added.
    /// </summary>
    internal const string PileupsSql = """
        WITH snap AS MATERIALIZED (
            SELECT collection_time, query_hash, session_id, NULLIF(btrim(wait_type), '') AS wait_type,
                   COALESCE(wait_time_ms, 0) AS wait_time_ms, COALESCE(blocking_session_id, 0) AS blocking_session_id, query_text
            FROM query_snapshots
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   query_hash IS NOT NULL
            AND   (query_text IS NULL OR query_text NOT LIKE 'WAITFOR%')
        ),
        piles AS (
            SELECT collection_time, query_hash,
                   COUNT(DISTINCT session_id) AS sessions,
                   COUNT(DISTINCT session_id) FILTER (WHERE blocking_session_id > 0) AS blocked_sessions,
                   MAX(wait_time_ms) AS max_wait_ms,
                   MAX(query_text) AS query_text
            FROM snap
            GROUP BY collection_time, query_hash
            HAVING COUNT(DISTINCT session_id) >= $4
            ORDER BY sessions DESC, collection_time DESC, query_hash
            LIMIT $5
        ),
        waits AS (
            SELECT s.collection_time, s.query_hash, s.wait_type, COUNT(DISTINCT s.session_id) AS n
            FROM snap AS s
            JOIN piles AS p ON p.collection_time = s.collection_time AND p.query_hash = s.query_hash
            GROUP BY s.collection_time, s.query_hash, s.wait_type
        )
        SELECT p.collection_time, p.query_hash, CAST(p.sessions AS bigint) AS sessions, CAST(p.blocked_sessions AS bigint) AS blocked_sessions,
               CAST(p.max_wait_ms AS bigint) AS max_wait_ms, p.query_text, w.wait_type, CAST(w.n AS bigint) AS wait_sessions
        FROM piles AS p
        LEFT JOIN waits AS w ON w.collection_time = p.collection_time AND w.query_hash = p.query_hash
        ORDER BY p.sessions DESC, p.collection_time DESC, p.query_hash, w.n DESC, w.wait_type
        """;

    internal static async Task<List<Pileup>> ReadPileupsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int minSessions, int limit, CancellationToken cancellationToken)
    {
        await using var command = WindowCommand(postgres, PileupsSql, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, minSessions);
        DarlingMcpReadParameters.AddInt(command, limit);
        var piles = new List<Pileup>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var time = reader.GetDateTime(0);
            var hash = reader.GetString(1);
            if (piles.Count == 0 || piles[^1].CollectionTime != time || piles[^1].QueryHash != hash)
            {
                piles.Add(new Pileup(time, hash, reader.GetInt64(2), reader.GetInt64(3), reader.GetInt64(4), reader.IsDBNull(5) ? null : reader.GetString(5), []));
            }

            if (!reader.IsDBNull(7))
            {
                piles[^1].Waits.Add((reader.IsDBNull(6) ? "(none)" : reader.GetString(6), reader.GetInt64(7)));
            }
        }

        return piles;
    }

    /// <summary>One blocked/blocking pair of a DMV blocking snapshot.</summary>
    internal sealed record BlockingPair(
        DateTime CollectionTime, int BlockedSpid, int BlockingSpid, long WaitTimeMs, string? BlockingLogin, string? BlockingHost, string? BlockingApp,
        string? BlockingSql, string? Database);

    /// <summary>
    /// For each time in $4 (<c>timestamp[]</c>), the DMV blocking snapshot nearest to it within $5 seconds, either side; ties go to the earlier
    /// snapshot. $1 server_id. Returns (asked time, snapshot time). The probe is a range scan on (server_id, collection_time) over 2 x $5 seconds
    /// per time, and the snapshot collector writes rows only while something is blocked.
    /// </summary>
    internal const string NearestBlockingSnapshotSql = """
        SELECT t.at AS asked, n.collection_time AS found
        FROM unnest($4::timestamp[]) AS t(at)
        CROSS JOIN LATERAL (
            SELECT b.collection_time
            FROM collect.dmv_blocking_snapshots AS b
            WHERE b.server_id = $1
            AND   b.collection_time >= t.at - make_interval(secs => $5)
            AND   b.collection_time <= t.at + make_interval(secs => $5)
            GROUP BY b.collection_time
            ORDER BY ABS(EXTRACT(EPOCH FROM (b.collection_time - t.at))), b.collection_time
            LIMIT 1
        ) AS n
        """;

    /// <summary>Every blocked/blocking pair of the snapshots in $2 (<c>timestamp[]</c>). $1 server_id.</summary>
    internal const string BlockingPairsSql = """
        SELECT collection_time, blocked_spid, blocking_spid, COALESCE(wait_time_ms, 0), blocking_login_name, blocking_host_name,
               blocking_client_app, blocking_sql_text, database_name
        FROM collect.dmv_blocking_snapshots
        WHERE server_id = $1
        AND   collection_time = ANY($2)
        """;

    internal static async Task<Dictionary<DateTime, DateTime>> ReadNearestBlockingSnapshotsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime[] times, int toleranceSeconds, CancellationToken cancellationToken)
    {
        var found = new Dictionary<DateTime, DateTime>();
        if (times.Length == 0)
        {
            return found;
        }

        await using var command = postgres.CreateCommand(NearestBlockingSnapshotSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = times.Select(t => DateTime.SpecifyKind(t, DateTimeKind.Unspecified)).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = (double)toleranceSeconds });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            found[reader.GetDateTime(0)] = reader.GetDateTime(1);
        }

        return found;
    }

    internal static async Task<List<BlockingPair>> ReadBlockingPairsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime[] snapshotTimes, CancellationToken cancellationToken)
    {
        var pairs = new List<BlockingPair>();
        if (snapshotTimes.Length == 0)
        {
            return pairs;
        }

        await using var command = postgres.CreateCommand(BlockingPairsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = snapshotTimes.Select(t => DateTime.SpecifyKind(t, DateTimeKind.Unspecified)).ToArray() });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            pairs.Add(new BlockingPair(
                reader.GetDateTime(0), reader.IsDBNull(1) ? 0 : reader.GetInt32(1), reader.IsDBNull(2) ? 0 : reader.GetInt32(2), reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetString(8)));
        }

        return pairs;
    }
}
