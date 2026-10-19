/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The DuckDB reads behind <c>get_cpu_by_database</c>, <c>get_load_by_caller</c> and <c>get_query_pileups</c> (#6114), the Lite twins of
/// Darling's <c>DarlingWhoDroveItReader</c> (#6072). Every figure keeps the store's precision: DuckDB timestamps carry microseconds and
/// nothing here cuts them. Ordering follows Darling's, with NULLS LAST spelled out on every key (PostgreSQL's ascending default), so a tie
/// or a NULL sorts the same way on both products.
/// </summary>
public partial class LocalDataService
{
    /// <summary>One database's CPU from the statement stats, with its heaviest statement and the procedure stats' own figure. Microseconds, as the collectors store them.</summary>
    public sealed record DatabaseCpuRow(
        string DatabaseName, double CpuUs, double ElapsedUs, long Executions, long Statements, double ServerCpuUs,
        string? TopQueryHash, double? TopCpuUs, long? TopExecutions, string? QueryText,
        double? ProcedureCpuUs, string? TopProcedure, double? TopProcedureCpuUs);

    /// <summary>
    /// CPU per database over the statement stats, grouped (database, query_hash) first and summed second, so a statement that ran under
    /// several plans is one statement. $1 server_id, $2/$3 window, $4 cap on databases. The server total is a window function over every
    /// database BEFORE the cap, so a share is a share of the server and not of the page. The procedure stats ride beside it for the
    /// databases on the page only; they are the SAME work the statement stats count and are never added to them.
    /// </summary>
    internal const string CpuByDatabaseSql = @"
WITH per_hash AS (
    SELECT COALESCE(database_name, '') AS database_name,
           query_hash,
           SUM(delta_worker_time) AS cpu_us,
           SUM(delta_elapsed_time) AS elapsed_us,
           SUM(delta_execution_count) AS executions
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY COALESCE(database_name, ''), query_hash
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
    SELECT database_name, query_hash, cpu_us, executions
    FROM (
        SELECT h.*, ROW_NUMBER() OVER (PARTITION BY database_name ORDER BY cpu_us DESC NULLS LAST, query_hash NULLS LAST) AS rn
        FROM per_hash AS h
    ) AS x
    WHERE rn = 1
),
page AS (
    SELECT d.database_name,
           CAST(COALESCE(d.cpu_us, 0) AS DOUBLE) AS cpu_us,
           CAST(COALESCE(d.elapsed_us, 0) AS DOUBLE) AS elapsed_us,
           CAST(COALESCE(d.executions, 0) AS BIGINT) AS executions,
           CAST(d.statements AS BIGINT) AS statements,
           CAST(COALESCE(SUM(d.cpu_us) OVER (), 0) AS DOUBLE) AS server_cpu_us,
           t.query_hash AS top_query_hash,
           CAST(t.cpu_us AS DOUBLE) AS top_cpu_us,
           CAST(t.executions AS BIGINT) AS top_executions,
           ROW_NUMBER() OVER (ORDER BY d.cpu_us DESC NULLS LAST, d.database_name NULLS LAST) AS rk
    FROM per_db AS d
    LEFT JOIN top_hash AS t ON t.database_name = d.database_name
    ORDER BY d.cpu_us DESC NULLS LAST, d.database_name NULLS LAST
    LIMIT $4
),
per_proc AS (
    SELECT COALESCE(q.database_name, '') AS database_name,
           COALESCE(q.schema_name, '') || '.' || COALESCE(q.object_name, '') AS procedure_name,
           SUM(q.delta_worker_time) AS cpu_us
    FROM v_procedure_stats AS q
    WHERE q.server_id = $1
    AND   q.collection_time >= $2
    AND   q.collection_time <= $3
    AND   COALESCE(q.database_name, '') IN (SELECT database_name FROM page)
    GROUP BY COALESCE(q.database_name, ''), COALESCE(q.schema_name, '') || '.' || COALESCE(q.object_name, '')
),
proc_db AS (
    SELECT database_name, SUM(cpu_us) AS cpu_us FROM per_proc GROUP BY database_name
),
top_proc AS (
    SELECT database_name, procedure_name, cpu_us
    FROM (
        SELECT p.*, ROW_NUMBER() OVER (PARTITION BY database_name ORDER BY cpu_us DESC NULLS LAST, procedure_name NULLS LAST) AS rn
        FROM per_proc AS p
    ) AS x
    WHERE rn = 1
),
txt AS (
    SELECT COALESCE(q.database_name, '') AS database_name, q.query_hash,
           arg_max(q.query_text, (q.collection_time, q.collection_id)) FILTER (WHERE q.query_text IS NOT NULL) AS query_text
    FROM v_query_stats AS q
    JOIN page AS p ON p.database_name = COALESCE(q.database_name, '') AND p.top_query_hash = q.query_hash
    WHERE q.server_id = $1
    AND   q.collection_time >= $2
    AND   q.collection_time <= $3
    GROUP BY COALESCE(q.database_name, ''), q.query_hash
)
SELECT p.database_name, p.cpu_us, p.elapsed_us, p.executions, p.statements, p.server_cpu_us,
       p.top_query_hash, p.top_cpu_us, p.top_executions, x.query_text,
       CAST(d.cpu_us AS DOUBLE) AS procedure_cpu_us, t.procedure_name AS top_procedure, CAST(t.cpu_us AS DOUBLE) AS top_procedure_cpu_us
FROM page AS p
LEFT JOIN txt AS x ON x.database_name = p.database_name AND x.query_hash = p.top_query_hash
LEFT JOIN proc_db AS d ON d.database_name = p.database_name
LEFT JOIN top_proc AS t ON t.database_name = p.database_name
ORDER BY p.rk";

    /// <summary>The databases ranked by statement CPU, the first <paramref name="limit"/> of them (pass the cap plus one to observe a cut).</summary>
    public async Task<List<DatabaseCpuRow>> GetCpuByDatabaseAsync(int serverId, DateTime startUtc, DateTime endUtc, int limit)
    {
        using var _q = TimeQuery("GetCpuByDatabaseAsync", "v_query_stats per database CPU (MCP)");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = CpuByDatabaseSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        command.Parameters.Add(new DuckDBParameter { Value = limit });

        var rows = new List<DatabaseCpuRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new DatabaseCpuRow(
                reader.GetString(0), reader.GetDouble(1), reader.GetDouble(2), reader.GetInt64(3), reader.GetInt64(4), reader.GetDouble(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetDouble(7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetDouble(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                reader.IsDBNull(12) ? null : reader.GetDouble(12)));
        }

        return rows;
    }

    /* ───────────────────────── get_load_by_caller ───────────────────────── */

    /// <summary>One caller group (host, program, login) over the active-query snapshots, with its heaviest statements.</summary>
    public sealed record CallerGroup(
        string HostName, string ProgramName, string LoginName, long Samples, long Captures, long Requests, double CpuMs, double ElapsedMs,
        double TotalCpuMs, double TotalElapsedMs, long TotalSamples, long TotalRequests, DateTime FirstSeen, DateTime LastSeen,
        List<CallerStatement> Statements);

    public sealed record CallerStatement(string QueryHash, long Samples, long Requests, double CpuMs, string? QueryText);

    /// <summary>
    /// The active-query snapshots of the window grouped by (host_name, program_name, login_name), ranked by cpu_ms. $1 server_id, $2/$3
    /// window, $4 groups on the page, $5 statements per group. A snapshot carries its request's CUMULATIVE cpu_time_ms and
    /// total_elapsed_time_ms at each capture, so each request counts ONCE, at its largest figure over its captures, and the groups sum
    /// those. A request is a session_id plus its start (collection_time minus total_elapsed_time_ms, cut to the second: the table has no
    /// request start column, and the start jitters by a few milliseconds between captures). A start within that jitter of a second
    /// boundary can land in two buckets and count the request twice, the price of a key with no request id; the same trade Darling's
    /// twin makes. A request that runs several statements adds its figure to each statement's per-hash total, so the per-statement
    /// figures can add up to more than the caller's. WAITFOR shells are dropped, as get_active_queries drops them. The totals are window
    /// functions over every group BEFORE the cap, so a share is a share of the window.
    /// </summary>
    internal const string LoadByCallerSql = @"
WITH snap AS (
    SELECT COALESCE(host_name, '') AS host_name,
           COALESCE(program_name, '') AS program_name,
           COALESCE(login_name, '') AS login_name,
           COALESCE(session_id, -1) AS session_id,
           date_trunc('second', collection_time - COALESCE(total_elapsed_time_ms, 0) * INTERVAL '1' MILLISECOND) AS request_start,
           query_hash,
           collection_time,
           COALESCE(cpu_time_ms, 0) AS cpu_ms,
           COALESCE(total_elapsed_time_ms, 0) AS elapsed_ms,
           query_text
    FROM v_query_snapshots
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   (query_text IS NULL OR query_text NOT LIKE 'WAITFOR%')
),
req AS (
    SELECT host_name, program_name, login_name, session_id, request_start,
           MAX(cpu_ms) AS cpu_ms, MAX(elapsed_ms) AS elapsed_ms
    FROM snap
    GROUP BY host_name, program_name, login_name, session_id, request_start
),
grp_req AS (
    SELECT host_name, program_name, login_name,
           COUNT(*) AS requests, SUM(cpu_ms) AS cpu_ms, SUM(elapsed_ms) AS elapsed_ms
    FROM req
    GROUP BY host_name, program_name, login_name
),
grp_snap AS (
    SELECT host_name, program_name, login_name,
           COUNT(*) AS samples,
           COUNT(DISTINCT collection_time) AS captures,
           MIN(collection_time) AS first_seen,
           MAX(collection_time) AS last_seen
    FROM snap
    GROUP BY host_name, program_name, login_name
),
grp AS (
    SELECT s.host_name, s.program_name, s.login_name, s.samples, s.captures, s.first_seen, s.last_seen,
           r.requests, r.cpu_ms, r.elapsed_ms
    FROM grp_snap AS s
    JOIN grp_req AS r ON r.host_name = s.host_name AND r.program_name = s.program_name AND r.login_name = s.login_name
),
ranked AS (
    SELECT g.*,
           SUM(cpu_ms) OVER () AS total_cpu_ms,
           SUM(elapsed_ms) OVER () AS total_elapsed_ms,
           SUM(samples) OVER () AS total_samples,
           SUM(requests) OVER () AS total_requests,
           ROW_NUMBER() OVER (ORDER BY cpu_ms DESC NULLS LAST, samples DESC NULLS LAST, host_name NULLS LAST, program_name NULLS LAST, login_name NULLS LAST) AS rk
    FROM grp AS g
),
page AS (SELECT * FROM ranked WHERE rk <= $4),
hash_req AS (
    SELECT s.host_name, s.program_name, s.login_name, s.session_id, s.request_start, s.query_hash,
           COUNT(*) AS samples, MAX(s.cpu_ms) AS cpu_ms
    FROM snap AS s
    JOIN page AS p ON p.host_name = s.host_name AND p.program_name = s.program_name AND p.login_name = s.login_name
    WHERE s.query_hash IS NOT NULL
    GROUP BY s.host_name, s.program_name, s.login_name, s.session_id, s.request_start, s.query_hash
),
per_hash AS (
    SELECT host_name, program_name, login_name, query_hash,
           SUM(samples) AS samples, COUNT(*) AS requests, SUM(cpu_ms) AS cpu_ms
    FROM hash_req
    GROUP BY host_name, program_name, login_name, query_hash
),
top_hash AS (
    SELECT * FROM (
        SELECT h.*, ROW_NUMBER() OVER (PARTITION BY host_name, program_name, login_name ORDER BY cpu_ms DESC NULLS LAST, samples DESC NULLS LAST, query_hash NULLS LAST) AS hr
        FROM per_hash AS h
    ) AS x
    WHERE hr <= $5
),
txt AS (
    SELECT s.query_hash, arg_max(s.query_text, s.collection_time) FILTER (WHERE s.query_text IS NOT NULL) AS query_text
    FROM snap AS s
    WHERE s.query_hash IN (SELECT query_hash FROM top_hash)
    GROUP BY s.query_hash
)
SELECT p.host_name, p.program_name, p.login_name,
       CAST(p.samples AS BIGINT) AS samples, CAST(p.captures AS BIGINT) AS captures, CAST(p.requests AS BIGINT) AS requests,
       CAST(p.cpu_ms AS DOUBLE) AS cpu_ms, CAST(p.elapsed_ms AS DOUBLE) AS elapsed_ms,
       CAST(p.total_cpu_ms AS DOUBLE) AS total_cpu_ms, CAST(p.total_elapsed_ms AS DOUBLE) AS total_elapsed_ms,
       CAST(p.total_samples AS BIGINT) AS total_samples, CAST(p.total_requests AS BIGINT) AS total_requests,
       p.first_seen, p.last_seen, p.rk,
       t.query_hash, CAST(t.samples AS BIGINT) AS statement_samples, CAST(t.requests AS BIGINT) AS statement_requests,
       CAST(t.cpu_ms AS DOUBLE) AS statement_cpu_ms,
       x.query_text
FROM page AS p
LEFT JOIN top_hash AS t ON t.host_name = p.host_name AND t.program_name = p.program_name AND t.login_name = p.login_name
LEFT JOIN txt AS x ON x.query_hash = t.query_hash
ORDER BY p.rk, t.hr";

    /// <summary>The caller groups ranked by CPU, the first <paramref name="groups"/> of them (pass the cap plus one to observe a cut), each with its top statements.</summary>
    public async Task<List<CallerGroup>> GetLoadByCallerAsync(int serverId, DateTime startUtc, DateTime endUtc, int groups, int statements)
    {
        using var _q = TimeQuery("GetLoadByCallerAsync", "v_query_snapshots grouped by caller (MCP)");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = LoadByCallerSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        command.Parameters.Add(new DuckDBParameter { Value = groups });
        command.Parameters.Add(new DuckDBParameter { Value = statements });

        var result = new List<CallerGroup>();
        long lastRank = -1;
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var rank = reader.GetInt64(14);
            if (rank != lastRank)
            {
                lastRank = rank;
                result.Add(new CallerGroup(
                    reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5),
                    reader.GetDouble(6), reader.GetDouble(7), reader.GetDouble(8), reader.GetDouble(9), reader.GetInt64(10), reader.GetInt64(11),
                    reader.GetDateTime(12), reader.GetDateTime(13), []));
            }

            if (!reader.IsDBNull(15))
            {
                result[^1].Statements.Add(new CallerStatement(
                    reader.GetString(15), reader.GetInt64(16), reader.GetInt64(17), reader.GetDouble(18), reader.IsDBNull(19) ? null : reader.GetString(19)));
            }
        }

        return result;
    }

    /* ───────────────────────── get_query_pileups ───────────────────────── */

    /// <summary>One capture time where several sessions ran one query_hash.</summary>
    public sealed record Pileup(
        DateTime CollectionTime, string QueryHash, long Sessions, long BlockedSessions, long MaxWaitMs, string? QueryText,
        List<(string WaitType, long Sessions)> Waits);

    /// <summary>
    /// The capture times where at least $4 distinct sessions ran the same non-NULL query_hash at once, most sessions first, newest first
    /// among equals, $5 of them. $1 server_id, $2/$3 window. The waits of each pile-up come from the same rows, so the counts add up to
    /// its sessions (a session with no wait has a NULL wait_type row).
    /// </summary>
    internal const string PileupsSql = @"
WITH snap AS (
    SELECT collection_time, query_hash, session_id, NULLIF(trim(wait_type), '') AS wait_type,
           COALESCE(wait_time_ms, 0) AS wait_time_ms, COALESCE(blocking_session_id, 0) AS blocking_session_id, query_text
    FROM v_query_snapshots
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
    ORDER BY sessions DESC NULLS LAST, collection_time DESC NULLS LAST, query_hash NULLS LAST
    LIMIT $5
),
waits AS (
    SELECT s.collection_time, s.query_hash, s.wait_type, COUNT(DISTINCT s.session_id) AS n
    FROM snap AS s
    JOIN piles AS p ON p.collection_time = s.collection_time AND p.query_hash = s.query_hash
    GROUP BY s.collection_time, s.query_hash, s.wait_type
)
SELECT p.collection_time, p.query_hash, CAST(p.sessions AS BIGINT) AS sessions, CAST(p.blocked_sessions AS BIGINT) AS blocked_sessions,
       CAST(p.max_wait_ms AS BIGINT) AS max_wait_ms, p.query_text, w.wait_type, CAST(w.n AS BIGINT) AS wait_sessions
FROM piles AS p
LEFT JOIN waits AS w ON w.collection_time = p.collection_time AND w.query_hash = p.query_hash
ORDER BY p.sessions DESC NULLS LAST, p.collection_time DESC NULLS LAST, p.query_hash NULLS LAST, w.n DESC NULLS LAST, w.wait_type NULLS LAST";

    /// <summary>The pile-ups of the window, the first <paramref name="limit"/> of them (pass the cap plus one to observe a cut).</summary>
    public async Task<List<Pileup>> GetQueryPileupsAsync(int serverId, DateTime startUtc, DateTime endUtc, int minSessions, int limit)
    {
        using var _q = TimeQuery("GetQueryPileupsAsync", "v_query_snapshots same-statement pile-ups (MCP)");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = PileupsSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        command.Parameters.Add(new DuckDBParameter { Value = minSessions });
        command.Parameters.Add(new DuckDBParameter { Value = limit });

        var piles = new List<Pileup>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
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
    public sealed record BlockingPair(
        DateTime CollectionTime, int BlockedSpid, int BlockingSpid, long WaitTimeMs, string? BlockingLogin, string? BlockingHost, string? BlockingApp,
        string? BlockingSql, string? Database);

    /// <summary>
    /// For each time asked, the DMV blocking snapshot (<c>dmv_blocking_snapshots</c>, the always-on collector that writes rows only while
    /// something is blocked) nearest to it within <paramref name="toleranceSeconds"/> either side; a tie goes to the earlier snapshot.
    /// Returns (asked time, snapshot time). One read lists the distinct snapshot times across the asked span, the choice is made here.
    /// </summary>
    public async Task<Dictionary<DateTime, DateTime>> GetNearestBlockingSnapshotsAsync(int serverId, IReadOnlyCollection<DateTime> times, int toleranceSeconds)
    {
        var found = new Dictionary<DateTime, DateTime>();
        if (times.Count == 0)
        {
            return found;
        }

        var tolerance = TimeSpan.FromSeconds(toleranceSeconds);
        using var _q = TimeQuery("GetNearestBlockingSnapshotsAsync", "v_dmv_blocking_snapshots snapshot times (MCP)");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT DISTINCT collection_time
FROM v_dmv_blocking_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY collection_time";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = times.Min() - tolerance });
        command.Parameters.Add(new DuckDBParameter { Value = times.Max() + tolerance });

        var snapshots = new List<DateTime>();
        using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                snapshots.Add(reader.GetDateTime(0));
            }
        }

        foreach (var at in times)
        {
            DateTime? best = null;
            var bestGap = TimeSpan.MaxValue;
            foreach (var s in snapshots)
            {
                var gap = (s - at).Duration();
                if (gap > tolerance)
                {
                    continue;
                }

                /* Ascending scan, so a strictly smaller gap is required to displace the earlier snapshot on a tie. */
                if (gap < bestGap)
                {
                    best = s;
                    bestGap = gap;
                }
            }

            if (best is not null)
            {
                found[at] = best.Value;
            }
        }

        return found;
    }

    /// <summary>Every blocked/blocking pair of the named snapshots.</summary>
    public async Task<List<BlockingPair>> GetBlockingPairsAsync(int serverId, IReadOnlyCollection<DateTime> snapshotTimes)
    {
        var pairs = new List<BlockingPair>();
        if (snapshotTimes.Count == 0)
        {
            return pairs;
        }

        using var _q = TimeQuery("GetBlockingPairsAsync", "v_dmv_blocking_snapshots pairs (MCP)");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var sql = new StringBuilder(@"
SELECT collection_time, blocked_spid, blocking_spid, COALESCE(wait_time_ms, 0), blocking_login_name, blocking_host_name,
       blocking_client_app, blocking_sql_text, database_name
FROM v_dmv_blocking_snapshots
WHERE server_id = $1
AND   collection_time IN (");
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        var ordinal = 2;
        foreach (var time in snapshotTimes)
        {
            sql.Append(ordinal > 2 ? ", $" : "$").Append(ordinal.ToString(CultureInfo.InvariantCulture));
            command.Parameters.Add(new DuckDBParameter { Value = time });
            ordinal++;
        }

        command.CommandText = sql.Append(')').ToString();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            pairs.Add(new BlockingPair(
                reader.GetDateTime(0), reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture),
                reader.IsDBNull(4) ? null : reader.GetString(4), reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6), reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetString(8)));
        }

        return pairs;
    }
}
