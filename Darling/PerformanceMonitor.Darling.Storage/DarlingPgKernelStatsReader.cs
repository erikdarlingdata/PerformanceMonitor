/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Reads <c>collect.pg_kernel_stats</c> — the kernel's own CPU and disk figures per query (#2603).
///
/// <para>Deltas, because the counters are cumulative. A reset is detected the same way
/// <c>DarlingPgWaitSamplingReader</c> does it, with one improvement this table can afford: the extension
/// reports <c>stats_since</c>, so a reset is a RECORDED FACT rather than an inference from a counter that
/// moved backwards. Both signals are used — the stamp changing is authoritative, and the backwards check
/// still catches a wraparound the stamp would not.</para>
///
/// <para>Ranked by CPU. That is the figure this table exists to add: elapsed time already lives in
/// <c>pg_statement_stats</c>, and ranking by bytes would answer a different question than the one the
/// panel asks.</para>
/// </summary>
public static class DarlingPgKernelStatsReader
{
    /// <param name="TotalCpuMs">User plus system CPU across the window. The ranking figure.</param>
    /// <param name="ExecReadBytes">Bytes that reached the DEVICE. Zero means the page cache served the
    /// read, not that nothing was read.</param>
    /// <param name="CounterReset">The extension's counters were reset inside the window, so the figures
    /// cover only the time since. Detected from <c>stats_since</c> moving, or from a counter going
    /// backwards.</param>
    public sealed record PgKernelStatRow(
        string? DatabaseName,
        long QueryId,
        double TotalCpuMs,
        double ExecUserTimeMs,
        double ExecSystemTimeMs,
        long ExecReadBytes,
        long ExecWriteBytes,
        long MajorFaults,
        bool CounterReset,
        DateTime CaptureTime);

    /// <summary>
    /// One page of per-query CPU plus the denominator its shares are taken over (#3541 A7).
    /// <para><c>WindowTotalCpuMs</c> is user plus system CPU across EVERY (database, query) series in the
    /// window, not across the rows on the page. Off the same statement as the rows, as a window aggregate
    /// over the differenced result before <c>LIMIT</c>, so it cannot drift from them. On the page rather than
    /// on <see cref="PgKernelStatRow"/>: a fact about the window, not a statement.</para>
    /// </summary>
    public sealed record PgKernelStatsPage(List<PgKernelStatRow> Rows, double WindowTotalCpuMs);

    /* Newest and oldest per (database, query) in the window, then differenced. The reset arm takes the
       newest value WHOLE rather than clamping to zero: GREATEST(new - old, 0) reports "no CPU" across a
       restart, which reads as a quiet server rather than as a reset.

       #3541 A7: window_total_cpu_ms is the WHOLE window's user + system CPU, on every row. A window aggregate
       over the differenced result - evaluated before ORDER BY / LIMIT - so it sums every series the window
       holds rather than the rows the cap admits. The tool used to divide each row by the sum of the rows it
       had fetched, so a three-row page summed to 100% of "total" CPU by construction.

       THE ORDER BY, AND WHY THERE IS A `differenced` LAYER. This read shipped `ORDER BY 3 + 4 DESC`, meant as
       "ordinal 3 plus ordinal 4" - user_ms plus system_ms. PostgreSQL reads a bare integer in ORDER BY as an
       output-column ordinal, but `3 + 4` is an EXPRESSION, so it is the constant 7, and a constant sort key
       orders nothing: the planner dropped it and the rows came back sorted by the tiebreak alone,
       (database_name, query_id). Verified with EXPLAIN on PostgreSQL 18 - `ORDER BY 1 + 2 DESC, x` plans as
       `Sort Key: x`. So "ranked by total CPU" was never true, and every page this read served under a limit
       was the alphabetically-first series rather than the hottest, which the A7 work found the moment a
       three-row page's shares came back 60 / 10 / 30. Found by executing the statement, not by reading it;
       a dozen string assertions on this SQL passed throughout. An ORDER BY cannot name a select-list alias
       inside an expression either, so the CASEs are given names one layer down and the outer query sorts
       by `user_ms + system_ms` - which also lets the window aggregate reference them by name instead of
       repeating both CASEs. */
    public const string PgKernelStatsSql = """
        WITH newest AS (
            SELECT DISTINCT ON (database_name, query_id)
                   database_name, query_id, exec_user_time_ms, exec_system_time_ms,
                   exec_read_bytes, exec_write_bytes, major_faults, stats_since, collection_time
            FROM pg_kernel_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, query_id, collection_time DESC
        ),
        oldest AS (
            SELECT DISTINCT ON (database_name, query_id)
                   database_name, query_id, exec_user_time_ms, exec_system_time_ms,
                   exec_read_bytes, exec_write_bytes, major_faults, stats_since
            FROM pg_kernel_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            ORDER BY database_name, query_id, collection_time ASC
        ),
        paired AS (
            SELECT
                n.database_name,
                n.query_id,
                n.collection_time,
                (n.stats_since IS DISTINCT FROM o.stats_since)
                    OR n.exec_user_time_ms < o.exec_user_time_ms          AS counter_reset,
                n.exec_user_time_ms   AS n_user,
                n.exec_system_time_ms AS n_system,
                n.exec_read_bytes     AS n_reads,
                n.exec_write_bytes    AS n_writes,
                n.major_faults        AS n_majflts,
                coalesce(o.exec_user_time_ms, 0)   AS o_user,
                coalesce(o.exec_system_time_ms, 0) AS o_system,
                coalesce(o.exec_read_bytes, 0)     AS o_reads,
                coalesce(o.exec_write_bytes, 0)    AS o_writes,
                coalesce(o.major_faults, 0)        AS o_majflts
            FROM newest AS n
            LEFT JOIN oldest AS o
              ON  o.database_name IS NOT DISTINCT FROM n.database_name
              AND o.query_id = n.query_id
        ),
        differenced AS (
            SELECT
                database_name,
                query_id,
                CASE WHEN counter_reset THEN n_user   ELSE n_user   - o_user   END AS user_ms,
                CASE WHEN counter_reset THEN n_system ELSE n_system - o_system END AS system_ms,
                CASE WHEN counter_reset THEN n_reads  ELSE n_reads  - o_reads  END AS read_bytes,
                CASE WHEN counter_reset THEN n_writes ELSE n_writes - o_writes END AS write_bytes,
                CASE WHEN counter_reset THEN n_majflts ELSE n_majflts - o_majflts END AS major_faults,
                counter_reset,
                collection_time
            FROM paired
        )
        SELECT
            database_name,
            query_id,
            user_ms,
            system_ms,
            read_bytes,
            write_bytes,
            major_faults,
            counter_reset,
            collection_time,
            SUM(user_ms + system_ms) OVER () AS window_total_cpu_ms
        FROM differenced
        ORDER BY user_ms + system_ms DESC, database_name, query_id
        LIMIT $4
        """;

    /// <summary>The rows alone — the WPF Viewer's grid, which has no column for the window total.</summary>
    public static async Task<List<PgKernelStatRow>> GetPgKernelStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default) =>
        (await GetPgKernelStatsPageAsync(postgres, serverId, startUtc, endUtc, limit, cancellationToken)).Rows;

    /// <summary>
    /// The paged read: <paramref name="limit"/> rows, most CPU first, and the whole window's CPU beside them.
    /// The MCP tool asks for <c>limit + 1</c> so it can OBSERVE truncation rather than infer it.
    /// </summary>
    public static async Task<PgKernelStatsPage> GetPgKernelStatsPageAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int limit,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<PgKernelStatRow>();
        double windowTotalCpuMs = 0;
        await using var command = postgres.CreateCommand(PgKernelStatsSql);
        command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
        command.Parameters.AddWithValue(serverId);
        /* SpecifyKind(Unspecified) at the BIND — see any sibling reader: Npgsql infers timestamptz from
           Kind=Utc and PostgreSQL resolves the comparison against these NAIVE columns at the store
           session's TimeZone, so east of UTC the window slides off the data. */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(limit);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var userMs = reader.IsDBNull(2) ? 0 : reader.GetDouble(2);
            var systemMs = reader.IsDBNull(3) ? 0 : reader.GetDouble(3);
            /* Identical on every row (OVER () with no partition); the last write wins with the same number. */
            windowTotalCpuMs = reader.IsDBNull(9) ? 0 : Convert.ToDouble(reader.GetValue(9));

            rows.Add(new PgKernelStatRow(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                QueryId: reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                TotalCpuMs: userMs + systemMs,
                ExecUserTimeMs: userMs,
                ExecSystemTimeMs: systemMs,
                ExecReadBytes: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                ExecWriteBytes: reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                MajorFaults: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                CounterReset: !reader.IsDBNull(7) && reader.GetBoolean(7),
                CaptureTime: reader.IsDBNull(8)
                    ? default
                    : DateTime.SpecifyKind(reader.GetDateTime(8), DateTimeKind.Utc)));
        }

        return new PgKernelStatsPage(rows, windowTotalCpuMs);
    }
}
