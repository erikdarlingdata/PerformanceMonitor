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

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>One high-impact query_hash aggregate over the window, scored by <see cref="HighImpactScorer"/>.</summary>
public sealed class HighImpactQuery
{
    public string QueryHash { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public long TotalExecutions { get; set; }
    public decimal TotalCpuMs { get; set; }
    public decimal TotalDurationMs { get; set; }
    public long TotalReads { get; set; }
    public long TotalWrites { get; set; }
    public decimal TotalMemoryMb { get; set; }
    public decimal CpuShare { get; set; }
    public decimal DurationShare { get; set; }
    public decimal ReadsShare { get; set; }
    public decimal WritesShare { get; set; }
    public decimal MemoryShare { get; set; }
    public decimal ExecutionsShare { get; set; }
    public int ImpactScore { get; set; }
    public string SampleQueryText { get; set; } = "";
    public string FullQueryText { get; set; } = "";

    /// <summary>The stored statement-level plan, text form first, else the decompressed gz form.</summary>
    public string? QueryPlanXml { get; set; }
}

/// <summary>The FinOps high-impact read: SQL, command and mapping.</summary>
public static class DarlingFinOpsHighImpactReader
{
    /// <summary>
    /// High-impact queries — 80/20 analysis across CPU/duration/reads/writes/memory/executions. Aggregates
    /// to query_hash level in SQL (with correlated sample-text subqueries, as Lite does), then scores in C#
    /// via <see cref="HighImpactScorer"/>. $1 server_id, $2 cutoff.
    /// </summary>
    public const string HighImpactQueriesSql = $@"
SELECT
    query_hash,
    MIN(database_name) AS database_name,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) / 1000.0 AS total_cpu_ms,
    SUM(delta_elapsed_time) / 1000.0 AS total_duration_ms,
    SUM(delta_logical_reads) AS total_reads,
    SUM(delta_logical_writes) AS total_writes,
    SUM(COALESCE(max_grant_kb, 0)) / 1024.0 AS total_memory_mb,
    (SELECT LEFT(qs2.query_text, 200) FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_text IS NOT NULL AND qs2.query_text != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS sample_query_text,
    (SELECT qs2.query_text FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_text IS NOT NULL AND qs2.query_text != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS full_query_text,
    (SELECT qs2.query_plan_xml FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_plan_xml IS NOT NULL AND qs2.query_plan_xml != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS query_plan_xml,
    (SELECT qs2.query_plan_gz FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash
     AND qs2.server_id = $1
     AND qs2.collection_time >= $2
     AND qs2.query_plan_gz IS NOT NULL
     ORDER BY qs2.delta_execution_count DESC NULLS LAST
     LIMIT 1) AS query_plan_gz
FROM v_query_stats AS qs
WHERE server_id = $1
AND   collection_time >= $2
AND   query_hash IS NOT NULL AND query_hash != ''
AND   delta_execution_count > 0
AND   {TimescaleSupport.IntervalHonestSourceFilter}
GROUP BY query_hash
HAVING SUM(delta_execution_count) > 0
ORDER BY SUM(delta_worker_time) DESC";

    /// <summary>Reads the window's query_hash aggregates for one server and returns the scored high-impact set.</summary>
    public static async Task<List<HighImpactQuery>> ReadAsync(
        NpgsqlDataSource dataSource, int serverId, int hoursBack, int commandTimeoutSeconds, CancellationToken cancellationToken, int topN = 10)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);

        await using var command = dataSource.CreateCommand(HighImpactQueriesSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });

        var rows = new List<HighImpactQuery>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new HighImpactQuery
            {
                QueryHash = reader.IsDBNull(0) ? "" : reader.GetString(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                TotalExecutions = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                TotalCpuMs = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                TotalDurationMs = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                TotalReads = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                TotalWrites = reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
                TotalMemoryMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                SampleQueryText = reader.IsDBNull(8) ? "" : reader.GetString(8),
                FullQueryText = reader.IsDBNull(9) ? "" : reader.GetString(9),
                /* #2069: the two correlated subqueries may land on DIFFERENT sample rows (one
                   pre-V54 text row, one post-V54 gz row); either is "a sample plan for the hash",
                   and text-first keeps the free form when both exist. */
                QueryPlanXml = PayloadDimensions.ResolveContent(
                    reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.IsDBNull(11) ? null : reader.GetFieldValue<byte[]>(11))
            });
        }

        return HighImpactScorer.Score(rows, topN);
    }
}
