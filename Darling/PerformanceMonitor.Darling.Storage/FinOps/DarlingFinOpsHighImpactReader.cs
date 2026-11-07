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

    /// <summary>How many databases the row's figures are summed across (#7695). <see cref="DatabaseName"/> is the one with the most CPU.</summary>
    public int DatabaseCount { get; set; } = 1;
    public long TotalExecutions { get; set; }
    public decimal TotalCpuMs { get; set; }
    public decimal TotalDurationMs { get; set; }
    public long TotalReads { get; set; }
    public long TotalWrites { get; set; }
    public decimal PeakMemoryMb { get; set; }
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
    /// High-impact queries: 80/20 analysis across CPU/duration/reads/writes/memory/executions. Aggregates to
    /// query_hash level in SQL, keeps the hashes the scorer can keep (the top $3 on each of its six measures), then
    /// scores in C# via <see cref="HighImpactScorer"/>. The aggregate carries NO text or plan: those are looked up for
    /// the kept rows only (<see cref="HighImpactSampleLookupSql"/>), because a correlated lookup per hash ran for every
    /// hash in the window before the scorer kept the top few dozen (#6803). $1 server_id, $2 cutoff, $3 the scorer's
    /// per-measure keep count.
    /// </summary>
    public const string HighImpactQueriesSql = $@"
WITH per_database AS
(
    -- #8084: the base table, not v_query_stats. This aggregate reads no text or plan column, so the view's payload
    -- joins have nothing to resolve for it. One group per (hash, database): its CPU subtotal picks the hash's
    -- database (#7695).
    SELECT query_hash, database_name,
           SUM(delta_execution_count) AS executions,
           SUM(delta_worker_time) AS worker_time,
           SUM(delta_elapsed_time) AS elapsed_time,
           SUM(delta_logical_reads) AS logical_reads,
           SUM(delta_logical_writes) AS logical_writes
    FROM collect.query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   query_hash IS NOT NULL AND query_hash != ''
    AND   delta_execution_count > 0
    AND   {TimescaleSupport.IntervalHonestSourceFilter}
    GROUP BY query_hash, database_name
),
per_hash AS
(
    SELECT
        query_hash,
        -- #7695: the database that did the most CPU for the hash in the window, not the alphabetically first one. The
        -- name breaks a tie, case-insensitively and then exactly, so a tie picks the same database every read.
        (ARRAY_AGG(database_name ORDER BY worker_time DESC NULLS LAST, LOWER(database_name), database_name)
            FILTER (WHERE database_name IS NOT NULL))[1] AS database_name,
        SUM(executions) AS total_executions,
        SUM(worker_time) AS total_worker_time,
        SUM(elapsed_time) AS total_elapsed_time,
        SUM(logical_reads) AS total_reads,
        SUM(logical_writes) AS total_writes,
        COUNT(DISTINCT database_name) AS database_count
    FROM per_database
    GROUP BY query_hash
    HAVING SUM(executions) > 0
),
ranked AS
(
    -- The scorer keeps the top $3 hashes on each of its six measures and scores that union. Each rank breaks a tie the
    -- way the scorer's stable sort over this statement's final order does: more CPU first, then query_hash. A NULL
    -- measure ranks as the 0 the reader maps it to.
    SELECT h.*, COALESCE(g.max_grant_kb, 0) AS peak_grant_kb,
           ROW_NUMBER() OVER (ORDER BY COALESCE(h.total_worker_time, 0) DESC, h.total_worker_time DESC, h.query_hash) AS cpu_rank,
           ROW_NUMBER() OVER (ORDER BY COALESCE(h.total_elapsed_time, 0) DESC, h.total_worker_time DESC, h.query_hash) AS duration_rank,
           ROW_NUMBER() OVER (ORDER BY COALESCE(h.total_reads, 0) DESC, h.total_worker_time DESC, h.query_hash) AS reads_rank,
           ROW_NUMBER() OVER (ORDER BY COALESCE(h.total_writes, 0) DESC, h.total_worker_time DESC, h.query_hash) AS writes_rank,
           ROW_NUMBER() OVER (ORDER BY COALESCE(g.max_grant_kb, 0) DESC, h.total_worker_time DESC, h.query_hash) AS memory_rank,
           ROW_NUMBER() OVER (ORDER BY h.total_executions DESC, h.total_worker_time DESC, h.query_hash) AS executions_rank
    FROM per_hash AS h
    -- #7166: max_grant_kb is a per-plan high-water mark since that plan was cached, so MAX across a hash's plans is
    -- the largest grant any plan ever had (the max_dop trap, #3648). The reading is the hash's newest row's, the same
    -- choice max_dop makes: one probe per hash on idx_query_stats_server_hash_time, newest collection first, newest
    -- plan first within it. Explicit NULLS LAST: DuckDB and Postgres default DESC null placement differently.
    LEFT JOIN LATERAL
    (
        SELECT qs.max_grant_kb
        FROM collect.query_stats AS qs
        WHERE qs.server_id = $1
        AND   qs.query_hash = h.query_hash
        AND   qs.collection_time >= $2
        AND   qs.delta_execution_count > 0
        AND   {TimescaleSupport.IntervalHonestSourceFilter}
        ORDER BY qs.collection_time DESC, qs.creation_time DESC NULLS LAST, qs.delta_worker_time DESC NULLS LAST
        LIMIT 1
    ) AS g ON true
)
SELECT
    query_hash,
    database_name,
    total_executions,
    total_worker_time / 1000.0 AS total_cpu_ms,
    total_elapsed_time / 1000.0 AS total_duration_ms,
    total_reads,
    total_writes,
    peak_grant_kb / 1024.0 AS peak_memory_mb,
    database_count
FROM ranked
WHERE cpu_rank <= $3 OR duration_rank <= $3 OR reads_rank <= $3
OR    writes_rank <= $3 OR memory_rank <= $3 OR executions_rank <= $3
ORDER BY total_worker_time DESC, query_hash";

    /// <summary>
    /// The sample text and stored plan for the kept query hashes, each from the hash's busiest row in the window (the
    /// same row choice the aggregate's per-hash lookups made before #6803). $1 server_id, $2 cutoff, $3 the kept
    /// query hashes (text[]). One row per hash; a hash with no text or plan returns NULL in that column. The plan
    /// comes back as text and as gzip bytes, one lookup per form (#2069: post-V54 plans are gzip bytes with the
    /// text NULL).
    /// </summary>
    public const string HighImpactSampleLookupSql = @"
SELECT
    h.query_hash,
    LEFT(t.query_text, 200) AS sample_query_text,
    t.query_text AS full_query_text,
    x.query_plan_xml,
    g.query_plan_gz
FROM unnest($3::text[]) AS h(query_hash)
LEFT JOIN LATERAL (
    SELECT qs2.query_text FROM v_query_stats qs2
    WHERE qs2.query_hash = h.query_hash
    AND qs2.server_id = $1
    AND qs2.collection_time >= $2
    AND qs2.query_text IS NOT NULL AND qs2.query_text != ''
    ORDER BY qs2.delta_execution_count DESC NULLS LAST
    LIMIT 1) AS t ON true
LEFT JOIN LATERAL (
    SELECT qs2.query_plan_xml FROM v_query_stats qs2
    WHERE qs2.query_hash = h.query_hash
    AND qs2.server_id = $1
    AND qs2.collection_time >= $2
    AND qs2.query_plan_xml IS NOT NULL AND qs2.query_plan_xml != ''
    ORDER BY qs2.delta_execution_count DESC NULLS LAST
    LIMIT 1) AS x ON true
LEFT JOIN LATERAL (
    SELECT qs2.query_plan_gz FROM v_query_stats qs2
    WHERE qs2.query_hash = h.query_hash
    AND qs2.server_id = $1
    AND qs2.collection_time >= $2
    AND qs2.query_plan_gz IS NOT NULL
    ORDER BY qs2.delta_execution_count DESC NULLS LAST
    LIMIT 1) AS g ON true";

    /// <summary>Reads the window's scorable query_hash aggregates for one server and returns the scored high-impact set.</summary>
    public static async Task<List<HighImpactQuery>> ReadAsync(
        NpgsqlDataSource dataSource, int serverId, int hoursBack, int commandTimeoutSeconds, CancellationToken cancellationToken, int topN = 10)
    {
        var cutoff = DateTime.UtcNow.AddHours(-hoursBack);
        var rows = await ReadCandidatesAsync(dataSource, serverId, cutoff, topN, commandTimeoutSeconds, cancellationToken);

        var kept = HighImpactScorer.Score(rows, topN);
        if (kept.Count == 0)
            return kept;

        /* The text and plan lookups run for the kept rows only (#6803). */
        await using var lookup = dataSource.CreateCommand(HighImpactSampleLookupSql);
        lookup.CommandTimeout = commandTimeoutSeconds;
        lookup.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        lookup.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });
        lookup.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = kept.ConvertAll(q => q.QueryHash).ToArray() });

        var byHash = new Dictionary<string, HighImpactQuery>(kept.Count, StringComparer.Ordinal);
        foreach (var q in kept) byHash[q.QueryHash] = q;
        await using var found = await lookup.ExecuteReaderAsync(cancellationToken);
        while (await found.ReadAsync(cancellationToken))
        {
            if (!byHash.TryGetValue(found.GetString(0), out var row)) continue;
            row.SampleQueryText = found.IsDBNull(1) ? "" : found.GetString(1);
            row.FullQueryText = found.IsDBNull(2) ? "" : found.GetString(2);
            /* #2069: the two lookups may land on DIFFERENT sample rows (one pre-V54 text row, one post-V54 gz
               row); either is "a sample plan for the hash", and text-first keeps the free form when both exist. */
            row.QueryPlanXml = PayloadDimensions.ResolveContent(
                found.IsDBNull(3) ? null : found.GetString(3),
                found.IsDBNull(4) ? null : found.GetFieldValue<byte[]>(4));
        }

        return kept;
    }

    /// <summary>
    /// The window's query_hash aggregates the scorer can keep (#8084): the top <paramref name="topN"/> hashes on each
    /// of its six measures, unscored, in CPU order. At most six times <paramref name="topN"/> rows, however many
    /// hashes the window holds. <see cref="HighImpactScorer.Score"/> over these rows is the same result as over every
    /// hash in the window: a hash outside every measure's top <paramref name="topN"/> is never kept, and the shares and
    /// percent ranks are computed over the kept set alone.
    /// </summary>
    public static async Task<List<HighImpactQuery>> ReadCandidatesAsync(
        NpgsqlDataSource dataSource, int serverId, DateTime cutoffUtc, int topN, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = dataSource.CreateCommand(HighImpactQueriesSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = topN });

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
                PeakMemoryMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                DatabaseCount = reader.IsDBNull(8) ? 1 : Convert.ToInt32(reader.GetValue(8))
            });
        }

        return rows;
    }
}
