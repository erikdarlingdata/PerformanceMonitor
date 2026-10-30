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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #6803: the FinOps High Impact and Optimization reads were slow on every call. High Impact ran four correlated lookups
/// (sample text, full text, plan text, plan gzip) for every query hash in the window before the scorer kept the top few dozen.
/// Optimization joined the text and plan dimensions onto every row of the window and sorted the text and plan payload by the
/// grouping keys, which spilled hundreds of MB to temp. Each test runs the SHIPPED statement on a seeded live store, and
/// compares the answer with the statement as it stood before the fix (kept here as the oracle) or reads the plan's counters.
/// The seed carries text and plans in every stored form: inline on the row (before the dimensions), in the dimension by digest,
/// and plans as gzip bytes.
/// </summary>
public sealed class FinOpsHighImpactOptimizationReadShapeLiveTests
{
    private const string ServerName = "darling-finops-readshape-6803";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* The pre-#6803 High Impact statement, verbatim, with the interval-honest filter written out. */
    private const string OldHighImpactSql = @"
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
     WHERE qs2.query_hash = qs.query_hash AND qs2.server_id = $1 AND qs2.collection_time >= $2
     AND qs2.query_text IS NOT NULL AND qs2.query_text != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST LIMIT 1) AS sample_query_text,
    (SELECT qs2.query_text FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash AND qs2.server_id = $1 AND qs2.collection_time >= $2
     AND qs2.query_text IS NOT NULL AND qs2.query_text != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST LIMIT 1) AS full_query_text,
    (SELECT qs2.query_plan_xml FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash AND qs2.server_id = $1 AND qs2.collection_time >= $2
     AND qs2.query_plan_xml IS NOT NULL AND qs2.query_plan_xml != ''
     ORDER BY qs2.delta_execution_count DESC NULLS LAST LIMIT 1) AS query_plan_xml,
    (SELECT qs2.query_plan_gz FROM v_query_stats qs2
     WHERE qs2.query_hash = qs.query_hash AND qs2.server_id = $1 AND qs2.collection_time >= $2
     AND qs2.query_plan_gz IS NOT NULL
     ORDER BY qs2.delta_execution_count DESC NULLS LAST LIMIT 1) AS query_plan_gz
FROM v_query_stats AS qs
WHERE server_id = $1 AND collection_time >= $2
AND   query_hash IS NOT NULL AND query_hash != ''
AND   delta_execution_count > 0
AND   sample_interval_seconds IS DISTINCT FROM 0
GROUP BY query_hash
HAVING SUM(delta_execution_count) > 0
ORDER BY SUM(delta_worker_time) DESC";

    /* The pre-#6803 Optimization statement, verbatim. */
    private const string OldExpensiveSql = @"
SELECT
    database_name,
    SUM(delta_worker_time) / 1000.0 AS total_cpu_ms,
    CAST(SUM(delta_worker_time) / 1000.0 / NULLIF(SUM(delta_execution_count), 0) AS DECIMAL(19,2)) AS avg_cpu_ms,
    SUM(delta_logical_reads) AS total_reads,
    CAST(SUM(delta_logical_reads) * 1.0 / NULLIF(SUM(delta_execution_count), 0) AS DECIMAL(19,0)) AS avg_reads,
    SUM(delta_execution_count) AS executions,
    LEFT(query_text, 200) AS query_preview,
    query_text AS full_query_text,
    MAX(query_plan_xml) AS query_plan_xml,
    MAX(query_plan_gz) AS query_plan_gz,
    MAX(query_hash) AS query_hash
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   delta_worker_time IS NOT NULL
AND   delta_worker_time > 0
AND   sample_interval_seconds IS DISTINCT FROM 0
GROUP BY database_name, sql_handle, query_text
ORDER BY SUM(delta_worker_time) DESC, database_name, sql_handle, query_text
LIMIT $3";

    private const int HashCount = 60;

    /* Real plans are tens of KB, and since the dimension tables they live in query_plan_dim (inline plans are the legacy form: none
       on a current store inside the window). The padding gives the dimension plans a payload wide enough to show in a sort. */
    private static readonly string PlanPadding = "<!--" + Padding(40000) + "-->";

    /* Incompressible (a fixed-seed byte stream as hex), so the engine cannot shrink it out of the sort. */
    private static string Padding(int length)
    {
        var bytes = new byte[length / 2];
        new Random(6803).NextBytes(bytes);
        return Convert.ToHexString(bytes);
    }

    /* #5608: the seed ends at LiveClock.AnchoredEnd(), so its chunk layout (and so every plan read) is the same at any hour. */
    private static DateTime At => LiveClock.AnchoredEnd();

    /* The window starts an hour before the first seeded row, and the hours the product reader looks back reach it from the real clock. */
    private static DateTime Cutoff => At.AddHours(-1);
    private static int HoursBack => (int)Math.Ceiling((DateTime.UtcNow - Cutoff).TotalHours);

    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource DataSource)?> SeedAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live High Impact and Optimization read-shape tests.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);
            var at = At;

            /* 60 query hashes. Each has six sample rows. Where the text lives rotates through the three stored forms:
               inline on the row, in query_text_dim by digest, and (every third hash) a second text row of the same hash
               whose text is in the dimension, so the busiest-row choice has two candidates. The plan rotates through
               inline text, a dimension text plan, a dimension gzip plan, and none. */
            for (var h = 0; h < HashCount; h++)
            {
                var hash = "0xRS" + h.ToString("D3");
                var db = "Db" + (h % 4);
                var handle = "0xHANDLE" + (h % 20).ToString("D2"); /* three hashes share one handle, so statements merge across hashes */
                var text = "SELECT /* " + (h % 20) + " */ " + hash;
                for (var r = 0; r < 6; r++)
                {
                    var when = at.AddMinutes(-r * 7);
                    var useDim = (h + r) % 2 == 0;
                    byte[]? textDigest = null;
                    if (useDim)
                    {
                        textDigest = PayloadDimensions.Digest(text);
                        await DarlingMcpTestData.ExecAsync(c, ct,
                            "INSERT INTO query_text_dim (digest, query_text, last_seen) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                            new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = textDigest }.Value!, text, when);
                    }

                    string? inlinePlan = null;
                    byte[]? planDigest = null;
                    switch (h % 4)
                    {
                        case 0: inlinePlan = "<ShowPlanXML inline=\"" + hash + r + "\"/>"; break;
                        case 1:
                            var xml = "<ShowPlanXML dim=\"" + hash + r + "\"/>" + PlanPadding;
                            planDigest = PayloadDimensions.Digest(xml);
                            await using (var dim = new NpgsqlCommand("INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING", c))
                            {
                                dim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = planDigest });
                                dim.Parameters.AddWithValue(xml);
                                dim.Parameters.AddWithValue(NpgsqlDbType.Timestamp, when);
                                await dim.ExecuteNonQueryAsync(ct);
                            }
                            break;
                        case 2:
                            var gz = "<ShowPlanXML gz=\"" + hash + r + "\"/>" + PlanPadding;
                            planDigest = PayloadDimensions.Digest(gz);
                            await using (var dim = new NpgsqlCommand("INSERT INTO query_plan_dim (digest, query_plan_gz, last_seen) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING", c))
                            {
                                dim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = planDigest });
                                dim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = PayloadDimensions.CompressContent(gz) });
                                dim.Parameters.AddWithValue(NpgsqlDbType.Timestamp, when);
                                await dim.ExecuteNonQueryAsync(ct);
                            }
                            break;
                    }

                    /* CPU values repeat across hashes, so the High Impact ranks and the Optimization cut both meet ties. */
                    var cpu = ((h * 7 + r * 3) % 11 + 1) * 100_000L;
                    await using var insert = new NpgsqlCommand(
                        @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                            query_text, query_text_digest, query_plan_xml, query_plan_digest, delta_execution_count, delta_worker_time,
                            delta_elapsed_time, delta_logical_reads, delta_logical_writes, max_grant_kb, sample_interval_seconds, last_execution_time)
                          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)", c);
                    insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
                    insert.Parameters.AddWithValue(NpgsqlDbType.Timestamp, when);
                    insert.Parameters.AddWithValue(ServerId);
                    insert.Parameters.AddWithValue(ServerName);
                    insert.Parameters.AddWithValue(db);
                    insert.Parameters.AddWithValue(hash);
                    insert.Parameters.AddWithValue(handle);
                    insert.Parameters.AddWithValue(NpgsqlDbType.Text, useDim ? DBNull.Value : text);
                    insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = (object?)textDigest ?? DBNull.Value });
                    insert.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)inlinePlan ?? DBNull.Value);
                    insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = (object?)planDigest ?? DBNull.Value });
                    insert.Parameters.AddWithValue(3L + r + h % 5);
                    insert.Parameters.AddWithValue(cpu);
                    insert.Parameters.AddWithValue(((h * 5 + r) % 9 + 2) * 150_000L);
                    insert.Parameters.AddWithValue(((h * 3 + r) % 7 + 1) * 1000L);
                    insert.Parameters.AddWithValue(((h * 11 + r) % 9) * 50L);
                    insert.Parameters.AddWithValue(((h * 13 + r) % 10 + 1) * 2048L);
                    insert.Parameters.AddWithValue(60);
                    insert.Parameters.AddWithValue(NpgsqlDbType.Timestamp, when);
                    await insert.ExecuteNonQueryAsync(ct);
                }
            }

            /* A statement with no text anywhere (no inline text, no digest), and a window-edge row the filter drops. */
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                    delta_execution_count, delta_worker_time, delta_logical_reads, sample_interval_seconds)
                  VALUES ($1,$2,$3,$4,'DbNoText','0xRSNOTEXT','0xHANDLENT',5,9000000,10,60), ($5,$2,$3,$4,'DbNoText','0xRSNOTEXT','0xHANDLENT',5,9000000,10,0)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, CollectionIdGenerator.Next());
            /* Plant a lot of unrelated dimension rows, so a read that scans or hashes the whole dimension is visible in the plan. */
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_text_dim (digest, query_text, last_seen)
                  SELECT sha256(('unrelated ' || g)::bytea), 'unrelated ' || g, $1 FROM generate_series(1, 3000) AS g", at);
            await DarlingMcpTestData.ExecAsync(c, ct, "ANALYZE query_stats; ANALYZE query_text_dim; ANALYZE query_plan_dim");
        }

        return (scratch, NpgsqlDataSource.Create(scratch.ConnectionString));
    }

    private static NpgsqlCommand Bind(NpgsqlDataSource dataSource, string sql, DateTime cutoff, int? topN = null)
    {
        var command = dataSource.CreateCommand(sql);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });
        if (topN is int n) command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = n });
        return command;
    }

    private static async Task<string> ExplainAsync(NpgsqlDataSource dataSource, string sql, DateTime cutoff, int? topN = null, string? arrayParameter = null, string? workMem = null)
    {
        /* Serial, so a node's rows are not averaged over parallel workers. */
        await using var connection = await dataSource.OpenConnectionAsync(TestContext.Current.CancellationToken);
        await using (var serial = new NpgsqlCommand("SET max_parallel_workers_per_gather = 0" + (workMem is null ? "" : "; SET work_mem = '" + workMem + "'"), connection))
            await serial.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, connection);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified) });
        if (topN is int n) command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = n });
        if (arrayParameter is not null) command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = arrayParameter.Split(',') });
        return (string)(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    /// <summary>The sum of the loops of every SubPlan node: each is one evaluation of a correlated subquery.</summary>
    private static double SubPlanEvaluations(string explainJson)
    {
        using var document = JsonDocument.Parse(explainJson);
        return Walk(document.RootElement[0].GetProperty("Plan"));

        static double Walk(JsonElement node)
        {
            var own = node.TryGetProperty("Parent Relationship", out var rel) && rel.GetString() == "SubPlan"
                ? node.GetProperty("Actual Loops").GetDouble()
                : 0.0;
            return own + (node.TryGetProperty("Plans", out var kids) ? kids.EnumerateArray().Sum(Walk) : 0.0);
        }
    }

    private static string Row(NpgsqlDataReader reader)
    {
        var parts = new string[reader.FieldCount];
        for (var i = 0; i < parts.Length; i++)
        {
            parts[i] = reader.IsDBNull(i) ? "<null>"
                : reader.GetValue(i) is byte[] bytes ? Convert.ToHexString(bytes)
                : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture)!;
        }
        return string.Join("|", parts);
    }

    private static async Task<List<string>> RowsAsync(NpgsqlDataSource dataSource, string sql, DateTime cutoff, int? topN, CancellationToken ct)
    {
        await using var command = Bind(dataSource, sql, cutoff, topN);
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) rows.Add(Row(reader));
        return rows;
    }

    [Fact]
    public async Task HighImpact_AnswersWhatItAnsweredBefore_TextAndPlanIncluded()
    {
        var ct = TestContext.Current.CancellationToken;
        var seeded = await SeedAsync(ct);
        await using var scratch = seeded!.Value.Scratch;
        await using var dataSource = seeded.Value.DataSource;

        foreach (var topN in new[] { 3, 10 })
        {
            /* The oracle: the statement as it stood, then the same scorer. */
            var cutoff = Cutoff;
            var oldRows = new List<HighImpactQuery>();
            await using (var command = Bind(dataSource, OldHighImpactSql, cutoff))
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    oldRows.Add(new HighImpactQuery
                    {
                        QueryHash = reader.GetString(0),
                        DatabaseName = reader.GetString(1),
                        TotalExecutions = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                        TotalCpuMs = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                        TotalDurationMs = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                        TotalReads = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
                        TotalWrites = reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
                        TotalMemoryMb = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                        SampleQueryText = reader.IsDBNull(8) ? "" : reader.GetString(8),
                        FullQueryText = reader.IsDBNull(9) ? "" : reader.GetString(9),
                        QueryPlanXml = PayloadDimensions.ResolveContent(
                            reader.IsDBNull(10) ? null : reader.GetString(10),
                            reader.IsDBNull(11) ? null : reader.GetFieldValue<byte[]>(11)),
                    });
                }
            }
            var expected = HighImpactScorer.Score(oldRows, topN);

            var actual = await DarlingFinOpsHighImpactReader.ReadAsync(dataSource, ServerId, HoursBack, 60, ct, topN);

            Assert.True(expected.Count > topN, "the seed must keep more than one measure's top rows");
            Assert.Equal(expected.Count, actual.Count);
            for (var i = 0; i < expected.Count; i++)
            {
                var (e, a) = (expected[i], actual[i]);
                Assert.Equal(e.QueryHash, a.QueryHash);
                Assert.Equal(e.DatabaseName, a.DatabaseName);
                Assert.Equal(e.TotalExecutions, a.TotalExecutions);
                Assert.Equal(e.TotalCpuMs, a.TotalCpuMs);
                Assert.Equal(e.TotalDurationMs, a.TotalDurationMs);
                Assert.Equal(e.TotalReads, a.TotalReads);
                Assert.Equal(e.TotalWrites, a.TotalWrites);
                Assert.Equal(e.TotalMemoryMb, a.TotalMemoryMb);
                Assert.Equal(e.ImpactScore, a.ImpactScore);
                Assert.Equal(e.SampleQueryText, a.SampleQueryText);
                Assert.Equal(e.FullQueryText, a.FullQueryText);
                Assert.Equal(e.QueryPlanXml, a.QueryPlanXml);
            }

            /* The seed holds all three stored forms among the kept rows, so the equality above is not vacuous. */
            Assert.Contains(actual, q => q.QueryPlanXml is not null && q.QueryPlanXml.Contains("inline", StringComparison.Ordinal));
            Assert.Contains(actual, q => q.FullQueryText.StartsWith("SELECT /*", StringComparison.Ordinal));
        }
    }

    [Fact]
    public async Task HighImpact_RanksWithoutAnyPerHashLookup_AndLooksUpOnlyTheKeptHashes()
    {
        var ct = TestContext.Current.CancellationToken;
        var seeded = await SeedAsync(ct);
        await using var scratch = seeded!.Value.Scratch;
        await using var dataSource = seeded.Value.DataSource;
        var cutoff = Cutoff;

        /* The ranking statement carries no correlated subquery at all. Before the fix it evaluated four per hash: 60 hashes, 240 evaluations. */
        Assert.Equal(0, SubPlanEvaluations(await ExplainAsync(dataSource, DarlingFinOpsHighImpactReader.HighImpactQueriesSql, cutoff)));

        /* The kept rows: the lookup then runs once per kept hash, never once per hash in the window. */
        var kept = (await DarlingFinOpsHighImpactReader.ReadAsync(dataSource, ServerId, HoursBack, 60, ct, topN: 2)).Select(q => q.QueryHash).ToArray();
        Assert.InRange(kept.Length, 2, HashCount / 2);
        var plan = await ExplainAsync(dataSource, DarlingFinOpsHighImpactReader.HighImpactSampleLookupSql, cutoff, arrayParameter: string.Join(",", kept));
        using var document = JsonDocument.Parse(plan);
        var inner = MaxLoops(document.RootElement[0].GetProperty("Plan"));
        Assert.True(inner > 0 && inner <= kept.Length, $"the lookup ran {inner} times for {kept.Length} kept hashes");

        /* The per-hash lookups are the Limit nodes of the lateral joins: each runs once per kept hash. */
        static double MaxLoops(JsonElement node)
        {
            var own = node.GetProperty("Node Type").GetString() == "Limit" ? node.GetProperty("Actual Loops").GetDouble() : 0.0;
            return Math.Max(own, node.TryGetProperty("Plans", out var kids) ? kids.EnumerateArray().Select(MaxLoops).DefaultIfEmpty(0).Max() : 0.0);
        }
    }

    [Fact]
    public async Task Optimization_AnswersWhatItAnsweredBefore_ColumnForColumn_InOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        var seeded = await SeedAsync(ct);
        await using var scratch = seeded!.Value.Scratch;
        await using var dataSource = seeded.Value.DataSource;
        var cutoff = Cutoff;

        foreach (var topN in new[] { 5, 20, 200 })
        {
            var expected = await RowsAsync(dataSource, OldExpensiveSql, cutoff, topN, ct);
            var actual = await RowsAsync(dataSource, DarlingFinOpsOptimizationReader.ExpensiveQueriesSql, cutoff, topN, ct);
            Assert.NotEmpty(expected);
            Assert.Equal(expected, actual);
        }

        /* The statements that merge: one text in two stored forms (inline and by digest), and one with no text at all. */
        var all = await RowsAsync(dataSource, DarlingFinOpsOptimizationReader.ExpensiveQueriesSql, cutoff, 200, ct);
        Assert.Contains(all, r => r.Contains("DbNoText|", StringComparison.Ordinal) && r.Contains("<null>", StringComparison.Ordinal));
        Assert.Contains(all, r => r.Contains("<ShowPlanXML inline=", StringComparison.Ordinal));
        Assert.Contains(all, r => r.Contains("<ShowPlanXML dim=", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Optimization_DoesNotSpillToTemp_WhereTheOldStatementDid()
    {
        var ct = TestContext.Current.CancellationToken;
        var seeded = await SeedAsync(ct);
        await using var scratch = seeded!.Value.Scratch;
        await using var dataSource = seeded.Value.DataSource;
        var cutoff = Cutoff;

        /* 360 rows of the window, each joined to a plan of 8 KB: the old statement sorted that payload by the grouping keys. */
        var before = TempBlocksWritten(await ExplainAsync(dataSource, OldExpensiveSql, cutoff, 20, workMem: "64kB"));
        var after = TempBlocksWritten(await ExplainAsync(dataSource, DarlingFinOpsOptimizationReader.ExpensiveQueriesSql, cutoff, 20, workMem: "64kB"));

        Assert.True(before > 0, $"the seed must make the old statement spill at 64kB work_mem: {before}");
        /* The groups hold a few dozen rows, so what is left is the group keys, not the payload: a fraction of what the payload cost. */
        Assert.True(after * 4 < before, $"temp blocks written: {before} before, {after} after");
    }

    /// <summary>The temp blocks the whole statement wrote, from the root node's counters.</summary>
    private static double TempBlocksWritten(string explainJson)
    {
        using var document = JsonDocument.Parse(explainJson);
        /* Each node's counter already includes its children, so the largest one is the statement's total, whatever the plan layout. */
        return Largest(document.RootElement[0].GetProperty("Plan"));

        static double Largest(JsonElement node)
        {
            var own = node.TryGetProperty("Temp Written Blocks", out var written) ? written.GetDouble() : 0.0;
            return Math.Max(own, node.TryGetProperty("Plans", out var kids) ? kids.EnumerateArray().Select(Largest).DefaultIfEmpty(0).Max() : 0.0);
        }
    }
}
