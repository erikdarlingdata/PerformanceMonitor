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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5361: the reconstructed-chain drill-down reads its 5,000 pair-rows WITHOUT statement text and fetches the whole text
/// of only the levels it shows, by event key, from the source each level's row came from (the blocked-process-report
/// view, or the DMV-snapshot view for an edge no report holds). What it prints must be what it printed when the pair
/// read carried the whole text: the same chains, the same levels in the same order, the same judged and cut text.
/// The oracle is that old flow, run in the test on the same rows: the pair read with the text columns restored, the
/// whole-text DMV append, the same reconstruction, <see cref="PgDrillDownCollector.StatementPreview"/> per text.
/// The seeded rows repeat an edge with different text per row (so the wrong row is visible), mix both sources, include a
/// plain statement past the cut, a named statement the filter withholds, an astral character at the cut, a NULL text
/// and a NULL ecid.
/// </summary>
[Collection("live-postgres")]
public sealed class StatementDrillDownTextFetchLiveTests
{
    private const string ServerName = "statement-drilldown-textfetch-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string Plain = "SELECT plain_ssf " + new string('x', 683);

    /// <summary>An astral character (a surrogate pair) whose first half is the 500th character: the cut must not split it.</summary>
    private static readonly string AstralAtCut = new string('a', 499) + "\U0001F600" + new string('b', 100);

    private static string Json(object? value) => JsonSerializer.Serialize(value);

    private static async Task InsertBprAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, int blockedSpid, int blockingSpid, long waitMs,
        string? blockedText, string? blockingText, int? blockedEcid = 0, int? blockingEcid = 0)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO blocked_process_reports
    (blocked_report_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms,
     lock_mode, blocked_sql_text, blocking_sql_text, blocked_ecid, blocking_ecid, monitor_loop)
VALUES ($1, $2, $3, $4, $2, 'FetchDb', $5, $6, $7, 'X', $8, $9, $10, $11, 7)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, blockedSpid, blockingSpid, waitMs,
            (object?)blockedText ?? DBNull.Value, (object?)blockingText ?? DBNull.Value,
            (object?)blockedEcid ?? DBNull.Value, (object?)blockingEcid ?? DBNull.Value);
    }

    private static async Task InsertDmvAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, int blockedSpid, int blockingSpid, long waitMs,
        string? blockedText, string? blockingText)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO dmv_blocking_snapshots
    (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, wait_time_ms,
     blocking_status, blocked_sql_text, blocking_sql_text, blocked_ecid, blocking_ecid)
VALUES ($1, $2, $3, $4, $2, 'FetchDb', $5, $6, $7, 'sleeping', $8, $9, 0, 0)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, blockedSpid, blockingSpid, waitMs,
            (object?)blockedText ?? DBNull.Value, (object?)blockingText ?? DBNull.Value);
    }

    /// <summary>The flow of the drill-down before #5361, run on the same store: whole text in every pair-row.</summary>
    private static async Task<string> OracleChainsJsonAsync(NpgsqlConnection connection, AnalysisContext context, CancellationToken ct)
    {
        var oracleSql = PgDrillDownCollector.ReconstructedChainsSql
            .Replace("''::text AS blocked_sql", "blocked_sql_text AS blocked_sql", StringComparison.Ordinal)
            .Replace("''::text AS blocking_sql", "blocking_sql_text AS blocking_sql", StringComparison.Ordinal);
        Assert.NotEqual(PgDrillDownCollector.ReconstructedChainsSql, oracleSql);

        using var cmd = new NpgsqlCommand(oracleSql, connection);
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(context.TimeRangeStart);
        cmd.Parameters.AddWithValue(context.TimeRangeEnd);
        cmd.Parameters.AddWithValue(EventWindowFloor.For(context.TimeRangeStart));
        var rows = new List<BlockingPairRow>();
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
                rows.Add(PgBlockingPairRowQuery.Read(reader));
        }

        await PgBlockingPairRowQuery.AppendDmvSnapshotRowsAsync(
            connection.CreateCommand, rows, context.ServerId, context.TimeRangeStart, context.TimeRangeEnd, ct, includeText: true);
        Assert.Contains(rows, r => r.BlockedSqlText.Length > 0);

        var reconstruction = BlockingChainReconstructor.Reconstruct(
            rows, maxDepth: 50, maxPairs: 5000, stepBudget: 100_000, scopeByMonitorLoop: false);
        return Json(reconstruction.Chains.Take(3).Select(chain => (object)new
        {
            apex_spid = chain.ApexSpid,
            apex_sleeping = chain.ApexSleeping,
            depth = chain.Depth,
            victim_count = chain.VictimCount,
            max_wait_ms = chain.MaxWaitMs,
            levels = chain.Levels.Select(l => new
            {
                level = l.Level,
                blocking_spid = l.BlockingSpid,
                blocked_spid = l.BlockedSpid,
                lock_mode = l.LockMode,
                wait_time_ms = l.WaitTimeMs,
                blocking_sql = PgDrillDownCollector.StatementPreview(l.BlockingSqlText),
                blocked_sql = PgDrillDownCollector.StatementPreview(l.BlockedSqlText)
            }).ToList()
        }).ToList());
    }

    [Fact]
    public async Task ReconstructedChains_PrintTheSameLevelsAndText_AsTheWholeTextPairRead_FromBothSources()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live drill-down text test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var end = DarlingMcpTestData.Naive(DateTime.UtcNow).AddMinutes(1);
            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
            };
            var t0 = new DateTime(context.TimeRangeStart.AddMinutes(30).Ticks / TimeSpan.TicksPerMinute * TimeSpan.TicksPerMinute, DateTimeKind.Unspecified);
            var named = StatementScrubCanary.UriStatement(500);

            /* Chain A, blocked-process reports: 60 -> 71 -> 72 -> 73. The edge 60 -> 71 repeats three times with different
               text per row; the longest wait (12,000, newest of the two) is the row the chain keeps. */
            await InsertBprAsync(connection, ct, t0.AddSeconds(1), 71, 60, 5_000, "SELECT older_a1", "SELECT older_b1");
            await InsertBprAsync(connection, ct, t0.AddSeconds(2), 71, 60, 12_000, named, Plain);
            await InsertBprAsync(connection, ct, t0.AddSeconds(3), 71, 60, 12_000, AstralAtCut, AstralAtCut + "tail");
            await InsertBprAsync(connection, ct, t0.AddSeconds(4), 72, 71, 3_000, "SELECT long_" + new string('y', 2_000), "SELECT blocking_72");
            await InsertBprAsync(connection, ct, t0.AddSeconds(5), 73, 72, 2_000, null, "");
            await InsertBprAsync(connection, ct, t0.AddSeconds(6), 74, 72, 1_000, "SELECT null_ecid", "SELECT null_ecid_blocker", blockedEcid: null, blockingEcid: null);

            /* Chain B, DMV snapshots only (no report holds it): 80 -> 81 -> 82. */
            await InsertDmvAsync(connection, ct, t0.AddMinutes(5).AddSeconds(1), 81, 80, 4_000, "SELECT dmv_old", "SELECT dmv_old_blocker");
            await InsertDmvAsync(connection, ct, t0.AddMinutes(5).AddSeconds(30), 81, 80, 9_000, named, Plain);
            await InsertDmvAsync(connection, ct, t0.AddMinutes(5).AddSeconds(31), 82, 81, 2_500, "SELECT dmv_82", AstralAtCut);

            /* Chain C: an edge both sources hold in one minute. The report wins and the DMV edge is dropped, so the text
               printed is the report's, not the snapshot's. */
            await InsertBprAsync(connection, ct, t0.AddMinutes(10).AddSeconds(5), 91, 90, 6_000, "SELECT from_report", "SELECT from_report_blocker");
            await InsertDmvAsync(connection, ct, t0.AddMinutes(10).AddSeconds(25), 91, 90, 7_000, "SELECT from_snapshot", "SELECT from_snapshot_blocker");

            var oracle = await OracleChainsJsonAsync(connection, context, ct);

            var finding = new AnalysisFinding { RootFactKey = "BLOCKING_CHAIN", StoryPath = "BLOCKING_CHAIN", PathKeys = ["BLOCKING_CHAIN"], Severity = 1.0 };
            await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
            Assert.NotNull(finding.DrillDown);
            var shipped = Json(finding.DrillDown!["reconstructed_blocking_chains"]);

            Assert.Equal(oracle, shipped);

            /* Positive controls: the comparison saw all three chains, both sources, the cut, the withheld marker and the
               text of the row the reconstruction kept (not an older or a snapshot row). */
            var chains = JsonSerializer.Deserialize<JsonElement>(shipped).EnumerateArray().ToList();
            Assert.Equal(3, chains.Count);
            Assert.Contains(SensitiveStatements.PlaceholderText, shipped, StringComparison.Ordinal);
            Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, shipped, StringComparison.Ordinal);
            Assert.Contains(Plain[..500], shipped, StringComparison.Ordinal);
            Assert.DoesNotContain(Plain[..501], shipped, StringComparison.Ordinal);
            Assert.Contains("SELECT dmv_82", shipped, StringComparison.Ordinal);
            Assert.Contains("SELECT from_report", shipped, StringComparison.Ordinal);
            Assert.DoesNotContain("SELECT from_snapshot", shipped, StringComparison.Ordinal);
            Assert.DoesNotContain("SELECT older_a1", shipped, StringComparison.Ordinal);
            Assert.Contains("SELECT null_ecid", shipped, StringComparison.Ordinal);

            /* The pair read itself carries no statement text. */
            using var cmd = new NpgsqlCommand(PgDrillDownCollector.ReconstructedChainsSql, connection);
            cmd.Parameters.AddWithValue(ServerId);
            cmd.Parameters.AddWithValue(context.TimeRangeStart);
            cmd.Parameters.AddWithValue(context.TimeRangeEnd);
            cmd.Parameters.AddWithValue(EventWindowFloor.For(context.TimeRangeStart));
            var pairRows = new List<BlockingPairRow>();
            await using (var reader = await cmd.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                    pairRows.Add(PgBlockingPairRowQuery.Read(reader));
            }

            Assert.True(pairRows.Count >= 7, "the BPR pair read returned " + pairRows.Count + " rows");
            Assert.All(pairRows, r => Assert.True(r.BlockedSqlText.Length == 0 && r.BlockingSqlText.Length == 0));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// The parameter-sensitive drill-down ranks plans WITHOUT text (<c>has_inline_text</c>) and reads the whole inline text of
    /// the five it keeps by (database, query hash, plan hash), then the dimension's text by digest for a kept plan with no
    /// inline text. It must print what it printed when the ranking read carried each row's inline text. The seeded
    /// plans: a newest row whose inline text differs from the older rows' (the newest wins), a plain statement past the
    /// cut, a named statement the filter withholds, an astral character at the cut under a NULL database, an EMPTY inline
    /// text (inline wins even when empty), a plan with no inline text on its newest row but some on older rows (the
    /// dimension's text), and two plans sharing one query hash with different plan hashes.
    /// </summary>
    [Fact]
    public async Task ParameterSensitivePlans_PrintTheSameText_AsTheRankingReadThatCarriedIt()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live drill-down text test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var end = DarlingMcpTestData.Naive(DateTime.UtcNow).AddMinutes(1);
            var windowStart = end.AddHours(-4);
            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = windowStart, TimeRangeEnd = end,
                ServerUtcOffset = TimeSpan.Zero, CancellationToken = ct
            };
            var named = StatementScrubCanary.UriStatement(500);

            /* plan: (database, query hash, plan hash, newest inline text, older inline text, dimension text or null). */
            var plans = new (string? Db, string Hash, string PlanHash, string? Newest, string? Older, string? Dim)[]
            {
                ("FetchDb", "0xQH_SAME", "0xPH_0", Plain, "SELECT older_0", null),
                ("FetchDb", "0xQH_SAME", "0xPH_1", named, "SELECT older_1", null),
                (null, "0xQH_2", "0xPH_2", AstralAtCut, null, null),
                ("FetchDb", "0xQH_3", "0xPH_3", "", "SELECT older_3", null),
                ("FetchDb", "0xQH_4", "0xPH_4", null, "SELECT older_4", "SELECT dimension_4 " + new string('d', 600)),
                ("FetchDb", "0xQH_5", "0xPH_5", "SELECT beyond_the_cap_5", null, null),
                ("FetchDb", "0xQH_6", "0xPH_6", "SELECT beyond_the_cap_6", null, null),
            };

            var compiledBeforeWindow = DateTime.SpecifyKind(windowStart.AddDays(-2), DateTimeKind.Unspecified);
            for (var p = 0; p < plans.Length; p++)
            {
                var plan = plans[p];
                for (var snapshot = 0; snapshot < 3; snapshot++)
                {
                    var newest = snapshot == 2;
                    var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes($"d6 {p} {newest}"));
                    if (newest && plan.Dim is not null)
                    {
                        await DarlingMcpTestData.ExecAsync(connection, ct,
                            "INSERT INTO query_text_dim (digest, query_text, last_seen) VALUES ($1, $2, $3) ON CONFLICT (digest) DO NOTHING",
                            digest, plan.Dim, DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
                    }

                    var inline = newest ? plan.Newest : (plan.Older is null ? null : plan.Older + " snapshot " + snapshot);
                    await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     creation_time, execution_count, min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, query_text_digest, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 20000, $10, 1024, 2048, 0, 0, $11, $12, 25)",
                        CollectionIdGenerator.Next(), DateTime.SpecifyKind(windowStart.AddMinutes(30 + (snapshot * 60)), DateTimeKind.Unspecified),
                        ServerId, ServerName, (object?)plan.Db ?? DBNull.Value, plan.Hash, plan.PlanHash, compiledBeforeWindow,
                        500L + snapshot, 20_000L * (60 - p), (object?)inline ?? DBNull.Value, digest);
                }
            }

            /* The oracle: the ranking read with the text restored (the shape before #5361), the reader's cap, and the
               dimension read for the kept plans. */
            var oracleSql = PgDrillDownCollector.ParameterSensitiveSql
                .Replace("query_text IS NOT NULL AS has_inline_text,", "query_text,", StringComparison.Ordinal)
                .Replace("        has_inline_text,", "        query_text,", StringComparison.Ordinal)
                .Replace("    o.has_inline_text,", "    o.query_text,", StringComparison.Ordinal);
            Assert.DoesNotContain("has_inline_text", oracleSql, StringComparison.Ordinal);
            var kept = new List<(object?[] Row, string? Inline, byte[]? Digest)>();
            using (var cmd = new NpgsqlCommand(oracleSql, connection))
            {
                cmd.Parameters.AddWithValue(ServerId);
                cmd.Parameters.AddWithValue(windowStart);
                cmd.Parameters.AddWithValue(end);
                await using var reader = await cmd.ExecuteReaderAsync(ct);
                while (kept.Count < PgDrillDownCollector.ParameterSensitiveMaxOffenders && await reader.ReadAsync(ct))
                {
                    var row = new object?[10];
                    for (var i = 0; i < 9; i++)
                        row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    kept.Add((row, reader.IsDBNull(9) ? null : reader.GetString(9), reader.IsDBNull(13) ? null : reader.GetFieldValue<byte[]>(13)));
                }
            }

            Assert.Equal(PgDrillDownCollector.ParameterSensitiveMaxOffenders, kept.Count);
            var dimension = new Dictionary<string, string>(StringComparer.Ordinal);
            await using (var dimCmd = new NpgsqlCommand(PgDrillDownCollector.ParameterSensitiveTextSql, connection))
            {
                dimCmd.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bytea,
                    Value = kept.Where(k => k.Inline is null && k.Digest is not null).Select(k => k.Digest!).ToArray()
                });
                await using var dimReader = await dimCmd.ExecuteReaderAsync(ct);
                while (await dimReader.ReadAsync(ct))
                    dimension[PgDrillDownCollector.DigestKey(dimReader.GetFieldValue<byte[]>(0))] = dimReader.GetString(1);
            }

            var oracle = Json(kept.Select(k => (object)new
            {
                database = k.Row[0] as string ?? "",
                query_hash = k.Row[1] as string ?? "",
                query_plan_hash = k.Row[2] as string ?? "",
                execution_count = k.Row[3] is null ? 0L : Convert.ToInt64(k.Row[3]),
                min_worker_time_us = k.Row[4] is null ? 0L : Convert.ToInt64(k.Row[4]),
                max_worker_time_us = k.Row[5] is null ? 0L : Convert.ToInt64(k.Row[5]),
                worker_ratio = k.Row[6] is null ? 0.0 : Convert.ToDouble(k.Row[6]),
                grant_ratio = k.Row[7] is null ? 0.0 : Convert.ToDouble(k.Row[7]),
                spills_on_some_inputs = k.Row[8] is not null && Convert.ToInt32(k.Row[8]) == 1,
                query_text = PgDrillDownCollector.StatementPreview(PgDrillDownCollector.SettledQueryText(k.Inline, k.Digest, dimension))
            }).ToList());

            var finding = new AnalysisFinding { RootFactKey = "PARAMETER_SENSITIVITY", StoryPath = "PARAMETER_SENSITIVITY", PathKeys = ["PARAMETER_SENSITIVITY"], Severity = 1.0 };
            await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);
            Assert.NotNull(finding.DrillDown);
            var shipped = Json(finding.DrillDown!["parameter_sensitive_queries"]);

            Assert.Equal(oracle, shipped);

            /* Positive controls: the newest row's text and not an older one's, the cut, the withheld marker, the empty inline
               text, the dimension's text, the plan cap, and a ranking read that carries no text. */
            Assert.Contains(Plain[..500], shipped, StringComparison.Ordinal);
            Assert.DoesNotContain(Plain[..501], shipped, StringComparison.Ordinal);
            Assert.Contains(SensitiveStatements.PlaceholderText, shipped, StringComparison.Ordinal);
            Assert.DoesNotContain(StatementScrubCanary.UriSecretPartial, shipped, StringComparison.Ordinal);
            Assert.DoesNotContain("older_", shipped, StringComparison.Ordinal);
            Assert.Contains("SELECT dimension_4", shipped, StringComparison.Ordinal);
            Assert.DoesNotContain("beyond_the_cap", shipped, StringComparison.Ordinal);
            Assert.Contains("\"query_text\":\"\"", shipped, StringComparison.Ordinal);

            using var rankCmd = new NpgsqlCommand(PgDrillDownCollector.ParameterSensitiveSql, connection);
            rankCmd.Parameters.AddWithValue(ServerId);
            rankCmd.Parameters.AddWithValue(windowStart);
            rankCmd.Parameters.AddWithValue(end);
            await using var rankReader = await rankCmd.ExecuteReaderAsync(ct);
            Assert.Equal("boolean", rankReader.GetDataTypeName(9));
            var ranked = 0;
            while (await rankReader.ReadAsync(ct))
                ranked++;
            Assert.Equal(plans.Length, ranked);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "blocked_process_reports", "dmv_blocking_snapshots", "query_stats" })
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM {table} WHERE server_id = $1", ServerId);

        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
