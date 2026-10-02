/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A5, the Darling half of the rendered marker: the eight trend tools' compiled descriptions carry the
/// shared sentence and no other Darling tool claims the key (the roster is the whole truth of "every"); the
/// Storage reader is the one door both the service and the viewer read through; and the viewer's reason word
/// and accent are the shared ones. The store-side execution is the live class below.
/// </summary>
public sealed class BaselineDiscontinuityRenderTests
{
    private static readonly string[] Roster =
    {
        "get_memory_trend", "get_perfmon_trend", "get_file_io_trend", "get_query_trend",
        "get_query_duration_trend", "get_procedure_duration_trend", "get_query_store_duration_trend",
        "get_wait_trend",
    };

    private static (string Name, MethodInfo Method)[] AllDarlingTools() => typeof(DarlingMcpTrendTools).Assembly.GetTypes()
        .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
        .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
        .Select(m => (Attribute: m.GetCustomAttribute<McpServerToolAttribute>(), Method: m))
        .Where(x => x.Attribute is not null)
        .Select(x => (x.Attribute!.Name!, x.Method))
        .ToArray();

    [Fact]
    public void TheEightTrendTools_DescribeTheKey_AndNoOtherToolClaimsIt()
    {
        var tools = AllDarlingTools().ToDictionary(t => t.Name, t => t.Method, StringComparer.Ordinal);

        foreach (var name in Roster)
        {
            Assert.True(tools.TryGetValue(name, out var method), $"{name} is not a Darling MCP tool");
            var description = method!.GetCustomAttribute<DescriptionAttribute>()!.Description;
            Assert.EndsWith(BaselineDiscontinuities.DescriptionSentence, description);
        }

        foreach (var (name, method) in tools)
        {
            if (Roster.Contains(name, StringComparer.Ordinal))
            {
                continue;
            }

            var description = method.GetCustomAttribute<DescriptionAttribute>()?.Description ?? string.Empty;
            Assert.DoesNotContain("discontinuities[]", description, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheWaitTrend_LivesInDataTools_AndTheSevenInTrendTools()
    {
        var byType = AllDarlingTools().ToDictionary(t => t.Name, t => t.Method.DeclaringType!, StringComparer.Ordinal);

        Assert.Equal(typeof(DarlingMcpDataTools), byType["get_wait_trend"]);
        foreach (var name in Roster.Where(n => n != "get_wait_trend"))
        {
            Assert.Equal(typeof(DarlingMcpTrendTools), byType[name]);
        }
    }

    /// <summary>The service reads through the Storage reader — the viewer's door — so the two cannot diverge.</summary>
    [Fact]
    public void TheServiceReader_ForwardsToTheStorageReader()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingTrendReader.cs");
        var start = source.IndexOf("GetBaselineDiscontinuitiesAsync(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = source[start..source.IndexOf(';', start)];
        Assert.Contains("BaselineDiscontinuityReader.ReadAsync(postgres, serverId, startUtc, endUtc, McpCommandDeadlines.ReadSeconds", body, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.Discontinuities.cs");
        Assert.Contains("BaselineDiscontinuityReader.ReadAsync(", viewer, StringComparison.Ordinal);
        Assert.Contains("ViewerCommandDeadlines.CurrentInteractiveReadSeconds", viewer, StringComparison.Ordinal);
    }
}

/// <summary>
/// #3653 A5 on a live PostgreSQL store: the two shared SQL texts execute on the engine (their DuckDB execution
/// is Lite.Tests' <c>BaselineDiscontinuityDuckDbTests</c>), the window bounds both sides, the persisted pair
/// reaches the reason word, two carriers fold to one marker, and a trend tool's payload carries the block as
/// its trailing key — empty on a continuous window. Planted the way the carriers write: one
/// <c>collection_log</c> row per carrier run with the #3161 note, and the pair in <c>collector_state</c> under
/// the carrier's name. Serialized with every other class that reaches the shared <c>DARLING_TEST_PG</c> store
/// (<c>LivePostgresCollectionHygieneTests</c>): its planted rows live in the shared <c>collection_log</c> and
/// <c>collector_state</c> under a name-derived server_id, and the cleanup deletes them.
/// </summary>
[Collection("live-postgres")]
public sealed class BaselineDiscontinuityLivePostgresTests
{
    private const string ServerName = "darling-discontinuity-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheReaderAndATrendTool_RenderThePlantedMarkers_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live discontinuity test.");

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
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            var epoch = now.AddHours(-2);
            var oldStart = new DateTime(2026, 9, 1, 8, 0, 0);
            var newStart = new DateTime(2026, 9, 20, 3, 11, 40);

            foreach (var (t, v) in new[] { (now.AddMinutes(-30), 40000m), (now.AddMinutes(-10), 41000m) })
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)", CollectionIdGenerator.Next(), t, ServerId, ServerName, v, 49152m, v - 5000m, 5000m);

            /* A continuous window first: the block is present and empty. */
            using (var doc = JsonDocument.Parse(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName)))
            {
                var block = doc.RootElement.GetProperty(BaselineDiscontinuities.PayloadKey);
                Assert.Equal(JsonValueKind.Array, block.ValueKind);
                Assert.Equal(0, block.GetArrayLength());
                Assert.Equal(BaselineDiscontinuities.PayloadKey, doc.RootElement.EnumerateObject().Last().Name);
            }

            /* Then the epoch: both SQL Server carriers on one pass, a quiet run between, a marker outside the
               window, a statements epoch with a host note in front, and a note that only MENTIONS the label. */
            await PlantLogAsync(connection, ct, "wait_stats", epoch, "identity_epoch_changes=1");
            await PlantLogAsync(connection, ct, "latch_stats", epoch.AddSeconds(5), null);
            await PlantLogAsync(connection, ct, "cpu_utilization", epoch.AddSeconds(40), "identity_epoch_changes=1");
            await PlantLogAsync(connection, ct, "wait_stats", epoch.AddHours(-30), "identity_epoch_changes=1");
            await PlantLogAsync(connection, ct, "pg_statement_stats", epoch.AddMinutes(20), "wall-clock budget (120s) reached; cycle abandoned; statements_epoch_changes=1");
            await PlantLogAsync(connection, ct, "query_stats", epoch.AddMinutes(30), "the identity_epoch_changes marker is prose here");
            await PlantStateAsync(connection, ct, "wait_stats", ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(new(newStart, "NODE1")), epoch.AddSeconds(-1));
            await PlantStateAsync(connection, ct, "wait_stats", ServerEpoch.IdentityPreviousStateKey, ServerEpoch.Serialize(new(oldStart, "NODE1")), epoch.AddSeconds(-1));
            await PlantStateAsync(connection, ct, "cpu_utilization", ServerEpoch.IdentityStateKey, ServerEpoch.Serialize(new(newStart, "NODE1")), epoch.AddSeconds(39));
            await PlantStateAsync(connection, ct, "cpu_utilization", ServerEpoch.IdentityPreviousStateKey, ServerEpoch.Serialize(new(oldStart, "NODE1")), epoch.AddSeconds(39));

            var read = await BaselineDiscontinuityReader.ReadAsync(postgres, ServerId, now.AddHours(-24), now, cancellationToken: ct);
            Assert.Equal(2, read.Count);
            Assert.Equal(epoch, read[0].At);
            Assert.Equal(BaselineDiscontinuities.RestartReason, read[0].Reason);
            Assert.Contains("2026-09-01 08:00:00 → 2026-09-20 03:11:40", read[0].Detail, StringComparison.Ordinal);
            Assert.Contains("observed by wait_stats, cpu_utilization", read[0].Detail, StringComparison.Ordinal);
            Assert.Equal(BaselineDiscontinuities.StatsResetReason, read[1].Reason);

            var wide = await BaselineDiscontinuityReader.ReadAsync(postgres, ServerId, now.AddHours(-48), now, cancellationToken: ct);
            Assert.Equal(3, wide.Count);
            Assert.Equal(BaselineDiscontinuities.IdentityReason, wide[0].Reason);

            Assert.Empty(await BaselineDiscontinuityReader.ReadAsync(postgres, ServerId, epoch.AddHours(-4), epoch.AddMinutes(-5), cancellationToken: ct));

            /* Through the tool: the same two, trailing, in the store's frame. */
            using (var doc = JsonDocument.Parse(await DarlingMcpTrendTools.GetMemoryTrend(postgres, ServerName)))
            {
                var block = doc.RootElement.GetProperty(BaselineDiscontinuities.PayloadKey);
                Assert.Equal(2, block.GetArrayLength());
                var first = block[0];
                Assert.Equal(BaselineDiscontinuities.RestartReason, first.GetProperty("reason").GetString());
                Assert.Equal(epoch.ToString("o"), first.GetProperty("at").GetString());
                Assert.Equal(BaselineDiscontinuities.PayloadKey, doc.RootElement.EnumerateObject().Last().Name);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static Task PlantLogAsync(NpgsqlConnection connection, CancellationToken ct, string collector, DateTime at, string? note) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector, DarlingMcpTestData.Naive(at), 100, "SUCCESS", note, 10);

    private static Task PlantStateAsync(NpgsqlConnection connection, CancellationToken ct, string collector, string key, string value, DateTime updatedAt) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (server_id, collector_name, state_key) DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at",
            ServerId, collector, key, value, DarlingMcpTestData.Naive(updatedAt));

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql = string.Join(" ", new[] { "memory_stats", "collection_log", "collector_state" }.Select(t => $"DELETE FROM {t} WHERE server_id = {ServerId};"))
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
