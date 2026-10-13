/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the Darling performance monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 items 10 and 12, the two tests of <see cref="McpToolAudit5633L3Tests"/> that drive a tool against a live PostgreSQL store
/// (audit_config's edition label, compare_analysis's baseline window). They live in their own class so the pure tests stay
/// unserialized, and so the live ones join the "live-postgres" collection like every other class that writes the shared store.
/// </summary>
[Collection("live-postgres")]
public sealed class McpToolAudit5633L3LivePostgresTests
{
    /// <summary>
    /// Review round 1 L-c: drives the tool, not its source text. A server whose store holds the Edition string "Developer Edition (64-bit)"
    /// (engine edition 3) must come back from audit_config with that string and no fallback note; the old shape mapped engine edition 3 to
    /// "Enterprise", which is what a regression in the wiring (a refactor that keeps the call but drops the stored row) would bring back.
    /// </summary>
    [Fact]
    public async Task AuditConfig_EchoesTheStoredEditionString_NotTheEngineEditionLabel()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the audit_config edition live test.");
        var ct = TestContext.Current.CancellationToken;

        var serverName = "audit5633_" + Guid.NewGuid().ToString("N");
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var when = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-2), DateTimeKind.Unspecified);
            await using (var seed = new NpgsqlConnection(cs))
            {
                await seed.OpenAsync(ct);
                await PgMigrations.MigrateAsync(seed, ct);
                await DarlingMcpTestData.RegisterServerAsync(seed, serverId, serverName, ct);
                await DarlingMcpTestData.ExecAsync(seed, ct,
                    @"INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                    CollectionIdGenerator.Next(), when, serverId, serverName, "cost threshold for parallelism", 5L, 5L, true, true);
                await DarlingMcpTestData.ExecAsync(seed, ct,
                    @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, engine_edition)
VALUES ($1,$2,$3,$4,'Developer Edition (64-bit)','15.0.4000.1','RTM',3)",
                    CollectionIdGenerator.Next(), when, serverId, serverName);
            }

            var analysisService = new DarlingAnalysisService(postgres);
            using var doc = JsonDocument.Parse(await DarlingMcpTools.AuditConfig(analysisService, postgres, serverName, ct));
            var root = doc.RootElement;

            Assert.Equal("Developer Edition (64-bit)", root.GetProperty("edition").GetString());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("edition_note").ValueKind);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var table in new[] { "server_config", "server_properties" })
                {
                    await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, $"DELETE FROM collect.{table} WHERE server_id = $1", serverId);
                }

                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.servers WHERE server_id = $1", serverId);
            });
        }
    }

    /// <summary>
    /// Review round 1 L-c: drives the tool. An overlapping baseline is refused naming both windows, and an omitted baseline follows
    /// hours_back (hours_back 24 reads a baseline 48 hours back, where the old fixed default of 28 hours overlapped the comparison window).
    /// The anchor is fixed so the rendered instants can be asserted, on the data-bearing payload and the empty-window envelope alike.
    /// </summary>
    [Fact]
    public async Task CompareAnalysis_RefusesOverlap_AndAnOmittedBaselineFollowsHoursBack()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the compare_analysis baseline live test.");
        var ct = TestContext.Current.CancellationToken;

        var serverName = "cmp5633_" + Guid.NewGuid().ToString("N");
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            await using (var seed = new NpgsqlConnection(cs))
            {
                await seed.OpenAsync(ct);
                await PgMigrations.MigrateAsync(seed, ct);
                await DarlingMcpTestData.RegisterServerAsync(seed, serverId, serverName, ct);
            }

            var service = new DarlingAnalysisService(postgres);
            var anchor = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);

            var overlapping = await DarlingMcpTools.CompareAnalysis(service, postgres, serverName, 4, 5, anchor.ToString("o"), ct);
            var message = McpHelpers.ErrorMessageOf(overlapping);
            Assert.Contains("overlap", message, StringComparison.Ordinal);
            Assert.Contains("last 4 hours", message, StringComparison.Ordinal);
            Assert.Contains("baseline_hours_back 5", message, StringComparison.Ordinal);

            var pastHalfCeiling = await DarlingMcpTools.CompareAnalysis(service, postgres, serverName, 100, null, anchor.ToString("o"), ct);
            Assert.Contains("no room", McpHelpers.ErrorMessageOf(pastHalfCeiling), StringComparison.Ordinal);

            var followed = await DarlingMcpTools.CompareAnalysis(service, postgres, serverName, 24, null, anchor.ToString("o"), ct);
            Assert.Contains(anchor.AddHours(-48).ToString("o"), followed, StringComparison.Ordinal);
            Assert.DoesNotContain(anchor.AddHours(-28).ToString("o"), followed, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM collect.servers WHERE server_id = $1", serverId);
            });
        }
    }
}
