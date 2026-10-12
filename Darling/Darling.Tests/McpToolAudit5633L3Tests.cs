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
/// #5633 items 10, 12 and 23, each pinned on the shape that was wrong:
/// audit_config called a Developer Edition server "Enterprise" (it mapped engine edition 3); compare_analysis
/// accepted overlapping windows (it only checked baseline_hours_back &gt; hours_back, and defaulted to 28 whatever
/// hours_back was); get_read_latency reported percentiles (bucket upper bounds) above max_ms.
/// </summary>
public sealed class McpToolAudit5633L3Tests
{
    [Theory]
    [InlineData("Developer Edition (64-bit)", 3)]
    [InlineData("Enterprise Evaluation Edition (64-bit)", 3)]
    [InlineData("Standard Edition (64-bit)", 2)]
    public void AuditEditionLabel_ReportsTheStoredEditionString_NotTheEngineEditionWord(string stored, int engineEdition)
    {
        var (name, note) = DarlingMcpTools.AuditEditionLabel(stored, engineEdition);

        Assert.Equal(stored, name);
        Assert.Null(note);
    }

    [Fact]
    public void AuditEditionLabel_FallsBackToTheEngineLabel_AndSaysSo_WhenTheStoreHoldsNoEditionString()
    {
        var (name, note) = DarlingMcpTools.AuditEditionLabel("  ", 3);

        Assert.Equal("Enterprise", name);
        Assert.NotNull(note);
        Assert.Contains("engine edition", note, StringComparison.Ordinal);
        Assert.Contains("Developer", note, StringComparison.Ordinal);

        var (unknownName, unknownNote) = DarlingMcpTools.AuditEditionLabel(null, 0);
        Assert.Equal("Unknown", unknownName);
        Assert.NotNull(unknownNote);
    }

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

    [Theory]
    [InlineData(4, null, 28)]
    [InlineData(1, null, 25)]
    [InlineData(24, null, 48)]
    [InlineData(48, null, 96)]
    [InlineData(4, 8, 8)]
    [InlineData(4, 100, 100)]
    public void ValidateBaselineWindow_AcceptsDisjointWindows_AndDefaultsFromHoursBack(int hoursBack, int? baseline, int expected)
    {
        Assert.Null(McpHelpers.ValidateBaselineWindow(hoursBack, baseline, out var resolved));
        Assert.Equal(expected, resolved);
    }

    [Theory]
    [InlineData(4, 5)]
    [InlineData(4, 7)]
    [InlineData(24, 30)]
    public void ValidateBaselineWindow_RefusesOverlappingWindows_NamingBoth(int hoursBack, int baseline)
    {
        var refusal = McpHelpers.ValidateBaselineWindow(hoursBack, baseline, out _);

        Assert.NotNull(refusal);
        var message = McpHelpers.ErrorMessageOf(refusal);
        Assert.Contains("overlap", message, StringComparison.Ordinal);
        Assert.Contains($"last {hoursBack} hours", message, StringComparison.Ordinal);
        Assert.Contains($"baseline_hours_back {baseline}", message, StringComparison.Ordinal);
        Assert.Contains($"{2 * hoursBack}", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateBaselineWindow_OmittedBaseline_PastHalfTheCeiling_IsRefusedWithTheRealReason()
    {
        var refusal = McpHelpers.ValidateBaselineWindow(100, null, out _);

        Assert.NotNull(refusal);
        Assert.Contains("no room", McpHelpers.ErrorMessageOf(refusal), StringComparison.Ordinal);
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

    [Fact]
    public void FromBuckets_CapsEachPercentileAtMax_SoNoneExceedsTheSlowestReadSeen()
    {
        var bounds = ReadLatencyAccumulator.BucketUpperBoundsMs;
        var counts = new long[bounds.Length + 1];
        /* The issue's field shape: every sample landed in the bucket past 3,568 ms, the slowest took 3,568. */
        counts[ReadLatencyAccumulator.BucketIndexFor(3_568)] = 100;

        foreach (var p in new[] { 0.50, 0.95, 0.99 })
        {
            var uncapped = ReadLatencyPercentiles.FromBuckets(counts, bounds, p);
            var capped = ReadLatencyPercentiles.FromBuckets(counts, bounds, p, maxMs: 3_568);

            Assert.True(uncapped!.Value.UpperBoundMs > 3_568, "the old shape: the bucket bound is above max");
            Assert.Equal(3_568, capped!.Value.UpperBoundMs);
            Assert.False(capped.Value.IsAtLeast);
        }
    }

    [Fact]
    public void FromBuckets_LeavesABoundAlone_WhenItIsAtOrBelowMax_AndKeepsTheOverflowFloor()
    {
        var bounds = ReadLatencyAccumulator.BucketUpperBoundsMs;
        var counts = new long[bounds.Length + 1];
        counts[ReadLatencyAccumulator.BucketIndexFor(50)] = 10;

        var low = ReadLatencyPercentiles.FromBuckets(counts, bounds, 0.5, maxMs: 90_000);
        Assert.Equal(bounds[ReadLatencyAccumulator.BucketIndexFor(50)], low!.Value.UpperBoundMs);

        var overflow = new long[bounds.Length + 1];
        overflow[^1] = 5;
        var floor = ReadLatencyPercentiles.FromBuckets(overflow, bounds, 0.5, maxMs: 300_000);
        Assert.Equal(bounds[^1], floor!.Value.UpperBoundMs);
        Assert.True(floor.Value.IsAtLeast);
    }
}
