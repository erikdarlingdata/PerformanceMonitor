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
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for <c>get_finops</c>: the tool surface, the closed view set, the refusals, the empty answer, the
/// impact band's cut points, and (live) that every row equals the storage read's on the same seed.
/// </summary>
/* #1776 own-store: both live facts (the refusal fact and LiveParity) mint their own scratch database through ScratchPostgres; the rest touch no store. */
public sealed class DarlingMcpFinOpsToolsTests
{
    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpFinOpsTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyGetFinOps_WithItsParametersAndClosedViewSet()
    {
        var methods = ToolMethods();
        Assert.Equal(new[] { "get_finops" }, methods.Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name).ToArray());
        Assert.NotNull(typeof(DarlingMcpFinOpsTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        var method = methods.Single();
        Assert.True(method.IsStatic);
        Assert.Equal(typeof(Task<string>), method.ReturnType);

        var described = method.GetParameters().Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null).ToArray();
        Assert.Equal(new[] { "view", "server_name", "hours_back", "limit" }, described.Select(p => p.Name).ToArray());
        Assert.False(described.Single(p => p.Name == "view").HasDefaultValue, "view is required");
        Assert.Equal(24, described.Single(p => p.Name == "hours_back").DefaultValue);
        Assert.Equal(10, described.Single(p => p.Name == "limit").DefaultValue);

        // Each set asserts its own views on its own lines, so two series never edit the same line.
        // FinOps web parity (#4843), set A: list your views below this line only.
        var setAViews = new[] { "utilization" };
        // FinOps web parity (#4843), set A ends.
        // Set A and set B are separated on purpose: keep this gap.
        //
        //
        //
        // FinOps web parity (#4843), set B: list your views below this line only.
        var setBViews = new[] { "high_impact", "database_resources", "application_connections" };
        // FinOps web parity (#4843), set B ends.
        var valid = DarlingMcpFinOpsTools.SetAValid + DarlingMcpFinOpsTools.SetBValid;
        foreach (var view in setAViews.Concat(setBViews))
        {
            Assert.Contains(view, DarlingMcpFinOpsTools.Views);
            Assert.Contains(view, valid, StringComparison.Ordinal);
        }
        Assert.Equal(setAViews.Length + setBViews.Length, DarlingMcpFinOpsTools.Views.Length);
        Assert.Equal(DarlingMcpFinOpsTools.Views.Length, DarlingMcpFinOpsTools.Views.Distinct().Count());
    }

    [Theory]
    [InlineData(0, "low")]
    [InlineData(59, "low")]
    [InlineData(60, "medium")]
    [InlineData(79, "medium")]
    [InlineData(80, "high")]
    [InlineData(100, "high")]
    public void ImpactBand_UsesTheViewersCutPoints(int score, string band)
    {
        Assert.Equal(band, HighImpactScorer.HighImpactBand(score));
    }

    [Fact]
    public async Task Refusals_NameTheParameter_AndAnUnknownViewListsTheValidOnes()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the get_finops refusal facts.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, FinOpsHighImpactReaderLiveTests.ServerId, FinOpsHighImpactReaderLiveTests.ServerName, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var name = FinOpsHighImpactReaderLiveTests.ServerName;

        var unknownView = await DarlingMcpFinOpsTools.GetFinOps(postgres, "nope", name, cancellationToken: ct);
        Assert.True(McpHelpers.IsRefusalEnvelope(unknownView), unknownView);
        var message = JsonDocument.Parse(unknownView).RootElement.GetProperty("message").GetString()!;
        Assert.Contains("'nope'", message, StringComparison.Ordinal);
        Assert.Contains("high_impact", message, StringComparison.Ordinal);
        Assert.Equal("view", JsonDocument.Parse(unknownView).RootElement.GetProperty("hints").GetProperty("parameter").GetString());

        foreach (var badHours in new[] { 0, -1, 169 })
        {
            var refused = await DarlingMcpFinOpsTools.GetFinOps(postgres, "high_impact", name, hours_back: badHours, cancellationToken: ct);
            Assert.True(McpHelpers.IsRefusalEnvelope(refused), $"hours_back {badHours}: {refused}");
            Assert.Equal("hours_back", JsonDocument.Parse(refused).RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        }

        foreach (var badLimit in new[] { 0, -1, 51 })
        {
            var refused = await DarlingMcpFinOpsTools.GetFinOps(postgres, "high_impact", name, limit: badLimit, cancellationToken: ct);
            Assert.True(McpHelpers.IsRefusalEnvelope(refused), $"limit {badLimit}: {refused}");
            Assert.Equal("limit", JsonDocument.Parse(refused).RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        }

        var unknownServer = await DarlingMcpFinOpsTools.GetFinOps(postgres, "high_impact", "no-such-server", cancellationToken: ct);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(unknownServer));

        // A server with no query statistics answers empty (or not_collected), never an error.
        var empty = await DarlingMcpFinOpsTools.GetFinOps(postgres, "high_impact", name, cancellationToken: ct);
        Assert.False(McpHelpers.IsErrorEnvelope(empty), empty);
        Assert.Contains(DarlingMcpTestData.StatusOf(empty), new[] { "empty", "not_collected" });
    }

    /* #1776 own-store: the fact seeds its own scratch database, so nothing here shares rows with another test. */
    public sealed class LiveParity
    {
        [Fact]
        public async Task EveryRow_EqualsTheStorageRead_OnTheSameSeed_InTheSameOrder()
        {
            var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
            Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live get_finops parity fact.");
            var ct = TestContext.Current.CancellationToken;
            await using var scratch = await FinOpsHighImpactReaderLiveTests.SeedAsync(cs!, ct);
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var expected = await DarlingFinOpsHighImpactReader.ReadAsync(postgres, FinOpsHighImpactReaderLiveTests.ServerId, 24, 60, ct);
            var json = await DarlingMcpFinOpsTools.GetFinOps(postgres, "high_impact", FinOpsHighImpactReaderLiveTests.ServerName, cancellationToken: ct);
            Assert.False(McpHelpers.IsErrorEnvelope(json), json);

            var root = JsonDocument.Parse(json).RootElement;
            Assert.Equal(FinOpsHighImpactReaderLiveTests.ServerName, root.GetProperty("server").GetString());
            Assert.Equal("high_impact", root.GetProperty("view").GetString());
            Assert.Equal(24, root.GetProperty("hours_back").GetInt32());
            var rows = root.GetProperty("rows").EnumerateArray().ToArray();
            Assert.Equal(expected.Count, rows.Length);
            Assert.NotEmpty(rows);
            for (var i = 0; i < rows.Length; i++)
            {
                var e = expected[i];
                var r = rows[i];
                Assert.Equal(e.QueryHash, r.GetProperty("query_hash").GetString());
                Assert.Equal(e.DatabaseName, r.GetProperty("database_name").GetString());
                Assert.Equal(e.TotalExecutions, r.GetProperty("total_executions").GetInt64());
                Assert.Equal(e.TotalCpuMs, r.GetProperty("total_cpu_ms").GetDecimal());
                Assert.Equal(e.TotalDurationMs, r.GetProperty("total_duration_ms").GetDecimal());
                Assert.Equal(e.TotalReads, r.GetProperty("total_reads").GetInt64());
                Assert.Equal(e.TotalWrites, r.GetProperty("total_writes").GetInt64());
                Assert.Equal(e.TotalMemoryMb, r.GetProperty("total_memory_mb").GetDecimal());
                Assert.Equal(e.CpuShare, r.GetProperty("cpu_share_pct").GetDecimal());
                Assert.Equal(e.DurationShare, r.GetProperty("duration_share_pct").GetDecimal());
                Assert.Equal(e.ReadsShare, r.GetProperty("reads_share_pct").GetDecimal());
                Assert.Equal(e.WritesShare, r.GetProperty("writes_share_pct").GetDecimal());
                Assert.Equal(e.MemoryShare, r.GetProperty("memory_share_pct").GetDecimal());
                Assert.Equal(e.ExecutionsShare, r.GetProperty("executions_share_pct").GetDecimal());
                Assert.Equal(e.ImpactScore, r.GetProperty("impact_score").GetInt32());
                Assert.Equal(HighImpactScorer.HighImpactBand(e.ImpactScore), r.GetProperty("impact_band").GetString());
                Assert.Equal(e.SampleQueryText, r.GetProperty("sample_query_text").GetString());
                Assert.Equal(!string.IsNullOrEmpty(e.QueryPlanXml), r.GetProperty("has_plan").GetBoolean());
                Assert.False(r.TryGetProperty("full_query_text", out _));
                Assert.False(r.TryGetProperty("query_plan_xml", out _));
            }

            // limit reaches the reader's topN: two kept per measure is a strict subset, in the reader's order.
            var narrow = await DarlingFinOpsHighImpactReader.ReadAsync(postgres, FinOpsHighImpactReaderLiveTests.ServerId, 24, 60, ct, topN: 2);
            var narrowJson = await DarlingMcpFinOpsTools.GetFinOps(postgres, "high_impact", FinOpsHighImpactReaderLiveTests.ServerName, limit: 2, cancellationToken: ct);
            Assert.False(McpHelpers.IsErrorEnvelope(narrowJson), narrowJson);
            var narrowRows = JsonDocument.Parse(narrowJson).RootElement.GetProperty("rows").EnumerateArray().ToArray();
            Assert.Equal(narrow.Select(q => q.QueryHash).ToArray(), narrowRows.Select(r => r.GetProperty("query_hash").GetString()).ToArray());
            Assert.NotEmpty(narrowRows);
            Assert.True(narrowRows.Length < rows.Length, $"limit 2 returned {narrowRows.Length} rows, the default {rows.Length}");
        }
    }
}
