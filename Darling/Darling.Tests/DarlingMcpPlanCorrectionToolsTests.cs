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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the automatic plan correction MCP slice (#2028) — the tool that closed the one collected table with
/// NO agent-readable path. The surface is exactly <c>get_plan_corrections</c> (static, on a
/// [McpServerToolType] class, returning Task&lt;string&gt;) with the standard
/// server_name/hours_back/limit contract Lite's twin mirrors verbatim; both reads are Postgres-dialect,
/// positional-param, against the base <c>plan_correction</c> table. The two SEMANTIC pins hold the layer
/// split the collector writes into one row: the recommendations read must DROP the enablement-only rows
/// (<c>recommendation_name IS NOT NULL</c> — a database with nothing to recommend lands one row whose
/// recommendation fields are NULL), and the tuning-state read must take exactly the NEWEST capture and
/// DISTINCT it back to one row per database (the enablement columns repeat on every recommendation row).
/// </summary>
public sealed class DarlingMcpPlanCorrectionToolsTests
{
    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpPlanCorrectionTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyGetPlanCorrections()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .ToArray();

        Assert.Equal(new[] { "get_plan_corrections" }, names);
        Assert.NotNull(typeof(DarlingMcpPlanCorrectionTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static for WithGeminiCompatibleTools"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    [Fact]
    public void ParamContract_StandardWindowedShape_ServerNameOptional()
    {
        var method = ToolMethods().Single();
        var mcpParams = method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name, p.HasDefaultValue))
            .ToArray();

        Assert.Equal(new[] { "server_name", "hours_back", "limit", "as_of", "full_text" }, mcpParams.Select(p => p.Name).ToArray());
        Assert.True(mcpParams.Single(p => p.Name == "server_name").HasDefaultValue, "server_name must be optional");
    }

    [Fact]
    public void PlanCorrectionsSql_DropsEnablementOnlyRows_WindowsOnCollectionTime()
    {
        var sql = DarlingPlanCorrectionReader.PlanCorrectionsSql;

        Assert.Contains("FROM plan_correction", sql, StringComparison.Ordinal);

        /* THE semantic pin: one collector row carries two layers, and a database with nothing to recommend
           lands an enablement-only row whose recommendation fields are NULL — without this predicate every
           such database shows up as a phantom "recommendation" in the tool output. */
        SqlTextPin.AssertExpresses(
            "recommendation_name IS NOT NULL",
            sql,
            "every enablement-only database comes back as a phantom recommendation");

        SqlTextPin.AssertExpresses("WHERE server_id = $1", sql, "the read is not scoped to the requested server");
        SqlTextPin.AssertExpresses("collection_time >= $2", sql, "the window's start no longer bounds the read");
        SqlTextPin.AssertExpresses("collection_time <= $3", sql, "the window's end no longer bounds the read");
        SqlTextPin.AssertExpresses("ORDER BY collection_time DESC", sql, "the newest recommendations no longer come first");

        /* #3541 A3: the cap is the CALLER'S, bound as $4, not the Viewer grid's literal 200. The literal gave
           every window the same ~16-hour reach over per-cycle re-captures and the tool published that page as
           the window's count. */
        SqlTextPin.AssertExpresses("LIMIT $4", sql, "the row cap is no longer the caller's limit");
        Assert.DoesNotContain("LIMIT 200", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AutomaticTuningSql_NewestCaptureOnly_OneRowPerDatabase()
    {
        var sql = DarlingPlanCorrectionReader.AutomaticTuningSql;

        Assert.Contains("SELECT DISTINCT", sql, StringComparison.Ordinal);
        Assert.Contains("FROM plan_correction", sql, StringComparison.Ordinal);

        /* The enablement columns repeat on every one of a database's recommendation rows — the snapshot is
           the newest capture DISTINCTed back to one row per database, never a window scan. */
        SqlTextPin.AssertExpresses(
            "collection_time = (SELECT MAX(collection_time) FROM plan_correction WHERE server_id = $1)",
            sql,
            "the snapshot is a window scan rather than the newest capture");
        Assert.Contains("force_last_good_plan_desired_state", sql, StringComparison.Ordinal);
        Assert.Contains("force_last_good_plan_actual_state", sql, StringComparison.Ordinal);
        SqlTextPin.AssertExpresses("ORDER BY database_name", sql, "the one row per database no longer arrives ordered");
    }

    [Theory]
    [InlineData(nameof(DarlingPlanCorrectionReader.PlanCorrectionsSql))]
    [InlineData(nameof(DarlingPlanCorrectionReader.AutomaticTuningSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = (string)typeof(DarlingPlanCorrectionReader).GetField(sqlName, BindingFlags.Public | BindingFlags.Static)!.GetValue(null)!;
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.DoesNotContain("[", sql, StringComparison.Ordinal);
    }
}

/// <summary>The live window-floor cases of get_plan_corrections (#4966).</summary>
[Collection("live-postgres")]
public sealed class DarlingMcpPlanCorrectionToolsLivePostgresTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* #4966: the window-floor notice on get_plan_corrections. Rows are windowed on collection_time, the probe's own column. */
    private const string WindowCollector = "plan_correction";

    private static string WindowName(string window) => "plan-correction-window-" + window;

    private static Task<string> CallWindowAsync(NpgsqlDataSource ds, string window, int hours, DateTime end, int limit = 25) =>
        DarlingMcpPlanCorrectionTools.GetPlanCorrections(ds, WindowName(window), hours, limit, as_of: WebDataStartNote.FormatWindowEnd(end));

    private static Task RunWindowAsync(string window, Func<NpgsqlConnection, NpgsqlDataSource, DateTime, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, [WindowCollector], [WindowName(window)], ["plan_correction"], body);

    private static Task SeedWindowAsync(NpgsqlConnection c, string window, DateTime created, DateTime? runsFrom, int step, DateTime end) =>
        WindowFloorLiveHarness.SeedServerAsync(c, WindowName(window), created, WindowCollector, runsFrom, step, end, ["plan_correction"], TestContext.Current.CancellationToken);

    /// <summary>A recommendation row (or, with a null name, an enablement-only row) captured at <paramref name="at"/>.</summary>
    private static Task SeedPlanRowAsync(NpgsqlConnection c, string window, DateTime at, string? recommendation) =>
        DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
            @"INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, force_last_good_plan_desired_state, force_last_good_plan_actual_state, recommendation_name, recommendation_state, query_id, query_text)
VALUES ($1,$2,$3,$4,$5,$6,$6,$7,$8,$9,$10)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerIdHelper.GetDeterministicHashCode(WindowName(window)), WindowName(window),
            "StackOverflow", "Enabled", (object?)recommendation ?? DBNull.Value, recommendation is null ? (object)DBNull.Value : "Active", 1000L, "SELECT 1");

    private static async Task SeedThreeRowsAsync(NpgsqlConnection c, string window, DateTime first)
    {
        for (var i = 0; i < 3; i++)
        {
            await SeedPlanRowAsync(c, window, first.AddHours(i), "PlanRegression_" + i);
        }
    }

    [Fact]
    public async Task ARowsAnswer_ForAServerAddedTwoDaysAgo_NamesWhereCoverageStarts_AndAPagedPageKeepsTheTwoFlagsApart_AgainstDevPostgres() =>
        await RunWindowAsync("added", async (c, ds, end) =>
        {
            var added = end.AddDays(-2);
            await SeedWindowAsync(c, "added", added, added, 30, end);
            await SeedThreeRowsAsync(c, "added", end.AddDays(-1));

            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "added", 168, end, limit: 2));

            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.True(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(McpHelpers.FormatEffectiveStart(added), root.GetProperty("effective_start").GetString());
            Assert.Equal(DarlingMcpWindowNotice.Build(added, end.AddHours(-168), "plan_correction").TruncationNote, root.GetProperty("truncation_note").GetString());
            Assert.False(root.TryGetProperty("effective_hours_back", out _));

            var names = root.EnumerateObject().Select(p => p.Name).ToList();
            Assert.Equal(["effective_start", "window_truncated", "truncation_note"], names.Skip(names.IndexOf("hours_back") + 1).Take(3));
            Assert.True(names.IndexOf("truncation_note") < names.IndexOf("automatic_tuning"));
        });

    [Fact]
    public async Task ARowsAnswer_WhoseFirstRowComesLate_IsCovered_AgainstDevPostgres() =>
        await RunWindowAsync("quiet", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "quiet", end.AddDays(-30), end.AddDays(-8), 60, end);
            await SeedThreeRowsAsync(c, "quiet", end.AddDays(-5));

            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "quiet", 168, end));

            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(System.Text.Json.JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
            var effective = DateTime.Parse(root.GetProperty("effective_start").GetString()!, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind);
            Assert.InRange((effective - end.AddHours(-168)).TotalSeconds, 0, 120);
        });

    [Fact]
    public async Task AnEmptyAnswer_PastCoverage_CarriesHints_AgainstDevPostgres() =>
        await RunWindowAsync("empty", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "empty", end.AddDays(-30), null, 30, end);

            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "empty", 1, end));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            var hints = root.GetProperty("hints");
            Assert.True(hints.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(System.Text.Json.JsonValueKind.Null, hints.GetProperty("effective_start").ValueKind);
            Assert.Contains("no collection of plan_correction", hints.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);
        });

    [Fact]
    public async Task ATuningOnlyAnswer_IsAData_Answer_AndIsProbedWhateverTheWindow_AgainstDevPostgres() =>
        await RunWindowAsync("tuning", async (c, ds, end) =>
        {
            /* The server's collection began 30 minutes into the one-hour window; the only row is an enablement snapshot from before it. */
            var began = end.AddMinutes(-30);
            await SeedWindowAsync(c, "tuning", began, began, 10, end);
            await SeedPlanRowAsync(c, "tuning", end.AddDays(-3), null);

            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "tuning", 1, end));

            Assert.False(root.TryGetProperty("status", out _));
            Assert.Equal(0, root.GetProperty("recommendations_returned").GetInt32());
            Assert.Equal(1, root.GetProperty("automatic_tuning").GetArrayLength());
            /* Probed though the window is one hour: the start is the floor, not the asked start. */
            Assert.Equal(McpHelpers.FormatEffectiveStart(began), root.GetProperty("effective_start").GetString());
        });

    [Fact]
    public async Task AShortWindow_WithRows_StartsNoProbe_AgainstDevPostgres() =>
        await RunWindowAsync("short", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "short", end.AddDays(-30), end.AddDays(-2), 30, end);
            await SeedPlanRowAsync(c, "short", end.AddMinutes(-20), "PlanRegression_short");
            var calls = 0;
            DarlingMcpWindowNotice.TestOnlyProbe = () => { calls++; return Task.FromResult<DateTime?>(null); };

            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "short", 1, end));

            Assert.Equal(0, calls);
            Assert.False(root.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(1, root.GetProperty("recommendations_returned").GetInt32());
        });

    [Fact]
    public async Task AFailedProbe_CostsTheNotice_NeverTheRows_AgainstDevPostgres() =>
        await RunWindowAsync("probefail", async (c, ds, end) =>
        {
            await SeedWindowAsync(c, "probefail", end.AddDays(-2), end.AddDays(-2), 30, end);
            await SeedThreeRowsAsync(c, "probefail", end.AddDays(-1));
            DarlingMcpWindowNotice.TestOnlyProbe = () => throw new TimeoutException("the probe's deadline passed");

            var root = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "probefail", 168, end));
            Assert.False(root.TryGetProperty("status", out _));
            Assert.Equal(3, root.GetProperty("recommendations_returned").GetInt32());
            Assert.False(root.TryGetProperty("effective_start", out _));
            Assert.False(root.TryGetProperty("window_truncated", out _));
            Assert.False(root.TryGetProperty("truncation_note", out _));

            var empty = WindowFloorLiveHarness.Parse(await CallWindowAsync(ds, "probefail", 1, end.AddDays(-9)));
            Assert.Equal("empty", empty.GetProperty("status").GetString());
            Assert.False(empty.TryGetProperty("hints", out _));
        });
}
