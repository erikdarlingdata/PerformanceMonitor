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
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Default Trace MCP slice — the single <c>get_default_trace_events</c> tool over Darling's stored
/// <c>default_trace_events</c>, the same name the Dashboard exposes. Ungated (no live store): the tool's
/// (server_name, hours_back, limit) contract, the reader SQL pin (Postgres dialect, event_time window, and —
/// crucially — that it reads the BASE table, NOT a v_* view, like server_properties), column parity against
/// the generated collector DDL, the Gemini-clean schema, and the two anti-drift facts that make item #4 a
/// faithful "feed the significance surface": the tool gates through the shared
/// <see cref="DefaultTraceEventSignificance"/>, and the viewer's <see cref="SystemEventSignificance"/>
/// delegates to that SAME shared gate (asserted equal, so the two can never drift).
/// </summary>
public sealed class DarlingMcpDefaultTraceToolsSurfaceAndSqlTests
{
    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpDefaultTraceTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyGetDefaultTraceEvents()
    {
        var names = ToolMethods().Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name).ToArray();
        Assert.Equal(new[] { "get_default_trace_events" }, names);
        Assert.NotNull(typeof(DarlingMcpDefaultTraceTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(ToolMethods(), m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(ToolMethods(), m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    [Fact]
    public void ParamContract_ServerNameHoursBackLimit_AllOptional()
    {
        var method = ToolMethods().Single();
        var p = method.GetParameters()
            .Where(x => x.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(x => (x.Name!, x.HasDefaultValue))
            .ToArray();
        Assert.Equal(new[] { "server_name", "hours_back", "limit", "as_of" }, p.Select(x => x.Item1).ToArray());
        Assert.All(p, x => Assert.True(x.Item2, $"{x.Item1} must be optional"));
    }

    [Fact]
    public void EventsByWindowSql_ReadsBaseTables_WindowsOnServerLocalEventTime()
    {
        var sql = DarlingDefaultTraceReader.EventsByWindowSql;

        /* Reads the BASE default_trace_events table — NOT a v_* view (like server_properties); the bare name
           resolves through search_path = collect, config, public. */
        Assert.Contains("FROM default_trace_events", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_default_trace_events", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_default_trace_events", string.Join(",", PgSchemaGenerator.AllPassthroughViews));

        Assert.Contains("dte.server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("event_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("event_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY dte.event_time DESC", sql, StringComparison.Ordinal);

        /* The window bounds (UTC) are converted to the server's LOCAL frame — the Default Trace StartTime is
           server-local — by the collected utc_offset_minutes (0 when none yet). */
        Assert.Contains("server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("utc_offset_minutes", sql, StringComparison.Ordinal);
        Assert.Contains("make_interval(mins => svr.offset_minutes)", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(", sql, StringComparison.Ordinal);

        /* Postgres dialect, positional params (no @, no now()/getdate). */
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTable()
    {
        var ddl = PgSchemaGenerator.CreateTable(DefaultTraceEventsCollector.Instance);
        Assert.Equal("default_trace_events", DefaultTraceEventsCollector.Instance.TargetTable);
        foreach (var col in new[]
        {
            "event_time", "event_name", "event_class", "spid", "database_name", "login_name", "host_name",
            "application_name", "object_name", "filename", "text_data", "error_number", "severity",
            "duration_us", "integer_data", "collection_time", "server_id",
        })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_NoRequiredParams()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpDefaultTraceTools>();
        using var provider = services.BuildServiceProvider();
        var tools = provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();

        Assert.Single(tools);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(tools[0].InputSchema)); /* server_name / hours_back / limit all default */
    }

    /* ---------------- the tool gate == the viewer's gate == the ONE shared Common classifier (anti-drift) ---------------- */

    [Theory]
    [InlineData("ErrorLog", 20, true)]
    [InlineData("ErrorLog", 16, true)]
    [InlineData("ErrorLog", 10, false)]
    [InlineData("ErrorLog", null, true)]
    [InlineData("Data File Auto Grow", null, true)]
    [InlineData("Object:Altered", null, true)]
    [InlineData("Audit DBCC Event", 0, true)]
    public void ViewerAndTool_ShareTheOneCommonSignificanceGate(string eventName, int? severity, bool expected)
    {
        /* The shared Common gate is the source of truth. */
        Assert.Equal(expected, DefaultTraceEventSignificance.IsSignificant(eventName, severity));
        /* The viewer's System Events surface delegates to it (so the viewer and the MCP tool agree). */
        Assert.Equal(expected, SystemEventSignificance.IsSignificantDefaultTraceEvent(eventName, severity));
    }

    [Fact]
    public void ViewerClassify_DelegatesToTheSharedClassifier()
    {
        foreach (var name in new[]
        {
            "ErrorLog", "Data File Auto Grow", "Object:Created", "Audit Change Audit Event",
            "Server Memory Change", "Something Else",
        })
        {
            Assert.Equal(DefaultTraceEventSignificance.Classify(name), SystemEventSignificance.ClassifyDefaultTraceEvent(name));
        }

        Assert.Equal(DefaultTraceEventSignificance.SevereErrorLogMinSeverity, SystemEventSignificance.DefaultTraceSevereErrorLogMinSeverity);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for <c>get_default_trace_events</c>. Plants stored Default Trace
/// rows — an auto-grow stall, a SEVERE ErrorLog, and a routine (below-floor) ErrorLog — then asserts the tool
/// returns the SIGNIFICANT set (the stall + the severe ErrorLog, tagged with categories) and DROPS the routine
/// ErrorLog, proving the shared severity gate runs end-to-end; plus the unknown-server error and the empty-store
/// miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpDefaultTraceToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-deftrace-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task GetDefaultTraceEvents_ReturnsSignificantSet_DropsRoutineErrorLog()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live default-trace-tools test.");

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
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

            async Task PlantAsync(string eventName, int? severity, string? textData, long? integerData, long? durationUs) =>
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO default_trace_events
(default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, severity, text_data, integer_data, duration_us)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, t, eventName,
                    (object?)severity ?? DBNull.Value, (object?)textData ?? DBNull.Value,
                    (object?)integerData ?? DBNull.Value, (object?)durationUs ?? DBNull.Value);

            await PlantAsync("Data File Auto Grow", null, null, 256, 1500000);      /* stall — significant */
            await PlantAsync("ErrorLog", 20, "SEVERE_MARKER", null, null);          /* severe — significant */
            await PlantAsync("ErrorLog", 10, "ROUTINE_MARKER", null, null);         /* below floor — dropped */

            var json = await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(json, ServerName, "events");

            /* The significant set: the stall (categorized) + the severe ErrorLog; the routine one is gated out. */
            Assert.Contains("AutoGrowShrink", json, StringComparison.Ordinal);
            Assert.Contains("SEVERE_MARKER", json, StringComparison.Ordinal);
            Assert.DoesNotContain("ROUTINE_MARKER", json, StringComparison.Ordinal);
            JsonAssert.Contains("\"total_events\": 2", json);

            /* Unknown server → the listing error; empty store → the miss. */
            Assert.StartsWith("Could not resolve server.", await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, "darling-no-such-server"), StringComparison.Ordinal);
            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(postgres, ServerName)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, bool keepServer = false)
    {
        var sql = $"DELETE FROM default_trace_events WHERE server_id = {ServerId};";
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
