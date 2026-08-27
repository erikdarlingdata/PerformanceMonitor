/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
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
/// Pins the system_health parse-on-read MCP slice — the nine get_health_parser_* tools over Darling's raw
/// system_health_events, the same names the Dashboard exposes. Ungated: the exact nine-tool surface, each
/// tool's (server_name, hours_back, limit) contract, the event-xml + database-name read SQL pins (Postgres
/// dialect, event_time window, event_type filter), column parity against the generated collector DDL, the
/// Gemini-clean schema, and the two things that make this a faithful port — (1) the reuse of the shared
/// <see cref="SystemHealthParser"/> shred, and (2) <see cref="SystemHealthSignificance"/>, the
/// service-side twin of the viewer's <see cref="SystemEventSignificance"/> (asserted equal, so the twin
/// can't drift), producing the same SIGNIFICANT warning set.
/// </summary>
public sealed class DarlingMcpHealthParserToolsSurfaceAndSqlTests
{
    private static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name));

    private static readonly string[] HealthParserToolSurface =
    {
        "get_health_parser_cpu_tasks",
        "get_health_parser_io_issues",
        "get_health_parser_memory_broker",
        "get_health_parser_memory_conditions",
        "get_health_parser_memory_node_oom",
        "get_health_parser_scheduler_issues",
        "get_health_parser_severe_errors",
        "get_health_parser_significant_waits",
        "get_health_parser_system_health",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpHealthParserTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheNineHealthParserTools()
    {
        var names = ToolMethods()
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(HealthParserToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpHealthParserTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(ToolMethods(), m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(ToolMethods(), m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    [Fact]
    public void ParamContract_EveryTool_ServerNameHoursBackLimit_AllOptional()
    {
        foreach (var tool in HealthParserToolSurface)
        {
            var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == tool);
            var p = method.GetParameters()
                .Where(x => x.GetCustomAttribute<DescriptionAttribute>() is not null)
                .Select(x => (x.Name!, x.HasDefaultValue))
                .ToArray();
            Assert.Equal(new[] { "server_name", "hours_back", "limit", "as_of" }, p.Select(x => x.Item1).ToArray());
            Assert.All(p, x => Assert.True(x.Item2, $"{tool}.{x.Item1} must be optional"));
        }
    }

    [Fact]
    public void EventsByTypeSql_WindowsOnEventTime_ByServerAndType()
    {
        var sql = DarlingSystemHealthReader.SystemHealthEventsByTypeSql;
        Assert.Contains("FROM v_system_health_events", sql, StringComparison.Ordinal);
        Assert.Contains("event_xml", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        /* Windows on event_time (the XE @timestamp — the event's real time), NOT collection_time, on the
           category's own event_type ($4), newest first — the viewer's choice. */
        Assert.Contains("event_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("event_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("event_type = $4", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY event_time DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseNameMapSql_LatestNamePerId_FromSizeStatsView()
    {
        var sql = DarlingSystemHealthReader.DatabaseNameMapSql;
        Assert.Contains("DISTINCT ON (database_id)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_database_size_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY database_id, collection_time DESC", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2484: the probe that lets an empty parse-on-read answer say WHICH nothing it found must read the
    /// SAME source the read itself reads. A probe on the base table would report a server as captured for
    /// rows the view-backed read can never return -- picking the wrong branch in precisely the case the
    /// probe exists to get right. It is also scoped to the event_type, and windowless by design.
    /// </summary>
    [Fact]
    public void HasAnyEventOfTypeSql_ProbesTheSameView_ScopedToType_AndIgnoresTheWindow()
    {
        var sql = DarlingSystemHealthReader.HasAnyEventOfTypeSql;
        Assert.Contains("FROM v_system_health_events", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("event_type = $2", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        /* Windowless: a time bound here would make the probe answer the same question the read just did. */
        Assert.DoesNotContain("event_time", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(DarlingSystemHealthReader.SystemHealthEventsByTypeSql))]
    [InlineData(nameof(DarlingSystemHealthReader.DatabaseNameMapSql))]
    [InlineData(nameof(DarlingSystemHealthReader.HasAnyEventOfTypeSql))]
    public void Reads_ArePostgresDialect_PositionalParams(string sqlName)
    {
        var sql = (string)typeof(DarlingSystemHealthReader).GetField(sqlName)!.GetValue(null)!;
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadColumns_ExistInTheGeneratedCollectorTables()
    {
        var she = PgSchemaGenerator.CreateTable(SystemHealthEventsCollector.Instance);
        Assert.Equal("system_health_events", SystemHealthEventsCollector.Instance.TargetTable);
        foreach (var col in new[] { "event_xml", "event_type", "event_time", "collection_time", "server_id" })
            Assert.Contains(col, she, StringComparison.Ordinal);
        Assert.Contains("v_system_health_events", PgSchemaGenerator.AllPassthroughViews);

        var dss = PgSchemaGenerator.CreateTable(DatabaseSizeStatsCollector.Instance);
        Assert.Equal("database_size_stats", DatabaseSizeStatsCollector.Instance.TargetTable);
        foreach (var col in new[] { "database_id", "database_name", "collection_time", "server_id" })
            Assert.Contains(col, dss, StringComparison.Ordinal);
        Assert.Contains("v_database_size_stats", PgSchemaGenerator.AllPassthroughViews);
    }

    private static List<ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpHealthParserTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_NoRequiredParams()
    {
        var tools = BuildToolSchemas();
        /* Derived from the pinned name list rather than restated, so a ninth tool cannot land with this
           literal left describing eight. */
        Assert.Equal(HealthParserToolSurface.Length, tools.Count);
        var violations = tools.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
        foreach (var t in tools)
            Assert.Empty(DarlingMcpSchemaAssert.RequiredOf(t.InputSchema));   /* server_name / hours_back / limit all default */
    }

    /* ---------------- significance predicates pinned on the ONE shared source (anti-drift) ----------------
       The sp_HealthParser per-category predicates now live once in SystemHealthSignificance (Common); the MCP
       tools and the viewer facade (SystemEventSignificance) both call it. The value assertions below pin the
       shared class to its canonical sp_HealthParser results; the facade-vs-shared equalities guard that the
       viewer entry point still delegates rather than growing an independent (drift-prone) copy. */

    [Fact]
    public void Significance_SchedulerIssue_MatchesViewer()
    {
        foreach (var status in new[] { "WARNING", "OK", "", null })
        {
            var r = new SchedulerIssueRecord { Status = status };
            Assert.Equal(SystemEventSignificance.IsSignificant(r), SystemHealthSignificance.IsSignificant(r));
        }
        Assert.True(SystemHealthSignificance.IsSignificant(new SchedulerIssueRecord { Status = "WARNING" }));
    }

    [Fact]
    public void Significance_SevereError_MatchesViewer_ByCutoffAndIgnoreList()
    {
        foreach (var (sev, num) in new[] { (24, 823), (19, 50000), (18, 50000), (16, 50000), (25, 17830), (20, 18056) })
        {
            var r = new SevereErrorRecord { Severity = sev, ErrorNumber = num };
            Assert.Equal(SystemEventSignificance.IsSignificant(r), SystemHealthSignificance.IsSignificant(r));
        }
        Assert.Equal(19, SystemHealthSignificance.SevereErrorMinSeverity);
        Assert.Contains(17830, SystemHealthSignificance.IgnoredSevereErrorNumbers);
        Assert.Contains(18056, SystemHealthSignificance.IgnoredSevereErrorNumbers);
    }

    [Fact]
    public void Significance_MemoryConditionsAndBroker_MatchViewer_LowOnly()
    {
        foreach (var note in new[] { "RESOURCE_MEMPHYSICAL_LOW", "RESOURCE_MEMPHYSICAL_HIGH", null })
        {
            var mc = new MemoryConditionsRecord { LastNotification = note };
            Assert.Equal(SystemEventSignificance.IsSignificant(mc), SystemHealthSignificance.IsSignificant(mc));
            var mb = new MemoryBrokerRecord { Notification = note };
            Assert.Equal(SystemEventSignificance.IsSignificant(mb), SystemHealthSignificance.IsSignificant(mb));
        }
    }

    [Fact]
    public void Significance_MemoryNodeOom_MatchesViewer_AlwaysSignificant()
    {
        var r = new MemoryNodeOomRecord();
        Assert.Equal(SystemEventSignificance.IsSignificant(r), SystemHealthSignificance.IsSignificant(r));
        Assert.True(SystemHealthSignificance.IsSignificant(r));
    }

    [Fact]
    public void Significance_CpuTasks_MatchesViewer_WarningAndPendingThreshold()
    {
        foreach (var (state, pending) in new (string?, int?)[] { ("WARNING", 10), ("WARNING", 15), ("WARNING", 9), ("WARNING", null), ("CLEAN", 100), (null, 100) })
        {
            var r = new CpuTasksRecord { State = state, PendingTasks = pending };
            Assert.Equal(SystemEventSignificance.IsSignificant(r), SystemHealthSignificance.IsSignificant(r));
        }
        Assert.Equal(10, SystemHealthSignificance.CpuTaskMinPendingTasks);
    }

    [Fact]
    public void Significance_IoIssues_MatchesViewer_WarningOnly()
    {
        foreach (var state in new[] { "WARNING", "CLEAN", "HAS_ISSUES", null })
        {
            var r = new IoIssuesRecord { State = state };
            Assert.Equal(SystemEventSignificance.IsSignificant(r), SystemHealthSignificance.IsSignificant(r));
        }
    }

    /// <summary>
    /// #2484: the ninth category. Its gate is the most selective of the family - four conditions, any one
    /// of which drops the row - which is exactly why an empty result has to distinguish "captured and none
    /// qualified" from "nothing captured". Pinned against the viewer facade like its eight siblings.
    /// </summary>
    [Fact]
    public void Significance_SignificantWait_MatchesViewer_OnAllFourConditions()
    {
        foreach (var (session, text, duration, waitType) in new (int?, string?, long?, string?)[]
        {
            (55, "SELECT 1", 500L, "LCK_M_X"),          /* the boundary: 500 is kept */
            (55, "SELECT 1", 499L, "LCK_M_X"),          /* one ms under, dropped */
            (0, "SELECT 1", 5000L, "LCK_M_X"),          /* system session, dropped */
            (55, null, 5000L, "LCK_M_X"),               /* no statement, dropped */
            (55, "BACKUP DATABASE test", 5000L, "LCK_M_X"), /* a backup waiting is its job, dropped */
            (55, "SELECT 1", 5000L, "LAZYWRITER_SLEEP"),    /* idle wait type, dropped */
        })
        {
            var r = new SignificantWaitRecord
            {
                SessionId = session,
                QueryText = text,
                DurationMs = duration,
                WaitType = waitType,
            };
            Assert.Equal(SystemEventSignificance.IsSignificant(r), SystemHealthSignificance.IsSignificant(r));
        }

        Assert.Equal(500, SystemHealthSignificance.SignificantWaitMinDurationMs);
        Assert.Contains("LAZYWRITER_SLEEP", SystemHealthSignificance.IgnoredWaitTypes);
    }

    /* ---------------- parsed-category round-trip: reuse SystemHealthParser + gate, over the shared fixtures ---------------- */

    [Fact]
    public void RoundTrip_ShredFixturesThenGate_KeepsOnlySignificantRows()
    {
        // The tools' pipeline: SystemHealthParser (Common, reused) shreds each fixture, then
        // SystemHealthSignificance keeps the significant rows. Assert the same significant set the
        // viewer's System Events tab surfaces.
        Assert.True(SystemHealthSignificance.IsSignificant(
            SystemHealthParser.ParseSchedulerIssue(LoadFixture("scheduler_monitor.xml"))!));            // WARNING
        Assert.True(SystemHealthSignificance.IsSignificant(
            SystemHealthParser.ParseSevereError(LoadFixture("error_reported.xml"))!));                  // severity 24
        Assert.True(SystemHealthSignificance.IsSignificant(
            SystemHealthParser.ParseMemoryNodeOom(LoadFixture("memory_node_oom.xml"))!));               // ungated
        Assert.True(SystemHealthSignificance.IsSignificant(
            SystemHealthParser.ParseCpuTasks(LoadFixture("sp_server_diagnostics_query_processing_warning.xml"))!)); // WARNING + 15 pending

        // The representative memory-conditions / broker fixtures are RESOURCE_MEMPHYSICAL_HIGH — dropped.
        Assert.False(SystemHealthSignificance.IsSignificant(
            SystemHealthParser.ParseMemoryConditions(LoadFixture("sp_server_diagnostics_resource.xml"))!));
        Assert.False(SystemHealthSignificance.IsSignificant(
            SystemHealthParser.ParseMemoryBroker(LoadFixture("memory_broker.xml"))!));
        // The clean QUERY_PROCESSING fixture is CLEAN / 0 pending — dropped.
        Assert.False(SystemHealthSignificance.IsSignificant(
            SystemHealthParser.ParseCpuTasks(LoadFixture("sp_server_diagnostics_query_processing.xml"))!));

        // I/O: both WARNING file rows are significant.
        var io = SystemHealthParser.ParseIoIssues(LoadFixture("sp_server_diagnostics_io_subsystem.xml"));
        Assert.Equal(2, io.Count(SystemHealthSignificance.IsSignificant));
    }

    [Fact]
    public void SystemHealth_IsUngated_KeptWhenEventTimePresent()
    {
        // get_health_parser_system_health has NO warnings-only filter — the tool keeps every SYSTEM snapshot
        // with a timestamp (the viewer's GetSystemHealthAsync contract), so the CLEAN fixture is a real row.
        var r = SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_system.xml"))!;
        Assert.Equal("CLEAN", r.State);
        Assert.True(r.EventTime.HasValue);
    }

    [Fact]
    public void SevereError_DatabaseNameResolution_MatchesViewer()
    {
        // Same rules as the viewer's ViewerDataService.ResolveDatabaseName: mapped id → name; 0/null → blank
        // (no DB context); a real-but-unmapped id surfaces the raw id rather than silently blanking.
        var map = new Dictionary<int, string> { [5] = "AdventureWorks", [1] = "master" };
        Assert.Equal("AdventureWorks", DarlingSystemHealthReader.ResolveDatabaseName(5, map));
        Assert.Equal("master", DarlingSystemHealthReader.ResolveDatabaseName(1, map));
        Assert.Equal("", DarlingSystemHealthReader.ResolveDatabaseName(0, map));
        Assert.Equal("", DarlingSystemHealthReader.ResolveDatabaseName(null, map));
        Assert.Equal("database_id 7", DarlingSystemHealthReader.ResolveDatabaseName(7, map));
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trip for the health-parser tools. Plants raw system_health_events rows
/// (real captured-event fixtures) across the categories + a database_size_stats mapping row, then asserts each
/// tool shreds + gates + resolves and returns its data-bearing envelope; an empty store returns the "empty"
/// miss.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpHealthParserToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-health-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name));

    [Fact]
    public async Task HealthParserTools_ReadPlantedEvents_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live health-parser-tools test.");

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

            async Task PlantEvent(string eventType, string fixture) =>
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO system_health_events (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    CollectionIdGenerator.Next(), t, ServerId, ServerName, t, eventType, LoadFixture(fixture));

            await PlantEvent(SystemHealthParser.SchedulerMonitorEvent, "scheduler_monitor.xml");
            await PlantEvent(SystemHealthParser.ErrorReportedEvent, "error_reported.xml");
            await PlantEvent(SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_system.xml");
            await PlantEvent(SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_query_processing_warning.xml");
            await PlantEvent(SystemHealthParser.SpServerDiagnosticsEvent, "sp_server_diagnostics_io_subsystem.xml");
            await PlantEvent(SystemHealthParser.MemoryNodeOomEvent, "memory_node_oom.xml");

            /* database_size_stats maps database_id 6 (the error_reported fixture's id) → a name for severe-error resolution. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id)
VALUES ($1,$2,$3,$4,$5,$6)", CollectionIdGenerator.Next(), t, ServerId, ServerName, "ProdDb", 6);

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpHealthParserTools.GetSchedulerIssues(postgres, ServerName), ServerName, "issues");

            var errors = await DarlingMcpHealthParserTools.GetSevereErrors(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(errors, ServerName, "errors");
            Assert.Contains("ProdDb", errors, StringComparison.Ordinal);   /* database_id 6 resolved to its name */

            DarlingMcpTestData.AssertEnvelope(await DarlingMcpHealthParserTools.GetSystemHealth(postgres, ServerName), ServerName, "entries");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpHealthParserTools.GetCPUTasks(postgres, ServerName), ServerName, "events");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpHealthParserTools.GetIOIssues(postgres, ServerName), ServerName, "issues");
            DarlingMcpTestData.AssertEnvelope(await DarlingMcpHealthParserTools.GetMemoryNodeOOM(postgres, ServerName), ServerName, "events");

            /* memory_conditions / memory_broker have no planted LOW rows → the "empty" miss (not a throw). */
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpHealthParserTools.GetMemoryConditions(postgres, ServerName)));
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpHealthParserTools.GetMemoryBroker(postgres, ServerName)));

            /* an unknown server resolves to the listing error. */
            Assert.StartsWith("Could not resolve server.", await DarlingMcpHealthParserTools.GetSystemHealth(postgres, "darling-no-such-server"), StringComparison.Ordinal);

            /* an empty store returns the miss. */
            await DeleteRowsAsync(connection, ct, keepServer: true);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(await DarlingMcpHealthParserTools.GetSchedulerIssues(postgres, ServerName)));

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
        var sql = $"DELETE FROM system_health_events WHERE server_id = {ServerId}; DELETE FROM database_size_stats WHERE server_id = {ServerId};";
        if (!keepServer) sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
