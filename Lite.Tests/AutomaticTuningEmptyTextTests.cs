/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7813: Lite's get_plan_corrections says which case an empty answer is: the server's version has no automatic tuning, or the collector
/// has stored no cycle for it yet (every cycle writes an enablement row per database, so an empty store is not a Query Store verdict).
/// The version comes from the connection status the way the PVS sentence reads it.
/// </summary>
public sealed class AutomaticTuningEmptyTextTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "AutoTuningEmptySrv";

    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly ServerConnection _server;
    private readonly LocalDataService _service;

    public AutomaticTuningEmptyTextTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _service = new LocalDataService(fixture.DuckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-autotuningempty-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        _server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(_server);
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private void SetServerMajorVersion(int major) =>
        _serverManager.GetConnectionStatus(_server.Id).SqlMajorVersion = major;

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private static string Message(JsonElement root) => root.GetProperty("message").GetString()!;

    private Task<string> AskAsync() => McpPlanCorrectionTools.GetPlanCorrections(_service, _serverManager, ServerName);

    [Fact]
    public async Task AServerBefore2017_HearsThatItsVersionHasNoAutomaticTuning()
    {
        SetServerMajorVersion(13);

        var root = Parse(await AskAsync());

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal(PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 13 }), Message(root));
        Assert.Contains("has no automatic tuning", Message(root), StringComparison.Ordinal);
    }

    /// <summary>The reachable 2017+ case: a registered server whose collector has stored no plan correction cycle.</summary>
    [Fact]
    public async Task AServerOn2017OrLater_WithNoStoredCycle_HearsThatTheCollectorHasNotStoredOne()
    {
        SetServerMajorVersion(15);

        var root = Parse(await AskAsync());

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal(PlanCorrectionCollector.NoRowsMessage(new CollectorTargetInfo { SqlMajorVersion = 15 }), Message(root));
        Assert.Contains("has not stored a cycle", Message(root), StringComparison.Ordinal);
        Assert.Contains("ollection health", Message(root), StringComparison.Ordinal);
        Assert.DoesNotContain("Query Store", Message(root), StringComparison.Ordinal);
        Assert.DoesNotContain("nothing has been recommended", Message(root), StringComparison.Ordinal);
    }

    /// <summary>A filtered empty answer is a verdict about the chosen database only and keeps its own sentence.</summary>
    [Fact]
    public async Task AFilteredEmptyAnswer_KeepsTheChosenDatabaseSentence()
    {
        SetServerMajorVersion(13);

        var root = Parse(await McpPlanCorrectionTools.GetPlanCorrections(_service, _serverManager, ServerName, database_name: "NoSuchDb"));

        Assert.Equal("No plan correction data found for the database NoSuchDb.", Message(root));
    }
}
