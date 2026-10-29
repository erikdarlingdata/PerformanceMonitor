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
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6894: on an Amazon RDS target the running_jobs collector never runs, and get_running_jobs answered a precondition that says a setup step
/// on the monitored server can fix it. Lite holds the RDS fact in memory (the connection check), so the tool reads it from there and answers
/// not_collected, the way it does for an Azure SQL Database. A server whose check has not found RDS keeps the old answer.
/// </summary>
public sealed class AwsRdsNotCollectedTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly ServerConnection _server = new() { ServerName = "RdsSrv", DisplayName = "RdsSrv" };

    public AwsRdsNotCollectedTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _tempDir = Path.Combine(Path.GetTempPath(), "AwsRdsNotCollected_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        _dataService = new LocalDataService(fixture.DuckDb);
        _serverManager = new ServerManager(configDir);
        _serverManager.AddServer(_server);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task GetRunningJobs_OnAnRdsServer_IsNotCollected_NamingRds()
    {
        _serverManager.GetConnectionStatus(_server.Id).IsAwsRds = true;

        using var doc = JsonDocument.Parse(await McpJobTools.GetRunningJobs(_dataService, _serverManager));

        Assert.Equal("not_collected", doc.RootElement.GetProperty("status").GetString());
        var message = doc.RootElement.GetProperty("message").GetString()!;
        Assert.Contains("Amazon RDS", message, StringComparison.Ordinal);
        Assert.Contains("running_jobs", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Possible cause", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task GetRunningJobs_WhenTheCheckFoundNoRds_KeepsThePreconditionAnswer()
    {
        using var doc = JsonDocument.Parse(await McpJobTools.GetRunningJobs(_dataService, _serverManager));

        Assert.NotEqual("not_collected", doc.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task FleetCollectionHealth_ListsAnRdsServerAsNotApplicable_WithTheRdsReason()
    {
        _serverManager.GetConnectionStatus(_server.Id).IsAwsRds = true;

        var json = await McpHealthTools.GetCollectionHealth(_dataService, _serverManager, server_name: "*", collector_name: "running_jobs");

        using var doc = JsonDocument.Parse(json);
        var notApplicable = doc.RootElement.GetProperty("not_applicable");
        Assert.Equal(1, notApplicable.GetArrayLength());
        Assert.Contains("Amazon RDS", notApplicable[0].GetProperty("reason").GetString(), StringComparison.Ordinal);
    }
}
