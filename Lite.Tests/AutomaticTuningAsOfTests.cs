/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The automatic-tuning snapshot beside the plan correction rows is the newest capture at or before the instant the answer is
/// as of. Two captures are stored, 08:00 with the state OLD and 10:00 with the state NEW: an answer as of 09:00 shows OLD, an
/// answer as of 10:00 or with no bound shows NEW, and an answer as of an instant before the first capture shows no snapshot.
/// </summary>
public sealed class AutomaticTuningAsOfTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "AutomaticTuningAsOfSrv";

    private static readonly DateTime FirstCapture = new(2026, 3, 10, 8, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime SecondCapture = FirstCapture.AddHours(2);

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly LocalDataService _service;
    private DuckDBConnection? _conn;
    private long _id = 1;

    public AutomaticTuningAsOfTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-automatictuningasof-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);

        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _conn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private async Task SeedCaptureAsync(DateTime collectionTime, string actualState)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_conn is null)
        {
            _conn = _duckDb.CreateConnection();
            await _conn.OpenAsync();
        }
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO plan_correction
    (collection_id, collection_time, server_id, server_name, database_name,
     force_last_good_plan_desired_state, force_last_good_plan_actual_state, force_last_good_plan_reason,
     recommendation_name, recommendation_state, score, query_id, query_text)
VALUES ($1, $2, $3, $4, 'DbA', 'Enabled', $5, 'ok', 'PlanRegression_1', 'Active', 50, 1000, 'SELECT 1')";
        cmd.Parameters.Add(new DuckDBParameter { Value = _id++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = actualState });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedBothCapturesAsync()
    {
        await SeedCaptureAsync(FirstCapture, "OLD");
        await SeedCaptureAsync(SecondCapture, "NEW");
    }

    [Fact]
    public async Task AnAnswerAsOfBetweenTheCaptures_ShowsTheEarlierCapture()
    {
        await SeedBothCapturesAsync();

        var row = Assert.Single(await _service.GetLatestAutomaticTuningAsync(_serverId, asOfUtc: FirstCapture.AddHours(1)));

        Assert.Equal("OLD", row.ActualState);
        Assert.Equal(FirstCapture, row.CollectionTime);
    }

    [Fact]
    public async Task AnAnswerAsOfACapturesOwnInstant_ShowsThatCapture()
    {
        await SeedBothCapturesAsync();

        Assert.Equal("OLD", Assert.Single(await _service.GetLatestAutomaticTuningAsync(_serverId, asOfUtc: FirstCapture)).ActualState);
        Assert.Equal("NEW", Assert.Single(await _service.GetLatestAutomaticTuningAsync(_serverId, asOfUtc: SecondCapture)).ActualState);
    }

    [Fact]
    public async Task AnAnswerWithNoBound_ShowsTheNewestCapture()
    {
        await SeedBothCapturesAsync();

        Assert.Equal("NEW", Assert.Single(await _service.GetLatestAutomaticTuningAsync(_serverId)).ActualState);
    }

    [Fact]
    public async Task AnAnswerAsOfBeforeTheFirstCapture_ShowsNoSnapshot()
    {
        await SeedBothCapturesAsync();

        Assert.Empty(await _service.GetLatestAutomaticTuningAsync(_serverId, asOfUtc: FirstCapture.AddMinutes(-1)));
    }

    /// <summary>The filter to one database narrows the rows of the capture the bound picked, not the capture.</summary>
    [Fact]
    public async Task AnAnswerAsOfBetweenTheCaptures_ForOneDatabase_ShowsTheEarlierCapture()
    {
        await SeedBothCapturesAsync();

        var row = Assert.Single(await _service.GetLatestAutomaticTuningAsync(_serverId, new[] { "DbA" }, FirstCapture.AddHours(1)));

        Assert.Equal("OLD", row.ActualState);
        Assert.Empty(await _service.GetLatestAutomaticTuningAsync(_serverId, new[] { "DbB" }, FirstCapture.AddHours(1)));
    }

    [Fact]
    public async Task TheTool_AsOfBetweenTheCaptures_ShowsTheEarlierCapturesState()
    {
        await SeedBothCapturesAsync();

        var json = await McpPlanCorrectionTools.GetPlanCorrections(_service, _serverManager, ServerName, 24, 25, as_of: "2026-03-10T09:00:00Z");
        var tuning = Assert.Single(JsonDocument.Parse(json).RootElement.GetProperty("automatic_tuning").EnumerateArray().ToArray());

        Assert.Equal("OLD", tuning.GetProperty("force_last_good_plan_actual_state").GetString());
    }
}
