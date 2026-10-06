/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
/// #5244 (PR4, lane L2): <c>database_name</c> on Lite's get_long_query_completions and get_plan_corrections, the twins
/// of Darling's list-taking readers. Each gets a DuckDB fixture with three databases where the filter returns only the
/// chosen database's rows, a blank name returns all of them, the row cap counts only the chosen database's rows (the
/// predicate sits on the raw rows, before the ranking and the limit), and a filtered empty answer keeps the
/// <c>empty</c> status word and says "for the chosen databases" the way Darling's tools do.
/// </summary>
public sealed class EventListDatabaseFilterToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "EventListDbFilterSrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private readonly LocalDataService _service;
    private DuckDBConnection? _seedConn;
    private long _nextId = 950000;
    private readonly DateTime _planBase = new(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day, DateTime.UtcNow.Hour, DateTime.UtcNow.Minute, 0, DateTimeKind.Utc);

    public EventListDatabaseFilterToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-eventlistdbfilter-" + Guid.NewGuid().ToString("N"));
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
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    private async Task ExecuteAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private static string[] Databases(JsonElement root, string array) =>
        root.GetProperty(array).EnumerateArray().Select(r => r.GetProperty("database_name").GetString()!).ToArray();

    /* ───────────────────────── get_long_query_completions ───────────────────────── */

    private Task SeedLongQueryAsync(string database, int seconds, int minutesAgo)
    {
        var when = Naive(DateTime.UtcNow.AddMinutes(-minutesAgo));
        return ExecuteAsync(@"
INSERT INTO long_query_completions (long_query_completion_id, collection_time, server_id, server_name, event_time, event_type, database_name, statement_text, duration_microseconds)
VALUES ($1, $2, $3, $4, $5, 'rpc_completed', $6, 'SELECT 1', $7)",
            _nextId++, when, _serverId, ServerName, when, database, seconds * 1_000_000L);
    }

    /// <summary>DbA holds the three slowest runs (9, 8 and 7 s), DbB one 1 s run and DbC one 2 s run.</summary>
    private async Task SeedThreeDatabaseLongQueriesAsync()
    {
        await SeedLongQueryAsync("DbA", 9, 5);
        await SeedLongQueryAsync("DbA", 8, 6);
        await SeedLongQueryAsync("DbA", 7, 7);
        await SeedLongQueryAsync("DbB", 1, 8);
        await SeedLongQueryAsync("DbC", 2, 9);
    }

    [Fact]
    public async Task LongQueries_OneName_ReturnsOnlyThatDatabasesRows()
    {
        await SeedThreeDatabaseLongQueriesAsync();

        var root = Parse(await McpLongQueryTools.GetLongQueryCompletions(
            _service, _serverManager, ServerName, 24, 30, database_name: "DbB"));

        Assert.Equal(new[] { "DbB" }, Databases(root, "completions"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task LongQueries_BlankName_ReturnsEveryDatabase(string? blank)
    {
        await SeedThreeDatabaseLongQueriesAsync();

        var root = Parse(await McpLongQueryTools.GetLongQueryCompletions(
            _service, _serverManager, ServerName, 24, 30, database_name: blank));

        Assert.Equal(5, root.GetProperty("completions_returned").GetInt32());
        Assert.Equal(new[] { "DbA", "DbA", "DbA", "DbC", "DbB" }, Databases(root, "completions"));
    }

    [Fact]
    public async Task LongQueries_TheLimitCountsOnlyTheChosenDatabasesRows()
    {
        await SeedThreeDatabaseLongQueriesAsync();

        // Unfiltered, limit 1 would be DbA's 9 s run. Filtered to DbB it is DbB's only run, and nothing was cut.
        var one = Parse(await McpLongQueryTools.GetLongQueryCompletions(
            _service, _serverManager, ServerName, 24, 1, database_name: "DbB"));
        Assert.Equal(new[] { "DbB" }, Databases(one, "completions"));
        Assert.False(one.GetProperty("truncated").GetBoolean());

        // DbA holds three runs: limit 2 is its two slowest and says the window held more.
        var two = Parse(await McpLongQueryTools.GetLongQueryCompletions(
            _service, _serverManager, ServerName, 24, 2, database_name: "DbA"));
        Assert.Equal(2, two.GetProperty("completions_returned").GetInt32());
        Assert.True(two.GetProperty("truncated").GetBoolean());
        Assert.Equal(new[] { 9000.0, 8000.0 }, two.GetProperty("completions").EnumerateArray().Select(r => r.GetProperty("duration_ms").GetDouble()).ToArray());
    }

    [Fact]
    public async Task LongQueries_FilteredEmpty_SaysTheChosenDatabases_AndStaysEmpty()
    {
        await SeedThreeDatabaseLongQueriesAsync();

        var root = Parse(await McpLongQueryTools.GetLongQueryCompletions(
            _service, _serverManager, ServerName, 24, 30, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains("for the chosen databases", root.GetProperty("message").GetString());
    }

    [Fact]
    public async Task LongQueries_UnfilteredEmpty_KeepsTheOriginalWording()
    {
        await SeedLongQueryAsync("DbA", 9, 5);

        var root = Parse(await McpLongQueryTools.GetLongQueryCompletions(
            _service, _serverManager, ServerName, 1, 30, as_of: DateTime.UtcNow.AddDays(-3).ToString("o")));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.DoesNotContain("chosen databases", root.GetProperty("message").GetString());
    }

    /* ───────────────────────── get_plan_corrections ───────────────────────── */

    private Task SeedPlanCorrectionAsync(string database, int index, int minutesAgo)
    {
        // Whole minutes off one base, so the rows of one capture share a collection_time exactly (the tuning snapshot is
        // the rows stamped with the server's newest collection_time).
        var when = Naive(_planBase.AddMinutes(-minutesAgo));
        return ExecuteAsync(@"
INSERT INTO plan_correction
    (collection_id, collection_time, server_id, server_name, database_name,
     force_last_good_plan_desired_state, force_last_good_plan_actual_state, force_last_good_plan_reason,
     recommendation_name, recommendation_state, score, query_id, query_text)
VALUES ($1, $2, $3, $4, $5, 'Enabled', 'Enabled', 'ok', $6, 'Active', $7, $8, 'SELECT 1')",
            _nextId++, when, _serverId, ServerName, database, $"PlanRegression_{database}_{index}", 50 + index, 1000L + index);
    }

    /// <summary>DbA holds three recommendations, DbB one and DbC one; the newest capture of each database is the same
    /// minute, so the automatic-tuning snapshot names all three.</summary>
    private async Task SeedThreeDatabasePlanCorrectionsAsync()
    {
        await SeedPlanCorrectionAsync("DbA", 1, 30);
        await SeedPlanCorrectionAsync("DbA", 2, 20);
        await SeedPlanCorrectionAsync("DbA", 3, 2);
        await SeedPlanCorrectionAsync("DbB", 4, 2);
        await SeedPlanCorrectionAsync("DbC", 5, 2);
    }

    [Fact]
    public async Task PlanCorrections_OneName_NarrowsBothLayers()
    {
        await SeedThreeDatabasePlanCorrectionsAsync();

        var root = Parse(await McpPlanCorrectionTools.GetPlanCorrections(
            _service, _serverManager, ServerName, 24, 25, database_name: "DbB"));

        Assert.Equal(new[] { "DbB" }, Databases(root, "recommendations"));
        Assert.Equal(new[] { "DbB" }, Databases(root, "automatic_tuning"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task PlanCorrections_BlankName_ReturnsEveryDatabase(string? blank)
    {
        await SeedThreeDatabasePlanCorrectionsAsync();

        var root = Parse(await McpPlanCorrectionTools.GetPlanCorrections(
            _service, _serverManager, ServerName, 24, 25, database_name: blank));

        Assert.Equal(5, root.GetProperty("recommendations_returned").GetInt32());
        Assert.Equal(new[] { "DbA", "DbB", "DbC" }, Databases(root, "automatic_tuning").OrderBy(d => d).ToArray());
    }

    [Fact]
    public async Task PlanCorrections_TheLimitCountsOnlyTheChosenDatabasesRows()
    {
        await SeedThreeDatabasePlanCorrectionsAsync();

        // Newest first with limit 2 over every database would be two of the three 2-minutes-ago rows. Filtered to DbA
        // the page is DbA's newest two, and the third DbA row makes it a truncated page.
        var root = Parse(await McpPlanCorrectionTools.GetPlanCorrections(
            _service, _serverManager, ServerName, 24, 2, database_name: "DbA"));

        Assert.Equal(new[] { "DbA", "DbA" }, Databases(root, "recommendations"));
        Assert.True(root.GetProperty("truncated").GetBoolean());

        var one = Parse(await McpPlanCorrectionTools.GetPlanCorrections(
            _service, _serverManager, ServerName, 24, 1, database_name: "DbC"));
        Assert.Equal(new[] { "DbC" }, Databases(one, "recommendations"));
        Assert.False(one.GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public async Task PlanCorrections_FilteredEmpty_SaysTheChosenDatabases_AndStaysEmpty()
    {
        await SeedThreeDatabasePlanCorrectionsAsync();

        var root = Parse(await McpPlanCorrectionTools.GetPlanCorrections(
            _service, _serverManager, ServerName, 24, 25, database_name: "NoSuchDb"));

        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Equal("No plan correction data found for the chosen databases.", root.GetProperty("message").GetString());
    }
}
