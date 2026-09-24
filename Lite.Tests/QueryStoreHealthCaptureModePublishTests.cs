/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3796, the code half on Lite: <c>get_query_store_health</c> publishes the two v64 capture modes as the two
/// TRAILING fields of every database row, verbatim where the store has them and <c>null</c> where it does not,
/// and the Query Store grid's read carries the same pair. Executed against a real DuckDB through the real tool
/// and the real <see cref="LocalDataService"/> — the <c>McpStatusEnvelopeTests</c> recipe — rather than pinned
/// in source, because the thing that can go wrong here is an ordinal: the read moved <c>capture_time</c> from
/// ordinal 10 to 12 to make room, and a reader that still took the stamp at 10 would throw on a string, or
/// worse, a reader that took the modes at the stamp's old ordinal would publish a timestamp as a mode.
///
/// <para>Three rows at one capture, one per null shape the description promises: a database on <c>ALL</c>
/// with wait stats <c>ON</c> (the plan-churn factory the issue names), a 2016-shaped database whose wait-stats
/// mode is NULL because the engine has no such column (the collector's <c>HasWaitStatsCaptureMode</c> gate),
/// and a pre-rung row inserted without either column, the way every row written before v64 sits in a
/// migrated database.</para>
/// </summary>
public sealed class QueryStoreHealthCaptureModePublishTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private static readonly DateTime CaptureTime = new(2026, 9, 20, 18, 0, 0, DateTimeKind.Unspecified);

    public QueryStoreHealthCaptureModePublishTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "QsCaptureModePublish_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _dataService = new LocalDataService(_duckDb);

        /* A real ServerManager with one enabled, Windows-auth server — no credential store touched. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private async Task SeedThreeShapesAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var conn = _duckDb.CreateConnection();
        await conn.OpenAsync();

        /* The churn factory: ALL, wait stats ON, sitting at 96 % of an 8 GB cap. */
        await ExecAsync(conn,
            "INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode) "
            + $"VALUES (-1, TIMESTAMP '2026-09-20 18:00:00', {_serverId}, 'TestServer', 'churny', 'READ_WRITE', 'READ_WRITE', 0, 7900, 8192, 'AUTO', 21, 200, 60, 'ALL', 'ON')");

        /* A 2016 engine: query_capture_mode exists there (AUTO), wait_stats_capture_mode does not, so the
           collector stored NULL by construction — the null that means "cannot say", never OFF. */
        await ExecAsync(conn,
            "INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode) "
            + $"VALUES (-2, TIMESTAMP '2026-09-20 18:00:00', {_serverId}, 'TestServer', 'legacy2016', 'READ_WRITE', 'READ_WRITE', 0, 100, 1000, 'AUTO', 30, 200, 60, 'AUTO', NULL)");

        /* A pre-rung row: the INSERT names neither column, exactly as a row written before v64 sits. */
        await ExecAsync(conn,
            "INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes) "
            + $"VALUES (-3, TIMESTAMP '2026-09-20 18:00:00', {_serverId}, 'TestServer', 'prerung', 'READ_WRITE', 'READ_WRITE', 0, 50, 1000, 'AUTO', 30, 200, 60)");
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// The tool, end to end over DuckDB: every database object ends with <c>query_capture_mode</c> then
    /// <c>wait_stats_capture_mode</c>, the values are the DMV spellings verbatim, and each null shape is a JSON
    /// <c>null</c> — not <c>""</c>, not an absent key, not <c>OFF</c>. The ten fields the tool carried before the
    /// rung keep their places in front (<c>database_name</c> first, <c>interval_length_minutes</c> tenth of the
    /// row's non-derived fields), and the stamp still reads as the capture, so the ordinal move left the
    /// clock where it was.
    /// </summary>
    [Fact]
    public async Task GetQueryStoreHealth_PublishesTheCaptureModes_TrailingOnEveryRow_WithNullWhereTheStoreHasNone()
    {
        await SeedThreeShapesAsync();

        var json = await McpConfigTools.GetQueryStoreHealth(_dataService, _serverManager);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("TestServer", root.GetProperty("server").GetString());
        Assert.Equal(3, root.GetProperty("database_count").GetInt32());
        Assert.Equal(CaptureTime, DateTime.Parse(root.GetProperty("captured_at").GetString()!, null, System.Globalization.DateTimeStyles.RoundtripKind));

        var rows = root.GetProperty("databases").EnumerateArray()
            .ToDictionary(d => d.GetProperty("database_name").GetString()!, d => d, StringComparer.Ordinal);
        Assert.Equal(new[] { "churny", "legacy2016", "prerung" }, rows.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray());

        Assert.Equal("ALL", rows["churny"].GetProperty("query_capture_mode").GetString());
        Assert.Equal("ON", rows["churny"].GetProperty("wait_stats_capture_mode").GetString());

        Assert.Equal("AUTO", rows["legacy2016"].GetProperty("query_capture_mode").GetString());
        Assert.Equal(JsonValueKind.Null, rows["legacy2016"].GetProperty("wait_stats_capture_mode").ValueKind);

        Assert.Equal(JsonValueKind.Null, rows["prerung"].GetProperty("query_capture_mode").ValueKind);
        Assert.Equal(JsonValueKind.Null, rows["prerung"].GetProperty("wait_stats_capture_mode").ValueKind);

        foreach (var (name, row) in rows)
        {
            var keys = row.EnumerateObject().Select(p => p.Name).ToArray();
            Assert.Equal("database_name", keys[0]);
            Assert.Equal("query_capture_mode", keys[^2]);
            Assert.Equal("wait_stats_capture_mode", keys[^1]);
            Assert.True(Array.IndexOf(keys, "interval_length_minutes") == keys.Length - 3,
                $"{name}: interval_length_minutes must be the last field before the two modes; keys were [{string.Join(", ", keys)}]");

            /* The ten original fields are untouched by the move — the cap-hit fold and the storage figures
               still read off the right ordinals. */
            Assert.True(row.GetProperty("state_matches_desired").GetBoolean());
            Assert.Equal(60, row.GetProperty("interval_length_minutes").GetInt64());
            Assert.Equal(200, row.GetProperty("max_plans_per_query").GetInt64());
        }

        Assert.Equal(7900, rows["churny"].GetProperty("current_storage_size_mb").GetInt64());
        Assert.Equal(8192, rows["churny"].GetProperty("max_storage_size_mb").GetInt64());
        Assert.Equal(96.4, rows["churny"].GetProperty("pct_of_cap").GetDouble(), 1);

        /* The database_name filter keeps the same shape on one row. */
        var one = await McpConfigTools.GetQueryStoreHealth(_dataService, _serverManager, database_name: "legacy2016");
        using var oneDoc = JsonDocument.Parse(one);
        var only = oneDoc.RootElement.GetProperty("databases").EnumerateArray().Single();
        Assert.Equal("AUTO", only.GetProperty("query_capture_mode").GetString());
        Assert.Equal(JsonValueKind.Null, only.GetProperty("wait_stats_capture_mode").ValueKind);
    }

    /// <summary>
    /// The grid's read, the same rows: the model carries the modes as nullable strings, and the two display
    /// properties the XAML binds render a null as the grid's absence glyph — never <c>""</c>, which reads like a
    /// value, and never <c>OFF</c>, which would be a claim the engine did not make.
    /// </summary>
    [Fact]
    public async Task GetLatestQueryStoreHealth_CarriesTheModes_AndTheGridRendersNullAsTheDash()
    {
        await SeedThreeShapesAsync();

        var rows = (await _dataService.GetLatestQueryStoreHealthAsync(_serverId)).ToDictionary(r => r.DatabaseName, StringComparer.Ordinal);
        Assert.Equal(3, rows.Count);

        Assert.Equal("ALL", rows["churny"].QueryCaptureMode);
        Assert.Equal("ON", rows["churny"].WaitStatsCaptureMode);
        Assert.Equal("ALL", rows["churny"].CaptureModeDisplay);
        Assert.Equal("ON", rows["churny"].WaitStatsCaptureModeDisplay);

        Assert.Equal("AUTO", rows["legacy2016"].QueryCaptureMode);
        Assert.Null(rows["legacy2016"].WaitStatsCaptureMode);
        Assert.Equal("\u2014", rows["legacy2016"].WaitStatsCaptureModeDisplay);

        Assert.Null(rows["prerung"].QueryCaptureMode);
        Assert.Null(rows["prerung"].WaitStatsCaptureMode);
        Assert.Equal("\u2014", rows["prerung"].CaptureModeDisplay);
        Assert.Equal("\u2014", rows["prerung"].WaitStatsCaptureModeDisplay);

        /* The stamp moved ordinals and still reads as the capture — the failure the ordinal shift could cause. */
        Assert.All(rows.Values, r => Assert.Equal(CaptureTime, r.CaptureTime));

        /* The database filter still binds after the SELECT grew: the clause's parameter numbering starts at $2
           regardless of how many columns the list carries. */
        var filtered = await _dataService.GetLatestQueryStoreHealthAsync(_serverId, new[] { "churny" });
        Assert.Equal("ALL", Assert.Single(filtered).QueryCaptureMode);
    }

    /// <summary>The description explains the modes — the ALL / AUTO / CUSTOM / NONE paragraph, the wait-stats
    /// cost, the null, and the consumers — on the Lite tool; the byte-equality with Darling's is pinned in
    /// <c>Darling.Tests.DarlingMcpConfigHistoryToolsSurfaceAndSqlTests</c>, which reads this file.</summary>
    [Fact]
    public void GetQueryStoreHealth_Description_ExplainsTheCaptureModes()
    {
        var description = typeof(McpConfigTools)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "get_query_store_health")
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

        foreach (var sentence in new[]
        {
            "CAPTURE MODE IS THE PLAN-CHURN KNOB",
            "ALL captures every query the engine compiles, one-off ad hoc statements included",
            "AUTO skips insignificant queries",
            "CUSTOM (2019+) is AUTO with operator-set thresholds",
            "NONE stops capturing NEW queries",
            "wait_stats_capture_mode ON (the default) records per-plan wait statistics",
            "null means the row predates the V137 rung or, for wait_stats_capture_mode, the engine is older than SQL Server 2017",
            "never OFF",
            /* #3898 D4: the issue ref that used to sit after this tool name came off the wire (head and tail);
               the sentence it named a source for is otherwise unchanged. */
            "get_query_store_clutter as its churn",
            "renders no verdict on them",
            "LATEST IS A TIME",
        })
        {
            Assert.Contains(sentence, description, StringComparison.Ordinal);
        }

        Assert.Contains("`query_capture_mode` ALL / AUTO / CUSTOM / NONE — ALL is the plan-churn factory; `wait_stats_capture_mode` ON / OFF; null = pre-rung row or pre-2017 engine", McpInstructions.Text, StringComparison.Ordinal);
    }
}
