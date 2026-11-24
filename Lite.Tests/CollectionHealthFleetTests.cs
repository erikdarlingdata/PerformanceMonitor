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
/// #6074 part 4, Lite half: <c>get_collection_health</c> with <c>server_name</c> "*" and a <c>collector_name</c> answers one row per enabled
/// server with the newest success and newest failure of that collector in the window, and a server with no run says so.
/// </summary>
public sealed class CollectionHealthFleetTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private long _nextId = 1;

    public CollectionHealthFleetTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* Best-effort cleanup */ }
    }

    private async Task RunAsync(ServerConnection server, string collector, string status, TimeSpan age, string? message = null, long rows = 0)
    {
        var serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
                            VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, 0, 0)";
        void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        P(_nextId++); P(serverId); P(server.ServerName); P(collector); P(DateTime.UtcNow - age); P(status); P(message); P(rows);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task TheFleetForm_GivesEachServersNewestSuccessAndFailure_AndNamesTheServerWithNoRun()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        var serverManager = new ServerManager(configDir);
        var a = new ServerConnection { ServerName = "fl-a", DisplayName = "Alpha" };
        var b = new ServerConnection { ServerName = "fl-b", DisplayName = "Beta" };
        var c = new ServerConnection { ServerName = "fl-c", DisplayName = "Gamma" };
        serverManager.AddServer(a);
        serverManager.AddServer(b);
        serverManager.AddServer(c);

        /* a: success then a NEWER failure; b: success only (plus a SKIPPED run, which is no failure); c: no run of this collector. */
        await RunAsync(a, "query_store", "SUCCESS", TimeSpan.FromHours(3));
        await RunAsync(a, "query_store", "PERMISSIONS", TimeSpan.FromHours(2), "older denial");
        await RunAsync(a, "query_store", "ERROR", TimeSpan.FromHours(1), "denied");
        await RunAsync(b, "query_store", "SUCCESS", TimeSpan.FromHours(2));
        await RunAsync(b, "query_store", "SKIPPED", TimeSpan.FromMinutes(5));
        await RunAsync(c, "wait_stats", "SUCCESS", TimeSpan.FromHours(1));

        var service = new LocalDataService(initializer);
        var root = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, serverManager, "*", collector_name: "query_store")).RootElement;
        var servers = root.GetProperty("servers").EnumerateArray().ToDictionary(s => s.GetProperty("server_name").GetString()!);
        Assert.Equal("fleet", root.GetProperty("scope").GetString());
        Assert.Equal("failing_since_last_success", servers["fl-a"].GetProperty("outcome").GetString());
        Assert.Equal("ERROR", servers["fl-a"].GetProperty("last_failure_status").GetString());
        Assert.Equal("denied", servers["fl-a"].GetProperty("last_failure_message").GetString());
        Assert.NotEqual(JsonValueKind.Null, servers["fl-a"].GetProperty("last_success_at").ValueKind);
        Assert.Equal("succeeding", servers["fl-b"].GetProperty("outcome").GetString());
        Assert.Equal(JsonValueKind.Null, servers["fl-b"].GetProperty("last_failure_at").ValueKind);
        Assert.Equal(JsonValueKind.Null, servers["fl-b"].GetProperty("last_failure_status").ValueKind);
        Assert.Equal(JsonValueKind.Null, servers["fl-b"].GetProperty("last_failure_message").ValueKind);
        Assert.Equal("no_run_in_window", servers["fl-c"].GetProperty("outcome").GetString());
        Assert.Equal(2, root.GetProperty("servers_with_a_run").GetInt32());
        Assert.Equal(1, root.GetProperty("servers_with_no_run").GetInt32());

        var refused = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, serverManager, "*")).RootElement;
        Assert.Equal("invalid", refused.GetProperty("status").GetString());
        var unknown = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, serverManager, "*", collector_name: "no_such_collector")).RootElement;
        Assert.Equal("invalid", unknown.GetProperty("status").GetString());
    }

    private static void AssertNear(DateTime? expected, DateTime? actual, string what)
    {
        Assert.NotNull(actual);
        Assert.True(expected is not null && (actual.Value - expected.Value).Duration() < TimeSpan.FromSeconds(30), $"{what}: expected near {expected:O}, read {actual:O}");
    }

    [Fact]
    public async Task TheFleetReads_GiveEachServersBandInputs_AsTheOneServerReadCountsThem()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        var serverManager = new ServerManager(configDir);
        var mixed = new ServerConnection { ServerName = "fl-mixed", DisplayName = "Mixed" };
        var regressed = new ServerConnection { ServerName = "fl-regressed", DisplayName = "Regressed" };
        var rest = new ServerConnection { ServerName = "fl-rest", DisplayName = "Rest" };
        serverManager.AddServer(mixed);
        serverManager.AddServer(regressed);
        serverManager.AddServer(rest);
        var id = (ServerConnection s) => RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s));
        var now = DateTime.UtcNow;

        /* mixed: two successes (one stored rows, one did not), an error, a named skip, then a SKIPPED run (not a named skip). */
        await RunAsync(mixed, "wait_stats", "SUCCESS", TimeSpan.FromHours(5), rows: 7);
        await RunAsync(mixed, "wait_stats", "SUCCESS", TimeSpan.FromHours(4));
        await RunAsync(mixed, "wait_stats", "ERROR", TimeSpan.FromHours(3), "boom");
        await RunAsync(mixed, "wait_stats", "EXTENSION_MISSING", TimeSpan.FromHours(2), "missing");
        await RunAsync(mixed, "wait_stats", "SKIPPED", TimeSpan.FromHours(1));

        /* regressed: rows 30 hours ago, then only the named skip. rest: only the named skip, and a run 8 days ago that stored rows. */
        await RunAsync(regressed, "wait_stats", "SUCCESS", TimeSpan.FromHours(30), rows: 40);
        await RunAsync(regressed, "wait_stats", "EXTENSION_MISSING", TimeSpan.FromHours(2), "missing");
        await RunAsync(regressed, "wait_stats", "EXTENSION_MISSING", TimeSpan.FromHours(1), "missing");
        await RunAsync(rest, "wait_stats", "SUCCESS", TimeSpan.FromDays(8), rows: 40);
        await RunAsync(rest, "wait_stats", "EXTENSION_MISSING", TimeSpan.FromHours(2), "missing");

        var service = new LocalDataService(initializer);
        var facts = (await service.GetCollectorFleetLivenessAsync("wait_stats", now.AddHours(-24), now)).ToDictionary(r => r.ServerId);

        /* mixed has a success and an error in the window, so it is no candidate for the longer read: its values are the window's. */
        var m = facts[id(mixed)];
        Assert.Equal(5, m.Runs);
        Assert.Equal(2, m.SuccessRuns);
        Assert.Equal(1, m.ErrorRuns);
        Assert.Equal(1, m.ExtensionMissingRuns);
        Assert.Equal(2, m.Failures);
        AssertNear(now - TimeSpan.FromHours(1), m.LastRunUtc, "mixed last run");
        AssertNear(now - TimeSpan.FromHours(1), m.LastNonSkipUtc, "mixed last non-skip (SKIPPED is not a named skip)");
        AssertNear(now - TimeSpan.FromHours(5), m.LastProductiveUtc, "mixed last productive");
        AssertNear(now - TimeSpan.FromHours(4), m.LastSuccessUtc, "mixed last success");

        /* regressed: the row's runs and success clock are the 24-hour window's, the band inputs are the 7 days'. */
        var r = facts[id(regressed)];
        Assert.Equal(2, r.Runs);
        Assert.Null(r.LastSuccessUtc);
        Assert.Equal(1, r.SuccessRuns);
        Assert.Equal(0, r.ErrorRuns);
        Assert.Equal(2, r.ExtensionMissingRuns);
        AssertNear(now - TimeSpan.FromHours(1), r.LastRunUtc, "regressed last run");
        AssertNear(now - TimeSpan.FromHours(30), r.LastNonSkipUtc, "regressed last non-skip");
        AssertNear(now - TimeSpan.FromHours(30), r.LastProductiveUtc, "regressed last productive");

        /* rest: the run 8 days ago is outside the 7 days, so it counts for nothing. */
        var q = facts[id(rest)];
        Assert.Equal(0, q.SuccessRuns);
        Assert.Equal(1, q.ExtensionMissingRuns);
        Assert.Null(q.LastNonSkipUtc);
        Assert.Null(q.LastProductiveUtc);

        /* The same data through the tool: a regression, not a setup state, and the plain rest is a setup state. */
        var root = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, serverManager, "*", collector_name: "wait_stats")).RootElement;
        var rows = root.GetProperty("servers").EnumerateArray().ToDictionary(x => x.GetProperty("server_name").GetString()!);
        Assert.True(rows["fl-regressed"].GetProperty("regressed_from_productive").GetBoolean());
        Assert.NotEqual("needs_setup", rows["fl-regressed"].GetProperty("outcome").GetString());
        Assert.Equal("needs_setup", rows["fl-rest"].GetProperty("outcome").GetString());
        Assert.False(rows["fl-rest"].GetProperty("regressed_from_productive").GetBoolean());
        Assert.Equal(1, root.GetProperty("servers_needing_setup").GetInt32());
    }

    [Fact]
    public async Task TheFleetForm_ReadsNeedsSetup_ForTheTwoWindowShapesThatMixedHoursBackWithSevenDays()
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        var serverManager = new ServerManager(configDir);
        var empty = new ServerConnection { ServerName = "fl-empty", DisplayName = "Empty" };
        var skipped = new ServerConnection { ServerName = "fl-skipped", DisplayName = "Skipped" };
        serverManager.AddServer(empty);
        serverManager.AddServer(skipped);

        /* empty (#9213): two successes that stored nothing 30 hours ago, outside the 24-hour window, then a skip every hour since. */
        await RunAsync(empty, "wait_stats", "SUCCESS", TimeSpan.FromHours(30));
        await RunAsync(empty, "wait_stats", "SUCCESS", TimeSpan.FromHours(29.5));
        for (var i = 1; i <= 28; i++)
        {
            await RunAsync(empty, "wait_stats", "EXTENSION_MISSING", TimeSpan.FromHours(i), "missing");
        }

        /* skipped (#9213): the window holds only an abandoned and a SKIPPED run, and the named skip is earlier in the 7 days. */
        await RunAsync(skipped, "wait_stats", "EXTENSION_MISSING", TimeSpan.FromHours(31), "missing");
        await RunAsync(skipped, "wait_stats", "EXTENSION_MISSING", TimeSpan.FromHours(30), "missing");
        await RunAsync(skipped, "wait_stats", "ABANDONED", TimeSpan.FromHours(2), "abandoned");
        await RunAsync(skipped, "wait_stats", "SKIPPED", TimeSpan.FromMinutes(20));

        var service = new LocalDataService(initializer);
        var root = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, serverManager, "*", collector_name: "wait_stats")).RootElement;
        var rows = root.GetProperty("servers").EnumerateArray().ToDictionary(x => x.GetProperty("server_name").GetString()!);
        Assert.Equal("needs_setup", rows["fl-empty"].GetProperty("outcome").GetString());
        Assert.Equal("needs_setup", rows["fl-skipped"].GetProperty("outcome").GetString());
        Assert.Equal(2, root.GetProperty("servers_needing_setup").GetInt32());
        Assert.Equal(0, root.GetProperty("servers_failing").GetInt32());

        /* The one-server read bands both the same way. */
        foreach (var name in new[] { "fl-empty", "fl-skipped" })
        {
            var one = JsonDocument.Parse(await McpHealthTools.GetCollectionHealth(service, serverManager, name)).RootElement;
            var collector = one.GetProperty("collectors").EnumerateArray().Single(c => c.GetProperty("collector").GetString() == "wait_stats");
            Assert.Equal("EXTENSION_MISSING", collector.GetProperty("status").GetString());
        }
    }
}
