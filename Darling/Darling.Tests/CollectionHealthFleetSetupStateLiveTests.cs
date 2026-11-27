/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #9202 (gated, DARLING_TEST_PG): the fleet per-collector form of <c>get_collection_health</c> (<c>server_name</c> "*" with a
/// <c>collector_name</c>) bands a server whose runs are all EXTENSION_MISSING the way the one-server read and the fleet overview do
/// (a setup state, not a failure), tells that rest from a regression, and stays inside the response budget on a large fleet.
/// </summary>
public sealed class CollectionHealthFleetSetupStateLiveTests
{
    private const string BaseSkip = "Set DARLING_TEST_PG to a Postgres connection string to run the fleet collection-health setup-state live tests.";
    private const string Collector = "wait_stats";

    private static Task RunAsync(
        NpgsqlConnection connection, int serverId, string serverName, string status, TimeSpan age, long rows, string? message, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected) VALUES ($1,$2,$3,$4,$5,0,$6,$7,$8)",
            CollectionIdGenerator.Next(), serverId, serverName, Collector, DarlingMcpTestData.Naive(DateTime.UtcNow - age), status, message, rows);

    private static async Task<int> RegisterAsync(NpgsqlConnection connection, string name, CancellationToken ct)
    {
        var id = ServerIdHelper.GetDeterministicHashCode(name);
        await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        return id;
    }

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection, NpgsqlDataSource DataSource)> OpenAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), BaseSkip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return (scratch, connection, NpgsqlDataSource.Create(scratch.ConnectionString));
    }

    /* The one-server read leaves regressed_from_productive off a compact row that is not regressed. */
    private static bool RegressedOn(JsonElement row) => row.TryGetProperty("regressed_from_productive", out var flag) && flag.GetBoolean();

    private static Dictionary<string, JsonElement> RowsByServer(JsonElement root) =>
        root.GetProperty("servers").EnumerateArray().ToDictionary(s => s.GetProperty("server_name").GetString()!);

    [Fact]
    public async Task AnExtensionMissingOnlyServer_ReadsNeedsSetup_NeverFailing_AndIsCountedApartFromFailures()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, dataSource) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var _d = dataSource;

        var never = await RegisterAsync(connection, "su-never", ct);
        var emptySuccesses = await RegisterAsync(connection, "su-empty", ct);
        var erroring = await RegisterAsync(connection, "su-error", ct);
        var healthy = await RegisterAsync(connection, "su-healthy", ct);

        /* Never produced here: the extension has been absent for every run in the window. */
        for (var i = 1; i <= 4; i++)
        {
            await RunAsync(connection, never, "su-never", "EXTENSION_MISSING", TimeSpan.FromHours(i), 0, "extension \"pg_wait_sampling\" is not installed", ct);
        }

        /* The #9101 shape: runs that stored nothing while the extension was absent, then the named skip for the same state. */
        await RunAsync(connection, emptySuccesses, "su-empty", "SUCCESS", TimeSpan.FromHours(6), 0, null, ct);
        await RunAsync(connection, emptySuccesses, "su-empty", "SUCCESS", TimeSpan.FromHours(5), 0, null, ct);
        await RunAsync(connection, emptySuccesses, "su-empty", "EXTENSION_MISSING", TimeSpan.FromHours(2), 0, "extension \"pg_wait_sampling\" is not installed", ct);
        await RunAsync(connection, emptySuccesses, "su-empty", "EXTENSION_MISSING", TimeSpan.FromHours(1), 0, "extension \"pg_wait_sampling\" is not installed", ct);

        await RunAsync(connection, erroring, "su-error", "ERROR", TimeSpan.FromHours(2), 0, "boom", ct);
        await RunAsync(connection, healthy, "su-healthy", "SUCCESS", TimeSpan.FromMinutes(10), 5, null, ct);

        var root = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: Collector, cancellationToken: ct)).RootElement;
        var rows = RowsByServer(root);

        Assert.Equal("needs_setup", rows["su-never"].GetProperty("outcome").GetString());
        Assert.Equal("needs_setup", rows["su-empty"].GetProperty("outcome").GetString());
        Assert.Equal("failing", rows["su-error"].GetProperty("outcome").GetString());
        Assert.Equal("succeeding", rows["su-healthy"].GetProperty("outcome").GetString());
        Assert.False(rows["su-never"].GetProperty("regressed_from_productive").GetBoolean());
        Assert.False(rows["su-empty"].GetProperty("regressed_from_productive").GetBoolean());
        Assert.Equal(2, root.GetProperty("servers_needing_setup").GetInt32());
        Assert.Equal(1, root.GetProperty("servers_failing").GetInt32());
        Assert.False(root.GetProperty("budget_truncated").GetBoolean());
    }

    [Fact]
    public async Task ACollectorThatProducedAndNowReportsExtensionMissing_ReadsAsARegression_NotAsRest()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, dataSource) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var _d = dataSource;

        var regressed = await RegisterAsync(connection, "su-regressed", ct);
        await RunAsync(connection, regressed, "su-regressed", "SUCCESS", TimeSpan.FromHours(8), 40, null, ct);
        await RunAsync(connection, regressed, "su-regressed", "SUCCESS", TimeSpan.FromHours(7), 40, null, ct);
        await RunAsync(connection, regressed, "su-regressed", "EXTENSION_MISSING", TimeSpan.FromHours(2), 0, "extension \"pg_wait_sampling\" is not installed", ct);
        await RunAsync(connection, regressed, "su-regressed", "EXTENSION_MISSING", TimeSpan.FromHours(1), 0, "extension \"pg_wait_sampling\" is not installed", ct);

        var root = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: Collector, cancellationToken: ct)).RootElement;
        var row = RowsByServer(root)["su-regressed"];

        Assert.NotEqual("needs_setup", row.GetProperty("outcome").GetString());
        Assert.Equal("failing_since_last_success", row.GetProperty("outcome").GetString());
        Assert.True(row.GetProperty("regressed_from_productive").GetBoolean());
        Assert.Equal(0, root.GetProperty("servers_needing_setup").GetInt32());
        Assert.Equal(1, root.GetProperty("servers_failing").GetInt32());
    }

    [Fact]
    public async Task AProductiveRunOlderThanHoursBackButInsideSevenDays_StillReadsAsARegression_OnTheFleetFormAndTheOneServerRead()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, dataSource) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var _d = dataSource;

        const string skipText = "extension \"pg_wait_sampling\" is not installed";
        var regressed = await RegisterAsync(connection, "su-win-regressed", ct);
        var neverProduced = await RegisterAsync(connection, "su-win-never", ct);
        var tooOld = await RegisterAsync(connection, "su-win-old", ct);

        /* Rows 30 hours ago, then only the named skip: outside the default 24 hours, inside the 7 days the one-server read bands over. */
        await RunAsync(connection, regressed, "su-win-regressed", "SUCCESS", TimeSpan.FromHours(30), 40, null, ct);
        for (var i = 1; i <= 3; i++)
        {
            await RunAsync(connection, regressed, "su-win-regressed", "EXTENSION_MISSING", TimeSpan.FromMinutes(i * 10), 0, skipText, ct);
        }

        for (var i = 1; i <= 3; i++)
        {
            await RunAsync(connection, neverProduced, "su-win-never", "EXTENSION_MISSING", TimeSpan.FromMinutes(i * 10), 0, skipText, ct);
        }

        /* Rows 8 days ago are outside the one-server read's window too, so this one has never produced as far as either read can see. */
        await RunAsync(connection, tooOld, "su-win-old", "SUCCESS", TimeSpan.FromDays(8), 40, null, ct);
        for (var i = 1; i <= 3; i++)
        {
            await RunAsync(connection, tooOld, "su-win-old", "EXTENSION_MISSING", TimeSpan.FromMinutes(i * 10), 0, skipText, ct);
        }

        async Task<JsonElement> OneServerAsync(string name)
        {
            var one = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, name, cancellationToken: ct)).RootElement;
            return one.GetProperty("collectors").EnumerateArray().Single(c => c.TryGetProperty("collector", out var n) && n.GetString() == Collector).Clone();
        }

        var oneRegressed = await OneServerAsync("su-win-regressed");
        var oneNever = await OneServerAsync("su-win-never");
        var oneOld = await OneServerAsync("su-win-old");
        Assert.True(RegressedOn(oneRegressed));
        Assert.NotEqual("EXTENSION_MISSING", oneRegressed.GetProperty("status").GetString());
        Assert.Equal("EXTENSION_MISSING", oneNever.GetProperty("status").GetString());
        Assert.Equal("EXTENSION_MISSING", oneOld.GetProperty("status").GetString());

        /* The default window and a one-hour one: the fleet form answers as the one-server read does on both. */
        foreach (int? hoursBack in new int?[] { null, 1 })
        {
            var root = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: Collector, hours_back: hoursBack, cancellationToken: ct)).RootElement;
            var rows = RowsByServer(root);
            var label = $"hours_back {hoursBack?.ToString() ?? "default"}";

            Assert.True(rows["su-win-regressed"].GetProperty("regressed_from_productive").GetBoolean(), label);
            Assert.NotEqual("needs_setup", rows["su-win-regressed"].GetProperty("outcome").GetString());
            Assert.Equal("needs_setup", rows["su-win-never"].GetProperty("outcome").GetString());
            Assert.False(rows["su-win-never"].GetProperty("regressed_from_productive").GetBoolean(), label);
            Assert.Equal("needs_setup", rows["su-win-old"].GetProperty("outcome").GetString());
            Assert.False(rows["su-win-old"].GetProperty("regressed_from_productive").GetBoolean(), label);
            Assert.Equal(2, root.GetProperty("servers_needing_setup").GetInt32());
            Assert.Equal(1, root.GetProperty("servers_failing").GetInt32());
        }
    }

    [Fact]
    public async Task EmptySuccessesOlderThanHoursBack_ThenOnlyExtensionMissing_ReadNeedsSetup_AsTheOneServerReadDoes()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, dataSource) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var _d = dataSource;

        const string skipText = "extension \"pg_wait_sampling\" is not installed";
        var id = await RegisterAsync(connection, "su-win-empty", ct);

        /* Two successes that stored nothing 30 hours ago (outside the default 24 hours, inside the 7 days), then a skip every hour since (#9213). */
        await RunAsync(connection, id, "su-win-empty", "SUCCESS", TimeSpan.FromHours(30), 0, null, ct);
        await RunAsync(connection, id, "su-win-empty", "SUCCESS", TimeSpan.FromHours(29.5), 0, null, ct);
        for (var i = 1; i <= 28; i++)
        {
            await RunAsync(connection, id, "su-win-empty", "EXTENSION_MISSING", TimeSpan.FromHours(i), 0, skipText, ct);
        }

        var one = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "su-win-empty", cancellationToken: ct)).RootElement;
        var oneServer = one.GetProperty("collectors").EnumerateArray().Single(c => c.TryGetProperty("collector", out var n) && n.GetString() == Collector);
        Assert.Equal("EXTENSION_MISSING", oneServer.GetProperty("status").GetString());

        var root = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: Collector, cancellationToken: ct)).RootElement;
        var row = RowsByServer(root)["su-win-empty"];
        Assert.Equal("needs_setup", row.GetProperty("outcome").GetString());
        Assert.Equal(1, root.GetProperty("servers_needing_setup").GetInt32());
        Assert.Equal(0, root.GetProperty("servers_failing").GetInt32());
    }

    [Fact]
    public async Task AWindowOfOnlyNonSuccessSkips_WithExtensionMissingEarlierInTheSevenDays_ReadsNeedsSetup_AsTheOneServerReadDoes()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, dataSource) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var _d = dataSource;

        const string skipText = "extension \"pg_wait_sampling\" is not installed";
        var id = await RegisterAsync(connection, "su-win-skipped", ct);

        /* Inside 24 hours only SKIPPED and ABANDONED runs, so no error, no skip that names the extension and no success. The skip came earlier in the 7 days (#9213). */
        await RunAsync(connection, id, "su-win-skipped", "EXTENSION_MISSING", TimeSpan.FromHours(31), 0, skipText, ct);
        await RunAsync(connection, id, "su-win-skipped", "EXTENSION_MISSING", TimeSpan.FromHours(30), 0, skipText, ct);
        await RunAsync(connection, id, "su-win-skipped", "ABANDONED", TimeSpan.FromHours(2), 0, "abandoned", ct);
        await RunAsync(connection, id, "su-win-skipped", "SKIPPED", TimeSpan.FromMinutes(20), 0, null, ct);

        var one = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "su-win-skipped", cancellationToken: ct)).RootElement;
        var oneServer = one.GetProperty("collectors").EnumerateArray().Single(c => c.TryGetProperty("collector", out var n) && n.GetString() == Collector);
        var oneStatus = oneServer.GetProperty("status").GetString();

        var root = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: Collector, cancellationToken: ct)).RootElement;
        var row = RowsByServer(root)["su-win-skipped"];
        Assert.Equal("EXTENSION_MISSING", oneStatus);
        Assert.Equal("needs_setup", row.GetProperty("outcome").GetString());
        Assert.Equal(1, root.GetProperty("servers_needing_setup").GetInt32());
        Assert.Equal(0, root.GetProperty("servers_failing").GetInt32());
    }

    [Fact]
    public async Task TheRunsAndFailuresOnARow_StayOnHoursBack_WhileTheBandReadsSevenDays()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, dataSource) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var _d = dataSource;

        var id = await RegisterAsync(connection, "su-win-counts", ct);
        await RunAsync(connection, id, "su-win-counts", "SUCCESS", TimeSpan.FromHours(30), 40, null, ct);
        await RunAsync(connection, id, "su-win-counts", "EXTENSION_MISSING", TimeSpan.FromHours(2), 0, "extension \"pg_wait_sampling\" is not installed", ct);
        await RunAsync(connection, id, "su-win-counts", "EXTENSION_MISSING", TimeSpan.FromHours(1), 0, "extension \"pg_wait_sampling\" is not installed", ct);

        var root = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: Collector, cancellationToken: ct)).RootElement;
        var row = RowsByServer(root)["su-win-counts"];

        Assert.Equal(2, row.GetProperty("runs").GetInt64());
        Assert.Equal(2, row.GetProperty("failures").GetInt64());
        Assert.Equal(JsonValueKind.Null, row.GetProperty("last_success_at").ValueKind);
        Assert.True(row.GetProperty("regressed_from_productive").GetBoolean());
    }

    [Fact]
    public async Task ALargeFleetWithALongMessage_StaysUnderTheResponseBudget_AndSaysItWasCut()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, dataSource) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var _d = dataSource;

        const int fleet = 150;
        var message = "extension \"pg_wait_sampling\" is not installed: " + new string('x', 253);
        Assert.Equal(300, message.Length);
        for (var i = 0; i < fleet; i++)
        {
            var name = $"su-big-{i:D3}";
            var id = await RegisterAsync(connection, name, ct);
            await RunAsync(connection, id, name, "EXTENSION_MISSING", TimeSpan.FromMinutes(30 + i), 0, message, ct);
        }

        var json = await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: Collector, cancellationToken: ct);
        var root = JsonDocument.Parse(json).RootElement;
        var bytes = Encoding.UTF8.GetByteCount(json);

        Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"the fleet form answered {bytes} bytes");
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        Assert.False(string.IsNullOrEmpty(root.GetProperty("budget_note").GetString()));
        var returned = root.GetProperty("servers").GetArrayLength();
        Assert.InRange(returned, 1, fleet - 1);
        Assert.All(root.GetProperty("servers").EnumerateArray(), s => Assert.Equal("needs_setup", s.GetProperty("outcome").GetString()));

        /* The counts at the top are over every server, whatever the page kept. */
        Assert.Equal(fleet, root.GetProperty("servers_with_a_run").GetInt32());
        Assert.Equal(fleet, root.GetProperty("servers_needing_setup").GetInt32());
        Assert.Equal(0, root.GetProperty("servers_failing").GetInt32());
    }

    [Fact]
    public async Task TheOverviewTheOneServerReadAndTheFleetForm_GiveOneAnswerForTheSameData()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, dataSource) = await OpenAsync(ct);
        await using var _s = scratch;
        await using var _c = connection;
        await using var _d = dataSource;

        var rest = await RegisterAsync(connection, "su-agree-rest", ct);
        var regressed = await RegisterAsync(connection, "su-agree-regressed", ct);
        for (var i = 1; i <= 3; i++)
        {
            await RunAsync(connection, rest, "su-agree-rest", "EXTENSION_MISSING", TimeSpan.FromMinutes(i * 20), 0, "extension \"pg_wait_sampling\" is not installed", ct);
        }

        await RunAsync(connection, regressed, "su-agree-regressed", "SUCCESS", TimeSpan.FromHours(8), 40, null, ct);
        await RunAsync(connection, regressed, "su-agree-regressed", "EXTENSION_MISSING", TimeSpan.FromMinutes(40), 0, "extension \"pg_wait_sampling\" is not installed", ct);
        await RunAsync(connection, regressed, "su-agree-regressed", "EXTENSION_MISSING", TimeSpan.FromMinutes(20), 0, "extension \"pg_wait_sampling\" is not installed", ct);

        /* The fleet form. */
        var fleetRoot = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, "*", collector_name: Collector, cancellationToken: ct)).RootElement;
        var fleetRows = RowsByServer(fleetRoot);

        /* The one-server read. */
        async Task<JsonElement> OneServerAsync(string name)
        {
            var one = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(dataSource, name, cancellationToken: ct)).RootElement;
            Assert.True(one.TryGetProperty("collectors", out var collectors), one.ToString()[..Math.Min(400, one.ToString().Length)]);
            return collectors.EnumerateArray().Single(c => c.TryGetProperty("collector", out var name) && name.GetString() == Collector).Clone();
        }

        var oneRest = await OneServerAsync("su-agree-rest");
        var oneRegressed = await OneServerAsync("su-agree-regressed");

        /* The overview. */
        var nowUtc = DateTime.UtcNow;
        var overview = await DarlingFleetReader.GetFleetOverviewAsync(dataSource, nowUtc.AddHours(-24), nowUtc, nowUtc, cancellationToken: ct);
        var overviewRest = overview.Cards.Single(c => c.ServerName == "su-agree-rest");
        var overviewRegressed = overview.Cards.Single(c => c.ServerName == "su-agree-regressed");

        /* Rest: a setup state on all three surfaces, failing on none. */
        Assert.Equal("EXTENSION_MISSING", oneRest.GetProperty("status").GetString());
        Assert.Equal("needs_setup", fleetRows["su-agree-rest"].GetProperty("outcome").GetString());
        Assert.Equal(0, overviewRest.FailedCollectorCount);
        Assert.Equal(1, overviewRest.NoPermissionCollectorCount);
        Assert.False(RegressedOn(oneRest));
        Assert.False(fleetRows["su-agree-rest"].GetProperty("regressed_from_productive").GetBoolean());

        /* Regression: all three surfaces say it regressed, and none reads it as a setup state. */
        Assert.NotEqual("EXTENSION_MISSING", oneRegressed.GetProperty("status").GetString());
        Assert.NotEqual("needs_setup", fleetRows["su-agree-regressed"].GetProperty("outcome").GetString());
        Assert.True(RegressedOn(oneRegressed));
        Assert.True(fleetRows["su-agree-regressed"].GetProperty("regressed_from_productive").GetBoolean());
        Assert.Equal(1, overviewRegressed.RegressedCollectorCount);
        Assert.Equal(0, overviewRegressed.NoPermissionCollectorCount);
    }
}
