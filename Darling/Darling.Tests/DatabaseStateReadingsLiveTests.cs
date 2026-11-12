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
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7662: a <c>state_desc</c> change takes the minute <c>database_states</c> first saw the new state instead of the next config snapshot.
/// These run the shared read (<see cref="DatabaseStateReadings.Sql"/>: the text[] name binding and the naive-UTC bounds) against a live
/// store, through the web tool and the desktop grid that both call it, and check a re-dated row is judged against the window.
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseStateReadingsLiveTests
{
    private static readonly string[] Tables = ["database_config", "database_states"];

    private static string Name(string scenario) => "darling-statetime-" + scenario;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>Two databases whose config snapshots at -40 h and -2 h show a state change (Sales: OFFLINE to ONLINE, Orders: ONLINE to
    /// OFFLINE). database_states first saw Sales ONLINE at -30 h and Orders OFFLINE at -10 h, with an earlier reading of the old state each.</summary>
    private static Task RunAsync(string scenario, Func<NpgsqlDataSource, DateTime, string, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, ["database_config"], [Name(scenario)], Tables, async (c, ds, end) =>
        {
            var name = Name(scenario);
            var serverId = ServerIdHelper.GetDeterministicHashCode(name);
            var ct = TestContext.Current.CancellationToken;
            await WindowFloorLiveHarness.SeedServerAsync(c, name, end.AddDays(-5), "database_config", end.AddDays(-4), 60, end.AddMinutes(-30), Tables, ct);
            foreach (var (database, hoursAgo, state) in new[]
            {
                ("Sales", 40, "OFFLINE"), ("Sales", 2, "ONLINE"), ("Orders", 40, "ONLINE"), ("Orders", 2, "OFFLINE"),
            })
            {
                await DarlingMcpTestData.ExecAsync(c, ct,
                    "INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, state_desc) VALUES ($1,$2,$3,$4,$5,$6)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddHours(-hoursAgo)), serverId, name, database, state);
            }

            foreach (var (database, hoursAgo, state) in new[]
            {
                ("Sales", 35, "OFFLINE"), ("Sales", 30, "ONLINE"), ("Sales", 29, "ONLINE"),
                ("Orders", 12, "ONLINE"), ("Orders", 10, "OFFLINE"), ("Orders", 9, "OFFLINE"),
            })
            {
                await DarlingMcpTestData.ExecAsync(c, ct,
                    "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,5,$6,FALSE)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddHours(-hoursAgo)), serverId, name, database, state);
            }

            await body(ds, end, name);
        });

    private static async Task<Dictionary<string, DateTime>> ToolStateTimesAsync(NpgsqlDataSource ds, string name, DateTime end, int hoursBack)
    {
        var root = JsonDocument.Parse(await DarlingMcpConfigHistoryTools.GetDatabaseConfigChanges(ds, name, hoursBack, WebDataStartNote.FormatWindowEnd(end))).RootElement;
        var all = root.GetProperty("changes").EnumerateArray().Concat(root.GetProperty("status_changes").EnumerateArray()).ToList();
        return all.Where(c => c.GetProperty("setting_name").GetString() == "state_desc")
            .ToDictionary(c => c.GetProperty("database_name").GetString()!, c => DateTime.Parse(c.GetProperty("change_time").GetString()!));
    }

    [Fact]
    public Task TheTool_DatesEachStateChangeByTheMinuteTheStateWasSeen() =>
        RunAsync("wide", async (ds, end, name) =>
        {
            var times = await ToolStateTimesAsync(ds, name, end, 168);

            Assert.Equal(2, times.Count);
            Assert.Equal(Naive(end.AddHours(-30)), Naive(times["Sales"]));
            Assert.Equal(Naive(end.AddHours(-10)), Naive(times["Orders"]));
        });

    [Fact]
    public Task TheTool_DropsAStateChangeSeenBeforeTheWindow_AndKeepsOneSeenInside() =>
        RunAsync("narrow", async (ds, end, name) =>
        {
            var times = await ToolStateTimesAsync(ds, name, end, 24);

            var kept = Assert.Single(times);
            Assert.Equal("Orders", kept.Key);
            Assert.Equal(Naive(end.AddHours(-10)), Naive(kept.Value));
        });

    [Fact]
    public Task TheDesktopGrid_DatesAndWindowsTheSameWay() =>
        RunAsync("viewer", async (ds, end, name) =>
        {
            await using var viewer = new ViewerDataService(ConnectionString!);
            var serverId = ServerIdHelper.GetDeterministicHashCode(name);

            var narrow = await viewer.GetDatabaseConfigChangesAsync(serverId, end.AddHours(-24), end, cancellationToken: TestContext.Current.CancellationToken);
            var orders = Assert.Single(narrow, r => r.SettingName == "state_desc");
            Assert.Equal("Orders", orders.DatabaseName);
            Assert.Equal(Naive(end.AddHours(-10)), Naive(orders.ChangeTime));

            var wide = await viewer.GetDatabaseConfigChangesAsync(serverId, end.AddDays(-7), end, cancellationToken: TestContext.Current.CancellationToken);
            Assert.Equal(Naive(end.AddHours(-30)), Naive(wide.Single(r => r.SettingName == "state_desc" && r.DatabaseName == "Sales").ChangeTime));
        });

    /// <summary>The shared read alone: only the named databases, only after the lower bound and up to the upper one, oldest first.</summary>
    [Fact]
    public Task TheSharedRead_BindsTheNamesAndBounds() =>
        RunAsync("read", async (ds, end, name) =>
        {
            var serverId = ServerIdHelper.GetDeterministicHashCode(name);
            var change = new ConfigChangeDiff.DatabaseConfigChange(end.AddHours(-2), "Sales", "state_desc", "OFFLINE", "ONLINE", end.AddHours(-31));

            var rows = await DatabaseStateReadings.ReadAsync(ds, serverId, [change], 30, TestContext.Current.CancellationToken);

            Assert.All(rows, r => Assert.Equal("Sales", r.DatabaseName));
            Assert.Equal([Naive(end.AddHours(-30)), Naive(end.AddHours(-29))], rows.Select(r => Naive(r.Time)).ToArray());
            Assert.Empty(await DatabaseStateReadings.ReadAsync(ds, serverId,
                [change with { SettingName = "recovery_model" }], 30, TestContext.Current.CancellationToken));
        });

    private static DateTime Naive(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
}
