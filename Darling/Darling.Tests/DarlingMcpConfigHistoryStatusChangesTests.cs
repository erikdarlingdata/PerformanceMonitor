/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// #7416 item 2: <c>log_reuse_wait_desc</c> is one of the 27 <c>database_config</c> columns, but it is the engine's live reason the
/// log cannot truncate and flips with no operator action. The change history lists settings only and reports those rows apart as
/// <c>status_changes</c>, so the history is not lost and a flip is not called a configuration change. The web grid reads this tool.
/// Lite twin: <c>DatabaseConfigStatusChangesTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpConfigHistoryStatusChangesLiveTests
{
    private static readonly string[] Tables = ["database_config"];

    private static string Name(string scenario) => "darling-confighist-status-" + scenario;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task RunAsync(string scenario, (int HoursAgo, string Recovery, string LogReuse)[] captures, Func<NpgsqlDataSource, DateTime, string, Task> body) =>
        WindowFloorLiveHarness.RunAsync(ConnectionString, ["database_config"], [Name(scenario)], Tables, async (c, ds, end) =>
        {
            var name = Name(scenario);
            var serverId = ServerIdHelper.GetDeterministicHashCode(name);
            await WindowFloorLiveHarness.SeedServerAsync(c, name, end.AddDays(-5), "database_config", end.AddDays(-4), 60, end.AddMinutes(-30),
                Tables, TestContext.Current.CancellationToken);
            foreach (var (hoursAgo, recovery, logReuse) in captures)
            {
                await DarlingMcpTestData.ExecAsync(c, TestContext.Current.CancellationToken,
                    "INSERT INTO database_config (config_id, capture_time, server_id, server_name, database_name, recovery_model, log_reuse_wait_desc) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(end.AddHours(-hoursAgo)), serverId, name, "Sales", recovery, logReuse);
            }

            await body(ds, end, name);
        });

    private static async Task<JsonElement> CallAsync(NpgsqlDataSource ds, string name, DateTime end) =>
        JsonDocument.Parse(await DarlingMcpConfigHistoryTools.GetDatabaseConfigChanges(ds, name, 168, WebDataStartNote.FormatWindowEnd(end))).RootElement;

    [Fact]
    public Task TheChangeList_HoldsSettingsOnly_AndTheStatusRowsAreReportedApart() =>
        RunAsync("mixed", [(30, "FULL", "NOTHING"), (20, "FULL", "ACTIVE_TRANSACTION"), (10, "SIMPLE", "ACTIVE_TRANSACTION")], async (ds, end, name) =>
        {
            var root = await CallAsync(ds, name, end);

            Assert.Equal(1, root.GetProperty("change_count").GetInt32());
            Assert.Equal("recovery_model", root.GetProperty("changes")[0].GetProperty("setting_name").GetString());
            Assert.Equal(1, root.GetProperty("status_change_count").GetInt32());
            var status = root.GetProperty("status_changes")[0];
            Assert.Equal("log_reuse_wait_desc", status.GetProperty("setting_name").GetString());
            Assert.Equal("NOTHING", status.GetProperty("old_value").GetString());
            Assert.Equal("ACTIVE_TRANSACTION", status.GetProperty("new_value").GetString());
        });

    /* The desktop viewer's Database Config Changes grid reads the snapshots itself (no tool in between). */
    [Fact]
    public Task TheDesktopGrid_ListsSettingsOnly() =>
        RunAsync("viewer", [(30, "FULL", "NOTHING"), (20, "FULL", "ACTIVE_TRANSACTION"), (10, "SIMPLE", "ACTIVE_TRANSACTION")], async (ds, end, name) =>
        {
            await using var viewer = new ViewerDataService(ConnectionString!);
            var rows = await viewer.GetDatabaseConfigChangesAsync(ServerIdHelper.GetDeterministicHashCode(name), end.AddDays(-7), end,
                cancellationToken: TestContext.Current.CancellationToken);

            Assert.Equal(new[] { "recovery_model" }, rows.Select(r => r.SettingName));
        });

    [Fact]
    public Task WhenOnlyTheStatusColumnMoved_NoSettingChangedAndTheHistoryIsKept() =>
        RunAsync("statusonly", [(30, "FULL", "NOTHING"), (20, "FULL", "ACTIVE_TRANSACTION"), (10, "FULL", "NOTHING")], async (ds, end, name) =>
        {
            var root = await CallAsync(ds, name, end);

            Assert.Equal(0, root.GetProperty("change_count").GetInt32());
            Assert.Equal(0, root.GetProperty("changes").GetArrayLength());
            Assert.Equal(2, root.GetProperty("status_change_count").GetInt32());
            Assert.All(root.GetProperty("status_changes").EnumerateArray(),
                row => Assert.Equal("log_reuse_wait_desc", row.GetProperty("setting_name").GetString()));
        });
}
