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
using Darling.Tests;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #7526: a window in which the procedure_stats collector ran and no procedure did work. With no database chosen the tool
/// answers measured zero points (the #5449 design Lite follows: an idle run is a 0 point). With a database chosen that matched
/// nothing the window still reads empty, because the chosen set had no row in it.
/// </summary>
[Collection("live-postgres")]
public sealed class ProcedureDurationTrendIdleWindowLiveTests
{
    private const string IdleCleanup =
        "DELETE FROM procedure_stats WHERE server_id = {0}; DELETE FROM collect.collection_log WHERE server_id = {0}";

    [Fact]
    public Task AnIdleWindow_WithNoDatabaseChosen_ReadsZeroPoints_AndAChosenDatabaseThatMatchedNothingReadsEmpty() =>
        DurationTrendDatabaseFilterLiveTests.WithSharedStoreAsync("trend-idle-window-7526", async (connection, postgres, serverId, serverName, now, ct) =>
        {
            /* The collector's runs every minute for the last three hours, each logged SUCCESS with no rows stored: runs and no
               procedure_stats row anywhere in the window. */
            await DarlingMcpTestData.ExecAsync(
                connection, ct,
                "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
                + "SELECT 7526000 + row_number() OVER (), $1, $2, 'procedure_stats', t + interval '3 seconds', 3000, 'SUCCESS', 0 "
                + "FROM generate_series($3::timestamp, $4::timestamp, interval '1 minute') AS t",
                serverId, serverName, DateTime.SpecifyKind(now.AddHours(-3), DateTimeKind.Unspecified), DateTime.SpecifyKind(now.AddMinutes(-1), DateTimeKind.Unspecified));

            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);

            var all = await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, serverName, 24, null, null, DatabaseFilter.All, budget, ct);
            var root = JsonDocument.Parse(all).RootElement;
            Assert.True(root.TryGetProperty("trend", out var trend), "an idle window with no database chosen must carry points, not the empty answer: " + all);
            var points = trend.EnumerateArray().ToList();
            Assert.NotEmpty(points);
            /* Zero is a measurement: the rated points are 0 ms of work per second, and none reads any work. */
            Assert.Contains(points, p => p.GetProperty("elapsed_ms_per_second") is { ValueKind: JsonValueKind.Number } n && n.GetDouble() == 0.0);
            Assert.DoesNotContain(points, p => p.GetProperty("elapsed_ms_per_second") is { ValueKind: JsonValueKind.Number } n && n.GetDouble() != 0.0);

            var chosen = await DarlingMcpTrendTools.GetProcedureDurationTrend(postgres, serverName, 24, null, null, DatabaseFilter.One("NoSuchDatabase"), budget, ct);
            Assert.Equal("empty", DarlingMcpTestData.StatusOf(chosen));
        }, IdleCleanup);
}
