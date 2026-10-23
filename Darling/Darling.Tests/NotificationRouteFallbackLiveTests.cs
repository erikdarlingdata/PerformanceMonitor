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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database and is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6036 against a real store: a route's <c>fallback_route_id</c> is set through the viewer's data service and the
/// <c>set_notification_route_fallback</c> tool, a self-reference and an unknown route are refused by both, deleting a route
/// clears every reference to it (the column has no foreign key), and both reads return the fallback by id and name.
/// </summary>
public sealed class NotificationRouteFallbackLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<ScratchPostgres> MintAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return scratch;
    }

    private static async Task<int> AddRouteAsync(NpgsqlDataSource source, string match, CancellationToken ct)
    {
        await using var command = source.CreateCommand(
            "INSERT INTO config.config_notification_routes (metric_match, smtp_recipients) VALUES ($1, 'ops@example.com') RETURNING route_id");
        command.Parameters.AddWithValue(match);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<int?> FallbackOfAsync(NpgsqlDataSource source, int routeId, CancellationToken ct)
    {
        await using var command = source.CreateCommand($"SELECT fallback_route_id FROM config.config_notification_routes WHERE route_id = {routeId}");
        var value = await command.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task TheViewer_StoresAFallback_RefusesSelfAndUnknown_ReadsItByIdAndName_AndDeleteClearsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the route fallback checks.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await MintAsync(ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var target = await AddRouteAsync(source, "Performance", ct);
            var routeId = await AddRouteAsync(source, "Blocking", ct);

            var row = (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == routeId);
            row.FallbackRouteId = target;
            await viewer.UpdateNotificationRouteAsync(row, ct);
            Assert.Equal(target, await FallbackOfAsync(source, routeId, ct));

            var read = (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == routeId);
            Assert.Equal(target, read.FallbackRouteId);
            Assert.Equal("Performance", read.FallbackRouteName);

            read.FallbackRouteId = routeId;
            await Assert.ThrowsAsync<ArgumentException>(() => viewer.UpdateNotificationRouteAsync(read, ct));
            read.FallbackRouteId = 99999;
            await Assert.ThrowsAsync<ArgumentException>(() => viewer.UpdateNotificationRouteAsync(read, ct));
            Assert.Equal(target, await FallbackOfAsync(source, routeId, ct));

            await viewer.DeleteNotificationRouteAsync(target, ct);
            Assert.Null(await FallbackOfAsync(source, routeId, ct));
            Assert.Single(await viewer.GetNotificationRoutesAsync(ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }

    [Fact]
    public async Task TheMcpTool_StoresAFallback_RefusesSelfAndUnknown_ReadsItByIdAndName_AndDeleteClearsIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to run the route fallback checks.");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await MintAsync(ct);
        var bodySucceeded = false;
        try
        {
            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var target = await AddRouteAsync(source, "Performance", ct);
            var routeId = await AddRouteAsync(source, "Blocking", ct);

            var set = JsonDocument.Parse(await DarlingMcpAlertTools.SetNotificationRouteFallback(source, routeId, target));
            Assert.Equal("updated", set.RootElement.GetProperty("status").GetString());
            Assert.Equal(target, await FallbackOfAsync(source, routeId, ct));

            /* #6403: the answer carries the previous fallback (none) and an undo call that, run, puts it back. */
            Assert.Equal(JsonValueKind.Null, set.RootElement.GetProperty("previous").GetProperty("fallback_route_id").ValueKind);
            var undo = set.RootElement.GetProperty("undo");
            Assert.Equal("set_notification_route_fallback", undo.GetProperty("tool").GetString());
            Assert.False(undo.GetProperty("arguments").TryGetProperty("fallback_route_id", out _));
            await DarlingMcpAlertTools.SetNotificationRouteFallback(source, undo.GetProperty("arguments").GetProperty("route_id").GetInt32());
            Assert.Null(await FallbackOfAsync(source, routeId, ct));
            var again = JsonDocument.Parse(await DarlingMcpAlertTools.SetNotificationRouteFallback(source, routeId, target));
            Assert.Equal("updated", again.RootElement.GetProperty("status").GetString());

            var off = JsonDocument.Parse(await DarlingMcpAlertTools.SetNotificationRouteEnabled(source, routeId, false)).RootElement;
            Assert.Equal("updated", off.GetProperty("status").GetString());
            Assert.True(off.GetProperty("previous").GetProperty("enabled").GetBoolean());
            Assert.Equal("set_notification_route_enabled", off.GetProperty("undo").GetProperty("tool").GetString());
            Assert.True(off.GetProperty("undo").GetProperty("arguments").GetProperty("enabled").GetBoolean());
            var back = JsonDocument.Parse(await DarlingMcpAlertTools.SetNotificationRouteEnabled(source, routeId, true)).RootElement;
            Assert.False(back.GetProperty("previous").GetProperty("enabled").GetBoolean());

            Assert.Equal("unchanged", JsonDocument.Parse(await DarlingMcpAlertTools.SetNotificationRouteFallback(source, routeId, target))
                .RootElement.GetProperty("status").GetString());

            var listed = JsonDocument.Parse(await DarlingMcpAlertTools.GetNotificationRoutes(source, ct)).RootElement
                .GetProperty("routes").EnumerateArray().Single(r => r.GetProperty("route_id").GetInt32() == routeId);
            Assert.Equal(target, listed.GetProperty("fallback_route_id").GetInt32());
            Assert.Equal("Performance", listed.GetProperty("fallback_route_name").GetString());

            Assert.Equal("invalid", JsonDocument.Parse(await DarlingMcpAlertTools.SetNotificationRouteFallback(source, routeId, routeId))
                .RootElement.GetProperty("status").GetString());
            Assert.Equal("invalid", JsonDocument.Parse(await DarlingMcpAlertTools.SetNotificationRouteFallback(source, routeId, 99999))
                .RootElement.GetProperty("status").GetString());
            Assert.Equal(target, await FallbackOfAsync(source, routeId, ct));

            await DarlingMcpAlertTools.DeleteNotificationRoute(source, target);
            Assert.Null(await FallbackOfAsync(source, routeId, ct));

            var other = await AddRouteAsync(source, "Deadlocks", ct);
            await DarlingMcpAlertTools.SetNotificationRouteFallback(source, routeId, other);
            await DarlingMcpAlertTools.SetNotificationRouteFallback(source, routeId, null);
            Assert.Null(await FallbackOfAsync(source, routeId, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
