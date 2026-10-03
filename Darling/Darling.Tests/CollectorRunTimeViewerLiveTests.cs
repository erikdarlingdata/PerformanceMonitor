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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Collector Schedules window's "Run at" column against a real store (#4938): the read, the save and the clear,
/// for the fleet row and for a server's row, driven the way the window drives them (read the rows, build the editable
/// schedule, edit a cell, validate, save). What the service then resolves from the stored rows is checked with its own
/// <see cref="StoreConfigProvider.ResolveSchedule"/>, so "None on a server over a fleet time" is judged by the rule
/// the collectors run on and not by a restatement of it.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class CollectorRunTimeViewerLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string SkipText = "Set DARLING_TEST_PG to a Postgres connection string to run the run-time column's viewer pins (each mints its own scratch database).";

    private const string Daily = "index_object_stats";

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    /// <summary>The stored run time of the collector's row, or null for a NULL column or no row.</summary>
    private static async Task<int?> StoredRunAtAsync(NpgsqlConnection connection, int? serverId, CancellationToken ct)
    {
        var scope = serverId is int id ? $"server_id = {id}" : "server_id IS NULL";
        var value = await ScalarAsync(connection,
            $"SELECT run_at_minute FROM config.config_collector_schedules WHERE {scope} AND collector_name = '{Daily}'", ct);
        return value is null or DBNull ? null : Convert.ToInt32(value);
    }

    private static async Task<long> RowCountAsync(NpgsqlConnection connection, int? serverId, CancellationToken ct)
    {
        var scope = serverId is int id ? $"server_id = {id}" : "server_id IS NULL";
        return Convert.ToInt64(await ScalarAsync(connection,
            $"SELECT COUNT(*) FROM config.config_collector_schedules WHERE {scope} AND collector_name = '{Daily}'", ct));
    }

    /// <summary>One save through the window's own steps: read the rows, build the editable schedule for the scope, let
    /// <paramref name="edit"/> change cells, validate, and write the scope back.</summary>
    private static async Task SaveAsync(
        ViewerDataService viewer, int? serverId, Action<List<CollectorScheduleEditItem>> edit, CancellationToken ct)
    {
        var rows = await viewer.GetCollectorSchedulesAsync(ct);
        var editing = CollectorScheduleOverlay.BuildEffectiveSchedule(rows, serverId);
        edit(editing);
        Assert.True(CollectorScheduleOverlay.ValidateSchedule(editing, out var error), error);

        if (serverId is int id)
        {
            await viewer.ReplaceServerSchedulesAsync(id, CollectorScheduleOverlay.ToServerOverrideRows(editing, id), ct);
        }
        else
        {
            await viewer.ReplaceFleetSchedulesAsync(CollectorScheduleOverlay.ToFleetOverrideRows(editing), ct);
        }
    }

    private static void SetRunAt(List<CollectorScheduleEditItem> editing, string text) =>
        editing.Single(i => i.Name == Daily).RunAtText = text;

    /// <summary>What a collector with this run time on this server would do, through the service's own resolver.</summary>
    private static async Task<int?> ResolvedRunAtAsync(ViewerDataService viewer, int serverId, CancellationToken ct)
    {
        var overrides = (await viewer.GetCollectorSchedulesAsync(ct))
            .Select(r => new ScheduleOverride(r.ServerId, r.CollectorName, r.FrequencyMinutes, r.RetentionDays, r.Enabled, r.Databases, r.RunAtMinute))
            .ToList();
        return StoreConfigProvider.ResolveSchedule(Daily, serverId, overrides).RunAtMinute;
    }

    [Fact]
    public async Task TheFleetRow_SetsAndKeepsAndClearsARunTime_ThroughTheWindowsOwnSteps()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* Nothing stored: the cell reads "Use default" and no row exists. */
            Assert.Equal(0, await RowCountAsync(connection, null, ct));
            await SaveAsync(viewer, null, editing => Assert.Equal("Use default", editing.Single(i => i.Name == Daily).RunAtText), ct);
            Assert.Equal(0, await RowCountAsync(connection, null, ct));

            /* SET: a fleet run time on a collector left at its default cadence still writes its row. */
            await SaveAsync(viewer, null, editing => SetRunAt(editing, "02:00"), ct);
            Assert.Equal(120, await StoredRunAtAsync(connection, null, ct));
            var read = Assert.Single(await viewer.GetCollectorSchedulesAsync(ct), r => r.CollectorName == Daily);
            Assert.Null(read.ServerId);
            Assert.Equal(120, read.RunAtMinute);

            /* KEEP: the window reads the stored time into the cell, and a save that edits something else carries it. */
            await SaveAsync(viewer, null, editing =>
            {
                Assert.Equal("02:00", editing.Single(i => i.Name == Daily).RunAtText);
                editing.Single(i => i.Name == "wait_stats").Enabled = false;
            }, ct);
            Assert.Equal(120, await StoredRunAtAsync(connection, null, ct));
            Assert.Equal(false, await ScalarAsync(connection,
                "SELECT enabled FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = 'wait_stats'", ct));

            /* EDIT: a new time replaces the old one. */
            await SaveAsync(viewer, null, editing => SetRunAt(editing, "23:59"), ct);
            Assert.Equal(1439, await StoredRunAtAsync(connection, null, ct));

            /* CLEAR: "Use default" removes it. The fleet row of a collector at its defaults is not stored at all. */
            await SaveAsync(viewer, null, editing => SetRunAt(editing, "Use default"), ct);
            Assert.Null(await StoredRunAtAsync(connection, null, ct));
            Assert.Equal(0, await RowCountAsync(connection, null, ct));

            /* A fleet row that has other overrides keeps its row and stores NULL for a cleared time. */
            await SaveAsync(viewer, null, editing =>
            {
                var item = editing.Single(i => i.Name == Daily);
                item.FrequencyMinutes = 2880;
                item.RunAtText = "02:00";
            }, ct);
            Assert.Equal(120, await StoredRunAtAsync(connection, null, ct));
            await SaveAsync(viewer, null, editing => SetRunAt(editing, ""), ct);
            Assert.Equal(1, await RowCountAsync(connection, null, ct));
            Assert.Null(await StoredRunAtAsync(connection, null, ct));
            Assert.Equal(2880, Convert.ToInt32(await ScalarAsync(connection,
                $"SELECT frequency_minutes FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = '{Daily}'", ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AServerRow_UsesDefaultNoneOrItsOwnTime_AndNoneStopsAFleetTime()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            const int server = 7;
            const int otherServer = 8;

            await SaveAsync(viewer, null, editing => SetRunAt(editing, "02:00"), ct);

            /* USE DEFAULT: a server that customizes without touching the cell keeps NULL, so it falls through to the
               fleet's time and follows a later change of it. The cell never took the fleet's value on. */
            await SaveAsync(viewer, server, editing => Assert.Equal("Use default", editing.Single(i => i.Name == Daily).RunAtText), ct);
            Assert.Equal(1, await RowCountAsync(connection, server, ct));
            Assert.Null(await StoredRunAtAsync(connection, server, ct));
            Assert.Equal(120, await ResolvedRunAtAsync(viewer, server, ct));

            await SaveAsync(viewer, null, editing => SetRunAt(editing, "03:00"), ct);
            Assert.Equal(180, await ResolvedRunAtAsync(viewer, server, ct));
            await SaveAsync(viewer, null, editing => SetRunAt(editing, "02:00"), ct);

            /* NONE: -1 on the server's row stops the fleet's time on that server only. */
            await SaveAsync(viewer, server, editing => SetRunAt(editing, "None"), ct);
            Assert.Equal(-1, await StoredRunAtAsync(connection, server, ct));
            Assert.Null(await ResolvedRunAtAsync(viewer, server, ct));
            Assert.Equal(120, await ResolvedRunAtAsync(viewer, otherServer, ct));
            Assert.Equal(120, await StoredRunAtAsync(connection, null, ct));

            /* KEEP: the cell shows None, and a save that edits something else carries -1 through. */
            await SaveAsync(viewer, server, editing =>
            {
                Assert.Equal("None", editing.Single(i => i.Name == Daily).RunAtText);
                editing.Single(i => i.Name == "wait_stats").Enabled = false;
            }, ct);
            Assert.Equal(-1, await StoredRunAtAsync(connection, server, ct));

            /* SET: its own time wins over the fleet's. */
            await SaveAsync(viewer, server, editing => SetRunAt(editing, "03:30"), ct);
            Assert.Equal(210, await StoredRunAtAsync(connection, server, ct));
            Assert.Equal(210, await ResolvedRunAtAsync(viewer, server, ct));

            /* KEEP, again with a time: edit a different cell and the time stays. */
            await SaveAsync(viewer, server, editing =>
            {
                Assert.Equal("03:30", editing.Single(i => i.Name == Daily).RunAtText);
                editing.Single(i => i.Name == "wait_stats").Enabled = true;
            }, ct);
            Assert.Equal(210, await StoredRunAtAsync(connection, server, ct));

            /* CLEAR: "Use default" returns the server to the fleet's time. */
            await SaveAsync(viewer, server, editing => SetRunAt(editing, "Use default"), ct);
            Assert.Null(await StoredRunAtAsync(connection, server, ct));
            Assert.Equal(120, await ResolvedRunAtAsync(viewer, server, ct));

            /* The window's "Use default schedule" box removes the server's rows, run time included. */
            await SaveAsync(viewer, server, editing => SetRunAt(editing, "04:00"), ct);
            Assert.Equal(240, await StoredRunAtAsync(connection, server, ct));
            await viewer.ReplaceServerSchedulesAsync(server, Array.Empty<CollectorScheduleRow>(), ct);
            Assert.Equal(0, await RowCountAsync(connection, server, ct));
            Assert.Equal(120, await ResolvedRunAtAsync(viewer, server, ct));
            Assert.Equal(120, await StoredRunAtAsync(connection, null, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task TheStoreRead_ReturnsTheRunTimeOnItsRow_AndNullWhenTheColumnIsNull()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), SkipText);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            foreach (var sql in new[]
            {
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (NULL, 'index_object_stats', 0)",
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (3, 'index_object_stats', -1)",
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, run_at_minute) VALUES (3, 'server_properties', 1439)",
                "INSERT INTO config.config_collector_schedules (server_id, collector_name) VALUES (3, 'wait_stats')",
            })
            {
                await using var seed = new NpgsqlCommand(sql, connection);
                await seed.ExecuteNonQueryAsync(ct);
            }

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var rows = await viewer.GetCollectorSchedulesAsync(ct);

            Assert.Equal(0, rows.Single(r => r.ServerId is null && r.CollectorName == "index_object_stats").RunAtMinute);
            Assert.Equal(-1, rows.Single(r => r.ServerId == 3 && r.CollectorName == "index_object_stats").RunAtMinute);
            Assert.Equal(1439, rows.Single(r => r.ServerId == 3 && r.CollectorName == "server_properties").RunAtMinute);
            Assert.Null(rows.Single(r => r.ServerId == 3 && r.CollectorName == "wait_stats").RunAtMinute);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
