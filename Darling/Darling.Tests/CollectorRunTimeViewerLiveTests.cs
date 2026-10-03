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
/// for the fleet and for a server, driven the way the window drives them (read the schedule rows and the run times,
/// build the editable schedule, edit a cell, validate, write the run-time changes, then the schedule rows). The run time
/// lives in <c>config.config_collector_run_times</c>, a table of its own, so a schedule Save never carries it. What the
/// service then resolves from the stored rows is checked with its own <see cref="StoreConfigProvider.ResolveSchedule"/>,
/// so "None on a server over a fleet time" is judged by the rule the collectors run on and not by a restatement of it.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection and serializing it would be pure slowdown. */
public sealed class CollectorRunTimeViewerLiveTests
{
    private const string Daily = "index_object_stats";
    private const string AnotherDaily = "server_properties";
    private const string SkipText = "DARLING_TEST_PG is not set: the viewer run-time tests need a disposable PostgreSQL store.";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Every stored run time, one text: <c>fleet/collector=minute</c> or <c>serverId/collector=minute</c>.</summary>
    private static async Task<string> RunTimesAsync(NpgsqlConnection connection, CancellationToken ct) =>
        (string)(await ScalarAsync(connection,
            "SELECT COALESCE(string_agg(COALESCE(server_id::text, 'fleet') || '/' || collector_name || '=' || run_at_minute, ',' ORDER BY server_id NULLS FIRST, collector_name), '') FROM config.config_collector_run_times", ct))!;

    private static async Task<long> ScheduleRowCountAsync(NpgsqlConnection connection, CancellationToken ct) =>
        (long)(await ScalarAsync(connection, "SELECT count(*) FROM config.config_collector_schedules", ct))!;

    /// <summary>The window's own steps for a scope: read both tables, build the editable schedule, let the caller edit it,
    /// validate, write the run-time changes (one statement each), then the schedule rows exactly as a released viewer does.</summary>
    private static async Task SaveAsync(
        ViewerDataService viewer, int? serverId, Action<List<CollectorScheduleEditItem>> edit, CancellationToken ct, bool usesDefault = false)
    {
        var schedules = await viewer.GetCollectorSchedulesAsync(ct);
        var runTimes = await viewer.GetCollectorRunTimesAsync(ct);
        var editing = CollectorScheduleOverlay.BuildEffectiveSchedule(schedules, runTimes, usesDefault ? null : serverId);
        edit(editing);

        if (!usesDefault)
        {
            Assert.True(CollectorScheduleOverlay.ValidateSchedule(editing, out var error), error);
        }

        var changes = CollectorScheduleOverlay.ToRunTimeChanges(editing, runTimes, serverId, usesDefault);
        if (changes.Count > 0)
        {
            await viewer.SaveCollectorRunTimesAsync(changes, ct);
        }

        if (serverId is int sid)
        {
            var rows = usesDefault ? new List<CollectorScheduleRow>() : CollectorScheduleOverlay.ToServerOverrideRows(editing, sid);
            await viewer.ReplaceServerSchedulesAsync(sid, rows, ct);
        }
        else
        {
            await viewer.ReplaceFleetSchedulesAsync(CollectorScheduleOverlay.ToFleetOverrideRows(editing), ct);
        }
    }

    private static void SetRunAt(List<CollectorScheduleEditItem> editing, string text, string collector = Daily) =>
        editing.Single(i => i.Name == collector).RunAtText = text;

    /// <summary>What the service resolves for the collector on the server, from the store's own rows.</summary>
    private static async Task<int?> ResolvedRunAtAsync(ViewerDataService viewer, int serverId, CancellationToken ct)
    {
        var schedules = (await viewer.GetCollectorSchedulesAsync(ct))
            .Select(r => new ScheduleOverride(r.ServerId, r.CollectorName, r.FrequencyMinutes, r.RetentionDays, r.Enabled, r.Databases))
            .ToList();
        var runTimes = (await viewer.GetCollectorRunTimesAsync(ct))
            .Select(r => new RunTimeOverride(r.ServerId, r.CollectorName, r.RunAtMinute))
            .ToList();
        return StoreConfigProvider.ResolveSchedule(Daily, serverId, StoreConfigProvider.MergeRunTimes(schedules, runTimes)).RunAtMinute;
    }

    [Fact]
    public async Task TheFleetRunTime_IsSetKeptAndCleared_ThroughTheRunTimeTable_AndNeverAScheduleRow()
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

            /* Nothing stored: the cell reads "Use default" and a save writes nothing anywhere. */
            await SaveAsync(viewer, null, editing => Assert.Equal("Use default", editing.Single(i => i.Name == Daily).RunAtText), ct);
            Assert.Equal("", await RunTimesAsync(connection, ct));
            Assert.Equal(0L, await ScheduleRowCountAsync(connection, ct));

            /* SET: a fleet time on a collector left at its default cadence is a run-time row and NO schedule row. */
            await SaveAsync(viewer, null, editing => SetRunAt(editing, "02:00"), ct);
            Assert.Equal($"fleet/{Daily}=120", await RunTimesAsync(connection, ct));
            Assert.Equal(0L, await ScheduleRowCountAsync(connection, ct));
            var read = Assert.Single(await viewer.GetCollectorRunTimesAsync(ct));
            Assert.Equal(new CollectorRunTimeRow(null, Daily, 120), read);

            /* KEEP: the window reads the stored time into the cell, and a save that edits only a schedule column (the
               schedule rows are deleted and inserted again) leaves the run time as it was. */
            await SaveAsync(viewer, null, editing =>
            {
                Assert.Equal("02:00", editing.Single(i => i.Name == Daily).RunAtText);
                editing.Single(i => i.Name == AnotherDaily).RetentionDays = 77;
            }, ct);
            Assert.Equal($"fleet/{Daily}=120", await RunTimesAsync(connection, ct));
            Assert.Equal(1L, await ScheduleRowCountAsync(connection, ct));

            /* EDIT: a new time replaces the old one in the same row. */
            await SaveAsync(viewer, null, editing => SetRunAt(editing, "03:15"), ct);
            Assert.Equal($"fleet/{Daily}=195", await RunTimesAsync(connection, ct));

            /* CLEAR: "Use default" deletes the row, and so does "None" (the fleet has nothing to stop). */
            await SaveAsync(viewer, null, editing => SetRunAt(editing, "Use default"), ct);
            Assert.Equal("", await RunTimesAsync(connection, ct));
            await SaveAsync(viewer, null, editing => SetRunAt(editing, "01:00"), ct);
            await SaveAsync(viewer, null, editing => SetRunAt(editing, "None"), ct);
            Assert.Equal("", await RunTimesAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AServerRunTime_UsesDefaultNoneOrItsOwnTime_AndNoneStopsAFleetTime()
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

            await SaveAsync(viewer, null, editing => SetRunAt(editing, "02:00"), ct);
            Assert.Equal(120, await ResolvedRunAtAsync(viewer, 7, ct));

            /* "Use default" on a customizing server writes no row for it, so the fleet's time applies. */
            await SaveAsync(viewer, 7, editing => Assert.Equal("Use default", editing.Single(i => i.Name == Daily).RunAtText), ct);
            Assert.Equal($"fleet/{Daily}=120", await RunTimesAsync(connection, ct));
            Assert.Equal(120, await ResolvedRunAtAsync(viewer, 7, ct));

            /* ITS OWN TIME replaces the fleet's for that server only. */
            await SaveAsync(viewer, 7, editing => SetRunAt(editing, "04:30"), ct);
            Assert.Equal($"fleet/{Daily}=120,7/{Daily}=270", await RunTimesAsync(connection, ct));
            Assert.Equal(270, await ResolvedRunAtAsync(viewer, 7, ct));
            Assert.Equal(120, await ResolvedRunAtAsync(viewer, 8, ct));

            /* NONE is -1 on the server's row, which stops the fleet's time for that server. */
            await SaveAsync(viewer, 7, editing => SetRunAt(editing, "None"), ct);
            Assert.Equal($"fleet/{Daily}=120,7/{Daily}=-1", await RunTimesAsync(connection, ct));
            Assert.Null(await ResolvedRunAtAsync(viewer, 7, ct));
            Assert.Equal(120, await ResolvedRunAtAsync(viewer, 8, ct));

            /* "Use the default schedule" deletes the server's schedule rows AND its run-time rows. */
            await SaveAsync(viewer, 7, _ => { }, ct, usesDefault: true);
            Assert.Equal($"fleet/{Daily}=120", await RunTimesAsync(connection, ct));
            Assert.Equal(120, await ResolvedRunAtAsync(viewer, 7, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AScheduleSave_ThroughTheViewersRealSql_LeavesEveryRunTimeAlone_AndApplyDefaultToAllRemovesOnlyTheServerOnes()
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
            await ExecAsync(connection,
                $"INSERT INTO config.config_collector_run_times (server_id, collector_name, run_at_minute) VALUES (NULL, '{Daily}', 0), (3, '{Daily}', -1), (3, '{AnotherDaily}', 1439)", ct);
            const string Expected = $"fleet/{Daily}=0,3/{Daily}=-1,3/{AnotherDaily}=1439";

            /* The viewer's own schedule statements, the fleet's and a server's, replace schedule rows only. */
            await viewer.ReplaceFleetSchedulesAsync(new[] { new CollectorScheduleRow(null, Daily, 1440, 30, true) }, ct);
            await viewer.ReplaceServerSchedulesAsync(3, new[] { new CollectorScheduleRow(3, Daily, null, null, true) }, ct);
            await viewer.ReplaceServerSchedulesAsync(3, Array.Empty<CollectorScheduleRow>(), ct);
            Assert.Equal(Expected, await RunTimesAsync(connection, ct));

            /* The read returns the rows as stored: minute 0, the last minute of the day, and -1 on a server row. */
            var rows = await viewer.GetCollectorRunTimesAsync(ct);
            Assert.Equal(0, rows.Single(r => r.ServerId is null).RunAtMinute);
            Assert.Equal(-1, rows.Single(r => r.ServerId == 3 && r.CollectorName == Daily).RunAtMinute);
            Assert.Equal(1439, rows.Single(r => r.ServerId == 3 && r.CollectorName == AnotherDaily).RunAtMinute);

            /* "Apply Default to All Servers" sends every server back to the fleet, run times included, and keeps the fleet's. */
            Assert.Equal(0, await viewer.ResetAllServerSchedulesAsync(ct));
            Assert.Equal(Expected, await RunTimesAsync(connection, ct));   /* the schedule reset alone leaves every run time */
            Assert.Equal(2, await viewer.ResetAllServerRunTimesAsync(ct));
            Assert.Equal($"fleet/{Daily}=0", await RunTimesAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The viewer's schema probe fails open when it throws, so a store below V160 can reach the editor. The run-time read
    /// must then be "no run times" (42P01 undefined_table), the editor's Run at cells must all read "Use default", and saving a run
    /// time must say the store is older than this viewer (not the raw error). A schedule save and the reset still work.</summary>
    [Fact]
    public async Task AStoreWithoutTheRunTimeTable_ReadsNoRunTimes_AndASaveSaysTheStoreIsOlder()
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
            await ExecAsync(connection, "DROP TABLE config.config_collector_run_times", ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            Assert.Empty(await viewer.GetCollectorRunTimesAsync(ct));
            var editing = CollectorScheduleOverlay.BuildEffectiveSchedule(
                await viewer.GetCollectorSchedulesAsync(ct), await viewer.GetCollectorRunTimesAsync(ct), null);
            Assert.All(editing, i => Assert.Equal(CollectorScheduleOverlay.UseDefaultRunAtText, i.RunAtText));

            var skew = await Assert.ThrowsAsync<ViewerSchemaSkewException>(() =>
                viewer.SaveCollectorRunTimesAsync(new[] { new CollectorRunTimeChange(null, Daily, 120) }, ct));
            Assert.Contains("Update or restart the Darling service", skew.Message, StringComparison.Ordinal);
            Assert.IsType<PostgresException>(skew.InnerException);

            /* Everything that does not touch a run time still works on such a store. */
            await viewer.ReplaceFleetSchedulesAsync(new[] { new CollectorScheduleRow(null, Daily, 1440, 30, true) }, ct);
            Assert.Equal(1L, await ScheduleRowCountAsync(connection, ct));
            Assert.Equal(0, await viewer.ResetAllServerSchedulesAsync(ct));
            Assert.Equal(0, await viewer.ResetAllServerRunTimesAsync(ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
