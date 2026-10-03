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
/// build the editable schedule, edit a cell, validate, then write the run-time changes and the schedule rows in ONE
/// transaction). The run time
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
    /// validate, then ONE call that writes the run-time changes (one statement each) and the schedule rows, exactly as a
    /// released viewer writes them, in one transaction.</summary>
    private static async Task SaveAsync(
        ViewerDataService viewer, int? serverId, Action<List<CollectorScheduleEditItem>> edit, CancellationToken ct,
        bool usesDefault = false, bool resetToDefaults = false, Func<Task>? afterOpen = null)
    {
        var schedules = await viewer.GetCollectorSchedulesAsync(ct);
        var runTimes = await viewer.GetCollectorRunTimesAsync(ct);
        var editing = CollectorScheduleOverlay.BuildEffectiveSchedule(schedules, runTimes, usesDefault ? null : serverId);
        if (resetToDefaults)
        {
            /* The window's Reset to Defaults replaces the whole grid with the shipped defaults. */
            editing = CollectorSchedulePresets.BuildDefaultSchedule();
        }

        edit(editing);

        /* Another writer (the CLI verb, another viewer) can change the store while the window is open. */
        if (afterOpen is not null)
        {
            await afterOpen();
        }

        if (!usesDefault)
        {
            Assert.True(CollectorScheduleOverlay.ValidateSchedule(editing, out var error), error);
        }

        /* The window's own rule: a schedule Reset (Reset to Defaults, or a server on "Use default schedule") also clears the scope's
           run-time rows, by scope and in the same transaction. */
        var clearRunTimes = resetToDefaults || usesDefault;
        var changes = CollectorScheduleOverlay.ToRunTimeChanges(editing, runTimes, serverId, usesDefault, clearRunTimes);
        var rows = serverId is int sid
            ? (usesDefault ? new List<CollectorScheduleRow>() : CollectorScheduleOverlay.ToServerOverrideRows(editing, sid))
            : CollectorScheduleOverlay.ToFleetOverrideRows(editing);
        await viewer.SaveCollectorScheduleAsync(serverId, rows, changes, clearRunTimes, ct);
    }

    /// <summary>The fleet row's stored frequency for the daily collector, or null when there is no such row (or no frequency on it).</summary>
    private static async Task<int?> FleetFrequencyAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await ScalarAsync(connection,
            $"SELECT frequency_minutes FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = '{Daily}'", ct) is int minutes
            ? minutes
            : null;

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
    public async Task AScheduleSave_ThroughTheViewersRealSql_LeavesEveryRunTimeAlone_AndApplyDefaultToAllRemovesOnlyTheServerRunTimes()
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

            /* "Apply Default to All Servers" is one call: it sends every server back to the fleet, run times included (no server
               schedule rows are left at this point, so the count is the two server run times), and keeps the fleet's. */
            Assert.Equal(2, await viewer.ResetAllServerSchedulesAsync(ct));
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

            /* Everything that does not touch a run time still works on such a store, the all-servers reset included: it removes the
               server's schedule rows and has no run-time table to clear. */
            await viewer.ReplaceFleetSchedulesAsync(new[] { new CollectorScheduleRow(null, Daily, 1440, 30, true) }, ct);
            await viewer.ReplaceServerSchedulesAsync(3, new[] { new CollectorScheduleRow(3, Daily, 720, 30, true) }, ct);
            Assert.Equal(2L, await ScheduleRowCountAsync(connection, ct));
            Assert.Equal(1, await viewer.ResetAllServerSchedulesAsync(ct));
            Assert.Equal(1L, await ScheduleRowCountAsync(connection, ct));

            /* The window's one-transaction Save on such a store: a Save that changed no run time sends no run-time statement and
               still saves its schedules; one that did says the store is older, and the schedule rows of that same Save are NOT
               written (they share its transaction). */
            await viewer.SaveCollectorScheduleAsync(
                null, new[] { new CollectorScheduleRow(null, Daily, 720, 30, true) }, Array.Empty<CollectorRunTimeChange>(), ct);
            Assert.Equal(720, await FleetFrequencyAsync(connection, ct));
            var oneTransaction = await Assert.ThrowsAsync<ViewerSchemaSkewException>(() => viewer.SaveCollectorScheduleAsync(
                null, new[] { new CollectorScheduleRow(null, Daily, 60, 30, true) }, new[] { new CollectorRunTimeChange(null, Daily, 120) }, ct));
            Assert.Contains("Update or restart the Darling service", oneTransaction.Message, StringComparison.Ordinal);
            Assert.Equal(720, await FleetFrequencyAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The window's Save is ONE transaction on one connection (#4938). The run-time write comes first, so a schedule write
    /// that fails after it (here a frequency the schedules table's own CHECK refuses) must take the run time back with it, for the
    /// fleet and for a server; a run-time write that fails (the run-time table's CHECK refuses -1 on a fleet row) must leave the
    /// schedule rows of that Save as they were; and the same Save with a valid schedule is the control that both halves land
    /// together.</summary>
    [Fact]
    public async Task AScheduleWriteThatFails_LeavesTheRunTimeUnsaved_BecauseTheSaveIsOneTransaction()
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

            /* The control: a valid fleet Save lands the run time and the schedule row together. */
            await viewer.SaveCollectorScheduleAsync(
                null, new[] { new CollectorScheduleRow(null, Daily, 1440, 30, true) }, new[] { new CollectorRunTimeChange(null, Daily, 120) }, ct);
            Assert.Equal($"fleet/{Daily}=120", await RunTimesAsync(connection, ct));
            Assert.Equal(1440, await FleetFrequencyAsync(connection, ct));

            /* FLEET: the run time moves 120 -> 300 and the schedule write then fails (frequency_minutes >= 0 refuses -1). Neither
               half may stay: the run time is still 120 and the schedule row is still the one the control wrote. */
            var fleetFailure = await Assert.ThrowsAsync<PostgresException>(() => viewer.SaveCollectorScheduleAsync(
                null, new[] { new CollectorScheduleRow(null, Daily, -1, 30, true) }, new[] { new CollectorRunTimeChange(null, Daily, 300) }, ct));
            Assert.Equal("23514", fleetFailure.SqlState);
            Assert.Equal($"fleet/{Daily}=120", await RunTimesAsync(connection, ct));
            Assert.Equal(1440, await FleetFrequencyAsync(connection, ct));
            Assert.Equal(1L, await ScheduleRowCountAsync(connection, ct));

            /* SERVER: a new server run time and a refused server schedule row. No server run time, no server schedule row. */
            var serverFailure = await Assert.ThrowsAsync<PostgresException>(() => viewer.SaveCollectorScheduleAsync(
                3, new[] { new CollectorScheduleRow(3, Daily, -1, null, true) }, new[] { new CollectorRunTimeChange(3, Daily, 180) }, ct));
            Assert.Equal("23514", serverFailure.SqlState);
            Assert.Equal($"fleet/{Daily}=120", await RunTimesAsync(connection, ct));
            Assert.Equal(1L, await ScheduleRowCountAsync(connection, ct));

            /* THE OTHER WAY: a run-time write the run-time table refuses (-1 is a server-row value only) leaves the schedule rows of
               that Save as they were, so the 1440 the control wrote is not replaced by this Save's 720. */
            var runTimeFailure = await Assert.ThrowsAsync<PostgresException>(() => viewer.SaveCollectorScheduleAsync(
                null, new[] { new CollectorScheduleRow(null, Daily, 720, 30, true) }, new[] { new CollectorRunTimeChange(null, Daily, -1) }, ct));
            Assert.Equal("23514", runTimeFailure.SqlState);
            Assert.Equal($"fleet/{Daily}=120", await RunTimesAsync(connection, ct));
            Assert.Equal(1440, await FleetFrequencyAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private const string RunTimeRows = "INSERT INTO config.config_collector_run_times (server_id, collector_name, run_at_minute) VALUES ";

    private const string ScheduleRows = "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes) VALUES ";

    /// <summary>"Apply Default to All Servers" is ONE call (#4938): it deletes every server's schedule rows and every server's run
    /// times, and keeps the fleet's schedule rows and run times. A server's run time is part of its override, and Reset means back
    /// to the shipped defaults, which have no fixed time. The count it returns is the rows removed from both tables.</summary>
    [Fact]
    public async Task ApplyDefaultToAllServers_RemovesEveryServersScheduleRowsAndRunTimes_AndKeepsTheFleets()
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
            await ExecAsync(connection, ScheduleRows + $"(NULL, '{Daily}', 1440), (3, '{Daily}', 2880), (7, '{AnotherDaily}', 720)", ct);
            await ExecAsync(connection, RunTimeRows + $"(NULL, '{Daily}', 0), (3, '{Daily}', -1), (3, '{AnotherDaily}', 1439), (7, '{Daily}', 300)", ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* Two server schedule rows and three server run times, in one call. */
            Assert.Equal(5, await viewer.ResetAllServerSchedulesAsync(ct));
            Assert.Equal(1L, await ScheduleRowCountAsync(connection, ct));
            Assert.Equal(1440, await FleetFrequencyAsync(connection, ct));
            Assert.Equal($"fleet/{Daily}=0", await RunTimesAsync(connection, ct));

            /* Nothing left that belongs to a server, so a second call removes nothing. */
            Assert.Equal(0, await viewer.ResetAllServerSchedulesAsync(ct));
            Assert.Equal($"fleet/{Daily}=0", await RunTimesAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The all-servers reset is one transaction (#4938): a failure after the schedule delete, here the run-time delete
    /// refused by a trigger, leaves the schedule rows AND the run times in place, and the same call with the trigger gone is the
    /// control that both halves land together.</summary>
    [Fact]
    public async Task ApplyDefaultToAllServers_ThatFailsAfterTheScheduleDelete_LeavesTheScheduleRowsAndTheRunTimesInPlace()
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
            await ExecAsync(connection, ScheduleRows + $"(NULL, '{Daily}', 1440), (3, '{Daily}', 2880), (7, '{AnotherDaily}', 720)", ct);
            await ExecAsync(connection, RunTimeRows + $"(NULL, '{Daily}', 0), (3, '{Daily}', -1), (7, '{Daily}', 300)", ct);
            const string Before = $"fleet/{Daily}=0,3/{Daily}=-1,7/{Daily}=300";
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* The schedule delete runs first, so a refused run-time delete has to take it back. */
            await ExecAsync(connection,
                "CREATE FUNCTION public.refuse_run_time_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'run times cannot be deleted'; END $$", ct);
            await ExecAsync(connection,
                "CREATE TRIGGER trg_refuse_run_time_delete BEFORE DELETE ON config.config_collector_run_times FOR EACH STATEMENT EXECUTE FUNCTION public.refuse_run_time_delete()", ct);

            var failure = await Assert.ThrowsAsync<PostgresException>(() => viewer.ResetAllServerSchedulesAsync(ct));
            Assert.Equal("P0001", failure.SqlState);
            Assert.Equal(3L, await ScheduleRowCountAsync(connection, ct));
            Assert.Equal(2L, await ScalarAsync(connection, "SELECT count(*) FROM config.config_collector_schedules WHERE server_id IS NOT NULL", ct));
            Assert.Equal(Before, await RunTimesAsync(connection, ct));

            /* The control: without the trigger the same call removes both. */
            await ExecAsync(connection, "DROP TRIGGER trg_refuse_run_time_delete ON config.config_collector_run_times", ct);
            Assert.Equal(4, await viewer.ResetAllServerSchedulesAsync(ct));
            Assert.Equal(1L, await ScheduleRowCountAsync(connection, ct));
            Assert.Equal($"fleet/{Daily}=0", await RunTimesAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>A schedule Reset deletes that scope's run-time rows, in the same Save (#4938). Reset to Defaults replaces the
    /// grid with the shipped defaults, which have no fixed time, so the Save leaves no run-time row for the scope: not the ones the
    /// window showed, not one another writer added while the window was open, and not one for a collector this build does not
    /// define. A server that "uses the default schedule" is the same Reset for that server. Every other scope keeps its rows.</summary>
    [Fact]
    public async Task ASaveAfterAReset_LeavesNoRunTimeRowForTheScope_AndKeepsEveryOtherScopes()
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
            await ExecAsync(connection,
                RunTimeRows + $"(NULL, '{Daily}', 120), (NULL, 'a_collector_this_build_lacks', 30), (7, '{Daily}', 300), (7, 'a_collector_this_build_lacks', 60), (8, '{Daily}', 200), (9, '{Daily}', 240)", ct);
            await using var viewer = new ViewerDataService(scratch.ConnectionString);

            /* SERVER 7: it holds a time for the collector the window shows, one for a collector this build does not define, and
               a third that another writer adds while the window is open. The Reset removes all three, and no other scope's. */
            await SaveAsync(viewer, 7, _ => { }, ct, resetToDefaults: true,
                afterOpen: () => ExecAsync(connection, RunTimeRows + $"(7, '{AnotherDaily}', 1439)", ct));
            Assert.Equal(
                $"fleet/a_collector_this_build_lacks=30,fleet/{Daily}=120,8/{Daily}=200,9/{Daily}=240",
                await RunTimesAsync(connection, ct));

            /* A time typed into the grid after the Reset is kept: the Save clears the scope, then writes what the grid holds. */
            await ExecAsync(connection, RunTimeRows + $"(8, '{AnotherDaily}', 90)", ct);
            await SaveAsync(viewer, 8, editing => SetRunAt(editing, "04:00"), ct, resetToDefaults: true);
            Assert.Equal(
                $"fleet/a_collector_this_build_lacks=30,fleet/{Daily}=120,8/{Daily}=240,9/{Daily}=240",
                await RunTimesAsync(connection, ct));

            /* "Use default schedule" for a server is the same Reset for that server. */
            await SaveAsync(viewer, 9, _ => { }, ct, usesDefault: true,
                afterOpen: () => ExecAsync(connection, RunTimeRows + $"(9, '{AnotherDaily}', 45)", ct));
            Assert.Equal(
                $"fleet/a_collector_this_build_lacks=30,fleet/{Daily}=120,8/{Daily}=240",
                await RunTimesAsync(connection, ct));

            /* THE FLEET: Reset to Defaults clears the fleet's rows (and the fleet only). */
            await SaveAsync(viewer, null, _ => { }, ct, resetToDefaults: true);
            Assert.Equal($"8/{Daily}=240", await RunTimesAsync(connection, ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
