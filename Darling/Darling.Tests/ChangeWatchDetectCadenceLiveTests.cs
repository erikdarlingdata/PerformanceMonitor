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
using PerformanceMonitor.Darling.Service.ChangeWatch;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.ChangeWatchDetectionLiveTests;
using static Darling.Tests.ChangeWatchLiveTests;

namespace Darling.Tests;

/// <summary>
/// #6234: change DETECTION runs at most once an hour per target (<see cref="ChangeWatchPass.DetectInterval"/>), while the evaluation of due
/// watches stays on every run. The pass has no seam that counts its reads, so each fact proves the gate through what the store holds: a change
/// seeded for the gated runs is NOT recorded by them, and the target's marks (which only a detect that finished moves) stay where they were.
/// Each fact runs on its own scratch database, with a fake clock the passes read.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ChangeWatchLiveTests.RunAsync (ScratchPostgres), so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class ChangeWatchDetectCadenceLiveTests
{
    private const string Dop = "max degree of parallelism";

    private static async Task<IReadOnlyDictionary<string, DateTime>> MarksAsync(Harness h, int serverId, CancellationToken ct) =>
        await h.Store.GetMarksAsync(serverId, ct);

    private static async Task<object?> ScalarAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, c);
        return await command.ExecuteScalarAsync(ct);
    }

    [Fact]
    public async Task ChangesBetweenTwoHourlyDetects_AreFoundAtTheNextOne_AndEachIsRecordedExactlyOnce()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            var t0 = h.Now;
            var clock = t0;
            var pass = h.NewPassAt(() => clock);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-30), Dop, 4, ct);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-5), Dop, 4, ct);

            /* The first run detects at once: on a store with history its marks start now and nothing stored is a change. */
            await pass.DetectAsync(ct);
            Assert.All((await MarksAsync(h, ServerId, ct)).Values, m => Assert.Equal(t0, m));

            /* A change at +10 minutes. Runs at +20 and +59 are inside the hour of the detect at +0: they read nothing, so the change is not recorded and no mark moves. */
            await ServerConfigAsync(h.Connection, t0.AddMinutes(10), Dop, 2, ct);
            foreach (var minutes in new[] { 20, 59 })
            {
                clock = t0.AddMinutes(minutes);
                await pass.DetectAsync(ct);
                Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
                Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
                Assert.All((await MarksAsync(h, ServerId, ct)).Values, m => Assert.Equal(t0, m));
            }

            /* A second change at +58, then the run at +61 is an hour after the last detect: it records both, under one watch. */
            await ServerConfigAsync(h.Connection, t0.AddMinutes(58), Dop, 1, ct);
            clock = t0.AddMinutes(61);
            await pass.DetectAsync(ct);
            Assert.Equal(2, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            Assert.All((await MarksAsync(h, ServerId, ct)).Values, m => Assert.Equal(clock - ChangeWatchPass.MarkSlack, m));

            /* The next detect is an hour on. Its marks sit five minutes before the last detect, so it reads the change at +58 a second time: the change key keeps it to one row. */
            clock = t0.AddMinutes(122);
            await pass.DetectAsync(ct);
            Assert.All((await MarksAsync(h, ServerId, ct)).Values, m => Assert.Equal(clock - ChangeWatchPass.MarkSlack, m));
            Assert.Equal(2, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(2, await CountAsync(h.Connection, "SELECT count(DISTINCT change_key) FROM config.change_watch_changes", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task ARestart_DetectsAtOnce_FindsTheChangeTheOldInstanceWouldHaveHeldBack_AndRecordsItOnce()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            var t0 = h.Now;
            var clock = t0;
            var before = h.NewPassAt(() => clock);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-30), Dop, 4, ct);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-5), Dop, 4, ct);
            await before.DetectAsync(ct);

            await ServerConfigAsync(h.Connection, t0.AddMinutes(10), Dop, 2, ct);

            /* The service restarts at +15. The instance that detected at +0 is still inside its hour. */
            clock = t0.AddMinutes(15);
            await before.DetectAsync(ct);
            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));

            /* A new instance has no last-detected time: its first run detects, from the marks the store kept, so the change is found. */
            var after = h.NewPassAt(() => clock);
            await after.DetectAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));

            /* The new instance is then gated like any other, and the old one detecting at its own hour finds nothing new: still one change. */
            clock = t0.AddMinutes(30);
            await after.DetectAsync(ct);
            Assert.Equal((t0.AddMinutes(15) - ChangeWatchPass.MarkSlack), (await MarksAsync(h, ServerId, ct))["server_config"]);
            clock = t0.AddMinutes(75);
            await before.DetectAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
        });
    }

    [Fact]
    public async Task ADetectThatComesLongAfterTheHour_StillReadsFromItsMarks_AndLosesNoChange()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            var t0 = h.Now;
            var clock = t0;
            var pass = h.NewPassAt(() => clock);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-30), Dop, 4, ct);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-5), Dop, 4, ct);
            await pass.DetectAsync(ct);

            /* A sweep that stalled (a pause, a slow store) comes back 100 minutes after the last detect. The change at +20 is 80 minutes old then:
               a lookback of 70 minutes would start the read after it and lose it, with only a warning. */
            await ServerConfigAsync(h.Connection, t0.AddMinutes(20), Dop, 2, ct);
            clock = t0.AddMinutes(100);
            await pass.DetectAsync(ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
        });
    }

    [Fact]
    public async Task EvaluatingDueWatches_StaysOnEveryRun_EvenWhenDetectionIsInsideItsHour()
    {
        await RunAsync(false, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            var clock = h.Now;
            var pass = h.NewPassAt(() => clock);
            await ServerConfigAsync(h.Connection, h.Now.AddMinutes(-30), Dop, 4, ct);
            await pass.RunOnceAsync(ct);

            /* A watch whose window ended 30 minutes ago. The next run is two minutes after the detect, so detection is skipped, and the watch is still evaluated. */
            await h.Store.RecordChangesAsync(ServerId, ServerName, await OneChange(h.Now.AddMinutes(-90), "max server memory"), ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE state = 'open'", ct));
            clock = h.Now.AddMinutes(2);
            await pass.RunOnceAsync(ct);

            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE state = 'open'", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE state = 'closed'", ct));
        });
    }

    [Fact]
    public async Task ASecondChangeInsideTheWindow_FoundAfterTheFirstDetect_JoinsTheFirstWatchBeforeItIsEvaluated()
    {
        await RunAsync(false, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);

            /* The store's clock is h.Now, so the scenario runs in the past and its last run is at h.Now: t0 is 71 minutes before. */
            var t0 = h.Now.AddMinutes(-71);
            var clock = t0;
            var pass = h.NewPassAt(() => clock);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-30), Dop, 4, ct);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-5), Dop, 4, ct);
            await pass.RunOnceAsync(ct);

            /* Change A at +10 is recorded by the hourly detect at +61: its watch's window ends at +70. */
            await ServerConfigAsync(h.Connection, t0.AddMinutes(10), Dop, 2, ct);
            clock = t0.AddMinutes(61);
            await pass.RunOnceAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(t0.AddMinutes(70), DateTime.SpecifyKind(
                (DateTime)(await ScalarAsync(h.Connection, "SELECT window_ends_at FROM config.change_watches", ct))!, DateTimeKind.Utc));

            /* Change B at +65 is inside A's window and after that detect. The run at +71 is inside the hour, and A's watch is due: B is read before A is
               evaluated, so it joins A's watch and moves the window's end to an hour after B. Nothing is evaluated yet. */
            await ServerConfigAsync(h.Connection, t0.AddMinutes(65), Dop, 1, ct);
            clock = t0.AddMinutes(71);
            await pass.RunOnceAsync(ct);

            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches WHERE state = 'open'", ct));
            Assert.Equal(2, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(DISTINCT watch_id) FROM config.change_watch_changes", ct));
            Assert.Equal(t0.AddMinutes(125), DateTime.SpecifyKind(
                (DateTime)(await ScalarAsync(h.Connection, "SELECT window_ends_at FROM config.change_watches", ct))!, DateTimeKind.Utc));
        });
    }

    [Fact]
    public async Task ATargetWhoseDetectFailed_WaitsTheBackoff_ThenIsDetectedAgain()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            var t0 = h.Now;
            var clock = t0;
            var pass = h.NewPassAt(() => clock);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-30), Dop, 4, ct);
            await ServerConfigAsync(h.Connection, t0.AddMinutes(-5), Dop, 4, ct);
            await pass.DetectAsync(ct);

            async Task Execute(string sql)
            {
                await using var command = new NpgsqlCommand(sql, h.Connection);
                await command.ExecuteNonQueryAsync(ct);
            }

            /* The detect at +61 fails: the server-config read has no view to read. The marks stay where the detect at +0 left them. */
            await ServerConfigAsync(h.Connection, t0.AddMinutes(30), Dop, 2, ct);
            await Execute("ALTER VIEW v_server_config RENAME TO v_server_config_gone");
            clock = t0.AddMinutes(61);
            await pass.DetectAsync(ct);
            Assert.All((await MarksAsync(h, ServerId, ct)).Values, m => Assert.Equal(t0, m));

            /* The read works again. A run one minute later is inside the backoff: it reads nothing, so the change is not recorded and no mark moves. */
            await Execute("ALTER VIEW v_server_config_gone RENAME TO v_server_config");
            clock = t0.AddMinutes(62);
            await pass.DetectAsync(ct);
            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.All((await MarksAsync(h, ServerId, ct)).Values, m => Assert.Equal(t0, m));

            /* Once the backoff has passed the target is detected again, and the change is found. */
            clock = t0.AddMinutes(61) + ChangeWatchPass.FailedDetectBackoff;
            await pass.DetectAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
        });
    }

    [Fact]
    public async Task OneReadWithAnHourOfChanges_OpensTheWatchTheyWouldHaveOpenedOneAtATime()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await AddServerAsync(h.Connection, ct);
            var a = h.Now.AddMinutes(-170);
            ChangeWatchChange Change(DateTime at, string what) => new($"server_config:{what}:{at:O}", "server_config", at, what);

            /* No watch is open. A at -170 and C at -80 come in one read: C is 90 minutes after A, past the hour A's watch would hold. A opens the watch alone
               and C is deferred, as it was when A was recorded within a tick and C a run later. */
            var opened = await h.Store.RecordAsync(ServerId, ServerName, [Change(a, "a"), Change(a.AddMinutes(90), "c")], ct);
            Assert.True(opened.Deferred);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(a.AddMinutes(60), DateTime.SpecifyKind(
                (DateTime)(await ScalarAsync(h.Connection, "SELECT window_ends_at FROM config.change_watches", ct))!, DateTimeKind.Utc));

            /* B at -120 is inside A's window, and C is inside the window B extends it to: one at a time both join, so in one read both join too. */
            var joined = await h.Store.RecordAsync(ServerId, ServerName, [Change(a.AddMinutes(50), "b"), Change(a.AddMinutes(90), "c")], ct);
            Assert.False(joined.Deferred);
            Assert.Equal(3, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watches", ct));
            Assert.Equal(a.AddMinutes(150), DateTime.SpecifyKind(
                (DateTime)(await ScalarAsync(h.Connection, "SELECT window_ends_at FROM config.change_watches", ct))!, DateTimeKind.Utc));
        });
    }

    [Fact]
    public async Task APostgresTarget_IsGatedTheSameWay_ItsSettingChangeIsFoundAtTheNextHourlyDetect()
    {
        await RunAsync(true, async (h, ct) =>
        {
            const int pgId = 21;
            await using (var insert = new NpgsqlCommand(
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date) " +
                "VALUES (21, 'pg-twentyone', 'pg-twentyone', true, 'postgres', now(), now())", h.Connection))
            {
                await insert.ExecuteNonQueryAsync(ct);
            }

            Task Config(DateTime at, string value) => SeedAsync(h.Connection, "pg_server_config", new Dictionary<string, object>
            {
                ["server_id"] = pgId, ["server_name"] = "pg-twentyone", ["collection_time"] = Naive(at), ["name"] = "work_mem",
                ["setting"] = value, ["source"] = "configuration file", ["database_name"] = DBNull.Value,
            }, ct);

            var t0 = h.Now;
            var clock = t0;
            var pass = h.NewPassAt(() => clock);
            await Config(t0.AddMinutes(-50), "4096");
            await Config(t0.AddMinutes(-10), "4096");
            await pass.DetectAsync(ct);

            await Config(t0.AddMinutes(5), "65536");
            clock = t0.AddMinutes(10);
            await pass.DetectAsync(ct);
            Assert.Equal(0, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));

            clock = t0.AddMinutes(61);
            await pass.DetectAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
            clock = t0.AddMinutes(122);
            await pass.DetectAsync(ct);
            Assert.Equal(1, await CountAsync(h.Connection, "SELECT count(*) FROM config.change_watch_changes", ct));
        });
    }
}
