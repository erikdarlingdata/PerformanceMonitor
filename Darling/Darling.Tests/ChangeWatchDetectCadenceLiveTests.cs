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
