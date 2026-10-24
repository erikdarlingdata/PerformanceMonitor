/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database through ScratchPostgres, which drops it; nothing is planted in a shared store. */

/// <summary>
/// #6390: the Viewer's Fleet Sweeps reads return what the web's sweeps page is served. <c>get_sweep_reports</c> is the web feed's
/// twin over the same store reads and the same <c>FleetSweepPresentation</c> builders, so its JSON for one seeded store is the
/// reference: the Viewer's timeline, newest sweep, sweep by id and watch items must equal it field for field. The store holds a
/// muted sweep, an alerts-on one, one older than the one-hour window, an open item, a fleet-scope item and a closed one, so a read
/// that dropped the window, took the wrong sweep, skipped the server names or the state filter would differ.
/// </summary>
public sealed class ViewerFleetSweepsReadTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static FleetSweepRun Run(long id, DateTime at, bool alertsEnabled, bool alive, long? previous, string report) =>
        new(id, at, at.AddHours(-1), at, previous, alertsEnabled, 2, 2, alive,
            """{"service_started_at":"2026-10-01T00:00:00.0000000Z","alert_passes_total":40,"alert_passes_previous":30,"alert_passes_advancing":true,"alert_read_failures_total":0,"notes":["a note"]}""",
            report);

    [Fact]
    public async Task TheViewersFleetSweepReads_ReturnWhatTheWebFeedReturns_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer Fleet Sweeps test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        /* An empty store: no sweeps, no newest sweep (absence, not an error), no watch items. */
        var emptyTimeline = await viewer.GetFleetSweepTimelineAsync(DateTime.UtcNow.AddHours(-1), DateTime.UtcNow, ct);
        Assert.Empty((JsonArray)emptyTimeline["sweeps"]!);
        Assert.Null(await viewer.GetFleetSweepDetailAsync(null, ct));
        Assert.Null(await viewer.GetFleetSweepDetailAsync(123, ct));
        Assert.Empty((JsonArray)(await viewer.GetFleetSweepWatchItemsAsync(null, ct))["items"]!);
        Assert.Empty(FleetSweepsRows.Runs(emptyTimeline));

        var now = DateTime.UtcNow;
        var oldAt = now.AddHours(-3);
        var mutedAt = now.AddMinutes(-50);
        var newestAt = now.AddMinutes(-30);
        long oldId = oldAt.Ticks, mutedId = mutedAt.Ticks, newestId = newestAt.Ticks;

        await FleetSweepStore.RecordSweepAsync(postgres,
            Run(oldId, oldAt, true, true, null, """{"no_previous_sweep":true,"fleet":{"bands":{"Healthy":2}}}"""),
            new[] { new FleetSweepServerVerdict(1, "server-a", "Healthy", null, null, null) },
            Array.Empty<FleetSweepWouldHavePagedEntry>(), Array.Empty<FleetSweepWatchItem>(), ct);

        await FleetSweepStore.RecordSweepAsync(postgres,
            Run(mutedId, mutedAt, false, false, oldId,
                """{"fleet":{"bands":{"Healthy":1,"Critical":1}},"watch":{"opened":["cpu-pinned"],"closed":[]},"would_have_paged":[{"server":"server-b"}],"changes":{"band_transitions":[{"server":"server-b","from":"Healthy","to":"Critical","reason":"cpu pinned"}],"new_servers":["server-b"],"departed_servers":[]}}"""),
            new[]
            {
                new FleetSweepServerVerdict(1, "server-a", "Healthy", "Healthy", null, null),
                new FleetSweepServerVerdict(2, "server-b", "Critical", "Healthy", "cpu pinned across the span", """{"cpu":99}"""),
            },
            new[] { new FleetSweepWouldHavePagedEntry(2, "high_cpu", """{"cpu":99,"minutes":18}""") },
            new[]
            {
                new FleetSweepWatchItem(2, "cpu-pinned", "cpu above the measured band", "open", 2, 0, mutedId, mutedId, mutedId, null, mutedAt, mutedAt, """{"cpu":99}"""),
                new FleetSweepWatchItem(FleetSweepStore.FleetScopeServerId, "collector-gap", "a collector stopped reporting", "pending", 1, 0, mutedId, mutedId, null, null, mutedAt, mutedAt, null),
                new FleetSweepWatchItem(1, "old-episode", "an episode that ended", "closed", 0, 2, oldId, oldId, oldId, mutedId, oldAt, mutedAt, null),
            }, ct);

        await FleetSweepStore.RecordSweepAsync(postgres,
            Run(newestId, newestAt, true, true, mutedId, """{"fleet":{"bands":{"Healthy":2}},"watch":{"opened":[],"closed":["cpu-pinned"]},"changes":{"band_transitions":[],"new_servers":[],"departed_servers":[]}}"""),
            new[]
            {
                new FleetSweepServerVerdict(1, "server-a", "Healthy", "Healthy", null, null),
                new FleetSweepServerVerdict(2, "server-b", "Healthy", "Critical", null, null),
            },
            Array.Empty<FleetSweepWouldHavePagedEntry>(),
            new[] { new FleetSweepWatchItem(2, "cpu-pinned", "cpu above the measured band", "carried", 1, 1, mutedId, newestId, mutedId, null, mutedAt, newestAt, """{"cpu":40}""") },
            ct);

        /* The reference: get_sweep_reports for the same store and the same hour, the default view (open + carried). */
        var web = JsonNode.Parse(await DarlingMcpFleetSweepTools.GetSweepReports(postgres, null, 1, null, null, null, ct))!.AsObject();

        /* The timeline: the two sweeps inside the hour, newest first, each with its report embedded. The 3-hour-old sweep is outside. */
        var timeline = await viewer.GetFleetSweepTimelineAsync(now.AddHours(-1), now, ct);
        var webSweeps = (JsonArray)web["sweeps"]!;
        var viewerSweeps = (JsonArray)timeline["sweeps"]!;
        Assert.Equal(2, webSweeps.Count);
        Assert.True(JsonNode.DeepEquals(webSweeps, viewerSweeps), "the Viewer's timeline differs from the web feed's");
        Assert.Equal(newestId.ToString(), viewerSweeps[0]!["sweep_id"]!.GetValue<string>());

        /* The newest sweep in full, and the muted one by id (it carries the would-have-paged ledger). */
        var latest = await viewer.GetFleetSweepDetailAsync(null, ct);
        Assert.True(JsonNode.DeepEquals(web["latest"], latest), "the Viewer's newest sweep differs from the web feed's");
        var muted = await viewer.GetFleetSweepDetailAsync(mutedId, ct);
        var webMuted = JsonNode.Parse(await DarlingMcpFleetSweepTools.GetSweepReports(postgres, null, 1, null, mutedId.ToString(), null, ct))!;
        Assert.True(JsonNode.DeepEquals(webMuted, muted), "the Viewer's sweep by id differs from the web feed's");
        Assert.Single((JsonArray)muted!["would_have_paged"]!);
        Assert.Null(latest!["would_have_paged"]);

        /* The worklist: open + carried by default (the fleet-scope pending item and the closed episode are not in it), then the closed state. */
        var items = await viewer.GetFleetSweepWatchItemsAsync(null, ct);
        Assert.True(JsonNode.DeepEquals(web["watch_items"], items), "the Viewer's watch items differ from the web feed's");
        Assert.Equal("server-b", items["items"]![0]!["server"]!.GetValue<string>());
        var webClosed = JsonNode.Parse(await DarlingMcpFleetSweepTools.GetSweepReports(postgres, null, 1, null, null, "closed", ct))!;
        var closed = await viewer.GetFleetSweepWatchItemsAsync("closed", ct);
        Assert.True(JsonNode.DeepEquals(webClosed["watch_items"], closed), "the Viewer's closed watch items differ from the web feed's");
        Assert.Equal("old-episode", closed["items"]![0]!["item"]!.GetValue<string>());

        /* The tab's rows over those nodes. */
        var runs = FleetSweepsRows.Runs(timeline);
        Assert.Equal(new[] { "", "INSTRUMENTS DEAD, MUTED" }, runs.Select(r => r.Flags));
        Assert.Equal("1 would have paged", runs[1].WouldHavePagedText);
        Assert.Equal("+1 watch", runs[1].WatchText);
        Assert.Equal("−1 watch", runs[0].WatchText);
        Assert.Equal("1 Critical  1 Healthy", runs[1].BandsText);
        Assert.Equal("2 / 2 servers reported", FleetSweepsRows.DetailSummary(muted!).Split("   |   ")[1]);

        /* The cadence line (#6390 S2b-B): no settings row yet reads blank, as the web page leaves it; then the Viewer's line and the
           fleet_sweep group the web page reads (get_alert_settings) agree on the same stored knob, enabled and disabled. */
        Assert.Equal("", FleetSweepsRows.CadenceText(await viewer.GetAlertSettingsAsync(ct)));
        foreach (var (enabled, interval, expected) in new[]
                 {
                     (true, 30, "sweeps every 30 min"),
                     (false, 45, "sweeps disabled (fleet_sweep.enabled: false)"),
                 })
        {
            await viewer.UpsertAlertSettingsAsync(new AlertSettingsRow { FleetSweepEnabled = enabled, FleetSweepIntervalMinutes = interval }, ct);
            /* The row get_alert_settings builds its fleet_sweep group from (enabled and interval_minutes, unchanged). */
            var (webRow, _) = await DarlingAlertReader.GetAlertConfigurationAsync(postgres, ct);
            Assert.Equal(enabled, webRow!.FleetSweepEnabled);
            Assert.Equal(interval, webRow.FleetSweepIntervalMinutes);
            Assert.Equal(expected, FleetSweepsRows.CadenceText(await viewer.GetAlertSettingsAsync(ct)));
        }
    }

    [Fact]
    public void TheTabsRows_UseTheWebPagesWording()
    {
        var at = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);
        var first = FleetSweepPresentation.BuildSweepDetailNode(
            Run(1, at, true, true, null, """{"no_previous_sweep":true}"""), Array.Empty<FleetSweepServerVerdict>(), Array.Empty<FleetSweepWouldHavePagedEntry>());
        Assert.Equal("First sweep — there is no previous sweep to diff against, so this document states absolutes.", FleetSweepsRows.ChangesNote(first));
        Assert.Null(FleetSweepsRows.WouldHavePaged(first));
        Assert.Empty(FleetSweepsRows.Verdicts(first));

        var quiet = FleetSweepPresentation.BuildSweepDetailNode(
            Run(2, at, false, true, 1, """{"changes":{"band_transitions":[],"new_servers":[],"departed_servers":[]}}"""),
            Array.Empty<FleetSweepServerVerdict>(), Array.Empty<FleetSweepWouldHavePagedEntry>());
        Assert.Equal("No band changes, arrivals or departures since the previous sweep.", FleetSweepsRows.ChangesNote(quiet));
        Assert.Empty(FleetSweepsRows.WouldHavePaged(quiet)!);

        var items = FleetSweepPresentation.BuildWatchItemsNode(
            new[]
            {
                new FleetSweepWatchItem(0, "gap", "gap", "pending", 1, 0, 1, 1, null, null, at, at, null),
                new FleetSweepWatchItem(7, "cpu", "cpu", "carried", 1, 1, 1, 2, 1, null, at, at, null),
            },
            new System.Collections.Generic.Dictionary<int, string>());
        var rows = FleetSweepsRows.WatchItems(items);
        Assert.Equal("Fleet", rows[0].Server);
        Assert.Equal($"1 of {FleetSweepWatchStateMachine.EntryConsecutiveSweeps} hits to open", rows[0].Position);
        Assert.Equal("server 7", rows[1].Server);
        Assert.Equal($"1 of {FleetSweepWatchStateMachine.ExitConsecutiveSweeps} quiet to close", rows[1].Position);
    }
}
