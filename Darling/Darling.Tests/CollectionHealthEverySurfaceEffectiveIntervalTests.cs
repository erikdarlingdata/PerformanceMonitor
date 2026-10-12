/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4999 (part of #4938): every collection-health surface judges a collector against the interval it is SCHEDULED at on
/// its server, not the one it shipped with. get_collection_health did first; the fleet roll-up (the per-server
/// healthy and failing counts of the fleet overview) and the viewer's collection-health reads (a server's own tab,
/// and the Overview cards and status bar) still judged against the shipped interval, so a collector an operator
/// slowed to every 720 minutes could band HEALTHY in one place and STALE in another.
///
/// <para>The rule is one: <see cref="CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes{T}"/> in
/// <c>PerformanceMonitor.Collectors</c>, the project the service and the viewer both reference. The worker's
/// <see cref="StoreConfigProvider.ResolveSchedule"/> picks its rows through the same
/// <see cref="CollectorScheduleDefaults.SelectScheduleOverrides{T}"/>. The cases below pin that the three are the
/// same answer, then drive each surface's own step with a row whose newest success is six hours old: past the
/// four-hour line a five-minute collector goes STALE at, well inside the 18 hours (one and a half intervals) a
/// collector scheduled every 720 minutes gets.</para>
/// </summary>
public sealed class CollectionHealthEverySurfaceEffectiveIntervalTests
{
    private const string FiveMinute = "memory_clerks";
    private const int ServerId = 7;
    private const int OtherServerId = 8;

    private const string FleetReaderPath = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingFleetReader.cs";
    private const string ViewerPath = "Darling/PerformanceMonitor.Darling.Viewer/ViewerDataService.CollectionHealth.cs";
    private const string StoreConfigPath = "Darling/PerformanceMonitor.Darling.Service/StoreConfigProvider.cs";

    private static ScheduleOverride Override(int? serverId, string name, int? minutes) =>
        new(serverId, name, minutes, RetentionDays: null, Enabled: true);

    private static int? Resolve(string name, params ScheduleOverride[] rows) =>
        CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(name, ServerId, rows);

    private static CollectorScheduleRow ViewerOverride(int? serverId, string name, int? minutes) =>
        new(serverId, name, minutes, RetentionDays: null, Enabled: true);

    /// <summary>The same rows as the viewer carries them.</summary>
    private static CollectorScheduleRow[] AsViewerRows(IEnumerable<ScheduleOverride> rows) =>
        rows.Select(r => ViewerOverride(r.ServerId, r.CollectorName, r.FrequencyMinutes)).ToArray();

    /// <summary>One row in the fourteen-column shape both fleet reads produce (server_id first), healthy on every count
    /// and with every instant <paramref name="hoursAgo"/> hours old, so the only thing that can move the band is the
    /// interval the row is judged against.</summary>
    private static DataTableReader FleetRow(string collector, double hoursAgo)
    {
        var table = new DataTable();
        table.Columns.Add("server_id", typeof(int));
        table.Columns.Add("collector_name", typeof(string));
        foreach (var name in new[] { "total_runs", "success_count", "error_count" })
        {
            table.Columns.Add(name, typeof(long));
        }

        table.Columns.Add("last_success_time", typeof(DateTime));
        table.Columns.Add("permission_denied_count", typeof(long));
        table.Columns.Add("last_run_time", typeof(DateTime));
        table.Columns.Add("abandoned_count", typeof(long));
        table.Columns.Add("extension_missing_count", typeof(long));
        table.Columns.Add("last_non_skip_time", typeof(DateTime));
        table.Columns.Add("last_productive_time", typeof(DateTime));
        table.Columns.Add("last_zero_row_streak_break_time", typeof(DateTime));
        table.Columns.Add("latest_run_note", typeof(string));

        var then = DateTime.UtcNow.AddHours(-hoursAgo);
        table.Rows.Add(ServerId, collector, 100L, 100L, 0L, then, 0L, then, 0L, 0L, then, then, then, DBNull.Value);
        var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return reader;
    }

    /* ───────────────────────── the one rule ───────────────────────── */

    private static IEnumerable<(string Scenario, ScheduleOverride[] Rows)> Scenarios(string name) =>
    [
        ("no override", Array.Empty<ScheduleOverride>()),
        ("fleet only", [Override(null, name, 720)]),
        ("server only", [Override(ServerId, name, 60)]),
        ("server beats fleet", [Override(null, name, 720), Override(ServerId, name, 60)]),
        ("another server's row is ignored", [Override(OtherServerId, name, 720)]),
        ("another server's row beside the fleet's", [Override(OtherServerId, name, 30), Override(null, name, 720)]),
        ("a name in another case", [Override(ServerId, name.ToUpperInvariant(), 90)]),
        ("a negative server row falls through to the fleet's", [Override(ServerId, name, -5), Override(null, name, 720)]),
        ("a null frequency falls through", [Override(ServerId, name, null), Override(null, name, 720)]),
        ("zero is honoured (on-load only)", [Override(ServerId, name, 0)]),
        ("a delta-family cadence past its cap falls through", [Override(ServerId, name, 100_000), Override(null, name, 15)]),
        ("another collector's row is ignored", [Override(ServerId, name + "_other", 720)]),
    ];

    /// <summary>
    /// The shared rule gives, for every catalog collector under every layering of overrides, exactly what the worker
    /// schedules the collector at: <see cref="StoreConfigProvider.ResolveSchedule"/> then
    /// <see cref="CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes"/>. The viewer carries the rows as its own
    /// record and resolves them through the same rule, so it gives the same answer too.
    /// </summary>
    [Fact]
    public void TheSharedResolution_GivesWhatTheWorkerSchedulesEveryCollectorAt_ForEveryLayeringOfOverrides()
    {
        var compared = 0;
        foreach (var name in CollectorScheduleDefaults.All.Keys)
        {
            foreach (var (scenario, rows) in Scenarios(name))
            {
                var worker = CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(
                    StoreConfigProvider.ResolveSchedule(name, ServerId, rows).FrequencyMinutes);

                Assert.True(
                    worker == CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(name, ServerId, rows),
                    $"{name}: {scenario}: the shared rule disagrees with the worker's schedule");
                Assert.True(
                    worker == CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(name, ServerId, AsViewerRows(rows)),
                    $"{name}: {scenario}: the viewer's rows resolve differently from the worker's");
                compared++;
            }
        }

        Assert.Equal(CollectorScheduleDefaults.All.Count * Scenarios(FiveMinute).Count(), compared);
    }

    [Fact]
    public void TheSharedResolution_PicksTheServerRow_ThenTheFleetRow_ThenTheShippedDefault()
    {
        Assert.Equal(5, CollectorScheduleDefaults.All[FiveMinute].FrequencyMinutes);

        Assert.Equal(5, Resolve(FiveMinute));
        Assert.Equal(720, Resolve(FiveMinute, Override(null, FiveMinute, 720)));
        Assert.Equal(60, Resolve(FiveMinute, Override(null, FiveMinute, 720), Override(ServerId, FiveMinute, 60)));
        Assert.Equal(5, Resolve(FiveMinute, Override(OtherServerId, FiveMinute, 720)));
        Assert.Equal(5, CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes<ScheduleOverride>(FiveMinute, ServerId, null));
    }

    [Fact]
    public void AnOnLoadCollectorReadsAsItsDailyRecapture_AndAnUnknownNameHasNoInterval()
    {
        Assert.Equal(0, CollectorScheduleDefaults.All["server_config"].FrequencyMinutes);
        Assert.Equal(CollectorScheduleDefaults.OnLoadRecaptureMinutes, Resolve("server_config"));
        Assert.Equal(60, Resolve("server_config", Override(null, "server_config", 60)));

        Assert.Null(Resolve("not_a_collector", Override(null, "not_a_collector", 60)));
    }

    /* ───────────────────────── each surface ───────────────────────── */

    /// <summary>
    /// The fleet roll-up: the same row it reads, mapped through the step the roll-up's loop calls with the server's
    /// own schedule rows. Judged against the shipped five minutes it is STALE; against the 720 its server runs it at,
    /// HEALTHY, as get_collection_health says for that server.
    /// </summary>
    [Fact]
    public void TheFleetRollUp_BandsACollectorAgainstTheIntervalItsServerSchedulesItAt()
    {
        using (var shipped = FleetRow(FiveMinute, hoursAgo: 6))
        {
            var row = DarlingFleetReader.MapFleetHealthRow(shipped);
            Assert.Equal(5, row.FrequencyMinutes);
            Assert.Equal(CollectorHealthClassifier.Stale, row.HealthStatus);
        }

        using (var scheduled = FleetRow(FiveMinute, hoursAgo: 6))
        {
            var row = DarlingFleetReader.MapFleetHealthRow(scheduled, ServerId, [Override(ServerId, FiveMinute, 720)]);
            Assert.Equal(720, row.FrequencyMinutes);
            Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);
        }

        /* The fleet-wide row reaches the server too, and another server's own row does not. */
        using (var fleetWide = FleetRow(FiveMinute, hoursAgo: 6))
        {
            var row = DarlingFleetReader.MapFleetHealthRow(fleetWide, ServerId, [Override(null, FiveMinute, 720)]);
            Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);
        }

        using (var elsewhere = FleetRow(FiveMinute, hoursAgo: 6))
        {
            var row = DarlingFleetReader.MapFleetHealthRow(elsewhere, ServerId, [Override(OtherServerId, FiveMinute, 720)]);
            Assert.Equal(5, row.FrequencyMinutes);
            Assert.Equal(CollectorHealthClassifier.Stale, row.HealthStatus);
        }
    }

    /// <summary>
    /// The viewer's Overview cards and status bar read the per-(server, collector) fleet breakdown, and its rows are
    /// stamped by server, so the band they count is the one the server's own tab shows.
    /// </summary>
    [Fact]
    public void TheViewersFleetBreakdown_BandsACollectorAgainstTheIntervalItsServerSchedulesItAt()
    {
        using var reader = FleetRow(FiveMinute, hoursAgo: 6);
        var row = ViewerDataService.MapFleetByServerRow(reader);
        Assert.Equal(CollectorHealthClassifier.Stale, row.HealthStatus);

        ViewerDataService.ApplyScheduledFrequencies([row], ServerId, [ViewerOverride(ServerId, FiveMinute, 720)]);

        Assert.Equal(720, row.EffectiveFrequencyMinutes);
        Assert.Equal(CollectorHealthClassifier.Healthy, row.HealthStatus);
    }

    /// <summary>The viewer's per-server Collection Health tab, with a per-server row beating the fleet-wide one.</summary>
    [Fact]
    public void TheViewersServerTab_BandsACollectorAgainstTheIntervalItIsScheduledAt_ServerRowFirst()
    {
        static CollectorHealthRow Row()
        {
            var then = DateTime.UtcNow.AddHours(-6);
            return new CollectorHealthRow
            {
                CollectorName = FiveMinute,
                TotalRuns = 100,
                SuccessCount = 100,
                LastSuccessTime = then,
                LastRunTime = then,
                LastNonSkipTime = then,
                LastProductiveTime = then,
            };
        }

        var shipped = Row();
        Assert.Equal(CollectorHealthClassifier.Stale, shipped.HealthStatus);

        var server = Row();
        ViewerDataService.ApplyScheduledFrequencies([server], ServerId, [ViewerOverride(ServerId, FiveMinute, 720)]);
        Assert.Equal(720, server.EffectiveFrequencyMinutes);
        Assert.Equal(CollectorHealthClassifier.Healthy, server.HealthStatus);

        /* The fleet row alone, then the server row over it: the server's 5 beats the fleet's 720. */
        var fleet = Row();
        ViewerDataService.ApplyScheduledFrequencies([fleet], ServerId, [ViewerOverride(null, FiveMinute, 720)]);
        Assert.Equal(CollectorHealthClassifier.Healthy, fleet.HealthStatus);

        var both = Row();
        ViewerDataService.ApplyScheduledFrequencies(
            [both], ServerId, [ViewerOverride(null, FiveMinute, 720), ViewerOverride(ServerId, FiveMinute, 5)]);
        Assert.Equal(5, both.EffectiveFrequencyMinutes);
        Assert.Equal(CollectorHealthClassifier.Stale, both.HealthStatus);

        /* A name the catalog does not know is left unstamped, keeping the classifier's floor thresholds. */
        var unknown = new CollectorHealthRow { CollectorName = "not_a_collector" };
        ViewerDataService.ApplyScheduledFrequencies([unknown], ServerId, [ViewerOverride(null, "not_a_collector", 720)]);
        Assert.Null(unknown.EffectiveFrequencyMinutes);
    }

    /* ───────────────────────── the wiring ───────────────────────── */

    /// <summary>
    /// The reads that need a store cannot be driven here, so what a case can pin is that each calls the step the cases
    /// above drive, once, after its rows are built, and that nothing resolves a second way.
    /// </summary>
    [Fact]
    public void EachSurfaceStampsThroughTheOneResolution_AndNoSurfaceResolvesItsOwn()
    {
        var fleet = ReadRepoFile(FleetReaderPath);
        var viewer = ReadRepoFile(ViewerPath);
        var storeConfig = ReadRepoFile(StoreConfigPath);

        /* The fleet roll-up reads the whole schedule table once, ahead of its health statement, and maps every row
           with its server's rows. */
        Assert.Equal(1, Count(fleet, "DarlingDataReader.ReadScheduleOverridesAsync(postgres, null, cancellationToken)"));
        Assert.Equal(1, Count(fleet, "MapFleetHealthRow(reader, serverId, serverOverrides)"));
        Assert.True(
            fleet.IndexOf("DarlingDataReader.ReadScheduleOverridesAsync(postgres, null, cancellationToken)", StringComparison.Ordinal)
            < fleet.IndexOf("postgres.CreateCommand(composedSql ?? FleetCollectionHealthSql)", StringComparison.Ordinal));

        /* The viewer stamps what its two row-building reads return, and resolves only through the shared rule. */
        Assert.Equal(1, Count(viewer, "ApplyScheduledFrequencies(items, serverId, scheduleOverrides);"));
        Assert.Equal(1, Count(viewer, "ApplyScheduledFrequencies(rows, serverId, scheduleOverrides.Where("));
        Assert.Equal(1, Count(viewer, "CollectorScheduleDefaults.ResolveEffectiveIntervalMinutes(row.CollectorName, serverId, overrides)"));
        Assert.DoesNotContain("ResolveFrequencyMinutes(", viewer, StringComparison.Ordinal);

        /* The worker's own resolution picks its rows through the shared selector, not a loop of its own. */
        Assert.Equal(1, Count(storeConfig, "CollectorScheduleDefaults.SelectScheduleOverrides(collectorName, serverId, overrides)"));
        var resolve = storeConfig.IndexOf("public static EffectiveSchedule ResolveSchedule(", StringComparison.Ordinal);
        var resolveEnd = storeConfig.IndexOf("ResolveFleetRetentionDays(", resolve, StringComparison.Ordinal);
        Assert.DoesNotContain("foreach (var o in overrides)", storeConfig[resolve..resolveEnd], StringComparison.Ordinal);
    }

    private static int Count(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
