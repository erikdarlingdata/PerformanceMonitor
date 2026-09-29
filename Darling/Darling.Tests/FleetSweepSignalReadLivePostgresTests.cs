/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches
   nothing on the shared one, so serializing it against the live-postgres collection would cost suite time
   and buy no isolation. */

/// <summary>
/// The fleet sweep's per-server signal read against a real store (#4747): the engine's tests build their
/// readings by hand, so nothing else drives <c>FleetSweepEngine.ReadServerSignalsAsync</c> through the
/// daily-summary statement. The case that matters is an outage span — a server that cannot be reached
/// writes no collection-log row, yet its "Collection Stopped" alert keeps firing, so the day spine holds
/// the span as a row that has alerts and no collector runs.
/// </summary>
public sealed class FleetSweepSignalReadLivePostgresTests
{
    private const int OutageServerId = 947401;
    private const string OutageServerName = "outage-server";
    private const int CollectingServerId = 947402;
    private const string CollectingServerName = "collecting-server";

    [Fact]
    public async Task AnOutageSpanHoldingOnlyCollectionStoppedAlerts_BandsNoData_AndIsNotCountedAsReported()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet-sweep signal read (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(dataSourceConnectionString);

        var spanEnd = new DateTime(2026, 9, 15, 12, 0, 0, DateTimeKind.Utc);
        var spanStart = spanEnd.AddHours(-1);

        await using (var seed = await postgres.OpenConnectionAsync(ct))
        {
            await DarlingMcpTestData.RegisterServerAsync(seed, OutageServerId, OutageServerName, ct);
            await DarlingMcpTestData.RegisterServerAsync(seed, CollectingServerId, CollectingServerName, ct);

            /* The outage: three "Collection Stopped" fires inside the span and NO collection-log row —
               a failed connect writes none. */
            foreach (var minute in new[] { 30, 35, 40 })
            {
                await InsertAlertAsync(seed, ct, spanStart.AddMinutes(minute), OutageServerId, OutageServerName, "Collection Stopped");
            }

            /* The control: one collector run inside the span, no alerts — a span that IS data. */
            await DarlingMcpTestData.ExecAsync(seed, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, $4, $5, $6)",
                CollectionIdGenerator.Next(), CollectingServerId, CollectingServerName, "wait_stats",
                DarlingMcpTestData.Naive(spanStart.AddMinutes(20)), "SUCCESS");
        }

        var outage = await FleetSweepEngine.ReadServerSignalsAsync(
            postgres, OutageServerId, OutageServerName, spanStart, spanEnd, ct);
        var collecting = await FleetSweepEngine.ReadServerSignalsAsync(
            postgres, CollectingServerId, CollectingServerName, spanStart, spanEnd, ct);

        Assert.Null(outage.ReadFault);
        Assert.Null(collecting.ReadFault);

        /* The shape that used to read as data: the spine holds the span (its alerts count) although the
           collectors ran zero times inside it. */
        Assert.Equal(3, outage.Signals.AlertCount);
        Assert.Equal(0, outage.Signals.CollectionRuns);
        Assert.False(outage.Signals.HasData);

        Assert.Equal(1, collecting.Signals.CollectionRuns);
        Assert.True(collecting.Signals.HasData);

        var composition = FleetSweepEngine.Compose(
            spanEnd,
            spanStart,
            alertsEnabled: true,
            serversExpected: 2,
            new[] { outage, collecting },
            previousRun: null,
            Array.Empty<FleetSweepServerVerdict>(),
            Array.Empty<FleetSweepWatchItem>(),
            new FleetSweepInstrumentCounters(spanEnd.AddHours(-6), AlertPassesTotal: 500, AlertReadFailuresTotal: 0),
            DeadlockRateThresholds.Default);

        /* No Data, not Warning with an alert count. */
        var outageVerdict = composition.Verdicts.Single(v => v.ServerId == OutageServerId);
        Assert.Equal(DailyHealthBandCalculator.Label(DailyHealthBand.NoData), outageVerdict.Band);

        /* Only the server that collected reported, and only the silent one is stale. */
        Assert.Equal(1, composition.Run.ServersReported);
        Assert.Contains(composition.WatchItems,
            w => w.ServerId == OutageServerId && w.ItemKey == FleetSweepEngine.CollectionStaleItemKey);
        Assert.DoesNotContain(composition.WatchItems,
            w => w.ServerId == CollectingServerId && w.ItemKey == FleetSweepEngine.CollectionStaleItemKey);
    }

    private static Task InsertAlertAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime alertTimeUtc, int serverId, string serverName, string metric) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 0, 0, FALSE, 'none', NULL, FALSE, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(alertTimeUtc), serverId, serverName, metric);
}
