/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4755 gated live round-trips (DARLING_TEST_PG) for how the alert notebook finds an alert's resolution or its
/// later firing. Status arms 1 and 2 used to scan a 24-hour, 200-row, newest-first, dismissed-excluded window of
/// <c>config_alert_log</c>, so the notebook said "Fired again" or "Unknown" for an alert that had cleared when the
/// Cleared row landed more than 24 hours later, had been dismissed, or sat behind 200 newer rows (a fleet-level
/// alert has no server id, so one cap covered every server). The status now comes from two targeted reads
/// (<see cref="AlertNotebookEndpoint.ReadStatusRowsAsync"/>) fed to the same
/// <see cref="AlertNotebookEndpoint.ResolveStatusAsync"/> the endpoint calls. Every row is tagged with a
/// per-class server name and deleted in cleanup, so the shared store is left as it was found.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertNotebookResolutionReadLiveTests
{
    private const string ServerNamePrefix = "alert-notebook-resolution-4755";
    private const int StoreServerId = -475501;
    private const int ServerA = -475502;
    private const int ServerB = -475503;
    private const int OtherServerBase = -475510;

    private const string StoreMetric = DarlingSelfAlertEvaluator.DiskPressureMetric;
    private const string StoreResolvedMetric = DarlingSelfAlertEvaluator.DiskPressureResolvedMetric;

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the alert-notebook resolution-read live tests.");
        return connectionString!;
    }

    private static async Task<NpgsqlConnection> OpenMigratedAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        return connection;
    }

    private static NpgsqlDataSource OpenDataSource(string connectionString) =>
        NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);

    /// <summary>Naive-UTC whole-second anchor <paramref name="hoursAgo"/> hours in the past, so the seeded rows
    /// stay in the past and the wire text ("Resolved at T") round-trips exactly.</summary>
    private static DateTime AnchorHoursAgo(int hoursAgo) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddHours(-hoursAgo));

    private static string Stamp(DateTime value) => value.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);

    private static Task InsertAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime alertTimeUtc, int serverId, string serverSuffix,
        string metric, bool dismissed = false) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 0, 0, FALSE, 'none', NULL, FALSE, $5, NULL, NULL)",
            DarlingMcpTestData.Naive(alertTimeUtc), serverId, ServerNamePrefix + serverSuffix, metric, dismissed);

    private static DarlingAlertReader.AlertHistoryReadRow MatchedRow(DateTime alertTime, int serverId, string metric) =>
        new(alertTime, serverId, ServerNamePrefix, metric, 0, 0, false, "none", null, false, null, false);

    /// <summary>The endpoint's own pair: the two targeted reads, then the status arms over what they returned.</summary>
    private static async Task<string> StatusAsync(
        NpgsqlDataSource postgres, int? serverId, bool fleetLevelStore, string metric, DateTime anchor,
        DarlingAlertReader.AlertHistoryReadRow? matchedRow, CancellationToken ct)
    {
        var now = DateTime.UtcNow;
        var rows = await AlertNotebookEndpoint.ReadStatusRowsAsync(
            postgres, serverId, fleetLevelStore, metric, anchor, now, matchedRow, ct);
        return await AlertNotebookEndpoint.ResolveStatusAsync(
            postgres, serverId, fleetLevelStore, metric, anchor, now, rows, matchedRow, NullLogger.Instance, ct);
    }

    private static Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE server_name LIKE $1", ServerNamePrefix + "%");

    /* ═══════════════════════════ #4755: the resolution read ═══════════════════════════ */

    [Fact]
    public async Task StoreSelfAlert_ClearedThirtyHoursLater_ReadsResolved()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = AnchorHoursAgo(40);
            await InsertAsync(connection, ct, anchor, StoreServerId, "-store", StoreMetric);
            /* Outside the old 24-hour window: the read that found nothing here reported "Unknown (not
               collected since ...)" for a store alert, which has no collector to have stopped. */
            await InsertAsync(connection, ct, anchor.AddHours(30), StoreServerId, "-store", StoreResolvedMetric);

            await using var postgres = OpenDataSource(cs);
            var status = await StatusAsync(
                postgres, serverId: null, fleetLevelStore: true, StoreMetric, anchor,
                MatchedRow(anchor, StoreServerId, StoreMetric), ct);

            Assert.Equal("Resolved at " + Stamp(anchor.AddHours(30)), status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task StoreSelfAlert_ClearedRowDismissed_StillReadsResolved()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = AnchorHoursAgo(6);
            await InsertAsync(connection, ct, anchor, StoreServerId, "-store", StoreMetric);
            /* The viewer's "Dismiss all" marks resolution rows dismissed too. The Cleared row is well inside the
               old window, so this isolates the dismissed filter as the only reason the old read missed it. */
            await InsertAsync(connection, ct, anchor.AddHours(2), StoreServerId, "-store", StoreResolvedMetric, dismissed: true);

            await using var postgres = OpenDataSource(cs);
            var status = await StatusAsync(
                postgres, serverId: null, fleetLevelStore: true, StoreMetric, anchor,
                MatchedRow(anchor, StoreServerId, StoreMetric), ct);

            Assert.Equal("Resolved at " + Stamp(anchor.AddHours(2)), status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task FleetLevelAlert_With250NewerRowsFromOtherServers_StillFindsTheResolution()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = AnchorHoursAgo(40);
            await InsertAsync(connection, ct, anchor, StoreServerId, "-store", StoreMetric);
            await InsertAsync(connection, ct, anchor.AddHours(1), StoreServerId, "-store", StoreResolvedMetric);

            /* 250 newer rows from other servers, all inside the first 24 hours after the anchor: a fleet-level
               alert has no server id, so the old read's single newest-first 200-row cap dropped the OLDEST rows,
               which is exactly where the resolution sits. Server ids vary so no per-server shape hides it. */
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
SELECT $1::timestamp + (g * interval '4 minutes'), $2 - (g % 5), $3, 'High CPU', 0, 0,
       FALSE, 'none', NULL, FALSE, FALSE, NULL, NULL
FROM generate_series(1, 250) AS g",
                DarlingMcpTestData.Naive(anchor.AddHours(2)), OtherServerBase, ServerNamePrefix + "-other");

            await using var postgres = OpenDataSource(cs);
            var status = await StatusAsync(
                postgres, serverId: null, fleetLevelStore: true, StoreMetric, anchor,
                MatchedRow(anchor, StoreServerId, StoreMetric), ct);

            Assert.Equal("Resolved at " + Stamp(anchor.AddHours(1)), status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ServerScopedAlert_IgnoresAnotherServersResolutionRow()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = AnchorHoursAgo(6);
            await InsertAsync(connection, ct, anchor, ServerA, "-a", "High CPU");
            await InsertAsync(connection, ct, anchor.AddHours(1), ServerB, "-b", "CPU Resolved");

            await using var postgres = OpenDataSource(cs);
            var status = await StatusAsync(
                postgres, serverId: ServerA, fleetLevelStore: false, "High CPU", anchor,
                MatchedRow(anchor, ServerA, "High CPU"), ct);

            /* Server B's Cleared row is the only resolution in the store; server A's alert has none. */
            Assert.DoesNotContain("Resolved at", status, StringComparison.Ordinal);
            Assert.StartsWith("Unknown (not collected since ", status, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ServerScopedAlert_ReadsItsOwnResolution_NotAnEarlierOneFromAnotherServer()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = AnchorHoursAgo(40);
            await InsertAsync(connection, ct, anchor, ServerA, "-a", "High CPU");
            await InsertAsync(connection, ct, anchor.AddHours(1), ServerB, "-b", "CPU Resolved");
            /* Server A's own Cleared row: past the old 24-hour window AND dismissed. */
            await InsertAsync(connection, ct, anchor.AddHours(30), ServerA, "-a", "CPU Resolved", dismissed: true);

            await using var postgres = OpenDataSource(cs);
            var status = await StatusAsync(
                postgres, serverId: ServerA, fleetLevelStore: false, "High CPU", anchor,
                MatchedRow(anchor, ServerA, "High CPU"), ct);

            Assert.Equal("Resolved at " + Stamp(anchor.AddHours(30)), status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task LowerCaseMetricInTheLink_StillFindsTheResolution_ByTheProductsOwnSpelling()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = AnchorHoursAgo(6);
            await InsertAsync(connection, ct, anchor, StoreServerId, "-store", StoreMetric);
            await InsertAsync(connection, ct, anchor.AddHours(1), StoreServerId, "-store", StoreResolvedMetric);

            await using var postgres = OpenDataSource(cs);
            var status = await StatusAsync(
                postgres, serverId: null, fleetLevelStore: true, "  store disk pressure ", anchor, matchedRow: null, ct);

            Assert.Equal("Resolved at " + Stamp(anchor.AddHours(1)), status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task ResolutionRowsAtOrBeforeTheAnchor_AreNotTheAlertsResolution()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = AnchorHoursAgo(6);
            await InsertAsync(connection, ct, anchor, StoreServerId, "-store", StoreMetric);
            /* A Cleared row from the PREVIOUS episode, and one stamped exactly at the anchor: neither is after
               the alert, so neither is its resolution. */
            await InsertAsync(connection, ct, anchor.AddHours(-1), StoreServerId, "-store", StoreResolvedMetric);
            await InsertAsync(connection, ct, anchor, StoreServerId, "-store", StoreResolvedMetric);

            await using var postgres = OpenDataSource(cs);
            var status = await StatusAsync(
                postgres, serverId: null, fleetLevelStore: true, StoreMetric, anchor,
                MatchedRow(anchor, StoreServerId, StoreMetric), ct);

            /* #4756: nothing after the alert, and a store alert has no collector to blame. */
            Assert.Equal("No resolution recorded", status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    /* ═══════════════════════════ #4755: the re-fire read ═══════════════════════════ */

    [Fact]
    public async Task LaterFiring_ThirtyHoursLater_AndDismissed_ReadsFiredAgain()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = AnchorHoursAgo(40);
            await InsertAsync(connection, ct, anchor, StoreServerId, "-store", StoreMetric);
            await InsertAsync(connection, ct, anchor.AddHours(30), StoreServerId, "-store", StoreMetric, dismissed: true);

            await using var postgres = OpenDataSource(cs);
            var status = await StatusAsync(
                postgres, serverId: null, fleetLevelStore: true, StoreMetric, anchor,
                MatchedRow(anchor, StoreServerId, StoreMetric), ct);

            Assert.Equal("Fired again at " + Stamp(anchor.AddHours(30)), status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task TheMatchedRowItself_IsNotItsOwnRefire()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            /* The link's `at` sits 5 minutes BEFORE the stored row (the match window reaches 15 minutes
               forward), so the matched row itself is after the anchor and must be skipped by the re-fire read. */
            var anchor = AnchorHoursAgo(40);
            var matchedAt = anchor.AddMinutes(5);
            await InsertAsync(connection, ct, matchedAt, StoreServerId, "-store", StoreMetric);

            await using var postgres = OpenDataSource(cs);
            var alone = await StatusAsync(
                postgres, serverId: null, fleetLevelStore: true, StoreMetric, anchor,
                MatchedRow(matchedAt, StoreServerId, StoreMetric), ct);

            Assert.Equal("No resolution recorded", alone);

            await InsertAsync(connection, ct, anchor.AddHours(30), StoreServerId, "-store", StoreMetric);
            var withRefire = await StatusAsync(
                postgres, serverId: null, fleetLevelStore: true, StoreMetric, anchor,
                MatchedRow(matchedAt, StoreServerId, StoreMetric), ct);

            Assert.Equal("Fired again at " + Stamp(anchor.AddHours(30)), withRefire);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }

    [Fact]
    public async Task LowerCaseMetricInTheLink_RefireIsFoundThroughTheMatchedRowsStoredSpelling()
    {
        var cs = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await OpenMigratedAsync(cs, ct);

        var bodySucceeded = false;
        try
        {
            var anchor = AnchorHoursAgo(6);
            await InsertAsync(connection, ct, anchor, StoreServerId, "-store", StoreMetric);
            await InsertAsync(connection, ct, anchor.AddHours(2), StoreServerId, "-store", StoreMetric);

            await using var postgres = OpenDataSource(cs);
            var status = await StatusAsync(
                postgres, serverId: null, fleetLevelStore: true, "store disk pressure", anchor,
                MatchedRow(anchor, StoreServerId, StoreMetric), ct);

            Assert.Equal("Fired again at " + Stamp(anchor.AddHours(2)), status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, DeleteRowsAsync);
        }
    }
}
