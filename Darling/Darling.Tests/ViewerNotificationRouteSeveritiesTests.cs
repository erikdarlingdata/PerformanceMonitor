/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6928 (V229): the viewer's part of the route severity filter. The store-version probe maps the new column to 229, the list names
/// the filter in words, and a route's choice round-trips through the viewer's insert, update and sealed save. A store below V229 has no
/// column, so the list still opens and every route reads as Both.
/// </summary>
/* #1776 own-store: its one live fact mints its own scratch database through ScratchPostgres and touches nothing on the shared
   one, so it is not serialized against the live-postgres collection. */
public sealed class ViewerNotificationRouteSeveritiesTests
{
    /// <summary>The first sixteen sentinels, all present (the V16 to V31 floor), so only the one named after them decides the answer.</summary>
    private static int Map(bool hasRouteSeverities) => ViewerDataService.MapProbedSchemaVersion(
        true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
        hasRouteSeverities: hasRouteSeverities);

    [Fact]
    public void TheProbeSentinel_MapsTheRouteSeveritiesColumn_ToV229_AndItsAbsence_ToAnOlderVersion()
    {
        Assert.Equal(229, Map(true));
        Assert.True(Map(false) < 229);
        Assert.Equal(229, ViewerDataService.RouteSeveritiesStoreSchemaVersion);
    }

    [Theory]
    [InlineData("both", "Both")]
    [InlineData("critical", "Critical only")]
    [InlineData("warning", "Warning only")]
    [InlineData("CRITICAL", "Critical only")]
    [InlineData("", "Both")]
    [InlineData("something-new", "Both")]
    public void TheGridWords_NameTheFilter_AndAnUnknownWordReadsAsBoth(string stored, string expected) =>
        Assert.Equal(expected, new NotificationRouteRow { Severities = stored }.SeveritiesDisplay);

    [Fact]
    public void ANewRoute_DefaultsToBoth_AndTheCloneAndTheDraftKeepTheChoice()
    {
        Assert.Equal(NotificationRouteKinds.SeveritiesBoth, new NotificationRouteRow().Severities);
        var row = new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "a@example.invalid", Severities = "critical" };
        Assert.Equal("critical", row.Clone().Severities);
        Assert.Equal("critical", row.ToDraft().Severities);
        Assert.Null(ViewerDataService.ValidateNotificationRoute(row));
        Assert.NotNull(ViewerDataService.ValidateNotificationRoute(new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "a@example.invalid", Severities = "major" }));
    }

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task Live_TheSeverityRoundTripsThroughTheViewer_AndAStoreBelowV229StillOpensTheList()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the viewer's route severity round trip.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var key = PasswordPrivateKey.Generate();
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
            await ExecAsync(connection, "INSERT INTO config.config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);

            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                Assert.True(await viewer.GetStoreSchemaVersionAsync(ct) >= 229);

                var inserted = await viewer.InsertNotificationRouteAsync(
                    new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "a@example.invalid", Severities = "critical" }, ct);
                var plain = await viewer.InsertNotificationRouteAsync(
                    new NotificationRouteRow { MetricMatch = "reports", SmtpRecipients = "b@example.invalid" }, ct);
                var sealedId = await viewer.SaveNotificationRouteSealedAsync(
                    new NotificationRouteRow { MetricMatch = "agent-jobs", SlackUrl = "https://hooks.example.invalid/w", Severities = "warning" },
                    null, new NotificationRow(), new ViewerPasswordSealer(key.PublicKey), null, ct);

                var read = await viewer.GetNotificationRoutesAsync(ct);
                Assert.Equal("critical", read.Single(r => r.RouteId == inserted).Severities);
                Assert.Equal("both", read.Single(r => r.RouteId == plain).Severities);
                Assert.Equal("warning", read.Single(r => r.RouteId == sealedId).Severities);
                Assert.Equal("Critical only", read.Single(r => r.RouteId == inserted).SeveritiesDisplay);

                var edited = read.Single(r => r.RouteId == inserted).Clone();
                edited.Severities = "warning";
                await viewer.UpdateNotificationRouteAsync(edited, ct);
                Assert.Equal("warning", (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == inserted).Severities);

                await Assert.ThrowsAsync<ArgumentException>(() => viewer.InsertNotificationRouteAsync(
                    new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "c@example.invalid", Severities = "major" }, ct));
            }

            /* A store below V229 has no column, and the V230, V232 and V233 columns go too: the version probe reads newest first and would still answer 230 without them. The list still reads, and every route is Both. */
            await ExecAsync(connection, "ALTER TABLE config.config_alert_settings DROP COLUMN blocking_warn_snapshot_wait_seconds, DROP COLUMN blocking_critical_snapshot_wait_seconds, DROP COLUMN daily_report_enabled, DROP COLUMN daily_report_send_at, DROP COLUMN daily_report_time_zone, DROP COLUMN daily_report_title", ct);
            await ExecAsync(connection, "ALTER TABLE config.config_notification_routes DROP COLUMN severities, DROP COLUMN report_notice", ct);
            /* The probe reads newest first, so the mute schedule columns (V231, #7040) go too, or the store would still read as V231. */
            await ExecAsync(connection, "ALTER TABLE config.config_mute_rules DROP COLUMN IF EXISTS recur_days, DROP COLUMN IF EXISTS recur_start_time, DROP COLUMN IF EXISTS recur_end_time, DROP COLUMN IF EXISTS recur_time_zone", ct);
            await using (var older = new ViewerDataService(scratch.ConnectionString))
            {
                Assert.True(await older.GetStoreSchemaVersionAsync(ct) < 229);
                var rows = await older.GetNotificationRoutesAsync(ct);
                Assert.Equal(3, rows.Count);
                Assert.All(rows, r => Assert.Equal("both", r.Severities));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
        }
    }
}
