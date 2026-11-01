/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6979 (V233): the viewer's part of the route report notice setting. The edit dialog offers the three choices in plain words, opens on the
/// stored one (Off for a new route), and Save puts the chosen one on the route. The list names the setting, the clone and the draft keep it,
/// and the store probe maps the column to 233. A store below V233 has no column, so the list still opens and every route reads as Off.
/// </summary>
/* #1776 own-store: its one live fact mints its own scratch database through ScratchPostgres and touches nothing on the shared
   one, so it is not serialized against the live-postgres collection. */
public sealed class ViewerNotificationRouteReportNoticeTests
{
    private static NotificationRouteRow Row(string reportNotice) => new()
    {
        RouteId = 5,
        MetricMatch = "Blocking",
        SmtpRecipients = "a@example.invalid",
        ReportNotice = reportNotice,
    };

    [Theory]
    [InlineData("off", "Off")]
    [InlineData("needs_person_or_change", "Needs a person or made a change")]
    [InlineData("all", "All reports")]
    [InlineData("someday", "Off")]
    [InlineData("", "Off")]
    public void TheGridWords_NameTheSetting_AndAnUnknownWordReadsAsOff(string stored, string expected) =>
        Assert.Equal(expected, new NotificationRouteRow { ReportNotice = stored }.ReportNoticeDisplay);

    [Fact]
    public void ANewRoute_DefaultsToOff_AndTheCloneAndTheDraftKeepTheChoice()
    {
        Assert.Equal(NotificationRouteKinds.ReportNoticeOff, new NotificationRouteRow().ReportNotice);
        var row = Row("all");
        Assert.Equal("all", row.Clone().ReportNotice);
        Assert.Equal("all", row.ToDraft().ReportNotice);
        Assert.Null(ViewerDataService.ValidateNotificationRoute(row));
        Assert.NotNull(ViewerDataService.ValidateNotificationRoute(Row("sometimes")));
    }

    [Fact]
    public void TheProbeSentinel_MapsTheReportNoticeColumn_ToV233_AndItsAbsence_ToAnOlderVersion()
    {
        static int Map(bool hasReportNotice) => ViewerDataService.MapProbedSchemaVersion(
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            hasReportNotice: hasReportNotice);
        Assert.Equal(233, Map(true));
        Assert.True(Map(false) < 233);
    }

    [Fact]
    public void TheDialog_OffersThePlainWordChoices_OpensOnTheStoredOne_AndANewRouteOpensOnOff()
    {
        StaTestThread.Run(() =>
        {
            var shown = new NotificationRouteEditDialog(null, null);
            var combo = (ComboBox)shown.FindName("ReportNoticeCombo");
            Assert.Equal(
                new[] { "Off", "Needs a person or made a change", "All reports" },
                combo.Items.Cast<ComboBoxItem>().Select(i => (string)i.Content).ToArray());
            Assert.Equal("off", ((ComboBoxItem)combo.SelectedItem).Tag);

            foreach (var stored in new[] { "off", "needs_person_or_change", "all" })
            {
                var dialog = new NotificationRouteEditDialog(null, Row(stored));
                var opened = (ComboBox)dialog.FindName("ReportNoticeCombo");
                Assert.Equal(stored, ((ComboBoxItem)opened.SelectedItem).Tag);
            }

            /* A word this build does not know opens as Off, so saving the route never turns a notice on. */
            var unknown = new NotificationRouteEditDialog(null, Row("someday"));
            Assert.Equal("off", ((ComboBoxItem)((ComboBox)unknown.FindName("ReportNoticeCombo")).SelectedItem).Tag);
        });
    }

    [Theory]
    [InlineData("off")]
    [InlineData("needs_person_or_change")]
    [InlineData("all")]
    public void Save_PutsTheChosenReportNoticeOnTheRoute(string choice)
    {
        StaTestThread.Run(() =>
        {
            /* The route opens on the other end of the list, so the test proves the box was read and not the stored value kept. */
            var dialog = new NotificationRouteEditDialog(null, Row(choice == "all" ? "off" : "all"));
            var combo = (ComboBox)dialog.FindName("ReportNoticeCombo");
            combo.SelectedItem = combo.Items.Cast<ComboBoxItem>().Single(i => Equals(i.Tag, choice));

            var save = typeof(NotificationRouteEditDialog).GetMethod("Save_Click", BindingFlags.Instance | BindingFlags.NonPublic)!;
            try
            {
                save.Invoke(dialog, new object?[] { null, new RoutedEventArgs() });
            }
            catch (TargetInvocationException ex) when (ex.InnerException is InvalidOperationException)
            {
                /* A dialog that was never shown cannot take a DialogResult: that is the last statement of a valid save. */
            }

            Assert.Equal(Visibility.Collapsed, ((TextBlock)dialog.FindName("ValidationText")).Visibility);
            Assert.Equal(choice, dialog.Route.ReportNotice);
        });
    }

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task Live_TheReportNoticeRoundTripsThroughTheViewer_AndAStoreBelowV233StillOpensTheList()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the viewer's route report notice round trip.");

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
                Assert.True(await viewer.GetStoreSchemaVersionAsync(ct) >= 233);

                var inserted = await viewer.InsertNotificationRouteAsync(
                    new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "a@example.invalid", ReportNotice = "needs_person_or_change" }, ct);
                var plain = await viewer.InsertNotificationRouteAsync(
                    new NotificationRouteRow { MetricMatch = "reports", SmtpRecipients = "b@example.invalid" }, ct);
                var sealedId = await viewer.SaveNotificationRouteSealedAsync(
                    new NotificationRouteRow { MetricMatch = "agent-jobs", SlackUrl = "https://hooks.example.invalid/w", ReportNotice = "all" },
                    null, new NotificationRow(), new ViewerPasswordSealer(key.PublicKey), null, ct);

                var read = await viewer.GetNotificationRoutesAsync(ct);
                Assert.Equal("needs_person_or_change", read.Single(r => r.RouteId == inserted).ReportNotice);
                Assert.Equal("off", read.Single(r => r.RouteId == plain).ReportNotice);
                Assert.Equal("all", read.Single(r => r.RouteId == sealedId).ReportNotice);
                Assert.Equal("All reports", read.Single(r => r.RouteId == sealedId).ReportNoticeDisplay);

                var edited = read.Single(r => r.RouteId == inserted).Clone();
                edited.ReportNotice = "all";
                await viewer.UpdateNotificationRouteAsync(edited, ct);
                Assert.Equal("all", (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == inserted).ReportNotice);

                await Assert.ThrowsAsync<ArgumentException>(() => viewer.InsertNotificationRouteAsync(
                    new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "c@example.invalid", ReportNotice = "sometimes" }, ct));
            }

            /* A store below V233 has no column. The list reads the older statement, and every route is Off. The severity column stays, so
               this also proves the V229 statement is still the one used below V233. */
            await ExecAsync(connection, "ALTER TABLE config.config_notification_routes DROP COLUMN report_notice", ct);
            /* The probe reads newest first, so the alert key column (V234, #7076) goes too, or the store would still read as V234. */
            await ExecAsync(connection, "ALTER TABLE config.config_alert_log DROP COLUMN alert_key", ct);
            /* And the people tables (V237, #7107), the newest sentinel. */
            await ExecAsync(connection, "DROP TABLE config.person_grant, config.person_identity, config.person", ct);
            await using (var older = new ViewerDataService(scratch.ConnectionString))
            {
                Assert.True(await older.GetStoreSchemaVersionAsync(ct) < 233);
                var rows = await older.GetNotificationRoutesAsync(ct);
                Assert.Equal(3, rows.Count);
                Assert.All(rows, r => Assert.Equal("off", r.ReportNotice));
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
