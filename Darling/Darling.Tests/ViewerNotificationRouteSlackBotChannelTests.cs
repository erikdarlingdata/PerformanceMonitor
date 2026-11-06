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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7656 (V252): the viewer's part of a route's Slack bot channel. The edit dialog opens on the stored id (blank for a new route), Save puts
/// the trimmed text on the route and refuses a channel name with the one sentence. The clone and the draft keep the id, and the store probe
/// maps the column to 252. A store below V252 has no column, so the list still opens and every route reads as none.
/// </summary>
/* #1776 own-store: its one live fact mints its own scratch database through ScratchPostgres and touches nothing on the shared
   one, so it is not serialized against the live-postgres collection. */
public sealed class ViewerNotificationRouteSlackBotChannelTests
{
    private const string Channel = "C0123ABCDEF";
    private const string OtherChannel = "G9ZZZ";

    private static NotificationRouteRow Row(string channel) => new()
    {
        RouteId = 5,
        MetricMatch = "Blocking",
        SmtpRecipients = "a@example.invalid",
        SlackBotChannel = channel,
    };

    [Fact]
    public void ANewRoute_HasNoBotChannel_AndTheCloneAndTheDraftKeepIt()
    {
        Assert.Equal("", new NotificationRouteRow().SlackBotChannel);
        var row = Row(Channel);
        Assert.Equal(Channel, row.Clone().SlackBotChannel);
        Assert.Equal(Channel, row.ToDraft().SlackBotChannel);
        Assert.Null(ViewerDataService.ValidateNotificationRoute(row));
        Assert.Equal(SlackChannelId.RefusalText, ViewerDataService.ValidateNotificationRoute(Row("#ops")));
    }

    [Fact]
    public void TheStoreVersionThatAddsTheColumn_Is252() =>
        Assert.Equal(252, ViewerDataService.RouteSlackBotChannelStoreSchemaVersion);

    [Fact]
    public void TheDialog_OpensOnTheStoredChannel_AndANewRouteOpensBlank()
    {
        StaTestThread.Run(() =>
        {
            var shown = new NotificationRouteEditDialog(null, null);
            Assert.Equal("", ((TextBox)shown.FindName("SlackBotChannelBox")).Text);

            var dialog = new NotificationRouteEditDialog(null, Row(Channel));
            Assert.Equal(Channel, ((TextBox)dialog.FindName("SlackBotChannelBox")).Text);
        });
    }

    [Theory]
    [InlineData(OtherChannel, OtherChannel)]
    [InlineData("  " + OtherChannel + "  ", OtherChannel)]
    [InlineData("", "")]
    public void Save_PutsTheTrimmedChannelOnTheRoute(string typed, string expected)
    {
        StaTestThread.Run(() =>
        {
            /* The route opens on another id, so the test proves the box was read and not the stored value kept. */
            var dialog = new NotificationRouteEditDialog(null, Row(Channel));
            ((TextBox)dialog.FindName("SlackBotChannelBox")).Text = typed;

            InvokeSave(dialog);

            Assert.Equal(Visibility.Collapsed, ((TextBlock)dialog.FindName("ValidationText")).Visibility);
            Assert.Equal(expected, dialog.Route.SlackBotChannel);
        });
    }

    [Theory]
    [InlineData("#ops")]
    [InlineData("c0123abc")]
    [InlineData("U0123ABC")]
    public void Save_RefusesAChannelName_WithTheSentence(string typed)
    {
        StaTestThread.Run(() =>
        {
            var dialog = new NotificationRouteEditDialog(null, Row(Channel));
            ((TextBox)dialog.FindName("SlackBotChannelBox")).Text = typed;

            InvokeSave(dialog);

            var validation = (TextBlock)dialog.FindName("ValidationText");
            Assert.Equal(Visibility.Visible, validation.Visibility);
            Assert.Contains(SlackChannelId.RefusalText, validation.Text, StringComparison.Ordinal);
        });
    }

    private static void InvokeSave(NotificationRouteEditDialog dialog)
    {
        var save = typeof(NotificationRouteEditDialog).GetMethod("Save_Click", BindingFlags.Instance | BindingFlags.NonPublic)!;
        try
        {
            save.Invoke(dialog, new object?[] { null, new RoutedEventArgs() });
        }
        catch (TargetInvocationException ex) when (ex.InnerException is InvalidOperationException)
        {
            /* A dialog that was never shown cannot take a DialogResult: that is the last statement of a valid save. */
        }
    }

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task Live_TheBotChannelRoundTripsThroughTheViewer_AndAStoreBelowV252StillOpensTheList()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the viewer's route bot channel round trip.");

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
                Assert.True(await viewer.GetStoreSchemaVersionAsync(ct) >= 252);

                var inserted = await viewer.InsertNotificationRouteAsync(
                    new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "a@example.invalid", SlackBotChannel = "  " + Channel + " " }, ct);
                var plain = await viewer.InsertNotificationRouteAsync(
                    new NotificationRouteRow { MetricMatch = "reports", SmtpRecipients = "b@example.invalid" }, ct);
                var sealedId = await viewer.SaveNotificationRouteSealedAsync(
                    new NotificationRouteRow { MetricMatch = "agent-jobs", SlackUrl = "https://hooks.example.invalid/w", SlackBotChannel = OtherChannel },
                    null, new NotificationRow(), new ViewerPasswordSealer(key.PublicKey), null, ct);

                var read = await viewer.GetNotificationRoutesAsync(ct);
                Assert.Equal(Channel, read.Single(r => r.RouteId == inserted).SlackBotChannel);
                Assert.Equal("", read.Single(r => r.RouteId == plain).SlackBotChannel);
                Assert.Equal(OtherChannel, read.Single(r => r.RouteId == sealedId).SlackBotChannel);

                var edited = read.Single(r => r.RouteId == inserted).Clone();
                edited.SlackBotChannel = OtherChannel;
                await viewer.UpdateNotificationRouteAsync(edited, ct);
                Assert.Equal(OtherChannel, (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == inserted).SlackBotChannel);

                var cleared = (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == sealedId).Clone();
                cleared.SlackBotChannel = "";
                await viewer.UpdateNotificationRouteAsync(cleared, ct);
                Assert.Equal("", (await viewer.GetNotificationRoutesAsync(ct)).Single(r => r.RouteId == sealedId).SlackBotChannel);

                await Assert.ThrowsAsync<ArgumentException>(() => viewer.InsertNotificationRouteAsync(
                    new NotificationRouteRow { MetricMatch = "performance", SmtpRecipients = "c@example.invalid", SlackBotChannel = "#ops" }, ct));
            }

            /* A store below V252 has no column. The list reads the V238 statement, and every route reads as none. The column is dropped as well
               as the sentinels, so a list that still asked for it would fail instead of reading the dropped value. */
            /* The probe reads newest first, so every later sentinel goes too, or the store would still read as the newest one left on disk. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 251, ct);
            await ExecAsync(connection, "ALTER TABLE config.config_notification_routes DROP COLUMN slack_bot_channel", ct);
            await using (var older = new ViewerDataService(scratch.ConnectionString))
            {
                Assert.True(await older.GetStoreSchemaVersionAsync(ct) < 252);
                var rows = await older.GetNotificationRoutesAsync(ct);
                Assert.Equal(3, rows.Count);
                Assert.All(rows, r => Assert.Equal("", r.SlackBotChannel));
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
