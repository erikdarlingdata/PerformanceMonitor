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
/// #7083 (V236): the viewer's part of a mute rule's action. The edit dialog offers Mute and the daily digest, opens on the stored one (Mute for
/// a new rule), and Save puts the chosen one on the rule. The probe maps the column to 236, and a rule round-trips through the viewer's insert,
/// update and read. A store below V236 still lists its rules and refuses a digest rule with an upgrade message.
/// </summary>
/* #1776 own-store: its one live fact mints its own scratch database through ScratchPostgres and touches nothing on the shared
   one, so it is not serialized against the live-postgres collection. */
public sealed class ViewerMuteRuleActionTests
{
    private static MuteRule Rule(string action) => new() { MetricName = "High CPU", ServerName = "srv", Reason = "r", Action = action };

    [Fact]
    public void TheDialog_OffersMuteAndTheDigest_OpensOnTheStoredOne_AndANewRuleOpensOnMute()
    {
        StaTestThread.Run(() =>
        {
            var shown = new MuteRuleEditDialog();
            var combo = (ComboBox)shown.FindName("ActionBox");
            Assert.Equal(new[] { "mute", "digest" }, combo.Items.Cast<ComboBoxItem>().Select(i => (string)i.Tag).ToArray());
            Assert.Equal("mute", ((ComboBoxItem)combo.SelectedItem).Tag);

            foreach (var stored in new[] { MuteRuleActions.Mute, MuteRuleActions.Digest })
            {
                var dialog = new MuteRuleEditDialog(Rule(stored));
                var opened = (ComboBox)dialog.FindName("ActionBox");
                Assert.Equal(stored, ((ComboBoxItem)opened.SelectedItem).Tag);
            }
        });
    }

    [Theory]
    [InlineData("mute")]
    [InlineData("digest")]
    public void Save_PutsTheChosenActionOnTheRule(string choice)
    {
        StaTestThread.Run(() =>
        {
            /* The rule opens on the other end of the choice, so the test proves the box was read and not the stored value kept. */
            var dialog = new MuteRuleEditDialog(Rule(choice == "digest" ? MuteRuleActions.Mute : MuteRuleActions.Digest));
            var combo = (ComboBox)dialog.FindName("ActionBox");
            combo.SelectedItem = combo.Items.Cast<ComboBoxItem>().Single(i => Equals(i.Tag, choice));

            var save = typeof(MuteRuleEditDialog).GetMethod("Save_Click", BindingFlags.Instance | BindingFlags.NonPublic)!;
            try
            {
                save.Invoke(dialog, new object?[] { null, new RoutedEventArgs() });
            }
            catch (TargetInvocationException ex) when (ex.InnerException is InvalidOperationException)
            {
                /* A dialog that was never shown cannot take a DialogResult: that is the last statement of a valid save. */
            }

            Assert.Equal(choice, dialog.Rule.Action);
        });
    }

    [Fact]
    public void TheProbeSentinel_MapsTheActionColumn_ToV236_AndItsAbsence_ToAnOlderVersion()
    {
        static int Map(bool hasMuteRuleAction) => ViewerDataService.MapProbedSchemaVersion(
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            hasMuteRuleAction: hasMuteRuleAction);
        Assert.Equal(236, Map(true));
        Assert.True(Map(false) < 236);
    }

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task Live_AnActionRoundTripsThroughTheViewer_AndAStoreBelowV236RefusesADigestRule()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the viewer's mute rule action round trip.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);

            await using (var viewer = new ViewerDataService(scratch.ConnectionString))
            {
                Assert.True(await viewer.SupportsMuteActionAsync(ct));
                var digest = Rule(MuteRuleActions.Digest);
                var mute = Rule(MuteRuleActions.Mute);
                mute.MetricName = "Blocking Detected";
                await viewer.InsertMuteRuleAsync(digest, ct);
                await viewer.InsertMuteRuleAsync(mute, ct);

                var read = await viewer.GetMuteRulesAsync(ct);
                Assert.True(read.Single(r => r.Id == digest.Id).IsDigestAction);
                Assert.False(read.Single(r => r.Id == mute.Id).IsDigestAction);

                var back = read.Single(r => r.Id == digest.Id);
                back.Action = MuteRuleActions.Mute;
                await viewer.UpdateMuteRuleAsync(back, ct);
                Assert.False((await viewer.GetMuteRulesAsync(ct)).Single(r => r.Id == digest.Id).IsDigestAction);

                await Assert.ThrowsAsync<ArgumentException>(() => viewer.InsertMuteRuleAsync(Rule("snooze"), ct));
            }

            /* A store below V236 has no column: the list still reads, a mute rule saves, and a digest rule is refused. */
            await RungRewindArtifacts.DropSentinelsAboveAsync(connection, 235, ct);
            await using (var older = new ViewerDataService(scratch.ConnectionString))
            {
                Assert.False(await older.SupportsMuteActionAsync(ct));
                Assert.Equal(2, (await older.GetMuteRulesAsync(ct)).Count);
                await older.InsertMuteRuleAsync(Rule(MuteRuleActions.Mute), ct);
                var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => older.InsertMuteRuleAsync(Rule(MuteRuleActions.Digest), ct));
                Assert.Contains("Upgrade", refused.Message);
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
