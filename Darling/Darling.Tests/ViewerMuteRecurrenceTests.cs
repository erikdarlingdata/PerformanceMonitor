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
/// #7040 (V231): the viewer's part of the recurring mute rule. The probe maps the schedule column to 231, the dialog's fields become a
/// checked schedule (a one-off rule never carries the zone the box starts with), the list column says whether a window is open, and a
/// rule round-trips through the viewer's insert, update and read. A store below V231 has no schedule columns, so the list still reads and
/// the viewer writes none.
/// </summary>
/* #1776 own-store: its one live fact mints its own scratch database through ScratchPostgres and touches nothing on the shared
   one, so it is not serialized against the live-postgres collection. */
public sealed class ViewerMuteRecurrenceTests
{
    [Fact]
    public void TheProbeSentinel_MapsTheScheduleColumn_ToV231_AndItsAbsence_ToAnOlderVersion()
    {
        static int Map(bool hasMuteRecurrence) => ViewerDataService.MapProbedSchemaVersion(
            true, true, true, true, true, true, true, true, true, true, true, true, true, true, true, true,
            hasMuteRecurrence: hasMuteRecurrence);

        Assert.Equal(231, Map(true));
        Assert.True(Map(false) < 231);
        Assert.Equal(231, ViewerDataService.RecurringMuteStoreSchemaVersion);
    }

    [Fact]
    public void ABlankSchedule_IsAOneOffRule_EvenWithTheZoneTheBoxStartsWith()
    {
        var rule = new MuteRule();
        Assert.Null(MuteRuleEditDialog.ApplySchedule(rule, [], "", " ", "America/New_York"));
        Assert.False(rule.IsRecurring);
        Assert.Null(rule.RecurTimeZone);
        Assert.Null(rule.RecurDays);
    }

    [Fact]
    public void TheDialogFields_BecomeACheckedSchedule_WithTheZoneNormalised()
    {
        var rule = new MuteRule();
        Assert.Null(MuteRuleEditDialog.ApplySchedule(rule, [DayOfWeek.Saturday, DayOfWeek.Sunday], "22:00", "02:00", " UTC "));
        Assert.True(rule.IsRecurring);
        Assert.Equal(new TimeOnly(22, 0), rule.RecurStartTime);
        Assert.Equal(new TimeOnly(2, 0), rule.RecurEndTime);
        Assert.Equal(MuteRecurrence.Bit(DayOfWeek.Saturday) | MuteRecurrence.Bit(DayOfWeek.Sunday), rule.RecurDays);
        Assert.Equal("UTC", rule.RecurTimeZone);

        var everyDay = new MuteRule();
        Assert.Null(MuteRuleEditDialog.ApplySchedule(everyDay, [], "01:00", "03:00", ""));
        Assert.Null(everyDay.RecurDays);
        Assert.Equal(MuteRecurrence.HostZoneIana(), everyDay.RecurTimeZone);
    }

    [Theory]
    [InlineData("22:00", "", "UTC")]
    [InlineData("", "02:00", "UTC")]
    [InlineData("22:00", "22:00", "UTC")]
    [InlineData("later", "02:00", "UTC")]
    [InlineData("22:00", "02:00", "Not/AZone")]
    public void ABrokenSchedule_IsRefusedWithAMessage(string start, string end, string zone)
    {
        Assert.False(string.IsNullOrEmpty(MuteRuleEditDialog.ApplySchedule(new MuteRule(), [DayOfWeek.Monday], start, end, zone)));
    }

    [Fact]
    public void DaysTickedWithNoTimes_AreRefused()
    {
        Assert.False(string.IsNullOrEmpty(MuteRuleEditDialog.ApplySchedule(new MuteRule(), [DayOfWeek.Monday], "", "", "UTC")));
    }

    [Fact]
    public void TheScheduleColumn_SaysWhetherAWindowIsOpen_AndAOneOffRuleShowsNothing()
    {
        var nightly = new MuteRule
        {
            RecurStartTime = new TimeOnly(22, 0), RecurEndTime = new TimeOnly(2, 0), RecurTimeZone = "UTC",
        };
        var inside = new DateTime(2026, 11, 2, 23, 0, 0, DateTimeKind.Utc);
        var outside = new DateTime(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);
        Assert.EndsWith("(in window now)", MuteScheduleConverter.Display(nightly, inside));
        Assert.Contains("22:00-02:00", MuteScheduleConverter.Display(nightly, outside));
        Assert.DoesNotContain("in window now", MuteScheduleConverter.Display(nightly, outside));
        Assert.Equal("", MuteScheduleConverter.Display(new MuteRule(), inside));
    }

    [Fact]
    public void ARecurringRule_IsNotAWholeServerSilence()
    {
        var silence = ViewerDataService.BuildServerSilenceRule(7, "SRV1");
        Assert.True(ViewerDataService.IsWholeServerSilence(silence, 7, "SRV1"));
        silence.RecurStartTime = new TimeOnly(22, 0);
        silence.RecurEndTime = new TimeOnly(2, 0);
        silence.RecurTimeZone = "UTC";
        Assert.False(ViewerDataService.IsWholeServerSilence(silence, 7, "SRV1"));
    }

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task Live_AScheduleRoundTripsThroughTheViewer_AndAStoreBelowV231WritesAndReadsNone()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the viewer's mute schedule round trip.");

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
                Assert.True(await viewer.SupportsMuteRecurrenceAsync(ct));

                var nightly = new MuteRule
                {
                    MetricName = "Long-Running Query",
                    RecurDays = MuteRecurrence.Bit(DayOfWeek.Saturday) | MuteRecurrence.Bit(DayOfWeek.Sunday),
                    RecurStartTime = new TimeOnly(22, 0),
                    RecurEndTime = new TimeOnly(2, 30),
                    RecurTimeZone = "America/New_York",
                };
                var oneOff = new MuteRule { MetricName = "High CPU" };
                await viewer.InsertMuteRuleAsync(nightly, ct);
                await viewer.InsertMuteRuleAsync(oneOff, ct);

                var read = await viewer.GetMuteRulesAsync(ct);
                var back = read.Single(r => r.Id == nightly.Id);
                Assert.Equal(nightly.RecurDays, back.RecurDays);
                Assert.Equal(new TimeOnly(22, 0), back.RecurStartTime);
                Assert.Equal(new TimeOnly(2, 30), back.RecurEndTime);
                Assert.Equal("America/New_York", back.RecurTimeZone);
                var plain = read.Single(r => r.Id == oneOff.Id);
                Assert.False(plain.IsRecurring);
                Assert.Null(plain.RecurDays);

                back.RecurStartTime = null;
                back.RecurEndTime = null;
                back.RecurDays = null;
                back.RecurTimeZone = null;
                await viewer.UpdateMuteRuleAsync(back, ct);
                Assert.False((await viewer.GetMuteRulesAsync(ct)).Single(r => r.Id == nightly.Id).IsRecurring);

                await Assert.ThrowsAsync<ArgumentException>(() => viewer.InsertMuteRuleAsync(
                    new MuteRule { MetricName = "High CPU", RecurStartTime = new TimeOnly(1, 0) }, ct));
            }

            /* A store below V231 has no columns: the list still reads, and the viewer refuses a schedule it cannot keep. */
            await ExecAsync(connection,
                "ALTER TABLE config.config_mute_rules DROP COLUMN recur_days, DROP COLUMN recur_start_time, DROP COLUMN recur_end_time, DROP COLUMN recur_time_zone", ct);
            await using (var older = new ViewerDataService(scratch.ConnectionString))
            {
                Assert.False(await older.SupportsMuteRecurrenceAsync(ct));
                Assert.Equal(2, (await older.GetMuteRulesAsync(ct)).Count);
                await older.InsertMuteRuleAsync(new MuteRule { MetricName = "Blocking Detected" }, ct);
                await Assert.ThrowsAsync<InvalidOperationException>(() => older.InsertMuteRuleAsync(
                    new MuteRule { MetricName = "High CPU", RecurStartTime = new TimeOnly(1, 0), RecurEndTime = new TimeOnly(2, 0), RecurTimeZone = "UTC" }, ct));
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
