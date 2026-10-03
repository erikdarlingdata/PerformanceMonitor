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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Collector Schedules window's "Run at" column (#4938), without a store: what the cell holds for each scope,
/// the rows a save writes from it, the checks a save runs and the text they show, and the conversion line under the
/// grid. All of it is pure, so it is tested without a window. The store round trip is
/// <see cref="CollectorRunTimeViewerLiveTests"/>.
/// </summary>
public sealed class CollectorRunTimeViewerTests
{
    private const string Daily = "index_object_stats";
    private const string AnotherDaily = "pg_index_usage_stats";
    private const string OnLoad = "server_properties";
    private const string Hourly = "wait_stats";

    private static CollectorScheduleEditItem Item(string name, string runAt, int? frequency = null)
    {
        var item = CollectorSchedulePresets.BuildDefaultSchedule().Single(i => i.Name == name);
        item.RunAtText = runAt;
        if (frequency is int f)
        {
            item.FrequencyMinutes = f;
        }

        return item;
    }

    private static bool Valid(CollectorScheduleEditItem item, out string error) =>
        CollectorScheduleOverlay.ValidateSchedule(new[] { item }, out error);

    [Fact]
    public void ANewItem_ShowsUseDefault_AndSoDoesEveryItemOfTheDefaultSchedule()
    {
        Assert.Equal("Use default", CollectorScheduleOverlay.UseDefaultRunAtText);
        Assert.Equal("None", CollectorScheduleOverlay.NoRunAtText);
        Assert.Equal(CollectorScheduleOverlay.UseDefaultRunAtText, new CollectorScheduleEditItem().RunAtText);
        Assert.All(CollectorSchedulePresets.BuildDefaultSchedule(), i => Assert.Equal(CollectorScheduleOverlay.UseDefaultRunAtText, i.RunAtText));
    }

    [Fact]
    public void TheCell_HoldsTheScopesOwnRunTime_NeverTheFleetOneOverlaidOnAServer()
    {
        var rows = new[]
        {
            new CollectorScheduleRow(null, Daily, null, null, true, null, 120),
            new CollectorScheduleRow(1, Daily, null, null, true, null, null),
            new CollectorScheduleRow(2, Daily, null, null, true, null, -1),
            new CollectorScheduleRow(3, Daily, null, null, true, null, 195),
        };

        string Cell(int? serverId) =>
            CollectorScheduleOverlay.BuildEffectiveSchedule(rows, serverId).Single(i => i.Name == Daily).RunAtText;

        Assert.Equal("02:00", Cell(null));
        /* A server whose own column is NULL shows "Use default", not the fleet's 02:00: if the cell took the fleet's
           value, a save would write it into every server row and "Use default" could never survive. */
        Assert.Equal("Use default", Cell(1));
        Assert.Equal("None", Cell(2));
        Assert.Equal("03:15", Cell(3));
        Assert.Equal("Use default", Cell(99));

        Assert.All(
            CollectorScheduleOverlay.BuildEffectiveSchedule(rows, 3).Where(i => i.Name != Daily),
            i => Assert.Equal("Use default", i.RunAtText));
    }

    [Fact]
    public void AFleetRunTime_OnADefaultCadenceCollector_StillWritesItsRow()
    {
        /* The #2064 and #3477 failure over again, one column further: a row that matches the code default writes
           nothing, so a run time on a collector left at its default cadence would silently never be stored. */
        var written = Assert.Single(CollectorScheduleOverlay.ToFleetOverrideRows(new[] { Item(Daily, "02:00") }));
        Assert.Null(written.ServerId);
        Assert.Equal(Daily, written.CollectorName);
        Assert.Equal(120, written.RunAtMinute);

        Assert.Empty(CollectorScheduleOverlay.ToFleetOverrideRows(new[] { Item(Daily, "Use default") }));
        Assert.Empty(CollectorScheduleOverlay.ToFleetOverrideRows(new[] { Item(Daily, "") }));
    }

    [Fact]
    public void NoneOnTheFleetRow_ClearsItToNull_BecauseTheFleetHasNothingToStop()
    {
        Assert.Empty(CollectorScheduleOverlay.ToFleetOverrideRows(new[] { Item(Daily, "None") }));

        var written = Assert.Single(CollectorScheduleOverlay.ToFleetOverrideRows(
            new[] { Item(Daily, "None", frequency: 2880) }));
        Assert.Null(written.RunAtMinute);
    }

    [Fact]
    public void AServersRows_CarryTheirOwnRunTime_NullFallsThrough_AndNoneIsMinusOne()
    {
        var rows = CollectorScheduleOverlay.ToServerOverrideRows(
            new[]
            {
                Item(Daily, "Use default"),
                Item(AnotherDaily, "None"),
                Item("pg_column_stats", "04:15"),
                Item("pg_index_bloat", " 23:59 "),
            },
            5);

        Assert.All(rows, r => Assert.Equal(5, r.ServerId));
        Assert.Null(rows.Single(r => r.CollectorName == Daily).RunAtMinute);
        Assert.Equal(-1, rows.Single(r => r.CollectorName == AnotherDaily).RunAtMinute);
        Assert.Equal(4 * 60 + 15, rows.Single(r => r.CollectorName == "pg_column_stats").RunAtMinute);
        Assert.Equal(23 * 60 + 59, rows.Single(r => r.CollectorName == "pg_index_bloat").RunAtMinute);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("Use default")]
    [InlineData("use default")]
    [InlineData("default")]
    public void TryParseRunAt_ReadsUseDefaultAsNull(string text)
    {
        Assert.True(CollectorScheduleOverlay.TryParseRunAt(text, out var minute, out var error));
        Assert.Null(minute);
        Assert.Equal("", error);
    }

    [Theory]
    [InlineData("None")]
    [InlineData("none")]
    [InlineData(" NONE ")]
    public void TryParseRunAt_ReadsNoneAsMinusOne(string text)
    {
        Assert.True(CollectorScheduleOverlay.TryParseRunAt(text, out var minute, out _));
        Assert.Equal(-1, minute);
    }

    [Theory]
    [InlineData("00:00", 0)]
    [InlineData("02:00", 120)]
    [InlineData(" 02:00 ", 120)]
    [InlineData("13:45", 825)]
    [InlineData("23:59", 1439)]
    public void TryParseRunAt_ReadsATwentyFourHourTime(string text, int expected)
    {
        Assert.True(CollectorScheduleOverlay.TryParseRunAt(text, out var minute, out var error));
        Assert.Equal(expected, minute);
        Assert.Equal("", error);
    }

    [Theory]
    [InlineData("24:00")]
    [InlineData("02:60")]
    [InlineData("2:5")]
    [InlineData("12:3")]
    [InlineData("1200")]
    [InlineData("02-00")]
    [InlineData("02:00pm")]
    [InlineData("-1:00")]
    [InlineData("abc")]
    [InlineData("nonsense")]
    public void TryParseRunAt_RefusesAnythingElse_WithTheSharedText(string text)
    {
        Assert.False(CollectorScheduleOverlay.TryParseRunAt(text, out var minute, out var error));
        Assert.Null(minute);
        Assert.Equal("Run at must be a 24-hour time from 00:00 to 23:59, such as 02:00.", error);
        Assert.Equal(CollectorRunTime.InvalidRunAtMessage, error);
    }

    [Theory]
    [InlineData("24:00")]
    [InlineData("2:5")]
    [InlineData("noon")]
    public void ASaveWithABadRunTime_IsRefused_NamingTheCollectorAndTheRule(string text)
    {
        Assert.False(Valid(Item(Daily, text), out var error));
        Assert.Contains(CollectorRunTime.InvalidRunAtMessage, error, StringComparison.Ordinal);
        Assert.Contains($"'{Daily}'", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(60)]
    [InlineData(1439)]
    [InlineData(1441)]
    [InlineData(2000)]
    public void ARunTime_OnAnIntervalThatIsNotWholeDays_IsRefused_WithTheSharedWholeDayText(int minutes)
    {
        Assert.False(Valid(Item(Daily, "02:00", frequency: minutes), out var error));
        Assert.Contains(CollectorRunTime.IntervalRefusalMessage(Daily, minutes), error, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHourlyRefusal_ReadsTheSharedSentence()
    {
        Assert.False(Valid(Item(Hourly, "02:00"), out var error));
        Assert.Contains(
            "'wait_stats' runs every 1 minutes. A run time works only for a collector that runs once a day or less often "
            + "(1440 minutes, or a multiple of 1440). Change the frequency or clear the run time.",
            error, StringComparison.Ordinal);
    }

    [Fact]
    public void ARunTime_IsAllowedOnADailyCollector_AMultipleOfADay_AndAnOnLoadCollector()
    {
        Assert.True(Valid(Item(Daily, "02:00"), out _));
        Assert.True(Valid(Item(Daily, "02:00", frequency: 2880), out _));
        Assert.True(Valid(Item(Daily, "02:00", frequency: 10080), out _));
        /* An on-load collector (0) recaptures daily, so it is judged as a daily one. */
        Assert.True(Valid(Item(OnLoad, "02:00"), out _));
    }

    [Fact]
    public void NoneAndUseDefault_AreAlwaysAllowed_EvenOnAnHourlyCollector_BecauseClearingIsTheRemedy()
    {
        Assert.True(Valid(Item(Hourly, "None"), out var error));
        Assert.Equal("", error);
        Assert.True(Valid(Item(Hourly, "Use default"), out _));
        Assert.True(Valid(Item(Hourly, ""), out _));
    }

    /* ------------------------------ the conversion line ------------------------------ */

    private static readonly DateTime Now = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);

    private const int ServerId = 41;

    private static string Describe(
        string runAtText, int frequencyMinutes = 1440, int? serverId = ServerId, int? fleetRunAtMinute = null,
        ServerClock? clock = null, bool azureSqlDatabase = false, string collector = Daily) =>
        CollectorScheduleRunAtText.Describe(collector, runAtText, frequencyMinutes, serverId, fleetRunAtMinute, clock, azureSqlDatabase, Now);

    [Fact]
    public void AFleetRow_ShowsTheSixtyMinuteSpread()
    {
        var text = Describe("02:00", serverId: null, clock: null);
        Assert.Contains("between 02:00 and 03:00", text, StringComparison.Ordinal);
        Assert.Contains("server time", text, StringComparison.Ordinal);

        Assert.Contains("between 23:30 and 00:30", Describe("23:30", serverId: null), StringComparison.Ordinal);
    }

    [Fact]
    public void AFleetRowWithNoTime_SaysItHasNoFixedTime()
    {
        var text = Describe("Use default", serverId: null);
        Assert.Contains("No fixed run time", text, StringComparison.Ordinal);
        Assert.DoesNotContain("between", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AServerRow_ShowsItsExactMinute_AndTheUtcConversion()
    {
        var clock = ServerClock.FixedOffset(-240);
        var slot = CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 2), 120, ServerId, clock.ToUtc);
        var local = clock.ToServerLocal(slot);

        var text = Describe("02:00", clock: clock);

        /* The same shape as the plan's example, "02:00 server time (06:00 UTC today)", with this server's own spread
           added to the time: the slot is run time + CadencePhaseOffset(serverId, 3600). */
        Assert.Contains($"{local:HH:mm} server time ({slot:HH:mm} UTC today)", text, StringComparison.Ordinal);
        Assert.Equal(CollectorCadence.CadencePhaseOffset(ServerId, 3600), CollectorRunTime.Spread(ServerId));
    }

    [Fact]
    public void AServerRow_ShowsTheNextRun_ForADailyCollector()
    {
        var clock = ServerClock.FixedOffset(-240);
        /* Now is 12:00 UTC, after today's slot (about 06:xx UTC), so the next run is tomorrow's. */
        var next = CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 3), 120, ServerId, clock.ToUtc);

        var text = Describe("02:00", clock: clock);

        Assert.Contains($"Next run: {next:yyyy-MM-dd HH:mm} UTC", text, StringComparison.Ordinal);
    }

    [Fact]
    public void NowBeforeTodaysSlot_PutsTheNextRunToday()
    {
        var clock = ServerClock.Utc;
        var today = CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 2), 18 * 60, ServerId, clock.ToUtc);

        var text = Describe("18:00", clock: clock);

        Assert.Contains($"Next run: {today:yyyy-MM-dd HH:mm} UTC", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AZoneAheadOfUtc_SaysTheUtcDayTheSlotFallsOn()
    {
        /* 02:00 at +10:00 is 16:00 UTC the day before. */
        var clock = ServerClock.FixedOffset(600);
        var slot = CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 2), 120, ServerId, clock.ToUtc);
        Assert.Equal(new DateTime(2026, 10, 1), slot.Date);

        Assert.Contains($"{slot:HH:mm} UTC yesterday", Describe("02:00", clock: clock), StringComparison.Ordinal);
    }

    [Fact]
    public void AServerWithNoClockYet_ReadsTheTimeAsUtc_AndSaysSo()
    {
        var text = Describe("02:00", clock: null);

        Assert.Contains("not known yet", text, StringComparison.Ordinal);
        Assert.Contains("UTC", text, StringComparison.Ordinal);
        var slot = CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 2), 120, ServerId, CollectorRunTime.LocalIsUtc);
        Assert.Contains($"{slot:HH:mm} server time", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AzureSqlDatabase_SaysItAlwaysReportsUtc()
    {
        var text = Describe("02:00", clock: ServerClock.Utc, azureSqlDatabase: true);

        Assert.Contains("Azure SQL Database always reports UTC", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AServerRow_UsingTheFleetsTime_NamesTheFleetTime_AndStillShowsItsOwnMinute()
    {
        var clock = ServerClock.Utc;
        var slot = CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 2), 120, ServerId, clock.ToUtc);

        var text = Describe("Use default", fleetRunAtMinute: 120, clock: clock);

        Assert.Contains("fleet-wide run time", text, StringComparison.Ordinal);
        Assert.Contains($"{slot:HH:mm} server time", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AServerRow_WithNoneOrNoTimeAnywhere_SaysThereIsNoFixedTime()
    {
        Assert.Contains("No fixed run time on this server", Describe("None", fleetRunAtMinute: 120), StringComparison.Ordinal);
        Assert.DoesNotContain("Next run", Describe("None", fleetRunAtMinute: 120), StringComparison.Ordinal);
        Assert.Contains("No fixed run time", Describe("Use default", fleetRunAtMinute: null), StringComparison.Ordinal);
    }

    [Fact]
    public void ATimeThatCannotApply_ShowsTheRefusalInsteadOfAConversion()
    {
        var text = Describe("02:00", frequencyMinutes: 60, collector: Hourly, clock: ServerClock.Utc);

        Assert.Contains(CollectorRunTime.IntervalRefusalMessage(Hourly, 60), text, StringComparison.Ordinal);
        Assert.DoesNotContain("Next run", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ABadTime_ShowsTheSharedMessage()
    {
        Assert.Equal(CollectorRunTime.InvalidRunAtMessage, Describe("25:61"));
    }

    [Fact]
    public void ACollectorThatRunsEveryFewDays_DoesNotClaimANextRunDate()
    {
        var text = Describe("02:00", frequencyMinutes: 2880, clock: ServerClock.Utc);

        Assert.Contains("every 2 days", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Next run:", text, StringComparison.Ordinal);
    }
}
