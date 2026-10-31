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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7040: the create and update verbs take a repeat schedule, hold it to the one check every write path runs, keep it in the
/// stored rule and its undo, tell a schedule from a duplicate, and report it (with in_window_now and the next window) on the read.
/// </summary>
public sealed class MuteRuleRecurrenceToolTests
{
    private static JsonObject Parse(string json) => JsonNode.Parse(json)!.AsObject();

    private static Task<string> Create(FakeMuteRuleStore store, string? server = "srv", string? metric = "Long-Running Query",
        string[]? days = null, string? start = null, string? end = null, string? zone = null) =>
        DarlingMcpAlertTools.CreateMuteRuleOver(store, server, metric, null, null, null, null, "nightly maintenance", null,
            recur_days: days, recur_start: start, recur_end: end, recur_time_zone: zone);

    [Fact]
    public async Task Create_StoresTheScheduleWithAnIanaZone_AndEchoesIt()
    {
        var store = new FakeMuteRuleStore();
        var answer = Parse(await Create(store, days: ["mon", "Friday"], start: "22:00", end: "02:00", zone: "Eastern Standard Time"));

        Assert.Equal("created", answer["status"]!.GetValue<string>());
        var rule = answer["mute_rule"]!.AsObject();
        Assert.Equal(["Mon", "Fri"], rule["recur_days"]!.AsArray().Select(n => n!.GetValue<string>()).ToArray());
        Assert.Equal("22:00", rule["recur_start_time"]!.GetValue<string>());
        Assert.Equal("02:00", rule["recur_end_time"]!.GetValue<string>());
        Assert.Equal("America/New_York", rule["recur_time_zone"]!.GetValue<string>());

        var stored = store.Row(rule["id"]!.GetValue<string>())!;
        Assert.Equal(MuteRecurrence.Bit(DayOfWeek.Monday) | MuteRecurrence.Bit(DayOfWeek.Friday), stored.RecurDays);
        Assert.Equal(new TimeOnly(22, 0), stored.RecurStartTime);
        Assert.Equal("America/New_York", stored.RecurTimeZone);
    }

    [Fact]
    public async Task Create_WithTimesAndNoZone_TakesTheHostZone_AndAServerOnlyRuleIsAllowed()
    {
        var store = new FakeMuteRuleStore();
        var answer = Parse(await Create(store, metric: null, start: "01:00", end: "03:00"));
        Assert.Equal("created", answer["status"]!.GetValue<string>());
        var rule = answer["mute_rule"]!.AsObject();
        Assert.Equal(MuteRecurrence.HostZoneIana(), rule["recur_time_zone"]!.GetValue<string>());
        Assert.Null(rule["recur_days"]);
        Assert.Null(rule["metric_name"]);
    }

    [Theory]
    [InlineData("22:00", null, null, null)]               // start without end
    [InlineData(null, "02:00", null, null)]               // end without start
    [InlineData("22:00", "22:00", null, null)]            // equal
    [InlineData("25:00", "02:00", null, null)]            // not a time
    [InlineData("22:00", "02:00", "Mars/Olympus_Mons", null)]
    [InlineData(null, null, "UTC", null)]                 // zone without times
    [InlineData(null, null, null, "Mon")]                 // days without times
    [InlineData("22:00", "02:00", null, "Funday")]        // bad day
    public async Task Create_RefusesABadSchedule_AndWritesNothing(string? start, string? end, string? zone, string? day)
    {
        var store = new FakeMuteRuleStore();
        var answer = Parse(await Create(store, days: day is null ? null : [day], start: start, end: end, zone: zone));
        Assert.Equal("invalid", answer["status"]!.GetValue<string>());
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task Create_ViaTheWebBody_TakesTheSameFields()
    {
        var store = new FakeMuteRuleStore();
        var body = new JsonObject
        {
            ["server_name"] = "srv",
            ["recur_days"] = new JsonArray("Sat", "Sun"),
            ["recur_start"] = "00:00",
            ["recur_end"] = "23:00",
            ["recur_time_zone"] = "UTC",
        };
        var answer = Parse(await DarlingMcpAlertTools.CreateMuteRuleCore(store, body.ToJsonString()));
        Assert.Equal("created", answer["status"]!.GetValue<string>());
        Assert.Equal("00:00", answer["mute_rule"]!["recur_start_time"]!.GetValue<string>());

        var bad = new JsonObject { ["server_name"] = "srv2", ["recur_start"] = "00:00" };
        var refused = Parse(await DarlingMcpAlertTools.CreateMuteRuleCore(store, bad.ToJsonString()));
        Assert.Equal("invalid", refused["status"]!.GetValue<string>());
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task Create_ADifferentScheduleIsNotADuplicate_TheSameOneIs()
    {
        var store = new FakeMuteRuleStore();
        Assert.Equal("created", Parse(await Create(store, days: ["Mon"], start: "22:00", end: "02:00", zone: "UTC"))["status"]!.GetValue<string>());
        Assert.Equal("created", Parse(await Create(store, days: ["Tue"], start: "22:00", end: "02:00", zone: "UTC"))["status"]!.GetValue<string>());
        Assert.Equal("already_exists", Parse(await Create(store, days: ["Mon"], start: "22:00", end: "02:00", zone: "UTC"))["status"]!.GetValue<string>());
        // A rule with no schedule is not the scheduled rule either.
        Assert.Equal("created", Parse(await Create(store))["status"]!.GetValue<string>());
        Assert.Equal(3, store.Count);
    }

    [Fact]
    public async Task Update_SetsAScheduleOnAPlainRule_AndTheUndoClearsItWhole()
    {
        var store = new FakeMuteRuleStore();
        var id = Parse(await Create(store))["mute_rule"]!["id"]!.GetValue<string>();

        var set = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id,
            """{"recur_days":["Sat","Sun"],"recur_start":"20:00","recur_end":"06:00","recur_time_zone":"America/Chicago"}"""));
        Assert.Equal("updated", set["status"]!.GetValue<string>());
        Assert.Equal("America/Chicago", store.Row(id)!.RecurTimeZone);
        Assert.Equal(new TimeOnly(6, 0), store.Row(id)!.RecurEndTime);

        // The undo is the update that sets the previous (empty) values back: it clears the whole schedule.
        var undo = set["undo"]!["arguments"]!.AsObject();
        var undone = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, undo["changes_json"]!.GetValue<string>()));
        Assert.Equal("updated", undone["status"]!.GetValue<string>());
        var row = store.Row(id)!;
        Assert.False(row.IsRecurring);
        Assert.Null(row.RecurDays);
        Assert.Null(row.RecurTimeZone);
    }

    [Fact]
    public async Task Update_ClearingBothTimes_ClearsDaysAndZoneWithThem()
    {
        var store = new FakeMuteRuleStore();
        var id = Parse(await Create(store, days: ["Mon"], start: "22:00", end: "02:00", zone: "UTC"))["mute_rule"]!["id"]!.GetValue<string>();

        var cleared = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, """{"recur_start":null,"recur_end":null}"""));
        Assert.Equal("updated", cleared["status"]!.GetValue<string>());
        var row = store.Row(id)!;
        Assert.False(row.IsRecurring);
        Assert.Null(row.RecurDays);
        Assert.Null(row.RecurTimeZone);
    }

    [Fact]
    public async Task Update_IsCheckedAsAWhole_AfterTheChangesLand()
    {
        var store = new FakeMuteRuleStore();
        var id = Parse(await Create(store, days: ["Mon"], start: "22:00", end: "02:00", zone: "UTC"))["mute_rule"]!["id"]!.GetValue<string>();

        // Moving the start onto the end makes the schedule empty: refused, the row keeps its old schedule.
        var refused = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, """{"recur_start":"02:00"}"""));
        Assert.Equal("invalid", refused["status"]!.GetValue<string>());
        Assert.Equal(new TimeOnly(22, 0), store.Row(id)!.RecurStartTime);

        // Clearing only the start leaves an end with no start: refused.
        var half = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, """{"recur_start":null}"""));
        Assert.Equal("invalid", half["status"]!.GetValue<string>());

        // The read spelling is accepted back, and one name per field only.
        var ok = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, """{"recur_start_time":"23:00"}"""));
        Assert.Equal("updated", ok["status"]!.GetValue<string>());
        var both = Parse(await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, """{"recur_start_time":"21:00","recur_start":"20:00"}"""));
        Assert.Equal("invalid", both["status"]!.GetValue<string>());
    }

    [Fact]
    public void ReadShape_SaysWhetherTheRuleIsInAWindowNow_AndWhenTheNextOneOpens()
    {
        var rule = new MuteRule
        {
            ServerName = "srv",
            RecurStartTime = new TimeOnly(22, 0),
            RecurEndTime = new TimeOnly(2, 0),
            RecurTimeZone = "UTC",
        };
        var inside = DarlingMcpAlertTools.BuildMuteRuleWithState(rule, new DateTime(2026, 11, 3, 23, 0, 0, DateTimeKind.Utc));
        Assert.True(inside["in_window_now"]!.GetValue<bool>());
        Assert.Equal("active", inside["state"]!.GetValue<string>());
        Assert.Equal("daily 22:00-02:00 UTC", inside["schedule"]!.GetValue<string>());
        Assert.StartsWith("2026-11-04T22:00:00", inside["next_window_start_utc"]!.GetValue<string>());

        var outside = DarlingMcpAlertTools.BuildMuteRuleWithState(rule, new DateTime(2026, 11, 3, 12, 0, 0, DateTimeKind.Utc));
        Assert.False(outside["in_window_now"]!.GetValue<bool>());
        Assert.StartsWith("2026-11-03T22:00:00", outside["next_window_start_utc"]!.GetValue<string>());

        var plain = DarlingMcpAlertTools.BuildMuteRuleWithState(new MuteRule(), DateTime.UtcNow);
        Assert.Null(plain["in_window_now"]);
    }

    [Fact]
    public void AScheduledRuleIsNotAQuietWindow()
    {
        var rule = new MuteRule
        {
            ExpiresAtUtc = DateTime.UtcNow.AddHours(1),
            Reason = DarlingMcpQuietWindowTools.ReasonPrefix,
        };
        Assert.True(DarlingMcpQuietWindowTools.IsQuietWindow(rule));
        rule.RecurStartTime = new TimeOnly(1, 0);
        rule.RecurEndTime = new TimeOnly(2, 0);
        Assert.False(DarlingMcpQuietWindowTools.IsQuietWindow(rule));
    }
}
