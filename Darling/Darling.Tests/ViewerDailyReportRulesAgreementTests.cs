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
using PerformanceMonitor.Darling.Service.Reports;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer keeps its own copy of the service's Daily Health Report setting rules (#7041), because the Viewer does not reference
/// the service project (#7221). Each fact runs both copies over one table of inputs and asserts they give the same answer, so a rule
/// changed in one copy and not the other fails here. The copies differ on purpose in three places, which are asserted as differences:
/// a blank title (the service refuses it, the Viewer stores NULL for it), the zone check (the Viewer asks "is it known",
/// the service resolves it), and <c>TextOrNull</c> (Viewer only).
/// </summary>
public sealed class ViewerDailyReportRulesAgreementTests
{
    private static string Repeat(int count) => new('x', count);

    private static IEnumerable<object?[]> Rows(string?[] values) => values.Select(v => new object?[] { v });

    public static IEnumerable<object?[]> SendAtInputs() => Rows(
        new string?[]
        {
        // Valid spellings and their boundaries.
        "07:00", "7:05", "00:00", "0:00", "23:59", "07:00:30", "07:00:59", "23:59:59", " 07:00 ", "\t07:00\n",
        // Out of range and malformed.
        "24:00", "07:60", "07:00:60", "99:99", "7", "07", "7:5", "07:0", "0700", "07-00", "07.00", "07:00:00:00", "7:00 PM", "-1:00", "noon", "x",
        // Missing and blank.
        null, "", " ", "\t",
        });

    [Theory]
    [MemberData(nameof(SendAtInputs))]
    public void SendAt_ParseFormatAndResolve_AgreeOnEveryInput(string? text)
    {
        var serviceOk = DailyHealthReportSettings.TryParseSendAt(text, out var serviceTime);
        var viewerOk = ViewerDailyReportRules.TryParseSendAt(text, out var viewerTime);

        Assert.Equal(serviceOk, viewerOk);
        Assert.Equal(serviceTime, viewerTime);
        Assert.Equal(DailyHealthReportSettings.ResolveSendAt(text), ViewerDailyReportRules.ResolveSendAt(text));
        Assert.Equal(DailyHealthReportSettings.FormatSendAt(serviceTime), ViewerDailyReportRules.FormatSendAt(viewerTime));
        Assert.Equal(DailyHealthReportSettings.FormatSendAt(DailyHealthReportSettings.ResolveSendAt(text)),
            ViewerDailyReportRules.FormatSendAt(ViewerDailyReportRules.ResolveSendAt(text)));
    }

    [Fact]
    public void SendAt_EveryMinuteOfTheDay_FormatsAndParsesTheSameInBothCopies()
    {
        for (var minute = 0; minute < 24 * 60; minute++)
        {
            var time = new TimeOnly(minute / 60, minute % 60);
            var serviceText = DailyHealthReportSettings.FormatSendAt(time);

            Assert.Equal(serviceText, ViewerDailyReportRules.FormatSendAt(time));
            Assert.True(DailyHealthReportSettings.TryParseSendAt(serviceText, out var serviceBack));
            Assert.True(ViewerDailyReportRules.TryParseSendAt(serviceText, out var viewerBack));
            Assert.Equal(time, serviceBack);
            Assert.Equal(time, viewerBack);
        }
    }

    [Fact]
    public void SendAt_TheDefaultAndTheFallback_AgreeAndMatch()
    {
        Assert.Equal(DailyHealthReportSettings.DefaultSendAt, ViewerDailyReportRules.DefaultSendAt);
        Assert.Equal("07:00", ViewerDailyReportRules.DefaultSendAt);
        Assert.Equal(new TimeOnly(7, 0), DailyHealthReportSettings.ResolveSendAt(null));
        Assert.Equal(new TimeOnly(7, 0), ViewerDailyReportRules.ResolveSendAt(null));
        Assert.Equal(new TimeOnly(7, 0), ViewerDailyReportRules.ResolveSendAt("not a time"));
    }

    public static IEnumerable<object?[]> ZoneInputs() => Rows(
        new string?[]
        {
        // A known id, spelled several ways.
        "UTC", " UTC ", "America/New_York", "Europe/London", "Asia/Kolkata", "Etc/GMT+5",
        // Unknown ids.
        "Mars/Olympus_Mons", "Not A Zone", "utc-ish", "America/", "/",
        // Missing and blank: the service host's own zone.
        null, "", " ", "\t",
        });

    [Theory]
    [MemberData(nameof(ZoneInputs))]
    public void Zone_KnownVersusUnknown_AgreeOnEveryInput(string? zoneId)
    {
        var serviceKnown = DailyHealthReportSettings.TryResolveZone(zoneId, out var serviceZone);
        var viewerKnown = ViewerDailyReportRules.IsKnownZone(zoneId);

        Assert.Equal(serviceKnown, viewerKnown);

        // A zone the service cannot resolve is scheduled in the host's zone; a blank one is the host's zone as well.
        if (!serviceKnown || string.IsNullOrWhiteSpace(zoneId))
        {
            Assert.Equal(TimeZoneInfo.Local.Id, serviceZone.Id);
            Assert.Equal(TimeZoneInfo.Local.Id, DailyHealthReportSettings.ResolveZone(zoneId).Id);
        }
        else
        {
            Assert.Equal(TimeZoneInfo.FindSystemTimeZoneById(zoneId.Trim()).Id, serviceZone.Id);
        }
    }

    public static TheoryData<string> NonBlankTitleInputs() => new()
    {
        // Length boundaries, counted after trimming.
        "x", Repeat(99), Repeat(100), Repeat(101), Repeat(500),
        " " + Repeat(100) + " ", " " + Repeat(101) + " ", "\t" + Repeat(100) + "\t",
        // Control characters, inside and at an edge.
        "two\nlines", "tab\there", "bell\a", "nul\0", "del\u007f", "next-line\u0085", "a\r\nb", "\u0001" + Repeat(10),
        // Ordinary text, including non-ASCII, which is not a control character.
        "Nightly fleet report", "Rapport quotidien été", "日次レポート", "Daily Health Report",
    };

    [Theory]
    [MemberData(nameof(NonBlankTitleInputs))]
    public void Title_ValidateAndResolve_AgreeOnEveryNonBlankInput(string title)
    {
        var serviceOk = DailyHealthReportSettings.TryValidateTitle(title, out var serviceError);
        var viewerOk = ViewerDailyReportRules.TryValidateTitle(title, out var viewerError);

        Assert.Equal(serviceOk, viewerOk);
        Assert.Equal(serviceError, viewerError);

        // What the Viewer stores for the text is what the service prints for it.
        Assert.Equal(DailyHealthReportSettings.ResolveTitle(title), ViewerDailyReportRules.TextOrNull(title));
    }

    [Fact]
    public void Title_TheLimit_AgreesAndIsOneHundred()
    {
        Assert.Equal(DailyHealthReportSettings.MaxTitleLength, ViewerDailyReportRules.MaxTitleLength);
        Assert.Equal(100, ViewerDailyReportRules.MaxTitleLength);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData("\t")]
    [InlineData("\n")]
    public void Title_Blank_ServiceRefusesItAndViewerStoresNullForTheDefault(string? title)
    {
        // The deliberate difference: the service's validator refuses a blank (a caller sends null for the default), while the Viewer's
        // editor never validates a blank, it stores NULL, and the service prints the default title for a NULL.
        if (title is not null)
        {
            Assert.False(DailyHealthReportSettings.TryValidateTitle(title, out var error));
            Assert.NotNull(error);
        }

        Assert.Equal(DBNull.Value, ViewerDailyReportRules.TextOrNull(title));
        Assert.Equal(DailyHealthReportSettings.DefaultTitle, DailyHealthReportSettings.ResolveTitle(title));
    }
}
