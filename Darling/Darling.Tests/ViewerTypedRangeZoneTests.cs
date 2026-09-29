/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: the display zone the viewer's custom range and lists convert in. <see cref="ViewerTimeHelper.DisplayZoneFor"/>
/// names the zone of each mode on an explicit server clock, and <see cref="ViewerTimeHelper.ClockForServerOrMachine"/>
/// is the ONE rule for a list row whose server has no collected clock (the viewer machine's offset).
/// US Eastern is the "server": spring change 2026-03-08 07:00Z, autumn change 2026-11-01 06:00Z, so the first 01:30
/// of the autumn day is 05:30Z and the second is 06:30Z.
/// </summary>
/* The CurrentDisplayZone test flips the process-wide statics (each restores in finally); one shared collection
   serializes it with every other class that does. */
[Collection("viewer-time-statics")]
public sealed class ViewerDisplayZoneTests
{
    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -300);

    [Fact]
    public void DisplayZoneFor_NamesTheZoneOfEachMode()
    {
        Assert.Same(TimeZoneInfo.Utc, ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.UTC, Eastern));
        Assert.Same(TimeZoneInfo.Local, ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.LocalTime, Eastern));

        var server = ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.ServerTime, Eastern);
        Assert.Same(Eastern.AsTimeZone(), server);
        Assert.Equal(TimeSpan.FromHours(-4), server.GetUtcOffset(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc)));
        Assert.Equal(TimeSpan.FromHours(-5), server.GetUtcOffset(new DateTime(2026, 12, 1, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void DisplayZoneFor_ServerTime_OnAFixedOffsetClock_IsThatFixedOffset()
    {
        var zone = ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.ServerTime, ServerClock.FixedOffset(330));
        Assert.Equal(TimeSpan.FromMinutes(330), zone.GetUtcOffset(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc)));
        Assert.Equal(TimeSpan.FromMinutes(330), zone.GetUtcOffset(new DateTime(2026, 12, 1, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void CurrentDisplayZone_ReadsTheProcessWideModeAndTheActiveServerClock()
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        try
        {
            ViewerTimeHelper.ActiveServerClock = Eastern;

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            Assert.Same(Eastern.AsTimeZone(), ViewerTimeHelper.CurrentDisplayZone());

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            Assert.Same(TimeZoneInfo.Utc, ViewerTimeHelper.CurrentDisplayZone());

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
            Assert.Same(TimeZoneInfo.Local, ViewerTimeHelper.CurrentDisplayZone());
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
        }
    }

    [Fact]
    public void ClockForServerOrMachine_UsesTheServersOwnClock_WhenItHasOne()
    {
        var clocks = new Dictionary<int, ServerClock> { [7] = Eastern };
        var machine = TimeZoneInfo.FindSystemTimeZoneById("India Standard Time");

        Assert.Same(clocks[7], ViewerTimeHelper.ClockForServerOrMachine(clocks, 7, machine, new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc)));
    }

    [Fact]
    public void ClockForServerOrMachine_UsesTheViewerMachinesOffset_WhenNoClockIsCollected()
    {
        var clocks = new Dictionary<int, ServerClock> { [7] = Eastern };
        var machine = TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time");

        /* Server 8 has no entry: the machine's offset at the given instant (-07:00 in July, -08:00 in December)
           as a FIXED clock, whatever instant is later converted on it. */
        var summer = ViewerTimeHelper.ClockForServerOrMachine(clocks, 8, machine, new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        Assert.Equal(-420, summer.OffsetMinutesAt(new DateTime(2026, 12, 1, 12, 0, 0, DateTimeKind.Utc)));

        var winter = ViewerTimeHelper.ClockForServerOrMachine(clocks, 8, machine, new DateTime(2026, 12, 1, 12, 0, 0, DateTimeKind.Utc));
        Assert.Equal(-480, winter.OffsetMinutesAt(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc)));
    }
}

/// <summary>
/// #4766: a custom range the viewer's server tab holds is two UTC instants, the pickers are a drawing of them, and
/// only a typed edit reads text back (<see cref="CustomRangeState.ApplyEdit"/> through
/// <see cref="ViewerTimeHelper.DisplayZoneFor"/>). The tab is a WPF control, so its typed path runs here on the same
/// <see cref="CustomRangeState"/> and zone it uses, and source pins hold the tab to that path: <c>GetCustomRangeUtc</c>
/// returns the held pair and the display-mode switch parses nothing.
/// </summary>
public sealed class ViewerTypedRangeTests
{
    private static DateTime Naive(int month, int day, int hour, int minute = 0) =>
        new(2026, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -300);

    private static TimeZoneInfo ServerZone => ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.ServerTime, Eastern);

    [Fact]
    public void ATypedRangeInTheRepeatedHour_NamesEveryInstantLabelledInIt()
    {
        /* 01:00 to 01:45 on the autumn change day. The old parse read both bounds as the FIRST 01:xx (05:00Z and
           05:45Z), a window that missed the whole second occurrence; the range now reaches to 06:45Z. */
        var range = new CustomRangeState();
        range.Set(Naive(11, 1, 0), Naive(11, 1, 12));

        range.ApplyEdit(Naive(11, 1, 1, 0), BoundSide.From, ServerZone);
        range.ApplyEdit(Naive(11, 1, 1, 45), BoundSide.To, ServerZone);

        Assert.Equal(Naive(11, 1, 5, 0), range.FromUtc);
        Assert.Equal(Naive(11, 1, 6, 45), range.ToUtc);
    }

    [Fact]
    public void ATypedRangeInTheSkippedHour_BothBoundsGiveTheChangeInstant()
    {
        /* 02:30 on the spring change day never happened. The old parse pushed it forward by the gap to 07:30Z. */
        var range = new CustomRangeState();
        range.Set(Naive(3, 8, 0), Naive(3, 8, 12));

        range.ApplyEdit(Naive(3, 8, 2, 30), BoundSide.From, ServerZone);
        range.ApplyEdit(Naive(3, 8, 2, 30), BoundSide.To, ServerZone);

        Assert.Equal(Naive(3, 8, 7, 0), range.FromUtc);
        Assert.Equal(Naive(3, 8, 7, 0), range.ToUtc);
    }

    [Fact]
    public void ATypedEditChangesOnlyItsOwnSide()
    {
        var range = new CustomRangeState();
        range.Set(Naive(11, 1, 5, 0), Naive(11, 1, 6, 45));

        range.ApplyEdit(Naive(11, 1, 0, 0), BoundSide.From, ServerZone);

        Assert.Equal(Naive(11, 1, 4, 0), range.FromUtc);
        Assert.Equal(Naive(11, 1, 6, 45), range.ToUtc);
    }

    [Fact]
    public void AHeldRange_SurvivesUtcServerUtc_AndLocalServerLocal_Switches()
    {
        /* 06:30Z to 07:30Z on the autumn change day reads 01:30 to 02:30 on the Eastern server: the SECOND 01:30.
           The old switch read the pickers back through the display time and returned 05:30Z, the first one. */
        var held = (From: Naive(11, 1, 6, 30), To: Naive(11, 1, 7, 30));
        var range = new CustomRangeState();
        range.Set(held.From, held.To);

        var utc = ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.UTC, Eastern);
        var server = ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.ServerTime, Eastern);
        var local = ViewerTimeHelper.DisplayZoneFor(TimeDisplayMode.LocalTime, Eastern);

        var utcText = range.Render(utc);
        Assert.Equal((Naive(11, 1, 6, 30), Naive(11, 1, 7, 30)), utcText);
        Assert.Equal((Naive(11, 1, 1, 30), Naive(11, 1, 2, 30)), range.Render(server));
        Assert.Equal(utcText, range.Render(utc));
        Assert.Equal(held.From, range.FromUtc);
        Assert.Equal(held.To, range.ToUtc);

        var localText = range.Render(local);
        _ = range.Render(server);
        Assert.Equal(localText, range.Render(local));
        Assert.Equal(held.From, range.FromUtc);
        Assert.Equal(held.To, range.ToUtc);

        /* The old parse of the Server picker text, for contrast: it names the first 01:30. */
        var oldParse = Eastern.ToUtc(Naive(11, 1, 1, 30));
        Assert.NotEqual(held.From, oldParse);
    }

    [Fact]
    public void ChoosingAPreset_HoldsNothing()
    {
        var range = new CustomRangeState();
        range.Set(Naive(11, 1, 6, 30), Naive(11, 1, 7, 30));

        range.Clear();

        Assert.False(range.IsCustom);
        Assert.Null(range.Render(ServerZone));
    }

    /* ---- source pins: the tab reaches the range only through the held instants ---- */

    [Fact]
    public void GetCustomRangeUtc_ReturnsTheHeldPair_AndDoesNotParseThePickers()
    {
        var body = StripComments(MemberText(TabSource(), "GetCustomRangeUtc"));

        Assert.DoesNotContain("DisplayToNaiveUtc", body);
        Assert.DoesNotContain("ConvertFromDisplay", body);
        Assert.DoesNotContain("GetDateTimeFromPickers", body);
        Assert.Contains("_customRange", body);
    }

    [Fact]
    public void TheDisplayModeSwitch_ParsesNothing_AndDrawsTheHeldRangeInTheNewZone()
    {
        var body = StripComments(MemberText(TabSource(), "TimeDisplayMode_SelectionChanged"));

        Assert.DoesNotContain("DisplayToNaiveUtc", body);
        Assert.DoesNotContain("ConvertFromDisplay", body);
        Assert.DoesNotContain("GetDateTimeFromPickers", body);
        Assert.DoesNotContain("ForDisplay", body);
        Assert.Contains("RenderCustomRange(", body);
        Assert.Contains("DisplayZoneFor(mode,", body);
    }

    [Theory]
    [InlineData("ApplyExternalTimeRange", "_customRange.Set(")]
    [InlineData("ApplyExternalTimeRange", "_customRange.Clear()")]
    [InlineData("SetToolbarWindowUtc", "_customRange.Set(")]
    [InlineData("TimeRangeCombo_SelectionChanged", "_customRange.Clear()")]
    [InlineData("TimeRangeCombo_SelectionChanged", "HoldPickersAsRange(")]
    [InlineData("ApplyPickerEdit", "_customRange.ApplyEdit(")]
    public void EveryPlaceThatSetsThePickers_HoldsTheRange(string member, string expected)
    {
        Assert.Contains(expected, StripComments(MemberText(TabSource(), member)));
    }

    [Theory]
    [InlineData("ApplyExternalTimeRange")]
    [InlineData("SetToolbarWindowUtc")]
    [InlineData("ApplyPickerEdit")]
    [InlineData("SetDisplayModeSelection")]
    [InlineData("RefreshServerClockAsync")]
    public void EveryPlaceThatMovesTheZoneOrTheRange_DrawsThePickersFromTheHeldRange(string member)
    {
        Assert.Contains("RenderCustomRange(", StripComments(MemberText(TabSource(), member)));
    }

    [Fact]
    public void ThePickersAreWrittenOnlyByTheDrawing_AndTheDefaultSeed()
    {
        var source = StripComments(TabSource());
        var rendering = StripComments(MemberText(TabSource(), "RenderCustomRange"));
        var comboSetup = StripComments(MemberText(TabSource(), "InitializeTimeComboBoxes"));
        var seed = StripComments(MemberText(TabSource(), "TimeRangeCombo_SelectionChanged"));

        var setters = new Regex(@"\b(?:From|To)(?:DatePicker\.SelectedDate|HourCombo\.SelectedIndex|MinuteCombo\.SelectedIndex)\s*=(?!=)");
        var total = setters.Matches(source).Count;
        var allowed = setters.Matches(rendering).Count + setters.Matches(comboSetup).Count + setters.Matches(seed).Count;

        Assert.Equal(allowed, total);
        Assert.Equal(6, setters.Matches(rendering).Count);
    }

    /* ---- helpers ---- */

    private static string TabSource([CallerFilePath] string thisFile = "") => ViewerSource("ViewerServerTab.TimeRange.cs", thisFile);

    /// <summary>The text of a viewer source file, found by walking up from this test file to the repo root.</summary>
    internal static string ViewerSource(string file, string thisFile)
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer", file);
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }

    /// <summary>The declaration and body of the member called <paramref name="name"/>: to the matching close brace,
    /// or to the semicolon of an expression-bodied member.</summary>
    internal static string MemberText(string source, string name)
    {
        var declaration = new Regex(
            @"^[ \t]*(?:public|private|internal|protected)[^\r\n;=]*\b" + Regex.Escape(name) + @"\s*(?=\(|=>|\{)",
            RegexOptions.Multiline);
        var match = declaration.Match(source);
        Assert.True(match.Success, $"member {name} not found");

        var i = match.Index + match.Length;
        if (source[i] == '(')
        {
            var depth = 0;
            for (; i < source.Length; i++)
            {
                if (source[i] == '(')
                {
                    depth++;
                }
                else if (source[i] == ')' && --depth == 0)
                {
                    break;
                }
            }
        }

        var after = source.IndexOfAny(new[] { '{', '=' }, i);
        if (source[after] == '=')
        {
            return source.Substring(match.Index, source.IndexOf(';', after) - match.Index + 1);
        }

        var braces = 0;
        for (var j = after; j < source.Length; j++)
        {
            if (source[j] == '{')
            {
                braces++;
            }
            else if (source[j] == '}' && --braces == 0)
            {
                return source.Substring(match.Index, j - match.Index + 1);
            }
        }

        throw new InvalidOperationException($"member {name} is not closed");
    }

    internal static string StripComments(string source) =>
        Regex.Replace(source, @"/\*.*?\*/|//[^\r\n]*", "", RegexOptions.Singleline);
}

/// <summary>
/// #4766: a list that shows several servers' times converts each row on ITS server's clock. With server A's clock
/// active on the tab, a row for server B in another zone reads B's hour in Server mode, a server with no collected
/// clock reads at the viewer machine's offset, and UTC and Local mode read as before. Covers the two lists the active
/// clock used to render alike: Manage Servers' "Last Collected" and the Alerts History time.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerMultiServerListClockTests
{
    private static readonly DateTime Collected = new(2026, 7, 1, 14, 30, 0, DateTimeKind.Unspecified);

    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -300);

    private static readonly ServerClock India = ServerClock.Resolve("India Standard Time", 330);

    private static ServerClock MachineOffsetFor(int serverId) =>
        ViewerTimeHelper.ClockForServerOrMachine(
            new Dictionary<int, ServerClock>(), serverId,
            TimeZoneInfo.FindSystemTimeZoneById("Pacific Standard Time"), new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

    private static ManagedServerListItem Server(int serverId, ServerClock? clock) =>
        new(new MonitoredServerRow { ServerId = serverId, Host = $"SQL{serverId}", Name = $"SQL{serverId}" }, isFavorite: false)
        {
            LastCollectedUtc = Collected,
            Clock = clock,
        };

    private static ViewerAlertRow Alert(int serverId, ServerClock? clock) => new()
    {
        AlertTime = Collected,
        ServerId = serverId,
        MetricName = "High CPU",
        CurrentValue = 95,
        ThresholdValue = 90,
        AlertSent = false,
        NotificationType = "tray",
        Muted = false,
        Clock = clock,
    };

    private static void WithActiveClock(TimeDisplayMode mode, ServerClock active, Action body)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            ViewerTimeHelper.ActiveServerClock = active;
            body();
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
        }
    }

    [Fact]
    public void ServerMode_EachRowConvertsOnItsOwnServersClock_NotTheActiveTabs()
    {
        WithActiveClock(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            /* 14:30Z: 10:30 in US Eastern (the active tab's server), 20:00 in India, 07:30 at a Pacific machine. */
            Assert.Equal("2026-07-01 10:30", Server(1, Eastern).LastCollectedDisplay);
            Assert.Equal("2026-07-01 20:00", Server(2, India).LastCollectedDisplay);
            Assert.Equal("2026-07-01 07:30", Server(3, MachineOffsetFor(3)).LastCollectedDisplay);

            Assert.Equal("2026-07-01 10:30:00", Alert(1, Eastern).TimeLocal);
            Assert.Equal("2026-07-01 20:00:00", Alert(2, India).TimeLocal);
            Assert.Equal("2026-07-01 07:30:00", Alert(3, MachineOffsetFor(3)).TimeLocal);
        });
    }

    [Fact]
    public void UtcMode_ReadsTheStoredTime_WhateverTheRowsClock()
    {
        WithActiveClock(TimeDisplayMode.UTC, Eastern, () =>
        {
            Assert.Equal("2026-07-01 14:30", Server(2, India).LastCollectedDisplay);
            Assert.Equal("2026-07-01 14:30:00", Alert(2, India).TimeLocal);
        });
    }

    [Fact]
    public void LocalMode_ReadsTheViewerMachinesTime_WhateverTheRowsClock()
    {
        WithActiveClock(TimeDisplayMode.LocalTime, Eastern, () =>
        {
            var expected = DateTime.SpecifyKind(Collected, DateTimeKind.Utc).ToLocalTime();

            Assert.Equal(expected.ToString("yyyy-MM-dd HH:mm"), Server(2, India).LastCollectedDisplay);
            Assert.Equal(expected.ToString("yyyy-MM-dd HH:mm:ss"), Alert(2, India).TimeLocal);
        });
    }

    [Fact]
    public void ARowBuiltWithoutAClock_FallsBackToTheActiveServersClock()
    {
        WithActiveClock(TimeDisplayMode.ServerTime, India, () =>
        {
            Assert.Equal("2026-07-01 20:00", Server(1, clock: null).LastCollectedDisplay);
            Assert.Equal("2026-07-01 20:00:00", Alert(1, clock: null).TimeLocal);
        });
    }

    /* ---- source pins: the lists read their clocks once and never render through the active tab's ---- */

    [Fact]
    public void ManageServers_ReadsTheClocksOncePerLoad_AndStampsEachRow()
    {
        var source = ViewerTypedRangeTests.ViewerSource("ManageServersWindow.xaml.cs", ThisFile());
        var display = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.MemberText(source, "LastCollectedDisplay"));
        var load = ViewerTypedRangeTests.StripComments(source);

        Assert.DoesNotContain("ForDisplay", display);
        Assert.Contains("ConvertToDisplay(utc, ViewerTimeHelper.CurrentDisplayMode, Clock", display);
        Assert.Single(Regex.Matches(load, @"GetServerClocksAsync\("));
        Assert.Contains("Clock = ViewerTimeHelper.ClockForServerOrMachine(clocks, row.ServerId, TimeZoneInfo.Local, nowUtc)", load);
    }

    [Fact]
    public void AlertHistory_ReadsTheClocksOncePerLoad_AndStampsEachRow()
    {
        var source = ViewerTypedRangeTests.ViewerSource("ViewerDataService.AlertHistory.cs", ThisFile());
        var display = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.MemberText(source, "TimeLocal"));
        var load = ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.MemberText(source, "GetAlertHistoryAsync"));

        Assert.DoesNotContain("ForDisplay", display);
        Assert.Contains("ConvertToDisplay(AlertTime, ViewerTimeHelper.CurrentDisplayMode, Clock", display);
        Assert.Single(Regex.Matches(load, @"GetServerClocksAsync\("));
        Assert.Contains("Clock = ViewerTimeHelper.ClockForServerOrMachine(clocks, rowServerId, TimeZoneInfo.Local, nowUtc)", load);
    }

    private static string ThisFile([CallerFilePath] string thisFile = "") => thisFile;
}
