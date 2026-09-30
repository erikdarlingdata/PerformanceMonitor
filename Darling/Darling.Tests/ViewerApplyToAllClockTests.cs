/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: "Apply to All" hands a range to every other open server tab, hidden ones too, and a tab that reloads
/// writes ITS server's clock onto the process-wide <c>ViewerTimeHelper.ActiveServerClock</c> before it renders. If a
/// hidden tab reloaded on that call, the last one to finish would leave its clock on the tab on screen: with a US
/// Eastern tab visible and a UTC tab behind it, the Eastern tab's 06:30Z point hovered as 06:30:00 instead of
/// 01:30:00 -05:00 and its ticks were drawn on UTC hours until that tab refreshed. Only the visible tab reloads; a
/// hidden one reloads when it is selected, which is when it draws the range it now holds.
/// </summary>
/* Source pins, not a run: a ViewerServerTab needs the viewer's application resources (its XAML resolves theme
   styles such as NumericCell at construction), and the test process has no Application to hold them. What the pins
   cannot catch: a hidden tab whose IsVisible reads true in the running app, or a different call that writes the
   clock. Both are outside what the source text says. */
public sealed class ViewerApplyToAllClockTests
{
    private static string Member(string file, string name)
        => ViewerTypedRangeTests.StripComments(ViewerTypedRangeTests.MemberText(ViewerTypedRangeTests.ViewerSource(file, ThisFile()), name));

    private static string ThisFile([CallerFilePath] string thisFile = "") => thisFile;

    [Fact]
    public void ApplyExternalTimeRange_ReloadsOnlyWhenTheTabIsVisible_AndItsReloadIsTheLastStatement()
    {
        var body = Member("ViewerServerTab.TimeRange.cs", "ApplyExternalTimeRange");

        /* The last statement of the method, then the method's own closing brace. An unguarded call anywhere
           else in the method would also be the only one the count below allows for, so the count is pinned too. */
        Assert.Matches(@"if\s*\(\s*IsVisible\s*\)\s*\{\s*_\s*=\s*RefreshActiveInnerTabAsync\(\)\s*;\s*\}\s*\}\s*$", body);
        Assert.Single(Regex.Matches(body, @"RefreshActiveInnerTabAsync\s*\("));
    }

    [Fact]
    public void ApplyExternalTimeRange_NeverWritesTheSharedClockItself()
    {
        var body = Member("ViewerServerTab.TimeRange.cs", "ApplyExternalTimeRange");

        Assert.DoesNotContain("ActiveServerClock", body);
        Assert.DoesNotContain("ApplyServerClockToHelper", body);
        Assert.DoesNotContain("UtcOffsetMinutes", body);
    }

    [Fact]
    public void ThePlaceThatBroadcastsTheRange_SkipsOnlyTheSource_AndASelectedTabReloadsItself()
    {
        /* The guard leaves a hidden tab stale until it is selected, which is safe only while selecting it reloads
           it: the tab switch runs RefreshVisibleAsync, which loads the selected server tab's active inner tab. */
        var broadcast = Member("MainWindow.xaml.cs", "OnApplyTimeRangeToAllRequested");
        var selection = Member("MainWindow.xaml.cs", "MainTabs_SelectionChanged");
        var load = Member("MainWindow.xaml.cs", "LoadVisibleTabAsync");

        Assert.Contains("ApplyExternalTimeRange(index, customFromUtc, customToUtc)", broadcast);
        Assert.Contains("!ReferenceEquals(serverTab, source)", broadcast);
        Assert.Contains("await RefreshVisibleAsync()", selection);
        Assert.Matches(@"case\s+TabItem\s*\{\s*Content:\s*ViewerServerTab\s+serverTab\s*\}\s*:\s*await\s+serverTab\.RefreshActiveInnerTabAsync\(\)", load);
    }
}
