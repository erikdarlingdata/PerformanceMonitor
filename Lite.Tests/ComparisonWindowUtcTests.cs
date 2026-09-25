/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4284: <c>GetQueryStatsComparisonAsync</c>/<c>GetProcedureStatsComparisonAsync</c>/<c>GetQueryStoreComparisonAsync</c>
/// (<c>Lite/Services/LocalDataService.QueryStats.cs</c> and <c>.QueryStore.cs</c>) compare their bounds straight
/// against UTC <c>collection_time</c>, with no offset conversion of their own. Two callers used to hand them a
/// server-local window instead: a custom range (ServerTab.Comparison.cs's old parameterless
/// <c>GetComparisonRange()</c>, and ServerTab.Refresh.cs's <c>cStart</c>/<c>cEnd</c>) and a slicer drag
/// (ServerTab.Slicers.cs's <c>fromServer</c>/<c>toServer</c>). On a server not on UTC, that shifted the
/// comparison window by the server's offset. Pins <see cref="ServerTab.ShiftComparisonRange"/>'s pure day-shift
/// arithmetic directly, and the call sites via source scan (the handlers are UI-bound, same approach
/// QueryWindowTruncationTests took for #4279/#4231).
/// </summary>
public sealed class ComparisonWindowUtcTests
{
    /// <summary>
    /// #4284 ruling item 2/6: the shift is a fixed duration (1 or 7 days), so it commutes with whatever UTC
    /// offset built the "current" window -- a server at UTC+5 and one at UTC-7 both get a baseline that is
    /// exactly their own current window moved back by the same number of days, covering the SAME span
    /// (ruling item 3: after a slicer drag, the baseline is the dragged window shifted back, not the whole
    /// preset range).
    /// </summary>
    [Theory]
    [InlineData(300)]   // UTC+5
    [InlineData(-420)]  // UTC-7
    public void ShiftComparisonRange_MatchesGridsUtcWindow_ShiftedByOneOrSevenDays(int utcOffsetMinutes)
    {
        // The same derivation CompareToCombo_SelectionChanged and the six ServerTab.Refresh.cs call sites
        // now use for "current": server-local pickers converted back to UTC by LocalDataService.GetQueriesTabWindowUtc.
        var fromServerLocal = new DateTime(2026, 3, 10, 9, 0, 0, DateTimeKind.Unspecified);
        var toServerLocal = new DateTime(2026, 3, 10, 17, 0, 0, DateTimeKind.Unspecified);
        var (currentStartUtc, currentEndUtc) = LocalDataService.GetQueriesTabWindowUtc(24, fromServerLocal, toServerLocal, utcOffsetMinutes);

        var yesterday = ServerTab.ShiftComparisonRange(1, currentStartUtc, currentEndUtc);
        Assert.NotNull(yesterday);
        Assert.Equal(currentStartUtc.AddDays(-1), yesterday!.Value.From);
        Assert.Equal(currentEndUtc.AddDays(-1), yesterday.Value.To);
        Assert.Equal(currentEndUtc - currentStartUtc, yesterday.Value.To - yesterday.Value.From);

        var lastWeek = ServerTab.ShiftComparisonRange(2, currentStartUtc, currentEndUtc);
        Assert.NotNull(lastWeek);
        Assert.Equal(currentStartUtc.AddDays(-7), lastWeek!.Value.From);
        Assert.Equal(currentEndUtc.AddDays(-7), lastWeek.Value.To);

        var sameDayLastWeek = ServerTab.ShiftComparisonRange(3, currentStartUtc, currentEndUtc);
        Assert.Equal(lastWeek, sameDayLastWeek);

        Assert.Null(ServerTab.ShiftComparisonRange(0, currentStartUtc, currentEndUtc));
        Assert.Null(ServerTab.ShiftComparisonRange(-1, currentStartUtc, currentEndUtc));
    }

    /// <summary>
    /// #4284 ruling item 6's source pin: every Refresh...ComparisonAsync call site passes a UTC bound -- never
    /// the server-local fromServer/toServer a slicer drag produces, nor a cStart built from fromDate in
    /// ServerTab.Refresh.cs. Confirmed by checking out ServerTab.Comparison.cs, ServerTab.Refresh.cs and
    /// ServerTab.Slicers.cs from origin/dev at 80948a66 (pre-#4284) and re-running this test: every assertion
    /// below failed against that pre-fix source.
    /// </summary>
    [Fact]
    public void ComparisonCallSites_TakeUtcBounds_NotServerLocalOnes()
    {
        var comparisonSource = File.ReadAllText(ControlsFile("ServerTab.Comparison.cs"));
        var refreshSource = File.ReadAllText(ControlsFile("ServerTab.Refresh.cs"));
        var slicersSource = File.ReadAllText(ControlsFile("ServerTab.Slicers.cs"));
        var combinedSource = comparisonSource + "\n" + refreshSource + "\n" + slicersSource;

        // No caller anywhere still calls the old parameterless GetComparisonRange() -- it now requires the
        // caller's own current UTC window as an argument.
        Assert.False(Regex.IsMatch(combinedSource, @"GetComparisonRange\(\)"),
            "a caller still invokes the parameterless GetComparisonRange() (#4284) -- it now takes the " +
            "caller's current UTC window as an argument.");

        // IsQueryStatsComparisonActive tests the combo directly (ruling item 2), not through GetComparisonRange.
        Assert.Contains("IsQueryStatsComparisonActive => CompareToCombo != null && CompareToCombo.SelectedIndex > 0;", comparisonSource);

        // Slicer handlers: comparisons take e.StartUtc/e.EndUtc, never the server-local fromServer/toServer
        // the grid read beside them uses.
        var slicerComparisonCallsOnUtc = Regex.Matches(slicersSource,
            @"Refresh(?:QueryStats|ProcStats|QueryStore)ComparisonAsync\(e\.StartUtc,\s*e\.EndUtc\)").Count;
        Assert.True(slicerComparisonCallsOnUtc == 3,
            $"expected all 3 OnXSlicerChanged comparison calls to pass e.StartUtc, e.EndUtc (found {slicerComparisonCallsOnUtc}) " +
            "-- fromServer/toServer are server-local and the comparison reads compare straight against UTC " +
            "collection_time (#4284).");
        Assert.False(Regex.IsMatch(slicersSource, @"Refresh(?:QueryStats|ProcStats|QueryStore)ComparisonAsync\([^)]*fromServer,\s*toServer\)"),
            "a slicer comparison call still passes server-local fromServer/toServer (#4284).");

        // ServerTab.Refresh.cs: no comparison call site builds its own cStart from fromDate any more.
        Assert.False(Regex.IsMatch(refreshSource, @"var\s+cStart\d?\s*="),
            "a comparison call site in ServerTab.Refresh.cs still builds a server-local cStart from fromDate (#4284).");

        // The six Refresh.cs comparison calls each share the IDENTICAL windowStart/windowEnd tuple the
        // banner call right beside them uses -- computed once, handed to both (ruling item 1).
        var pairedCalls = Regex.Matches(refreshSource,
            @"Refresh(?:QueryStats|ProcStats|QueryStore)ComparisonAsync\((windowStart\d?),\s*(windowEnd\d?)\);\s*" +
            @"await RefreshWindowTruncatedBannerAsync\(QueryWindowRelation\.\w+,\s*\w+,\s*\1,\s*\2\);");
        Assert.True(pairedCalls.Count == 6,
            $"expected all 6 ServerTab.Refresh.cs comparison calls to share the SAME windowStart/windowEnd tuple " +
            $"the adjacent banner call uses (found {pairedCalls.Count}) -- a divergent tuple means the comparison " +
            "and the banner could disagree about the window again (#4284).");

        // CompareToCombo_SelectionChanged routes its three comparison refreshes through the same
        // GetQueriesTabWindowUtc tuple the grid reads use, not a bare DateTime.UtcNow/fromDate pair.
        Assert.Contains(
            "var (currentStart, currentEnd) = LocalDataService.GetQueriesTabWindowUtc(hoursBack, fromDate, toDate, ServerTimeHelper.UtcOffsetMinutes);",
            comparisonSource);
    }

    private static string ControlsFile(string name) => Path.Combine(ControlsDir(), name);

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
