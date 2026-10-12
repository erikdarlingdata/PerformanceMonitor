/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The plain text searches the analyzer asks when a regex is skipped must give the answer the regex gives, so a
/// statement is judged the same way whether or not the plan's time limit has passed.
/// </summary>
public sealed class PlanTextSearchTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(5);

    [Theory]
    [InlineData("SELECT 1 OPTION (OPTIMIZE FOR UNKNOWN)")]
    [InlineData("select 1 option (optimize   for\tunknown)")]
    [InlineData("OPTIMIZE FOR\r\nUNKNOWN")]
    [InlineData("OPTIMIZE FOR (@p UNKNOWN)")]
    [InlineData("OPTIMIZEFOR UNKNOWN")]
    [InlineData("OPTIMIZE FOR UNKNOWNS")]
    [InlineData("OPTIMIZE OPTIMIZE FOR UNKNOWN")]
    [InlineData("SELECT 1")]
    [InlineData("")]
    public void OptimizeForUnknown_MatchesTheRegex(string text)
    {
        var expected = Regex.IsMatch(text, @"OPTIMIZE\s+FOR\s+UNKNOWN", RegexOptions.IgnoreCase, Limit);
        Assert.Equal(expected, PlanTextSearch.ContainsWords(text, false, false, "OPTIMIZE", "FOR", "UNKNOWN"));
    }

    [Theory]
    [InlineData("OPTION (MAXDOP 1)")]
    [InlineData("option (maxdop  1)")]
    [InlineData("OPTION (MAXDOP 10)")]
    [InlineData("OPTION (MAXDOP 1_)")]
    [InlineData("OPTION (MAXDOP1)")]
    [InlineData("OPTION (MAXDOP MAXDOP 1)")]
    [InlineData("MAXDOP 1")]
    [InlineData("MAXDOP")]
    [InlineData("OPTION (MAXDOP 4)")]
    public void MaxdopOne_MatchesTheRegex(string text)
    {
        var expected = Regex.IsMatch(text, @"MAXDOP\s+1\b", RegexOptions.IgnoreCase, Limit);
        Assert.Equal(expected, PlanTextSearch.ContainsWords(text, false, true, "MAXDOP", "1"));
    }

    [Theory]
    [InlineData("WHERE a NOT IN (SELECT b FROM t)")]
    [InlineData("WHERE a not   in(1)")]
    [InlineData("WHERE a NOT INTEGER")]
    [InlineData("WHERE a ANOT IN (1)")]
    [InlineData("WHERE a IN (1)")]
    [InlineData("NOT")]
    public void NotIn_MatchesTheRegex(string text)
    {
        var expected = Regex.IsMatch(text, @"\bNOT\s+IN\b", RegexOptions.IgnoreCase, Limit);
        Assert.Equal(expected, PlanTextSearch.ContainsWords(text, true, true, "NOT", "IN"));
    }

    [Theory]
    [InlineData(" LOCAL FAST_FORWARD ")]
    [InlineData(" local ")]
    [InlineData(" LOCALE ")]
    [InlineData(" NONLOCAL ")]
    [InlineData(" _LOCAL ")]
    [InlineData("LOCAL")]
    [InlineData(" STATIC ")]
    public void Local_MatchesTheRegex(string text)
    {
        var expected = Regex.IsMatch(text, @"\bLOCAL\b", RegexOptions.IgnoreCase, Limit);
        Assert.Equal(expected, PlanTextSearch.ContainsWords(text, true, true, "LOCAL"));
    }

    [Theory]
    [InlineData("PROBE([Bitmap1001],[t].[b])")]
    [InlineData("PROBE([Bitmap1001],[t].[b]) AND PROBE([Bitmap1002],[t].[c])")]
    [InlineData("probe ([Bitmap1001],f(x)) or PROBE([Bitmap1002],[t].[c])")]
    [InlineData("[t].[a]=(1) AND PROBE([Bitmap1001],[t].[b])")]
    [InlineData("PROBE([Bitmap1001],[t].[b]) AND [t].[a]=(1)")]
    [InlineData("PROBE([Bitmap1001],[t].[b]")]
    [InlineData("PROBE")]
    [InlineData("ANDROID")]
    public void ProbeOnly_AgreesWithTheRegexPair_OnTheShapesAPlanHolds(string predicate)
    {
        var stripped = Regex.Replace(predicate, @"PROBE\s*\([^()]*(?:\([^()]*\)[^()]*)*\)", "", RegexOptions.IgnoreCase, Limit).Trim();
        stripped = Regex.Replace(stripped, @"\b(AND|OR)\b", "", RegexOptions.IgnoreCase, Limit).Trim();

        Assert.Equal(stripped.Length == 0, PlanTextSearch.IsProbeOnly(predicate));
    }

    [Fact]
    public void TheSearches_AreLinear_OnALongInputWithManyNearMisses()
    {
        var text = string.Concat(System.Linq.Enumerable.Repeat("OPTIMIZE FOR   ", 200_000)) + "UNKNOWN";
        var clock = System.Diagnostics.Stopwatch.StartNew();

        Assert.True(PlanTextSearch.ContainsWords(text, false, false, "OPTIMIZE", "FOR", "UNKNOWN"));
        Assert.False(PlanTextSearch.ContainsWords(new string(' ', 4_000_000), true, true, "NOT", "IN"));
        Assert.False(PlanTextSearch.ContainsWords("MAXDOP" + new string(' ', 4_000_000), false, true, "MAXDOP", "1"));
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5), $"took {clock.Elapsed}");
    }
}
