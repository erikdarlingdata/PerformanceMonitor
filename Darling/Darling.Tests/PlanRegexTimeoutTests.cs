/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The plan analyzer runs regular expressions on statement text and predicate strings from plan XML that a
/// caller may have written. Each one has a 250 ms match timeout, and a timeout answers "nothing found" instead of
/// throwing: <c>IsMatch</c> gives false, <c>Match</c> gives no match, <c>Matches</c> gives an empty list and
/// <c>Replace</c> returns the input unchanged. A tiny timeout through the helpers' <see cref="TimeSpan"/> overloads
/// forces the timeout here in a few milliseconds.
/// </summary>
public sealed class PlanRegexTimeoutTests
{
    // Catastrophic backtracking: about 2^40 ways to fail the trailing $, far past any timeout.
    private const string SlowPattern = @"(a+)+$";
    private static readonly string SlowInput = new string('a', 40) + "!";
    private static readonly TimeSpan Tiny = TimeSpan.FromMilliseconds(1);

    [Fact]
    public void TheCraftedInput_ReallyPassesTheTimeout()
    {
        // Guards the tests below against a pattern the engine happens to handle fast.
        Assert.Throws<RegexMatchTimeoutException>(() => Regex.IsMatch(SlowInput, SlowPattern, RegexOptions.None, Tiny));
    }

    [Fact]
    public void DefaultTimeout_IsAQuarterOfASecond()
    {
        Assert.Equal(250, PlanRegex.TimeoutMilliseconds);
    }

    [Fact]
    public void IsMatch_OnTimeout_ReturnsFalse_InsteadOfThrowing()
    {
        Assert.False(PlanRegex.IsMatch(SlowInput, SlowPattern, RegexOptions.None, Tiny));
    }

    [Fact]
    public void Match_OnTimeout_ReturnsNoMatch_InsteadOfThrowing()
    {
        var match = PlanRegex.Match(SlowInput, SlowPattern, RegexOptions.None, Tiny);

        Assert.False(match.Success);
    }

    [Fact]
    public void Replace_OnTimeout_ReturnsTheInputUnchanged_InsteadOfThrowing()
    {
        var result = PlanRegex.Replace(SlowInput, SlowPattern, "", RegexOptions.None, Tiny);

        Assert.Equal(SlowInput, result);
    }

    [Fact]
    public void RegexOverloads_OnTimeout_GiveTheSameFallbacks_ForAGeneratedRegexField()
    {
        // A [GeneratedRegex] field carries its timeout in the instance, so the test builds one with a tiny timeout.
        var regex = new Regex(SlowPattern, RegexOptions.None, Tiny);
        Assert.Throws<RegexMatchTimeoutException>(() => regex.IsMatch(SlowInput));
        // A MatchCollection runs lazily, so the timeout surfaces while the caller enumerates it.
        Assert.Throws<RegexMatchTimeoutException>(() => regex.Matches(SlowInput).Cast<Match>().ToList());

        Assert.False(PlanRegex.IsMatch(regex, SlowInput));
        Assert.False(PlanRegex.Match(regex, SlowInput).Success);
        Assert.Empty(PlanRegex.Matches(regex, SlowInput));
    }

    [Theory]
    [InlineData("SELECT 1 OPTION (MAXDOP 1)", @"MAXDOP\s+1\b", RegexOptions.IgnoreCase)]
    [InlineData("SELECT 1 OPTION (MAXDOP 10)", @"MAXDOP\s+1\b", RegexOptions.IgnoreCase)]
    [InlineData("select top 5 * from t", @"\bTOP\b", RegexOptions.None)]
    [InlineData("SELECT TOP 5 * FROM t", @"\bTOP\b", RegexOptions.None)]
    [InlineData("", @"\bTOP\b", RegexOptions.None)]
    public void IsMatch_GivesTheSameAnswerAsRegex_OnNormalInput(string input, string pattern, RegexOptions options)
    {
        Assert.Equal(Regex.IsMatch(input, pattern, options), PlanRegex.IsMatch(input, pattern, options));
    }

    [Fact]
    public void Match_GivesTheSameMatchAsRegex_OnNormalInput()
    {
        const string text = "DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT 1";
        const string pattern = @"\bDECLARE\s+\w+\s+(?:INSENSITIVE\s+|SCROLL\s+)*CURSOR\b(.*?)\bFOR\b";
        const RegexOptions options = RegexOptions.IgnoreCase | RegexOptions.Singleline;

        var expected = Regex.Match(text, pattern, options);
        var actual = PlanRegex.Match(text, pattern, options);

        Assert.True(actual.Success);
        Assert.Equal(expected.Groups[1].Value, actual.Groups[1].Value);
        Assert.Equal(" LOCAL FAST_FORWARD ", actual.Groups[1].Value);
    }

    [Fact]
    public void Replace_GivesTheSameTextAsRegex_OnNormalInput()
    {
        const string predicate = "[t].[a]=(1) AND PROBE([Bitmap1001],[t].[b])";
        const string pattern = @"\s*AND\s+PROBE\s*\([^()]*(?:\([^()]*\)[^()]*)*\)";

        var actual = PlanRegex.Replace(predicate, pattern, "", RegexOptions.IgnoreCase);

        Assert.Equal(Regex.Replace(predicate, pattern, "", RegexOptions.IgnoreCase), actual);
        Assert.Equal("[t].[a]=(1)", actual);
    }

    [Fact]
    public void Matches_ListsEveryMatch_InOrder_OnNormalInput()
    {
        var regex = new Regex(@"\[\w+\]", RegexOptions.None, TimeSpan.FromSeconds(5));

        var matches = PlanRegex.Matches(regex, "[a].[b].[c]");

        Assert.Equal(new[] { "[a]", "[b]", "[c]" }, matches.Select(m => m.Value).ToArray());
        Assert.Equal(regex.Matches("[a].[b].[c]").Count, matches.Count);
        Assert.Equal("[b]", matches[^2].Value);
    }

    // ---- The analyzer itself: every regex is behind a helper ----

    private static string AnalyzerSource([CallerFilePath] string thisFile = "")
    {
        var path = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "PerformanceMonitor.PlanAnalysis", "PlanAnalyzer.cs"));
        // Comments mention the helpers by name, and PlanAnalyzer.cs has block comments whose continuation lines
        // carry no prefix, so only the walk separates them from code. It also blanks each regex's pattern text.
        return CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path));
    }

    [Fact]
    public void PlanAnalyzer_RunsNoRegexOutsideTheHelpers()
    {
        var code = AnalyzerSource();

        // Static calls (Regex.IsMatch, Regex.Replace ...) and calls on a field or a generated-regex method.
        Assert.DoesNotMatch(new Regex(@"(?<![\w.])Regex\.(IsMatch|Match|Matches|Replace|Split|Count|EnumerateMatches)\("), code);
        Assert.DoesNotMatch(new Regex(@"\b(?!PlanRegex\b)\w*Regex\.(IsMatch|Match|Matches|Replace|Split|Count|EnumerateMatches)\("), code);
        Assert.DoesNotMatch(new Regex(@"\w+RegExp\(\)\.\w+\("), code);

        // Not vacuous: the sites are there, and they go through the helpers.
        Assert.True(Regex.Matches(code, @"PlanRegex\.(IsMatch|Match|Matches|Replace)\(").Count >= 25,
            "PlanAnalyzer.cs is expected to route its 27 regex calls through PlanRegex.");
    }

    [Fact]
    public void PlanAnalyzer_EveryGeneratedRegexCarriesTheTimeout()
    {
        var code = AnalyzerSource();

        var attributes = Regex.Matches(code, @"\[GeneratedRegex\([^\n]*\)\]").Cast<Match>().Select(m => m.Value).ToList();
        Assert.Equal(Regex.Matches(code, @"\[GeneratedRegex\(").Count, attributes.Count);
        Assert.True(attributes.Count >= 13, $"expected the 13 generated regexes, found {attributes.Count}.");
        foreach (var attribute in attributes)
            Assert.Contains("matchTimeoutMilliseconds: PlanRegex.TimeoutMilliseconds", attribute);
    }
}
