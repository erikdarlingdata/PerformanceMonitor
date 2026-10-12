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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The plan-analysis project runs regular expressions on statement text and predicate strings from plan XML that a
/// caller may have written. Two limits apply: each call has a 250 ms match timeout, and all the calls of one plan
/// share a time budget. Neither one throws, and neither one quietly changes an answer: the check is recorded, the
/// analyzer notes it on the statement, and each call site answers on the side that keeps a warning. A tiny timeout
/// through the helpers' <see cref="TimeSpan"/> overloads, and a zero budget, force both in a few milliseconds.
/// </summary>
[Collection("timing")]
public sealed class PlanRegexTimeoutTests
{
    // Unbounded backtracking: about 2^40 ways to fail the trailing $, far past any timeout.
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
    public void DefaultTimeout_IsAQuarterOfASecond_AndThePlanBudgetIsTwoSeconds()
    {
        Assert.Equal(250, PlanRegex.TimeoutMilliseconds);
        Assert.Equal(2000, PlanRegex.PlanBudgetMilliseconds);
    }

    // ---- A timeout: the answer the call site chose, and a record of the skipped check ----

    [Fact]
    public void IsMatch_OnTimeout_GivesTheCallersAnswer_InsteadOfThrowing()
    {
        using var plan = PlanRegex.BeginPlan();

        Assert.False(PlanRegex.IsMatch(SlowInput, SlowPattern, RegexOptions.None, Tiny, "first check"));
        Assert.True(PlanRegex.IsMatch(SlowInput, SlowPattern, RegexOptions.None, Tiny, "second check", onTimeout: true));

        Assert.Equal(new[] { "first check", "second check" }, PlanRegex.TakeSkipped());
        Assert.Empty(PlanRegex.TakeSkipped());
    }

    [Fact]
    public void Match_OnTimeout_ReturnsNoMatch_AndSaysItDidNotComplete()
    {
        using var plan = PlanRegex.BeginPlan();

        var match = PlanRegex.Match(SlowInput, SlowPattern, RegexOptions.None, Tiny, "a match", out var completed);

        Assert.False(match.Success);
        Assert.False(completed);
        Assert.Equal(new[] { "a match" }, PlanRegex.TakeSkipped());
    }

    [Fact]
    public void Replace_OnTimeout_ReturnsTheInputUnchanged_AndSaysItDidNotComplete()
    {
        using var plan = PlanRegex.BeginPlan();

        var result = PlanRegex.Replace(SlowInput, SlowPattern, "", RegexOptions.None, Tiny, "a replace", out var completed);

        Assert.Equal(SlowInput, result);
        Assert.False(completed);
        Assert.Equal(new[] { "a replace" }, PlanRegex.TakeSkipped());
    }

    [Fact]
    public void RegexOverloads_OnTimeout_GiveTheSameFallbacks_ForARegexInstance()
    {
        // A [GeneratedRegex] field carries its timeout in the instance, so the test builds one with a tiny timeout.
        var regex = new Regex(SlowPattern, RegexOptions.None, Tiny);
        using var plan = PlanRegex.BeginPlan();

        Assert.False(PlanRegex.IsMatch(regex, SlowInput, "instance"));
        Assert.True(PlanRegex.IsMatch(regex, SlowInput, "instance", onTimeout: true));
        Assert.False(PlanRegex.TryIsMatch(regex, SlowInput, "instance", out var matched));
        Assert.False(matched);
        Assert.False(PlanRegex.Match(regex, SlowInput, "instance", out var completed).Success);
        Assert.False(completed);
        Assert.Equal(new[] { "instance" }, PlanRegex.TakeSkipped());
    }

    [Fact]
    public void Matches_KeepsTheMatchesFoundBeforeATimeout_AndSaysThePassWasCutShort()
    {
        // "foo" is found at once; the scan after it hits the slow branch.
        var regex = new Regex(@"foo|(a+)+$", RegexOptions.None, Tiny);
        var input = "foo " + SlowInput;
        using var plan = PlanRegex.BeginPlan();

        var run = PlanRegex.Matches(regex, input, "a pass");
        var found = run.Select(m => m.Value).ToList();

        Assert.Equal(new[] { "foo" }, found);
        Assert.True(run.Incomplete);
        Assert.Equal(new[] { "a pass" }, PlanRegex.TakeSkipped());
    }

    [Fact]
    public void Matches_IsLazy_SoAnEarlyExitNeverReachesTheSlowTail()
    {
        var regex = new Regex(@"foo|(a+)+$", RegexOptions.None, Tiny);
        var input = "foo " + SlowInput;
        using var plan = PlanRegex.BeginPlan();

        var run = PlanRegex.Matches(regex, input, "a pass");
        foreach (var match in run)
        {
            Assert.Equal("foo", match.Value);
            break;
        }

        Assert.False(run.Incomplete);
        Assert.Empty(PlanRegex.TakeSkipped());
    }

    // ---- The plan budget ----

    [Fact]
    public void ASpentBudget_SkipsEveryFurtherCall_AndRecordsEachCheckOnce()
    {
        using var plan = PlanRegex.BeginPlan(TimeSpan.Zero);

        Assert.False(PlanRegex.IsMatch("TOP 5", @"\bTOP\b", RegexOptions.None, "top check"));
        Assert.True(PlanRegex.IsMatch("TOP 5", @"\bTOP\b", RegexOptions.None, "top check", onTimeout: true));
        Assert.False(PlanRegex.Match("a", "a", RegexOptions.None, "a match", out var completed).Success);
        Assert.False(completed);
        var run = PlanRegex.Matches(new Regex("a", RegexOptions.None, TimeSpan.FromSeconds(1)), "aaa", "a pass");
        Assert.Empty(run);
        Assert.True(run.Incomplete);

        Assert.Equal(new[] { "top check", "a match", "a pass" }, PlanRegex.TakeSkipped());
    }

    [Fact]
    public void TheBudget_IsSharedByTheCallsOfOnePlan_NotGivenToEachOne()
    {
        // 30 ms in all: the first slow call runs into the budget's remainder instead of the 250 ms default,
        // and the calls after it are skipped without running.
        using var plan = PlanRegex.BeginPlan(TimeSpan.FromMilliseconds(30));
        var clock = System.Diagnostics.Stopwatch.StartNew();

        Assert.False(PlanRegex.IsMatch(SlowInput, SlowPattern, RegexOptions.None, "slow one"));
        var afterFirst = clock.Elapsed;
        for (var i = 0; i < 20; i++)
            Assert.True(PlanRegex.IsMatch(SlowInput, SlowPattern, RegexOptions.None, "slow " + i, onTimeout: true));

        Assert.True(afterFirst < TimeSpan.FromMilliseconds(PlanRegex.TimeoutMilliseconds), $"the first call took {afterFirst}");
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(2), $"21 calls took {clock.Elapsed}");
        Assert.Equal(21, PlanRegex.TakeSkipped().Count);
    }

    [Fact]
    public void ABudget_IsEndedWithItsScope_AndANestedScopeSharesTheOuterOne()
    {
        using (var outer = PlanRegex.BeginPlan(TimeSpan.Zero))
        {
            using (var inner = PlanRegex.BeginPlan(TimeSpan.FromSeconds(30)))
            {
                Assert.False(PlanRegex.IsMatch("a", "a", RegexOptions.None, "inside"));
            }

            Assert.False(PlanRegex.IsMatch("a", "a", RegexOptions.None, "outer still spent"));
            Assert.Equal(new[] { "inside", "outer still spent" }, PlanRegex.TakeSkipped());
        }

        // Outside any plan there is no budget, and nothing is recorded.
        Assert.True(PlanRegex.IsMatch("a", "a", RegexOptions.None, "no plan"));
        Assert.Empty(PlanRegex.TakeSkipped());
    }

    [Fact]
    public void TheBudget_IsPerThread()
    {
        using var plan = PlanRegex.BeginPlan(TimeSpan.Zero);
        var otherThread = true;
        var thread = new Thread(() => otherThread = PlanRegex.IsMatch("a", "a", RegexOptions.None, "other thread"));
        thread.Start();
        thread.Join();

        Assert.True(otherThread);
    }

    // ---- Normal input: the same answers as the plain Regex calls ----

    [Theory]
    [InlineData("SELECT 1 OPTION (MAXDOP 1)", @"MAXDOP\s+1\b", RegexOptions.IgnoreCase)]
    [InlineData("SELECT 1 OPTION (MAXDOP 10)", @"MAXDOP\s+1\b", RegexOptions.IgnoreCase)]
    [InlineData("select top 5 * from t", @"\bTOP\b", RegexOptions.None)]
    [InlineData("SELECT TOP 5 * FROM t", @"\bTOP\b", RegexOptions.None)]
    [InlineData("", @"\bTOP\b", RegexOptions.None)]
    public void IsMatch_GivesTheSameAnswerAsRegex_OnNormalInput(string input, string pattern, RegexOptions options)
    {
        using var plan = PlanRegex.BeginPlan();

        Assert.Equal(Regex.IsMatch(input, pattern, options), PlanRegex.IsMatch(input, pattern, options, "a check"));
        Assert.Empty(PlanRegex.TakeSkipped());
    }

    [Fact]
    public void Match_GivesTheSameMatchAsRegex_OnNormalInput()
    {
        const string text = "DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT 1";
        const string pattern = @"\bDECLARE\s+\w+\s+(?:INSENSITIVE\s+|SCROLL\s+)*CURSOR\b(.*?)\bFOR\b";
        const RegexOptions options = RegexOptions.IgnoreCase | RegexOptions.Singleline;

        var expected = Regex.Match(text, pattern, options);
        var actual = PlanRegex.Match(text, pattern, options, "a match", out var completed);

        Assert.True(completed);
        Assert.True(actual.Success);
        Assert.Equal(expected.Groups[1].Value, actual.Groups[1].Value);
        Assert.Equal(" LOCAL FAST_FORWARD ", actual.Groups[1].Value);
    }

    [Fact]
    public void Replace_GivesTheSameTextAsRegex_OnNormalInput()
    {
        const string predicate = "[t].[a]=(1) AND PROBE([Bitmap1001],[t].[b])";
        const string pattern = @"\s*AND\s+PROBE\s*\([^()]*(?:\([^()]*\)[^()]*)*\)";

        var actual = PlanRegex.Replace(predicate, pattern, "", RegexOptions.IgnoreCase, "a replace", out var completed);

        Assert.True(completed);
        Assert.Equal(Regex.Replace(predicate, pattern, "", RegexOptions.IgnoreCase), actual);
        Assert.Equal("[t].[a]=(1)", actual);
    }

    [Fact]
    public void Matches_ListsEveryMatch_InOrder_OnNormalInput()
    {
        var regex = new Regex(@"\[\w+\]", RegexOptions.None, TimeSpan.FromSeconds(5));

        var run = PlanRegex.Matches(regex, "[a].[b].[c]", "a pass");

        Assert.Equal(new[] { "[a]", "[b]", "[c]" }, run.Select(m => m.Value).ToArray());
        Assert.False(run.Incomplete);
    }

    // ---- The analyzer: a cut-short check is named, and each site keeps its warning ----

    private static PlanStatement Analyze(string statementText, TimeSpan budget)
    {
        var statement = new PlanStatement { StatementText = statementText };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [statement] }] };
        PlanAnalyzer.Analyze(plan, null, null, budget, CancellationToken.None);
        return statement;
    }

    [Fact]
    public void Analyze_WithAnEmptyBudget_NamesTheSkippedChecks_AndKeepsTheOptimizeForUnknownWarning()
    {
        const string text = "SELECT a FROM dbo.t OPTION (OPTIMIZE FOR UNKNOWN)";

        var spent = Analyze(text, TimeSpan.Zero);
        var normal = Analyze(text, TimeSpan.FromSeconds(30));

        // The hint is in the text, and a check that could not run keeps the warning.
        Assert.Contains(spent.PlanWarnings, w => w.WarningType == "Optimize For Unknown");
        Assert.Contains(normal.PlanWarnings, w => w.WarningType == "Optimize For Unknown");

        var note = Assert.Single(spent.PlanWarnings, w => w.WarningType == PlanAnalyzer.SkippedChecksWarningType);
        Assert.Contains("too large or complex", note.Message);
        Assert.Contains("OPTIMIZE FOR UNKNOWN hint", note.Message);
        Assert.Equal(40, note.RuleNumber);
        Assert.DoesNotContain(normal.PlanWarnings, w => w.WarningType == PlanAnalyzer.SkippedChecksWarningType);
    }

    [Fact]
    public void Analyze_WithAnEmptyBudget_NamesTheChecksOnEveryStatement()
    {
        var first = new PlanStatement { StatementText = "SELECT 1 OPTION (OPTIMIZE FOR UNKNOWN)" };
        var second = new PlanStatement { StatementText = "SELECT 2 OPTION (OPTIMIZE FOR UNKNOWN)" };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [first, second] }] };

        PlanAnalyzer.Analyze(plan, null, null, TimeSpan.Zero, CancellationToken.None);

        Assert.Single(first.PlanWarnings, w => w.WarningType == PlanAnalyzer.SkippedChecksWarningType);
        Assert.Single(second.PlanWarnings, w => w.WarningType == PlanAnalyzer.SkippedChecksWarningType);
    }

    [Fact]
    public void ReadsAnotherInput_WhenTheCheckIsCutShort_KeepsTheWarning()
    {
        // "[@p1]" alone is a parameter list: no other input.
        Assert.False(PlanAnalyzer.ReadsAnotherInput("[@p1]"));

        using var plan = PlanRegex.BeginPlan(TimeSpan.Zero);
        Assert.True(PlanAnalyzer.ReadsAnotherInput("[@p1]"));
        Assert.Contains("lookup branch column references", PlanRegex.TakeSkipped());
    }

    [Fact]
    public void DetectNonSargablePattern_WhenACheckIsCutShort_ReportsTheUnverifiedReason_NotNothing()
    {
        const string predicate = "[t].[a]=[@1]";
        Assert.Null(PlanAnalyzer.DetectNonSargablePattern(predicate));

        using var plan = PlanRegex.BeginPlan(TimeSpan.Zero);
        Assert.Equal(PlanAnalyzer.UnverifiedNonSargableReason, PlanAnalyzer.DetectNonSargablePattern(predicate));
    }

    [Fact]
    public void ConvertImplicitWrapsColumn_WhenTheCheckIsCutShort_AssumesTheWorst()
    {
        const string predicate = "[t].[a]=CONVERT_IMPLICIT(int,[@1],0)";
        Assert.False(PlanAnalyzer.ConvertImplicitWrapsColumn(predicate));

        using var plan = PlanRegex.BeginPlan(TimeSpan.Zero);
        Assert.True(PlanAnalyzer.ConvertImplicitWrapsColumn(predicate));
    }

    [Fact]
    public void ManyFunctionsOnTheParameterSide_StopsAtTheBudget_AndSaysSo()
    {
        // Each ISNULL sits on the parameter side, so the scan walks every one, and each walk reads the predicate
        // again. The budget bounds the whole pass instead of letting it run to the end.
        var predicate = string.Join(" AND ", Enumerable.Range(0, 3000).Select(i => $"[t].[c{i}]=isnull([@p],(1))"));
        var clock = System.Diagnostics.Stopwatch.StartNew();

        using var plan = PlanRegex.BeginPlan(TimeSpan.FromMilliseconds(50));
        var reason = PlanAnalyzer.DetectNonSargablePattern(predicate);

        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10), $"took {clock.Elapsed}");
        // A check that could not finish keeps the warning: some reason comes back, never null.
        Assert.NotNull(reason);
        Assert.NotEmpty(PlanRegex.TakeSkipped());
    }

    // ---- The whole project: every regex is behind a helper and carries the timeout ----

    private static IReadOnlyList<(string File, string Code)> ProjectSources([CallerFilePath] string thisFile = "")
    {
        var directory = Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "PerformanceMonitor.PlanAnalysis"));
        // Comments mention the helpers by name, and some files have block comments whose continuation lines carry
        // no prefix, so only the walk separates them from code. It also blanks each regex's pattern text.
        return Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar)
                && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar))
            .Select(f => (Path.GetFileName(f), CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(f))))
            .ToList();
    }

    /// <summary>The text between the parenthesis that opens at <paramref name="open"/> and its match.</summary>
    private static string Arguments(string code, int open)
    {
        var depth = 0;
        for (var i = open; i < code.Length; i++)
        {
            if (code[i] == '(')
                depth++;
            else if (code[i] == ')' && --depth == 0)
                return code.Substring(open, i - open + 1);
        }

        throw new Xunit.Sdk.XunitException("unbalanced parentheses");
    }

    [Fact]
    public void ProjectSources_AreTheWholeProject()
    {
        var names = ProjectSources().Select(s => s.File).ToList();

        Assert.Contains("PlanAnalyzer.cs", names);
        Assert.Contains("ParameterCard.cs", names);
        Assert.Contains("ReproScriptBuilder.cs", names);
        Assert.Contains("QueryModificationDetector.cs", names);
        Assert.Contains("PlanRegex.cs", names);
    }

    [Fact]
    public void PlanAnalysisProject_RunsNoRegexOutsideTheHelpers()
    {
        var sources = ProjectSources().Where(s => s.File != "PlanRegex.cs").ToList();

        // The names of every Regex-typed field, local or generated-regex method, whatever they are called.
        var names = sources
            .SelectMany(s => Regex.Matches(s.Code, @"\bRegex\s+(\w+)\s*(?:=|\(|;)").Cast<Match>().Select(m => m.Groups[1].Value))
            .Distinct()
            .ToList();
        Assert.True(names.Count >= 15, $"expected the project's regex fields, found {names.Count}.");

        foreach (var (file, code) in sources)
        {
            // Static calls (Regex.IsMatch, Regex.Replace ...).
            Assert.DoesNotMatch(new Regex(@"(?<![\w.])Regex\.(IsMatch|Match|Matches|Replace|Split|Count|EnumerateMatches)\("), code);

            // Calls on a field, a local or a generated-regex method, by any name.
            foreach (var name in names)
            {
                var instanceCall = new Regex($@"(?<![\w.]){Regex.Escape(name)}(?:\(\))?\.(IsMatch|Match|Matches|Replace|Split|Count|EnumerateMatches)\(");
                Assert.True(!instanceCall.IsMatch(code), $"{file} calls {name} directly instead of through PlanRegex.");
            }

            Assert.DoesNotMatch(new Regex(@"\w+RegExp\(\)\.\w+\("), code);
        }

        // Not vacuous: the sites are there, and they go through the helpers.
        var analyzer = sources.Single(s => s.File == "PlanAnalyzer.cs").Code;
        Assert.True(Regex.Matches(analyzer, @"PlanRegex\.(IsMatch|TryIsMatch|Match|Matches|Replace)\(").Count >= 25,
            "PlanAnalyzer.cs is expected to route its regex calls through PlanRegex.");
    }

    [Fact]
    public void PlanAnalysisProject_EveryRegexCarriesTheTimeout()
    {
        var sources = ProjectSources().Where(s => s.File != "PlanRegex.cs").ToList();
        var generated = 0;
        var constructed = 0;

        foreach (var (file, code) in sources)
        {
            foreach (Match attribute in Regex.Matches(code, @"\[GeneratedRegex\("))
            {
                generated++;
                var arguments = Arguments(code, attribute.Index + attribute.Length - 1);
                Assert.True(arguments.Contains("matchTimeoutMilliseconds: PlanRegex.TimeoutMilliseconds"),
                    $"{file}: a [GeneratedRegex] has no match timeout: {arguments}");
            }

            // new Regex(...) and the target-typed form, Regex s_x = new(...).
            foreach (Match construction in Regex.Matches(code, @"\bnew\s+Regex\(|\bRegex\s+\w+\s*=\s*new\("))
            {
                constructed++;
                var arguments = Arguments(code, construction.Index + construction.Length - 1);
                Assert.True(arguments.Contains("PlanRegex.DefaultTimeout") || arguments.Contains("TimeoutMilliseconds"),
                    $"{file}: a new Regex has no match timeout: {arguments}");
            }
        }

        Assert.True(generated >= 17, $"expected the project's generated regexes, found {generated}.");
        Assert.True(constructed >= 2, $"expected the compiled regexes, found {constructed}.");
    }
}
