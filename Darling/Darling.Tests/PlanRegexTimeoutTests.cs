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
    public void Analyze_WithAnEmptyBudget_DecidesTheOptimizeForUnknownHintFromTheText_AndRecordsNoSkip()
    {
        const string withHint = "SELECT a FROM dbo.t OPTION (OPTIMIZE FOR UNKNOWN)";
        const string withoutHint = "SELECT a FROM dbo.t WHERE a = 1";

        var spent = Analyze(withHint, TimeSpan.Zero);
        var normal = Analyze(withHint, TimeSpan.FromSeconds(30));
        var lacking = Analyze(withoutHint, TimeSpan.Zero);

        // A text search answers the question the skipped regex would have, so the answers match and no skip is named.
        Assert.Contains(spent.PlanWarnings, w => w.WarningType == "Optimize For Unknown");
        Assert.Contains(normal.PlanWarnings, w => w.WarningType == "Optimize For Unknown");
        Assert.DoesNotContain(spent.PlanWarnings, w => w.Message.Contains("OPTIMIZE FOR UNKNOWN hint"));
        // A statement without the hint never gets the warning, whatever the budget.
        Assert.DoesNotContain(lacking.PlanWarnings, w => w.WarningType == "Optimize For Unknown");
    }

    [Fact]
    public void Analyze_WithAnEmptyBudget_NamesTheSkippedChecks()
    {
        // The cursor declaration is read by a regex with no text-search answer: it is named as skipped.
        var spent = Analyze("DECLARE c CURSOR FAST_FORWARD FOR SELECT 1", TimeSpan.Zero);

        var note = Assert.Single(spent.PlanWarnings, w => w.WarningType == PlanAnalyzer.SkippedChecksWarningType);
        Assert.Contains("too large or complex", note.Message);
        Assert.Contains("cursor declaration", note.Message);
        Assert.Equal(40, note.RuleNumber);
        Assert.DoesNotContain(spent.PlanWarnings, w => w.WarningType == "Cursor Missing LOCAL");

        var normal = Analyze("DECLARE c CURSOR FAST_FORWARD FOR SELECT 1", TimeSpan.FromSeconds(30));
        Assert.Contains(normal.PlanWarnings, w => w.WarningType == "Cursor Missing LOCAL");
        Assert.DoesNotContain(normal.PlanWarnings, w => w.WarningType == PlanAnalyzer.SkippedChecksWarningType);
    }

    private static PlanStatement SerialStatement(string text) => new()
    {
        StatementText = text,
        NonParallelPlanReason = "MaxDOPSetToOne",
        StatementSubTreeCost = 5.0
    };

    private static PlanStatement AnalyzeStatement(PlanStatement statement, TimeSpan budget)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [statement] }] };
        PlanAnalyzer.Analyze(plan, null, null, budget, CancellationToken.None);
        return statement;
    }

    [Fact]
    public void Analyze_WithAnEmptyBudget_KeepsTheSerialPlanWarningWhenTheTextHoldsMaxdop1_AndDropsItWhenItDoesNot()
    {
        var with = AnalyzeStatement(SerialStatement("SELECT 1 OPTION (MAXDOP 1)"), TimeSpan.Zero);
        var without = AnalyzeStatement(SerialStatement("SELECT 1 FROM dbo.t"), TimeSpan.Zero);

        var warning = Assert.Single(with.PlanWarnings, w => w.WarningType == "Serial Plan");
        Assert.Equal(PlanWarningSeverity.Warning, warning.Severity);
        Assert.DoesNotContain(without.PlanWarnings, w => w.WarningType == "Serial Plan");
        Assert.DoesNotContain(with.PlanWarnings, w => w.Message.Contains("MAXDOP 1 hint"));
    }

    /// <summary>A clock that moves 1 ms on every read: a plan with a budget of n ms is out of time after about n reads.</summary>
    private static Func<TimeSpan> SteppingClock()
    {
        var now = TimeSpan.Zero;
        return () => now += TimeSpan.FromMilliseconds(1);
    }

    [Fact]
    public void Analyze_WhereverTheTimeLimitFalls_NeverReportsWhatTheTextLacks()
    {
        // The limit passes at a different check on each run. A statement with none of the hints below gets none
        // of their warnings, and a predicate with a function only on the parameter side never reads as a column.
        for (var reads = 0; reads <= 80; reads++)
        {
            try
            {
                PlanRegex.ClockForTest = SteppingClock();
                var cursor = AnalyzeStatement(new PlanStatement { StatementText = "DECLARE c CURSOR LOCAL FAST_FORWARD FOR SELECT 1 OPTION (MAXDOP 4)" }, TimeSpan.FromMilliseconds(reads));
                Assert.DoesNotContain(cursor.PlanWarnings, w => w.WarningType is "Cursor Missing LOCAL" or "Optimize For Unknown");

                PlanRegex.ClockForTest = SteppingClock();
                var serial = AnalyzeStatement(SerialStatement("SELECT 1 FROM dbo.t"), TimeSpan.FromMilliseconds(reads));
                Assert.DoesNotContain(serial.PlanWarnings, w => w.WarningType == "Serial Plan");

                PlanRegex.ClockForTest = SteppingClock();
                var scan = AnalyzeScanWithPredicate("[dbo].[t].[a]=isnull([@p],(1)) AND [dbo].[t].[b]=upper([@q]) AND [dbo].[t].[c]=CONVERT_IMPLICIT(int,[@r],0)", TimeSpan.FromMilliseconds(reads));
                Assert.DoesNotContain(scan.RootNode!.Warnings, w => w.Message.Contains("wrapping column") || w.Message.Contains("Function call") || w.Message.Contains("Implicit conversion"));
            }
            finally
            {
                PlanRegex.ClockForTest = null;
            }
        }
    }

    [Fact]
    public void Analyze_ADeadlineThatPassesInsideTheConvertImplicitLoop_StopsTheLoop_AndWordsTheResultAsUnverified()
    {
        // The clock moves with every read, so the limit passes a few matches into the loop, not at its start.
        PlanRegex.ClockForTest = SteppingClock();
        try
        {
            using var plan = PlanRegex.BeginPlan(TimeSpan.FromMilliseconds(40));
            var reason = PlanAnalyzer.DetectNonSargablePattern(HugeConvertImplicitPredicate(5_000));

            Assert.Equal(PlanAnalyzer.UnverifiedNonSargableReason, reason);
            Assert.Contains("CONVERT_IMPLICIT in a predicate", PlanRegex.TakeSkipped());
        }
        finally
        {
            PlanRegex.ClockForTest = null;
        }
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
    public void ReadsAnotherInput_WhenTheCheckIsCutShort_ProvesNothing_AndNamesTheSkippedCheck()
    {
        // "[@p1]" alone is a parameter list: no other input.
        Assert.False(PlanAnalyzer.ReadsAnotherInput("[@p1]"));

        // A text that does name another input proves it only when the pass can read it.
        Assert.True(PlanAnalyzer.ReadsAnotherInput("[dbo].[t].[c]"));

        using var plan = PlanRegex.BeginPlan(TimeSpan.Zero);
        Assert.False(PlanAnalyzer.ReadsAnotherInput("[dbo].[t].[c]"));
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
    public void ConvertImplicitWrapsColumn_WhenTheCheckIsCutShort_ReportsNoColumn_AndNamesTheSkippedCheck()
    {
        const string predicate = "[t].[a]=CONVERT_IMPLICIT(int,[@1],0)";
        Assert.False(PlanAnalyzer.ConvertImplicitWrapsColumn(predicate));

        using var plan = PlanRegex.BeginPlan(TimeSpan.Zero);
        Assert.False(PlanAnalyzer.ConvertImplicitWrapsColumn(predicate));
        Assert.Contains("CONVERT_IMPLICIT in a predicate", PlanRegex.TakeSkipped());
    }

    private static string HugeConvertImplicitPredicate(int depth) =>
        "[dbo].[t].[a]='" + string.Concat(Enumerable.Repeat("CONVERT_IMPLICIT(", depth)) + new string(')', depth) + "'";

    private static PlanStatement AnalyzeScanWithPredicate(string predicate, TimeSpan budget)
    {
        var scan = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            ObjectName = "dbo.t",
            Predicate = predicate
        };
        var statement = new PlanStatement { StatementText = "SELECT a FROM dbo.t", RootNode = scan };
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [statement] }] };
        PlanAnalyzer.Analyze(plan, null, null, budget, CancellationToken.None);
        return statement;
    }

    [Fact]
    public void Analyze_AHugeConvertImplicitPredicate_StopsAtTheTimeLimit_AndNamesTheSkippedCheck()
    {
        // CONVERT_IMPLICIT( matches inside a string literal too, and each match scans to its closing parenthesis:
        // the work outside the regex calls grows with the square of the predicate's length.
        var predicate = HugeConvertImplicitPredicate(25_000);
        var clock = System.Diagnostics.Stopwatch.StartNew();

        var statement = AnalyzeScanWithPredicate(predicate, TimeSpan.FromMilliseconds(200));

        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(5), $"took {clock.Elapsed}");
        var note = Assert.Single(statement.RootNode!.Warnings.Concat(statement.PlanWarnings),
            w => w.WarningType == PlanAnalyzer.SkippedChecksWarningType);
        Assert.Contains("CONVERT_IMPLICIT in a predicate", note.Message);
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

    // A static call on the Regex type, by its short or its fully qualified name. Only a word character before
    // "Regex" rules it out (PlanRegex.IsMatch is a helper call).
    private static readonly Regex StaticRegexCall =
        new(@"(?<!\w)Regex\.(IsMatch|Match|Matches|Replace|Split|Count|EnumerateMatches)\(", RegexOptions.None, TimeSpan.FromSeconds(5));

    // new Regex(...) and the target-typed form, Regex s_x = new(...), also fully qualified.
    private const string RegexConstruction = @"\bnew\s+(?:global::)?(?:System\s*\.\s*Text\s*\.\s*RegularExpressions\s*\.\s*)?Regex\(|\bRegex\s+\w+\s*=\s*new\(";

    [Theory]
    [InlineData("var found = Regex.IsMatch(text, \"a\");")]
    [InlineData("var found = System.Text.RegularExpressions.Regex.IsMatch(text, \"a\");")]
    [InlineData("var replaced = global::System.Text.RegularExpressions.Regex.Replace(text, \"a\", \"b\");")]
    [InlineData("var parts = System.Text.RegularExpressions.Regex.Split(text, \"a\");")]
    public void TheStaticCallCheck_CatchesTheShortAndTheFullyQualifiedForm(string code)
    {
        Assert.Matches(StaticRegexCall, code);
    }

    [Theory]
    [InlineData("var found = PlanRegex.IsMatch(text, \"a\", RegexOptions.None, \"check\");")]
    [InlineData("var found = MyRegex.Match(text);")]
    public void TheStaticCallCheck_LeavesTheHelperCallsAlone(string code)
    {
        Assert.DoesNotMatch(StaticRegexCall, code);
    }

    [Theory]
    [InlineData("var r = new Regex(\"a\");")]
    [InlineData("var r = new System.Text.RegularExpressions.Regex(\"a\");")]
    [InlineData("var r = new global::System.Text.RegularExpressions.Regex(\"a\");")]
    [InlineData("Regex r = new(\"a\");")]
    public void TheConstructionCheck_CatchesTheShortAndTheFullyQualifiedForm(string code)
    {
        Assert.Matches(RegexConstruction, code);
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
            // Static calls (Regex.IsMatch, Regex.Replace ...), also fully qualified.
            Assert.DoesNotMatch(StaticRegexCall, code);

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
            foreach (Match construction in Regex.Matches(code, RegexConstruction))
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
