/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The .NET evaluation of the shared statement filter (#4348): the translation of the pattern, its guards, the
/// verdicts, the per-value timeout and the elapsed-time budget. The corpus is shared with the PostgreSQL
/// parity test in <see cref="SensitiveStatementsParityLiveTests"/>.
/// </summary>
public sealed class SensitiveStatementsTests
{
    [Fact]
    public void TheCorpusIsNamedInEveryVariant()
    {
        var failures = new List<string>();
        foreach (var text in SensitiveStatementCorpus.Named)
        {
            foreach (var variant in SensitiveStatementCorpus.Variants(text))
            {
                if (!SensitiveStatements.Names(variant))
                {
                    failures.Add("should be named: " + variant);
                }
            }
        }

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    [Fact]
    public void TheNeighboursAreNotNamed()
    {
        var failures = SensitiveStatementCorpus.NotNamed
            .Where(SensitiveStatements.Names)
            .Select(text => "should NOT be named: " + text)
            .ToList();

        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures));
    }

    [Fact]
    public void JudgeIsNotCleanExactlyWhenNamesIsTrue_OnTheCorpus()
    {
        var all = SensitiveStatementCorpus.Named.SelectMany(SensitiveStatementCorpus.Variants)
            .Concat(SensitiveStatementCorpus.NotNamed);
        foreach (var text in all)
        {
            Assert.Equal(SensitiveStatements.Judge(text) != SensitiveStatements.Verdict.Clean, SensitiveStatements.Names(text));
        }
    }

    [Fact]
    public void NullAndEmptyAreNeverNamed()
    {
        Assert.False(SensitiveStatements.Names(null));
        Assert.False(SensitiveStatements.Names(string.Empty));
        Assert.Equal(SensitiveStatements.Verdict.Clean, SensitiveStatements.Judge(string.Empty));
        Assert.Null(SensitiveStatements.Text(null));
        Assert.Equal(string.Empty, SensitiveStatements.Text(string.Empty));
    }

    /// <summary>The translated pattern is pinned as a literal: a change to the shared pattern or to the
    /// translation has to show up here on purpose. No POSIX class survives it.</summary>
    [Fact]
    public void TheTranslatedPatternIsPinnedAndHasNoPosixClassLeft()
    {
        var translated = SensitiveStatements.TranslatedPattern;

        Assert.NotNull(translated);
        Assert.DoesNotContain("[:", translated, StringComparison.Ordinal);
        Assert.Equal(PinnedTranslation, translated);
    }

    private const string PinnedTranslation = """
        (?:(?<!(?-i:[0-9A-Za-z_]))(?=(?-i:[0-9A-Za-z_]))(?:(create|alter)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+(role|user|group|subscription|server)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|password(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(=|to)?([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(e?'|u&'|[$][^0-9])|(pg)?password[\s]*=[\s]*[^$\s]|(create|alter)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+(login|credential)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|scoped([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+credential(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|([a-z0-9_]*(password|passwd|pwd|secret)|key_source)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(=|to)?([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(n?'|e'|u&'|0x|[$][^0-9])|pwd[\s]*=[\s]*[^$@\s]|(sp_addlogin|sp_password|sp_addlinkedsrvlogin|sp_addapprole|sp_approlepassword|sp_setapprole|sp_change_users_login|sp_adddistributor|sp_changedistributor_password|sp_adddistpublisher|sp_addsubscriber|sp_link_publication|sp_control_dbmasterkey_password|sp_xp_cmdshell_proxy_account)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|(encryptbypassphrase|decryptbypassphrase|decryptbykeyautocert|decryptbykeyautoasymkey|decryptbyasymkey|decryptbycert|signbycert|signbyasymkey|pwdencrypt|pwdcompare)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|opendatasource(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|openrowset([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*[(]([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*n?'|[a-z0-9_]*(password|passwd|pwd|secret)(]|")([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*=([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(n?'|e'|u&'|0x)|([a-z0-9_]*(password|passwd|pwd|secret)|key_source)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+((as|constant)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+)?[[]?[a-z_][a-z0-9_.]*]?([\s]*[(][\s0-9a-z,]{0,20}[)])?([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(:?=|default(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_])))([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(n?'|e'|u&'|0x|[$][^0-9])))|[a-z][a-z0-9+.-]*://[^\s/@:]+:[^\s/@]+@
        """;

    [Fact]
    public void ABackslashInTheSourceFailsTheGuard_AndEveryInputIsNamed()
    {
        Assert.False(SensitiveStatements.TryTranslate("a\\sb", out _));
        var judge = SensitiveStatements.CreateJudge("a\\sb", SensitiveStatements.MatchTimeout);

        Assert.Equal(SensitiveStatements.Verdict.Named, judge("SELECT 1"));
    }

    [Theory]
    [InlineData("^abc")]
    [InlineData("abc$")]
    [InlineData("(a|^b)")]
    public void AnAnchorOutsideABracketFailsTheGuard_AndEveryInputIsNamed(string source)
    {
        Assert.False(SensitiveStatements.TryTranslate(source, out _));
        var judge = SensitiveStatements.CreateJudge(source, SensitiveStatements.MatchTimeout);

        Assert.Equal(SensitiveStatements.Verdict.Named, judge("SELECT 1"));
    }

    [Fact]
    public void AnUnknownPosixClassFailsTheGuard_AndEveryInputIsNamed()
    {
        Assert.False(SensitiveStatements.TryTranslate("[[:alpha:]]", out _));
        var judge = SensitiveStatements.CreateJudge("[[:alpha:]]", SensitiveStatements.MatchTimeout);

        Assert.Equal(SensitiveStatements.Verdict.Named, judge("SELECT 1"));
    }

    /// <summary>An anchor or a stray <c>]</c> that the guard must understand: <c>^</c> and <c>$</c> inside a
    /// bracket are fine, and a <c>]</c> that closes no bracket is a literal (T9 needs it).</summary>
    [Fact]
    public void BracketsAndALiteralCloseBracketPassTheGuard()
    {
        Assert.True(SensitiveStatements.TryTranslate("[^$[:space:]]+(]|\")x", out var translated));
        Assert.Equal("[^$\\s]+(]|\")x", translated);
        Assert.True(SensitiveStatements.TryTranslate("[]^]x", out _));
        Assert.False(SensitiveStatements.TryTranslate("[abc", out _));
    }

    /// <summary>The word-start assertion is hoisted out of the alternatives that share it (a speed-up only),
    /// and an alternative without it stays outside the group.</summary>
    [Fact]
    public void TheSharedWordStartIsHoistedOutOfTheAlternatives()
    {
        Assert.True(SensitiveStatements.TryTranslate("[[:<:]]ab|[[:<:]]cd|ef", out var translated));

        Assert.Equal("(?:(?<!(?-i:[0-9A-Za-z_]))(?=(?-i:[0-9A-Za-z_]))(?:ab|cd))|ef", translated);
        var judge = SensitiveStatements.CreateJudge("[[:<:]]ab|[[:<:]]cd|ef", SensitiveStatements.MatchTimeout);
        Assert.Equal(SensitiveStatements.Verdict.Named, judge("x cd"));
        Assert.Equal(SensitiveStatements.Verdict.Named, judge("xef"));
        Assert.Equal(SensitiveStatements.Verdict.Clean, judge("xab"));
    }

    [Fact]
    public void ThePatternItselfPassesEveryGuard()
    {
        Assert.True(SensitiveStatements.TryTranslate(SensitiveStatements.Pattern, out _));
    }

    /// <summary>The .NET judge compiles the pattern as two regexes (#5320 M1), both cut out of the one shared
    /// constant. Put back together with <c>|</c> they are exactly <see cref="SensitiveStatements.Pattern"/>. The
    /// first is the thirteen alternatives before T10, and its translation is the literal the whole judge was before
    /// T10 existed; the second is T10 alone. PostgreSQL still reads the whole constant.</summary>
    [Fact]
    public void TheTwoJudgePartsPutBackTogetherAreExactlyThePattern()
    {
        var count = SensitiveStatements.JudgeHeadAlternatives;
        Assert.True(SensitiveStatements.TrySplitAlternatives(SensitiveStatements.Pattern, count, out var head, out var tail));

        Assert.Equal(SensitiveStatements.Pattern, head + "|" + tail);
        Assert.False(SensitiveStatements.TrySplitAlternatives(tail, 1, out _, out _), "the second part is one alternative");
        Assert.True(SensitiveStatements.TrySplitAlternatives(head, count - 1, out _, out _));
        Assert.False(SensitiveStatements.TrySplitAlternatives(head, count, out _, out _), "the first part is exactly " + count + " alternatives");
        Assert.StartsWith("[[:<:]]([a-z0-9_]*(password|passwd|pwd|secret)|key_source)", tail, StringComparison.Ordinal);
        Assert.True(SensitiveStatements.TryTranslate(head, out var headTranslated));
        Assert.True(SensitiveStatements.TryTranslate(tail, out var tailTranslated));
        Assert.Equal(PinnedHeadTranslation, headTranslated);
        Assert.Equal(PinnedTailTranslation, tailTranslated);
    }

    /// <summary>The first judge part, A1-T9, translated: byte for byte the literal
    /// <see cref="TheTranslatedPatternIsPinnedAndHasNoPosixClassLeft"/> pinned before T10 was appended.</summary>
    private const string PinnedHeadTranslation = """
        (?:(?<!(?-i:[0-9A-Za-z_]))(?=(?-i:[0-9A-Za-z_]))(?:(create|alter)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+(role|user|group|subscription|server)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|password(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(=|to)?([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(e?'|u&'|[$][^0-9])|(pg)?password[\s]*=[\s]*[^$\s]|(create|alter)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+(login|credential)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|scoped([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+credential(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|([a-z0-9_]*(password|passwd|pwd|secret)|key_source)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(=|to)?([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(n?'|e'|u&'|0x|[$][^0-9])|pwd[\s]*=[\s]*[^$@\s]|(sp_addlogin|sp_password|sp_addlinkedsrvlogin|sp_addapprole|sp_approlepassword|sp_setapprole|sp_change_users_login|sp_adddistributor|sp_changedistributor_password|sp_adddistpublisher|sp_addsubscriber|sp_link_publication|sp_control_dbmasterkey_password|sp_xp_cmdshell_proxy_account)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|(encryptbypassphrase|decryptbypassphrase|decryptbykeyautocert|decryptbykeyautoasymkey|decryptbyasymkey|decryptbycert|signbycert|signbyasymkey|pwdencrypt|pwdcompare)(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|opendatasource(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_]))|openrowset([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*[(]([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*n?'|[a-z0-9_]*(password|passwd|pwd|secret)(]|")([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*=([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(n?'|e'|u&'|0x)))|[a-z][a-z0-9+.-]*://[^\s/@:]+:[^\s/@]+@
        """;

    /// <summary>The second judge part, T10 alone, translated.</summary>
    private const string PinnedTailTranslation = """
        (?<!(?-i:[0-9A-Za-z_]))(?=(?-i:[0-9A-Za-z_]))([a-z0-9_]*(password|passwd|pwd|secret)|key_source)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+((as|constant)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+)?[[]?[a-z_][a-z0-9_.]*]?([\s]*[(][\s0-9a-z,]{0,20}[)])?([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(:?=|default(?<=(?-i:[0-9A-Za-z_]))(?!(?-i:[0-9A-Za-z_])))([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(n?'|e'|u&'|0x|[$][^0-9])
        """;

    [Theory]
    [InlineData("a(b|c)|[|]d|e", 1, "a(b|c)", "[|]d|e")]
    [InlineData("a(b|c)|[|]d|e", 2, "a(b|c)|[|]d", "e")]
    [InlineData("[[:<:]]x|[^|]y|[]|]z|w", 1, "[[:<:]]x", "[^|]y|[]|]z|w")]
    [InlineData("[[:<:]]x|[^|]y|[]|]z|w", 3, "[[:<:]]x|[^|]y|[]|]z", "w")]
    [InlineData("(a|(b|c))|d", 1, "(a|(b|c))", "d")]
    public void AlternativesAreCutOnlyAtATopLevelBar(string source, int count, string expectedHead, string expectedTail)
    {
        Assert.True(SensitiveStatements.TrySplitAlternatives(source, count, out var head, out var tail));

        Assert.Equal(expectedHead, head);
        Assert.Equal(expectedTail, tail);
    }

    [Theory]
    [InlineData("a|b", 0)]
    [InlineData("a|b", 2)]
    [InlineData("a(b|c)", 1)]
    [InlineData("[a|b", 1)]
    [InlineData("a|", 1)]
    public void ASourceWithNoAlternativeAfterTheFirstCountIsNotSplit(string source, int count)
    {
        Assert.False(SensitiveStatements.TrySplitAlternatives(source, count, out var head, out var tail));

        Assert.Equal(source, head);
        Assert.Equal(string.Empty, tail);
    }

    /// <summary>Two regexes are a speed change only (#5320 M1): over the corpus and the seeded fuzz set the split
    /// judge and the one-regex judge give the same verdict, so "named when either matches" is exact OR. The
    /// first part alone is built too, so the test also shows the fuzz set reaches strings only the second part names.</summary>
    [Fact]
    public void TheSplitJudgeGivesTheSameVerdictAsTheOneRegexJudge_OverTheCorpusAndAFuzzSet()
    {
        var timeout = TimeSpan.FromSeconds(5);
        Assert.True(SensitiveStatements.TrySplitAlternatives(
            SensitiveStatements.Pattern, SensitiveStatements.JudgeHeadAlternatives, out var headSource, out _));
        var one = SensitiveStatements.CreateJudge(SensitiveStatements.Pattern, timeout, prefilter: false);
        var two = SensitiveStatements.CreateJudge(
            SensitiveStatements.Pattern, timeout, prefilter: false, headAlternatives: SensitiveStatements.JudgeHeadAlternatives);
        var headOnly = SensitiveStatements.CreateJudge(headSource, timeout, prefilter: false);

        var named = 0;
        var tailOnly = 0;
        var failures = new List<string>();
        foreach (var text in CorpusAndFuzz())
        {
            var verdict = one(text);
            named += verdict == SensitiveStatements.Verdict.Named ? 1 : 0;
            tailOnly += verdict == SensitiveStatements.Verdict.Named && headOnly(text) == SensitiveStatements.Verdict.Clean ? 1 : 0;
            if (two(text) != verdict)
            {
                failures.Add("split and single differ: " + text);
            }
        }

        Assert.True(named > 1_000, $"the fuzz set names only {named} strings, too few to pin anything");
        Assert.True(tailOnly > 50, $"only {tailOnly} strings are named by the second part alone, too few to pin anything");
        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures.Take(20)));
    }

    /// <summary>A <see cref="SensitiveStatements.BudgetedRegex"/> runs at the largest limit that fits the time
    /// left (#5320 M1), so the second judge part never runs past what the first left of the one budget. With
    /// 250 ms the limits are 187, 93, 46 and 23 ms. The text backtracks without end, so each call ends at the
    /// limit it ran with: every reading is at least 60% of the limit, and the best of three is within a generous
    /// overshoot of it.</summary>
    [Theory]
    [InlineData(200, 187)]
    [InlineData(150, 93)]
    [InlineData(90, 46)]
    [InlineData(45, 23)]
    public void ABudgetedRegexRunsAtTheLargestLimitThatFitsWhatIsLeft(int remainingMs, int expectedLimitMs)
    {
        var regex = new SensitiveStatements.BudgetedRegex("(a|aa)+b", TimeSpan.FromMilliseconds(250));
        var text = new string('a', 60) + "c";

        var best = long.MaxValue;
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var watch = Stopwatch.StartNew();
            Assert.Throws<System.Text.RegularExpressions.RegexMatchTimeoutException>(
                () => regex.IsMatch(text, TimeSpan.FromMilliseconds(remainingMs)));
            watch.Stop();
            Assert.True(watch.ElapsedMilliseconds >= expectedLimitMs * 0.6, $"stopped after {watch.ElapsedMilliseconds} ms, below the {expectedLimitMs} ms limit");
            best = Math.Min(best, watch.ElapsedMilliseconds);
        }

        Assert.True(best <= expectedLimitMs + 100, $"best of 3 took {best} ms for a {expectedLimitMs} ms limit");
    }

    [Fact]
    public void ABudgetedRegexWithLessTimeLeftThanItsSmallestLimitTimesOutWithoutRunning()
    {
        var regex = new SensitiveStatements.BudgetedRegex("b", TimeSpan.FromMilliseconds(250));

        Assert.True(regex.IsMatch("xb", TimeSpan.FromMilliseconds(250)));
        Assert.False(regex.IsMatch("xa", TimeSpan.FromMilliseconds(30)));
        Assert.Throws<System.Text.RegularExpressions.RegexMatchTimeoutException>(() => regex.IsMatch("xb", TimeSpan.FromMilliseconds(20)));
        Assert.Throws<System.Text.RegularExpressions.RegexMatchTimeoutException>(() => regex.IsMatch("xb", TimeSpan.Zero));
        Assert.Throws<System.Text.RegularExpressions.RegexMatchTimeoutException>(() => regex.IsMatch("xb", TimeSpan.FromMilliseconds(-5)));
    }

    /// <summary>The two parts share the one per-value timeout (#5320 M1). A fake clock says the first part took a
    /// given time; the second part then gets only what is left. The first part is a plain <c>x</c>; the second
    /// backtracks without end on the text, so its real time shows the limit it was given: nothing used runs it at
    /// 187 ms, 100 ms used at 93 ms, 200 ms used at 46 ms, and 240 ms used (under the smallest 23 ms limit left) is
    /// TimedOut at once.</summary>
    [Theory]
    [InlineData(0, 187)]
    [InlineData(100, 93)]
    [InlineData(200, 46)]
    [InlineData(240, 0)]
    public void TheSecondJudgePartRunsOnlyInsideWhatTheFirstLeftOfTheBudget(int firstPartMs, int limitMs)
    {
        var text = new string('a', 60) + "d";
        var best = long.MaxValue;
        for (var attempt = 0; attempt < 3; attempt++)
        {
            var calls = 0;
            var judge = SensitiveStatements.CreateJudge(
                "x|(a|aa)+c",
                TimeSpan.FromMilliseconds(250),
                prefilter: false,
                headAlternatives: 1,
                clock: () => TimeSpan.FromMilliseconds(calls++ == 0 ? 0 : firstPartMs));
            var watch = Stopwatch.StartNew();
            var verdict = judge(text);
            watch.Stop();

            Assert.Equal(SensitiveStatements.Verdict.TimedOut, verdict);
            Assert.True(watch.ElapsedMilliseconds >= limitMs * 0.6, $"stopped after {watch.ElapsedMilliseconds} ms");
            best = Math.Min(best, watch.ElapsedMilliseconds);
        }

        Assert.True(best <= limitMs + 100, $"best of 3 took {best} ms; the second part had {limitMs} ms");
    }

    /// <summary>A part that matches names the value whichever part it is, and a text neither matches is clean.</summary>
    [Fact]
    public void ATwoPartJudgeNamesAValueEitherPartMatches()
    {
        var judge = SensitiveStatements.CreateJudge("x|y|z", SensitiveStatements.MatchTimeout, headAlternatives: 2);

        Assert.Equal(SensitiveStatements.Verdict.Named, judge("--x--"));
        Assert.Equal(SensitiveStatements.Verdict.Named, judge("--y--"));
        Assert.Equal(SensitiveStatements.Verdict.Named, judge("--z--"));
        Assert.Equal(SensitiveStatements.Verdict.Clean, judge("--w--"));
    }

    /// <summary>The production judge is the split judge (#5320 N2), pinned without a stopwatch. A fake clock says the
    /// first part used 240 ms of the 250 ms budget, so the second part has too little time left and the value is
    /// TimedOut. <c>x_pwd = HASHBYTES(1)</c> reaches the full judge (the pre-check's superset matches it) and no
    /// part names it, so a judge that has gone back to one regex answers Clean here and fails this test.</summary>
    [Fact]
    public void TheProductionJudgeRunsItsSecondPartInsideWhatTheFirstLeft()
    {
        var calls = 0;
        var judge = SensitiveStatements.CreateProductionJudge(
            clock: () => TimeSpan.FromMilliseconds(calls++ == 0 ? 0 : 240));

        Assert.Equal(SensitiveStatements.Verdict.TimedOut, judge("x_pwd = HASHBYTES(1)"));
    }

    /// <summary>The warm-up builds the shared judge, can run again, and does not throw (#5320 N1).</summary>
    [Fact]
    public void WarmUpBuildsTheSharedJudgeAndCanRunAgain()
    {
        SensitiveStatements.WarmUp();
        SensitiveStatements.WarmUp();

        Assert.True(SensitiveStatements.JudgeIsBuilt);
        Assert.Equal(SensitiveStatements.Verdict.Clean, SensitiveStatements.Judge("SELECT 1"));
    }

    /// <summary>1 MB of ordinary SQL with a trailing near miss that only the pre-check's superset hits (#5320 M1).
    /// <c>x_pwd = HASHBYTES(1)</c> has no word start, so the pre-check matches it (T4 without its assertion) and the
    /// full judge must run and answer Clean. With the whole pattern as one compiled regex that took about 226 ms
    /// a megabyte and the value timed out and was withheld; as two regexes it is Clean inside the 250 ms timeout.</summary>
    [Fact]
    public void AMegabyteOfOrdinarySqlWithATrailingNearMissIsCleanInsideTheTimeout()
    {
        SensitiveStatements.Judge("warm the regex up");
        var unit = "SELECT col_a, col_b FROM dbo.t WHERE c = 1 AND d <> 2 ";
        var text = string.Concat(Enumerable.Repeat(unit, 1_000_000 / unit.Length + 1)).Substring(0, 1_000_000)
            + " x_pwd = HASHBYTES(1)";

        var best = long.MaxValue;
        var clean = false;
        for (var attempt = 0; attempt < 5 && !(clean && best < 250); attempt++)
        {
            var watch = Stopwatch.StartNew();
            var verdict = SensitiveStatements.Judge(text);
            watch.Stop();

            if (verdict == SensitiveStatements.Verdict.Clean)
            {
                clean = true;
                best = Math.Min(best, watch.ElapsedMilliseconds);
            }
        }

        Assert.True(clean, "never came back Clean");
        Assert.True(best < 250, $"best Clean took {best} ms");
    }

    /// <summary>A typed secret-named variable followed by a long dash banner, in a value where something else
    /// passes the pre-check (#5320 L2). The typed-declaration alternative's gaps after the type backtrack on a run of
    /// dashes (the known comment-run case); the second judge part runs out of its time and the value is named by the
    /// timeout. PostgreSQL answers Clean. That errs toward withholding, and an atomic rewrite of the gap is ruled
    /// out, so the row pins the .NET answer: TimedOut, never Clean, back inside about the match timeout.</summary>
    [Fact]
    public void ATypedSecretVariableBehindADashBannerIsNamedByTimeout_WhenAnotherPartPassesThePreCheck()
    {
        SensitiveStatements.Judge("warm the regex up");
        foreach (var text in SensitiveStatementCorpus.NamedByTimeoutInDotNetOnly)
        {
            var best = long.MaxValue;
            for (var attempt = 0; attempt < 3; attempt++)
            {
                var watch = Stopwatch.StartNew();
                var verdict = SensitiveStatements.Judge(text);
                watch.Stop();

                Assert.Equal(SensitiveStatements.Verdict.TimedOut, verdict);
                best = Math.Min(best, watch.ElapsedMilliseconds);
            }

            Assert.True(best <= 400, $"best of 3 took {best} ms");
            Assert.True(SensitiveStatements.Names(text));
        }
    }

    [Fact]
    public void AMatchThatRunsPastItsTimeoutGivesTimedOut()
    {
        // Nested alternation under a quantifier backtracks exponentially on a near miss.
        // Without the pre-check: its linear-time engine would answer this toy pattern (which has no assertion to drop) Clean.
        var judge = SensitiveStatements.CreateJudge("(a|aa)+b", TimeSpan.FromMilliseconds(50), prefilter: false);

        var verdict = judge(new string('a', 60) + "c");

        Assert.Equal(SensitiveStatements.Verdict.TimedOut, verdict);
        Assert.Equal(SensitiveStatements.Verdict.Named, judge("xaab"));
        Assert.Equal(SensitiveStatements.Verdict.Clean, judge("xyz"));
    }

    // The timing tests take the best of several attempts: the test assembly runs many classes in parallel
    // and a loaded machine can stretch one wall-clock reading; the verdict itself must hold on every attempt.

    /// <summary>The two comment-token strings the plan once pinned as TimedOut. The linear-time pre-check
    /// (#5320 M1) finds no <c>role</c>, <c>user</c> or literal in them, so they are Clean, which is also what
    /// PostgreSQL answers.</summary>
    [Fact]
    public void EachAdversarialStringIsCleanWithin50Ms()
    {
        SensitiveStatements.Judge("warm the regex up");
        foreach (var text in SensitiveStatementCorpus.Adversarial)
        {
            var best = BestOfJudging(text, SensitiveStatements.Verdict.Clean, 5);

            Assert.True(best < 50, $"best of 5 took {best} ms: {text}");
            Assert.False(SensitiveStatements.Names(text));
        }
    }

    /// <summary>The same for the strings aimed at the typed-declaration alternative (#5320): a password word, many
    /// comment tokens or a long length part, and no literal. The pre-check finds no literal, so they are Clean.</summary>
    [Fact]
    public void EachTypedDeclarationAdversarialStringIsCleanWithin50Ms()
    {
        SensitiveStatements.Judge("warm the regex up");
        foreach (var text in SensitiveStatementCorpus.TypedDeclarationAdversarial)
        {
            var best = BestOfJudging(text, SensitiveStatements.Verdict.Clean, 5);

            Assert.True(best < 50, $"best of 5 took {best} ms: {text.Substring(0, 24)}");
            Assert.False(SensitiveStatements.Names(text));
        }
    }

    /// <summary>Ordinary banner comments: a run of dashes after <c>create</c>, a comment after a column named
    /// <c>pwd</c>, and so on. The full judge splits a dash run exponentially on these, so only the pre-check
    /// keeps them Clean.</summary>
    [Fact]
    public void OrdinaryDashBannersAreCleanWithin50Ms()
    {
        var dashes = new string('-', 60);
        var banners = new[]
        {
            "----" + dashes + "\n-- Create\n----" + dashes + "\nCREATE TABLE dbo.t (id int);",
            "-- Create " + dashes + "\nSELECT 1",
            "SELECT 1 AS pwd -- " + dashes,
            "UPDATE t SET secret -- " + dashes + "\n= 1",
            "create " + dashes + "x",
            "create" + string.Concat(Enumerable.Repeat(" --", 20)),
        };
        SensitiveStatements.Judge("warm the regex up");
        foreach (var text in banners)
        {
            var best = BestOfJudging(text, SensitiveStatements.Verdict.Clean, 5);

            Assert.True(best < 50, $"best of 5 took {best} ms: {text.Substring(0, Math.Min(40, text.Length))}");
        }
    }

    /// <summary>A string the pre-check still hits (<c>xcreate login</c> has no word start, so only the superset
    /// matches) but the full judge cannot finish: it keeps the TimedOut verdict, which counts as named. A
    /// planted near-hit can still force a timeout, which is what the process-wide memo is for.</summary>
    [Fact]
    public void ANearHitThatReachesTheFullJudgeStillTimesOutAndStaysNamed()
    {
        var text = "xcreate login; create" + string.Concat(Enumerable.Repeat(" --", 40)) + "x";
        SensitiveStatements.Judge("warm the regex up");

        var best = BestOfJudging(text, SensitiveStatements.Verdict.TimedOut, 3);

        Assert.True(best <= 300, $"best of 3 took {best} ms");
        Assert.True(SensitiveStatements.Names(text));
    }

    /// <summary>A typed declaration of a password variable behind many comment tokens (#5320). The full judge may
    /// backtrack on the second string, so the answer is Named or TimedOut (named either way), never Clean, and the
    /// call returns inside the match timeout.</summary>
    [Fact]
    public void ATypedDeclarationBehindManyCommentTokensIsNeverClean_AndReturnsInsideTheTimeout()
    {
        var comments = string.Concat(Enumerable.Repeat(" --", 40));
        SensitiveStatements.Judge("warm the regex up");
        foreach (var text in new[]
        {
            "declare @pwd" + comments + "\n nvarchar(20) = N'x'",
            "declare @pwd" + comments + " nvarchar(20) = N'x'",
        })
        {
            var watch = Stopwatch.StartNew();
            var verdict = SensitiveStatements.Judge(text);
            watch.Stop();

            Assert.NotEqual(SensitiveStatements.Verdict.Clean, verdict);
            Assert.True(watch.ElapsedMilliseconds < 1000, $"took {watch.ElapsedMilliseconds} ms: {text.Substring(0, 24)}");
        }
    }

    /// <summary>A 100 KB binary literal (200,000 hex characters) in an INSERT, and the other long single-run
    /// shapes that made the credential-URL branch quadratic in the full judge (#5320 M2).</summary>
    [Fact]
    public void ALongHexLiteralAndOtherLongRunsAreCleanAndFast()
    {
        var texts = new[]
        {
            "INSERT INTO dbo.Files (Id, Body) VALUES (7, 0x" + new string('A', 200_000) + ")",
            "INSERT INTO dbo.Files (Id, Body) VALUES (7, 0x" + new string('a', 1_000_000) + ")",
            "SELECT '" + new string('a', 1_000_000) + "'",
        };
        SensitiveStatements.Judge("warm the regex up");
        foreach (var text in texts)
        {
            var best = BestOfJudging(text, SensitiveStatements.Verdict.Clean, 5);

            Assert.True(best < 100, $"best of 5 took {best} ms for {text.Length} characters");
        }
    }

    private static long BestOfJudging(string text, SensitiveStatements.Verdict expected, int attempts)
    {
        var best = long.MaxValue;
        for (var attempt = 0; attempt < attempts; attempt++)
        {
            var watch = Stopwatch.StartNew();
            var verdict = SensitiveStatements.Judge(text);
            watch.Stop();

            Assert.Equal(expected, verdict);
            best = Math.Min(best, watch.ElapsedMilliseconds);
        }

        return best;
    }

    private static IEnumerable<string> CorpusAndFuzz()
    {
        var corpus = SensitiveStatementCorpus.Named.SelectMany(SensitiveStatementCorpus.Variants)
            .Concat(SensitiveStatementCorpus.NotNamed);
        return corpus.Concat(FuzzStrings());
    }

    private static readonly string[] s_fuzzTokens =
    {
        "create", "alter", "role", "user", "group", "subscription", "server", "login", "credential", "scoped",
        "password", "passwd", "pwd", "secret", "key_source", "x_pwd", "xpassword", "pgpassword", "sp_addlogin",
        "sp_password", "encryptbypassphrase", "pwdcompare", "opendatasource", "openrowset", "select", "from",
        "update", "set", "dbo.t", "col", "bulk", "(", ")", "'", "n'", "e'", "u&'", "0x", "0xff", "$1", "$a", "=",
        "to", ":", "@", "/", "://", "user:pw@", "http://", "]", "[", "\"", "[pwd]", "\"secret\"", ";", ",", "_",
        "9", "a", " ", " ", " ", "\n", "\t", "--", "-- c", "/* c */", "/*", "*/", "*",
        "nvarchar", "varchar(20)", "(max)", "text", "int", "as", "constant", "default", ":=", "[nvarchar]", "sys.sysname",
    };

    /// <summary>A seeded set of short strings built from the tokens the pattern is made of, so a large share of
    /// them reach a branch of the pattern.</summary>
    private static IEnumerable<string> FuzzStrings()
    {
        var random = new Random(5324);
        for (var i = 0; i < 30_000; i++)
        {
            var count = random.Next(1, 14);
            var builder = new StringBuilder();
            for (var j = 0; j < count; j++)
            {
                builder.Append(s_fuzzTokens[random.Next(s_fuzzTokens.Length)]);
                if (random.Next(3) == 0)
                {
                    builder.Append(' ');
                }
            }

            yield return builder.ToString();
        }
    }

    /// <summary>The pre-check is a superset of the full pattern (#5320 M1): every string the full judge names,
    /// the pre-check also hits, and the pre-checked judge gives the same verdict as the full judge alone. Run
    /// over the corpus and a seeded fuzz set; no PostgreSQL needed.</summary>
    [Fact]
    public void ThePrefilterHitsEveryStringTheFullJudgeNames_OverTheCorpusAndAFuzzSet()
    {
        var timeout = TimeSpan.FromSeconds(5);
        var prefilter = SensitiveStatements.CreatePrefilter(SensitiveStatements.Pattern, timeout);
        Assert.NotNull(prefilter);
        var full = SensitiveStatements.CreateJudge(SensitiveStatements.Pattern, timeout, prefilter: false);
        var combined = SensitiveStatements.CreateJudge(SensitiveStatements.Pattern, timeout);

        var named = 0;
        var failures = new List<string>();
        foreach (var text in CorpusAndFuzz())
        {
            var verdict = full(text);
            if (verdict == SensitiveStatements.Verdict.Named)
            {
                named++;
                if (!prefilter!.IsMatch(text))
                {
                    failures.Add("the pre-check missed a named string: " + text);
                }
            }

            if (combined(text) != verdict)
            {
                failures.Add("the pre-checked judge disagrees with the full judge: " + text);
            }
        }

        Assert.True(named > 1_000, $"the fuzz set names only {named} strings, too few to pin anything");
        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures.Take(20)));
    }

    /// <summary>The pre-check uses no lookaround and is built from the one definition: it is the translation
    /// with only the word-boundary assertions removed.</summary>
    [Fact]
    public void ThePrefilterSourceIsTheTranslationWithoutItsAssertions()
    {
        Assert.True(SensitiveStatements.TryTranslate(SensitiveStatements.Pattern, factor: false, dropAssertions: true, out var superset));

        Assert.DoesNotContain("(?<", superset, StringComparison.Ordinal);
        Assert.DoesNotContain("(?=", superset, StringComparison.Ordinal);
        Assert.DoesNotContain("(?!", superset, StringComparison.Ordinal);
        Assert.DoesNotContain("[:", superset, StringComparison.Ordinal);
        Assert.StartsWith("(create|alter)", superset, StringComparison.Ordinal);
    }

    [Fact]
    public void AFailedGuardGivesNoPrefilter_AndAPatternItCannotBuildKeepsTheFullJudge()
    {
        Assert.Null(SensitiveStatements.CreatePrefilter("a\\b", TimeSpan.FromSeconds(1)));
        Assert.Null(SensitiveStatements.CreatePrefilter("[[:bogus:]]", TimeSpan.FromSeconds(1)));
        // A pattern with a lookahead of its own cannot run NonBacktracking, so there is no pre-check and the
        // full judge still answers.
        Assert.Null(SensitiveStatements.CreatePrefilter("a(?=b)", TimeSpan.FromSeconds(1)));
        var judge = SensitiveStatements.CreateJudge("a(?=b)", TimeSpan.FromSeconds(1));
        Assert.Equal(SensitiveStatements.Verdict.Named, judge("xab"));
        Assert.Equal(SensitiveStatements.Verdict.Clean, judge("xac"));
    }

    /// <summary>Hoisting the shared word start out of the alternatives is only a search-order change (#5320 L5).
    /// The factored and unfactored forms give the same verdict over the corpus and a seeded fuzz set, so the
    /// factoring is pinned without a PostgreSQL rig. Both run without the pre-check, so it is the regex that is
    /// compared.</summary>
    [Fact]
    public void TheFactoredAndUnfactoredFormsGiveTheSameVerdict_OverTheCorpusAndAFuzzSet()
    {
        var timeout = TimeSpan.FromSeconds(5);
        Assert.True(SensitiveStatements.TryTranslate(SensitiveStatements.Pattern, factor: true, dropAssertions: false, out var factoredSource));
        Assert.True(SensitiveStatements.TryTranslate(SensitiveStatements.Pattern, factor: false, dropAssertions: false, out var unfactoredSource));
        Assert.NotEqual(factoredSource, unfactoredSource);
        var factored = SensitiveStatements.CreateJudge(SensitiveStatements.Pattern, timeout, factor: true, prefilter: false);
        var unfactored = SensitiveStatements.CreateJudge(SensitiveStatements.Pattern, timeout, factor: false, prefilter: false);

        var named = 0;
        var failures = new List<string>();
        foreach (var text in CorpusAndFuzz())
        {
            var verdict = factored(text);
            named += verdict == SensitiveStatements.Verdict.Named ? 1 : 0;
            if (unfactored(text) != verdict)
            {
                failures.Add("factored and unfactored differ: " + text);
            }
        }

        Assert.True(named > 1_000, $"the fuzz set names only {named} strings, too few to pin anything");
        Assert.True(failures.Count == 0, string.Join(Environment.NewLine, failures.Take(20)));
    }

    [Fact]
    public void AMillionCharOrdinaryStatementIsCleanAndFast()
    {
        SensitiveStatements.Judge("warm the regex up");
        var unit = "SELECT col_a, col_b FROM dbo.t WHERE c = 1 AND d <> 2 ";
        var text = string.Concat(Enumerable.Repeat(unit, 1_000_000 / unit.Length + 1)).Substring(0, 1_000_000);

        var best = long.MaxValue;
        var clean = false;
        for (var attempt = 0; attempt < 5 && !clean; attempt++)
        {
            var watch = Stopwatch.StartNew();
            var verdict = SensitiveStatements.Judge(text);
            watch.Stop();

            clean = verdict == SensitiveStatements.Verdict.Clean;
            best = Math.Min(best, watch.ElapsedMilliseconds);
        }

        Assert.True(clean, $"never came back Clean; best attempt {best} ms");
        Assert.True(best < 250, $"took {best} ms");
    }

    [Fact]
    public void TextReturnsThePlaceholderForANamedValue_AndTheSameInstanceOtherwise()
    {
        var clean = new string("SELECT 1 FROM dbo.t".ToCharArray());

        Assert.Equal(SensitiveStatements.PlaceholderText, SensitiveStatements.Text("CREATE LOGIN [a] WITH PASSWORD = 'p'"));
        Assert.Same(clean, SensitiveStatements.Text(clean));
    }

    [Fact]
    public void TextWithholdsAValueThatIsNamedOnlyOnceHtmlDecoded()
    {
        // "&#32;" and "&#x20;" decode to a space, so the encoded form hides the statement from a plain read.
        var encoded = "CREATE&#32;LOGIN [a] FROM WINDOWS";
        Assert.False(SensitiveStatements.Names(encoded));

        Assert.Equal(SensitiveStatements.PlaceholderText, SensitiveStatements.Text(encoded));
        var harmless = "SELECT 'a &amp; b' FROM dbo.t";
        Assert.Same(harmless, SensitiveStatements.Text(harmless));
    }

    /// <summary>With IgnoreCase .NET reads U+212A (the Kelvin sign) as a word character, which PostgreSQL's
    /// C-locale word-start check does not (#5320 L1). The word class is case-sensitive, so a Kelvin sign next
    /// to a named keyword is a boundary here too, and the pre-check still hits it.</summary>
    [Fact]
    public void AKelvinSignNextToANamedKeywordIsAWordBoundary_AndThePrefilterStillHitsIt()
    {
        const string text = "x \u212Asp_addlogin 'a'";
        var timeout = TimeSpan.FromSeconds(5);
        var prefilter = SensitiveStatements.CreatePrefilter(SensitiveStatements.Pattern, timeout);

        Assert.Equal(SensitiveStatements.Verdict.Named, SensitiveStatements.Judge(text));
        Assert.True(SensitiveStatements.Names(text));
        Assert.NotNull(prefilter);
        Assert.Matches(prefilter!, text);
        Assert.Equal(SensitiveStatements.Verdict.Named, SensitiveStatements.CreateJudge(SensitiveStatements.Pattern, timeout, prefilter: false)(text));
    }

    // ---- JudgeBudget -------------------------------------------------------------------------------------

    [Fact]
    public void ABudgetStopsRunningTheJudgeOnceSpent_AndCountsTheRestUnjudged()
    {
        // A fake clock, not a sleep: each judge call advances it 30 ms, so a 100 ms budget is spent after
        // exactly four calls (0, 30, 60 and 90 ms elapsed all leave it unspent; 120 ms spends it) (#5320).
        var calls = 0;
        var now = TimeSpan.Zero;
        var budget = new SensitiveStatements.JudgeBudget(TimeSpan.FromMilliseconds(100), _ =>
        {
            calls++;
            now += TimeSpan.FromMilliseconds(30);
            return SensitiveStatements.Verdict.Clean;
        }, clock: () => now);

        var verdicts = Enumerable.Range(0, 20).Select(i => budget.Judge("v" + i.ToString(CultureInfo.InvariantCulture))).ToList();

        Assert.True(budget.Spent);
        Assert.Equal(4, calls);
        Assert.Equal(20 - calls, budget.Unjudged);
        Assert.All(verdicts.Skip(calls), v => Assert.Equal(SensitiveStatements.Verdict.Named, v));
        Assert.All(verdicts.Take(calls), v => Assert.Equal(SensitiveStatements.Verdict.Clean, v));
        Assert.Equal(TimeSpan.FromMilliseconds(120), budget.Elapsed);

        var before = calls;
        budget.Judge("one more");
        Assert.Equal(before, calls);
        Assert.Equal(20 - calls + 1, budget.Unjudged);
    }

    [Fact]
    public void ABudgetCountsNamedAndTimedOutVerdicts()
    {
        var verdicts = new Queue<SensitiveStatements.Verdict>(new[]
        {
            SensitiveStatements.Verdict.Named,
            SensitiveStatements.Verdict.TimedOut,
            SensitiveStatements.Verdict.Clean,
        });
        var budget = new SensitiveStatements.JudgeBudget(TimeSpan.FromSeconds(30), _ => verdicts.Dequeue());

        budget.Judge("a");
        budget.Judge("b");
        budget.Judge("c");

        Assert.Equal(1, budget.Named);
        Assert.Equal(1, budget.TimedOut);
        Assert.Equal(0, budget.Unjudged);
        Assert.False(budget.Spent);
    }

    [Fact]
    public void AddElapsedChargesTheBudget()
    {
        var ran = false;
        var budget = new SensitiveStatements.JudgeBudget(TimeSpan.FromSeconds(1), _ =>
        {
            ran = true;
            return SensitiveStatements.Verdict.Clean;
        });

        budget.AddElapsed(TimeSpan.FromSeconds(1));

        Assert.True(budget.Spent);
        Assert.Equal(SensitiveStatements.Verdict.Named, budget.Judge("x"));
        Assert.False(ran);
        Assert.Equal(1, budget.Unjudged);
    }

    [Fact]
    public void AJudgeThatThrowsIsNamed()
    {
        var budget = new SensitiveStatements.JudgeBudget(TimeSpan.FromSeconds(1), _ => throw new InvalidOperationException("x"));

        Assert.Equal(SensitiveStatements.Verdict.Named, budget.Judge("x"));
        Assert.Equal(1, budget.Named);
    }

    /// <summary>The build of the shared judge (the regex compile, the pre-check and a warm-up match) is not
    /// charged to a budget (#5320 L3): the first call is charged its match time only. The fake clock moves
    /// 100 ms inside the build and 1 ms inside the match.</summary>
    [Fact]
    public void TheFirstCallOfABudgetIsChargedItsMatchTimeNotTheBuild()
    {
        var now = TimeSpan.Zero;
        var built = 0;
        var shared = new Lazy<Func<string, SensitiveStatements.Verdict>>(() =>
        {
            built++;
            now += TimeSpan.FromMilliseconds(100);
            return _ =>
            {
                now += TimeSpan.FromMilliseconds(1);
                return SensitiveStatements.Verdict.Clean;
            };
        });
        var budget = new SensitiveStatements.JudgeBudget(TimeSpan.FromSeconds(1), judge: null, clock: () => now, shared: shared);

        Assert.Equal(SensitiveStatements.Verdict.Clean, budget.Judge("SELECT 1"));
        Assert.Equal(SensitiveStatements.Verdict.Clean, budget.Judge("SELECT 2"));

        Assert.Equal(1, built);
        Assert.Equal(TimeSpan.FromMilliseconds(2), budget.Elapsed);
        Assert.False(budget.Spent);
    }

    [Fact]
    public void ABudgetWithoutAFakeJudgeUsesTheSharedPattern()
    {
        var budget = new SensitiveStatements.JudgeBudget(TimeSpan.FromSeconds(5));

        Assert.Equal(SensitiveStatements.Verdict.Named, budget.Judge("CREATE LOGIN [a] FROM WINDOWS"));
        Assert.Equal(SensitiveStatements.Verdict.Clean, budget.Judge("SELECT 1"));
    }

    // ---- Measurements (recorded in the test diagnostics on every run) -------------------------------------

    [Fact]
    public void Measurements_AreRecordedAndWithinTheBounds()
    {
        SensitiveStatements.Judge("warm the regex up");

        // Throughput over 10,000 generated ordinary statements, 200-4,000 chars each.
        var fragments = new[]
        {
            "SELECT col_a, col_b, COUNT(*) FROM dbo.Orders AS o JOIN dbo.Customers AS c ON c.id = o.customer_id ",
            "WHERE o.created_at >= @from AND o.status IN (1, 2, 3) AND c.region <> N'north' ",
            "GROUP BY col_a, col_b HAVING COUNT(*) > 10 ORDER BY 3 DESC; ",
            "UPDATE dbo.Inventory SET qty = qty - @n, touched = SYSUTCDATETIME() WHERE sku = @sku; ",
            "INSERT INTO dbo.AuditTrail (kind, detail) VALUES (N'order', N'shipped'); ",
            "/* a block comment that explains the next line */ -- and a line comment ",
        };
        var random = new Random(4348);
        var statements = new string[10_000];
        long totalChars = 0;
        for (var i = 0; i < statements.Length; i++)
        {
            var length = random.Next(200, 4001);
            var builder = new StringBuilder(length + 120);
            while (builder.Length < length)
            {
                builder.Append(fragments[random.Next(fragments.Length)]);
            }

            statements[i] = builder.ToString(0, length);
            totalChars += length;
        }

        var watch = Stopwatch.StartNew();
        var named = 0;
        foreach (var statement in statements)
        {
            if (SensitiveStatements.Judge(statement) != SensitiveStatements.Verdict.Clean)
            {
                named++;
            }
        }

        watch.Stop();
        var megabytes = totalChars / 1_048_576.0;
        Record($"Judge over 10,000 statements: {megabytes:F1} MB in {watch.ElapsedMilliseconds} ms = {megabytes / watch.Elapsed.TotalSeconds:F1} MB/s, named={named}");
        Assert.Equal(0, named);

        // The 1,000,000-character guard.
        var unit = "SELECT col_a, col_b FROM dbo.t WHERE c = 1 AND d <> 2 ";
        var big = string.Concat(Enumerable.Repeat(unit, 1_000_000 / unit.Length + 1)).Substring(0, 1_000_000);
        watch.Restart();
        var bigVerdict = SensitiveStatements.Judge(big);
        watch.Stop();
        Record($"Judge on 1,000,000 chars: {bigVerdict}, {watch.Elapsed.TotalMilliseconds:F1} ms");

        // The two judge parts and the whole pattern as one regex, on the same 1,000,000 characters (#5320 M1), without
        // the pre-check and with a timeout that cannot fire, so the numbers are the cost of the matching alone.
        Assert.True(SensitiveStatements.TrySplitAlternatives(
            SensitiveStatements.Pattern, SensitiveStatements.JudgeHeadAlternatives, out var headSource, out var tailSource));
        var longTimeout = TimeSpan.FromSeconds(30);
        foreach (var (label, source, parts) in new[]
        {
            ("part 1 (A1-T9)", headSource, 0),
            ("part 2 (T10)", tailSource, 0),
            ("both parts", SensitiveStatements.Pattern, SensitiveStatements.JudgeHeadAlternatives),
            ("whole pattern as one regex", SensitiveStatements.Pattern, 0),
        })
        {
            var judge = SensitiveStatements.CreateJudge(source, longTimeout, prefilter: false, headAlternatives: parts);
            judge(big);
            var bestMs = double.MaxValue;
            for (var attempt = 0; attempt < 3; attempt++)
            {
                watch.Restart();
                judge(big);
                watch.Stop();
                bestMs = Math.Min(bestMs, watch.Elapsed.TotalMilliseconds);
            }

            Record($"Judge on 1,000,000 chars, {label}, no pre-check: best of 3 {bestMs:F1} ms");
        }

        // Each adversarial string.
        foreach (var text in SensitiveStatementCorpus.Adversarial)
        {
            watch.Restart();
            var verdict = SensitiveStatements.Judge(text);
            watch.Stop();
            Record($"Judge on adversarial '{text.Substring(0, 8)}...': {verdict}, {watch.Elapsed.TotalMilliseconds:F1} ms");
        }

        // The worst overrun past the limit with a 250 ms judge (read-time limit 1.5 s, and a limit that is not
        // a multiple of the judge's time).
        foreach (var limitMs in new[] { 1500, 1400, 1260 })
        {
            var budget = new SensitiveStatements.JudgeBudget(TimeSpan.FromMilliseconds(limitMs), _ =>
            {
                Thread.Sleep(250);
                return SensitiveStatements.Verdict.TimedOut;
            });
            watch.Restart();
            while (!budget.Spent)
            {
                budget.Judge("x");
            }

            watch.Stop();
            var overrun = budget.Elapsed - TimeSpan.FromMilliseconds(limitMs);
            Record($"JudgeBudget limit {limitMs} ms, 250 ms judge: stopped at {budget.Elapsed.TotalMilliseconds:F0} ms, overrun {overrun.TotalMilliseconds:F0} ms");
            Assert.True(overrun < TimeSpan.FromMilliseconds(300), $"overrun {overrun.TotalMilliseconds} ms");
        }
    }

    private static void Record(string message) => TestContext.Current.SendDiagnosticMessage(message);
}
