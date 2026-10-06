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
        (?:(?<![0-9A-Za-z_])(?=[0-9A-Za-z_])(?:(create|alter)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+(role|user|group|subscription|server)(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|password(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(=|to)?([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(e?'|u&'|[$][^0-9])|(pg)?password[\s]*=[\s]*[^$\s]|(create|alter)([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+(login|credential)(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|scoped([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)+credential(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|([a-z0-9_]*(password|passwd|pwd|secret)|key_source)(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(=|to)?([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(n?'|e'|u&'|0x|[$][^0-9])|pwd[\s]*=[\s]*[^$@\s]|(sp_addlogin|sp_password|sp_addlinkedsrvlogin|sp_addapprole|sp_approlepassword|sp_setapprole|sp_change_users_login|sp_adddistributor|sp_changedistributor_password|sp_adddistpublisher|sp_addsubscriber|sp_link_publication|sp_control_dbmasterkey_password|sp_xp_cmdshell_proxy_account)(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|(encryptbypassphrase|decryptbypassphrase|decryptbykeyautocert|decryptbykeyautoasymkey|decryptbyasymkey|decryptbycert|signbycert|signbyasymkey|pwdencrypt|pwdcompare)(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|opendatasource(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|openrowset([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*[(]([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*n?'|[a-z0-9_]*(password|passwd|pwd|secret)(]|")([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*=([\s]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^\p{Cc}]*)*(n?'|e'|u&'|0x)))|[a-z][a-z0-9+.-]*://[^\s/@:]+:[^\s/@]+@
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

        Assert.Equal("(?:(?<![0-9A-Za-z_])(?=[0-9A-Za-z_])(?:ab|cd))|ef", translated);
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

    [Fact]
    public void AMatchThatRunsPastItsTimeoutGivesTimedOut()
    {
        // Nested alternation under a quantifier backtracks exponentially on a near miss.
        var judge = SensitiveStatements.CreateJudge("(a|aa)+b", TimeSpan.FromMilliseconds(50));

        var verdict = judge(new string('a', 60) + "c");

        Assert.Equal(SensitiveStatements.Verdict.TimedOut, verdict);
        Assert.Equal(SensitiveStatements.Verdict.Named, judge("xaab"));
        Assert.Equal(SensitiveStatements.Verdict.Clean, judge("xyz"));
    }

    // The two timing tests take the best of several attempts: the test assembly runs many classes in parallel
    // and a loaded machine can stretch one wall-clock reading; the verdict itself must hold on every attempt
    // for the adversarial strings.
    [Fact]
    public void EachAdversarialStringGivesTimedOutWithin300Ms()
    {
        SensitiveStatements.Judge("warm the regex up");
        foreach (var text in SensitiveStatementCorpus.Adversarial)
        {
            var best = long.MaxValue;
            for (var attempt = 0; attempt < 4; attempt++)
            {
                var watch = Stopwatch.StartNew();
                var verdict = SensitiveStatements.Judge(text);
                watch.Stop();

                Assert.Equal(SensitiveStatements.Verdict.TimedOut, verdict);
                best = Math.Min(best, watch.ElapsedMilliseconds);
            }

            Assert.True(best <= 300, $"best of 4 took {best} ms: {text}");
            Assert.True(SensitiveStatements.Names(text));
        }
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

    // ---- JudgeBudget -------------------------------------------------------------------------------------

    [Fact]
    public void ABudgetStopsRunningTheJudgeOnceSpent_AndCountsTheRestUnjudged()
    {
        var calls = 0;
        var budget = new SensitiveStatements.JudgeBudget(TimeSpan.FromMilliseconds(100), _ =>
        {
            calls++;
            Thread.Sleep(30);
            return SensitiveStatements.Verdict.Clean;
        });

        var verdicts = Enumerable.Range(0, 20).Select(i => budget.Judge("v" + i.ToString(CultureInfo.InvariantCulture))).ToList();

        Assert.True(budget.Spent);
        Assert.InRange(calls, 3, 5);
        Assert.Equal(20 - calls, budget.Unjudged);
        Assert.All(verdicts.Skip(calls), v => Assert.Equal(SensitiveStatements.Verdict.Named, v));
        Assert.All(verdicts.Take(calls), v => Assert.Equal(SensitiveStatements.Verdict.Clean, v));
        Assert.True(budget.Elapsed >= TimeSpan.FromMilliseconds(100));

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
