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
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, #5320 (PR B, lane R1a): the collection-time statement filter session. The budget tests use a fake judge and
/// a fake clock, so no test waits on real time; the identity and canary tests use the real judge.
/// </summary>
public sealed class SensitiveStatementSessionTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;

    private static readonly TimeSpan Fifteen = TimeSpan.FromSeconds(15);

    /// <summary>A judge that counts its calls, charges a fixed cost to a fake clock, and answers by a rule.</summary>
    private sealed class FakeJudge(TimeSpan cost, Func<string, SensitiveStatements.Verdict>? rule = null)
    {
        public TimeSpan Now { get; private set; }

        public List<string> Seen { get; } = new();

        public Func<TimeSpan> Clock => () => Now;

        public SensitiveStatements.Verdict Judge(string text)
        {
            Seen.Add(text);
            Now += cost;
            return rule?.Invoke(text) ?? SensitiveStatements.Verdict.Clean;
        }
    }

    private static SensitiveStatements.Session NewSession(
        FakeJudge fake,
        SensitiveStatements.TimedOutMemo? memo = null,
        TimeSpan? limit = null) =>
        new(fake.Judge, fake.Clock, limit ?? Fifteen, memo ?? new SensitiveStatements.TimedOutMemo());

    // ---- budget --------------------------------------------------------------------------------------------

    [Fact]
    public void ValuesAfterTheBudgetIsSpentAreUnjudgedMarkers()
    {
        var fake = new FakeJudge(TimeSpan.FromSeconds(8));
        var session = NewSession(fake);

        Assert.Equal("a", session.Text("a"));
        Assert.Equal("b", session.Text("b"));
        Assert.True(session.Spent);

        var third = session.Text("c");

        Assert.Equal(Marker, third);
        Assert.Equal(2, fake.Seen.Count);
        Assert.Equal(1, session.Unjudged);
        Assert.Equal(0, session.Named);
    }

    [Fact]
    public void TryTextReturnsFalseForAValueLeftUnjudgedByASpentBudget_AndTrueForANamedOne()
    {
        var fake = new FakeJudge(
            TimeSpan.FromSeconds(8),
            text => text == "named" ? SensitiveStatements.Verdict.Named : SensitiveStatements.Verdict.Clean);
        var session = NewSession(fake);

        Assert.True(session.TryText("named", out var namedResult));
        Assert.Equal(Marker, namedResult);
        Assert.True(session.TryText("ok", out _));

        Assert.False(session.TryText("late", out var lateResult));
        Assert.Equal(Marker, lateResult);
        Assert.Equal(1, session.Named);
        Assert.Equal(1, session.Unjudged);
    }

    [Fact]
    public void AnUnjudgedValueIsNotRememberedSoALaterSessionJudgesIt()
    {
        var spent = new FakeJudge(TimeSpan.FromSeconds(20));
        var first = NewSession(spent);
        first.Text("x");
        Assert.False(first.TryText("y", out _));

        var fresh = new FakeJudge(TimeSpan.FromMilliseconds(1));
        var second = NewSession(fresh);

        Assert.True(second.TryText("y", out var result));
        Assert.Equal("y", result);
        Assert.Equal(new[] { "y" }, fresh.Seen);
    }

    [Fact]
    public void ABudgetCrossedMidXmlGivesTheWholeMarker_AndTheNextSessionGivesTheScrubbedPlan()
    {
        // r2 Q5. The fake judge calls the real judge and charges 5 s a value, so the plan runs out of budget
        // part way through.
        var plan = StatementScrubCanary.CanaryPlan();
        var slow = new FakeJudge(TimeSpan.FromSeconds(5), SensitiveStatements.Judge);
        var first = NewSession(slow);

        Assert.False(first.TryXml(plan, out var withheldWhole));
        Assert.Equal(Marker, withheldWhole);
        Assert.Equal(1, first.Unjudged);

        var next = new SensitiveStatements.Session();
        Assert.True(next.TryXml(plan, out var scrubbed));

        Assert.NotEqual(Marker, scrubbed);
        Assert.NotEqual(plan, scrubbed);
        Assert.Contains(Marker, scrubbed, StringComparison.Ordinal);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, scrubbed, StringComparison.Ordinal);
        }

        Assert.Contains("canary_plain_ssf", scrubbed, StringComparison.Ordinal);
        Assert.Equal(0, next.Unjudged);
    }

    [Fact]
    public void ABudgetAlreadySpentGivesTheWholeMarkerForADocumentThatNeedsJudging()
    {
        var fake = new FakeJudge(TimeSpan.FromSeconds(20));
        var session = NewSession(fake);
        session.Text("x");

        Assert.False(session.TryXml("<r><a>v1</a><b>v2</b></r>", out var result));
        Assert.Equal(Marker, result);
        Assert.Equal(1, session.Unjudged);
    }

    // ---- failure and identity ------------------------------------------------------------------------------

    [Fact]
    public void AThrowingJudgeGivesTheMarker_ForTextAndForXml_AndNeverThrows()
    {
        SensitiveStatements.Session NewThrowing() =>
            new(_ => throw new InvalidOperationException("judge failed"), null, Fifteen, new SensitiveStatements.TimedOutMemo());

        Assert.Equal(Marker, NewThrowing().Text("SELECT 1"));
        // A throwing judge names every value, so the document keeps its shape and no value survives.
        var xml = NewThrowing().Xml("<r><a>SELECT 1</a></r>");
        Assert.Contains(Marker, xml, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT 1", xml, StringComparison.Ordinal);
    }

    [Fact]
    public void PlainTextAndPlainXmlComeBackAsTheSameInstance()
    {
        foreach (var session in new[] { new SensitiveStatements.Session(), NewSession(new FakeJudge(TimeSpan.Zero)) })
        {
            var text = new string(StatementScrubCanary.PlainStatement.ToCharArray());
            Assert.Same(text, session.Text(text));

            var withAmpersand = new string("SELECT 1 WHERE a = 1 AND b &lt; 2".ToCharArray());
            Assert.Same(withAmpersand, session.Text(withAmpersand));

            var xml = new string("<r><a x=\"plain\">SELECT 1</a></r>".ToCharArray());
            Assert.Same(xml, session.Xml(xml));

            Assert.Null(session.Text(null));
            Assert.Same(string.Empty, session.Text(string.Empty));
            Assert.Null(session.Xml(null));
            Assert.Same(string.Empty, session.Xml(string.Empty));
        }
    }

    [Fact]
    public void ANamedValueIsTheMarker_IncludingOneNamedOnlyAfterHtmlDecoding()
    {
        var session = new SensitiveStatements.Session();

        Assert.Equal(Marker, session.Text(StatementScrubCanary.CanaryStatement));
        Assert.Equal(Marker, session.Text("CREATE&#32;LOGIN [a] WITH PASSWORD = N'S3cret'"));
        Assert.Equal(2, session.Named);
    }

    [Fact]
    public void TheCanaryPlanScrubbedInTwoSessionsGivesEqualStringsAndEqualHashes()
    {
        var plan = StatementScrubCanary.CanaryPlan();

        var first = new SensitiveStatements.Session().Xml(plan)!;
        var second = new SensitiveStatements.Session().Xml(plan)!;

        Assert.Equal(first, second);
        Assert.Equal(SHA256.HashData(Encoding.UTF8.GetBytes(first)), SHA256.HashData(Encoding.UTF8.GetBytes(second)));
        Assert.NotEqual(plan, first);
        Assert.Contains(Marker, first, StringComparison.Ordinal);
        foreach (var needle in StatementScrubCanary.SecretNeedles.Take(3))
        {
            Assert.DoesNotContain(needle, first, StringComparison.Ordinal);
        }

        Assert.Contains("canary_plain_ssf", first, StringComparison.Ordinal);
        Assert.Contains("param-canary-ssf", first, StringComparison.Ordinal);
    }

    // ---- memo ----------------------------------------------------------------------------------------------

    [Fact]
    public void TheSessionMemoJudgesARepeatedValueOnce()
    {
        var fake = new FakeJudge(TimeSpan.FromMilliseconds(1));
        var session = NewSession(fake);

        for (var i = 0; i < 5; i++)
        {
            Assert.Equal("SELECT 1", session.Text(new string("SELECT 1".ToCharArray())));
        }

        Assert.Single(fake.Seen);
        Assert.Equal(4, session.MemoHits);
        Assert.Equal(5, session.Values);
    }

    [Fact]
    public void TheSessionMemoKeepsANamedVerdictAndCountsEachValueWithheld()
    {
        var fake = new FakeJudge(
            TimeSpan.FromMilliseconds(1),
            _ => SensitiveStatements.Verdict.Named);
        var session = NewSession(fake);

        Assert.Equal(Marker, session.Text("s"));
        Assert.Equal(Marker, session.Text("s"));

        Assert.Single(fake.Seen);
        Assert.Equal(2, session.Named);
    }

    [Fact]
    public void ThereIsNoTimeoutCount_ASessionWithTenTimeoutsUnderTheBudgetStillJudgesTheEleventhValue()
    {
        var fake = new FakeJudge(
            TimeSpan.FromMilliseconds(250),
            text => text.StartsWith("slow", StringComparison.Ordinal)
                ? SensitiveStatements.Verdict.TimedOut
                : SensitiveStatements.Verdict.Clean);
        var session = NewSession(fake);

        for (var i = 0; i < 10; i++)
        {
            Assert.Equal(Marker, session.Text("slow " + i));
        }

        Assert.False(session.Spent);
        var value = "SELECT 11";
        Assert.True(session.TryText(value, out var result));
        Assert.Same(value, result);
        Assert.Equal(11, fake.Seen.Count);
        Assert.Equal(10, session.TimedOut);
        Assert.Equal(0, session.Unjudged);
    }

    [Fact]
    public void ASecondSessionWithTheSameSlowValueSpendsNoTimeOnIt()
    {
        // r2 M-G, A-5: a value whose match timed out once is remembered process-wide (the memo is shared here
        // the way the public sessions share theirs).
        var memo = new SensitiveStatements.TimedOutMemo();
        var slow = "slow-" + Guid.NewGuid();
        var firstFake = new FakeJudge(TimeSpan.FromMilliseconds(250), _ => SensitiveStatements.Verdict.TimedOut);
        var first = NewSession(firstFake, memo);
        Assert.Equal(Marker, first.Text(slow));
        Assert.Equal(250, first.ElapsedMs);
        Assert.Equal(1, memo.Count);

        var secondFake = new FakeJudge(TimeSpan.FromMilliseconds(250), _ => SensitiveStatements.Verdict.TimedOut);
        var second = NewSession(secondFake, memo);

        Assert.Equal(Marker, second.Text(slow));
        Assert.Empty(secondFake.Seen);
        Assert.Equal(0, second.ElapsedMs);
        Assert.Equal(1, second.TimedOutMemoHits);
        Assert.Equal(0, second.TimedOut);
        Assert.Equal(1, second.Named);
    }

    [Fact]
    public void OnlyTimedOutVerdictsAreRememberedProcessWide()
    {
        var memo = new SensitiveStatements.TimedOutMemo();
        var fake = new FakeJudge(
            TimeSpan.FromMilliseconds(1),
            text => text == "named" ? SensitiveStatements.Verdict.Named : SensitiveStatements.Verdict.Clean);

        var session = NewSession(fake, memo);
        session.Text("named");
        session.Text("clean");

        Assert.Equal(0, memo.Count);
    }

    [Fact]
    public void TheProcessWideMemoIsClearedWhenFull_AndStaysUnderItsLimit()
    {
        var memo = new SensitiveStatements.TimedOutMemo();
        for (var i = 0; i < 5000; i++)
        {
            memo.Add("v" + i);
        }

        Assert.InRange(memo.Count, 1, 4096);
        Assert.True(memo.Contains("v4999"));
        Assert.False(memo.Contains("v0"));
    }

    [Fact]
    public void ThePublicSessionSharesOneProcessWideMemo()
    {
        var value = "shared-" + Guid.NewGuid();
        SensitiveStatements.TimedOutMemo.Shared.Add(value);

        var session = new SensitiveStatements.Session();

        Assert.Equal(Marker, session.Text(value));
        Assert.Equal(1, session.TimedOutMemoHits);
        Assert.Equal(0, session.ElapsedMs);
    }

    // ---- the cycle's measurements --------------------------------------------------------------------------

    private static CollectorContext NewContext() => new()
    {
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
    };

    [Fact]
    public void ACycleThatJudgedNothingCarriesNoScrubMeasurements()
    {
        var context = NewContext();
        var session = context.BeginStatementScrub();
        session.Text(null);
        session.Text(string.Empty);

        Assert.Empty(context.Measurements);
        Assert.Empty(NewContext().Measurements);
    }

    [Fact]
    public void ACycleThatJudgedAValueCarriesTheFourMeasurements_AndASecondReadAddsNothingTwice()
    {
        var context = NewContext();
        var first = context.BeginStatementScrub();
        first.Text(StatementScrubCanary.CanaryStatement);
        first.Text(StatementScrubCanary.PlainStatement);
        var second = context.BeginStatementScrub();
        second.Text(StatementScrubCanary.CanaryStatement.Replace("canary_ssf", "other_ssf", StringComparison.Ordinal));

        long Get(string label) => context.Measurements.Single(m => m.Label == label).Value;

        Assert.Equal(2, Get(CollectorContext.StatementScrubNamedMeasurement));
        Assert.Equal(0, Get(CollectorContext.StatementScrubTimeoutsMeasurement));
        Assert.Equal(0, Get(CollectorContext.StatementScrubUnjudgedMeasurement));
        Assert.True(Get(CollectorContext.StatementScrubMsMeasurement) >= 0);
        Assert.Equal(4, context.Measurements.Count);

        // Reading again adds only what a session did since.
        Assert.Equal(2, Get(CollectorContext.StatementScrubNamedMeasurement));
        second.Text("CREATE LOGIN [again] WITH PASSWORD = N'x'");
        Assert.Equal(3, Get(CollectorContext.StatementScrubNamedMeasurement));
        Assert.Equal(4, context.Measurements.Count);
    }

    [Fact]
    public void TheScrubMeasurementLabelsAreLegalAndDistinct()
    {
        var labels = new[]
        {
            CollectorContext.StatementScrubNamedMeasurement,
            CollectorContext.StatementScrubTimeoutsMeasurement,
            CollectorContext.StatementScrubUnjudgedMeasurement,
            CollectorContext.StatementScrubMsMeasurement,
        };

        Assert.All(labels, label => Assert.True(CollectorMeasurementNote.IsValidLabel(label), label));
        Assert.Equal(4, labels.Distinct(StringComparer.Ordinal).Count());
    }

    // ---- measured (recorded, not compared) -----------------------------------------------------------------

    private static string Plan(int targetBytes)
    {
        var node = "<RelOp NodeId=\"{0}\" PhysicalOp=\"Index Seek\" LogicalOp=\"Index Seek\" EstimateRows=\"1\">"
            + "<OutputList><ColumnReference Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Column=\"c{0}\"/></OutputList>"
            + "<IndexScan Ordered=\"1\"><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[ix]\" IndexKind=\"NonClustered\"/></IndexScan></RelOp>";
        var builder = new StringBuilder(targetBytes + 1024);
        builder.Append("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4000.1\">")
            .Append("<BatchSequence><Batch><Statements><StmtSimple StatementText=\"SELECT c FROM dbo.t\" StatementId=\"1\" StatementType=\"SELECT\">")
            .Append("<QueryPlan CachedPlanSize=\"16\">");
        for (var i = 0; builder.Length < targetBytes; i++)
        {
            builder.AppendFormat(node, i);
        }

        return builder.Append("</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>").ToString();
    }

    private static void Record(string message) => TestContext.Current.SendDiagnosticMessage(message);

    [Fact]
    public void SessionTextThroughputOnTenThousandGeneratedStatementsIsRecorded()
    {
        var fragments = new[]
        {
            "SELECT a.col_a, b.col_b FROM dbo.TableA AS a JOIN dbo.TableB AS b ON a.id = b.a_id ",
            "WHERE a.created >= @from AND a.created < @to AND b.status IN (1, 2, 3) ",
            "INSERT INTO dbo.Audit (kind, detail) VALUES (N'x', @d); UPDATE dbo.T SET c = c + 1 WHERE k = @k; ",
            "GROUP BY a.col_a ORDER BY COUNT(*) DESC OPTION (RECOMPILE) ",
        };
        var random = new Random(5320);
        var statements = new string[10_000];
        long chars = 0;
        for (var i = 0; i < statements.Length; i++)
        {
            var length = random.Next(200, 4001);
            var builder = new StringBuilder(length + 120);
            while (builder.Length < length)
            {
                builder.Append(fragments[random.Next(fragments.Length)]);
            }

            builder.Append(" /* ").Append(i).Append(" */");
            statements[i] = builder.ToString();
            chars += statements[i].Length;
        }

        var session = new SensitiveStatements.Session();
        var watch = Stopwatch.StartNew();
        foreach (var statement in statements)
        {
            Assert.Same(statement, session.Text(statement));
        }

        watch.Stop();
        var megabytes = chars / 1_048_576.0;
        Record($"Session.Text over 10,000 statements: {megabytes:F1} MB in {watch.ElapsedMilliseconds} ms = {megabytes / watch.Elapsed.TotalSeconds:F1} MB/s, named={session.Named}, unjudged={session.Unjudged}");
        Assert.Equal(0, session.Named);
        Assert.Equal(0, session.Unjudged);
    }

    [Theory]
    [InlineData(512 * 1024)]
    [InlineData(27 * 1024 * 1024)]
    public void SessionXmlThroughputOnAPlanIsRecorded(int bytes)
    {
        var plan = Plan(bytes);
        var session = new SensitiveStatements.Session();

        var watch = Stopwatch.StartNew();
        var result = session.Xml(plan);
        watch.Stop();

        var megabytes = plan.Length / 1_048_576.0;
        Record($"Session.Xml on a {megabytes:F1} MB plan: {watch.ElapsedMilliseconds} ms = {megabytes / watch.Elapsed.TotalSeconds:F1} MB/s, unjudged={session.Unjudged}, spent={session.Spent}");
        Assert.Same(plan, result);
    }

    [Fact]
    public void TheMemoHitRateOnFiftyThousandQueryStoreShapedRowsWithTwoThousandDistinctTextsIsRecorded()
    {
        var random = new Random(4348);
        var distinct = new string[2_000];
        for (var i = 0; i < distinct.Length; i++)
        {
            distinct[i] = "SELECT c" + i + " FROM dbo.t" + i + " WHERE k = @k" + new string('x', random.Next(100, 2000));
        }

        var session = new SensitiveStatements.Session();
        var watch = Stopwatch.StartNew();
        for (var row = 0; row < 50_000; row++)
        {
            // A fresh instance per row, as a reader materializes one.
            session.Text(new string(distinct[random.Next(distinct.Length)].AsSpan()));
        }

        watch.Stop();
        var rate = 100.0 * session.MemoHits / session.Values;
        Record($"Memo over 50,000 rows with 2,000 distinct texts: hits={session.MemoHits} of {session.Values} = {rate:F1}% in {watch.ElapsedMilliseconds} ms");
        Assert.Equal(50_000, session.Values);
        Assert.True(session.MemoHits >= 50_000 - 2_000, $"hits {session.MemoHits}");
    }
}
