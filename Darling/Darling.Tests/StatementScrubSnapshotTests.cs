/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, rows 8, 19 and 20 of the collection census: the live snapshot of running requests
/// (<c>query_snapshots</c>: query_text, query_plan, live_query_plan), plan correction (<c>plan_correction</c>:
/// query_text, implementation_script) and the oversized-plan sweep (<c>oversized_plan_backlog</c>: plan_xml). Each
/// test plants the canary in the collector's input, runs the read and then <c>WritePayload</c> into a
/// <see cref="StatementScrubRecordingWriter"/>, and asserts on what was written: no secret needle, the statement
/// column exactly the marker, the plan parseable with the marker in statement 1 and the kept needles in statement 2,
/// and a plain statement the SAME instance the reader returned.
/// </summary>
public sealed partial class StatementCollectionCensusTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;

    private static CollectorContext SnapshotContext() => new()
    {
        ServerId = 7,
        ServerName = "ssf-test",
        CollectionTime = new DateTime(2026, 10, 6, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
        CurrentDatabaseName = "ssf_db",
    };

    /// <summary>A reader shaped like <c>query_snapshots</c>' 35 columns: a session id, then the columns the read
    /// guards with IsDBNull. Only the statement text and the two plans are set.</summary>
    private static DataTableReader SnapshotReader(params (string? Text, string? Plan, string? Live)[] rows)
    {
        var table = new DataTable("query_snapshots");
        for (var i = 0; i < 35; i++)
        {
            table.Columns.Add("c" + i, typeof(object));
        }

        var id = 1;
        foreach (var (text, plan, live) in rows)
        {
            var values = new object?[35];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = DBNull.Value;
            }

            values[0] = id++;
            values[3] = (object?)text ?? DBNull.Value;
            values[4] = (object?)plan ?? DBNull.Value;
            values[5] = (object?)live ?? DBNull.Value;
            table.Rows.Add(values);
        }

        return table.CreateDataReader();
    }

    private static async Task<(List<QuerySnapshotsCollector.Row> Rows, StatementScrubRecordingWriter Writer)> ReadAndWriteSnapshotsAsync(
        DataTableReader reader)
    {
        var context = SnapshotContext();
        var rows = await QuerySnapshotsCollector.Instance.ReadAsync(reader, context, CancellationToken.None);
        var writer = new StatementScrubRecordingWriter();
        foreach (var row in rows)
        {
            QuerySnapshotsCollector.Instance.WritePayload(row, writer, context);
        }

        return (rows, writer);
    }

    private static void AssertNoSecretNeedle(StatementScrubRecordingWriter writer)
    {
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.False(writer.AnyStringContains(needle), "a written value still holds " + needle);
        }
    }

    private static void AssertFilteredCanaryPlan(string? written)
    {
        Assert.NotNull(written);
        var parsed = System.Xml.Linq.XDocument.Parse(written!);
        Assert.NotNull(parsed.Root);
        Assert.Contains(Marker, written!, StringComparison.Ordinal);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, written!, StringComparison.Ordinal);
        }

        foreach (var kept in StatementScrubCanary.KeptNeedles)
        {
            Assert.Contains(kept, written!, StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task QuerySnapshots_QueryText_IsWithheld()
    {
        var plain = new string(StatementScrubCanary.PlainStatement.AsSpan());
        var (rows, writer) = await ReadAndWriteSnapshotsAsync(
            SnapshotReader((StatementScrubCanary.CanaryStatement, null, null), (plain, null, null)));

        Assert.Equal(Marker, rows[0].QueryText);
        Assert.Same(plain, rows[1].QueryText);
        AssertNoSecretNeedle(writer);
        Assert.Contains(Marker, writer.Strings);
        Assert.Contains(plain, writer.Strings);

        /* The same input judged twice reads the same (the family has no digest, so identity is the text itself). */
        var (again, _) = await ReadAndWriteSnapshotsAsync(
            SnapshotReader((StatementScrubCanary.CanaryStatement, null, null), (plain, null, null)));
        Assert.Equal(rows[0].QueryText, again[0].QueryText);
        Assert.Equal(rows[1].QueryText, again[1].QueryText);
    }

    [Fact]
    public async Task QuerySnapshots_QueryPlan_IsFiltered()
    {
        var (rows, writer) = await ReadAndWriteSnapshotsAsync(
            SnapshotReader((null, StatementScrubCanary.CanaryPlan(), null)));

        AssertNoSecretNeedle(writer);
        AssertFilteredCanaryPlan(rows[0].QueryPlan);
        Assert.Null(rows[0].LiveQueryPlan);
    }

    [Fact]
    public async Task QuerySnapshots_LiveQueryPlan_IsFiltered()
    {
        var (rows, writer) = await ReadAndWriteSnapshotsAsync(
            SnapshotReader((null, null, StatementScrubCanary.CanaryPlan())));

        AssertNoSecretNeedle(writer);
        AssertFilteredCanaryPlan(rows[0].LiveQueryPlan);
        Assert.Null(rows[0].QueryPlan);
    }

    [Fact]
    public async Task QuerySnapshots_APlainPlan_IsTheSameInstanceTheReaderReturned()
    {
        var plain = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"" + StatementScrubCanary.PlainStatement + "\" /></Statements></Batch></BatchSequence></ShowPlanXML>";
        var (rows, _) = await ReadAndWriteSnapshotsAsync(SnapshotReader((null, plain, plain)));

        Assert.Same(plain, rows[0].QueryPlan);
        Assert.Same(plain, rows[0].LiveQueryPlan);
    }

    /// <summary>A reader shaped like plan correction's 38 columns (ordinals 0-37); only the statement text (14) and
    /// the implementation script (37) are set.</summary>
    private static DataTableReader PlanCorrectionReader(string? queryText, string? script)
    {
        var table = new DataTable("plan_correction");
        for (var i = 0; i < 38; i++)
        {
            table.Columns.Add("c" + i, typeof(object));
        }

        var values = new object?[38];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = DBNull.Value;
        }

        values[14] = (object?)queryText ?? DBNull.Value;
        values[37] = (object?)script ?? DBNull.Value;
        table.Rows.Add(values);
        return table.CreateDataReader();
    }

    [Fact]
    public async Task PlanCorrection_QueryText_IsWithheld()
    {
        var context = SnapshotContext();
        var plain = new string(StatementScrubCanary.PlainStatement.AsSpan());

        /* Both reads: the Azure per-database read (ReadAsync) and the per-database item read (ReadItemAsync). */
        var azure = await PlanCorrectionCollector.Instance.ReadAsync(
            PlanCorrectionReader(StatementScrubCanary.CanaryStatement, null), context, CancellationToken.None);
        var item = new List<PlanCorrectionCollector.Row>();
        await PlanCorrectionCollector.Instance.ReadItemAsync(
            "ssf_db", PlanCorrectionReader(StatementScrubCanary.CanaryStatement, null), item, context, CancellationToken.None);
        var kept = await PlanCorrectionCollector.Instance.ReadAsync(
            PlanCorrectionReader(plain, null), context, CancellationToken.None);

        var writer = new StatementScrubRecordingWriter();
        foreach (var row in azure.Concat(item).Concat(kept))
        {
            PlanCorrectionCollector.Instance.WritePayload(row, writer, context);
        }

        Assert.Equal(Marker, azure[0].QueryText);
        Assert.Equal(Marker, item[0].QueryText);
        Assert.Same(plain, kept[0].QueryText);
        AssertNoSecretNeedle(writer);
    }

    [Fact]
    public async Task PlanCorrection_ImplementationScript_IsWithheld()
    {
        var context = SnapshotContext();
        var item = new List<PlanCorrectionCollector.Row>();
        await PlanCorrectionCollector.Instance.ReadItemAsync(
            "ssf_db", PlanCorrectionReader(null, StatementScrubCanary.CanaryStatement), item, context, CancellationToken.None);
        var plain = new string(StatementScrubCanary.PlainStatement.AsSpan());
        var kept = await PlanCorrectionCollector.Instance.ReadAsync(
            PlanCorrectionReader(null, plain), context, CancellationToken.None);

        var writer = new StatementScrubRecordingWriter();
        foreach (var row in item.Concat(kept))
        {
            PlanCorrectionCollector.Instance.WritePayload(row, writer, context);
        }

        Assert.Equal(Marker, item[0].ImplementationScript);
        Assert.Same(plain, kept[0].ImplementationScript);
        AssertNoSecretNeedle(writer);
    }

    /* ---- oversized_plan_backlog.plan_xml: the sweep's judging step ---- */

    [Fact]
    public void OversizedPlanSweep_PlanXml_IsFilteredBeforeItIsStored()
    {
        var (verdict, stored, error) = OversizedPlanBacklogSweep.JudgeFetchedPlan(
            new SensitiveStatements.Session(), StatementScrubCanary.CanaryPlan(), 0);

        Assert.Equal(OversizedPlanBacklogSweep.PlanFetchVerdict.Captured, verdict);
        Assert.Null(error);
        AssertFilteredCanaryPlan(stored);
    }

    [Fact]
    public void OversizedPlanSweep_PlanXml_APlainPlanIsTheSameInstance()
    {
        var plain = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>"
            + "<StmtSimple StatementText=\"" + StatementScrubCanary.PlainStatement + "\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

        var (verdict, stored, _) = OversizedPlanBacklogSweep.JudgeFetchedPlan(new SensitiveStatements.Session(), plain, 0);

        Assert.Equal(OversizedPlanBacklogSweep.PlanFetchVerdict.Captured, verdict);
        Assert.Same(plain, stored);
    }

    [Fact]
    public void OversizedPlanSweep_PlanXml_APlanTheBudgetCannotCover_StaysClaimableInsteadOfStoringTheMarker()
    {
        /* A session whose budget is already spent: the fetch is Failed (the row stays claimable and the next pass,
           with a fresh budget, fetches it again), and nothing is handed to the store. */
        var spent = new SensitiveStatements.Session(null, null, TimeSpan.Zero, null);

        var (verdict, stored, error) = OversizedPlanBacklogSweep.JudgeFetchedPlan(spent, StatementScrubCanary.CanaryPlan(), 0);

        Assert.Equal(OversizedPlanBacklogSweep.PlanFetchVerdict.Failed, verdict);
        Assert.Null(stored);
        Assert.False(string.IsNullOrEmpty(error));
        Assert.Equal(1, spent.Unjudged);
    }

    /* A session whose budget has room when the plan starts and runs out inside the plan: each judged value costs a
       second against a 1.5-second budget, so the plan itself is what spends it. */
    private static SensitiveStatements.Session SessionThePlanOverruns()
    {
        var now = TimeSpan.Zero;
        return new SensitiveStatements.Session(
            _ =>
            {
                now += TimeSpan.FromSeconds(1);
                return SensitiveStatements.Verdict.Clean;
            },
            () => now,
            TimeSpan.FromMilliseconds(1500),
            new SensitiveStatements.TimedOutMemo());
    }

    [Fact]
    public void OversizedPlanSweep_APlanThatOverrunsTheBudgetOnItsFirstAttempt_StaysClaimable()
    {
        var session = SessionThePlanOverruns();
        Assert.False(session.Spent);

        var (verdict, stored, error) = OversizedPlanBacklogSweep.JudgeFetchedPlan(session, StatementScrubCanary.CanaryPlan(), 0);

        Assert.Equal(OversizedPlanBacklogSweep.PlanFetchVerdict.Failed, verdict);
        Assert.Null(stored);
        Assert.False(string.IsNullOrEmpty(error));
        Assert.True(session.Spent);
    }

    [Fact]
    public void OversizedPlanSweep_APlanThatOverrunsTheBudgetAgain_IsStoredOnceAsTheWholePlanMarker()
    {
        var attempts = OversizedPlanBacklogSweep.MaxJudgeAttempts - 1;
        Assert.True(attempts >= 1, "the first judging failure must always leave the row claimable");

        var (verdict, stored, error) = OversizedPlanBacklogSweep.JudgeFetchedPlan(
            SessionThePlanOverruns(), StatementScrubCanary.CanaryPlan(), attempts);

        /* Captured is the verdict that stamps captured_at, which the claim excludes: the row is resolved. */
        Assert.Equal(OversizedPlanBacklogSweep.PlanFetchVerdict.Captured, verdict);
        Assert.Equal(SensitiveStatements.PlaceholderText, stored);
        Assert.Null(error);
    }

    [Fact]
    public void OversizedPlanSweep_ASessionAnEarlierPlanSpent_NeverRetiresARow_HoweverManyAttemptsItHas()
    {
        /* The budget was gone before this plan was reached: that says nothing about the plan. */
        var spent = new SensitiveStatements.Session(null, null, TimeSpan.Zero, null);

        var (verdict, stored, _) = OversizedPlanBacklogSweep.JudgeFetchedPlan(spent, StatementScrubCanary.CanaryPlan(), 500);

        Assert.Equal(OversizedPlanBacklogSweep.PlanFetchVerdict.Failed, verdict);
        Assert.Null(stored);
    }

    [Fact]
    public void OversizedPlanSweep_PassesItsBatchSessionIn_AndMakesNoSessionOfItsOwnInTheJudgingStep()
    {
        var source = System.IO.File.ReadAllText(RepoFile.PathTo("Darling/PerformanceMonitor.Darling.Service/OversizedPlanBacklogSweep.cs"));
        var bodies = StatementColumnCensusTests.BodiesOf(source, "OversizedPlanBacklogSweep", "JudgeFetchedPlan");

        Assert.Single(bodies);
        Assert.DoesNotContain("new SensitiveStatements.Session", bodies[0], StringComparison.Ordinal);
        Assert.DoesNotContain("??=", bodies[0], StringComparison.Ordinal);
    }

    /* ---- the worst case, for the part file: 200 snapshot rows with 47 KB plans plus one 27 MB plan ---- */

    [Fact]
    public async Task QuerySnapshots_WorstCase_TwoHundredPlansAndAHugeOne_FinishesInsideTheSessionBudget()
    {
        var rowPlan = SizedPlan(47 * 1024);
        var rows = new List<(string? Text, string? Plan, string? Live)>();
        for (var i = 0; i < 200; i++)
        {
            /* Distinct values so the verdict memo cannot answer for the batch. */
            rows.Add(($"SELECT {i} FROM dbo.t{i}", rowPlan.Replace("ssf_pad", "ssf_pad" + i, StringComparison.Ordinal), null));
        }

        rows.Add((null, SizedPlan(27 * 1024 * 1024), null));

        var clock = Stopwatch.StartNew();
        var (read, writer) = await ReadAndWriteSnapshotsAsync(SnapshotReader(rows.ToArray()));
        clock.Stop();

        TestContext.Current.SendDiagnosticMessage(
            "snapshot worst case: " + clock.ElapsedMilliseconds + " ms, " + read.Count + " rows, "
            + read.Count(r => r.QueryPlan == Marker) + " plans withheld whole");

        Assert.Equal(201, read.Count);
        AssertNoSecretNeedle(writer);
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(20), "the read took " + clock.Elapsed);
    }

    private static string SizedPlan(int approximateChars)
    {
        const string head = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence><Batch><Statements>";
        const string tail = "</Statements></Batch></BatchSequence></ShowPlanXML>";
        var sb = new System.Text.StringBuilder(approximateChars + 256);
        sb.Append(head);
        for (var n = 0; sb.Length < approximateChars; n++)
        {
            /* Each statement is distinct, so the verdict memo cannot answer for the document. */
            sb.Append("<StmtSimple StatementText=\"SELECT ssf_pad FROM dbo.t WHERE c = @c AND n = ").Append(n).Append("\" StatementId=\"1\" />");
        }

        sb.Append(tail);
        return sb.ToString();
    }
}
