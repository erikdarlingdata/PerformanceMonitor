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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320 (part of #4348): alert notifications go through the statement filter. The engine's <c>FireAsync</c>
/// filters every outcome before it logs or delivers it; the finding senders filter a finding before they send or
/// record it. These pins run the real engine against a fake deliverer, so an alert that carries a statement or a
/// report in its details, its attachments or its short message reaches the deliverer withheld.
/// </summary>
[Collection("timing")]
public sealed class StatementFilterAlertTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;

    /// <summary>A blocked-process report whose two input buffers hold the canary and the plain statement.</summary>
    internal static string ReportXml() =>
        "<blocked-process-report monitorLoop=\"1\"><blocked-process><process id=\"p1\" spid=\"55\">"
        + "<inputbuf>" + StatementScrubCanary.CanaryStatement + "</inputbuf></process></blocked-process>"
        + "<blocking-process><process id=\"p2\" spid=\"155\"><inputbuf>" + StatementScrubCanary.PlainStatement
        + "</inputbuf></process></blocking-process></blocked-process-report>";

    /// <summary>Every string an outcome would put in front of a channel, joined, so one needle search covers it.</summary>
    internal static string Everything(AlertOutcome outcome)
    {
        var parts = new List<string?> { outcome.DetailText, outcome.ShortMessage, outcome.CurrentValue, outcome.ThresholdValue };
        parts.AddRange(Everything(outcome.Context));
        return string.Join("\n", parts.Where(p => p is not null));
    }

    internal static IEnumerable<string?> Everything(AlertContext? context)
    {
        if (context is null)
        {
            yield break;
        }

        yield return context.AttachmentXml;
        foreach (var item in context.Details)
        {
            yield return item.Heading;
            yield return item.Body;
            foreach (var (label, value) in item.Fields)
            {
                yield return label;
                yield return value;
            }

            foreach (var record in item.Records)
            {
                yield return record.Summary;
                foreach (var (label, text) in record.Texts)
                {
                    yield return label;
                    yield return text;
                }
            }
        }

        foreach (var incident in (context.Incidents ?? new List<AlertIncident>()).Where(i => i is not null))
        {
            foreach (var involved in incident.InvolvedObjects)
            {
                yield return involved;
            }

            yield return incident.Attachment?.Xml;
            foreach (var field in incident.DetailFields ?? Array.Empty<AlertIncidentField>())
            {
                yield return field.Value;
            }
        }
    }

    internal static void AssertNoSecret(string text)
    {
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, text, StringComparison.Ordinal);
        }
    }

    internal static AlertEngineTests.Harness BlockingHarness(out AlertEngineTests.Harness h)
    {
        h = new AlertEngineTests.Harness();
        h.Settings.BlockingEnabled = true;
        var row = AlertEngineTests.BlockingRow(55);
        row.BlockedSqlText = StatementScrubCanary.CanaryStatement;
        row.BlockingSqlText = StatementScrubCanary.PlainStatement;
        row.BlockedProcessReportXml = ReportXml();
        h.Adapter.Blocking.Add(row);
        return h;
    }

    [Fact]
    public async Task Engine_BlockingAlert_DeliversDetailsAndReportWithTheStatementWithheld()
    {
        var h = BlockingHarness(out _);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        var everything = Everything(outcome);
        AssertNoSecret(everything);
        Assert.Contains(Marker, everything, StringComparison.Ordinal);
        /* The rest of the alert is unchanged: the plain statement, the report's other buffer and the database. */
        Assert.Contains(StatementScrubCanary.PlainStatement, everything, StringComparison.Ordinal);
        Assert.Contains("StackOverflow", everything, StringComparison.Ordinal);
        Assert.NotNull(Assert.Single(outcome.Context!.Incidents!).Attachment);
        var attachment = outcome.Context.Incidents![0].Attachment!;
        Assert.Contains(Marker, attachment.Xml, StringComparison.Ordinal);
        Assert.Contains(StatementScrubCanary.PlainStatement, attachment.Xml, StringComparison.Ordinal);
        Assert.NotNull(System.Xml.Linq.XDocument.Parse(attachment.Xml));
    }

    [Fact]
    public async Task Engine_BlockingAlert_FlatDetailTextIsRebuiltFromTheFilteredContext()
    {
        var h = BlockingHarness(out _);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        /* The engine's flat text is the flattening of its context; after the filter it still is, so the two
           surfaces a channel renders (structured details and flat text) say the same thing. */
        Assert.False(string.IsNullOrEmpty(outcome.DetailText));
        Assert.Equal(AlertContextBuilders.ContextToDetailText(outcome.Context), outcome.DetailText);
        Assert.Contains(Marker, outcome.DetailText, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Engine_LongRunningQueryAlert_WithholdsTheQueryTextAndTheToastBody()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LongRunningQueryEnabled = true;
        h.Adapter.LongRunning.Add(new LongRunningQueryInfo
        {
            SessionId = 71,
            DatabaseName = "StackOverflow",
            QueryText = StatementScrubCanary.CanaryStatement,
            ElapsedSeconds = 45 * 60,
            CpuTimeMs = 1000,
            QueryHash = "0x9AAF0129E4E9AD07",
        });

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        var everything = Everything(outcome);
        AssertNoSecret(everything);
        Assert.Contains(Marker, everything, StringComparison.Ordinal);
        Assert.Contains("71", everything, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Engine_LongRunningQueryAlert_APlainQueryReachesTheDelivererUnchanged()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.LongRunningQueryEnabled = true;
        h.Adapter.LongRunning.Add(new LongRunningQueryInfo
        {
            SessionId = 71,
            DatabaseName = "StackOverflow",
            QueryText = StatementScrubCanary.PlainStatement,
            ElapsedSeconds = 45 * 60,
            CpuTimeMs = 1000,
            QueryHash = "0x9AAF0129E4E9AD07",
        });

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var everything = Everything(Assert.Single(h.Deliverer.Outcomes));
        Assert.Contains("canary_plain_ssf", everything, StringComparison.Ordinal);
        Assert.DoesNotContain(Marker, everything, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Engine_MuteRuleKeyedOnTheRawText_StillMutes_WhileTheDeliveredOutcomeIsFiltered()
    {
        var h = new AlertEngineTests.Harness
        {
            /* A rule that matches the statement as stored (before the filter existed): it is asked about the
               RAW text, so it still matches and the alert is recorded as muted. */
            IsMuted = context => context.QueryText?.Contains("canary_ssf", StringComparison.Ordinal) == true,
        };
        h.Settings.LongRunningQueryEnabled = true;
        h.Adapter.LongRunning.Add(new LongRunningQueryInfo
        {
            SessionId = 71,
            DatabaseName = "StackOverflow",
            QueryText = StatementScrubCanary.CanaryStatement,
            ElapsedSeconds = 45 * 60,
            CpuTimeMs = 1000,
            QueryHash = "0x9AAF0129E4E9AD07",
        });

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(outcome.Muted);
        AssertNoSecret(Everything(outcome));
    }

    [Fact]
    public void Apply_AnAlertWithNothingNamed_ComesBackAsTheSameInstance()
    {
        var context = new AlertContext();
        var item = new AlertDetailItem { Heading = "Blocking Chain #1" };
        item.Fields.Add(("Blocked Query", StatementScrubCanary.PlainStatement));
        context.Details.Add(item);
        context.AttachmentXml = ReportXml().Replace(StatementScrubCanary.CanaryStatement, StatementScrubCanary.PlainStatement, StringComparison.Ordinal);
        var outcome = new AlertOutcome(
            "1", "SRV", "Blocking Detected", "1", "1", context,
            AlertContextBuilders.ContextToDetailText(context), null, null, false, null, "short");

        Assert.Same(outcome, AlertStatementFilter.Apply(outcome));
        Assert.Same(context, AlertStatementFilter.Apply(context));
    }

    [Fact]
    public void Apply_JudgesEveryValueAnAlertCarries()
    {
        var context = new AlertContext { AttachmentXml = ReportXml(), AttachmentFileName = "blocked_process_report.xml" };
        var item = new AlertDetailItem { Heading = StatementScrubCanary.CanaryStatement, Body = StatementScrubCanary.CanaryStatement };
        item.Fields.Add(("Blocked Query", StatementScrubCanary.CanaryStatement));
        item.Fields.Add(("Database", "StackOverflow"));
        item.Records.Add(new AlertDetailRecord(1, StatementScrubCanary.CanaryStatement,
            new List<(string Label, string Text)> { ("Query Text", StatementScrubCanary.CanaryStatement), ("Other", StatementScrubCanary.PlainStatement) }));
        context.Details.Add(item);
        context.Incidents = new List<AlertIncident>
        {
            new("key", new[] { "dbo.t" }, DetailFields: new[] { new AlertIncidentField("Victim SQL", StatementScrubCanary.CanaryStatement) },
                Attachment: new AlertIncidentAttachment(ReportXml(), AlertIncidentAttachment.BlockedProcessReportFileName)),
        };

        var filtered = AlertStatementFilter.Apply(context)!;

        Assert.NotSame(context, filtered);
        var everything = string.Join("\n", Everything(filtered).Where(p => p is not null));
        AssertNoSecret(everything);
        Assert.Contains(StatementScrubCanary.PlainStatement, everything, StringComparison.Ordinal);
        Assert.Equal("StackOverflow", filtered.Details[0].Fields[1].Value);
        Assert.Equal("blocked_process_report.xml", filtered.AttachmentFileName);
        Assert.Equal("key", filtered.Incidents![0].DedupKey);
        Assert.Equal(AlertIncidentAttachment.BlockedProcessReportFileName, filtered.Incidents[0].Attachment!.FileName);

        /* The original is untouched: a context is shared with the caller's own state. */
        Assert.Contains(StatementScrubCanary.CanaryStatement, context.Details[0].Fields[0].Value, StringComparison.Ordinal);
        Assert.Contains("S3cret-canary-ssf", context.AttachmentXml, StringComparison.Ordinal);
    }

    [Fact]
    public void Apply_DetailTextThatIsNotTheFlatteningIsJudgedWhole()
    {
        var context = new AlertContext();
        var outcome = new AlertOutcome(
            "1", "SRV", "Self Alert", "1", "1", context,
            "The collector failed on " + StatementScrubCanary.CanaryStatement, null, null, false, null,
            "short " + StatementScrubCanary.CanaryStatement);

        var filtered = AlertStatementFilter.Apply(outcome);

        Assert.Equal(Marker, filtered.DetailText);
        Assert.Equal(Marker, filtered.ShortMessage);
    }

    [Fact]
    public void Apply_AFailureDeliversTheAlertWithItsDetailsAndAttachmentsDropped()
    {
        var context = new AlertContext { AttachmentXml = ReportXml(), AttachmentFileName = "blocked_process_report.xml" };
        /* A null item cannot be read: the filter must fail closed, not pass the alert through. */
        context.Details.Add(new AlertDetailItem { Heading = "ok" });
        context.Details.Add(null!);
        context.Incidents = new List<AlertIncident>
        {
            new("key", new[] { "dbo.t" }, DetailFields: new[] { new AlertIncidentField("Victim SQL", StatementScrubCanary.CanaryStatement) },
                Attachment: new AlertIncidentAttachment(ReportXml(), AlertIncidentAttachment.BlockedProcessReportFileName)),
        };
        var outcome = new AlertOutcome(
            "1", "SRV", "Blocking Detected", "1", "1", context, "flat " + StatementScrubCanary.CanaryStatement,
            null, null, false, null, "short " + StatementScrubCanary.CanaryStatement);

        var filtered = AlertStatementFilter.Apply(outcome);

        Assert.Empty(filtered.Context!.Details);
        Assert.Null(filtered.Context.AttachmentXml);
        Assert.Null(filtered.Context.AttachmentFileName);
        var incident = Assert.Single(filtered.Context.Incidents!);
        Assert.Equal("key", incident.DedupKey);
        Assert.Null(incident.Attachment);
        Assert.Null(incident.DetailFields);
        Assert.Equal(Marker, filtered.DetailText);
        Assert.Equal(Marker, filtered.ShortMessage);
        AssertNoSecret(Everything(filtered));
    }

    [Fact]
    public void Apply_AQuerySetThatSlowsTheJudge_FinishesInsideTheBudget()
    {
        /* Many long values built to make the judge work: nested gaps and unterminated comments. Past the budget a value
           is withheld unjudged, so the work is bounded however many arrive.

           #5459: this asserted `took < 1950 ms` on the real clock, so a runner that stalled for a moment (a collection,
           a descheduled thread) failed it though the budget logic was fine. The budget's clock is now a stepped one:
           every judge call is charged 50 ms, so the budget (1.5 s, plus a little earned per value) is spent after a few
           dozen of the 800 calls, on any machine. The real judge still runs on every value it is given. The wall-clock
           bound is kept at 10x only to catch a hang. */
        var context = new AlertContext();
        for (var i = 0; i < 400; i++)
        {
            var item = new AlertDetailItem { Heading = "Query " + i };
            var gap = string.Concat(Enumerable.Repeat("/* " + i + " */ -- x\n ", 600));
            item.Fields.Add(("Query Text", "create " + gap + " /* never closed " + new string('a', 20_000)));
            context.Details.Add(item);
        }

        var step = TimeSpan.FromMilliseconds(50);
        var budget = SteppingBudget(step);
        var stopwatch = Stopwatch.StartNew();
        AlertContext filtered;
        using (AlertStatementFilter.UseBudget(() => budget))
        {
            filtered = AlertStatementFilter.Apply(context)!;
        }

        stopwatch.Stop();

        Assert.True(stopwatch.ElapsedMilliseconds < 19_500, "took " + stopwatch.ElapsedMilliseconds + " ms");
        Assert.Equal(400, filtered.Details.Count);
        Assert.True(budget.Spent, "the budget did not run out");
        Assert.True(budget.Unjudged > 0, "nothing was withheld unjudged");
        // The judge ran only while the budget was open: it is charged exactly one step per call, so the time charged
        // can pass the limit by at most the one call that crossed the line. That is the bound, in the budget's own time.
        Assert.True(budget.Elapsed < budget.Limit + step, $"charged {budget.Elapsed} against a limit of {budget.Limit}");
    }

    [Fact]
    public void ApplyFinding_JudgesTheContextAndTheProse()
    {
        var context = new AlertContext();
        var item = new AlertDetailItem { Heading = "Top Cpu Queries" };
        item.Fields.Add(("Query Text", StatementScrubCanary.CanaryStatement));
        context.Details.Add(item);
        var alert = new FindingAlert(
            "Analysis: High CPU", "SRV", "1", "1", "101", context, 0.9, 0.5,
            "prose " + StatementScrubCanary.CanaryStatement, true);

        var filtered = AlertStatementFilter.Apply(alert);

        Assert.NotSame(alert, filtered);
        Assert.Equal(Marker, filtered.DetailText);
        AssertNoSecret(string.Join("\n", Everything(filtered.Context)));
        Assert.Equal("Analysis: High CPU", filtered.MetricName);
        Assert.Equal(0.9, filtered.Severity);

        var clean = alert with { DetailText = "prose", Context = new AlertContext() };
        Assert.Same(clean, AlertStatementFilter.Apply(clean));
    }

    [Fact]
    public void ApplyFindings_ReturnsTheSameListWhenNothingIsNamed_AndOnlyTheNamedAlertIsReplaced()
    {
        var clean = new FindingAlert("A", "SRV", "1", "1", "101", new AlertContext(), 0.9, 0.5, "plain", true);
        var named = new FindingAlert("B", "SRV", "1", "1", "101", new AlertContext(), 0.9, 0.5, StatementScrubCanary.CanaryStatement, true);

        var untouched = new List<FindingAlert> { clean };
        Assert.Same(untouched, AlertStatementFilter.Apply(untouched));

        var both = new List<FindingAlert> { clean, named };
        var filtered = AlertStatementFilter.Apply(both);
        Assert.Same(clean, filtered[0]);
        Assert.Equal(Marker, filtered[1].DetailText);
        Assert.Equal(StatementScrubCanary.CanaryStatement, both[1].DetailText);
    }

    [Fact]
    public async Task Engine_AFourMegabyteReport_ReachesTheDelivererFilteredNotWithheldWhole()
    {
        await StatementFilterWarmUp.EnsureAsync();
        using var pinned = UnspendableBudget();
        var h = BlockingHarness(out _);
        /* One report with a 4 MB tail of harmless elements: the walk reads all of it, withholds only the
           canary statement, and does not run out of budget. */
        h.Adapter.Blocking[0].BlockedProcessReportXml = ReportXml().Replace(
            "</blocked-process-report>",
            string.Concat(Enumerable.Repeat("<note>" + new string('x', 400) + "</note>", 10_000)) + "</blocked-process-report>",
            StringComparison.Ordinal);

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        AssertNoSecret(Everything(outcome));
        var xml = Assert.Single(outcome.Context!.Incidents!).Attachment!.Xml;
        Assert.True(xml.Length > 4_000_000, "the report was withheld whole: " + xml.Length);
        Assert.Contains(StatementScrubCanary.PlainStatement, xml, StringComparison.Ordinal);
    }

    [Fact]
    public void Apply_AFourMegabyteReport_ComesBackFilteredNotWithheldWhole()
    {
        StatementFilterWarmUp.Ensure();
        using var pinned = UnspendableBudget();
        var report = ReportXml().Replace(
            "</blocked-process-report>",
            string.Concat(Enumerable.Repeat("<note>" + new string('x', 400) + "</note>", 10_000)) + "</blocked-process-report>",
            StringComparison.Ordinal);
        var context = new AlertContext { AttachmentXml = report };

        var filtered = AlertStatementFilter.Apply(context)!;

        /* #5459: this asserted `took < 1950 ms` under the real 1.5 s + 0.5 s per MB wall-clock budget, so a slow runner
           withheld the document whole and failed it. The budget is pinned so it cannot run out; the proof that a
           document past the budget is withheld is Apply_ABudgetAlreadySpent_WithholdsTheDocumentWholeAndEveryValue. */
        Assert.Contains(StatementScrubCanary.PlainStatement, filtered.AttachmentXml!, StringComparison.Ordinal);
        Assert.True(filtered.AttachmentXml!.Length > 4_000_000, "the report was withheld whole: " + filtered.AttachmentXml.Length);
        AssertNoSecret(filtered.AttachmentXml!);
    }

    /// <summary>A budget that cannot run out on a slow runner (#5459): one hour, which is above the 10 s cap a budget
    /// grows to, so it stays one hour. Every clean value and document comes back as it went in.</summary>
    internal static IDisposable UnspendableBudget() =>
        AlertStatementFilter.UseBudget(() => new SensitiveStatements.JudgeBudget(TimeSpan.FromHours(1)));

    /// <summary>A budget that is already spent when the filter first looks at it (#5459): an hour of elapsed time is
    /// charged up front, which no document can earn back (the cap is 10 s). The real judge still runs, so nothing is
    /// faked except the time the budget has already been charged.</summary>
    private static SensitiveStatements.JudgeBudget SpentBudget()
    {
        var budget = new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget);
        budget.AddElapsed(TimeSpan.FromHours(1));
        Assert.True(budget.Spent);
        return budget;
    }

    [Fact]
    public void Apply_ABudgetAlreadySpent_WithholdsTheDocumentWholeAndEveryValue()
    {
        // #5459: the other half of the pinned budget. Past the budget a value is withheld unjudged, never passed through.
        StatementFilterWarmUp.Ensure();
        var report = ReportXml().Replace(
            "</blocked-process-report>",
            string.Concat(Enumerable.Repeat("<note>" + new string('x', 400) + "</note>", 10_000)) + "</blocked-process-report>",
            StringComparison.Ordinal);
        var context = new AlertContext { AttachmentXml = report };
        var item = new AlertDetailItem { Heading = "Plain" };
        item.Fields.Add(("Query Text", StatementScrubCanary.PlainStatement));
        context.Details.Add(item);

        SensitiveStatements.JudgeBudget? made = null;
        using (AlertStatementFilter.UseBudget(() => made = SpentBudget()))
        {
            var filtered = AlertStatementFilter.Apply(context)!;

            Assert.Equal(Marker, filtered.AttachmentXml);
            var field = Assert.Single(Assert.Single(filtered.Details).Fields);
            Assert.Equal(Marker, field.Value);
            Assert.DoesNotContain(StatementScrubCanary.PlainStatement, string.Join(Environment.NewLine, Everything(filtered)), StringComparison.Ordinal);
        }

        Assert.NotNull(made);
        Assert.True(made!.Unjudged > 0, "nothing was counted unjudged");
        Assert.Equal(0, made.Named);
    }

    [Fact]
    public void ApplyFinding_ABudgetAlreadySpent_WithholdsThePlainProse()
    {
        StatementFilterWarmUp.Ensure();
        var alert = new FindingAlert("Analysis: High CPU", "SRV", "1", "1", "101", new AlertContext(), 0.9, 0.5, "plain prose", true);

        using (AlertStatementFilter.UseBudget(SpentBudget))
        {
            Assert.Equal(Marker, AlertStatementFilter.Apply(alert).DetailText);
        }

        Assert.Equal("plain prose", AlertStatementFilter.Apply(alert).DetailText);
    }

    /// <summary>A report with a 4 MB tail of harmless elements, made different per <paramref name="distinct"/>
    /// so no two share a string (#5477).</summary>
    private static string BigReport(int distinct) =>
        ReportXml().Replace(
            "</blocked-process-report>",
            string.Concat(Enumerable.Repeat("<note>" + distinct.ToString(System.Globalization.CultureInfo.InvariantCulture) + new string('x', 399) + "</note>", 10_000))
                + "</blocked-process-report>",
            StringComparison.Ordinal);

    private static AlertEngineTests.Harness ThreeIncidentHarness()
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.BlockingEnabled = true;
        for (var i = 0; i < 3; i++)
        {
            var row = AlertEngineTests.BlockingRow(55 + i);
            row.ContentiousObject = "StackOverflow.dbo.Table" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            row.BlockedSqlText = StatementScrubCanary.CanaryStatement;
            row.BlockingSqlText = StatementScrubCanary.PlainStatement;
            row.BlockedProcessReportXml = BigReport(i);
            h.Adapter.Blocking.Add(row);
        }

        return h;
    }

    [Fact]
    public async Task Engine_ThreeIncidentsWithDifferentFourMegabyteReports_EachReachesTheDelivererFilteredNotWithheldWhole()
    {
        // #5477: the budget grows with the distinct documents it judges, so the third report is not starved by the first two.
        await StatementFilterWarmUp.EnsureAsync();
        using var pinned = UnspendableBudget();
        var h = ThreeIncidentHarness();

        await h.Build().EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        AssertNoSecret(Everything(outcome));
        var incidents = outcome.Context!.Incidents!;
        Assert.Equal(3, incidents.Count);
        foreach (var incident in incidents)
        {
            var xml = incident.Attachment!.Xml;
            Assert.True(xml.Length > 4_000_000, "a report was withheld whole: " + xml.Length);
            Assert.Contains(StatementScrubCanary.PlainStatement, xml, StringComparison.Ordinal);
            Assert.Contains(Marker, xml, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ApplyCore_ThreeIncidentsWithOneBlockingAlertContext_JudgesEachDistinctReportOnce()
    {
        // The engine's own context: the alert's attachment is the first incident's report (the same string).
        StatementFilterWarmUp.Ensure();
        var h = ThreeIncidentHarness();
        var context = AlertContextBuilders.BuildBlockingContext("SRV", h.Adapter.Blocking, Array.Empty<string>())!;
        Assert.Equal(3, context.Incidents!.Count);
        Assert.Same(context.AttachmentXml, context.Incidents[0].Attachment!.Xml);

        // #5459: the filtered result is read under a budget that cannot run out (a slow runner withheld the third report).
        var pinned = new SensitiveStatements.JudgeBudget(TimeSpan.FromHours(1));
        var filtered = AlertStatementFilter.ApplyCore(context, pinned)!;
        Assert.Equal(3, filtered.Incidents!.Count(i => i.Attachment!.Xml.Length > 4_000_000));

        // The accounting is read off a budget that starts where production's does. Earning and the memo do not depend on
        // whether the time ran out, so a slow runner changes neither number.
        var budget = new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget);
        AlertStatementFilter.ApplyCore(context, budget);

        // Three distinct reports: the alert-level copy of the first one was a memo hit, not a second walk.
        Assert.Equal(3, budget.DocumentPasses);
        Assert.Equal(3, pinned.DocumentPasses);
        // And the budget earned 0.5 s per MB of each of them (a little under 2.0 s each, so about 7.4 s), not 1.5 s in all.
        Assert.InRange(budget.Limit, TimeSpan.FromSeconds(7.25), TimeSpan.FromSeconds(8.5));
    }

    /// <summary>A budget whose clock moves <paramref name="step"/> on every read, so each judge call is charged exactly
    /// <paramref name="step"/> however fast or slow the machine is (#5459). The real judge still runs.</summary>
    private static SensitiveStatements.JudgeBudget SteppingBudget(TimeSpan step)
    {
        var now = TimeSpan.Zero;
        return new SensitiveStatements.JudgeBudget(SensitiveStatements.ReadBudget, clock: () => now += step);
    }

    [Fact]
    public void ApplyCore_ThreeIncidents_ABudgetThatRunsOutMidway_WithholdsWholeAndKeepsTheAccounting()
    {
        // #5459: the cause of the old "expected 3, actual 2". The filter judges under a wall-clock budget, and a runner
        // slow enough to spend it withholds a report whole. Here the clock charges 50 ms per judge call, so the budget
        // (1.5 s plus about 2 s a report, against about 10,000 values a report) is spent part of the way in, on any
        // machine. A report the budget was spent on comes back as the marker, nothing named leaks, and the numbers the
        // test above reads (passes, limit) are the same whether the time ran out or not.
        StatementFilterWarmUp.Ensure();
        var h = ThreeIncidentHarness();
        var context = AlertContextBuilders.BuildBlockingContext("SRV", h.Adapter.Blocking, Array.Empty<string>())!;

        var budget = SteppingBudget(TimeSpan.FromMilliseconds(50));
        var filtered = AlertStatementFilter.ApplyCore(context, budget)!;

        Assert.True(budget.Spent, "the budget did not run out");
        Assert.True(filtered.Incidents!.Count(i => i.Attachment!.Xml.Length > 4_000_000) < 3, "no report was withheld");
        Assert.Contains(filtered.Incidents!, i => i.Attachment!.Xml == Marker);
        AssertNoSecret(string.Join(Environment.NewLine, Everything(filtered)));
        Assert.Equal(3, budget.DocumentPasses);
        Assert.InRange(budget.Limit, TimeSpan.FromSeconds(7.25), TimeSpan.FromSeconds(8.5));
    }
}
