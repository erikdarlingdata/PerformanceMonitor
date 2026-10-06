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
public sealed class StatementFilterAlertTests
{
    private const string Marker = SensitiveStatements.PlaceholderText;

    /// <summary>A blocked-process report whose two input buffers hold the canary and the plain statement.</summary>
    private static string ReportXml() =>
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

    private static void AssertNoSecret(string text)
    {
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, text, StringComparison.Ordinal);
        }
    }

    private static AlertEngineTests.Harness BlockingHarness(out AlertEngineTests.Harness h)
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
        /* Many long values built to make the judge work: nested gaps and unterminated comments. Past the 1.5 s
           budget a value is withheld unjudged, so the call is bounded however many arrive. */
        var context = new AlertContext();
        for (var i = 0; i < 400; i++)
        {
            var item = new AlertDetailItem { Heading = "Query " + i };
            var gap = string.Concat(Enumerable.Repeat("/* " + i + " */ -- x\n ", 600));
            item.Fields.Add(("Query Text", "create " + gap + " /* never closed " + new string('a', 20_000)));
            context.Details.Add(item);
        }

        var stopwatch = Stopwatch.StartNew();
        var filtered = AlertStatementFilter.Apply(context)!;
        stopwatch.Stop();

        Assert.True(stopwatch.ElapsedMilliseconds < 1950, "took " + stopwatch.ElapsedMilliseconds + " ms");
        Assert.Equal(400, filtered.Details.Count);
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
    public void Apply_AFourMegabyteReport_FinishesInsideTheBudget()
    {
        var report = ReportXml().Replace(
            "</blocked-process-report>",
            string.Concat(Enumerable.Repeat("<note>" + new string('x', 400) + "</note>", 10_000)) + "</blocked-process-report>",
            StringComparison.Ordinal);
        var context = new AlertContext { AttachmentXml = report };

        var stopwatch = Stopwatch.StartNew();
        var filtered = AlertStatementFilter.Apply(context)!;
        stopwatch.Stop();

        /* The budget is 1.5 s, and a document that crosses it is withheld whole, so a result that still holds
           the plain statement proves the walk finished inside it. */
        Assert.True(stopwatch.ElapsedMilliseconds < 1950, "took " + stopwatch.ElapsedMilliseconds + " ms");
        Assert.Contains(StatementScrubCanary.PlainStatement, filtered.AttachmentXml!, StringComparison.Ordinal);
        AssertNoSecret(filtered.AttachmentXml!);
    }
}
