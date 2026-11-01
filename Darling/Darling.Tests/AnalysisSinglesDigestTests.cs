/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Analysis Singles Digest (#3712) — the third daily document: the extraction from digest-routed ledger
/// rows (dedup per story, family parsing, the top-movers order, the by-family blocks), the DOCUMENT a human
/// reads (rendered pure, so its lines are assertable), and the fire/dedup lifecycle through the recording
/// deliverer and the in-memory stamp store, seam for seam with <c>FleetSweepRollupTests</c>.
/// </summary>
public class AnalysisSinglesDigestTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private static readonly DateTime Day = new(2026, 9, 19, 12, 0, 0, DateTimeKind.Utc);

    /* ---------------- fixtures ---------------- */

    private static string ContextJson(string reason, params string[] dedupKeys)
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = "Diagnosis" });
        context.Routing = new AlertRoutingDto(FindingRouting.DigestText, reason);
        if (dedupKeys.Length > 0)
        {
            context.Incidents = dedupKeys.Select(k => new AlertIncident(k, Array.Empty<string>())).ToList();
        }
        return AlertContextSerializer.Serialize(context);
    }

    private static AnalysisSinglesDigestReader.DigestRoutedRow Row(
        DateTime at, int serverId, string server, string category, string hash8, double severity,
        string reason = "uncorroborated: a lone fact — 1 fact in the chain and 0 of 5 co-fire checks matched.",
        params string[] dedupKeys)
        => new(at, serverId, server, $"Analysis: {category} [{hash8}]", severity, ContextJson(reason, dedupKeys));

    private sealed class RecordingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Outcomes { get; } = new();
        public AlertDelivery? Report { get; set; }

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            return Task.CompletedTask;
        }

        public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            await DeliverAsync(outcome, cancellationToken);
            return Report;
        }
    }

    private sealed class RecordingHistoryStore : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => Task.CompletedTask;
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }

    private sealed class MemoryStampStore : ISelfAlertDeliveryStampStore
    {
        public Dictionary<string, DateTime> Stamps { get; } = new(StringComparer.Ordinal);

        public Task<DateTime?> GetDeliveredAtUtcAsync(string stateKey, CancellationToken cancellationToken) =>
            Task.FromResult(Stamps.TryGetValue(stateKey, out var at) ? at : (DateTime?)null);

        public Task RecordDeliveredAtUtcAsync(string stateKey, DateTime deliveredAtUtc, CancellationToken cancellationToken)
        {
            Stamps[stateKey] = deliveredAtUtc;
            return Task.CompletedTask;
        }
    }

    private sealed class Harness
    {
        public DarlingConfig Config { get; } = new();
        public RecordingDeliverer Deliverer { get; } = new();
        public MemoryStampStore Stamps { get; } = new();
        public bool Muted { get; set; }
        public DateTime Now { get; set; } = Day;

        public DarlingSelfAlertEvaluator Build() => new(
            new DarlingAlertSettings(Config), Deliverer, new RecordingHistoryStore(), _ => Muted,
            logger: null, utcNow: () => Now, deliveryStamps: Stamps);
    }

    private static Task ApplyAsync(Harness h, DarlingSelfAlertEvaluator e, IReadOnlyList<AnalysisSinglesDigestReader.DigestRoutedRow> rows) =>
        e.ApplyAnalysisSinglesDigestAsync(rows, h.Now - DarlingSelfAlertEvaluator.AnalysisSinglesDigestInterval, h.Now, Ct);

    private static AnalysisSinglesDigestReader.DigestRoutedRow[] ThreeSinglesOnTwoServers() => new[]
    {
        Row(Day.AddHours(-5), 1, "pm-server-1", "anomaly", "aaaaaaaa", 1.62, dedupKeys: new[] { "dk-1" }),
        Row(Day.AddHours(-3), 2, "pm-server-2", "anomaly", "bbbbbbbb", 1.91, dedupKeys: new[] { "dk-2a", "dk-2b" }),
        Row(Day.AddHours(-2), 1, "pm-server-1", "queries", "cccccccc", 1.55),
    };

    /* ---------------- extraction ---------------- */

    [Fact]
    public void Extract_CountsDistinctSinglesAndServers_AndOrdersTopMoversBySeverity()
    {
        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(ThreeSinglesOnTwoServers());

        Assert.Equal(3, facts.Singles);
        Assert.Equal(2, facts.Servers);
        Assert.Equal(3, facts.Rows);
        Assert.Equal(new[] { 1.91, 1.62, 1.55 }, facts.TopMovers.Select(m => m.Severity).ToArray());
        Assert.Equal("pm-server-2", facts.TopMovers[0].Server);
        Assert.Equal(new[] { "dk-2a", "dk-2b" }, facts.TopMovers[0].DedupKeys);
        Assert.Contains("0 of 5", facts.TopMovers[0].Reason, StringComparison.Ordinal);
    }

    /// <summary>A story re-recorded after a restart is ONE single, at the newest row's severity and reason —
    /// the raw row count is kept beside it so the document can say the collapse happened.</summary>
    [Fact]
    public void Extract_CollapsesARestartsSecondRow_ToOneSingleAtTheNewestSeverity()
    {
        var rows = new[]
        {
            Row(Day.AddHours(-6), 1, "pm-server-1", "anomaly", "aaaaaaaa", 1.62),
            Row(Day.AddHours(-1), 1, "pm-server-1", "anomaly", "aaaaaaaa", 1.71, reason: "uncorroborated: newer"),
        };

        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(rows);

        Assert.Equal(1, facts.Singles);
        Assert.Equal(2, facts.Rows);
        var single = Assert.Single(facts.TopMovers);
        Assert.Equal(1.71, single.Severity);
        Assert.Equal("uncorroborated: newer", single.Reason);
        Assert.Equal(Day.AddHours(-1), single.LastRecordedUtc);
    }

    [Fact]
    public void Extract_GroupsByFamilyThenServer_MostPopulousFirst()
    {
        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(ThreeSinglesOnTwoServers());

        Assert.Equal(new[] { "anomaly", "queries" }, facts.Families.Select(f => f.Family).ToArray());
        var anomaly = facts.Families[0];
        Assert.Equal(2, anomaly.Singles);
        Assert.Equal(2, anomaly.Servers.Count);
        /* Ties on count break by name, so the document is stable across runs. */
        Assert.Equal(new[] { "pm-server-1", "pm-server-2" }, anomaly.Servers.Select(s => s.Server).ToArray());
    }

    [Theory]
    [InlineData("Analysis: anomaly [deadbeef]", "anomaly")]
    [InlineData("Analysis: cpu_pressure [12345678]", "cpu_pressure")]
    [InlineData("Analysis: finding []", "finding")]
    [InlineData("Analysis:  [x]", "Analysis:  [x]")]
    [InlineData("High CPU", "High CPU")]
    public void FamilyOf_ParsesTheMetricNameShape_AndKeepsAnythingElseWhole(string metricName, string expected)
    {
        Assert.Equal(expected, DarlingSelfAlertEvaluator.FamilyOf(metricName));
    }

    [Fact]
    public void Extract_KeepsARowWhoseContextDoesNotParse_WithAnEmptyReasonAndNoKeys()
    {
        var rows = new[]
        {
            new AnalysisSinglesDigestReader.DigestRoutedRow(Day.AddHours(-1), 1, "pm-server-1", "Analysis: anomaly [aaaaaaaa]", 1.6, "not json"),
            new AnalysisSinglesDigestReader.DigestRoutedRow(Day.AddHours(-1), 2, "", "Analysis: anomaly [bbbbbbbb]", 1.6, null),
        };

        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(rows);

        Assert.Equal(2, facts.Singles);
        Assert.All(facts.TopMovers, m => Assert.Equal(string.Empty, m.Reason));
        Assert.All(facts.TopMovers, m => Assert.Empty(m.DedupKeys));
        /* A row with no server name falls back to the id rather than an empty server line. */
        Assert.Contains(facts.TopMovers, m => m.Server == "2");
    }

    [Fact]
    public void Extract_OfNothing_IsZeroEverywhere()
    {
        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(Array.Empty<AnalysisSinglesDigestReader.DigestRoutedRow>());
        Assert.Equal(0, facts.Singles);
        Assert.Equal(0, facts.Servers);
        Assert.Empty(facts.TopMovers);
        Assert.Empty(facts.Families);
    }

    /* ---------------- the document ---------------- */

    [Fact]
    public void Render_LeadsWithTheHeadline_ListsMoversWithKeysReasonsAndFingerprints_AndTheFamilyBlocks()
    {
        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(ThreeSinglesOnTwoServers());
        var (shortMessage, detail) = DarlingSelfAlertEvaluator.RenderAnalysisSinglesDigest(facts, Day.AddDays(-1), Day);

        Assert.Equal("3 analysis singles across 2 servers", shortMessage);
        Assert.StartsWith(shortMessage, detail, StringComparison.Ordinal);

        /* The span, in the round-trip format the rollup uses. */
        Assert.Contains(Day.AddDays(-1).ToString("o"), detail, StringComparison.Ordinal);
        Assert.Contains(Day.ToString("o"), detail, StringComparison.Ordinal);

        /* Top movers: rank, server, the KEY a page would have carried, the severity, the reason, the fingerprints. */
        Assert.Contains("1. pm-server-2 — Analysis: anomaly [bbbbbbbb] — severity 1.91 — uncorroborated:", detail, StringComparison.Ordinal);
        Assert.Contains("dedup: dk-2a, dk-2b", detail, StringComparison.Ordinal);
        Assert.Contains("2. pm-server-1 — Analysis: anomaly [aaaaaaaa] — severity 1.62", detail, StringComparison.Ordinal);
        Assert.Contains("3. pm-server-1 — Analysis: queries [cccccccc] — severity 1.55", detail, StringComparison.Ordinal);

        /* By family, then server. */
        Assert.Contains("- anomaly: 2 singles across 2 servers", detail, StringComparison.Ordinal);
        Assert.Contains("- queries: 1 singles across 1 servers", detail, StringComparison.Ordinal);
        Assert.Contains("pm-server-1: Analysis: anomaly [aaaaaaaa] 1.62", detail, StringComparison.Ordinal);

        /* Where the full finding lives, and how paging is turned back on: the STORE home first (the Viewer's
           combo or update_alert_settings, live within a sweep — #3712's V137 code half), with darling.json's
           analysis.uncorroboratedRoute named as what the store column defers to when NULL, never as "edit the
           file and restart". */
        Assert.Contains("get_analysis_findings", detail, StringComparison.Ordinal);
        Assert.Contains("notification_type", detail, StringComparison.Ordinal);
        Assert.Contains("update_alert_settings (analysis.uncorroborated_route;", detail, StringComparison.Ordinal);
        Assert.Contains("Settings > Automated Analysis", detail, StringComparison.Ordinal);
        Assert.Contains("analysis.uncorroboratedRoute", detail, StringComparison.Ordinal);
        Assert.Contains("governs only while the column is NULL", detail, StringComparison.Ordinal);
        Assert.DoesNotContain("in darling.json.", detail, StringComparison.Ordinal);

        /* Nothing collapsed, so the collapse sentence is absent. */
        Assert.DoesNotContain("ledger rows collapsed", detail, StringComparison.Ordinal);
    }

    [Fact]
    public void Render_StatesTheCollapse_WhenRowsOutnumberSingles()
    {
        var rows = new[]
        {
            Row(Day.AddHours(-6), 1, "pm-server-1", "anomaly", "aaaaaaaa", 1.62),
            Row(Day.AddHours(-1), 1, "pm-server-1", "anomaly", "aaaaaaaa", 1.71),
        };
        var (_, detail) = DarlingSelfAlertEvaluator.RenderAnalysisSinglesDigest(
            DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(rows), Day.AddDays(-1), Day);

        Assert.Contains("2 ledger rows collapsed to 1 distinct singles", detail, StringComparison.Ordinal);
    }

    /// <summary>The caps state their remainders: a fleet day with more singles than the movers list holds says
    /// how many it did not rank, and a family with more servers than it names says how many it elided.</summary>
    [Fact]
    public void Render_StatesEveryRemainderItsCapsLeaveOut()
    {
        var rows = Enumerable.Range(1, 14)
            .Select(i => Row(Day.AddHours(-1), i, $"pm-server-{i:00}", "anomaly", $"{i:x8}", 1.5 + i / 100.0))
            .ToArray();

        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(rows);
        var (shortMessage, detail) = DarlingSelfAlertEvaluator.RenderAnalysisSinglesDigest(facts, Day.AddDays(-1), Day);

        Assert.StartsWith("14 analysis singles across 14 servers", shortMessage, StringComparison.Ordinal);
        Assert.Contains("Top movers by severity (10 of 14):", detail, StringComparison.Ordinal);
        Assert.Contains("+ 4 more singles below the top 10", detail, StringComparison.Ordinal);
        Assert.Contains("- anomaly: 14 singles across 14 servers", detail, StringComparison.Ordinal);
        Assert.Contains("+ 4 more servers", detail, StringComparison.Ordinal);
        /* The highest severity ranks first — server 14 at 1.64. */
        Assert.Contains("1. pm-server-14 — ", detail, StringComparison.Ordinal);
    }

    /* ---------------- the fire shape and lifecycle ---------------- */

    [Fact]
    public async Task TheDigest_FiresUnderItsOwnMetric_WithNoSeverityOverride_AndTheCountAsItsValue()
    {
        var h = new Harness();
        var e = h.Build();

        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(DarlingSelfAlertEvaluator.AnalysisSinglesDigestMetric, fired.MetricName);
        Assert.Equal("singlesdigest", fired.ServerKey);
        Assert.Equal(DarlingSelfAlertEvaluator.StoreServerLabel, fired.ServerName);
        Assert.Null(fired.Severity);
        Assert.Equal(3, fired.NumericCurrentValue);
        Assert.Equal(0, fired.NumericThresholdValue);
        Assert.Contains("no threshold", fired.ThresholdValue, StringComparison.Ordinal);
        Assert.StartsWith("3 analysis singles across 2 servers", fired.ShortMessage, StringComparison.Ordinal);
        Assert.False(fired.Muted);
    }

    [Fact]
    public async Task TheDigest_DoesNotRepeatInsideItsInterval_AndDoesAfterIt_AndStampsTheStore()
    {
        var h = new Harness();
        var e = h.Build();

        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());
        Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal(Day, h.Stamps.Stamps[PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey]);

        h.Now = Day.Add(DarlingSelfAlertEvaluator.AnalysisSinglesDigestInterval).AddMinutes(-1);
        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());
        Assert.Single(h.Deliverer.Outcomes);

        h.Now = h.Now.AddMinutes(2);
        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    /// <summary>#3580's rule, inherited: a stamp from a PREVIOUS process gates this one, so a restart inside the
    /// interval does not re-announce the day's digest.</summary>
    [Fact]
    public async Task AStampFromAnEarlierProcess_GatesAFreshOne()
    {
        var h = new Harness();
        h.Stamps.Stamps[PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey] = Day.AddHours(-2);
        var e = h.Build();

        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task AFailedDelivery_WritesNoStamp_SoTheNextTickRetries()
    {
        var h = new Harness();
        h.Deliverer.Report = FailedDelivery();
        var e = h.Build();

        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());
        Assert.Single(h.Deliverer.Outcomes);
        Assert.False(h.Stamps.Stamps.ContainsKey(PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey));

        h.Now = Day.AddHours(1);
        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());
        Assert.Equal(2, h.Deliverer.Outcomes.Count);
    }

    /// <summary>The send core's failed shape, built through the one factory that can produce it.</summary>
    private static AlertDelivery FailedDelivery() =>
        AlertDelivery.FromFanout(
            new EmailFanoutResult(AlertChannelOutcome.NotAttempted, null, AlertChannelOutcome.Failed, "Slack: 500", AnyChannelConfigured: true),
            muted: false, trayChannelPresent: false);

    [Fact]
    public async Task AnEmptyDay_PostsNothing_AndDoesNotConsumeTheInterval()
    {
        var h = new Harness();
        var e = h.Build();

        await ApplyAsync(h, e, Array.Empty<AnalysisSinglesDigestReader.DigestRoutedRow>());
        Assert.Empty(h.Deliverer.Outcomes);

        /* The firing twin, same instant. */
        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());
        Assert.Single(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task TheDigest_IsMasterGated()
    {
        var h = new Harness();
        h.Config.Alerts.Enabled = false;
        var e = h.Build();

        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());

        Assert.Empty(h.Deliverer.Outcomes);
    }

    [Fact]
    public async Task TheDigest_HonorsTheSharedMuteSeam()
    {
        var h = new Harness { Muted = true };
        var e = h.Build();

        await ApplyAsync(h, e, ThreeSinglesOnTwoServers());

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.True(fired.Muted);
    }

    /* ---------------- the taxonomy the third document joins ---------------- */

    [Fact]
    public void TheMetric_IsRegisteredEverywhereItsTwoSiblingsAre()
    {
        var metric = DarlingSelfAlertEvaluator.AnalysisSinglesDigestMetric;
        Assert.Equal("Analysis Singles Digest", metric);

        Assert.Equal(AlertFamily.Reports, AlertFamily.Of(metric));
        Assert.True(AlertFamily.IsClassified(metric));
        Assert.True(AlertMetricClassifier.IsInformational(metric));
        Assert.False(AlertMetricClassifier.IsWarning(metric));
        Assert.False(AlertMetricClassifier.IsCritical(metric));
        Assert.False(AlertMetricClassifier.IsResolution(metric));
        Assert.Equal("3", AlertMetricClassifier.FormatHistoryValue(metric, 3));
        Assert.Equal(("info", AlertHistoryRowSeverity.SourceMetricName), AlertHistoryRowSeverity.Describe(metric, null));
    }

    [Fact]
    public void TheReader_SelectsByTheDigestDisposition_NotByName()
    {
        Assert.Contains("notification_type = $3", AnalysisSinglesDigestReader.DigestRoutedSql, StringComparison.Ordinal);
        Assert.Contains("FROM config.config_alert_log", AnalysisSinglesDigestReader.DigestRoutedSql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIKE", AnalysisSinglesDigestReader.DigestRoutedSql, StringComparison.Ordinal);
        Assert.Equal(DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, AnalysisSinglesDigestReader.CommandTimeoutSeconds);
    }

    /// <summary>The page-cooldown seed excludes digest rows through the SAME constant the writers use — the
    /// exclusion is spelled through <see cref="AlertDelivery.ChannelDigest"/>, not a retyped literal.</summary>
    [Fact]
    public void ThePageSeed_ExcludesDigestRows_ThroughTheSharedConstant()
    {
        Assert.Equal("\nAND   notification_type <> 'digest'", PgAlertHistoryStore.DigestExclusionFilter);
        Assert.Contains(AlertDelivery.ChannelDigest, PgAlertHistoryStore.DigestExclusionFilter, StringComparison.Ordinal);
    }

    /* ---------------- #3834: the structured rows beside the prose ---------------- */

    /// <summary>
    /// The singles digest's rows, and why structuring THIS one matters most: an operator promoting a single
    /// to a full drill-down needs the key and the dedup fingerprints by name, and an automation consuming
    /// {{context_json}} was previously left to parse them out of a sentence. So each top-mover row carries
    /// the KEY a page would have carried (the alert-history metric_name, with the story hash
    /// get_analysis_findings resolves), the gate's reason and the fingerprints — as FIELDS, not as
    /// AlertContext.Incidents, because those keys belong to other findings and populating Incidents would
    /// enter a once-a-day report into per-event splitting and the incident delivery filter.
    /// </summary>
    [Fact]
    public async Task TheSinglesDigestFire_CarriesARowPerMover_WithTheKeyAndDedupFingerprints()
    {
        var h = new Harness();
        var rows = ThreeSinglesOnTwoServers();
        await ApplyAsync(h, h.Build(), rows);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.NotNull(fired.Context);
        var context = fired.Context;

        var summary = Assert.Single(context.Details, d => d.Heading == "Singles summary");
        var summaryFields = summary.Fields.ToDictionary(f => f.Label, f => f.Value, StringComparer.Ordinal);
        Assert.Equal("3", summaryFields["Singles"]);
        Assert.Equal("2", summaryFields["Servers"]);

        /* The top mover is the highest severity — the ranking the alert already applied, not re-sorted. */
        var top = context.Details.First(d => d.Heading.StartsWith("#1 ", StringComparison.Ordinal));
        var fields = top.Fields.ToDictionary(f => f.Label, f => f.Value, StringComparer.Ordinal);
        Assert.Equal("pm-server-2", fields["Server"]);
        Assert.Equal("Analysis: anomaly [bbbbbbbb]", fields["Key"]);
        Assert.Equal("1.91", fields["Severity"]);
        Assert.Contains("uncorroborated", fields["Routing reason"], StringComparison.Ordinal);
        Assert.Equal("dk-2a, dk-2b", fields["Dedup keys"]);

        /* The family blocks are rows too, one per family. */
        Assert.Single(context.Details, d => d.Heading == "Family: anomaly");
        Assert.Single(context.Details, d => d.Heading == "Family: queries");

        Assert.Null(context.Incidents);

        /* The prose is the renderer's own output, unchanged, and the delivery gate still passes it. */
        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(rows);
        Assert.Equal(
            DarlingSelfAlertEvaluator.RenderAnalysisSinglesDigest(
                facts, h.Now - DarlingSelfAlertEvaluator.AnalysisSinglesDigestInterval, h.Now).Detail,
            fired.DetailText);
        Assert.NotEqual(AlertDetailText.Flatten(context), fired.DetailText);
        Assert.Equal(fired.DetailText, AlertDetailText.ProseForDelivery(fired.DetailText, context));
    }

    /* ---------------- #7086: the held-alert section and the Daily Digest title ---------------- */

    private static AnalysisSinglesDigestReader.DigestRoutedRow Held(
        DateTime at, int serverId, string server, string metric, string reason)
        => new(at, serverId, server, metric, 5, ContextJson(reason));

    [Fact]
    public async Task ADayWithHeldRowsAndSingles_SendsOneDailyDigest_WithBothSections_AndTheCombinedShortMessage()
    {
        var h = new Harness();
        var rows = new[]
        {
            Held(Day.AddHours(-9), 1, "pm-server-1", "Runnable Queue Warning", "first breach, held for one more sample"),
            Held(Day.AddHours(-6), 1, "pm-server-1", "Runnable Queue Warning", "second hold"),
            Held(Day.AddHours(-2), 1, "pm-server-1", "Runnable Queue Warning", "latest hold"),
            Row(Day.AddHours(-5), 1, "pm-server-1", "anomaly", "aaaaaaaa", 1.62),
            Row(Day.AddHours(-3), 2, "pm-server-2", "anomaly", "bbbbbbbb", 1.91),
        }.OrderBy(r => r.AlertTime).ToArray();

        await ApplyAsync(h, h.Build(), rows);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("3 held alerts and 2 analysis singles across 2 servers", fired.ShortMessage);
        Assert.Equal(5, fired.NumericCurrentValue);

        var detail = fired.DetailText!;
        Assert.Contains("Held back from paging (3 alerts in 1 group):", detail, StringComparison.Ordinal);
        Assert.Contains(
            "- pm-server-1 - Runnable Queue Warning: 3 held, first 2026-09-19 03:00 UTC, last 2026-09-19 10:00 UTC. latest hold",
            detail, StringComparison.Ordinal);
        Assert.DoesNotContain("second hold", detail, StringComparison.Ordinal);
        Assert.Contains("Analysis singles (2 across 2 servers).", detail, StringComparison.Ordinal);
        Assert.Contains("Top movers by severity (2 of 2):", detail, StringComparison.Ordinal);
        /* The held section comes first: it is the part nobody has been told about. */
        Assert.True(detail.IndexOf("Held back from paging", StringComparison.Ordinal)
                    < detail.IndexOf("Analysis singles (", StringComparison.Ordinal));

        /* The structured rows carry both sections too. */
        Assert.Contains(fired.Context!.Details, d => d.Heading == "Held back from paging");
        var group = Assert.Single(fired.Context.Details, d => d.Heading == "pm-server-1 - Runnable Queue Warning");
        Assert.Equal("3", group.Fields.Single(f => f.Label == "Held").Value);
        Assert.Contains(fired.Context.Details, d => d.Heading == "Singles summary");
    }

    [Fact]
    public async Task ADayWithOnlyHeldRows_SendsTheDigest_WithoutASinglesSection()
    {
        var h = new Harness();
        var rows = new[]
        {
            Held(Day.AddHours(-4), 1, "pm-server-1", "Forced Plan Failing", "first failure"),
            Held(Day.AddHours(-1), 1, "pm-server-1", "Forced Plan Failing", "first failure"),
        };

        await ApplyAsync(h, h.Build(), rows);

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("2 held alerts across 1 server", fired.ShortMessage);
        Assert.Equal(2, fired.NumericCurrentValue);
        Assert.Contains("Held back from paging (2 alerts in 1 group):", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Analysis singles", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain("Top movers", fired.DetailText, StringComparison.Ordinal);
        Assert.DoesNotContain(fired.Context!.Details, d => d.Heading == "Singles summary");
        Assert.Equal(Day, h.Stamps.Stamps[PgSelfAlertDeliveryStampStore.AnalysisSinglesDigestStateKey]);
    }

    [Fact]
    public async Task ADayWithNoRowsAtAll_SendsNothing_AndDoesNotConsumeTheInterval()
    {
        var h = new Harness();

        await ApplyAsync(h, h.Build(), Array.Empty<AnalysisSinglesDigestReader.DigestRoutedRow>());

        Assert.Empty(h.Deliverer.Outcomes);
        Assert.Empty(h.Stamps.Stamps);
    }

    [Fact]
    public void HeldRows_GroupByServerAndMetric_MostHeldFirst_WithTheNewestReason()
    {
        var rows = new[]
        {
            Held(Day.AddHours(-10), 1, "pm-server-1", "Poison Wait", "old reason"),
            Held(Day.AddHours(-9), 2, "pm-server-2", "Poison Wait", "other server"),
            Held(Day.AddHours(-8), 1, "pm-server-1", "Poison Wait", "new reason"),
            Held(Day.AddHours(-7), 1, "pm-server-1", "Blocking Detected", "blocking reason"),
            Held(Day.AddHours(-6), 1, "pm-server-1", "Poison Wait", "newest reason"),
        };

        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(rows);

        Assert.Equal(5, facts.Held);
        Assert.Equal(0, facts.Singles);
        Assert.Equal(2, facts.Servers);
        Assert.Equal(3, facts.HeldGroups.Count);
        var first = facts.HeldGroups[0];
        Assert.Equal(("pm-server-1", "Poison Wait", 3), (first.Server, first.Metric, first.Count));
        Assert.Equal(Day.AddHours(-10), first.FirstUtc);
        Assert.Equal(Day.AddHours(-6), first.LastUtc);
        Assert.Equal("newest reason", first.Reason);
        /* The ties (one each) sort by server then metric. */
        Assert.Equal("Blocking Detected", facts.HeldGroups[1].Metric);
        Assert.Equal("pm-server-2", facts.HeldGroups[2].Server);
    }

    [Fact]
    public void AGroupOfOne_ReadsAsOneTime_AndAnEmptyReasonAddsNothing()
    {
        var group = new DarlingSelfAlertEvaluator.HeldAlertGroup(
            "pm-server-1", "Poison Wait", 1, Day, Day, "");

        Assert.Equal("pm-server-1 - Poison Wait: 1 held, at 2026-09-19 12:00 UTC", DarlingSelfAlertEvaluator.HeldGroupLine(group));
    }

    [Fact]
    public void TheHeldList_StatesTheGroupsItsCapLeavesOut()
    {
        var rows = Enumerable.Range(1, 45)
            .Select(i => Held(Day.AddHours(-1), i, $"pm-server-{i:00}", "Poison Wait", "held"))
            .ToArray();

        var facts = DarlingSelfAlertEvaluator.ExtractSinglesDigestFacts(rows);
        var (_, detail) = DarlingSelfAlertEvaluator.RenderAnalysisSinglesDigest(facts, Day.AddDays(-1), Day);

        Assert.Contains("Held back from paging (45 alerts in 45 groups):", detail, StringComparison.Ordinal);
        Assert.Contains("+ 5 more groups, not listed", detail, StringComparison.Ordinal);
    }

    /// <summary>The title changes, the key does not: the generic webhook's metric, the route and mute key and the history
    /// grids all still read "Analysis Singles Digest", while email, Slack and Teams show "Daily Digest".</summary>
    [Fact]
    public async Task TheDigest_ShowsTheDailyDigestTitle_AndKeepsTheMetricNameInTheWebhookPayload()
    {
        var h = new Harness();
        await ApplyAsync(h, h.Build(), new[] { Held(Day.AddHours(-1), 1, "pm-server-1", "Poison Wait", "held") });

        var fired = Assert.Single(h.Deliverer.Outcomes);
        Assert.Equal("Analysis Singles Digest", fired.MetricName);
        Assert.Equal("Daily Digest", fired.DisplayName);

        var branding = new AlertBranding("Test Edition", null);
        var webhook = WebhookAlertService.BuildGenericPayload(
            fired.MetricName, fired.ServerName, fired.CurrentValue, fired.ThresholdValue, branding,
            context: fired.Context, detailText: fired.DetailText);
        using (var document = System.Text.Json.JsonDocument.Parse(webhook))
        {
            Assert.Equal("Analysis Singles Digest", document.RootElement.GetProperty("metric").GetString());
        }

        var slack = WebhookAlertService.BuildSlackPayload(
            fired.MetricName, fired.ServerName, fired.CurrentValue, fired.ThresholdValue, branding,
            context: fired.Context, detailText: fired.DetailText, displayName: fired.DisplayName);
        Assert.Contains("Daily Digest", slack, StringComparison.Ordinal);
        Assert.DoesNotContain("Analysis Singles Digest", slack, StringComparison.Ordinal);

        var (html, _) = EmailTemplateBuilder.BuildAlertEmail(
            fired.MetricName, fired.ServerName, fired.CurrentValue, fired.ThresholdValue, 15, branding,
            fired.Context, fired.DetailText, fired.DisplayName, null);
        Assert.Contains("Daily Digest", html, StringComparison.Ordinal);
        Assert.DoesNotContain("Analysis Singles Digest", html, StringComparison.Ordinal);
    }
}
