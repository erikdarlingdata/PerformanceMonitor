/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Confidence chooses the channel (#3712): the routing gate's arms, its persisted read-side twin, the
/// ledger record it writes, and the notification service's two roads — pinned without a store so the
/// whole set runs anywhere the Notifications assembly does.
///
/// <para><b>What the pins hold.</b> (1) Every <see cref="FindingRouting.Classify(AnalysisFinding, FindingRoute)"/>
/// arm, in the order the class remarks list them, each naming its reason from the corroboration COMPONENTS
/// and never from the confidence scalar. (2) <see cref="FindingRouting.ClassifyPersisted"/> agrees with the
/// write-side decision over the whole small-integer catalogue domain, which is the property that lets Lite's
/// grid mark a persisted finding truthfully. (3) The routing record survives the context-JSON round trip and
/// is readable off the row without rehydrating it. (4) <see cref="AnalysisNotificationService.NotifyAsync"/>:
/// an uncorroborated single takes the digest road (sender told <see cref="FindingRoute.Digest"/>, no tray),
/// a corroborated finding pages, a mixed incident is led by a PAGE member even when the single outranks it,
/// and — design point 2 — a single that gains corroboration on the SAME hash pages as a new firing rather
/// than being held by its digest entry.</para>
/// </summary>
public class FindingRoutingTests
{
    /* ---------------- the gate's arms ---------------- */

    [Fact]
    public void Absolution_IsPage_ByConstruction_UnderEitherKnob()
    {
        foreach (var knob in new[] { FindingRoute.Digest, FindingRoute.Page })
        {
            var decision = FindingRouting.Classify(StoryConfidence.AbsolutionRootKey, 1, 0, 0, knob);
            Assert.Equal(FindingRoute.Page, decision.Route);
            Assert.StartsWith("absolution:", decision.Reason, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SameStatementPileup_IsPage_ByConstruction_UnderEitherKnob()
    {
        foreach (var knob in new[] { FindingRoute.Digest, FindingRoute.Page })
        {
            var decision = FindingRouting.Classify(StoryConfidence.PileupRootKey, 1, 0, 0, knob);
            Assert.Equal(FindingRoute.Page, decision.Route);
            Assert.StartsWith("detector-measured:", decision.Reason, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(11)]
    public void TwoOrMoreFactsInTheChain_IsPage_WhateverTheAmplifiersSay(int factCount)
    {
        var decision = FindingRouting.Classify("ANOMALY_CPU_SPIKE", factCount, matchedAmplifiers: 0, definedAmplifiers: 5, FindingRoute.Digest);
        Assert.Equal(FindingRoute.Page, decision.Route);
        Assert.Contains($"{factCount} facts in the chain", decision.Reason, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, 5)]
    [InlineData(3, 5)]
    [InlineData(5, 5)]
    public void ALoneFactWithAMatchedCoFireCheck_IsPage(int matched, int defined)
    {
        var decision = FindingRouting.Classify("ANOMALY_CPU_SPIKE", 1, matched, defined, FindingRoute.Digest);
        Assert.Equal(FindingRoute.Page, decision.Route);
        Assert.Contains($"{matched} of {defined} amplifier checks", decision.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void ALoneFactWithNoMatchedCheck_IsDigest_AndTheReasonNamesTheComponents()
    {
        var withCatalogue = FindingRouting.Classify("ANOMALY_CPU_SPIKE", 1, 0, 5, FindingRoute.Digest);
        Assert.Equal(FindingRoute.Digest, withCatalogue.Route);
        Assert.Contains("0 of 5 co-fire checks matched", withCatalogue.Reason, StringComparison.Ordinal);

        var noCatalogue = FindingRouting.Classify("SCH_M", 1, 0, 0, FindingRoute.Digest);
        Assert.Equal(FindingRoute.Digest, noCatalogue.Route);
        Assert.Contains("no co-fire checks defined", noCatalogue.Reason, StringComparison.Ordinal);

        /* The reason never quotes the confidence scalar — the gate reads components (design point 1). */
        Assert.DoesNotContain("0.20", withCatalogue.Reason, StringComparison.Ordinal);
        Assert.DoesNotContain("confidence", withCatalogue.Reason, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheKnob_MovesOnlyTheUncorroboratedArm()
    {
        var single = FindingRouting.Classify("ANOMALY_CPU_SPIKE", 1, 0, 5, FindingRoute.Page);
        Assert.Equal(FindingRoute.Page, single.Route);
        Assert.Contains("uncorroborated", single.Reason, StringComparison.Ordinal);
        Assert.Contains("analysis.uncorroborated_route is 'page'", single.Reason, StringComparison.Ordinal);

        /* The corroborated arms read identically under either value. */
        Assert.Equal(
            FindingRouting.Classify("X", 2, 0, 0, FindingRoute.Digest),
            FindingRouting.Classify("X", 2, 0, 0, FindingRoute.Page));
        Assert.Equal(
            FindingRouting.Classify("X", 1, 1, 3, FindingRoute.Digest),
            FindingRouting.Classify("X", 1, 1, 3, FindingRoute.Page));
    }

    [Fact]
    public void AFindingThatNeverSetItsFactCount_ReadsAsALoneFact_NotAsZeroFacts()
    {
        var decision = FindingRouting.Classify("X", factCount: 0, matchedAmplifiers: 0, definedAmplifiers: 0, FindingRoute.Digest);
        Assert.Equal(FindingRoute.Digest, decision.Route);
        Assert.Contains("1 fact in the chain", decision.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFindingOverload_ReadsTheFourComponentsOffTheFinding()
    {
        var finding = new AnalysisFinding { RootFactKey = "ANOMALY_CPU_SPIKE", FactCount = 1, MatchedAmplifiers = 1, DefinedAmplifiers = 5 };
        Assert.Equal(FindingRoute.Page, FindingRouting.Classify(finding, FindingRoute.Digest).Route);

        finding.MatchedAmplifiers = 0;
        Assert.Equal(FindingRoute.Digest, FindingRouting.Classify(finding, FindingRoute.Digest).Route);

        Assert.Throws<ArgumentNullException>(() => FindingRouting.Classify(null!, FindingRoute.Digest));
    }

    /* ---------------- spelling ---------------- */

    [Theory]
    [InlineData("page", FindingRoute.Page)]
    [InlineData("  DIGEST ", FindingRoute.Digest)]
    [InlineData("Page", FindingRoute.Page)]
    public void RouteText_RoundTrips_CaseInsensitively(string text, FindingRoute expected)
    {
        Assert.Equal(expected, FindingRouting.TryParseRoute(text));
        Assert.Equal(expected, FindingRouting.TryParseRoute(FindingRouting.RouteText(expected)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    [InlineData("pager")]
    [InlineData("1")]
    public void RouteText_ThatIsNeitherSpelling_ParsesToNoOpinion(string? text)
    {
        Assert.Null(FindingRouting.TryParseRoute(text));
    }

    [Fact]
    public void TheDecisionsRouteText_IsTheLowerCaseName_ForThePersistedRow()
    {
        Assert.Equal("digest", FindingRouting.Classify("X", 1, 0, 0, FindingRoute.Digest).RouteText);
        Assert.Equal("page", FindingRouting.Classify("X", 2, 0, 0, FindingRoute.Digest).RouteText);
        Assert.Equal(FindingRouting.DigestText, FindingRouting.RouteText(FindingRoute.Digest));
        Assert.Equal(FindingRouting.PageText, FindingRouting.RouteText(FindingRoute.Page));
    }

    /* ---------------- the persisted twin ---------------- */

    /// <summary>
    /// The read-side derivation agrees with the write-side decision for every (matched, defined, pathLength)
    /// the traversal can build with a small-integer catalogue — the domain <c>StoryConfidence</c>'s own
    /// no-collision pins enumerate. The write side reads components; the read side re-derives the one
    /// component it lacks from the scalar the components produced; if the two ever disagree, Lite's grid
    /// would mark a paged finding "not paged" or the reverse.
    /// </summary>
    [Fact]
    public void ClassifyPersisted_AgreesWithClassify_OverTheWholeCatalogueDomain()
    {
        for (var pathLength = 1; pathLength <= StoryConfidence.MaxPathNodes; pathLength++)
        {
            for (var defined = 0; defined <= 12; defined++)
            {
                for (var matched = 0; matched <= defined; matched++)
                {
                    var confidence = StoryConfidence.Compute(matched, defined, pathLength);
                    foreach (var knob in new[] { FindingRoute.Digest, FindingRoute.Page })
                    {
                        var written = FindingRouting.Classify("ANOMALY_CPU_SPIKE", pathLength, matched, defined, knob);
                        var read = FindingRouting.ClassifyPersisted("ANOMALY_CPU_SPIKE", confidence, pathLength, knob);
                        Assert.True(written.Route == read.Route,
                            $"path {pathLength}, {matched}/{defined} matched, knob {knob}: wrote {written.Route}, read {read.Route} (confidence {confidence})");
                    }
                }
            }
        }
    }

    [Fact]
    public void ClassifyPersisted_ReadsALegacyPathShapeSingle_AsUncorroborated()
    {
        /* A lone symptom persisted at exactly 1.0 before #3538 — StoryConfidence.DescribeBasis's own reading:
           a path-length statistic, not an evidence score, and a lone-symptom 1.0 means UNCORROBORATED. */
        var decision = FindingRouting.ClassifyPersisted("PAGEIOLATCH_SH", 1.0, 1, FindingRoute.Digest);
        Assert.Equal(FindingRoute.Digest, decision.Route);
        Assert.Contains("pre-#3538", decision.Reason, StringComparison.Ordinal);

        /* The two by-construction 1.0s are named first and never read as legacy. */
        Assert.Equal(FindingRoute.Page, FindingRouting.ClassifyPersisted(StoryConfidence.PileupRootKey, 1.0, 1, FindingRoute.Digest).Route);
        Assert.Equal(FindingRoute.Page, FindingRouting.ClassifyPersisted(StoryConfidence.AbsolutionRootKey, 1.0, 1, FindingRoute.Digest).Route);
    }

    [Fact]
    public void ClassifyPersisted_SaysWhenTheMatchedCheckArm_WasReDerived()
    {
        var decision = FindingRouting.ClassifyPersisted("ANOMALY_CPU_SPIKE", StoryConfidence.Compute(1, 5, 1), 1, FindingRoute.Digest);
        Assert.Equal(FindingRoute.Page, decision.Route);
        Assert.Contains("re-derived from the persisted confidence", decision.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLens_MarksOnlyANotifyWorthyDigestRoutedFinding()
    {
        var lens = new FindingRoutingLens(NotifySeverity: 1.5, UncorroboratedRoute: FindingRoute.Digest);

        var belowFloor = new AnalysisFinding { RootFactKey = "ANOMALY_CPU_SPIKE", Severity = 1.2, Confidence = StoryConfidence.Floor, FactCount = 1 };
        Assert.Null(lens.NotPagedReason(belowFloor));

        var single = new AnalysisFinding { RootFactKey = "ANOMALY_CPU_SPIKE", Severity = 1.7, Confidence = StoryConfidence.Floor, FactCount = 1 };
        var reason = lens.NotPagedReason(single);
        Assert.NotNull(reason);
        Assert.Contains("uncorroborated", reason, StringComparison.Ordinal);

        var chain = new AnalysisFinding { RootFactKey = "ANOMALY_CPU_SPIKE", Severity = 1.7, Confidence = 0.6, FactCount = 2 };
        Assert.Null(lens.NotPagedReason(chain));

        /* Under the page knob nothing is ever marked — everything notify-worthy was delivered. */
        Assert.Null(new FindingRoutingLens(1.5, FindingRoute.Page).NotPagedReason(single));
    }

    /* ---------------- the ledger record ---------------- */

    [Fact]
    public void TheRoutingRecord_RoundTripsThroughTheContextJson_AndReadsOffTheRowAlone()
    {
        var context = new AlertContext();
        context.Details.Add(new AlertDetailItem { Heading = "Diagnosis" });
        context.Routing = new AlertRoutingDto(FindingRouting.DigestText, "uncorroborated: a lone fact — 1 fact in the chain and 0 of 5 co-fire checks matched.");

        var json = AlertContextSerializer.Serialize(context);

        var read = AlertContextSerializer.TryReadRouting(json);
        Assert.NotNull(read);
        Assert.Equal("digest", read!.Route);
        Assert.Contains("0 of 5", read.Reason, StringComparison.Ordinal);

        Assert.True(AlertContextSerializer.TryDeserialize(json, out var rehydrated));
        Assert.Equal(context.Routing, rehydrated.Routing);
    }

    [Fact]
    public void ARowWithoutARoutingRecord_ReadsNull_NotAGuess()
    {
        var engineAlert = new AlertContext();
        engineAlert.Details.Add(new AlertDetailItem { Heading = "Blocking" });
        Assert.Null(AlertContextSerializer.TryReadRouting(AlertContextSerializer.Serialize(engineAlert)));

        /* Pre-#3712 shape, by hand: no Routing property at all. */
        Assert.Null(AlertContextSerializer.TryReadRouting("{\"Details\":[]}"));
        Assert.Null(AlertContextSerializer.TryReadRouting(null));
        Assert.Null(AlertContextSerializer.TryReadRouting("not json"));
        /* A foreign shape under the right name is "no decision", not a decision with a null route. */
        Assert.Null(AlertContextSerializer.TryReadRouting("{\"Details\":[],\"Routing\":{\"Reason\":\"x\"}}"));
    }

    [Fact]
    public void TheDigestDisposition_IsStateCarrying_NeverSent_AndLabelledDigest()
    {
        var delivery = AlertDelivery.RoutedToDigest();
        Assert.False(delivery.Sent);
        Assert.Equal(AlertDelivery.ChannelDigest, delivery.Channel);
        Assert.Null(delivery.SendError);
        Assert.Contains(AlertDelivery.ChannelDigest, AlertDelivery.StateCarryingChannels);
        Assert.DoesNotContain(AlertDelivery.ChannelDigest, AlertDelivery.DeliveringChannels);

        /* Both SKUs' grids read the same word, and a Lite store never says "Shown" for it. */
        Assert.Equal(AlertDeliveryStatus.Digest, AlertDeliveryStatus.Describe(false, AlertDelivery.ChannelDigest, null, producerHadTrayChannel: true));
        Assert.Equal(AlertDeliveryStatus.Digest, AlertDeliveryStatus.Describe(false, AlertDelivery.ChannelDigest, null, producerHadTrayChannel: false));
    }

    /* ---------------- the two roads through NotifyAsync ---------------- */

    private sealed class CapturingSender : IFindingAlertSender
    {
        public List<FindingAlert> Sent { get; } = new();
        public Dictionary<string, DateTime> Seeds { get; } = new(StringComparer.Ordinal);

        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) =>
            Task.FromResult(Seeds.TryGetValue($"{serverId}:{metricName}", out var t) ? (DateTime?)t : null);

        /// <summary>#3916: what the fake reports the row recorded. Null = not delivered.</summary>
        public AlertDelivery? Delivery { get; set; }
        public Task<AlertDelivery?> SendFindingAlertAsync(FindingAlert alert)
        {
            Sent.Add(alert);
            return Task.FromResult<AlertDelivery?>(Delivery);
        }
    }

    private sealed class Settings : IAlertSettings
    {
        public bool SmtpEnabled => false;
        public string SmtpServer => "";
        public int SmtpPort => 587;
        public bool SmtpUseSsl => true;
        public string SmtpUsername => "";
        public string SmtpFromAddress => "";
        public string SmtpRecipients => "";
        public string? GetSmtpPassword() => null;
        public int EmailCooldownMinutes => 15;
        public bool TeamsWebhookEnabled => false;
        public string TeamsWebhookUrl => "";
        public string TeamsProxyAddress => "";
        public bool SlackWebhookEnabled => false;
        public string SlackWebhookUrl => "";
        public string SlackProxyAddress => "";
        public bool GenericWebhookEnabled => false;
        public string GenericWebhookUrl => "";
        public string GenericWebhookHeadersJson => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";
        public bool PagerDutyEnabled => false;
        public string PagerDutyRoutingKey => "";
        public bool PagerDutyUseEuRegion => false;
        public string PagerDutyProxyAddress => "";
        public double AnalysisNotifySeverity { get; init; } = 1.5;
        public int AnalysisNotifyCooldownMinutes { get; init; } = 360;
        public int AnalysisPageCap { get; init; } = 10;
        public string TriageBaseUrl => "";
        public FindingRoute UncorroboratedFindingRoute { get; init; } = FindingRoute.Digest;
    }

    private static AnalysisFinding Finding(
        string hash, double severity, int factCount, int matched, int defined = 5,
        string rootKey = "ANOMALY_CPU_SPIKE", string category = "anomaly", string incidentId = "", int serverId = 7)
        => new()
        {
            ServerId = serverId,
            ServerName = "a-server",
            Category = category,
            StoryPath = factCount >= 2 ? rootKey + " → PLAN_REGRESSION" : rootKey,
            StoryPathHash = hash,
            IncidentId = incidentId,
            Severity = severity,
            Confidence = StoryConfidence.Compute(matched, defined, factCount),
            FactCount = factCount,
            MatchedAmplifiers = matched,
            DefinedAmplifiers = defined,
            RootFactKey = rootKey,
            RootFactValue = 1.0,
            TimeRangeStart = DateTime.UtcNow.AddHours(-4),
            TimeRangeEnd = DateTime.UtcNow,
        };

    private static (AnalysisNotificationService Notifier, CapturingSender Sender, List<(string Title, string Message)> Toasts) Build(Settings? settings = null)
    {
        var sender = new CapturingSender();
        var toasts = new List<(string, string)>();
        var notifier = new AnalysisNotificationService(
            sender, settings ?? new Settings(), f => f.ServerId.ToString(),
            NullLogger<AnalysisNotificationService>.Instance,
            isServerSilenced: null,
            showTrayNotification: (t, m) => toasts.Add((t, m)));
        return (notifier, sender, toasts);
    }

    [Fact]
    public async Task AnUncorroboratedSingle_TakesTheDigestRoad_WithTheReasonOnTheRow_AndNoToast()
    {
        var (notifier, sender, toasts) = Build();

        await notifier.NotifyAsync(new[] { Finding("aaaaaaaa00000001", severity: 1.8, factCount: 1, matched: 0) });

        var alert = Assert.Single(sender.Sent);
        Assert.Equal(FindingRoute.Digest, alert.Route);
        Assert.NotNull(alert.Context.Routing);
        Assert.Equal("digest", alert.Context.Routing!.Route);
        Assert.Contains("0 of 5 co-fire checks matched", alert.Context.Routing.Reason, StringComparison.Ordinal);
        /* Everything a page carries, the digest row carries: the same metric name, severity and detail. */
        Assert.StartsWith("Analysis: anomaly [aaaaaaaa]", alert.MetricName, StringComparison.Ordinal);
        Assert.Equal(1.8, alert.Severity);
        Assert.Contains("Facts in chain: 1", alert.DetailText, StringComparison.Ordinal);
        Assert.Empty(toasts);
    }

    [Fact]
    public async Task ACorroboratedFinding_Pages_WithThePageDecisionOnTheRow_AndToasts()
    {
        var (notifier, sender, toasts) = Build();

        await notifier.NotifyAsync(new[] { Finding("bbbbbbbb00000001", severity: 1.8, factCount: 2, matched: 0) });

        var alert = Assert.Single(sender.Sent);
        Assert.Equal(FindingRoute.Page, alert.Route);
        Assert.Equal("page", alert.Context.Routing!.Route);
        Assert.Contains("2 facts in the chain", alert.Context.Routing.Reason, StringComparison.Ordinal);
        Assert.Single(toasts);
    }

    [Fact]
    public async Task ASingleBelowTheFloor_IsNotRoutedAtAll()
    {
        var (notifier, sender, _) = Build();
        await notifier.NotifyAsync(new[] { Finding("cccccccc00000001", severity: 1.2, factCount: 1, matched: 0) });
        Assert.Empty(sender.Sent);
    }

    [Fact]
    public async Task UnderThePageKnob_ASinglePages_AndTheReasonSaysWhy()
    {
        var (notifier, sender, toasts) = Build(new Settings { UncorroboratedFindingRoute = FindingRoute.Page });

        await notifier.NotifyAsync(new[] { Finding("dddddddd00000001", severity: 1.8, factCount: 1, matched: 0) });

        var alert = Assert.Single(sender.Sent);
        Assert.Equal(FindingRoute.Page, alert.Route);
        Assert.Contains("analysis.uncorroborated_route is 'page'", alert.Context.Routing!.Reason, StringComparison.Ordinal);
        Assert.Single(toasts);
    }

    /// <summary>
    /// A mixed incident pages, led by a PAGE member — even when the uncorroborated single carries the higher
    /// severity — and the single rides on the page as co-fired, exactly the "true and redundant" shape the
    /// live batch was made of. No separate digest row is written for it.
    /// </summary>
    [Fact]
    public async Task AMixedIncident_IsLedByAPageMember_AndNamesTheSingleAsCoFired()
    {
        var (notifier, sender, _) = Build();

        var single = Finding("eeeeeeee00000001", severity: 1.9, factCount: 1, matched: 0, incidentId: "inc-1", category: "anomaly");
        var chain = Finding("eeeeeeee00000002", severity: 1.6, factCount: 2, matched: 0, rootKey: "CPU_SQL_PERCENT", category: "cpu", incidentId: "inc-1");

        await notifier.NotifyAsync(new[] { single, chain });

        var alert = Assert.Single(sender.Sent);
        Assert.Equal(FindingRoute.Page, alert.Route);
        Assert.StartsWith("Analysis: cpu [eeeeeeee", alert.MetricName, StringComparison.Ordinal);
        Assert.Contains(alert.Context.Details, d => d.Heading == "Co-fired in this incident");
    }

    [Fact]
    public async Task AnAllSingleIncident_WritesOneDigestRow_LedByItsHighestSingle()
    {
        var (notifier, sender, _) = Build();

        var a = Finding("ffffffff00000001", severity: 1.9, factCount: 1, matched: 0, incidentId: "inc-2");
        var b = Finding("ffffffff00000002", severity: 1.6, factCount: 1, matched: 0, incidentId: "inc-2");

        await notifier.NotifyAsync(new[] { b, a });

        var alert = Assert.Single(sender.Sent);
        Assert.Equal(FindingRoute.Digest, alert.Route);
        Assert.Equal(1.9, alert.Severity);
    }

    /// <summary>
    /// Design point 2: escalation is the corroboration event. The SAME story hash, digest-routed on cycle 1,
    /// gains a matched co-fire check on cycle 2 at the same severity — no worsening, no cooldown expiry — and
    /// pages as a NEW firing, because the digest route cooled in its own key namespace and left the page
    /// namespace untouched.
    /// </summary>
    [Fact]
    public async Task ASingleThatGainsCorroboration_PagesAsANewFiring_NotHeldByItsDigestEntry()
    {
        var (notifier, sender, toasts) = Build();

        await notifier.NotifyAsync(new[] { Finding("abcdef0100000001", severity: 1.8, factCount: 1, matched: 0) });
        Assert.Equal(FindingRoute.Digest, Assert.Single(sender.Sent).Route);
        Assert.Empty(toasts);

        /* Same cycle again: the digest entry holds (fresh-or-worsening), no second row. */
        await notifier.NotifyAsync(new[] { Finding("abcdef0100000001", severity: 1.8, factCount: 1, matched: 0) });
        Assert.Single(sender.Sent);

        /* The corroboration arrives. Same hash, same severity. */
        await notifier.NotifyAsync(new[] { Finding("abcdef0100000001", severity: 1.8, factCount: 1, matched: 1) });

        Assert.Equal(2, sender.Sent.Count);
        Assert.Equal(FindingRoute.Page, sender.Sent[1].Route);
        Assert.Single(toasts);
    }

    /// <summary>
    /// The restart half of design point 2: the sender's seed answers for the story (as the store would after
    /// a restart) and the page road takes it — but the SEED is the store's, and the store excludes digest
    /// rows; what this pins is that a seeded page bucket holds the page exactly as before (#2054), so the
    /// exclusion in the two stores is load-bearing and not belt-and-braces.
    /// </summary>
    [Fact]
    public async Task ASeededPageBucket_StillHoldsAPage_WhichIsWhyTheStoresExcludeDigestRows()
    {
        var (notifier, sender, _) = Build();
        var finding = Finding("0123456700000001", severity: 1.8, factCount: 2, matched: 0);
        sender.Seeds[$"7:{FindingMessageFormatter.MetricName(finding)}"] = DateTime.UtcNow.AddMinutes(-10);

        await notifier.NotifyAsync(new[] { finding });

        Assert.Empty(sender.Sent);
    }

    [Fact]
    public async Task TheDigestRoad_NeverConsultsTheSeed()
    {
        var (notifier, sender, _) = Build();
        var finding = Finding("7654321000000001", severity: 1.8, factCount: 1, matched: 0);
        /* A seed for this key would be a PAGE row's; the digest road does not read it, so a single is
           recorded once per process regardless of what the store remembers. */
        sender.Seeds[$"7:{FindingMessageFormatter.MetricName(finding)}"] = DateTime.UtcNow.AddMinutes(-10);

        await notifier.NotifyAsync(new[] { finding });

        Assert.Equal(FindingRoute.Digest, Assert.Single(sender.Sent).Route);
    }

    [Fact]
    public void FindingAlert_DefaultsItsRouteToPage_ForCallersThatPredateTheGate()
    {
        var alert = new FindingAlert("m", "s", "v", "t", "1", new AlertContext(), 1.8, 1.5, "d", false);
        Assert.Equal(FindingRoute.Page, alert.Route);
    }

    [Fact]
    public void TheBuildStoryComponents_ArriveOnTheStory_AndTheFinding()
    {
        /* The InferenceEngine reads matched/defined off the root fact's AmplifierResults at BuildStory; both
           finding stores copy them across. This pins the model half without a store: the two members exist
           on both types and default to 0, which the gate reads as "no catalogue, nothing matched". */
        var story = new AnalysisStory();
        Assert.Equal(0, story.MatchedAmplifiers);
        Assert.Equal(0, story.DefinedAmplifiers);
        var finding = new AnalysisFinding();
        Assert.Equal(0, finding.MatchedAmplifiers);
        Assert.Equal(0, finding.DefinedAmplifiers);
    }
}
