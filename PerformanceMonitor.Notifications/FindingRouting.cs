/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The channel an analysis finding is routed to (#3712).
/// </summary>
public enum FindingRoute
{
    /// <summary>The paging channels — email, Teams, Slack, PagerDuty, the generic webhook — and Lite's tray.
    /// Earned by corroboration; see <see cref="FindingRouting.Classify"/>.</summary>
    Page = 0,

    /// <summary>The daily digest and the web/MCP surfaces only. Nothing interrupts; nothing is suppressed —
    /// the finding is persisted, visible on every read surface the instant it fires, and recorded in the
    /// alert-history ledger with this route and the reason, so "why didn't this page" is answerable from the
    /// row.</summary>
    Digest = 1,
}

/// <summary>
/// The one routing decision for a finding: where it goes and why, in words an operator can read off the
/// ledger row. <see cref="Reason"/> is a single sentence naming the corroboration components the decision
/// read, so a re-weighting of the confidence formula cannot change what the row says happened.
/// </summary>
/// <param name="Route">Where the finding goes.</param>
/// <param name="Reason">Why, naming the components — never the confidence scalar.</param>
public sealed record FindingRouteDecision(FindingRoute Route, string Reason)
{
    /// <summary>The persisted spelling of <see cref="Route"/> (<c>page</c> / <c>digest</c>): the enum's name
    /// lower-cased, spelled through one method so the ledger, the MCP read and the digest reader agree on the
    /// text without each holding a copy. A NAME rather than an ordinal for the reason <c>Severity</c> is
    /// persisted by name — the column outlives any build.</summary>
    public string RouteText => FindingRouting.RouteText(Route);
}

/// <summary>
/// The read-side view of the routing gate (#3712) for a surface that renders PERSISTED findings and wants
/// to say which of them the gate kept off the paging channels — Lite's Recommendations grid marks those
/// "not paged — uncorroborated". Carries the two settings the write-side decision took, so the marker and
/// the decision read the same knobs: a finding below <see cref="NotifySeverity"/> was never a paging
/// candidate and gets NO marker (it was not routed at all), one at or above it is classified through
/// <see cref="FindingRouting.ClassifyPersisted"/>.
/// </summary>
/// <param name="NotifySeverity">The notify floor the write path applied (<c>IAlertSettings.AnalysisNotifySeverity</c>).</param>
/// <param name="UncorroboratedRoute">The knob the write path applied (<c>IAlertSettings.UncorroboratedFindingRoute</c>).</param>
public sealed record FindingRoutingLens(double NotifySeverity, FindingRoute UncorroboratedRoute)
{
    /// <summary>
    /// The reason a persisted finding did NOT page, or null when it paged or was never a paging candidate
    /// (below the notify floor). Built from the persisted fields only — the root key, the confidence and the
    /// chain length — through <see cref="FindingRouting.ClassifyPersisted"/>, whose reason says when the
    /// matched-check arm was re-derived from the scalar.
    /// </summary>
    public string? NotPagedReason(AnalysisFinding finding)
    {
        if (finding is null)
            throw new ArgumentNullException(nameof(finding));

        if (finding.Severity < NotifySeverity)
            return null;

        var decision = FindingRouting.ClassifyPersisted(finding.RootFactKey, finding.Confidence, finding.FactCount, UncorroboratedRoute);
        return decision.Route == FindingRoute.Digest ? decision.Reason : null;
    }
}

/// <summary>
/// Confidence chooses the channel (#3712): the pure routing gate both SKUs' notification paths consult
/// before a finding reaches a paging channel.
///
/// <para><b>The defect.</b> Confidence has measured corroboration since #3538 A6 — but every finding that
/// crossed the severity floor still took the same road to the same channel. The first day the recalibrated
/// engine ran on a large production fleet: forty-plus anomaly pages in seven hours, severity 1.5–1.9,
/// confidence 0.20–0.35, interleaved with the handful that needed a human, and the operator's own reading of
/// the channel was "no idea what to focus on". That is alert fatigue as a mechanism — not wrong alerts but
/// UNRANKED ones, where the cost lands on the true positives that stop being seen. The low-confidence pages
/// were largely true and redundant: anomaly stories attached to a CPU flap already paging through the CPU
/// family, to a server inside its own maintenance job, to a burst the platform alert had already delivered.
/// Uncorroborated single-fact stories are, empirically, mostly echoes.</para>
///
/// <para><b>The gate reads the corroboration COMPONENTS, not the confidence scalar</b> (design point 1). A
/// finding pages when any of these holds, and the reason names which:</para>
/// <list type="bullet">
///   <item><description><b>Two or more facts in the chain</b> (<see cref="AnalysisFinding.FactCount"/> ≥ 2):
///   each traversed edge is a fired predicate onto a fact that itself scored.</description></item>
///   <item><description><b>A matched co-fire check</b> (<see cref="AnalysisFinding.MatchedAmplifiers"/> ≥ 1):
///   every amplifier the scorer defines is a predicate over ANOTHER fact — a sibling anomaly fired in the same
///   window, an absolute fact at its concerning bar, a wait significant — so one match is one corroborating
///   fact the engine went looking for and found.</description></item>
///   <item><description><b>The two by-construction 1.0 stories</b>, named by root key: the absolution story
///   (every fact scored, none rooted — never notify-worthy anyway, but never a "lone fact" either) and the
///   same-statement pileup, which is a directly observed convoy against its own baseline, not a single
///   z-score excursion.</description></item>
/// </list>
/// <para>Everything else is a lone fact with no corroboration and routes to the digest. The extremity escape
/// (<c>FactScorer.IsExtremeAnomaly</c>, #3526) keeps its SEVERITY power — an extreme single still clears the
/// notify floor and still leads its story — but its published intent, extreme AND corroborated, is now the
/// enforced bar for a page: the scorer writes no marker for extremity onto the fact or the finding, so this
/// gate cannot see it and does not try to; an extremity-escaped single with no matched check is still a
/// single and lands in the digest with its severity intact.</para>
///
/// <para><b>Why components and not the scalar.</b> The scalar is <c>0.20 + 0.48 × share + 0.32 × depth</c>
/// today, and the inverse is exact: a one-fact story above 0.20 has a matched check. It stops being exact
/// the day a weight moves, and a paging bar that silently moves with a formula constant is what design point
/// 1 forbids. <see cref="ClassifyPersisted"/> is the one place the inverse IS taken — on a row read back from
/// the store, where the components were never persisted — and it says so in its reason.</para>
///
/// <para><b>The knob.</b> <see cref="UncorroboratedFindingRoute"/> is the one master setting: <c>digest</c>
/// (shipped) routes uncorroborated findings to the digest, <c>page</c> restores the pre-#3712 behaviour where
/// every notify-worthy finding pages. It does not touch the corroborated arms — a corroborated finding pages
/// under either value.</para>
/// </summary>
public static class FindingRouting
{
    /// <summary>The persisted spelling of <see cref="FindingRoute.Page"/>.</summary>
    public const string PageText = "page";

    /// <summary>The persisted spelling of <see cref="FindingRoute.Digest"/>.</summary>
    public const string DigestText = "digest";

    /// <summary>The persisted / wire spelling of a route — see <see cref="FindingRouteDecision.RouteText"/>.</summary>
    public static string RouteText(FindingRoute route) => route == FindingRoute.Digest ? DigestText : PageText;

    /// <summary>The persisted / wire spelling as an extension, for a settings writer that has to read as
    /// <c>root["…"] = App.X.…</c> — Lite's <c>AlertSettingsControlWiringTests</c> census keys the "every assigned
    /// static is persisted" pin on that shape, so the conversion hangs off the value rather than wrapping it.</summary>
    public static string ToWireText(this FindingRoute route) => RouteText(route);

    /// <summary>
    /// Parses a route's persisted or configured spelling (<c>page</c> / <c>digest</c>, case-insensitive,
    /// trimmed). Null for anything else, so a hand-edited value neither throws nor silently becomes a page.
    /// </summary>
    public static FindingRoute? TryParseRoute(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return null;

        var trimmed = text.Trim();
        if (string.Equals(trimmed, PageText, StringComparison.OrdinalIgnoreCase))
            return FindingRoute.Page;
        if (string.Equals(trimmed, DigestText, StringComparison.OrdinalIgnoreCase))
            return FindingRoute.Digest;
        return null;
    }

    /// <summary>
    /// Routes a finding produced by THIS run — one whose corroboration components are in hand
    /// (<see cref="AnalysisFinding.FactCount"/>, <see cref="AnalysisFinding.MatchedAmplifiers"/>,
    /// <see cref="AnalysisFinding.DefinedAmplifiers"/>, <see cref="AnalysisFinding.RootFactKey"/>). Pure: no
    /// clock, no I/O, no settings beyond <paramref name="uncorroboratedRoute"/>.
    /// </summary>
    /// <param name="finding">The finding, with the write-path components populated.</param>
    /// <param name="uncorroboratedRoute">Where a lone uncorroborated fact goes — the operator's knob. A
    /// corroborated finding pages regardless.</param>
    public static FindingRouteDecision Classify(AnalysisFinding finding, FindingRoute uncorroboratedRoute)
    {
        if (finding is null)
            throw new ArgumentNullException(nameof(finding));

        return Classify(
            finding.RootFactKey, finding.FactCount, finding.MatchedAmplifiers, finding.DefinedAmplifiers,
            uncorroboratedRoute);
    }

    /// <summary>
    /// The pure arithmetic behind <see cref="Classify(AnalysisFinding, FindingRoute)"/>, exposed so every arm
    /// can be pinned without building a finding. The arms are tested in the order they are listed in the class
    /// remarks; the first that holds names the reason.
    /// </summary>
    public static FindingRouteDecision Classify(
        string? rootFactKey, int factCount, int matchedAmplifiers, int definedAmplifiers,
        FindingRoute uncorroboratedRoute)
    {
        if (string.Equals(rootFactKey, StoryConfidence.AbsolutionRootKey, StringComparison.Ordinal))
            return new FindingRouteDecision(FindingRoute.Page,
                "absolution: every fact was scored and none rooted a finding — 1.0 by construction, not a lone fact.");

        if (string.Equals(rootFactKey, StoryConfidence.PileupRootKey, StringComparison.Ordinal))
            return new FindingRouteDecision(FindingRoute.Page,
                "detector-measured: the same-statement pileup is a directly observed convoy against its own baseline — 1.0 by construction, not a lone fact.");

        if (factCount >= 2)
            return new FindingRouteDecision(FindingRoute.Page,
                $"corroborated: {factCount.ToString(CultureInfo.InvariantCulture)} facts in the chain — each traversed edge is a fired predicate onto a fact that itself scored.");

        if (matchedAmplifiers >= 1)
            return new FindingRouteDecision(FindingRoute.Page,
                $"corroborated: a co-fire check matched ({matchedAmplifiers.ToString(CultureInfo.InvariantCulture)} of {definedAmplifiers.ToString(CultureInfo.InvariantCulture)} amplifier checks on the root fact) — another fact the engine went looking for was there.");

        /* A story has at least its root; a factCount below 1 is a caller that never set it, and the sentence
           should still be true of a lone fact rather than claim "0 facts". */
        var chain = Math.Max(1, factCount).ToString(CultureInfo.InvariantCulture);
        var uncorroborated = definedAmplifiers > 0
            ? $"uncorroborated: a lone fact — {chain} fact in the chain and 0 of {definedAmplifiers.ToString(CultureInfo.InvariantCulture)} co-fire checks matched."
            : $"uncorroborated: a lone fact — {chain} fact in the chain and no co-fire checks defined for its root.";

        return uncorroboratedRoute == FindingRoute.Page
            ? new FindingRouteDecision(FindingRoute.Page, uncorroborated + " Paged because analysis.uncorroborated_route is 'page'.")
            : new FindingRouteDecision(FindingRoute.Digest, uncorroborated);
    }

    /// <summary>
    /// Routes a finding READ BACK from the store, where the corroboration components were never persisted
    /// (there is no column for them and no rung window to add one — #3712 keeps the ledger context JSON as
    /// the routing record). The chain length and the root key survive the round-trip and answer three of the
    /// four arms exactly; the matched-check arm is the one that has to be RE-DERIVED from the scalar, which is
    /// exact today because a one-fact story earns confidence above <see cref="StoryConfidence.Floor"/> only
    /// through a matched check — and the reason says the derivation was taken, so a reader knows this is the
    /// read-side twin and not the write-side decision. A legacy path-shape row (a lone symptom persisted at
    /// exactly 1.0 before #3538) is UNCORROBORATED by <see cref="StoryConfidence.DescribeBasis"/>'s own
    /// reading and routes to the digest here for the same reason.
    /// <para>For DISPLAY (Lite's Recommendations grid marking a finding as "not paged — uncorroborated"),
    /// never for the paging decision itself, which <see cref="Classify(AnalysisFinding, FindingRoute)"/>
    /// makes on the write path with the components in hand.</para>
    /// </summary>
    public static FindingRouteDecision ClassifyPersisted(
        string? rootFactKey, double confidence, int factCount, FindingRoute uncorroboratedRoute)
    {
        if (string.Equals(rootFactKey, StoryConfidence.AbsolutionRootKey, StringComparison.Ordinal)
            || string.Equals(rootFactKey, StoryConfidence.PileupRootKey, StringComparison.Ordinal)
            || factCount >= 2)
        {
            /* Three arms the persisted row answers exactly; the components are the same ones Classify reads. */
            return Classify(rootFactKey, factCount, matchedAmplifiers: 0, definedAmplifiers: 0, uncorroboratedRoute);
        }

        if (StoryConfidence.IsLegacyPathShape(confidence, factCount))
        {
            return uncorroboratedRoute == FindingRoute.Page
                ? new FindingRouteDecision(FindingRoute.Page, "uncorroborated: a lone symptom persisted at the pre-#3538 path-shape 1.0, which is not an evidence score. Paged because analysis.uncorroborated_route is 'page'.")
                : new FindingRouteDecision(FindingRoute.Digest, "uncorroborated: a lone symptom persisted at the pre-#3538 path-shape 1.0, which is not an evidence score.");
        }

        /* The one re-derived arm. A one-fact story's path-depth term is exactly 0, so anything above the
           floor came from the amplifier share, i.e. from at least one matched check. 1e-9 is the same
           tolerance StoryConfidence uses for its own exact comparisons. */
        if (confidence > StoryConfidence.Floor + 1e-9)
        {
            return new FindingRouteDecision(FindingRoute.Page,
                "corroborated: a co-fire check matched — re-derived from the persisted confidence, which a one-fact story earns above the 0.20 floor only through the amplifier share.");
        }

        return uncorroboratedRoute == FindingRoute.Page
            ? new FindingRouteDecision(FindingRoute.Page, "uncorroborated: a lone fact at the 0.20 floor — no co-fire check matched. Paged because analysis.uncorroborated_route is 'page'.")
            : new FindingRouteDecision(FindingRoute.Digest, "uncorroborated: a lone fact at the 0.20 floor — no co-fire check matched.");
    }
}
