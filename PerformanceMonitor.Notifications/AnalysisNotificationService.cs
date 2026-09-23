/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Routes high-severity analysis findings into the notification channels.
/// Filters by severity, dedups per finding (so a recurring finding does not
/// re-notify every analysis cycle), composes a readable message, and hands off
/// to an <see cref="IFindingAlertSender"/> — each app's <c>EmailAlertService</c> —
/// which fans out to email + Slack + Teams and records the alert per that app's
/// history cadence.
///
/// <para>
/// Shared between Lite and Dashboard (Plan E E3c). The per-app divergences are absorbed
/// by two injected dependencies: a <c>serverId</c> resolver (Lite uses the finding's int
/// id as a string; Dashboard resolves the matching <c>ServerConnection</c> GUID and falls
/// back to the int id) and the <see cref="IFindingAlertSender"/> (which owns the per-app
/// record cadence — Approach B, plan §4.6).
/// </para>
///
/// <para><b>#3712: confidence chooses the channel.</b> Crossing the severity floor makes a finding
/// notify-worthy; it does not by itself earn a PAGE. Each notify-worthy incident is routed by
/// <see cref="FindingRouting.Classify(AnalysisFinding, FindingRoute)"/> on its members' corroboration
/// components: an incident with a corroborated member takes the page road (the channels, Lite's tray),
/// an incident of uncorroborated singles takes the digest road — persisted as a finding, recorded in the
/// ledger with the <see cref="AlertContext.Routing"/> decision and <see cref="AlertDelivery.RoutedToDigest"/>,
/// named in the daily digest, and delivered to no channel. The two roads cool in separate key namespaces so
/// a story that later gains corroboration pages as a NEW firing rather than as a repeat of a digest entry.
/// The knob is <see cref="IAlertSettings.UncorroboratedFindingRoute"/>.</para>
/// </summary>
public sealed class AnalysisNotificationService : IDisposable
{
    /// <summary>
    /// How long a PAGE-road incident waits before it is delivered (#3916), so that every page the fleet
    /// raises inside one window is counted together against <see cref="IAlertSettings.AnalysisPageCap"/>:
    /// at or under the cap each incident is its own message, over it they collapse into ONE summary that
    /// names every one. Darling has no fleet-wide cycle — each server's sweep calls
    /// <see cref="NotifyAsync"/> on its own schedule — so a cap without a window could only ever count one
    /// server's incidents; the window is what makes the cap FLEET-wide.
    /// <para><b>Sizing.</b> H must be at least the #1581 cold-start stagger
    /// (<c>DarlingWorker.ColdStartSpreadSeconds</c>, 150 s) plus a margin, so that a restart or install
    /// burst — every server's first sweep, spread over that stagger, each re-paging every story it holds —
    /// lands in ONE window and arrives as one summary rather than dozens of per-server messages. Five
    /// minutes is the stagger plus a full stagger of margin. Pinned against the constant in Darling.Tests.</para>
    /// <para><b>Cost.</b> Every analysis page arrives up to five minutes later than it did. That is the
    /// price, and it is small beside what it is measured against: a re-notify cooldown floored at 30 minutes,
    /// on findings that are themselves the product of 5+ minute analysis passes. No page is lost to the
    /// window — a queued page that the process stops before delivering is dropped UNSTAMPED
    /// (<see cref="Dispose"/>), so nothing holds it and the next run re-attempts it.</para>
    /// <para><b>No severity bypass.</b> A critical page does not skip the window, deliberately:
    /// <see cref="CriticalSeverityCutoff"/> (1.5) equals the production notify threshold, so every
    /// notify-worthy finding on a production store is already critical-band and a critical bypass would
    /// exempt the whole population — the window and the cap would govern nothing.</para>
    /// </summary>
    public static readonly TimeSpan PageHoldBackWindow = TimeSpan.FromMinutes(5);

    private readonly IFindingAlertSender _sender;
    private readonly IAlertSettings _settings;
    private readonly Func<AnalysisFinding, string> _resolveServerId;
    private readonly Func<string, bool>? _isServerSilenced;
    private readonly ILogger<AnalysisNotificationService> _logger;
    private readonly Action<string, string>? _showTrayNotification;
    private readonly Func<int, string, Task<bool>>? _isStoryMuted;
    private readonly TimeSpan _holdBack;

    /// <summary>
    /// The page road's hold-back queue (#3916), keyed by the LEAD's page bucket key: a re-report of a
    /// queued incident inside the window REPLACES its entry rather than adding a second page. Guarded by
    /// <see cref="_pendingLock"/>, with the one-shot <see cref="_flushTimer"/> armed by the first enqueue of
    /// a window. Nothing in the queue is stamped — a queued page is not a delivery; the buckets are stamped
    /// only by <see cref="FlushPendingAsync"/>, with what the flush actually delivered.
    /// </summary>
    private Dictionary<string, PendingPage> _pending = new(StringComparer.Ordinal);
    private readonly object _pendingLock = new();
    private Timer? _flushTimer;
    private bool _disposed;
    private readonly SemaphoreSlim _flushGate = new(1, 1);

    /// <summary>One queued page: the composed alert, its lead, the resolved serverId, and every member with
    /// the bucket key it stamps once the flush delivers.</summary>
    private sealed record PendingPage(
        FindingAlert Alert,
        AnalysisFinding Lead,
        string ServerId,
        IReadOnlyList<(AnalysisFinding Member, string Key)> Members);

    /// <summary>
    /// Per-symptom notification state (escalate-on-CRITICAL keying). A below-critical finding is
    /// keyed by its incident ("{serverId}:{IncidentId}", falling back to its StoryPathHash when it has
    /// no incident id — legacy rows / absolution findings — so those never collapse into one shared
    /// bucket), so co-fired non-critical symptoms stay one-e-mail-per-incident. A CRITICAL-band finding
    /// (severity &gt;= <see cref="CriticalSeverityCutoff"/>) is instead keyed by its OWN StoryPathHash, so
    /// a new critical escalates past that dedup and is never silently held inside an already-alerted
    /// incident. Seeded lazily from the alert log on first lookup per key so a symptom that just fired
    /// stays suppressed across an app restart.
    ///
    /// <para>#2054 fresh-or-worsening: each bucket remembers the severity it last NOTIFIED at and the
    /// last time its story was SEEN at all. A story that keeps firing at the same severity notifies
    /// once and then stays silent — the first weekday on a 52-server fleet showed why: ~46 of 50
    /// alerts were the IDENTICAL ambient chain sitting exactly at the notify threshold, once per
    /// server, drowning the one genuine outlier. Re-notification needs the story to WORSEN
    /// (<see cref="WorseningStep"/>) — the same standing-condition treatment the PVS-pressure alert
    /// shipped with — or to RESOLVE first: a bucket unseen for 2 × AnalysisNotifyCooldownMinutes is
    /// pruned, so a chain that stops firing and later returns is fresh and notifies again. Pruning is
    /// by last-SEEN, deliberately not last-notified: a standing ambient chain refreshes its bucket
    /// every cycle and never ages back into freshness while it persists.</para>
    /// </summary>
    private readonly ConcurrentDictionary<string, BucketState> _cooldowns = new();

    /// <summary>One bucket's memory: when it last notified, at what severity, and when its story was
    /// last SEEN (notified or held — the prune horizon runs on this, so a persisting story cannot age
    /// back into freshness while it keeps firing), and whether that notification was DELIVERED.
    /// <para>#3916: <c>Delivered</c> is what arms the #2054 steady-severity hold. A hold must be earned by a
    /// delivery — a bucket stamped by a send that reached no one throttles re-attempts to one per
    /// cooldown but does not hold the story silent.</para></summary>
    private sealed record BucketState(DateTime LastNotified, double LastNotifiedSeverity, DateTime LastSeen, bool Delivered);

    /// <summary>
    /// How much a story's severity must rise over its last-notified level to re-notify while it keeps
    /// firing (#2054) — the analysis twin of the PVS alert's worsening re-fire. 0.25 on the 0–2 band:
    /// the fleet's ambient chain sat at exactly 1.50 while the one server worth a second look reached
    /// 1.80, so a quarter-band step separates precisely those two on live evidence. A restart-seeded
    /// bucket has no persisted severity and conservatively assumes the notify threshold, so the first
    /// post-restart re-notify needs threshold + step.
    /// </summary>
    private const double WorseningStep = 0.25;

    /// <summary>
    /// Severity at/above which a finding is CRITICAL-band — the canonical <c>&gt;= 1.5</c> cutoff every
    /// recommendation reader bands CRITICAL by (Dashboard <c>RecommendationDeduper.FromEngineSeverity</c>,
    /// Lite <c>LiteRecommendationsReader.SeverityBand</c>, and the Darling viewer's
    /// <c>ViewerDataService.SeverityBand</c>). A finding at/above this escalates past the per-incident
    /// e-mail dedup: it gets its own per-finding re-notify bucket, so a new critical joining an
    /// already-alerted incident e-mails fresh and is never silently held. Below-critical co-fired
    /// symptoms stay deduped under the incident bucket. Not a fresh literal — it keys off the same
    /// value the shared severity bands use (there is no single named constant to import; the readers
    /// each spell the cutoff inline, and this service references only PerformanceMonitor.Analysis).
    /// </summary>
    private const double CriticalSeverityCutoff = 1.5;

    /// <param name="sender">The per-app alert dispatcher (its <c>EmailAlertService</c>).</param>
    /// <param name="settings">Alert settings (severity threshold + cooldown; clamped per app).</param>
    /// <param name="serverIdResolver">
    /// Resolves the persistence <c>serverId</c> for a finding. Lite: <c>f =&gt; f.ServerId.ToString()</c>;
    /// Dashboard: the <c>IServerManager</c> GUID lookup with the int-id fallback.
    /// </param>
    /// <param name="logger">Diagnostic logger.</param>
    /// <param name="isServerSilenced">
    /// Optional predicate (keyed by resolved <c>serverId</c>) that suppresses notifications
    /// for a silenced server. Dashboard passes its <c>AlertStateService.IsAnySilencingActive</c>
    /// so "Silence All Alerts" also stops analysis-finding emails; Lite has no silencing
    /// feature and leaves this null (never silenced).
    /// </param>
    /// <param name="showTrayNotification">
    /// Optional <c>(title, message)</c> sink raised for every finding that notifies — the same
    /// notify-worthy set that reaches email/webhook — so a local-only user with no channel
    /// configured still gets a visible signal. Dashboard wires this to its tray
    /// <c>NotificationService.ShowNotification</c> (which itself honors the global
    /// notifications-enabled pref and marshals to the UI thread); Lite leaves it null. Best-effort:
    /// invoked inside the per-finding try, so a sink fault is logged, not propagated.
    /// </param>
    /// <param name="isStoryMuted">
    /// Optional <c>(serverId, storyPathHash)</c> mute read (#3916), consulted at FLUSH time for every queued
    /// page: a mute rule written inside the hold-back window drops the page, unstamped, exactly as the
    /// pipeline's own mute filter would have dropped the finding had the rule existed a pass earlier. Null
    /// means no re-check (the pipeline's filter is the only one).
    /// </param>
    public AnalysisNotificationService(
        IFindingAlertSender sender,
        IAlertSettings settings,
        Func<AnalysisFinding, string> serverIdResolver,
        ILogger<AnalysisNotificationService> logger,
        Func<string, bool>? isServerSilenced = null,
        Action<string, string>? showTrayNotification = null,
        Func<int, string, Task<bool>>? isStoryMuted = null)
        : this(sender, settings, serverIdResolver, logger, PageHoldBackWindow,
               isServerSilenced, showTrayNotification, isStoryMuted)
    {
    }

    /// <summary>Test seam (#3916): the same service with its hold-back window overridden, so the timer's own
    /// flush can be pinned with a short window.</summary>
    internal AnalysisNotificationService(
        IFindingAlertSender sender,
        IAlertSettings settings,
        Func<AnalysisFinding, string> serverIdResolver,
        ILogger<AnalysisNotificationService> logger,
        TimeSpan holdBack,
        Func<string, bool>? isServerSilenced = null,
        Action<string, string>? showTrayNotification = null,
        Func<int, string, Task<bool>>? isStoryMuted = null)
    {
        _sender = sender;
        _settings = settings;
        _resolveServerId = serverIdResolver;
        _logger = logger;
        _holdBack = holdBack;
        _isServerSilenced = isServerSilenced;
        _showTrayNotification = showTrayNotification;
        _isStoryMuted = isStoryMuted;
    }

    /// <summary>
    /// Notifies at or above the configured severity, collapsing co-fired symptoms to one e-mail per
    /// incident per cycle — EXCEPT a CRITICAL-band symptom (severity &gt;= <see cref="CriticalSeverityCutoff"/>),
    /// which holds its own re-notify bucket and so escalates past the dedup: a new critical joining an
    /// already-alerted incident sends a fresh e-mail naming that critical symptom, while below-critical
    /// co-fired symptoms stay held under the incident bucket. Each cycle sends one e-mail per incident,
    /// led by the highest-severity member whose bucket is not cooling and naming the rest as co-fired.
    /// Never throws.
    /// </summary>
    public async Task NotifyAsync(IReadOnlyList<AnalysisFinding> findings)
    {
        if (findings is null || findings.Count == 0)
            return;

        // Bounds are enforced in the settings adapter (Dashboard clamps AnalysisNotifySeverity
        // to [0, 2] and AnalysisNotifyCooldownMinutes to [30, 10080]; Lite passes through);
        // the service consumes the already-clamped values.
        var threshold = _settings.AnalysisNotifySeverity;
        var cooldown = TimeSpan.FromMinutes(_settings.AnalysisNotifyCooldownMinutes);
        /* #3712: the one knob the routing gate takes — where a lone uncorroborated fact goes. Read once per
           batch like the threshold, so one cycle routes every incident by the same rule. */
        var uncorroboratedRoute = _settings.UncorroboratedFindingRoute;
        var now = DateTime.UtcNow;

        /* Drop entries UNSEEN for 2× cooldown so the dict stays bounded AND a resolved story becomes
           fresh again (#2054): last-SEEN, not last-notified — a standing ambient chain refreshes its
           bucket every cycle it fires and so never ages back into freshness while it persists, while
           a chain that genuinely stopped firing is forgotten and re-alerts on recurrence. If a key
           here also matches an incident in this batch, the per-incident seed below re-adds it from
           history; that's a wash, not a bug. Both key namespaces (page and digest, see BucketKey) age
           out through this one loop. */
        var pruneBefore = now - TimeSpan.FromTicks(cooldown.Ticks * 2);
        foreach (var stale in _cooldowns)
        {
            if (stale.Value.LastSeen < pruneBefore)
                _cooldowns.TryRemove(stale.Key, out _);
        }

        /* Group the notify-worthy findings by incident so co-firing symptoms collapse into ONE e-mail
           per cycle. The group token is the finding's IncidentId, falling back to its StoryPathHash
           when empty — a legacy or absolution finding has no incident id, and without the fallback
           every empty-id finding would collapse into a single bucket. ServerId is part of the key so
           two servers never share a bucket. GroupBy preserves first-seen order, keeping alert ordering
           stable. The severity floor stays UPSTREAM of the routing gate: a finding below it is not
           notify-worthy on any channel, and the gate only ever decides between the channels a
           notify-worthy finding may reach. */
        var incidents = findings
            .Where(f => f is not null && f.Severity >= threshold)
            .GroupBy(f => $"{f.ServerId}:{IncidentToken(f)}");

        foreach (var incident in incidents)
        {
            /* Members ordered so the highest-severity finding leads and the rest are named as co-fired.
               Highest severity wins, root-key- then hash-tiebroken for a deterministic order (mirrors
               IncidentId.Compute's severity-desc, ordinal-root-key tiebreak). */
            var members = incident
                .OrderByDescending(f => f.Severity)
                .ThenBy(f => f.RootFactKey, StringComparer.Ordinal)
                .ThenBy(f => f.StoryPathHash, StringComparer.Ordinal)
                .ToList();

            /* ServerId is identical for every member (it is part of the group key); resolve once. */
            var serverId = _resolveServerId(members[0]);

            /* Honor per-server silencing (Dashboard "Silence All Alerts"). Checked after
               resolving serverId but before the cooldown seed/stamp, so a silenced server
               neither notifies nor consumes its cooldown — unsilencing resumes immediately. */
            if (_isServerSilenced is not null && _isServerSilenced(serverId))
                continue;

            /* #3712: confidence chooses the channel. Every member is classified on its corroboration
               COMPONENTS (a second fact in the chain, a matched co-fire check, or one of the two
               by-construction stories) — never on the confidence scalar — and the incident takes the
               PAGE road when any member earned it, the DIGEST road when none did. An uncorroborated
               single riding in a paging incident is named on the page as co-fired, exactly as before: it
               is accounted for by the corroborated finding it attached to, which is the "true and
               redundant" shape the live batch was made of. */
            var decisions = new Dictionary<AnalysisFinding, FindingRouteDecision>(ReferenceEqualityComparer.Instance);
            foreach (var m in members)
                decisions[m] = FindingRouting.Classify(m, uncorroboratedRoute);

            var anyPage = members.Any(m => decisions[m].Route == FindingRoute.Page);
            var route = anyPage ? FindingRoute.Page : FindingRoute.Digest;

            /* Per-member re-notification bucket (escalate-on-CRITICAL): a CRITICAL-band member
               (severity >= the shared >= 1.5 cutoff) gets its OWN per-finding bucket keyed on its
               StoryPathHash, so a NEW critical symptom escalates past the incident dedup and e-mails
               even when the incident already alerted on another member. A below-critical member stays
               on the shared per-incident bucket, so co-fired non-critical symptoms remain one-e-mail-
               per-incident.

               #3712: the ROUTE is a key component. A digest-routed incident cools in its own namespace
               ("digest|…"), so the day its story gains corroboration — a fact joins the chain (a new
               hash, so a new key on either road) or a co-fire check starts matching (the SAME hash, a
               flipped route) — the page namespace holds no bucket for it and it pages as a NEW firing.
               Escalation is the corroboration event (design point 2); a digest entry must never be the
               cooldown that eats it. The page namespace keeps the pre-#3712 key text byte for byte. */
            string BucketKey(AnalysisFinding f)
            {
                var key = f.Severity >= CriticalSeverityCutoff
                    ? $"{serverId}:{f.StoryPathHash}"
                    : $"{serverId}:{IncidentToken(f)}";
                return route == FindingRoute.Digest ? DigestBucketPrefix + key : key;
            }

            /* Seed each not-yet-known PAGE bucket from the alert log on first lookup so a symptom that
               was DELIVERED shortly before an app restart is not re-fired afterward. The persisted
               equivalent is the latest DELIVERED-page row for that member's metric_name (which embeds the
               finding's hash) — #3916: a hold must be earned by a delivery. The seed used to read the
               latest row regardless of delivery, and a month-old undelivered row (no channel reached
               anyone) held one production story silent for 35 days; a row that reached no one now seeds
               nothing, so the story is heard on its next firing. Seeded buckets are Delivered by
               construction, because the read admits only delivered rows.
               History carries no severity, so the seed conservatively assumes the notify threshold —
               the minimum a notified finding can have had — and the first post-restart re-notify needs
               threshold + WorseningStep (#2054). Below-critical members share one bucket, so only the
               first (highest-severity) below-critical member — the one that would lead a
               below-critical e-mail — is looked up.

               #3712: the store's seed read EXCLUDES digest-routed rows (IAlertHistoryStore.GetLastDeliveredPageUtcAsync,
               every SKU), so a digest entry written before a restart cannot seed the page bucket of the
               escalation that follows it. The DIGEST namespace is deliberately NOT seeded: there is no
               route-filtered seed on the sender seam, and the cost of not having one is bounded and
               channel-free — after a restart a standing single is re-recorded in the ledger once, no
               channel is consulted, and the digest document reads distinct stories over its span rather
               than counting rows. */
            if (route == FindingRoute.Page)
            {
                foreach (var m in members)
                {
                    var seedKey = BucketKey(m);
                    if (_cooldowns.ContainsKey(seedKey))
                        continue;
                    var lastPersisted = await _sender.GetLastDeliveredPageUtcAsync(serverId, FindingMessageFormatter.MetricName(m));
                    if (lastPersisted.HasValue)
                        _cooldowns.TryAdd(seedKey, new BucketState(lastPersisted.Value, threshold, now, Delivered: true));
                }
            }

            /* Lead = the highest-severity member whose bucket allows a notification (#2054
               fresh-or-worsening): a member with NO bucket is fresh and notifies; a member with a
               bucket re-notifies ONLY when it worsened by WorseningStep over the severity it last
               notified at AND its last notification is outside the cooldown (the cooldown keeps a
               rapidly-climbing story from e-mailing every analysis cycle; genuine escalation is
               band-jumping, and a new critical already gets its own fresh hash bucket). Steady
               severity holds forever while the story keeps firing — the whole point: an ambient
               fleet truth notifies once per server, not once per cooldown expiry. The whole incident
               is held only when EVERY member holds (send-if-any-fresh, unchanged).

               #3916: the steady-severity arm applies only to a bucket whose last send was DELIVERED. An
               undelivered story re-attempts once per cooldown (floor 30 min), not every cycle: every
               cycle would write a history row per story per analysis interval on a store with no channel
               configured — a row flood — and once per cooldown is the email family's effective bound.

               #3712: on the page road only a PAGE-routed member may lead — a digest-routed single in a
               paging incident is named as co-fired, never put at the head of a page it did not earn.
               On the digest road every member is digest-routed and any may lead. */
            AnalysisFinding? lead = null;
            foreach (var m in members)
            {
                if (route == FindingRoute.Page && decisions[m].Route != FindingRoute.Page)
                    continue;
                if (_cooldowns.TryGetValue(BucketKey(m), out var state)
                    && (now - state.LastNotified < cooldown || (state.Delivered && m.Severity < state.LastNotifiedSeverity + WorseningStep)))
                    continue;
                lead = m;
                break;
            }

            if (lead is null)
            {
                /* Held entirely — still refresh every member's last-SEEN so a standing story keeps
                   its bucket alive (the prune horizon runs on LastSeen; see the field doc). */
                foreach (var m in members)
                {
                    if (_cooldowns.TryGetValue(BucketKey(m), out var held))
                        _cooldowns[BucketKey(m)] = held with { LastSeen = now };
                }

                continue;
            }

            try
            {
                var context = FindingMessageFormatter.BuildContext(lead, threshold);

                /* #3712: the gate's decision rides the row on BOTH roads — a page says why it earned the
                   channel, a digest entry says why it did not — persisted as the trailing Routing member of
                   the context JSON, the way the fired tier rides for severity_source. Never rendered to a
                   channel: BuildContext's card is unchanged, and the digest road reaches no card. */
                var decision = decisions[lead];
                context.Routing = new AlertRoutingDto(decision.RouteText, decision.Reason);

                /* Name the OTHER findings that co-fired in THIS incident, in the one message, so the
                   single alert still accounts for everything the incident surfaced. Reuses the shared
                   CoFiredSummary the viewer/MCP surfaces use, but with the INCIDENT-scoped lead-in so the
                   body matches the "Co-fired in this incident" heading (the default window wording would
                   be inaccurate here — these siblings are incident-scoped, not the whole run). Null (and
                   so no item) for a lone finding, leaving the message byte-identical to the pre-dedup
                   single-finding path. */
                var coFired = CoFiredSummary.Line(
                    CoFiredSummary.OtherTitles(FindingTitle(lead),
                        members.Select(m => (FindingTitle(m), m.Severity))),
                    leadIn: CoFiredSummary.IncidentLeadIn);
                if (coFired is not null)
                    context.Details.Add(new AlertDetailItem { Heading = "Co-fired in this incident", Body = coFired });

                /* SendFindingAlertAsync fans out to email + Slack + Teams and records the
                   alert per this app's cadence, and returns the AlertDelivery the row recorded
                   (#3916; null = the sender caught). Every send stamps the buckets' cooldown below —
                   that throttles a no-channel store to one re-attempt per cooldown rather than a row
                   per cycle — but only a DELIVERED send arms the #2054 hold. On the digest road
                   (#3712) the sender consults no channel and records the row with the digest
                   disposition; the same call, so the two roads cannot drift in what they persist. */
                var alert = new FindingAlert(
                    FindingMessageFormatter.MetricName(lead),
                    lead.ServerName,
                    FindingMessageFormatter.CurrentValue(lead),
                    threshold.ToString("F1"),
                    serverId,
                    context,
                    lead.Severity,
                    threshold,
                    FindingMessageFormatter.DetailText(lead, threshold),
                    /* The prose is not delivered, and the row still persists it. DetailText and
                       BuildContext's "Diagnosis" item are two renderings of the SAME values — story,
                       severity, notify threshold, confidence, fact count, database, window — so a channel
                       that renders the structured context and the prose prints every one of them twice.
                       They differ in labels ("Facts in chain" vs "Facts", one combined severity line vs
                       two fields) and in the window separator, so the textual equality in
                       AlertDetailText.ProseForDelivery cannot see that they are the same facts; the
                       producer has to say so. Delivery is the only half suppressed: DetailText remains
                       config_alert_log.detail_text, which is the sole copy of these facts on the surfaces
                       that render no structured context, and the input the mute pre-fill parses. */
                    DeliverDetailText: false,
                    Route: route);

                /* #3916: the PAGE road queues. The page waits out the hold-back window with every other page
                   the fleet raises in it, and the flush decides — against the cap — whether it goes as its
                   own message or inside one summary. Keyed by the lead's page bucket key, so a re-report of
                   the same incident inside the window replaces its entry. NOTHING is stamped here: the
                   buckets are stamped by the flush, with what it delivered, and a page dropped at stop
                   leaves no trace to hold it. The tray balloon moved to the flush with the send. */
                if (route == FindingRoute.Page)
                {
                    var keyed = new List<(AnalysisFinding, string)>(members.Count);
                    foreach (var m in members)
                        keyed.Add((m, BucketKey(m)));
                    Enqueue(BucketKey(lead), new PendingPage(alert, lead, serverId, keyed));
                    continue;
                }

                /* The DIGEST road is unchanged and immediate: its delivery is the ledger row, it interrupts
                   no one, and it is not counted against the page cap. */
                var delivery = await _sender.SendFindingAlertAsync(alert);

                /* Stamp EVERY member's bucket (send-if-any-fresh, stamp-all): the e-mail named every
                   member, so none should re-fire until it WORSENS past what was just notified — the
                   just-led critical on its hash bucket, each co-fired critical on its own, and every
                   below-critical member on the shared incident bucket. Members arrive severity-DESC,
                   so the FIRST write to a shared bucket carries its highest member's severity; later
                   (lower) members must not overwrite it downward, or a mid-severity member would
                   spuriously "worsen" past the lowest next cycle. Stamped in the namespace of the road
                   taken (BucketKey), so a digest entry never occupies a page bucket. */
                /* #3916: what the hold is earned by. The page road is delivered when a channel sent
                   (email/webhook) OR a tray sink is wired — a wired sink is Dashboard's toast, raised just
                   above, and Dashboard is the only host that wires one (Lite and Darling wire none, so
                   there only a sent channel counts). The digest road is Delivered: its delivery IS the
                   ledger record — it never reaches a channel by design — and an undelivered digest bucket
                   would re-record a standing single every cooldown. */
                var delivered = route == FindingRoute.Digest
                    || delivery?.Sent == true
                    || _showTrayNotification is not null;

                var stamped = new HashSet<string>(StringComparer.Ordinal);
                foreach (var m in members)
                {
                    var key = BucketKey(m);
                    if (stamped.Add(key))
                        _cooldowns[key] = new BucketState(now, m.Severity, now, delivered);
                }
            }
            catch (Exception ex)
            {
                /* SendFindingAlertAsync is documented never to throw; this guards a
                   formatter defect so one bad incident cannot abort the rest. */
                _logger.LogError(
                    $"AnalysisNotificationService: failed to notify on incident {incident.Key}: {ex.GetType().Name}: {ex.Message}");
            }
        }
    }

    /// <summary>Queues one page for the hold-back window (#3916); the first enqueue of a window arms the
    /// one-shot flush timer. A re-report of a queued key replaces its entry. Dropped after
    /// <see cref="Dispose"/>.</summary>
    private void Enqueue(string key, PendingPage page)
    {
        lock (_pendingLock)
        {
            if (_disposed)
                return;
            _pending[key] = page;
            _flushTimer ??= new Timer(_ => _ = FlushPendingAsync(), null, _holdBack, Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>
    /// Delivers every page queued in the current hold-back window (#3916). Timer-driven in production;
    /// internal so tests flush deterministically. Serialized, and never throws.
    /// <para>Each queued page is re-checked first — the server's silence and the story's mute, either of which
    /// may have been applied inside the window — and a dropped page is NOT stamped. Then, against
    /// <see cref="IAlertSettings.AnalysisPageCap"/>: at or under the cap each page is its own message, stamped
    /// per page by what it delivered; over it, ONE summary names every page and every named member's bucket
    /// is stamped with the summary's delivery — a summary that reached someone earns the #2054 hold for every
    /// story it named, exactly as the individual pages would have. Pages are ordered newest first by analysis
    /// time, then by severity, so a summary reads the freshest incident first.</para>
    /// </summary>
    internal async Task FlushPendingAsync()
    {
        await _flushGate.WaitAsync().ConfigureAwait(false);
        try
        {
            Dictionary<string, PendingPage> batch;
            lock (_pendingLock)
            {
                batch = _pending;
                _pending = new Dictionary<string, PendingPage>(StringComparer.Ordinal);
                _flushTimer?.Dispose();
                _flushTimer = null;
                if (_disposed)
                    return;
            }

            if (batch.Count == 0)
                return;

            var live = new List<PendingPage>(batch.Count);
            foreach (var page in batch.Values)
            {
                if (_isServerSilenced is not null && _isServerSilenced(page.ServerId))
                    continue;
                if (_isStoryMuted is not null && await IsMutedAsync(page).ConfigureAwait(false))
                    continue;
                live.Add(page);
            }

            if (live.Count == 0)
                return;

            live = live
                .OrderByDescending(p => p.Lead.AnalysisTime)
                .ThenByDescending(p => p.Lead.Severity)
                .ToList();

            var cap = Math.Max(1, _settings.AnalysisPageCap);
            if (live.Count <= cap)
            {
                foreach (var page in live)
                {
                    try
                    {
                        var delivery = await _sender.SendFindingAlertAsync(page.Alert).ConfigureAwait(false);

                        /* Always raise the tray balloon for a notify-worthy incident (user choice), the
                           same visible signal threshold alerts already pop — so a local-only user with no
                           email/webhook still sees it. No-op when the host wired no sink (Lite, Darling) or
                           tray notifications are disabled (the sink checks the pref). Page road only. */
                        if (_showTrayNotification is not null)
                        {
                            var (title, message) = FindingMessageFormatter.BalloonText(page.Lead);
                            _showTrayNotification(title, message);
                        }

                        Stamp(page, delivery?.Sent == true || _showTrayNotification is not null, DateTime.UtcNow);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(
                            $"AnalysisNotificationService: failed to deliver a held page for {page.Alert.MetricName} on {page.Alert.ServerName}: {ex.GetType().Name}: {ex.Message}");
                    }
                }

                return;
            }

            /* Over the cap: ONE summary naming every page. The sender records one row per named incident,
               each carrying the summary's delivery, so the restart seed finds every named story. */
            var summaryDelivery = await _sender.SendFindingSummaryAsync(live.Select(p => p.Alert).ToList()).ConfigureAwait(false);
            if (_showTrayNotification is not null)
            {
                var servers = live.Select(p => p.ServerId).Distinct(StringComparer.Ordinal).Count();
                _showTrayNotification(
                    $"Analysis: {live.Count} findings on {servers} server{(servers == 1 ? "" : "s")}",
                    string.Join("; ", live.Take(3).Select(p => FindingMessageFormatter.BalloonText(p.Lead).Title))
                        + (live.Count > 3 ? $"; +{live.Count - 3} more" : ""));
            }

            var delivered = summaryDelivery?.Sent == true || _showTrayNotification is not null;
            var at = DateTime.UtcNow;
            foreach (var page in live)
                Stamp(page, delivered, at);
        }
        catch (Exception ex)
        {
            _logger.LogError($"AnalysisNotificationService: page flush failed: {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            _flushGate.Release();
        }
    }

    /// <summary>The flush's mute re-check. Fails OPEN: a mute read that throws delivers the page, because a
    /// page lost to a read error is worse than one delivered past a rule written inside the window.</summary>
    private async Task<bool> IsMutedAsync(PendingPage page)
    {
        try
        {
            return await _isStoryMuted!(page.Lead.ServerId, page.Lead.StoryPathHash ?? string.Empty).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogError($"AnalysisNotificationService: mute re-check failed for {page.Alert.MetricName}: {ex.GetType().Name}: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Stamps EVERY member's bucket of a delivered-or-attempted page (send-if-any-fresh, stamp-all) — the
    /// page named every member, so none should re-fire until it WORSENS past what was just notified. Members
    /// arrive severity-DESC, so the FIRST write to a shared bucket carries its highest member's severity.
    /// <paramref name="delivered"/> is what arms the #2054 hold (#3916).
    /// </summary>
    private void Stamp(PendingPage page, bool delivered, DateTime at)
    {
        var stamped = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (m, key) in page.Members)
        {
            if (stamped.Add(key))
                _cooldowns[key] = new BucketState(at, m.Severity, at, delivered);
        }
    }

    /// <summary>
    /// Cancels the hold-back timer and DROPS every queued page unstamped and unrecorded (#3916). A page lost
    /// at stop leaves no row and no bucket, so nothing holds its story and the next run re-attempts it.
    /// </summary>
    public void Dispose()
    {
        lock (_pendingLock)
        {
            _disposed = true;
            _pending.Clear();
            _flushTimer?.Dispose();
            _flushTimer = null;
        }
    }

    /// <summary>
    /// The key-namespace prefix a digest-routed incident cools under (#3712). The page namespace has NO
    /// prefix — its keys are the pre-#3712 text byte for byte, so a restart-seeded page bucket and an
    /// in-process one spell the same key — and the pipe cannot occur in a serverId (an int, or a GUID) or
    /// a StoryPathHash (hex), so the two namespaces can never collide.
    /// </summary>
    private const string DigestBucketPrefix = "digest|";

    /// <summary>
    /// The batch/cooldown grouping token for a finding: its incident id, or its StoryPathHash when the
    /// incident id is empty (legacy / absolution findings) so those stay one-bucket-per-finding rather
    /// than collapsing together.
    /// </summary>
    private static string IncidentToken(AnalysisFinding f) =>
        string.IsNullOrEmpty(f.IncidentId) ? f.StoryPathHash : f.IncidentId;

    /// <summary>
    /// A finding's human-readable title for the co-fired cross-reference — its advice headline when the
    /// shared library has one, else its root fact key (finally its category), matching the title the
    /// viewer/MCP co-fired surfaces use.
    /// </summary>
    private static string FindingTitle(AnalysisFinding f) =>
        FactAdvice.GetForFinding(f)?.Headline
            ?? (string.IsNullOrEmpty(f.RootFactKey) ? f.Category : f.RootFactKey);
}

/// <summary>
/// Composes the arguments for an analysis-finding notification. The engine never
/// populates <see cref="AnalysisFinding.StoryText"/>, so the readable message is
/// built here from the finding's structured fields and drill-down detail.
/// </summary>
internal static class FindingMessageFormatter
{
    private const int FieldValueLimit = 300;

    /// <summary>
    /// Alert metric name. The "Analysis: " prefix groups these in the Alerts tab; the
    /// short hash suffix makes each distinct finding unique, so the {serverId}:{metricName}
    /// cooldown cannot collapse two findings sharing a Category.
    /// </summary>
    public static string MetricName(AnalysisFinding finding)
    {
        var hash = finding.StoryPathHash ?? string.Empty;
        var shortHash = hash.Length >= 8 ? hash[..8] : hash;
        var category = string.IsNullOrEmpty(finding.Category) ? "finding" : finding.Category;
        return $"Analysis: {category} [{shortHash}]";
    }

    /// <summary>
    /// Headline value — the root fact and its value, plus baseline context for anomaly findings.
    /// </summary>
    public static string CurrentValue(AnalysisFinding finding)
    {
        var root = string.IsNullOrEmpty(finding.RootFactKey) ? finding.Category : finding.RootFactKey;
        var sb = new StringBuilder(root);

        if (finding.RootFactValue.HasValue)
            sb.Append($" ({finding.RootFactValue.Value:F1})");

        if (finding.RootFactMetadata is { Count: > 0 })
        {
            var baseline = BaselineContextFormatter.FormatBaselineContext(finding.RootFactMetadata);
            if (baseline is { Count: > 0 })
            {
                var parts = baseline.Select(kv => $"{Humanize(kv.Key)} {kv.Value}");
                sb.Append(" — ").Append(string.Join(", ", parts));
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Tray-balloon title + message for a finding (WS2). Concise by design — balloons truncate.
    /// Title names the category + server; message is the headline value (root fact + baseline
    /// context), prefixed with the database when the finding is database-scoped.
    /// </summary>
    public static (string Title, string Message) BalloonText(AnalysisFinding finding)
    {
        var category = string.IsNullOrEmpty(finding.Category) ? "Performance finding" : finding.Category;
        var server = string.IsNullOrEmpty(finding.ServerName) ? "this server" : finding.ServerName;
        var title = $"Analysis: {category} on {server}";

        var message = CurrentValue(finding);
        if (!string.IsNullOrEmpty(finding.DatabaseName))
            message = $"{finding.DatabaseName}: {message}";

        return (title, message);
    }

    /// <summary>
    /// Plain-text detail block — the causal chain and supporting metadata. Used as the
    /// alert's <c>detailText</c> (Lite persists it on every analysis row; Dashboard uses it
    /// on the no-channel "tray" fallback row). Threshold is threaded through as a parameter.
    /// </summary>
    public static string DetailText(AnalysisFinding finding, double notifyThreshold)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"  Story: {finding.StoryPath}");
        sb.AppendLine($"  Severity: {finding.Severity:F2} (notify threshold {notifyThreshold:F1})");
        sb.AppendLine($"  Confidence: {finding.Confidence:F2}");
        sb.AppendLine($"  Facts in chain: {finding.FactCount}");

        if (!string.IsNullOrEmpty(finding.DatabaseName))
            sb.AppendLine($"  Database: {finding.DatabaseName}");

        if (finding.TimeRangeStart.HasValue && finding.TimeRangeEnd.HasValue)
            sb.AppendLine($"  Window: {finding.TimeRangeStart.Value:u} - {finding.TimeRangeEnd.Value:u}");

        return sb.ToString().TrimEnd();
    }

    /// <summary>
    /// Builds the structured <see cref="AlertContext"/> for the alert template.
    /// First detail item is the Diagnosis summary; then the Advice/Remediation block
    /// (when the shared analysis library has one for the finding's root fact-key); then
    /// the finding's drill-down detail flattened into label/value pairs.
    /// <para>#3612: the order is load-bearing on Slack, not just readable. The Slack builder keeps the
    /// longest LEADING run of details that fits its 50-block message budget and drops the rest with a
    /// stated omission, so most-essential-first here is what decides that the finding (Diagnosis, Advice,
    /// the T-SQL pointer) survives and the drill-down's tail and the per-hash incident fingerprints go
    /// first. A five-fact story attaches seven drill-downs and up to fifteen incidents through this method
    /// — 19 to 21 items, 55 to 59 blocks before the budget — and three such pages were lost before it
    /// existed. Anything added here goes AFTER what it is less important than.</para>
    /// </summary>
    public static AlertContext BuildContext(AnalysisFinding finding, double notifyThreshold)
    {
        var context = new AlertContext();

        /* Diagnosis summary — fits inside the 600px email template (label column 120px, value column ~480px). */
        var diagnosis = new AlertDetailItem { Heading = "Diagnosis" };
        diagnosis.Fields.Add(("Story", finding.StoryPath ?? string.Empty));
        diagnosis.Fields.Add(("Severity", finding.Severity.ToString("F2")));
        diagnosis.Fields.Add(("Notify threshold", notifyThreshold.ToString("F1")));
        diagnosis.Fields.Add(("Confidence", finding.Confidence.ToString("F2")));
        diagnosis.Fields.Add(("Facts", finding.FactCount.ToString()));
        if (!string.IsNullOrEmpty(finding.DatabaseName))
            diagnosis.Fields.Add(("Database", finding.DatabaseName));
        if (finding.TimeRangeStart.HasValue && finding.TimeRangeEnd.HasValue)
            diagnosis.Fields.Add(("Window", $"{finding.TimeRangeStart.Value:u} → {finding.TimeRangeEnd.Value:u}"));
        context.Details.Add(diagnosis);

        /* Advice (Investigation/Remediation prose) + generated remediation T-SQL, when the
           shared analysis library has a block for this finding's root fact-key. Inserted
           after Diagnosis and before the drill-down so every surface renders the same order:
           Diagnosis → Advice → Remediation T-SQL → drill-down. */
        var advice = FactAdvice.GetForFinding(finding);
        if (advice is not null)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = advice.Headline,
                Body = $"Investigation: {advice.Investigation}\n\nRemediation: {advice.Remediation}"
            });

            if (!string.IsNullOrEmpty(advice.RemediationTsql))
            {
                context.Details.Add(new AlertDetailItem
                {
                    Heading = "Remediation T-SQL",
                    Body = advice.RemediationTsql,
                    IsCodeBlock = true,
                    /* Structured, typed payload for an in-app Apply (PR-B). Rides in the
                       persisted contextJson; may be null (e.g. PARAMETER_SENSITIVITY has
                       advice + prose but no force action), in which case no Apply affordance
                       is offered. Built from the same drill-down the preview rendered. */
                    Remediation = FactRemediation.BuildAction(finding)
                });
            }
        }

        /* B3 Phase 3 (PR-B): the DESTRUCTIVE "Enable RCSI (advanced)" affordance is a
           SEPARATE detail item from the always-safe DB-config Apply — a distinct view
           with its own singular Remediation (FactKey "RCSI"), so the two Apply buttons
           live on two views and can never cross. Emitted on ANY config_issues-bearing
           finding where RCSI is OFF + the §3.3 enrichment is present (BuildRcsiAction
           returns non-null); it is NOT gated behind the always-safe block's
           advice.RemediationTsql condition (a finding can offer RCSI even when no
           always-safe setting is wrong). The risk-of-not-changing figures are captured
           HERE (the finding is in hand) onto the action so the in-app dialog renders the
           REAL numbers at apply time; the in-app consent gate is what makes it live. */
        var rcsiAction = FactRemediation.BuildRcsiAction(finding);
        if (rcsiAction is not null)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = "Enable RCSI (advanced)",
                Body = FactRemediation.GenerateRcsiPreview(finding),
                IsCodeBlock = true,
                Remediation = rcsiAction
            });

            /* Cross-surface disclosure (§6): the two-sided risk renders as READ-ONLY
               prose on email (both bodies) / webhook (all flow from context.Details) —
               you cannot consent through an email, so there is NO checkbox gate off-app.
               The in-app dialog renders the SAME RiskDisclosure as acknowledge-each-risk
               checkboxes (the only surface that ENFORCES consent). Built from advice.Risks
               (FactAdvice.GetForFinding), which the MCP findings output also reads. */
            var risksBody = RenderRiskDisclosureBody(advice?.Risks);
            if (risksBody is not null)
            {
                context.Details.Add(new AlertDetailItem
                {
                    Heading = "RCSI — risks of changing / not changing",
                    Body = risksBody
                });
            }
        }

        /* Clear-cached-plan (§5/§6, PR-B): the DESTRUCTIVE "Clear cached plan (advanced)"
           affordance is a SEPARATE detail item from the CPU finding's always-safe advice
           — its own view with its own singular Remediation (FactKey "CLEAR_PLAN"), so it
           can never cross the force-plan / DB-config / RCSI affordances (each keys on a
           distinct FactKey). Emitted on a CPU finding (CPU_SQL_PERCENT / CPU_SPIKE) that
           carries an abnormal_cpu_plans drill-down with >= 1 qualifying row (BuildClearPlanAction
           returns non-null); returns null otherwise → NO item. Mirrors the RCSI second-item
           pattern exactly. The risk-of-not-changing figures (the anomaly ratio / per-exec
           CPU / window CPU%) are captured HERE onto the action so the in-app dialog renders
           the REAL numbers at apply time; the in-app consent gate is what makes it live. */
        var clearPlanAction = FactRemediation.BuildClearPlanAction(finding);
        if (clearPlanAction is not null)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = "Clear cached plan (advanced)",
                Body = FactRemediation.GenerateClearPlanPreview(finding),
                IsCodeBlock = true,
                Remediation = clearPlanAction
            });

            /* Cross-surface disclosure (§5): the two-sided CLEAR_PLAN risk renders as
               READ-ONLY prose on email (both bodies) / webhook (all flow from
               context.Details). You cannot consent through an email, so there is NO
               checkbox gate off-app; the in-app dialog renders the SAME RiskDisclosure as
               acknowledge-each-risk checkboxes. Built from advice.Risks (FactAdvice.GetForFinding),
               which the MCP findings output also reads. */
            var clearRisksBody = RenderRiskDisclosureBody(advice?.Risks);
            if (clearRisksBody is not null)
            {
                context.Details.Add(new AlertDetailItem
                {
                    Heading = "Clear cached plan — risks of changing / not changing",
                    Body = clearRisksBody
                });
            }
        }

        /* Drill-down values are anonymous types behind object (a bare object, or a
           List<object> of them). Round-trip through System.Text.Json and walk as
           JsonElement — robust to any shape DrillDownCollector emits. */
        if (finding.DrillDown is { Count: > 0 })
        {
            foreach (var (key, value) in finding.DrillDown)
            {
                if (value is not null && DrillDownItem(key, value) is { } item)
                    context.Details.Add(item);
            }
        }

        /* #1140: derive dedup incidents from the drill-down so this (secondary, anomaly) alert path
           carries the SAME fingerprints as the live "Detected" path. Deadlock -> involved-object set,
           blocking -> contentious object / query pair, query/CPU -> query_hash. Appended after the
           detail items, so the existing Diagnosis->Advice->drill-down order is preserved. */
        AlertIncidentRenderer.Apply(context, BuildIncidents(finding));

        return context;
    }

    /// <summary>
    /// #1140: derives the dedup incidents for the anomaly-finding alert path from the finding's
    /// drill-down, reusing the same shared groupers/fingerprint as the live builders so a deadlock,
    /// blocking chain, or long-running query produces an identical key on either path. Returns an
    /// empty list when the drill-down carries no fingerprintable identity.
    /// </summary>
    private static List<AlertIncident> BuildIncidents(AnalysisFinding finding)
    {
        var result = new List<AlertIncident>();
        if (finding.DrillDown is not { Count: > 0 })
            return result;

        var server = finding.ServerName ?? string.Empty;

        if (TryGetRows(finding.DrillDown, "top_deadlocks", out var deadlockRows))
        {
            var events = deadlockRows.Select(r =>
                new DeadlockIncidentGrouper.DeadlockEvent(SplitObjects(GetField(r, "objects"))));
            result.AddRange(DeadlockIncidentGrouper.Group(server, events).Select(g => g.Incident));
            return result;
        }

        if (TryGetRows(finding.DrillDown, "top_blocking_chains", out var blockingRows))
        {
            var events = blockingRows.Select(r => new BlockingIncidentGrouper.BlockedEvent(
                GetField(r, "database"), GetField(r, "contentious_object"),
                GetField(r, "blocked_sql"), GetField(r, "blocking_sql"), GetLongField(r, "wait_time_ms")));
            result.AddRange(BlockingIncidentGrouper.Group(server, events).Select(g => g.Incident));
            return result;
        }

        /* Query / CPU findings: one incident per distinct query_hash across any drill-down section. */
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (_, value) in finding.DrillDown)
        {
            if (value is null)
                continue;
            foreach (var row in AsRows(value))
            {
                var queryHash = GetField(row, "query_hash");
                if (string.IsNullOrEmpty(queryHash) || !seen.Add(queryHash))
                    continue;
                var db = GetField(row, "database");
                var incident = AlertFingerprint.ForKey(server, AlertFingerprint.Query, queryHash,
                    string.IsNullOrEmpty(db) ? System.Array.Empty<string>() : new[] { db });
                if (incident is not null)
                    result.Add(incident);
            }
        }
        return result;
    }

    private static bool TryGetRows(Dictionary<string, object> drillDown, string key, out List<JsonElement> rows)
    {
        rows = (drillDown.TryGetValue(key, out var value) && value is not null)
            ? AsRows(value)
            : new List<JsonElement>();
        return rows.Count > 0;
    }

    /// <summary>Round-trips a drill-down value (anonymous object or List&lt;object&gt;) through JSON and
    /// returns its object rows — the same robust shape-walk <see cref="FlattenInto"/> relies on.</summary>
    private static List<JsonElement> AsRows(object value)
    {
        var rows = new List<JsonElement>();
        try
        {
            var element = JsonSerializer.SerializeToElement(value);
            if (element.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in element.EnumerateArray())
                    if (item.ValueKind == JsonValueKind.Object)
                        rows.Add(item);
            }
            else if (element.ValueKind == JsonValueKind.Object)
            {
                rows.Add(element);
            }
        }
        catch { /* unexpected shape -> no rows */ }
        return rows;
    }

    private static string GetField(JsonElement row, string name) =>
        row.ValueKind == JsonValueKind.Object && row.TryGetProperty(name, out var p) && p.ValueKind == JsonValueKind.String
            ? p.GetString() ?? string.Empty
            : string.Empty;

    private static long GetLongField(JsonElement row, string name) =>
        row.ValueKind == JsonValueKind.Object && row.TryGetProperty(name, out var p)
            && p.ValueKind == JsonValueKind.Number && p.TryGetInt64(out var v)
            ? v : 0L;

    private static string[] SplitObjects(string joined) =>
        string.IsNullOrWhiteSpace(joined)
            ? System.Array.Empty<string>()
            : joined.Split(", ", StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    /// <summary>
    /// Renders the two-sided <see cref="RiskDisclosure"/> as read-only prose for the
    /// cross-surface (email / webhook) disclosure item (B3 Phase 3, §6). Sections are
    /// separated by blank lines so the email/webhook prose renderer emits one paragraph
    /// per chunk. Returns null when there is nothing to disclose.
    /// </summary>
    private static string? RenderRiskDisclosureBody(RiskDisclosure? risks)
    {
        if (risks is null ||
            (risks.RisksOfChanging.Count == 0 && risks.RisksOfNotChanging.Count == 0))
            return null;

        var sb = new StringBuilder();
        sb.Append("Risks of CHANGING:");
        foreach (var r in risks.RisksOfChanging)
            sb.Append("\n\n• ").Append(r.Text);
        sb.Append("\n\nRisks of NOT changing:");
        foreach (var r in risks.RisksOfNotChanging)
            sb.Append("\n\n• ").Append(r.Text);
        return sb.ToString();
    }

    /// <summary>How many rows of an array-shaped drill-down reach the alert. The collectors write five;
    /// three is what a chat card can carry and still be read as a card.</summary>
    private const int DrillDownRowLimit = 3;

    /// <summary>
    /// Flattens one drill-down value into the item's label/value field pairs. Arrays are capped at
    /// the first <see cref="DrillDownRowLimit"/> elements; nested objects/arrays are rendered as compact JSON.
    /// <para><b>#3644: an array of OBJECTS is also carried as <see cref="AlertDetailItem.Records"/>.</b> The
    /// flat pairs (<c>#1 Database</c>, <c>#1 Query Hash</c>, … <c>#1 Query Text</c>, <c>#2 Database</c>, …)
    /// are right for every surface that lays them out in one column, and wrong for exactly one: Slack's
    /// two-across <c>fields</c> grid, where a seven-attribute record never stays together — #1's text lands
    /// beside #2's hash, #3's database beside #2's SQL — and the reader reconstructs each query by hunting
    /// <c>#N</c> labels across two columns and several screen-heights. Read live on a production High CPU
    /// page during a sustained CPU arc. Fields are for PAIRED scalars; a repeating record is a list of rows,
    /// so each row is ALSO packed as one record: the ordinal, one summary line of its non-empty scalars in
    /// the record's own order (<see cref="IsTextProperty"/> decides which properties are text), and its
    /// text(s) separately, so a renderer can set the summary over the text instead of beside it. The fields
    /// are not changed by a byte — the persisted row, the email table, the in-app grid and every other flat
    /// reader see what they saw — and the records are only populated when every kept element is an object,
    /// so a mixed or scalar array keeps the flat shape alone (a scalar has no record to be).</para>
    /// </summary>
    /// <summary>
    /// One drill-down section as a finding alert's detail item carries it: <see cref="DrillDownHeading"/> over the
    /// section flattened by <see cref="FlattenInto"/>. Null when its shape is unexpected (the entry is skipped and the
    /// rest kept) or it flattens to nothing. Shared with the Darling re-mask of finding alerts stored before #4005
    /// (#4012's review), so a rewritten row holds exactly what this builds from the normalized section.
    /// </summary>
    internal static AlertDetailItem? DrillDownItem(string key, object value)
    {
        var item = new AlertDetailItem { Heading = DrillDownHeading(key) };
        try
        {
            FlattenInto(item, JsonSerializer.SerializeToElement(value));
        }
        catch
        {
            /* Unexpected value shape — skip this drill-down entry, keep the rest. */
            return null;
        }

        return item.Fields.Count > 0 ? item : null;
    }

    /// <summary>The heading a drill-down section's detail item carries: its key, humanized.</summary>
    internal static string DrillDownHeading(string key) => Humanize(key);

    private static void FlattenInto(AlertDetailItem item, JsonElement element)
    {
        var fields = item.Fields;
        switch (element.ValueKind)
        {
            case JsonValueKind.Array:
                var index = 0;
                var allObjects = true;
                foreach (var child in element.EnumerateArray())
                {
                    if (index >= DrillDownRowLimit)
                        break;
                    index++;

                    if (child.ValueKind == JsonValueKind.Object)
                    {
                        foreach (var prop in child.EnumerateObject())
                            fields.Add(($"#{index} {Humanize(prop.Name)}", ScalarText(prop.Value)));
                        item.Records.Add(ToRecord(index, child));
                    }
                    else
                    {
                        allObjects = false;
                        fields.Add(($"#{index}", ScalarText(child)));
                    }
                }

                if (!allObjects)
                    item.Records.Clear();
                break;

            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                    fields.Add((Humanize(prop.Name), ScalarText(prop.Value)));
                break;

            default:
                fields.Add(("value", ScalarText(element)));
                break;
        }
    }

    /// <summary>
    /// One drill-down row as an <see cref="AlertDetailRecord"/> (#3644). The summary is the row's non-empty
    /// scalar properties, in the row's own property order, as <c>Label: value</c> joined by <c>·</c> — the
    /// same humanized label the flat field carries, so a reader moving between surfaces matches them by eye
    /// — with numbers group-separated (<see cref="SummaryNumber"/>), because a summary line is read, where
    /// the flat field's raw digits are copied. Nulls and empty strings are left out of the summary (the
    /// non-AG <c>replica_role</c> is empty on nearly every server, and "Replica Role:" followed by nothing
    /// is noise on a line meant to be scanned); the flat field keeps them. Nested values render as the
    /// compact JSON the flat field renders. Text properties go to <see cref="AlertDetailRecord.Texts"/>
    /// whole — not through <see cref="Truncate"/>: the collectors bound them (LEFT(…, 500)), the email has no
    /// ceiling, and the Slack renderer states its own cut.
    /// </summary>
    private static AlertDetailRecord ToRecord(int ordinal, JsonElement row)
    {
        var summary = new StringBuilder();
        var texts = new List<(string Label, string Text)>();
        foreach (var prop in row.EnumerateObject())
        {
            var label = Humanize(prop.Name);
            if (IsTextProperty(prop.Name) && prop.Value.ValueKind == JsonValueKind.String)
            {
                var text = prop.Value.GetString();
                if (!string.IsNullOrEmpty(text))
                    texts.Add((label, text));
                continue;
            }

            var value = prop.Value.ValueKind switch
            {
                JsonValueKind.Null => string.Empty,
                JsonValueKind.Number => SummaryNumber(prop.Value),
                _ => ScalarText(prop.Value)
            };
            if (value.Length == 0)
                continue;

            if (summary.Length > 0)
                summary.Append(" · ");
            summary.Append(label).Append(": ").Append(value);
        }

        return new AlertDetailRecord(ordinal, summary.ToString(), texts);
    }

    /// <summary>
    /// Whether a drill-down property carries SQL text rather than a scalar (#3644): every text-bearing
    /// property the collectors emit is named for it — <c>query_text</c>, <c>blocked_sql</c> / <c>blocking_sql</c> /
    /// <c>victim_sql</c>, <c>create_statement</c> / <c>alter_statement</c> — so the suffix is the rule, and
    /// a new collector that names its text the same way is grouped correctly without a change here. A
    /// hash (<c>query_hash</c>, <c>query_plan_hash</c>) is a scalar and stays in the summary.
    /// </summary>
    private static bool IsTextProperty(string name) =>
        name.EndsWith("_text", StringComparison.OrdinalIgnoreCase)
        || name.EndsWith("_sql", StringComparison.OrdinalIgnoreCase)
        || name.EndsWith("_statement", StringComparison.OrdinalIgnoreCase)
        || name.Equals("sql", StringComparison.OrdinalIgnoreCase)
        || name.Equals("text", StringComparison.OrdinalIgnoreCase);

    /// <summary>A JSON number for a summary line: integral values with group separators
    /// (<c>3,088,689</c>), fractional ones to at most two decimals (<c>14.2</c>, <c>221.37</c>), invariant
    /// culture. The flat field keeps the raw digits.</summary>
    private static string SummaryNumber(JsonElement number)
    {
        if (number.TryGetInt64(out var whole))
            return whole.ToString("N0", CultureInfo.InvariantCulture);
        if (number.TryGetDouble(out var real))
            return Math.Floor(real) == real && Math.Abs(real) < 1e15
                ? real.ToString("N0", CultureInfo.InvariantCulture)
                : real.ToString("#,##0.##", CultureInfo.InvariantCulture);
        return number.GetRawText();
    }

    /// <summary>Renders a single JSON value as truncated display text.</summary>
    private static string ScalarText(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => Truncate(element.GetString() ?? string.Empty),
            JsonValueKind.Number => element.GetRawText(),
            JsonValueKind.True => "true",
            JsonValueKind.False => "false",
            JsonValueKind.Null => string.Empty,
            // Nested object/array — show compact raw JSON rather than recursing further.
            _ => Truncate(element.GetRawText())
        };
    }

    /// <summary>Turns a snake_case key into spaced Title Case ("top_blocking_chains" -> "Top Blocking Chains").</summary>
    private static string Humanize(string key)
    {
        if (string.IsNullOrEmpty(key))
            return key;

        var words = key.Replace('_', ' ').Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return string.Join(' ', words.Select(w => char.ToUpperInvariant(w[0]) + w[1..]));
    }

    private static string Truncate(string text)
    {
        return text.Length <= FieldValueLimit ? text : text[..FieldValueLimit] + "…";
    }
}
