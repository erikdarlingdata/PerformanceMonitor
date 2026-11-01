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
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Sends alert notifications to Microsoft Teams, Slack, and/or a generic JSON endpoint via webhooks.
/// Color-coded accent bars match the existing email alert severity mapping.
/// <para>
/// Shared between Lite and Dashboard (Plan E E3c). Per-app branding (edition label +
/// optional snooze hint) arrives via <see cref="AlertBranding"/>; credentials live in
/// each app's settings adapter (<see cref="IAlertSettings"/>) — this service carries no
/// credential storage of its own. The Dashboard-only <c>Current</c> handle and the
/// MCP/health surfaces stay app-side: Dashboard keeps a reference to the injected
/// instance.
/// </para>
/// <para>
/// The generic channel (#1506) is the deliberate answer to "run a script/exe when an alert fires":
/// it drives arbitrary automation (PagerDuty, Opsgenie, n8n, a GitHub <c>repository_dispatch</c> that
/// re-runs a workflow) through an operator-authored JSON body + headers, with no process-execution
/// surface in a signed binary.
/// </para>
/// </summary>
public partial class WebhookAlertService
{
    /* Webhooks do not deliver the copy-paste T-SQL (too large, wrong channel) — they point
       at the email / in-app dialog instead. */
    private const string TsqlWebhookHint = "See email or in-app Alert Details for the copy-paste T-SQL.";
    private static readonly JsonSerializerOptions s_jsonOptions = new() { PropertyNamingPolicy = null };

    /// <summary>
    /// The generic channel's body template when the operator has not authored one — a flat JSON object
    /// exercising every placeholder, which most endpoints (n8n, Zapier, a bare HTTP listener) accept as-is.
    /// </summary>
    public const string DefaultGenericBodyTemplate =
        """
        {
          "severity": "{{severity}}",
          "metric": "{{metric}}",
          "server": "{{server}}",
          "value": "{{value}}",
          "threshold": "{{threshold}}",
          "context": "{{context}}",
          "timestamp": "{{timestamp}}"
        }
        """;

    /* GitHub's REST API 403s any request without a User-Agent, and repository_dispatch is the motivating
       use case (#1506), so the generic channel defaults one instead of handing the operator a 403 they
       have to diagnose. An explicit User-Agent in the headers JSON overrides it. */
    private const string DefaultUserAgent = "PerformanceMonitor";

    /* #1154: per-incident-fingerprint cooldown (was a per-(serverId, metricName)
       ConcurrentDictionary). Keyed per #1140 dedup fingerprint so a distinct incident in the
       window is delivered; falls back to the metric-level key when an alert carries no
       fingerprintable incident. */
    private readonly IncidentCooldown _cooldown;

    /* #3430: the per-metric ceiling on REPEAT posts, so one fault on N servers costs a bounded number of
       cards rather than N per window. One instance per service, which is one per host — the aggregation axis
       is the metric across the whole fleet, so it cannot live anywhere narrower. */
    private readonly RepeatDeliveryBudget _repeatBudget = new();

    private readonly IAlertSettings _settings;
    private readonly AlertBranding _branding;
    private readonly ILogger<WebhookAlertService> _logger;

    /* The clock every payload this service renders stamps itself from. Injectable because the stamp is
       second-precision, and a caller comparing two independently-built payloads for byte-identity (#3330's
       pin, via #3355) can only do so if both render the same instant by construction — on the wall clock,
       a second boundary landing between the two builds is a diff in the value under test. A Func rather
       than a settings member, matching DarlingSelfAlertEvaluator's clock seam. */
    private readonly Func<DateTime> _utcNow;

    private int _consecutiveTeamsFailures;
    private string? _lastTeamsError;
    private int _consecutiveSlackFailures;
    private string? _lastSlackError;
    private int _consecutiveGenericFailures;
    private string? _lastGenericError;
    private int _consecutivePagerDutyFailures;
    private string? _lastPagerDutyError;

    /// <param name="historyStore">
    /// Optional alert-history store used to seed the per-fingerprint webhook cooldown across an app
    /// restart (#1145, mirroring the email seed #981). When null the cooldown is purely in-memory
    /// (the pre-#1145 behavior, seeding disabled) — the test call sites pass null.
    /// </param>
    /// <param name="utcNow">
    /// The clock the rendered payload stamps come from; defaults to <see cref="DateTime.UtcNow"/>. A fixed
    /// clock makes two fan-outs render the same stamp by construction, which is what lets a caller compare
    /// two independently-built payloads byte-for-byte (#3355). It does NOT feed the cooldown: whether an
    /// incident is inside its window is a gating decision on real elapsed time, not a rendered value.
    /// </param>
    public WebhookAlertService(
        IAlertSettings settings,
        AlertBranding branding,
        ILogger<WebhookAlertService> logger,
        IAlertHistoryStore? historyStore = null,
        Func<DateTime>? utcNow = null)
    {
        _settings = settings;
        _branding = branding;
        _logger = logger;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
        _cooldown = new IncidentCooldown(
            keyPrefix: "webhook:",
            // Null store -> null seed delegate -> no restart seeding (preserves the pre-#1145 in-memory path).
            seedLastSentUtc: historyStore is null
                ? null
                : (serverId, metricName, dedupKey) =>
                    historyStore.GetLastWebhookSentUtcAsync(serverId, metricName, dedupKey));
    }

    /* The four per-channel configuration gates, named so the fan-out's own if-conditions and
       AnyWebhookConfigured are the SAME expressions rather than two lists that have to be kept in step.
       Adding a channel and forgetting the disjunction would otherwise report "nothing configured" for a
       deployment that has one. */
    private bool TeamsConfigured =>
        _settings.TeamsWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.TeamsWebhookUrl);

    private bool SlackConfigured =>
        _settings.SlackWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.SlackWebhookUrl);

    private bool GenericConfigured =>
        _settings.GenericWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.GenericWebhookUrl);

    private bool PagerDutyConfigured =>
        _settings.PagerDutyEnabled && !string.IsNullOrWhiteSpace(_settings.PagerDutyRoutingKey);

    /// <summary>
    /// Whether any webhook channel is configured, so a caller can tell "no channel is set up" from "a
    /// channel is set up and this alert did not go out". Answers from configuration only — it never
    /// consults a cooldown and never attempts anything.
    ///
    /// <para>#3598: a route can configure a channel the parent row does not have (a paging key on the
    /// <c>performance</c> route alone is how "only pages page" is spelled), so an enabled route carrying any
    /// webhook destination counts as configured too. Still configuration-only: whether THIS alert's family
    /// resolves to that route is the fan-out's post-cooldown question, and an alert that resolves to nothing
    /// reports <see cref="WebhookFanoutResult.NotAttempted"/> from there.</para>
    /// </summary>
    public bool AnyWebhookConfigured =>
        TeamsConfigured || SlackConfigured || GenericConfigured || PagerDutyConfigured
        || NotificationRouter.AnyRouteConfiguresAWebhook(_settings.NotificationRoutes);

    /// <summary>
    /// Sends webhook alerts to all configured channels (Teams and/or Slack).
    /// Respects the email cooldown setting for throttling. Throws only the caller's own cancellation.
    /// </summary>
    /// <param name="detailText">
    /// #3297: the alert's flat prose detail. Resolved ONCE here, the same way <c>triageUrl</c> below is
    /// and for the same reason — all four channels must carry the same text for the same firing, and a
    /// per-channel resolution would be four places for one of them to drift or be forgotten. #3313 moved
    /// that resolution INTO <see cref="IncidentDeliveryFilter.ForDelivery"/>, which is still this one
    /// place: the prose and the rendered incident set are two halves of one answer, and the prose has to
    /// be resolved against the alert's own context rather than the filtered copy.
    /// </param>
    /// <param name="displayName">
    /// #3303: the human-facing name a custom rule carries, rendered in the Teams/Slack titles and the
    /// PagerDuty summary in place of <paramref name="metricName"/>. Null/empty (every built-in alert)
    /// renders the metric name unchanged. Never reaches the generic channel — see that branch below.
    /// </param>
    /// <param name="deliveryMode">
    /// #3430: the effective <see cref="AlertNotificationMode"/> the caller resolved for this alert's server,
    /// which decides whether the per-metric repeat ceiling applies. <see cref="AlertNotificationMode.Summary"/>
    /// aggregates; <see cref="AlertNotificationMode.PerEvent"/> does not, because that mode exists so
    /// downstream automation gets one message per distinct incident and can count recurrences on the #1140
    /// fingerprint. <c>null</c> — the default, and what the deprecated Dashboard shell and the test call sites
    /// pass — also does not aggregate: a mode nobody stated is not Summary, and the direction that costs a
    /// post is preferable to the direction that costs an announcement.
    /// </param>
    /// <param name="cancellationToken">
    /// #4752: cancels the posts in flight, so a service that is stopping does not wait out an endpoint that
    /// never answers. A cancel from this token is a stop request and not a failed delivery, so it comes out of
    /// this method as the <see cref="OperationCanceledException"/> it is: no channel is recorded
    /// <see cref="AlertChannelOutcome.Failed"/> and no channel's failure count moves, and the deliverer's
    /// shutdown path writes no history row for it. It is the only exception this method throws. A post that
    /// merely times out is different: it ends as that channel's recorded <see cref="AlertChannelOutcome.Failed"/>.
    /// Optional so every caller that has no token to give compiles unchanged.
    /// </param>
    /// <param name="investigationJson">
    /// #5821: the generic channel's <c>{{investigation_json}}</c> value, composed and masked by the caller; null renders
    /// the token empty. No other channel reads it.
    /// </param>
    public async Task<WebhookFanoutResult> TrySendWebhookAlertsAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId = "",
        AlertContext? context = null,
        string? detailText = null,
        string? displayName = null,
        AlertNotificationMode? deliveryMode = null,
        string? investigationJson = null,
        CancellationToken cancellationToken = default)
    {
        /* Answered before the cooldown and the budget, not after. A channel that does not exist cannot be
           throttled or folded, and reporting a suppression for one would put a mechanism on the alert-log
           row for a store that has no webhook at all — #3427's defect in the opposite direction. It also
           spares an SMTP-only store the cooldown's seed query on every alert. */
        if (!AnyWebhookConfigured)
        {
            return WebhookFanoutResult.NotAttempted;
        }

        try
        {
            /* #1154: per-fingerprint cooldown. Post if any incident in this alert is outside its
               window (a distinct fingerprint is not throttled by an unrelated prior incident); stamp
               every candidate key only after a successful post. Seeds the webhook last-sent time from
               the alert log on first touch per key (#1145), unless the store is null (no seeding). No
               incidents -> the metric-level fallback key (today's behavior). WHETHER to post only;
               #3313's filter below decides WHICH incidents the post contains. */
            var window = TimeSpan.FromMinutes(_settings.EmailCooldownMinutes);

            /* #3598 / #5366: ONE snapshot of the webhook settings for this whole delivery, and the route
               resolved from it. Taken here, ahead of the cooldown (the route resolution is pure, so a throttled
               alert resolving it costs nothing), because the cooldown key below depends on whether PagerDuty
               is routed for this alert. Every later read of a webhook setting, the PagerDuty flag included,
               comes from this one snapshot. With an adapter that does not override SnapshotForDelivery (Lite)
               it is the live settings. */
            var delivery = _settings.SnapshotForDelivery();
            var route = NotificationRouter.Resolve(metricName, delivery.NotificationRoutes, delivery, NotificationRouter.ParseServerId(serverId),
                AlertSeverity.RouteLevelOf(metricName, context?.SeverityOverride));

            /* #5469: the PagerDuty auto-resolve behaviour reaches the cooldown only when PagerDuty will
               actually be sent this alert: the flag on AND a destination for this metric. The flag is read
               ONCE, here, into autoResolveFlag. The scope, the seed, both clears, the triage link and the
               PagerDuty send (its dedup key and its event action) all use that one value, so they name one
               key and one action even if the flag is saved while the post is in flight (Lite, which does not
               freeze a snapshot, included). With the flag off, or PagerDuty unset or not routed for the
               metric, every key is the one dev has always used. */
            var autoResolveFlag = delivery.PagerDutyAutoResolve;
            var pagerDutyAutoResolve = autoResolveFlag && route.PagerDuty.Destination is not null;

            /* Each AG replica is its own incident, so its cooldown window is its own too (Teams, Slack and the
               generic webhook share it: one notice per replica per window). Null (not applying, not the AG
               pair, or no identity) is the per-server key of today. The pair's other edge rides along so the
               history seed can tell which incident a stored send belongs to (#5469 restart case). */
            var cooldownScope = PairedReplicaScope(metricName, context, pagerDutyAutoResolve);
            string? pairedEdge = null;
            if (pagerDutyAutoResolve && AlertFamily.TryGetPairedEdge(metricName, out var otherEdge))
            {
                pairedEdge = otherEdge;
            }

            var decision = await _cooldown.EvaluateAsync(
                serverId, metricName, context?.Incidents, window, cooldownScope, pairedEdge,
                bypassCooldown: context?.EscalationBypassesCooldown == true /* #5641 */);

            if (!decision.ShouldSend)
            {
                return WebhookFanoutResult.Throttled;
            }

            /* #3430: the cooldown bounds posts per FINGERPRINT, and AlertFingerprint hashes the server name
               into every fingerprint, so one fault on N servers is N fingerprints and N posts per window. The
               budget bounds REPEATS per metric across the whole fleet instead. A first notice is exempt and
               posts here unchanged; a repeat that finds the metric's window already spent is folded onto the
               metric's roster and named by the next carrier's card, never dropped. Evaluated on the
               cooldown's own instant so the two decisions cannot disagree about "now". */
            var budget = _repeatBudget.Evaluate(
                metricName, serverName, decision, window,
                aggregateRepeats: deliveryMode == AlertNotificationMode.Summary,
                incidents: context?.Incidents,
                serverKey: serverId);

            if (!budget.ShouldSend)
            {
                /* Debug, not Information: a fold happens on every sweep of every co-affected server, which is
                   the volume this issue is about. The aggregate worth a log line is the carrier's roster size
                   below, which happens once per window. */
                _logger.LogDebug(
                    "Webhook post for {Metric} on {Server} folded into the metric's roster ({Entries} entr(ies) pending)",
                    metricName, serverName, budget.RosterEntryCount);
                return WebhookFanoutResult.Folded;
            }

            bool sent = false;

            /* Whether any channel was reached at all, and the first one that came back unsuccessful. The
               four gates below are the same expressions AnyWebhookConfigured is built from, so the early
               return above already guarantees at least one attempt — this is measured rather than inferred
               from that equality, because a fifth channel added to one list and not the other would
               otherwise make "attempted and failed" the answer for a channel that was never reached. */
            bool attempted = false;
            string? firstError = null;

            /* #6718: the {{dedup_key}} the generic post rendered, handed back so the deliverer closes that key. */
            string? genericDedupKey = null;

            /* The dedup_key the PagerDuty trigger carried, handed back for the same reason: the resolve closes that key. */
            string? pagerDutyDedupKey = null;

            /* #4750: what each channel's send did, keyed by the channel name, so the history row's route record
               can say which channel delivered and which failed. `sent` above answers only "did any channel
               get through", so a channel that failed beside one that delivered was visible nowhere but a
               counter no one reads. The outcome only: the error text is not kept here, because it can name
               the endpoint's URL, which is a secret. */
            var channelOutcomes = new Dictionary<string, AlertChannelOutcome>();

            /* The channel NAME is kept with its error. Four channels report into one string, so "500 Internal
               Server Error" without it names no endpoint an operator could go and fix. */
            void Record(string channel, string? error)
            {
                channelOutcomes[channel] = error is null ? AlertChannelOutcome.Delivered : AlertChannelOutcome.Failed;

                if (error is null)
                {
                    sent = true;
                }
                else
                {
                    firstError ??= $"{channel}: {error}";
                }
            }

            /* #3313: the decision above says the alert posts; this says WHICH of its incidents the card
               contains. Rendering all of them re-delivered fingerprints that were still inside their own
               window and had gone out minutes earlier, riding along on whichever sibling was fresh. Applied
               ONCE for the whole fan-out, like triageUrl below and for the same reason: four channels
               describing the same firing must not disagree about what it covers. Resolves the prose too,
               against the UNFILTERED context — see IncidentDeliveryFilter for why that basis is the only
               correct one. #3297's null-when-redundant behaviour is unchanged. #3430's roster rides through
               the same call for the same reason, and because that function owns the delivery-scoped copy. */
            var render = IncidentDeliveryFilter.ForDelivery(
                context, detailText, decision.DeliverableDedupKeys, budget.Roster);
            var renderContext = render.Context;
            var prose = render.Prose;

            /* #3355: the firing's instant, read ONCE for the whole fan-out and threaded to every builder,
               for the reason triageUrl below and the render above give — four channels describing the same
               firing must not disagree about WHEN it fired, and a per-builder read makes that a race with
               the second hand. It also feeds triageUrl, so the link's key and the cards' stamps name one
               instant. Rendered stamps only: the cooldown decides on real elapsed time and reads its own
               clock, because "is this fingerprint still inside its window" is not a display concern. */
            var nowUtc = _utcNow();

            /* #2710: the triage-page link, computed ONCE for the whole fan-out so all four channels carry
               the SAME URL for the same firing. Keyed by (server, metric, now, dedup key) rather than an
               alert-history id, because the history row is written AFTER delivery — the page resolves the
               row on read. Null (base URL unset/invalid) means every channel omits the link; delivery is
               never gated on it. The dedup key uses the same serverId-else-serverName identity the generic
               channel's {{dedup_key}} token uses, and it reads the RENDERED incidents, so the anchor names an
               incident the card actually shows. The link and PagerDuty follow the auto-resolve flag and so
               correlate with each other; the generic {{dedup_key}} token does NOT follow it (#5469), so under
               auto-resolve a paired edge's link and PagerDuty key are the pair's, the token is per state. */
            var triageUrl = TriageLink.Build(
                _settings.TriageBaseUrl, serverName, metricName, nowUtc,
                DerivePagerDutyDedupKey(
                    string.IsNullOrEmpty(serverId) ? serverName : serverId, metricName, renderContext, autoResolveFlag));

            /* #3598: WHERE each channel posts, resolved ONCE for the whole fan-out (see the snapshot and the route
               at the top of this method: the cooldown key needs the PagerDuty answer, and the resolution is pure
               so resolving it before the cooldown changes nothing the cooldown or budget decide) — so one
               firing is one delivery decision regardless of where it lands. Exact-metric route, then family route,
               then the parent row's own destination, per channel; with zero routes every destination below
               IS the settings member the four gates above read, so the fan-out is the pre-routes one byte
               for byte. A channel that resolves to nothing is not attempted, exactly as an unconfigured one
               was not. The decision rides the result so the deliverer can record it on the history row. */
            /* #5366: the routing decision and every channel's URL, headers, body template and proxy below
               read from the delivery snapshot taken above, so a saved setting that changes while the posts are
               in flight cannot send a later channel somewhere the decision did not name. */
            if (route.Teams.Destination is { } teamsUrl)
            {
                attempted = true;
                Record(NotificationRouter.TeamsChannel, await TrySendTeamsAlertAsync(delivery, teamsUrl, metricName, serverName, currentValue, thresholdValue, renderContext, triageUrl, prose, nowUtc, displayName, cancellationToken));
            }

            if (route.Slack.Destination is { } slackUrl)
            {
                attempted = true;
                Record(NotificationRouter.SlackChannel, await TrySendSlackAlertAsync(delivery, slackUrl, metricName, serverName, currentValue, thresholdValue, renderContext, triageUrl, prose, nowUtc, displayName, cancellationToken));
            }

            if (route.Generic.Destination is { } genericUrl)
            {
                /* Generic webhook: the payload's "metric" field is a machine key an automation correlates on,
                   so it stays the immutable metric name — the display name is a human-title concern only, and
                   this channel has no title. The prose detail DOES go, because it is alert content. */
                attempted = true;
                /* #6718: the key is derived once, here, from the filtered context the post renders, and the post is handed
                   this value. The deliverer records the same value, so a resolution closes the key the receiver opened even
                   when the filter dropped the alert's first incident (the key anchors on the first incident it keeps). */
                genericDedupKey = DeriveGenericDedupKey(serverId, serverName, metricName, renderContext);
                Record(NotificationRouter.GenericChannel, await TrySendGenericAlertAsync(delivery, genericUrl, metricName, serverName, currentValue, thresholdValue, serverId, renderContext, triageUrl, prose, nowUtc, investigationJson, GenericSigningFor(delivery, route.Generic.RouteId, genericUrl), dedupKeyOverride: genericDedupKey, cancellationToken: cancellationToken));
            }

            if (route.PagerDuty.Destination is { } pagerDutyKey)
            {
                attempted = true;
                /* The trigger's key is derived once, here, with the arguments the send always used (the filtered context and
                   the flag read above, so the paired-edge and per-replica rules apply as before), and the send is handed this
                   value. The deliverer records it, so the resolve closes the incident this trigger opened even when the
                   filter dropped the alert's first incident. */
                pagerDutyDedupKey = DerivePagerDutyDedupKey(serverId, metricName, renderContext, autoResolveFlag);
                var pagerDutyError = await TrySendPagerDutyAlertAsync(delivery, autoResolveFlag, pagerDutyKey, metricName, serverName, currentValue, thresholdValue, serverId, renderContext, triageUrl, prose, nowUtc, displayName, dedupKeyOverride: pagerDutyDedupKey, cancellationToken: cancellationToken);
                Record(NotificationRouter.PagerDutyChannel, pagerDutyError);

                /* With auto-resolve on, a DELIVERED close re-opens the lifecycle: the cooldown entry the
                   pair's FIRING left behind describes an incident PagerDuty just closed, so without the
                   clear the NEXT firing replays against that stamp and never posts (and the seeded
                   history row keeps the pattern alive across a restart until the entry lapses). The clear
                   runs only here — a resolve that actually posts — not on throttle, fold, or error paths,
                   and it names exactly the pair's firing metric, so a failed close never re-arms an outage
                   whose incident may still be open in PagerDuty. */
                if (pagerDutyError is null
                    && pagerDutyAutoResolve
                    && AlertFamily.RecoveryPairs.TryGetValue(metricName, out var clearedFiring))
                {
                    /* The AG pair's clear is per REPLICA (#5469 part 2): the firing and recovery keys carry
                       the replica identity under this same flag, so one replica's reconnect forgets only
                       its own firing entry and a sibling that is still down keeps its window. The server
                       pair, and an AG edge without the identity, clear the per-server key as before. */
                    _cooldown.ClearMetric(serverId, clearedFiring, cooldownScope);
                }

                /* #5469: the other half of the same re-arm. The recovery stamps ITS OWN key on its first send,
                   and the clear above names only the firing's key, so a second outage that recovers inside
                   the window met the first recovery's stamp: its resolve was throttled and PagerDuty's
                   second incident stayed open. A DELIVERED firing of the pair, under the same condition as
                   the clear above, therefore forgets its closing edge's entry — the new incident has not
                   been closed yet, so the next recovery is a first notice again. Like the clear above it names
                   the replica's own key for the AG pair (cooldownScope), so a sibling replica's recovery
                   window is left alone. */
                if (pagerDutyError is null
                    && pagerDutyAutoResolve
                    && AlertFamily.TryGetRecoveryOf(metricName, out var recoveryOfFiring))
                {
                    _cooldown.ClearMetric(serverId, recoveryOfFiring, cooldownScope);
                }
            }

            if (sent)
            {
                _cooldown.Stamp(decision);

                /* #3430: clear only the roster entries this card named, so anything folded while the four
                   posts were in flight is named by the next carrier instead of being committed away by this
                   one. Logged at Information because it is the once-per-window aggregate — it states how
                   many servers this single post stood in for. */
                _repeatBudget.Commit(budget);
                if (budget.RosterEntryCount > 0)
                {
                    _logger.LogInformation(
                        "Webhook post for {Metric} on {Server} carried {Entries} already-reported incident(s) from other servers",
                        metricName, serverName, budget.RosterEntryCount);
                }
            }
            else
            {
                /* Nothing was delivered, so this post never named the roster and never spent the metric's
                   window — the same rule the cooldown applies by stamping only on success. Belt and braces
                   with the reservation's own expiry: whichever of the two runs, the next repeat can post. */
                _repeatBudget.Release(budget);
            }

            return sent ? WebhookFanoutResult.Delivered with { Route = route, ChannelOutcomes = channelOutcomes, GenericDedupKey = genericDedupKey, PagerDutyDedupKey = pagerDutyDedupKey }
                : attempted ? WebhookFanoutResult.Failed(firstError) with { Route = route, ChannelOutcomes = channelOutcomes, GenericDedupKey = genericDedupKey, PagerDutyDedupKey = pagerDutyDedupKey }
                : WebhookFanoutResult.NotAttempted with { Route = route };
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* #4752: a stop request from the caller, whether it lands in a post or in the cooldown's seed
               query, is not a failed delivery. It leaves here as the cancellation it is, so the deliverer's
               shutdown path writes no history row for a delivery the service abandoned. Nothing is stamped
               or committed for it either: the cooldown and the roster treat the alert as not sent. */
            throw;
        }
        catch (Exception ex)
        {
            /* Reached by the cooldown's seed query, the roster build or the triage-link derivation — before
               any post. It is still a failure of this alert's delivery and not a suppression, and it is the
               one route to a `failed` row that no per-channel health counter records, so the message is the
               only place the reason survives. */
            _logger.LogError($"TrySendWebhookAlertsAsync outer error: {ex.Message}");
            return WebhookFanoutResult.Failed($"webhook fan-out: {ex.Message}");
        }
    }

    /// <summary>
    /// Sends a test notification to Microsoft Teams. Returns null on success, error message on failure.
    /// </summary>
    public static async Task<string?> SendTestTeamsAsync(string webhookUrl, string? proxyAddress, AlertBranding branding)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(webhookUrl))
                return "Teams webhook URL is not configured.";

            var payload = BuildTeamsPayload("Test Notification", "", "Webhook configuration verified", "", branding, isTest: true);
            return await PostWebhookAsync(webhookUrl, payload, proxyAddress, bodyInResult: true);
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    /// <summary>
    /// Sends a test notification to Slack. Returns null on success, error message on failure.
    /// </summary>
    public static async Task<string?> SendTestSlackAsync(string webhookUrl, string? proxyAddress, AlertBranding branding)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(webhookUrl))
                return "Slack webhook URL is not configured.";

            var payload = BuildSlackPayload("Test Notification", "", "Webhook configuration verified", "", branding, isTest: true);
            return await PostWebhookAsync(webhookUrl, payload, proxyAddress, bodyInResult: true);
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    /// <summary>
    /// Sends a test notification to the generic endpoint. Returns null on success, error message on failure.
    /// Surfaces a bad template / bad headers JSON as that error message, so the operator sees the problem in
    /// the settings window instead of only in a log after the next real alert.
    /// </summary>
    public static async Task<string?> SendTestGenericAsync(
        string webhookUrl,
        string? headersJson,
        string? bodyTemplate,
        string? proxyAddress,
        AlertBranding branding,
        WebhookSigningKeys? signing = null)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(webhookUrl))
                return "Webhook URL is not configured.";

            if (!TryParseHeaders(headersJson, out var headers, out var headerError))
                return headerError;

            /* Same stand-in context as Save-time validation, for the same reason: the Test button must
               exercise the raw tokens with quote-bearing structure or a mis-quoted one test-sends clean. */
            var payload = BuildGenericPayload(
                "Test Notification", "", "Webhook configuration verified", "", branding,
                isTest: true, bodyTemplate: bodyTemplate, context: ValidationStandInContext());

            if (!IsWellFormedJson(payload, out var bodyError))
                return bodyError;

            /* #6012: the sender signs its own test when it holds a key (Lite, which is the sender); Darling's Viewer passes none and
               sends the test unsigned, as before. */
            if (signing is { HasKey: true })
            {
                var sendUtc = DateTime.UtcNow;
                var signed = new WebhookSendSigning(signing.LiveKeys(sendUtc), WebhookSigning.NewMessageId(), WebhookSigning.UnixSeconds(sendUtc));
                return await PostWebhookAsync(webhookUrl, payload, proxyAddress, headers, WebhookPostTimeout, signed, CancellationToken.None, bodyInResult: true);
            }

            return await PostWebhookAsync(webhookUrl, payload, proxyAddress, headers, bodyInResult: true);
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    public (int ConsecutiveFailures, string? LastError) GetTeamsHealth() =>
        (_consecutiveTeamsFailures, _lastTeamsError);

    public (int ConsecutiveFailures, string? LastError) GetSlackHealth() =>
        (_consecutiveSlackFailures, _lastSlackError);

    public (int ConsecutiveFailures, string? LastError) GetGenericHealth() =>
        (_consecutiveGenericFailures, _lastGenericError);

    public (int ConsecutiveFailures, string? LastError) GetPagerDutyHealth() =>
        (_consecutivePagerDutyFailures, _lastPagerDutyError);

    /// <summary>
    /// The failures in a row at which a webhook channel counts as failing (#4750). It is the count the
    /// channels' own logging already turns on: each channel logs its first this-many failures in a row at
    /// Error and after that only every 50th, so a channel that reaches it has gone quiet in the log while it
    /// is still failing. Darling's "Notification Channel Failing" alert and Lite's tray notice both read
    /// this constant, so the point where the log goes quiet and the point where the signal speaks cannot
    /// drift apart.
    /// </summary>
    public const int FailingChannelThreshold = 3;

    /// <summary>
    /// Every channel's failures in a row (#4750), for the hosts' "a channel keeps failing" signal: a channel
    /// can fail for weeks while another one delivers every alert, and until this the counts were read only by
    /// tests. The COUNT only, deliberately not the last error: a webhook error can carry the endpoint's URL,
    /// and a Slack or Teams webhook URL is the credential, so nothing built from this can leak it. A channel
    /// that never failed, or that delivered since its last failure, reads 0. Names are the
    /// <see cref="NotificationRouter"/> channel names.
    ///
    /// <para><b>A channel with no destination left.</b> Turning a failing channel off is the expected response
    /// to the signal, and a channel with nothing to send to can neither fail nor deliver, so its count would sit
    /// at its last value until the process restarted and the signal would never close. Each count therefore
    /// says whether the channel is still <see cref="WebhookChannelFailureCount.Configured"/>: the parent's
    /// settings carry a destination, or an enabled route does
    /// (<see cref="NotificationRouter.AnyRouteConfiguresWebhookChannel"/>; Lite's routes count too since #5750, read
    /// from the cache, which the dispatcher-thread status timer reaches without loading). For a channel with none, this returns that channel's CURRENT count with Configured
    /// false, so the host can close what it announced, and THEN resets the count to 0 and clears the last
    /// error, as a delivery does. A channel that is turned back on starts from zero, and does not raise the
    /// signal at once on a count from before it was turned off. So the read is not idempotent for a channel
    /// with no destination: the second read of it returns 0.</para>
    /// </summary>
    public IReadOnlyList<WebhookChannelFailureCount> GetChannelFailureCounts() => new[]
    {
        ChannelFailureCount(NotificationRouter.TeamsChannel, TeamsConfigured, ref _consecutiveTeamsFailures, ref _lastTeamsError),
        ChannelFailureCount(NotificationRouter.SlackChannel, SlackConfigured, ref _consecutiveSlackFailures, ref _lastSlackError),
        ChannelFailureCount(NotificationRouter.GenericChannel, GenericConfigured, ref _consecutiveGenericFailures, ref _lastGenericError),
        ChannelFailureCount(NotificationRouter.PagerDutyChannel, PagerDutyConfigured, ref _consecutivePagerDutyFailures, ref _lastPagerDutyError),
    };

    /// <summary>One channel's entry for <see cref="GetChannelFailureCounts"/>: a configured channel reports its
    /// count; one with no destination reports its current count as unconfigured and clears its tallies.</summary>
    private WebhookChannelFailureCount ChannelFailureCount(
        string channel, bool settingsConfigured, ref int failures, ref string? lastError)
    {
        var configured = settingsConfigured
            || NotificationRouter.AnyRouteConfiguresWebhookChannel(_settings.NotificationRoutes, channel);
        if (configured)
        {
            return new WebhookChannelFailureCount(channel, Volatile.Read(ref failures), Configured: true);
        }

        var count = Interlocked.Exchange(ref failures, 0);
        lastError = null;
        return new WebhookChannelFailureCount(channel, count, Configured: false);
    }
}

/// <summary>
/// What <see cref="WebhookAlertService.TrySendWebhookAlertsAsync"/> did, in the
/// <see cref="AlertChannelOutcome"/> vocabulary, with the first failing channel's error text.
///
/// <para><b>Why not a bool.</b> One bool for four channels made a failed post indistinguishable from a
/// working cooldown in the alert log: both arrived as false with no error, and
/// <see cref="AlertDelivery.FromFanout"/> had nothing to label them apart with. The error is the FIRST
/// failure only, prefixed with its channel name — four channels and one string, so the alternative is a
/// concatenation nobody reads, and one named endpoint is more actionable than four unnamed ones. Each
/// channel's full failure history stays on its own consecutive-failure counter and health getter.</para>
/// </summary>
/// <param name="Outcome">What happened, over the whole fan-out.</param>
/// <param name="SendError">
/// The first failing channel's error, prefixed with that channel's name. Non-null only on
/// <see cref="AlertChannelOutcome.Failed"/>; a throttled or folded fan-out attempted nothing and so has
/// nothing to report.
/// </param>
/// <param name="Route">
/// #3598: where the fan-out resolved each channel to, for the deliverer to record on the history row. Set
/// only when resolution happened — delivered, failed, or resolved-to-nothing — and null on the outcomes
/// that never got that far (throttled, folded, nothing configured), which is the ledger's "no destination
/// was consulted". Trailing and defaulted so every existing construction and pin compiles unchanged.
/// </param>
/// <param name="ChannelOutcomes">
/// #4750: what each channel's send did, keyed by the <see cref="NotificationRouter"/> channel names, for the
/// deliverer to record beside <paramref name="Route"/>. Set on the two outcomes that attempted a channel
/// (delivered, or failed on every channel it tried) and null everywhere else. It holds the outcome only, never
/// the error text: that text can name the endpoint's URL, and <paramref name="SendError"/> stays the one place
/// the first failure's reason is written. Trailing and defaulted like <paramref name="Route"/>.
/// </param>
/// <param name="GenericDedupKey">
/// #6718: the <c>{{dedup_key}}</c> the generic webhook's post rendered, from the #3313 delivery-filtered context. Set
/// when the generic channel was attempted, null otherwise. The deliverer records it so the alert's resolution renders
/// exactly this key. Trailing and defaulted like <paramref name="Route"/>.
/// </param>
/// <param name="PagerDutyDedupKey">
/// The <c>dedup_key</c> the PagerDuty trigger carried, from the same filtered context and auto-resolve flag the trigger used.
/// Set when PagerDuty was attempted, null otherwise. One trigger per fire, so one key. The deliverer records it so the
/// resolve closes exactly the incident this trigger opened. Trailing and defaulted like <paramref name="Route"/>.
/// </param>
public readonly record struct WebhookFanoutResult(
    AlertChannelOutcome Outcome,
    string? SendError,
    NotificationRouteDecision? Route = null,
    IReadOnlyDictionary<string, AlertChannelOutcome>? ChannelOutcomes = null,
    string? GenericDedupKey = null,
    string? PagerDutyDedupKey = null)
{
    /// <summary>Whether a channel delivered. At least one did; the rest may have failed, and each of those
    /// is on its own health counter.</summary>
    public bool Sent => Outcome == AlertChannelOutcome.Delivered;

    /// <summary>No webhook channel was consulted: none is configured, or the caller suppressed the whole
    /// fan-out.</summary>
    public static WebhookFanoutResult NotAttempted { get; } = new(AlertChannelOutcome.NotAttempted, null);

    /// <summary>A cooldown window was still open, so nothing was attempted.</summary>
    public static WebhookFanoutResult Throttled { get; } = new(AlertChannelOutcome.Throttled, null);

    /// <summary>The metric's per-fleet repeat budget folded this delivery onto another server's.</summary>
    public static WebhookFanoutResult Folded { get; } = new(AlertChannelOutcome.Folded, null);

    /// <summary>Delivered on at least one channel.</summary>
    public static WebhookFanoutResult Delivered { get; } = new(AlertChannelOutcome.Delivered, null);

    /// <summary>Attempted, and nothing delivered.</summary>
    public static WebhookFanoutResult Failed(string? error) => new(AlertChannelOutcome.Failed, error);
}
