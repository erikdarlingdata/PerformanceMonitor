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
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
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
public class WebhookAlertService
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
    /// Respects the email cooldown setting for throttling. Never throws.
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
    public async Task<WebhookFanoutResult> TrySendWebhookAlertsAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId = "",
        AlertContext? context = null,
        string? detailText = null,
        string? displayName = null,
        AlertNotificationMode? deliveryMode = null)
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
            var decision = await _cooldown.EvaluateAsync(
                serverId, metricName, context?.Incidents, window);

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
                incidents: context?.Incidents);

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

            /* The channel NAME is kept with its error. Four channels report into one string, so "500 Internal
               Server Error" without it names no endpoint an operator could go and fix. */
            void Record(string channel, string? error)
            {
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
               channel's {{dedup_key}} token uses, so link, token, and PagerDuty all correlate — and it
               reads the RENDERED incidents, so the anchor names an incident the card actually shows. */
            var triageUrl = TriageLink.Build(
                _settings.TriageBaseUrl, serverName, metricName, nowUtc,
                DerivePagerDutyDedupKey(string.IsNullOrEmpty(serverId) ? serverName : serverId, metricName, renderContext));

            /* #3598: WHERE each channel posts, resolved ONCE for the whole fan-out and AFTER the cooldown and
               the budget above (design point 2) — so one firing is one delivery decision regardless of where
               it lands, and a throttled alert never resolves at all. Exact-metric route, then family route,
               then the parent row's own destination, per channel; with zero routes every destination below
               IS the settings member the four gates above read, so the fan-out is the pre-routes one byte
               for byte. A channel that resolves to nothing is not attempted, exactly as an unconfigured one
               was not. The decision rides the result so the deliverer can record it on the history row. */
            var route = NotificationRouter.Resolve(metricName, _settings.NotificationRoutes, _settings);

            if (route.Teams.Destination is { } teamsUrl)
            {
                attempted = true;
                Record(NotificationRouter.TeamsChannel, await TrySendTeamsAlertAsync(teamsUrl, metricName, serverName, currentValue, thresholdValue, renderContext, triageUrl, prose, nowUtc, displayName));
            }

            if (route.Slack.Destination is { } slackUrl)
            {
                attempted = true;
                Record(NotificationRouter.SlackChannel, await TrySendSlackAlertAsync(slackUrl, metricName, serverName, currentValue, thresholdValue, renderContext, triageUrl, prose, nowUtc, displayName));
            }

            if (route.Generic.Destination is { } genericUrl)
            {
                /* Generic webhook: the payload's "metric" field is a machine key an automation correlates on,
                   so it stays the immutable metric name — the display name is a human-title concern only, and
                   this channel has no title. The prose detail DOES go, because it is alert content. */
                attempted = true;
                Record(NotificationRouter.GenericChannel, await TrySendGenericAlertAsync(genericUrl, metricName, serverName, currentValue, thresholdValue, serverId, renderContext, triageUrl, prose, nowUtc));
            }

            if (route.PagerDuty.Destination is { } pagerDutyKey)
            {
                attempted = true;
                Record(NotificationRouter.PagerDutyChannel, await TrySendPagerDutyAlertAsync(pagerDutyKey, metricName, serverName, currentValue, thresholdValue, serverId, renderContext, triageUrl, prose, nowUtc, displayName));
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

            return sent ? WebhookFanoutResult.Delivered with { Route = route }
                : attempted ? WebhookFanoutResult.Failed(firstError) with { Route = route }
                : WebhookFanoutResult.NotAttempted with { Route = route };
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
            return await PostWebhookAsync(webhookUrl, payload, proxyAddress);
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
            return await PostWebhookAsync(webhookUrl, payload, proxyAddress);
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
        AlertBranding branding)
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

            return await PostWebhookAsync(webhookUrl, payload, proxyAddress, headers);
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

    #region Teams

    /// <summary>Posts to Teams. Returns null when the post succeeded, or the error text when it did not —
    /// a bool loses the reason, and the reason is what the alert log's <c>send_error</c> carries on a
    /// <see cref="AlertDelivery.ChannelFailed"/> row. <paramref name="webhookUrl"/> is the ROUTED destination
    /// (#3598) — the parent's URL when no route touched this firing — while the proxy stays the parent's:
    /// a route says where a family lands, not how the channel type is reached.</summary>
    private async Task<string?> TrySendTeamsAlertAsync(
        string webhookUrl,
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertContext? context,
        string? triageUrl,
        string? detailText,
        DateTime nowUtc,
        string? displayName = null)
    {
        try
        {
            var payload = BuildTeamsPayload(metricName, serverName, currentValue, thresholdValue, _branding, context: context, triageUrl: triageUrl,
                detailText: detailText, displayName: displayName, nowUtc: nowUtc);
            var error = await PostWebhookAsync(webhookUrl, payload, _settings.TeamsProxyAddress);

            if (error != null)
            {
                _consecutiveTeamsFailures++;
                _lastTeamsError = error;

                if (_consecutiveTeamsFailures <= 3)
                    _logger.LogError($"TEAMS WEBHOOK FAILED ({_consecutiveTeamsFailures}x): {error}");
                else if (_consecutiveTeamsFailures % 50 == 0)
                    _logger.LogError($"TEAMS WEBHOOK STILL FAILING: {_consecutiveTeamsFailures} failures. Last: {error}");

                return error;
            }

            if (_consecutiveTeamsFailures > 0)
                _logger.LogInformation($"Teams webhook recovered after {_consecutiveTeamsFailures} failure(s)");

            _consecutiveTeamsFailures = 0;
            _lastTeamsError = null;
            _logger.LogInformation($"Teams webhook sent for {metricName} on {serverName}");
            return null;
        }
        catch (Exception ex)
        {
            _consecutiveTeamsFailures++;
            _lastTeamsError = ex.Message;
            _logger.LogError($"Teams webhook error: {ex.Message}");
            return ex.Message;
        }
    }

    /// <summary>
    /// #2710: the Datadog-parity <c>resource_name</c> tag — the first incident's involved objects,
    /// joined. Mirrors the SAME "first incident is the correlation anchor" precedent
    /// <see cref="DerivePagerDutyDedupKey"/> already uses for an alert carrying more than one
    /// fingerprint: whichever incident PagerDuty's dedup_key names is also the one a human reading
    /// the tag should look at first. Null when the alert carries no fingerprintable incident (alert
    /// type not wired to #1140's <see cref="AlertContext.Incidents"/>, or the objects were
    /// unresolved) — a top-level tag naming nothing would read worse than the tag being absent.
    /// <see cref="AlertFingerprint.ForObjects"/>'s callers filter out blank objects before they ever
    /// reach here, but a caller of the sibling <see cref="AlertFingerprint.ForKey"/> overload can pass
    /// an unfiltered blank display object (review catch: <c>AlertContextBuilders.VolumeFreeSpaceIncidents</c>
    /// / <c>AnomalousJobIncidents</c> pass the raw mount point / job name) — so the join is checked
    /// for blank the same way <see cref="AlertFingerprint.ForKey"/> already checks <c>Database</c>,
    /// rather than trusting every caller to have pre-filtered.
    /// </summary>
    private static string? DeriveResourceName(AlertContext? context)
    {
        if (context?.Incidents is not { Count: > 0 } incidents)
            return null;

        var objects = incidents[0].InvolvedObjects;
        if (objects.Count == 0)
            return null;

        var joined = string.Join(", ", objects);
        return string.IsNullOrWhiteSpace(joined) ? null : joined;
    }

    /// <summary>
    /// #2710: the Datadog-parity <c>env</c>-adjacent database scope — the first incident's
    /// <see cref="AlertIncident.Database"/>, the SAME incident <see cref="DeriveResourceName"/>
    /// reads (kept as its own tag rather than folded into resource_name: per #2361's doc comment on
    /// <see cref="AlertIncident.Database"/>, it is WHERE the resource lives, not what it is). Null on
    /// every alert type <see cref="AlertIncident.Database"/> already documents as unscoped (a volume
    /// or a job is not database-scoped) or with no incident at all.
    /// </summary>
    private static string? DeriveResourceDatabase(AlertContext? context) =>
        context?.Incidents is { Count: > 0 } incidents ? incidents[0].Database : null;

    /// <summary>
    /// Builds an O365 MessageCard payload for Teams incoming webhooks.
    /// The themeColor property renders as a colored accent bar at the top of the card.
    /// <para>#2710: a non-null <paramref name="triageUrl"/> adds a <c>potentialAction</c> OpenUri button —
    /// the MessageCard-native link affordance — pointing at the computed triage page. Null (base URL unset,
    /// or a test send) renders exactly the pre-#2710 card.</para>
    /// <para>#3297: <paramref name="detailText"/> is the alert's flat prose detail. It becomes its own
    /// <c>Details</c> section ahead of the per-incident ones, because a multi-paragraph remedy in a
    /// label/value fact renders as an unreadable column — <c>text</c> is the MessageCard-native place for
    /// prose, and the snooze footer already uses it.</para>
    /// <para>#3612: this card carries NO analogue of the Slack block budget, on purpose. The MessageCard
    /// format has no section-count limit (its reference offers "don't include more than 10 sections" as a
    /// readability guideline, not a rejection), and the connector's one hard ceiling is the ~28 KB payload.
    /// The analysis page that Slack lost renders here at 16.6 KB with its 21 items and 17.4 KB at the 25
    /// items the heaviest shape the story can produce (fifteen incidents, the most three five-row
    /// drill-downs can surface) — about 210 bytes per incident, so the cap is some fifty incidents away
    /// from any shape a producer can emit. <c>SlackDetailsSizeTests</c> pins the heaviest shape against
    /// the cap so a producer that widens the page finds out there rather than in a lost delivery.</para>
    /// </summary>
    internal static string BuildTeamsPayload(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertBranding branding,
        bool isTest = false,
        AlertContext? context = null,
        string? triageUrl = null,
        string? detailText = null,
        string? displayName = null,
        DateTime? nowUtc = null)
    {
        var (hexColor, badgeText, emoji) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var prose = AlertDetailText.ProseForDelivery(detailText, context);
        /* Title/summary show the human name when present; ForMetric above stays on the immutable metric
           name (the severity key), and a null/empty display name renders the metric name unchanged. */
        var titleName = string.IsNullOrEmpty(displayName) ? metricName : displayName;
        var themeColor = hexColor.TrimStart('#');
        /* One instant, with the local rendering DERIVED from it rather than read separately: two reads can
           land either side of a second, which would have the card's own "Time (UTC)" and "Time (Local)"
           facts naming two different seconds of one alert. SpecifyKind because an injected clock may hand
           back an Unspecified DateTime, which ToLocalTime would otherwise treat as already local. */
        var utcNow = nowUtc ?? DateTime.UtcNow;
        var localNow = DateTime.SpecifyKind(utcNow, DateTimeKind.Utc).ToLocalTime();

        var facts = new List<object>();

        if (isTest)
        {
            facts.Add(new { name = "Status", value = "Webhook configuration is working correctly" });
            facts.Add(new { name = "Sent at", value = localNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }
        else
        {
            facts.Add(new { name = "Server", value = serverName });
            var resourceName = DeriveResourceName(context);
            if (resourceName is not null)
                facts.Add(new { name = "Resource", value = resourceName });
            var database = DeriveResourceDatabase(context);
            if (!string.IsNullOrEmpty(database))
                facts.Add(new { name = "Database", value = database });
            facts.Add(new { name = "Current Value", value = currentValue });
            facts.Add(new { name = "Threshold", value = thresholdValue });
            facts.Add(new { name = "Time (UTC)", value = utcNow.ToString("yyyy-MM-dd HH:mm:ss") });
            facts.Add(new { name = "Time (Local)", value = localNow.ToString("yyyy-MM-dd HH:mm:ss") });
        }

        /* #2108: each Fields-carrying detail item becomes its OWN section further down, so a
           multi-incident alert reads as labeled, self-contained units instead of one flat fact
           list where a victim's fields and its fingerprint's fields drift apart. Advice prose and
           remediation-T-SQL items stay folded into the lead section's facts — they are commentary
           on the whole alert, not incidents. */
        var itemSections = new List<object>();

        /* #3297: ahead of the per-incident sections — it is the actionable half of the alert. */
        if (prose is not null)
        {
            itemSections.Add(new { activityTitle = "Details", text = prose, markdown = true });
        }

        if (context?.Details != null)
        {
            foreach (var detail in context.Details)
            {
                if (detail.IsCodeBlock)
                {
                    /* Remediation T-SQL: point at the email / in-app dialog, never inline it. */
                    facts.Add(new { name = detail.Heading, value = TsqlWebhookHint });
                    continue;
                }

                if (!string.IsNullOrEmpty(detail.Body))
                {
                    /* Advice prose: one "Advice" fact for the headline, then a fact per
                       Investigation/Remediation paragraph (split on the blank line).
                       Body is exclusive with Fields, matching the email and Slack surfaces
                       which skip Fields when Body is present. */
                    facts.Add(new { name = "Advice", value = detail.Heading });
                    foreach (var para in detail.Body.Split("\n\n", StringSplitOptions.RemoveEmptyEntries))
                    {
                        var (label, text) = SplitProseLabel(para);
                        facts.Add(new { name = label, value = text });
                    }
                    continue;
                }

                var itemFacts = new List<object>();
                if (detail.Records.Count > 0)
                {
                    /* #3644: a record-shaped detail is one fact per ROW — name "#N", value the summary line
                       over the text(s) in backticks — rather than one fact per attribute (seven per row). The
                       MessageCard fact list is a name/value table, so the flat fields did not interleave here
                       the way Slack's grid did; the compact list is the same reading order in a third of the
                       rows, and the same row numbers a reader sees on Slack and in the email. Fact values
                       render markdown; a MessageCard supports inline code but no fenced block, and the
                       text's own backticks would end the span early, so they are replaced with a straight
                       quote (a T-SQL text never carries one; a PostgreSQL text rarely). */
                    foreach (var record in detail.Records)
                    {
                        var value = new StringBuilder(record.Summary);
                        foreach (var (label, text) in record.Texts)
                        {
                            if (value.Length > 0)
                            {
                                value.Append("  \n");
                            }

                            if (record.Texts.Count > 1)
                            {
                                value.Append('_').Append(label).Append("_  \n");
                            }

                            value.Append('`').Append(text.Replace('`', '\'')).Append('`');
                        }

                        itemFacts.Add(new { name = string.Create(CultureInfo.InvariantCulture, $"#{record.Ordinal}"), value = value.ToString() });
                    }
                }
                else
                {
                    foreach (var (label, value) in detail.Fields)
                    {
                        itemFacts.Add(new { name = label, value });
                    }
                }

                itemSections.Add(new
                {
                    activityTitle = detail.Heading,
                    facts = itemFacts,
                    markdown = true
                });
            }
        }

        var title = isTest
            ? $"{emoji} TEST — {metricName}"
            : $"{emoji} {badgeText} — {titleName}";

        var sections = new List<object>
        {
            new
            {
                activityTitle = title,
                activitySubtitle = isTest ? branding.EditionName : $"{branding.EditionName} — {serverName}",
                facts,
                markdown = true
            }
        };
        sections.AddRange(itemSections);

        if (!isTest && branding.SnoozeHint is not null)
        {
            sections.Add(new { text = branding.SnoozeHint });
        }

        var summary = isTest
            ? "[SQL Monitor] Test Notification"
            : $"[SQL Monitor] {badgeText}: {titleName} on {serverName}";

        /* #2710: the OpenUri action carries its schema keys as REAL "@type" (a Dictionary, because a C#
           @-identifier only escapes the keyword — the existing card's `@type` serializes as "type", a
           looseness Teams tolerates on the envelope but potentialAction is stricter about). Two shapes
           rather than a nullable property, because System.Text.Json serializes a null member and a
           "potentialAction": null key is exactly the kind of half-present field connectors choke on. */
        object card = triageUrl is null
            ? new
            {
                @type = "MessageCard",
                @context = "http://schema.org/extensions",
                themeColor,
                summary,
                sections
            }
            : new
            {
                @type = "MessageCard",
                @context = "http://schema.org/extensions",
                themeColor,
                summary,
                sections,
                potentialAction = new object[]
                {
                    new Dictionary<string, object>
                    {
                        ["@type"] = "OpenUri",
                        ["name"] = TriageLink.LinkLabel(triageUrl),
                        ["targets"] = new object[] { new { os = "default", uri = triageUrl } }
                    }
                }
            };

        return JsonSerializer.Serialize(card, s_jsonOptions);
    }

    #endregion

    #region Slack

    /// <summary>Posts to Slack. Null when the post succeeded, the error text when it did not — see
    /// <see cref="TrySendTeamsAlertAsync"/>, including for the routed <paramref name="webhookUrl"/>.</summary>
    private async Task<string?> TrySendSlackAlertAsync(
        string webhookUrl,
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertContext? context,
        string? triageUrl,
        string? detailText,
        DateTime nowUtc,
        string? displayName = null)
    {
        try
        {
            var payload = BuildSlackPayload(metricName, serverName, currentValue, thresholdValue, _branding, context: context, triageUrl: triageUrl,
                detailText: detailText, displayName: displayName, nowUtc: nowUtc);
            var error = await PostWebhookAsync(webhookUrl, payload, _settings.SlackProxyAddress);

            if (error != null)
            {
                _consecutiveSlackFailures++;
                _lastSlackError = error;

                if (_consecutiveSlackFailures <= 3)
                    _logger.LogError($"SLACK WEBHOOK FAILED ({_consecutiveSlackFailures}x): {error}");
                else if (_consecutiveSlackFailures % 50 == 0)
                    _logger.LogError($"SLACK WEBHOOK STILL FAILING: {_consecutiveSlackFailures} failures. Last: {error}");

                return error;
            }

            if (_consecutiveSlackFailures > 0)
                _logger.LogInformation($"Slack webhook recovered after {_consecutiveSlackFailures} failure(s)");

            _consecutiveSlackFailures = 0;
            _lastSlackError = null;
            _logger.LogInformation($"Slack webhook sent for {metricName} on {serverName}");
            return null;
        }
        catch (Exception ex)
        {
            _consecutiveSlackFailures++;
            _lastSlackError = ex.Message;
            _logger.LogError($"Slack webhook error: {ex.Message}");
            return ex.Message;
        }
    }

    /// <summary>
    /// Slack's documented ceiling on the <c>fields</c> array of one <c>section</c> block. A section over it
    /// is rejected as invalid_blocks, which fails the WHOLE message rather than degrading it — so a body
    /// that grows past it loses the alert entirely, silently from the reader's side.
    /// </summary>
    private const int SlackSectionFieldLimit = 10;

    /// <summary>
    /// Appends <paramref name="fields"/> as however many <c>section</c> blocks it takes to keep each one
    /// inside <see cref="SlackSectionFieldLimit"/>. Consecutive sections carry no divider between them, so
    /// a split reads as one continued block.
    ///
    /// <para>The field count per item is not bounded by anything upstream: an incident item already emits
    /// its forensic detail, its dedup metadata, occurrence counts and an incident start, and #3442's
    /// per-party deadlock facts add up to five more. Splitting here rather than capping per producer means
    /// no producer has to know what every other producer contributed to the same item.</para>
    ///
    /// <para>An empty list appends nothing: a section with neither <c>text</c> nor a non-empty
    /// <c>fields</c> is itself invalid.</para>
    /// </summary>
    private static void AddSlackFieldSections(List<object> blocks, List<object> fields)
    {
        for (var i = 0; i < fields.Count; i += SlackSectionFieldLimit)
        {
            var take = Math.Min(SlackSectionFieldLimit, fields.Count - i);
            blocks.Add(new { type = "section", fields = fields.GetRange(i, take) });
        }
    }

    /// <summary>
    /// Slack's documented ceiling on ONE text object's characters — per text object, not per message, so a
    /// section block's mrkdwn text is bound by it however few blocks the message carries. Exceeding it
    /// rejects the WHOLE message (HTTP 400, spelled <c>invalid_attachments</c> when the blocks ride inside
    /// the colored attachment), not just the oversized block. Measured live (#3493), on the first two
    /// nights of digest delivery: both big-fleet stores' 20-mover Collector Cost Digests failed exactly
    /// this way while the small store's 1-mover digest delivered — and one store's Fleet Sweep Rollup
    /// delivered through the SAME webhook 80 milliseconds after its digest failed, so the webhook was
    /// never the suspect. The single <c>*Details*</c> section was over this ceiling.
    /// </summary>
    internal const int SlackTextObjectLimit = 3000;

    /// <summary>
    /// Slack's documented ceiling on blocks per message. A fifty-first block fails the delivery as surely
    /// as an oversized text object does, and it fails it with the SAME error string — which is how #3612
    /// hid behind #3493 for two builds. Both splitters spend only what the payload's other blocks leave
    /// over: the prose splitter (<see cref="AddSlackProseSections"/>) since #3493, the structured details
    /// (<see cref="AddSlackDetailBlocks"/>) since #3612.
    ///
    /// <para>Measured live (#3612): three analysis pages — the product's highest-severity compound
    /// findings, five-fact stories rooted in a plan regression — died with <c>invalid_attachments</c>
    /// across two builds while a three-fact page delivered through the same webhook the same hour. The
    /// monitoring seat's read of the persisted <c>context_json</c> named the separator: every text
    /// object on the failed pages was under its cap (their longest body was 2,122 characters, and a
    /// DELIVERED page carried a 2,835-character one), and the failed pages carried 19–21 detail items against
    /// the largest delivered page's 18. At roughly two blocks per item plus the fixed head and tail,
    /// that is the fifty-block line. The fixture in <c>SlackDetailsSizeTests</c> reproduces the shape
    /// from source: ten items the story's drill-downs produce (33 blocks) plus one two-block incident
    /// item per distinct query hash, so seven or more incidents cross the limit.</para>
    /// </summary>
    internal const int SlackMessageBlockLimit = 50;

    /// <summary>
    /// Slack's documented ceiling on ONE text object inside a section's <c>fields</c> — tighter than the
    /// <see cref="SlackTextObjectLimit"/> a section's own text gets. Applied to every detail field
    /// (<see cref="SlackFieldText"/>) as the field-side half of the #3612 discipline. No observed page
    /// has approached it (the analysis formatter truncates drill-down values at 300 characters and the
    /// longest measured field text is 318), so this is hygiene rather than the fix; the fix is the block
    /// budget.
    /// </summary>
    internal const int SlackFieldTextLimit = 2000;

    /// <summary>
    /// The most of a detail's heading a Slack section carries. Headings are producer literals and
    /// humanized drill-down keys — the longest observed is an advice headline under 100 characters — so
    /// this never fires on a real page; it exists because the body splitter sizes its per-section
    /// capacity as the text-object ceiling minus the heading's width, and an unbounded heading would
    /// leave it no capacity at all.
    /// </summary>
    private const int SlackDetailHeadingLimit = 500;

    /// <summary>
    /// The most one record of a record-shaped detail (<see cref="AlertDetailItem.Records"/>, #3644) may
    /// occupy in a section's text: its bold summary line plus its code-blocked text(s), markup included.
    /// Sized so that a record ALWAYS fits a section beside the detail's heading — the heading is bounded at
    /// <see cref="SlackDetailHeadingLimit"/> and wrapped in <c>*…*\n</c> (four more units) — which is
    /// what lets <see cref="AddSlackRecordSections"/> pack records greedily and open a fresh section on the
    /// first one that does not fit, with no record ever cut between sections. No producer comes near it:
    /// the widest drill-down row (Regressed Queries, twelve scalars and a 500-character text) is under
    /// 900. A text that would push a record past it is cut with the count stated, through
    /// <see cref="SlackBoundedText"/> — the same note, in the same characters, as a cut field.
    /// </summary>
    private const int SlackRecordLimit = SlackTextObjectLimit - SlackDetailHeadingLimit - 4;

    /// <summary>The most of a record's summary line a Slack section carries before the texts take the rest
    /// of the record's room. The producer packs a row's scalars into it — twelve labelled numbers is under
    /// 400 — so this never fires on a real page; it exists so the texts' share below is always positive.</summary>
    private const int SlackRecordSummaryLimit = 1000;

    /// <summary>
    /// What the stated-omission item for dropped details costs: its own divider, so it reads as the
    /// final item rather than as a continuation of whichever detail happened to be last, and one section.
    /// </summary>
    private const int SlackDetailOmissionCost = 2;

    /// <summary>How much of the omitted details' heading list the stated-omission item quotes before
    /// cutting it, so a message with dozens of dropped items cannot blow the omission section past the
    /// text-object ceiling it exists to respect.</summary>
    private const int SlackOmittedHeadingsLimit = 1000;

    /// <summary>
    /// Where every stated omission points: the full document has always lived in the alert row (the
    /// prose as <c>detail_text</c>, the details as <c>context_json</c>) and in the email body, the way
    /// <see cref="TsqlWebhookHint"/> already points there. One string, so the three omission sites
    /// (prose lines, dropped details, truncated fields) cannot drift into three different directions.
    /// </summary>
    private const string SlackOmissionPointer = " - see email or in-app Alert Details for the full text.";

    /// <summary>The prose sections' leading header — on the FIRST section only; see
    /// <see cref="AddSlackProseSections"/> for why continuations carry nothing.</summary>
    private const string SlackProseHeader = "*Details*\n";

    /// <summary>Marks the continuation halves of a single line hard-split by
    /// <see cref="SplitProseIntoSectionSafeLines"/>, so the reader sees one line that was cut rather
    /// than two that were written.</summary>
    private const string SlackProseContinuationMarker = "(cont.) ";

    /// <summary>How much of the first omitted line the stated-omission line quotes. Enough to identify a
    /// digest mover (collector, server and the headline figures all sit in the line's first stretch);
    /// bounded so a pathological line cannot blow the omission line past the very ceiling it exists to
    /// respect.</summary>
    private const int SlackOmissionFragmentLimit = 120;

    /// <summary>
    /// The room the omission-path packing holds back in its final block for the stated-omission line:
    /// the line's fixed text (~93 chars), a 13-digit separator-grouped line count, and the quoted
    /// fragment at its cap plus its own truncation ellipsis — 231 worst case, rounded up.
    /// </summary>
    private const int SlackOmissionReserve = 256;

    /// <summary>
    /// Appends <paramref name="prose"/> as however many mrkdwn <c>section</c> blocks it takes to keep
    /// every text object inside <see cref="SlackTextObjectLimit"/> — the
    /// <see cref="AddSlackFieldSections"/> precedent one level up (#3493). The split lands on LINE
    /// boundaries, because the long-prose producers are line-oriented documents (the Collector Cost
    /// Digest is one mover per line) and a line cut mid-thought misstates a figure. Consecutive sections
    /// carry no divider between them, so a split reads as one continued document; only the first section
    /// leads with <paramref name="header"/>, and continuations carry nothing — a repeated header would
    /// read as several detail sections rather than one that continued.
    ///
    /// <para><b>One splitter, two callers (#3612).</b> The header is a parameter because the alert's
    /// flat prose (<see cref="SlackProseHeader"/>) and a structured detail's <see cref="AlertDetailItem.Body"/>
    /// (<c>*Heading*</c>) are the same thing to Slack — one mrkdwn section under one ceiling — and #3493
    /// covered only the first. The analysis pages carry their whole substance as details and deliver no
    /// prose at all, so a second, detail-side splitter would have been a second contract to keep in step;
    /// instead <see cref="AddSlackDetailBlocks"/> routes every body through this one, and the omission
    /// wording, the continuation marker and the packing arithmetic are shared by construction.</para>
    ///
    /// <para><b>The block budget, and what yields to it.</b> <paramref name="blockBudget"/> is what the
    /// payload's other blocks leave under <see cref="SlackMessageBlockLimit"/>, and the PROSE is what
    /// degrades when it cannot fit — never the structured details. The precedence is decided by what the
    /// real payload shapes carry: the producers with long prose (the digest, the self-alerts) fire with
    /// no structured context at all, and the alerts with heavy per-incident details carry prose that is
    /// short or suppressed as redundant (<see cref="AlertDetailText.ProseForDelivery"/>), so the yielding
    /// branch never costs a real payload both halves at once. Since #3612 the details are themselves
    /// bounded, and they reserve one block for the prose when there is one, so every caller hands in a
    /// budget of at least one — and the packer floors it at one locally as well, so the invariant the
    /// repack loop depends on (it terminates by reaching its final block) does not live only in three
    /// call sites' subtraction (review note on #3618).</para>
    ///
    /// <para><b>Degrading is stated, never silent.</b> A prose the budget cannot hold keeps as many whole
    /// lines as fit and ends with one omission line naming HOW MANY lines were dropped and quoting the
    /// first of them — the producers rank their lines most-significant-first (the digest orders movers by
    /// magnitude), so the first dropped line is the headline of what the reader is not seeing, and an
    /// omission note that is itself vague would recreate the silent-truncation problem one level up. The
    /// full text has always lived in the alert row and the email body, so the omission line points there
    /// (<see cref="SlackOmissionPointer"/>) the way <see cref="TsqlWebhookHint"/> already does.</para>
    ///
    /// <para>A budget of one matches the pre-#3493 block cost exactly (the old single section also cost
    /// one block), so a payload whose OTHER blocks already crowd the message limit is no worse off than
    /// it ever was — the prose does not decide that verdict.</para>
    /// </summary>
    private static void AddSlackProseSections(List<object> blocks, string header, string prose, int blockBudget)
    {
        /* Uniform per-section line capacity, sized to the first section (the only one carrying the
           header): continuations run a header's width under the ceiling, which keeps the packing
           single-pass. The header is bounded by its producers (SlackDetailHeadingLimit for a detail's
           heading), so the capacity is always comfortably positive. */
        var capacity = SlackTextObjectLimit - header.Length;

        /* The one-section fast path IS the pre-#3493 rendering, byte for byte — 1-mover digests and
           every ordinary alert take it, and so does every detail body an analysis page has ever carried
           (the longest measured is 2,835 characters), so their payloads do not change shape at all. */
        if (prose.Length <= capacity)
        {
            blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = header + prose } });
            return;
        }

        /* The floor is local on purpose: PackProseLines' repack loop terminates by reaching its final
           block, which a budget of zero would never present, and a budget below one cannot render any
           section anyway. Every caller already hands in at least one; this keeps that true if one of
           them is edited. */
        var lines = SplitProseIntoSectionSafeLines(prose, capacity);
        var texts = PackProseLines(lines, capacity, Math.Max(1, blockBudget));

        for (var i = 0; i < texts.Count; i++)
        {
            var text = i == 0 ? header + texts[i] : texts[i];
            blocks.Add(new { type = "section", text = new { type = "mrkdwn", text } });
        }
    }

    /// <summary>
    /// The prose, as lines that each fit a section on their own. A single line longer than
    /// <paramref name="capacity"/> — pathological, but a delivery that fails over it would be this bug
    /// again — hard-splits at character boundaries (whole characters, through <see cref="SlackCutLength"/>,
    /// since #3622), every continuation piece marked with <see cref="SlackProseContinuationMarker"/>.
    /// </summary>
    private static List<string> SplitProseIntoSectionSafeLines(string prose, int capacity)
    {
        var lines = new List<string>();
        foreach (var raw in prose.Split('\n'))
        {
            if (raw.Length <= capacity)
            {
                lines.Add(raw);
                continue;
            }

            var start = 0;
            while (start < raw.Length)
            {
                var prefix = start == 0 ? string.Empty : SlackProseContinuationMarker;
                /* #3622: the piece ends on a character boundary, never between the halves of a surrogate
                   pair or through a combining sequence — the index arithmetic alone put an emoji's two
                   halves in two sections, and the reader saw two replacement glyphs and no emoji. The
                   loop always advances: the capacity is in the thousands and the marker is eight, so the
                   cut can never back off to nothing. */
                var take = SlackCutLength(raw.AsSpan(start), capacity - prefix.Length);
                lines.Add(prefix + raw.Substring(start, take));
                start += take;
            }
        }

        return lines;
    }

    /// <summary>
    /// Packs <paramref name="lines"/> greedily into section texts of at most <paramref name="capacity"/>
    /// characters. When the packing fits <paramref name="blockBudget"/> that is the answer; when it does
    /// not, the lines are repacked into exactly the budget with the final section reserving
    /// <see cref="SlackOmissionReserve"/> for the stated-omission line, which then closes the document.
    /// </summary>
    private static List<string> PackProseLines(List<string> lines, int capacity, int blockBudget)
    {
        var texts = new List<string>();
        var sb = new StringBuilder();
        foreach (var line in lines)
        {
            if (sb.Length > 0 && sb.Length + 1 + line.Length > capacity)
            {
                texts.Add(sb.ToString());
                sb.Clear();
            }

            if (sb.Length > 0)
            {
                sb.Append('\n');
            }

            sb.Append(line);
        }

        if (sb.Length > 0)
        {
            texts.Add(sb.ToString());
        }

        if (texts.Count <= blockBudget)
        {
            return texts;
        }

        /* Over budget: repack into exactly blockBudget sections, the last one holding room back for the
           omission line. Blocks before the last pack identically to the pass above, so this can never
           fit MORE lines than the pass that already overflowed — the omission line is always earned. */
        texts.Clear();
        sb.Clear();
        var placed = 0;
        while (placed < lines.Count)
        {
            var line = lines[placed];
            var finalBlock = texts.Count == blockBudget - 1;
            var reserve = finalBlock ? SlackOmissionReserve + 1 : 0;
            var joiner = sb.Length > 0 ? 1 : 0;

            if (sb.Length + joiner + line.Length + reserve > capacity)
            {
                if (finalBlock)
                {
                    break;
                }

                texts.Add(sb.ToString());
                sb.Clear();
                continue;
            }

            if (joiner == 1)
            {
                sb.Append('\n');
            }

            sb.Append(line);
            placed++;
        }

        var dropped = lines.Count - placed;
        var firstDropped = lines[placed];
        var fragment = firstDropped.Length <= SlackOmissionFragmentLimit
            ? firstDropped
            : firstDropped[..SlackCutLength(firstDropped, SlackOmissionFragmentLimit)] + "...";
        var noun = dropped == 1 ? "line" : "lines";
        var omission = string.Create(CultureInfo.InvariantCulture,
            $"... and {dropped:N0} more {noun}, first omitted: \"{fragment}\"{SlackOmissionPointer}");

        if (sb.Length > 0)
        {
            sb.Append('\n');
        }

        sb.Append(omission);
        texts.Add(sb.ToString());
        return texts;
    }

    /// <summary>
    /// Appends <paramref name="details"/> as divider-led block runs — the rendering every structured
    /// detail has always had — inside <paramref name="blockBudget"/>, dropping WHOLE details from the
    /// end and stating what was dropped when they cannot all fit. The detail-side half of the #3493
    /// discipline, added by #3612 after three analysis pages were lost to it.
    ///
    /// <para><b>Why the block total is the fix, and why it is a budget on blocks rather than on facts or
    /// details.</b> An analysis page carries its whole finding as details: a Diagnosis item, the advice,
    /// the remediation T-SQL pointer, one item per drill-down the story's fact keys attach (a five-fact
    /// plan-regression story attaches seven, and three of them are 30–40 fields wide, so four or five
    /// blocks each), and one incident item per distinct query hash the drill-downs surfaced (up to fifteen).
    /// Measured from source for that story: two head blocks, 33 blocks for the ten fixed items, two
    /// blocks per incident, and two footer blocks — 37 + 2×incidents — so the seventh incident is the
    /// fifty-first block. The count that matters is blocks: a four-fact story with wider drill-downs can
    /// reach the line with fewer items, and the SAME 18 items can land either side of it depending on how
    /// many fields each carries, which is why the budget is computed from the rendered runs and not from
    /// any count upstream of them.</para>
    ///
    /// <para><b>What drops first.</b> Producers order their details most-essential-first — the analysis
    /// path emits Diagnosis, then Advice, then the T-SQL pointer, then the drill-downs, then the incident
    /// fingerprints; the engine alerts emit the finding's own items before the per-incident ones — so
    /// keeping the longest PREFIX that fits keeps the finding and drops the drill-down's tail. A body
    /// detail that reaches the boundary is not dropped whole: it takes the blocks that remain and states
    /// its own line omission, because the advice is the finding and a shortened advice beats a missing
    /// one. No detail is skipped to make room for a smaller one behind it; a reader who sees
    /// "Incident 6 of 11" and then the omission item knows exactly where the message stopped. The
    /// omission item's two blocks are held back only while something can still be dropped: the LAST
    /// detail is offered the whole remainder, so a page whose only degradation is a shortened body
    /// carries no omission item — the body's own line omission is the whole truth of it — and no item is
    /// dropped to make room for an announcement that costs what the item did.</para>
    ///
    /// <para><b>The omission is stated, and it names the items.</b> The final run is a divider and one
    /// section naming how many details were dropped and listing their headings — the headings ARE the
    /// identity of a detail ("Regressed Queries", "Incident 7 of 11"), so a reader knows what to open
    /// the alert for — and pointing at the surfaces that carry the whole document
    /// (<see cref="SlackOmissionPointer"/>): the alert row's <c>context_json</c> renders in full in-app
    /// and in the email, neither of which has a block limit. Its cost
    /// (<see cref="SlackDetailOmissionCost"/>) is charged against the budget before any detail is
    /// kept, so the omission item is always affordable when it is needed.</para>
    ///
    /// <para><b>A page that fits is byte-identical.</b> When every run fits the budget the runs are
    /// appended exactly as the pre-#3612 loop appended them — the same anonymous shapes in the same
    /// order — so every engine alert and every analysis page that delivered before this change
    /// serializes to the same bytes after it. The text-object caps the runs apply
    /// (<see cref="RenderSlackDetail"/>) only change a payload that would have been rejected.</para>
    /// </summary>
    private static void AddSlackDetailBlocks(List<object> blocks, List<AlertDetailItem> details, int blockBudget)
    {
        if (details.Count == 0)
        {
            return;
        }

        /* First pass: every detail rendered whole, each body free to take as many sections as it needs.
           This is the answer whenever it fits, and it is the pre-#3612 rendering. */
        var runs = new List<List<object>>(details.Count);
        var total = 0;
        foreach (var detail in details)
        {
            var run = RenderSlackDetail(detail, bodyBlockBudget: int.MaxValue);
            runs.Add(run);
            total += run.Count;
        }

        if (total <= blockBudget)
        {
            foreach (var run in runs)
            {
                blocks.AddRange(run);
            }

            return;
        }

        /* Over budget: keep the longest prefix whose runs fit beside the omission item. A body detail at
           the boundary is re-rendered into whatever remains rather than dropped — see the doc block. The
           omission item's cost is bounded by the budget itself so a degenerate budget (one block) still
           states the omission rather than overflowing; the real budgets are in the forties. A budget of
           zero or less leaves no block to state anything in, so nothing is appended — the only honest
           rendering of a budget that cannot exist: the head is two blocks and the footer at most two, so
           the caller hands in forty-five or more, and a change to that composition is what this comment
           is for. */
        var omissionCost = Math.Min(SlackDetailOmissionCost, Math.Max(0, blockBudget));
        var spent = 0;
        var kept = 0;
        while (kept < details.Count)
        {
            /* The omission item is only owed when something AFTER this detail is dropped, so the last
               detail is offered the whole remainder, reserve included: a last body shrinks into two more
               blocks, and a last small item that fits in the reclaimed two is kept instead of being
               replaced by an omission item of the same cost that would announce it (review catch on
               #3618). For every other detail the reserve stands, and a body shrunk into it fills the
               remainder exactly, so the detail after it necessarily drops and the item is earned. */
            var last = kept == details.Count - 1;
            var remaining = blockBudget - spent - (last ? 0 : omissionCost);
            var run = runs[kept];
            if (run.Count > remaining)
            {
                var detail = details[kept];
                /* One divider plus at least one section is the least a body detail can cost. */
                if (string.IsNullOrEmpty(detail.Body) || detail.IsCodeBlock || remaining < 2)
                {
                    break;
                }

                run = RenderSlackDetail(detail, bodyBlockBudget: remaining - 1);
                if (run.Count > remaining)
                {
                    break;
                }
            }

            blocks.AddRange(run);
            spent += run.Count;
            kept++;
        }

        /* Nothing dropped — the boundary body's own line omission already states its cut — or no block
           to state a drop in: either way an omission item here would be a false statement. */
        var dropped = details.Count - kept;
        if (dropped == 0 || omissionCost == 0)
        {
            return;
        }

        var list = new StringBuilder();
        for (var i = kept; i < details.Count; i++)
        {
            if (list.Length > 0)
            {
                list.Append("; ");
            }

            list.Append(details[i].Heading);
        }

        var listed = list.ToString();
        var headings = listed.Length <= SlackOmittedHeadingsLimit
            ? listed
            : listed[..SlackCutLength(listed, SlackOmittedHeadingsLimit)] + "...";

        var noun = dropped == 1 ? "detail" : "details";
        var omission = string.Create(CultureInfo.InvariantCulture,
            $"*Omitted from this message*\n{dropped:N0} more {noun} did not fit Slack's {SlackMessageBlockLimit}-block message limit: {headings}{SlackOmissionPointer}");

        if (omissionCost == SlackDetailOmissionCost)
        {
            blocks.Add(new { type = "divider" });
        }

        blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = omission } });
    }

    /// <summary>
    /// One detail's block run: its leading divider, then the shape its kind has always rendered — a
    /// fixed pointer for remediation T-SQL (never inlined on a chat surface), a <c>*Heading*</c>-led
    /// mrkdwn section for a body, record sections for a record-shaped detail (#3644, below), or field
    /// sections (heading first, then <c>*label:*</c> fields) for everything else. #3612 bounds each text
    /// object in the run: the body goes through the prose splitter (<see cref="AddSlackProseSections"/>)
    /// with the heading as its header and <paramref name="bodyBlockBudget"/> as its block budget, and every
    /// field through <see cref="SlackFieldText"/>. A detail inside every cap renders the pre-#3612 bytes.
    /// </summary>
    private static List<object> RenderSlackDetail(AlertDetailItem detail, int bodyBlockBudget)
    {
        var run = new List<object> { new { type = "divider" } };
        var heading = SlackHeading(detail.Heading);

        if (detail.IsCodeBlock)
        {
            /* Remediation T-SQL: point at the email / in-app dialog, never inline it. */
            run.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{heading}*\n{TsqlWebhookHint}" } });
            return run;
        }

        if (!string.IsNullOrEmpty(detail.Body))
        {
            /* Advice prose flows as mrkdwn sections; the synthesized Body is
               "Investigation: ...\n\nRemediation: ..." which Slack renders verbatim. One section when it
               fits the ceiling (every observed body does); split on line boundaries when it does not. */
            AddSlackProseSections(run, $"*{heading}*\n", detail.Body, bodyBlockBudget);
            return run;
        }

        if (detail.Records.Count > 0)
        {
            /* #3644: a repeating record is a list of rows, not a grid of pairs. The fields this detail also
               carries are the same content, and they are what every other surface renders. */
            AddSlackRecordSections(run, heading, detail.Records);
            return run;
        }

        var detailFields = new List<object>();
        detailFields.Add(new { type = "mrkdwn", text = $"*{heading}*" });

        foreach (var (label, value) in detail.Fields)
        {
            detailFields.Add(new { type = "mrkdwn", text = SlackFieldText(label, value) });
        }

        AddSlackFieldSections(run, detailFields);
        return run;
    }

    /// <summary>
    /// A record-shaped detail (#3644) as mrkdwn sections: <c>*Heading*</c> leads the first, and each record
    /// is one visual unit under it — a bold line <c>*#N · Database: … · Total Cpu Ms: 3,088,689 · …*</c>
    /// over its text in a triple-backtick code block — stacked top to bottom. Read live on a production High
    /// CPU page: the same rows as seven <c>fields</c> per query put query #1's text beside query #2's hash and
    /// #3's database beside #2's SQL, because Slack fills a section's <c>fields</c> two across in submission
    /// order; a section's <c>text</c> has one column, so a record cannot be pulled apart by its neighbour,
    /// and a long text dislocates nothing but itself. The code block also takes the SQL OUT of mrkdwn
    /// interpretation, which a field never did (<c>&gt;</c> at a line start is a quote, <c>*</c> pairs
    /// bold).
    ///
    /// <para><b>Block cost: the divider plus as many sections as the records pack into under the
    /// text-object ceiling — ONE for every drill-down the producer emits.</b> #3644 sketched one section per
    /// record (three records, three sections); that costs MORE than the fields did for a narrow record —
    /// three three-attribute rows are ten fields, one section, two blocks, and would become four — and the
    /// #3612 budget is measured in blocks, so a layout fix that widened the card would push incidents off
    /// the page to buy readability. Packing does not: records are appended to the current section while
    /// the section stays inside <see cref="SlackTextObjectLimit"/>, and a record that would cross it opens
    /// the next section (no heading on a continuation — a repeated heading would read as a second detail),
    /// so the cost is <c>1 + ceil(records ÷ what fits)</c>, at most one section per record and never more
    /// than the fields cost. Each record is bounded by <see cref="SlackRecordLimit"/>, which is what
    /// guarantees any record fits a section with the heading; the fixture's six record-shaped drill-downs
    /// (3 rows each, texts at the collector's 500) measure 474 to 2,614 characters and one section apiece,
    /// so the five-fact page's fixed items fall from 33 blocks to 20 and the message from 37 + 2×incidents
    /// to 24 + 2×incidents — thirteen incidents fit where six did, and the three production pages #3612
    /// could deliver only by dropping incidents now deliver whole (<c>SlackDetailsSizeTests</c>). Visually
    /// one section holding three records and three sections holding one each are the same stack; Slack
    /// puts no rule between consecutive sections.</para>
    ///
    /// <para><b>Inside a record.</b> The summary is bounded at <see cref="SlackRecordSummaryLimit"/> with a
    /// trailing ellipsis (it never fires; see the constant), then the texts share what is left of the
    /// record's room equally, each through <see cref="SlackBoundedText"/> so a cut states its count in
    /// characters the reader can count, on a whole character (#3622). One text carries no label — the
    /// heading says what the drill-down is and the summary says whose row this is; "Query Text" would add
    /// nothing — and two or more (a blocking chain's blocked and blocking SQL) are each led by their label
    /// in italics so the reader can tell them apart.</para>
    /// </summary>
    private static void AddSlackRecordSections(List<object> run, string heading, List<AlertDetailRecord> records)
    {
        var section = new StringBuilder("*").Append(heading).Append('*');

        void Flush()
        {
            run.Add(new { type = "section", text = new { type = "mrkdwn", text = section.ToString() } });
            section.Clear();
        }

        foreach (var record in records)
        {
            var unit = SlackRecordText(record);
            if (section.Length + 1 + unit.Length > SlackTextObjectLimit)
            {
                Flush();
                section.Append(unit);
            }
            else
            {
                section.Append('\n').Append(unit);
            }
        }

        Flush();
    }

    /// <summary>One record's unit of text — see <see cref="AddSlackRecordSections"/> — inside
    /// <see cref="SlackRecordLimit"/>.</summary>
    private static string SlackRecordText(AlertDetailRecord record)
    {
        var summary = record.Summary.Length <= SlackRecordSummaryLimit
            ? record.Summary
            : record.Summary[..SlackCutLength(record.Summary, SlackRecordSummaryLimit)] + "...";
        var unit = new StringBuilder();
        unit.Append("*#").Append(record.Ordinal.ToString(CultureInfo.InvariantCulture));
        if (summary.Length > 0)
        {
            unit.Append(" · ").Append(summary);
        }

        unit.Append('*');

        if (record.Texts.Count == 0)
        {
            return unit.ToString();
        }

        /* Each text's share of the record's remaining room, after its own markup: the code fence
           ("\n```\n" + "\n```", nine units) and, when there are several, the italic label line. */
        var labelled = record.Texts.Count > 1;
        var markup = 0;
        foreach (var (label, _) in record.Texts)
        {
            markup += 9 + (labelled ? label.Length + 3 : 0);
        }

        var share = Math.Max(0, (SlackRecordLimit - unit.Length - markup) / record.Texts.Count);
        foreach (var (label, text) in record.Texts)
        {
            if (labelled)
            {
                unit.Append("\n_").Append(label).Append('_');
            }

            unit.Append("\n```\n").Append(SlackBoundedText(string.Empty, text, share)).Append("\n```");
        }

        return unit.ToString();
    }

    /// <summary>A detail heading bounded to <see cref="SlackDetailHeadingLimit"/>; unchanged for every
    /// heading a producer has ever emitted. The cut lands on a whole character (#3622): a heading IS the
    /// detail's identity, and one ending in half an emoji or a letter shorn of its accent names a
    /// different thing.</summary>
    private static string SlackHeading(string heading) =>
        heading.Length <= SlackDetailHeadingLimit ? heading : heading[..SlackCutLength(heading, SlackDetailHeadingLimit)] + "...";

    /// <summary>
    /// One detail field's text object, <c>*label:*</c> over its value, inside
    /// <see cref="SlackFieldTextLimit"/>. A value too long for the field keeps its leading stretch and
    /// states the cut inline — how many characters were omitted and where the whole value lives — the
    /// same posture as the prose and detail omissions, in the field's own text because a field has no
    /// room for a closing line. Two passes size the kept stretch: the first against the longest count
    /// the note could carry, the second against the count it actually carries, so the result never
    /// exceeds the limit. Every field inside the limit renders the pre-#3612 bytes.
    ///
    /// <para>#3622: the kept stretch ends on a whole character (<see cref="SlackCutLength"/>), and the
    /// omitted count is stated in the characters a reader would count — text elements, so one emoji is
    /// one, a letter with its combining accent is one — not in UTF-16 units. Kept plus omitted is the
    /// value's own character count, which is the only arithmetic a reader can check. The count can
    /// only be at or below the UTF-16 count the first sizing pass allowed for, so the note can only be
    /// narrower than the room held for it, and the field stays inside its limit.</para>
    /// </summary>
    private static string SlackFieldText(string label, string value) =>
        SlackBoundedText($"*{label}:*\n", value, SlackFieldTextLimit);

    /// <summary>
    /// <paramref name="prefix"/> over <paramref name="value"/> inside <paramref name="limit"/>, the cut
    /// stated inline when there is one — the mechanics of <see cref="SlackFieldText"/>, which is this at
    /// <see cref="SlackFieldTextLimit"/> with a <c>*label:*</c> prefix, lifted out (#3644) so a record's
    /// code-blocked text (<see cref="SlackRecordText"/>, empty prefix, the record's share) states its cut in
    /// exactly the words and the characters a cut field does. One note, one arithmetic, two sites.
    /// </summary>
    private static string SlackBoundedText(string prefix, string value, int limit)
    {
        if (prefix.Length + value.Length <= limit)
        {
            return prefix + value;
        }

        static string Note(int omitted) => string.Create(CultureInfo.InvariantCulture,
            $"... ({omitted:N0} more characters{SlackOmissionPointer.TrimEnd('.')})");

        /* The label is producer-controlled and short; a label that alone crowds the field is bounded so
           the arithmetic below stays positive. */
        if (prefix.Length > limit / 2)
        {
            prefix = prefix[..SlackCutLength(prefix, limit / 2)];
        }

        var keep = SlackCutLength(value, Math.Max(0, limit - prefix.Length - Note(value.Length).Length));
        /* Counting the omitted characters IS a walk of the omitted tail — the pre-#3622 subtraction counted
           units, which is the thing that was wrong — so this costs the tail's length, once, on the
           truncation path only. Every producer bounds its values upstream (the analysis formatter cuts
           drill-down text at 300 characters; the longest measured field is 318), so the tail is short in
           practice and the cost is the value's own length in the worst case (review note on #3625). */
        var note = Note(new StringInfo(value[keep..]).LengthInTextElements);
        return prefix + value[..keep] + note;
    }

    /// <summary>
    /// The length of the longest leading stretch of <paramref name="text"/> that fits inside
    /// <paramref name="limit"/> UTF-16 units without cutting through a character (#3622). Every Slack text
    /// cut in this builder passes through here — the prose hard split and the omission line's quoted
    /// fragment (#3493); the detail heading, the field label, the field value and the omitted-headings
    /// list (#3612) — because each of them sized its cut in UTF-16 units, and a cut landing between the
    /// two halves of a surrogate pair (any emoji, any supplementary-plane character, in a query text, a
    /// database name or an advice string) left an unpaired surrogate at the boundary. <c>JsonSerializer</c>
    /// does not emit that as a lone escape and Slack does not reject it — measured here, System.Text.Json
    /// substitutes U+FFFD, the same relaxation <see cref="EscapeForJson"/> already relies on — so the
    /// payload delivered, and what it delivered was wrong: the reader saw a replacement glyph (\uFFFD) that
    /// was never in the value, the "leading stretch" a cut field claims to show was no longer a prefix of
    /// the value, the hard split lost the character outright (each half became its own glyph, one per
    /// section), and the field's stated "N more characters" counted UTF-16 units the reader cannot see.
    /// #3622 expected the invalid-payload door; the door it actually opened is a payload that lies.
    ///
    /// <para><b>Grapheme, not code point, at every site.</b> The cut lands on a text-element boundary — an
    /// extended grapheme cluster, the unit a reader counts as one character: an emoji with its skin-tone
    /// modifier, a base letter with its combining accent, a CR LF pair — so no site can leave half a
    /// visible character behind, and the one omission that is stated in characters
    /// (<see cref="SlackFieldText"/>) cuts and counts in the same unit. The recommendation on #3622 was a
    /// code-point cut at the two long-prose sites on cost grounds, and that trade does not exist: the walk
    /// is bounded by <paramref name="limit"/>, not by the text — it stops at the first element that would
    /// cross the limit, and the window it hands the segmenter ends two units past the limit rather than at
    /// the end of the text (see the loop) — so it costs the same at a 120-character fragment as at a
    /// 3,000-character section, and one rule at six sites is cheaper to keep true than two. The one input a whole-element cut cannot
    /// serve is a single element wider than the whole limit — a run of combining marks with no base, the
    /// "Zalgo text" a query comment can carry — where keeping whole elements would keep nothing and the
    /// hard-split loop would never advance; there the cut falls back to the code-point boundary (one unit
    /// before a surrogate pair, else the limit itself), because a valid payload showing a broken glyph
    /// beats a delivery that hangs. Callers pass limits in the hundreds and thousands, so the fallback
    /// always keeps at least one unit.</para>
    /// </summary>
    internal static int SlackCutLength(ReadOnlySpan<char> text, int limit)
    {
        if (text.Length <= limit)
        {
            return text.Length;
        }

        if (limit <= 0)
        {
            return 0;
        }

        /* text.Length > limit here, so the window is never empty while cut <= limit, and every element is
           at least one unit wide, so the walk terminates at the first element that would cross.

           The window handed to the segmenter ends two units past the limit, not at the end of the text.
           Two units hold any scalar that straddles the limit whole, and grapheme boundaries are decided
           between one scalar and the next (every rule's context is to the LEFT), so every boundary
           decision at or before the limit is the one the full text would make: an element that ends
           inside the window ends where the full text ends it, and an element that reaches the window's
           end has crossed the limit however the full text would segment the rest of it. That is what
           makes the cost claim in the doc block true — without the window, one run of combining marks
           makes this call scan to the end of the text before concluding that it crosses (review note on
           #3625). */
        var cut = 0;
        while (true)
        {
            var window = text.Slice(cut, Math.Min(text.Length - cut, limit - cut + 2));
            var element = StringInfo.GetNextTextElementLength(window);
            if (cut + element > limit)
            {
                break;
            }

            cut += element;
        }

        if (cut > 0)
        {
            return cut;
        }

        /* No whole element fits: a code-point boundary keeps the payload valid and the caller moving. */
        return char.IsHighSurrogate(text[limit - 1]) && char.IsLowSurrogate(text[limit]) ? limit - 1 : limit;
    }

    /// <summary>
    /// Builds a Slack incoming webhook payload with a colored attachment sidebar.
    /// Uses Slack Block Kit for rich formatting.
    /// <para>#2710: a non-null <paramref name="triageUrl"/> adds an actions block with a LINK button (a url
    /// button needs no interactivity config on the webhook, unlike an action_id button) pointing at the
    /// computed triage page, placed above the "Sent by" context footer. Null renders the pre-#2710 payload.</para>
    /// <para>#3297: <paramref name="detailText"/> is the alert's flat prose detail, rendered as mrkdwn
    /// <c>section</c> blocks directly under the field block — Block Kit's home for prose, and above
    /// the per-incident dividers so the remedy reads before the drill-down. #3493: as many section blocks
    /// as its size needs rather than one, each inside Slack's per-text-object ceiling and all of them
    /// inside the message's block budget — see <see cref="AddSlackProseSections"/>.</para>
    /// <para>#3612: the structured details are under the same discipline — every body through the same
    /// splitter, every field inside the field cap, and the whole run inside the block budget the head and
    /// footer leave, dropping whole details from the end with the omission stated — see
    /// <see cref="AddSlackDetailBlocks"/>. The message never exceeds <see cref="SlackMessageBlockLimit"/>
    /// blocks by construction, whichever half is heavy.</para>
    /// </summary>
    internal static string BuildSlackPayload(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertBranding branding,
        bool isTest = false,
        AlertContext? context = null,
        string? triageUrl = null,
        string? detailText = null,
        string? displayName = null,
        DateTime? nowUtc = null)
    {
        var (hexColor, badgeText, emoji) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var prose = AlertDetailText.ProseForDelivery(detailText, context);
        /* Human name in the header when present; ForMetric above keeps the immutable metric-name key. */
        var titleName = string.IsNullOrEmpty(displayName) ? metricName : displayName;
        /* One instant, local derived from it — see the Teams builder's note. */
        var utcNow = nowUtc ?? DateTime.UtcNow;
        var localNow = DateTime.SpecifyKind(utcNow, DateTimeKind.Utc).ToLocalTime();

        var title = isTest
            ? $"{emoji} TEST — {metricName}"
            : $"{emoji} {badgeText} — {titleName}";

        var blocks = new List<object>
        {
            new
            {
                type = "header",
                text = new { type = "plain_text", text = title, emoji = true }
            }
        };

        var fields = new List<object>();

        if (isTest)
        {
            fields.Add(new { type = "mrkdwn", text = "*Status:*\nWebhook configuration is working correctly" });
            fields.Add(new { type = "mrkdwn", text = $"*Sent at:*\n{localNow:yyyy-MM-dd HH:mm:ss}" });
        }
        else
        {
            fields.Add(new { type = "mrkdwn", text = $"*Server:*\n{serverName}" });
            var resourceName = DeriveResourceName(context);
            if (resourceName is not null)
                fields.Add(new { type = "mrkdwn", text = $"*Resource:*\n{resourceName}" });
            var database = DeriveResourceDatabase(context);
            if (!string.IsNullOrEmpty(database))
                fields.Add(new { type = "mrkdwn", text = $"*Database:*\n{database}" });
            fields.Add(new { type = "mrkdwn", text = $"*Current Value:*\n{currentValue}" });
            fields.Add(new { type = "mrkdwn", text = $"*Threshold:*\n{thresholdValue}" });
            fields.Add(new { type = "mrkdwn", text = $"*Time (UTC):*\n{utcNow:yyyy-MM-dd HH:mm:ss}" });
            fields.Add(new { type = "mrkdwn", text = $"*Time (Local):*\n{localNow:yyyy-MM-dd HH:mm:ss}" });
        }

        AddSlackFieldSections(blocks, fields);

        /* #3493 / #3612: the message is composed in three runs and budgeted in the order of what is
           FIXED first. The head (above: one header block and one field section, since the lead fields
           never exceed ten) and the footer (below: the triage button when there is one, and the context
           line — one or two blocks) are small and cannot yield; the structured details are bounded next,
           against what the head and footer leave under the 50-block limit less one block held for the
           prose when there is one; the prose is packed last into exactly what remains. Composed out of
           visual order because a budget can only count blocks that already exist. The visual order is
           unchanged: head, prose, details, footer — exactly where each has always rendered. */
        var footerBlocks = new List<object>();

        /* #2710: the triage-page link button, above the footer so it reads as part of the alert rather than
           the boilerplate. A url button opens the link directly with no Slack app interactivity required. */
        if (triageUrl is not null)
        {
            footerBlocks.Add(new
            {
                type = "actions",
                elements = new object[]
                {
                    new
                    {
                        type = "button",
                        text = new { type = "plain_text", text = TriageLink.LinkLabel(triageUrl), emoji = false },
                        url = triageUrl
                    }
                }
            });
        }

        var contextElements = new List<object>
        {
            new { type = "mrkdwn", text = $"Sent by {branding.EditionName}" }
        };
        if (!isTest && branding.SnoozeHint is not null)
        {
            contextElements.Add(new { type = "mrkdwn", text = branding.SnoozeHint });
        }

        footerBlocks.Add(new
        {
            type = "context",
            elements = contextElements
        });

        /* #3612: the structured details, inside the budget the head and footer leave — less the one
           block the prose needs to state itself when there is prose. The analysis pages that were lost
           carry their whole finding here and deliver no prose, so on them this budget is the fix; see
           AddSlackDetailBlocks for what drops first and how the omission is stated. */
        var detailBlocks = new List<object>();
        if (context?.Details is { Count: > 0 } details)
        {
            var proseReserve = prose is not null ? 1 : 0;
            AddSlackDetailBlocks(detailBlocks, details,
                blockBudget: SlackMessageBlockLimit - blocks.Count - footerBlocks.Count - proseReserve);
        }

        /* #3297: the prose detail, before the per-incident dividers; #3493: split across as many section
           blocks as its size needs, inside the block budget the head, details and footer leave over —
           at least one, because the details reserved it. */
        if (prose is not null)
        {
            AddSlackProseSections(blocks, SlackProseHeader, prose,
                blockBudget: SlackMessageBlockLimit - blocks.Count - detailBlocks.Count - footerBlocks.Count);
        }

        blocks.AddRange(detailBlocks);
        blocks.AddRange(footerBlocks);

        var payload = new
        {
            attachments = new object[]
            {
                new { color = hexColor, blocks }
            }
        };

        return JsonSerializer.Serialize(payload, s_jsonOptions);
    }

    #endregion

    #region Generic

    /// <summary>Posts to the generic channel. Null when the post succeeded, the error text when it did not
    /// — see <see cref="TrySendTeamsAlertAsync"/>. A malformed headers JSON or body template reports here
    /// too: an operator config error still delivers nothing, and naming it is the difference between a
    /// fixable row and a bare "failed".</summary>
    private async Task<string?> TrySendGenericAlertAsync(
        string webhookUrl,
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId,
        AlertContext? context,
        string? triageUrl,
        string? detailText,
        DateTime nowUtc)
    {
        try
        {
            /* A malformed headers JSON / body template is an operator config error, not a transport
               failure — but it still counts as a failure so the health surface and the log throttle
               report a channel that is delivering nothing, and it must never throw into the alert loop. */
            if (!TryParseHeaders(_settings.GenericWebhookHeadersJson, out var headers, out var headerError))
            {
                RecordGenericFailure(headerError!);
                return headerError;
            }

            var payload = BuildGenericPayload(
                metricName, serverName, currentValue, thresholdValue, _branding,
                context: context, bodyTemplate: _settings.GenericWebhookBodyTemplate, serverId: serverId,
                triageUrl: triageUrl, detailText: detailText, nowUtc: nowUtc);

            if (!IsWellFormedJson(payload, out var bodyError))
            {
                RecordGenericFailure(bodyError!);
                return bodyError;
            }

            /* #3598: the routed endpoint; headers, body template and proxy stay the parent's — a route
               redirects the POST, it does not re-author it. */
            var error = await PostWebhookAsync(
                webhookUrl, payload, _settings.GenericWebhookProxyAddress, headers);

            if (error != null)
            {
                RecordGenericFailure(error);
                return error;
            }

            if (_consecutiveGenericFailures > 0)
                _logger.LogInformation($"Generic webhook recovered after {_consecutiveGenericFailures} failure(s)");

            _consecutiveGenericFailures = 0;
            _lastGenericError = null;
            _logger.LogInformation($"Generic webhook sent for {metricName} on {serverName}");
            return null;
        }
        catch (Exception ex)
        {
            _consecutiveGenericFailures++;
            _lastGenericError = ex.Message;
            _logger.LogError($"Generic webhook error: {ex.Message}");
            return ex.Message;
        }
    }

    /* The Teams/Slack log-throttle shape (loud for the first 3, then every 50th) — factored out only
       because the generic channel fails from three places (headers, body, transport). */
    private void RecordGenericFailure(string error)
    {
        _consecutiveGenericFailures++;
        _lastGenericError = error;

        if (_consecutiveGenericFailures <= 3)
            _logger.LogError($"GENERIC WEBHOOK FAILED ({_consecutiveGenericFailures}x): {error}");
        else if (_consecutiveGenericFailures % 50 == 0)
            _logger.LogError($"GENERIC WEBHOOK STILL FAILING: {_consecutiveGenericFailures} failures. Last: {error}");
    }

    /// <summary>
    /// Substitutes the alert's values into the operator's JSON body template. Every placeholder expands to
    /// JSON-ESCAPED text: the tokens sit inside JSON string literals in the template
    /// (<c>"server": "{{server}}"</c>), so a server name, wait type, or error message containing a quote or
    /// backslash would otherwise terminate the literal early and corrupt — or inject into — the posted JSON.
    /// The escaping goes through <see cref="JsonSerializer"/> (see <see cref="EscapeForJson"/>) — NOT
    /// <c>JsonEncodedText.Encode</c>, which throws on the lone surrogates SQL Server names can carry — and the
    /// two surrounding quotes are stripped to leave exactly the escaped INNER text a token inside a literal needs.
    /// <para>
    /// #2302: the three automation tokens are the exception, and two of them deliberately BYPASS the
    /// escaping. <c>{{context_json}}</c> / <c>{{incidents_json}}</c> are raw JSON VALUES substituted
    /// unquoted (<c>"context": {{context_json}}</c>) — escaping them would turn structure back into the
    /// flattened string the token exists to replace. Their shape is the SAME
    /// <see cref="AlertContextSerializer"/> projection persisted as the alert-history ContextJson, so a
    /// consumer parses one shape whether it reads the webhook or the history row — except that code-block
    /// bodies and remediation payloads are redacted first (<see cref="RedactForWebhook"/>): the copy-paste
    /// T-SQL follows the same never-on-a-webhook rule as every other channel here. <c>{{dedup_key}}</c> is
    /// an ordinary escaped string carrying the same key the PagerDuty channel derives — including its
    /// stable metric+server fallback when an alert has no incident — so tickets correlate across channels.
    /// A template that quotes a raw token anyway produces malformed JSON and is caught by the caller's
    /// well-formedness check, surfacing as a config error rather than a silent bad post.
    /// </para>
    /// <para>
    /// #2710: <c>{{resource_name}}</c> / <c>{{database}}</c> are ordinary escaped strings, empty when the
    /// alert carries no fingerprintable incident — a template author already has the same data via
    /// <c>{{incidents_json}}</c>, but these two save hand-parsing JSON for the common case of one
    /// Datadog-shaped <c>resource_name:</c> tag. Same "first incident" derivation as every other channel
    /// here (<see cref="DeriveResourceName"/> / <see cref="DeriveResourceDatabase"/>). <c>{{triage_url}}</c>
    /// is likewise an ordinary escaped string — the SAME computed triage-page link the Teams/Slack/PagerDuty
    /// channels carry (<see cref="TriageLink.Build"/>), empty when no <see cref="IAlertSettings.TriageBaseUrl"/>
    /// is configured, so a template using it stays well-formed either way.
    /// </para>
    /// <para>
    /// #3297: <c>{{detail}}</c> is the alert's flat prose detail — what happened and the operator action
    /// that clears it — as an ordinary escaped string, empty when the alert carries no prose over and above
    /// its structured context. It ALSO leads <c>{{context}}</c>: the default template and every template an
    /// operator already saved carry <c>{{context}}</c> and none of them can carry a token that did not
    /// exist when they were written, so a token alone would have left every existing generic-channel
    /// deployment still dropping the detail. <c>{{detail}}</c> exists on top of that for a template that
    /// needs the prose on its own — mapped into a ticket body field, say — rather than mixed with the
    /// flattened structure.
    /// </para>
    /// </summary>
    internal static string BuildGenericPayload(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertBranding branding,
        bool isTest = false,
        AlertContext? context = null,
        string? bodyTemplate = null,
        string serverId = "",
        string? triageUrl = null,
        string? detailText = null,
        DateTime? nowUtc = null)
    {
        var (_, badgeText, _) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var template = string.IsNullOrWhiteSpace(bodyTemplate) ? DefaultGenericBodyTemplate : bodyTemplate!;

        var prose = AlertDetailText.ProseForDelivery(detailText, context);

        var contextText = isTest
            ? $"Webhook configuration is working correctly. Sent by {branding.EditionName}."
            : RenderContextForTemplate(context, branding, prose);

        /* Keyed on the numeric serverId the fan-out passes — the same identity the LIVE PagerDuty path
           feeds DerivePagerDutyDedupKey — so the two channels' keys are equal for the same alert. The
           serverName arm is THIS channel's own fallback for callers with no id (the settings-window test
           send); it is not a guarantee PagerDuty's path shares, so a caller wanting cross-channel
           correlation must pass the id. */
        var dedupKey = DerivePagerDutyDedupKey(
            string.IsNullOrEmpty(serverId) ? serverName : serverId, metricName, context);

        var values = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["metric"] = EscapeForJson(isTest ? $"TEST — {metricName}" : metricName),
            ["server"] = EscapeForJson(serverName),
            ["value"] = EscapeForJson(currentValue),
            ["threshold"] = EscapeForJson(thresholdValue),
            ["severity"] = EscapeForJson(isTest ? "TEST" : badgeText),
            ["context"] = EscapeForJson(contextText),
            ["timestamp"] = EscapeForJson((nowUtc ?? DateTime.UtcNow).ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)),
            /* Raw JSON values — never EscapeForJson (see the doc comment). "{}" / "[]" rather than empty
               so a template's `"context": {{context_json}}` stays well-formed on a context-less alert.
               Serialized from a REDACTED copy: the copy-paste remediation T-SQL never leaves the process
               on any webhook channel, and a raw token is not an exception to that rule. */
            ["context_json"] = context is null ? "{}" : AlertContextSerializer.Serialize(RedactForWebhook(context)),
            ["incidents_json"] = AlertContextSerializer.SerializeIncidents(context),
            ["dedup_key"] = EscapeForJson(dedupKey),
            ["resource_name"] = EscapeForJson(DeriveResourceName(context) ?? ""),
            ["database"] = EscapeForJson(DeriveResourceDatabase(context) ?? ""),
            ["triage_url"] = EscapeForJson(triageUrl ?? ""),
            /* The test send substitutes the same canned line {{context}} gets, so a template that maps
               {{detail}} into a required field is exercised with a value rather than validating against an
               empty string and only failing on the first real alert. */
            ["detail"] = EscapeForJson(isTest ? $"Webhook configuration is working correctly. Sent by {branding.EditionName}." : prose ?? ""),
        };

        /* Single pass: a MatchEvaluator's output is NOT re-scanned, so a value that itself contains the
           literal text of another token (e.g. a server name "{{timestamp}}") is left verbatim rather than
           re-expanded — which chained .Replace() calls would do, letting one field pull another's contents
           into itself. An unknown "{{foo}}" isn't in the alternation, so it stays literal (today's behavior). */
        return s_genericPlaceholders.Replace(template, m => values[m.Groups[1].Value]);
    }

    /// <summary>
    /// Every token <see cref="BuildGenericPayload"/> substitutes, in the order the matcher tries them.
    /// <para>context_json before context: alternation is ordered, and while the closing <c>}}</c> would
    /// force a backtrack to the right answer anyway, longest-first means correctness never leans on it.</para>
    /// <para>A LIST, and the matcher is built from it, because both apps' Settings windows print the token
    /// set as help text and an operator cannot use a token they never learn exists. That help text has now
    /// drifted twice — #2710 added <c>triage_url</c> to Darling's list and not Lite's, and #3297 added
    /// <c>detail</c> to neither — so <c>Lite.Tests.GenericWebhookTests</c> checks both windows against this
    /// list rather than against a second copy of it that would be free to drift the same way.</para>
    /// </summary>
    internal static readonly string[] GenericBodyTokens =
    {
        "metric", "server", "value", "threshold", "severity",
        "context_json", "incidents_json", "dedup_key", "resource_name", "database", "triage_url",
        "context", "timestamp", "detail"
    };

    private static readonly System.Text.RegularExpressions.Regex s_genericPlaceholders =
        new(@"\{\{(" + string.Join("|", GenericBodyTokens) + @")\}\}",
            System.Text.RegularExpressions.RegexOptions.Compiled);

    /// <summary>
    /// Flattens the structured alert context into one plain-text line for <c>{{context}}</c> — the generic
    /// endpoint has no card schema to render into. Follows the Teams/Slack rule: remediation T-SQL is never
    /// inlined, it points at the email / in-app dialog.
    /// <para>#3297: <paramref name="prose"/>, the alert's flat detail, leads the line when present. A
    /// prose-only alert used to render this as the bare "Sent by ..." boilerplate — the whole alert body
    /// reduced to a signature.</para>
    /// </summary>
    private static string RenderContextForTemplate(AlertContext? context, AlertBranding branding, string? prose = null)
    {
        var parts = new List<string>();

        if (prose is not null)
        {
            parts.Add(prose.Replace("\r\n", " ", StringComparison.Ordinal).Replace("\n", " ", StringComparison.Ordinal));
        }

        if (context?.Details is not { Count: > 0 })
        {
            return parts.Count == 0 ? $"Sent by {branding.EditionName}" : string.Join(" | ", parts);
        }

        foreach (var detail in context.Details)
        {
            if (detail.IsCodeBlock)
            {
                parts.Add($"{detail.Heading}: {TsqlWebhookHint}");
                continue;
            }

            if (!string.IsNullOrEmpty(detail.Body))
            {
                parts.Add($"{detail.Heading}: {detail.Body.Replace("\n\n", " ", StringComparison.Ordinal).Replace("\n", " ", StringComparison.Ordinal)}");
                continue;
            }

            foreach (var (label, value) in detail.Fields)
            {
                parts.Add($"{detail.Heading} — {label}: {value}");
            }
        }

        return parts.Count == 0 ? $"Sent by {branding.EditionName}" : string.Join(" | ", parts);
    }

    /// <summary>
    /// The webhook posture applied to structure (#2302 review catch): every channel in this file replaces
    /// copy-paste remediation T-SQL with <see cref="TsqlWebhookHint"/> before anything leaves the process —
    /// Teams, Slack, PagerDuty's custom_details, and this channel's own <c>{{context}}</c> flattening — and
    /// a raw-JSON token is not an exception. Code-block items keep their heading and the flag (so a consumer
    /// can see a remediation EXISTS) but carry the hint as their body and no <c>Remediation</c> payload; the
    /// typed payload is likewise stripped from every item defensively. Returns a COPY — the same context
    /// instance flows on to the other channels, and mutating it here would redact their email too.
    /// <para>#3297: this covers the STRUCTURED context, which is the whole of what it has ever protected —
    /// generated <c>FactRemediation</c> payloads. An alert's flat prose detail is delivered verbatim and
    /// deliberately; see <see cref="AlertDetailText"/> for why, and for the pin that keeps a generated
    /// command out of it.</para>
    /// </summary>
    private static AlertContext RedactForWebhook(AlertContext context)
    {
        var redacted = new AlertContext { Incidents = context.Incidents };
        foreach (var detail in context.Details)
        {
            if (detail.IsCodeBlock)
            {
                redacted.Details.Add(new AlertDetailItem
                {
                    Heading = detail.Heading,
                    Body = TsqlWebhookHint,
                    IsCodeBlock = true
                });
                continue;
            }

            if (detail.Remediation is null)
            {
                redacted.Details.Add(detail);
                continue;
            }

            redacted.Details.Add(new AlertDetailItem
            {
                Heading = detail.Heading,
                Fields = detail.Fields,
                /* #3644: the copy is the whole item less its payload. Inert today — this copy is only
                   serialized, and the serializer carries fields, not records — but a copy that silently
                   dropped a member is the kind of drift the next renderer of this copy would inherit. */
                Records = detail.Records,
                Body = detail.Body,
                IsCodeBlock = false
            });
        }

        return redacted;
    }

    /// <summary>
    /// Escapes a value for interpolation INSIDE a JSON string literal — the escaped inner text, without the
    /// surrounding quotes. HTML-sensitive and non-ASCII characters escape to <c>\uXXXX</c>; over-escaping is
    /// still valid JSON and keeps the payload ASCII-safe.
    /// <para>
    /// Uses <see cref="JsonSerializer"/> rather than <see cref="JsonEncodedText.Encode(string)"/> deliberately:
    /// SQL Server nvarchar/sysname can hold a LONE SURROGATE (e.g. an object or login name containing
    /// <c>NCHAR(0xD800)</c>, or a RAISERROR message), and <c>JsonEncodedText.Encode</c> THROWS
    /// <see cref="ArgumentException"/> on invalid UTF-16 — which would let a low-privileged user drop the
    /// generic webhook for the very incident they cause while email/Teams/Slack (which use
    /// <c>JsonSerializer</c>) still deliver. The serializer relaxes invalid UTF-16 to U+FFFD and never throws,
    /// matching the sibling channels. The result is a quoted JSON string; strip the two surrounding quotes to
    /// get the inner text a token inside a literal needs.
    /// </para>
    /// </summary>
    private static string EscapeForJson(string? value)
    {
        var quoted = JsonSerializer.Serialize(value ?? "");
        return quoted.Substring(1, quoted.Length - 2);
    }

    /// <summary>
    /// Validates the generic channel's headers JSON + body template without sending anything — for the
    /// settings windows' Save path, so a typo is caught where the operator can fix it rather than silently
    /// dropping every future alert. Returns null when the config is usable, else the error to show.
    /// Both inputs are optional: empty headers mean "no custom headers", an empty template means
    /// <see cref="DefaultGenericBodyTemplate"/>.
    /// </summary>
    public static string? ValidateGenericConfig(string? headersJson, string? bodyTemplate)
    {
        if (!TryParseHeaders(headersJson, out _, out var headerError))
        {
            return headerError;
        }

        /* Render with placeholder-shaped stand-ins: substitution is what can break the JSON, so validating
           the raw template would miss a token sitting outside a string literal. The stand-in CONTEXT is
           what makes the raw tokens honest here (#2310 review catch): with a null context they render to
           the quote-free `{}` / `[]`, so a mis-quoted raw token — `"context": "{{context_json}}"` —
           validates clean and only breaks on the first real alert that carries structure. */
        var rendered = BuildGenericPayload(
            "Test Notification", "Test Server", "0", "0",
            new AlertBranding("Performance Monitor", null), isTest: true, bodyTemplate: bodyTemplate,
            context: ValidationStandInContext());

        return IsWellFormedJson(rendered, out var bodyError) ? null : bodyError;
    }

    /// <summary>
    /// The stand-in context Save-time validation and the settings Test send render the raw tokens with.
    /// Its serialization is guaranteed to contain double quotes (every JSON property name carries them),
    /// so quoting a raw token in a template breaks <see cref="IsWellFormedJson"/> at Save/Test — where
    /// the operator can see and fix it — instead of validating clean against the trivial <c>{}</c> and
    /// failing silently into the log on the first deadlock alert with real structure. One incident, so
    /// <c>{{incidents_json}}</c> is exercised the same way.
    /// </summary>
    internal static AlertContext ValidationStandInContext() => new()
    {
        Details =
        {
            new AlertDetailItem
            {
                Heading = "Validation",
                Fields = { ("Check", "stand-in \"quoted\" value") }
            }
        },
        Incidents = new List<AlertIncident>
        {
            new("0000000000000000", new List<string> { "validation.dbo.stand_in" })
        }
    };

    /// <summary>
    /// True when the text is the built-in default body template (newline-insensitive), or empty. The settings
    /// windows pre-fill the body box with <see cref="DefaultGenericBodyTemplate"/> when nothing is stored, so
    /// Save uses this to persist the empty "use the default" sentinel instead of freezing a copy of today's
    /// default — which would otherwise lock that operator out of future default improvements.
    /// </summary>
    public static bool IsDefaultBodyTemplate(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
        {
            return true;
        }

        static string Normalize(string s) => s.Replace("\r\n", "\n", StringComparison.Ordinal).Replace("\r", "\n", StringComparison.Ordinal).Trim();
        return string.Equals(Normalize(text), Normalize(DefaultGenericBodyTemplate), StringComparison.Ordinal);
    }

    /// <summary>
    /// True when the generic webhook would send credentials in the clear: an <c>http://</c> (not https) URL
    /// carrying at least one header — the headers hold the <c>Authorization</c> bearer token, which a plaintext
    /// POST exposes to anyone on the path. The settings windows use this to prompt a non-blocking confirm at
    /// Save/Test (it is NOT blocked: a plaintext POST to a trusted LAN listener is legitimate). Returns false
    /// for https, for a headerless http URL, and for a URL that does not parse (the send surfaces its own error).
    /// </summary>
    public static bool IsCleartextHttpWithHeaders(string? url, string? headersJson)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri)
            || !string.Equals(uri.Scheme, "http", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        /* Only warn when a header is actually present; a malformed headers JSON is caught by its own
           validation path, so treat "doesn't parse" as "no header to expose here". */
        return TryParseHeaders(headersJson, out var headers, out _) && headers.Count > 0;
    }

    /// <summary>
    /// Parses the operator's headers JSON object into request headers. An empty/whitespace value is valid
    /// (no custom headers). Malformed JSON, a non-object root, or a non-string value returns false with a
    /// clear message the caller reports — the alert loop never sees an exception.
    /// </summary>
    internal static bool TryParseHeaders(
        string? headersJson,
        out Dictionary<string, string> headers,
        out string? error)
    {
        headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        error = null;

        if (string.IsNullOrWhiteSpace(headersJson))
        {
            return true;
        }

        try
        {
            using var document = JsonDocument.Parse(headersJson);

            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                error = "Webhook headers must be a JSON object, e.g. {\"Authorization\": \"Bearer <token>\"}.";
                return false;
            }

            foreach (var property in document.RootElement.EnumerateObject())
            {
                if (property.Value.ValueKind != JsonValueKind.String)
                {
                    error = $"Webhook header '{property.Name}' must have a string value.";
                    return false;
                }

                var value = property.Value.GetString() ?? "";

                /* Reject CR/LF in the name or value. HttpRequestHeaders.TryAddWithoutValidation lets a CR/LF
                   in a VALUE through onto the wire, splitting one header into two — so a bearer token pasted
                   with a stray newline would silently smuggle a garbage header. Caught here (which runs at
                   Save/Test via ValidateGenericConfig) the operator sees it immediately. */
                if (HasControlChar(property.Name) || HasControlChar(value))
                {
                    error = $"Webhook header '{property.Name}' may not contain control characters (e.g. CR or LF).";
                    return false;
                }

                headers[property.Name] = value;
            }

            return true;
        }
        catch (JsonException ex)
        {
            error = $"Webhook headers are not valid JSON: {ex.Message}";
            return false;
        }
    }

    /// <summary>True when the string contains any C0/C1 control character (CR, LF, tab, NUL, …).</summary>
    private static bool HasControlChar(string s)
    {
        foreach (var c in s)
        {
            if (char.IsControl(c))
            {
                return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Validates the substituted body before it goes on the wire, so a typo in the operator's template is
    /// reported as a config error (in the settings test, or the channel's health) rather than as an opaque
    /// 400 from the endpoint.
    /// </summary>
    private static bool IsWellFormedJson(string payload, out string? error)
    {
        try
        {
            using var _ = JsonDocument.Parse(payload);
            error = null;
            return true;
        }
        catch (JsonException ex)
        {
            error = $"Webhook body template did not produce valid JSON: {ex.Message}";
            return false;
        }
    }

    #endregion

    #region PagerDuty

    /// <summary>Posts to PagerDuty Events v2. Null when the post succeeded, the error text when it did not
    /// — see <see cref="TrySendTeamsAlertAsync"/>.</summary>
    private async Task<string?> TrySendPagerDutyAlertAsync(
        string routingKey,
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId,
        AlertContext? context,
        string? triageUrl,
        string? detailText,
        DateTime nowUtc,
        string? displayName = null)
    {
        try
        {
            /* Derive the dedup_key from the same fingerprint the cooldown uses, so repeated alerts for
               the same ongoing incident correlate into one PagerDuty alert. Falls back to a stable
               metric+server key when there is no incident. */
            var dedupKey = DerivePagerDutyDedupKey(serverId, metricName, context);

            /* #3598: the routed routing key (a PagerDuty SERVICE is a destination); the EU-region flag and
               proxy stay the parent's. */
            var payload = BuildPagerDutyPayload(
                metricName, serverName, currentValue, thresholdValue, _branding,
                routingKey, context: context, dedupKey: dedupKey, triageUrl: triageUrl,
                detailText: detailText, displayName: displayName, nowUtc: nowUtc);

            var endpoint = PagerDutyEndpoint(_settings.PagerDutyUseEuRegion);
            var error = await PostWebhookAsync(endpoint, payload, _settings.PagerDutyProxyAddress);

            if (error != null)
            {
                _consecutivePagerDutyFailures++;
                _lastPagerDutyError = error;

                if (_consecutivePagerDutyFailures <= 3)
                    _logger.LogError($"PAGERDUTY WEBHOOK FAILED ({_consecutivePagerDutyFailures}x): {error}");
                else if (_consecutivePagerDutyFailures % 50 == 0)
                    _logger.LogError($"PAGERDUTY WEBHOOK STILL FAILING: {_consecutivePagerDutyFailures} failures. Last: {error}");

                return error;
            }

            if (_consecutivePagerDutyFailures > 0)
                _logger.LogInformation($"PagerDuty webhook recovered after {_consecutivePagerDutyFailures} failure(s)");

            _consecutivePagerDutyFailures = 0;
            _lastPagerDutyError = null;
            _logger.LogInformation($"PagerDuty webhook sent for {metricName} on {serverName}");
            return null;
        }
        catch (Exception ex)
        {
            _consecutivePagerDutyFailures++;
            _lastPagerDutyError = ex.Message;
            _logger.LogError($"PagerDuty webhook error: {ex.Message}");
            return ex.Message;
        }
    }

    /// <summary>
    /// Builds a PagerDuty Events API v2 payload. Always sends event_action: "trigger" (no resolve wiring —
    /// matches Teams/Slack/Generic which also don't deliver "Cleared" notifications). The dedup_key correlates
    /// repeated triggers for the same ongoing incident into one PagerDuty alert.
    /// <para>#2710: a non-null <paramref name="triageUrl"/> rides in BOTH the Events v2 <c>links</c> array
    /// (which PD renders as a first-class link on the alert) and <c>custom_details["Triage"]</c> (so an
    /// integration reading only the details table still gets it). Null renders the pre-#2710 payload — no
    /// empty <c>links</c> key is ever sent.</para>
    /// <para>#3297: <paramref name="detailText"/>, the alert's flat prose detail, rides in
    /// <c>custom_details["Details"]</c>. Not in <c>summary</c>: PD-CEF caps that at 1024 characters and it
    /// is the one-line headline PD pages on, while custom_details is the table view and what most
    /// downstream integrations read — the same reasoning that put the triage link there.</para>
    /// </summary>
    internal static string BuildPagerDutyPayload(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertBranding branding,
        string routingKey,
        bool isTest = false,
        AlertContext? context = null,
        string? dedupKey = null,
        string? serverId = null,
        string? triageUrl = null,
        string? detailText = null,
        string? displayName = null,
        DateTime? nowUtc = null)
    {
        var (_, badgeText, _) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var severity = MapToPagerDutySeverity(badgeText);
        /* The PD summary (the incident title) shows the human name when present; severity above and the
           dedup_key below stay on the immutable metric name so correlation/dedup are rename-safe. */
        var titleName = string.IsNullOrEmpty(displayName) ? metricName : displayName;
        var utcNow = nowUtc ?? DateTime.UtcNow;

        /* PD-CEF caps summary at 1024 chars — no truncation needed given the source strings, but document
           the constraint matching this codebase's habit of documenting limits even when unreachable. */
        var summary = isTest
            ? "Webhook configuration verified"
            : $"{titleName} on {serverName}: {currentValue} (threshold {thresholdValue})";

        var source = isTest
            ? branding.EditionName
            : serverName;

        var customDetails = BuildPagerDutyCustomDetails(
            isTest, branding, context, triageUrl, AlertDetailText.ProseForDelivery(detailText, context));

        /* Derive dedup_key from the incident fingerprint when not explicitly provided, falling back to a
           stable metric+server key. This ensures PagerDuty correlates repeated alerts for the same incident. */
        var effectiveDedupKey = dedupKey ?? DerivePagerDutyDedupKey(serverId ?? serverName, metricName, context);

        /* A string-keyed dictionary rather than the previous anonymous type, so the #2710 links array can be
           present-or-absent (Events v2 accepts links: [] but an absent key is the honest "no link" shape and
           keeps the linkless payload byte-identical to pre-#2710). Insertion order is preserved by
           Dictionary in practice but nothing here depends on key order. */
        var payload = new Dictionary<string, object>
        {
            ["routing_key"] = routingKey,
            ["event_action"] = "trigger",
            ["dedup_key"] = effectiveDedupKey,
            ["payload"] = new
            {
                summary,
                source,
                severity,
                timestamp = utcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture),
                component = "SQL Server Performance Monitor",
                custom_details = customDetails
            },
            ["client"] = branding.EditionName
        };

        if (triageUrl is not null)
        {
            payload["links"] = new object[] { new { href = triageUrl, text = TriageLink.LinkLabel(triageUrl) } };
        }

        return JsonSerializer.Serialize(payload, s_jsonOptions);
    }

    /// <summary>
    /// Maps the internal AlertSeverity badge text to PagerDuty's four-level severity enum.
    /// CRITICAL → critical, ALERT → error, WARNING → warning, RESOLVED/INFO/other → info.
    /// </summary>
    private static string MapToPagerDutySeverity(string badgeText)
    {
        return badgeText.ToUpperInvariant() switch
        {
            "CRITICAL" => "critical",
            "ALERT" => "error",
            "WARNING" => "warning",
            _ => "info"
        };
    }

    /// <summary>
    /// Builds the custom_details object for the PagerDuty payload. Flattens context details the same way
    /// Teams/Slack do (T-SQL code blocks → TsqlWebhookHint, never inlined), but as a JSON object (key/value
    /// pairs) rather than a flattened string, since PD-CEF's custom_details renders as a table in the UI.
    /// </summary>
    private static Dictionary<string, object> BuildPagerDutyCustomDetails(
        bool isTest,
        AlertBranding branding,
        AlertContext? context,
        string? triageUrl = null,
        string? prose = null)
    {
        var details = new Dictionary<string, object>();

        if (isTest)
        {
            details["Status"] = "Webhook configuration is working correctly";
            details["Sent by"] = branding.EditionName;
            return details;
        }

        /* #2710: Datadog-parity tags, added before the empty-Details early return so an alert type
           that ever carries Incidents without a matching Details item (none do today — Apply and
           BuildDeadlockContext always render one alongside — but nothing enforces that pairing)
           still gets them. The triage link rides here TOO (not only in the top-level links array),
           because custom_details is what PD's table view and most downstream integrations read. */
        var resourceName = DeriveResourceName(context);
        if (resourceName is not null)
            details["Resource"] = resourceName;
        var database = DeriveResourceDatabase(context);
        if (!string.IsNullOrEmpty(database))
            details["Database"] = database;
        if (triageUrl is not null)
            details["Triage"] = triageUrl;

        /* #3297: before the per-incident keys and before the empty-Details return, which is the branch a
           prose-only self-alert takes — the branch that used to reduce the whole alert to "Sent by". */
        if (prose is not null)
            details["Details"] = prose;

        if (context?.Details is null || context.Details.Count == 0)
        {
            details["Sent by"] = branding.EditionName;
            return details;
        }

        foreach (var detail in context.Details)
        {
            if (detail.IsCodeBlock)
            {
                /* Remediation T-SQL: point at the email / in-app dialog, never inline it. */
                details[detail.Heading] = TsqlWebhookHint;
                continue;
            }

            if (!string.IsNullOrEmpty(detail.Body))
            {
                /* Advice prose: flatten paragraphs into a single string (PD custom_details values are strings). */
                details[detail.Heading] = detail.Body.Replace("\n\n", " | ", StringComparison.Ordinal)
                                                      .Replace("\n", " ", StringComparison.Ordinal);
                continue;
            }

            foreach (var (label, value) in detail.Fields)
            {
                details[$"{detail.Heading} — {label}"] = value;
            }
        }

        return details;
    }

    /// <summary>
    /// Derives the PagerDuty dedup_key from the same fingerprint the cooldown uses, so repeated alerts for
    /// the same ongoing incident correlate into one PagerDuty alert. Falls back to a stable metric+server
    /// key when there is no incident (mirrors the cooldown's own "no incidents → metric-level fallback key" rule).
    ///
    /// <para>Internal (#4220), not private: <see cref="EmailSendCore"/> reads it too, so the triage link's
    /// dedup key agrees with PagerDuty's for the same firing across every channel, email included.</para>
    /// </summary>
    internal static string DerivePagerDutyDedupKey(string serverId, string metricName, AlertContext? context)
    {
        var incidents = context?.Incidents;
        if (incidents is { Count: > 0 })
        {
            /* Use the first incident's dedup key — PagerDuty correlates on a single dedup_key, and multiple
               distinct incidents in one alert are rare; if they occur, the first incident's key is as good
               a correlation anchor as any (the cooldown already evaluated all of them). */
            var firstKey = incidents[0].DedupKey;
            if (!string.IsNullOrEmpty(firstKey))
                return firstKey;
        }

        /* No incident or blank key: fall back to a stable metric+server key, matching the cooldown's
           own fallback shape (without the "webhook:" prefix — PagerDuty's dedup_key is its own namespace). */
        return $"{serverId}:{metricName}";
    }

    private static string PagerDutyEndpoint(bool useEuRegion) =>
        useEuRegion ? "https://events.eu.pagerduty.com/v2/enqueue" : "https://events.pagerduty.com/v2/enqueue";

    /// <summary>
    /// Sends a test notification to PagerDuty. Returns null on success, error message on failure.
    /// </summary>
    public static async Task<string?> SendTestPagerDutyAsync(string routingKey, bool useEuRegion, AlertBranding branding, string? proxyAddress = null)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(routingKey))
                return "PagerDuty routing key is not configured.";

            /* Synthetic dedup key so repeated test sends don't collide with real incidents. */
            var testDedupKey = "test-" + Guid.NewGuid();

            var payload = BuildPagerDutyPayload(
                "Test Notification", "", "Webhook configuration verified", "",
                branding, routingKey, isTest: true, dedupKey: testDedupKey);

            var endpoint = PagerDutyEndpoint(useEuRegion);
            return await PostWebhookAsync(endpoint, payload, proxyAddress);
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    #endregion

    #region Shared

    /// <summary>
    /// Splits a synthesized advice paragraph ("Investigation: ..." / "Remediation: ...") into a
    /// (label, value) pair on the first ": ". Falls back to ("Detail", paragraph) when there is no
    /// leading label. Depends on advice prose containing no interior blank-line break so each
    /// Body chunk is a single labelled paragraph (audited safe for the current FactAdvice blocks);
    /// a future block with a paragraph break degrades to the "Detail" fallback rather than breaking.
    /// </summary>
    private static (string Label, string Value) SplitProseLabel(string paragraph)
    {
        var idx = paragraph.IndexOf(": ", StringComparison.Ordinal);
        if (idx > 0)
        {
            return (paragraph.Substring(0, idx), paragraph.Substring(idx + 2));
        }
        return ("Detail", paragraph);
    }

    /* Reuse HttpClients instead of newing one (plus a handler) per send. A fresh
       HttpClient/handler per request leaks sockets into TIME_WAIT and can exhaust
       the ephemeral port range when many servers alert. One pooled client covers the
       no-proxy case; proxied clients are cached per proxy address. */
    private static readonly HttpClient s_defaultClient =
        new(new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5) })
        { Timeout = TimeSpan.FromSeconds(30) };

    private static readonly ConcurrentDictionary<string, HttpClient> s_proxyClients = new();

    private static HttpClient GetHttpClient(string? proxyAddress)
    {
        if (string.IsNullOrWhiteSpace(proxyAddress))
            return s_defaultClient;

        return s_proxyClients.GetOrAdd(proxyAddress, addr =>
            new HttpClient(new SocketsHttpHandler
            {
                Proxy = new WebProxy(addr),
                UseProxy = true,
                PooledConnectionLifetime = TimeSpan.FromMinutes(5)
            })
            { Timeout = TimeSpan.FromSeconds(30) });
    }

    /// <summary>
    /// Posts a JSON payload to a webhook URL. Returns null on success, error message on failure.
    /// </summary>
    /// <param name="headers">
    /// The generic channel's operator-authored request headers. <c>null</c> — what Teams/Slack pass — sends
    /// exactly today's request (no custom headers, no User-Agent); those two endpoints need neither.
    /// </param>
    private static async Task<string?> PostWebhookAsync(
        string webhookUrl,
        string jsonPayload,
        string? proxyAddress,
        IReadOnlyDictionary<string, string>? headers = null)
    {
        var client = GetHttpClient(proxyAddress);
        using var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        using var request = new HttpRequestMessage(HttpMethod.Post, webhookUrl) { Content = content };

        if (headers != null)
        {
            ApplyHeaders(request, content, headers);
        }

        using var response = await client.SendAsync(request);

        if (response.IsSuccessStatusCode)
            return null;

        /* Cap the destination's error body: it goes into the log + the health getter, and an unbounded read
           lets a hostile/misconfigured endpoint bloat both (and, if it echoes request headers, spill more of
           them). The first 2 KB is plenty to diagnose a 4xx/5xx. */
        var body = await response.Content.ReadAsStringAsync();
        if (body.Length > 2048)
            body = string.Concat(body.AsSpan(0, 2048), "…(truncated)");

        return $"HTTP {(int)response.StatusCode}: {body}";
    }

    /// <summary>
    /// Applies the operator's headers to the request, routing content headers (a Content-Type override) onto
    /// the content — setting one on <see cref="HttpRequestMessage.Headers"/> is rejected, so a naive add would
    /// silently drop it. Values are added without validation: they are the operator's own, and a strict parse
    /// would reject perfectly good tokens (CR/LF is already rejected upstream in <see cref="TryParseHeaders"/>).
    /// </summary>
    internal static void ApplyHeaders(HttpRequestMessage request, HttpContent content, IReadOnlyDictionary<string, string> headers)
    {
        foreach (var (name, value) in headers)
        {
            if (request.Headers.TryAddWithoutValidation(name, value))
            {
                continue;
            }

            /* A content header (e.g. Content-Type) — StringContent already set one, so replace it. */
            content.Headers.Remove(name);
            content.Headers.TryAddWithoutValidation(name, value);
        }

        if (!request.Headers.Contains("User-Agent"))
        {
            request.Headers.TryAddWithoutValidation("User-Agent", DefaultUserAgent);
        }
    }

    #endregion
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
public readonly record struct WebhookFanoutResult(AlertChannelOutcome Outcome, string? SendError, NotificationRouteDecision? Route = null)
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
