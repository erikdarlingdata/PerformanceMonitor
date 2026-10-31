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

public partial class WebhookAlertService
{
    /// <summary>
    /// Closes the PagerDuty incident an external alert's fire opened (#5654), and does nothing else: the clear's email,
    /// Teams, Slack and generic webhook posts are #5825's resolution send. Sends a <c>resolve</c> on the dedup key the fire's trigger used
    /// (<paramref name="firingMetric"/> on <paramref name="serverId"/>), to the routing key the route resolves for that
    /// metric, and only when PagerDuty auto-resolve is on. Returns <see cref="AlertChannelOutcome.NotAttempted"/> when it
    /// is off or PagerDuty is not routed for the metric, <see cref="AlertChannelOutcome.Delivered"/> or
    /// <see cref="AlertChannelOutcome.Failed"/> otherwise. Never throws except for the caller's own cancellation.
    /// </summary>
    public Task<AlertChannelOutcome> TrySendPagerDutyResolveAsync(
        string firingMetric, string serverName, string serverId, string? detailText,
        AlertSeverityLevel? firedSeverity = null, CancellationToken cancellationToken = default) =>
        TrySendPagerDutyResolveAsync(firingMetric, serverName, serverId, detailText, new[] { firedSeverity }, cancellationToken);

    /// <summary>
    /// The same close for an episode that fired at more than one tier (#6928). The route is resolved at each of
    /// <paramref name="firedSeverities"/> (a null entry resolves with no tier, as before the filter), and a <c>resolve</c> goes to every
    /// distinct routing key that gives, so a Warning-only route's incident is closed after the alert climbed to Critical and a
    /// Critical-only route took the later fires. Delivered when a resolve posted and none failed, Failed when any failed.
    /// </summary>
    public async Task<AlertChannelOutcome> TrySendPagerDutyResolveAsync(
        string firingMetric, string serverName, string serverId, string? detailText,
        IReadOnlyCollection<AlertSeverityLevel?> firedSeverities, CancellationToken cancellationToken = default)
    {
        try
        {
            var delivery = _settings.SnapshotForDelivery();
            if (!delivery.PagerDutyAutoResolve)
            {
                return AlertChannelOutcome.NotAttempted;
            }

            /* #6928: routed at each severity the fire went out at, so the close lands on the key each fire's route carried, not on a
               route the severity filter would send another tier to. */
            var keys = new List<string>();
            foreach (var severity in firedSeverities.Count == 0 ? new AlertSeverityLevel?[] { null } : firedSeverities)
            {
                var route = NotificationRouter.Resolve(firingMetric, delivery.NotificationRoutes, delivery, NotificationRouter.ParseServerId(serverId), severity);
                if (route.PagerDuty.Destination is { } pagerDutyKey && !keys.Contains(pagerDutyKey, StringComparer.Ordinal))
                {
                    keys.Add(pagerDutyKey);
                }
            }

            if (keys.Count == 0)
            {
                return AlertChannelOutcome.NotAttempted;
            }

            var failed = false;
            foreach (var pagerDutyKey in keys)
            {
                var error = await TrySendPagerDutyAlertAsync(
                    delivery, autoResolve: true, pagerDutyKey, firingMetric, serverName, "resolved", "", serverId,
                    context: null, triageUrl: null, detailText, _utcNow(), displayName: null,
                    resolveIncident: true, cancellationToken: cancellationToken);
                if (error is null)
                {
                    /* As the built-in pairs do after a delivered close: the cooldown entry the fire stamped describes an
                       incident PagerDuty just closed, so the next fire of the same key must open a new one instead of being
                       throttled against it. Only a resolve that posted clears it (#5654). */
                    _cooldown.ClearMetric(serverId, firingMetric, null);
                }
                else
                {
                    failed = true;
                }
            }

            return failed ? AlertChannelOutcome.Failed : AlertChannelOutcome.Delivered;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError($"PagerDuty resolve error for {firingMetric}: {ex.Message}");
            return AlertChannelOutcome.Failed;
        }
    }

    /// <summary>
    /// Posts one alert resolution (#5825) to the Teams, Slack or generic webhook destination its fire reached, in that
    /// channel's alert shape with <paramref name="title"/> (the resolution's own name, such as "CPU Resolved") as the
    /// headline. No cooldown and no route: the caller passes the destination the fire used. Null when the post
    /// succeeded, the error text when it did not; never throws except for the caller's own cancellation.
    /// </summary>
    /// <param name="genericDedupKey">#6718: the <c>{{dedup_key}}</c> the fire's generic post rendered. Required for the
    /// generic channel, which renders it as given and does not derive one: a key derived here has no incidents to anchor
    /// on and would close a key the fire never opened. Null on the generic channel is not sent and returns an error.
    /// Ignored on Teams and Slack.</param>
    internal async Task<string?> TrySendResolutionAsync(
        string channel, string destination, AlertResolutionFacts facts, string serverId, string? genericDedupKey,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(facts);
        var delivery = _settings.SnapshotForDelivery();
        /* The card's Server fact names the server; the detail prose carries the alert, when it fired and cleared, how
           long it lasted and the last value; the card's time is the clear's. No threshold row. */
        var detailText = facts.DetailText();
        var title = facts.Title;
        var serverName = facts.ServerName;
        var triageUrl = facts.TriageUrl;
        var now = facts.ClearedAtUtc;
        return channel switch
        {
            NotificationRouter.TeamsChannel => await TrySendTeamsAlertAsync(
                delivery, destination, title, serverName, "resolved", "", context: null, triageUrl, detailText, now,
                displayName: null, cancellationToken),
            NotificationRouter.SlackChannel => await TrySendSlackAlertAsync(
                delivery, destination, title, serverName, "resolved", "", context: null, triageUrl, detailText, now,
                displayName: null, cancellationToken),
            /* #5998: a resolution names its destination, not its route, so its keys are found by URL. A destination that
               matches no configured generic URL while a signing key would not open may be the one whose key failed: it is
               not sent, rather than sent unsigned. */
            NotificationRouter.GenericChannel when delivery.GenericSigningKeyUnreadable && !IsConfiguredGenericUrl(delivery, destination) =>
                GenericSigningKeyUnreadableText,
            /* #6718: {{dedup_key}} is the key the fire's generic post rendered, which the caller recorded from the fire, so the
               receiver sees one key for the episode and {{status}} says resolved. {{metric}} stays the resolution's title. No key,
               no send: there is nothing here to derive the fire's key from. */
            NotificationRouter.GenericChannel when genericDedupKey is null =>
                GenericResolutionWithoutKeyText,
            NotificationRouter.GenericChannel => await TrySendGenericAlertAsync(
                delivery, destination, title, serverName, "resolved", "", serverId, context: null, triageUrl, detailText, now,
                cancellationToken: cancellationToken, signing: GenericSigningFor(delivery, routeId: null, destination),
                status: GenericStatusResolved,
                dedupKeyOverride: genericDedupKey),
            _ => $"{channel} does not carry a resolution post",
        };
    }

    /// <summary>
    /// #6003, D8: a notice (an investigation request that expired with no agent on it) to one Teams or Slack destination,
    /// rendered as an alert card. Null on success, else the error. Any other channel is refused: a notice goes to people.
    /// </summary>
    internal async Task<string?> TrySendNoticeAsync(
        string channel, string destination, string title, string serverName, string currentValue, string detailText,
        CancellationToken cancellationToken = default)
    {
        var delivery = _settings.SnapshotForDelivery();
        var now = _utcNow();
        return channel switch
        {
            NotificationRouter.TeamsChannel => await TrySendTeamsAlertAsync(
                delivery, destination, title, serverName, currentValue, "", context: null, triageUrl: null, detailText, now,
                displayName: null, cancellationToken),
            NotificationRouter.SlackChannel => await TrySendSlackAlertAsync(
                delivery, destination, title, serverName, currentValue, "", context: null, triageUrl: null, detailText, now,
                displayName: null, cancellationToken),
            _ => $"{channel} does not carry a notice",
        };
    }

    /// <summary>
    /// #6003, D5: posts one investigation event to an agent route's own generic URL, signed with that route's keys (#5998),
    /// with the event's id as <c>webhook-id</c> so a receiver that saw it already can drop it. <paramref name="payload"/> is
    /// sent as given: no body template and no configured headers, which belong to the alert channel. A route whose key would
    /// not open is not sent to, rather than sent unsigned. Null on success, else the error.
    /// </summary>
    internal async Task<string?> TrySendInvestigationEventAsync(
        int routeId, string url, string payload, string eventId, CancellationToken cancellationToken = default)
    {
        var delivery = _settings.SnapshotForDelivery();
        var signing = GenericSigningFor(delivery, routeId, url);
        if (!signing.HasKey && delivery.GenericSigningKeyUnreadable)
        {
            return GenericSigningKeyUnreadableText;
        }

        var sendUtc = _utcNow();
        var signed = signing.HasKey
            ? new WebhookSendSigning(signing.LiveKeys(sendUtc), eventId, WebhookSigning.UnixSeconds(sendUtc))
            : null;
        try
        {
            /* #6456: an empty header set, not null. No configured headers are sent (they belong to the alert channel), but a non-null
               set is what puts the default User-Agent on a generic post, so an event carries the same sender identity as an alert. */
            return await PostWebhookAsync(
                url, payload, delivery.GenericWebhookProxyAddress, headers: new Dictionary<string, string>(), WebhookPostTimeout, signed, cancellationToken, _logger);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }

    /// <summary>
    /// Closes one PagerDuty incident an alert's fire opened (#5825): a <c>resolve</c> on <paramref name="dedupKey"/> (the
    /// key the fire's trigger used) to <paramref name="routingKey"/> (the service the fire went to). The caller checks
    /// <c>pagerduty_auto_resolve</c>. A delivered resolve clears the fire's cooldown entry, as
    /// <see cref="TrySendPagerDutyResolveAsync"/> does, so the next fire opens a new incident. Null when it posted, the
    /// error text when it did not; never throws except for the caller's own cancellation.
    /// </summary>
    internal async Task<string?> TrySendPagerDutyResolveAsync(
        string routingKey, string dedupKey, string firingMetric, string serverName, string serverId, string? detailText,
        CancellationToken cancellationToken = default)
    {
        var error = await TrySendPagerDutyAlertAsync(
            _settings.SnapshotForDelivery(), autoResolve: true, routingKey, firingMetric, serverName, "resolved", "", serverId,
            context: null, triageUrl: null, detailText, _utcNow(), displayName: null,
            resolveIncident: true, dedupKeyOverride: dedupKey, cancellationToken: cancellationToken);
        if (error is null)
        {
            _cooldown.ClearMetric(serverId, firingMetric, null);
        }

        return error;
    }

    internal Func<string, string, Task<string?>>? PostPagerDutyAsyncOverride { get; set; }

    /// <summary>Posts to PagerDuty Events v2. Null when the post succeeded, the error text when it did not
    /// — see <see cref="TrySendTeamsAlertAsync"/>.</summary>
    private async Task<string?> TrySendPagerDutyAlertAsync(
        IAlertSettings settings,
        bool autoResolve,
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
        string? displayName = null,
        bool resolveIncident = false,
        string? dedupKeyOverride = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            /* Derive the dedup_key from the same fingerprint the cooldown uses, so repeated alerts for
               the same ongoing incident correlate into one PagerDuty alert. Falls back to a stable
               metric+server key when there is no incident. #5825: an alert resolution passes the key its fire
               opened, because the clear carries no incidents to derive it from. */
            var dedupKey = dedupKeyOverride ?? DerivePagerDutyDedupKey(serverId, metricName, context, autoResolve);

            /* #3598: the routed routing key (a PagerDuty SERVICE is a destination); the EU-region flag and
               proxy stay the parent's. */
            var payload = BuildPagerDutyPayload(
                metricName, serverName, currentValue, thresholdValue, _branding,
                routingKey, context: context, dedupKey: dedupKey, triageUrl: triageUrl,
                detailText: detailText, displayName: displayName, nowUtc: nowUtc,
                autoResolve: autoResolve, resolveIncident: resolveIncident);

            /* #4752: test-only override for the PagerDuty send above (below). A test funneling an
               alert THROUGH the service must read the payload the wire saw — which a real post won't let
               it, because the only POST the PagerDuty body has ever taken spells the fixed Events v2
               production endpoint (it is not routed, unlike the token channels). The override answers the
               send and captures the payload instead; it is on the instance (not static), so one test's
               capture can never leak into another's run. Production construction sites never set it. */
            var endpoint = PagerDutyEndpoint(settings.PagerDutyUseEuRegion);
            var error = PostPagerDutyAsyncOverride is not null
                ? await PostPagerDutyAsyncOverride(endpoint, payload)
                : await PostWebhookAsync(endpoint, payload, settings.PagerDutyProxyAddress, cancellationToken: cancellationToken, bodyLog: _logger);

            if (error != null)
            {
                _consecutivePagerDutyFailures++;
                _lastPagerDutyError = error;

                if (_consecutivePagerDutyFailures <= FailingChannelThreshold)
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* #4752: the caller's cancel passes through, as in the Teams send. */
            throw;
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
    /// Builds a PagerDuty Events API v2 payload. Firing conditions send event_action: "trigger". The CLOSING
    /// edge of an edge-type alert pair (<see cref="AlertFamily.RecoveryPairs"/> — "Server Restored",
    /// "AG Replica Reconnected") sends "resolve" ONLY when <paramref name="autoResolve"/> is set — an opt-in,
    /// PagerDuty-only auto-close of the incident its firing edge's trigger opened — otherwise it is an
    /// info-severity trigger on the same dedup_key and the incident stays open. Teams/Slack/Generic still
    /// deliver no "Cleared" notifications; this is PagerDuty's own incident lifecycle. The dedup_key
    /// correlates repeated triggers for the same ongoing incident into one PagerDuty alert.
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
        DateTime? nowUtc = null,
        bool autoResolve = false,
        bool resolveIncident = false)
    {
        var (_, badgeText, _) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var severity = MapToPagerDutySeverity(badgeText);
        /* The PD summary (the incident title) shows the human name when present; severity above and the
           dedup_key below stay on the immutable metric name so correlation/dedup are rename-safe. */
        var titleName = ExternalTitle(string.IsNullOrEmpty(displayName) ? metricName : displayName, context);
        var utcNow = nowUtc ?? DateTime.UtcNow;

        /* Auto-resolve is opt-in and PagerDuty-only: the closing edge of an edge-type pair (the
           <see cref="AlertFamily.RecoveryPairs"/> census — connection restore, replica reconnect) closes the
           incident its firing edge's trigger opened, via event_action "resolve" on the SAME dedup_key. Off
           (the default), the closing edge is an info-severity trigger and the incident stays open, so the
           tool never auto-resolves a third-party incident unasked. A test notification always triggers (it
           carries a throwaway key and has no incident to close). */
        /* #5654: an external alert's clear closes the incident its fire opened. The caller names it with
           resolveIncident (and only with auto-resolve on), because an external name has no static pair. */
        var eventAction = !isTest && autoResolve && (resolveIncident || AlertFamily.RecoveryPairs.ContainsKey(metricName))
            ? "resolve"
            : "trigger";

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
           stable metric+server key. This ensures PagerDuty correlates repeated alerts for the same incident.
           Carries the operator's auto-resolve choice so the resolve lands on the same key the trigger opened:
           the paired-lifecycle rename only applies when the flag is on (off keeps the shipped keys). */
        var effectiveDedupKey = dedupKey ?? DerivePagerDutyDedupKey(serverId ?? serverName, metricName, context,
            autoResolve: autoResolve);

        /* A string-keyed dictionary rather than the previous anonymous type, so the #2710 links array can be
           present-or-absent (Events v2 accepts links: [] but an absent key is the honest "no link" shape and
           keeps the linkless payload byte-identical to pre-#2710). Insertion order is preserved by
           Dictionary in practice but nothing here depends on key order. */
        var payload = new Dictionary<string, object>
        {
            ["routing_key"] = routingKey,
            ["event_action"] = eventAction,
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

        /* #5642: the newest-sample age, once, when the prose does not already carry it. */
        var pdAgeLine = AlertDetailText.SampleAgeLineFor(context, prose);
        if (pdAgeLine is not null)
            details["Sample age"] = pdAgeLine;

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
    /// The send-path twin of <see cref="DerivePagerDutyDedupKey"/>: the same key derivation, with the
    /// operator's PagerDutyAutoResolve choice already read from <paramref name="settings"/>, so the live
    /// call sites stay one-liners rather than each repeating it. Static: it reads a settings parameter,
    /// not the service's own — this is also what lets the email path (which holds a WebhookAlertService
    /// only for the fan-out) and the test path pass their own.
    /// </summary>
    internal static string DerivePagerDutyDedupKeyForSend(string serverId, string metricName, AlertContext? context, IAlertSettings settings) =>
        DerivePagerDutyDedupKey(serverId, metricName, context, settings.PagerDutyAutoResolve);

    /// <summary>
    /// The per-replica identity of an AG pair edge, or null (#5469): non-null only with auto-resolve on, for
    /// either edge of the AG Replica Disconnected / Reconnected pair, and only when the alert carries
    /// <see cref="AlertContext.AgReplicaIdentity"/>. The ONE place the rule lives: the PagerDuty dedup key
    /// appends it so each replica is its own incident, and the webhook cooldown folds it into the metric-level
    /// key so each replica's window is its own too. Null leaves both keys exactly as they were, so a flag off,
    /// the server pair and an AG edge without the identity are all byte-identical to the per-server shape.
    /// The AG firing literal is spelled here rather than read from PerformanceMonitor.Common's AG consts: this
    /// assembly must not take that dependency.
    /// </summary>
    internal static string? PairedReplicaScope(string metricName, AlertContext? context, bool autoResolve)
    {
        if (!autoResolve
            || !AlertFamily.IsPairedEdge(metricName)
            || !string.Equals(AlertFamily.Canonical(metricName), "AG Replica Disconnected", StringComparison.Ordinal)
            || string.IsNullOrWhiteSpace(context?.AgReplicaIdentity))
        {
            return null;
        }

        return context.AgReplicaIdentity;
    }

    /// <summary>
    /// Derives the PagerDuty dedup_key from the same fingerprint the cooldown uses, so repeated alerts for
    /// the same ongoing incident correlate into one PagerDuty alert. Falls back to a stable server+incident
    /// key when there is no incident (mirrors the cooldown's own "no incidents → metric-level fallback key"
    /// rule).
    ///
    /// <para><b>Edge pairs are renamed to their incident — but only when the operator asked for the paired
    /// lifecycle.</b> The edge-type alerts ("Server Unreachable" / "Server Restored", "AG Replica
    /// Disconnected" / "AG Replica Reconnected") are two halves of ONE incident, so when
    /// <paramref name="autoResolve"/> is on both edges derive the key from the pair's canonical FIRING name
    /// (<see cref="AlertFamily.RecoveryPairs"/>) instead of keying on the metric name: the name encodes the
    /// current STATE, so using it minted a distinct dedup_key per edge and PagerDuty showed two incidents for
    /// one outage (and a closing edge could never resolve the open one — its key named a different
    /// incident). Every future pair added to that census joins this by construction. With the default off,
    /// each edge keeps its own state-named key, so the triage link reads exactly as it did before this rung:
    /// correlation is an opt-in behaviour change, scoped entirely to the operator who switched it on. The
    /// generic webhook's <c>{{dedup_key}}</c> token is NOT part of the opt-in (#5469): its send derives the key
    /// with the default <c>autoResolve = false</c>, so it stays on per-state keys with the flag on or off.</para>
    ///
    /// <para>The AG pair's key is also scoped per replica: "{serverId}:AG Replica Disconnected:{ag}:{replica}"
    /// from <see cref="AlertContext.AgReplicaIdentity"/>. Without that there is still one key per SERVER for
    /// the whole group: with auto-resolve on, one replica's reconnect would resolve the incident a
    /// still-disconnected sibling is still holding open (and with the re-fire defaulting to 0, nothing opens
    /// it again). The identity is a single "AG:replica" member (not two) so it can't be set half-way; a pair
    /// that arrives WITHOUT the identity keeps the old server-level key rather than failing the render —
    /// the per-replica key is the better incident, not a contract, and a deploy that hasn't applied the new
    /// fire sites yet still converges to one key per server per firing.</para>
    ///
    /// <para>The rename runs before the incident-fingerprint path on purpose: a closing edge that ever
    /// carried incident fingerprints would hash them under its own state name and split the pair again, and
    /// the deliverable recovery set is pinned to stateless notices — the incident identity IS the firing's
    /// canonical name. The consumers that follow the flag (PagerDuty and the triage link) share the
    /// normalized key, which is the cross-channel correlation those call sites document. The generic
    /// <c>{{dedup_key}}</c> token does not follow it (#5469).</para>
    ///
    /// <para>Internal (#4220), not private: <see cref="EmailSendCore"/> reads it too, so the triage link's
    /// dedup key agrees with PagerDuty's for the same firing across every channel, email included.</para>
    /// </summary>
    internal static string DerivePagerDutyDedupKey(string serverId, string metricName, AlertContext? context, bool autoResolve = false)
    {
        /* The paired lifecycle, gated on the operator's opt-in: when on, BOTH edges of a pair derive the
           firing's identity (so the pair is one incident); when off, each edge keeps its own state-named
           key, byte-identical to the shipped behavior. IsPairedEdge covers the recovery's own name AND the
           firing's: the closing edge's TryGetValue binds its pair's firing, and the firing edge's name is
           recognized as the pair's incident directly — so both sides converge on it. The connection pair
           needs no identity member (one connection state per server). The AG pair appends the "AG:replica"
           identity its fire sites carry — one replica's reconnect must resolve only ITS incident. An AG edge
           with no identity cannot happen from the shipped fire sites, so it keeps the per-server key rather
           than failing the render. The AG firing literal is spelled here rather than read from
           PerformanceMonitor.Common's AG consts: this assembly must not take that dependency. */
        if (autoResolve && AlertFamily.IsPairedEdge(metricName))
        {
            var firing = AlertFamily.Canonical(metricName);
            var replica = PairedReplicaScope(metricName, context, autoResolve);
            if (replica is not null)
            {
                return $"{serverId}:{firing}:{replica}";
            }

            return $"{serverId}:{firing}";
        }

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
            return await PostWebhookAsync(endpoint, payload, proxyAddress, bodyInResult: true);
        }
        catch (Exception ex)
        {
            return ex.Message;
        }
    }
}
