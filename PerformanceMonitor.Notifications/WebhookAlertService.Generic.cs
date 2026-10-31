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
    /// <summary>Posts to the generic channel. Null when the post succeeded, the error text when it did not
    /// — see <see cref="TrySendTeamsAlertAsync"/>. A malformed headers JSON or body template reports here
    /// too: an operator config error still delivers nothing, and naming it is the difference between a
    /// fixable row and a bare "failed".</summary>
    private async Task<string?> TrySendGenericAlertAsync(
        IAlertSettings settings,
        string webhookUrl,
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId,
        AlertContext? context,
        string? triageUrl,
        string? detailText,
        DateTime nowUtc,
        string? investigationJson = null,
        WebhookSigningKeys? signing = null,
        string status = GenericStatusFiring,
        string? dedupKeyOverride = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            /* A malformed headers JSON / body template is an operator config error, not a transport
               failure — but it still counts as a failure so the health surface and the log throttle
               report a channel that is delivering nothing, and it must never throw into the alert loop. */
            if (!TryParseHeaders(settings.GenericWebhookHeadersJson, out var headers, out var headerError))
            {
                RecordGenericFailure(headerError!);
                return headerError;
            }

            /* #5366: the configured headers go only to the generic URL itself or a path under it on the same
               scheme, host and port. A route to any other URL gets the body but not the headers. */
            if (headers.Count > 0 && !HeadersApplyTo(webhookUrl, settings.GenericWebhookUrl))
            {
                headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                _logger.LogInformation("Generic webhook headers were not sent: the routed endpoint is not the generic URL or a path under it");
            }

            var payload = BuildGenericPayload(
                metricName, serverName, currentValue, thresholdValue, _branding,
                context: context, bodyTemplate: settings.GenericWebhookBodyTemplate, serverId: serverId,
                triageUrl: triageUrl, detailText: detailText, nowUtc: nowUtc, investigationJson: investigationJson,
                status: status, dedupKeyOverride: dedupKeyOverride);

            if (!IsWellFormedJson(payload, out var bodyError))
            {
                RecordGenericFailure(bodyError!);
                return bodyError;
            }

            /* #5998: a destination with a signing key is signed with that destination's keys (the caller picks them:
               the settings row's for its URL, a route's for the route's own URL). The timestamp is the send's own
               instant, not the firing's, because the receiver rejects a timestamp outside its window; the same read
               decides whether the previous key is still in its overlap. */
            WebhookSendSigning? signed = null;
            if (signing is { HasKey: true })
            {
                var sendUtc = _utcNow();
                signed = new WebhookSendSigning(signing.LiveKeys(sendUtc), WebhookSigning.NewMessageId(), WebhookSigning.UnixSeconds(sendUtc));
            }

            /* #3598: the routed endpoint; headers, body template and proxy stay the parent's — a route
               redirects the POST, it does not re-author it. */
            var error = await PostWebhookAsync(
                webhookUrl, payload, settings.GenericWebhookProxyAddress, headers, WebhookPostTimeout, signed, cancellationToken, _logger);

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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* #4752: the caller's cancel passes through, as in the Teams send. */
            throw;
        }
        catch (Exception ex)
        {
            _consecutiveGenericFailures++;
            _lastGenericError = ex.Message;
            _logger.LogError($"Generic webhook error: {ex.Message}");
            return ex.Message;
        }
    }

    /// <summary>What a generic resolution says when it is not sent because a signing key would not open (#5998).</summary>
    internal const string GenericSigningKeyUnreadableText =
        "Not sent: a generic webhook signing key could not be opened, and a signed destination is never sent unsigned.";

    /// <summary>
    /// The signing keys for a generic send to <paramref name="destination"/> (#5998, decision D2): the route's own keys
    /// when <paramref name="routeId"/> names the route whose generic URL it is, the settings row's keys when it is the
    /// settings row's generic URL, and otherwise the keys of the route whose generic URL it is. None when no key is set.
    /// A route URL is never signed with the settings row's keys.
    /// </summary>
    internal static WebhookSigningKeys GenericSigningFor(IAlertSettings settings, int? routeId, string? destination)
    {
        var routes = settings.NotificationRoutes ?? Array.Empty<NotificationRoute>();
        if (routeId is { } id)
        {
            foreach (var route in routes)
            {
                if (route is not null && route.RouteId == id && !string.IsNullOrWhiteSpace(route.GenericUrl))
                {
                    return route.GenericSigning ?? WebhookSigningKeys.None;
                }
            }
        }

        var target = destination?.Trim() ?? "";
        if (target.Length > 0 && string.Equals(target, settings.GenericWebhookUrl?.Trim(), StringComparison.Ordinal))
        {
            return settings.GenericWebhookSigning ?? WebhookSigningKeys.None;
        }

        foreach (var route in routes)
        {
            if (route is not null && target.Length > 0 && string.Equals(target, route.GenericUrl?.Trim(), StringComparison.Ordinal))
            {
                return route.GenericSigning ?? WebhookSigningKeys.None;
            }
        }

        return WebhookSigningKeys.None;
    }

    private static bool IsConfiguredGenericUrl(IAlertSettings settings, string? destination)
    {
        var target = destination?.Trim() ?? "";
        return target.Length > 0
            && (string.Equals(target, settings.GenericWebhookUrl?.Trim(), StringComparison.Ordinal)
                || (settings.NotificationRoutes ?? Array.Empty<NotificationRoute>())
                    .Any(r => r is not null && string.Equals(target, r.GenericUrl?.Trim(), StringComparison.Ordinal)));
    }

    /// <summary>Whether the generic headers go to <paramref name="routeUrl"/> (#5366): it has the same scheme, host
    /// and port as <paramref name="genericUrl"/>, and its path is the generic path or continues it at a "/"
    /// boundary, so "/alerts" covers "/alerts" and "/alerts/team-a" but not "/alerts2" or "/other". Paths compare
    /// case-sensitively after URI parsing; the query string is ignored. A generic URL whose path is "/" covers
    /// only a route whose path is exactly "/", and a route path containing %2F or %5C (any case) matches nothing.
    /// Text that is not an absolute URL matches nothing, not even itself.</summary>
    internal static bool HeadersApplyTo(string? routeUrl, string? genericUrl)
    {
        if (!Uri.TryCreate(routeUrl?.Trim(), UriKind.Absolute, out var a) || !Uri.TryCreate(genericUrl?.Trim(), UriKind.Absolute, out var b))
        {
            return false;
        }

        if (!string.Equals(a.Scheme, b.Scheme, StringComparison.OrdinalIgnoreCase)
            || !string.Equals(a.IdnHost, b.IdnHost, StringComparison.OrdinalIgnoreCase)
            || a.Port != b.Port)
        {
            return false;
        }

        var genericPath = b.AbsolutePath.TrimEnd('/');
        var routePath = a.AbsolutePath;

        /* A route path with an encoded slash or backslash does not take the headers: the "/" boundary below is for
           plain paths only. */
        if (routePath.Contains("%2F", StringComparison.OrdinalIgnoreCase) || routePath.Contains("%5C", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        /* A generic URL at the site root has an empty path here: it covers only the root itself, not every path
           on the host. */
        if (genericPath.Length == 0)
        {
            return routePath == "/";
        }

        return routePath.Equals(genericPath, StringComparison.Ordinal)
            || routePath.StartsWith(genericPath + "/", StringComparison.Ordinal);
    }

    /* The Teams/Slack log-throttle shape (loud for the first 3, then every 50th) — factored out only
       because the generic channel fails from three places (headers, body, transport). */
    private void RecordGenericFailure(string error)
    {
        _consecutiveGenericFailures++;
        _lastGenericError = error;

        if (_consecutiveGenericFailures <= FailingChannelThreshold)
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
    /// <para>
    /// #5821: <c>{{investigation_json}}</c> is the investigation request the alert opened or joined (Darling and, since #5820, Lite), as one
    /// JSON object the caller has already composed and masked at <c>mcp.dataDetail</c>. It is the one token whose
    /// rendering follows where the template puts it: unquoted (<c>"investigation": {{investigation_json}}</c>) it is the
    /// raw object, and inside a string literal (<c>"investigation": "{{investigation_json}}"</c>) it is that object's text,
    /// JSON-escaped, so both templates post well-formed JSON. With no request (no agent route, a muted alert, a report,
    /// a recovery, a route at its limit) it is the empty string: <c>""</c> bare, nothing inside quotes.
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
        DateTime? nowUtc = null,
        string? investigationJson = null,
        string status = GenericStatusFiring,
        string? dedupKeyOverride = null)
    {
        var (_, badgeText, _) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var template = string.IsNullOrWhiteSpace(bodyTemplate) ? DefaultGenericBodyTemplate : bodyTemplate!;

        var prose = AlertDetailText.ProseForDelivery(detailText, context);

        var contextText = isTest
            ? $"Webhook configuration is working correctly. Sent by {branding.EditionName}."
            : RenderContextForTemplate(context, branding, prose);

        /* Keyed on the numeric serverId the fan-out passes — the same identity the LIVE PagerDuty path
           feeds DerivePagerDutyDedupKey — so the two channels' keys are equal for the same alert, except
           an edge pair with PagerDuty auto-resolve on: this call keeps the default autoResolve = false on
           purpose (#5469), so the token stays on per-state keys while PagerDuty's follows the flag. The
           serverName arm is THIS channel's own fallback for callers with no id (the settings-window test
           send); it is not a guarantee PagerDuty's path shares, so a caller wanting cross-channel
           correlation must pass the id. */
        var dedupKey = dedupKeyOverride ?? DeriveGenericDedupKey(serverId, serverName, metricName, context);

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
            ["status"] = EscapeForJson(status),
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
        return s_genericPlaceholders.Replace(template, m =>
            m.Groups[1].Value == InvestigationToken
                ? InvestigationValue(investigationJson, InsideJsonString(template, m.Index))
                : values[m.Groups[1].Value]);
    }

    private const string InvestigationToken = "investigation_json";

    /// <summary>#6718: the <c>{{status}}</c> value on a fire (and on the settings window's test send).</summary>
    internal const string GenericStatusFiring = "firing";

    /// <summary>#6718: the <c>{{status}}</c> value on a resolution send.</summary>
    internal const string GenericStatusResolved = "resolved";

    /// <summary>#6718: the error a generic resolution reports when the caller gave no fire key.</summary>
    internal const string GenericResolutionWithoutKeyText = "generic resolution not sent: no dedup key recorded from its fire";

    /// <summary>
    /// #6718: the generic webhook's <c>{{dedup_key}}</c> for an alert: the serverId-else-serverName identity and the default
    /// <c>autoResolve = false</c> (#5469), so the paired-edge and per-replica rules never apply to it. The one place that
    /// derivation lives. The fan-out calls it once on the delivery-filtered context and hands the result to the post and back
    /// to the deliverer, so the resolution renders that value rather than deriving its own.
    /// </summary>
    internal static string DeriveGenericDedupKey(string serverId, string serverName, string metricName, AlertContext? context) =>
        DerivePagerDutyDedupKey(string.IsNullOrEmpty(serverId) ? serverName : serverId, metricName, context);

    /// <summary>
    /// #5821: whether a generic body template places <c>{{investigation_json}}</c>. A blank template is the default, which
    /// does not, so Darling reads an investigation request back for a page only when its operator asked for it.
    /// </summary>
    public static bool TemplateCarriesInvestigationToken(string? bodyTemplate) =>
        !string.IsNullOrWhiteSpace(bodyTemplate)
        && bodyTemplate.Contains("{{" + InvestigationToken + "}}", StringComparison.Ordinal);

    /// <summary>
    /// <c>{{investigation_json}}</c>'s text at its place in the template (#5821): inside a string literal the object's
    /// JSON-escaped text (nothing when there is no request); bare, the raw object (<c>""</c> when there is none).
    /// </summary>
    private static string InvestigationValue(string? investigationJson, bool insideString)
    {
        if (string.IsNullOrWhiteSpace(investigationJson))
        {
            return insideString ? "" : "\"\"";
        }

        return insideString ? EscapeForJson(investigationJson) : investigationJson;
    }

    /// <summary>
    /// Whether <paramref name="index"/> in the operator's template falls inside a JSON string literal: counts the
    /// unescaped double quotes before it. Read on the raw template, whose tokens hold no quotes.
    /// </summary>
    internal static bool InsideJsonString(string template, int index)
    {
        var inside = false;
        for (var i = 0; i < index; i++)
        {
            var c = template[i];
            if (inside && c == '\\')
            {
                i++;
            }
            else if (c == '"')
            {
                inside = !inside;
            }
        }

        return inside;
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
        "context", "timestamp", "detail", "status", InvestigationToken
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

        /* #5642: the newest-sample age, once, when the prose does not already carry it. */
        var ageLine = AlertDetailText.SampleAgeLineFor(context, prose);
        if (ageLine is not null)
        {
            parts.Add(ageLine);
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
        var redacted = new AlertContext
        {
            Incidents = context.Incidents,
            NewestSample = context.NewestSample,
            /* #5641: transient and never serialized, but a copy that drops a member is the drift this census-pinned type guards against. */
            EscalationBypassesCooldown = context.EscalationBypassesCooldown
        };
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
}
