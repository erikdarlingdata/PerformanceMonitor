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
    private readonly IAlertSettings _settings;
    private readonly AlertBranding _branding;
    private readonly ILogger<WebhookAlertService> _logger;

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
    public WebhookAlertService(
        IAlertSettings settings,
        AlertBranding branding,
        ILogger<WebhookAlertService> logger,
        IAlertHistoryStore? historyStore = null)
    {
        _settings = settings;
        _branding = branding;
        _logger = logger;
        _cooldown = new IncidentCooldown(
            keyPrefix: "webhook:",
            // Null store -> null seed delegate -> no restart seeding (preserves the pre-#1145 in-memory path).
            seedLastSentUtc: historyStore is null
                ? null
                : (serverId, metricName, dedupKey) =>
                    historyStore.GetLastWebhookSentUtcAsync(serverId, metricName, dedupKey));
    }

    /// <summary>
    /// Sends webhook alerts to all configured channels (Teams and/or Slack).
    /// Respects the email cooldown setting for throttling. Never throws.
    /// </summary>
    public async Task<bool> TrySendWebhookAlertsAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId = "",
        AlertContext? context = null)
    {
        try
        {
            /* #1154: per-fingerprint cooldown. Post if any incident in this alert is outside its
               window (a distinct fingerprint is not throttled by an unrelated prior incident); stamp
               every candidate key only after a successful post. Seeds the webhook last-sent time from
               the alert log on first touch per key (#1145), unless the store is null (no seeding). No
               incidents -> the metric-level fallback key (today's behavior). */
            var decision = await _cooldown.EvaluateAsync(
                serverId, metricName, context?.Incidents,
                TimeSpan.FromMinutes(_settings.EmailCooldownMinutes));

            if (!decision.ShouldSend)
            {
                return false;
            }

            bool sent = false;

            if (_settings.TeamsWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.TeamsWebhookUrl))
            {
                sent |= await TrySendTeamsAlertAsync(metricName, serverName, currentValue, thresholdValue, context);
            }

            if (_settings.SlackWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.SlackWebhookUrl))
            {
                sent |= await TrySendSlackAlertAsync(metricName, serverName, currentValue, thresholdValue, context);
            }

            if (_settings.GenericWebhookEnabled && !string.IsNullOrWhiteSpace(_settings.GenericWebhookUrl))
            {
                sent |= await TrySendGenericAlertAsync(metricName, serverName, currentValue, thresholdValue, serverId, context);
            }

            if (_settings.PagerDutyEnabled && !string.IsNullOrWhiteSpace(_settings.PagerDutyRoutingKey))
            {
                sent |= await TrySendPagerDutyAlertAsync(metricName, serverName, currentValue, thresholdValue, serverId, context);
            }

            if (sent)
            {
                _cooldown.Stamp(decision);
            }

            return sent;
        }
        catch (Exception ex)
        {
            _logger.LogError($"TrySendWebhookAlertsAsync outer error: {ex.Message}");
            return false;
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

    private async Task<bool> TrySendTeamsAlertAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertContext? context)
    {
        try
        {
            var payload = BuildTeamsPayload(metricName, serverName, currentValue, thresholdValue, _branding, context: context);
            var error = await PostWebhookAsync(_settings.TeamsWebhookUrl, payload, _settings.TeamsProxyAddress);

            if (error != null)
            {
                _consecutiveTeamsFailures++;
                _lastTeamsError = error;

                if (_consecutiveTeamsFailures <= 3)
                    _logger.LogError($"TEAMS WEBHOOK FAILED ({_consecutiveTeamsFailures}x): {error}");
                else if (_consecutiveTeamsFailures % 50 == 0)
                    _logger.LogError($"TEAMS WEBHOOK STILL FAILING: {_consecutiveTeamsFailures} failures. Last: {error}");

                return false;
            }

            if (_consecutiveTeamsFailures > 0)
                _logger.LogInformation($"Teams webhook recovered after {_consecutiveTeamsFailures} failure(s)");

            _consecutiveTeamsFailures = 0;
            _lastTeamsError = null;
            _logger.LogInformation($"Teams webhook sent for {metricName} on {serverName}");
            return true;
        }
        catch (Exception ex)
        {
            _consecutiveTeamsFailures++;
            _lastTeamsError = ex.Message;
            _logger.LogError($"Teams webhook error: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Builds an O365 MessageCard payload for Teams incoming webhooks.
    /// The themeColor property renders as a colored accent bar at the top of the card.
    /// </summary>
    /// <summary>
    /// #2710: the Datadog-parity <c>resource_name</c> tag — the first incident's involved objects,
    /// joined. Mirrors the SAME "first incident is the correlation anchor" precedent
    /// <see cref="DerivePagerDutyDedupKey"/> already uses for an alert carrying more than one
    /// fingerprint: whichever incident PagerDuty's dedup_key names is also the one a human reading
    /// the tag should look at first. Null when the alert carries no fingerprintable incident (alert
    /// type not wired to #1140's <see cref="AlertContext.Incidents"/>, or the objects were
    /// unresolved) — a top-level tag naming nothing would read worse than the tag being absent.
    /// </summary>
    private static string? DeriveResourceName(AlertContext? context)
    {
        if (context?.Incidents is not { Count: > 0 } incidents)
            return null;

        var objects = incidents[0].InvolvedObjects;
        return objects.Count > 0 ? string.Join(", ", objects) : null;
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

    internal static string BuildTeamsPayload(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertBranding branding,
        bool isTest = false,
        AlertContext? context = null)
    {
        var (hexColor, badgeText, emoji) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var themeColor = hexColor.TrimStart('#');
        var utcNow = DateTime.UtcNow;
        var localNow = DateTime.Now;

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
                foreach (var (label, value) in detail.Fields)
                {
                    itemFacts.Add(new { name = label, value });
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
            : $"{emoji} {badgeText} — {metricName}";

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

        var card = new
        {
            @type = "MessageCard",
            @context = "http://schema.org/extensions",
            themeColor,
            summary = isTest
                ? "[SQL Monitor] Test Notification"
                : $"[SQL Monitor] {badgeText}: {metricName} on {serverName}",
            sections
        };

        return JsonSerializer.Serialize(card, s_jsonOptions);
    }

    #endregion

    #region Slack

    private async Task<bool> TrySendSlackAlertAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertContext? context)
    {
        try
        {
            var payload = BuildSlackPayload(metricName, serverName, currentValue, thresholdValue, _branding, context: context);
            var error = await PostWebhookAsync(_settings.SlackWebhookUrl, payload, _settings.SlackProxyAddress);

            if (error != null)
            {
                _consecutiveSlackFailures++;
                _lastSlackError = error;

                if (_consecutiveSlackFailures <= 3)
                    _logger.LogError($"SLACK WEBHOOK FAILED ({_consecutiveSlackFailures}x): {error}");
                else if (_consecutiveSlackFailures % 50 == 0)
                    _logger.LogError($"SLACK WEBHOOK STILL FAILING: {_consecutiveSlackFailures} failures. Last: {error}");

                return false;
            }

            if (_consecutiveSlackFailures > 0)
                _logger.LogInformation($"Slack webhook recovered after {_consecutiveSlackFailures} failure(s)");

            _consecutiveSlackFailures = 0;
            _lastSlackError = null;
            _logger.LogInformation($"Slack webhook sent for {metricName} on {serverName}");
            return true;
        }
        catch (Exception ex)
        {
            _consecutiveSlackFailures++;
            _lastSlackError = ex.Message;
            _logger.LogError($"Slack webhook error: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Builds a Slack incoming webhook payload with a colored attachment sidebar.
    /// Uses Slack Block Kit for rich formatting.
    /// </summary>
    internal static string BuildSlackPayload(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        AlertBranding branding,
        bool isTest = false,
        AlertContext? context = null)
    {
        var (hexColor, badgeText, emoji) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var utcNow = DateTime.UtcNow;
        var localNow = DateTime.Now;

        var title = isTest
            ? $"{emoji} TEST — {metricName}"
            : $"{emoji} {badgeText} — {metricName}";

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

        blocks.Add(new { type = "section", fields });

        if (context?.Details != null)
        {
            foreach (var detail in context.Details)
            {
                blocks.Add(new { type = "divider" });

                if (detail.IsCodeBlock)
                {
                    /* Remediation T-SQL: point at the email / in-app dialog, never inline it. */
                    blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{detail.Heading}*\n{TsqlWebhookHint}" } });
                    continue;
                }

                if (!string.IsNullOrEmpty(detail.Body))
                {
                    /* Advice prose flows as a single mrkdwn section; the synthesized Body is
                       "Investigation: ...\n\nRemediation: ..." which Slack renders verbatim. */
                    blocks.Add(new { type = "section", text = new { type = "mrkdwn", text = $"*{detail.Heading}*\n{detail.Body}" } });
                    continue;
                }

                var detailFields = new List<object>();
                detailFields.Add(new { type = "mrkdwn", text = $"*{detail.Heading}*" });

                foreach (var (label, value) in detail.Fields)
                {
                    detailFields.Add(new { type = "mrkdwn", text = $"*{label}:*\n{value}" });
                }

                blocks.Add(new { type = "section", fields = detailFields });
            }
        }

        var contextElements = new List<object>
        {
            new { type = "mrkdwn", text = $"Sent by {branding.EditionName}" }
        };
        if (!isTest && branding.SnoozeHint is not null)
        {
            contextElements.Add(new { type = "mrkdwn", text = branding.SnoozeHint });
        }

        blocks.Add(new
        {
            type = "context",
            elements = contextElements
        });

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

    private async Task<bool> TrySendGenericAlertAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId,
        AlertContext? context)
    {
        try
        {
            /* A malformed headers JSON / body template is an operator config error, not a transport
               failure — but it still counts as a failure so the health surface and the log throttle
               report a channel that is delivering nothing, and it must never throw into the alert loop. */
            if (!TryParseHeaders(_settings.GenericWebhookHeadersJson, out var headers, out var headerError))
            {
                RecordGenericFailure(headerError!);
                return false;
            }

            var payload = BuildGenericPayload(
                metricName, serverName, currentValue, thresholdValue, _branding,
                context: context, bodyTemplate: _settings.GenericWebhookBodyTemplate, serverId: serverId);

            if (!IsWellFormedJson(payload, out var bodyError))
            {
                RecordGenericFailure(bodyError!);
                return false;
            }

            var error = await PostWebhookAsync(
                _settings.GenericWebhookUrl, payload, _settings.GenericWebhookProxyAddress, headers);

            if (error != null)
            {
                RecordGenericFailure(error);
                return false;
            }

            if (_consecutiveGenericFailures > 0)
                _logger.LogInformation($"Generic webhook recovered after {_consecutiveGenericFailures} failure(s)");

            _consecutiveGenericFailures = 0;
            _lastGenericError = null;
            _logger.LogInformation($"Generic webhook sent for {metricName} on {serverName}");
            return true;
        }
        catch (Exception ex)
        {
            _consecutiveGenericFailures++;
            _lastGenericError = ex.Message;
            _logger.LogError($"Generic webhook error: {ex.Message}");
            return false;
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
    /// here (<see cref="DeriveResourceName"/> / <see cref="DeriveResourceDatabase"/>).
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
        string serverId = "")
    {
        var (_, badgeText, _) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var template = string.IsNullOrWhiteSpace(bodyTemplate) ? DefaultGenericBodyTemplate : bodyTemplate!;

        var contextText = isTest
            ? $"Webhook configuration is working correctly. Sent by {branding.EditionName}."
            : RenderContextForTemplate(context, branding);

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
            ["timestamp"] = EscapeForJson(DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture)),
            /* Raw JSON values — never EscapeForJson (see the doc comment). "{}" / "[]" rather than empty
               so a template's `"context": {{context_json}}` stays well-formed on a context-less alert.
               Serialized from a REDACTED copy: the copy-paste remediation T-SQL never leaves the process
               on any webhook channel, and a raw token is not an exception to that rule. */
            ["context_json"] = context is null ? "{}" : AlertContextSerializer.Serialize(RedactForWebhook(context)),
            ["incidents_json"] = AlertContextSerializer.SerializeIncidents(context),
            ["dedup_key"] = EscapeForJson(dedupKey),
            ["resource_name"] = EscapeForJson(DeriveResourceName(context) ?? ""),
            ["database"] = EscapeForJson(DeriveResourceDatabase(context) ?? ""),
        };

        /* Single pass: a MatchEvaluator's output is NOT re-scanned, so a value that itself contains the
           literal text of another token (e.g. a server name "{{timestamp}}") is left verbatim rather than
           re-expanded — which chained .Replace() calls would do, letting one field pull another's contents
           into itself. An unknown "{{foo}}" isn't in the alternation, so it stays literal (today's behavior). */
        return s_genericPlaceholders.Replace(template, m => values[m.Groups[1].Value]);
    }

    /* context_json before context: alternation is ordered, and while the closing \}\} would force a
       backtrack to the right answer anyway, longest-first means correctness never leans on it. */
    private static readonly System.Text.RegularExpressions.Regex s_genericPlaceholders =
        new(@"\{\{(metric|server|value|threshold|severity|context_json|incidents_json|dedup_key|resource_name|database|context|timestamp)\}\}",
            System.Text.RegularExpressions.RegexOptions.Compiled);

    /// <summary>
    /// Flattens the structured alert context into one plain-text line for <c>{{context}}</c> — the generic
    /// endpoint has no card schema to render into. Follows the Teams/Slack rule: remediation T-SQL is never
    /// inlined, it points at the email / in-app dialog.
    /// </summary>
    private static string RenderContextForTemplate(AlertContext? context, AlertBranding branding)
    {
        if (context?.Details is not { Count: > 0 })
        {
            return $"Sent by {branding.EditionName}";
        }

        var parts = new List<string>();

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

    private async Task<bool> TrySendPagerDutyAlertAsync(
        string metricName,
        string serverName,
        string currentValue,
        string thresholdValue,
        string serverId,
        AlertContext? context)
    {
        try
        {
            /* Derive the dedup_key from the same fingerprint the cooldown uses, so repeated alerts for
               the same ongoing incident correlate into one PagerDuty alert. Falls back to a stable
               metric+server key when there is no incident. */
            var dedupKey = DerivePagerDutyDedupKey(serverId, metricName, context);

            var payload = BuildPagerDutyPayload(
                metricName, serverName, currentValue, thresholdValue, _branding,
                _settings.PagerDutyRoutingKey, context: context, dedupKey: dedupKey);

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

                return false;
            }

            if (_consecutivePagerDutyFailures > 0)
                _logger.LogInformation($"PagerDuty webhook recovered after {_consecutivePagerDutyFailures} failure(s)");

            _consecutivePagerDutyFailures = 0;
            _lastPagerDutyError = null;
            _logger.LogInformation($"PagerDuty webhook sent for {metricName} on {serverName}");
            return true;
        }
        catch (Exception ex)
        {
            _consecutivePagerDutyFailures++;
            _lastPagerDutyError = ex.Message;
            _logger.LogError($"PagerDuty webhook error: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// Builds a PagerDuty Events API v2 payload. Always sends event_action: "trigger" (no resolve wiring —
    /// matches Teams/Slack/Generic which also don't deliver "Cleared" notifications). The dedup_key correlates
    /// repeated triggers for the same ongoing incident into one PagerDuty alert.
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
        string? serverId = null)
    {
        var (_, badgeText, _) = AlertSeverity.ForMetric(metricName, context?.SeverityOverride);
        var severity = MapToPagerDutySeverity(badgeText);
        var utcNow = DateTime.UtcNow;

        /* PD-CEF caps summary at 1024 chars — no truncation needed given the source strings, but document
           the constraint matching this codebase's habit of documenting limits even when unreachable. */
        var summary = isTest
            ? "Webhook configuration verified"
            : $"{metricName} on {serverName}: {currentValue} (threshold {thresholdValue})";

        var source = isTest
            ? branding.EditionName
            : serverName;

        var customDetails = BuildPagerDutyCustomDetails(isTest, branding, context);

        /* Derive dedup_key from the incident fingerprint when not explicitly provided, falling back to a
           stable metric+server key. This ensures PagerDuty correlates repeated alerts for the same incident. */
        var effectiveDedupKey = dedupKey ?? DerivePagerDutyDedupKey(serverId ?? serverName, metricName, context);

        var payload = new
        {
            routing_key = routingKey,
            event_action = "trigger",
            dedup_key = effectiveDedupKey,
            payload = new
            {
                summary,
                source,
                severity,
                timestamp = utcNow.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture),
                component = "SQL Server Performance Monitor",
                custom_details = customDetails
            },
            client = branding.EditionName
        };

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
        AlertContext? context)
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
           still gets them. */
        var resourceName = DeriveResourceName(context);
        if (resourceName is not null)
            details["Resource"] = resourceName;
        var database = DeriveResourceDatabase(context);
        if (!string.IsNullOrEmpty(database))
            details["Database"] = database;

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
    /// </summary>
    private static string DerivePagerDutyDedupKey(string serverId, string metricName, AlertContext? context)
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
