/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Read-only view of the settings the alert/notification services consume.
/// Implemented per-app by an adapter over that app's settings store
/// (Lite: App.* statics; Dashboard: UserPreferences + CredentialService).
/// All members are pass-through reads of live values — callers must see the
/// current setting on every access (no caching), so a settings reload is
/// reflected immediately, matching today's direct App.* reads.
/// </summary>
public interface IAlertSettings
{
    /* SMTP */
    bool   SmtpEnabled { get; }
    string SmtpServer { get; }
    int    SmtpPort { get; }
    bool   SmtpUseSsl { get; }
    string SmtpUsername { get; }
    string SmtpFromAddress { get; }
    string SmtpRecipients { get; }

    /// <summary>SMTP password from secure storage; null if unset.</summary>
    string? GetSmtpPassword();

    /* Throttle shared by email + webhook channels */
    int EmailCooldownMinutes { get; }

    /* Teams webhook */
    bool   TeamsWebhookEnabled { get; }
    string TeamsWebhookUrl { get; }
    string TeamsProxyAddress { get; }

    /* Slack webhook */
    bool   SlackWebhookEnabled { get; }
    string SlackWebhookUrl { get; }
    string SlackProxyAddress { get; }

    /* Generic webhook (#1506): POSTs an operator-authored JSON body to any endpoint, so an alert can
       drive a system we ship no adapter for — PagerDuty, Opsgenie, n8n, or a GitHub repository_dispatch
       that re-runs a workflow. Deliberately the answer to "run a script/exe on alert": it covers the
       same automation need with no process-execution surface in a signed binary. */
    bool   GenericWebhookEnabled { get; }
    string GenericWebhookUrl { get; }

    /// <summary>
    /// A JSON object of request headers, e.g. <c>{"Authorization":"Bearer ghp_...","Accept":"application/vnd.github+json"}</c>.
    /// Carries bearer tokens, so every app stores this alongside the URL as a SECRET (Lite/Dashboard:
    /// Credential Manager; Darling: a column-REVOKEd control-plane column). Malformed JSON fails the
    /// send with a logged error rather than throwing into the alert loop.
    /// </summary>
    string GenericWebhookHeadersJson { get; }

    /// <summary>
    /// The JSON request body, with <c>{{metric}}</c>, <c>{{server}}</c>, <c>{{value}}</c>,
    /// <c>{{threshold}}</c>, <c>{{severity}}</c>, <c>{{context}}</c> and <c>{{timestamp}}</c>
    /// placeholders substituted per alert, plus the #2302 automation tokens:
    /// <c>{{context_json}}</c> / <c>{{incidents_json}}</c> (raw JSON values, substituted unquoted)
    /// and <c>{{dedup_key}}</c> (the PagerDuty-shape correlation key). Empty falls back to
    /// <see cref="WebhookAlertService.DefaultGenericBodyTemplate"/>.
    /// </summary>
    string GenericWebhookBodyTemplate { get; }

    string GenericWebhookProxyAddress { get; }

    /* PagerDuty webhook — Events API v2. The routing key is a bearer-secret-like opaque token
       (comparable to the Teams/Slack/Generic webhook URLs), so every app stores it the same way
       those URLs are stored (Lite/Dashboard: Credential Manager; Darling: a column-REVOKEd
       control-plane column). No separate enable flag — enabled is derived from a non-empty routing
       key, matching every existing webhook channel's "no speculative enable flags" rule. */
    bool   PagerDutyEnabled { get; }
    string PagerDutyRoutingKey { get; }
    bool   PagerDutyUseEuRegion { get; }

    /// <summary>Optional proxy for the PagerDuty channel (#1945) - PagerDuty's endpoints are fixed public
    /// URLs, so a locked-down network that proxies webhook egress needs this like the sibling channels.
    /// Empty means direct, matching Teams/Slack/Generic.</summary>
    string PagerDutyProxyAddress { get; }

    /* Scheduled-analysis notifications */
    double AnalysisNotifySeverity { get; }
    int    AnalysisNotifyCooldownMinutes { get; }
}
