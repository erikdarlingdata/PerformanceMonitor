/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Adapts Lite's App.* static settings to IAlertSettings. Pass-through only —
/// reads live App values on each access so a settings reload is seen immediately.
/// Stateless; safe to construct once and share. Plan D delivers this; swapping
/// the backing store for a real preferences file is a later, separate change
/// that will not touch the alert services.
/// </summary>
public sealed class AppAlertSettings : IAlertSettings
{
    public bool   SmtpEnabled     => App.SmtpEnabled;
    public string SmtpServer      => App.SmtpServer;
    public int    SmtpPort        => App.SmtpPort;
    public bool   SmtpUseSsl      => App.SmtpUseSsl;
    public string SmtpUsername    => App.SmtpUsername;
    public string SmtpFromAddress => App.SmtpFromAddress;
    public string SmtpRecipients  => App.SmtpRecipients;
    public string? GetSmtpPassword() => App.GetSmtpPassword();

    public int EmailCooldownMinutes => App.EmailCooldownMinutes;

    public bool   TeamsWebhookEnabled => App.TeamsWebhookEnabled;
    public string TeamsWebhookUrl     => App.TeamsWebhookUrl;
    public string TeamsProxyAddress   => App.TeamsProxyAddress;

    public bool   SlackWebhookEnabled => App.SlackWebhookEnabled;
    public string SlackWebhookUrl     => App.SlackWebhookUrl;
    public string SlackProxyAddress   => App.SlackProxyAddress;

    public bool   GenericWebhookEnabled      => App.GenericWebhookEnabled;
    public string GenericWebhookUrl          => App.GenericWebhookUrl;
    public string GenericWebhookHeadersJson  => App.GenericWebhookHeadersJson;
    public string GenericWebhookBodyTemplate => App.GenericWebhookBodyTemplate;
    public string GenericWebhookProxyAddress => App.GenericWebhookProxyAddress;

    /* PagerDuty: the enable flag AND a non-empty routing key, matching the sibling channels' shape
       (TrySendWebhookAlertsAsync checks both for every channel). */
    public bool   PagerDutyEnabled      => App.PagerDutyWebhookEnabled && !string.IsNullOrWhiteSpace(App.PagerDutyRoutingKey);
    public string PagerDutyRoutingKey   => App.PagerDutyRoutingKey;
    public bool   PagerDutyUseEuRegion  => App.PagerDutyUseEuRegion;
    public string PagerDutyProxyAddress => App.PagerDutyProxyAddress;

    public double AnalysisNotifySeverity        => App.AnalysisNotifySeverity;
    public int    AnalysisNotifyCooldownMinutes => App.AnalysisNotifyCooldownMinutes;
    /* #3916: the analysis page cap per hold-back window is Lite's per-event cap (default 10). */
    public int    AnalysisPageCap               => Math.Clamp(App.AlertPerEventMaxPerCycle, 1, 100);

    /// <summary>#3712: Lite's live value for the one routing knob — overriding the interface's shipped default
    /// so the Settings window's choice reaches the gate on the next analysis pass, no restart.</summary>
    public FindingRoute UncorroboratedFindingRoute => App.AnalysisUncorroboratedRoute;

    /* #2710: Lite serves no web dashboard, so there is no triage page to link to — empty is the honest
       constant, and the shared webhook service omits the link on every channel because of it. */
    public string TriageBaseUrl => "";
}
