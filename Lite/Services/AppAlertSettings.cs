/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

    /* #6012 (Darling's #5998): a saved signing key that will not open turns the settings row's generic URL off, so a destination
       that was set up signed is never sent unsigned. */
    public bool   GenericWebhookEnabled      => App.GenericWebhookEnabled && !GenericSigningKeyUnreadable;
    public string GenericWebhookUrl          => GenericSigningKeyUnreadable ? "" : App.GenericWebhookUrl;
    public string GenericWebhookHeadersJson  => App.GenericWebhookHeadersJson;
    public string GenericWebhookBodyTemplate => App.GenericWebhookBodyTemplate;

    private readonly RouteSecretAccess? _signingSecrets;

    /// <summary>The app's settings; <paramref name="signingSecrets"/> is where the signing keys are read (null: Windows Credential
    /// Manager). A test passes an in-memory pair.</summary>
    public AppAlertSettings(RouteSecretAccess? signingSecrets = null)
    {
        _signingSecrets = signingSecrets;
    }

    private RouteSecretAccess Secrets => _signingSecrets ?? RouteSecretAccess.CredentialManager();

    private WebhookSigningKeys OpenSigning(out bool unreadable) =>
        LiteWebhookSigningKeys.Open(
            Secrets, LiteWebhookSigningKeys.SettingsName(false), LiteWebhookSigningKeys.SettingsName(true),
            new LiteSigningState(App.GenericSigningSet, App.GenericSigningUntilUtc), DateTime.UtcNow, out unreadable);

    /// <summary>#6012: the signing keys of the settings row's generic URL, opened. None when no key is saved.</summary>
    public WebhookSigningKeys GenericWebhookSigning => OpenSigning(out _);

    /// <summary>#6012: a key is saved and will not open.</summary>
    public bool GenericSigningKeyUnreadable
    {
        get
        {
            if (!App.GenericSigningSet)
            {
                return false;
            }

            OpenSigning(out var unreadable);
            return unreadable;
        }
    }
    public string GenericWebhookProxyAddress => App.GenericWebhookProxyAddress;

    /* PagerDuty: the enable flag AND a non-empty routing key, matching the sibling channels' shape
       (TrySendWebhookAlertsAsync checks both for every channel). */
    public bool   PagerDutyEnabled      => App.PagerDutyWebhookEnabled && !string.IsNullOrWhiteSpace(App.PagerDutyRoutingKey);
    public string PagerDutyRoutingKey   => App.PagerDutyRoutingKey;
    public bool   PagerDutyUseEuRegion  => App.PagerDutyUseEuRegion;
    public string PagerDutyProxyAddress => App.PagerDutyProxyAddress;
    public bool   PagerDutyAutoResolve  => App.PagerDutyAutoResolve;

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

    /// <summary>#5750: the scoped notification routes, expanded to server-id sets, from the static cache that the route
    /// store, the tag writers and the server writers invalidate. Empty until the main window installs the loader, which is
    /// the state before the feature (every alert goes to the app-wide destinations).</summary>
    public IReadOnlyList<NotificationRoute> NotificationRoutes => NotificationRouteCache.Current;
}
