/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Transient <see cref="IAlertSettings"/> backed by a single <see cref="UserPreferences"/>
    /// instance rather than the live <see cref="Interfaces.IUserPreferencesService"/>. Used by
    /// the "test before you save" path (<c>SettingsWindow.TestEmailButton_Click</c>), which builds
    /// a <see cref="UserPreferences"/> from the live SMTP textboxes the user just typed and sends a
    /// test email with <i>those</i> values — NOT the saved preferences. Routing the test through
    /// <see cref="DashboardAlertSettings"/> (which reads saved prefs) would silently test stale
    /// settings, so this distinct adapter is mandatory.
    ///
    /// <para>
    /// The SMTP password still comes from Credential Manager: the test path saves the typed
    /// password to the credential store first, then the send reads it back, so
    /// <see cref="GetSmtpPassword"/> routes to <see cref="DashboardAlertCredentials"/>, the
    /// same credential helper as the saved adapter.
    /// </para>
    /// </summary>
    public sealed class UserPreferencesAlertSettings : IAlertSettings
    {
        private readonly UserPreferences _prefs;

        public UserPreferencesAlertSettings(UserPreferences prefs)
        {
            _prefs = prefs;
        }

        public bool   SmtpEnabled     => _prefs.SmtpEnabled;
        public string SmtpServer      => _prefs.SmtpServer;
        public int    SmtpPort        => _prefs.SmtpPort;
        public bool   SmtpUseSsl      => _prefs.SmtpUseSsl;
        public string SmtpUsername    => _prefs.SmtpUsername;
        public string SmtpFromAddress => _prefs.SmtpFromAddress;
        public string SmtpRecipients  => _prefs.SmtpRecipients;
        public string? GetSmtpPassword() => DashboardAlertCredentials.GetSmtpPassword();

        public int EmailCooldownMinutes => _prefs.EmailCooldownMinutes;

        public bool   TeamsWebhookEnabled => _prefs.TeamsWebhookEnabled;
        public string TeamsWebhookUrl     => DashboardAlertCredentials.GetTeamsWebhookUrl();
        public string TeamsProxyAddress   => _prefs.TeamsProxyAddress;

        public bool   SlackWebhookEnabled => _prefs.SlackWebhookEnabled;
        public string SlackWebhookUrl     => DashboardAlertCredentials.GetSlackWebhookUrl();
        public string SlackProxyAddress   => _prefs.SlackProxyAddress;

        /* Off for the Dashboard — see DashboardAlertSettings: the generic channel (#1506) is a Lite/Darling
           feature, and the deprecated Dashboard grows no settings surface to author it from. */
        public bool   GenericWebhookEnabled      => false;
        public string GenericWebhookUrl          => "";
        public string GenericWebhookHeadersJson  => "";
        public string GenericWebhookBodyTemplate => "";
        public string GenericWebhookProxyAddress => "";

        /* PagerDuty is a Lite/Darling feature; the deprecated Dashboard gets stub-only members. */
        public bool   PagerDutyEnabled      => false;
        public string PagerDutyRoutingKey   => "";
        public bool   PagerDutyUseEuRegion  => false;
        public string PagerDutyProxyAddress => "";

        public double AnalysisNotifySeverity        => Math.Clamp(_prefs.AnalysisNotifySeverity, 0.0, 2.0);
        public int    AnalysisNotifyCooldownMinutes => Math.Clamp(_prefs.AnalysisNotifyCooldownMinutes, 30, 10080);

        /* #2710: the deprecated Dashboard serves no web dashboard - no triage page to link to. */
        public string TriageBaseUrl => "";
    }
}
