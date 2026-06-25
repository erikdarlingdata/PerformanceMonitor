/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Adapts Dashboard's <see cref="IUserPreferencesService"/> + credential statics to
    /// <see cref="IAlertSettings"/>. Pass-through only — reads live preferences on every
    /// access (no caching) so a settings save is seen immediately, matching today's direct
    /// <c>GetPreferences()</c> reads in the alert services. This is the <b>saved-prefs</b>
    /// adapter used by the live alert path; the test-send path uses a transient
    /// form-values adapter instead (see <see cref="UserPreferencesAlertSettings"/>).
    ///
    /// <para>
    /// The SMTP password and webhook URLs live in Windows Credential Manager, not in
    /// <see cref="UserPreferences"/>; the adapter routes those through
    /// <see cref="DashboardAlertCredentials"/>.
    /// </para>
    ///
    /// <para>
    /// The analysis-notify bounds (<see cref="AnalysisNotifySeverity"/> to [0, 2],
    /// <see cref="AnalysisNotifyCooldownMinutes"/> to [30, 10080]) are clamped here — the
    /// settings boundary — so the consuming services need no inline clamp. Clamp is
    /// idempotent, so per-access clamping is safe.
    /// </para>
    /// </summary>
    public sealed class DashboardAlertSettings : IAlertSettings
    {
        private readonly IUserPreferencesService _preferencesService;

        public DashboardAlertSettings(IUserPreferencesService preferencesService)
        {
            _preferencesService = preferencesService;
        }

        private UserPreferences Prefs => _preferencesService.GetPreferences();

        public bool   SmtpEnabled     => Prefs.SmtpEnabled;
        public string SmtpServer      => Prefs.SmtpServer;
        public int    SmtpPort        => Prefs.SmtpPort;
        public bool   SmtpUseSsl      => Prefs.SmtpUseSsl;
        public string SmtpUsername    => Prefs.SmtpUsername;
        public string SmtpFromAddress => Prefs.SmtpFromAddress;
        public string SmtpRecipients  => Prefs.SmtpRecipients;
        public string? GetSmtpPassword() => DashboardAlertCredentials.GetSmtpPassword();

        public int EmailCooldownMinutes => Prefs.EmailCooldownMinutes;

        public bool   TeamsWebhookEnabled => Prefs.TeamsWebhookEnabled;
        public string TeamsWebhookUrl     => DashboardAlertCredentials.GetTeamsWebhookUrl();
        public string TeamsProxyAddress   => Prefs.TeamsProxyAddress;

        public bool   SlackWebhookEnabled => Prefs.SlackWebhookEnabled;
        public string SlackWebhookUrl     => DashboardAlertCredentials.GetSlackWebhookUrl();
        public string SlackProxyAddress   => Prefs.SlackProxyAddress;

        public double AnalysisNotifySeverity        => Math.Clamp(Prefs.AnalysisNotifySeverity, 0.0, 2.0);
        public int    AnalysisNotifyCooldownMinutes => Math.Clamp(Prefs.AnalysisNotifyCooldownMinutes, 30, 10080);
    }
}
