/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Dashboard-owned helper for the alert credentials that live in Windows
    /// Credential Manager rather than in <c>UserPreferences</c>: the SMTP password
    /// and the Teams/Slack webhook URLs.
    /// <para>
    /// E3c relocated these read/write statics off <c>EmailAlertService</c> /
    /// <c>WebhookAlertService</c> so the shared <see cref="PerformanceMonitor.Notifications"/>
    /// services carry no <see cref="CredentialService"/> dependency or Dashboard
    /// credential keys. The <see cref="DashboardAlertSettings"/> /
    /// <see cref="UserPreferencesAlertSettings"/> adapters and <c>SettingsWindow</c>
    /// route through here.
    /// </para>
    /// </summary>
    public static class DashboardAlertCredentials
    {
        private const string SmtpCredentialKey = "PerformanceMonitorDashboard_SMTP";
        private const string TeamsWebhookCredentialKey = "TeamsWebhook";
        private const string SlackWebhookCredentialKey = "SlackWebhook";

        private static readonly CredentialService s_credentialService = new();

        /// <summary>
        /// Gets the stored SMTP password from the credential manager.
        /// </summary>
        public static string? GetSmtpPassword()
        {
            try
            {
                var credential = s_credentialService.GetCredential(SmtpCredentialKey);
                return credential?.Password;
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to retrieve SMTP password: {ex.Message}");
                return null;
            }
        }

        /// <summary>
        /// Saves the SMTP password to the credential manager.
        /// </summary>
        public static void SaveSmtpPassword(string password, string username)
        {
            try
            {
                s_credentialService.SaveCredential(SmtpCredentialKey, string.IsNullOrEmpty(username) ? "smtp" : username, password);
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to save SMTP password: {ex.Message}");
            }
        }

        /// <summary>
        /// Gets a webhook URL from Windows Credential Manager.
        /// </summary>
        public static string GetWebhookUrl(string credentialKey)
        {
            try
            {
                var cred = s_credentialService.GetCredential(credentialKey);
                return cred?.Password ?? "";
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to retrieve webhook URL for {credentialKey}: {ex.Message}");
                return "";
            }
        }

        /// <summary>
        /// Saves a webhook URL to Windows Credential Manager.
        /// </summary>
        public static void SaveWebhookUrl(string credentialKey, string url)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(url))
                {
                    s_credentialService.DeleteCredential(credentialKey);
                }
                else
                {
                    s_credentialService.SaveCredential(credentialKey, "webhook", url);
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Failed to save webhook URL for {credentialKey}: {ex.Message}");
            }
        }

        public static string GetTeamsWebhookUrl() => GetWebhookUrl(TeamsWebhookCredentialKey);
        public static string GetSlackWebhookUrl() => GetWebhookUrl(SlackWebhookCredentialKey);
        public static void SaveTeamsWebhookUrl(string url) => SaveWebhookUrl(TeamsWebhookCredentialKey, url);
        public static void SaveSlackWebhookUrl(string url) => SaveWebhookUrl(SlackWebhookCredentialKey, url);
    }
}
