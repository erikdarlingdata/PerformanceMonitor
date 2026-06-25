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

    /* Scheduled-analysis notifications */
    double AnalysisNotifySeverity { get; }
    int    AnalysisNotifyCooldownMinutes { get; }
}
