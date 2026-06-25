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

    public double AnalysisNotifySeverity        => App.AnalysisNotifySeverity;
    public int    AnalysisNotifyCooldownMinutes => App.AnalysisNotifyCooldownMinutes;
}
