/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using Microsoft.Extensions.Logging;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The service-log form of the settings windows' SMTP warning. A setup with SSL off sends its login to the mail
/// server without encryption. The settings windows say so when someone saves, tests or validates; this says so
/// in the service log for the setups no window touched: one declared in darling.json, and one saved before the
/// windows warned. It runs at service start and on every config reload, on the settings the service holds.
/// </summary>
internal static class DarlingSmtpWarning
{
    /// <summary>
    /// The warning for these settings, or null when they are fine or SMTP is not set up. The same rule the
    /// windows use (<see cref="EmailSendCore.CleartextCredentialsWarning"/>). The password counts as set when
    /// either password field holds text; its value is never read.
    /// </summary>
    internal static string? Warning(SmtpConfig smtp)
    {
        if (string.IsNullOrWhiteSpace(smtp.Host))
        {
            return null;
        }

        var passwordSet = !string.IsNullOrWhiteSpace(smtp.EncryptedPassword) || !string.IsNullOrWhiteSpace(smtp.Password);
        return EmailSendCore.CleartextCredentialsWarning(smtp.UseSsl, smtp.Username, passwordSet);
    }

    /// <summary>Logs <see cref="Warning"/> at Warning level when there is one.</summary>
    internal static void Log(SmtpConfig smtp, ILogger logger)
    {
        var warning = Warning(smtp);
        if (warning is not null)
        {
            logger.LogWarning("SMTP settings for {Host}:{Port}: {Warning}", smtp.Host, smtp.Port, warning);
        }
    }
}
