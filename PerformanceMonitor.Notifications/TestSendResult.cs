/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Notifications;

/// <summary>
/// What one test send did (#9636), in the shape a host can answer with: whether it was delivered, the HTTP status the endpoint
/// returned (webhook channels, null when no response came back), the SMTP status the mail server reported (email, null when it
/// reported none), the failure's text, and the exception's type when the send threw. <paramref name="NotConfigured"/> is set when
/// the channel had nothing to send to and nothing was sent. The text can carry what the endpoint or the exception said, so a
/// host that returns it to a caller strips the channel's secrets first.
/// </summary>
public sealed record TestSendResult(
    bool Delivered,
    int? HttpStatus,
    string? Error,
    string? ExceptionType,
    string? SmtpStatus = null,
    bool NotConfigured = false)
{
    /// <summary>A send that went nowhere: the channel is not set up, or the values it needs did not pass the checks a real send makes.</summary>
    public static TestSendResult Refused(string? error, bool notConfigured = false) =>
        new(false, null, error, null, NotConfigured: notConfigured);
}
