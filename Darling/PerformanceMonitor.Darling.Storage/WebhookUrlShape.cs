/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one copy of the URL shape rule (#6231): an absolute http or https URL with a host and no user info. The heartbeat URL
/// check and every writer of a webhook URL (the Viewer's settings and route windows, the MCP route tools) use it, so a value
/// that cannot be sent to is refused at save with the same words.
/// </summary>
public static class WebhookUrlShape
{
    /// <summary>Why a URL is refused. <see cref="None"/> means it is fine.</summary>
    public enum Problem
    {
        None,
        Shape,
        UserInfo,
        BlockedAddress,
    }

    /// <summary>Parses a literal URL: absolute, http or https, a host, and no user info.</summary>
    public static Problem TryParse(string? text, out Uri? uri)
    {
        uri = null;
        if (!Uri.TryCreate(text, UriKind.Absolute, out var parsed)
            || (parsed.Scheme != Uri.UriSchemeHttp && parsed.Scheme != Uri.UriSchemeHttps)
            || string.IsNullOrEmpty(parsed.Host))
        {
            return Problem.Shape;
        }

        /* HttpClient never turns user info into an Authorization header, so a URL with it would just fail as HTTP 401 with no
           hint. Refused up front instead. */
        if (!string.IsNullOrEmpty(parsed.UserInfo))
        {
            return Problem.UserInfo;
        }

        /* A host written as a link-local or cloud instance address is refused at save, the same addresses the connect guard refuses at
           send. A host name is checked at send, when it resolves. */
        if (System.Net.IPAddress.TryParse((parsed.IdnHost.EndsWith('.') ? parsed.IdnHost[..^1] : parsed.IdnHost), out var literal) && PerformanceMonitor.Notifications.WebhookConnectGuard.IsRefused(literal))
        {
            return Problem.BlockedAddress;
        }

        uri = parsed;
        return Problem.None;
    }

    /// <summary>The refusal sentence for a proxy address written as a link-local or cloud instance IP literal (#6392), or null when
    /// it is fine. The send connects to the proxy through the same guard, so such a proxy could never be reached; this says so at
    /// save. A proxy host name is checked at send, when it resolves; a value of another shape is left to the send, as before.</summary>
    public static string? ProxyRefusal(string? proxy, string label)
    {
        var text = proxy?.Trim();
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        if (!Uri.TryCreate(text, UriKind.Absolute, out var parsed) || string.IsNullOrEmpty(parsed.Host))
        {
            if (!Uri.TryCreate("http://" + text, UriKind.Absolute, out parsed))
            {
                return null;
            }
        }

        var host = parsed.IdnHost.EndsWith('.') ? parsed.IdnHost[..^1] : parsed.IdnHost;
        return System.Net.IPAddress.TryParse(host, out var literal) && PerformanceMonitor.Notifications.WebhookConnectGuard.IsRefused(literal)
            ? $"The {label} must not be a link-local or cloud instance address (169.254.0.0/16, fe80::/10, fd00:ec2::254, 168.63.129.16, 100.100.100.200). (The value is not echoed.)"
            : null;
    }

    /// <summary>The refusal sentence for a webhook URL at save, or null when it is fine. The value is never in the text.</summary>
    public static string? Refusal(string? text, string label) => TryParse(text, out _) switch
    {
        Problem.Shape => $"The {label} must be an absolute http or https URL with a host. (The value is not echoed.)",
        Problem.UserInfo => $"The {label} must not carry a user name or password; put any token in the path. (The value is not echoed.)",
        Problem.BlockedAddress => $"The {label} must not be a link-local or cloud instance address (169.254.0.0/16, fe80::/10, fd00:ec2::254, 168.63.129.16, 100.100.100.200). (The value is not echoed.)",
        _ => null,
    };
}
