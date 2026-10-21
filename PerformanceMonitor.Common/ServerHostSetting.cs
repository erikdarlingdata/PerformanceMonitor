/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one rule every path that takes a SQL Server host for a NEW or EDITED registration applies to the text (#6352):
/// the web add and edit routes, the MCP <c>add_servers</c> and <c>edit_server</c> tools, the command-line add, the Darling
/// Viewer's dialogs and server store, and Lite's dialogs and server manager.
///
/// <para>SqlClient reads a protocol prefix at the start of a Data Source (<c>np:</c>, <c>lpc:</c>, <c>admin:</c> and the
/// like) and a path-looking one (<c>\\host\pipe\...</c>, <c>\host</c>, <c>//host/...</c>) as a named pipe, shared
/// memory or a dedicated admin connection instead of TCP. A named pipe is opened over SMB with the Windows credentials
/// of the process that connects, even for a registration that uses SQL authentication. So a host is refused when it
/// starts with <c>\</c> or <c>/</c>, or with letters followed by <c>:</c>, except <c>tcp:</c>, which only
/// names the protocol the connection uses anyway and which existing registrations may carry. An IPv6 literal is
/// not a prefix (<c>fe80::1</c>, <c>2001:db8::1</c>, <c>::1</c>, and <c>abcd::1</c>, whose first word is all letters):
/// text that parses as an IP address is allowed as it is. Plain names, <c>host,port</c>, <c>host\instance</c>,
/// listener names and IPv4 literals pass.</para>
///
/// <para>Existing rows are never rewritten, and collecting from an existing registration is not blocked: callers apply
/// this to what a person types into a new or changed host field. #6254's replica field can call <see cref="Refusal"/>
/// too.</para>
/// </summary>
public static class ServerHostSetting
{
    /// <summary>The longest value echoed back in a refusal, so a long paste does not fill the message.</summary>
    private const int MaxEchoLength = 80;

    /// <summary>
    /// Null when <paramref name="host"/> may be saved as a SQL Server host; otherwise the plain sentence to refuse
    /// with, naming the value and the reason. Null or blank text is not this rule's business (callers refuse a
    /// missing host themselves), so it returns null.
    /// </summary>
    public static string? Refusal(string? host)
    {
        if (string.IsNullOrWhiteSpace(host))
        {
            return null;
        }

        var text = host.Trim();
        var rest = text;

        /* tcp: is allowed, but only in front of a host the rule would accept on its own: "tcp:np:x" and
           "tcp:\host" are not made safe by the harmless word in front. SqlClient trims the protocol word before it
           compares it, so "tcp :np:x" and "TCP<tab>:np:x" are the same thing and "np :host" is a named pipe: the
           word before the colon is trimmed (all Unicode whitespace, U+00A0 and U+3000 included) before it is read. */
        while (true)
        {
            var tcpColon = rest.IndexOf(':');
            if (tcpColon > 0 && rest[..tcpColon].Trim().Equals("tcp", StringComparison.OrdinalIgnoreCase))
            {
                rest = rest[(tcpColon + 1)..].TrimStart();
                continue;
            }

            break;
        }

        if (rest.Length == 0)
        {
            return Message(text, "has no host after the tcp: prefix");
        }

        if (rest[0] == '\\' || rest[0] == '/')
        {
            return Message(text, "starts with a slash, which SqlClient reads as a network path (a named pipe)");
        }

        var colon = rest.IndexOf(':');
        if (colon > 0)
        {
            var prefix = rest[..colon].Trim();
            if (prefix.Length > 0 && IsAllLetters(prefix) && !IPAddress.TryParse(rest, out _))
            {
                return Message(text, $"starts with a protocol prefix ({prefix}:), which can send the connection over a named pipe or the dedicated admin connection; only tcp: is accepted");
            }
        }

        return null;
    }

    private static bool IsAllLetters(ReadOnlySpan<char> text)
    {
        foreach (var c in text)
        {
            if (!char.IsLetter(c))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsBidiControl(char c) =>
        c == '\u061C' || c == '\u200E' || c == '\u200F' || (c >= '\u202A' && c <= '\u202E') || (c >= '\u2066' && c <= '\u2069');

    private static string Message(string host, string why)
    {
        /* The host is echoed into log lines and error text: control characters (CR/LF would forge a log line) and
           bidi overrides (they reorder what a person reads) are shown as '?' (#6352). */
        var clean = string.Create(host.Length, host, static (span, src) =>
        {
            for (var i = 0; i < src.Length; i++)
            {
                var c = src[i];
                span[i] = (char.IsControl(c) && c != '\t') || IsBidiControl(c) || c == '\u2028' || c == '\u2029' ? '?' : c;
            }
        });
        var shown = clean.Length > MaxEchoLength ? clean[..MaxEchoLength] + "..." : clean;
        return $"The host '{shown}' is not accepted: it {why}. Use the server's name or address, as name, name,port or name\\instance.";
    }
}
