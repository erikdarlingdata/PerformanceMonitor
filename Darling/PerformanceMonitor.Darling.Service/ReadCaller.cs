/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Who made a read (#5793), as one short string that holds no personal data. The read record (<c>collect.slow_reads.caller</c>
/// and <c>collect.read_latency.distinct_callers</c>) stores only what this class returns; it never receives the request, the
/// cookie, a token or an address.
///
/// <para><b>MCP:</b> the client's own <c>clientInfo</c> name and version, as in <c>claude-desktop 1.2.3</c>. The MCP host runs
/// stateless, so a client on the <c>initialize</c> handshake (protocol 2025-11-25 and earlier) has no client info on a tool
/// call: the handshake ran in another request. For that client the caller is <c>ua:</c> plus the first product token of its
/// <c>User-Agent</c> (<c>ua:node/22.3.0</c>), the one header read here. <c>unknown</c> when neither is present.</para>
///
/// <para><b>Web and composed panels:</b> <c>web:</c> plus the first 12 hex characters of an HMAC-SHA256 of the web session
/// key, keyed by a salt drawn at random when the process starts. The salt is never stored or logged, so the stored value
/// cannot be turned back into the session key, and it changes at every service start. Two reads in one session share the
/// value; two sessions do not. A read with no session (a loopback server with no sign-in) records <c>web:none</c>.</para>
/// </summary>
internal static class ReadCaller
{
    /// <summary>The caller of an MCP read that sent neither client info nor a User-Agent.</summary>
    internal const string Unknown = "unknown";

    /// <summary>The caller of a web read that carried no session.</summary>
    internal const string WebNoSession = "web:none";

    /// <summary>The longest caller stored; a longer client name and version is cut to this.</summary>
    internal const int MaxLength = 80;

    /// <summary>How many hex characters of the session hash are kept.</summary>
    internal const int WebHashHexLength = 12;

    /* Drawn once per process and held only here: never persisted, never logged. */
    private static readonly byte[] s_webSalt = RandomNumberGenerator.GetBytes(32);

    /* The caller of the MCP tool call in progress, set by the tool filter around run_custom_view_panel so the shared
       composed-panel runner can record it without a new tool parameter. */
    private static readonly AsyncLocal<string?> s_ambientMcpCaller = new();

    /// <summary>The MCP caller set by the tool filter for the call in progress, or null outside one.</summary>
    internal static string? AmbientMcpCaller => s_ambientMcpCaller.Value;

    /// <summary>Sets the MCP caller for the rest of the current async flow (the tool filter's call to the tool).</summary>
    internal static void SetAmbientMcpCaller(string? caller) => s_ambientMcpCaller.Value = caller;

    /// <summary>The MCP caller: client info name and version when the client sent a name, else <c>ua:</c> and the first
    /// product token of <paramref name="userAgent"/>, else <c>unknown</c>. Trimmed to <see cref="MaxLength"/>.</summary>
    internal static string OfMcpClient(string? clientName, string? clientVersion, string? userAgent)
    {
        var name = clientName?.Trim();
        if (!string.IsNullOrEmpty(name))
        {
            var version = clientVersion?.Trim();
            return Cap(string.IsNullOrEmpty(version) ? name : name + " " + version);
        }

        var product = FirstProductToken(userAgent);
        return product is null ? Unknown : Cap("ua:" + product);
    }

    /// <summary>The first product token of a User-Agent value (<c>node/22.3.0</c> from <c>node/22.3.0 (win32; x64)</c>), or
    /// null when there is none. Comments in parentheses and every later token are dropped.</summary>
    internal static string? FirstProductToken(string? userAgent)
    {
        if (string.IsNullOrWhiteSpace(userAgent))
        {
            return null;
        }

        var text = userAgent.TrimStart();
        var end = 0;
        while (end < text.Length && !char.IsWhiteSpace(text[end]) && text[end] != '(' && !char.IsControl(text[end]))
        {
            end++;
        }

        return end == 0 ? null : text[..end];
    }

    /// <summary>The web caller for <paramref name="sessionKey"/> under this process's salt.</summary>
    internal static string OfWebSession(string? sessionKey) => OfWebSession(sessionKey, s_webSalt);

    /// <summary>The web caller for <paramref name="sessionKey"/> under <paramref name="salt"/>: <c>web:</c> plus the first
    /// <see cref="WebHashHexLength"/> hex characters of HMAC-SHA256(salt, key), or <c>web:none</c> with no key.</summary>
    internal static string OfWebSession(string? sessionKey, byte[] salt)
    {
        if (string.IsNullOrEmpty(sessionKey))
        {
            return WebNoSession;
        }

        var mac = HMACSHA256.HashData(salt, Encoding.UTF8.GetBytes(sessionKey));
        return "web:" + Convert.ToHexStringLower(mac)[..WebHashHexLength];
    }

    private static string Cap(string value) => value.Length > MaxLength ? value[..MaxLength] : value;
}
