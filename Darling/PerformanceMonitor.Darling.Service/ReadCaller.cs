/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
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

    /* #6231: the server-side identity of the bearer token the call came in on, set beside the caller. */
    private static readonly AsyncLocal<string?> s_ambientMcpToken = new();

    /// <summary>The token identity set by the tool filter for the call in progress, or null outside one.</summary>
    internal static string? AmbientMcpToken => s_ambientMcpToken.Value;

    /// <summary>Sets the token identity for the rest of the current async flow (the tool filter's call to the tool).</summary>
    internal static void SetAmbientMcpToken(string? tokenIdentity) => s_ambientMcpToken.Value = tokenIdentity;

    /// <summary>
    /// What the service itself knows about who made a call: the bearer token the request carried, shown as the first 8 hex
    /// characters of its SHA-256 (<c>token 3fa9c1d2</c>), which names a token without revealing it, or <c>no token</c> when the
    /// request carried none. The service has one token per endpoint and no token labels, so this is the whole server-side identity.
    /// </summary>
    internal static string OfMcpToken(string? bearerToken) =>
        string.IsNullOrEmpty(bearerToken)
            ? "no token"
            : "token " + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(bearerToken)))[..8].ToLowerInvariant();

    /// <summary>The server-side identity of a call (#6542): <c>named token NAME</c> when a named token was presented, else the fingerprint
    /// of the legacy token (<see cref="OfMcpToken"/>). A name is a label the operator chose, never a secret.</summary>
    internal static string OfMcpIdentity(string? namedTokenName, string? bearerToken) =>
        string.IsNullOrEmpty(namedTokenName) ? OfMcpToken(bearerToken) : "named token " + namedTokenName;

    /// <summary>
    /// The text a write log line carries for who made the call: the token identity the service worked out, then the name the client
    /// gave itself, labelled as the client's own claim (any client can send any name).
    /// </summary>
    internal static string WriteIdentity() =>
        (AmbientMcpToken ?? "token unknown") + ", client says: " + (AmbientMcpCaller ?? Unknown);

    /// <summary>The token part of the identity a store row records for an MCP write: <c>named token NAME</c> when a named token was
    /// used, <c>legacy token</c> for the endpoint's one legacy token (the fingerprint <see cref="WriteIdentity"/> logs is left out of
    /// store rows, which read-only logins and agent briefs can read), <c>no token</c> when the request carried none.</summary>
    internal static string StoreTokenIdentity()
    {
        var token = AmbientMcpToken;
        if (string.IsNullOrEmpty(token))
            return "token unknown";
        if (token.StartsWith("named token ", StringComparison.Ordinal) || token == "no token")
            return token;
        return "legacy token";
    }

    /// <summary>The identity an MCP write records in a store row: <see cref="StoreTokenIdentity"/>, then the name the client gave
    /// itself, labelled as the client's own claim the way <see cref="WriteIdentity"/> labels it. Log lines keep <see cref="WriteIdentity"/>.</summary>
    internal static string StoreIdentity() =>
        StoreTokenIdentity() + ", client says: " + (AmbientMcpCaller ?? Unknown);

    /// <summary>The MCP caller set by the tool filter for the call in progress, or null outside one.</summary>
    internal static string? AmbientMcpCaller => s_ambientMcpCaller.Value;

    /// <summary>Sets the MCP caller for the rest of the current async flow (the tool filter's call to the tool).</summary>
    internal static void SetAmbientMcpCaller(string? caller) => s_ambientMcpCaller.Value = caller;

    /// <summary>The MCP caller: client info name and version when the client sent a name, else <c>ua:</c> and the first
    /// product token of <paramref name="userAgent"/>, else <c>unknown</c>. Every part is client-supplied text, so each one
    /// runs through <see cref="CustomAlertEvaluator.SanitizeDisplayText"/> (#5654) before it is stored: a line break or
    /// other control character becomes one space, and a character that reorders or hides text (U+202E and the rest of the
    /// Unicode Format category) is dropped, so the caller <c>get_slow_reads</c> serves is the text that was stored. The
    /// result is then cut to <see cref="MaxLength"/> on whole text elements, never inside a surrogate pair or an emoji
    /// sequence.</summary>
    internal static string OfMcpClient(string? clientName, string? clientVersion, string? userAgent)
    {
        var name = CustomAlertEvaluator.SanitizeDisplayText(clientName, int.MaxValue);
        if (name.Length > 0)
        {
            var version = CustomAlertEvaluator.SanitizeDisplayText(clientVersion, int.MaxValue);
            return Cap(version.Length == 0 ? name : name + " " + version);
        }

        var product = CustomAlertEvaluator.SanitizeDisplayText(FirstProductToken(userAgent), int.MaxValue);
        return product.Length == 0 ? Unknown : Cap("ua:" + product);
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

    /* Cut on text elements, not on a UTF-16 index: an index cut can keep the high half of a surrogate pair alone, which the
       store's UTF-8 writer refuses or the JSON writer turns into U+FFFD (#3625). The kept text is at most MaxLength UTF-16
       characters and ends on a whole text element; a space left at the cut is trimmed. */
    private static string Cap(string value)
    {
        if (value.Length <= MaxLength)
        {
            return value;
        }

        var end = 0;
        while (end < value.Length)
        {
            var next = StringInfo.GetNextTextElementLength(value, end);
            if (end + next > MaxLength)
            {
                break;
            }

            end += next;
        }

        return value[..end].TrimEnd();
    }
}
