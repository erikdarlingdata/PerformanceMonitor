/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The one policy for what counts as a secret in a key name or in a text value. The slow-read log applies it at
/// write time and the diagnostics bundle applies it to every key and string it exports, so the two cannot drift.
/// A key is a secret when its lower-cased name carries one of <see cref="IsSecretKey"/>'s fragments; a value is a
/// secret when it names a credential (<see cref="ValueLooksSecret"/>). <see cref="RedactText"/> removes the secret
/// part of a longer text where it can be bounded.
/// </summary>
internal static class SecretTextGuard
{
    /// <summary>What replaces a secret value.</summary>
    internal const string RedactedMarker = "[redacted]";

    private static readonly string[] s_secretFragments =
    {
        "password", "passwd", "pwd", "secret", "token", "apikey", "api_key", "key", "credential", "webhook",
        "connectionstring", "connection_string", "auth",
    };

    /* Fragments that mark a VALUE as a secret. Narrower than the key list: a bare "key" or "auth" would drop ordinary text. */
    private static readonly string[] s_secretValueFragments =
    {
        "password", "passwd", "pwd=", "secret", "token=", "apikey", "api_key", "bearer ",
    };

    private static readonly Regex s_credentialShape = new(
        @"://[^/@\s]+:[^/@\s]*@|(password|pwd)\s*=",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant,
        TimeSpan.FromMilliseconds(100));

    /* A key=value pair in a connection string or log line: the value runs to the next ';' or the end of the line.
       A quoted value runs to its closing quote. */
    private static readonly Regex s_secretPair = new(
        @"(?<key>\b(?:password|passwd|pwd|encrypted[ _]?password|token|api[ _]?key|secret))(?<sep>\s*[=:]\s*)(?<value>""[^""]*""|'[^']*'|[^;\r\n""']*)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant,
        TimeSpan.FromMilliseconds(250));

    private static readonly Regex s_bearer = new(
        @"(?<key>\bbearer)(?<sep>\s+)(?<value>[^\s;,""']+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant,
        TimeSpan.FromMilliseconds(250));

    private static readonly Regex s_redactedAssignment = new(
        @"(password|pwd)\s*=\s*\[redacted\]",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant,
        TimeSpan.FromMilliseconds(250));

    private static readonly Regex s_urlCredential = new(
        @"(?<scheme>[a-z][a-z0-9+.\-]*://)[^/@\s:]+:[^/@\s]*@",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant,
        TimeSpan.FromMilliseconds(250));

    /// <summary>True when a key's lower-cased name carries a secret fragment.</summary>
    internal static bool IsSecretKey(string key)
    {
        if (key == "dedup_key")
        {
            return false;
        }

        var lowered = key.ToLowerInvariant();
        foreach (var fragment in s_secretFragments)
        {
            if (lowered.Contains(fragment, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// True when a (trimmed) text value carries a secret fragment or has the shape of a credential
    /// (<c>://user:pass@</c> or <c>password=</c>). A regex timeout counts as secret: the safe direction.
    /// </summary>
    internal static bool ValueLooksSecret(string cleaned)
    {
        foreach (var fragment in s_secretValueFragments)
        {
            if (cleaned.Contains(fragment, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        try
        {
            return s_credentialShape.IsMatch(cleaned);
        }
        catch (RegexMatchTimeoutException)
        {
            return true;
        }
    }

    /// <summary>
    /// Removes the secret part of a text where it can be bounded: <c>Password=...</c> up to the <c>;</c> or the end of
    /// the line, <c>Bearer ...</c> to the next space, and <c>://user:pass@</c> to <c>://[redacted]@</c>. A text that
    /// still has the shape of a credential afterwards is replaced whole. A text with no secret comes back unchanged.
    /// </summary>
    internal static string RedactText(string text)
    {
        if (text.Length == 0)
        {
            return text;
        }

        try
        {
            var result = s_urlCredential.Replace(text, m => m.Groups["scheme"].Value + RedactedMarker + "@");
            result = s_secretPair.Replace(result, m => m.Groups["key"].Value + m.Groups["sep"].Value + RedactedMarker);
            result = s_bearer.Replace(result, m => m.Groups["key"].Value + m.Groups["sep"].Value + RedactedMarker);
            var probe = s_redactedAssignment.Replace(result, string.Empty);
            return s_credentialShape.IsMatch(probe) ? RedactedMarker : result;
        }
        catch (RegexMatchTimeoutException)
        {
            return RedactedMarker;
        }
    }
}
