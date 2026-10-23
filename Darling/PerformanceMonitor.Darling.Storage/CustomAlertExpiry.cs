// Copyright (c) 2026 Darling Data, LLC. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Globalization;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The optional expiry of a custom alert rule (#6235): one parse, one refusal and one wire spelling for the MCP tools and the web
/// routes, so the two surfaces cannot disagree. It mirrors the mute rules' <c>expires_at_utc</c>: a naive-UTC instant, NULL means the
/// rule never expires, and "expired" is computed (<c>expires_at_utc &lt;= now</c>), never stored.
/// </summary>
internal static class CustomAlertExpiry
{
    /// <summary>The spelling the refusals quote, so a caller sees what to send.</summary>
    internal const string Spelling = "an ISO-8601 UTC timestamp (for example 2026-08-01T00:00:00Z)";

    /// <summary>
    /// Parses an expiry the caller sent. A value with no offset is read as UTC; a value with one is converted. The result is cut to
    /// whole microseconds, the precision <c>timestamp</c> keeps, so what an answer says is what the store holds.
    /// Returns (value, null) when parsed, or (null, message) when it is not a timestamp.
    /// </summary>
    internal static (DateTime? Value, string? Error) Parse(string text, string field)
    {
        if (!DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var parsed))
        {
            return (null, $"'{field}' must be {Spelling}, or empty to clear the expiry.");
        }

        var micro = new DateTime(parsed.Ticks - (parsed.Ticks % 10), DateTimeKind.Utc);
        return (micro, null);
    }

    /// <summary>The refusal for an expiry that is not in the future, or null when it is. A NULL expiry is never refused.</summary>
    internal static string? RefusePast(DateTime? expiresAtUtc, DateTime nowUtc, string field) =>
        expiresAtUtc is { } e && e <= nowUtc
            ? $"'{field}' ({Format(e)}) is already in the past, so the rule would never evaluate. Send a future instant, or omit it for a rule that does not expire."
            : null;

    /// <summary>The wire spelling of an expiry: round-trip ISO-8601 with a trailing Z, or null.</summary>
    internal static string? Format(DateTime? expiresAtUtc) =>
        expiresAtUtc is { } e
            ? DateTime.SpecifyKind(e, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture)
            : null;

    /// <summary>
    /// The expiry of a CREATE (#6235): omitted or empty means the rule does not expire; otherwise an ISO-8601 UTC instant that must be in
    /// the future. Shared by the MCP create tool, its agent arm and the web create route, so the three refuse the same way.
    /// </summary>
    internal static (DateTime? Expiry, string? Error) ResolveCreateExpiry(string? sent)
    {
        if (string.IsNullOrWhiteSpace(sent))
        {
            return (null, null);
        }

        var (parsed, parseError) = CustomAlertExpiry.Parse(sent, "expires_at_utc");
        if (parseError is not null)
        {
            return (null, parseError);
        }

        var past = CustomAlertExpiry.RefusePast(parsed, DateTime.UtcNow, "expires_at_utc");
        return past is not null ? (null, past) : (parsed, null);
    }

    /// <summary>
    /// The expiry of an UPDATE (#6235), in the vocabulary the description uses: <c>null</c> keeps <paramref name="current"/>, empty
    /// clears it, anything else is an ISO-8601 UTC instant. A value that is NOT a change (the rule's own stored expiry sent back) is
    /// never refused, so re-saving an already-expired rule still works; a new value must be in the future.
    /// </summary>
    internal static (DateTime? Expiry, string? Error) ResolveUpdateExpiry(string? sent, DateTime? current)
    {
        if (sent is null)
        {
            return (current, null);
        }

        if (string.IsNullOrWhiteSpace(sent))
        {
            return (null, null);
        }

        var (parsed, parseError) = CustomAlertExpiry.Parse(sent, "expires_at_utc");
        if (parseError is not null)
        {
            return (null, parseError);
        }

        if (parsed == current)
        {
            return (parsed, null);
        }

        var past = CustomAlertExpiry.RefusePast(parsed, DateTime.UtcNow, "expires_at_utc");
        return past is not null ? (null, past) : (parsed, null);
    }
}
