/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;

namespace PerformanceMonitor.Common;

/// <summary>
/// Shared helpers for MCP tools.
/// </summary>
internal static class McpHelpers
{
    /// <summary>
    /// Maximum hours of history allowed (7 days).
    /// </summary>
    public const int MaxHoursBack = 168;

    /// <summary>
    /// Maximum rows/items to return.
    /// </summary>
    public const int MaxTop = 1000;

    /// <summary>
    /// Shared JSON serializer options for MCP tool results — compact, not indented (#2350).
    ///
    /// <para>The only consumer of an MCP tool result is a language model, and indentation buys a model
    /// nothing. It was costing roughly 23% of the bytes of a record-heavy result (measured on a 15-field
    /// blocking-event shape: 2,977 → 2,297 at 10 rows, 29,082 → 22,462 at 100). <b>The token saving is smaller
    /// than the byte saving</b> — BPE tokenizers pack runs of spaces efficiently — so this is not the 23%
    /// win it looks like in bytes. It is still free, and it compounds where it matters: tool results are the
    /// bulk of what fills an agent's context on a real incident, and the fleet-wide reads are the widest
    /// results we return.</para>
    ///
    /// <para>Deliberately NOT applied to the config files (servers.json, profiles, schedules, alert state).
    /// Those are read and hand-edited by people, and <c>ServerManager</c>/<c>ProfileManager</c>/
    /// <c>ScheduleManager</c> keep their own indented options for that reason. This object is MCP output only
    /// — every one of its ~78 call sites serializes a tool result or the web endpoint twin of one.</para>
    ///
    /// <para>Nothing parses our output positionally: it is JSON to a JSON reader on both sides, and the tests
    /// that touch this object assert field NAMES (there is no naming policy here, so snake_case comes from
    /// <c>[JsonPropertyName]</c> attributes) rather than layout.</para>
    /// </summary>
    public static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = false };

    /// <summary>
    /// Truncates a string to the specified maximum length, adding a truncation suffix.
    /// </summary>
    public static string? Truncate(string? value, int maxLength)
    {
        if (value == null || value.Length <= maxLength) return value;
        return value[..maxLength] + "... (truncated)";
    }

    /// <summary>
    /// Validates hours_back parameter. Returns null if valid, error message if invalid.
    /// </summary>
    public static string? ValidateHoursBack(int hoursBack)
    {
        if (hoursBack <= 0)
            return $"Invalid hours_back value '{hoursBack}'. Must be a positive integer (1-{MaxHoursBack}).";
        if (hoursBack > MaxHoursBack)
            return $"hours_back value '{hoursBack}' exceeds maximum of {MaxHoursBack} hours (7 days). Use a smaller value.";
        return null;
    }

    /// <summary>
    /// Validates top/limit parameter. Returns null if valid, error message if invalid.
    /// </summary>
    public static string? ValidateTop(int top, string paramName = "limit")
    {
        if (top <= 0)
            return $"Invalid {paramName} value '{top}'. Must be a positive integer (1-{MaxTop}).";
        if (top > MaxTop)
            return $"{paramName} value '{top}' exceeds maximum of {MaxTop}. Use a smaller value.";
        return null;
    }

    /// <summary>
    /// Formats an exception as a user-friendly error message.
    /// </summary>
    public static string FormatError(string operation, Exception ex)
    {
        return $"Error during {operation}: {ex.Message}";
    }

    /// <summary>
    /// Builds a consistent JSON envelope for a NON-DATA outcome — a legitimate miss — so an LLM
    /// consumer can branch on the kind of nothing it got back. Data-bearing results keep their own
    /// shape and must NOT use this.
    /// </summary>
    /// <param name="status">
    /// One word from the small miss vocabulary:
    /// <list type="bullet">
    /// <item><c>empty</c> — a true negative: we looked and there is genuinely nothing (all clear).</item>
    /// <item><c>not_collected</c> — the input names something this server does not collect.</item>
    /// <item><c>unavailable</c> — it existed but is not retrievable now (evicted, purged, or not collected yet).</item>
    /// </list>
    /// </param>
    /// <param name="message">The human-readable explanation (kept intact from the prior bare-string text).</param>
    /// <param name="hints">Optional structured payload to help the caller recover (e.g. the counters that ARE collected). Omitted from the JSON when null.</param>
    public static string Status(string status, string message, object? hints = null)
    {
        return hints is null
            ? JsonSerializer.Serialize(new { status, message }, JsonOptions)
            : JsonSerializer.Serialize(new { status, message, hints }, JsonOptions);
    }
}
