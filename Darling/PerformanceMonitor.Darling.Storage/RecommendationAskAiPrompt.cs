/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The text "Ask AI" copies for a Recommendations finding (#6389). One builder for the Viewer's card and the web's route, so the two
/// say the same thing for the same finding. It holds no key and calls no model: it is a prompt for the reader's own MCP client.
/// </summary>
public static class RecommendationAskAiPrompt
{
    /// <summary>
    /// The MCP investigation prompt for a finding. Pure (no clock, no UI) so the interpolation is unit-testable. The window times are
    /// formatted in whatever zone the caller passed (both callers pass the monitored server's local time). Ported verbatim from Lite's prompt.
    /// </summary>
    public static string Build(string serverName, string title, DateTime from, DateTime to)
    {
        return string.Format(
            CultureInfo.InvariantCulture,
            "Using the PerformanceMonitor MCP tools, investigate this finding on server \"{0}\": " +
            "\"{1}\". It was flagged around {2:yyyy-MM-dd HH:mm}–{3:HH:mm}. Call analyze_server / " +
            "get_analysis_findings and the relevant wait/blocking/memory tools, then tell me the " +
            "likely cause and what to do.",
            serverName, title, from, to);
    }

    /// <summary>
    /// The window of a finding that carries none: the two hours before <paramref name="utcNow"/>, each end shown on
    /// <paramref name="serverClock"/> (#4766). The two hours are real hours: taking the server's "now" and subtracting two hours
    /// from that wall-clock time spans one real hour too few across a spring-forward change.
    /// </summary>
    public static (DateTime From, DateTime To) FallbackWindow(ServerClock serverClock, DateTime utcNow)
    {
        ArgumentNullException.ThrowIfNull(serverClock);
        return (serverClock.ToServerLocal(utcNow.AddHours(-2)), serverClock.ToServerLocal(utcNow));
    }

    /// <summary>The prompt for a finding on <paramref name="serverClock"/>: its window (or the fallback when it has none) in server-local time.</summary>
    public static string BuildFor(string serverName, string title, DateTime? windowStartUtc, DateTime? windowEndUtc, ServerClock serverClock, DateTime utcNow)
    {
        ArgumentNullException.ThrowIfNull(serverClock);
        var (from, to) = windowStartUtc is { } start && windowEndUtc is { } end
            ? (serverClock.ToServerLocal(start), serverClock.ToServerLocal(end))
            : FallbackWindow(serverClock, utcNow);
        return Build(serverName, title, from, to);
    }
}
