/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The ambient snapshot of the FILE-LEVEL alert knobs the MCP tools report (#3712) — the
/// <see cref="DarlingPeerDirectory"/> shape, for the same reason it exists: the alert MCP tools are static
/// methods over the store, and a darling.json value is not in the store. <c>get_alert_settings</c> reads
/// every other knob off <c>config_alert_settings</c>; <c>analysis.uncorroborated_route</c> has no column
/// (one un-landed rung at a time, repo-wide), so without a publish the tool could only report a constant
/// dressed as a reading, which is the placeholder failure <c>McpAlertSettingsKeyTests</c>' own history
/// rejected for <c>ag.disconnect_refire_minutes</c>.
///
/// <para>Published from both config-loading hosts — the worker and the MCP host — because either may reach
/// its config first (the peers precedent, verbatim). Both load the same file in the same process, so the two
/// publishes install the same value. Until something publishes, <see cref="UncorroboratedRoute"/> is null,
/// which <c>get_alert_settings</c> emits as-is: "not published" is a true statement in a test harness and
/// must not read as "digest".</para>
/// </summary>
internal static class DarlingFileLevelAlertSettings
{
    private static volatile string? s_uncorroboratedRoute;

    /// <summary>The published <c>analysis.uncorroboratedRoute</c> as its wire spelling (<c>page</c> /
    /// <c>digest</c>), or null when nothing has published or the file held a value that parses to neither
    /// — in which case the service applies the shipped default, and the reader is told nothing rather
    /// than told the default under the file's name.</summary>
    internal static string? UncorroboratedRoute => s_uncorroboratedRoute;

    /// <summary>Installs the file's value. The parse is <see cref="FindingRouting.TryParseRoute"/>, the same
    /// one <c>DarlingAlertSettings</c> applies, so what this reports is what the gate uses.</summary>
    internal static void Publish(AnalysisConfig? analysis)
    {
        var route = FindingRouting.TryParseRoute(analysis?.UncorroboratedRoute);
        s_uncorroboratedRoute = route is { } r ? FindingRouting.RouteText(r) : null;
    }

    /// <summary>Resets the snapshot — for tests, which must not leak one fixture's file value into another.</summary>
    internal static void ResetForTests() => s_uncorroboratedRoute = null;
}
