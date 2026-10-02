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
/// The ambient snapshot of the FILE half of the #3712 route knob, for the MCP tools — the
/// <see cref="DarlingPeerDirectory"/> shape, for the same reason it exists: the alert MCP tools are static
/// methods over the store, and a darling.json value is not in the store. The knob has TWO homes since V137:
/// <c>config_alert_settings.analysis_uncorroborated_route</c> (read off the row like every other knob, and
/// the home that wins when it holds a route) and darling.json's <c>analysis.uncorroboratedRoute</c>, which
/// governs when the column is NULL. <c>get_alert_settings</c> reports the EFFECTIVE route and which home
/// decided it, so it still needs the file's value beside the row's, and this is the file value's only road
/// into a static tool. Before the rung this publish WAS the reading (the column did not exist, and without
/// it the tool could only report a constant dressed as a reading, the placeholder failure
/// <c>McpAlertSettingsKeyTests</c>' own history rejected for <c>ag.disconnect_refire_minutes</c>); now it is
/// the fall-through arm, and the resolver is <c>DarlingAlertSettings.ResolveUncorroboratedRoute</c>.
///
/// <para>Published from both config-loading hosts — the worker and the MCP host — because either may reach
/// its config first (the peers precedent, verbatim). Both load the same file in the same process, so the two
/// publishes install the same value. The worker publishes the file's value whether or not the store has
/// swapped its section in: <c>AnalysisConfig.UncorroboratedRoute</c> is the file half and stays the file half
/// across the swap (<c>StoreConfigProvider.LoadViewAsync</c> carries it), and this reads only that member,
/// never the store half beside it. Until something publishes, <see cref="UncorroboratedRoute"/> is null,
/// which the resolver reads as "the file holds no route" and resolves to <c>default</c>: a true statement in
/// a test harness, where "file" would claim a value nothing loaded.</para>
/// </summary>
internal static class DarlingFileLevelAlertSettings
{
    private static volatile string? s_uncorroboratedRoute;

    /// <summary>The published <c>analysis.uncorroboratedRoute</c> as its wire spelling (<c>page</c> /
    /// <c>digest</c>), or null when nothing has published or the file held a value that parses to neither
    /// — in which case the file half contributes nothing to the resolution, and a NULL store column falls
    /// through to the shipped default under the name <c>default</c> rather than under the file's.</summary>
    internal static string? UncorroboratedRoute => s_uncorroboratedRoute;

    /// <summary>Installs the file's value — <see cref="AnalysisConfig.UncorroboratedRoute"/> alone, never the
    /// store half beside it. The parse is <see cref="FindingRouting.TryParseRoute"/>, the same one the resolver
    /// applies to the file arm, so what this reports is what the gate falls through to.</summary>
    internal static void Publish(AnalysisConfig? analysis)
    {
        var route = FindingRouting.TryParseRoute(analysis?.UncorroboratedRoute);
        s_uncorroboratedRoute = route is { } r ? FindingRouting.RouteText(r) : null;
    }

    /// <summary>Resets the snapshot — for tests, which must not leak one fixture's file value into another.</summary>
    internal static void ResetForTests() => s_uncorroboratedRoute = null;
}
