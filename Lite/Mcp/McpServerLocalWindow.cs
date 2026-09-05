/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// Supplies the UTC offset for the MCP reads whose window is expressed in the MONITORED SERVER'S local wall
/// clock rather than in UTC — <c>get_cpu_utilization</c> (<c>cpu_utilization_stats.sample_time</c>) and
/// <c>get_default_trace_events</c> (<c>default_trace_events.event_time</c>). Every other MCP read windows on
/// <c>collection_time</c>, which is naive UTC, and needs nothing from here.
///
/// <para><b>Why an MCP tool cannot use the desktop's offset.</b> <c>ServerTimeHelper.UtcOffsetMinutes</c> is
/// process-wide state written only by the WPF tab paths, so it holds whichever server the UI last selected —
/// or, with no tab ever opened, the LITE HOST's own offset, which belongs to no monitored server at all. An
/// MCP tool picks its <c>server_id</c> from <c>server_name</c> independently of that, so reading the static
/// lets the window and the server_id name two different servers. The result is a shifted or empty answer and
/// no error: <c>sample_time</c> compared against the wrong clock simply matches nothing, which reads as "no
/// data" rather than as a fault.</para>
/// </summary>
internal static class McpServerLocalWindow
{
    /// <summary>
    /// This server's own offset, or a ready-to-return status when the store does not hold one. Mirrors
    /// <see cref="ServerResolver.ResolveOrError"/>'s shape: <c>var (offset, error) = await ...; if (error
    /// != null) return error;</c>. <c>OffsetMinutes</c> is not meaningful whenever <c>Error</c> is set.
    ///
    /// <para><b>Why a status and not a fallback.</b> The two candidate fallbacks — 0, or the desktop
    /// static — are both guesses about a real server's clock, and a guess here produces an answer that
    /// cannot be told apart from a correct one. That is the same reasoning that made #2495 refuse an
    /// unusable <c>as_of</c> instead of quietly answering as of now. <c>unavailable</c> rather than
    /// <c>not_collected</c>: the offset IS collected on this engine — <c>server_properties</c> is enabled by
    /// default and writes it on load — so the honest reading is "supported here, not retrievable yet", and
    /// the message names the collector so an operator has somewhere to go.</para>
    /// </summary>
    public static async Task<(int OffsetMinutes, string? Error)> OffsetOrErrorAsync(
        LocalDataService dataService,
        int serverId,
        string serverName)
    {
        var offset = await dataService.GetServerUtcOffsetMinutesAsync(serverId);

        return offset is { } minutes
            ? (minutes, null)
            : (0, McpHelpers.Status(
                "unavailable",
                $"No UTC offset has been collected for '{serverName}', and this read's window is expressed in that "
                + "server's local wall clock, so it cannot be built. Run the server_properties collector for this "
                + "server (it runs on load) and retry."));
    }
}
