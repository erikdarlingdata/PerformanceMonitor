/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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
    /// This server's collected offset, or 0 when the store holds none.
    ///
    /// <para><b>Why 0 and not the desktop static.</b> 0 is a fixed point: two identical MCP calls get the
    /// same window from it no matter what the desktop is doing, which is the property this whole seam exists
    /// to restore. Falling back to the static would reinstate exactly the coupling being removed, and would
    /// do it on the least visible path.</para>
    ///
    /// <para><b>Why a fallback and not a refusal.</b> A refusal here would fire on the state every server
    /// passes through on its first cycle — <c>server_properties</c> is an on-load collector, so a server has
    /// no stored offset until it has completed one — and it would fire BEFORE the read, pre-empting the two
    /// answers that are more fundamental than any window: <c>not_collected</c> when the engine cannot run
    /// this collector at all (#2511), and the read's own <c>empty</c> when nothing has been collected yet.
    /// <c>EngineCapabilityMissTests</c> pins both, and names the failure mode precisely: rendering "we do not
    /// know" as "this will never work" is the same defect wearing the fix's clothes. A server with no offset
    /// almost always has no server-local rows either, so the read reaches those explanations on its own.</para>
    ///
    /// <para><b>The residual, stated rather than hidden.</b> A server that has server-local ROWS but no
    /// stored offset — data collected before schema v42 added the column, with no <c>server_properties</c>
    /// collection since — gets a UTC window over a server-local column, so its answer is skewed by the
    /// server's true offset. That is the pre-existing behaviour for that server rather than a new one, it is
    /// self-correcting on the next on-load collection, and it is no longer contingent on which tab the
    /// desktop last had open.</para>
    /// </summary>
    public static async Task<int> OffsetForAsync(LocalDataService dataService, int serverId)
        => await dataService.GetServerUtcOffsetMinutesAsync(serverId) ?? 0;
}
