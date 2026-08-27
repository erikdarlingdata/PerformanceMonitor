/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1560 (live MCP toggle): the supervisor's pure decision table and the worker→host state seam. The
/// viewer's Settings toggle writes config_service.mcp_enabled / mcp_port; the worker publishes the live
/// values to <see cref="McpRuntimeState"/> on every reload; the host's supervisor polls and
/// starts/stops/rebinds WITHOUT a service restart. These pins hold the decision table and the seam's
/// coherence; the Kestrel lifecycle itself is exercised by the gated live suite and field use.
/// </summary>
public sealed class DarlingMcpSupervisorTests
{
    [Theory]
    /* not running: start only when enabled */
    [InlineData(false, 0, false, 5152, DarlingMcpHostService.McpSupervisorAction.None)]
    [InlineData(false, 0, true, 5152, DarlingMcpHostService.McpSupervisorAction.Start)]
    /* running: stop on disable — the no-restart kill switch, network-exposed included */
    [InlineData(true, 5152, false, 5152, DarlingMcpHostService.McpSupervisorAction.Stop)]
    /* running + enabled: rebind only on a port change */
    [InlineData(true, 5152, true, 5152, DarlingMcpHostService.McpSupervisorAction.None)]
    [InlineData(true, 5152, true, 5199, DarlingMcpHostService.McpSupervisorAction.Restart)]
    public void DecideMcpAction_CoversTheWholeTable(
        bool running, int runningPort, bool enabled, int desiredPort, DarlingMcpHostService.McpSupervisorAction expected)
    {
        Assert.Equal(expected, DarlingMcpHostService.DecideMcpAction(running, runningPort, enabled, desiredPort));
    }

    [Fact]
    public void McpRuntimeState_UnpublishedReadsNull_ThenLatestCoherentSnapshot()
    {
        var state = new McpRuntimeState();

        /* Pre-publish: the host falls back to the FILE values (the pre-#1560 behavior). */
        Assert.Null(state.Read());

        state.Publish(enabled: true, port: 5152);
        var first = state.Read();
        Assert.NotNull(first);
        Assert.True(first!.Enabled);
        Assert.Equal(5152, first.Port);

        /* A re-publish swaps ONE immutable snapshot reference — a reader can never see a torn
           (old Enabled, new Port) mix. */
        state.Publish(enabled: false, port: 5199);
        var second = state.Read();
        Assert.NotNull(second);
        Assert.False(second!.Enabled);
        Assert.Equal(5199, second.Port);
        Assert.NotSame(first, second);
    }

    /// <summary>Cadence tripwires: the supervisor reacts within ~one reload beacon (5s poll) and a
    /// persistent start failure retries on a calm 30s backoff, not every tick.</summary>
    [Fact]
    public void SupervisorCadences_ArePinned()
    {
        Assert.Equal(TimeSpan.FromSeconds(5), DarlingMcpHostService.SupervisorPollInterval);
        Assert.Equal(TimeSpan.FromSeconds(30), DarlingMcpHostService.FailedStartBackoff);
    }

    /* ================================================================================================
       #2389: the SOURCE pin. The defect was not in a decision function — it was that the supervisor's
       `published?.Enabled ?? config.Mcp.Enabled` silently took a side and had no way to say so, while
       the MCP network block beside it stayed file-authoritative. A behavioural test of the old code
       passes: it resolved the right value, it just could not report it. So pin the CALL SITE, which is what a
       future "simplification" back to the null-coalesce would break.
       ================================================================================================ */

    [Fact]
    public void TheSupervisor_ResolvesThroughTheProvenanceAwareHelper_NotASilentNullCoalesce()
    {
        var source = ReadHostSource("DarlingMcpHostService.cs");

        /* The silent form, gone from both halves of the resolution. */
        Assert.DoesNotContain("published?.Enabled ??", source, StringComparison.Ordinal);
        Assert.DoesNotContain("published?.Port ??", source, StringComparison.Ordinal);

        /* Replaced by the shared resolver, and BOTH of its diagnostics wired up: the override report (the
           disagreement, at the point of override) and the origin clause (on the start line the operator
           greps). Either one missing leaves half the confusion in place. */
        Assert.Contains("DarlingHostBinding.ResolveEndpointToggle(", source, StringComparison.Ordinal);
        Assert.Contains("DarlingHostBinding.DescribeToggleOverride(", source, StringComparison.Ordinal);
        Assert.Contains("DarlingHostBinding.DescribeToggleOrigin(", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The disagreement is a WARNING and it is deduplicated, not emitted on every 5s poll tick: the report is
    /// compared to the last one emitted, and cleared when the planes agree again so a LATER re-divergence is
    /// still reported. An undeduplicated warning would be 17,280 lines a day and get filtered out, which is the
    /// same silence in a different costume.
    /// </summary>
    [Fact]
    public void TheOverrideReport_IsWarnedOncePerDistinctState()
    {
        var source = ReadHostSource("DarlingMcpHostService.cs");

        Assert.Contains("_logger.LogWarning(\"{Report}\", overrideReport);", source, StringComparison.Ordinal);
        Assert.Contains("!string.Equals(overrideReport, lastOverrideReport, StringComparison.Ordinal)", source, StringComparison.Ordinal);
        Assert.Contains("lastOverrideReport = overrideReport;", source, StringComparison.Ordinal);
    }

    /// <summary>Reads the real host source, copied beside the test binary by the csproj (the same fixture the
    /// Host-header guard pins parse).</summary>
    private static string ReadHostSource(string fileName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", fileName);
        Assert.True(File.Exists(path), $"{fileName} was not copied beside the test binary — check the csproj None/Link item.");

        var source = File.ReadAllText(path);
        /* Guard the guard: an unrecognizable restructure must fail loudly, not pass vacuously. */
        Assert.Contains("var published = _state.Read();", source, StringComparison.Ordinal);
        return source;
    }
}
