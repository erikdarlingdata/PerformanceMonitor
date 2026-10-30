/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// What the Agent Alerts tab reads for one server (#6874): the same answer <c>get_agent_alerts</c> gives. <see cref="Status"/> is
/// <c>ok</c> or <c>gaps</c> when a capture exists (the words of <see cref="AgentAlertStatus"/>), and otherwise the read's own word:
/// <c>not_collected</c> (this engine has no Agent alerts), <c>precondition</c> (the collector is refused or switched off here) or
/// <c>unavailable</c> (no capture in the window). <see cref="Message"/> is the sentence for the last three.
/// </summary>
public sealed record ViewerAgentAlertsRead(string Status, string? Message, AgentAlertInstanceStatus? InstanceStatus, AgentAlertRead? Read)
{
    public const string NotCollected = "not_collected";
    public const string Precondition = "precondition";
    public const string Unavailable = "unavailable";

    /// <summary>A capture exists, so there is an instance status to show.</summary>
    public bool HasCapture => Read is { HasCapture: true };
}

public sealed partial class ViewerDataService
{
    /// <summary>The window of captures the tab reads, as <c>get_agent_alerts</c> defaults to: fired is judged between the first and last.</summary>
    public const int AgentAlertsHoursBack = 24;

    /// <summary>
    /// The server's SQL Server Agent alerts and the gaps in them over the <paramref name="hoursBack"/> hours ending at
    /// <paramref name="windowEndUtc"/>. The reader and the status are the ones the MCP tool calls, so both surfaces say the same words.
    /// With no capture the answer follows the tool's order: the engine gap (<c>not_collected</c>), then the collector's latest named
    /// skip or its switched-off route (<c>precondition</c>), then <c>unavailable</c>.
    /// </summary>
    public async Task<ViewerAgentAlertsRead> ReadAgentAlertsAsync(
        DarlingServer server, DateTime windowEndUtc, int hoursBack = AgentAlertsHoursBack, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(server);
        var read = await DarlingAgentAlertReader.ReadAsync(
            _dataSource, server.ServerId, windowEndUtc.AddHours(-hoursBack), windowEndUtc,
            ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        if (read.HasCapture)
        {
            var status = AgentAlertStatus.Evaluate(read.Alerts);
            return new ViewerAgentAlertsRead(status.Word, null, status, read);
        }

        var engineGap = ViewerServerTab.EngineGapNote(server.DisplayName, server.EngineEdition, server.EngineKind, "agent_alerts", server.AwsRdsKind);
        if (engineGap is not null)
        {
            return new ViewerAgentAlertsRead(ViewerAgentAlertsRead.NotCollected, engineGap, null, read);
        }

        var (text, _) = await ViewerServerTab.ReadEngineGapStateAsync(
            (serverId, collector) => GetCollectorLastRunAsync(serverId, collector, cancellationToken), null,
            server.ServerId, server.DisplayName, server.EngineEdition, server.EngineKind, "agent_alerts", 0,
            (serverId, collector) => GetLatestCollectorOutcomeAsync(serverId, collector, cancellationToken), server.AwsRdsKind,
            () => GetServiceStartedAsync(cancellationToken));
        if (!string.IsNullOrEmpty(text))
        {
            return new ViewerAgentAlertsRead(ViewerAgentAlertsRead.Precondition, text, null, read);
        }

        return new ViewerAgentAlertsRead(
            ViewerAgentAlertsRead.Unavailable,
            string.Create(CultureInfo.InvariantCulture,
                $"No SQL Server Agent alert capture in the {hoursBack}-hour window ending {DateTime.SpecifyKind(windowEndUtc, DateTimeKind.Utc):o}. The agent_alerts collector has not run in it."),
            null, read);
    }
}
