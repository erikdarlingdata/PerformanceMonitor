/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The start-up and <c>--validate-config</c> checks for agent dispatch (#6263, part of #5632): setups where
/// <c>mcp.allowInvestigations</c> is on but no agent can work. One pure decision, so the service log and the CLI verb
/// say the same thing. A <see cref="AgentDispatchSeverity.Problem"/> is a setup that cannot work. A
/// <see cref="AgentDispatchSeverity.Hint"/> is a setup that works but will open no request yet. Each message says
/// what to change. Nothing here changes how the service starts: the tools stay off or served exactly as
/// <see cref="DarlingMcpWritePolicy"/> decides.
/// </summary>
internal static class DarlingAgentDispatchChecks
{
    internal enum AgentDispatchSeverity
    {
        /// <summary>The setup cannot work as written.</summary>
        Problem,

        /// <summary>The setup works, but nothing will happen until the operator adds the missing piece.</summary>
        Hint,
    }

    internal readonly record struct AgentDispatchFinding(AgentDispatchSeverity Severity, string Message);

    internal const string WritesWinMessage =
        "mcp.allowInvestigations and mcp.allowWrites are both on. mcp.allowWrites wins: the write tools are served and " +
        "claim_investigation_request and submit_investigation_report are not, because one bearer token cannot tell an agent " +
        "from a person. To serve the investigation tools to an agent, set mcp.allowWrites to false.";

    internal const string McpOffMessage =
        "mcp.allowInvestigations is on, but the MCP server is off, so no agent can connect. Turn MCP on " +
        "(run --enable-mcp, or set mcp.enabled to true before the first start), or set mcp.allowInvestigations to false.";

    internal const string NoTokenMessage =
        "mcp.allowInvestigations is on, but no bearer token is set, so the investigation tools stay off. " +
        "Set mcp.network.encryptedToken (made with --encrypt-password) or mcp.network.token.";

    internal const string NoAgentRouteMessage =
        "mcp.allowInvestigations is on, but no enabled notification route has an agent level above Off, so no alert " +
        "opens a request. In the Viewer's route editor, set Agent investigation to Investigate (or higher) on a route.";

    /// <summary>
    /// The findings for these settings.
    /// </summary>
    /// <param name="mcp">The <c>mcp</c> section.</param>
    /// <param name="mcpEnabled">Whether the MCP server runs: the store's switch once it exists, else <c>mcp.enabled</c>.</param>
    /// <param name="bearerToken">The resolved MCP bearer token; empty or null when none is set.</param>
    /// <param name="tokenUnreadable">Why a configured token could not be read, or null.</param>
    /// <param name="agentRouteCount">How many enabled routes have an agent level of 1 or more; null when the store could not be read.</param>
    internal static IReadOnlyList<AgentDispatchFinding> Evaluate(
        McpConfig mcp, bool mcpEnabled, string? bearerToken, string? tokenUnreadable, int? agentRouteCount)
    {
        var findings = new List<AgentDispatchFinding>();
        if (!mcp.AllowInvestigations)
        {
            return findings;
        }

        if (mcp.AllowWrites)
        {
            findings.Add(new(AgentDispatchSeverity.Problem, WritesWinMessage));
        }

        if (!mcpEnabled)
        {
            findings.Add(new(AgentDispatchSeverity.Problem, McpOffMessage));
        }

        if (!mcp.AllowWrites)
        {
            if (tokenUnreadable is not null)
            {
                findings.Add(new(
                    AgentDispatchSeverity.Problem,
                    "mcp.allowInvestigations is on, but the MCP bearer token could not be read (" + tokenUnreadable +
                    "), so the investigation tools stay off. Fix mcp.network.encryptedToken or mcp.network.token."));
            }
            else if (string.IsNullOrWhiteSpace(bearerToken))
            {
                findings.Add(new(AgentDispatchSeverity.Problem, NoTokenMessage));
            }
            else if (DarlingHostBinding.DescribeTokenProblem(bearerToken) is { } tokenProblem)
            {
                findings.Add(new(
                    AgentDispatchSeverity.Problem,
                    "mcp.allowInvestigations is on, but the MCP bearer token " + tokenProblem +
                    ", so the investigation tools stay off. Set a longer mcp.network.encryptedToken or mcp.network.token."));
            }

            if (agentRouteCount == 0)
            {
                findings.Add(new(AgentDispatchSeverity.Hint, NoAgentRouteMessage));
            }
        }

        return findings;
    }

    /// <summary>Resolves the configured MCP token the way the MCP host does, without logging it. An empty token and a null
    /// reason mean none is set.</summary>
    internal static (string Token, string? Unreadable) ResolveToken(McpConfig mcp)
    {
        var network = mcp.Network;
        if (network is null
            || (string.IsNullOrWhiteSpace(network.EncryptedToken) && string.IsNullOrWhiteSpace(network.Token)))
        {
            return ("", null);
        }

        try
        {
            var token = network.ResolveToken(out _);
            return string.IsNullOrWhiteSpace(token)
                ? ("", "it resolved to empty")
                : (token, null);
        }
        catch (Exception ex)
        {
            return ("", ex.Message);
        }
    }

    /// <summary>Counts the enabled notification routes whose agent level is 1 or more.</summary>
    internal const string CountAgentRoutesSql =
        "SELECT count(*) FROM config.config_notification_routes WHERE enabled AND agent_level >= 1";

    /// <summary>The count, or null when the store cannot answer. Never throws except on cancellation.</summary>
    internal static async Task<int?> TryCountAgentRoutesAsync(NpgsqlDataSource dataSource, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = dataSource.CreateCommand(CountAgentRoutesSql);
            command.CommandTimeout = 15;
            var value = await command.ExecuteScalarAsync(cancellationToken);
            return value is null or DBNull ? null : Convert.ToInt32(value, System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            return null;
        }
    }
}
