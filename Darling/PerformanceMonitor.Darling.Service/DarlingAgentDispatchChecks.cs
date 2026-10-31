/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The start-up and <c>--validate-config</c> checks for agent dispatch (#6263, part of #5632): setups where
/// <c>mcp.allowInvestigations</c> is on but no agent can work. One pure decision, so the service log and the CLI verb
/// say the same thing. A <see cref="AgentDispatchSeverity.Warning"/> is a setup that cannot serve an agent as written. A
/// <see cref="AgentDispatchSeverity.Note"/> is a setup that works but will open no request yet. Each message says
/// what to change. Nothing here changes how the service starts or what <c>--validate-config</c> exits with: every one of
/// these configs ran before this check existed (writes win and dispatch is simply not served), so an upgrade must not
/// start failing on them. The tools stay off or served exactly as <see cref="DarlingMcpWritePolicy"/> decides.
/// </summary>
internal static class DarlingAgentDispatchChecks
{
    internal enum AgentDispatchSeverity
    {
        /// <summary>The setup cannot serve an agent as written. Printed as a warning; it never changes an exit code.</summary>
        Warning,

        /// <summary>The setup works, but nothing will happen until the operator adds the missing piece.</summary>
        Note,
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

        /* #6542: named tokens and no legacy token. mcp.allowWrites and mcp.allowInvestigations then govern nothing, each token's role
           decides, so the legacy-token findings below do not apply. */
        var namedOnly = mcp.HasNamedTokens && string.IsNullOrWhiteSpace(bearerToken) && tokenUnreadable is null;

        if (mcp.AllowWrites && !namedOnly)
        {
            findings.Add(new(AgentDispatchSeverity.Warning, WritesWinMessage));
        }

        if (!mcpEnabled)
        {
            findings.Add(new(AgentDispatchSeverity.Warning, McpOffMessage));
        }

        if (!mcp.AllowWrites && !namedOnly)
        {
            if (tokenUnreadable is not null)
            {
                findings.Add(new(
                    AgentDispatchSeverity.Warning,
                    "mcp.allowInvestigations is on, but the MCP bearer token could not be read (" + tokenUnreadable +
                    "), so the investigation tools stay off. Fix mcp.network.encryptedToken or mcp.network.token."));
            }
            else if (string.IsNullOrWhiteSpace(bearerToken))
            {
                findings.Add(new(AgentDispatchSeverity.Warning, NoTokenMessage));
            }
            else if (DarlingHostBinding.DescribeTokenProblem(bearerToken) is { } tokenProblem)
            {
                findings.Add(new(
                    AgentDispatchSeverity.Warning,
                    "mcp.allowInvestigations is on, but the MCP bearer token " + tokenProblem +
                    ", so the investigation tools stay off. Set a longer mcp.network.encryptedToken or mcp.network.token."));
            }

            if (agentRouteCount == 0)
            {
                findings.Add(new(AgentDispatchSeverity.Note, NoAgentRouteMessage));
            }
        }

        return findings;
    }

    /// <summary>The sentence for named tokens when no agent token can be served while an enabled route has an agent level (#6542).</summary>
    internal const string NoAgentTokenMessage =
        "mcp.tokens has no usable token with the agent role, but an enabled notification route has an agent level above Off, so a " +
        "request opens and no agent can claim it. Add a token with role agent to mcp.tokens.";

    /// <summary>
    /// The <c>mcp.tokens</c> findings for <c>--validate-config</c> (#6542), one sentence each, never a token value: the problems
    /// <see cref="DarlingMcpTokenSet.Check"/> finds, then <see cref="NoAgentTokenMessage"/> when no agent can connect. Empty when
    /// <c>mcp.tokens</c> is not set. <paramref name="agentRouteCount"/> is null when the store could not be read.
    /// </summary>
    internal static IReadOnlyList<string> NamedTokenFindings(McpConfig mcp, string legacyToken, int? agentRouteCount)
    {
        if (!mcp.HasNamedTokens)
        {
            return Array.Empty<string>();
        }

        var findings = DarlingMcpTokenSet.Check(mcp, legacyToken, DarlingMcpTokenSet.ResolveEntry, out var usable);
        var legacyAgent = legacyToken.Length > 0 && mcp.AllowInvestigations && !mcp.AllowWrites;
        var agentToken = legacyAgent || usable.Exists(t => string.Equals(t.Role, McpTokenEntry.AgentRole, StringComparison.Ordinal));
        if (!agentToken && agentRouteCount > 0)
        {
            findings.Add(NoAgentTokenMessage);
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

    /// <summary>The sentence for a web token that has the same value as the MCP token (#6265): the web token is refused for sign-in,
    /// because an MCP agent holds the MCP token and the web token signs in as a person.</summary>
    internal const string WebTokenSameAsMcpMessage =
        "The web token has the same value as the MCP token, so it is refused for web sign-in: an agent that holds the MCP token must not be " +
        "able to sign in to the web as a person. Set a different web token (web.network.encryptedToken or web.network.token).";

    /// <summary>True when the web token and the MCP token are both set and the same value.</summary>
    internal static bool WebTokenIsMcpToken(string? webToken, string? mcpToken) =>
        DarlingHostBinding.FixedTimeTokenEquals(webToken, mcpToken);

    /// <summary>True when the web token has the value of any of <paramref name="mcpTokens"/> (#7023: the legacy token and every named token
    /// that is served). Every value is compared, so the time taken does not say which one matched.</summary>
    internal static bool WebTokenIsMcpToken(string? webToken, IReadOnlyList<string>? mcpTokens)
    {
        var same = false;
        foreach (var mcpToken in mcpTokens ?? Array.Empty<string>())
        {
            same |= WebTokenIsMcpToken(webToken, mcpToken);
        }

        return same;
    }

    /// <summary>Every MCP token value a client can present (#7023): the legacy token when one is set, then each named <c>mcp.tokens</c> entry
    /// the host serves (the ones that pass <see cref="DarlingMcpTokenSet.Check"/>), whatever its role. An agent or a reader that
    /// holds the web token's value could otherwise sign in to the web as a person, as the legacy token could (#6265). Values are never logged.</summary>
    internal static List<string> ResolveAllTokens(McpConfig mcp)
    {
        var values = new List<string>();
        var legacy = ResolveToken(mcp).Token;
        if (legacy.Length > 0)
        {
            values.Add(legacy);
        }

        DarlingMcpTokenSet.Check(mcp, legacy, DarlingMcpTokenSet.ResolveEntry, out var usable);
        values.AddRange(usable.Select(entry => entry.Token));
        return values;
    }

    /// <summary>The configured web token, resolved the way the web host does, without logging it. Empty when none is set or it cannot be
    /// read (the web host reports that itself).</summary>
    internal static string ResolveWebToken(WebConfig web)
    {
        var network = web.Network;
        if (network is null
            || (string.IsNullOrWhiteSpace(network.EncryptedToken) && string.IsNullOrWhiteSpace(network.Token)))
        {
            return "";
        }

        try
        {
            return network.ResolveToken(out _) ?? "";
        }
        catch (Exception)
        {
            return "";
        }
    }

    /// <summary>The warning for <c>--validate-config</c> and the service log when the configured web token equals the MCP token, else
    /// null. A warning and not an error, so a setup that started before still starts.</summary>
    internal static string? WebTokenFinding(DarlingConfig config) =>
        WebTokenIsMcpToken(ResolveWebToken(config.Web), ResolveAllTokens(config.Mcp)) ? WebTokenSameAsMcpMessage : null;

    /// <summary>Counts the enabled notification routes whose agent level is 1 or more.</summary>
    internal const string CountAgentRoutesSql =
        "SELECT count(*) FROM config.config_notification_routes WHERE enabled AND agent_level >= 1";

    /// <summary>The store's MCP switch and the agent route count for <c>--validate-config</c>, each null when the store cannot
    /// answer. Never throws except on cancellation.</summary>
    internal static async Task<(bool? McpEnabled, int? AgentRoutes)> TryReadStoreFactsAsync(
        string connectionString, CancellationToken cancellationToken)
    {
        try
        {
            using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            budget.CancelAfter(TimeSpan.FromSeconds(ServiceCommandDeadlines.CliStoreReadSeconds));
            await using var source = NpgsqlDataSource.Create(DarlingStoreConnection.PinSessionTimeZoneUtc(connectionString));
            bool? enabled = null;
            await using (var toggle = source.CreateCommand("SELECT mcp_enabled FROM config.config_service WHERE id = 1"))
            {
                toggle.CommandTimeout = ServiceCommandDeadlines.CliBudgetBackstopSeconds;
                if (await toggle.ExecuteScalarAsync(budget.Token) is bool stored)
                {
                    enabled = stored;
                }
            }

            return (enabled, await TryCountAgentRoutesAsync(source, budget.Token));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception)
        {
            return (null, null);
        }
    }

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
