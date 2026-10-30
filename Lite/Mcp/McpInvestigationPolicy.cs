using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The serving gate for the four investigation tools (#5820): the tools an agent uses to list, claim and report on
/// investigation requests. Served only when <c>mcp_allow_investigations</c> is on AND <c>mcp_allow_writes</c> is off
/// (<see cref="Served(McpSettings)"/>) AND a usable MCP token is in force (not <c>mcp_allow_tokenless_loopback</c>; the host
/// decides this in <see cref="McpHostService.DecideServing"/>). A client holding BOTH the mute tools and the agent tools is
/// ruled out by the two never running together. Same rule as Darling's <c>mcp.allowInvestigations</c> (<c>DarlingMcpWritePolicy</c>).
///
/// <para>Gated by tool NAME, like <see cref="McpWritePolicy"/>: a tool that is not served is ABSENT from the served
/// collection, so <c>tools/list</c> omits it and <c>tools/call</c> fails as an unknown tool, the same refusal a write
/// tool gets. <c>wait_for_investigation_requests</c> is not in the set: it only waits and reads, and is never gated.</para>
/// </summary>
internal static class McpInvestigationPolicy
{
    /// <summary>The settings.json key that serves the investigation tools.</summary>
    internal const string InvestigationsSetting = "mcp_allow_investigations";

    /// <summary>The four tools the setting serves.</summary>
    internal static readonly IReadOnlySet<string> InvestigationTools = new HashSet<string>(StringComparer.Ordinal)
    {
        "list_investigation_requests",
        "claim_investigation_request",
        "submit_investigation_report",
        "get_investigation_reports",
    };

    /// <summary>Of those, the two that change investigation state (not the monitored servers, not the operator's
    /// settings). They carry neither the read-only nor the destructive hint.</summary>
    internal static readonly IReadOnlySet<string> StateChangingTools = new HashSet<string>(StringComparer.Ordinal)
    {
        "claim_investigation_request",
        "submit_investigation_report",
    };

    internal static bool IsInvestigationTool(string toolName) => InvestigationTools.Contains(toolName);

    internal static bool IsStateChanging(string toolName) => StateChangingTools.Contains(toolName);

    /// <summary>Whether the investigation tools are served: the setting on and the write tools off.</summary>
    internal static bool Served(McpSettings settings) => Served(settings.AllowInvestigations, settings.AllowWrites);

    /// <summary>The rule on the two flags, for the host, which holds them separately.</summary>
    internal static bool Served(bool allowInvestigations, bool allowWrites) => allowInvestigations && !allowWrites;

    /// <summary>
    /// The start's log line for the gate, or null when there is nothing new to say. Served: one Information line naming the
    /// tools and saying any local process can claim. Both settings on: ONE Critical line, the tools stay OFF. Setting off: null.
    /// One decision for the start and the tests.
    /// </summary>
    internal static (Microsoft.Extensions.Logging.LogLevel Level, string Message)? StartupLine(bool allowInvestigations, bool allowWrites, bool tokenUsable)
    {
        if (!allowInvestigations)
        {
            return null;
        }

        if (allowWrites)
        {
            return (Microsoft.Extensions.Logging.LogLevel.Critical,
                $"The MCP investigation tools are OFF: {InvestigationsSetting} needs {McpWritePolicy.WritesSetting} off, and both are on. "
                + $"Turn {McpWritePolicy.WritesSetting} off to serve them.");
        }

        if (!tokenUsable)
        {
            return (Microsoft.Extensions.Logging.LogLevel.Critical,
                $"The MCP investigation tools are OFF: {InvestigationsSetting} needs {McpTokenStore.TokenRequirement}, "
                + $"and {McpSettings.AllowTokenlessLoopbackSetting} must be off. Not served: {string.Join(", ", InvestigationTools)}.");
        }

        return (Microsoft.Extensions.Logging.LogLevel.Information,
            $"MCP investigation tools are ON ({InvestigationsSetting}): {string.Join(", ", InvestigationTools)} are served. "
            + "Every client needs the MCP token; any client that has it can list, claim and report on investigation requests.");
    }

    /// <summary>Removes the four tools from <paramref name="tools"/> IN PLACE unless <paramref name="served"/>.</summary>
    internal static void Apply(McpServerPrimitiveCollection<McpServerTool>? tools, bool served)
    {
        if (tools is null || served)
        {
            return;
        }

        foreach (var tool in tools.Where(t => IsInvestigationTool(t.ProtocolTool.Name)).ToList())
        {
            tools.Remove(tool);
        }
    }

    /// <summary>
    /// The registration the host calls. A <c>PostConfigure</c> registered BEFORE <see cref="McpWritePolicy.AddMcpWritePolicy"/>,
    /// so the server instructions' catalog (built there) does not count tools this configuration never offers.
    /// </summary>
    internal static IServiceCollection AddMcpInvestigationPolicy(this IServiceCollection services, bool served) =>
        services.PostConfigure<McpServerOptions>(options => Apply(options.ToolCollection, served));
}
