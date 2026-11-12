using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The serving gate for the investigation tools that change investigation state (#5820, #8461): the tools an agent uses to claim
/// and report on investigation requests. Served only when <c>mcp_allow_investigations</c> is on AND <c>mcp_allow_writes</c> is off
/// (<see cref="Served(McpSettings)"/>) AND a usable MCP token is in force (not <c>mcp_allow_tokenless_loopback</c>; the host
/// decides this in <see cref="McpHostService.DecideServing"/>). A client holding BOTH the mute tools and the agent tools is
/// ruled out by the two never running together. Same rule as Darling's <c>mcp.allowInvestigations</c> (<c>DarlingMcpWritePolicy</c>).
///
/// <para>The gated set is built from the shared kind list (<see cref="InvestigationToolNames.StateChanging"/>), so a shared name is
/// gated by the same rule in both apps. The investigation reads (<see cref="InvestigationToolNames.Reads"/>:
/// <c>list_investigation_requests</c> and <c>get_investigation_reports</c>, the two reads Lite registers; the shared list also holds
/// <c>wait_for_investigation_requests</c>, a Darling-only tool that Lite does not register) are ordinary reads, served in every
/// configuration as Darling serves them, and are never gated here.</para>
///
/// <para>Gated by tool NAME, like <see cref="McpWritePolicy"/>: a tool that is not served is ABSENT from the served
/// collection, so <c>tools/list</c> omits it and <c>tools/call</c> fails as an unknown tool, the same refusal a write
/// tool gets.</para>
/// </summary>
internal static class McpInvestigationPolicy
{
    /// <summary>The settings.json key that serves the state-changing investigation tools.</summary>
    internal const string InvestigationsSetting = "mcp_allow_investigations";

    /// <summary>The names <see cref="McpInvestigationTools"/> declares, read once from its tool attributes.</summary>
    private static readonly IReadOnlySet<string> DeclaredTools = new HashSet<string>(
        typeof(McpInvestigationTools).GetMethods()
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
            .OfType<string>(),
        StringComparer.Ordinal);

    /// <summary>
    /// The tools the setting serves: the state-changing investigation tools of the shared kind list that Lite has. Lite has no chat
    /// thread, so <c>post_investigation_message</c> is Darling's alone and is neither gated nor named here; the day Lite gains a tool
    /// placed on that list, it is gated without an edit here.
    /// </summary>
    internal static readonly IReadOnlySet<string> InvestigationTools = new HashSet<string>(
        InvestigationToolNames.StateChanging.Where(DeclaredTools.Contains), StringComparer.Ordinal);

    /// <summary>The same set under the name the annotation code reads it by: these tools change investigation state (not the
    /// monitored servers, not the operator's settings) and carry neither the read-only nor the destructive hint.</summary>
    internal static IReadOnlySet<string> StateChangingTools => InvestigationTools;

    /// <summary>The investigation reads Lite has: the shared <see cref="InvestigationToolNames.Reads"/> that <see cref="McpInvestigationTools"/>
    /// declares. Served in every configuration; the instructions name them from here.</summary>
    internal static readonly IReadOnlySet<string> ReadTools = new HashSet<string>(
        InvestigationToolNames.Reads.Where(DeclaredTools.Contains), StringComparer.Ordinal);

    /// <summary>True for a tool this gate serves: a state-changing investigation tool. The reads are not investigation tools in this sense.</summary>
    internal static bool IsInvestigationTool(string toolName) => InvestigationTools.Contains(toolName);

    internal static bool IsStateChanging(string toolName) => StateChangingTools.Contains(toolName);

    /// <summary>Whether the state-changing investigation tools are served: the setting on and the write tools off.</summary>
    internal static bool Served(McpSettings settings) => Served(settings.AllowInvestigations, settings.AllowWrites);

    /// <summary>The rule on the two flags, for the host, which holds them separately.</summary>
    internal static bool Served(bool allowInvestigations, bool allowWrites) => allowInvestigations && !allowWrites;

    /// <summary>
    /// The start's log line for the gate, or null when there is nothing new to say. Served: one Information line naming the
    /// tools and saying any client with the token can claim. Both settings on: ONE Critical line, the tools stay OFF. Setting off: null.
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
            $"MCP investigation tools are ON ({InvestigationsSetting}): {string.Join(", ", InvestigationTools)} are served; the investigation reads are served in every configuration. "
            + "Every client needs the MCP token; any client that has it can claim and report on investigation requests.");
    }

    /// <summary>Removes the state-changing investigation tools from <paramref name="tools"/> IN PLACE unless <paramref name="served"/>. The reads stay.</summary>
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
