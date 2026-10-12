using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The switch in front of every MCP tool that CHANGES what Lite stores. MCP is off by default, but once it is on,
/// every tool the server registers is callable by any client that reaches the port, and the loopback endpoint
/// needs no token. A client that approves tool calls automatically could then mute analysis findings without the
/// operator ever being asked. So the write tools are behind one setting in settings.json,
/// <see cref="WritesSetting"/> (<see cref="McpSettings.AllowWrites"/>), default OFF. It is the twin of Darling's
/// <c>mcp.allowWrites</c> (<c>DarlingMcpWritePolicy</c>).
///
/// <para>A write that is not allowed is ABSENT from the served tool collection, not merely hidden from
/// <c>tools/list</c>: the SDK's <c>tools/call</c> dispatch consults the same collection, so a call to it fails as
/// an unknown tool. The instructions name the setting so an agent that is refused knows what to ask the operator
/// for.</para>
///
/// <para>Every tool also carries the MCP annotation hints (<see cref="ApplyAnnotations"/>): reads are
/// <c>readOnlyHint</c>, writes are <c>destructiveHint</c>. Under the MCP spec a tool with NO hint counts as
/// destructive, so the hints only tell a client which tools are safe to call without asking; they protect nothing
/// by themselves, which is why they come with the setting and not instead of it.</para>
///
/// <para>Unlike Darling, Lite adds no bearer token: its endpoint is loopback-only behind the shared Host-header
/// guard, and it has no token to resolve. The setting is the whole switch.</para>
/// </summary>
internal static class McpWritePolicy
{
    /// <summary>The settings.json key that serves the write tools.</summary>
    internal const string WritesSetting = "mcp_allow_writes";

    /// <summary>
    /// Every registered tool that writes to what Lite stores: the analysis mute and its undo. The other 88 tools
    /// only read. <c>analyze_server</c> persists the findings it derives but changes no setting of the
    /// operator's, and it is a read here, as it is on Darling. <c>McpWritePolicyTests</c> pins this set against
    /// the registered tool names by verb, so a new <c>create_</c>/<c>update_</c>/<c>delete_</c>/<c>set_</c>/
    /// <c>add_</c>/<c>remove_</c>/<c>mute_</c>/<c>unmute_</c> tool that is not listed here fails the tests
    /// instead of shipping unswitched.
    /// </summary>
    internal static readonly IReadOnlySet<string> WriteTools = new HashSet<string>(StringComparer.Ordinal)
    {
        "mute_analysis_finding",
        "unmute_analysis_finding",
    };

    /// <summary>The verb prefixes that mark a tool name as a write. The census test classifies every registered
    /// tool by these, so the set above cannot silently fall behind the registrations.</summary>
    internal static readonly string[] WriteVerbPrefixes =
    [
        "add_", "remove_", "create_", "update_", "delete_", "set_", "mute_", "unmute_",
    ];

    internal static bool IsWrite(string toolName) => WriteTools.Contains(toolName);

    /// <summary>
    /// Removes every write tool from <paramref name="tools"/> IN PLACE unless <paramref name="allowWrites"/> is
    /// true (the host runs this once, when the SDK's options are built, so every route serves the narrowed set),
    /// and stamps the annotation hints on the rest. Null-safe for a host with no tools registered.
    /// </summary>
    internal static void Apply(McpServerPrimitiveCollection<McpServerTool>? tools, bool allowWrites)
    {
        if (tools is null)
        {
            return;
        }

        if (!allowWrites)
        {
            var refused = tools.Where(tool => IsWrite(tool.ProtocolTool.Name)).ToList();
            foreach (var tool in refused)
            {
                tools.Remove(tool);
            }
        }

        ApplyAnnotations(tools);
    }

    /// <summary>A fresh collection holding only the tools that are served under <paramref name="allowWrites"/>;
    /// the input is untouched. The same shape as Darling's <c>FilterToolCollection</c>.</summary>
    internal static McpServerPrimitiveCollection<McpServerTool> FilterToolCollection(
        McpServerPrimitiveCollection<McpServerTool>? allTools, bool allowWrites)
    {
        var served = new McpServerPrimitiveCollection<McpServerTool>();

        if (allTools is not null)
        {
            foreach (var tool in allTools)
            {
                if (allowWrites || !IsWrite(tool.ProtocolTool.Name))
                {
                    served.Add(tool);
                }
            }
        }

        return served;
    }

    /// <summary>
    /// Stamps the MCP annotation hints centrally, from the tool NAME, so a read tool needs no attribute change to
    /// carry <c>readOnlyHint</c>: every tool outside <see cref="WriteTools"/> is read-only and not destructive;
    /// every write is destructive and not read-only. A title an attribute already set is kept.
    /// </summary>
    internal static void ApplyAnnotations(IEnumerable<McpServerTool>? tools)
    {
        if (tools is null)
        {
            return;
        }

        foreach (var tool in tools)
        {
            var protocol = tool.ProtocolTool;
            var annotations = protocol.Annotations ?? new ToolAnnotations();
            var write = IsWrite(protocol.Name);
            annotations.ReadOnlyHint = !write;
            annotations.DestructiveHint = write;
            protocol.Annotations = annotations;
        }
    }

    /// <summary>
    /// The registration the host calls: once the SDK has gathered every registered tool into
    /// <c>McpServerOptions.ToolCollection</c>, drop the writes the setting does not offer and stamp the hints.
    /// <c>PostConfigure</c> runs after the SDK's own options setup, so it sees the full set, and the narrowed
    /// collection is the one every request is served from. Default (setting absent): every write tool is absent.
    /// </summary>
    internal static IServiceCollection AddMcpWritePolicy(this IServiceCollection services, bool allowWrites) =>
        services.PostConfigure<McpServerOptions>(options => Apply(options.ToolCollection, allowWrites));
}
