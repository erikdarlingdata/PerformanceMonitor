using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The switch in front of every MCP tool that CHANGES what Lite stores. MCP is off by default, but once it is on,
/// every tool the server registers is callable by any client that is let in. A client that approves tool calls automatically could then mute analysis findings without the
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
/// <para>Lite's endpoint is loopback-only behind the shared Host-header guard and, like Darling's, needs the MCP token
/// unless mcp_allow_tokenless_loopback is on. The write tools need <see cref="WritesSetting"/> AND a usable token
/// (<see cref="McpTokenStore.IsUsableToken"/>: <see cref="McpTokenStore.MinimumTokenLength"/> characters or more, with
/// <see cref="McpTokenStore.MinimumDistinctTokenCharacters"/> or more different ones): with mcp_allow_tokenless_loopback on, only the read tools are served
/// and the writes stay off (<see cref="McpHostService.DecideServing"/>, which Darling's <c>DarlingMcpWritePolicy.ResolveForStart</c>
/// mirrors). The sample settings (<c>settings.sample.json</c>) and the README's MCP section say so. The setting has no UI
/// control, so it has no tooltip.</para>
/// </summary>
internal static class McpWritePolicy
{
    /// <summary>The settings.json key that serves the write tools.</summary>
    internal const string WritesSetting = "mcp_allow_writes";

    /// <summary>
    /// Every registered tool that writes to what Lite stores: the analysis mute and its undo, and the notification-route disable and delete. The other tools
    /// only read. <c>analyze_server</c> persists the findings it derives but changes no setting of the
    /// operator's, and it is a read here, as it is on Darling. <c>McpWritePolicyTests</c> lists every
    /// registered tool and fails unless each one is in this set or on that test's explicit read-only list, so a new
    /// tool fails the tests until someone classifies it, instead of shipping unswitched.
    /// </summary>
    internal static readonly IReadOnlySet<string> WriteTools = new HashSet<string>(StringComparer.Ordinal)
    {
        "mute_analysis_finding",
        "unmute_analysis_finding",

        /* #5750: the notification-route disable and delete. Classified here, the one place a write is declared, so the existing
           switch serves them only when mcp_allow_writes is true. get_notification_routes is a read. */
        "set_notification_route_enabled",
        "delete_notification_route",

        /* #5991: the query-tag and query-tag-rule writes, Darling's twins. The three reads (list_query_tags,
           list_query_tag_rules, test_query_tag_rule) are ordinary reads. */
        "create_query_tag",
        "update_query_tag",
        "delete_query_tag",
        "assign_query_tag",
        "unassign_query_tag",
        "create_query_tag_rule",
        "update_query_tag_rule",
        "delete_query_tag_rule",
    };

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
            /* #5820: claim and submit change investigation state only, so they are neither read-only nor destructive. */
            var stateOnly = !write && McpInvestigationPolicy.IsStateChanging(protocol.Name);
            annotations.ReadOnlyHint = !write && !stateOnly;
            annotations.DestructiveHint = write;
            protocol.Annotations = annotations;
        }
    }

    /// <summary>
    /// The registration the host calls: once the SDK has gathered every registered tool into
    /// <c>McpServerOptions.ToolCollection</c>, drop the writes the setting does not offer and stamp the hints.
    /// <c>PostConfigure</c> runs after the SDK's own options setup, so it sees the full set, and the narrowed
    /// collection is the one every request is served from. Default (setting absent): every write tool is absent.
    /// #5789: it also sets the server instructions, from the tool names before and after the narrowing, so their
    /// census counts the set <c>tools/list</c> serves; they used to carry a fixed catalog count.
    /// </summary>
    internal static IServiceCollection AddMcpWritePolicy(this IServiceCollection services, bool allowWrites) =>
        services.PostConfigure<McpServerOptions>(options =>
        {
            var catalog = ToolNames(options.ToolCollection);
            Apply(options.ToolCollection, allowWrites);
            options.ServerInstructions = McpInstructions.Build(catalog, ToolNames(options.ToolCollection));
        });

    /// <summary>The protocol names of <paramref name="tools"/>, in collection order; empty for null.</summary>
    internal static List<string> ToolNames(IEnumerable<McpServerTool>? tools)
    {
        var names = new List<string>();
        if (tools is null)
            return names;

        foreach (var tool in tools)
            names.Add(tool.ProtocolTool.Name);

        return names;
    }
}
