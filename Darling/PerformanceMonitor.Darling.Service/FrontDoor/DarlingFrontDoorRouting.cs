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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;

namespace PerformanceMonitor.Darling.Service.FrontDoor;

/// <summary>How the front door sends a tool that names no server and no store (#6840).</summary>
internal enum FrontDoorToolClass
{
    /// <summary>The tool changes or reads one store's own state, so a call must name the store (or a server that is on one store).
    /// A write never fans out.</summary>
    StoreRequired,

    /// <summary>The tool computes from its arguments alone, so any one store answers it the same.</summary>
    AnyOneStore,

    /// <summary>Every other tool: asked of every store, the answers kept apart.</summary>
    FanOut,
}

/// <summary>
/// Which of its three classes a tool is in, by name (#6840). The lists are names and not a reading of the tool's source, so a tool
/// added later is in no class until someone puts it in one, and the census test fails on it.
/// </summary>
internal static class DarlingFrontDoorRouting
{
    /// <summary>The argument that names a store.</summary>
    internal const string StoreArgument = "store";

    /// <summary>The argument that names a server.</summary>
    internal const string ServerArgument = "server_name";

    /// <summary>The name that asks for every server, so for every store.</summary>
    internal const string AllServers = "*";

    /// <summary>The tools a store-required answer is owed for beyond the write policy's three sets: they read or run one store's own
    /// saved state (a Custom View, a custom alert rule, the investigation queue), so which store holds it is not a guess.</summary>
    internal static readonly IReadOnlySet<string> ExtraStoreRequiredTools = new HashSet<string>(StringComparer.Ordinal)
    {
        "run_custom_view_panel",
        "test_custom_alert_rule",
        "backtest_custom_alert_rule",
        "wait_for_investigation_requests",
    };

    /// <summary>The tools that compute from their arguments alone (a plan, a definition, a guide), so the first healthy store answers.</summary>
    internal static readonly IReadOnlySet<string> AnyOneStoreTools = new HashSet<string>(StringComparer.Ordinal)
    {
        "get_tool_guide",
        "analyze_plan_xml",
        "validate_custom_view",
        "validate_custom_alert_rule",
        "describe_custom_view_catalog",
        "list_custom_alert_templates",
    };

    /// <summary>The arguments whose value is a row id inside one store. An id from one store means nothing in another, so a call that
    /// carries one is store-required.</summary>
    internal static readonly IReadOnlySet<string> StoreLocalIdArguments = new HashSet<string>(StringComparer.Ordinal)
    {
        "rule_id", "view_id", "route_id", "fallback_route_id", "tag_id", "parent_id", "note_id", "request_id", "claim_id",
        "command_id", "journal_id", "sweep_id", "called_id", "query_id", "route_ids", "server_ids",
    };

    /// <summary>The tools that name no server and are asked of every store when no store is named (#6840). Together with the other two
    /// sets and the tools that take <c>server_name</c>, they cover every registered tool: a census test fails on one that is in none.</summary>
    internal static readonly IReadOnlySet<string> FanOutTools = new HashSet<string>(StringComparer.Ordinal)
    {
        "find_database",
        "get_agent_changes",
        "get_collection_state",
        "get_collector_cost",
        "get_command_result",
        "get_compiles_after_evict",
        "get_custom_alert_rule",
        "get_custom_view",
        "get_finops_inventory",
        "get_fleet_overview",
        "get_investigation_evidence",
        "get_mute_rules",
        "get_notification_routes",
        "get_read_latency",
        "get_server_actions",
        "get_store_host",
        "get_store_log",
        "get_store_metrics",
        "get_store_query_history",
        "get_store_query_stats",
        "get_sweep_reports",
        "list_custom_alert_rules",
        "list_custom_views",
        "list_query_tag_rules",
        "list_query_tags",
        "list_servers",
        "test_query_tag_rule",
    };

    /// <summary>True when <paramref name="tool"/> is in the store-required class: any write, investigation or server admin tool, or one
    /// of <see cref="ExtraStoreRequiredTools"/>.</summary>
    internal static bool IsStoreRequired(string tool) =>
        DarlingMcpWritePolicy.IsWrite(tool)
        || DarlingMcpWritePolicy.IsInvestigation(tool)
        || DarlingMcpWritePolicy.IsServerAdmin(tool)
        || ExtraStoreRequiredTools.Contains(tool);

    /// <summary>The class of a tool that names no server. A name in none of the lists is a fan-out read at run time (every write is
    /// in the write policy's sets, which the existing census already holds every tool to), and the census test fails it at build time.</summary>
    internal static FrontDoorToolClass ClassOf(string tool) =>
        IsStoreRequired(tool) ? FrontDoorToolClass.StoreRequired
        : AnyOneStoreTools.Contains(tool) ? FrontDoorToolClass.AnyOneStore
        : FrontDoorToolClass.FanOut;

    /// <summary>The classes <paramref name="tool"/> is listed in, for the census: one name in two lists is a defect.</summary>
    internal static int ClassesListedIn(string tool) =>
        (IsStoreRequired(tool) ? 1 : 0) + (AnyOneStoreTools.Contains(tool) ? 1 : 0) + (FanOutTools.Contains(tool) ? 1 : 0);

    /// <summary>True when the call carries a store-local id with a value.</summary>
    internal static bool CarriesStoreLocalId(IDictionary<string, JsonElement>? arguments)
    {
        if (arguments is null)
        {
            return false;
        }

        foreach (var name in StoreLocalIdArguments)
        {
            if (arguments.TryGetValue(name, out var value) && value.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The string a call passes for <paramref name="name"/>, trimmed, or null when it passes none or an empty one.</summary>
    internal static string? StringArgument(IDictionary<string, JsonElement>? arguments, string name)
    {
        if (arguments is null || !arguments.TryGetValue(name, out var value))
        {
            return null;
        }

        var text = value.ValueKind == JsonValueKind.String ? value.GetString() : value.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined ? null : value.ToString();
        return string.IsNullOrWhiteSpace(text) ? null : text.Trim();
    }

    /// <summary>Where one call goes. Built by <see cref="PlanAsync"/>.</summary>
    internal abstract record Plan
    {
        private Plan() { }

        /// <summary>One store, named.</summary>
        internal sealed record ToStore(string Store) : Plan;

        /// <summary>The first healthy store.</summary>
        internal sealed record AnyOne : Plan;

        /// <summary>Every store that is not held.</summary>
        internal sealed record FanOut : Plan;

        /// <summary>No store: the call is answered here with this refusal text (the shared refusal envelope).</summary>
        internal sealed record Refuse(string Json) : Plan;
    }

    /// <summary>
    /// Decides where a call goes, in this order: an explicit <c>store</c> wins (a <c>server_name</c> that is on another store is
    /// refused); a <c>server_name</c> other than <c>*</c> goes through the directory, which refreshes once when it finds no match; and
    /// otherwise the tool's class decides. A write is never planned as <see cref="Plan.FanOut"/>: a call that carries a store-local id
    /// or is in the store-required class and names no store is refused.
    /// </summary>
    internal static async Task<Plan> PlanAsync(IFrontDoorDirectory directory, string tool, IDictionary<string, JsonElement>? arguments, CancellationToken ct)
    {
        var serverName = StringArgument(arguments, ServerArgument);
        var named = serverName is not null && !string.Equals(serverName, AllServers, StringComparison.Ordinal);

        /* 1. An explicit store wins. */
        if (arguments is not null && arguments.TryGetValue(StoreArgument, out var storeValue)
            && storeValue.ValueKind is not (JsonValueKind.Null or JsonValueKind.Undefined))
        {
            var requested = StringArgument(arguments, StoreArgument);
            var store = requested is null ? null : directory.Find(requested);
            if (store is null)
            {
                return new Plan.Refuse(McpHelpers.Refusal(
                    StoreArgument,
                    requested is null
                        ? $"store is empty. Name one of the configured stores: {StoreList(directory)}."
                        : $"'{requested}' is not a configured store. The configured stores are: {StoreList(directory)}."));
            }

            if (named && directory.ResolveServer(serverName!) is FrontDoorRoute.ToStore elsewhere
                && !string.Equals(elsewhere.Store, store.Name, StringComparison.OrdinalIgnoreCase))
            {
                return new Plan.Refuse(McpHelpers.Refusal(
                    ServerArgument,
                    $"'{serverName}' is on store '{elsewhere.Store}', not on store '{store.Name}'. Drop store, or name the store the server is on."));
            }

            return new Plan.ToStore(store.Name);
        }

        /* 2. A server name goes through the directory. A miss refreshes the directory once (it limits itself to one refresh per
           30 seconds) and looks again, which covers a server added since the last refresh. */
        if (named)
        {
            var route = directory.ResolveServer(serverName!);
            /* An ambiguous name is not a miss: it matched in more than one store, and a refresh cannot settle that. */
            if (route is FrontDoorRoute.Refused { Ambiguous: false })
            {
                await directory.RefreshOnMissAsync(ct).ConfigureAwait(false);
                route = directory.ResolveServer(serverName!);
            }

            if (route is FrontDoorRoute.ToStore found)
            {
                return new Plan.ToStore(found.Store);
            }

            if (route is FrontDoorRoute.Refused refused)
            {
                return new Plan.Refuse(McpHelpers.Refusal(ServerArgument, refused.Message));
            }
        }

        /* 3. The tool's class decides. */
        var toolClass = ClassOf(tool);
        if (toolClass == FrontDoorToolClass.StoreRequired || CarriesStoreLocalId(arguments))
        {
            return new Plan.Refuse(McpHelpers.Refusal(
                StoreArgument,
                $"'{tool}' works on one store's own records, so the front door needs the store: pass store (one of: {StoreList(directory)}), "
                + "or a server_name that is on exactly one store."));
        }

        return toolClass == FrontDoorToolClass.AnyOneStore ? new Plan.AnyOne() : new Plan.FanOut();
    }

    private static string StoreList(IFrontDoorDirectory directory) =>
        string.Join(", ", directory.Stores.Select(s => s.Store.Name));
}

/// <summary>
/// The front door's router (#6840): decides where a tool call goes (<see cref="DarlingFrontDoorRouting"/>), makes the calls to the
/// stores through <see cref="IFrontDoorMcpPeers"/>, and builds the answer. A single-store answer is the store's own result, unchanged.
/// </summary>
internal sealed class DarlingFrontDoorRouter
{
    private readonly IFrontDoorDirectory _directory;
    private readonly IFrontDoorMcpPeers _peers;
    private readonly TimeSpan _peerTimeout;
    private readonly ILogger? _logger;

    internal DarlingFrontDoorRouter(IFrontDoorDirectory directory, IFrontDoorMcpPeers peers, TimeSpan peerTimeout, ILogger? logger = null)
    {
        _directory = directory;
        _peers = peers;
        _peerTimeout = peerTimeout;
        _logger = logger;
    }

    /// <summary>The configured store names, in file order.</summary>
    internal IReadOnlyList<string> StoreNames => _directory.Stores.Select(s => s.Store.Name).ToList();

    /// <summary>Routes one call and answers it. <paramref name="agentRole"/> is true when the caller holds the agent role, so the store
    /// sees the agent token and not the operator one. A caller cancel (<paramref name="ct"/>) cancels every call in flight.</summary>
    internal async Task<CallToolResult> CallAsync(string tool, IDictionary<string, JsonElement>? arguments, bool agentRole, CancellationToken ct)
    {
        var plan = await DarlingFrontDoorRouting.PlanAsync(_directory, tool, arguments, ct).ConfigureAwait(false);
        var forwarded = WithoutStore(arguments);

        switch (plan)
        {
            case DarlingFrontDoorRouting.Plan.Refuse refuse:
                return TextResult(refuse.Json, isError: false);

            case DarlingFrontDoorRouting.Plan.ToStore single:
            {
                var outcome = await CallOneAsync(single.Store, tool, forwarded, agentRole, passProtocolErrors: true, ct).ConfigureAwait(false);
                return outcome.Result ?? TextResult(Unavailable(outcome.Store, outcome.Message!), isError: true);
            }

            case DarlingFrontDoorRouting.Plan.AnyOne:
            {
                var candidates = _directory.Stores
                    .Where(s => s.Health != FrontDoorStoreHealth.AuthFailed)
                    .OrderBy(s => s.Health)
                    .Select(s => s.Store.Name)
                    .ToList();
                StoreOutcome? last = null;
                foreach (var store in candidates)
                {
                    last = await CallOneAsync(store, tool, forwarded, agentRole, passProtocolErrors: false, ct).ConfigureAwait(false);
                    if (last.Result is not null)
                    {
                        return last.Result;
                    }
                }

                return TextResult(
                    last is null
                        ? McpHelpers.Status("unavailable", "No store is reachable right now, so the front door cannot answer.")
                        : Unavailable(last.Store, last.Message!),
                    isError: true);
            }

            default:
                return await DarlingFrontDoorFanOut.RunAsync(
                    tool, _directory.Stores, store => CallOneAsync(store, tool, forwarded, agentRole, passProtocolErrors: false, ct), ct).ConfigureAwait(false);
        }
    }

    /// <summary>The arguments the store sees: the caller's, without <c>store</c>, which only the front door reads.</summary>
    private static Dictionary<string, JsonElement> WithoutStore(IDictionary<string, JsonElement>? arguments)
    {
        var copy = new Dictionary<string, JsonElement>(StringComparer.Ordinal);
        if (arguments is not null)
        {
            foreach (var pair in arguments)
            {
                if (!string.Equals(pair.Key, DarlingFrontDoorRouting.StoreArgument, StringComparison.Ordinal))
                {
                    copy[pair.Key] = pair.Value;
                }
            }
        }

        return copy;
    }

    /// <summary>One call to one store. The store's own result comes back as it is (<c>busy</c> and an error result included, with no
    /// retry), and a store that cannot be asked comes back as an unavailable message. The caller's cancel is the one thing that throws,
    /// except on the single-store path (<paramref name="passProtocolErrors"/>), where a store's JSON-RPC error passes through as the SDK's
    /// <see cref="McpProtocolException"/>. On the any-one and fan-out paths every other failure of one store is that store's own entry,
    /// so one store that cannot answer a tool (an older build, say) never costs the other stores' answers.</summary>
    private async Task<StoreOutcome> CallOneAsync(
        string store, string tool, Dictionary<string, JsonElement> arguments, bool agentRole, bool passProtocolErrors, CancellationToken ct)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        linked.CancelAfter(_peerTimeout);
        try
        {
            var call = new CallToolRequestParams { Name = tool, Arguments = new Dictionary<string, JsonElement>(arguments, StringComparer.Ordinal) };
            var result = await _peers.CallAsync(store, call, agentRole, linked.Token).ConfigureAwait(false);
            return new StoreOutcome(store, result, null);
        }
        catch (FrontDoorPeerException ex)
        {
            _logger?.LogDebug("Front door: store {Store} could not be asked for {Tool}: {Kind}", store, tool, ex.Kind);
            return new StoreOutcome(store, null, ex.Message);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return new StoreOutcome(store, null, $"Store '{store}' did not answer within {_peerTimeout.TotalSeconds:0} seconds.");
        }
        catch (McpProtocolException) when (passProtocolErrors)
        {
            throw;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            _logger?.LogDebug(ex, "Front door: store {Store} failed {Tool}: {Type}", store, tool, ex.GetType().Name);
            return new StoreOutcome(store, null, $"Store '{store}' could not answer {tool}: {ex.Message}");
        }
    }

    private static string Unavailable(string store, string message) =>
        McpHelpers.Status("unavailable", message, new { store });

    private static CallToolResult TextResult(string text, bool isError) =>
        new() { Content = new List<ContentBlock> { new TextContentBlock { Text = text } }, IsError = isError };

    /// <summary>One store's answer to one call: its result, or why it could not be asked.</summary>
    internal sealed record StoreOutcome(string Store, CallToolResult? Result, string? Message);
}
