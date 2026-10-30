/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The server action switch tools (#6269): <c>set_server_action</c> turns one server's switch for one agent server action
/// (<c>force_plan</c>, <c>evict_plan</c>, <c>kill_session</c>, <c>recompile_module</c>, <c>create_plan_guide</c>, <c>drop_plan_guide</c>) on or off, and <c>get_server_actions</c> reads every server's switches.
///
/// <para><b>Served only under <c>mcp.allowServerAdmin</c></b> (with <c>mcp.allowWrites</c> and a token), like <c>edit_server</c>, and listed in
/// <see cref="DarlingMcpWritePolicy.NeverAgentTools"/>, so an agent that investigates an alert can never turn on its own server actions.
/// The write is <see cref="ServerActionSwitches.SetAsync"/> on the service's own (store owner) data source: the <c>mcp</c> role holds no write on
/// the table and gets none. The change is journaled <c>server_action_switched</c> by <c>mcp:</c> and the caller
/// (<see cref="ReadCaller.WriteIdentity"/>), with the state before and after.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpServerActionTools
{
    internal const string SetToolName = "set_server_action";

    /// <summary>The journal actor of an MCP switch change: <c>mcp:</c> plus the caller the request filter worked out. Never <c>agent:</c> and never <c>person:</c>.</summary>
    internal static string ActorOf() => "mcp:" + ReadCaller.StoreIdentity();

    [McpServerTool(Name = "set_server_action"), Description(
        "Turns ONE server's switch for ONE agent server action on or off: force_plan, evict_plan, recompile_module, create_plan_guide and drop_plan_guide (SQL Server only) and kill_session (SQL Server and PostgreSQL). All switches start off; an agent can act on a server only while its switch for that action is on (and the other conditions hold: Level 3, the service's agent actions on, a remediation login given as an env: or file: reference). Served only with mcp.allowServerAdmin, and never offered to an agent that investigates an alert. The change is journaled with who made it, from MCP, and the state before and after. Reversed by calling it again with the other value. The answer gives the state before and after and the call that puts it back.")]
    public static Task<string> SetServerAction(
        [Description("The server's full name, exactly as list_servers shows it (a disabled server too). A part of a name is refused and the near matches are listed.")] string server_name,
        [Description("force_plan, evict_plan, kill_session, recompile_module, create_plan_guide or drop_plan_guide.")] string action,
        [Description("True to turn the switch on, false to turn it off.")] bool enabled,
        NpgsqlDataSource postgres,
        StoreOwnerSource? ownerSource = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        RunAsync(postgres, ownerSource?.Read(), logger, server_name, action, enabled, ActorOf(), cancellationToken);

    /// <summary>The tool body, so a test drives it with its own data sources and caller. <paramref name="owner"/> is the store owner's data source.</summary>
    internal static async Task<string> RunAsync(
        NpgsqlDataSource postgres, NpgsqlDataSource? owner, ILogger? logger, string serverName, string action, bool enabled, string actor, CancellationToken ct)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(serverName))
            {
                return Refuse("invalid", "server_name is required. Nothing was changed.");
            }

            if (owner is null)
            {
                return Refuse("no_owner_source", "The service has not published its store owner connection yet, so the switch cannot be written. Nothing was changed.");
            }

            /* One server, named exactly: this write must never land on a neighbour whose name merely contains the text. The lookup is the one
               edit_server and remove_server run: every definition (a disabled server too), an exact name only. A partial name is refused with the
               near matches listed, so the caller picks one (#6269). */
            var definitions = await DarlingMcpServerAdminTools.LoadDefinitionsForRemovalAsync(postgres);
            var servers = definitions.Select(d => d.Server).ToList();
            var target = DarlingMcpServerAdminTools.ResolveForRemoval(servers, serverName, nameClash: NameClash.Refuse);
            if (target.Candidates.Count == 0)
            {
                return Refuse("not_found", $"Could not resolve server '{Clip(serverName)}': it is not a defined server. Nothing was changed.");
            }

            if (target.MatchedBy != "exact" || target.Candidates.Count > 1)
            {
                var near = string.Join(", ", target.Candidates.Take(10).Select(c => c.ServerName));
                return Refuse("ambiguous",
                    $"'{Clip(serverName)}' is not the exact name of one server (it matches {target.Candidates.Count} as {(target.MatchedBy == "exact" ? "an exact name" : "part of a name")}: {near}). "
                    + "Send ONE server's full name. Nothing was changed.");
            }

            var resolved = target.Candidates[0];
            var result = await ServerActionSwitches.SetAsync(owner, resolved.ServerId, action, enabled, actor, ServerActionSurfaces.Mcp, ct);
            var node = result.ToJson();
            if (result.Status == "set")
            {
                logger?.LogWarning("MCP server action switch {Action} on {Server}: {Before} -> {After}, caller {Caller}",
                    result.Action, result.ServerName, result.Before, result.After, actor);
                node["undo"] = new JsonObject
                {
                    ["tool"] = SetToolName,
                    ["arguments"] = new JsonObject { ["server_name"] = result.ServerName, ["action"] = result.Action, ["enabled"] = result.Before },
                };
            }

            return node.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(SetToolName, ex);
        }
    }

    [McpServerTool(Name = "get_server_actions"), Description(
        "Lists every monitored server's switch for each agent server action (force_plan, evict_plan, kill_session, recompile_module, create_plan_guide, drop_plan_guide): on or off (a server with no row is off), who last set it and when, and whether the server's engine can run the action at all (valid false: the switch cannot be turned on). A read. set_server_action changes a switch where mcp.allowServerAdmin is on.")]
    public static async Task<string> GetServerActions(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        try
        {
            var rows = await ServerActionSwitches.ListAsync(postgres, cancellationToken);
            return ListToJson(rows).ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_server_actions", ex);
        }
    }

    /// <summary>The list as the MCP read and the web GET both answer it: <c>{servers_returned, servers: [{server_id, server_name, engine, engine_kind (postgresql|sqlserver), actions: [{action, valid, enabled, set_by, updated_at_utc}]}]}</c>.</summary>
    internal static JsonObject ListToJson(System.Collections.Generic.IReadOnlyList<ServerActionSwitchRow> rows) => new()
    {
        ["servers_returned"] = rows.Count,
        ["servers"] = new JsonArray(rows.Select(r => (JsonNode)new JsonObject
        {
            ["server_id"] = r.ServerId,
            ["server_name"] = r.ServerName,
            ["engine"] = r.Engine,
            ["engine_kind"] = r.EngineKind == CollectorTargetEngine.PostgreSql ? "postgresql" : "sqlserver",
            ["actions"] = new JsonArray(r.Actions.Select(a => (JsonNode)new JsonObject
            {
                ["action"] = a.Action,
                ["valid"] = a.Valid,
                ["enabled"] = a.Enabled,
                ["set_by"] = a.SetBy,
                ["updated_at_utc"] = a.UpdatedAtUtc?.ToString("o", CultureInfo.InvariantCulture),
            }).ToArray()),
        }).ToArray()),
    };

    private static string Clip(string text) => text.Length <= 80 ? text : text[..80];

    private static string Refuse(string code, string message) =>
        new JsonObject { ["status"] = "refused", ["code"] = code, ["message"] = message }.ToJsonString();
}
