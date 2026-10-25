/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The collection-state tools (#6230 part A): <c>pause_collection</c> / <c>resume_collection</c> (all collection),
/// <c>disable_server</c> / <c>enable_server</c> (one server), <c>set_server_excluded_databases</c> (one server's excluded
/// databases) and <c>reload_config</c>, with <c>get_collection_state</c> to read the state before and after. The writes are served
/// under <c>mcp.allowWrites</c> and are never offered to an investigation agent (<see cref="DarlingMcpWritePolicy.NeverAgentTools"/>).
///
/// <para><b>One write path.</b> Pause and resume queue the same command the Viewer queues, through <c>config.request_mcp_command</c>
/// (<see cref="DarlingMcpCommandQueue"/>): the service runs it, the row is stamped <c>mcp</c>, and the <c>mcp</c> login holds no write on the paused flag.
/// Enable, disable and reload build the <see cref="ClaimedCommand"/> a queued command
/// row would carry and run the plan <see cref="DarlingCommandExecutor.ResolvePlan"/> returns through
/// <see cref="DarlingCommandExecutor.ExecuteStoreWriteAsync(NpgsqlDataSource, CommandPlan, CancellationToken)"/>: the same SQL the
/// command plane runs. The excluded-databases write is <see cref="MonitoredServerExclusions"/>, shared with the Viewer. This class owns no
/// write SQL and no bounds of its own. A server is found the way <c>edit_server</c> and <c>remove_server</c> find one; a name that matches
/// several servers is answered with the candidates, never picked from. Each server write logs at Warning with the tool, the server and the caller (<see cref="ReadCaller.WriteIdentity"/>: the token's fingerprint, then the client's own name).</para>
///
/// <para><b>Every answer says what changed, what it was, and the call that puts it back.</b> The service re-reads these on its next
/// config poll (the reload beacon the writes bump), so there is no restart.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpCollectionStateTools
{
    private const string TakesEffect = "The running service re-reads this on its next config poll; no restart is needed.";

    private sealed record ServerRow(int ServerId, string Name, string StorageName, string Engine, bool Enabled, IReadOnlyList<string> Excluded, decimal MonthlyCostUsd);

    [McpServerTool(Name = "pause_collection"), Description(
        "Pauses ALL collection, alerting, analysis and purge in the service: queues the same pause command the Viewer's Pause button queues, which the service runs and which sets its paused flag. Data already collected stays, and nothing is deleted. A pass already running finishes; no new pass starts until resume_collection. Served only with mcp.allowWrites. The answer gives the command id, the previous state, the exact call that undoes it and the read that shows the result (get_command_result, then get_collection_state).")]
    public static Task<string> PauseCollection(NpgsqlDataSource postgres, ILogger? logger = null, CancellationToken cancellationToken = default) =>
        ApplyPauseAsync(postgres, "pause_collection", pause: true, logger, cancellationToken);

    [McpServerTool(Name = "resume_collection"), Description(
        "Resumes collection after pause_collection: queues the same resume command the Viewer's Resume button queues, which the service runs and which clears its paused flag, so the next pass runs. Served only with mcp.allowWrites. The answer gives the command id, the previous state, the exact call that undoes it and the read that shows the result (get_command_result, then get_collection_state).")]
    public static Task<string> ResumeCollection(NpgsqlDataSource postgres, ILogger? logger = null, CancellationToken cancellationToken = default) =>
        ApplyPauseAsync(postgres, "resume_collection", pause: false, logger, cancellationToken);

    [McpServerTool(Name = "disable_server"), Description(
        "Takes ONE monitored server out of collection: sets its enabled flag to false. Its definition, tags and collected history stay; the service skips it from its next config poll. Reversed by enable_server. Served only with mcp.allowWrites. The answer gives the previous state and the exact call that undoes it; get_collection_state lists each server's enabled flag.")]
    public static Task<string> DisableServer(
        NpgsqlDataSource postgres,
        [Description("Server name (the name list_servers shows), or a unique part of it.")] string server_name,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ApplyEnabledAsync(postgres, "disable_server", server_name, enable: false, logger, cancellationToken);

    [McpServerTool(Name = "enable_server"), Description(
        "Brings a disabled monitored server back into collection: sets its enabled flag to true; the service picks it up on its next config poll. Reverses disable_server. Served only with mcp.allowWrites. The answer gives the previous state and the exact call that undoes it; get_collection_state lists each server's enabled flag.")]
    public static Task<string> EnableServer(
        NpgsqlDataSource postgres,
        [Description("Server name (the name list_servers shows), or a unique part of it.")] string server_name,
        ILogger? logger = null,
        CancellationToken cancellationToken = default) =>
        ApplyEnabledAsync(postgres, "enable_server", server_name, enable: true, logger, cancellationToken);

    [McpServerTool(Name = "set_server_excluded_databases"), Description(
        "Replaces ONE server's excluded-databases list: the databases the collectors skip on that server. The list you send is the whole new list; an empty list clears it. At most 256 names of at most 128 characters each, no control characters; repeats (any casing) are kept once. Data already collected stays. The service applies it on its next config poll. Served only with mcp.allowWrites. The answer gives the previous list and the exact call that restores it; get_collection_state lists each server's excluded databases.")]
    public static async Task<string> SetServerExcludedDatabases(
        NpgsqlDataSource postgres,
        [Description("Server name (the name list_servers shows), or a unique part of it.")] string server_name,
        [Description("The complete new list of database names to exclude. Empty clears the list.")] string[] databases,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        const string tool = "set_server_excluded_databases";
        try
        {
            if (databases is null)
            {
                return Refuse(tool, "invalid", "databases is required: send the whole list of database names to exclude, or an empty list to clear it. Nothing was changed.");
            }

            if (MonitoredServerExclusions.Refusal(databases) is string refusal)
            {
                return Refuse(tool, "invalid", refusal + " Nothing was changed.");
            }

            var (server, error) = await ResolveAsync(postgres, tool, server_name, cancellationToken);
            if (server is null)
            {
                return error!;
            }

            var wanted = MonitoredServerExclusions.Prepare(databases);
            await using (var command = MonitoredServerExclusions.CreateSetCommand(postgres, server.ServerId, wanted, McpCommandDeadlines.ReadSeconds))
            {
                await command.ExecuteNonQueryAsync(cancellationToken);
            }

            var after = await MonitoredServerExclusions.ReadAsync(postgres, server.ServerId, cancellationToken) ?? wanted;
            LogWrite(logger, tool, server);
            var changed = !server.Excluded.SequenceEqual(after, StringComparer.Ordinal);
            return new JsonObject
            {
                ["status"] = changed ? "changed" : "unchanged",
                ["tool"] = tool,
                ["server"] = server.Name,
                ["storage_name"] = server.StorageName,
                ["what_changed"] = after.Count == 0
                    ? $"{server.Name} has no excluded databases now; every database is collected."
                    : $"{server.Name} now excludes {after.Count} database(s): {string.Join(", ", after)}.",
                ["previous"] = new JsonObject { ["excluded_databases"] = Strings(server.Excluded) },
                ["current"] = new JsonObject { ["excluded_databases"] = Strings(after) },
                ["undo"] = Undo(tool, new JsonObject { ["server_name"] = server.StorageName, ["databases"] = Strings(server.Excluded) }),
                ["check"] = "get_collection_state",
                ["takes_effect"] = TakesEffect,
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    [McpServerTool(Name = "reload_config"), Description(
        "Asks the running service to re-read its configuration from the store now, instead of on its next poll. It changes no setting: it bumps the store's config version, which the service watches. Served only with mcp.allowWrites. The answer gives the version before and after; there is nothing to undo.")]
    public static async Task<string> ReloadConfig(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        const string tool = "reload_config";
        try
        {
            var before = await ReadServiceAsync(postgres, cancellationToken);
            await DarlingCommandExecutor.ExecuteStoreWriteAsync(
                postgres, DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(0, "reload", null, null, "mcp")), cancellationToken);
            var after = await ReadServiceAsync(postgres, cancellationToken);
            return new JsonObject
            {
                ["status"] = "changed",
                ["tool"] = tool,
                ["what_changed"] = "A configuration reload was signaled. No setting changed.",
                ["previous"] = new JsonObject { ["config_version"] = before.ConfigVersion },
                ["current"] = new JsonObject { ["config_version"] = after.ConfigVersion },
                ["undo"] = "None needed: a reload changes no setting.",
                ["check"] = "get_collection_state",
                ["takes_effect"] = TakesEffect,
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    [McpServerTool(Name = "get_collection_state"), Description(
        "Shows whether collection is paused, the store's config version, and every monitored server's enabled flag, excluded databases and monthly cost in USD (0 means none is set; disabled servers included). get_server_settings shows the rest of the fields edit_server changes. Read it before and after pause_collection, resume_collection, disable_server, enable_server, set_server_excluded_databases or reload_config. A server with enabled false is skipped by collection.")]
    public static async Task<string> GetCollectionState(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        try
        {
            var service = await ReadServiceAsync(postgres, cancellationToken);
            var servers = await ReadServersAsync(postgres, cancellationToken);
            return new JsonObject
            {
                ["status"] = "ok",
                ["paused"] = service.Paused,
                ["config_version"] = service.ConfigVersion,
                ["summary"] = service.Paused
                    ? "Collection is PAUSED: no new pass starts until resume_collection."
                    : $"Collection is running. {servers.Count(s => s.Enabled)} of {servers.Count} server(s) enabled.",
                ["servers"] = new JsonArray(servers.Select(ServerJson).ToArray<JsonNode?>()),
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_collection_state", ex);
        }
    }

    /// <summary>
    /// #6402: the read of the fields <c>edit_server</c> changes. It is the web edit route's own read
    /// (<see cref="DarlingMcpServerAdminTools.PostgresServerEditStore.ReadRowAsync"/>) and the same value shape the
    /// <c>edit_server</c> conflict answer carries (<see cref="DarlingMcpServerAdminTools.CurrentValuesOf"/>), so a read and an edit
    /// never disagree about a field. Names the server the way <c>edit_server</c> does. Never returns a password or an AWS external ID.
    /// </summary>
    [McpServerTool(Name = "get_server_settings"), Description(
        "Shows the settings edit_server changes, for ONE monitored server, with their current values: display name, host, port, database, read-only intent, auth, username, encrypt mode, trust certificate, multi-subnet failover, Query Store replica, monthly cost in USD (0 = none set), AWS role, and modified_at. A read; no password is ever returned."
        + "<<GUIDE>>"
        + "Read it before edit_server, and pass modified_at back as expected_modified_at so the edit fails instead of overwriting a change made since. A disabled server is found too. aws_external_id_set says whether an AWS external ID is stored (the ID itself is never returned). Whether a password is stored cannot be read here: that column is write-only for this role. server_name is the name list_servers shows, the storage name, or a part of either that matches ONE server; several matches are never picked from, the answer lists them. A server defined only in darling.json has no stored definition and is not found. The answer is {status:\"ok\", settings:{...}}, the same fields edit_server's conflict answer calls current.")]
    public static async Task<string> GetServerSettings(
        NpgsqlDataSource postgres,
        [Description("Server name (the name list_servers shows), or a unique part of it.")] string server_name,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var (server, error) = await ResolveAsync(postgres, "get_server_settings", server_name, cancellationToken);
            if (error is not null || server is null)
            {
                return error ?? Refuse("get_server_settings", "not_found", $"No monitored server is named '{server_name}'.");
            }

            var row = await new DarlingMcpServerAdminTools.PostgresServerEditStore(postgres).ReadRowAsync(server.ServerId, cancellationToken);
            if (row is null)
            {
                return Refuse("get_server_settings", "not_found", $"'{server.Name}' was defined when this call resolved it but its definition was gone by the time it was read.");
            }

            return System.Text.Json.JsonSerializer.Serialize(
                new { status = "ok", settings = DarlingMcpServerAdminTools.CurrentValuesOf(row) }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_server_settings", ex);
        }
    }

    private static JsonNode ServerJson(ServerRow s) => new JsonObject
    {
        ["server_id"] = s.ServerId,
        ["server"] = s.Name,
        ["storage_name"] = s.StorageName,
        ["engine"] = s.Engine,
        ["enabled"] = s.Enabled,
        ["excluded_databases"] = Strings(s.Excluded),
        /* #6402: 0 means no cost is set (the unset value every FinOps read treats as "no cost"). */
        ["monthly_cost_usd"] = s.MonthlyCostUsd,
    };

    private static JsonArray Strings(IEnumerable<string> values) =>
        new(values.Select(v => (JsonNode?)JsonValue.Create(v)).ToArray());

    private static JsonObject Undo(string tool, JsonObject arguments) => new() { ["tool"] = tool, ["arguments"] = arguments };

    private static string Refuse(string tool, string reason, string message) =>
        new JsonObject
        {
            ["status"] = "refused",
            ["tool"] = tool,
            ["reason"] = reason,
            ["message"] = message,
            ["changed"] = false,
        }.ToJsonString();

    /// <summary>Pause and resume go through the command queue, the same <c>pause</c> / <c>resume</c> command the Viewer queues: the
    /// service runs it and the row shows who asked and what came of it. The answer gives the command id and the read that shows the result.</summary>
    private static async Task<string> ApplyPauseAsync(NpgsqlDataSource postgres, string tool, bool pause, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            var before = await ReadServiceAsync(postgres, cancellationToken);

            /* Decide from the state the queue is heading for, not the flag alone: a pause or resume that is still queued has not
               moved the flag yet, and the service runs them in command_id order, so the last one decides where the flag ends up.
               An undo of a pause that is still pending therefore queues the resume (it runs after the pause). */
            var pending = await ReadPendingPauseCommandsAsync(postgres, cancellationToken);
            var effective = pending.Count == 0 ? before.Paused : pending[^1].CommandType == "pause";
            if (effective == pause)
            {
                var queuedAlready = pending.Count == 0 ? null : pending[^1];
                var unchanged = new JsonObject
                {
                    ["status"] = "unchanged",
                    ["tool"] = tool,
                    ["what_changed"] = queuedAlready is null
                        ? (pause ? "Collection was already paused. Nothing was queued." : "Collection was not paused. Nothing was queued.")
                        : $"A {queuedAlready.CommandType} (command {queuedAlready.CommandId.ToString(CultureInfo.InvariantCulture)}) is already queued and will leave collection "
                            + (pause ? "paused" : "running") + ". Nothing was queued.",
                    ["current"] = new JsonObject { ["paused"] = before.Paused },
                    ["check"] = "get_collection_state",
                };
                if (queuedAlready is not null)
                {
                    unchanged["check_with"] = new JsonObject { ["tool"] = "get_command_result", ["command_id"] = queuedAlready.CommandId };
                }

                return unchanged.ToJsonString();
            }

            var commandType = pause ? "pause" : "resume";
            var queued = await DarlingMcpCommandQueue.QueueAsync(postgres, commandType, null, null, cancellationToken);
            if (queued.Reason is not null)
            {
                return Refuse(tool, queued.Reason, queued.Message ?? "The request was refused. Nothing was queued.");
            }

            logger?.LogWarning("MCP {Tool} queued command {CommandId} ({CommandType}) for the whole service, caller {Caller}",
                tool, queued.CommandId, commandType, ReadCaller.WriteIdentity());
            var runsAfter = pending.Count == 0 ? null : pending[^1];
            return new JsonObject
            {
                ["status"] = "queued",
                ["tool"] = tool,
                ["command_id"] = queued.CommandId,
                ["runs_after"] = runsAfter is null ? null : new JsonObject { ["command_id"] = runsAfter.CommandId, ["command_type"] = runsAfter.CommandType },
                ["what_changed"] = pause
                    ? "A pause was queued, the same command the Viewer's Pause button queues. When the service runs it, no new pass starts until resume_collection; a pass already running finishes."
                    : "A resume was queued, the same command the Viewer's Resume button queues. When the service runs it, the next pass runs.",
                ["previous"] = new JsonObject { ["paused"] = before.Paused },
                ["undo"] = Undo(pause ? "resume_collection" : "pause_collection", new JsonObject()),
                ["check_with"] = new JsonObject { ["tool"] = "get_command_result", ["command_id"] = queued.CommandId },
                ["check"] = runsAfter is null
                    ? "get_collection_state shows the paused flag once the command has run."
                    : $"The {runsAfter.CommandType} (command {runsAfter.CommandId.ToString(CultureInfo.InvariantCulture)}) was still queued, so this {commandType} (command {queued.CommandId.ToString(CultureInfo.InvariantCulture)}) runs after it. "
                        + "get_collection_state shows the paused flag once both have run.",
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    private sealed record PendingPauseCommand(long CommandId, string CommandType);

    /// <summary>The pause and resume commands still queued or running, oldest first: the order the service claims them in.</summary>
    private static async Task<List<PendingPauseCommand>> ReadPendingPauseCommandsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(
            "SELECT command_id, command_type FROM config.config_command " +
            "WHERE command_type IN ('pause', 'resume') AND status IN ('pending', 'in_progress') ORDER BY command_id");
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        var rows = new List<PendingPauseCommand>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PendingPauseCommand(reader.GetInt64(0), reader.GetString(1)));
        }

        return rows;
    }

    private static void LogWrite(ILogger? logger, string tool, ServerRow server) =>
        logger?.LogWarning("MCP {Tool} changed server id {ServerId} ({Server}), caller {Caller}",
            tool, server.ServerId, server.StorageName, ReadCaller.WriteIdentity());

    private static async Task<string> ApplyEnabledAsync(
        NpgsqlDataSource postgres, string tool, string? serverName, bool enable, ILogger? logger, CancellationToken cancellationToken)
    {
        try
        {
            var (server, error) = await ResolveAsync(postgres, tool, serverName, cancellationToken);
            if (server is null)
            {
                return error!;
            }

            var plan = DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(0, enable ? "enable_server" : "disable_server", server.ServerId, null, "mcp"));
            if (plan.Kind != CommandKind.StoreWrite)
            {
                return Refuse(tool, "invalid", (plan.FailReason ?? "The request was refused.") + " Nothing was changed.");
            }

            var changed = server.Enabled != enable;
            if (changed)
            {
                await DarlingCommandExecutor.ExecuteStoreWriteAsync(postgres, plan, cancellationToken);
                LogWrite(logger, tool, server);
            }

            var after = (await ReadServersAsync(postgres, cancellationToken)).FirstOrDefault(s => s.ServerId == server.ServerId);
            return new JsonObject
            {
                ["status"] = changed ? "changed" : "unchanged",
                ["tool"] = tool,
                ["server"] = server.Name,
                ["storage_name"] = server.StorageName,
                ["what_changed"] = changed
                    ? $"{server.Name} is now {(enable ? "enabled: collection includes it again" : "disabled: collection skips it")}."
                    : $"{server.Name} was already {(enable ? "enabled" : "disabled")}. Nothing changed.",
                ["previous"] = new JsonObject { ["enabled"] = server.Enabled },
                ["current"] = new JsonObject { ["enabled"] = after?.Enabled },
                ["undo"] = Undo(server.Enabled ? "enable_server" : "disable_server", new JsonObject { ["server_name"] = server.StorageName }),
                ["check"] = "get_collection_state",
                ["takes_effect"] = TakesEffect,
            }.ToJsonString();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError(tool, ex);
        }
    }

    /// <summary>Finds one server among ALL definitions (a disabled server must be found to be enabled), with the matching rule
    /// <c>edit_server</c> and <c>remove_server</c> use: the storage name <c>list_servers</c> shows, else the name, else a part of either that
    /// matches ONE server. A name that matches several is never picked from: the answer lists the candidates by storage name.</summary>
    private static async Task<(ServerRow? Server, string? Error)> ResolveAsync(
        NpgsqlDataSource postgres, string tool, string? serverName, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(serverName))
        {
            return (null, Refuse(tool, "invalid", "server_name is required. Nothing was changed."));
        }

        var servers = await ReadServersAsync(postgres, cancellationToken);
        var typed = serverName.Trim();
        var target = DarlingMcpServerAdminTools.ResolveForRemoval(
            servers.Select(s => new RegisteredServer(s.ServerId, s.StorageName, s.Name)).ToList(), typed,
            nameClash: NameClash.Refuse);
        if (target.Candidates.Count == 1)
        {
            return (servers.First(s => s.ServerId == target.Candidates[0].ServerId), null);
        }

        if (target.Candidates.Count == 0)
        {
            return (null, Refuse(tool, "not_found",
                $"No monitored server is named '{typed}'. Known servers (storage names): " + string.Join(", ", servers.Select(s => s.StorageName)) + ". Nothing was changed."));
        }

        return (null, Refuse(tool, "ambiguous",
            $"'{typed}' matches more than one server: " + string.Join(", ", target.Candidates.Select(c => c.ServerName))
            + ". Send one storage name from this list as server_name. Nothing was changed."));
    }

    private static async Task<List<ServerRow>> ReadServersAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var rows = new List<ServerRow>();
        await using var command = postgres.CreateCommand(
            "SELECT server_id, name, host, database, COALESCE(read_only_intent, false), engine, port, is_enabled, excluded_databases, COALESCE(monthly_cost_usd, 0) "
            + "FROM config.config_monitored_servers ORDER BY name, server_id");
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var engine = reader.IsDBNull(5) ? null : reader.GetString(5);
            rows.Add(new ServerRow(
                reader.GetInt32(0), reader.GetString(1),
                ServerIdHelper.BuildStorageName(
                    reader.GetString(2), reader.IsDBNull(3) ? null : reader.GetString(3), reader.GetBoolean(4), engine, reader.IsDBNull(6) ? 0 : reader.GetInt32(6)),
                engine ?? "sqlserver", reader.GetBoolean(7),
                reader.IsDBNull(8) ? Array.Empty<string>() : reader.GetFieldValue<string[]>(8),
                reader.GetDecimal(9)));
        }

        return rows;
    }

    private static async Task<(bool Paused, long ConfigVersion)> ReadServiceAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand("SELECT paused, config_version FROM config.config_service WHERE id = 1");
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken)
            ? (reader.GetBoolean(0), Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture))
            : (false, 0);
    }
}
