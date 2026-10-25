/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>Where a server action switch was changed from (#6269). Written into the journal row's detail.</summary>
public static class ServerActionSurfaces
{
    public const string Viewer = "viewer";
    public const string Web = "web";
    public const string Mcp = "mcp";
}

/// <summary>One server's view of one action's switch (#6269): <see cref="Valid"/> is false when the engine cannot run the action (the
/// switch then cannot be turned on). <see cref="Enabled"/> is false when there is no row, as the gate reads it.</summary>
public sealed record ServerActionSwitchState(string Action, bool Valid, bool Enabled, string? SetBy, DateTime? UpdatedAtUtc);

/// <summary>One monitored server and its action switches (#6269).</summary>
public sealed record ServerActionSwitchRow(int ServerId, string ServerName, string Engine, IReadOnlyList<ServerActionSwitchState> Actions)
{
    /// <summary>The engine the registry's raw spelling names (pg, aurora and mixed case read as PostgreSQL), through the one shared parser. Both screens label the engine from this (#6269).</summary>
    public CollectorTargetEngine EngineKind => MonitoredEngineKind.ParseConfiguredEngine(Engine);
}

/// <summary>The answer to one switch change (#6269). <see cref="Code"/> is null on success: <c>set</c> or <c>unchanged</c> (the switch already held that
/// state, so nothing was written or journaled). A refusal is <c>unknown_action</c>, <c>unknown_server</c> or <c>not_for_engine</c>.</summary>
public sealed record ServerActionSwitchResult(
    bool Succeeded, string Status, string? Code, string Message, int? ServerId, string? ServerName, string? Action, bool? Before, bool? After, long? JournalId)
{
    public JsonObject ToJson()
    {
        var node = new JsonObject { ["status"] = Status };
        if (Code is not null)
            node["code"] = Code;
        node["message"] = Message;
        if (ServerId is not null)
            node["server_id"] = ServerId;
        if (ServerName is not null)
            node["server_name"] = ServerName;
        if (Action is not null)
            node["action"] = Action;
        if (Before is not null)
            node["before"] = Before.Value ? "on" : "off";
        if (After is not null)
            node["after"] = After.Value ? "on" : "off";
        if (JournalId is not null)
            node["journal_id"] = JournalId;
        return node;
    }
}

/// <summary>
/// The one writer of <c>config.config_server_actions</c> (#6269). Every surface (the Viewer's <c>set_server_action</c> command, the web
/// route, the MCP tool) calls <see cref="SetAsync"/> with the store OWNER's data source: no other role holds a write on the table, and
/// none is given one. It checks the action against the fixed list and the server's engine, writes the row and journals
/// <c>server_action_switched</c> in ONE transaction, with who made the change, from which surface, and the state before and after.
/// A change is journaled as the caller made it (<c>person:&lt;login&gt;</c> or <c>mcp:&lt;token and client&gt;</c>): it never reads as an agent's.
/// </summary>
public static class ServerActionSwitches
{
    /// <summary>The journal event of a switch change. Not an <c>action_called</c> row, so the Agent changes list (an agent's calls) never shows it.</summary>
    public const string JournalEvent = "server_action_switched";

    /* #6727: sql_engine_edition (5 Azure SQL Database, 8 Azure SQL Managed Instance) is what the tools read to refuse an Azure target, and the registry
       does not carry it. A server with no collect.servers row yet, or no probed edition, reads NULL and answers as an ordinary SQL Server. The lock
       stays on the registry row (FOR SHARE OF cannot name the nullable side of the join). */
    internal const string ServerSql = @"
SELECT m.server_id, m.name, m.engine, c.sql_engine_edition
FROM config.config_monitored_servers AS m
LEFT JOIN collect.servers AS c ON c.server_id = m.server_id
WHERE m.server_id = $1
FOR SHARE OF m";

    internal const string ListSql = @"
SELECT s.server_id, s.name, s.engine, a.action, a.enabled, a.set_by, a.updated_at, c.sql_engine_edition
FROM config.config_monitored_servers AS s
LEFT JOIN collect.servers AS c ON c.server_id = s.server_id
LEFT JOIN config.config_server_actions AS a ON a.server_id = s.server_id
ORDER BY s.name, s.server_id";

    /* The registry holds the raw darling.json spelling ("pg", "aurora"), so the engine is read through the one parser the collectors use (#6269). */
    private static bool IsPostgres(string? engine) =>
        MonitoredEngineKind.ParseConfiguredEngine(engine) == CollectorTargetEngine.PostgreSql;

    /// <summary>
    /// Sets one server's switch for one action. <paramref name="actor"/> is the journal actor, already in the shape of its surface
    /// (<see cref="AgentChangeUndo.PersonActor"/> for a person); it is also stored as the row's <c>set_by</c>. Never throws for a refusal.
    /// A change to the state the switch already holds writes and journals nothing.
    /// <para>The change takes the exclusive form of <see cref="ServerActionSwitchLock"/> first, for on and off alike, so it waits for an agent write
    /// in flight on that (server, action) and a write that starts after it reads the committed switch. Every statement of the change waits at most
    /// <paramref name="lockWaitSeconds"/> (60 s, the wait a switch-off already has); past that nothing is changed and the answer is
    /// <c>switch_busy</c>.</para>
    /// </summary>
#pragma warning disable CA1068 // #6269: lockWaitSeconds is a test knob that trails the token; putting it first would shift every positional caller
    public static async Task<ServerActionSwitchResult> SetAsync(
        NpgsqlDataSource owner, int serverId, string? action, bool enabled, string actor, string surface, CancellationToken cancellationToken = default,
        int lockWaitSeconds = InvestigationRequestStore.AgentOffSwitchWaitSeconds)
    {
#pragma warning restore CA1068
        ArgumentNullException.ThrowIfNull(owner);
        if (string.IsNullOrWhiteSpace(actor))
            throw new ArgumentException("A switch change needs the name of who made it.", nameof(actor));

        var name = (action ?? string.Empty).Trim();
        if (!ServerActionNames.All.Contains(name, StringComparer.Ordinal))
        {
            return Refused("unknown_action", $"'{Clip(name)}' is not a server action. The actions are {string.Join(", ", ServerActionNames.All)}.", null, null, null);
        }

        await using var connection = await owner.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            return await SetInTransactionAsync(connection, transaction, serverId, name, enabled, actor, surface, lockWaitSeconds, cancellationToken);
        }
        catch (Exception ex) when (IsWaitTimeout(ex, cancellationToken))
        {
            /* The transaction is rolled back as it is disposed: the switch row and the journal are as they were. */
            return Refused("switch_busy", SwitchBusyMessage, serverId, null, name);
        }
    }

    /// <summary>The refusal a switch change answers when an agent change on the server was still running when the wait ended (#6269).</summary>
    public const string SwitchBusyMessage =
        "The switch was not changed: an agent change on this server was still running after 60 seconds. Try again in a minute.";

    /* A command that ran past its CommandTimeout surfaces as an NpgsqlException over a TimeoutException, or as the server's own 57014 when its cancel
       arrives first. The caller's own cancellation is neither: it stays a cancellation. */
    private static bool IsWaitTimeout(Exception ex, CancellationToken cancellationToken) =>
        !cancellationToken.IsCancellationRequested
        && (ex is NpgsqlException { InnerException: TimeoutException } || ex is PostgresException { SqlState: PostgresErrorCodes.QueryCanceled });

    private static async Task<ServerActionSwitchResult> SetInTransactionAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, string name,
        bool enabled, string actor, string surface, int lockWaitSeconds, CancellationToken cancellationToken)
    {
        /* First, for on and off, before any row is read or written: an agent write holds the shared form to the end of its own transaction. */
        await ServerActionSwitchLock.AcquireExclusiveAsync(connection, transaction, serverId, name, lockWaitSeconds, cancellationToken);

        string? serverName = null;
        string? engine = null;
        int? edition = null;
        await using (var command = new NpgsqlCommand(ServerSql, connection, transaction) { CommandTimeout = lockWaitSeconds, Parameters = { new NpgsqlParameter<int> { TypedValue = serverId } } })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                serverName = reader.GetString(1);
                engine = reader.IsDBNull(2) ? null : reader.GetString(2);
                edition = reader.IsDBNull(3) ? null : reader.GetInt32(3);
            }
        }

        if (serverName is null)
            return Refused("unknown_server", "That server is not in the registry.", serverId, null, name);

        /* Turning a switch OFF is never refused for the engine: a switch left on before an engine edit must still be clearable (#6269). */
        if (enabled && !ServerActionNames.IsValidForEngine(name, IsPostgres(engine), edition))
        {
            return Refused("not_for_engine",
                $"{ServerActionNames.Label(name)} cannot run on {serverName}: it is {ServerActionNames.EngineDescription(IsPostgres(engine), edition)}. The switch was not changed.", serverId, serverName, name);
        }

        /* No row is off, as the gate reads it. The row is locked so two changes in a row see each other's state. */
        bool before;
        await using (var command = new NpgsqlCommand(
            "SELECT enabled FROM config.config_server_actions WHERE server_id = $1 AND action = $2 FOR UPDATE", connection, transaction)
        {
            CommandTimeout = lockWaitSeconds,
            Parameters = { new NpgsqlParameter<int> { TypedValue = serverId }, new NpgsqlParameter<string> { TypedValue = name, NpgsqlDbType = NpgsqlDbType.Text } },
        })
        {
            before = await command.ExecuteScalarAsync(cancellationToken) is true;
        }

        if (before == enabled)
        {
            await transaction.CommitAsync(cancellationToken);
            return new ServerActionSwitchResult(true, "unchanged", null,
                $"{ServerActionNames.Label(name)} on {serverName} was already {(enabled ? "on" : "off")}. Nothing was changed.", serverId, serverName, name, before, enabled, null);
        }

        await using (var command = new NpgsqlCommand(@"
INSERT INTO config.config_server_actions (server_id, action, enabled, set_by, updated_at)
VALUES ($1, $2, $3, $4, now())
ON CONFLICT (server_id, action) DO UPDATE SET enabled = EXCLUDED.enabled, set_by = EXCLUDED.set_by, updated_at = EXCLUDED.updated_at",
            connection, transaction)
        {
            CommandTimeout = lockWaitSeconds,
            Parameters =
            {
                new NpgsqlParameter<int> { TypedValue = serverId },
                new NpgsqlParameter<string> { TypedValue = name, NpgsqlDbType = NpgsqlDbType.Text },
                new NpgsqlParameter<bool> { TypedValue = enabled },
                new NpgsqlParameter<string> { TypedValue = actor, NpgsqlDbType = NpgsqlDbType.Text },
            },
        })
        {
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        var detail = new JsonObject
        {
            ["action"] = name,
            ["surface"] = surface,
            ["server_name"] = serverName,
            ["before"] = before ? "on" : "off",
            ["after"] = enabled ? "on" : "off",
        };
        long journalId;
        await using (var command = new NpgsqlCommand(InvestigationRequestStore.AgentActionJournalSql, connection, transaction)
        {
            CommandTimeout = lockWaitSeconds,
            Parameters =
            {
                new NpgsqlParameter<DateTime> { TypedValue = DateTime.UtcNow, NpgsqlDbType = NpgsqlDbType.TimestampTz },
                new NpgsqlParameter<Guid?> { TypedValue = null, NpgsqlDbType = NpgsqlDbType.Uuid },
                new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer },
                new NpgsqlParameter<string> { TypedValue = JournalEvent, NpgsqlDbType = NpgsqlDbType.Text },
                new NpgsqlParameter<string> { TypedValue = actor, NpgsqlDbType = NpgsqlDbType.Text },
                new NpgsqlParameter<string?> { TypedValue = null, NpgsqlDbType = NpgsqlDbType.Text },
                new NpgsqlParameter<string> { TypedValue = detail.ToJsonString(), NpgsqlDbType = NpgsqlDbType.Text },
            },
        })
        {
            journalId = Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
        }

        await transaction.CommitAsync(cancellationToken);
        return new ServerActionSwitchResult(true, "set", null,
            $"{ServerActionNames.Label(name)} on {serverName} is now {(enabled ? "on" : "off")} (it was {(before ? "on" : "off")}).", serverId, serverName, name, before, enabled, journalId);
    }

    /// <summary>Every monitored server with a state for each action: a server with no row reads off. <see cref="ServerActionSwitchState.Valid"/> is false for an action the engine cannot run.</summary>
    public static async Task<IReadOnlyList<ServerActionSwitchRow>> ListAsync(NpgsqlDataSource source, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(source);
        var servers = new List<(int Id, string Name, string Engine, int? Edition, Dictionary<string, (bool Enabled, string? SetBy, DateTime? At)> Rows)>();
        await using var connection = await source.OpenConnectionAsync(cancellationToken);
        await using var command = new NpgsqlCommand(ListSql, connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var id = reader.GetInt32(0);
            if (servers.Count == 0 || servers[^1].Id != id)
            {
                servers.Add((id, reader.GetString(1), reader.IsDBNull(2) ? "sqlserver" : reader.GetString(2),
                    reader.IsDBNull(7) ? null : reader.GetInt32(7), new(StringComparer.Ordinal)));
            }

            if (!reader.IsDBNull(3))
            {
                servers[^1].Rows[reader.GetString(3)] = (reader.GetBoolean(4), reader.IsDBNull(5) ? null : reader.GetString(5),
                    reader.IsDBNull(6) ? null : DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc));
            }
        }

        return servers.Select(s => new ServerActionSwitchRow(s.Id, s.Name, s.Engine,
            ServerActionNames.All.Select(a =>
            {
                var valid = ServerActionNames.IsValidForEngine(a, IsPostgres(s.Engine), s.Edition);
                return s.Rows.TryGetValue(a, out var row)
                    ? new ServerActionSwitchState(a, valid, row.Enabled, row.SetBy, row.At)
                    : new ServerActionSwitchState(a, valid, false, null, null);
            }).ToList())).ToList();
    }

    private static string Clip(string text) => text.Length <= 40 ? text : text[..40];

    private static ServerActionSwitchResult Refused(string code, string message, int? serverId, string? serverName, string? action) =>
        new(false, "refused", code, message, serverId, serverName, action, null, null, null);
}
