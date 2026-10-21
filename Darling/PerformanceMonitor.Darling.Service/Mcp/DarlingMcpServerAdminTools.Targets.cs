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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Over MCP, a registration that a person scoped a route to is kept where it connects (#6281): a notification route with an
/// agent setting scoped to a server id, or to a tag, covers that registration, so a second registration of the same machine
/// (another database or read-only intent, which hash to a new id) would sit outside the route. <c>add_servers</c> over MCP
/// therefore refuses an entry whose machine (engine, host and port) is held, under a different storage name, by a
/// registration (removed ones included) that has such a scoped route. Without one the entry is added as before. A route with
/// no scope applies to every server, so it never counts. The same storage name is not refused here: it is the existing
/// duplicate answer, or the re-use of a removed registration's own id.
/// </summary>
public sealed partial class DarlingMcpServerAdminTools
{
    /// <summary>A registration an add is compared with: a live definition (host, engine and port known) or a removed one
    /// (<c>collect.servers</c> keeps only its storage name).</summary>
    internal sealed record RegisteredTarget(int ServerId, string StorageKey, string? Host, string? Engine, int Port, string Label, bool Removed);

    /// <summary>A check a write runs inside its own transaction, after the identity lock (#6281): non-null is the refusal text.</summary>
    internal delegate Task<string?> HeldCheck(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken);

    private const int DefaultSqlServerPort = 1433;
    private const int DefaultPostgresPort = 5432;

    /// <summary>
    /// What one engine instance is, with the spellings of it folded together (#6281): case, a trailing dot on the host, a
    /// <c>tcp:</c> prefix, a <c>host,port</c> spelling (the port is taken from it when the port field is empty),
    /// <c>host\MSSQLSERVER</c> (the default instance, so the same as <c>host</c>) and an explicit default port (1433, or
    /// 5432 for PostgreSQL) all give one key. A named instance with no port stays apart from the default instance. Only the
    /// text is folded: a short name and its full domain name, or a name and its address, are different keys (#6285).
    /// </summary>
    internal static string MachineKeyOf(string host, string? engine, int port)
    {
        var postgres = ServerIdHelper.BuildStorageName("h", null, false, engine) != "h";
        var name = host.Trim();
        var instance = "";
        if (!postgres)
        {
            if (name.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase))
            {
                name = name.Substring(4).TrimStart();
            }

            var comma = name.LastIndexOf(',');
            if (comma >= 0
                && int.TryParse(name.Substring(comma + 1).Trim(), NumberStyles.None, CultureInfo.InvariantCulture, out var spelled)
                && spelled is > 0 and <= 65535)
            {
                port = port > 0 ? port : spelled;
                name = name.Substring(0, comma);
            }

            var slash = name.IndexOf('\\');
            if (slash >= 0)
            {
                instance = name.Substring(slash + 1).Trim();
                name = name.Substring(0, slash);
                if (string.Equals(instance, "MSSQLSERVER", StringComparison.OrdinalIgnoreCase))
                {
                    instance = "";
                }
            }
        }

        name = name.Trim().TrimEnd('.');
        if (port <= 0 && instance.Length == 0)
        {
            port = postgres ? DefaultPostgresPort : DefaultSqlServerPort;
        }

        return string.Create(CultureInfo.InvariantCulture, $"{(postgres ? "pg" : "sql")}|{name.ToLowerInvariant()}\\{instance.ToLowerInvariant()}|{port}");
    }

    /// <summary>True when <paramref name="held"/> is a registration of the machine <paramref name="entry"/> names, under any
    /// database or read-only intent. The host is compared as <see cref="MachineKeyOf"/> folds it.</summary>
    internal static bool SameTarget(RegisteredTarget held, ParsedServerEntry entry)
    {
        var config = entry.ProbeConfig;
        var wanted = MachineKeyOf(config.Host, config.Engine, config.Port);
        if (!held.Removed)
        {
            return held.Host is not null && string.Equals(MachineKeyOf(held.Host, held.Engine, held.Port), wanted, StringComparison.Ordinal);
        }

        /* A removed registration is known by its storage name alone: host[:database][:pg][:port][:RO]. The database, engine
           and port cannot be told apart from the name for certain (a database called 1433), so each way of reading its tail is
           tried and one that gives the entry's machine counts: that errs toward refusing. */
        var name = held.StorageKey;
        if (name.EndsWith(":RO", StringComparison.OrdinalIgnoreCase))
        {
            name = name.Substring(0, name.Length - 3);
        }

        var prefix = "";
        if (name.StartsWith("tcp:", StringComparison.OrdinalIgnoreCase))
        {
            prefix = name.Substring(0, 4);
            name = name.Substring(4);
        }

        var colon = name.IndexOf(':');
        var hostToken = prefix + (colon < 0 ? name : name.Substring(0, colon));
        var tail = colon < 0 ? Array.Empty<string>() : name.Substring(colon + 1).Split(':');
        /* One segment after the host is a database, or an engine token, or a port: the plain reading (a database, the SQL Server
           default port) is tried first. */
        if (tail.Length <= 1 && string.Equals(MachineKeyOf(hostToken, null, 0), wanted, StringComparison.Ordinal))
        {
            return true;
        }

        var last = tail.Length > 0 ? tail[^1] : "";
        var portTail = int.TryParse(last, NumberStyles.None, CultureInfo.InvariantCulture, out var port) && port > 0;
        var beforePort = portTail ? (tail.Length > 1 ? tail[^2] : "") : last;
        return string.Equals(beforePort, "pg", StringComparison.OrdinalIgnoreCase)
            ? string.Equals(MachineKeyOf(hostToken, EnginePostgres, portTail ? port : 0), wanted, StringComparison.Ordinal)
            : portTail && string.Equals(MachineKeyOf(hostToken, null, port), wanted, StringComparison.Ordinal);
    }

    /// <summary>The refusal for an entry whose machine another registration holds while a route with an agent setting is
    /// scoped to that registration, or null. <paramref name="scopedRoute"/> is the shared lookup edit_server uses.
    /// <paramref name="exceptServerId"/> is the registration an edit changes: it is not held against its own new address.</summary>
    internal static async Task<string?> TargetHeldRefusalAsync(
        IReadOnlyList<RegisteredTarget> held, ParsedServerEntry entry,
        Func<int, CancellationToken, Task<int?>> scopedRoute, CancellationToken cancellationToken, int? exceptServerId = null, bool forEdit = false)
    {
        foreach (var other in held)
        {
            if (other.ServerId == exceptServerId
                || string.Equals(other.StorageKey, entry.StorageKey, StringComparison.OrdinalIgnoreCase) || !SameTarget(other, entry))
            {
                continue;
            }

            if (await scopedRoute(other.ServerId, cancellationToken) is not { } routeId)
            {
                continue;
            }

            return (forEdit ? "Not saved: " : "Not added: ") + $"{entry.ProbeConfig.Host} is already registered as '{other.Label}'"
                + (other.Removed ? " (a removed registration)" : "")
                + $", and route {routeId} (an agent setting) is scoped to it, so "
                + (forEdit ? "this registration, moved there, would sit outside that route. " : "a second registration of the machine would sit outside that route. ")
                + (forEdit ? "Where a registration connects is changed in the Viewer or on the web dashboard." : "Register another database or intent in the Viewer or on the web dashboard.");
        }

        return null;
    }

    private const string RegisteredTargetsSql =
        "SELECT host, database, read_only_intent, engine, port, name, server_id FROM config_monitored_servers";

    private const string RemovedRegistrationsSql =
        "SELECT server_name, display_name, server_id FROM collect.servers WHERE server_id NOT IN (SELECT server_id FROM config_monitored_servers)";

    private static async Task<List<RegisteredTarget>> LoadRegisteredTargetsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        return await LoadRegisteredTargetsAsync(connection, null, cancellationToken);
    }

    /// <summary>The same read on an open connection, inside <paramref name="transaction"/> when one is given.</summary>
    private static async Task<List<RegisteredTarget>> LoadRegisteredTargetsAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        var targets = new List<RegisteredTarget>();
        await using (var command = new NpgsqlCommand(RegisteredTargetsSql, connection) { Transaction = transaction })
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var host = reader.GetString(0);
                var engine = reader.IsDBNull(3) ? null : reader.GetString(3);
                var port = reader.IsDBNull(4) ? 0 : reader.GetInt32(4);
                var key = StorageKeyOf(reader);
                var label = reader.IsDBNull(5) || string.IsNullOrWhiteSpace(reader.GetString(5)) ? key : reader.GetString(5);
                targets.Add(new RegisteredTarget(reader.GetInt32(6), key, host, engine, port, label, false));
            }
        }

        await using (var command = new NpgsqlCommand(RemovedRegistrationsSql, connection) { Transaction = transaction })
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var name = reader.GetString(0);
                var label = reader.IsDBNull(1) || string.IsNullOrWhiteSpace(reader.GetString(1)) ? name : reader.GetString(1);
                targets.Add(new RegisteredTarget(reader.GetInt32(2), name, null, null, 0, label, true));
            }
        }

        return targets;
    }

    /// <summary>The same rule as <see cref="TargetHeldRefusalAsync"/>, read inside the write's own transaction, after the
    /// identity lock: a route that was set up while the probe ran is seen here, and nothing is written. Null when nothing
    /// holds the machine. <paramref name="exceptServerId"/> is the registration an edit changes.</summary>
    internal static async Task<string?> TargetHeldRefusalInTransactionAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, ParsedServerEntry entry, int? exceptServerId, CancellationToken cancellationToken, bool forEdit = false)
    {
        var held = await LoadRegisteredTargetsAsync(connection, transaction, cancellationToken);
        return await TargetHeldRefusalAsync(held, entry,
            (serverId, token) => ScopedAgentRouteAsync(connection, transaction, serverId, token), cancellationToken, exceptServerId, forEdit);
    }
}
