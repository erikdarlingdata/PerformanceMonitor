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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Collectors;

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
/// <para>The host text alone cannot tell that two names reach one machine (a short name and its full domain name, a name and its
/// address), so once the add or edit has connected, the name the server reports for itself (#6285) is compared with the name
/// collected for each registration, and a match refuses just as the text match does. The text match stays: the identity adds to it.</para>
/// </summary>
public sealed partial class DarlingMcpServerAdminTools
{
    /// <summary>A registration an add is compared with: a live definition (host, engine and port known) or a removed one
    /// (<c>collect.servers</c> keeps only its storage name).</summary>
    internal sealed record RegisteredTarget(int ServerId, string StorageKey, string? Host, string? Engine, int Port, string Label, bool Removed,
        string? Identity = null);

    /// <summary>
    /// The machine and instance a SQL Server reports for itself (<c>SERVERPROPERTY('ServerName')</c>), as one comparable key, or
    /// null when there is none to compare (#6285). Case is folded. An Azure SQL Database names no machine of its own, so it has
    /// none (edition 5). Resolved IP addresses are never compared: Azure SQL Database logical servers share gateway addresses.
    /// </summary>
    internal static string? MachineIdentityOf(string? serverName, int engineEdition)
    {
        if (engineEdition == 5 || string.IsNullOrWhiteSpace(serverName))
        {
            return null;
        }

        return serverName.Trim().ToLowerInvariant();
    }

    /// <summary>True for an identity the PostgreSQL probe makes (<c>system_identifier:port</c>). A SQL Server's name never holds a colon, so
    /// the colon tells the two kinds apart without the registration's engine, which a removed registration no longer has.</summary>
    internal static bool IsPostgresIdentity(string? identity) => identity is not null && identity.Contains(':', StringComparison.Ordinal);

    /// <summary>#6340 round 2 (N1): true when a PostgreSQL target answered the probe but gave no identity, because the login may not call
    /// <c>pg_control_system()</c>. An Aurora target gives none by design (the connect does not ask it), so the host-text match alone
    /// decides there, and this is false for it.</summary>
    internal static bool PostgresIdentityUnread(ConnectionProbeResult probe, string? probedIdentity) =>
        probedIdentity is null && probe.Engine == CollectorTargetEngine.PostgreSql && !probe.IsAurora;

    /// <summary>True when the server the add or edit reached reports the same identity that was collected for <paramref name="held"/>.
    /// Either side having none is no match: such a registration is compared by its text alone.</summary>
    internal static bool SameIdentity(RegisteredTarget held, string? probedIdentity) =>
        probedIdentity is not null && held.Identity is not null
        && string.Equals(held.Identity, probedIdentity, StringComparison.Ordinal);

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
        return SameMachine(held, config.Host, config.Engine, config.Port);
    }

    /// <summary>The Query Store replica of an entry is a SQL Server endpoint the service connects to, so it is a machine the
    /// entry points at exactly as its host is (#6254). The value is a host[,port] or a listener name; the port and instance are
    /// folded from it by <see cref="MachineKeyOf"/>.</summary>
    internal static bool SameReplicaTarget(RegisteredTarget held, ParsedServerEntry entry)
        => QueryStoreReplicaSetting.Normalize(entry.ProbeConfig.QueryStoreReplica) is { } replica
            && SameMachine(held, replica, null, 0);

    private static bool SameMachine(RegisteredTarget held, string host, string? engine, int hostPort)
    {
        var wanted = MachineKeyOf(host, engine, hostPort);
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
    /// <paramref name="exceptServerId"/> is the registration an edit changes: it is not held against its own new address.
    /// <paramref name="probedIdentity"/> is what the server the add or edit connected to reports for itself (#6285), or null before
    /// the connection or when none was read: the text match applies either way, the identity match only when it is given.
    /// <paramref name="textMatch"/> is false for an edit that does not move the registration's address (a change to another connection
    /// field): then only the identity can refuse, and not when the server still reports the identity this registration already had,
    /// because the machine did not change.</summary>
    internal static async Task<string?> TargetHeldRefusalAsync(
        IReadOnlyList<RegisteredTarget> held, ParsedServerEntry entry,
        Func<int, CancellationToken, Task<int?>> scopedRoute, CancellationToken cancellationToken, int? exceptServerId = null, bool forEdit = false,
        string? probedIdentity = null, bool textMatch = true, bool postgresIdentityUnread = false, bool checkReplica = true)
    {
        if (exceptServerId is { } editedId && probedIdentity is not null
            && held.FirstOrDefault(t => t.ServerId == editedId)?.Identity == probedIdentity)
        {
            probedIdentity = null;
        }

        foreach (var other in held)
        {
            /* #6285 round 1 (M2): the registration being edited, and, for an ADD only, a removed registration this entry's own
               storage key re-uses (the same id, so the route that covers it follows). The key is compared as the id is made, by
               its exact spelling: a different spelling of a removed covered registration's key becomes a new id outside the route.
               An EDIT keeps its own id, so moving to a removed registration's key is not re-use. A live registration under the
               same key is the duplicate gates' answer. */
            if (other.ServerId == exceptServerId
                || (!forEdit && ServerIdOf(entry) == other.ServerId))
            {
                continue;
            }

            var sameText = textMatch && SameTarget(other, entry);
            /* #6254: the Query Store replica is a machine the service connects to for this registration, so it is held to the same rule
               as the host. An edit passes checkReplica only when it changes the replica (a host-only edit leaves it alone). */
            var replicaHeld = !sameText && checkReplica && SameReplicaTarget(other, entry);
            var sameIdentity = SameIdentity(other, probedIdentity);
            /* #6340 round 2 (N1): a PostgreSQL probe that read no identity (the login may not call pg_control_system()) cannot say
               whether it reached the cluster a covered PostgreSQL registration holds under another name, so it is refused rather than
               matched by host text alone. Only for an entry that moves an address (an add, or an edit of the address). */
            var identityUnread = postgresIdentityUnread && textMatch && IsPostgresIdentity(other.Identity);
            if (!sameText && !replicaHeld && !sameIdentity && !identityUnread)
            {
                continue;
            }

            if (await scopedRoute(other.ServerId, cancellationToken) is not { } routeId)
            {
                continue;
            }

            if (!sameText && !replicaHeld && !sameIdentity)
            {
                return (forEdit ? "Not saved: " : "Not added: ") + $"{entry.ProbeConfig.Host} cannot be checked against '{other.Label}'"
                    + (other.Removed ? " (a removed registration)" : "")
                    + $": the login this call connected with may not read the server's identity (pg_control_system()), and route {routeId} (an agent setting) "
                    + "is scoped to that PostgreSQL registration, so "
                    + (forEdit ? "this registration, moved there, " : "a second registration of the same cluster ")
                    + "could sit outside that route. Use a login that may call pg_control_system() (PostgreSQL grants it to PUBLIC unless it was revoked: "
                    + "GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system() TO the login), or change this in the Viewer or on the web dashboard.";
            }

            var named = replicaHeld ? QueryStoreReplicaSetting.Normalize(entry.ProbeConfig.QueryStoreReplica)! : entry.ProbeConfig.Host;
            return (forEdit ? "Not saved: " : "Not added: ") + $"{named} is already registered as '{other.Label}'"
                + (other.Removed ? " (a removed registration)" : "")
                + (sameText || replicaHeld ? "" : " (the server reports the same name for itself)")
                + $", and route {routeId} (an agent setting) is scoped to it, so "
                + (forEdit ? "this registration, moved there, would sit outside that route. " : "a second registration of the machine would sit outside that route. ")
                + (forEdit ? "Where a registration connects is changed in the Viewer or on the web dashboard." : "Register another database or intent in the Viewer or on the web dashboard.");
        }

        return null;
    }

    private const string RegisteredTargetsSql =
        "SELECT host, database, read_only_intent, engine, port, name, server_id FROM config_monitored_servers";

    /// <summary>The identity each registration, live or removed, reported on its last connect (#6285, #6339), from the registry column
    /// the connect writes (V212). Not from <c>collect.server_properties</c>: that table's <c>server_name</c> is the registration's
    /// storage name, not what the server reports, and a retention purge deletes its rows. A registration that never connected since
    /// the column arrived, or one that reported none, is not in the answer and is matched by its text alone.</summary>
    private const string CollectedIdentitiesSql =
        "SELECT server_id, machine_identity FROM collect.servers WHERE machine_identity IS NOT NULL";

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

        var identities = new Dictionary<int, string>();
        await using (var command = new NpgsqlCommand(CollectedIdentitiesSql, connection) { Transaction = transaction })
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (MachineIdentityOf(reader.GetString(1), 0) is { } identity)
                {
                    identities[reader.GetInt32(0)] = identity;
                }
            }
        }

        for (var i = 0; i < targets.Count; i++)
        {
            if (identities.TryGetValue(targets[i].ServerId, out var collected))
            {
                targets[i] = targets[i] with { Identity = collected };
            }
        }

        return targets;
    }

    /// <summary>The same rule as <see cref="TargetHeldRefusalAsync"/>, read inside the write's own transaction, after the
    /// identity lock: a route that was set up while the probe ran is seen here, and nothing is written. Null when nothing
    /// holds the machine. <paramref name="exceptServerId"/> is the registration an edit changes.</summary>
    internal static async Task<string?> TargetHeldRefusalInTransactionAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, ParsedServerEntry entry, int? exceptServerId, CancellationToken cancellationToken, bool forEdit = false, string? probedIdentity = null,
        bool textMatch = true, bool postgresIdentityUnread = false, bool checkReplica = true)
    {
        var held = await LoadRegisteredTargetsAsync(connection, transaction, cancellationToken);
        return await TargetHeldRefusalAsync(held, entry,
            (serverId, token) => ScopedAgentRouteAsync(connection, transaction, serverId, token), cancellationToken, exceptServerId, forEdit, probedIdentity, textMatch, postgresIdentityUnread, checkReplica);
    }
}
