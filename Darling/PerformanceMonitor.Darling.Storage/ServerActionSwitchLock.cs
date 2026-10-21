/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The lock between an agent's server action and whoever changes that server's switch for that action (#5632).
/// <c>config.config_server_actions</c> is SELECT-only for every role but the store owner, and PostgreSQL will not lock a row for a role
/// that cannot update it, so the lock is an advisory one on a key for (server, action), in the style of the store's other advisory locks
/// (<c>hashtextextended(..., 5632)</c>).
/// <para>An agent's write takes the <b>shared</b> form in its own transaction, before it re-reads the switch, and holds it to the end of
/// the transaction, which is the end of the write. A switch writer must take the <b>exclusive</b> form (<see cref="AcquireExclusiveAsync"/>)
/// in the transaction that changes the row. Then a switch turned off waits for the writes in flight, and a write that starts after it sees
/// the committed switch. Many agent writes share the lock and never wait on each other.</para>
/// </summary>
public static class ServerActionSwitchLock
{
    private const string KeyExpression = "hashtextextended('server_action:' || $1::text || ':' || $2, 5632)";

    /// <summary>What an agent write runs: <c>pg_advisory_xact_lock_shared</c> on the key for (server, action).</summary>
    public const string SharedSql = "SELECT pg_advisory_xact_lock_shared(" + KeyExpression + ")";

    /// <summary>What a switch writer runs before it changes a switch row: <c>pg_advisory_xact_lock</c> on the same key.</summary>
    public const string ExclusiveSql = "SELECT pg_advisory_xact_lock(" + KeyExpression + ")";

    /// <summary>Takes the shared lock for <paramref name="serverAction"/> on <paramref name="serverId"/>, held until <paramref name="transaction"/> ends.</summary>
    public static Task AcquireSharedAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, string serverAction,
        int commandTimeoutSeconds, CancellationToken cancellationToken) =>
        RunAsync(SharedSql, connection, transaction, serverId, serverAction, commandTimeoutSeconds, cancellationToken);

    /// <summary>Takes the exclusive lock, which waits for every agent write in flight on that (server, action) and holds new ones back until
    /// <paramref name="transaction"/> ends. A switch writer calls it in the same transaction as its write of the switch row.</summary>
    public static Task AcquireExclusiveAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId, string serverAction,
        int commandTimeoutSeconds, CancellationToken cancellationToken) =>
        RunAsync(ExclusiveSql, connection, transaction, serverId, serverAction, commandTimeoutSeconds, cancellationToken);

    private static async Task RunAsync(string sql, NpgsqlConnection connection, NpgsqlTransaction transaction, int serverId,
        string serverAction, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serverAction, NpgsqlDbType = NpgsqlDbType.Text });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
