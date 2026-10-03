/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The install id's row in the store (#4961): the eight characters that tell this install's Extended Events sessions
/// from another install's on a server both monitor. <see cref="InstallId"/> owns the format and makes new ids; this
/// class owns where the id lives and the one rule about when it changes.
///
/// <para><b>One maker, any number of readers.</b> <see cref="EnsureAsync"/> is the service's, called once at start,
/// after the store's migrations and before any worker. <see cref="TryReadAsync(NpgsqlConnection, CancellationToken)"/>
/// is for everything else (the CLI, the Viewer) and writes nothing: a reader that made an id would make one for a
/// store nobody started.</para>
///
/// <para><b>The binding.</b> Beside the id the row keeps the cluster's <c>system_identifier</c> and the store
/// database's OID. They are what a physical copy of this store keeps and a different store does not, and neither is a
/// host name (a recreated container gets a new host name and is still the same store). When the stored binding is
/// not this store's, or the stored id is not a valid id, the id is replaced and one Warning names both. The old id's
/// sessions are left alone, because the install it came from may still run.</para>
///
/// <para><b>A server that refuses the cluster id.</b> A login without superuser rights may call
/// <c>pg_control_system()</c> by default, but a managed or hardened server can revoke it, and the service has to start
/// there. A refusal (and only a refusal) of that read makes the binding the database's OID alone: the row is stored
/// with a NULL cluster id and one Information line says so. The OID always counts when a stored row is compared with
/// the store; the cluster id counts only when the stored row and the current read both have one. So a grant that
/// changes later, in either direction, never makes a new id on its own.</para>
///
/// <para><b>Safe to run twice at once.</b> Two starts on one store (a restart overlapping its predecessor, two
/// containers on one database) insert with <c>ON CONFLICT DO NOTHING</c> and read the row back, so they end on the
/// same id. A replacement is an UPDATE guarded by the id it read, so exactly one of the starts that saw the same bad
/// row makes the new id and logs the Warning; the others read the row it made.</para>
/// </summary>
public static class StoreInstallId
{
    /// <summary>The store this connection is on: the cluster's <c>system_identifier</c> and the database's OID.
    /// A login without superuser rights may call <c>pg_control_system()</c> by default, and a server that revokes it
    /// refuses with SQLSTATE 42501, which <see cref="EnsureAsync"/> answers with <see cref="DatabaseOidSql"/>.</summary>
    public const string BindingSql = @"
SELECT s.system_identifier, d.oid::bigint
FROM pg_control_system() AS s
CROSS JOIN pg_database AS d
WHERE d.datname = current_database()";

    /// <summary>The database's OID alone, for a store whose server refuses this login the cluster id. Every login can
    /// read it, so a refusal of this statement is not answered with a fallback.</summary>
    public const string DatabaseOidSql = @"
SELECT d.oid::bigint
FROM pg_database AS d
WHERE d.datname = current_database()";

    /// <summary>The row, if there is one. The only statement a reader runs.</summary>
    public const string ReadSql = @"
SELECT install_id, system_identifier, database_oid
FROM config.config_install_id
WHERE id = 1";

    /// <summary>Makes the row if no start has yet; a start that loses the race changes nothing.</summary>
    public const string InsertSql = @"
INSERT INTO config.config_install_id (id, install_id, system_identifier, database_oid)
VALUES (1, $1, $2, $3)
ON CONFLICT (id) DO NOTHING";

    /// <summary>Replaces the id and its binding, but only while the row still holds the id this start read, so of
    /// several starts that saw the same row exactly one replaces it.</summary>
    public const string ReplaceSql = @"
UPDATE config.config_install_id
SET install_id = $1, system_identifier = $2, database_oid = $3, created_at = (now() AT TIME ZONE 'UTC')
WHERE id = 1 AND install_id = $4";

    /// <summary>The deadline on every statement here, in seconds. Each touches one row of a table that holds one row, so
    /// a statement that takes longer than this is waiting on something (a peer holding the row, a store that has
    /// stopped answering), and the start's own retry is the better place to wait than the statement.</summary>
    public const int CommandTimeoutSeconds = 30;

    /// <summary>How many times a start re-reads before it gives up: each pass either finds a usable row or watches
    /// another start make one, so more than a couple means the row is being changed under it without end.</summary>
    private const int MaxAttempts = 5;

    /// <summary>What this connection's store is. The cluster id is null when the server refused it (see
    /// <see cref="DatabaseOidSql"/>); the OID is always known.</summary>
    private readonly record struct Binding(long? SystemIdentifier, long DatabaseOid);

    /// <summary>What the row holds. The cluster id is null when the start that made the row could not read it.</summary>
    private readonly record struct Row(string InstallId, long? SystemIdentifier, long DatabaseOid);

    /// <summary>
    /// The service's: returns this store's install id, making the row when it is absent and replacing the id when it
    /// does not belong to this store. Call it once at start, after the migrations and before any worker.
    /// </summary>
    public static async Task<string> EnsureAsync(NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(logger);

        var binding = await ReadBindingAsync(connection, logger, cancellationToken);

        for (var attempt = 0; attempt < MaxAttempts; attempt++)
        {
            await InsertIfAbsentAsync(connection, InstallId.NewId(), binding, cancellationToken);

            var row = await ReadRowAsync(connection, cancellationToken);
            if (row is not { } stored)
            {
                continue;   // the row went between the insert and the read: make it again
            }

            var validId = InstallId.IsValid(stored.InstallId);
            var sameStore = IsSameStore(stored, binding);
            if (validId && sameStore)
            {
                return stored.InstallId;
            }

            var replacement = InstallId.NewId();
            if (await ReplaceAsync(connection, stored.InstallId, replacement, binding, cancellationToken))
            {
                logger.LogWarning(
                    "The install id stored for this store, '{OldInstallId}', cannot be kept: {Reason}. This install made a new id, " +
                    "'{NewInstallId}'. Extended Events sessions named for the old id are left alone, because the install that made " +
                    "them may still be running.",
                    stored.InstallId, DescribeWhy(validId, sameStore, stored, binding), replacement);
            }

            /* Read again either way: the row now holds ours when this start won the replacement and the winner's when
               another start did, and either is what every start on this store must agree on. */
        }

        throw new InvalidOperationException(
            "The install id row could not be settled: it kept changing while this start was reading it.");
    }

    /// <summary>
    /// For the CLI and the Viewer: the stored id when the row exists and holds a valid id, otherwise null (no row yet,
    /// no table yet on a store older than the migration that makes it, or a stored value that is not an id). It never
    /// inserts, updates or repairs: the service is the only thing that makes the row. It does not read the store's
    /// binding at all, so a server that refuses the cluster id changes nothing for it; a row stored without a cluster
    /// id reads like any other.
    /// </summary>
    public static async Task<string?> TryReadAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        try
        {
            var row = await ReadRowAsync(connection, cancellationToken);
            return row is { } found && InstallId.IsValid(found.InstallId) ? found.InstallId : null;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return null;
        }
    }

    /// <summary><see cref="TryReadAsync(NpgsqlConnection, CancellationToken)"/> on a connection of the data source's.</summary>
    public static async Task<string?> TryReadAsync(NpgsqlDataSource dataSource, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        await using var connection = await dataSource.OpenConnectionAsync(cancellationToken);
        return await TryReadAsync(connection, cancellationToken);
    }

    /// <summary>Whether a stored row belongs to this store. The database's OID always counts. The cluster id counts
    /// only when the stored row and the current read both have one, so a grant that changes later (a refusal lifted,
    /// or put in place) never makes a new id on its own.</summary>
    private static bool IsSameStore(Row stored, Binding binding)
    {
        if (stored.DatabaseOid != binding.DatabaseOid)
        {
            return false;
        }

        return stored.SystemIdentifier is not { } storedCluster
            || binding.SystemIdentifier is not { } currentCluster
            || storedCluster == currentCluster;
    }

    private static string DescribeWhy(bool validId, bool sameStore, Row stored, Binding binding)
    {
        var notAnId = "it is not eight lowercase hex digits";
        var otherStore =
            $"it was made for a different store ({Place(stored.SystemIdentifier, stored.DatabaseOid)}; " +
            $"this store is {Place(binding.SystemIdentifier, binding.DatabaseOid)})";
        return !validId && !sameStore ? notAnId + " and " + otherStore : !validId ? notAnId : otherStore;
    }

    /// <summary>A store as the Warning names it: the cluster too when it was read.</summary>
    private static string Place(long? systemIdentifier, long databaseOid)
    {
        return systemIdentifier is { } cluster ? $"cluster {cluster}, database OID {databaseOid}" : $"database OID {databaseOid}";
    }

    /// <summary>Reads the cluster and the database. A server that refuses this login the cluster id (SQLSTATE 42501,
    /// insufficient_privilege, and only that) is answered with the database's OID alone, and one Information line says
    /// so; any other failure is not a refusal and still fails the start, which the worker's start-up retry then takes
    /// over. The OID read sits outside the catch, so a failure of its own is never taken for a refusal.</summary>
    private static async Task<Binding> ReadBindingAsync(NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken)
    {
        if (await TryReadClusterAndDatabaseAsync(connection, cancellationToken) is { } binding)
        {
            return binding;
        }

        await using var command = new NpgsqlCommand(DatabaseOidSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        if (await command.ExecuteScalarAsync(cancellationToken) is not long databaseOid)
        {
            throw new InvalidOperationException("The store's database could not be identified.");
        }

        logger.LogInformation(
            "The install id is bound to the store's database alone, because the cluster's identifier can't be read with this login.");
        return new Binding(null, databaseOid);
    }

    /// <summary>The cluster and the database in one statement, or null when the server refused the cluster id.</summary>
    private static async Task<Binding?> TryReadClusterAndDatabaseAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = new NpgsqlCommand(BindingSql, connection) { CommandTimeout = CommandTimeoutSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                throw new InvalidOperationException("The store's cluster and database could not be identified.");
            }

            return new Binding(reader.GetInt64(0), reader.GetInt64(1));
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return null;
        }
    }

    private static async Task<Row?> ReadRowAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(ReadSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new Row(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), reader.GetInt64(2));
    }

    /// <summary>The cluster id as a statement parameter: typed, so a NULL (a refused read) reaches the server as a
    /// bigint NULL rather than a value whose type it has to guess.</summary>
    private static NpgsqlParameter ClusterIdParameter(Binding binding)
    {
        return new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = (object?)binding.SystemIdentifier ?? DBNull.Value };
    }

    private static async Task InsertIfAbsentAsync(NpgsqlConnection connection, string installId, Binding binding, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(InsertSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { Value = installId });
        command.Parameters.Add(ClusterIdParameter(binding));
        command.Parameters.Add(new NpgsqlParameter { Value = binding.DatabaseOid });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>True when this call changed the row, false when another start already had.</summary>
    private static async Task<bool> ReplaceAsync(
        NpgsqlConnection connection, string oldInstallId, string newInstallId, Binding binding, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(ReplaceSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { Value = newInstallId });
        command.Parameters.Add(ClusterIdParameter(binding));
        command.Parameters.Add(new NpgsqlParameter { Value = binding.DatabaseOid });
        command.Parameters.Add(new NpgsqlParameter { Value = oldInstallId });
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }
}
