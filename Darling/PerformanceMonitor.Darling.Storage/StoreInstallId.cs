/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// <para><b>The binding.</b> Beside the id the row keeps the store database's OID, the OID of this table, and the
/// cluster's <c>system_identifier</c>. The two OIDs say which store this is: a physical copy keeps both, and so does
/// <c>pg_upgrade</c>, which the managed store goes through at each major upgrade (see <see cref="IsSameStore"/>); a
/// store made again by dump and restore, or by another install's migrations, does not. The cluster id is not part of
/// that answer, because <c>pg_upgrade</c> makes a new one: a changed cluster id is rebound on the row and the id stays.
/// None of the three is a host name (a recreated container gets a new host name and is still the same store). When the
/// stored binding is not this store's, or the stored id is not a valid id, the id is replaced and one Warning names
/// both ids and which binding differed. The old id's sessions are left alone, because the install it came from may
/// still run.</para>
///
/// <para><b>A server that refuses the cluster id.</b> A login without superuser rights may call
/// <c>pg_control_system()</c> by default, but a managed or hardened server can revoke it, and the service has to start
/// there. A refusal (and only a refusal) of that read leaves the cluster id out of the binding: the row is stored with
/// a NULL cluster id and one Information line says so. Every login can read the two OIDs. A row made before the table's
/// OID was kept (it has none yet) is compared as it was then, by the database's OID and by the cluster id when the row
/// and the current read both have one, and its table OID is filled in on the next start. So a grant that changes
/// later, in either direction, never makes a new id on its own.</para>
///
/// <para><b>Safe to run twice at once.</b> Two starts on one store (a restart overlapping its predecessor, two
/// containers on one database) insert with <c>ON CONFLICT DO NOTHING</c> and read the row back, so they end on the
/// same id. A replacement is an UPDATE guarded by the id it read, so exactly one of the starts that saw the same bad
/// row makes the new id and logs the Warning; the others read the row it made. A rebind of a changed or missing
/// binding value is guarded the same way, and by the value still being stale, so one of the starts logs it.</para>
/// </summary>
public static class StoreInstallId
{
    /// <summary>The store this connection is on: the cluster's <c>system_identifier</c>, the database's OID and the OID
    /// of the install id table. A login without superuser rights may call <c>pg_control_system()</c> by default, and a
    /// server that revokes it refuses with SQLSTATE 42501, which <see cref="EnsureAsync"/> answers with
    /// <see cref="DatabaseOidSql"/>.</summary>
    public const string BindingSql = @"
SELECT s.system_identifier, d.oid::bigint, 'config.config_install_id'::regclass::oid::bigint
FROM pg_control_system() AS s
CROSS JOIN pg_database AS d
WHERE d.datname = current_database()";

    /// <summary>The two OIDs alone, for a store whose server refuses this login the cluster id. Every login can read
    /// them, so a refusal of this statement is not answered with a fallback.</summary>
    public const string DatabaseOidSql = @"
SELECT d.oid::bigint, 'config.config_install_id'::regclass::oid::bigint
FROM pg_database AS d
WHERE d.datname = current_database()";

    /// <summary>The row, if there is one. The only statement a reader runs, so it names no column a store older than
    /// the migration that adds <c>table_oid</c> lacks: the CLI can read a store the upgraded service has not migrated
    /// yet.</summary>
    public const string ReadSql = @"
SELECT install_id, system_identifier, database_oid
FROM config.config_install_id
WHERE id = 1";

    /// <summary>The row with its table OID, for <see cref="EnsureAsync"/> alone: it runs after the migrations, so the
    /// column is there.</summary>
    public const string ReadWithTableOidSql = @"
SELECT install_id, system_identifier, database_oid, table_oid
FROM config.config_install_id
WHERE id = 1";

    /// <summary>Makes the row if no start has yet; a start that loses the race changes nothing.</summary>
    public const string InsertSql = @"
INSERT INTO config.config_install_id (id, install_id, system_identifier, database_oid, table_oid)
VALUES (1, $1, $2, $3, $4)
ON CONFLICT (id) DO NOTHING";

    /// <summary>Replaces the id and its binding, but only while the row still holds the id this start read, so of
    /// several starts that saw the same row exactly one replaces it.</summary>
    public const string ReplaceSql = @"
UPDATE config.config_install_id
SET install_id = $1, system_identifier = $2, database_oid = $3, table_oid = $4, created_at = (now() AT TIME ZONE 'UTC')
WHERE id = 1 AND install_id = $5";

    /// <summary>Writes each binding value of a row that is this store's and is missing or changed, and keeps the id:
    /// the table OID when the row has none (a row made before it was kept), and the cluster id when the current one is
    /// known and differs (a stored NULL included, a refusal that has lifted). Guarded by the id this start read and by
    /// the value still being stale, so of several starts that saw the same row exactly one writes it and logs.</summary>
    public const string RebindSql = @"
UPDATE config.config_install_id
SET table_oid = COALESCE(table_oid, $1), system_identifier = COALESCE($2::bigint, system_identifier)
WHERE id = 1 AND install_id = $3
  AND (table_oid IS NULL OR ($2::bigint IS NOT NULL AND system_identifier IS DISTINCT FROM $2::bigint))";

    /// <summary>The deadline on every statement here, in seconds. Each touches one row of a table that holds one row, so
    /// a statement that takes longer than this is waiting on something (a peer holding the row, a store that has
    /// stopped answering), and the start's own retry is the better place to wait than the statement.</summary>
    public const int CommandTimeoutSeconds = 30;

    /// <summary>How many times a start re-reads before it gives up: each pass either finds a usable row or watches
    /// another start make one, so more than a couple means the row is being changed under it without end.</summary>
    private const int MaxAttempts = 5;

    /// <summary>What this connection's store is. The cluster id is null when the server refused it (see
    /// <see cref="DatabaseOidSql"/>); the two OIDs are always known.</summary>
    private readonly record struct Binding(long? SystemIdentifier, long DatabaseOid, long TableOid);

    /// <summary>What the row holds. The cluster id is null when the start that made the row could not read it; the table
    /// OID is null on a row made before it was kept, and on a row a reader read (it does not select it).</summary>
    private readonly record struct Row(string InstallId, long? SystemIdentifier, long DatabaseOid, long? TableOid);

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

            var row = await ReadRowAsync(connection, ReadWithTableOidSql, cancellationToken);
            if (row is not { } stored)
            {
                continue;   // the row went between the insert and the read: make it again
            }

            var validId = InstallId.IsValid(stored.InstallId);
            var sameStore = IsSameStore(
                stored.SystemIdentifier, stored.DatabaseOid, stored.TableOid,
                binding.SystemIdentifier, binding.DatabaseOid, binding.TableOid);
            if (validId && sameStore)
            {
                if (!NeedsRebind(stored, binding))
                {
                    return stored.InstallId;
                }

                if (await RebindAsync(connection, stored.InstallId, binding, cancellationToken))
                {
                    if (stored.SystemIdentifier is { } storedCluster && binding.SystemIdentifier is { } currentCluster && storedCluster != currentCluster)
                    {
                        logger.LogInformation(
                            "The cluster's identifier changed from {StoredClusterId} to {CurrentClusterId} since the install id " +
                            "'{InstallId}' was made. The store is the same one (its database and the install id table keep their OIDs " +
                            "through a major upgrade), so the id stays and the row now carries the new cluster id.",
                            storedCluster, currentCluster, stored.InstallId);
                    }

                    return stored.InstallId;
                }

                continue;   // another start changed the row between this read and the update: read it again
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
    /// id reads like any other, and a store still below the migration that adds <c>table_oid</c> reads like any other
    /// too, because <see cref="ReadSql"/> names no column that store lacks.
    /// </summary>
    public static async Task<string?> TryReadAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        try
        {
            var row = await ReadRowAsync(connection, ReadSql, cancellationToken);
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

    /// <summary>
    /// Whether a stored row belongs to this store. The database's OID always counts. When the row has its table's OID
    /// (every row made since the migration that keeps it) that decides the rest and the cluster id does not count, because
    /// it is the one binding a major upgrade changes. A row that has no table OID yet is compared as it was before: by the
    /// cluster id too, when the row and the current read both have one, so a grant that changes later (a refusal lifted,
    /// or put in place) never makes a new id on its own.
    ///
    /// <para><b>Why the two OIDs survive what the cluster id does not.</b> <c>pg_upgrade</c> gives the new cluster a new
    /// <c>system_identifier</c> but keeps the database's OID (PostgreSQL 15 release notes, section pg_upgrade: "Make
    /// pg_upgrade preserve tablespace and database OIDs, as well as relation relfilenode numbers"; pg_dump's
    /// <c>--binary-upgrade</c> output creates the database with <c>CREATE DATABASE ... WITH TEMPLATE = template0 OID = ...</c>)
    /// and the table's (the same output calls <c>binary_upgrade_set_next_heap_pg_class_oid</c> before each table, under the
    /// comment "For binary upgrade, must preserve pg_class oids and relfilenodes": src/bin/pg_dump/pg_dump.c,
    /// <c>binary_upgrade_set_pg_class_oids</c>). A dump and restore without that mode, which is how a clone or a move by
    /// logical copy is made, takes new OIDs from the new cluster's counters.</para>
    /// </summary>
    public static bool IsSameStore(
        long? storedClusterId, long storedDatabaseOid, long? storedTableOid,
        long? currentClusterId, long currentDatabaseOid, long currentTableOid)
    {
        if (storedDatabaseOid != currentDatabaseOid)
        {
            return false;
        }

        if (storedTableOid is { } storedTable)
        {
            return storedTable == currentTableOid;
        }

        return storedClusterId is not { } storedCluster
            || currentClusterId is not { } currentCluster
            || storedCluster == currentCluster;
    }

    /// <summary>Whether a row that is this store's still has a binding value to write: its table OID when it has none,
    /// or the cluster id when the current one is known and the row's differs (or is missing).</summary>
    private static bool NeedsRebind(Row stored, Binding binding)
    {
        return stored.TableOid is null
            || (binding.SystemIdentifier is { } current && stored.SystemIdentifier != current);
    }

    private static string DescribeWhy(bool validId, bool sameStore, Row stored, Binding binding)
    {
        var notAnId = "it is not eight lowercase hex digits";
        if (validId)
        {
            return "it was made for a different store (" + DescribeDifference(stored, binding) + ")";
        }

        return sameStore
            ? notAnId
            : notAnId + " and it was made for a different store (" + DescribeDifference(stored, binding) + ")";
    }

    /// <summary>Which binding differed, as the Warning names it: each OID that changed, with the value in this store and
    /// the value in the row. The cluster id is named only for a row that has no table OID yet, the one case in which it
    /// decides.</summary>
    private static string DescribeDifference(Row stored, Binding binding)
    {
        var differences = new List<string>();
        if (stored.DatabaseOid != binding.DatabaseOid)
        {
            differences.Add($"the database OID is {binding.DatabaseOid} here and {stored.DatabaseOid} in the row");
        }

        if (stored.TableOid is { } storedTable && storedTable != binding.TableOid)
        {
            differences.Add($"the install id table's OID is {binding.TableOid} here and {storedTable} in the row");
        }

        if (stored.TableOid is null && stored.SystemIdentifier is { } storedCluster
            && binding.SystemIdentifier is { } currentCluster && storedCluster != currentCluster)
        {
            differences.Add($"the cluster id is {currentCluster} here and {storedCluster} in the row");
        }

        return string.Join("; ", differences);
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
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            throw new InvalidOperationException("The store's database could not be identified.");
        }

        logger.LogInformation(
            "The install id is bound to the store's database alone, because the cluster's identifier can't be read with this login.");
        return new Binding(null, reader.GetInt64(0), reader.GetInt64(1));
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

            return new Binding(reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2));
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            return null;
        }
    }

    private static async Task<Row?> ReadRowAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = CommandTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var tableOid = reader.FieldCount > 3 && !reader.IsDBNull(3) ? reader.GetInt64(3) : (long?)null;
        return new Row(reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), reader.GetInt64(2), tableOid);
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
        command.Parameters.Add(new NpgsqlParameter { Value = binding.TableOid });
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
        command.Parameters.Add(new NpgsqlParameter { Value = binding.TableOid });
        command.Parameters.Add(new NpgsqlParameter { Value = oldInstallId });
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    /// <summary>True when this call wrote the row's missing or changed binding values, false when another start already
    /// had (or replaced the id), in which case the caller reads the row again.</summary>
    private static async Task<bool> RebindAsync(NpgsqlConnection connection, string installId, Binding binding, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(RebindSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { Value = binding.TableOid });
        command.Parameters.Add(ClusterIdParameter(binding));
        command.Parameters.Add(new NpgsqlParameter { Value = installId });
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }
}
