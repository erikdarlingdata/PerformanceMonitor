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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One object a rung of one start created, changed or dropped, as <c>collect.service_start_rung_objects</c> keeps it (#9438).</summary>
/// <param name="StartId">The start whose ladder applied the rung.</param>
/// <param name="RungVersion">The rung. 0 is the marker a start writes when its ladder began on an empty store.</param>
/// <param name="Attempt">The start's try at the ladder that applied the rung, as the rung row's attempt.</param>
/// <param name="Ordinal">The row's place in the rung's list, from 0.</param>
/// <param name="Schema">The object's schema.</param>
/// <param name="ObjectName">The object's name.</param>
/// <param name="Column">The column, for a column row.</param>
/// <param name="Kind">A <see cref="MigrationObjectKind"/> word.</param>
/// <param name="Action">A <see cref="MigrationObjectAction"/> word.</param>
public sealed record ServiceStartRungObject(
    Guid StartId, int RungVersion, int Attempt, int Ordinal, string Schema, string ObjectName, string? Column, string Kind, string Action);

/// <summary>One start's objects for one rung, as <see cref="ServiceStartRungObjects.ReadRungAsync"/> returns them.</summary>
public sealed record ServiceStartRungObjectList(
    Guid StartId, string ServiceHost, string ProductVersion, DateTime StartedUtc, string RungName, int Attempt, IReadOnlyList<ServiceStartRungObject> Objects);

/// <summary>
/// What each migration rung created, changed or dropped (V311, #9438): one row per object in <c>collect.service_start_rung_objects</c>, a child of the
/// rung row in <c>collect.service_start_rungs</c> (#9415) keyed by start, rung version and attempt. The runner takes a catalog snapshot before and after
/// each rung and diffs them (<see cref="MigrationCatalogSnapshot"/>), and the service writes the rows with the rung rows once the tables exist.
///
/// <para><b>A store with no version when the ladder starts records no object rows.</b> A ladder from version 0 runs every rung, and listing every object
/// as created tells no one anything. The start writes one marker row instead (rung 0, kind <c>ladder</c>, action <c>fresh_install</c>), and the read
/// shows it as <c>objects: fresh_install</c> on each of that start's rung rows.</para>
///
/// <para><b>Retention.</b> The rows go with their start, in <see cref="ServiceStartHistory.RetentionSql"/>: <see cref="ServiceStartHistory.RetentionDays"/> days
/// after the start's last flush, the same as the rung rows.</para>
/// </summary>
public static class ServiceStartRungObjects
{
    /// <summary>The migration that creates the table. One constant, so a renumber is one edit.</summary>
    public const int Version = 311;

    /// <summary>The rung number of the fresh-install marker row. No real rung has it.</summary>
    public const int MarkerRung = 0;

    /// <summary>The table, created empty, and the index the by-rung read uses. Idempotent, and no data moves.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.service_start_rung_objects
(
    start_id uuid NOT NULL,
    rung_version integer NOT NULL,
    attempt integer NOT NULL,
    ordinal integer NOT NULL,
    schema_name text NOT NULL,
    object_name text NOT NULL,
    column_name text,
    object_kind text NOT NULL,
    action text NOT NULL,
    CONSTRAINT pk_service_start_rung_objects PRIMARY KEY (start_id, rung_version, attempt, ordinal)
);

CREATE INDEX IF NOT EXISTS idx_service_start_rung_objects_rung
    ON collect.service_start_rung_objects(rung_version, start_id);";

    /// <summary>One object row. A row written twice (a flush that failed after the insert) is kept as it was. $1 id, $2 rung, $3 attempt, $4 ordinal, $5 schema, $6 object, $7 column, $8 kind, $9 action.</summary>
    public const string InsertSql = @"
INSERT INTO collect.service_start_rung_objects (start_id, rung_version, attempt, ordinal, schema_name, object_name, column_name, object_kind, action)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (start_id, rung_version, attempt, ordinal) DO NOTHING;";

    /// <summary>
    /// The newest starts that have object rows for rung $1, with those rows. $2 host filter (null for every host), $3 earliest process start (naive UTC, null for no
    /// bound), $4 how many starts. Newest start first, and each start's rows in the order the rung listed them.
    /// </summary>
    public const string SelectRungSql = @"
WITH picked AS
(
    SELECT s.start_id, s.service_host, s.product_version, s.process_started_at
    FROM collect.service_starts AS s
    WHERE EXISTS (SELECT 1 FROM collect.service_start_rung_objects AS e WHERE e.rung_version = $1 AND e.start_id = s.start_id)
    AND   ($2::text IS NULL OR s.service_host = $2::text)
    AND   ($3::timestamp IS NULL OR s.process_started_at >= $3::timestamp)
    ORDER BY s.process_started_at DESC, s.start_id
    LIMIT $4
)
SELECT p.start_id, p.service_host, p.product_version, p.process_started_at, coalesce(r.rung_name, ''), o.attempt, o.ordinal,
       o.schema_name, o.object_name, o.column_name, o.object_kind, o.action
FROM picked AS p
JOIN collect.service_start_rung_objects AS o ON o.start_id = p.start_id AND o.rung_version = $1
LEFT JOIN collect.service_start_rungs AS r ON r.start_id = o.start_id AND r.rung_version = o.rung_version AND r.attempt = o.attempt
ORDER BY p.process_started_at DESC, p.start_id, o.attempt, o.ordinal;";

    /// <summary>Writes object rows. Call after the start row and its rung rows are written.</summary>
    public static async Task InsertAsync(
        NpgsqlDataSource dataSource, IReadOnlyList<ServiceStartRungObject> rows, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(rows);

        foreach (var row in rows)
        {
            await using var command = dataSource.CreateCommand(InsertSql);
            command.CommandTimeout = 30;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Uuid, Value = row.StartId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = row.RungVersion });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = row.Attempt });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = row.Ordinal });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = row.Schema });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = row.ObjectName });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)row.Column ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = row.Kind });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = row.Action });
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// The rows of <paramref name="rungVersion"/> for the newest <paramref name="limit"/> starts that have any, newest first. <paramref name="sinceUtc"/> keeps the
    /// starts that began at or after it, and <paramref name="serviceHost"/> one host's starts. A store without the tables throws <see cref="PostgresException"/> with
    /// <see cref="PostgresErrorCodes.UndefinedTable"/>, so the caller can say why.
    /// </summary>
    public static async Task<IReadOnlyList<ServiceStartRungObjectList>> ReadRungAsync(
        NpgsqlDataSource dataSource, int rungVersion, int limit, string? serviceHost, DateTime? sinceUtc, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        var lists = new List<(Guid StartId, string Host, string Build, DateTime StartedUtc, string RungName, int Attempt, List<ServiceStartRungObject> Rows)>();
        await using var command = dataSource.CreateCommand(SelectRungSql);
        command.CommandTimeout = 30;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = rungVersion });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)serviceHost ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = sinceUtc is { } since ? ServiceStartHistory.Naive(DateTime.SpecifyKind(since, DateTimeKind.Utc)) : DBNull.Value,
        });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var startId = reader.GetGuid(0);
            var attempt = reader.GetInt32(5);
            if (lists.Count == 0 || lists[^1].StartId != startId || lists[^1].Attempt != attempt)
            {
                lists.Add((startId, reader.GetString(1), reader.GetString(2),
                    DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc), reader.GetString(4), attempt, new List<ServiceStartRungObject>()));
            }

            lists[^1].Rows.Add(new ServiceStartRungObject(
                startId, rungVersion, attempt, reader.GetInt32(6), reader.GetString(7), reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetString(9), reader.GetString(10), reader.GetString(11)));
        }

        return lists
            .Select(l => new ServiceStartRungObjectList(l.StartId, l.Host, l.Build, l.StartedUtc, l.RungName, l.Attempt, l.Rows))
            .ToList();
    }
}
