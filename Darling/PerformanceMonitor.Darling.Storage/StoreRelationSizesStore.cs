/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableHealthReader;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One size pass of the store and the time it finished (naive UTC, from the clock the service runs on).</summary>
public sealed record MeasuredSizePass(RelationSizePass Pass, DateTime MeasuredAtUtc);

/// <summary>What one <see cref="StoreRelationSizesStore.WriteAsync"/> changed: relation rows deleted, and rows inserted or updated (rows already equal are not counted).</summary>
public readonly record struct StoreRelationSizesWrite(int Deleted, int Upserted);

/// <summary>
/// #9374: the newest measured size of every relation the Store Health reads size, held in the store so the Viewer's first open shows them at
/// once instead of waiting for a size pass of its own. On a store with thousands of chunks the size pass is most of the first open's wait, and
/// relation sizes change slowly.
///
/// <para><b>Shape (V301).</b> <c>collect.store_relation_sizes_latest</c> holds one row per relation: <c>rel_oid</c> (the key), the relation's schema and
/// name beside it, the oid of the heap an index hangs under (0 for a heap), the main-fork bytes, the other forks' bytes (indexes only) and the TOAST
/// total (heaps only). Those are the columns of <see cref="RelationSizePass"/>, which both presized reads are fed from, so a read fed the stored rows
/// runs the same statement a live pass would feed. <c>collect.store_relation_sizes_measured</c> holds one row: when the pass finished.</para>
///
/// <para><b>Key.</b> The relation oid, the key the presized reads join on. A dropped and recreated relation (a chunk, a table) is a new oid, so the
/// old size never attaches to the new relation, and the old row goes at the next write. The schema and name are for a person reading the table.</para>
///
/// <para><b>Write.</b> <see cref="WriteAsync"/> runs in one transaction: it deletes the rows of relations not in the pass (or no longer in
/// <c>pg_class</c>), then upserts the pass and updates a row only where a stored column differs. Between two passes a few dozen of about 25,000
/// relations change size, so a full rewrite would leave about 25,000 dead row versions on every write. The measured time is its own one-row table and
/// is written on every write: a per-row stamp would either go stale on the rows the upsert leaves alone or bring the dead rows back.</para>
///
/// <para><b>Retention.</b> None: the table is the newest pass and nothing else, kept in step by the delete above.</para>
/// </summary>
public static class StoreRelationSizesStore
{
    /// <summary>The store schema version that adds the two tables.</summary>
    public const int Version = 301;

    /// <summary>
    /// The rung (V301): two empty plain tables, no data moves. The latest table is <c>fillfactor = 80</c> so the size columns, which no index
    /// covers, update in place (a HOT update) on the free space each page keeps, and a pass that changes a few dozen rows does not grow the
    /// primary key's index. An older build never reads them.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.store_relation_sizes_latest
(
    rel_oid bigint NOT NULL,
    schema_name text NOT NULL,
    rel_name text NOT NULL,
    parent_oid bigint NOT NULL,
    main_bytes bigint NOT NULL,
    other_forks_bytes bigint NOT NULL,
    toast_bytes bigint NOT NULL,
    CONSTRAINT pk_store_relation_sizes_latest PRIMARY KEY (rel_oid)
)
WITH (fillfactor = 80);

CREATE TABLE IF NOT EXISTS collect.store_relation_sizes_measured
(
    only_row boolean NOT NULL DEFAULT true,
    measured_at timestamp(6) without time zone NOT NULL,
    CONSTRAINT pk_store_relation_sizes_measured PRIMARY KEY (only_row),
    CONSTRAINT ck_store_relation_sizes_measured_one_row CHECK (only_row)
);";

    private const string DeleteGoneSql = @"
DELETE FROM collect.store_relation_sizes_latest AS l
WHERE NOT (l.rel_oid = ANY($1::bigint[]))
   OR NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class AS c WHERE c.oid = l.rel_oid::oid)";

    private const string UpsertSql = @"
INSERT INTO collect.store_relation_sizes_latest AS l
    (rel_oid, schema_name, rel_name, parent_oid, main_bytes, other_forks_bytes, toast_bytes)
SELECT s.rel_oid, n.nspname, c.relname, s.parent_oid, s.main_bytes, s.other_forks_bytes, s.toast_bytes
FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[])
        AS s(rel_oid, parent_oid, main_bytes, other_forks_bytes, toast_bytes)
JOIN pg_catalog.pg_class AS c ON c.oid = s.rel_oid::oid
JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
LEFT JOIN collect.store_relation_sizes_latest AS o ON o.rel_oid = s.rel_oid
WHERE o.rel_oid IS NULL
   OR (o.schema_name, o.rel_name, o.parent_oid, o.main_bytes, o.other_forks_bytes, o.toast_bytes)
      IS DISTINCT FROM (n.nspname, c.relname, s.parent_oid, s.main_bytes, s.other_forks_bytes, s.toast_bytes)
ON CONFLICT (rel_oid) DO UPDATE
SET schema_name = EXCLUDED.schema_name,
    rel_name = EXCLUDED.rel_name,
    parent_oid = EXCLUDED.parent_oid,
    main_bytes = EXCLUDED.main_bytes,
    other_forks_bytes = EXCLUDED.other_forks_bytes,
    toast_bytes = EXCLUDED.toast_bytes
WHERE (l.schema_name, l.rel_name, l.parent_oid, l.main_bytes, l.other_forks_bytes, l.toast_bytes)
      IS DISTINCT FROM
      (EXCLUDED.schema_name, EXCLUDED.rel_name, EXCLUDED.parent_oid, EXCLUDED.main_bytes, EXCLUDED.other_forks_bytes, EXCLUDED.toast_bytes)";

    private const string StampSql = @"
INSERT INTO collect.store_relation_sizes_measured (only_row, measured_at)
VALUES (true, $1)
ON CONFLICT (only_row) DO UPDATE SET measured_at = EXCLUDED.measured_at";

    private const string ReadSql = @"
SELECT l.rel_oid, l.parent_oid, l.main_bytes, l.other_forks_bytes, l.toast_bytes
FROM collect.store_relation_sizes_latest AS l";

    private const string ReadStampSql = "SELECT measured_at FROM collect.store_relation_sizes_measured";

    /// <summary>
    /// Replaces the stored sizes with <paramref name="measured"/> in one transaction (see the class remarks): relations not in the pass are
    /// deleted, changed ones are updated, unchanged ones are left out of the upsert's SELECT, so they are neither locked nor rewritten (#9374; a DO UPDATE ... WHERE alone still locks every conflicting row), and the measured time is set. Returns how many relation rows the write
    /// deleted and how many it inserted or updated, so a caller can log what moved.
    /// </summary>
    public static async Task<StoreRelationSizesWrite> WriteAsync(
        NpgsqlDataSource postgres, MeasuredSizePass measured, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(measured);
        var pass = measured.Pass;
        await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.RelationSizes, cancellationToken).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);

        int deleted;
        await using (var delete = new NpgsqlCommand(DeleteGoneSql, connection, transaction))
        {
            delete.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            AddArray(delete, pass.Oids);
            deleted = await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        int upserted;
        await using (var upsert = new NpgsqlCommand(UpsertSql, connection, transaction))
        {
            upsert.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            AddArray(upsert, pass.Oids);
            AddArray(upsert, pass.Parents);
            AddArray(upsert, pass.Main);
            AddArray(upsert, pass.OtherForks);
            AddArray(upsert, pass.Toast);
            upserted = await upsert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await using (var stamp = new NpgsqlCommand(StampSql, connection, transaction))
        {
            stamp.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
            stamp.Parameters.Add(new NpgsqlParameter
            {
                Value = DateTime.SpecifyKind(measured.MeasuredAtUtc, DateTimeKind.Unspecified),
                NpgsqlDbType = NpgsqlDbType.Timestamp,
            });
            await stamp.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new StoreRelationSizesWrite(deleted, upserted);
    }

    /// <summary>
    /// The stored sizes and when they were measured, in the shape both presized reads take
    /// (<see cref="DarlingStoreTableHealthReader.GetAsync(NpgsqlDataSource, DateTime, RelationSizePass, CancellationToken)"/> and
    /// <see cref="DarlingStoreTableHealthReader.GetIndexesAsync(NpgsqlDataSource, RelationSizePass, CancellationToken)"/>), or null for a store
    /// behind V301 (no table) or one nothing has written to yet, so the caller reads live as it does today. The stamp and the rows are read in one
    /// snapshot, so a write between the two reads cannot pair an old time with new rows.
    /// </summary>
    public static Task<MeasuredSizePass?> ReadLastSizesAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default) =>
        ReadLastSizesAsync(postgres, afterStamp: null, cancellationToken);

    /// <summary>The read of <see cref="ReadLastSizesAsync(NpgsqlDataSource, CancellationToken)"/>, with <paramref name="afterStamp"/> awaited between
    /// the stamp statement and the rows statement. A test commits a write there to prove the two statements share one snapshot (#9374).</summary>
    internal static async Task<MeasuredSizePass?> ReadLastSizesAsync(
        NpgsqlDataSource postgres, Func<CancellationToken, Task>? afterStamp, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(StoreSessionNames.RelationSizes, cancellationToken).ConfigureAwait(false);
            await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait(false);

            DateTime measuredAt;
            await using (var stamp = new NpgsqlCommand(ReadStampSql, connection, transaction))
            {
                stamp.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                var value = await stamp.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (value is not DateTime stored)
                {
                    return null;
                }

                measuredAt = DateTime.SpecifyKind(stored, DateTimeKind.Utc);
            }

            if (afterStamp is not null)
            {
                await afterStamp(cancellationToken).ConfigureAwait(false);
            }

            var oids = new List<long>();
            var parents = new List<long>();
            var main = new List<long>();
            var other = new List<long>();
            var toast = new List<long>();
            await using (var command = new NpgsqlCommand(ReadSql, connection, transaction))
            {
                command.CommandTimeout = StorageCommandDeadlines.McpReadSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    oids.Add(reader.GetInt64(0));
                    parents.Add(reader.GetInt64(1));
                    main.Add(reader.GetInt64(2));
                    other.Add(reader.GetInt64(3));
                    toast.Add(reader.GetInt64(4));
                }
            }

            return oids.Count == 0
                ? null
                : new MeasuredSizePass(
                    new RelationSizePass(oids.ToArray(), parents.ToArray(), main.ToArray(), other.ToArray(), toast.ToArray()),
                    measuredAt);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return null;
        }
    }

    private static void AddArray(NpgsqlCommand command, long[] values) =>
        command.Parameters.Add(new NpgsqlParameter { Value = values, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint });
}
