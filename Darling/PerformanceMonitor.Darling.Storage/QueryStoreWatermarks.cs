/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The per-batch Query Store watermark side table (#5684, V178): one row per <c>(server_id, database_name, collection_time)</c>
/// carrying that key's <c>MAX(last_execution_time)</c> and the <c>(query_id, plan_id)</c> of one row that holds it. This is the WRITE
/// half. The per-database watermark seed read (<c>MAX(last_execution_time)</c> over a (server, database) pair's last three hours of
/// <c>collect.query_store_stats</c>) visits 22,000 to 47,000 rows to answer one number; the side table lets a reader touch about
/// thirty-six rows instead.
///
/// <para><b>A value that is too HIGH is the dangerous direction.</b> A watermark above the real maximum skips Query Store rows
/// forever, a watermark below it only re-reads some. The table is therefore built so a missing or low row can only LOWER the answer:
/// the writer runs behind a savepoint and a fault never fails the batch (a batch without a side row is a batch the reader does not
/// see, which is lower), a second batch at one key can only raise the stored value, and the row carries the ids of the raw row that
/// holds the maximum so the reader can check that row still exists in raw before it trusts the value.</para>
///
/// <para><b>Grain.</b> A batch is normally one database at one <c>collection_time</c> (the Azure and enumerated paths write one batch
/// per database per cycle), but the key is the three-part one, so a batch with several databases or a second batch at the same stamp
/// (a backfill, a re-attempt) lands on the same rows and cannot duplicate them. Rows with a NULL <c>database_name</c> are skipped:
/// the reader's <c>database_name = $2</c> never matches one.</para>
///
/// <para><b>Types</b> mirror <c>query_store_stats</c> exactly: <c>collection_time</c> and <c>batch_max</c> are naive-UTC
/// <c>timestamp</c> (microseconds kept), <c>max_query_id</c> and <c>max_plan_id</c> are <c>bigint</c>. <c>batch_max</c>, and with it
/// the two ids, is NULL for a key whose rows all have a NULL <c>last_execution_time</c>: the row still says "this batch was seen".</para>
///
/// <para><b>Coverage start.</b> <see cref="CoverageTable"/> holds one row, <c>covered_since</c>, written once by the V178 rung
/// (a re-run keeps the first value). Every batch whose <c>collection_time</c> is at or after it was written by a build that writes
/// this table. It is NOT <c>MIN(collection_time)</c> of the side table: the backfill writes backdated <c>collection_time</c> values, so
/// a pair's oldest side row proves nothing about what lies before it. A reader bounds its raw arm by this value: raw below it, side
/// table at or after it.</para>
///
/// <para><b>A plain table, on both store kinds.</b> About one row per database per collection, kept for
/// <see cref="RetentionDays"/> days (the reader never looks back more than three hours). Not a collector, absent from
/// <c>CollectorCatalog.All</c>, not a hypertable; the <c>collect</c> schema's blanket SELECT and the owner's default privileges cover it.</para>
/// </summary>
public static class QueryStoreWatermarks
{
    /// <summary>The store schema version of the rung that creates the tables (V178). A store below it has none.</summary>
    public const int RungVersion = 178;

    /// <summary>The per-batch watermark table.</summary>
    public const string Table = "collect.query_store_watermarks";

    /// <summary>The one-row coverage-start table.</summary>
    public const string CoverageTable = "collect.query_store_watermarks_coverage";

    /// <summary>The unique key's constraint (and index) name.</summary>
    public const string KeyConstraintName = "pk_query_store_watermarks";

    /// <summary>The time index's name, which serves the retention sweep.</summary>
    public const string TimeIndexName = "ix_query_store_watermarks_time";

    /// <summary>
    /// How long a side row is kept: a fixed two days, not Query Store's retention. The reader looks back three hours at most, so
    /// the horizon only has to clear that with room for a long pause; keeping 30 days of a table whose rows are read for 3 hours
    /// would be pure bloat.
    /// </summary>
    public const int RetentionDays = 2;

    /// <summary>The savepoint the write runs behind.</summary>
    public const string SavepointName = "qswm_write";

    /// <summary>
    /// The two tables and the coverage row, as the V178 rung runs them. Idempotent: <c>IF NOT EXISTS</c> on the tables and indexes,
    /// and the coverage row is inserted once (<c>ON CONFLICT DO NOTHING</c> on its single-row key), so a re-run keeps the first
    /// <c>covered_since</c>. The value is the store's own clock as naive UTC (<c>now() AT TIME ZONE 'UTC'</c>, independent of the
    /// session time zone). No backfill: the tables start empty and a reader treats everything before <c>covered_since</c> as raw's.
    /// The rung embeds this text, so once V178 has shipped, editing it edits a shipped rung: add a new rung instead.
    /// </summary>
    public const string CreateSql = """
CREATE TABLE IF NOT EXISTS collect.query_store_watermarks
(
    server_id integer NOT NULL,
    database_name text NOT NULL,
    collection_time timestamp NOT NULL,
    batch_max timestamp,
    max_query_id bigint,
    max_plan_id bigint,
    CONSTRAINT pk_query_store_watermarks PRIMARY KEY (server_id, database_name, collection_time)
);
CREATE INDEX IF NOT EXISTS ix_query_store_watermarks_time ON collect.query_store_watermarks (collection_time);
CREATE TABLE IF NOT EXISTS collect.query_store_watermarks_coverage
(
    singleton boolean NOT NULL DEFAULT true,
    covered_since timestamp NOT NULL,
    CONSTRAINT pk_query_store_watermarks_coverage PRIMARY KEY (singleton),
    CONSTRAINT ck_query_store_watermarks_coverage_singleton CHECK (singleton)
);
INSERT INTO collect.query_store_watermarks_coverage (singleton, covered_since)
VALUES (true, (now() AT TIME ZONE 'UTC'))
ON CONFLICT (singleton) DO NOTHING;
""";

    /// <summary>
    /// Writes one collection's side rows: <c>$1</c> is the server id and <c>$2</c> the batch's <c>collection_time</c> (naive UTC
    /// <c>timestamp</c>). It READS BACK the rows the store holds for that key (those of earlier committed batches and this batch's,
    /// visible on the COPY's transaction), served by the <c>(server_id, collection_time)</c> index, and takes per database the row
    /// with the highest <c>last_execution_time</c> (<c>DISTINCT ON</c> with <c>NULLS LAST</c>, ties broken by the lowest query_id,
    /// plan_id so a re-run picks the same row). The ids are NULL when that maximum is NULL. A NULL <c>database_name</c> is skipped.
    ///
    /// <para><b>Only up.</b> On a conflict the stored row is replaced only when the incoming <c>batch_max</c> is higher, or the
    /// stored one is NULL and the incoming one is not. A lower or equal value, a NULL, and a re-run all leave the row alone, so a
    /// retry or a second batch at the same stamp can never lower it. (The read-back covers the whole collection, so it normally
    /// agrees; the guard is what makes the property independent of that.) Equal values keep the first row's ids on purpose: both
    /// name a raw row that holds the value.</para>
    /// </summary>
    public const string UpsertSql = """
INSERT INTO collect.query_store_watermarks AS w
    (server_id, database_name, collection_time, batch_max, max_query_id, max_plan_id)
SELECT DISTINCT ON (s.database_name)
    s.server_id,
    s.database_name,
    s.collection_time,
    s.last_execution_time,
    CASE WHEN s.last_execution_time IS NULL THEN NULL ELSE s.query_id END,
    CASE WHEN s.last_execution_time IS NULL THEN NULL ELSE s.plan_id END
FROM collect.query_store_stats AS s
WHERE s.server_id = $1::integer
AND   s.collection_time = $2::timestamp
AND   s.database_name IS NOT NULL
ORDER BY s.database_name, s.last_execution_time DESC NULLS LAST, s.query_id, s.plan_id
ON CONFLICT (server_id, database_name, collection_time) DO UPDATE SET
    batch_max = EXCLUDED.batch_max,
    max_query_id = EXCLUDED.max_query_id,
    max_plan_id = EXCLUDED.max_plan_id
WHERE EXCLUDED.batch_max IS NOT NULL
AND   (w.batch_max IS NULL OR EXCLUDED.batch_max > w.batch_max);
""";

    /// <summary>
    /// The retention sweep's statement (<c>$1</c> is the cutoff, naive UTC): one plain DELETE, served by the time index. About one
    /// row per database per collection for two days, so one execution is the whole purge.
    /// </summary>
    public const string PruneSql = """
DELETE FROM collect.query_store_watermarks
WHERE collection_time < $1::timestamp;
""";

    /// <summary>
    /// Writes the batch's side rows on the COPY's own transaction, behind <see cref="SavepointName"/>. A fault rolls back to the
    /// savepoint, is logged, and returns false: the batch commits without its side rows, which only lowers what a reader sees. A
    /// cancellation propagates (the whole transaction is abandoned). Returns true when the statement ran.
    /// </summary>
    public static async Task<bool> TryUpsertBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        DateTime collectionTime,
        int commandTimeoutSeconds,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);

        await transaction.SaveAsync(SavepointName, cancellationToken);
        try
        {
            await using var command = new NpgsqlCommand(UpsertSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });

            /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` - see PgCollectorRowWriter. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified) });
            await command.ExecuteNonQueryAsync(cancellationToken);
            await transaction.ReleaseAsync(SavepointName, cancellationToken);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await transaction.RollbackAsync(SavepointName, cancellationToken);
            logger?.LogWarning(ex,
                "Query Store watermark side table (#5684): the batch for server {ServerId} at {CollectionTime} stored its raw rows but missed its watermark rows; a reader sees a lower watermark for it, never a higher one",
                serverId, collectionTime.ToString("o", CultureInfo.InvariantCulture));
            return false;
        }
    }
}
