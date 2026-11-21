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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The per-collection summary of <c>collect.query_stats</c> (#9147, V286): one row per (server, collection, database) holding
/// what the raw duration trend sums from every row of that collection. The trend's per-collection CTE
/// (<c>DurationTrendRouting</c>) read every <c>query_stats</c> row of a 24 hour window, which on a busy server is 375,000 rows
/// and 32,170 heap pages for a 479-point answer, because each page holds whole 730 byte rows and the read uses four columns. The
/// collector now writes the summary in the same transaction as the raw batch (<see cref="UpsertBatchAsync"/>), and the trend reads
/// it in place of the rows for a server whose summary covers the whole window. This class is the storage half: the table, the
/// writer's statements and the retention predicate. The read is <c>DurationTrendRouting.RawCollectionsCte</c>.
///
/// <para><b>Grain and measures.</b> <c>(server_id, collection_time, database_name)</c>, per database because the viewer's database
/// filter sums only the chosen databases of a collection. Every measure is the aggregate the trend's CTE takes over the same rows,
/// with no row filter (a restart row counts, as it does in the trend): <c>sum_elapsed</c> = SUM(delta_elapsed_time),
/// <c>sum_exec</c> = SUM(delta_execution_count), <c>n</c> = COUNT(*) (the filtered trend's <c>matched_rows</c>),
/// <c>max_interval</c> = MAX(sample_interval_seconds). The two sums and the max are NULL when every row's value is NULL, as the
/// aggregates are over the rows, so the three-state interval contract (#3540: 0 = restart, NULL = unknown, positive = measured)
/// reads the same from here. PostgreSQL keeps the microseconds, so every value is exact.</para>
///
/// <para><b>NULL database_name.</b> The key is a unique index <c>NULLS NOT DISTINCT</c> (the form V170 and V176 use): the NULL
/// database is one group, as GROUP BY makes it, and a database filter treats it here exactly as it treats the rows.</para>
///
/// <para><b>Where the read may use it.</b> The table is empty when created and nothing backfills it. The read takes a server's
/// summary only when its oldest row AND the server's floor (<see cref="FloorTable"/>) are at or before the window start
/// (<c>covered</c> in the CTE), and reads raw otherwise. That is exact: every collection from the floor onward has a summary row, so
/// the first collection after the window start finds its previous collection (the gap the rate divides by) in the summary, as raw
/// would give it. A rollback that ran an older build writes raw rows with no summary row, which would be a hole after the oldest row,
/// so <see cref="FloorMoveSql"/> moves the server's floor up to the batch that finds the hole. The read then goes to raw until a window
/// starts at or after the floor. Summary rows below the floor stay until the retention sweep ages them out, and no covered read uses
/// them. Until that next batch runs, the hole is raw collections newer than the summary's newest row, so the read also needs the other
/// end: it takes the summary only when no raw collection of the server lies after the summary's newest row and at or before the window end.</para>
///
/// <para><b>The floor is one row per server, not a delete (#9193).</b> The floor move once deleted every summary row at or before the
/// anchor, inside the batch's transaction. That delete grows with databases times collections times retention, so past the batch's
/// command timeout it rolled the batch back, left the hole, and every later batch did the same. The floor table (V290) holds one
/// <c>valid_from</c> per server, so the move is one upsert whatever the summary holds.</para>
///
/// <para><b>A plain table, on both store kinds,</b> like the hour ledger (V164) and the procedure totals (V176): not a collector,
/// not in <c>CollectorCatalog.All</c>, not a hypertable, about one row per database per collection. It sits in the
/// <c>collect</c> schema, so the blanket SELECT and the owner's default privileges cover it, and the collector writes as the owner.
/// Retention is the retention sweep's: the same days as <c>query_stats</c>, so a summary row never outlives the raw rows of its age
/// (raw keeps a chunk past that cutoff when a purge gate holds it, which only makes raw hold more than the summary).</para>
/// </summary>
public static class QueryStatsCollectionSummary
{
    /// <summary>The store schema version of the rung that creates the table (V286). A store below it has no table.</summary>
    public const int RungVersion = 286;

    /// <summary>The summary table.</summary>
    public const string Table = "collect.query_stats_collection";

    /// <summary>The store schema version of the rung that creates <see cref="FloorTable"/> (V290, #9193). A store below it has no floor table.</summary>
    public const int FloorRungVersion = 290;

    /// <summary>
    /// The floor table (V290, #9193): one row per server, <c>valid_from</c> the <c>collection_time</c> of the batch that last found a hole.
    /// Every summary row at or after it is whole. A server with no row has no floor.
    /// </summary>
    public const string FloorTable = "collect.query_stats_collection_floor";

    /// <summary>The unique key's index name.</summary>
    public const string KeyIndexName = "ux_query_stats_collection";

    /// <summary>
    /// The table, its unique key and the time index, as the V286 rung runs them. Idempotent, and empty when created. Editing this text
    /// edits a shipped rung: add a new rung instead.
    /// </summary>
    public const string CreateSql = """
CREATE TABLE IF NOT EXISTS collect.query_stats_collection
(
    server_id integer NOT NULL,
    collection_time timestamp NOT NULL,
    database_name text,
    n bigint NOT NULL,
    sum_elapsed bigint,
    sum_exec bigint,
    max_interval integer
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_query_stats_collection ON collect.query_stats_collection (server_id, collection_time, database_name) NULLS NOT DISTINCT;
CREATE INDEX IF NOT EXISTS ix_query_stats_collection_time ON collect.query_stats_collection (collection_time);
""";

    /// <summary>
    /// The floor table as the V290 rung runs it. Idempotent, empty when created, and no data moves. Editing this text edits a shipped rung:
    /// add a new rung instead.
    /// </summary>
    public const string FloorCreateSql = """
CREATE TABLE IF NOT EXISTS collect.query_stats_collection_floor
(
    server_id integer PRIMARY KEY,
    valid_from timestamp NOT NULL
);
""";

    /// <summary>
    /// Writes the summary of one collection: <c>$1</c> is the server id and <c>$2</c> the collection's <c>collection_time</c> (naive UTC
    /// <c>timestamp</c>). It READS BACK the rows the store holds for that collection and sets one row per database, so it is exact and
    /// idempotent when one collection spans several batches (Azure SQL Database writes one batch per database at one
    /// <c>collection_time</c>), when a late batch lands in an older collection, and when a batch is attempted again: the second run
    /// recomputes the same values from the rows and never adds to them. The sums are cast to <c>bigint</c> (SUM of a bigint is numeric);
    /// a sum past bigint would fail the statement, which is 292,000 years of elapsed time in one collection. Run it on the COPY's own
    /// transaction, after the COPY and before the commit.
    ///
    /// <para><b>Limited to the batch's databases.</b> <c>$3</c> is the <c>text[]</c> of the non-NULL database names the batch wrote and
    /// <c>$4</c> says whether the batch wrote a NULL database; a NULL <c>$3</c> reads the whole collection. A read-back of the whole
    /// collection in each of N per-database batches would grow with the square of N. Summaries are per database and a batch holds whole
    /// databases (the read-back still counts rows of earlier batches of the same database), so the result is exact. The NULL database
    /// matches only through <c>$4</c>, as <c>= ANY</c> never matches a NULL.</para>
    /// </summary>
    public const string UpsertSql = """
INSERT INTO collect.query_stats_collection AS t
    (server_id, collection_time, database_name, n, sum_elapsed, sum_exec, max_interval)
SELECT
    server_id,
    collection_time,
    database_name,
    COUNT(*),
    SUM(delta_elapsed_time)::bigint,
    SUM(delta_execution_count)::bigint,
    MAX(sample_interval_seconds)
FROM collect.query_stats
WHERE server_id = $1::integer
AND   collection_time = $2::timestamp
AND   ($3::text[] IS NULL OR database_name = ANY($3::text[]) OR ($4::boolean AND database_name IS NULL))
GROUP BY server_id, collection_time, database_name
ON CONFLICT (server_id, collection_time, database_name) DO UPDATE SET
    n = EXCLUDED.n,
    sum_elapsed = EXCLUDED.sum_elapsed,
    sum_exec = EXCLUDED.sum_exec,
    max_interval = EXCLUDED.max_interval;
""";

    /// <summary>
    /// Moves the server's floor (<see cref="FloorTable"/>) up to this batch when a collection exists that has no summary row, run on the
    /// COPY's transaction before <see cref="UpsertSql"/>: <c>$1</c> is the server id and <c>$2</c> the batch's <c>collection_time</c>.
    /// The anchor is the server's newest summary row at or before the batch. When a <c>query_stats</c> row of the server lies strictly
    /// after the anchor and strictly before the batch, an older build wrote it (a rollback past V286 and forward again), so the summary is
    /// not whole across that stretch. The statement then upserts ONE floor row, <c>valid_from</c> = the batch's time (never moved
    /// backwards: <c>GREATEST</c> of the stored value and the batch's), and the read stays on raw until a window starts at or after it.
    /// It deletes nothing (#9193): the summary rows below the floor stay for the retention sweep, and no covered read uses them. Such a
    /// raw row exists exactly when the newest raw row before the batch is after the anchor, so <c>prior</c> reads only that one row
    /// (<c>ORDER BY collection_time DESC LIMIT 1</c> stops at the first chunk that holds a row of the server). The hole test is a
    /// one-time filter over two scalar subselects, so the planner evaluates it once whatever plan it picks, and the insert never runs
    /// when there is no hole. The anchor must not be a join column of a raw-table <c>EXISTS</c> (#9147): the planner then cannot use it
    /// as an index bound or to exclude chunks, and the normal case, where no such row exists, read every chunk of the server's raw
    /// history before it answered no. Normally the statement writes nothing and costs one probe of the <c>(server_id, collection_time)</c>
    /// index in the newest chunk. The anchor takes rows AT the batch's time too, so the later batches of an Azure cycle (one
    /// <c>collection_time</c>, one batch per database) find their own cycle's row as the anchor and never trigger it. A server with no
    /// summary row (a NULL anchor) or no raw row before the batch writes nothing. Returns the floor rows written (0 or 1).
    /// </summary>
    public const string FloorMoveSql = """
WITH anchor AS
(
    SELECT max(s.collection_time) AS at
    FROM collect.query_stats_collection AS s
    WHERE s.server_id = $1::integer
    AND   s.collection_time <= $2::timestamp
),
prior AS
(
    SELECT q.collection_time AS at
    FROM collect.query_stats AS q
    WHERE q.server_id = $1::integer
    AND   q.collection_time < $2::timestamp
    ORDER BY q.collection_time DESC
    LIMIT 1
)
INSERT INTO collect.query_stats_collection_floor AS f
    (server_id, valid_from)
SELECT
    $1::integer,
    $2::timestamp
WHERE (SELECT p.at FROM prior AS p) > (SELECT a.at FROM anchor AS a)
ON CONFLICT (server_id) DO UPDATE SET
    valid_from = GREATEST(f.valid_from, EXCLUDED.valid_from);
""";

    /// <summary>
    /// The databases a query_stats batch holds, for the read-back limit (<see cref="UpsertBatchAsync"/>'s <c>databaseNames</c>): one entry
    /// per distinct database, null for the NULL database. Matches any <see cref="IEnumerable{T}"/> of
    /// <see cref="QueryStatsCollector.Row"/>, so the runner's <c>List&lt;TRow&gt;</c> with <c>TRow</c> = Row qualifies. Anything else
    /// returns null, and a null collection reads the whole collection (correct, but unbounded).
    /// </summary>
    public static IReadOnlyCollection<string?>? DatabaseNamesOf(object? rows) =>
        rows is IEnumerable<QueryStatsCollector.Row> typed
            ? typed.Select(static row => (string?)row.DatabaseName).Distinct().ToList()
            : null;

    /// <summary>
    /// Brings the summary up to date for the collection stamped <paramref name="collectionTime"/> of <paramref name="serverId"/> on the
    /// COPY's transaction: the floor move (<see cref="FloorMoveSql"/>) and then the upsert (<see cref="UpsertSql"/>). No savepoint and no
    /// catch, the hour ledger's shape (#4605): a fault throws out of the batch, the transaction rolls back and the batch's raw rows are
    /// not stored, so raw and the summary commit together or not at all, and the read can trust a summary row to equal its collection's
    /// rows. A <paramref name="rowsWritten"/> of 0 or less writes nothing and costs no round trip. <paramref name="databaseNames"/> are
    /// the databases the batch wrote (null entries are the NULL database); a null collection reads the whole collection. Returns the
    /// summary rows the upsert wrote (one per database in the batch).
    /// </summary>
    public static async Task<int> UpsertBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        DateTime collectionTime,
        int rowsWritten,
        int commandTimeoutSeconds,
        IReadOnlyCollection<string?>? databaseNames,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);

        if (rowsWritten <= 0)
        {
            return 0;
        }

        /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` - see PgCollectorRowWriter. */
        var stamp = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified);

        await using (var move = new NpgsqlCommand(FloorMoveSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            move.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            move.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });
            await move.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var command = new NpgsqlCommand(UpsertSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });

        /* $3: the non-NULL names (NULL array = no limit); $4: whether the batch holds the NULL database. */
        var names = databaseNames?.Where(static name => name is not null).Distinct(StringComparer.Ordinal).ToArray();
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = names is null ? DBNull.Value : names });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = databaseNames?.Any(static name => name is null) ?? false });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
