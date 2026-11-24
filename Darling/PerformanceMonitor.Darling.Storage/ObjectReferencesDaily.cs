/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis.ObjectReferences;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The per-day summary of which names Query Store statements referenced (V297, #8784 part 2): one row per (server, statement database,
/// referenced database, schema, name, kind, day) with the executions of the statements that named it, so <c>get_object_usage</c> can say
/// whether a view was used at all across a year without keeping a year of statement text.
///
/// <para><b>Why a table of its own.</b> Query Store statement text (<c>collect.query_store_text</c>) is kept about as long as
/// <c>query_store</c>'s retention, 30 days by default, and the daily Query Store summary only ten. A decommissioning review needs a year. Like
/// <see cref="ObjectExecutionsDaily"/>, this table keeps only what that question reads, for as long as <c>index_object_stats</c> is kept: it
/// applies the retention <see cref="ObjectExecutionsDaily"/> has applied (its grace period for a shorter one included), so the module and the
/// view evidence always reach back the same distance.</para>
///
/// <para><b>Where a day comes from.</b> Each statement (<c>query_id</c>) in <c>query_store_top_daily</c> for the (server, day), with its
/// executions summed over the day and its text from <c>query_store_text</c>, goes through <see cref="ObjectReferenceAggregator"/>: parsed with
/// ScriptDom for the names it reads as tables, views and table-valued functions, its local names (CTEs, aliases, temp tables) left out. A
/// statement that does not parse is searched as text, whole words only, for the views the newest object list holds for its database. A
/// statement with no stored text is counted, not looked into. <see cref="DatabaseDaysTable"/> holds per (server, database, day) how many
/// statements were looked at, with no text and unparsed, so the reader can say how much the evidence covers.</para>
///
/// <para><b>How it is kept.</b> <see cref="RunTickAsync"/> runs after <see cref="ObjectExecutionsDaily.RunTickAsync"/> on the same
/// housekeeping tick. It builds every (server, day) whose Query Store summary was built since this table last built it, newest first, at
/// most <see cref="MaxBuildsPerTick"/> a tick and no longer than <see cref="MaxTickDuration"/>, each in its own transaction with its built
/// row. A build only ever adds, as <see cref="ObjectExecutionsDaily"/>'s copy does: a name already stored for the day keeps the larger count
/// and the newer execution time, so a rebuild from a source whose text has since been pruned cannot take evidence away. Parsed statements are
/// cached by a hash of their text across ticks (<see cref="ScanCache"/>), since the same statement comes back every day it runs.</para>
/// </summary>
public static class ObjectReferencesDaily
{
    /// <summary>The store schema version that creates these tables.</summary>
    public const int Version = 297;

    /// <summary>The summary table.</summary>
    public const string Table = "collect.object_references_daily";

    /// <summary>The per-(server, day) record of which Query Store summary build each day was built from.</summary>
    public const string BuiltTable = "collect.object_references_daily_built";

    /// <summary>The (server, database, day) rows: how many statements were looked at.</summary>
    public const string DatabaseDaysTable = "collect.object_references_daily_databases";

    /// <summary>The most (server, day) builds one tick runs.</summary>
    public const int MaxBuildsPerTick = 60;

    /// <summary>No new build starts once a tick has run this long; the rest is planned again next tick.</summary>
    public static readonly TimeSpan MaxTickDuration = TimeSpan.FromMinutes(5);

    /// <summary>Command timeout of each statement.</summary>
    public const int CommandTimeoutSeconds = 300;

    /// <summary>The parsed statements, shared across ticks for the life of the service.</summary>
    public static QueryTextScanCache ScanCache { get; } = new();

    /// <summary>
    /// The three tables as the V297 rung runs them. Idempotent. The rung embeds this text, so editing it after it ships edits a shipped
    /// rung: add a new rung instead. Plain tables on every store. The primary key serves the build's upsert and the read's same-database
    /// arm; <c>idx_object_references_daily_cross_database</c>, partial on the rows that name another database, serves the read's
    /// cross-database arm (#8784 review); <c>idx_object_references_daily_day</c> serves the retention delete. Needs no GRANT: the <c>collect</c> schema's blanket SELECT covers a
    /// table a migration introduces.
    /// </summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.object_references_daily
(
    server_id integer NOT NULL,
    database_name text NOT NULL,
    referenced_database text NOT NULL,
    referenced_schema text NOT NULL,
    referenced_name text NOT NULL,
    is_function boolean NOT NULL,
    day date NOT NULL,
    execution_count numeric NOT NULL,
    statement_count integer NOT NULL,
    last_execution_time timestamp,
    text_matched boolean NOT NULL,
    referenced_schema_as_seen text,
    referenced_name_as_seen text,
    CONSTRAINT pk_object_references_daily PRIMARY KEY (server_id, database_name, referenced_database, referenced_schema, referenced_name, is_function, day)
);

CREATE INDEX IF NOT EXISTS idx_object_references_daily_day ON collect.object_references_daily (day);

CREATE INDEX IF NOT EXISTS idx_object_references_daily_cross_database ON collect.object_references_daily (server_id, referenced_database, day)
    WHERE referenced_database <> '';

CREATE TABLE IF NOT EXISTS collect.object_references_daily_built
(
    server_id integer NOT NULL,
    day date NOT NULL,
    source_built_at timestamp NOT NULL,
    built_at timestamp NOT NULL,
    CONSTRAINT pk_object_references_daily_built PRIMARY KEY (server_id, day)
);

CREATE TABLE IF NOT EXISTS collect.object_references_daily_databases
(
    server_id integer NOT NULL,
    database_name text NOT NULL,
    day date NOT NULL,
    statements bigint NOT NULL,
    statements_without_text bigint NOT NULL,
    statements_unparsed bigint NOT NULL,
    execution_count numeric NOT NULL,
    CONSTRAINT pk_object_references_daily_databases PRIMARY KEY (server_id, database_name, day)
);";

    /// <summary>The retention <see cref="ObjectExecutionsDaily"/>'s cleanup is applying, or no row before its first tick.</summary>
    internal const string AppliedRetentionSql = "SELECT applied_days FROM collect.object_executions_daily_retention;";

    /// <summary>
    /// The (server, day) builds due, newest first: every built Query Store summary day that this table has not built, or built from an older
    /// summary build. $1 the retention edge (a date), $2 the cap.
    /// </summary>
    internal const string PlanSql = @"
SELECT b.server_id, b.day, b.built_at
FROM collect.query_store_top_daily_built AS b
LEFT JOIN collect.object_references_daily_built AS o
  ON  o.server_id = b.server_id
  AND o.day = b.day
WHERE b.day >= $1
AND   (o.server_id IS NULL OR o.source_built_at < b.built_at)
ORDER BY
    b.day DESC,
    b.server_id
LIMIT $2;";

    /// <summary>
    /// The views of every database of one server, from each database's newest object list: the names an unparsed statement's text is
    /// searched for. $1 server_id. The anchor column comes third, as every newest-capture read projects it
    /// (<c>McpPayloadContractCensusTests</c>); the build reads only the first two.
    /// </summary>
    internal const string FallbackNamesSql = @"
SELECT i.database_name, i.object_name, i.collection_time
FROM collect.object_inventory AS i
WHERE i.server_id = $1
AND   i.type_desc = 'VIEW'
AND   i.database_name IS NOT NULL
AND   i.object_name IS NOT NULL
AND   i.collection_time =
      (
          SELECT max(l.collection_time)
          FROM collect.object_inventory AS l
          WHERE l.server_id = i.server_id
          AND   l.database_name = i.database_name
      );";

    /// <summary>
    /// One (server, day)'s statements: each query's executions summed over the day, its newest execution and its stored text (NULL where none
    /// is stored). $1 server_id, $2 day.
    /// </summary>
    internal const string StatementsSql = @"
SELECT
    t.database_name,
    COALESCE(sum(t.execution_count_sum), 0),
    max(t.last_execution_time_max),
    q.query_sql_text
FROM collect.query_store_top_daily AS t
LEFT JOIN collect.query_store_text AS q
  ON  q.server_id = t.server_id
  AND q.database_name = t.database_name
  AND q.query_id = t.query_id
WHERE t.server_id = $1
AND   t.day = $2
AND   t.database_name IS NOT NULL
GROUP BY
    t.database_name,
    t.query_id,
    q.query_sql_text;";

    /// <summary>
    /// Upserts one (server, day)'s referenced names from parallel arrays. $1 server_id, $2 day, $3..$13 the columns. A name already stored
    /// keeps the larger count and the newer execution time (PostgreSQL's GREATEST skips a NULL), and text_matched stays true only while
    /// every build of the day found the name by the text search alone.
    ///
    /// <para><b>The key is folded once, in the aggregator</b> (#8784 review). The referenced database, schema and name are stored as
    /// <c>ObjectReferenceAggregator.FoldName</c> folded them, with no <c>lower()</c> here: one build merges <c>dbo.VSales</c> and
    /// <c>dbo.vSales</c> into one row, and a case-sensitive key holding the spelling met first would let two builds of the same day leave two
    /// rows and the read count that day twice. Folding in one place also means two names the aggregator keeps apart cannot fold to one key
    /// here (PostgreSQL's <c>lower()</c> and .NET's case rules differ on a few letters, such as U+0130), which would make the upsert affect a
    /// row twice and fail the build. The spelling as seen is kept beside the key ($12, $13) for display, the first build's spelling
    /// winning, so a view guessed from the summary where no object list is kept is not reported under a lower-cased name that a
    /// case-sensitive collation does not resolve.</para>
    /// </summary>
    internal const string UpsertReferencesSql = @"
INSERT INTO collect.object_references_daily
    (server_id, database_name, referenced_database, referenced_schema, referenced_name, is_function, day, execution_count, statement_count,
     last_execution_time, text_matched, referenced_schema_as_seen, referenced_name_as_seen)
SELECT $1, u.database_name, u.referenced_database, u.referenced_schema, u.referenced_name, u.is_function, $2,
       u.execution_count, u.statement_count, u.last_execution_time, u.text_matched, u.schema_as_seen, u.name_as_seen
FROM unnest($3::text[], $4::text[], $5::text[], $6::text[], $7::boolean[], $8::numeric[], $9::integer[], $10::timestamp[], $11::boolean[],
            $12::text[], $13::text[])
     AS u (database_name, referenced_database, referenced_schema, referenced_name, is_function, execution_count, statement_count,
           last_execution_time, text_matched, schema_as_seen, name_as_seen)
ON CONFLICT (server_id, database_name, referenced_database, referenced_schema, referenced_name, is_function, day) DO UPDATE
SET execution_count = GREATEST(collect.object_references_daily.execution_count, EXCLUDED.execution_count),
    statement_count = GREATEST(collect.object_references_daily.statement_count, EXCLUDED.statement_count),
    last_execution_time = GREATEST(collect.object_references_daily.last_execution_time, EXCLUDED.last_execution_time),
    text_matched = collect.object_references_daily.text_matched AND EXCLUDED.text_matched,
    referenced_schema_as_seen = COALESCE(collect.object_references_daily.referenced_schema_as_seen, EXCLUDED.referenced_schema_as_seen),
    referenced_name_as_seen = COALESCE(collect.object_references_daily.referenced_name_as_seen, EXCLUDED.referenced_name_as_seen);";

    /// <summary>Upserts one (server, day)'s database counts from parallel arrays. $1 server_id, $2 day, $3..$7 the columns. The larger
    /// statement count stays, with the counts that came with it.</summary>
    internal const string UpsertDatabasesSql = @"
INSERT INTO collect.object_references_daily_databases
    (server_id, database_name, day, statements, statements_without_text, statements_unparsed, execution_count)
SELECT $1, u.database_name, $2, u.statements, u.statements_without_text, u.statements_unparsed, u.execution_count
FROM unnest($3::text[], $4::bigint[], $5::bigint[], $6::bigint[], $7::numeric[])
     AS u (database_name, statements, statements_without_text, statements_unparsed, execution_count)
ON CONFLICT (server_id, database_name, day) DO UPDATE
SET statements = EXCLUDED.statements,
    statements_without_text = EXCLUDED.statements_without_text,
    statements_unparsed = EXCLUDED.statements_unparsed,
    execution_count = EXCLUDED.execution_count
WHERE EXCLUDED.statements >= collect.object_references_daily_databases.statements;";

    /// <summary>Records the build. $1 server_id, $2 day, $3 the summary build it was built from, $4 now.</summary>
    internal const string UpsertBuiltSql = @"
INSERT INTO collect.object_references_daily_built (server_id, day, source_built_at, built_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (server_id, day) DO UPDATE
SET source_built_at = EXCLUDED.source_built_at,
    built_at = EXCLUDED.built_at;";

    /// <summary>The retention deletes: days before $1 (a date). One statement per command.</summary>
    internal static readonly string[] RetentionDeleteSql =
    [
        "DELETE FROM collect.object_references_daily WHERE day < $1;",
        "DELETE FROM collect.object_references_daily_built WHERE day < $1;",
        "DELETE FROM collect.object_references_daily_databases WHERE day < $1;",
    ];

    /// <summary>One planned build.</summary>
    public readonly record struct Build(int ServerId, DateOnly Day, DateTime SourceBuiltAt);

    /// <summary>What one tick did.</summary>
    public readonly record struct TickResult(int Built, int Failed, int Deferred);

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>
    /// One tick: read the retention <see cref="ObjectExecutionsDaily"/> applies, delete the expired days, plan, then build each planned day
    /// in its own transaction. A build that fails is logged (naming the server and day) and the loop goes on; the next tick plans it again.
    /// </summary>
    public static async Task<TickResult> RunTickAsync(
        NpgsqlDataSource dataSource, DateTime nowUtc, ILogger logger, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataSource);
        ArgumentNullException.ThrowIfNull(logger);

        var clock = Stopwatch.StartNew();
        await using var connection = await dataSource.OpenConnectionAsync(StoreSessionNames.ObjectReferencesDaily, cancellationToken);

        int retentionDays;
        await using (var applied = new NpgsqlCommand(AppliedRetentionSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            var value = await applied.ExecuteScalarAsync(cancellationToken);
            if (value is null or DBNull)
            {
                await using var configured = new NpgsqlCommand(ObjectExecutionsDaily.RetentionDaysSql, connection) { CommandTimeout = CommandTimeoutSeconds };
                value = await configured.ExecuteScalarAsync(cancellationToken);
            }

            retentionDays = Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        var edge = ObjectExecutionsDaily.RetentionEdge(nowUtc, retentionDays);
        foreach (var sql in RetentionDeleteSql)
        {
            await using var delete = new NpgsqlCommand(sql, connection) { CommandTimeout = CommandTimeoutSeconds };
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = edge });
            await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        var plan = new List<Build>();
        await using (var planCommand = new NpgsqlCommand(PlanSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            planCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = edge });
            planCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxBuildsPerTick + 1 });
            await using var reader = await planCommand.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                plan.Add(new Build(reader.GetInt32(0), reader.GetFieldValue<DateOnly>(1), reader.GetDateTime(2)));
            }
        }

        var fallbackByServer = new Dictionary<int, Dictionary<string, List<string>>>();
        int built = 0, failed = 0, deferred = plan.Count > MaxBuildsPerTick ? 1 : 0;
        for (var i = 0; i < plan.Count && i < MaxBuildsPerTick; i++)
        {
            if (clock.Elapsed >= MaxTickDuration)
            {
                deferred = 1;
                break;
            }

            var item = plan[i];
            try
            {
                if (!fallbackByServer.TryGetValue(item.ServerId, out var fallback))
                {
                    fallback = await ReadFallbackNamesAsync(connection, item.ServerId, cancellationToken);
                    fallbackByServer.Add(item.ServerId, fallback);
                }

                await BuildDayAsync(connection, item, fallback, nowUtc, cancellationToken);
                built++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failed++;
                logger.LogWarning(
                    "Object references summary: building server {ServerId} day {Day:yyyy-MM-dd} failed (SqlState {SqlState}); the next tick retries: {Message}",
                    item.ServerId, item.Day, (ex as PostgresException)?.SqlState ?? "n/a", ex.Message);
            }
        }

        return new TickResult(built, failed, deferred);
    }

    /// <summary>The views of each database of one server, from each database's newest object list.</summary>
    internal static async Task<Dictionary<string, List<string>>> ReadFallbackNamesAsync(
        NpgsqlConnection connection, int serverId, CancellationToken cancellationToken)
    {
        var names = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(FallbackNamesSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var database = reader.GetString(0);
            if (!names.TryGetValue(database, out var list))
            {
                list = new List<string>();
                names.Add(database, list);
            }

            list.Add(reader.GetString(1));
        }

        return names;
    }

    /// <summary>Builds one (server, day): reads and scans its statements, then upserts its names, its database counts and its built row in
    /// one transaction. Returns the name rows written.</summary>
    internal static async Task<int> BuildDayAsync(
        NpgsqlConnection connection, Build item, IReadOnlyDictionary<string, List<string>> fallbackNames, DateTime nowUtc,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(fallbackNames);

        var aggregator = new ObjectReferenceAggregator(
            database => fallbackNames.TryGetValue(database, out var names) ? names : Array.Empty<string>(), ScanCache);

        await using (var read = new NpgsqlCommand(StatementsSql, connection) { CommandTimeout = CommandTimeoutSeconds })
        {
            read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = item.ServerId });
            read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = item.Day });
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                aggregator.Add(
                    reader.GetString(0),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(1) ? 0 : ToInt64(reader.GetDecimal(1)),
                    reader.IsDBNull(2) ? null : reader.GetDateTime(2));
            }
        }

        var references = aggregator.References;
        var databases = aggregator.Databases;

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        if (references.Count > 0)
        {
            await using var upsert = new NpgsqlCommand(UpsertReferencesSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = item.ServerId });
            upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = item.Day });
            AddArray(upsert, NpgsqlDbType.Text, references.Select(r => r.DatabaseName).ToArray());
            AddArray(upsert, NpgsqlDbType.Text, references.Select(r => r.ReferencedDatabaseKey).ToArray());
            AddArray(upsert, NpgsqlDbType.Text, references.Select(r => r.SchemaKey).ToArray());
            AddArray(upsert, NpgsqlDbType.Text, references.Select(r => r.ObjectNameKey).ToArray());
            AddArray(upsert, NpgsqlDbType.Boolean, references.Select(r => r.IsFunction).ToArray());
            AddArray(upsert, NpgsqlDbType.Numeric, references.Select(r => (decimal)r.Executions).ToArray());
            AddArray(upsert, NpgsqlDbType.Integer, references.Select(r => r.Statements).ToArray());
            AddArray(upsert, NpgsqlDbType.Timestamp, references.Select(r => r.LastExecution is { } t ? Unspecified(t) : (DateTime?)null).ToArray());
            AddArray(upsert, NpgsqlDbType.Boolean, references.Select(r => r.TextMatched).ToArray());
            AddArray(upsert, NpgsqlDbType.Text, references.Select(r => r.SchemaName).ToArray());
            AddArray(upsert, NpgsqlDbType.Text, references.Select(r => r.ObjectName).ToArray());
            await upsert.ExecuteNonQueryAsync(cancellationToken);
        }

        if (databases.Count > 0)
        {
            await using var upsert = new NpgsqlCommand(UpsertDatabasesSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
            upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = item.ServerId });
            upsert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = item.Day });
            AddArray(upsert, NpgsqlDbType.Text, databases.Select(d => d.DatabaseName).ToArray());
            AddArray(upsert, NpgsqlDbType.Bigint, databases.Select(d => d.Statements).ToArray());
            AddArray(upsert, NpgsqlDbType.Bigint, databases.Select(d => d.StatementsWithoutText).ToArray());
            AddArray(upsert, NpgsqlDbType.Bigint, databases.Select(d => d.StatementsUnparsed).ToArray());
            AddArray(upsert, NpgsqlDbType.Numeric, databases.Select(d => (decimal)d.Executions).ToArray());
            await upsert.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var builtRow = new NpgsqlCommand(UpsertBuiltSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            builtRow.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = item.ServerId });
            builtRow.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Date, Value = item.Day });
            builtRow.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(item.SourceBuiltAt) });
            builtRow.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Unspecified(nowUtc) });
            await builtRow.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return references.Count;
    }

    private static void AddArray<T>(NpgsqlCommand command, NpgsqlDbType elementType, T[] values) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | elementType, Value = values });

    private static long ToInt64(decimal value) => value >= long.MaxValue ? long.MaxValue : value <= 0 ? 0 : (long)value;
}
