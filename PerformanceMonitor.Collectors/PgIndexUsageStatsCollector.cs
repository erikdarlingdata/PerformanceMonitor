/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-index usage — is this index earning its keep (#2541).
/// <para><b>An unused index costs more on PostgreSQL than on SQL Server</b>, which is the reason this is
/// not simply <c>get_index_usage</c>'s twin. On both engines a never-scanned index costs write throughput
/// and space. On PostgreSQL it ALSO participates in every <c>VACUUM</c>: index cleanup is a large share of
/// vacuum work, so a dead index directly slows the maintenance that <see cref="PgAutovacuumStatsCollector"/>
/// and <see cref="PgWraparoundStatsCollector"/> exist to watch. A dead index on a hot table is a vacuum
/// problem as much as a write problem.</para>
///
/// <para><b>What is collected is deliberately more than <c>idx_scan</c>.</b> "Nobody scans it" is the easy
/// half; "and it is safe to drop" is the half that has to survive contact with a real schema, and the
/// catalog facts that decide it are cheap to carry and impossible to reconstruct later. So every row
/// carries whether the index backs a constraint, is a primary key, is unique, is the replica identity, is
/// partial, is an expression index, and its full <c>pg_get_indexdef</c> text. Without those a read can only
/// say "unused", and <b>"unused" is not "droppable"</b> — see the note on
/// <see cref="PayloadColumns"/>.</para>
///
/// <para><b><c>stats_reset</c> travels with every row</b> rather than being joined from
/// <see cref="PgDatabaseStatsCollector"/> at read time. <c>idx_scan</c> is cumulative since the statistics
/// were last reset, so "0 scans" means nothing without the reset timestamp beside it: reporting an index
/// whose database was reset an hour ago as unused is exactly the wrong advice, and it is the advice a
/// scan count alone produces. The value is per database and identical for every row of one collection, so
/// carrying it costs one scalar subquery per cycle and makes the row self-contained — a cross-collector
/// join could not be relied on, because <c>pg_database_stats</c> is a separate collector that can be
/// disabled, gated off, or simply not have a sample in the same window.</para>
///
/// <para>Runs once per database: <c>pg_stat_user_indexes</c> shows only the connected database's indexes,
/// with no cross-database equivalent — the same constraint that makes
/// <see cref="PgAutovacuumStatsCollector"/> a fan-out. The CADENCE, though, is inherited from
/// <c>index_object_stats</c> — the SQL Server collector answering the same question — and is DAILY rather
/// than that sibling's hourly: "has anything scanned this index" is a structural question about a schema,
/// not a rate, so an hourly sample would re-record the same catalog facts 24 times a day and pay 24x the
/// fan-out connections to do it.</para>
/// </summary>
public sealed class PgIndexUsageStatsCollector : PostgresCollectorDefinitionBase<PgIndexUsageStatsCollector.Row>
{
    public static PgIndexUsageStatsCollector Instance { get; } = new();

    private PgIndexUsageStatsCollector()
    {
    }

    /// <summary>
    /// Indexes at or above this size are collected. Below it there is nothing to model: the entire argument
    /// for dropping an unused index is the write amplification, the space, and the vacuum cleanup it costs,
    /// and all three scale with its size. 64 KB is eight pages — an empty btree is one page and a btree over
    /// a few thousand narrow rows is well under this, so the floor removes the rows whose finding would be
    /// "you could reclaim 16 KB" without removing any index whose removal would be worth doing.
    /// <para>This is a fan-out cost control, and it is the whole of one: the alternative of collecting every
    /// index in every database scales with schema size on exactly the large clusters where the fan-out is
    /// already the expensive part (#2468 was this shape for query_store). An INVALID index is kept
    /// regardless of size — a failed <c>CREATE INDEX CONCURRENTLY</c> is a finding at any size, because the
    /// planner will not use it while writes still maintain it.</para>
    /// </summary>
    internal const long MinimumIndexBytes = 65536;

    public readonly record struct Row(
        string SchemaName,
        string TableName,
        string IndexName,
        long IndexScans,
        long TuplesRead,
        long TuplesFetched,
        long BlocksRead,
        long BlocksHit,
        long IndexBytes,
        long TableBytes,
        bool IsUnique,
        bool IsPrimaryKey,
        bool IsValid,
        bool IsReady,
        bool IsReplicaIdentity,
        bool IsPartial,
        bool IsExpression,
        bool SupportsConstraint,
        string IndexMethod,
        int ColumnCount,
        string IndexDefinition,
        DateTime? LastScan,
        DateTime? StatsReset);

    /* VERSION FLOORS, measured against live PostgreSQL 13, 14, 15, 16, 17 and 18 rather than read out of the
       documentation, by listing the actual columns of the actual catalogs on each major. Exactly one column
       selected here has a floor:

         last_idx_scan                                     : 16+.  ABSENT on 13/14/15 — verified by running
                                                             the gated-ON form against each and getting
                                                             ERROR: column i.last_idx_scan does not exist.
                                                             That error fails the WHOLE collection, every
                                                             cycle, which is why this is substituted rather
                                                             than simply selected.

       Everything else was confirmed PRESENT on all six majors, so it is stated as checked rather than
       assumed: schemaname/relname/indexrelname/relid/indexrelid/idx_scan/idx_tup_read/idx_tup_fetch on
       pg_stat_user_indexes, idx_blks_read/idx_blks_hit on pg_statio_user_indexes, indisunique/indisprimary/
       indisvalid/indisready/indisreplident/indpred/indexprs/indnatts on pg_index, conindid on pg_constraint,
       amname on pg_am, and pg_get_indexdef()/pg_relation_size().

       last_idx_scan is substituted with a typed NULL rather than omitted, so the row SHAPE does not change
       across a mixed-version fleet — the same rule PgAutovacuumStatsCollector follows for the PG13
       insert-vacuum columns. NULL is the honest value: on PG15 the server genuinely does not know when the
       index was last scanned. A sentinel timestamp would have to be a real instant and would read as one.

       last_idx_scan is `timestamp with time zone`; AT TIME ZONE 'UTC' rather than ::timestamp, because the
       cast renders in the SESSION's TimeZone and the store contract is naive UTC.

       pg_relation_size is evaluated inside the subquery so it is computed ONCE per index rather than
       re-evaluated by the filter and the ordering. The outer WHERE reads the computed column. */
    internal static string BuildQueryText(int postgresMajorVersion)
    {
        var lastScan = postgresMajorVersion >= 16
            ? "(i.last_idx_scan AT TIME ZONE 'UTC')"
            : "NULL::timestamp";

        return $@"
SELECT
    s.schema_name,
    s.table_name,
    s.index_name,
    s.index_scans,
    s.tuples_read,
    s.tuples_fetched,
    s.blocks_read,
    s.blocks_hit,
    s.index_bytes,
    s.table_bytes,
    s.is_unique,
    s.is_primary_key,
    s.is_valid,
    s.is_ready,
    s.is_replica_identity,
    s.is_partial,
    s.is_expression,
    s.supports_constraint,
    s.index_method,
    s.column_count,
    s.index_definition,
    s.last_scan,
    s.stats_reset
FROM
(
    SELECT
        i.schemaname                                                     AS schema_name,
        i.relname                                                        AS table_name,
        i.indexrelname                                                   AS index_name,
        i.idx_scan::bigint                                               AS index_scans,
        i.idx_tup_read::bigint                                           AS tuples_read,
        i.idx_tup_fetch::bigint                                          AS tuples_fetched,
        coalesce(io.idx_blks_read, 0)::bigint                            AS blocks_read,
        coalesce(io.idx_blks_hit, 0)::bigint                             AS blocks_hit,
        pg_relation_size(i.indexrelid)::bigint                           AS index_bytes,
        pg_relation_size(i.relid)::bigint                                AS table_bytes,
        x.indisunique                                                    AS is_unique,
        x.indisprimary                                                   AS is_primary_key,
        x.indisvalid                                                     AS is_valid,
        x.indisready                                                     AS is_ready,
        x.indisreplident                                                 AS is_replica_identity,
        (x.indpred IS NOT NULL)                                          AS is_partial,
        (x.indexprs IS NOT NULL)                                         AS is_expression,
        EXISTS (SELECT 1 FROM pg_catalog.pg_constraint AS con
                WHERE con.conindid = i.indexrelid)                       AS supports_constraint,
        am.amname                                                        AS index_method,
        x.indnatts::int                                                  AS column_count,
        pg_get_indexdef(i.indexrelid)                                    AS index_definition,
        {lastScan}                                                       AS last_scan,
        ((SELECT d.stats_reset FROM pg_catalog.pg_stat_database AS d
          WHERE d.datname = current_database()) AT TIME ZONE 'UTC')      AS stats_reset
    FROM pg_catalog.pg_stat_user_indexes AS i
    JOIN pg_catalog.pg_index AS x
      ON x.indexrelid = i.indexrelid
    JOIN pg_catalog.pg_class AS ic
      ON ic.oid = i.indexrelid
    JOIN pg_catalog.pg_am AS am
      ON am.oid = ic.relam
    LEFT JOIN pg_catalog.pg_statio_user_indexes AS io
      ON io.indexrelid = i.indexrelid
) AS s
WHERE s.index_bytes >= {MinimumIndexBytes}
   OR NOT s.is_valid
ORDER BY s.index_scans ASC, s.index_bytes DESC";
    }

    public override string Name => "pg_index_usage_stats";

    /// <summary>
    /// <c>pg_index_usage_stats</c> shadows no <c>pg_catalog</c> object. That is checked rather than assumed,
    /// because <c>pg_catalog</c> is searched implicitly and FIRST: a store table named after a catalog view
    /// breaks <c>CREATE INDEX</c> with 42809 and makes every unqualified read resolve to the MONITORING
    /// store's own copy. <c>CollectorTableCatalogShadowTests</c> asserts it against a live catalog.
    /// </summary>
    public override string TargetTable => "pg_index_usage_stats";

    /// <summary>
    /// Writers only, and for exactly the reason <see cref="PgAutovacuumStatsCollector"/> is: on a standby
    /// the counters this collector exists to read are the STANDBY's own, and they are not the writer's.
    /// <para>An index the primary's workload scans a million times an hour reads as <c>idx_scan = 0</c> on a
    /// replica that nobody queries — and this collector's entire output is a judgement about whether a zero
    /// means "drop it". Collecting a replica's zeros would produce a confidently wrong "this index is
    /// unused" for an index that is load-bearing on the writer, which is the single worst answer this
    /// surface can give. Gating off is the safe direction here: a missing answer sends someone looking,
    /// a wrong one sends them to <c>DROP INDEX</c>.</para>
    /// <para>This differs from <see cref="PgBlockingCollector"/> and <see cref="PgIoStatsCollector"/>, which
    /// run ON standbys because the standby's own numbers are the finding there. Copying either gate by
    /// reflex is the mistake; the question is whose counters answer the question being asked.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsInRecovery;

    /// <summary>
    /// <c>pg_stat_user_indexes</c> is scoped to the connected database and PostgreSQL has no cross-database
    /// read, so this is necessarily a fan-out — the second one, after
    /// <see cref="PgAutovacuumStatsCollector"/>. On PostgreSQL a fan-out is one CONNECTION per database per
    /// cycle, which is why the schedule is DAILY here rather than that collector's hourly; see the cadence
    /// note in the class remarks.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(BuildQueryText(context.Target.PostgresMajorVersion));

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* Not read from the result set: the per-database loop sets CurrentDatabaseName, and the connection's
           database IS the row's database, so it is authoritative here in a way a parsed value would not be.
           Same reasoning as PgAutovacuumStatsCollector. */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("index_name", CollectorColumnType.Varchar),
        /* The three usage counters, cumulative since stats_reset. idx_scan is the headline; the other two
           are what separates a useful index from a badly-shaped one. An index with a huge tuples_read and a
           small tuples_fetched is being scanned widely and returning little - a candidate for a better
           definition rather than for a DROP, which is a different finding with a different fix. */
        new CollectorColumn("index_scans", CollectorColumnType.BigInt),
        new CollectorColumn("tuples_read", CollectorColumnType.BigInt),
        new CollectorColumn("tuples_fetched", CollectorColumnType.BigInt),
        /* From pg_statio_user_indexes. These accrue on WRITES as well as reads, which is what makes them
           worth carrying next to idx_scan: an index with zero scans and millions of block hits is being
           maintained by the write path and read by nobody, and that is the cost being paid stated in the
           server's own units rather than inferred from the index's size. */
        new CollectorColumn("blocks_read", CollectorColumnType.BigInt),
        new CollectorColumn("blocks_hit", CollectorColumnType.BigInt),
        /* index_bytes is what a DROP reclaims. table_bytes is beside it so the read can express the index
           as a share of its table - an unused index bigger than the table it indexes reads very differently
           from one that is 1% of it, and neither number alone says which this is. */
        new CollectorColumn("index_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("table_bytes", CollectorColumnType.BigInt),

        /* Everything from here down exists so a read can say what an index CANNOT be dropped for. This is
           the substance of the collector, not metadata around it: idx_scan = 0 is one fact, and on its own
           it produces advice that breaks schemas.

           is_unique / is_primary_key / supports_constraint : dropping it drops the CONSTRAINT. A unique
               index backing a UNIQUE or PRIMARY KEY constraint enforces correctness whether or not the
               planner ever chooses it for a scan, and idx_scan does not count constraint enforcement - a
               uniqueness check is not a scan of the kind this counter increments. So the single most
               common "unused index" on any schema is one that must never be dropped, and nothing in the
               usage counters can tell you so.
           is_replica_identity : dropping it breaks logical replication of UPDATE and DELETE for the table.
           is_partial / is_expression : these read as unused when the planner cannot MATCH them, which is a
               different condition with a different fix. index_definition is carried so a human can tell
               "nobody needs this" from "the predicate no longer matches how the query is written".
           is_valid / is_ready : an invalid index is a failed CREATE INDEX CONCURRENTLY. The planner will
               not use it, so it reports zero scans forever, while writes still maintain it - the one case
               where "unused" and "drop it" genuinely coincide, and it is invisible without this column. */
        new CollectorColumn("is_unique", CollectorColumnType.Boolean),
        new CollectorColumn("is_primary_key", CollectorColumnType.Boolean),
        new CollectorColumn("is_valid", CollectorColumnType.Boolean),
        new CollectorColumn("is_ready", CollectorColumnType.Boolean),
        new CollectorColumn("is_replica_identity", CollectorColumnType.Boolean),
        new CollectorColumn("is_partial", CollectorColumnType.Boolean),
        new CollectorColumn("is_expression", CollectorColumnType.Boolean),
        new CollectorColumn("supports_constraint", CollectorColumnType.Boolean),
        /* btree/hash/gin/gist/brin/spgist. A GIN index's idx_scan means something different from a btree's,
           and a BRIN index is small by construction so the size argument for dropping it barely applies. */
        new CollectorColumn("index_method", CollectorColumnType.Varchar),
        new CollectorColumn("column_count", CollectorColumnType.Integer),
        /* The full definition, so the read can show what the index actually IS. This is what turns
           "gadget_partial_idx is unused" into a decision somebody can make without opening psql. */
        new CollectorColumn("index_definition", CollectorColumnType.Varchar),
        /* NULL on PG15 and below - the server does not record it. On 16+ it is the difference between "never
           scanned since the reset" and "last scanned in March", which is the distinction that decides
           whether a zero is evidence of anything. */
        new CollectorColumn("last_scan", CollectorColumnType.Timestamp),
        /* The denominator for every counter in the row. NULL means never reset, which is the ordinary state
           and means the counters run back to the start of the statistics system - NOT that the value is
           unknown. The read has to say which of those it is looking at. */
        new CollectorColumn("stats_reset", CollectorColumnType.Timestamp),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                SchemaName: reader.GetString(0),
                TableName: reader.GetString(1),
                IndexName: reader.GetString(2),
                IndexScans: Long(reader, 3),
                TuplesRead: Long(reader, 4),
                TuplesFetched: Long(reader, 5),
                BlocksRead: Long(reader, 6),
                BlocksHit: Long(reader, 7),
                IndexBytes: Long(reader, 8),
                TableBytes: Long(reader, 9),
                IsUnique: Bool(reader, 10),
                IsPrimaryKey: Bool(reader, 11),
                IsValid: Bool(reader, 12),
                IsReady: Bool(reader, 13),
                IsReplicaIdentity: Bool(reader, 14),
                IsPartial: Bool(reader, 15),
                IsExpression: Bool(reader, 16),
                SupportsConstraint: Bool(reader, 17),
                IndexMethod: reader.IsDBNull(18) ? string.Empty : reader.GetString(18),
                ColumnCount: reader.IsDBNull(19) ? 0 : reader.GetInt32(19),
                IndexDefinition: reader.IsDBNull(20) ? string.Empty : reader.GetString(20),
                /* Genuinely nullable, both of them, and NULL means something specific in each case:
                   last_scan is NULL on PG15 and below (not recorded) and on 16+ for an index never scanned
                   since the counters were reset; stats_reset is NULL until the first reset ever. A -1
                   sentinel is not available to a timestamp and a zero date would read as a real instant. */
                LastScan: reader.IsDBNull(21) ? null : reader.GetDateTime(21),
                StatsReset: reader.IsDBNull(22) ? null : reader.GetDateTime(22)));
        }

        return rows;

        /* The counters are NOT NULL in the catalog, so a NULL here would be a shape change rather than a
           value: 0 is the correct reading of "absent" for a cumulative count that has never moved. This is
           the opposite choice from PgDatabaseStatsCollector, where the columns really can be NULL and the
           NULL has to survive the differencing. */
        static long Long(DbDataReader r, int ordinal) => r.IsDBNull(ordinal) ? 0 : r.GetInt64(ordinal);

        static bool Bool(DbDataReader r, int ordinal) => !r.IsDBNull(ordinal) && r.GetBoolean(ordinal);
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas stored. The counters are cumulative and are stored RAW, so every window stays
           recomputable at read time and there is no delta state to fall out of step with the samples - the
           same rule PgIoStatsCollector and PgDatabaseStatsCollector follow. It also means the read can
           answer both questions the data supports: the lifetime count ("never scanned since the reset") and
           the windowed one ("not scanned in the last 30 days"), which is the more useful of the two and is
           only available because the store keeps history the server does not. */
        writer
            .Value(context.CurrentDatabaseName)
            .Value(row.SchemaName)
            .Value(row.TableName)
            .Value(row.IndexName)
            .Value(row.IndexScans)
            .Value(row.TuplesRead)
            .Value(row.TuplesFetched)
            .Value(row.BlocksRead)
            .Value(row.BlocksHit)
            .Value(row.IndexBytes)
            .Value(row.TableBytes)
            .Value(row.IsUnique)
            .Value(row.IsPrimaryKey)
            .Value(row.IsValid)
            .Value(row.IsReady)
            .Value(row.IsReplicaIdentity)
            .Value(row.IsPartial)
            .Value(row.IsExpression)
            .Value(row.SupportsConstraint)
            .Value(row.IndexMethod)
            .Value(row.ColumnCount)
            .Value(row.IndexDefinition)
            .Value(row.LastScan)
            .Value(row.StatsReset);
    }
}
