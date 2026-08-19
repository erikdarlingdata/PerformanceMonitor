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
/// Per-table autovacuum state — whether autovacuum is keeping up, table by table.
/// <para>Dead tuples on their own are not a finding. Every PostgreSQL table has some, and the number
/// that matters is not the count but the count <i>relative to the threshold that triggers a vacuum</i>:
/// autovacuum fires at <c>autovacuum_vacuum_threshold + autovacuum_vacuum_scale_factor * reltuples</c>,
/// so 500,000 dead tuples is routine on a 50-million-row table and a five-alarm fire on a 10,000-row
/// one. This collector computes that threshold per table and stores it alongside the counts, which is
/// what turns a number nobody can act on into a ratio anybody can.</para>
/// <para>The threshold has to be computed per table rather than read from the GUCs, because
/// <c>ALTER TABLE ... SET (autovacuum_vacuum_scale_factor = ...)</c> is common on exactly the big hot
/// tables where the global default is wrong. Reading the GUC alone would report a threshold the server
/// is not using — worse than reporting none, because it looks authoritative.</para>
/// <para>Runs once per database: <c>pg_stat_user_tables</c> shows only the connected database's tables,
/// with no cross-database equivalent. This is the first PostgreSQL collector on the per-database
/// fan-out path, and on PostgreSQL that path means one connection per database per cycle — which is why
/// the cadence is hourly rather than per-minute.</para>
/// </summary>
public sealed class PgAutovacuumStatsCollector : PostgresCollectorDefinitionBase<PgAutovacuumStatsCollector.Row>
{
    public static PgAutovacuumStatsCollector Instance { get; } = new();

    private PgAutovacuumStatsCollector()
    {
    }

    public readonly record struct Row(
        string SchemaName,
        string TableName,
        long LiveTuples,
        long DeadTuples,
        long ModsSinceAnalyze,
        long InsertsSinceVacuum,
        long VacuumThreshold,
        long InsertVacuumThreshold,
        long AnalyzeThreshold,
        bool AutovacuumDisabled,
        long TotalBytes,
        DateTime? LastVacuum,
        DateTime? LastAutovacuum,
        DateTime? LastAnalyze,
        DateTime? LastAutoanalyze,
        long VacuumCount,
        long AutovacuumCount,
        long AnalyzeCount,
        long AutoanalyzeCount);

    /* Version gating:
         PG13+ : autovacuum_vacuum_insert_threshold / _scale_factor and n_ins_since_vacuum — the
                 insert-only path. Before it, an append-only table was never vacuumed by the dead-tuple
                 rule (it has no dead tuples) and so was never frozen either, which is one of the
                 classic routes to a wraparound emergency. Substituted with -1 below rather than
                 omitted, so the row shape does not change across a mixed-version fleet.

       The reloptions lookups are the reason this query is not two lines. Each per-table override is
       read out of pg_class.reloptions via pg_options_to_table() and falls back to the GUC, mirroring
       exactly what the autovacuum launcher itself does. Parsing the raw text[] instead would mean
       re-implementing the option syntax; pg_options_to_table is the server's own parser and needs no
       special grant.

       reltuples is -1, not 0, on a table that has never been analyzed (PG14+ distinguishes "empty"
       from "unknown"). GREATEST(...,0) keeps that from producing a NEGATIVE threshold, which would
       make a never-analyzed table look permanently overdue.
       All four maintenance timestamps are `timestamp with time zone` and are converted with
       AT TIME ZONE 'UTC' rather than ::timestamp. The cast form renders the instant in the SESSION's
       TimeZone before dropping the offset, so it agrees with UTC only while every parameter group says
       UTC — true across this fleet today, which is exactly what makes the bug invisible until it is not.
       The store contract is naive UTC product-wide, so the conversion has to be explicit. */
    private static string BuildQueryText(int postgresMajorVersion)
    {
        var supportsInsertThreshold = postgresMajorVersion >= 13;

        var insertsSinceVacuum = supportsInsertThreshold ? "t.n_ins_since_vacuum::bigint" : "-1::bigint";
        var insertThreshold = supportsInsertThreshold
            ? @"(coalesce(
                   (SELECT option_value FROM pg_options_to_table(c.reloptions)
                    WHERE option_name = 'autovacuum_vacuum_insert_threshold')::bigint,
                   current_setting('autovacuum_vacuum_insert_threshold')::bigint)
                + coalesce(
                   (SELECT option_value FROM pg_options_to_table(c.reloptions)
                    WHERE option_name = 'autovacuum_vacuum_insert_scale_factor')::float8,
                   current_setting('autovacuum_vacuum_insert_scale_factor')::float8)
                  * GREATEST(c.reltuples, 0))::bigint"
            : "-1::bigint";

        /* Only tables with pending work. A table with no dead tuples, no modifications since its last
           analyze, and no inserts since its last vacuum has had no writes since maintenance last ran:
           there is nothing for autovacuum to do and nothing to report, and on a database with thousands
           of mostly-static tables those rows would be the overwhelming majority of the volume.

           The insert clause is not optional decoration. An append-only table has NO dead tuples and NO
           modifications, so the first two predicates both miss it — and an append-only table that never
           gets vacuumed is never frozen either, which is one of the classic routes into a wraparound
           emergency. Filtering on dead tuples alone would drop exactly the tables whose risk this
           collector is meant to surface.

           autovacuum_enabled = false is kept regardless of activity: a table with autovacuum switched
           off is a finding even while it is momentarily clean. */
        var insertActivityClause = supportsInsertThreshold ? "OR t.n_ins_since_vacuum > 0" : string.Empty;
        return $@"
SELECT
    t.schemaname                                                     AS schema_name,
    t.relname                                                        AS table_name,
    t.n_live_tup::bigint                                             AS live_tuples,
    t.n_dead_tup::bigint                                             AS dead_tuples,
    t.n_mod_since_analyze::bigint                                    AS mods_since_analyze,
    {insertsSinceVacuum}                                             AS inserts_since_vacuum,
    (coalesce(
        (SELECT option_value FROM pg_options_to_table(c.reloptions)
         WHERE option_name = 'autovacuum_vacuum_threshold')::bigint,
        current_setting('autovacuum_vacuum_threshold')::bigint)
     + coalesce(
        (SELECT option_value FROM pg_options_to_table(c.reloptions)
         WHERE option_name = 'autovacuum_vacuum_scale_factor')::float8,
        current_setting('autovacuum_vacuum_scale_factor')::float8)
       * GREATEST(c.reltuples, 0))::bigint                           AS vacuum_threshold,
    {insertThreshold}                                                AS insert_vacuum_threshold,
    (coalesce(
        (SELECT option_value FROM pg_options_to_table(c.reloptions)
         WHERE option_name = 'autovacuum_analyze_threshold')::bigint,
        current_setting('autovacuum_analyze_threshold')::bigint)
     + coalesce(
        (SELECT option_value FROM pg_options_to_table(c.reloptions)
         WHERE option_name = 'autovacuum_analyze_scale_factor')::float8,
        current_setting('autovacuum_analyze_scale_factor')::float8)
       * GREATEST(c.reltuples, 0))::bigint                           AS analyze_threshold,
    coalesce(
        (SELECT lower(option_value) = 'false' FROM pg_options_to_table(c.reloptions)
         WHERE option_name = 'autovacuum_enabled'), false)           AS autovacuum_disabled,
    pg_total_relation_size(t.relid)::bigint                          AS total_bytes,
    (t.last_vacuum AT TIME ZONE 'UTC')                               AS last_vacuum,
    (t.last_autovacuum AT TIME ZONE 'UTC')                           AS last_autovacuum,
    (t.last_analyze AT TIME ZONE 'UTC')                              AS last_analyze,
    (t.last_autoanalyze AT TIME ZONE 'UTC')                          AS last_autoanalyze,
    t.vacuum_count::bigint                                           AS vacuum_count,
    t.autovacuum_count::bigint                                       AS autovacuum_count,
    t.analyze_count::bigint                                          AS analyze_count,
    t.autoanalyze_count::bigint                                      AS autoanalyze_count
FROM pg_stat_user_tables AS t
JOIN pg_class AS c
  ON c.oid = t.relid
WHERE (
        t.n_dead_tup > 0
     OR t.n_mod_since_analyze > 0
     {insertActivityClause}
     OR coalesce((SELECT lower(option_value) = 'false' FROM pg_options_to_table(c.reloptions)
                  WHERE option_name = 'autovacuum_enabled'), false)
      )
ORDER BY t.n_dead_tup DESC";
    }

    public override string Name => "pg_autovacuum_stats";

    public override string TargetTable => "pg_autovacuum_stats";

    /// <summary>
    /// Writers only. This is not a permissions or availability gate — the view is perfectly readable on a
    /// standby — it is that on a standby every counter it reports is ZERO.
    /// <para>Measured on Aurora PostgreSQL 17.7, same cluster, same database, same 15 tables: the writer
    /// reported 13,654,458 dead tuples and 150,790,506 live tuples, while the reader reported 0 for
    /// n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum AND n_live_tup. These are the writer's stats
    /// collector's numbers and they are not replicated.</para>
    /// <para>Left ungated, a reader target would produce zero rows, the activity filter would read that as
    /// "no table has pending work", and the tool would report perfect autovacuum health for a cluster
    /// 13 million dead tuples behind. A confidently wrong healthy answer is worse than no answer, which is
    /// why this gates rather than collecting and hoping the consumer notices.</para>
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsInRecovery;

    /// <summary>
    /// <c>pg_stat_user_tables</c> is scoped to the connected database and PostgreSQL has no
    /// cross-database read, so this is necessarily a fan-out.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context)
        => new(BuildQueryText(context.Target.PostgresMajorVersion));

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        /* Not read from the result set: the per-database loop sets CurrentDatabaseName, and the
           connection's database IS the row's database, so it is authoritative here in a way a value
           parsed out of the payload could not be. */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("live_tuples", CollectorColumnType.BigInt),
        new CollectorColumn("dead_tuples", CollectorColumnType.BigInt),
        new CollectorColumn("mods_since_analyze", CollectorColumnType.BigInt),
        /* -1 = the server predates the insert-only vacuum rule (PG13). */
        new CollectorColumn("inserts_since_vacuum", CollectorColumnType.BigInt),
        /* The whole point of the collector: the count is meaningless without the line it has to cross. */
        new CollectorColumn("vacuum_threshold", CollectorColumnType.BigInt),
        new CollectorColumn("insert_vacuum_threshold", CollectorColumnType.BigInt),
        new CollectorColumn("analyze_threshold", CollectorColumnType.BigInt),
        new CollectorColumn("autovacuum_disabled", CollectorColumnType.Boolean),
        new CollectorColumn("total_bytes", CollectorColumnType.BigInt),
        new CollectorColumn("last_vacuum", CollectorColumnType.Timestamp),
        new CollectorColumn("last_autovacuum", CollectorColumnType.Timestamp),
        new CollectorColumn("last_analyze", CollectorColumnType.Timestamp),
        new CollectorColumn("last_autoanalyze", CollectorColumnType.Timestamp),
        new CollectorColumn("vacuum_count", CollectorColumnType.BigInt),
        new CollectorColumn("autovacuum_count", CollectorColumnType.BigInt),
        new CollectorColumn("analyze_count", CollectorColumnType.BigInt),
        new CollectorColumn("autoanalyze_count", CollectorColumnType.BigInt),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                SchemaName: reader.GetString(0),
                TableName: reader.GetString(1),
                LiveTuples: reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                DeadTuples: reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                ModsSinceAnalyze: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                InsertsSinceVacuum: reader.IsDBNull(5) ? -1 : reader.GetInt64(5),
                VacuumThreshold: reader.IsDBNull(6) ? -1 : reader.GetInt64(6),
                InsertVacuumThreshold: reader.IsDBNull(7) ? -1 : reader.GetInt64(7),
                AnalyzeThreshold: reader.IsDBNull(8) ? -1 : reader.GetInt64(8),
                AutovacuumDisabled: !reader.IsDBNull(9) && reader.GetBoolean(9),
                TotalBytes: reader.IsDBNull(10) ? -1 : reader.GetInt64(10),
                LastVacuum: reader.IsDBNull(11) ? null : reader.GetDateTime(11),
                LastAutovacuum: reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                LastAnalyze: reader.IsDBNull(13) ? null : reader.GetDateTime(13),
                LastAutoanalyze: reader.IsDBNull(14) ? null : reader.GetDateTime(14),
                VacuumCount: reader.IsDBNull(15) ? 0 : reader.GetInt64(15),
                AutovacuumCount: reader.IsDBNull(16) ? 0 : reader.GetInt64(16),
                AnalyzeCount: reader.IsDBNull(17) ? 0 : reader.GetInt64(17),
                AutoanalyzeCount: reader.IsDBNull(18) ? 0 : reader.GetInt64(18)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. Every column here is a level or a lifetime count read against a threshold — the
           question is "how far past the line is this table now", not "how much moved since last time".
           The vacuum/analyze counts are cumulative, but their useful reading is the timestamps beside
           them, which say when maintenance last ran without any arithmetic. */
        writer
            .Value(context.CurrentDatabaseName)
            .Value(row.SchemaName)
            .Value(row.TableName)
            .Value(row.LiveTuples)
            .Value(row.DeadTuples)
            .Value(row.ModsSinceAnalyze)
            .Value(row.InsertsSinceVacuum)
            .Value(row.VacuumThreshold)
            .Value(row.InsertVacuumThreshold)
            .Value(row.AnalyzeThreshold)
            .Value(row.AutovacuumDisabled)
            .Value(row.TotalBytes)
            .Value(row.LastVacuum)
            .Value(row.LastAutovacuum)
            .Value(row.LastAnalyze)
            .Value(row.LastAutoanalyze)
            .Value(row.VacuumCount)
            .Value(row.AutovacuumCount)
            .Value(row.AnalyzeCount)
            .Value(row.AutoanalyzeCount);
    }
}
