/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Which columns are filtered on, how selectively, and where the planner's estimate was wrong —
/// <c>pg_qualstats</c> (#2603).
///
/// <para><b>What it answers that nothing else here does.</b> <c>pg_index_usage_stats</c> says which indexes
/// were USED; this says which predicates were EVALUATED, including the ones with no index behind them. A
/// column that filters two hundred thousand rows down to a handful, repeatedly, with no index, is the
/// index-candidate signal — and it is invisible to every other collector, because nothing records a scan
/// that had no index to record.</para>
///
/// <para><b><c>sample_count</c> is a SAMPLE, and the rate is stored beside it.</b>
/// <c>pg_qualstats.sample_rate</c> defaults to <c>1/max_connections</c> — <b>0.01</b> on the rig. Measured:
/// <c>pg_qualstats()</c> returned ZERO rows on a server that had just run the very queries it was meant to
/// record, purely because of that default. A reader that treats these counts as complete will conclude a
/// busy server has no interesting predicates, so the rate rides on every row and the panel says what it
/// means.</para>
///
/// <para><b><c>worst_estimate_error_ratio</c> is the reason to look.</b> Measured on the rig: a predicate
/// whose mean estimate error was <b>57.9x</b> sat beside one at 1.04x. The first is a plan built on a wrong
/// row count; the second is the planner doing fine. Selectivity says an index might help, the error ratio
/// says the planner does not understand the column at all — different problems, different fixes.</para>
///
/// <para><b>Per database, and that is load-bearing twice over.</b> The function returns CLUSTER-WIDE rows
/// keyed by <c>dbid</c>, but <c>lrelid</c> and <c>lattnum</c> are OIDs that mean something only inside their
/// own database, while <c>pg_class</c> and <c>pg_attribute</c> are per-database catalogs. Joining without
/// scoping does one of two things depending on OID luck: silently DROPS the other databases' rows, or
/// resolves them to whatever local object happens to share the OID and reports a confident wrong column
/// name. Measured: two rows for other databases, neither colliding locally that day — so the failure would
/// have been silent loss, and a different day would have produced the wrong name. Scoped to the connected
/// database, which is the same fix <c>pg_buffer_usage</c> needed (#2544) and the same class as #2599.</para>
///
/// <para>The extension is per-database too — <c>CREATE EXTENSION</c> in one database does not create the
/// function in another — so databases without it raise <c>42883</c> and degrade to a named non-fatal skip,
/// exactly as <c>pg_index_bloat</c> does without pgstattuple.</para>
/// </summary>
public sealed class PgPredicateStatsCollector : PostgresCollectorDefinitionBase<PgPredicateStatsCollector.Row>
{
    public static readonly PgPredicateStatsCollector Instance = new();

    private PgPredicateStatsCollector()
    {
    }

    /// <param name="SampleCount">Occurrences OBSERVED, which is a sample of the real number. Divide nothing
    /// by this without reading <paramref name="SampleRate"/> first.</param>
    /// <param name="RowsFiltered">Rows the predicate discarded. High against
    /// <paramref name="RowsEvaluated"/> with no index is the index-candidate signal.</param>
    /// <param name="WorstEstimateErrorRatio">How far the planner's row estimate was from reality, at its
    /// worst. A large value means the plan was built on a wrong number.</param>
    /// <param name="SampleRate">What fraction of executions the extension recorded. Defaults to
    /// <c>1/max_connections</c>, so it is USUALLY not 1.</param>
    public readonly record struct Row(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? ColumnName,
        string? Operator,
        long QueryId,
        long SampleCount,
        long RowsEvaluated,
        long RowsFiltered,
        double WorstEstimateErrorRatio,
        double SampleRate);

    /* public.pg_qualstats(), not pg_catalog - an extension function lives in the schema the extension was
       created in, the correction #2561 needed for pgstatindex.

       The dbid filter is the whole reason this runs per database: see the type header. Dropping it does not
       widen the result, it corrupts it.

       ORDER BY rows filtered, because that is the index-candidate signal. Ordering by sample_count would
       rank by how often the SAMPLER happened to fire. */
    private const string QueryText = @"
SELECT
    current_database()::text                                  AS database_name,
    n.nspname::text                                           AS schema_name,
    c.relname::text                                           AS table_name,
    a.attname::text                                           AS column_name,
    o.oprname::text                                           AS operator,
    q.queryid::bigint                                         AS query_id,
    sum(q.occurences)::bigint                                 AS sample_count,
    sum(q.execution_count)::bigint                            AS rows_evaluated,
    sum(q.nbfiltered)::bigint                                 AS rows_filtered,
    max(q.mean_err_estimate_ratio)::double precision          AS worst_estimate_error_ratio,
    coalesce(
        nullif(current_setting('pg_qualstats.sample_rate', true), '')::double precision,
        1.0)                                                  AS sample_rate
FROM public.pg_qualstats() AS q
JOIN pg_catalog.pg_class AS c
  ON c.oid = q.lrelid
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = c.relnamespace
JOIN pg_catalog.pg_attribute AS a
  ON  a.attrelid = q.lrelid
  AND a.attnum = q.lattnum
JOIN pg_catalog.pg_operator AS o
  ON o.oid = q.opno
WHERE q.dbid = (SELECT oid FROM pg_catalog.pg_database WHERE datname = current_database())
AND   n.nspname NOT IN ('pg_catalog', 'information_schema')
GROUP BY n.nspname, c.relname, a.attname, o.oprname, q.queryid
ORDER BY sum(q.nbfiltered) DESC, sum(q.execution_count) DESC
LIMIT 500";

    public override string Name => "pg_predicate_stats";

    public override string TargetTable => "pg_predicate_stats";

    /// <summary>Any PostgreSQL target; an absent extension degrades to a named non-fatal skip.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>
    /// Per database, because the OIDs the rows carry are only meaningful in their own database and the
    /// catalogs that resolve them are per-database. See the type header for what happens without this.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("column_name", CollectorColumnType.Varchar),
        /* The operator SYMBOL, not its OID: '~~' is LIKE and '>' is a range scan, and which one it is
           changes whether an index would even help. An OID here would need a second lookup to mean
           anything, in a catalog that is per-database. */
        new CollectorColumn("operator", CollectorColumnType.Varchar),
        new CollectorColumn("query_id", CollectorColumnType.BigInt),
        new CollectorColumn("sample_count", CollectorColumnType.BigInt),
        new CollectorColumn("rows_evaluated", CollectorColumnType.BigInt),
        new CollectorColumn("rows_filtered", CollectorColumnType.BigInt),
        new CollectorColumn("worst_estimate_error_ratio", CollectorColumnType.Double),
        /* Stored per row rather than once per collection: the setting is reloadable, so two rows in the
           same table can genuinely have been sampled at different rates. */
        new CollectorColumn("sample_rate", CollectorColumnType.Double),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                DatabaseName: reader.IsDBNull(0) ? null : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? null : reader.GetString(1),
                TableName: reader.IsDBNull(2) ? null : reader.GetString(2),
                ColumnName: reader.IsDBNull(3) ? null : reader.GetString(3),
                Operator: reader.IsDBNull(4) ? null : reader.GetString(4),
                QueryId: reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                SampleCount: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                RowsEvaluated: reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                RowsFiltered: reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                WorstEstimateErrorRatio: reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
                /* Defaulted to 1.0, not 0: a zero rate would make any reader scaling by it divide by zero
                   or multiply the workload to nothing. 1.0 says "assume complete", which is the safe
                   direction to be wrong in - it under-claims rather than inventing volume. */
                SampleRate: reader.IsDBNull(10) ? 1.0 : reader.GetDouble(10)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DatabaseName)
            .Value(row.SchemaName)
            .Value(row.TableName)
            .Value(row.ColumnName)
            .Value(row.Operator)
            .Value(row.QueryId)
            .Value(row.SampleCount)
            .Value(row.RowsEvaluated)
            .Value(row.RowsFiltered)
            .Value(row.WorstEstimateErrorRatio)
            .Value(row.SampleRate);
    }
}
