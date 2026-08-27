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
/// Per-column planner statistics — <c>pg_stats</c> (#2543), the companion to plan capture.
///
/// <para>A plan says what the planner DID. These say why it thought that was reasonable. For the commonest
/// class of PostgreSQL performance problem — a bad row estimate producing a nested loop over millions of
/// rows — the plan shows the symptom and this shows the cause.</para>
///
/// <para><b>The value columns are deliberately NOT collected, and that is the central decision here.</b>
/// <c>most_common_vals</c> and <c>histogram_bounds</c> contain raw column values. Measured on a table with
/// realistic columns, they came back as <c>{"Ana Ruiz","Cy Patel",...}</c>, an identifier fragment, and a
/// list of live email addresses. Collecting them copies customer data out of the monitored database into the
/// monitoring store, under OUR retention rather than the customer's — the same exposure as the
/// <c>auto_explain</c> literal-value problem on #2538, and not something to discover after it ships.</para>
///
/// <para><b>Every finding this is wanted for survives without them.</b> <c>most_common_freqs</c> is
/// frequencies only, no values, and skew is the whole parameter-sensitivity signal: measured, a status
/// column whose top value covers 59.85% of the table is parameter-sensitive, and knowing WHICH value it is
/// adds nothing to that conclusion. Anyone who needs the value can run one query against their own database,
/// where the data already lives. <c>histogram_bounds</c> was also the largest column by bytes (3,251 against
/// 228 for the MCV array and 140 for the frequencies), so dropping both is the cheaper option as well as the
/// safer one.</para>
///
/// <para><b><c>n_distinct</c> is not a count when it is negative.</b> <c>-1</c> means "distinct ≈ every
/// row"; any negative is a RATIO of the row count rather than a quantity. It is stored as a real, and any
/// read that formats it has to branch on the sign or it will report "-1 distinct values".</para>
///
/// <para><b>Two ways this legitimately returns nothing, and they are different.</b> <c>pg_stats</c> filters
/// on <c>has_column_privilege</c>, so a monitoring role without SELECT on a table sees no rows for it —
/// measured: a <c>pg_monitor</c>-only role gets ZERO rows where a superuser gets all of them, and
/// <c>pg_statistic</c> underneath is permission-denied outright. That is why Datadog ships a
/// <c>SECURITY DEFINER</c> helper. Row-level security empties the view too, via the third conjunct of the
/// same filter. This slice deliberately does NOT install a helper: it collects what the granted role can
/// see, and records the shortfall so the read can say which of the two is in force rather than reporting an
/// absence of statistics as an absence of problems.</para>
///
/// <para>Per-database, because <c>pg_stats</c> describes the connected database only.</para>
/// </summary>
public sealed class PgColumnStatsCollector : PostgresCollectorDefinitionBase<PgColumnStatsCollector.Row>
{
    public static PgColumnStatsCollector Instance { get; } = new();

    private PgColumnStatsCollector()
    {
    }

    /// <param name="DatabaseName">The database these statistics describe. Load-bearing, not decorative:
    /// this collector runs once per database, and <c>pg_stats</c> describes the connected database only, so
    /// without it rows from two databases that share a schema collide indistinguishably (#2599).</param>
    /// <param name="NDistinct">Negative values are a RATIO of row count, not a quantity. See the type
    /// header.</param>
    /// <param name="Correlation">Physical/logical ordering correlation. Near zero is why an index scan was
    /// rejected on a column that "obviously" has an index.</param>
    /// <param name="TopValueFrequency">The share of the table held by the single most common value, from
    /// <c>most_common_freqs[1]</c>. The parameter-sensitivity signal, carrying no value itself.</param>
    /// <param name="CommonValueCount">How many entries the MCV list holds — the shape of the skew, again
    /// without the values.</param>
    public readonly record struct Row(
        string? DatabaseName,
        string? SchemaName,
        string? TableName,
        string? ColumnName,
        float? NDistinct,
        float? NullFrac,
        int? AvgWidth,
        float? Correlation,
        float? TopValueFrequency,
        int? CommonValueCount);

    /* Restricted to tables above a size floor. Statistics on a 40-page table cannot produce a misestimate
       anyone will ever notice - the planner is choosing between two cheap paths - so collecting every column
       of every tiny table would multiply the row count by the long tail of the schema for no finding. 128
       pages is 1 MB at the default block size.

       most_common_freqs[1] rather than the whole array: the first element IS the skew signal, and storing
       the array would be storing a distribution nobody reads to more depth than its head.

       cardinality(most_common_vals) reads only the LENGTH of that array and never its contents, which is the
       one thing worth knowing about it - how many values dominate - without copying any of them.

       pg_stats is a view over pg_statistic filtered by has_column_privilege, so this returns only what the
       monitoring role may read. That is a feature of the view rather than something to work around here;
       the shortfall is reported by the read instead. */
    private const string QueryText = @"
SELECT
    current_database()::text                AS database_name,
    s.schemaname::text                      AS schema_name,
    s.tablename::text                       AS table_name,
    s.attname::text                         AS column_name,
    s.n_distinct                            AS n_distinct,
    s.null_frac                             AS null_frac,
    s.avg_width                             AS avg_width,
    s.correlation                           AS correlation,
    /* The head of the frequency array only. NULL when the column has no MCV list at all, which is itself
       informative - a perfectly uniform column has none. */
    s.most_common_freqs[1]                  AS top_value_frequency,
    cardinality(s.most_common_vals)         AS common_value_count
FROM pg_catalog.pg_stats AS s
JOIN pg_catalog.pg_class AS c
  ON c.relname = s.tablename
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = c.relnamespace
 AND n.nspname = s.schemaname
WHERE s.schemaname NOT IN ('pg_catalog', 'information_schema')
AND   c.relkind IN ('r', 'm', 'p')
AND   c.relpages >= 128
ORDER BY c.relpages DESC, s.schemaname, s.tablename, s.attname
/* Bounded (#2617's lesson applied before it bites). This is a catalog read rather than a page scan, so
   it is nowhere near as costly as pg_index_bloat was - but it was still unbounded, and row count here is
   tables x columns: 361 tables over the size floor on one measured target, and a wide schema turns that
   into five figures per database per DAY. The ORDER BY already puts the biggest tables first, so the cap
   keeps exactly the columns a plan-shape question is asked about. */
LIMIT 5000";

    public override string Name => "pg_column_stats";

    public override string TargetTable => "pg_column_stats";

    /// <summary>
    /// Every PostgreSQL target. <c>pg_stats</c> is core, and a standby's copy is the primary's — replicated
    /// with the rest of <c>pg_statistic</c> — so the answer is valid there and worth having when the
    /// question is why a read replica chose a bad plan.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    /// <summary>
    /// Per-database: <c>pg_stats</c> describes the CONNECTED database only. A cluster-wide claim built from
    /// one database's statistics would be silently missing every table in every other one.
    /// </summary>
    public override bool RunsPerDatabase(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("schema_name", CollectorColumnType.Varchar),
        new CollectorColumn("table_name", CollectorColumnType.Varchar),
        new CollectorColumn("column_name", CollectorColumnType.Varchar),
        /* Stored as a floating type, NOT an integer count. Negative values are a ratio of row count and an
           integer column would either reject them or, worse, keep them and let a reader render "-1 distinct
           values". */
        new CollectorColumn("n_distinct", CollectorColumnType.Double),
        new CollectorColumn("null_frac", CollectorColumnType.Double),
        new CollectorColumn("avg_width", CollectorColumnType.Integer),
        /* Near zero explains a rejected index scan on an indexed column - the single most common "why did it
           seq scan" answer that a plan alone cannot give. */
        new CollectorColumn("correlation", CollectorColumnType.Double),
        /* The skew signal, carrying no customer value. See the type header for why the array it comes from
           is deliberately not stored. */
        new CollectorColumn("top_value_frequency", CollectorColumnType.Double),
        new CollectorColumn("common_value_count", CollectorColumnType.Integer),
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
                /* Nullable throughout and never a sentinel. These are planner inputs a reader interprets
                   directly, and every one of them has a meaningful zero - null_frac 0, correlation 0 - so a
                   sentinel would collide with a real answer. */
                NDistinct: reader.IsDBNull(4) ? null : reader.GetFloat(4),
                NullFrac: reader.IsDBNull(5) ? null : reader.GetFloat(5),
                AvgWidth: reader.IsDBNull(6) ? null : reader.GetInt32(6),
                Correlation: reader.IsDBNull(7) ? null : reader.GetFloat(7),
                TopValueFrequency: reader.IsDBNull(8) ? null : reader.GetFloat(8),
                CommonValueCount: reader.IsDBNull(9) ? null : reader.GetInt32(9)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. These are a CURRENT description of the data, refreshed by ANALYZE; the history exists
           so somebody can see that n_distinct moved on the day a plan changed, which is the question this
           gets opened for. */
        writer
            .Value(row.DatabaseName)
            .Value(row.SchemaName)
            .Value(row.TableName)
            .Value(row.ColumnName)
            .Value(row.NDistinct)
            .Value(row.NullFrac)
            .Value(row.AvgWidth)
            .Value(row.Correlation)
            .Value(row.TopValueFrequency)
            .Value(row.CommonValueCount);
    }
}
