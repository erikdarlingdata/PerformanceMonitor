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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The stored-row half of #1907, which the collector-side fix cannot reach (#1912).
///
/// <para>Before #1907 the collector stored Query Store's FLUSHED slice and its still-IN-MEMORY slice of one
/// <c>runtime_stats_interval_id</c> as two separate rows. They are ADDITIVE members of one interval, so the
/// read-side dedup — which keeps exactly one row per key — reports a fraction of the interval's true work.
/// #1907 made that choice deterministic (it resolves to the flushed slice) but deliberately did not call it a
/// fix. This collapses those rows into the single row the collector would write today.</para>
///
/// <para><b>The pre-fix signature is exact, which is what makes this safe to re-run.</b> Two or more rows
/// sharing the ENTIRE read-side dedup key AND <c>collection_time</c> can only be split slices: since #1907 the
/// collector emits at most one row per interval per cycle, so a post-fix row can never join such a group. The
/// collapse is therefore idempotent — a second run finds nothing — and it cannot touch correctly-collected
/// data even if the operator runs it years later.</para>
///
/// <para><b>Column handling is DERIVED from the collector's own payload, not restated here.</b> The
/// classification is the same one <c>QueryStoreCollector.BuildPayloadBody</c> applies at collection —
/// <c>execution_count</c> sums, every <c>avg_</c> column takes the count-weighted mean, <c>min_</c> and
/// <c>max_</c> take the extreme — and it is computed by walking <see cref="CollectorDefinitionBase{TRow}.PayloadColumns"/>.
/// A column added to the collector is classified here automatically, on the same prefix convention #1907
/// already pins by test, instead of silently falling into the pass-through bucket.</para>
/// </summary>
public static class QueryStoreSliceRepair
{
    /// <summary>The store table the repair operates on.</summary>
    public const string Table = "query_store_stats";

    /// <summary>
    /// The dedup key, identical to the read side's <c>PARTITION BY</c> plus the two columns every store row
    /// carries. <c>collection_time</c> is what turns it from "the same interval" into "the same interval in
    /// ONE cycle" — the pre-fix signature. <c>server_id</c>/<c>server_name</c> scope it to one monitored
    /// server, since a fleet store holds many.
    /// </summary>
    public static readonly string[] KeyColumns =
    [
        "server_id",
        "server_name",
        "collection_time",
        "database_name",
        "query_id",
        "plan_id",
        "runtime_stats_interval_id",
        "first_execution_time",
        "execution_type_desc",
        "replica_role",
    ];

    /// <summary>
    /// <c>last_execution_time</c> is the interval's span END, so it takes MAX rather than being carried
    /// through from an arbitrary slice — the same treatment the collector gives it. Named explicitly because
    /// it is the one column whose correct handling does not follow from its prefix.
    /// </summary>
    private const string LastExecutionTime = "last_execution_time";

    private const string ExecutionCount = "execution_count";

    /// <summary>
    /// How each payload column combines, in the store's physical column order.
    /// </summary>
    private static IEnumerable<(string Name, string Expression)> CombinedColumns()
    {
        /* collection_id is the store's own prefix id, not payload: one of the collapsed rows' ids survives,
           which is what a single collected row would have carried anyway. It identifies a collection, and the
           collapsed row belongs to exactly the collection its slices came from. */
        yield return ("collection_id", "min(s.collection_id)");

        foreach (var column in QueryStoreCollector.Instance.PayloadColumns)
        {
            var name = column.Name;

            if (KeyColumns.Contains(name, StringComparer.Ordinal))
            {
                continue;
            }

            yield return (name, CombineExpression(name, column.Type));
        }
    }

    /// <summary>
    /// The aggregate for one column. Public so the test that pins the classification can assert on the same
    /// function the SQL is built from rather than on a copy of its rules.
    /// </summary>
    public static string CombineExpression(string column, CollectorColumnType type)
    {
        ArgumentNullException.ThrowIfNull(column);

        if (string.Equals(column, ExecutionCount, StringComparison.Ordinal))
        {
            return $"sum(s.{ExecutionCount})";
        }

        if (string.Equals(column, LastExecutionTime, StringComparison.Ordinal))
        {
            return $"max(s.{LastExecutionTime})";
        }

        /* The count-WEIGHTED mean. Query Store stores an average and a count but never a total, so
           avg * count recovers each slice's total and the quotient of the sums is the interval's true
           average — the identical expression QueryStoreCollector.WeightedAverage emits at collection.
           NULLIF guards the divide-by-zero rather than failing a whole repair over a zero-execution row. */
        if (column.StartsWith("avg_", StringComparison.Ordinal))
        {
            return $"(sum(s.{column}::double precision * s.{ExecutionCount}) / NULLIF(sum(s.{ExecutionCount}), 0))::bigint";
        }

        if (column.StartsWith("min_", StringComparison.Ordinal))
        {
            return $"min(s.{column})";
        }

        if (column.StartsWith("max_", StringComparison.Ordinal))
        {
            return $"max(s.{column})";
        }

        /* PostgreSQL has no min()/max() over boolean — unlike DuckDB, which is why the Darling viewer already
           folds is_forced_plan through bool_or(). Classified by the collector's DECLARED TYPE rather than by
           the column's name, so a future boolean cannot land in the min() branch and fail the repair at
           runtime the way this one did the first time it ran. */
        if (type == CollectorColumnType.Boolean)
        {
            return $"bool_or(s.{column})";
        }

        /* Everything else is an attribute of the interval rather than a measurement of it — the query text,
           the plan hash, the failure reason. Every slice of one interval carries the same value, so any of
           them is the value; min() is chosen over an arbitrary pick only because it is deterministic. */
        return $"min(s.{column})";
    }

    /// <summary>
    /// Counts the pre-fix groups and the collection-time range they span, without changing anything. This is
    /// both the dry-run report and the input to the refresh clamp: the range comes from the rows that will
    /// actually be collapsed, so it can never reach below raw's own extent — which is what keeps the refresh
    /// off the region where it would DESTROY materialized history rather than rebuild it.
    /// </summary>
    public static string SurveySql =>
        $"""
        SELECT
            count(*) AS split_groups,
            coalesce(sum(rows_in_group), 0) AS split_rows,
            min(collection_time) AS oldest,
            max(collection_time) AS newest
        FROM
        (
            SELECT
                collection_time,
                count(*) AS rows_in_group
            FROM collect.{Table}
            GROUP BY {string.Join(", ", KeyColumns)}
            HAVING count(*) > 1
        ) AS g
        """;

    /// <summary>
    /// The collapse split into the three statements that must run in order.
    ///
    /// <para>They are issued SEPARATELY, never as one batch: Npgsql cannot send a multi-statement command
    /// carrying positional parameters — it raises 42601 here, and on a failure-isolated path it has been known
    /// to die SILENTLY instead, which is worse. They share one transaction, and the temp table is
    /// <c>ON COMMIT DROP</c>, so the staging survives between them and cleans itself up either way.</para>
    /// </summary>
    public static CollapseStatements BuildCollapseStatements()
    {
        var sql = BuildCollapseSql();
        var stage = sql.IndexOf("DELETE FROM", StringComparison.Ordinal);
        var insert = sql.IndexOf("INSERT INTO", StringComparison.Ordinal);

        return new CollapseStatements(
            sql[..stage].TrimEnd().TrimEnd(';'),
            sql[stage..insert].TrimEnd().TrimEnd(';'),
            sql[insert..].TrimEnd().TrimEnd(';'));
    }

    /// <summary>The staging, delete and insert steps, in the order they must run.</summary>
    public sealed record CollapseStatements(string Stage, string Delete, string Insert);

    /// <summary>
    /// The collapse itself, for one collection-time slice, as one script.
    ///
    /// <para>Staged through a temp table rather than done as one statement because the DELETE has to match on
    /// a key containing NULLABLE columns — <c>runtime_stats_interval_id</c> is NULL on every row collected
    /// before #1841 tier 2, and <c>replica_role</c> on anything pre-2022 — and <c>IN</c>/<c>=</c> against a
    /// NULL yields NULL rather than true. Every key comparison is <c>IS NOT DISTINCT FROM</c>, so a legacy row
    /// with NULLs is matched instead of silently skipped, which would leave the very oldest rows (the ones
    /// most likely to be past raw's edge) uncollapsed while reporting success.</para>
    ///
    /// <para><b>The wide payload columns ride through this aggregate deliberately, and that is safe HERE
    /// though it was not in Lite (#2876, answering the sibling question raised by #2771).</b> Lite's
    /// equivalent collapse exhausted its memory budget pushing <c>query_plan_text</c> through a per-group
    /// aggregate — at 31,426 split intervals it raised a hard <c>Out of Memory Error</c> — and was rewritten
    /// to collapse in place. This shape was measured against PostgreSQL 18 rather than assumed to differ, at
    /// the same 31,426 groups and again at 51,426, carrying 3,033 MB of decompressed plan text:</para>
    ///
    /// <list type="number">
    /// <item><b>PostgreSQL spills where DuckDB dies.</b> The planner picks <c>GroupAggregate</c> over an
    /// external merge sort — 4.3 s, 125 MB of temp files, negligible resident memory. Forced onto the
    /// <c>HashAggregate</c> that is Lite's failure shape, it still bounded itself (45 batches, 1.1 GB peak,
    /// spilling the rest) and completed. Neither path can raise the error Lite raised.</item>
    /// <item><b>TOAST keeps the wide values out of the sort payload.</b> 3,033 MB of logical plan text
    /// spilled as 125 MB, because the sorted tuples carry out-of-line pointers and <c>min()</c> detoasts
    /// lazily. DuckDB materialises the column into the aggregate's own blocks, which is why the same row
    /// count costs it two orders of magnitude more.</item>
    /// <item><b>The full projection is what CHOOSES the safe plan.</b> Roughly fifty transition states per
    /// group make <c>GroupAggregate</c> win on cost; a cut-down three-column version of this same query
    /// plans as an unspilled <c>HashAggregate</c> at 1.6 GB. So the breadth the issue framed as the hazard
    /// is load-bearing — narrowing this projection toward Lite's shape would move it TOWARD the risk, not
    /// away from it.</item>
    /// </list>
    ///
    /// <para>Independently, the repair has nothing left to do: the shipped <see cref="SurveySql"/> over the
    /// whole production hypertable returns zero split groups, because #1907 stopped the collector emitting
    /// them and the historical ones have aged past raw's 4-day edge. The bounded slicing plus
    /// <see cref="SliceStatementTimeoutSeconds"/> is therefore the permanent answer here, not a mitigation
    /// awaiting the in-place rewrite.</para>
    /// </summary>
    public static string BuildCollapseSql()
    {
        var projections = new List<string>();
        projections.AddRange(KeyColumns.Select(k => "s." + k));
        projections.AddRange(CombinedColumns().Select(c => $"{c.Expression} AS {c.Name}"));

        var select = string.Join(",\n            ", projections);

        var joinPredicate = string.Join(
            "\n            AND ",
            KeyColumns.Select(k => $"t.{k} IS NOT DISTINCT FROM r.{k}"));

        /* The insert column list must be the STORE's physical order, which is the prefix columns then the
           payload. Named explicitly rather than relying on SELECT * so a column added to the middle of the
           payload cannot shift values into the wrong column. */
        var insertColumns = new List<string> { "collection_id" };
        insertColumns.AddRange(["collection_time", "server_id", "server_name"]);
        insertColumns.AddRange(QueryStoreCollector.Instance.PayloadColumns
            .Select(c => c.Name)
            .Where(n => !string.Equals(n, "collection_id", StringComparison.Ordinal)));

        var insertList = string.Join(", ", insertColumns);
        var selectBack = string.Join(", ", insertColumns.Select(c => "r." + c));

        return $"""
        CREATE TEMP TABLE qs_slice_repair ON COMMIT DROP AS
        SELECT
            {select}
        FROM collect.{Table} AS s
        WHERE s.collection_time >= $1
        AND   s.collection_time <  $2
        GROUP BY {string.Join(", ", KeyColumns.Select(k => "s." + k))}
        HAVING count(*) > 1;

        DELETE FROM collect.{Table} AS t
        USING qs_slice_repair AS r
        WHERE {joinPredicate};

        INSERT INTO collect.{Table} ({insertList})
        SELECT {selectBack} FROM qs_slice_repair AS r;
        """;
    }

    /// <summary>
    /// Per-statement timeout for the repair's heavy statements (#2105 field failure): Npgsql's
    /// default 30s killed the STAGE aggregation on a store fresh off a large catch-up — a day
    /// slice's GROUP BY spools every row of the day including the query-text payloads, and the
    /// verb runs beside the live service (a managed store cannot stop it — stopping the service
    /// stops Postgres), so collector writes and compression jobs contend for the same chunks.
    /// The failure read as "Exception while reading from stream" after 0 rows, which is how an
    /// Npgsql command timeout surfaces — nothing in the message says timeout. Fifteen minutes is
    /// deliberately generous-but-bounded: the slice transaction holds chunk locks, so infinite
    /// (the VACUUM precedent) is wrong here.
    /// </summary>
    public const int SliceStatementTimeoutSeconds = 900;

    /// <summary>
    /// Runs the collapse over one half-open collection-time slice and returns how many rows it removed.
    ///
    /// <para>One transaction per slice: the DELETE and the INSERT must not be separable, or an abort between
    /// them destroys the interval outright rather than leaving it split. Slicing keeps that transaction — and
    /// the locks it takes on chunks a compression job may also want — short.</para>
    ///
    /// <para>The removed count is DERIVED from the statements' own affected-row counts — the DELETE removes
    /// every row of every split group and the INSERT restores one combined row per group, so
    /// <c>deleted − reinserted</c> IS the net removal. The previous shape bracketed the work with two
    /// window-wide <c>COUNT(*)</c> scans to compute the same number, which on the stores this verb exists
    /// for (measured: ~12 s per day-wide scan, hash-aggregate spill + a backward index scan over the
    /// uncompressed hot chunk) paid the slice's dominant cost twice more per slice for pure bookkeeping.</para>
    /// </summary>
    public static async Task<long> CollapseSliceAsync(
        NpgsqlConnection connection, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* #2105 field failure round two: the repair's DELETE touches COMPRESSED chunks (a store old
           enough to need this repair has had its compression policy running the whole time), and
           TimescaleDB caps decompression at 100k tuples per DML transaction by default — the field
           run died at `53400: tuple decompression limit exceeded` four minutes in. Lift it for this
           transaction only (SET LOCAL dies with the transaction), the same rail-lift the retention
           purge's fallback DELETE already does — deliberate bulk decompression is this verb's job.
           On a store without the extension the qualified name is a placeholder GUC, safe everywhere. */
        await using (var lift = new NpgsqlCommand(
            "SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0", connection, transaction))
        {
            await lift.ExecuteNonQueryAsync(cancellationToken);
        }

        var statements = BuildCollapseStatements();

        await using (var stage = new NpgsqlCommand(statements.Stage, connection, transaction) { CommandTimeout = SliceStatementTimeoutSeconds })
        {
            stage.Parameters.AddWithValue(fromUtc);
            stage.Parameters.AddWithValue(toUtc);
            await stage.ExecuteNonQueryAsync(cancellationToken);
        }

        long deleted;
        await using (var delete = new NpgsqlCommand(statements.Delete, connection, transaction) { CommandTimeout = SliceStatementTimeoutSeconds })
        {
            deleted = await delete.ExecuteNonQueryAsync(cancellationToken);
        }

        long reinserted;
        await using (var insert = new NpgsqlCommand(statements.Insert, connection, transaction) { CommandTimeout = SliceStatementTimeoutSeconds })
        {
            reinserted = await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return deleted - reinserted;
    }

    /// <summary>The survey result: what a dry run reports and what a real run plans from.</summary>
    public sealed record Survey(long SplitGroups, long SplitRows, DateTime? OldestUtc, DateTime? NewestUtc)
    {
        /// <summary>Rows the collapse would remove — one per extra slice, so groups subtracted from rows.</summary>
        public long RowsRemoved => SplitRows - SplitGroups;

        public bool HasWork => SplitGroups > 0;
    }

    public static async Task<Survey> SurveyAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        /* Same #2105 timeout treatment: the survey aggregates the whole table's key columns, and a
           dry run must not die on the store size the repair exists to handle. */
        await using var command = new NpgsqlCommand(SurveySql, connection) { CommandTimeout = SliceStatementTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);

        if (!await reader.ReadAsync(cancellationToken))
        {
            return new Survey(0, 0, null, null);
        }

        return new Survey(
            reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
            reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
            reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            reader.IsDBNull(3) ? null : reader.GetDateTime(3));
    }
}
