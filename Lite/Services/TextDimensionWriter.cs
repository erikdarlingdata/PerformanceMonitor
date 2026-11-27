/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The write side of the query text dimension (#9241). A collector's batch of a diverted table is appended to a per-connection TEMP
/// stage that has the table's columns without the digest columns, then published in one transaction: the texts the dimension does
/// not hold yet go into <c>query_text_dim</c>, and the batch goes into the table with a NULL inline text and the text's digest.
///
/// <para><b>One dimension row per digest.</b> The dimension has no key (Lite avoids ART indexes), so the anti-join in the publish
/// statement is the only thing that keeps a digest unique, and two connections that publish the same new text at once would each
/// find it missing and each add it. A collector's write holds no store lock (the Query Store backfill holds the shared read lock),
/// so two servers' collectors, or query_stats and Query Store, can do exactly that. A semaphore per dimension table serialises the publish: the second writer
/// begins its transaction after the first has committed, so it sees the text. A duplicate digest would multiply rows in every
/// <c>vh_</c> read.</para>
/// </summary>
internal static class TextDimensionWriter
{
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> s_locks = new(StringComparer.Ordinal);

    /// <summary>The name of the per-connection TEMP stage of <paramref name="table"/>.</summary>
    internal static string StageName(string table) => "pm_text_stage_" + table;

    /// <summary>How a batch of one table is written.</summary>
    internal enum WritePlan
    {
        /// <summary>Straight into the table through one positional appender: the table has no digest column.</summary>
        Plain,

        /// <summary>Through the stage with the text inline and the digest NULL: the table has its digest column but its <c>vh_</c> view does not exist.</summary>
        Inline,

        /// <summary>Through the stage and the dimension: the text is stored once and the table row carries its digest.</summary>
        Dimension,
    }

    /// <summary>
    /// How a batch of <paramref name="table"/> is written. A table is diverted only when its <c>vh_</c> view exists in this store: a
    /// store where the view could not be built (a digest column whose add failed, retried at the next start) reads the table's inline
    /// text, so a text written without it would read back NULL. A table that has its digest column can never take the positional
    /// appender, because the column sits after the payload and the appender wants a value for every column, so even an inline batch
    /// goes through the stage and is inserted by name.
    /// </summary>
    internal static WritePlan PlanFor(DuckDBConnection connection, string table)
    {
        var digests = LiteTextDimensions.For(table).Select(d => d.DigestColumn).Distinct().ToList();
        if (digests.Count == 0)
        {
            return WritePlan.Plain;
        }

        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT
    (SELECT count(*) FROM information_schema.columns WHERE table_schema = 'main' AND table_name = '{table}' AND column_name IN ({string.Join(", ", digests.Select(c => $"'{c}'"))})),
    (SELECT count(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'VIEW' AND table_name = '{LiteTextDimensions.ViewName(table)}')";
        using var reader = command.ExecuteReader();
        reader.Read();
        var hasDigestColumns = Convert.ToInt64(reader.GetValue(0)) == digests.Count;
        var hasView = Convert.ToInt64(reader.GetValue(1)) > 0;

        if (!hasDigestColumns)
        {
            return WritePlan.Plain;
        }

        return hasView ? WritePlan.Dimension : WritePlan.Inline;
    }

    /// <summary>Creates the stage empty: the table's columns in the table's order, with the digest columns left out.</summary>
    internal static void CreateStage(DuckDBConnection connection, string table)
    {
        var excluded = string.Join(", ", LiteTextDimensions.For(table).Select(d => d.DigestColumn).Distinct());
        Execute(connection, $"CREATE OR REPLACE TEMP TABLE {StageName(table)} AS SELECT * EXCLUDE ({excluded}) FROM {table} LIMIT 0");
    }

    /// <summary>Drops the stage. Never throws: it runs in a finally block after a failed write.</summary>
    internal static void DropStage(DuckDBConnection connection, string table)
    {
        try
        {
            Execute(connection, $"DROP TABLE IF EXISTS {StageName(table)}");
        }
        catch (DuckDBException)
        {
            /* The connection is unusable, and the stage dies with it. */
        }
    }

    /// <summary>
    /// Publishes the staged batch: through the dimension in one transaction under the dimension locks, or inline in one statement.
    /// Throws on any failure after rolling the transaction back, so nothing of the batch is stored.
    /// </summary>
    internal static void Publish(DuckDBConnection connection, string table, WritePlan plan)
    {
        var stage = StageName(table);
        if (plan == WritePlan.Inline)
        {
            /* One statement, so it is its own transaction, and no dimension row is involved. */
            Execute(connection, $"INSERT INTO {table} BY NAME SELECT * FROM {stage}");
            return;
        }

        var dims = LiteTextDimensions.For(table).ToList();

        /* Sorted, so two tables that divert into the same dimensions always take the locks in the same order. */
        var locks = dims.Select(d => d.DimTable).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal)
            .Select(n => s_locks.GetOrAdd(n, _ => new SemaphoreSlim(1, 1))).ToList();

        foreach (var l in locks)
        {
            l.Wait();
        }

        try
        {
            Execute(connection, "BEGIN TRANSACTION");
            try
            {
                foreach (var d in dims)
                {
                    Execute(connection, $@"
INSERT INTO {d.DimTable} (digest, query_text)
SELECT s.dg, s.t
FROM (
    SELECT {LiteTextDimensions.DigestSql(d.TextColumn)} AS dg, any_value({d.TextColumn}) AS t
    FROM {stage}
    WHERE {d.TextColumn} IS NOT NULL
    GROUP BY dg
) AS s
WHERE NOT EXISTS (SELECT 1 FROM {d.DimTable} AS e WHERE e.digest = s.dg)");
                }

                var nulled = string.Join(", ", dims.Select(d => $"CAST(NULL AS VARCHAR) AS {d.TextColumn}"));
                var digests = string.Join(", ", dims.Select(d => $"{LiteTextDimensions.DigestSql(d.TextColumn)} AS {d.DigestColumn}"));
                Execute(connection, $"INSERT INTO {table} BY NAME SELECT * REPLACE ({nulled}), {digests} FROM {stage}");
                Execute(connection, "COMMIT");
            }
            catch
            {
                try { Execute(connection, "ROLLBACK"); }
                catch (DuckDBException) { /* no transaction left to roll back */ }
                throw;
            }
        }
        finally
        {
            foreach (var l in locks)
            {
                l.Release();
            }
        }
    }

    /// <summary>
    /// Deletes the dimension rows no table references any more and returns how many went (#9241). A text stays in the dimension
    /// after its last hot row is archived or collapsed, so this is what lets the dimension shrink with the hot store.
    ///
    /// <para><b>The caller holds the store's WRITE lock.</b> That is what excludes a read-lock holder (the Query Store backfill),
    /// but a collector's write holds no store lock, so the write lock alone does not exclude it. The dimension semaphore does:
    /// <see cref="Publish"/> takes it before its transaction begins and keeps it through the commit, and this takes it for the
    /// DELETE, in the same sorted order. A publish that already found a digest in the dimension has then committed its fact rows
    /// before the DELETE looks for references, and one that starts after finds the row gone and adds it again. Without it, a
    /// batch that reuses a digest whose last row was just archived would commit a row whose text this DELETE had removed.
    /// There is no cycle: a publisher holds the semaphore and never waits for a store lock, and this takes the semaphore under
    /// the write lock, which waits for readers that are themselves only waiting for the semaphore.</para>
    ///
    /// <para>Throws on a failure, having deleted nothing: the caller decides whether that matters.</para>
    /// </summary>
    internal static long RemoveUnreferenced(DuckDBConnection connection)
    {
        long removed = 0;
        foreach (var dimTable in LiteTextDimensions.All.Select(d => d.DimTable).Distinct(StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal))
        {
            /* One NOT EXISTS per referencing column, ANDed: each plans as an anti-join on the digest. */
            var unreferenced = string.Join(" AND ", LiteTextDimensions.All
                .Where(d => d.DimTable == dimTable)
                .Select(d => $"NOT EXISTS (SELECT 1 FROM {d.Table} AS f WHERE f.{d.DigestColumn} = d.digest)")
                .Distinct(StringComparer.Ordinal));
            var gate = s_locks.GetOrAdd(dimTable, _ => new SemaphoreSlim(1, 1));
            gate.Wait();
            try
            {
                using var command = connection.CreateCommand();
                command.CommandText = $"DELETE FROM {dimTable} AS d WHERE {unreferenced}";
                removed += Convert.ToInt64(command.ExecuteNonQuery());
            }
            finally
            {
                gate.Release();
            }
        }

        return removed;
    }

    /// <summary>
    /// <see cref="RemoveUnreferenced"/> for a maintenance step that must not fail on it: logs the rows removed in one line, and a
    /// failure as a warning, because an unused text left in the dimension costs space only and the next pass removes it. The caller
    /// holds the store's write lock. Returns the rows removed, 0 after a failure.
    /// </summary>
    internal static long TryRemoveUnreferenced(DuckDBConnection connection, ILogger? logger, string after)
    {
        try
        {
            var removed = RemoveUnreferenced(connection);
            logger?.LogInformation("Removed {Rows} unused query texts from the text table after {Step}", removed, after);
            return removed;
        }
        catch (Exception ex)
        {
            logger?.LogWarning(ex, "Could not remove unused query texts from the text table after {Step}; they stay until the next pass", after);
            return 0;
        }
    }

    private static void Execute(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
