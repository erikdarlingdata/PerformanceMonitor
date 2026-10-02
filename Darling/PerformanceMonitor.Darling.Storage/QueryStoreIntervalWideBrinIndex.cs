/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// A BRIN index on <c>collect.query_store_interval_wide (collection_time)</c> (#4605), built in the
/// background after the service is up rather than by a migration rung.
///
/// <para><b>Why the table needs it.</b> Nothing serves <c>collection_time</c> on this table: its only
/// secondary index leads with <c>first_execution_time</c>. A Custom Views Query Store panel over 12 hours or
/// more reads the table on <c>collection_time</c> and Parallel-Seq-Scans all of it. On a large production
/// monitoring store (28 GB, 3.1 of 9 days held) a 24-hour fleet panel read 3.18M blocks, 98.8 s cold. With
/// the index, on the same store at <c>random_page_cost</c> 1.1, a warm Parallel Bitmap Heap Scan read 24 h in
/// 4.3 s (1.14M blocks), 12 h in 1.6 s, 6 h in 0.8 s and 48 h in 9.2 s. Those figures are from a store still
/// inside its first retention cycle. Once the 9-day purge runs, the freed early pages are reused by new
/// rows, so BRIN ranges widen: windows ending now should stay selective, while historical windows lose
/// selectivity and the plan may flip back to a Seq Scan for part of the cycle. Results stay exact and
/// nothing is slower than without the index. If that matters, the remedy is a periodic
/// <c>REINDEX INDEX CONCURRENTLY</c> or re-summarizing the ranges, which this class does not do.</para>
///
/// <para><b>Why BRIN and not a btree.</b> The writer's upsert (<c>QueryStoreIntervalWide.cs</c>,
/// <c>ON CONFLICT ... DO UPDATE SET collection_time = EXCLUDED.collection_time, ...</c>) rewrites
/// <c>collection_time</c> on every update, and 99.9% of this table's updates are HOT (heap-only) today. Any
/// btree on <c>collection_time</c>, or a covering <c>INCLUDE</c> of the measures, would make every one of them
/// non-HOT: the regression class #4250 fixed. From PostgreSQL 16 an update stays HOT when the only indexed
/// columns it changes are summarizing (BRIN) ones. Measured on that store in one rolled-back transaction:
/// updating <c>collection_time</c> and <c>execution_count</c> on 20 rows was 20/20 HOT; the control, updating
/// <c>first_execution_time</c> (btree-indexed) on 20 other rows, was 0/20. The column's physical correlation
/// was 0.9998, which is what makes a BRIN selective: rows land in time order and an update rewrites the
/// value to a newer time within the same neighbourhood.</para>
///
/// <para><b>Below PostgreSQL 16 the index is not built.</b> Without summarizing-index HOT support the BRIN
/// would make every upsert non-HOT, so <see cref="Decide"/> skips the build and the ensure logs why. A
/// bring-your-own store on PostgreSQL 14 or 15 keeps today's plans and today's HOT rate.</para>
///
/// <para><b>Why not a migration rung.</b> Migrations run in one transaction and block startup, and
/// <c>CREATE INDEX CONCURRENTLY</c> cannot run inside a transaction (25001). A plain in-rung build would hold
/// a lock that blocks the writer for the whole heap read: 108 s at 25 GB, minutes at a full 9 days. A
/// background <c>CONCURRENTLY</c> build blocked no writes when measured. An applied rung also never
/// changes, so the ensure is idempotent and runs on every start instead: a fresh store builds an empty
/// table's index instantly and no table-creation change is needed.</para>
///
/// <para><b>Hypertables are refused.</b> The table is a plain heap. TimescaleDB refuses <c>CONCURRENTLY</c>
/// on a hypertable, so if the table is ever converted the ensure skips with a warning instead of failing
/// or retrying.</para>
///
/// <para><b>Autosummarize.</b> <c>autosummarize = on</c> lets autovacuum work items summarize each new block
/// range as the table grows. A range that is not summarized yet is always read by the scan, so results stay
/// correct while the summary catches up; the index is 856 kB on that store.</para>
///
/// <para><b>The start delay, the validity read and the build.</b> They are
/// <see cref="QueryStoreBackgroundIndexes"/>'s, shared with the two btrees #4952 adds: it waits
/// <see cref="QueryStoreBackgroundIndexes.StartDelay"/> (20 minutes) first, because after an install or restart a
/// big store's volume sits at its IOPS cap for about 15 minutes (cold cache, migrations, the retention purge, the
/// continuous-aggregate refresh) and a full-heap read on top slows all of it. One attempt is made per service
/// start; a failure or an interrupted build is retried at the next start. An interrupted <c>CONCURRENTLY</c> build
/// leaves an INVALID index behind, which <c>IF NOT EXISTS</c> would silently keep, so the ensure reads validity
/// first and drops an INVALID leftover before building.</para>
///
/// <para><b>Reads take the index only at a low <c>random_page_cost</c>.</b> At the default of 4 the planner
/// still chooses the sequential scan for a 12-hour window even with this index present; at about 1.1 or
/// lower it chooses a Bitmap Heap Scan over the BRIN. The managed store sets 1.1; a bring-your-own store
/// on SSD needs its operator to.</para>
/// </summary>
public static class QueryStoreIntervalWideBrinIndex
{
    /// <summary>The index's schema-qualified name.</summary>
    public const string IndexName = "collect.ix_query_store_interval_wide_collection_time_brin";

    /// <summary>First PostgreSQL version whose HOT logic tolerates changes to summarizing-index columns.</summary>
    public const int MinimumServerVersionNum = 160000;

    internal const string CreateSql =
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_query_store_interval_wide_collection_time_brin "
        + "ON collect.query_store_interval_wide USING brin (collection_time) WITH (autosummarize = on);";

    internal const string DropSql =
        "DROP INDEX CONCURRENTLY IF EXISTS collect.ix_query_store_interval_wide_collection_time_brin;";

    /// <summary>
    /// This index as <see cref="QueryStoreBackgroundIndexes"/> builds it: <c>CONCURRENTLY</c> on the plain table, no
    /// hypertable form (a hypertable is skipped with a warning), and not below PostgreSQL 16.
    /// </summary>
    public static readonly QueryStoreBackgroundIndexes.IndexSpec Spec = new(
        IndexName,
        "collect.query_store_interval_wide",
        CreateSql,
        null,
        DropSql,
        DropSql,
        MinimumServerVersionNum,
        "a BRIN index on collection_time would make the upsert non-HOT below PG 16");

    /// <summary>What the ensure does about the index.</summary>
    public enum BrinAction
    {
        /// <summary>Build (or keep) the index.</summary>
        Build,

        /// <summary>Skip: the server predates summarizing-index HOT support.</summary>
        SkipServerVersionBelowSixteen,

        /// <summary>Skip: the table is a hypertable, which refuses <c>CREATE INDEX CONCURRENTLY</c>.</summary>
        SkipHypertable,
    }

    /// <summary>The decision and, for a skip, the reason to log.</summary>
    public readonly record struct BrinDecision(BrinAction Action, string Reason);

    /// <summary>
    /// The pure build-or-skip decision, <see cref="QueryStoreBackgroundIndexes.Decide"/> for this index. The version
    /// check comes first: below 160000 a BRIN on <c>collection_time</c> makes every upsert non-HOT, whatever the table
    /// is.
    /// </summary>
    public static BrinDecision Decide(int serverVersionNum, bool tableIsHypertable)
    {
        var decision = QueryStoreBackgroundIndexes.Decide(Spec, serverVersionNum, tableIsHypertable);
        var action = decision.Action switch
        {
            QueryStoreBackgroundIndexes.IndexAction.SkipServerVersion => BrinAction.SkipServerVersionBelowSixteen,
            QueryStoreBackgroundIndexes.IndexAction.SkipHypertable => BrinAction.SkipHypertable,
            _ => BrinAction.Build,
        };
        return new BrinDecision(action, decision.Reason);
    }

    /// <summary>
    /// Makes sure the BRIN index exists and is valid, on <paramref name="connection"/>, which must be open and
    /// outside any transaction (<c>CONCURRENTLY</c> fails with 25001 inside one).
    /// </summary>
    public static Task EnsureAsync(NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken) =>
        QueryStoreBackgroundIndexes.EnsureAsync(connection, Spec, logger, cancellationToken);
}
