/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingCollectorRunner
{
    /// <summary>
    /// The cycle's distinct referenced plans with their live hashes — the probe's whole input (#2312).
    /// Generic because the dispatch loop is; any batch that is not query_store rows extracts nothing, which
    /// #2902 measured happening on every cycle — <c>CapturePlanXml</c> is a SKU flag, so it gates the SKU
    /// and not the collector, and all five enumerating collectors reach this. Extracting nothing is the
    /// right answer for them; what was wrong is that the fetch then read a carryover key they shared.
    ///
    /// <para>When one plan appears in several rows (several intervals), a non-null hash wins over a null
    /// one — the probe compares against whatever the engine reported, and null only means the payload row
    /// predated the hash column.</para>
    /// </summary>
    private static IReadOnlyList<(long PlanId, string? PlanHash)> ExtractPlanReferences<TRow>(List<TRow> batch, HashSet<long>? onlyPlanIds = null)
    {
        if (batch is not List<QueryStoreCollector.Row> rows || rows.Count == 0)
        {
            return Array.Empty<(long, string?)>();
        }

        var seen = new Dictionary<long, string?>();
        foreach (var row in rows)
        {
            if (row.PlanId <= 0 || (onlyPlanIds is not null && !onlyPlanIds.Contains(row.PlanId)))
            {
                continue;
            }

            if (!seen.TryGetValue(row.PlanId, out var hash) || (hash is null && row.QueryPlanHash is not null))
            {
                seen[row.PlanId] = row.QueryPlanHash;
            }
        }

        var references = new List<(long PlanId, string? PlanHash)>(seen.Count);
        foreach (var entry in seen)
        {
            references.Add((entry.Key, entry.Value));
        }

        references.Sort((a, b) => a.PlanId.CompareTo(b.PlanId));
        return references;
    }

    /// <summary>Text twin of <see cref="ExtractPlanReferences"/>, keyed by query_id with query_hash.</summary>
    private static IReadOnlyList<(long QueryId, string? QueryHash)> ExtractTextReferences<TRow>(List<TRow> batch, HashSet<long>? onlyQueryIds = null)
    {
        if (batch is not List<QueryStoreCollector.Row> rows || rows.Count == 0)
        {
            return Array.Empty<(long, string?)>();
        }

        var seen = new Dictionary<long, string?>();
        foreach (var row in rows)
        {
            if (row.QueryId <= 0 || (onlyQueryIds is not null && !onlyQueryIds.Contains(row.QueryId)))
            {
                continue;
            }

            if (!seen.TryGetValue(row.QueryId, out var hash) || (hash is null && row.QueryHash is not null))
            {
                seen[row.QueryId] = row.QueryHash;
            }
        }

        var references = new List<(long QueryId, string? QueryHash)>(seen.Count);
        foreach (var entry in seen)
        {
            references.Add((entry.Key, entry.Value));
        }

        references.Sort((a, b) => a.QueryId.CompareTo(b.QueryId));
        return references;
    }

    /// <summary>
    /// Clears this database's plan-fetch backoff (#2776) without disturbing the size it learned.
    /// </summary>
    /// <remarks>
    /// Called from the "nothing to fetch this cycle" early returns, which are the two ways a pass can end
    /// well without reaching the success line at the bottom of the fetch. Without this a database that
    /// failed once and then went quiet would keep the count pinned — nothing resets it, because nothing
    /// runs — and the first pass after the work came back, possibly hours later and against a completely
    /// different store, would be narrowed for a reason that expired long ago. Only the counter is cleared;
    /// the learned average is the expensive part and it stays. Advisory, so a lost race is fine: the next
    /// idle cycle clears it again.
    /// </remarks>
    private void ClearPlanFetchBackoff((int ServerId, string Database, string Collector) carryKey)
    {
        if (_observedPlanSize.TryGetValue(carryKey, out var estimate) && estimate.ConsecutiveFetchFailures > 0)
        {
            _observedPlanSize.TryUpdate(carryKey, QueryStorePlanXmlState.RecordFetchSuccess(estimate), estimate);
        }
    }

    /// <summary>
    /// Puts the BORROWED store connection back into a usable state after a fetch faulted on it (#2819).
    ///
    /// <para><b>Why this is required rather than tidy.</b> The fetches swallow their own exceptions and
    /// return normally, so <c>readItem</c> still hands the driver a non-null batch built from the target rows
    /// it already read. <c>EnumeratedCollectorDriver.RunAsync</c> then calls <c>writeBatch</c> OUTSIDE its
    /// per-item try/catch, and that path is documented to propagate — "a flush failure PROPAGATES, storage
    /// failure is systemic". So a fetch that broke the connection would not cost one database its plan XML;
    /// it would fail the runtime-stats write for that item and abort the REST OF THE SWEEP, every remaining
    /// database for that collector this cycle. While the fetches held private connections that was
    /// structurally impossible, and borrowing must not buy pool headroom at the price of a whole-cycle
    /// abort.</para>
    ///
    /// <para><b>Takes no CancellationToken on purpose.</b> Every call site has one — query_store's per-item
    /// wall-clock budget — and an expired budget is the likeliest reason this runs at all, so reopening
    /// under it would fail on a token check before attempting a connection. Recovery of a connection SHARED
    /// by every remaining database must not be abandoned because one slow database ran out of time.</para>
    ///
    /// <para>A transient fault — a cancelled command, a dropped socket — leaves the pooled connection
    /// unusable but the store perfectly reachable, and that is the case this recovers: reopen, and the
    /// caller's write proceeds as if the fetch had its own connection. If the reopen ALSO fails the store is
    /// genuinely unreachable, which is exactly the systemic condition the driver's propagate-on-flush
    /// behaviour exists for, so the failure is left to travel — swallowed here, surfaced there, unchanged
    /// from before this borrowing.</para>
    /// </summary>
    private async Task<bool> RestoreBorrowedStoreConnectionAsync(
        NpgsqlConnection storeConnection,
        ServerRuntime server,
        string databaseName)
    {
        if (storeConnection.State == ConnectionState.Open)
        {
            return true;
        }

        try
        {
            /* Close first: Npgsql will not reopen a Broken connection in place, and Close on an already
               closed one is a no-op rather than a fault. */
            await storeConnection.CloseAsync();

            /* CancellationToken.None, deliberately, and this method takes no token so a caller cannot pass
               a cancelled one by reflex. The token available at every call site is query_store's per-item
               wall-clock budget, and an EXPIRED budget is the single most likely reason we are here — so
               reopening under it would make OpenAsync throw OperationCanceledException off a token check
               before it ever attempted a connection. That is recovery failing closed at exactly the moment
               it is needed, and worse than not trying: the OCE would escape this method's own non-OCE catch
               and skip the caller's remaining backoff bookkeeping too.

               One slow database's budget is not a reason to abandon a connection SHARED by every database
               still to come in this sweep. Real shutdown is still respected — it tears the process down
               regardless, and this is one short reconnect, not a loop. */
            await storeConnection.OpenAsync(CancellationToken.None);

            _logger?.LogWarning(
                "Reopened the shared store connection after a Query Store fetch fault on '{Server}' database [{Database}] — the fetch borrows the collector body's connection, and leaving it broken would fail this item's runtime-stats write and abort the rest of the sweep.",
                server.Config.DisplayName, databaseName);

            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The store itself is unreachable. Deliberately swallowed: the write that follows will fail on
               the same connection and PROPAGATE, which is the correct handling of a systemic store failure
               and the behaviour that predates this borrowing. Logged so the cause is visible there. */
            _logger?.LogWarning(ex,
                "Could not reopen the shared store connection after a Query Store fetch fault on '{Server}' database [{Database}] — the store looks unreachable, so this cycle's remaining writes will fail systemically.",
                server.Config.DisplayName, databaseName);

            return false;
        }
    }

    /// <summary>
    /// One Query Store database's two deferred fetches, run right after that database's runtime-statistics read
    /// and on the connection that read it: the plan XML first (#2210), then the statement text (#2150). Shared by
    /// both places that read a Query Store database - the enumerated per-item read (SQL Server, Managed Instance)
    /// and the per-database loop Azure SQL Database takes, where <c>QueryStoreCollector.RunsPerDatabase</c> is
    /// <c>IsAzureSqlDb</c>. That loop used to read and flush without calling either fetch; every scheduled run
    /// nulls the payload's inline text and plan (see <see cref="CollectorContext.FetchQueryTextSeparately"/>), so
    /// every Azure SQL Database row was stored with neither statement text nor plan.
    ///
    /// <para><paramref name="databaseName"/> is the database THIS read targeted, never the registration's initial
    /// catalog. The by-ids queries run <c>EXECUTE [db].sys.sp_executesql</c>, which Azure SQL Database accepts only
    /// when the named database is the connection's current one, and the per-database connection is exactly that.</para>
    ///
    /// <para>Each fetch isolates its own failures (it logs and returns), so neither costs the database its runtime
    /// statistics. A budget expiry is the exception and propagates to the caller's budget handling. The caller
    /// clears the readings first (<see cref="ClearFetchPhaseStamps"/>) and reads them after.</para>
    /// </summary>
    private async Task FetchQueryStorePlansAndTextAsync<TRow>(
        ITargetProvider targetProvider,
        DbConnection targetConnection,
        NpgsqlConnection storeConnection,
        ServerRuntime server,
        string databaseName,
        string collectorName,
        CollectorContext context,
        int commandTimeoutSeconds,
        List<TRow> batch,
        CancellationToken cancellationToken)
    {
        /* #2210: this database's plan-XML fetch, right after its runtime-stats read. A separate
           query on purpose - it ships in plan_id order, so a budget cut truncates a SUFFIX,
           which is the only reason the watermark can advance from a cut pass at all. */
        /* The engine gate: this fetch is Query-Store-only. The enumerated path serves PostgreSQL
           targets since #2213; query_store declares TargetEngine = SqlServer so it never reaches
           here for one, but relying on the catalog for that would be an invariant held somewhere
           else. #5813: the gate was `targetConnection is SqlConnection`, which narrowed the type
           AND gated the engine, but nothing in either fetch is SqlClient-specific (a command from
           the run's provider and a DbDataReader), and the type test kept the fake-target harness
           from ever reaching the fetch. The provider's engine is the same gate in production (the
           SQL Server provider is the only one that reports SqlServer, and its commands are the
           SqlCommands the SqlConnection overload built), and it is a fact a fake can state. */
        /* #6211: which of this database's plans (and their queries) are worth a fetch this run. Null = every plan, the behaviour before the
           setting (the setting at 0, a batch that is not query_store rows, or a store read that failed: a fault here must cost fetches, never
           the forced plans). Ranked over the rows already read, so it adds nothing to the monitored server. */
        var fetchSelection = ((context.CapturePlanXml || context.FetchQueryTextSeparately) && targetProvider.Engine == CollectorTargetEngine.SqlServer)
            ? await SelectQueryStoreFetchAsync(storeConnection, server, databaseName, context, batch, cancellationToken)
            : null;

        if (context.CapturePlanXml && targetProvider.Engine == CollectorTargetEngine.SqlServer)
        {
            /* #2312 investigation: timed so the log split can say whether the invariant
               per-cycle cost lives HERE rather than in the payload - a 0-row cycle's
               blended sql: could not distinguish them. */
            /* #2854: stamped from finally. This one is the PARENT of the sub-split #2816
               already fixed, which makes a bare stamp here worse than the defect it fixed:
               probe/target/write stamp from their own handlers and report real values, so a
               throwing fetch printed plan_fetch:0ms above non-zero children. PlanFetchOtherMs
               then clamps a negative residual to zero and the line reads as precise while
               being arithmetically impossible. */
            var planFetchWatch = Stopwatch.StartNew();
            try
            {
                await FetchAndStorePlansAsync(targetProvider, targetConnection, storeConnection,
                    server, databaseName, collectorName, context, commandTimeoutSeconds, ExtractPlanReferences(batch), fetchSelection?.PlanIds, cancellationToken);
            }
            finally
            {
                context.PerItemPlanFetchMs = planFetchWatch.ElapsedMilliseconds;
            }
        }

        /* #2150: and this database's statement-text fetch, for the same reason and with the
           same shape - the payload no longer carries query_sql_text, because selecting it
           inside the shipping TOP/ORDER BY made a Top-N Sort materialize nvarchar(max) text
           for the whole qualifying set (measured 4.67s vs 0.45s time-to-first-row). Ships in
           query_id order so a budget cut is a suffix, which is what lets the watermark
           advance from a cut pass.

           Gated on the same flag the payload branches on, so the two can never disagree
           about who owns the text: if the column is nulled, this runs. */
        if (context.FetchQueryTextSeparately && targetProvider.Engine == CollectorTargetEngine.SqlServer)
        {
            /* #2312 investigation: same split as the plan fetch above. */
            /* #2854: stamped from finally, same parent/child inconsistency as the plan fetch. */
            var textFetchWatch = Stopwatch.StartNew();
            try
            {
                await FetchAndStoreQueryTextAsync(targetProvider, targetConnection, storeConnection,
                    server, databaseName, collectorName, context, commandTimeoutSeconds, ExtractTextReferences(batch), fetchSelection?.QueryIds, cancellationToken);
            }
            finally
            {
                context.PerItemTextFetchMs = textFetchWatch.ElapsedMilliseconds;
            }
        }
    }

    /// <summary>The always-fetch look-back (#6211): the newest forced-plans capture and the plan-correction rows within this window name plans that are fetched whatever their rank.</summary>
    private static readonly TimeSpan s_alwaysFetchLookBack = TimeSpan.FromDays(1);

    /// <summary>
    /// #6211: the plans of one database's <c>query_store</c> batch that are worth a text and plan fetch this run, or <c>null</c> for "all of them".
    ///
    /// <para>The ranking itself is <see cref="QueryStoreFetchSelection"/>: the top N per database-hour on seven metrics, in the service, over the
    /// rows already read (no statement on the monitored server, so nothing here touches a <c>sys.query_store_*</c> view). This method adds the two
    /// store-side always-fetch sets: the queries with a forced plan in the newest forced-plans capture (#6221 judges a force by the stored XML of
    /// the query's other plans, so every plan of such a query is kept) and the plans the plan-correction collector names (the regressed plan and
    /// the last good plan: the only "regressed" signal Darling collects). A plan that does not rank is still "not stored" to the next run, so it
    /// is fetched then if it ranks; nothing is carried for it, so the candidate list cannot grow with the unranked.</para>
    ///
    /// <para>Fails open: a store read that throws returns <c>null</c> and the run fetches everything, because skipping a forced plan on a fault
    /// is worse than one expensive run.</para>
    /// </summary>
    private async Task<QueryStoreFetchSelection.Selection?> SelectQueryStoreFetchAsync<TRow>(
        NpgsqlConnection storeConnection,
        ServerRuntime server,
        string databaseName,
        CollectorContext context,
        List<TRow> batch,
        CancellationToken cancellationToken)
    {
        var topN = StoreConfigProvider.ClampQueryStoreFetchTopN(_queryStoreFetchTopN());
        if (topN <= 0 || batch is not List<QueryStoreCollector.Row> rows || rows.Count == 0)
        {
            return null;
        }

        try
        {
            if (!await RestoreBorrowedStoreConnectionAsync(storeConnection, server, databaseName))
            {
                return null;
            }

            var since = DateTime.SpecifyKind(context.CollectionTime - s_alwaysFetchLookBack, DateTimeKind.Unspecified);
            var alwaysQueries = new HashSet<long>();
            using (var forced = new NpgsqlCommand(@"
SELECT DISTINCT query_id
FROM collect.query_store_forced_plans
WHERE server_id = $1
AND   database_name = $2
AND   plan_id IS NOT NULL
AND   query_id IS NOT NULL
AND   collection_time = (SELECT MAX(m.collection_time)
                         FROM collect.query_store_forced_plans AS m
                         WHERE m.server_id = $1
                         AND   m.database_name = $2
                         AND   m.collection_time >= $3)", storeConnection))
            {
                forced.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                forced.Parameters.Add(new NpgsqlParameter<int> { TypedValue = server.ServerId });
                forced.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
                forced.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = since });
                await using var reader = await forced.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    alwaysQueries.Add(reader.GetInt64(0));
                }
            }

            var alwaysPlans = new HashSet<long>();
            using (var correction = new NpgsqlCommand(@"
SELECT regressed_plan_id, last_good_plan_id
FROM collect.plan_correction
WHERE server_id = $1
AND   database_name = $2
AND   collection_time >= $3", storeConnection))
            {
                correction.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                correction.Parameters.Add(new NpgsqlParameter<int> { TypedValue = server.ServerId });
                correction.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
                correction.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = since });
                await using var reader = await correction.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    if (!reader.IsDBNull(0)) alwaysPlans.Add(reader.GetInt64(0));
                    if (!reader.IsDBNull(1)) alwaysPlans.Add(reader.GetInt64(1));
                }
            }

            var (stored, _) = await ReadStoredHourRowsAsync(storeConnection, server.ServerId, databaseName, context.CollectionTime, rows, cancellationToken);

            var selection = QueryStoreFetchSelection.Select(rows, topN, alwaysPlans, alwaysQueries, stored);
            _logger?.LogDebug(
                "  [{Server}] query_store [{Database}] fetch top {TopN}: {Selected} of {Considered} plan(s) selected ({Ranked} by rank, the rest forced or named by plan correction)",
                server.Config.DisplayName, databaseName, topN, selection.PlanIds.Count, selection.ConsideredPlans, selection.RankedPlans);
            return selection;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning(ex,
                "query_store fetch top-N selection on '{Server}' database [{Database}] could not read the forced or plan-correction plans; fetching every new plan this run.",
                server.Config.DisplayName, databaseName);
            return null;
        }
    }

    /// <summary>#6211: the stored-hour read from the wide table: one bounded range (server, database, interval ids, partition-pruning floor on <c>first_execution_time</c>).</summary>
    internal const string StoredHourWideSql = @"
SELECT plan_id, query_id, runtime_stats_interval_id, execution_type_desc, replica_role, execution_count, avg_cpu_time_us, avg_duration_us,
       avg_physical_io_reads, avg_logical_io_writes, max_query_max_used_memory, max_tempdb_space_used
FROM collect.query_store_interval_wide
WHERE server_id = $1
AND   database_name = $2
AND   first_execution_time >= $3
AND   runtime_stats_interval_id = ANY($4)";

    /// <summary>
    /// #6211: the hour's rows earlier runs already stored, so the rank is over the whole database-hour (see <see cref="QueryStoreFetchSelection"/>):
    /// one row per (plan, interval, execution type, replica), the newest snapshot, for the intervals this batch names.
    ///
    /// <para><b>Wide table first.</b> This runs every cycle for every server. Raw <c>query_store_stats</c> holds several cumulative snapshots per
    /// interval, so reading it needs a DISTINCT ON sort over all of them. <c>collect.query_store_interval_wide</c> (V145) already holds exactly the
    /// latest snapshot per interval identity, every outcome, with the same columns, and is partitioned by day on <c>first_execution_time</c>
    /// (the interval's own start at the earliest, never before it: <see cref="QueryStoreIntervalWide.IntervalStartSlack"/>). So the read is
    /// one bounded range of the table: <c>server_id</c>, the database, the interval ids, and a <c>first_execution_time</c> floor that prunes
    /// the partitions.</para>
    ///
    /// <para><b>Raw only where the wide table cannot answer</b> (its coverage gate): when this server has no coverage row, the row's
    /// <c>filled_since</c> is later than the oldest interval start in the batch (the table was not yet filled for that hour, so right after
    /// an install the first hour reads raw, which is small then), or the table has a pending batch (an apply that failed). Then the answer is
    /// the raw read this method used first. The gate is conservative: a wide table that lags only leaves a competitor out of the rank, so the
    /// batch plans rank higher and the selection takes more plans, never fewer.</para>
    ///
    /// <para>Rows with no <c>runtime_stats_interval_id</c> (collected before the column existed) are not read here and rank as one batch
    /// (<see cref="QueryStoreFetchSelection"/>), which also selects more, never less.</para>
    /// </summary>
    internal static async Task<(List<QueryStoreCollector.Row> Rows, bool UsedWideTable)> ReadStoredHourRowsAsync(
        NpgsqlConnection storeConnection,
        int serverId,
        string databaseName,
        DateTime collectionTime,
        List<QueryStoreCollector.Row> rows,
        CancellationToken cancellationToken)
    {
        var stored = new List<QueryStoreCollector.Row>();
        var intervalIds = rows.Where(r => r.RuntimeStatsIntervalId is not null).Select(r => r.RuntimeStatsIntervalId!.Value).Distinct().ToArray();
        if (intervalIds.Length == 0)
        {
            return (stored, false);
        }

        var starts = rows.Where(r => r.IntervalStartTimeUtc is not null).Select(r => r.IntervalStartTimeUtc!.Value).ToList();
        var floor = DateTime.SpecifyKind(starts.Count > 0 ? starts.Min() : collectionTime - s_alwaysFetchLookBack, DateTimeKind.Unspecified);

        bool wideCovers;
        using (var gate = new NpgsqlCommand(@"
SELECT EXISTS
(
    SELECT 1
    FROM collect.query_store_interval_wide_coverage AS c
    WHERE c.server_id = $1
    AND   c.filled_since <= $2
)
AND NOT EXISTS
(
    SELECT 1
    FROM collect.query_store_interval_wide_pending AS p
    WHERE p.server_id = $1
);", storeConnection))
        {
            gate.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
            gate.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            gate.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor });
            wideCovers = (bool)(await gate.ExecuteScalarAsync(cancellationToken))!;
        }

        string sql = wideCovers
            ? StoredHourWideSql
            : @"
SELECT DISTINCT ON (plan_id, runtime_stats_interval_id, execution_type_desc, replica_role)
       plan_id, query_id, runtime_stats_interval_id, execution_type_desc, replica_role, execution_count, avg_cpu_time_us, avg_duration_us,
       avg_physical_io_reads, avg_logical_io_writes, max_query_max_used_memory, max_tempdb_space_used
FROM query_store_stats
WHERE server_id = $1
AND   database_name = $2
AND   collection_time >= $3
AND   runtime_stats_interval_id = ANY($4)
ORDER BY plan_id, runtime_stats_interval_id, execution_type_desc, replica_role, collection_time DESC";

        /* Bounded below by the oldest interval start in the batch. The wide table's partition key is first_execution_time, which is at or after
           the interval's start (the slack below is the documented margin for a clock edge); raw is bounded on collection_time, which is at or
           after the interval's start too. */
        var lower = wideCovers ? floor - QueryStoreIntervalWide.IntervalStartSlack : floor;
        using var hour = new NpgsqlCommand(sql, storeConnection);
        hour.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
        hour.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        hour.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName });
        hour.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = lower });
        hour.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = intervalIds });
        await using var reader = await hour.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            stored.Add(new QueryStoreCollector.Row
            {
                DatabaseName = databaseName,
                PlanId = reader.GetInt64(0),
                QueryId = reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                RuntimeStatsIntervalId = reader.GetInt64(2),
                ExecutionTypeDesc = reader.IsDBNull(3) ? null : reader.GetString(3),
                ReplicaRole = reader.IsDBNull(4) ? null : reader.GetString(4),
                ExecutionCount = reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                AvgCpuTimeUs = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                AvgDurationUs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                AvgPhysicalIoReads = reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                AvgLogicalIoWrites = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                MaxQueryMaxUsedMemory = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                MaxTempdbSpaceUsed = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
            });
        }

        return (stored, wideCovers);
    }

    /// <summary>
    /// Zeroes the plan-fetch and text-fetch readings on a context that is reused from one database to the next.
    /// Called before each database is read, by both the enumerated per-item read and the Azure per-database loop,
    /// so a database whose fetch is skipped or faults early cannot hand the previous database's split to the log
    /// line or to the run's fetch sums as its own.
    /// </summary>
    private static void ClearFetchPhaseStamps(CollectorContext context)
    {
        context.PerItemPlanFetchMs = 0;
        context.PerItemTextFetchMs = 0;
        context.PerItemPlanRenderedRows = 0;
        context.PerItemPlanRenderedBytes = 0;

        /* #2811: the sub-phases clear on the SAME rule as their parents, and for the same
           reason - an item whose fetch faults before setting them must not print the previous
           database's split as its own. A stale sub-split is worse than a stale total, because
           it looks precise. */
        context.PerItemPlanProbeMs = 0;
        context.PerItemPlanTargetMs = 0;
        context.PerItemPlanWriteMs = 0;
        context.PerItemPlanChunks = 0;
        context.PerItemPlanIdsAttempted = 0;
        context.PerItemPlanProbeIds = 0;
        context.PerItemTextProbeMs = 0;
        context.PerItemTextTargetMs = 0;
        context.PerItemTextWriteMs = 0;
        context.PerItemTextChunks = 0;
        context.PerItemTextIdsAttempted = 0;
        context.PerItemTextProbeIds = 0;
    }

    /// <summary>
    /// The plan-fetch and text-fetch sub-split lines for one database, each only when that fetch actually ran.
    /// They ride their OWN lines rather than nesting inside the per-database row line, because that line is parsed
    /// by tooling outside this repo and "don't break the parser" outranks "one line to grep". A text-only pass
    /// prints one line and a collector that never fetches prints none. Debug, like the line they decompose (#3102).
    /// </summary>
    private void LogFetchPhaseSplits(ServerRuntime server, string collectorName, string databaseName, CollectorContext context)
    {
        if (context.PerItemPlanFetchMs > 0)
        {
            _logger?.LogDebug("  [{Server}] {Collector} [{Database}] plan_fetch:{PlanFetchMs}ms = probe:{ProbeMs}ms + target:{TargetMs}ms + write:{WriteMs}ms + other:{OtherMs}ms ({Chunks} chunk(s), {Ids} ids, {ProbeIds} probed)",
                server.Config.DisplayName, collectorName, databaseName, context.PerItemPlanFetchMs,
                context.PerItemPlanProbeMs, context.PerItemPlanTargetMs, context.PerItemPlanWriteMs,
                context.PlanFetchOtherMs, context.PerItemPlanChunks, context.PerItemPlanIdsAttempted,
                context.PerItemPlanProbeIds);
        }

        if (context.PerItemTextFetchMs > 0)
        {
            _logger?.LogDebug("  [{Server}] {Collector} [{Database}] text_fetch:{TextFetchMs}ms = probe:{ProbeMs}ms + target:{TargetMs}ms + write:{WriteMs}ms + other:{OtherMs}ms ({Chunks} chunk(s), {Ids} ids, {ProbeIds} probed)",
                server.Config.DisplayName, collectorName, databaseName, context.PerItemTextFetchMs,
                context.PerItemTextProbeMs, context.PerItemTextTargetMs, context.PerItemTextWriteMs,
                context.TextFetchOtherMs, context.PerItemTextChunks, context.PerItemTextIdsAttempted,
                context.PerItemTextProbeIds);
        }
    }

    /// <summary>
    /// The activity-driven plan-XML fetch for one database (#2312 Finding 2): touch-and-probe the store for
    /// the cycle's referenced plans — which refreshes map/dim liveness (Finding 3's unwired TouchSql, now
    /// the same round trip) and answers which plans are missing or hash-stale — then fetch exactly those by
    /// id, budget-bounded, and land them into the shared dimension plus the map. The store is the
    /// watermark: a caught-up database's missing set is EMPTY and no target query runs at all, which is the
    /// property the retired catalog walk lacked (measured 23s per cycle to discover "nothing new").
    ///
    /// <para>Failure-isolated, and that is load-bearing rather than defensive: plan XML is an enrichment on
    /// top of runtime statistics, so a fetch that throws must not cost the database its runtime stats. It
    /// logs and returns; whatever did not land is still missing from the store, so the next cycle that
    /// references it re-selects it by construction.</para>
    ///
    /// <para>Budget-deferred and capped ids go to <see cref="_planFetchCarryover"/>, because the probe's
    /// input is each cycle's batch references: a plan referenced ONCE whose fetch was deferred would
    /// otherwise never re-enter the probe. Ids the target no longer has (Query Store cleanup took the plan
    /// between reference and fetch) are dropped from the debt — but only on a pass that provably completed
    /// uncut, because inside a cut pass "absent from the result" and "excluded by the budget predicate" are
    /// indistinguishable from the client.</para>
    /// </summary>
    private async Task FetchAndStorePlansAsync(
        ITargetProvider targetProvider,
        DbConnection targetConnection,
        NpgsqlConnection storeConnection,
        ServerRuntime server,
        string databaseName,
        string collectorName,
        CollectorContext context,
        int itemTimeout,
        IReadOnlyList<(long PlanId, string? PlanHash)> references,
        HashSet<long>? fetchOnlyPlanIds,
        CancellationToken cancellationToken)
    {
        /* Hoisted out of the try (#2776) so the catch can advance this database's backoff counter — the
           handler needs the same key the body uses. Carries the collector since #2902 — see
           FetchStateKey for why the database alone let four other collectors drain this one's debt. */
        var carryKey = FetchStateKey(server.ServerId, databaseName, collectorName);

        /* Hoisted for the same shape of reason (#2816): the probe stamp sits at the END of the probe phase,
           so a probe that THROWS jumps clean over it and reports probe:0ms. The residual then absorbs the
           whole cost into other:, which is documented as this method's own bookkeeping — "the cost would be
           in our own code, not in either database" — and that is the exact INVERSE of the truth for a store
           round trip that timed out. The target and write phases already stamp from finally blocks for
           precisely this reason (#2811); the probe was the one phase that did not. Measured on 2026-09-03:
           one failed probe put 43,053ms into other:, 97% of the whole fleet's other: budget that day. */
        var probeWatch = new Stopwatch();
        var probeStamped = false;

        /* #2819: set by the catch, acted on AFTER it. The restore needs an await, and an await inside a
           catch makes the compiler lift the handler's body out of the exception region — which would break
           #2816's pin that the probe stamp is reachable from inside a handler, and with it the guarantee
           that a throwing probe reports its own time instead of donating it to other:. Flag here, await
           below: the property #2816 holds and the recovery #2819 needs are not in tension, they just cannot
           share a block. */
        var storeConnectionSuspect = false;

        try
        {
            /* FIRST statement in the try, ahead of the quiet-cycle return below, and that ordering is
               the whole point. The catch further down filters out OperationCanceledException, and OCE is
               the fault most likely here: this method's token is the per-item wall-clock budget, not host
               shutdown, and Npgsql throws OCE when a caller-supplied token fires. So a database that simply
               ran slow mid-probe cancels, propagates past that catch, and never reaches the restore at the
               bottom — leaving the BORROWED connection broken for whoever runs next.

               Putting the repair after the early return would have missed exactly the case that matters.
               "Nothing referenced, nothing owed" is derived from this database's own just-read rows and is
               completely independent of whether it has runtime stats to WRITE, so a busy-but-stable database
               routinely returns early here while still handing the driver a non-empty batch. writeBatch then
               runs on the still-broken connection, outside any per-item try/catch, and propagates — aborting
               the rest of the sweep. That is precisely the whole-cycle abort this borrowing must not buy.

               Classifying on connection STATE rather than exception type is the discipline
               EnumeratedCollectorDriver.ItemBudgetExpired already applies to the budget question: the tokens
               and the connection know, the exception type does not. Free when nothing is wrong — the helper
               returns immediately on an Open connection — and outside the probe measurement on purpose,
               since a reopen is recovery rather than store work. */
            /* Timed into probe:, not left to other:. A reconnect is store-connection time — the very cost
               this PR removed from the steady-state path — and other: is documented as work in NEITHER
               database, so letting a reopen land there would recreate the misattribution #2816 fixed for
               the probe stamp. Zero on the ordinary pass: the helper returns on a state check. */
            probeWatch.Restart();
            if (!await RestoreBorrowedStoreConnectionAsync(storeConnection, server, databaseName))
            {
                /* The store is still down. Returning beats falling through: the probe below would fault on
                   the same Broken connection, log a second "could not reopen" for one underlying outage,
                   and the text sibling would double it again per item. This codebase caps repetitive
                   failure logging deliberately (MaxLoggedProbeFailures), and attempting store work already
                   known to be doomed is not diagnosis. The carryover is untouched, so nothing is forgotten
                   and the next cycle re-selects it.

                   Stamped before returning, like every other exit from this method: the failed reconnect
                   still took real wall clock, the caller's fetch watch still counted it, and leaving it
                   unstamped would hand the one measurable part of an outage to the other: residual. */
                context.PerItemPlanProbeMs = probeWatch.ElapsedMilliseconds;
                return;
            }

            var hasCarryover = _planFetchCarryover.TryGetValue(carryKey, out var carriedIds);
            if (references.Count == 0 && !hasCarryover)
            {
                /* The steady quiet cycle: nothing referenced, nothing owed. Zero store reads, zero target
                   queries — the whole point of the reshape.

                   Stamped anyway, matching the missing.Count == 0 return below. Normally this is 0ms, but
                   the repair above CAN have reconnected a connection a previous item broke, and that is
                   real store time: leaving it unstamped would drop it into the other: residual, which is
                   documented as work in neither database. A quiet cycle should read as free because it WAS
                   free, not because its one real cost went unattributed. */
                context.PerItemPlanProbeMs = probeWatch.ElapsedMilliseconds;
                ClearPlanFetchBackoff(carryKey);
                return;
            }

            /* #2811 named this phase "the store connection open PLUS the touch/probe round trip", and
               declined to split them because doing so "would name a connection pool rather than a cost".
               #2819 measured it and it WAS the connection pool: the probe SQL runs in 0.4ms (nothing stale)
               to 37ms (40 stale ids), while the phase measured 673-6,663ms — and a cycle with zero ids,
               which issues no SQL at all, still cost 673ms. That is acquisition, not work.

               So this method no longer acquires. It borrows the caller's connection — the one opened once
               per collector body at the top of RunEnumeratedAsync and described there as "one pooled store
               connection for the whole body", a promise this method and its text sibling were quietly
               breaking by opening a second and third. The driver is strictly sequential (read an item, then
               await its write), so that connection is provably IDLE across exactly the window this fetch
               occupies; there is no concurrent use to collide with.

               What that buys: ~228 acquisitions per cycle (114 (server, database) pairs x two fetches)
               against a MaxPoolSize of 24 collapse to zero, and the pool slot that used to be held across
               the SQL Server target fetch — measured at 104,799ms on one database — is no longer held at
               all, because it was already held by the caller regardless.

               The trade, stated at its real size rather than a flattering one: the borrowed connection is
               the BODY's, opened once per collector run and reused for every database in the sweep — so a
               store fault in here does not just cost this item its write, it breaks the connection every
               SUBSEQUENT item in the cycle will probe and write on. That is a larger blast radius than a
               private connection had, and worth naming plainly.

               It is still the right trade, because the write path already had exactly this shape: writeBatch
               has always run on this same shared connection, so those later writes were going to fail on it
               regardless. What changes is that their probes fail alongside, and a broken store connection
               means the store is unreachable anyway. The driver's per-item catch skips each affected item
               and the next cycle re-selects it, which is the recovery either arm takes.

               probeWatch now times the probe ROUND TRIP only, which is what the phase name always claimed.
               Started HERE rather than at the declaration so the quiet-cycle return above stays outside the
               measurement, while a reconnect above it does not vanish into the residual. */

            var missing = new SortedSet<long>();
            if (hasCarryover)
            {
                foreach (var id in carriedIds!)
                {
                    missing.Add(id);
                }
            }

            if (references.Count > 0)
            {
                /* #3189/#3216: skip the store round trip entirely for references a recent probe already
                   confirmed resolved-and-current — see QueryStoreProbeCache for the measurement (77-80%
                   fleet recurrence) and why the TTL is the same GuardHours the touch guard already treats
                   as an acceptable staleness bound for these rows. Cache hits are implicitly resolved and
                   current; only misses reach the probe below. */
                var toProbe = _planProbeCache.SelectNeedingProbe(
                    server.ServerId, databaseName, references, context.CollectionTime, s_probeCacheTtl);
                if (toProbe.Count < references.Count)
                {
                    var toProbeIds = new HashSet<long>(toProbe.Count);
                    foreach (var reference in toProbe)
                    {
                        toProbeIds.Add(reference.Id);
                    }

                    foreach (var reference in references)
                    {
                        if (!toProbeIds.Contains(reference.PlanId))
                        {
                            /* Cache-confirmed resolved and current: if it was carried debt, it is paid. */
                            missing.Remove(reference.PlanId);
                        }
                    }
                }

                /* #2823: the probe's INPUT size, stamped where the input is known — now the size of what
                   actually reaches the store, which is the figure that determines the round trip's cost
                   post-cache. probe: scales with this (~0.61ms/reference), not with PerItemPlanIdsAttempted,
                   which counts only what came back missing — so a pass probing hundreds of references and
                   owing nothing logs 0 ids while doing real store work. Logging one and dividing by the
                   other produced a phantom 140x gap twice (#2819, #2822). */
                context.PerItemPlanProbeIds = toProbe.Count;
                if (toProbe.Count > 0)
                {
                    var verdicts = await QueryStoreFetchProbe.TouchAndProbePlansAsync(
                        storeConnection, server.ServerId, databaseName, toProbe, context.CollectionTime, itemTimeout, cancellationToken);
                    var hashById = new Dictionary<long, string?>(toProbe.Count);
                    foreach (var reference in toProbe)
                    {
                        hashById[reference.Id] = reference.Hash;
                    }

                    foreach (var verdict in verdicts)
                    {
                        if (!verdict.Resolved || verdict.HashStale)
                        {
                            /* #6211: every reference was touched and probed above (that is the only last_seen refresh a stored plan has),
                               but only the selected ones are added to what is fetched (the debt already owed is in `missing` from the start). A plan that did not rank is not stored
                               and not carried: it is fetched on a later run if it ranks then. */
                            if (fetchOnlyPlanIds is null || fetchOnlyPlanIds.Contains(verdict.Id))
                            {
                                missing.Add(verdict.Id);
                            }
                        }
                        else
                        {
                            /* Resolved and current: if it was carried debt, it is paid, and the cache
                               remembers this exact (id, hash) so the NEXT cycle that still references it
                               can skip the round trip too. */
                            missing.Remove(verdict.Id);
                            _planProbeCache.Confirm(server.ServerId, databaseName, verdict.Id, hashById[verdict.Id], context.CollectionTime);
                        }
                    }
                }
            }

            /* Stamped BEFORE the nothing-owed return below, so a pass that is pure store round trip still
               reports where its milliseconds went. That pass is the interesting one: it issues no target
               query at all, so anything it costs is unambiguously the store. */
            context.PerItemPlanProbeMs = probeWatch.ElapsedMilliseconds;
            probeStamped = true;

            if (missing.Count == 0)
            {
                /* The probe round-tripped the store and came back with nothing owed — a stronger proof of
                   store health than the quiet cycle above, since this one actually wrote touch timestamps.
                   Treat it as the completed pass it is (#2776). */
                _planFetchCarryover.TryRemove(carryKey, out _);
                ClearPlanFetchBackoff(carryKey);
                return;
            }

            var budget = context.TextByteBudgetOverride ?? 12 * 1024 * 1024;

            /* #2312 Finding 1 (#2322): cap the attempt from THIS database's learned average instead of the
               160KB seed every pass — zero AvgBytes means never learned, which is the seed's job. The cap
               bounds server-side DECOMPRESSION (the running total materializes every plan it measures), so
               it stays load-bearing even though the walk it originally sized is gone. */
            var estimate = _observedPlanSize.TryGetValue(carryKey, out var carriedEstimate)
                ? carriedEstimate
                : default;
            var cap = QueryStorePlanXmlState.CandidatePlanCount(
                estimate.AvgBytes > 0 ? estimate.AvgBytes : null, budget, estimate.CatchUpInProgress, out var clamped);
            if (clamped)
            {
                _logger?.LogInformation(
                    "query_store plan fetch on '{Server}' database [{Database}]: candidate cap clamped to {K} — a bound sized this pass, not a measurement.",
                    server.Config.DisplayName, databaseName, cap);
            }

            /* #2776: narrow the width by the consecutive-failure count before it is used. A database whose
               store write keeps timing out re-paid FULL decompression every cycle and re-attempted a write
               the store had already proven it could not commit; halving per failure converges on a width
               that fits. Inert at zero failures, floored so the database never stops. */
            var backedOff = QueryStorePlanXmlState.NarrowForFailures(cap, estimate.ConsecutiveFetchFailures);
            if (backedOff != cap)
            {
                _logger?.LogInformation(
                    "query_store plan fetch on '{Server}' database [{Database}]: width narrowed {Cap} -> {Narrowed} after {Failures} consecutive failure(s) — backing off, not giving up; a completed pass restores full width.",
                    server.Config.DisplayName, databaseName, cap, backedOff, estimate.ConsecutiveFetchFailures);
                cap = backedOff;
            }

            /* Ascending ids (SortedSet order) so the budget's in-SQL cut and the cross-chunk break are
               deterministic — the same debt is retried in the same order until paid. */
            var attempt = missing.Take(cap).ToList();
            var attempted = new List<long>(attempt.Count);
            var fetched = new List<FetchedPlan>();
            var shippedBytes = 0L;
            var brokeOnBudget = false;

            /* #2811: the TARGET half, and only the target half — the statement plus its read loop, summed
               across chunks. Query building and the budget check sit outside deliberately: they are our
               arithmetic, and folding them in here would let this number quietly absorb the thing the
               residual is meant to expose. */
            var targetMs = 0L;

            /* #5813: the plan fetch's TIME budget for this database and pass, beside the byte budget above.
               The field's slow query_store runs spent 91 s of 97 here (about 136 plans at up to 2 s each).
               Each chunk asks for as many plans as the time left fits at this database's last measured time
               per plan (QueryStoreReadBudget.PlanFetchWidth), so a slow database asks for fewer and fits, and
               the chunk runs under the time left as a linked token (shutdown and the item budget keep
               precedence), so a pass that is slower than predicted is still cut at the budget. Either way the
               ids not received stay in `missing` and go to the carryover, which the next pass fetches first;
               the uncut-only cleanup below stays off (brokeOnBudget), so no id the cut did not reach is
               dropped. */
            var planFetchClock = Stopwatch.StartNew();
            double? msPerPlan = _planFetchMsPerPlan.TryGetValue(carryKey, out var measuredMsPerPlan) ? measuredMsPerPlan : null;
            var timeBound = false;
            var cutOnTime = false;
            var lastChunkWidth = 0;
            var offset = 0;

            while (offset < attempt.Count)
            {
                if (shippedBytes >= budget)
                {
                    brokeOnBudget = true;
                    break;
                }

                var timeLeft = QueryStorePlanFetchBudgetSpan - planFetchClock.Elapsed;
                var width = QueryStoreReadBudget.PlanFetchWidth(timeLeft, msPerPlan, PlanFetchIdsPerStatement, firstChunk: offset == 0);
                if (width <= 0)
                {
                    /* The time left does not fit one more plan: leave the rest to the next pass. */
                    brokeOnBudget = true;
                    timeBound = true;
                    break;
                }

                var chunk = attempt.GetRange(offset, Math.Min(width, attempt.Count - offset)).ToArray();
                offset += chunk.Length;
                lastChunkWidth = chunk.Length;

                var query = QueryStoreCollector.Instance.BuildPlanFetchByIdsQuery(
                    databaseName, context, chunk, budget - shippedBytes);

                using var fetchBudget = EnumeratedCollectorDriver.StartItemBudget(timeLeft, cancellationToken);
                var fetchToken = fetchBudget?.Token ?? cancellationToken;
                var chunkWatch = Stopwatch.StartNew();
                try
                {
                    using var command = CreateCollectorCommand(targetProvider, query, targetConnection, itemTimeout);
                    await using var reader = await command.ExecuteReaderAsync(fetchToken);
                    attempted.AddRange(chunk);
                    while (await reader.ReadAsync(fetchToken))
                    {
                        var planXml = reader.IsDBNull(2) ? null : reader.GetString(2);
                        fetched.Add(new FetchedPlan(
                            reader.GetInt64(0),
                            planXml,
                            reader.IsDBNull(1) ? null : reader.GetString(1)));
                        if (planXml is not null)
                        {
                            /* nvarchar length * 2 is DATALENGTH exactly — no server round-trip needed. */
                            shippedBytes += (long)planXml.Length * 2;
                        }
                    }
                }
                catch (Exception ex) when (ex is not OutOfMemoryException && QueryStoreReadBudget.Expired(fetchBudget, cancellationToken))
                {
                    /* #5813: the plan-fetch time budget fired, not shutdown or the item budget (those propagate
                       as before). Keep every plan already received; they are written below. */
                    brokeOnBudget = true;
                    timeBound = true;
                    cutOnTime = true;
                }
                finally
                {
                    /* finally, not after the block: a chunk that TIMES OUT is the case this instrumentation
                       most needs to describe, and stamping only on success would report target:0ms for it —
                       "the target was free" is the exact misreading this change exists to end. The ids and
                       chunk count are stamped here too so the per-id cost stays computable on a failed pass. */
                    targetMs += chunkWatch.ElapsedMilliseconds;
                    context.PerItemPlanTargetMs = targetMs;
                    context.PerItemPlanChunks++;
                    context.PerItemPlanIdsAttempted += chunk.Length;
                }

                if (cutOnTime)
                {
                    break;
                }
            }

            /* #5813: the time per plan the next pass sizes its chunks from. A pass cut before it received
               anything records twice the cut chunk's share, so the next pass asks for at most half as many:
               the statement decompresses every requested plan before it ships one, so the same width would be
               cut empty on every pass. */
            var nextMsPerPlan = QueryStoreReadBudget.NextMsPerPlan(msPerPlan, targetMs, fetched.Count, cutOnTime, lastChunkWidth);
            if (nextMsPerPlan is double nextMs)
            {
                _planFetchMsPerPlan[carryKey] = nextMs;
            }

            var cutEmpty = cutOnTime && fetched.Count == 0;

            /* NULL-XML rows count for the cap/catch-up comparison (they shipped, and the writer records
               their content-less marker) but not for the average's divisor (they carried no bytes). */
            var plansMeasured = 0;
            foreach (var plan in fetched)
            {
                if (plan.PlanXml is not null)
                {
                    plansMeasured++;
                }
            }

            /* #5813: a pass the time budget ended early stopped short of its cap with ids still owed, which is a
               backlog, not the drain Learn reads "fewer than the cap" as. Said explicitly rather than through the
               window: the common cut received nothing, and a ship count of zero cannot say "cut" (review round 1). */
            _observedPlanSize[carryKey] =
                QueryStorePlanXmlState.Learn(
                    estimate, shippedBytes, fetched.Count, plansMeasured, cap, budget, timeBound);

            var returned = new HashSet<long>(fetched.Count);
            if (fetched.Count > 0)
            {
                /* #2811: the store WRITE, on its own stopwatch. #2777 raised this command's timeout from
                   Npgsql's unchosen 30s default to 500s because it was being cancelled mid-write; that fixed
                   the failure but left the duration unmeasured, so "the write is slow" stayed an assertion. */
                var writeWatch = Stopwatch.StartNew();
                IReadOnlyList<long> landed;
                try
                {
                    /* #4348: the cycle's statement-filter session, so the writer's counters join the cycle's
                       statement_scrub_* measurements. The writer drops a row the session could not judge. */
                    landed = await QueryStorePlanWriter.WriteAsync(
                        storeConnection, server.ServerId, databaseName, fetched, context.CollectionTime, itemTimeout, context.BeginStatementScrub(),
                        cancellationToken);
                }
                finally
                {
                    /* Same finally-not-after reasoning as the target chunks: a cancelled write is precisely
                       the event #2777 chased, and it must not report write:0ms. */
                    context.PerItemPlanWriteMs = writeWatch.ElapsedMilliseconds;
                }

                foreach (var id in landed)
                {
                    missing.Remove(id);
                }

                foreach (var plan in fetched)
                {
                    returned.Add(plan.PlanId);
                }
            }

            /* Target-side-gone cleanup, only when the pass provably completed UNCUT: every chunk issued
               and the in-SQL predicate never fired (a fired cut leaves shipped at or past the remaining
               budget by the oversized-admission arithmetic). On such a pass an attempted id with no
               returned row does not exist in sys.query_store_plan any more — Query Store cleanup took it
               between reference and fetch — and carrying it forever would be the content-less stall
               wearing a new hat. */
            if (!brokeOnBudget && shippedBytes < budget && attempted.Count == attempt.Count)
            {
                foreach (var id in attempted)
                {
                    if (!returned.Contains(id))
                    {
                        missing.Remove(id);
                    }
                }
            }

            if (missing.Count > 0)
            {
                var owed = new long[missing.Count];
                missing.CopyTo(owed);
                _planFetchCarryover[carryKey] = owed;
            }
            else
            {
                _planFetchCarryover.TryRemove(carryKey, out _);
            }

            /* #2776: the pass completed — restore full width. Recorded HERE rather than inside Learn
               because Learn runs before the store write, so a pass that threw would otherwise clear its own
               backoff on the way down. Reaching this line is the only proof the write actually committed.

               #5813: except a pass the time budget cut before it received any plan. It made no progress, and
               the same width would be cut empty again, so it counts as a failure: the backoff narrows the
               next pass's cap as #2776 does for a thrown pass, beside the halved time per plan above. Any plan
               received is progress and restores full width as before. */
            _observedPlanSize.AddOrUpdate(
                carryKey,
                cutEmpty
                    ? static (_, _) => QueryStorePlanXmlState.RecordFetchFailure(default)
                    : static (_, _) => default,
                cutEmpty
                    ? static (_, current, _) => QueryStorePlanXmlState.RecordFetchFailure(current)
                    : static (_, current, _) => QueryStorePlanXmlState.RecordFetchSuccess(current),
                0);

            if (timeBound)
            {
                NoteQueryStorePlanFetchBudget(
                    server, databaseName, cutOnTime, fetched.Count, missing.Count,
                    QueryStoreReadBudget.PlanFetchWidth(QueryStorePlanFetchBudgetSpan, nextMsPerPlan, PlanFetchIdsPerStatement, firstChunk: true),
                    nextMsPerPlan);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2816: the probe stamp is unreachable once its own await throws, so stamp it here instead.
               Guarded by the flag rather than by `== 0` because a throw in the LATER target or write phases
               must not overwrite an already-honest probe reading with the whole elapsed span — and a probe
               that genuinely measured 0ms is not distinguishable from an unstamped one by value alone. */
            if (!probeStamped)
            {
                context.PerItemPlanProbeMs = probeWatch.ElapsedMilliseconds;
            }

            storeConnectionSuspect = true;

            /* #2776: advance the backoff so the next pass attempts a narrower width. Read-modify-write
               through the dictionary rather than the local `estimate`, because Learn may already have
               written a newer record for this key and clobbering it would discard this pass's size
               learning. */
            var failed = _observedPlanSize.AddOrUpdate(
                carryKey,
                static (_, _) => QueryStorePlanXmlState.RecordFetchFailure(default),
                static (_, current, _) => QueryStorePlanXmlState.RecordFetchFailure(current),
                0);

            _logger?.LogWarning(ex,
                "query_store plan fetch failed on '{Server}' database [{Database}] ({Failures} consecutive) — runtime statistics are unaffected, and whatever did not land is still missing from the store, so the next cycle that references it re-selects it at a narrower width.",
                server.Config.DisplayName, databaseName, failed.ConsecutiveFetchFailures);
        }

        /* Outside the catch on purpose — see the flag's declaration. Runs before this method returns, so
           the caller's writeBatch (which propagates, and would abort the whole sweep) never meets a broken
           borrowed connection that a reopen could have saved. */
        if (storeConnectionSuspect)
        {
            /* Charged to probe: for the same reason as the top-of-try repair — a reconnect is store time,
               and other: is documented as work in neither database. */
            var reopenWatch = Stopwatch.StartNew();
            await RestoreBorrowedStoreConnectionAsync(storeConnection, server, databaseName);
            context.PerItemPlanProbeMs += reopenWatch.ElapsedMilliseconds;
        }
    }

    /// <summary>
    /// One database's statement-text fetch, the sibling of <see cref="FetchAndStorePlansAsync"/> — the
    /// #2150 split (text out of the runtime stream) driven the #2312 way (activity, not a watermark walk):
    /// touch-and-probe <c>query_store_text</c> for the cycle's referenced query_ids, fetch exactly the
    /// missing or hash-stale ones by id, land them. The hash-stale arm is the Query Store RESET detector —
    /// ids renumber on a reset, so a stored hash differing from the live one means the id names a
    /// different statement now, and its text is refetched within one cycle instead of waiting on the
    /// retired daily re-walk.
    ///
    /// <para><b>A failure here is text-only.</b> Runtime statistics are already written by the time this
    /// runs, and whatever did not land is still missing from the store — so a throw leaves the rows in
    /// place with their text unresolved and the next cycle that references them re-selects them. That is
    /// why this is a warning rather than a failure of the collector.</para>
    ///
    /// <para>Simpler than the plan fetch on purpose, in the same two ways the builders differ: no
    /// candidate-cap estimator (DATALENGTH on text is cheap — no decompression to bound) and larger id
    /// chunks. The budget, the carry-over debt, and the uncut-pass target-side-gone cleanup all work
    /// exactly as the plan side documents, and since #6102 so does the per-database time budget
    /// (<see cref="QueryStoreReadBudget.TextFetchBudget"/>).</para>
    /// </summary>
    private async Task FetchAndStoreQueryTextAsync(
        ITargetProvider targetProvider,
        DbConnection targetConnection,
        NpgsqlConnection storeConnection,
        ServerRuntime server,
        string databaseName,
        string collectorName,
        CollectorContext context,
        int itemTimeout,
        IReadOnlyList<(long QueryId, string? QueryHash)> references,
        HashSet<long>? fetchOnlyQueryIds,
        CancellationToken cancellationToken)
    {
        /* Hoisted out of the try (#2776), same reason as the plan side: the catch advances the backoff.
           Collector-scoped since #2902, same as the plan side and for the same measured reason. */
        var carryKey = FetchStateKey(server.ServerId, databaseName, collectorName);

        /* #2816: hoisted for the same reason as the plan side — a probe that throws would otherwise report
           probe:0ms and hand its whole cost to the other: residual, inverting where the time was spent. */
        var probeWatch = new Stopwatch();
        var probeStamped = false;

        /* #2819: set by the catch, acted on AFTER it. The restore needs an await, and an await inside a
           catch makes the compiler lift the handler's body out of the exception region — which would break
           #2816's pin that the probe stamp is reachable from inside a handler, and with it the guarantee
           that a throwing probe reports its own time instead of donating it to other:. Flag here, await
           below: the property #2816 holds and the recovery #2819 needs are not in tension, they just cannot
           share a block. */
        var storeConnectionSuspect = false;

        try
        {
            /* FIRST statement in the try, ahead of the quiet-cycle return below, and that ordering is
               the whole point. The catch further down filters out OperationCanceledException, and OCE is
               the fault most likely here: this method's token is the per-item wall-clock budget, not host
               shutdown, and Npgsql throws OCE when a caller-supplied token fires. So a database that simply
               ran slow mid-probe cancels, propagates past that catch, and never reaches the restore at the
               bottom — leaving the BORROWED connection broken for whoever runs next.

               Putting the repair after the early return would have missed exactly the case that matters.
               "Nothing referenced, nothing owed" is derived from this database's own just-read rows and is
               completely independent of whether it has runtime stats to WRITE, so a busy-but-stable database
               routinely returns early here while still handing the driver a non-empty batch. writeBatch then
               runs on the still-broken connection, outside any per-item try/catch, and propagates — aborting
               the rest of the sweep. That is precisely the whole-cycle abort this borrowing must not buy.

               Classifying on connection STATE rather than exception type is the discipline
               EnumeratedCollectorDriver.ItemBudgetExpired already applies to the budget question: the tokens
               and the connection know, the exception type does not. Free when nothing is wrong — the helper
               returns immediately on an Open connection — and outside the probe measurement on purpose,
               since a reopen is recovery rather than store work. */
            /* Timed into probe:, not left to other:. A reconnect is store-connection time — the very cost
               this PR removed from the steady-state path — and other: is documented as work in NEITHER
               database, so letting a reopen land there would recreate the misattribution #2816 fixed for
               the probe stamp. Zero on the ordinary pass: the helper returns on a state check. */
            probeWatch.Restart();
            if (!await RestoreBorrowedStoreConnectionAsync(storeConnection, server, databaseName))
            {
                /* The store is still down. Returning beats falling through: the probe below would fault on
                   the same Broken connection, log a second "could not reopen" for one underlying outage,
                   and the text sibling would double it again per item. This codebase caps repetitive
                   failure logging deliberately (MaxLoggedProbeFailures), and attempting store work already
                   known to be doomed is not diagnosis. The carryover is untouched, so nothing is forgotten
                   and the next cycle re-selects it.

                   Stamped before returning, like every other exit from this method: the failed reconnect
                   still took real wall clock, the caller's fetch watch still counted it, and leaving it
                   unstamped would hand the one measurable part of an outage to the other: residual. */
                context.PerItemTextProbeMs = probeWatch.ElapsedMilliseconds;
                return;
            }

            var hasCarryover = _textFetchCarryover.TryGetValue(carryKey, out var carriedIds);
            if (references.Count == 0 && !hasCarryover)
            {
                /* Nothing referenced, nothing owed — so any carried failure count is stale (#2776).
                   Probe stamped for the same reason as the plan side: 0ms normally, but a reconnect
                   performed by the repair above is store time and must not fall into other:. */
                context.PerItemTextProbeMs = probeWatch.ElapsedMilliseconds;
                _textFetchFailures.TryRemove(carryKey, out _);
                return;
            }

            /* #2819: same borrowed connection as the plan side, for the same measured reason — this method
               was the SECOND of the two per-item acquisitions, and its zero-id probes measured 893ms, the
               worst floor of either. See the plan side for why borrowing is safe (the driver reads an item
               and then awaits its write, so the caller's connection is idle across this window) and what it
               trades (a store fault here breaks the caller's connection rather than a private one).

               probeWatch covers the probe round trip and, on a pass that needed one, the recovery reconnect
               above — both are store time, and neither belongs in the other: residual. */

            var missing = new SortedSet<long>();
            if (hasCarryover)
            {
                foreach (var id in carriedIds!)
                {
                    missing.Add(id);
                }
            }

            if (references.Count > 0)
            {
                /* #3189/#3216: same cache-before-probe shape as the plan side — see QueryStoreProbeCache
                   and _planProbeCache's call site for the measurement and the correctness argument. */
                var toProbe = _textProbeCache.SelectNeedingProbe(
                    server.ServerId, databaseName, references, context.CollectionTime, s_probeCacheTtl);
                if (toProbe.Count < references.Count)
                {
                    var toProbeIds = new HashSet<long>(toProbe.Count);
                    foreach (var reference in toProbe)
                    {
                        toProbeIds.Add(reference.Id);
                    }

                    foreach (var reference in references)
                    {
                        if (!toProbeIds.Contains(reference.QueryId))
                        {
                            missing.Remove(reference.QueryId);
                        }
                    }
                }

                /* #2823: probe input size — see the plan-side comment. Now the post-cache size. */
                context.PerItemTextProbeIds = toProbe.Count;
                if (toProbe.Count > 0)
                {
                    var verdicts = await QueryStoreFetchProbe.TouchAndProbeTextsAsync(
                        storeConnection, server.ServerId, databaseName, toProbe, context.CollectionTime, itemTimeout, cancellationToken);
                    var hashById = new Dictionary<long, string?>(toProbe.Count);
                    foreach (var reference in toProbe)
                    {
                        hashById[reference.Id] = reference.Hash;
                    }

                    foreach (var verdict in verdicts)
                    {
                        if (!verdict.Resolved || verdict.HashStale)
                        {
                            /* #6211: text twin of the plan side: all references are touched and probed, only the selected queries
                               are added to what is fetched (carried debt is in `missing` already). */
                            if (fetchOnlyQueryIds is null || fetchOnlyQueryIds.Contains(verdict.Id))
                            {
                                missing.Add(verdict.Id);
                            }
                        }
                        else
                        {
                            missing.Remove(verdict.Id);
                            _textProbeCache.Confirm(server.ServerId, databaseName, verdict.Id, hashById[verdict.Id], context.CollectionTime);
                        }
                    }
                }
            }

            context.PerItemTextProbeMs = probeWatch.ElapsedMilliseconds;
            probeStamped = true;

            if (missing.Count == 0)
            {
                /* Probed the store, nothing owed: the same end-well-without-fetching case as the plan side. */
                _textFetchCarryover.TryRemove(carryKey, out _);
                _textFetchFailures.TryRemove(carryKey, out _);
                return;
            }

            var budget = context.TextByteBudgetOverride ?? 12 * 1024 * 1024;

            /* #2776: the text fetch has no candidate cap by design — DATALENGTH on text is cheap, so only
               the byte budget bounds it and the whole missing set is normally attempted. That stays true
               while the fetch is healthy. Once it starts throwing, the unbounded set is the problem: the
               store write is what times out, and re-attempting the identical width guarantees the identical
               timeout. Narrowing by consecutive failures converges on a width the store can commit, and is
               inert (full set) at zero failures. */
            var textFailures = _textFetchFailures.TryGetValue(carryKey, out var carriedFailures)
                ? carriedFailures
                : 0;
            var textWidth = QueryStorePlanXmlState.NarrowForFailures(missing.Count, textFailures);
            if (textWidth != missing.Count)
            {
                _logger?.LogInformation(
                    "query_store text fetch on '{Server}' database [{Database}]: width narrowed {Full} -> {Narrowed} after {Failures} consecutive failure(s) — backing off, not giving up; a completed pass restores full width.",
                    server.Config.DisplayName, databaseName, missing.Count, textWidth, textFailures);
            }

            var attempt = new List<long>(textWidth);
            foreach (var id in missing)
            {
                if (attempt.Count >= textWidth)
                {
                    break;
                }

                attempt.Add(id);
            }

            var attempted = new List<long>(attempt.Count);
            var fetched = new List<FetchedQueryText>();
            var shippedBytes = 0L;
            var brokeOnBudget = false;

            /* #2811: target half only, same contract as the plan side. */
            var targetMs = 0L;

            /* #6102: the text fetch's TIME budget for this database and pass, beside the byte budget above and the
               twin of the plan fetch's (#5813). In the field the text fetch spent 20 to 27 s a pass on 320 to 580
               ids with nothing but the byte budget and the 500 s command timeout bounding it. Each chunk asks for as
               many ids as the time left fits at this database's last measured time per id
               (QueryStoreReadBudget.TextFetchWidth), and runs under the time left as a linked token (shutdown and the
               item budget keep precedence), so a pass slower than predicted is still cut at the budget. Either way the
               ids not received stay in `missing` and go to the carryover, which the next pass fetches first; the
               uncut-only cleanup below stays off (brokeOnBudget), so no id the cut did not reach is dropped. */
            var textFetchClock = Stopwatch.StartNew();
            double? msPerText = _textFetchMsPerText.TryGetValue(carryKey, out var measuredMsPerText) ? measuredMsPerText : null;
            var timeBound = false;
            var cutOnTime = false;
            var lastChunkWidth = 0;
            var offset = 0;

            while (offset < attempt.Count)
            {
                if (shippedBytes >= budget)
                {
                    brokeOnBudget = true;
                    break;
                }

                var timeLeft = QueryStoreTextFetchBudgetSpan - textFetchClock.Elapsed;
                var width = QueryStoreReadBudget.TextFetchWidth(timeLeft, msPerText, TextFetchIdsPerStatement, firstChunk: offset == 0);
                if (width <= 0)
                {
                    /* The time left does not fit one more id: leave the rest to the next pass. */
                    brokeOnBudget = true;
                    timeBound = true;
                    break;
                }

                var chunk = attempt.GetRange(offset, Math.Min(width, attempt.Count - offset)).ToArray();
                offset += chunk.Length;
                lastChunkWidth = chunk.Length;

                var query = QueryStoreCollector.Instance.BuildTextFetchByIdsQuery(
                    databaseName, context, chunk, budget - shippedBytes);

                using var fetchBudget = EnumeratedCollectorDriver.StartItemBudget(timeLeft, cancellationToken);
                var fetchToken = fetchBudget?.Token ?? cancellationToken;
                var chunkWatch = Stopwatch.StartNew();
                try
                {
                    using var command = CreateCollectorCommand(targetProvider, query, targetConnection, itemTimeout);
                    await using var reader = await command.ExecuteReaderAsync(fetchToken);
                    attempted.AddRange(chunk);
                    while (await reader.ReadAsync(fetchToken))
                    {
                        var text = reader.IsDBNull(2) ? null : reader.GetString(2);
                        fetched.Add(new FetchedQueryText(
                            reader.GetInt64(0),
                            text,
                            reader.IsDBNull(1) ? null : reader.GetString(1)));
                        if (text is not null)
                        {
                            shippedBytes += (long)text.Length * 2;
                        }
                    }
                }
                catch (Exception ex) when (ex is not OutOfMemoryException && QueryStoreReadBudget.Expired(fetchBudget, cancellationToken))
                {
                    /* #6102: the text-fetch time budget fired, not shutdown or the item budget (those propagate as
                       before). Keep every text already received; they are written below. */
                    brokeOnBudget = true;
                    timeBound = true;
                    cutOnTime = true;
                }
                finally
                {
                    targetMs += chunkWatch.ElapsedMilliseconds;
                    context.PerItemTextTargetMs = targetMs;
                    context.PerItemTextChunks++;
                    context.PerItemTextIdsAttempted += chunk.Length;
                }

                if (cutOnTime)
                {
                    break;
                }
            }

            /* #6102: the time per id the next pass sizes its chunks from. A pass cut before it received anything
               records twice the cut chunk's share, so the next pass asks for at most half as many: the statement
               materialises every requested text before it ships one, so the same width would be cut empty again. */
            var nextMsPerText = QueryStoreReadBudget.NextMsPerText(msPerText, targetMs, fetched.Count, cutOnTime, lastChunkWidth);
            if (nextMsPerText is double nextMs)
            {
                _textFetchMsPerText[carryKey] = nextMs;
            }

            var cutEmpty = cutOnTime && fetched.Count == 0;

            var returned = new HashSet<long>(fetched.Count);
            if (fetched.Count > 0)
            {
                var writeWatch = Stopwatch.StartNew();
                IReadOnlyList<long> landed;
                try
                {
                    /* #4348: the cycle's statement-filter session, so the writer's counters join the cycle's
                       statement_scrub_* measurements. The writer drops a row the session could not judge. */
                    landed = await QueryStoreTextWriter.WriteAsync(
                        storeConnection, server.ServerId, databaseName, fetched, context.CollectionTime, itemTimeout, context.BeginStatementScrub(),
                        cancellationToken);
                }
                finally
                {
                    context.PerItemTextWriteMs = writeWatch.ElapsedMilliseconds;
                }
                foreach (var id in landed)
                {
                    missing.Remove(id);
                }

                foreach (var text in fetched)
                {
                    returned.Add(text.QueryId);
                }
            }

            /* Same uncut-pass cleanup as the plan side: an id the target no longer serves must not become
               permanent debt. */
            if (!brokeOnBudget && shippedBytes < budget && attempted.Count == attempt.Count)
            {
                foreach (var id in attempted)
                {
                    if (!returned.Contains(id))
                    {
                        missing.Remove(id);
                    }
                }
            }

            if (missing.Count > 0)
            {
                var owed = new long[missing.Count];
                missing.CopyTo(owed);
                _textFetchCarryover[carryKey] = owed;
            }
            else
            {
                _textFetchCarryover.TryRemove(carryKey, out _);
            }

            /* #2776: the pass completed — restore full width. Removing the key rather than zeroing it keeps
               the dictionary to just the databases currently backing off.

               #6102: except a pass the time budget cut before it received any text. It made no progress, and the
               same width would be cut empty again, so it counts as a failure as on the plan side: the backoff
               narrows the next pass's width beside the halved time per id above. Any text received is progress and
               restores full width as before. */
            if (cutEmpty)
            {
                _textFetchFailures.AddOrUpdate(
                    carryKey,
                    1,
                    static (_, current) => current >= QueryStorePlanXmlState.MaxBackoffHalvings
                        ? QueryStorePlanXmlState.MaxBackoffHalvings
                        : current + 1);
            }
            else
            {
                _textFetchFailures.TryRemove(carryKey, out _);
            }

            if (timeBound)
            {
                NoteQueryStoreTextFetchBudget(
                    server, databaseName, cutOnTime, fetched.Count, missing.Count,
                    QueryStoreReadBudget.TextFetchWidth(QueryStoreTextFetchBudgetSpan, nextMsPerText, TextFetchIdsPerStatement, firstChunk: true),
                    nextMsPerText);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #2816: same as the plan side — an unstamped probe would otherwise report probe:0ms and push
               its cost into the other: residual. */
            if (!probeStamped)
            {
                context.PerItemTextProbeMs = probeWatch.ElapsedMilliseconds;
            }

            storeConnectionSuspect = true;

            /* #2776: advance the backoff so the next pass attempts a narrower width. Saturates at the same
               halving count the plan side uses, so the counter stays meaningful rather than unbounded. */
            var failures = _textFetchFailures.AddOrUpdate(
                carryKey,
                1,
                static (_, current) => current >= QueryStorePlanXmlState.MaxBackoffHalvings
                    ? QueryStorePlanXmlState.MaxBackoffHalvings
                    : current + 1);

            _logger?.LogWarning(ex,
                "query_store text fetch failed on '{Server}' database [{Database}] ({Failures} consecutive) — runtime statistics are already written, and whatever did not land is still missing from the store, so the next cycle that references those statements re-selects them at a narrower width.",
                server.Config.DisplayName, databaseName, failures);
        }

        /* Outside the catch on purpose — see the flag's declaration. Runs before this method returns, so
           the caller's writeBatch (which propagates, and would abort the whole sweep) never meets a broken
           borrowed connection that a reopen could have saved. */
        if (storeConnectionSuspect)
        {
            /* Charged to probe: for the same reason as the top-of-try repair — a reconnect is store time,
               and other: is documented as work in neither database. */
            var reopenWatch = Stopwatch.StartNew();
            await RestoreBorrowedStoreConnectionAsync(storeConnection, server, databaseName);
            context.PerItemTextProbeMs += reopenWatch.ElapsedMilliseconds;
        }
    }
}
