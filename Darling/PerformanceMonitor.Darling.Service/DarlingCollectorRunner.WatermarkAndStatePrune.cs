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
    /// Upserts what the definition observed this cycle (<see cref="CollectorContext.PendingState"/>),
    /// after the cycle completed — so a cycle that collected zero rows still records what it saw, which is
    /// the whole point of keeping this state off the payload. Best-effort: a failed write leaves the older
    /// value, and the next cycle re-derives from it or falls back.
    /// </summary>
    public async Task SaveCollectorStateAsync(
        int serverId, string collectorName, IReadOnlyDictionary<string, string> state, CancellationToken cancellationToken)
    {
        if (state.Count == 0)
        {
            return;
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            foreach (var entry in state)
            {
                /* One statement per key: Npgsql's positional parameters cannot span a multi-statement
                   batch (they bind to the FIRST statement and the rest fail silently), and this loop
                   runs over a single declared key today. */
                using var command = new NpgsqlCommand(@"
INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name, state_key)
DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at", connection);
                command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(collectorName);
                command.Parameters.AddWithValue(entry.Key);
                command.Parameters.AddWithValue(entry.Value);
                /* Naive UTC, Kind-Unspecified — the product-wide PG timestamp discipline
                   (PgAlertStateStore.NaiveUtcNow, DarlingObservability, the storedCollectionTime below).
                   updated_at is `timestamp` WITHOUT time zone, and binding a Kind=Utc DateTime does not
                   fail: Npgsql infers `timestamptz` from the Kind, PostgreSQL casts it into the column,
                   and the cast renders it in the SERVER's zone — so the row lands silently offset by the
                   server's UTC offset (measured at exactly 4h on an America/New_York store) while every
                   other timestamp in the store is UTC. Nothing throws and nothing logs; the column simply
                   disagrees with the rest of the store. */
                command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
                await command.ExecuteNonQueryAsync(cancellationToken);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Storing collector state for {Collector} failed; next cycle uses the older value", collectorName);
        }
    }

    /// <summary>
    /// Records a clamp-opened Query Store hole for the #2022 backfill worker, under the WORKER's
    /// collector_state name (not the definition's — query_store still declares no state keys).
    /// Merged wider with any pending hole so a repeat outage cannot overwrite an unserviced one.
    /// Best-effort: a lost record means a lost backfill opportunity, never wrong data — the live
    /// path's own WARNING already disclosed the hole.
    /// </summary>
    private async Task RecordQueryStoreBackfillHoleAsync(
        int serverId, string databaseName, DateTime fromUtc, DateTime toUtc, CancellationToken cancellationToken)
    {
        try
        {
            var key = QueryStoreBackfillState.HoleKeyPrefix + databaseName;
            var existing = await GetCollectorStateAsync(serverId, QueryStoreBackfillState.StateCollectorName, cancellationToken);
            var merged = QueryStoreBackfillState.MergeHole(existing.TryGetValue(key, out var encoded) ? encoded : null, fromUtc, toUtc);
            await SaveCollectorStateAsync(
                serverId, QueryStoreBackfillState.StateCollectorName,
                new Dictionary<string, string>(StringComparer.Ordinal) { [key] = QueryStoreBackfillState.EncodeHole(merged.FromUtc, merged.ToUtc) },
                cancellationToken);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Recording query_store backfill hole for [{Database}] failed; the live WARNING remains the disclosure", databaseName);
        }
    }

    /// <summary>
    /// Deletes ONE collector_state key — the backfill worker's retirement path for a serviced or
    /// expired hole record (#2022). Best-effort like its siblings: a failed delete leaves the row,
    /// and the worker's scan re-derives the same verdict next tick.
    /// </summary>
    public async Task DeleteCollectorStateKeyAsync(
        int serverId, string collectorName, string stateKey, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                "DELETE FROM collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3", connection);
            command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(collectorName);
            command.Parameters.AddWithValue(stateKey);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Deleting collector state {Key} for {Collector} failed; next tick re-derives", stateKey, collectorName);
        }
    }

    /// <summary>
    /// Retires one collector's per-database <c>collector_state</c> rows for databases the server no longer
    /// has (#2188). One statement per (owner, prefix) pair — <c>$1</c> server_id, <c>$2</c> collector_name,
    /// <c>$3</c> the key prefix, which is also what reconstructs each live database's key for the anti-join.
    ///
    /// <para><b>The existence list is <c>database_states</c>, not the collector's enumeration.</b> That is
    /// the whole design. query_store's enumeration is a heavily FILTERED list — ONLINE only, AG primaries
    /// only, the excluded-database filter, the vendor-name screen, <c>HAS_DBACCESS</c>, and a per-database
    /// probe that can fail — so a database missing from one cycle's items is far more often offline,
    /// excluded or unprobeable than dropped, and pruning on that absence would delete live watermarks on
    /// exactly the servers that have such databases. <c>database_states</c> is an unfiltered
    /// <c>SELECT ... FROM sys.databases</c>, so it answers the only question being asked here: does this
    /// name still exist on the instance.</para>
    ///
    /// <para><b>Guarded on the snapshot existing</b> (<c>newest IS NOT NULL</c>): a server that has never
    /// collected database_states, or whose rows have aged out, produces an empty snapshot, and an unguarded
    /// anti-join against nothing deletes EVERY row. The subselect always yields one row (MAX over zero rows
    /// is NULL), so the guard is what turns "no snapshot" into "prune nothing" instead of "prune all".</para>
    ///
    /// <para><b>And guarded on the snapshot being NEWER than the state row</b>
    /// (<c>s.updated_at &lt; snapshot.newest</c>), which is the stronger of the two and the one that makes
    /// this correct rather than merely usually-correct. Existing is not the same as CURRENT: if
    /// database_states stops collecting for a server — a per-server schedule change, a failing collector,
    /// anything — the newest snapshot freezes, and every database created after that instant is missing from
    /// it while being perfectly alive. Presence alone would prune such a database's watermark on EVERY cycle
    /// forever, silently paying a full plan-XML refetch each time: the exact cost #2164 exists to remove,
    /// with a log line confidently calling a live database dropped. A snapshot cannot judge a row written
    /// after it was taken. This holds regardless of the two collectors' relative cadences, so nothing here
    /// depends on database_states being scheduled more often than query_store; both stamps are the SERVICE
    /// clock's naive UTC (<c>collectionTime</c> and <see cref="SaveCollectorStateAsync"/> both read
    /// <c>DateTime.UtcNow</c>), never the monitored server's. A genuinely dropped database is still pruned:
    /// its last state write necessarily precedes any snapshot taken after the drop.</para>
    ///
    /// <para>The two guards overlap — <c>&lt;</c> against a NULL newest is already NULL, so the freshness
    /// test alone would cover the empty snapshot. The explicit NULL check stays anyway: "no snapshot prunes
    /// nothing" is a promise worth reading off the statement instead of deriving from three-valued
    /// logic.</para>
    ///
    /// <para><b>Bounded consequence, either way.</b> Deleting a watermark that should have stayed costs one
    /// full plan-XML refetch for that database and nothing else — the same conservative path an absent or
    /// expired watermark already takes (<see cref="QueryStorePlanXmlState.Resolve"/>), which is why racing
    /// an in-flight cycle is safe: the write-back is an upsert, so a cycle that had already loaded the state
    /// simply restores the row it is still using, and a row deleted for a genuinely dropped database has no
    /// cycle to race.</para>
    ///
    /// <para><b>Not reached on Azure SQL DB</b>, where <c>DatabaseStateCollector.AppliesTo</c> is false and
    /// there is therefore no snapshot to check — the guard makes it a no-op rather than a mass delete. Those
    /// orphans stay (#2191 tracks the Azure arm, including why that path's own database list cannot be used
    /// as the existence check); the accumulation is bounded to one ~100-byte row per database name ever
    /// seen.</para>
    ///
    /// <para>Best-effort like every sibling here: a failed prune leaves the rows and the next cycle retries.
    /// Nothing downstream reads them — an orphan is a row nobody asks about, which is why this is hygiene
    /// rather than a correctness fix.</para>
    /// </summary>
    internal const string PruneOrphanedDatabaseStateKeysSql = @"
DELETE FROM collector_state s
USING (SELECT MAX(collection_time) AS newest FROM database_states WHERE server_id = $1) snapshot
WHERE s.server_id = $1
AND   s.collector_name = $2
AND   starts_with(s.state_key, $3)
AND   snapshot.newest IS NOT NULL
AND   s.updated_at < snapshot.newest
AND   NOT EXISTS
      (
          SELECT 1
          FROM database_states ds
          WHERE ds.server_id = $1
          AND   ds.collection_time = snapshot.newest
          AND   s.state_key = $3 || ds.database_name
      )
RETURNING s.state_key";

    /// <summary>
    /// The Azure SQL DB variant (#2191): prune every per-database state key that is not the ONE database this
    /// registration names. <c>$1</c> server_id, <c>$2</c> collector_name, <c>$3</c> key prefix, <c>$4</c> the
    /// registration's own database.
    ///
    /// <para><b>Why this needs no snapshot, and no freshness guard.</b> The on-prem statement anti-joins
    /// <c>database_states</c> because the question there is "does this name still exist on the instance", and
    /// it needs the two guards because a SNAPSHOT can be empty or stale. Here there is no snapshot: after
    /// #2220 a registration that names a database sweeps only that database, so its one legitimate key is
    /// derivable from the connection string's own catalog — which is current by construction and cannot go
    /// stale, be empty, or be filtered. That is why #2191 looked unfixable when it was filed and is not now:
    /// it asked for "an authoritative unfiltered sys.databases read from master, used only on the success
    /// path", and #2220 removed the need for any master read on this path at all.</para>
    ///
    /// <para><b>What it actually deletes, today.</b> Mostly #2220's residue. Before that fix each Azure
    /// registration swept every sibling database on the logical server and wrote a watermark for each, all
    /// under its own server_id — so these keys are the state half of that contamination. <c>collector_state</c>
    /// carries no retention (it is state, not facts), so unlike the collected rows those orphans would
    /// otherwise persist forever rather than ageing out.</para>
    ///
    /// <para>Bounded consequence, exactly as on the on-prem path: deleting a watermark that should have
    /// stayed costs one full plan-XML refetch for that database and nothing else.</para>
    /// </summary>
    internal const string PruneForeignDatabaseStateKeysSql = @"
DELETE FROM collector_state s
WHERE s.server_id = $1
AND   s.collector_name = $2
AND   starts_with(s.state_key, $3)
AND   s.state_key <> $3 || $4
RETURNING s.state_key";

    /// <summary>
    /// Runs <see cref="PruneOrphanedDatabaseStateKeysSql"/> for every owner/prefix in the SHARED
    /// <see cref="QueryStorePerDatabaseState.PrunableKeys"/> — the same set Lite's DuckDB twin
    /// (<c>RemoteCollectorService.PruneOrphanedQueryStoreDatabaseStateAsync</c>) iterates, so a prefix
    /// cannot end up pruned on one SKU and orphaning on the other. Once per query_store cycle for one
    /// server. Separate statements rather than one combined predicate because Npgsql's positional
    /// parameters cannot span a multi-statement batch, and three narrow deletes down the primary key are
    /// easier to read than one that ORs three prefixes together.
    /// </summary>
    internal async Task<bool> PruneOrphanedQueryStoreDatabaseStateAsync(int serverId, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var pruned = new List<string>();

            foreach (var (owner, prefix) in QueryStorePerDatabaseState.PrunableKeys)
            {
                using var command = new NpgsqlCommand(PruneOrphanedDatabaseStateKeysSql, connection);
                command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(owner);
                command.Parameters.AddWithValue(prefix);

                /* RETURNING rather than a rows-affected count: the only symptom of a WRONG delete here is a
                   silent refetch, so a bare number would leave nothing to diagnose it with. The keys name
                   the databases, which is what makes a mistaken prune visible in the log. */
                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    pruned.Add(reader.GetString(0));
                }
            }

            if (pruned.Count > 0)
            {
                /* Information, like DarlingObservability's orphaned-server sweep: rare, and it names a
                   database lifecycle event the operator may not know the monitor noticed. */
                _logger?.LogInformation(
                    "[server_id {ServerId}] pruned {Count} query_store state row(s) for database(s) no longer on the server: {Keys}",
                    serverId, pruned.Count, string.Join(", ", pruned));
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Pruning orphaned query_store database state failed; next cycle retries");
            return false;
        }
    }

    /// <summary>#4660: the per-cycle entry: runs the prune when <see cref="OrphanStatePrune.IsDue"/> says so, and records
    /// only a success, so a failure is retried on the next cycle.</summary>
    internal async Task PruneOrphanedQueryStoreDatabaseStateIfDueAsync(int serverId, CancellationToken cancellationToken)
    {
        var now = DateTime.UtcNow;
        if (!OrphanStatePrune.IsDue(_lastOrphanStatePruneUtc.TryGetValue(serverId, out var last) ? last : null, now))
        {
            return;
        }

        if (await PruneOrphanedQueryStoreDatabaseStateAsync(serverId, cancellationToken))
        {
            _lastOrphanStatePruneUtc[serverId] = now;
        }
    }

    /// <summary>
    /// The PostgreSQL arm of the per-database state prune (#3153): retire every key whose database is not
    /// in the list THIS SWEEP enumerated. <c>$1</c> server_id, <c>$2</c> collector_name, <c>$3</c> key
    /// prefix, <c>$4</c> the enumerated database names as <c>text[]</c>.
    ///
    /// <para><b>Why not <see cref="PruneOrphanedDatabaseStateKeysSql"/>.</b> That one anti-joins
    /// <c>database_states</c>, which is <c>sys.databases</c> from a SQL Server collector, so it holds no
    /// rows for a PostgreSQL <c>server_id</c> — and it is guarded by <c>snapshot.newest IS NOT NULL</c>, so
    /// on this engine it would delete nothing on every cycle forever while looking exactly like a working
    /// prune. See <see cref="PgPerDatabaseCollectorState"/>.</para>
    ///
    /// <para><b>One array parameter, not one per database.</b> A name-per-parameter IN list would put this
    /// statement on the same parameter budget the cursor splice it is cleaning up already competes for.
    /// The array binds as one <c>text[]</c> whatever the database count.</para>
    ///
    /// <para><b>No pattern matching anywhere in it, which is load-bearing for names.</b>
    /// <c>starts_with</c> is a plain prefix test and <c>= ANY</c> is equality, so a database name is
    /// compared as the literal text it is. A <c>LIKE</c>-shaped prune would read <c>_</c> and <c>%</c> in a
    /// database name as wildcards and keep or delete the wrong row. Verified on PostgreSQL 17 that
    /// <c>substr(state_key, length(prefix) + 1)</c> recovers the name exactly for a multi-byte name
    /// (<c>length</c> and <c>substr</c> are both character-based, so they agree), that the comparison is
    /// case-SENSITIVE as PostgreSQL identifiers are, and that <c>a_b</c> and <c>a%b</c> are matched
    /// literally rather than as patterns.</para>
    ///
    /// <para><b><c>array_length($4, 1) > 0</c> is deliberate redundancy.</b> The caller already refuses to
    /// prune on an empty enumeration — an empty list is how a login that cannot read <c>pg_database</c>
    /// presents, and pruning against it would delete every cursor on the server. Repeating the check here
    /// means a future caller that forgets it gets a no-op instead of a wipe: the failure lands on the side
    /// that costs re-measurement rather than the side that costs the mechanism.</para>
    /// </summary>
    internal const string PrunePgPerDatabaseStateKeysSql = @"
DELETE FROM collector_state s
WHERE s.server_id = $1
AND   s.collector_name = $2
AND   starts_with(s.state_key, $3)
AND   array_length($4, 1) > 0
AND   NOT (substr(s.state_key, length($3) + 1) = ANY($4))
RETURNING s.state_key";

    /// <summary>
    /// Prunes <see cref="PgPerDatabaseCollectorState.PrunableKeys"/> entries owned by this definition
    /// against the database list the cycle actually enumerated (#3153).
    ///
    /// <para>Gated on the REGISTRY rather than on a collector name: an entry is what makes a definition
    /// eligible, so a second per-database PostgreSQL collector needs a registry line and no host edit.
    /// Every other collector's <c>PrunableKeys</c> lookup misses and this returns without opening a
    /// connection, so the sweep pays nothing for it.</para>
    ///
    /// <para>Best-effort, like its query_store sibling: a failed prune leaves the rows for the next cycle,
    /// and the splice cap keeps the statement executable in the meantime.</para>
    /// </summary>
    internal async Task<List<string>> PrunePgPerDatabaseStateAsync(
        int serverId, string collectorName, IReadOnlyList<string> enumeratedDatabases,
        CancellationToken cancellationToken)
    {
        var pruned = new List<string>();

        var owned = PgPerDatabaseCollectorState.PrunableKeys
            .Where(pair => string.Equals(pair.Owner, collectorName, StringComparison.Ordinal))
            .ToArray();

        /* An empty enumeration is a permissions failure, not an empty server: every cursor would look
           orphaned. Refused here as well as in the statement. */
        if (owned.Length == 0 || enumeratedDatabases.Count == 0)
        {
            return pruned;
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var live = enumeratedDatabases.ToArray();

            foreach (var (owner, prefix) in owned)
            {
                using var command = new NpgsqlCommand(PrunePgPerDatabaseStateKeysSql, connection);
                command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(owner);
                command.Parameters.AddWithValue(prefix);
                command.Parameters.AddWithValue(live);

                /* RETURNING, not a rows-affected count, for the reason the query_store prune gives: the
                   only symptom of a WRONG delete is a silent restart of that database's pass, so the keys
                   have to name the databases or there is nothing to diagnose it with. */
                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    pruned.Add(reader.GetString(0));
                }
            }

            if (pruned.Count > 0)
            {
                _logger?.LogInformation(
                    "[server_id {ServerId}] pruned {Count} {Collector} rotation cursor(s) for database(s) no longer enumerated: {Keys}",
                    serverId, pruned.Count, collectorName, string.Join(", ", pruned));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(
                ex, "Pruning orphaned {Collector} per-database state failed; next cycle retries", collectorName);
        }

        return pruned;
    }

    /// <summary>
    /// The Azure SQL DB arm of the #2188 prune (#2191): retire every per-database state key that does not
    /// belong to the one database this registration names.
    ///
    /// <para>Runs the same shared <see cref="QueryStorePerDatabaseState.PrunableKeys"/> set as the on-prem
    /// path, so a prefix cannot be pruned on one target type and left orphaning on the other.</para>
    /// </summary>
    /// <param name="ownDatabase">
    /// The registration's own database — the connection string's initial catalog. Callers must only reach
    /// here when that is non-empty: a registration naming no database (or naming <c>master</c>) is a
    /// registration of the logical SERVER, whose legitimate database set is everything on it, and pruning
    /// against a single name there would delete every live watermark it has.
    /// </param>
    internal async Task PruneForeignQueryStoreDatabaseStateAsync(
        int serverId, string ownDatabase, CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(ownDatabase))
        {
            return;
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var pruned = new List<string>();

            foreach (var (owner, prefix) in QueryStorePerDatabaseState.PrunableKeys)
            {
                using var command = new NpgsqlCommand(PruneForeignDatabaseStateKeysSql, connection);
                command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(owner);
                command.Parameters.AddWithValue(prefix);
                command.Parameters.AddWithValue(ownDatabase);

                using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    pruned.Add(reader.GetString(0));
                }
            }

            if (pruned.Count > 0)
            {
                /* Names the keys rather than counting them, like the on-prem twin: the only symptom of a
                   wrong delete here is a silent refetch, so a bare number would leave nothing to diagnose
                   with. On an Azure server upgraded past #2220 this fires ONCE and clears that fix's state
                   residue, which is worth saying plainly rather than looking like a recurring anomaly. */
                _logger?.LogInformation(
                    "[server_id {ServerId}] pruned {Count} query_store state row(s) belonging to databases other than this registration's [{Database}]: {Keys}",
                    serverId, pruned.Count, ownDatabase, string.Join(", ", pruned));
            }
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Pruning foreign query_store database state failed; next cycle retries");
        }
    }

    /// <summary>
    /// The #2022 backfill write entry: the SAME private COPY writer every live path routes through
    /// (dimension diversion, positional contract, naive-UTC stamp), on its own store connection.
    /// <paramref name="collectionTime"/> is the slice's BACKDATED ceiling — see QueryStoreBackfill's
    /// horizon contract for why that is safe only inside the raw tier's window.
    /// </summary>
    public async Task<int> WriteBackfillBatchAsync<TRow>(
        ICollectorDefinition<TRow> definition, List<TRow> rows, ServerRuntime server,
        DateTime collectionTime, CollectorContext context, CancellationToken cancellationToken)
    {
        try
        {
            await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);
            var written = await WriteBatchAsync(pgConnection, definition, rows, server, collectionTime, context, cancellationToken);

            /* #6254: backfill reads the primary and ships rows with their own old times; the ship guard learns them so a look-back
               read that meets the same row does not write it again. */
            if (rows is List<QueryStoreCollector.Row> backfilled && _queryStoreReplicaStates.TryGetValue(server.ServerId, out var replicaState))
            {
                foreach (var group in backfilled.GroupBy(static r => r.DatabaseName ?? string.Empty, StringComparer.Ordinal))
                {
                    replicaState.Guard.RecordShipped(group.Key, group.ToList());
                }
            }

            return written;
        }
        finally
        {
            /* Backfill only inserts, so it can only raise the true maximum and a stale cache is merely low, never
               too high. The invalidate exists for exactness: without it the cache would lag the store and the
               live path would re-collect rows the backfill already wrote, as duplicates. It runs in a finally
               because the commit may be unknown after a fault. */
            foreach (var db in DistinctQueryStoreDatabases(rows) ?? [])
            {
                _databaseWatermarkCache.Invalidate(server.ServerId, db);
            }
        }
    }

    /// <summary>
    /// Postgres twin of Lite's GetLastCollectedTimeForDatabaseAsync: the newest already-collected
    /// value for ONE database, for definitions with a PerDatabaseWatermarkColumn (Azure SQL DB
    /// per-database XE capture, #1535). Null on first run for that database or on failure — the
    /// caller falls back to the definition's documented window.
    ///
    /// <para><paramref name="collectedSince"/> bounds the read on <c>collection_time</c> — the
    /// PARTITIONING column, so the bound actually prunes chunks (#2344). Null keeps the unbounded
    /// behaviour, which is correct for any reader whose watermark is NOT clamped; pass
    /// <see cref="WatermarkPolicy.ReadFloor"/> only from a caller whose value is, and read that method's
    /// remarks for why the bound provably changes no answer. Unbounded, this is a <c>MAX</c> over a
    /// non-partitioning column with no time predicate — every chunk in retention, per database, per
    /// cycle, at a cost that grows with the store rather than the workload.</para>
    /// </summary>
    public async Task<DateTime?> GetLastCollectedTimeForDatabaseAsync(
        int serverId, string tableName, string columnName, string databaseColumnName, string databaseName,
        CancellationToken cancellationToken, DateTime? collectedSince = null)
        => (await ReadLastCollectedTimeForDatabaseAsync(
            serverId, tableName, columnName, databaseColumnName, databaseName, cancellationToken, collectedSince)).Value;

    /// <summary>
    /// The read behind <see cref="GetLastCollectedTimeForDatabaseAsync"/>, plus whether it succeeded: a null
    /// from a failed read must never be cached as "no rows". The Query Store cache's seed uses its twin,
    /// <see cref="ReadLastCollectedTimeAndWitnessForDatabaseAsync"/>, which also returns the witness.
    /// </summary>
    internal async Task<(DateTime? Value, bool Succeeded)> ReadLastCollectedTimeForDatabaseAsync(
        int serverId, string tableName, string columnName, string databaseColumnName, string databaseName,
        CancellationToken cancellationToken, DateTime? collectedSince = null)
    {
        var failed = false;
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);

            /* #4197: same probe-then-confirm as the server-scoped twins. collectedSince non-null (Azure
               SQL DB's query_store clamp) is unchanged; null now bounds to RecentWatermarkWindow first, then
               (#5515) to WidenedWatermarkWindow, and falls back to the true unbounded read only on a second
               miss, instead of skipping straight to it. */
            var autoWindow = collectedSince is null;
            var bound = collectedSince ?? DateTime.UtcNow - WatermarkPolicy.RecentWatermarkWindow;

            var sql = BuildServerWatermarkForDatabaseSql(tableName, columnName, databaseColumnName, bounded: true);
            using (var command = new NpgsqlCommand(sql, connection))
            {
                /* Parity with the server-scoped twin (#2795): explicit, not Npgsql's inherited 30 s. */
                command.CommandTimeout = CommandTimeoutSeconds;
                command.Parameters.AddWithValue(serverId);
                command.Parameters.AddWithValue(databaseName);
                /* Naive like every other timestamp bound in this store (#1969): a Utc Kind infers
                   timestamptz and Postgres would convert it into the session zone on the way in. */
                command.Parameters.AddWithValue(DateTime.SpecifyKind(bound, DateTimeKind.Unspecified));

                var result = await command.ExecuteScalarAsync(cancellationToken);
                if (result is DateTime dt)
                {
                    return (dt, true);
                }
            }

            if (autoWindow)
            {
                /* #5515: the same second rung as the server-scoped twin. A database whose newest row is days old
                   (a quiet Azure database's last deadlock or blocked-process report) missed the 6 h probe on
                   every cycle and paid the unbounded read each time, per database; the week answers it. */
                using (var widened = new NpgsqlCommand(sql, connection))
                {
                    widened.CommandTimeout = CommandTimeoutSeconds;
                    widened.Parameters.AddWithValue(serverId);
                    widened.Parameters.AddWithValue(databaseName);
                    widened.Parameters.AddWithValue(DateTime.SpecifyKind(
                        DateTime.UtcNow - WatermarkPolicy.WidenedWatermarkWindow, DateTimeKind.Unspecified));
                    var widenedResult = await widened.ExecuteScalarAsync(cancellationToken);
                    if (widenedResult is DateTime widenedDt)
                    {
                        return (widenedDt, true);
                    }
                }

                using var fallback = new NpgsqlCommand(
                    BuildServerWatermarkForDatabaseSql(tableName, columnName, databaseColumnName, bounded: false), connection);
                fallback.CommandTimeout = CommandTimeoutSeconds;
                fallback.Parameters.AddWithValue(serverId);
                fallback.Parameters.AddWithValue(databaseName);
                var fallbackResult = await fallback.ExecuteScalarAsync(cancellationToken);
                if (fallbackResult is DateTime fallbackDt)
                {
                    return (fallbackDt, true);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            failed = true;
            /* Parity with the server-scoped twin (#2795). #2344's bound makes a TIMEOUT unlikely here,
               but every other failure — dropped connection, bad SQL — still returned a null that reads
               as a first run, and silence is the property that let the twin's version of this survive
               for months under a green suite. */
            _logger?.LogWarning(
                "Per-database watermark read failed for server {ServerId} database {Database} on "
                + "{Table}.{Column} — falling back to the collector's default window, which re-collects "
                + "data already stored: {Message}",
                serverId, databaseName, tableName, columnName, ex.Message);
        }
        return (null, !failed);
    }

    /// <summary>True when the seed read is the Query Store one the per-batch side table (#5684) answers.</summary>
    private static bool IsQueryStoreWatermarkShape(string tableName, string columnName, string databaseColumnName) =>
        string.Equals(tableName, "query_store_stats", StringComparison.Ordinal)
        && string.Equals(columnName, "last_execution_time", StringComparison.Ordinal)
        && string.Equals(databaseColumnName, "database_name", StringComparison.Ordinal);

    /// <summary>
    /// The seed read for the per-database Query Store watermark cache (#4749): the bounded read behind
    /// <see cref="ReadLastCollectedTimeForDatabaseAsync"/> (same bound, same failure handling), plus the
    /// witness, the newest <c>collection_time</c> among the rows at the returned value inside the bound. It is
    /// the row's own stamp and never this read's time, which is an upper bound and would keep serving the
    /// value after its row left the floor. A null value has a null witness, and a failed read reports
    /// <c>Succeeded</c> false so a null from it is never cached as "no rows".
    /// </summary>
    internal async Task<(DateTime? Value, DateTime? Witness, bool Succeeded)> ReadLastCollectedTimeAndWitnessForDatabaseAsync(
        int serverId, string tableName, string columnName, string databaseColumnName, string databaseName,
        DateTime collectedSince, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var rawSql = BuildServerWatermarkWithWitnessForDatabaseSql(tableName, columnName, databaseColumnName);
            if (IsQueryStoreWatermarkShape(tableName, columnName, databaseColumnName))
            {
                /* #5684: the stitched read (side table + coverage + verification), never above the raw read. */
                var (stitchedValue, stitchedWitness) = await _queryStoreWatermarkReader.ReadAsync(
                    connection, serverId, databaseName, collectedSince, rawSql, CommandTimeoutSeconds, cancellationToken);
                return (stitchedValue, stitchedWitness, true);
            }

            using var command = new NpgsqlCommand(rawSql, connection);
            command.CommandTimeout = CommandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(databaseName);
            /* Naive like every other timestamp bound in this store (#1969). */
            command.Parameters.AddWithValue(DateTime.SpecifyKind(collectedSince, DateTimeKind.Unspecified));

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken) && reader.GetValue(0) is DateTime value)
            {
                return (value, reader.GetValue(1) as DateTime?, true);
            }

            return (null, null, true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogWarning(
                "Per-database watermark read failed for server {ServerId} database {Database} on "
                + "{Table}.{Column} — falling back to the collector's default window, which re-collects "
                + "data already stored: {Message}",
                serverId, databaseName, tableName, columnName, ex.Message);
            return (null, null, false);
        }
    }

    /// <summary>
    /// Gets the most recent value of a monotonic bigint identity column from Postgres for incremental
    /// collection — the numeric twin of <see cref="GetLastCollectedTimeAsync"/> (job_history dedups on
    /// <c>instance_id</c>, sysjobhistory's IDENTITY bigint). Returns null on first run or if the query
    /// fails (caller uses its documented first-run/fallback path).
    ///
    /// <para>#4197: this was the one of the three server-scoped watermark reads with no bound at all —
    /// job_history is its only caller, and every cycle scanned every chunk in retention to find one bigint.
    /// Same probe-then-confirm as the timestamp twins now: bound on <c>collection_time</c> (the
    /// partitioning column — <c>instance_id</c> does not partition the table, so it cannot prune a chunk)
    /// to <see cref="WatermarkPolicy.RecentWatermarkWindow"/> first, and fall back to the true unbounded
    /// <c>MAX</c> only when the probe finds no row.</para>
    /// </summary>
    public async Task<long?> GetLastCollectedInstanceIdAsync(
        int serverId, string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            var bound = DateTime.UtcNow - WatermarkPolicy.RecentWatermarkWindow;
            using (var command = new NpgsqlCommand(
                BuildServerWatermarkInstanceIdSql(tableName, columnName, bounded: true), connection))
            {
                /* Parity with both timestamp twins (#2795): explicit, not Npgsql's inherited 30 s. */
                command.CommandTimeout = CommandTimeoutSeconds;
                command.Parameters.AddWithValue(serverId);
                /* Naive like every other timestamp bound in this store (#1969). */
                command.Parameters.AddWithValue(DateTime.SpecifyKind(bound, DateTimeKind.Unspecified));
                var result = await command.ExecuteScalarAsync(cancellationToken);
                if (result is not null && result != DBNull.Value)
                {
                    return Convert.ToInt64(result);
                }
            }

            /* #5515: the week between the recent window and the whole slice, as in GetLastCollectedTimeAsync. The
               same bounded statement with an older bound. */
            using (var widened = new NpgsqlCommand(
                BuildServerWatermarkInstanceIdSql(tableName, columnName, bounded: true), connection))
            {
                widened.CommandTimeout = CommandTimeoutSeconds;
                widened.Parameters.AddWithValue(serverId);
                widened.Parameters.AddWithValue(DateTime.SpecifyKind(
                    DateTime.UtcNow - WatermarkPolicy.WidenedWatermarkWindow, DateTimeKind.Unspecified));
                var widenedResult = await widened.ExecuteScalarAsync(cancellationToken);
                if (widenedResult is not null && widenedResult != DBNull.Value)
                {
                    return Convert.ToInt64(widenedResult);
                }
            }

            using (var fallback = new NpgsqlCommand(
                BuildServerWatermarkInstanceIdSql(tableName, columnName, bounded: false), connection))
            {
                fallback.CommandTimeout = CommandTimeoutSeconds;
                fallback.Parameters.AddWithValue(serverId);
                var fallbackResult = await fallback.ExecuteScalarAsync(cancellationToken);
                if (fallbackResult is not null && fallbackResult != DBNull.Value)
                {
                    return Convert.ToInt64(fallbackResult);
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Parity with both timestamp twins (#2795). job_history is small enough that this is
               unlikely to time out, but a swallowed failure here sets HasCollectedBefore down the
               first-run path with no trace anywhere — the same silence, one table over. */
            _logger?.LogWarning(
                "Numeric watermark read failed for server {ServerId} on {Table}.{Column} — falling back "
                + "to the collector's first-run path: {Message}",
                serverId, tableName, columnName, ex.Message);
        }
        return null;
    }

    /// <summary>
    /// Whether a prior SUCCESS row exists in collection_log for this collector+server — the "has collected
    /// before" signal (<see cref="CollectorContext.HasCollectedBefore"/>), consulted only when the watermark
    /// is null. Returns false on any failure, which errs toward the all-history first run (correct for a
    /// genuinely fresh store).
    /// </summary>
    public async Task<bool> HasPriorCollectorSuccessAsync(int serverId, string collectorName, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(
                "SELECT EXISTS(SELECT 1 FROM collection_log WHERE server_id = $1 AND collector_name = $2 AND status = 'SUCCESS')", connection);
            command.CommandTimeout = ServiceCommandDeadlines.CollectionSweepSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(collectorName);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            return result is bool b && b;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Fail toward first-run (all-history) — matches a fresh store with no log yet. A cancelled
               request must propagate rather than read as an ordinary read failure, matching Lite's twin. */
            return false;
        }
    }

    /// <summary>
    /// Drops any cached master-inaccessible verdict for a server that has just reconnected.
    ///
    /// Azure SQL DB reports "this login may not read master" and "you cannot reach this server right
    /// now" with overlapping error numbers, so a verdict formed while a server was failing is not
    /// trustworthy. The moment it answers again is the moment to discard that verdict and re-probe.
    /// Without this, a transient outage permanently misfiles a login that CAN read master, and
    /// database-scoped collection stays degraded until the service restarts (#1506).
    /// </summary>
    public void OnServerReconnected(int serverId)
    {
        if (_azureMasterInaccessibleSince.TryRemove(serverId, out _))
        {
            _logger?.LogInformation("[server_id {ServerId}] reconnected — re-probing master for database-scoped collectors.", serverId);
        }

        /* #4197 part b: drop every cached watermark for this server on reconnect, unconditionally —
           unlike the master-access verdict above this has no "was anything cached" branch worth logging,
           because a reconnect after a connection-level failure (DarlingWorker's own catch-all sets
           server.Runtime = null and schedules a reconnect) is exactly the situation where the store's
           real watermark may have moved by means this cache never saw (a re-add under the same server_id
           reaches this same path — see the remarks on ServerWatermarkCache.InvalidateServer). */
        _watermarkCache.InvalidateServer(serverId);
        _databaseWatermarkCache.InvalidateServer(serverId);

        /* #4961: the legacy session's record is read again at the next full pass, and the line about an older install's
           session is logged again, once per connect. */
        _legacyLongQuery?.OnServerReconnected(serverId);
    }
}
