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
    public async Task<CollectorRunResult> RunAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerRuntime server,
        CancellationToken cancellationToken)
    {
        try
        {
            return await RunWithSplitCharacterRetryAsync(definition, server, cancellationToken);
        }
        catch
        {
            /* #4197 part b: ANY fault in the run — collection, dedup, write, or cancel — drops this
               (server, collector)'s cached watermark, so the next call re-seeds from the store rather than
               risk advancing past rows that never committed (a watermark ahead of committed data SKIPS
               events, the one failure direction this cache must never take; falling behind only re-collects
               duplicates, the safe direction). Unconditional — dropping an entry that was never cached is a
               no-op on ServerWatermarkCache.Invalidate, so no eligibility check is needed here. */
            _watermarkCache.Invalidate(server.ServerId, definition.Name);

            /* Rows for earlier items may have committed and the faulting item's may or may not have, so
               no cached per-database Query Store watermark can be trusted. */
            if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
            {
                _databaseWatermarkCache.InvalidateServer(server.ServerId);
            }

            throw;
        }
    }

    /// <summary>
    /// #4197 part b: the watermark cache hit/miss/seed block, extracted out of <see cref="RunCoreAsync"/>
    /// so a live-Postgres-only test can exercise the cache-hit/miss/seed/read-count behaviour without a
    /// live SQL Server target (RunCoreAsync collects from one before it ever reaches this point). Pure
    /// move — the SQL strings and the eligibility/seed/read logic are byte-identical to what RunCoreAsync
    /// inlined before this refactor.
    ///
    /// <para><b>The #2797 gate is called IN HERE</b>, not passed in as a precomputed bool: this is the body
    /// that actually reaches <see cref="GetLastCollectedTimeAsync"/>/<see cref="GetLastCollectedTimeWithFrameAsync"/>,
    /// so <see cref="ServerWatermarkDispatchGateTests"/>' invariant — every body that performs the
    /// server-scoped read also calls the gate — holds with a single call site rather than a caller/callee
    /// split that the IL walk would otherwise have to bridge by name. <paramref name="dispatchProbe"/> is the
    /// probe context <see cref="RunCoreAsync"/> already builds for the SAME question; the discarded flag
    /// comes back in the tuple because <c>RunCoreAsync</c> still needs it for <c>hasCollectedBefore</c>.</para>
    /// </summary>
    internal async Task<(DateTime? Watermark, bool WatermarkFromUtcColumn, long? NumericWatermark, long ServerWatermarkMs, bool WatermarkCacheEligible, bool ServerWatermarkDiscarded)> ResolveServerWatermarkAsync<TRow>(
        ServerRuntime server,
        ICollectorDefinition<TRow> definition,
        CollectorContext dispatchProbe,
        DateTime? serverReadFloor,
        CancellationToken cancellationToken)
    {
        var serverWatermarkDiscarded = ServerWatermarkIsDiscarded(definition, dispatchProbe);

        var watermarkCacheEligible = definition.WatermarkColumn is not null
            && !serverWatermarkDiscarded
            && definition.WatermarkValueAccessor is not null;
        var cachedWatermark = watermarkCacheEligible
            ? _watermarkCache.TryGet(server.ServerId, definition.Name)
            : null;

        var serverWatermarkWatch = Stopwatch.StartNew();
        long serverWatermarkMs;
        DateTime? watermark;
        var watermarkFromUtcColumn = false;
        if (cachedWatermark is { } hit)
        {
            /* Cache hit: use the cached value and issue NO store read for either watermark. */
            watermark = hit.Value;
            watermarkFromUtcColumn = hit.FromUtcColumn;
            serverWatermarkMs = 0;
        }
        else
        {
            try
            {
                if (definition.WatermarkColumn is null || serverWatermarkDiscarded)
                {
                    watermark = null;
                }
                else if (definition.UtcWatermarkColumn is null)
                {
                    watermark = await GetLastCollectedTimeAsync(server.ServerId, definition.TargetTable, definition.WatermarkColumn, cancellationToken, serverReadFloor);
                }
                else
                {
                    /* #3778: a definition with a UTC twin beside its watermark column (cpu_utilization's
                       sample_time_utc beside the server-LOCAL sample_time) gets the pair read in one round trip
                       and the FRAME of what came back, so its dedup compares like with like: the twin where the
                       store has one, the local stamp until the first post-upgrade run has stored one. Its own
                       method rather than a parameter on the read above, so every other definition's watermark
                       SQL is the byte-identical string it was (TimeHonestyRungTests pins both strings). */
                    (watermark, watermarkFromUtcColumn) = await GetLastCollectedTimeWithFrameAsync(
                        server.ServerId, definition.TargetTable, definition.WatermarkColumn, definition.UtcWatermarkColumn,
                        cancellationToken, serverReadFloor);
                }
            }
            finally
            {
                serverWatermarkMs = serverWatermarkWatch.ElapsedMilliseconds;
            }
        }

        /* Numeric (bigint) watermark = the newest already-collected value of the definition's monotonic
           identity column (job_history's instance_id), read from Postgres — the bigint twin of the timestamp
           watermark above. Null for every collector that declares no numeric watermark (the common case),
           so no extra query runs for them. Skipped on a cache hit for the same reason the timestamp read
           above is. */
        long? numericWatermark;
        if (cachedWatermark is { } numericHit)
        {
            numericWatermark = numericHit.NumericValue;
        }
        else
        {
            numericWatermark = definition.NumericWatermarkColumn is null
                ? null
                : await GetLastCollectedInstanceIdAsync(server.ServerId, definition.TargetTable, definition.NumericWatermarkColumn, cancellationToken);

            /* Seed the cache from this miss so every call after this one for this (server, collector)
               skips both store reads. Only when the definition is eligible (a live accessor and not on
               a fan-out path) — an ineligible definition's watermark is never written to the cache, so a
               later collector-name reuse under a different accessor policy can't read a stale entry. */
            if (watermarkCacheEligible)
            {
                _watermarkCache.Seed(server.ServerId, definition.Name, watermark, watermarkFromUtcColumn, numericWatermark);
            }
        }

        return (watermark, watermarkFromUtcColumn, numericWatermark, serverWatermarkMs, watermarkCacheEligible, serverWatermarkDiscarded);
    }

    /// <summary>
    /// #4487: the post-write regressed-vs-honest branch, extracted out of the plain (non-fan-out) write
    /// path in <see cref="RunCoreAsync"/> for the same live-test reason as <see cref="ResolveServerWatermarkAsync"/>
    /// and <see cref="AdvanceServerWatermark"/> above. Pure move: a batch that carries an identity
    /// regression (job_history's guarded filter collapsed to its bounded-window arm because the target's
    /// own MAX(instance_id) fell below the store's watermark — an identity reseed, an msdb restore, or a
    /// failover to a replica with a lower identity; the rows alone can't say which) must not be MAXed
    /// against the stale cached value the way an ordinary batch is: Advance keeps the GREATER of the two,
    /// which is still the OLD epoch's higher number, and every cycle after would keep taking the regressed
    /// arm even though the store itself has already self-healed. Invalidate instead, so the next run
    /// re-seeds from the store — which by then holds this run's new-epoch rows and returns the new,
    /// honest max.
    /// </summary>
    internal void CommitServerWatermark<TRow>(
        ServerRuntime server,
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        bool watermarkFromUtcColumn,
        IReadOnlyList<CollectorMeasurement> measurements)
    {
        var regressed = measurements.Any(m => m.Label == JobHistoryCollector.IdentityRegressionsMeasurement && m.Value > 0);
        if (regressed)
        {
            _watermarkCache.Invalidate(server.ServerId, definition.Name);
        }
        else
        {
            AdvanceServerWatermark(server, definition, rows, watermarkFromUtcColumn);
        }
    }

    /// <summary>
    /// #4197 part b: the post-write cache-advance block, extracted out of the plain (non-fan-out) write
    /// path in <see cref="RunCoreAsync"/> for the same live-test reason as <see cref="ResolveServerWatermarkAsync"/>
    /// above. Pure move.
    /// </summary>
    internal void AdvanceServerWatermark<TRow>(
        ServerRuntime server,
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        bool watermarkFromUtcColumn)
    {
        if (definition.WatermarkValueAccessor is not { } accessor)
        {
            return;
        }

        DateTime? batchMax = null;
        foreach (var row in rows)
        {
            var value = accessor(row);
            if (value is { } v && (batchMax is null || v > batchMax.Value))
            {
                batchMax = v;
            }
        }

        /* #4197: PostgreSQL's `timestamp` truncates to microsecond resolution (10 .NET ticks) on write —
           the same resolution CollectionTimeClock stores. A row's in-memory value can carry finer ticks
           than what the store actually persisted, so the cache must floor to what a fresh read would
           return, or the equivalence pin (cached vs. a fresh GetLastCollectedTimeAsync) drifts by those
           sub-microsecond ticks. */
        if (batchMax is { } batchMaxValue)
        {
            batchMax = new DateTime(batchMaxValue.Ticks - (batchMaxValue.Ticks % TimeSpan.TicksPerMicrosecond), batchMaxValue.Kind);
        }

        long? batchMaxNumeric = null;
        if (definition.NumericWatermarkValueAccessor is { } numericAccessor)
        {
            foreach (var row in rows)
            {
                var value = numericAccessor(row);
                if (value is { } v && (batchMaxNumeric is null || v > batchMaxNumeric.Value))
                {
                    batchMaxNumeric = v;
                }
            }
        }

        _watermarkCache.Advance(server.ServerId, definition.Name, batchMax, watermarkFromUtcColumn, batchMaxNumeric);
    }

    /// <summary>
    /// The per-database Query Store watermark: the cache when it can prove the bounded store read's answer,
    /// otherwise that read (reseeding the cache when the read succeeded). The read also returns the witness
    /// row's <c>collection_time</c> (#4749), so a database whose newest row is recent but which has had no
    /// batch since the seed keeps hitting the cache until that row leaves the floor.
    /// </summary>
    internal async Task<DateTime?> ResolveQueryStoreDatabaseWatermarkAsync(
        ServerRuntime server, string table, string column, string dbColumn, string database,
        DateTime readFloor, DateTime collectionTime, CancellationToken ct)
    {
        if (_databaseWatermarkCache.TryGet(server.ServerId, database, readFloor, collectionTime, out var cached))
        {
            return cached;
        }

        var token = _databaseWatermarkCache.TokenFor(server.ServerId, database);
        var (value, witness, ok) = await ReadLastCollectedTimeAndWitnessForDatabaseAsync(
            server.ServerId, table, column, dbColumn, database, readFloor, ct);
        if (ok)
        {
            _databaseWatermarkCache.Seed(server.ServerId, database, value, readFloor, collectionTime, token, witness);
        }

        return value;
    }

    /// <summary>What a Query Store item's batch contributes to the cache, computed at read time.</summary>
    internal static StagedDatabaseWatermark StageQueryStoreDatabaseWatermark<TRow>(
        List<TRow> batch, string database, DateTime collectionTime)
    {
        if (batch is not List<QueryStoreCollector.Row> rows)
        {
            return new StagedDatabaseWatermark(null, true, collectionTime);
        }

        DateTime? max = null;
        var foreign = false;
        foreach (var r in rows)
        {
            if (!string.Equals(r.DatabaseName, database, StringComparison.Ordinal))
            {
                foreign = true;
                continue;
            }

            if (r.LastExecutionTime is DateTime v && (max is null || v > max))
            {
                max = v;
            }
        }

        return new StagedDatabaseWatermark(max, foreign, collectionTime);
    }

    /// <summary>
    /// A failed item: drop its staged contribution, and its cache entry only if a batch was staged (#5684, the
    /// same rule as the Azure per-database arm's #5514 gate). The staging happens at the end of the item's
    /// read, and the driver's writeBatch runs only after the read returned, outside the try that routes to
    /// onItemError. So a fault that reaches here with nothing staged came before any write of this database's
    /// <c>query_store_stats</c> rows (the plan and text fetches write other tables), the stored maximum did not
    /// move, and the cached value is still exact. A database that fails every cycle must not turn back into a
    /// store read every cycle. With a staged batch the write may or may not have committed, so the key goes.
    /// </summary>
    internal void DiscardQueryStoreDatabaseWatermark(
        ServerRuntime server, string database, Dictionary<string, StagedDatabaseWatermark> staged)
    {
        if (staged.Remove(database))
        {
            _databaseWatermarkCache.Invalidate(server.ServerId, database);
        }
    }

    /// <summary>Lands a staged contribution. Called only after the item's COPY transaction committed.</summary>
    internal void CommitQueryStoreDatabaseWatermark(ServerRuntime server, string database, StagedDatabaseWatermark staged)
    {
        if (staged.Foreign)
        {
            _databaseWatermarkCache.Invalidate(server.ServerId, database);
            return;
        }

        _databaseWatermarkCache.Advance(server.ServerId, database, staged.BatchMax, staged.CollectionTime);
    }

    /// <summary>
    /// #4735 item 1: a text read of the log tail that starts inside a multi-byte character is refused by PostgreSQL
    /// (22021) before this process sees a byte, and it says the same about a real bad byte. The read is repeated in
    /// the same cycle with its start moved forward by 1, then 2, then 3 bytes, which reaches a character boundary
    /// when the start was the cause. The attempts write no ERROR row: only the last refusal leaves this method,
    /// and the general handler records that one as it always did. Recognised by SQLSTATE, on the text route of
    /// the three log-tail collectors, and never for a proven write to the store.
    ///
    /// <para><b>Once per cycle while an invalid byte stays in the window.</b> The retries cure a start inside a
    /// character. A byte that is invalid in the database encoding, anywhere in the 4 MB window, is refused at all
    /// four starts, on every cycle, so a cycle that used every start and was refused at each is remembered
    /// (<c>_pgLogRefusedAtEveryShift</c>). Until a read succeeds, that server's collector makes one read at shift 0 and
    /// no retries, the cost it had before the retries existed. A read that starts inside a character still gets
    /// its retries the first time it happens, because nothing is remembered until every start has been refused.</para>
    /// </summary>
    private Task<CollectorRunResult> RunWithSplitCharacterRetryAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerRuntime server,
        CancellationToken cancellationToken)
        => RunWithSplitCharacterRetryAsync(
            definition.Name,
            server,
            (shift, token) => RunCoreAsync(definition, server, shift, token),
            cancellationToken);

    /// <summary>
    /// The retry loop itself, over a read that takes its start shift, so a test can drive it with a read that
    /// counts its calls instead of one that needs a target (#4735).
    /// </summary>
    internal async Task<CollectorRunResult> RunWithSplitCharacterRetryAsync(
        string collectorName,
        ServerRuntime server,
        Func<int, CancellationToken, Task<CollectorRunResult>> runCore,
        CancellationToken cancellationToken)
    {
        var readsLogTail = ReadsPgServerLogTail(collectorName);
        var refusedKey = (ReadBinaryFileCacheKey(server), collectorName);

        /* Read once, before the first attempt: this cycle's own refusals set the memory for the NEXT cycle. */
        var refusedAtEveryShiftLastCycle = readsLogTail && _pgLogRefusedAtEveryShift.ContainsKey(refusedKey);

        for (var shift = 0; ; shift++)
        {
            try
            {
                var result = await runCore(shift, cancellationToken);

                if (readsLogTail)
                {
                    _pgLogRefusedAtEveryShift.TryRemove(refusedKey, out _);
                }

                return result;
            }
            catch (PostgresException pg) when (readsLogTail
                && !refusedAtEveryShiftLastCycle
                && !CollectorFaultCopyPhase.IsProvenStoreWrite(pg)
                && PgServerLogTail.ShouldRetryFromLaterStart(pg.SqlState, shift, PgReadBinaryFileGranted(server)))
            {
                _logger?.LogDebug(
                    "{Collector} on '{Server}': PostgreSQL refused the log slice as not valid in the database encoding; "
                    + "retrying with the read start moved forward {Shift} byte(s) (#4735)",
                    collectorName, server.Config.DisplayName, shift + 1);
            }
            catch (PostgresException pg) when (readsLogTail
                && !CollectorFaultCopyPhase.IsProvenStoreWrite(pg)
                && PgServerLogTail.IsEncodingRefusal(pg.SqlState, PgReadBinaryFileGranted(server)))
            {
                /* Refused and not retried above: either every start was just refused, or the last cycle's memory
                   held this cycle to its one read. An invalid byte is in the window, and the refusal goes on to the
                   general handler as it always did. */
                if (_pgLogRefusedAtEveryShift.TryAdd(refusedKey, 0))
                {
                    _logger?.LogDebug(
                        "{Collector} on '{Server}': PostgreSQL refused the log slice at every read start, so a byte that is "
                        + "not valid in the database encoding is in the window; later cycles make one read until a read "
                        + "succeeds (#4735)",
                        collectorName, server.Config.DisplayName);
                }

                throw;
            }
        }
    }

    /// <summary>Whether the cached verdict says the binary route (pg_read_binary_file) is granted, which reads bytea and carries no encoding check.</summary>
    private static bool PgReadBinaryFileGranted(ServerRuntime server) =>
        PgReadBinaryFileCapability.TryGetCachedVerdict(ReadBinaryFileCacheKey(server), out var granted) && granted;

    private async Task<CollectorRunResult> RunCoreAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerRuntime server,
        int pgLogReadShiftBytes,
        CancellationToken cancellationToken)
    {
        /* #3936: nudged forward (by, in practice, a handful of ticks) rather than a bare DateTime.UtcNow
           when it would collide with this SAME (server, collector) pair's last stamp — see
           CollectionTimeClock's own remarks for the run-overlap and clock-resolution cases that used to
           store two different snapshots under one collection_time, making "the newest reading" ambiguous
           for every reader downstream. */
        var collectionTime = CollectionTimeClock.NextStrictlyAfter(server.ServerId, definition.Name, DateTime.UtcNow);

        /* Some collectors don't exist on some targets (e.g. ring buffers on Azure SQL DB) —
           skip the cycle entirely, matching Lite. CollectorCatalog.AppliesTo composes the
           engine-dialect check over the definition's own gate, so a T-SQL definition can never be
           dispatched at a non-SQL-Server target. */
        if (!CollectorCatalog.AppliesTo(definition, server.Target))
        {
            return new CollectorRunResult(0, 0, 0, CollectorContext.NoMeasurements);
        }

        /* Watermark = the newest already-collected value of the definition's time column,
           read from Postgres (Lite reads DuckDB here).

           #2344's read floor, applied to the SERVER-scoped read the way it already is to the
           per-database one below. Name-guarded on the same collector and for the same reason
           WatermarkPolicy's remarks give: the bound is only sound where the caller CLAMPS, and a
           ring-buffer source whose legitimate catch-up spans days must keep reading its whole history.
           The clamp and the bound travel together. query_store declares both watermark columns, so
           before this guard it paid the unbounded server-scoped cost on top of the bounded
           per-database reads — the 2,092-cancellations-a-day the method's remarks record. */
        var serverReadFloor = string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
            ? WatermarkPolicy.ReadFloor(collectionTime)
            : null;

        var excludedDatabases = server.Config.ExcludedDatabases?.ToArray() ?? Array.Empty<string>();

        /* #3477: this collector's database scope on this server, resolved ONCE at dispatch (per-server
           row > fleet row > unscoped, the schedule table's own layering) so the enumeration, the
           per-database loop and the dispatch probe all see the same list within one run. Empty =
           unscoped = every database the server enumerates — the shipped state of every install. */
        var databaseScope = _databaseScope(definition.Name, server.ServerId);

        /* #2797: on the two FAN-OUT paths the answer to this read is thrown away — both of them overwrite
           context.Watermark with a per-database value before any query is built — so skip the round trip
           there, and only there. ServerWatermarkIsDiscarded holds the predicate and the argument for why it
           is a DISPATCH-PATH question rather than a watermark-column one.

           Note what this does to the read floor just above: query_store is the only collector that guard
           names, and the gate is true for query_store on EVERY target (it runs per database on Azure and
           enumerates everywhere else), so the floor it computes is now unused. Kept rather than deleted
           because it is the BOUND on this read, and removing it would make #2796's correctness depend on
           #2797's gate continuing to fire — two unrelated conditions that must not become one.

           The probe context exists because the real one cannot be built yet: hasCollectedBefore below is
           computed FROM this read, and CollectorContext.HasCollectedBefore/NumericWatermark/State are all
           init-only, so constructing the cycle's context ahead of the read would mean widening three
           deliberately-immutable members on a type both SKUs and every definition share. It carries the
           real Target and the real ExcludedDatabases — everything the five BuildEnumerationQuery
           implementations actually read — and nothing this read produces. */
        var dispatchProbe = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = collectionTime,
            Deltas = _deltas,
            Target = server.Target,
            ExcludedDatabases = excludedDatabases,
            /* #3477: BuildEnumerationQuery now reads the scope too, so the probe carries it for the
               same reason it carries the exclusions — the contract is "everything the enumeration
               builders actually read", not "everything that changes the probe's answer". */
            DatabaseScope = databaseScope,
        };
        /* #2851: timed because this is a STORE round trip that the server-scoped path's sql: stopwatch does
           not cover — it runs before that stopwatch starts. #2796 measured a sibling store read at 50s cold
           on a bounded-only-by-luck predicate, so "the watermark read is free" is an assumption worth
           holding a number against rather than believing. finally, not a trailing assignment (#2816): a
           throwing read must still report how long it ran. Zero when the definition declares no watermark
           column, or when #2797's gate skipped the read, which is honest either way — no read happened. */
        /* #4197 part b: the cache hit/miss/seed block, extracted verbatim into its own method (step 1 of
           the measure lane's brief) so a live-Postgres-only test can call it directly — RunCoreAsync itself
           needs a live SQL Server target to reach this point, which the live-PG rig this lane runs on
           cannot provide. Behaviour and SQL strings are unchanged; this is a pure move. The #2797 gate call
           now lives INSIDE the seam (ServerWatermarkDispatchGateTests' invariant needs the gate in the same
           body as the read it guards), so RunCoreAsync passes the probe context and gets the discarded flag
           back rather than computing it here and passing a bool in. */
        var (watermark, watermarkFromUtcColumn, numericWatermark, serverWatermarkMs, watermarkCacheEligible, serverWatermarkDiscarded) =
            await ResolveServerWatermarkAsync(server, definition, dispatchProbe, serverReadFloor, cancellationToken);

        /* Only when the watermark came back null: tell a TRUE first run from a store merely emptied by
           retention, so default_trace_events uses a bounded window instead of re-scanning all .trc history
           (CollectorContext.HasCollectedBefore). Skipped in the common (non-null watermark) path.

           #2797 gates this on the SAME flag, and it is not incidental: this branch keys off `watermark is
           null`, so skipping the read above would otherwise READ AS A FIRST RUN and fire this store query
           on precisely the cycles the gate exists to make cheaper — trading one round trip for another and
           netting nothing. The two consumers of HasCollectedBefore (default_trace_events, job_history)
           declare no PerDatabaseWatermarkColumn, so the flag is false for both on every target and neither
           loses the signal. */
        bool hasCollectedBefore = definition.WatermarkColumn is not null
            && !serverWatermarkDiscarded
            && watermark is null
            && await HasPriorCollectorSuccessAsync(server.ServerId, definition.Name, cancellationToken);

        /* Per-server state the definition declared keys for — the watermark's sibling for facts no MAX()
           over the collected rows can produce (default_trace_events' last-seen trace FILE, #1962). No
           declared keys (every other collector) means no query runs. Mirrors Lite. */
        var collectorState = definition.StateKeys.Count == 0
            ? null
            : await GetCollectorStateAsync(server.ServerId, definition.Name, cancellationToken);

        /* #2188: retire the per-database state rows of databases that no longer exist, BEFORE the load
           below, so this cycle also works from a cleaned dictionary rather than one carrying names the
           server dropped. Runs for query_store regardless of plan capture, because the backfill worker's
           per-database keys orphan the same way and are pruned in the same pass.

           Gated on the SAME AppliesTo that decides whether database_states is collected at all, rather than
           leaning on the statement's own empty-snapshot guard to no-op: on Azure SQL DB there is no snapshot
           by design, so this would otherwise run three guaranteed-no-op deletes on every cycle forever and
           #2191's boundary would be emergent rather than stated. */
        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
            && DatabaseStateCollector.Instance.AppliesTo(server.Target))
        {
            await PruneOrphanedQueryStoreDatabaseStateIfDueAsync(server.ServerId, cancellationToken);
        }
        else if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                 && server.Target.IsAzureSqlDb)
        {
            /* #2191's boundary, now crossable. Azure SQL DB has no database_states snapshot by design, which
               is why this was a stated no-op — but after #2220 a registration that names a database sweeps
               only that database, so its one legitimate key is the connection string's own catalog. No
               master read, nothing filtered, nothing that can go stale. A registration naming NO database is
               still skipped: it is a registration of the logical SERVER, and a single-name prune there would
               delete every live watermark it legitimately has. */
            var ownDatabase = new SqlConnectionStringBuilder(server.ConnectionString).InitialCatalog;
            if (AzureSweepScope.OwnDatabaseOrEmpty(ownDatabase).Count > 0)
            {
                await PruneForeignQueryStoreDatabaseStateAsync(server.ServerId, ownDatabase, cancellationToken);
            }
        }

        /* #2312: the plan and text watermark reads that used to merge in here (the #2164/#2150 host-owned
           state families) are GONE — the fetches are activity-driven against the store's own map/text
           tables now, so there is no persisted resume point to load. V77 deleted the orphaned rows. */

        /* #2312: the open-interval refresh stamps, merged into the flat State. Read unconditionally for
           query_store (the skip applies regardless of plan capture), and merged rather than replacing so a
           store predating this state keeps working: absent keys read as "include the open interval",
           which is the conservative behavior. */
        if (string.Equals(definition.Name, "query_store", StringComparison.Ordinal))
        {
            var openIntervalState = await GetCollectorStateAsync(
                server.ServerId, QueryStoreOpenIntervalState.StateCollectorName, cancellationToken);

            if (openIntervalState is { Count: > 0 })
            {
                var merged = new Dictionary<string, string>(StringComparer.Ordinal);
                if (collectorState is not null)
                {
                    foreach (var entry in collectorState)
                    {
                        merged[entry.Key] = entry.Value;
                    }
                }

                foreach (var entry in openIntervalState)
                {
                    merged[entry.Key] = entry.Value;
                }

                collectorState = merged;
            }
        }

        var capturePlanXml = ShouldCapturePlanXmlFor(definition.Name, server.ServerId, server.CapturePlans);
        var planFetchFlags = StampPlanFetchFlags(definition.Name, capturePlanXml, server.Target, server.CapturePlans);
        var context = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = collectionTime,
            Deltas = _deltas,
            LogHashKey = _logHashKey,
            Target = server.Target,
            Watermark = watermark,
            WatermarkFromUtcColumn = watermarkFromUtcColumn,
            NumericWatermark = numericWatermark,
            HasCollectedBefore = hasCollectedBefore,
            State = collectorState ?? CollectorContext.NoState,
            IgnoredWaitTypes = IgnoredWaitDefaults.All,
            /* The same array the #2797 dispatch probe above was built with, deliberately shared rather than
               re-derived: the probe's answer is only sound if it saw the exclusions this cycle will actually
               use, and two independent reads of server.Config could disagree. */
            ExcludedDatabases = excludedDatabases,
            /* #3477: same shared-not-re-derived rule for the scope — one resolution per run, above. */
            DatabaseScope = databaseScope,
            PerfmonCounterOverride = null,
            LongQuerySessionName = LongQuerySessionName(),
            /* #2862: plan capture is additionally cadence-gated for procedure_stats — see
               ShouldCapturePlanForCollector. Every other collector reads exactly CapturePlansFor(server.CapturePlans) (#6771).
               Only this path is gated: FetchRowsAsync below is the on-demand live fetch, which an
               operator asked for by name and which stores nothing, so it always renders. */
            CapturePlanXml = capturePlanXml,
            /* #5158: query_stats' main query leaves the plan out and the plan fetch below renders only the plans this
               host has not committed. */
            DeferPlanXmlFetch = planFetchFlags.DeferPlanXmlFetch,
            /* #5158: procedure_stats' identity columns, so the host can recognize a module plan. Both flags come from one
               resolution of the mode (StampPlanFetchFlags); the reader close and the reuse pass read it back from here. */
            PlanIdentityColumns = planFetchFlags.PlanIdentityColumns,
            /* #4735 item 1: 0 on the first attempt; RunWithSplitCharacterRetryAsync raises it after a 22021. */
            PgLogReadShiftBytes = pgLogReadShiftBytes,
            /* #2150: ON. query_sql_text is no longer carried on every runtime-stats row — it is fetched once
               per query_id into collect.query_store_text (FetchAndStoreQueryTextAsync, below) and resolved
               back by the readers, all six of which now prefer that table and fall back to the fact row's
               own column.
               Why this had to be one change and not two: text is immutable per query_id, but the runtime
               rows are re-collected every cycle, so the inline column re-shipped the same statement text on
               every snapshot — that is what made query_store_stats the largest table in the store. Flipping
               nulls the inline column, so a reader that had not been converted would have shown blank text
               for new rows while looking perfectly healthy. Rows collected BEFORE this flip still carry
               their text inline, which is why the readers keep the fallback instead of switching over. */
            FetchQueryTextSeparately = true,
            /* #2164: 0 from the default provider means "no override" — the collector keeps its own
               constant. Converted MB -> bytes here so the store knob stays operator-friendly. */
            TextByteBudgetOverride = _textBudgetMb() > 0 ? _textBudgetMb() * 1024 * 1024 : null,
            CollectSchemaChangeEvents = _collectSchemaChanges(),
        };

        /* Two accumulators, not one contiguous read-then-write pair: the enumeration and Azure paths now
           FLUSH each database's rows before reading the next (#1556), so SQL and storage slices interleave.
           Wall-clock (sqlMs + storageMs) and rows_collected totals stay coherent — collection_log is
           unchanged; only the split is now a sum of interleaved slices. */
        long sqlMs = 0;
        long storageMs = 0;
        var rowsWritten = 0;

        /* #2851: whether the server-scoped branch ran at all. The phase VALUES live on the context (next
           to PerItemOpenMs, the enumerated twin) so the stamps are property setters a reachability pin can
           see; only this flag needs method scope, because the success return sits outside the branch that
           sets it. Left false on the enumerated and Azure branches, which have their own per-item split
           (#2164) and no server-scoped line to hang this one off — the flag, not the values, is what tells
           a reader which of those two situations produced a zero. */
        bool serverPhasesMeasured = false;

        /* The per-database rollup (#2472). Both fan-out shapes feed it — the enumeration driver's
           onItemComplete hook and the Azure per-database connection loop — so a collector that fans out on
           one branch on Azure and the other on-prem reports the same shape either way. A run that never
           fans out never calls Observe and the accumulator stays empty, which is how the columns end up
           NULL on ~98 percent of collection_log rows. */
        var fanout = new FanoutCostAccumulator();

        /* The per-database FETCH split, summed (#2860). Fed by the two branches that fetch - the enumerated
           branch, inside its per-item read, and the Azure per-database loop for query_store - so a collector
           that never fetches never calls Observe and the accumulator stays empty, which is how these ten
           columns end up NULL on ~98 percent of collection_log rows. Declared beside the fan-out accumulator
           because it is the same kind of thing: a cross-item rollup the run's single row could not otherwise
           carry. */
        var fetchPhases = new FetchPhaseCostAccumulator();

        /* The collection_log note for this run (#1837) — null on every ordinary path. Only the enumeration
           branch sets it, but it is declared here so the note reaches the single success return below when
           items WERE found and merely some of their probes failed. Lite's twin is _lastCollectionNote. */
        string? collectionNote = null;
        string? queryStoreReadSource = null; // #6254: the run's read_source summary, null unless a replica is set

        /* The engine's provider, resolved ONCE for both branches. It used to be resolved only inside the
           per-database branch, and the branch below opened a hardcoded SqlConnection — so every collector
           that does NOT fan out per database was handed a SQL Server connection whatever the target was.
           Six of the seven PostgreSQL collectors then shipping took that path (only pg_autovacuum_stats
           fanned out), and SqlClient rejects Npgsql's keywords while parsing the connection string, before any query runs:
           "Keyword not supported: 'host'". Worse, an ArgumentException is neither SqlException nor
           PostgresException, so it missed BOTH classification arms in DarlingWorker and recorded a raw
           ERROR every sweep forever — including for all three Tier 0 outage predictors. */
        var targetProvider = TargetProviderOverrideForTests?.Invoke(server.Target) ?? TargetProviders.For(server.Target);

        if (definition.RunsPerDatabase(context.Target))
        {
            /* Azure SQL DB scopes some DMVs to the connected database — run the query once per
               database, skipping (and debug-logging) databases that error, matching Lite.

               Definitions with a database-scoped watermark (the XE ring-buffer collectors, whose
               per-database sessions dispatch independently) get the query rebuilt per database
               against that database's own newest already-collected value — the single server-wide
               watermark would let one busy database's newer event silence another database's older
               event still sitting in its ring buffer (#1535). Everything else keeps the
               build-once plan.

               Honor CommandTimeoutSecondsOverride here (#1556): this path previously passed the constant
               60s cap where Lite's twin already honored the override, a latent bug — index_object_stats
               needs 300s per database on Azure, so on a large Azure database its per-database read would
               have timed out at 60s. */
            var plan = definition.PerDatabaseWatermarkColumn is null || definition.WatermarkColumn is null
                ? definition.BuildQuery(context)
                : null;
            var perDbTimeout = definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds;
            var perDbProvider = targetProvider;

            /* Two enumeration paths because the FAILURE semantics genuinely differ, not the SQL. On
               Azure SQL DB an inaccessible master has a real fallback (collect the one connected
               database) and a re-probe throttle to stop hammering it; on PostgreSQL a login that
               cannot read pg_database cannot monitor the server at all, so inventing a fallback would
               turn a permissions problem into a silent one-database collection. */
            var databases = server.Target.Engine == CollectorTargetEngine.PostgreSql
                ? await GetPostgresDatabaseListAsync(server, databaseScope, cancellationToken)
                : AzureDatabaseListOverrideForTests is { } listOverride
                    ? await listOverride(server, cancellationToken)
                    : await GetAzureDatabaseListAsync(server, databaseScope, cancellationToken);

            /* The long-query trace leaves a database monitored as its own server to that registration, and never
               keeps a session in master, so its read does too (LongQueryCompletionsCollector.SkipsSeparatelyMonitoredDatabases).
               A logical server lists master beside its user databases. master goes first and is not counted as
               listed: the note below speaks of user databases, and a list of master alone has none to blame it on
               (#4961). Other collectors read the list as it came. Mirrors Lite. */
            var listedDatabaseCount = databases.Count;

            /* #6312: an XE read (attention, blocked_process_report) is of a database-scoped session, which an
               Azure SQL Database master cannot host. Reading it answered "XE session is not running" on every sweep and
               counted master as a failed database, so a healthy server carried a failure note and a real failure hid
               behind it. Left out and not counted as listed, like the long-query read below. Mirrors Lite. */
            if (definition.SkipsMasterDatabase)
            {
                databases = AzureSweepScope.WithoutMaster(databases);
                listedDatabaseCount = databases.Count;
            }

            if (definition.SkipsSeparatelyMonitoredDatabases)
            {
                databases = databases.FindAll(LongQueryTraceDatabases.CanHoldSession);
                listedDatabaseCount = databases.Count;
                databases = AzureSweepScope.WithoutSeparatelyMonitored(databases, SeparatelyMonitoredDatabasesFor(server));
            }

            /* #4961: a list with nothing left to read used to record SUCCESS, 0 rows and no note, which reads as
               "nothing ran". The note names why nothing was read (every user database monitored as its own server,
               or every database excluded). The status stays SUCCESS: nothing failed. It is held here and merged
               into the assignment that follows the loop, because that assignment is unconditional and would erase
               a note set before it. A database scope can empty the list too, and the exclusions are then
               not known to be the reason, so a scoped collector gets the separately-monitored note or none.
               Mirrors Lite. */
            var emptyListNote = EmptyDatabaseListNote.For(
                listedDatabaseCount,
                databases.Count,
                definition.SkipsSeparatelyMonitoredDatabases,
                exclusionsConfigured: server.Config.ExcludedDatabases is { Count: > 0 },
                databaseScoped: databaseScope.Count > 0);

            var attempted = 0;
            var failed = 0;
            Exception? firstFailure = null;

            /* #2623: the names, not just the count. A partial loss composes a note naming which databases
               were skipped, because the count alone does not tell an operator whether the ONE database
               that matters is in the collected set or the skipped one. */
            var failedDatabases = new List<string>();

            /* #1875: this path reads the trailing probe-failure set once PER DATABASE, so the note and the
               log cap are decided for the cycle after the loop rather than inside it — see
               CycleProbeFailures for why neither generalizes from the single-read plain path. */
            var cycleProbeFailures = new CycleProbeFailures(context.Target.IsAwsRds);

            /* One pooled store connection for the whole body; one binary COPY per database on it
               (completing an importer commits that database — commit-1..N-1 semantics on abort). */
            await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);

            /* #6875: the same pass budget and rotation as the enumerated driver, for the per-database connection loop. */
            var (azurePassBudget, azureOrder) = BeginPass(definition, server.ServerId, databases);
            if (azurePassBudget is not null)
            {
                databases = [.. azureOrder];
            }

            string? azureLastStarted = null;
            string? azureStoppedAfter = null;

            foreach (var databaseName in databases)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (attempted > 0 && azurePassBudget is { Spent: true })
                {
                    azureStoppedAfter = azureLastStarted;
                    break;
                }

                azureLastStarted = databaseName;
                attempted++;

                /* #2150: THE path the field report is on — Azure SQL DB collects query_store per database
                   here, not through the enumerated driver, so the wall-clock ceiling has to be applied on
                   both. Null for every collector that declares none, in which case dbToken IS
                   cancellationToken and this loop is byte-for-byte what it was. */
                using var dbBudget = EnumeratedCollectorDriver.StartItemBudget(
                    definition.PerItemWallClockBudget, cancellationToken);
                var dbToken = dbBudget?.Token ?? cancellationToken;

                /* #2312: this database's open-interval stamp, staged at decision time and landed only
                   after its read and flush succeed — per iteration, so a fault cannot leak a stamp
                   into a sibling database's landing. */
                string? stagedOpenIntervalStamp = null;

                /* #5514: this database's contribution to the per-database Query Store watermark cache, staged
                   after the read and landed only once the flush returned — per iteration, like the stamp above,
                   so a fault cannot leak one database's contribution into a sibling's. */
                StagedDatabaseWatermark? stagedDatabaseWatermark = null;

                /* The definition's own per-item staged state (CollectorContext.StagedItemState), cleared per
                   iteration for the same reason: a fault must not leak this database's staged cursor into a
                   sibling's landing. It lands below, after this database's flush, and nowhere else. */
                context.DropStagedItemState();

                /* #2896: the DECLARATION is hoisted, the START is not. The catch arms below print this
                   database's split and a split needs its parent, so the stopwatch has to be in scope
                   there — but it is still started at exactly the statement it was started at before, so
                   the interval it measures is unchanged and dbSqlMs is the same number it always was.
                   That matters because dbSqlMs is not log-only: it feeds sqlMs, fanout.Observe and
                   collection_log.sql_duration_ms, and starting the stopwatch above the watermark read
                   and BuildQuery to make it non-null would widen all three to satisfy a log line.

                   Null therefore means "the fault landed before the timed region" — in the watermark
                   read, the adaptive shrink's store write, or BuildQuery. There is genuinely no parent
                   in that case, and the compiler enforces the check rather than a comment claiming it. */
                Stopwatch? sqlSlice = null;
                /* #5813 review: set once this database's slice is in sqlMs. A fault before that point (a read the
                   budget ended, a login that failed) folds the slice in its catch arm instead, as the enumerated
                   driver's finally does for every item: the fan-out counts that database (and a timed-out one at the
                   budget's milliseconds), so leaving its SQL time out would put the item count and sql_duration_ms on
                   different denominators, the #2472 shape. */
                var sliceFolded = false;
                try
                {
                    /* The authoritative database_name for XE rows read on this path — see
                       CollectorContext.CurrentDatabaseName. */
                    context.CurrentDatabaseName = databaseName;

                    /* #4961, #5640: the deadlock, blocked-process and attention reads name the session the ensure chose for THIS database, so
                       the name is set per database, beside the database name. Every other definition leaves it null. */
                    context.AlwaysOnSessionName = definition switch
                    {
                        DeadlocksCollector => AlwaysOnReadSessionName(server, databaseName, AlwaysOnXeSessionKind.Deadlock),
                        BlockedProcessReportCollector => AlwaysOnReadSessionName(server, databaseName, AlwaysOnXeSessionKind.BlockedProcess),
                        AttentionCollector => AlwaysOnReadSessionName(server, databaseName, AlwaysOnXeSessionKind.Attention),
                        _ => null,
                    };

                    /* #2855: cleared once per iteration, because this loop reuses ONE context across every
                       database and without the reset a database whose read faults would print the PREVIOUS
                       database's split as its own — a stale timing that looks precise is worse than no
                       timing at all. Same rule, and the same reason, as the enumerated readItem's clear of
                       its own stamps.

                       #2896 moved it from just below the stopwatch to HERE, above the watermark read and
                       BuildQuery. #2855 only needed it before the CONNECT, because only the success path
                       read the split; the catch arms now read it too, and a fault in the watermark read
                       reaches them without having passed the old position — so the flag and the three
                       stamps would still hold the previous database's values and the fault line would
                       confidently attribute another database's connect to this one. Nothing between here
                       and the connect reads any PerDatabase* field, so the success path is unchanged.

                       This is also what makes PerDatabasePhasesMeasured a SUFFICIENT gate on the fault
                       path: the flag is only ever set from the connect's finally below, which is
                       downstream of both this clear and the stopwatch, so a true flag now implies both
                       that these stamps are this database's own and that sqlSlice was started. */
                    context.PerDatabaseConnectMs = 0;
                    context.PerDatabaseOpenMs = 0;
                    context.PerDatabaseDrainMs = 0;
                    context.PerDatabasePhasesMeasured = false;

                    /* The plan and text fetch readings, on the same rule: query_store's fetches run on this
                       branch too now, and this loop reuses ONE context across every database, so a database
                       that skips or faults its fetch would otherwise add the previous database's figures to
                       the run's fetch sums as its own. */
                    ClearFetchPhaseStamps(context);

                    var dbPlan = plan;
                    if (dbPlan is null)
                    {
                        /* Null (no rows for this database yet) falls back to the definition's
                           documented first-run window, per database. No clamp is applied HERE because
                           this branch also serves the XE ring-buffer collectors (deadlocks / BPR),
                           where flooring a stale watermark would WRONGLY truncate legitimate catch-up
                           — those sources roll past the catch-up horizon on their own. query_store also
                           branch on Azure SQL DB (#1836) and does need the bound, so it applies
                           WatermarkPolicy.ClampCatchup inside its own cutoff computation: the clamp
                           travels with the collector that needs it instead of with the path. */
                    /* dbToken throughout this branch (#2150 review catch): the interface contract says the
                       budget covers "the watermark refresh, the command, and the whole drain", and the
                       enumerated path's perItemWatermark delegate already honours that. Leaving these three
                       store round-trips on cancellationToken made THIS loop — the one the field report is
                       actually on — the only place the promise was not kept, and a store that has stopped
                       answering is exactly the stall the budget exists to bound. Safe for the hole records
                       specifically: a budget expiry abandons the whole pass, so the watermark does not
                       advance, the clamp is re-derived next cycle, and the hole is re-recorded (merged wider
                       with any already pending) rather than lost. */
                        /* #2344: same bound as the enumerated arm. Safe here for the same reason and
                           by a different route — this branch does not clamp itself, but query_store's own
                           BuildCutoffParameters does (the #1836 double-clamp the policy documents), so the
                           value this read returns is clamped before anything uses it. */
                        var azureReadFloor = string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                            ? WatermarkPolicy.ReadFloor(collectionTime)
                            : null;
                        /* #5514: query_store on this arm resolves its watermark through the per-database cache,
                           exactly as the enumerated arm does. #4669 left this arm out of the cache ("it shares its
                           read with the unbounded XE collectors") and invalidated the key every pass, so every
                           Azure SQL Database paid one bounded store read per database per cycle: 181,050 calls in
                           13 days on a 43-server store. The key is read through the same floor the enumerated arm
                           uses, advanced from this database's batch only after its write returned (stage below,
                           commit in the success block), and dropped by both fault arms. The XE ring-buffer
                           collectors have no read floor (azureReadFloor is null) and keep their own read. */
                        if (azureReadFloor is DateTime azureCacheFloor)
                        {
                            context.Watermark = await ResolveQueryStoreDatabaseWatermarkAsync(
                                server, definition.TargetTable, definition.WatermarkColumn!,
                                definition.PerDatabaseWatermarkColumn!, databaseName, azureCacheFloor, collectionTime, dbToken);
                        }
                        else
                        {
                            context.Watermark = await GetLastCollectedTimeForDatabaseAsync(
                                server.ServerId, definition.TargetTable, definition.WatermarkColumn!,
                                definition.PerDatabaseWatermarkColumn!, databaseName, dbToken, azureReadFloor);
                        }

                        /* #2111 adaptive shrink, Azure arm — tighten BEFORE BuildQuery: the
                           definition's own clamp only floors OLDER watermarks, so a tighter one
                           passes through untouched. The skipped range is recorded as a hole here
                           (wider than the clamp's own record would be, so the block below firing
                           too would merge, not conflict). */
                        var azureFailures = ConsecutiveQueryStoreItemFailures(server.ServerId, databaseName);
                        if (azureFailures > 0
                            && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                        {
                            var adaptiveSpan = QueryStoreBackfillState.AdaptiveSpan(WatermarkPolicy.MaxCatchup, azureFailures);
                            var tighterFloor = collectionTime - adaptiveSpan;
                            if (context.Watermark is DateTime azureRaw)
                            {
                                if (azureRaw < tighterFloor)
                                {
                                    _logger?.LogWarning(
                                        "query_store on '{Server}' database [{Database}] adaptive catch-up shrink: {Failures} consecutive failed cycles — window narrowed to {Minutes:F0}m; the skipped range rides the backfill hole.",
                                        server.Config.DisplayName, databaseName, azureFailures, adaptiveSpan.TotalMinutes);
                                    await RecordQueryStoreBackfillHoleAsync(server.ServerId, databaseName, azureRaw, tighterFloor, dbToken);
                                    context.Watermark = tighterFloor;
                                }
                            }
                            else
                            {
                                /* Never-succeeded database: tighten the first-run fallback too (the
                                   review catch); no hole — pre-watermark history is the tail's job. */
                                _logger?.LogWarning(
                                    "query_store on '{Server}' database [{Database}] adaptive first-contact shrink: {Failures} consecutive failed cycles — first-run window narrowed to {Minutes:F0}m.",
                                    server.Config.DisplayName, databaseName, azureFailures, adaptiveSpan.TotalMinutes);
                                context.Watermark = tighterFloor;
                            }
                        }

                        /* #2312, Azure arm: same per-database open-interval decision as the enumerated
                           delegate, BEFORE BuildQuery bakes the predicate. Staged into the local, landed
                           only in the post-flush success block below — a per-database fault this loop
                           tolerates must re-include next cycle, not spend the refresh window. */
                        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                        {
                            var includeOpen = QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
                                context.State, databaseName, collectionTime);
                            context.IncludeOpenInterval = includeOpen;
                            if (includeOpen)
                            {
                                stagedOpenIntervalStamp = QueryStoreOpenIntervalState.Format(collectionTime);
                            }
                        }

                        dbPlan = definition.BuildQuery(context);

                        /* The definition clamped its own cutoff — surface the same WARNING the
                           enumeration path emits, so the bounded history hole stays LOGGED and does
                           not become the one silent hole in a policy whose whole premise is that it
                           is visible. Mirrors Lite. */
                        if (context.CatchupClampApplied)
                        {
                            _logger?.LogWarning(
                                "{Collector} on '{Server}' database [{Database}] catch-up clamped to {Hours}h (stored watermark {Raw:o} is older) — a bounded, logged history hole.",
                                definition.Name, server.Config.DisplayName, databaseName, WatermarkPolicy.MaxCatchup.TotalHours, context.Watermark);

                            /* #2058 (the Azure arm of #2022's hole recording): context.Watermark still
                               holds the RAW value here — the definition clamped only its own cutoff
                               parameter — so the hole is (raw, re-derived clamp floor), same merge
                               semantics as the enumerated site. Only query_store both clamps AND has a
                               backfill worker; the name guard keeps the XE collectors that share this
                               branch from growing backfill state they have no worker for. */
                            if (context.Watermark.HasValue
                                && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                                && WatermarkPolicy.ClampCatchup(context.Watermark, collectionTime) is DateTime azureClampedFloor)
                            {
                                await RecordQueryStoreBackfillHoleAsync(
                                    server.ServerId, databaseName, context.Watermark.Value, azureClampedFloor, dbToken);
                            }
                        }
                    }

                    sqlSlice = Stopwatch.StartNew();
                    List<TRow> batch;

                    /* dbToken, not cancellationToken (#2150): connect, execute and drain are the phases the
                       budget bounds. The FLUSH below deliberately stays on cancellationToken — abandoning a
                       write already in flight would trade a slow cycle for a partially-written one. */
                    /* #2855: the CONNECT is a phase in its own right here, and only here. This branch opens a
                       connection per database, so on Azure SQL DB every cycle pays a fresh login per
                       database — a cost that had nowhere to appear and therefore sat inside the blended
                       number with everything else. Timed around OpenDatabaseConnectionAsync alone, so
                       command construction lands in the residual rather than in a phase that is supposed to
                       mean "reaching the database".

                       Stamped from a finally, with the flag, for the #2854 reason: a login that times out or
                       a per-database budget that fires during the handshake is exactly the case this exists
                       to attribute, and a trailing assignment is skipped on precisely that path. The stamp
                       would then read connect:0ms and hand its whole cost to other: — the one term whose job
                       is to be small and unattributed. The flag rides the same finally so the two can never
                       disagree about whether a measurement happened. */
                    var connectWatch = Stopwatch.StartNew();
                    DbConnection openedConnection;
                    try
                    {
                        openedConnection = await OpenDatabaseConnectionAsync(perDbProvider, server, databaseName, dbToken);
                    }
                    finally
                    {
                        context.PerDatabaseConnectMs = connectWatch.ElapsedMilliseconds;
                        context.PerDatabasePhasesMeasured = true;
                    }

                    /* Held to the end of the iteration rather than closed with the read: query_store's plan and
                       text fetches below run on this database's own connection, after its command and reader
                       are disposed. Every other collector on this branch simply holds an idle connection for
                       the flush, which is milliseconds. */
                    using var dbConnection = openedConnection;

                    /* #6143: an extension is created per database, in whatever schema that database chose, so a
                       collector that reads one rebuilds its query here, on this database's own connection, once
                       that database's extension schemas are known. Every other collector keeps the plan above. */
                    if (await ResolvePgExtensionSchemasAsync(context, definition, dbConnection, _logger, dbToken))
                    {
                        dbPlan = definition.BuildQuery(context);
                    }

                    using (var dbCommand = CreateCollectorCommand(perDbProvider, dbPlan, dbConnection, perDbTimeout))
                    {
                        /* #2855: the open, same contract as the other two paths — ExecuteReaderAsync returns
                           only when the first rowset is available, so this is server-side work before the
                           first row, which no client-side budget shortens. Hoisted out of the `using` header
                           only so the reader below keeps its original disposal scope. */
                        /* #5813: the per-database budget on the runtime-stats statement, open through drain, the
                           same as the enumerated readItem (query_store only; null for the rest). Linked to
                           dbToken, so the #2150 item budget and shutdown still win their own cases. */
                        using var readBudget = QueryStoreReadBudget.Start(definition.Name, QueryStoreReadBudgetSpan, dbToken);
                        var readToken = readBudget?.Token ?? dbToken;
                        var openWatch = Stopwatch.StartNew();
                        DbDataReader openedReader;
                        try
                        {
                            openedReader = await dbCommand.ExecuteReaderAsync(readToken);
                        }
                        catch (Exception ex) when (ex is not OutOfMemoryException && QueryStoreReadBudget.Expired(readBudget, dbToken))
                        {
                            throw new QueryStoreReadTimeoutException(databaseName, QueryStoreReadBudgetSpan, ex);
                        }
                        finally
                        {
                            context.PerDatabaseOpenMs = openWatch.ElapsedMilliseconds;
                        }

                        using var dbReader = openedReader;

                        /* #2855: the drain, measured rather than inferred — the ServerScopeDrainMs argument.
                           A residual drain would silently absorb command construction and the trailing
                           probe-failure rowset below, and then a large other: would say nothing about our
                           own code. From a finally so a budget expiry mid-stream reports how far it got. */
                        var drainWatch = Stopwatch.StartNew();
                        try
                        {
                            batch = await definition.ReadAsync(dbReader, context, readToken);
                        }
                        catch (Exception ex) when (ex is not OutOfMemoryException && QueryStoreReadBudget.Expired(readBudget, dbToken))
                        {
                            throw new QueryStoreReadTimeoutException(databaseName, QueryStoreReadBudgetSpan, ex);
                        }
                        finally
                        {
                            context.PerDatabaseDrainMs = drainWatch.ElapsedMilliseconds;
                        }

                        /* #1875: the payload path's probe-failure contract, on the path that used to
                           ignore it. blocked_process_report is the declaring collector that also runs per
                           database (Azure SQL DB, #1535), so before this its batch produced the trailing
                           set and the loop simply never advanced the reader to it — the rows were built
                           and dropped. Read HERE, still inside the reader and inside the per-database
                           try, so a diagnostics fault stays a one-database skip like any other. */
                        if (definition.EmitsProbeFailures)
                        {
                            cycleProbeFailures.Add(
                                await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(dbReader, dbToken, context.Target.IsAwsRds));
                        }
                    }
                    /* #6182: procedure_stats' plan phase on Azure SQL Database. #5449 moved the plan out of the main query into a second
                       query and wired that into the server-scoped path only, so every procedure this loop read was stored with a NULL
                       plan and get_procedure_plan_xml found nothing for any of them. The same query, for this database's rows, on this
                       database's own connection after its reader is disposed, inside the same budget and the same SQL slice. */
                    if ((object)batch is List<ProcedureStatsCollector.Row> planBatch)
                    {
                        await RunProcedureStatsDatabasePlanPhaseAsync(perDbProvider, dbConnection, server, context, planBatch, dbToken);
                    }

                    /* #6009: procedure_stats' module phase on Azure SQL Database, where this loop is the collector's path: this database's
                       watched modules, on this database's own connection, after its reader is disposed. Sends nothing when no module of
                       this database is watched. Inside the slice, so its time counts as this database's SQL time. */
                    if ((object)batch is List<ProcedureStatsCollector.Row> moduleBatch && moduleBatch.Count > 0)
                    {
                        await RunProcedureStatsModulePhaseAsync(perDbProvider, null, dbConnection, server, context, moduleBatch, null, dbToken);
                    }

                    /* Read ONCE. A second read a few statements later returns a larger number, and the
                       per-item total would then exceed the blended total it is a ratio against — a dominance
                       a hair above the truth, on every Azure run (#2472). Small, and wrong in the direction
                       that matters.

                       #2896 review catch: STOPPED rather than merely read once, and stopped BEFORE the read
                       so the read is the frozen value. "Read once" was a convention the success path kept by
                       having only one read; the fault arms below now hold the same stopwatch, and everything
                       between here and them — the flush above all — kept accumulating into it. A store write
                       that throws (WriteBatchAsync deliberately runs on cancellationToken, so it CAN throw
                       into these arms) would have printed a sql: larger than the dbSqlMs already folded into
                       sqlMs and fanout.Observe for this same database, with the whole difference landing in
                       other: — storage latency reported as unattributed SQL-side residual, which is the
                       exact mis-attribution this instrumentation exists to prevent.

                       Stopping makes the two paths print the same parent by construction rather than by
                       both happening to read at the same moment. It does NOT widen dbSqlMs: the interval
                       still ends here, a hair earlier than the old read if anything. A fault BEFORE this
                       point still finds the stopwatch running and reports how far it got, which is what the
                       fault line is for. */
                    sqlSlice.Stop();
                    var dbSqlMs = sqlSlice.ElapsedMilliseconds;
                    sqlMs += dbSqlMs;
                    sliceFolded = true;

                    /* query_store's plan XML and statement text, for THIS database, on its own connection. The
                       payload always carries a NULL plan and (FetchQueryTextSeparately) no text, so these two
                       fetches are the only way either reaches the store - and this loop, which Azure SQL
                       Database takes because QueryStoreCollector.RunsPerDatabase is IsAzureSqlDb, read and
                       flushed without ever calling them: every Azure row was stored with neither.

                       databaseName, never the registration's initial catalog: the by-ids queries run
                       [db].sys.sp_executesql, which Azure accepts only for the connection's current database,
                       and dbConnection is connected to exactly this one.

                       BEFORE the flush, as on the enumerated path, and the order matters beyond symmetry: the
                       fetch repairs the shared store connection it borrows before it touches it, so the flush
                       that follows never meets a connection a previous database's budget expiry broke. After
                       the flush that repair would come one database too late. dbToken, so the wall-clock
                       budget bounds the fetch as it bounds the read; an expiry lands in the budget arm below
                       and the database is re-read next cycle, exactly as on the enumerated path.

                       Outside dbSqlMs on purpose (the #2896 pins): the read's connect/open/drain/other split
                       keeps meaning what it meant, and the fetch is reported on its own lines and in the run's
                       fetch sums. Its time is folded into sqlMs and the fan-out figure below, because the
                       enumerated path counts it inside the item's SQL slice and a database that is expensive
                       only because of its fetches should still read as expensive. */
                    long dbFetchMs = 0;
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        await FetchQueryStorePlansAndTextAsync(
                            perDbProvider, dbConnection, pgConnection, server, databaseName, definition.Name, context, perDbTimeout, batch, dbToken);

                        dbFetchMs = context.PerItemPlanFetchMs + context.PerItemTextFetchMs;
                        sqlMs += dbFetchMs;
                    }

                    /* #5514: what this batch contributes to the cache, computed from the rows as read (the same
                       staging the enumerated arm does after its fetch) and landed below, after the flush. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        stagedDatabaseWatermark = StageQueryStoreDatabaseWatermark(batch, databaseName, collectionTime);
                    }

                    /* Flush this database before reading the next — peak memory is one database's rows. */
                    long dbStorageMs = 0;
                    if (batch.Count > 0)
                    {
                        var storageSlice = Stopwatch.StartNew();
                        PerDatabaseWriteFaultForTests?.Invoke(databaseName);
                        rowsWritten += await WriteBatchAsync(pgConnection, definition, batch, server, collectionTime, context, cancellationToken);
                        dbStorageMs = storageSlice.ElapsedMilliseconds;
                        storageMs += dbStorageMs;
                    }

                    /* The read AND the flush both succeeded: only now may the state the definition staged for
                       this database (a telemetry cursor, the shred gate's execution count) be saved. A failed
                       write throws past this line into the per-database catch, which skips the database while
                       a sibling's success still saves PendingState - so state landed any earlier would advance
                       past rows that were never stored. */
                    context.LandStagedItemState();

                    /* #2472: this database's slice, counted even when its batch was empty — an empty batch
                       still paid for its read, and that read is in the blended total the rollup is a ratio
                       against. Observed here rather than beside the log line below for the same reason the
                       completion hook fires after the flush: both slices are only known once the write is
                       done. */
                    fanout.Observe(databaseName, dbSqlMs + dbFetchMs + dbStorageMs);

                    /* And this database's fetch readings into the run's fetch sums (#2860), for every completed
                       database and not gated on rows, for the reason the enumerated hook states: a quiet
                       database can still pay real fetch time draining ids deferred from an earlier pass. Here,
                       past the flush, because the enumerated hook fires only for items whose read and flush
                       both landed and the sums should mean the same thing on both branches. The readings are
                       still this database's own: they clear at the top of the next iteration, not here. */
                    fetchPhases.Observe(context);

                    /* #2855: this branch's per-database phase line — the line it did not emit at all. Read
                       after the flush, like the other two paths, so pg: is this database's own figure.

                       Gated on the split being present rather than on a figure being non-zero, and the
                       null-or-value decision lives on the context so "no split" and "no line" are one
                       decision made once. A `connect: > 0` gate would suppress the whole line for a pooled
                       connect that really did cost nothing — the case where the rest of the line is most
                       worth reading, and the mistake #2854 had to undo on the enumerated path.

                       NOT gated on batch.Count > 0, unlike the enumerated path's #1565 quiet-on-zero rule.
                       That rule guards a ROW-COUNT line, whose payload is the row count, and a zero-row one
                       carries nothing. This is a PHASE line, and its payload — the connect above all — is
                       paid on a quiet database exactly as on a busy one. The server-scoped phase line is the
                       closer sibling and prints for every measured run regardless of rows; suppressing quiet
                       databases here would leave them emitting nothing whatsoever, which is the state this
                       issue is about.

                       Its OWN line rather than folded into anything, the #2811/#2851 rule: these lines are
                       parsed by tooling outside this repo, so "don't break the parser" outranks "one line to
                       grep". Nothing here is persisted — a per-database split is N:1 against collection_log
                       and shaping that is the open decision in #2860.

                       THE SUCCESS PATH IS NO LONGER THE ONLY ONE (#2896). This site still prints only when
                       the read and flush both landed; a database that faults is printed by
                       LogPerDatabaseFaultSplit from the catch arms below, which is the case #2855's stamps
                       were placed in a finally to serve and which used to reach a log that said nothing but
                       an error string.

                       The obstacle #2855 recorded was that dbSqlMs is out of scope in the catch, and that
                       hoisting sqlSlice above the watermark read and BuildQuery to fix it would silently
                       widen dbSqlMs — which is not log-only: it feeds sqlMs, the fan-out rollup and
                       collection_log's sql_duration_ms. Neither of the two options it named was taken. Only
                       the DECLARATION moved above the try; the START stayed exactly where it was, so the
                       interval is unchanged, dbSqlMs is the same number, and no second stopwatch measures a
                       second interval that a reader would then have to reconcile with this one.

                       #3102: Debug, matching the enumerated branch's block below and the generic fault
                       arm's own level. A database that SUCCEEDED has no error for its split to sit beside,
                       which is exactly LogPerDatabaseFaultSplit's rule about a phase line being louder
                       than the thing it decomposes. */
                    if (context.PerDatabasePhasesFrom(dbSqlMs) is { } dbPhases)
                    {
                        _logger?.LogDebug(
                            "  [{Server}] {Collector} [{Database}] sql:{SqlMs}ms = connect:{ConnectMs}ms + open:{OpenMs}ms + drain:{DrainMs}ms + other:{OtherMs}ms ({Rows} rows, pg:{PgMs}ms)",
                            server.Config.DisplayName, definition.Name, databaseName, dbSqlMs,
                            dbPhases.ConnectMs, dbPhases.OpenMs, dbPhases.DrainMs, dbPhases.OtherMs,
                            batch.Count, dbStorageMs);
                    }

                    /* The fetch sub-splits, on their own lines after the phase line they are not part of, and
                       only for a fetch that ran (query_store; nothing else sets the readings). */
                    LogFetchPhaseSplits(server, definition.Name, databaseName, context);

                    /* Same per-database bounded-cycle WARNING the enumeration path emits from
                       onItemComplete, mirroring Lite. Reachable here since #1836 put query_store — the
                       only collector that declares either bound — on this branch for Azure SQL DB;
                       without it a database whose cycle was cut at the bound would look like a clean
                       collection. Since #1960 a bound DEFERS the backlog to the next cycle's resume
                       from the shipped boundary rather than dropping it — this log is how a long
                       catch-up stays observable. Read after the flush, as on the other path: the
                       context signal stays this database's until the next read resets it. */
                    var capHit = definition.PerItemRowCountWarnThreshold is int cap && batch.Count >= cap;
                    if (capHit || context.PerItemTextBudgetExceeded)
                    {
                        _logger?.LogWarning(
                            "{Collector} on '{Server}' database [{Database}] hit its per-database collection bound ({Reason}) — shipped {ShippedMB:F1}MB up to {Boundary}; the backlog resumes from that boundary next cycle.",
                            definition.Name, server.Config.DisplayName, databaseName,
                            capHit ? $"row cap {definition.PerItemRowCountWarnThreshold}" : "text byte budget",
                            context.PerItemTextBytesShipped / (1024.0 * 1024.0),
                            context.PerItemShippedBoundary?.ToString("o") ?? "n/a");
                    }

                    /* #2111: success resets the adaptive-shrink count on the Azure arm too. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        OnQueryStoreItemSucceeded(server.ServerId, databaseName);

                        /* #5514: the flush returned, so the staged batch is in the store and the cache may
                           advance past it. Never earlier: a watermark ahead of committed rows skips them. */
                        if (stagedDatabaseWatermark is { } landedWatermark)
                        {
                            CommitQueryStoreDatabaseWatermark(server, databaseName, landedWatermark);
                        }

                        /* #2312: read and flush both landed — the staged open-interval stamp may too. */
                        if (stagedOpenIntervalStamp is not null)
                        {
                            context.PendingState[QueryStoreOpenIntervalState.KeyFor(databaseName)] = stagedOpenIntervalStamp;
                        }
                    }
                }
                catch (OutOfMemoryException)
                {
                    /* AHEAD of the budget arm, because ItemBudgetExpired classifies on the TOKENS and never
                       looks at the exception type (review catch). Without this, an OOM thrown while the
                       budget's timer had already fired — materializing a large batch, or inside the store
                       write — would be caught by that arm and logged as a routine per-database timeout,
                       silently breaking the invariant the generic catch below states outright. The shared
                       EnumeratedCollectorDriver already orders it this way; these two loops did not. */
                    throw;
                }
                catch (Exception ex) when (EnumeratedCollectorDriver.ItemBudgetExpired(dbBudget, cancellationToken))
                {
                    /* #2150: this database ran out of wall clock. Counted as a per-database failure so the
                       cycle moves on — one database must not be able to starve the rest, which is the harm
                       the field report describes. Ahead of the generic catch because a cancelled command
                       does not reliably arrive as an OperationCanceledException, so that filter cannot be
                       trusted to claim it; the token check is what keeps a real shutdown out of this arm.
                       The provider's own cancellation exception is dropped in favour of the budget message:
                       it describes HOW the read was stopped, not why. */
                    _ = ex;
                    var budgetFailure = EnumeratedCollectorDriver.ItemBudgetException(
                        definition.PerItemWallClockBudget!.Value);
                    failed++;
                    failedDatabases.Add(databaseName);
                    CollectorFaultDatabase.Stamp(budgetFailure, databaseName);
                    firstFailure ??= budgetFailure;

                    /* Same #2111 stamp the generic arm makes, and it MATTERS more here: this is what turns
                       the bound from a cut that repeats forever into one that converges. The consecutive
                       count narrows this database's next catch-up window, so a database that cannot finish
                       in the budget keeps halving until it can. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        OnQueryStoreItemFailed(server.ServerId, databaseName);

                        /* #5514: a batch was staged, so its write may or may not have committed: drop this key and
                           let the next cycle read the store. A fault before the stage wrote nothing, and the key
                           stays. That matters for a PARTIAL failure: a database that fails every cycle (offline, a
                           login that cannot reach it) beside healthy siblings must not turn back into a store read
                           every cycle. When EVERY database fails (a one-database registration, the common shape)
                           the loop rethrows and RunAsync's catch drops all of this server's query_store keys
                           anyway, so there the kept key buys nothing. Correctness is the same either way: a kept
                           key is equal to the store or stale-low, never ahead. */
                        if (stagedDatabaseWatermark is not null)
                        {
                            _databaseWatermarkCache.Invalidate(server.ServerId, databaseName);
                        }
                    }

                    /* WARNING, not Debug, unlike the routine per-database skip beside it: an offline
                       database is ordinary and this is a collector that could not finish its work. */
                    _logger?.LogWarning(
                        "{Collector} on '{Server}' database [{Database}] {Message}",
                        definition.Name, server.Config.DisplayName, databaseName, budgetFailure.Message);

                    /* #2896: and the split behind it, at the arm's own WARNING. This is the case the budget
                       exists to bound, and connect: is the term that says whether the wall clock went on
                       reaching the database or on reading it — which the message above cannot say. */
                    FoldFailedSlice(sqlSlice, sliceFolded, ref sqlMs);
                    LogPerDatabaseFaultSplit(
                        _logger, LogLevel.Warning, context, sqlSlice,
                        server.Config.DisplayName, definition.Name, databaseName, "budget expired");
                }
                catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException)
                {
                    /* OOM is filtered OUT of this per-database skip and propagates: it is fatal, not a
                       routine one-database miss. */
                    failed++;
                    failedDatabases.Add(databaseName);
                    /* #2997: which database this was, carried ON the exception. When every database fails
                       the loop rethrows firstFailure bare, and the fault handlers upstream have only
                       ServerRuntime.ConnectedDatabase to fall back on - an init-only field stamped once at
                       the initial probe, which for a RunsPerDatabase collector is not the database that
                       failed. The stamp travels with the exception through the rethrow, so a message can
                       name the database it means. */
                    CollectorFaultDatabase.Stamp(ex, databaseName);
                    /* #6312: a connection failure outranks the others, so an outage (a firewall rule, a paused database)
                       is not reported as the missing XE session some other database answered first. */
                    firstFailure = AzureSweepScope.PreferConnectionFailure(firstFailure, ex, DarlingXeSessions.ErrorNumbersOf);

                    /* #2111: the yield-to-live stamp + adaptive-shrink count for the Azure SQL DB
                       arm — query_store reaches THIS per-database loop there, not the enumeration
                       path's onItemError, and without the stamp the backfill worker would never
                       yield on an Azure target (the review catch on #2112). Same query_store-only
                       guard as the hole recording above. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        OnQueryStoreItemFailed(server.ServerId, databaseName);

                        /* #5514: a batch was staged, so its write may or may not have committed: drop this key and
                           let the next cycle read the store. A fault before the stage wrote nothing, and the key
                           stays. That matters for a PARTIAL failure: a database that fails every cycle (offline, a
                           login that cannot reach it) beside healthy siblings must not turn back into a store read
                           every cycle. When EVERY database fails (a one-database registration, the common shape)
                           the loop rethrows and RunAsync's catch drops all of this server's query_store keys
                           anyway, so there the kept key buys nothing. Correctness is the same either way: a kept
                           key is equal to the store or stale-low, never ahead. */
                        if (stagedDatabaseWatermark is not null)
                        {
                            _databaseWatermarkCache.Invalidate(server.ServerId, databaseName);
                        }
                    }

                    /* #5813: a Query Store read that ran out of its budget is the run's slowest item under its marked
                       name and warns once per database per UTC day; anything else is the routine skip. */
                    if (!NoteQueryStoreReadTimeout(server, ex, fanout))
                    {
                        _logger?.LogDebug("Skipping database '{Database}' for {Collector}: {Error}", databaseName, definition.Name, ex.Message);
                    }

                    /* #2896: the split, at the SAME Debug as the skip it decomposes — deliberately, and it
                       is the level question the issue raises answered rather than dodged.

                       This arm stays Debug because the level is not what makes a persistently-failing
                       database visible, and treating it as though it were would trade a real signal for a
                       loud one. A partial loss already composes BuildPartialFailureNote below — "N of M
                       database(s) failed", up to three named, plus the first error — into
                       collection_log.error_message, which is PERSISTED and queryable and is what
                       get_collection_health reports as last_note/note_count. A database failing every cycle
                       for a week shows up there as a note on every run; a Warning would only have shown up
                       in whatever log window had not yet rolled. And a TOTAL loss is already a Warning plus
                       a rethrow further down, so it classifies and reaches the Capture Down self-alert.
                       Raising this arm would emit one Warning per database per cycle per collector, which on
                       a target with many databases is the flood #1875's one-capped-burst rule exists to
                       stop.

                       So the split rides the arm rather than the arm rising to meet it: a phase line louder
                       than the error it decomposes puts a timing on the default log with no reason beside
                       it, which reads as an unexplained slow database rather than a skipped one. At Debug
                       the two lines arrive together, which is the state the issue is about — before this,
                       turning Debug on bought the error string and no timing at all. */
                    FoldFailedSlice(sqlSlice, sliceFolded, ref sqlMs);
                    LogPerDatabaseFaultSplit(
                        _logger, LogLevel.Debug, context, sqlSlice,
                        server.Config.DisplayName, definition.Name, databaseName, "failed");
                }
            }

            context.CurrentDatabaseName = null;

            /* #3153: retire per-database state for databases this sweep did not enumerate, using the list
               it just swept. AFTER the loop on purpose - the enumeration is what makes the list
               authoritative, and a database that merely failed to READ is still enumerated, so its cursor
               is kept. No-ops for every collector without a PgPerDatabaseCollectorState entry. */
            await PrunePgPerDatabaseStateAsync(server.ServerId, definition.Name, databases, cancellationToken);

            /* #1875: ONE note for the cycle and ONE capped log burst, composed from every database's
               failures together. Assigned unconditionally — a cycle where nothing failed composes null,
               which is exactly what this path carried before. The empty-list note (#4961) rides in this
               assignment: it is the one place the note is set, so nothing assigned earlier can be erased. */
            collectionNote = EnumeratedCollectorDriver.MergeNotes(
                emptyListNote,
                EnumeratedCollectorDriver.MergeNotes(
                    cycleProbeFailures.Note,
                    EnumeratedCollectorDriver.MergeNotes(
                        EnumeratedCollectorDriver.BuildPartialFailureNote(
                            failed, attempted, failedDatabases, firstFailure?.Message),
                        EndPass(azurePassBudget, definition, server.ServerId, azureStoppedAfter, attempted, databases.Count))));
            LogEnumerationProbeFailures(definition, server, cycleProbeFailures.Failures);

            /* One database failing is routine (offline, mid-restore, a permissions oddity) and stays a
               debug-logged skip. EVERY database failing is a systemic fault — before this check the run
               recorded SUCCESS with zero rows, which on the XE collectors also made the SESSION_MISSING
               classification (RunXeTolerantAsync → the Capture Down self-alert) unreachable on Azure.
               Rethrow the first failure so RunOneAsync classifies it (SESSION_MISSING / PERMISSIONS /
               ERROR) instead. Mirrors Lite's definition runner. */
            if (attempted > 0 && failed == attempted && firstFailure is not null)
            {
                LogFailedInAllDatabases(server, definition.Name, attempted, firstFailure);

                /* #5813: the fan-out rides on the rethrown failure, so the worker's ERROR row names a database whose
                   Query Store read timed out. On a one-database server that is the only row the timeout ever writes. */
                CollectorFaultFanout.Stamp(firstFailure, fanout.Result);
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(firstFailure).Throw();
            }
        }
        else
        {
            /* #5320: the provider resolved at the top of this method (the test override when one is set, TargetProviders.For
               otherwise), so the server-wide path has the same fake-reader seam the per-database path has. */
            using var targetConnection = CreateTargetConnection(server, targetProvider);
            await targetConnection.OpenAsync(cancellationToken);

            /* #5132: the target's own session id, resolved right after the open and BEFORE any reader
               exists on this connection. SQL Server's property is 0 under MARS at every point, so the id
               comes from a lookup cached per physical connection (one SELECT @@SPID per new connection,
               none on pool reuse); see TargetSessionIdCache. Best-effort: null on any failure.

               The lookup is TIMED into sqlMs (no open-phase column covers this point: ServerScopeOpenMs
               and PerItemOpenMs time only the reader opens). It stays OUTSIDE PerItemWallClockBudget on
               purpose; the lookup's own 5 s CommandTimeout bounds it. */
            var sessionIdWatch = Stopwatch.StartNew();
            Guid sessionIdKey;
            try
            {
                (context.TargetSessionId, sessionIdKey) =
                    await TargetSessionIdCache.Shared.ResolveAsync(targetConnection, _logger, cancellationToken);
            }
            finally
            {
                sqlMs += sessionIdWatch.ElapsedMilliseconds;
            }

            /* #6254: the per-server ship guard and capturing-secondary tracker, and whether this server reads Query Store from a
               replica. Set BEFORE BuildEnumerationQuery: with a replica set the primary enumeration returns a third result set. */
            var isQueryStoreRun = string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal);
            await using var qsRun = isQueryStoreRun
                ? await PrepareQueryStoreReplicaRunAsync(server, context, cancellationToken)
                : null;

            var enumerationPlan = definition.BuildEnumerationQuery(context);
            if (enumerationPlan is not null)
            {
                /* Enumeration shape (the [db].sys.sp_executesql idiom): list items first, then
                   run one query per item ON THE SAME CONNECTION; an item that fails is skipped
                   with a warning, matching Lite. */
                var listSlice = Stopwatch.StartNew();
                EnumerationOutcome enumeration;
                using (var enumerationCommand = CreateCollectorCommand(targetProvider, enumerationPlan, targetConnection, CommandTimeoutSeconds))
                using (var enumerationReader = await enumerationCommand.ExecuteReaderAsync(cancellationToken))
                {
                    /* Shared read (#1837): the item list, then the OPTIONAL second result set of items the
                       enumeration could not probe. Both hosts route through it so the item read, the
                       failure read, and the note wording cannot drift. */
                    enumeration = await EnumeratedCollectorDriver.ReadEnumerationAsync(enumerationReader, cancellationToken, context.Target.IsAwsRds);
                    if (qsRun is { Configured: true })
                    {
                        foreach (var fact in await QueryStoreCollector.ReadPrimaryFactsAsync(enumerationReader, cancellationToken))
                        {
                            qsRun.PrimaryFacts[fact.Name] = fact;
                        }
                    }
                }
                sqlMs += listSlice.ElapsedMilliseconds;

                var items = enumeration.Items;
                collectionNote = enumeration.Note;
                LogEnumerationProbeFailures(definition, server, enumeration.ProbeFailures);

                if (items.Count == 0)
                {
                    /* Nothing failed outright, so this stays SUCCESS/0 rows — but the note (the
                       empty-enumeration breadcrumb, the probe-failure summary, or both) rides onto the
                       collection_log row so it is distinguishable from a healthy collector whose databases
                       were simply quiet (#1837). Mirrors Lite's _lastCollectionNote. */
                    return new CollectorRunResult(0, sqlMs, 0, CollectorContext.NoMeasurements, enumeration.Note);
                }

                /* #6875: a collector with a pass budget reads the databases from where the last pass stopped. */
                var (passBudget, passItems) = BeginPass(definition, server.ServerId, items);
                items = passItems;

                /* Optional quick scalar probe (query_store's live PRODUCTVERSION check) —
                   best-effort on a 10-second budget; failure leaves the documented default. */
                var probeSlice = Stopwatch.StartNew();
                var probePlan = definition.BuildEnumerationProbe(context);
                if (probePlan is not null)
                {
                    try
                    {
                        using var probeCommand = CreateCollectorCommand(targetProvider, probePlan, targetConnection, 10);
                        var probeResult = await probeCommand.ExecuteScalarAsync(cancellationToken);
                        if (probeResult is not null && probeResult != DBNull.Value)
                        {
                            context.EnumerationProbeResult = probeResult;
                        }
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger?.LogDebug("Enumeration probe for {Collector} failed; using defaults: {Error}",
                            definition.Name, ex.Message);
                    }
                }
                sqlMs += probeSlice.ElapsedMilliseconds;

                if (qsRun is not null)
                {
                    /* #6254: databases that left the enumeration drop their guard entries; with a replica set, open it and read its facts. */
                    qsRun.State.Guard.RetainDatabases(items);
                    if (qsRun.Configured)
                    {
                        var replicaSlice = Stopwatch.StartNew();
                        await OpenQueryStoreReplicaAsync(qsRun, server, context, targetProvider, targetConnection, cancellationToken);
                        sqlMs += replicaSlice.ElapsedMilliseconds;
                    }
                }

                var itemTimeout = definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds;

                /* One pooled store connection for the whole body; the driver writes one binary COPY per
                   database on it, flushing each before reading the next.

                   #2819: "for the whole body" is now literally true. The Query Store plan and text fetches
                   used to open their own on every item — a third and a fourth connection per database, ~228
                   acquisitions per cycle against MaxPoolSize=24, at a measured 673-893ms floor each — and
                   they now borrow this one. Safe precisely because of the flushing order named above: the
                   driver reads an item and then awaits its write, so this connection is idle for the whole
                   read, which is when those fetches run. */
                await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);

                /* #2312: open-interval stamps STAGED at decision time (perItemWatermark, below), landed
                   into PendingState only from onItemComplete — which the driver invokes solely after the
                   item's read AND flush succeeded. Staging them straight into PendingState would let a
                   per-item fault the driver tolerates still "spend" the 15-minute refresh window for a
                   cycle that captured nothing (the review catch): PendingState flushes as long as the
                   whole run survives. Keyed per item, so one database's decision cannot land on another. */
                var stagedOpenIntervalStamps = new Dictionary<string, string>(StringComparer.Ordinal);

                /* Query Store per-database watermark contributions: staged at read, landed ONLY from
                   onItemComplete, i.e. after the item's COPY transaction committed. */
                var stagedDatabaseWatermarks = new Dictionary<string, StagedDatabaseWatermark>(StringComparer.Ordinal);

                /* #6254: per-item state of this run: the raw stored watermark (a fallback recomputes its cutoff from it), what the read
                   saw before the ship guard filtered it, and the holes a read cut by the row cap or byte budget leaves. */
                var qsRawWatermarks = new Dictionary<string, DateTime?>(StringComparer.Ordinal);
                var qsReadStats = new Dictionary<string, (int RawCount, DateTime? RawMax, bool Cut)>(StringComparer.Ordinal);
                var qsCutHoles = new Dictionary<string, (DateTime From, DateTime To)>(StringComparer.Ordinal);

            /* #6254: one database's cutoff, hole records, adaptive shrink and open-interval choice for the given routing decision
               (null when the collector has none). The body the per-item watermark step always ran, moved here so a mid-run fallback
               from the replica to the primary can run it again with the fallback's decision. */
            async Task ApplyQueryStoreCutoffAsync(string item, DateTime? raw, QueryStoreRouteDecision? decision, CancellationToken ct)
            {
            /* #6254: the cutoff decided on the RAW watermark. With the look-back (a replica read, a primary read on a server with a replica
               set, or a database with a capturing secondary) it starts at W - L and a clamped one records the hole from W - L; with no
               look-back it is today's clamp and hole exactly. */
            var cut = QueryStoreReplicaRouting.ComputeCutoff(
                raw, collectionTime, decision?.LookBack ?? TimeSpan.Zero, decision is not null && qsRun!.State.Guard.WasCut(item));
            var clamped = cut.Cutoff;
            if (cut.ClampFired)
            {
                _logger?.LogWarning(
                    "{Collector} on '{Server}' database [{Database}] catch-up clamped to {Hours}h (stored watermark {Raw:o} is older) — a bounded, logged history hole.",
                    definition.Name, server.Config.DisplayName, item, WatermarkPolicy.MaxCatchup.TotalHours, raw!.Value);

                /* #2022: the clamp opens a hole (raw, clamped) the live path will never
                   revisit — its next cutoff IS the clamped floor. Record it for the
                   backfill worker, merged wider with any hole already pending for this
                   database. Only query_store reaches this lambda today; the name guard
                   keeps a future enumeration collector from inheriting backfill state
                   it has no worker for. */
                if (cut.HoleFrom is DateTime holeFrom && cut.HoleTo is DateTime holeTo
                    && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                {
                    await RecordQueryStoreBackfillHoleAsync(server.ServerId, item, holeFrom, holeTo, ct);
                }
            }

            /* #2111 adaptive shrink (promoted from reserve on field evidence — a member
               whose 1h window intermittently exceeds the command timeout stays stuck for
               hours): after N consecutive live failures the window halves per failure
               toward 15 minutes, and the range the tighter floor skips rides the SAME
               hole records the clamp writes — deferred to the trickle, never dropped.
               Success resets the count, so a recovered member is back at full width
               next cycle. */
            var failures = ConsecutiveQueryStoreItemFailures(server.ServerId, item);
            if (failures > 0
                && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
            {
                var span = QueryStoreBackfillState.AdaptiveSpan(WatermarkPolicy.MaxCatchup, failures);
                var tighterFloor = collectionTime - span;
                if (clamped is DateTime current)
                {
                    if (current < tighterFloor)
                    {
                        _logger?.LogWarning(
                            "query_store on '{Server}' database [{Database}] adaptive catch-up shrink: {Failures} consecutive failed cycles — window narrowed to {Minutes:F0}m; the skipped range rides the backfill hole.",
                            server.Config.DisplayName, item, failures, span.TotalMinutes);
                        await RecordQueryStoreBackfillHoleAsync(server.ServerId, item, current, tighterFloor, ct);
                        clamped = tighterFloor;
                    }
                }
                else
                {
                    /* Never-succeeded database (null watermark): the definition's 60-minute
                       first-run fallback is MaxCatchup-sized, so it can be exactly the window
                       that cannot fit — tighten it the same way (the review catch on the first
                       cut, which gated shrink on a non-null watermark and left first contact
                       retrying the full width forever). No hole record: pre-watermark history
                       is the backfill TAIL's job by design. */
                    _logger?.LogWarning(
                        "query_store on '{Server}' database [{Database}] adaptive first-contact shrink: {Failures} consecutive failed cycles — first-run window narrowed to {Minutes:F0}m.",
                        server.Config.DisplayName, item, failures, span.TotalMinutes);
                    clamped = tighterFloor;
                }
            }

            context.Watermark = clamped;

            /* #2312: decide per database whether this cycle reads the OPEN interval. The
               stamp is only STAGED here — it lands in PendingState from onItemComplete,
               after this item's read and flush actually succeeded, so a per-item fault
               (which this driver swallows by design) re-includes next time instead of
               spending the refresh window on a cycle that captured nothing. Name-guarded
               like the hole records: only query_store's payload reads the flag. */
            if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
            {
                /* #6254: a replica read takes the open interval every run and stages NO qsowm stamp (a later primary fallback would
                   otherwise skip the open interval for 30 minutes on top of a 20-25 minute old snapshot); a primary read keeps the throttle. */
                var includeOpen = decision?.IncludeOpenIntervalEveryRun == true
                    || QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(context.State, item, collectionTime);
                context.IncludeOpenInterval = includeOpen;
                context.QueryStoreReadSource = decision?.Source ?? QueryStoreReadSource.Primary;
                context.QueryStoreLookBack = decision?.LookBack ?? TimeSpan.Zero;
                context.StageQueryStoreOpenIntervalStamp = decision?.StageOpenIntervalStamp ?? true;
                if (includeOpen && context.StageQueryStoreOpenIntervalStamp)
                {
                    stagedOpenIntervalStamps[QueryStoreOpenIntervalState.KeyFor(item)] =
                        QueryStoreOpenIntervalState.Format(collectionTime);
                }
                else
                {
                    stagedOpenIntervalStamps.Remove(QueryStoreOpenIntervalState.KeyFor(item));
                }
            }
            }

                /* #6254: one database's Query Store read from the given connection: the replica's, or the server's own. The body the
                   readItem step always ran, moved here so a replica read that fails can run again on the primary in the same run. */
                async Task<List<TRow>> ReadQueryStoreItemAsync(
                    string item, DbConnection readConnection, QueryStoreRouteDecision? decision, CancellationToken ct)
                {
    var batch = new List<TRow>();
    context.DropStagedItemState();
    using var itemCommand = CreateCollectorCommand(targetProvider, definition.BuildPerItemQuery(item, context), readConnection, itemTimeout);
    /* #2164: time the OPEN separately from the drain. ExecuteReaderAsync returns only
       when the first rowset is available, so for query_store's staged batch this is the
       #pm_qs_slice aggregate plus time-to-first-row — the part no client-side budget can
       shorten. Everything after is streaming, which the budget does govern. The blended
       sql: number could not tell those apart, which is why a 5x payload cut looked like
       it did nothing. */
    /* Cleared BEFORE the open so an item whose open faults cannot log the previous
       item's split as its own — a stale timing is worse than no timing. The watermark
       phase is NOT cleared here: it ran already, for THIS item, and clearing it would
       hand its milliseconds to drain. The fetch phases clear on the same rule. */
    context.PerItemOpenMs = 0;
    ClearFetchPhaseStamps(context);
    context.PerItemPhasesMeasured = false;
    /* #2854: stamped from finally, and the reader is hoisted out of the try only so the
       `using` below keeps its original disposal scope. A trailing assignment here was
       skipped whenever the open threw — a command timeout, a cancelled budget.

       Latent for the same reason as the watermark above: a throwing open never reaches
       onItemComplete, so the reading it would have produced cannot currently print. What
       it WOULD have produced is why the stamp is worth fixing regardless — DrainMsFrom
       subtracts open from the item total, so an unstamped 500s open hands its whole cost
       to drain: and blames row streaming for a statement that never returned a row. The
       flag is set in the same finally so the two can never disagree. */
    /* #5813: the per-database budget on this ONE statement, open through drain (null for every
       collector but query_store). The plan and text fetches below stay outside it. A timed-out
       read ships nothing, so this database's watermark stays where it was and the next pass
       reads from it; the driver records the item as failed and moves on to the next database. */
    using var readBudget = QueryStoreReadBudget.Start(definition.Name, QueryStoreReadBudgetSpan, ct);
    var readToken = readBudget?.Token ?? ct;
    var openWatch = Stopwatch.StartNew();
    DbDataReader? openedReader = null;
    try
    {
        try
        {
            openedReader = await itemCommand.ExecuteReaderAsync(readToken);
        }
        finally
        {
            context.PerItemOpenMs = openWatch.ElapsedMilliseconds;
            context.PerItemPhasesMeasured = true;
        }
        await definition.ReadItemAsync(item, openedReader, batch, context, readToken);
    }
    catch (Exception ex) when (ex is not OutOfMemoryException && QueryStoreReadBudget.Expired(readBudget, ct))
    {
        openedReader?.Dispose();
        throw new QueryStoreReadTimeoutException(item, QueryStoreReadBudgetSpan, ex);
    }
    catch
    {
        /* The reader keeps the scope it always had on success (the fetches below run with it
           still open); on any other fault it is closed here, as its `using` used to close it. */
        openedReader?.Dispose();
        throw;
    }
    using var itemReader = openedReader;

    /* #6254: what the read saw BEFORE the guard filters it. The tracker sees every read, the setting-empty ones included, so a
       database whose rows start arriving from a secondary gets its look-back on the next run (#6279). A read that uses the
       look-back then ships only rows new or newer than what this server already shipped for the key. */
    var rawCount = 0;
    DateTime? rawMax = null;
    if (qsRun is not null && batch is List<QueryStoreCollector.Row> qsRows)
    {
        rawCount = qsRows.Count;
        foreach (var qsRow in qsRows)
        {
            if (qsRow.LastExecutionTime is DateTime rowTime && (rawMax is null || rowTime > rawMax))
            {
                rawMax = rowTime;
            }
        }

        qsRun.State.Tracker.Observe(item, qsRows, collectionTime);
        if (decision is { UsesLookBack: true })
        {
            var shipped = qsRun.State.Guard.Filter(item, qsRows);
            qsRows.Clear();
            qsRows.AddRange(shipped);
        }
    }

    /* #2210 / #2150: this database's plan-XML and statement-text fetches, right after its
       runtime-stats read. One method, because the Azure SQL Database per-database loop runs the
       same two for each database it reads; the why and the shape are on the method. */
    await FetchQueryStorePlansAndTextAsync(
        targetProvider, readConnection, pgConnection, server, item, definition.Name, context, itemTimeout, batch, ct);

    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
    {
        stagedDatabaseWatermarks[item] = StageQueryStoreDatabaseWatermark(batch, item, collectionTime);
    }

    /* #6254: was this read cut by the row cap or the byte budget? Judged on the rows BEFORE the guard dropped any. */
    if (qsRun is not null && decision is { UsesLookBack: true })
    {
        var capHit = definition.PerItemRowCountWarnThreshold is int cap && rawCount >= cap;
        qsReadStats[item] = (rawCount, rawMax, capHit || context.PerItemTextBudgetExceeded);
    }
    else
    {
        qsReadStats.Remove(item);
    }

    return batch;

                }

                var driverResult = await EnumeratedCollectorDriver.RunAsync<TRow>(
                    items,
                    /* Per-database watermark refresh + the catch-up clamp, computed INSIDE the loop —
                       this is the per-item cutoff site the plan's LOUD FLAG requires the clamp to live at.
                       Only query_store (the sole enumeration collector with a per-database timestamp
                       watermark) reaches this; the two snapshot collectors are watermark-less. */
                    perItemWatermark: definition.PerDatabaseWatermarkColumn is null || definition.WatermarkColumn is null
                        ? null
                        : async (item, ct) =>
                        {
                            /* #2164: the driver's per-item stopwatch starts BEFORE this delegate, so the
                               watermark refresh — a STORE read, plus a store write on the clamp path below —
                               would otherwise be silently counted as row-streaming time. Measured here so
                               DrainMsFrom can subtract it; the whole point of the split is that each number
                               names one real phase. */
                            /* #2854: hoisted and stamped from finally, not assigned after the awaits below.
                               A trailing assignment is SKIPPED when an await throws, and this delegate's
                               awaits are store reads and a store write — precisely the ones #2796 found being
                               cancelled.

                               LATENT rather than live, and the distinction is worth writing down: a throwing
                               watermark refresh propagates out of the driver's per-item try, which routes to
                               onItemError, leaves `batch` null and `continue`s past onItemComplete
                               (EnumeratedCollectorDriver.cs:635-702). onItemComplete is the ONLY reader of
                               these fields, so that item prints no split line at all and the bad number never
                               reaches a log. Stamped from finally anyway, because the field's honesty should
                               not rest on a caller two files away continuing to discard the item, and because
                               the pin derives its set from CollectorContext and requires every phase stamp to
                               be handler-reachable. */
                            var watermarkWatch = Stopwatch.StartNew();
                            try
                            {
                            /* #2344: bound the read for the ONE collector whose value is clamped right
                               below. Name-guarded rather than applied to every enumerating definition,
                               for the reason WatermarkPolicy's remarks give: a ring-buffer source whose
                               legitimate catch-up spans days must keep reading its whole history, and the
                               floor would silently truncate it. The clamp and the bound travel together. */
                            var readFloor = string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                                ? WatermarkPolicy.ReadFloor(collectionTime)
                                : null;
                            var raw = readFloor is DateTime cacheFloor
                                ? await ResolveQueryStoreDatabaseWatermarkAsync(
                                    server, definition.TargetTable, definition.WatermarkColumn!,
                                    definition.PerDatabaseWatermarkColumn!, item, cacheFloor, collectionTime, ct)
                                : await GetLastCollectedTimeForDatabaseAsync(
                                    server.ServerId, definition.TargetTable, definition.WatermarkColumn!,
                                    definition.PerDatabaseWatermarkColumn!, item, ct, readFloor);
                            /* #6254: the routing decision for this database (null for every other collector), then the cutoff, hole
                               records, adaptive shrink and open-interval choice it implies, which a mid-run fallback to the primary
                               repeats with its own decision (ApplyQueryStoreCutoffAsync). */
                            QueryStoreRouteDecision? decision = null;
                            if (qsRun is not null)
                            {
                                decision = qsRun.Decide(item, collectionTime);
                                qsRawWatermarks[item] = raw;
                            }

                            await ApplyQueryStoreCutoffAsync(item, raw, decision, ct);
                            }
                            finally
                            {
                                context.PerItemWatermarkMs = watermarkWatch.ElapsedMilliseconds;
                            }
                        },
                    readItem: async (item, ct) =>
                    {
                        var decision = qsRun?.CurrentDecision(item);
                        if (decision?.Source == QueryStoreReadSource.Replica && qsRun!.Connection is { } replicaConnection)
                        {
                            try
                            {
                                return await ReadQueryStoreItemAsync(item, replicaConnection, decision, ct);
                            }
                            catch (Exception ex) when (!ct.IsCancellationRequested && ex is not OutOfMemoryException)
                            {
                                /* #6254: one database's replica read failed (a timeout included): it reads the primary in this same run,
                                   reason replica_unavailable, and the failure counts toward no shrink and no yield. */
                                _logger?.LogWarning(
                                    "query_store on '{Server}' database [{Database}]: the replica read failed ({Message}); reading the primary this run.",
                                    server.Config.DisplayName, item, ex.Message);
                                decision = qsRun.FallBack(item, collectionTime);
                                await ApplyQueryStoreCutoffAsync(item, qsRawWatermarks.GetValueOrDefault(item), decision, ct);
                                qsRun.State.Guard.Discard(item);
                            }
                        }

                        return await ReadQueryStoreItemAsync(item, targetConnection, decision, ct);
                    },
                    writeBatch: (batch, ct) => WriteBatchAsync(pgConnection, definition, batch, server, collectionTime, context, ct),
                    onItemComplete: (item, batchCount, itemSqlMs, itemStorageMs) =>
                    {
                        /* #2472: the per-database cost the blended collection_log row cannot carry. Counted
                           for every completed item, including the quiet ones the log line below skips —
                           their read time is in the blended total, so leaving them out would inflate the
                           dominance ratio of whichever database happened to have rows. */
                        fanout.Observe(item, itemSqlMs + itemStorageMs);

                        /* #2860: and this item's FETCH split into the run's sums, for every completed item
                           rather than only the ones the line below prints. That line is gated on
                           batchCount > 0, but a quiet database can still pay real fetch time — #2902's
                           carryover means an empty-batch pass may drain ids deferred from an earlier one —
                           so gating the sums the same way would drop exactly the time hardest to attribute.
                           The accumulator reads the PerItem* members, which are still live here: they clear
                           at the START of the next item's read, not at the end of this one. */
                        fetchPhases.Observe(context);

                        /* #2111: a completed item resets the adaptive-shrink count — recovery returns
                           the member to the full catch-up width on its next cycle. */
                        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                        {
                            /* #6254: a replica read vouches for nothing about the primary's window, so only a primary read resets the count. */
                            if (qsRun?.CurrentDecision(item) is not { FailureCountsTowardShrink: false })
                            {
                                OnQueryStoreItemSucceeded(server.ServerId, item);
                            }

                            if (stagedDatabaseWatermarks.Remove(item, out var stagedWatermark))
                            {
                                CommitQueryStoreDatabaseWatermark(server, item, stagedWatermark);
                            }

                            /* #2312: NOW the open-interval stamp may land — this hook only fires after
                               the item's read and flush both succeeded. Remove, not read: a stamp left
                               staged (read faulted) must not leak into a later run's landing. */
                            if (stagedOpenIntervalStamps.Remove(QueryStoreOpenIntervalState.KeyFor(item), out var landedStamp))
                            {
                                context.PendingState[QueryStoreOpenIntervalState.KeyFor(item)] = landedStamp;
                            }

                            /* #6254: the write succeeded, so the ship guard keeps what this look-back read shipped. A read the row cap or
                               byte budget cut starts its next read at W with no look-back, and the hole it leaves (W - L, W) is recorded
                               once the run is done; the next read after that one clears the mark. */
                            if (qsRun?.CurrentDecision(item) is { UsesLookBack: true } shippedDecision)
                            {
                                var readStats = qsReadStats.GetValueOrDefault(item);
                                qsRun.State.Guard.Commit(
                                    item, readStats.RawMax ?? qsRawWatermarks.GetValueOrDefault(item), shippedDecision.LookBack);
                                qsRun.State.Guard.MarkCut(item, readStats.Cut);
                                if (readStats.Cut && readStats.RawMax is DateTime cutWatermark)
                                {
                                    qsCutHoles[item] = QueryStoreReplicaRouting.CutHole(cutWatermark, shippedDecision.LookBack);
                                }
                            }
                        }

                        /* Enumerated items stage no definition state today; landed here so the rule holds for
                           any that start to: this hook fires only after the item's read and flush succeeded. */
                        context.LandStagedItemState();

                        /* Per-DATABASE line for non-empty batches (#1565): the per-server summary blends
                           every database into one number, which hid a single busy database's 50s burst
                           behind four quiet siblings. Quiet databases (0 rows — the 2-of-3 cycles between
                           Query Store's 900s flushes) stay silent.

                           #3102: every line in this block is Debug, below the default filter. This is the
                           branch that multiplies — one line per DATABASE per cycle, and two or three of
                           them for a collector that fetches plans or text — so it is where the level
                           decision is worth the most and where a per-cycle timing on the default log
                           displaces the errors the log is opened for. Suppressed, not removed: the whole
                           block returns at Debug (Darling/README.md, "Logs"). */
                        if (batchCount > 0)
                        {
                            /* #2164: open vs drain, because they have different fixes. A pass that is nearly
                               all OPEN is bound by server-side work before the first row (for query_store,
                               the #pm_qs_slice aggregate) and no client-side budget or payload trimming will
                               touch it; a pass that is mostly drain is bound by moving rows, where the byte
                               budget and the link are the levers. Only emitted when the host measured it.

                               #2854: the gate is the FLAG, not `PerItemOpenMs > 0`. That form conflated a
                               genuinely instant open with a path that measured nothing, and suppressed the
                               whole split for the former — a real loss, because a fast open is exactly the
                               item whose drain: number is worth reading. No value alone can separate those
                               two states; a flag set beside the stamp can. */
                            if (context.PerItemPhasesMeasured)
                            {
                                /* #2312: the fetch phases print only when a separate fetch actually ran,
                                   so every other collector's line is byte-identical to before. */
                                if (context.PerItemPlanFetchMs > 0 || context.PerItemTextFetchMs > 0)
                                {
                                    _logger?.LogDebug("  [{Server}] {Collector} [{Database}] => {Rows} rows (sql:{SqlMs}ms = wm:{WatermarkMs}ms + open:{OpenMs}ms + drain:{DrainMs}ms + plan_fetch:{PlanFetchMs}ms + text_fetch:{TextFetchMs}ms, pg:{PgMs}ms)",
                                        server.Config.DisplayName, definition.Name, item, batchCount, itemSqlMs,
                                        context.PerItemWatermarkMs, context.PerItemOpenMs, context.DrainMsFrom(itemSqlMs),
                                        context.PerItemPlanFetchMs, context.PerItemTextFetchMs, itemStorageMs);

                                    /* #2811: the sub-splits ride their OWN lines, emitted by the method the Azure per-database
                                       loop shares - see LogFetchPhaseSplits for why they are not folded into the line above. */
                                    LogFetchPhaseSplits(server, definition.Name, item, context);
                                }
                                else
                                {
                                    _logger?.LogDebug("  [{Server}] {Collector} [{Database}] => {Rows} rows (sql:{SqlMs}ms = wm:{WatermarkMs}ms + open:{OpenMs}ms + drain:{DrainMs}ms, pg:{PgMs}ms)",
                                        server.Config.DisplayName, definition.Name, item, batchCount, itemSqlMs,
                                        context.PerItemWatermarkMs, context.PerItemOpenMs, context.DrainMsFrom(itemSqlMs), itemStorageMs);
                                }
                            }
                            else
                            {
                                _logger?.LogDebug("  [{Server}] {Collector} [{Database}] => {Rows} rows (sql:{SqlMs}ms, pg:{PgMs}ms)",
                                    server.Config.DisplayName, definition.Name, item, batchCount, itemSqlMs, itemStorageMs);
                            }
                        }

                        var capHit = definition.PerItemRowCountWarnThreshold is int cap && batchCount >= cap;
                        if (capHit || context.PerItemTextBudgetExceeded)
                        {
                            _logger?.LogWarning(
                                "{Collector} on '{Server}' database [{Database}] hit its per-database collection bound ({Reason}) — shipped {ShippedMB:F1}MB up to {Boundary}; the backlog resumes from that boundary next cycle.",
                                definition.Name, server.Config.DisplayName, item,
                                capHit ? $"row cap {definition.PerItemRowCountWarnThreshold}" : "text byte budget",
                                context.PerItemTextBytesShipped / (1024.0 * 1024.0),
                                context.PerItemShippedBoundary?.ToString("o") ?? "n/a");
                        }
                    },
                    onItemError: (item, ex) =>
                    {
                        /* #2111: stamp the yield-to-live signal (any database's live failure vouches
                           for the whole replica being contended) + the per-database adaptive-shrink
                           count. */
                        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                        {
                            /* #6254: a failed replica read counts toward no #2111 shrink and no yield; and whatever the guard staged for
                               this database is dropped, so its rows ship next run. */
                            if (qsRun?.CurrentDecision(item) is not { FailureCountsTowardShrink: false })
                            {
                                OnQueryStoreItemFailed(server.ServerId, item);
                            }

                            qsRun?.State.Guard.Discard(item);
                            DiscardQueryStoreDatabaseWatermark(server, item, stagedDatabaseWatermarks);
                        }

                        /* #5813: a read that ran out of its budget is the run's slowest item under its plain name,
                           and warns once per database per UTC day rather than on every pass. */
                        if (NoteQueryStoreReadTimeout(server, ex, fanout))
                        {
                            return;
                        }

                        _logger?.LogWarning("Failed to collect {Collector} from [{Database}] on '{Server}': {Message}",
                            definition.Name, item, server.Config.DisplayName, ex.Message);
                    },
                    cancellationToken,
                    /* #2150: the per-database wall-clock ceiling, straight off the definition, so this
                       argument leaves a cycle whose collector declares none exactly as it was.
                       Not "every collector but query_store", which is what this said: four definitions
                       declare a budget and two of them also enumerate, so plan_correction arrives here
                       non-null too. */
                    perItemBudget: definition.PerItemWallClockBudget,
                    passBudget: passBudget);

                /* #3754: the enumerated fan-out's failure account, treated exactly as the Azure per-database
                   loop above treats its own (#2623) - and for the first time. Until this the driver handed
                   each per-item exception to onItemError, that closure logged one WARNING per database,
                   and nothing else happened: a collector whose every item threw returned Rows = 0 and this
                   method returned SUCCESS with zero rows. On Azure SQL DB database_scoped_config did that on
                   every monitored database every sweep - the [db].sys.sp_executesql idiom is rejected there -
                   and get_collection_health reported it HEALTHY with errors: 0 and a sentence saying it had
                   read and found nothing, while the service log carried the real failure on every cycle.

                   Same two outcomes as the sibling loop, from the driver's shared numbers rather than a
                   second hand-rolled count: SOME items lost composes the partial note (the names, the
                   count, the first error) onto the SUCCESS row, merged with whatever probe-failure note the
                   enumeration already left; EVERY item lost rethrows the first failure so RunOneAsync
                   classifies the run from the real exception (PERMISSIONS on a denial number, ERROR
                   otherwise) - a note on a row about to carry an error message would only compete with it,
                   which is why PartialFailureNote composes null in that case. The database name rides ON the
                   exception (#2997) so the fault handler upstream can name the database it means rather
                   than the runtime's connected one, which for an enumerated collector is never the one that
                   failed. rowsWritten and the slices are assigned only on the surviving path: a run that
                   read nothing has no rows to report and RunOneAsync's fault arm reports its own wall clock. */
                collectionNote = EnumeratedCollectorDriver.MergeNotes(collectionNote, driverResult.PartialFailureNote);
                collectionNote = EnumeratedCollectorDriver.MergeNotes(
                    collectionNote,
                    EndPass(passBudget, definition, server.ServerId, driverResult.StoppedAfter, driverResult.Attempted, driverResult.Total));

                if (driverResult.AllItemsFailed)
                {
                    LogFailedInAllDatabases(server, definition.Name, driverResult.Attempted, driverResult.FirstError);
                    CollectorFaultDatabase.Stamp(driverResult.FirstError, driverResult.FailedItems?[0]);
                    CollectorFaultFanout.Stamp(driverResult.FirstError, fanout.Result); // #5813, as the Azure loop's rethrow
                    System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(driverResult.FirstError!).Throw();
                }

                rowsWritten = driverResult.Rows;
                sqlMs += driverResult.SqlMs;
                storageMs += driverResult.StorageMs;

                if (qsRun is not null)
                {
                    foreach (var (cutDatabase, cutHole) in qsCutHoles)
                    {
                        await RecordQueryStoreBackfillHoleAsync(server.ServerId, cutDatabase, cutHole.From, cutHole.To, cancellationToken);
                    }

                    queryStoreReadSource = QueryStoreReplicaRouting.Summarize(qsRun.Decisions, qsRun.Configured);
                }
            }
            else
            {
                /* Plain single-query path (server-scoped): read all rows, then write them in one batch
                   (supplemental never runs for per-database collectors). Routed through WriteBatchAsync
                   so all three paths share one writer.

                   #2673: the primary read + DRAIN is bounded by the collector's PerItemWallClockBudget
                   (one item = the whole server here). The 60s per-command timeout covers only EXECUTION,
                   not the drain of a large result set, so a heavy server-scoped collector (procedure_stats,
                   query_stats) could occupy a monitored server for minutes — the exact profile we must never
                   present. Null budget = itemToken IS cancellationToken and this block is byte-for-byte what
                   it was. */
                /* #4046 part 1c: resolved on the connection that is about to run the query, BEFORE
                   BuildQuery decides which of the two tail routes to send — a target's grant can arrive
                   with no reconnect, so this is checked (through its own hourly cache) independently of
                   how long the connection has been open, rather than folded into the connect-time facts on
                   CollectorTargetInfo that live for the connection's whole life. */
                await ResolvePgReadBinaryFileGrantAsync(
                    context, definition.Name, targetConnection, server, cancellationToken);
                /* #4251: same shape, same connection, right beside the grant check above — pg_server_config
                   is the other server-scoped PostgreSQL collector that must decide which of two query texts
                   to send before BuildQuery runs. */
                await ResolvePgFileSettingsReadableAsync(
                    context, definition.Name, targetConnection, server, _logger, cancellationToken);
                /* #6143: same connection, same point: the schema of each extension the collector reads. */
                await ResolvePgExtensionSchemasAsync(context, definition, targetConnection, _logger, cancellationToken);

                var sqlSlice = Stopwatch.StartNew();
                var plan = definition.BuildQuery(context);
                List<TRow> rows;
                List<QueryStatsPlanKey>? pendingPlanKeys = null;
                List<ProcedureStatsPlanKey>? pendingProcedureKeys = null;
                /* #2851: this branch IS the server-scoped path, so its phases are measured from here on.
                   Set before the read rather than after it so the wall-clock-budget catch below reports the
                   phases of the cycle it abandoned — that is the case where "where did the time go" matters
                   most, and the one a trailing assignment would leave at zero. */
                serverPhasesMeasured = true;

                using var itemBudget = EnumeratedCollectorDriver.StartItemBudget(definition.PerItemWallClockBudget, cancellationToken);
                var itemToken = itemBudget?.Token ?? cancellationToken;

                /* #2880: the out-of-band arm. Started BEFORE the open and on its own clock, because the
                   forensics found the degradation presents at ExecuteReaderAsync as well as in the drain —
                   the four cheapest collectors' open_ms ran 3x to 152x their own baselines in the body
                   before each abandoned run — so a watchdog scoped to the drain alone would miss the phase
                   that names the cause. Inert for every collector that declares no wall-clock budget and for
                   every non-SQL-Server target; see StallProbeArm.Start.

                   The counting reader is read from the arm's thread without synchronisation, deliberately.
                   The hazard here is STALENESS, not tearing: a 64-bit read is atomic on every architecture
                   this ships to, so the arm cannot observe half of a counter — it can only observe an
                   earlier value of one, which mis-states a diagnostic figure by at most the rows read since
                   the cache line was last seen. Putting a lock, or a volatile write, in the hot read loop of
                   66 collectors to serve a once-per-stall sample would be the wrong trade by a wide margin.

                   Both stale readings fail in the CONSERVATIVE direction, which is what makes the trade safe
                   rather than merely cheap: a lagging byte count reads the stream as SLOWER than it is, and a
                   not-yet-visible reader reference reads as -1, which Decide treats as "still executing".
                   Neither can talk the probe out of firing on a target that has earned one. */
                var readWatch = Stopwatch.StartNew();
                DrainCountingDataReader? countingForProbe = null;
                using var stallProbeArm = StallProbeArm.Start(
                    server.Target.Engine,
                    definition.PerItemWallClockBudget,
                    observe: () => new StallProbeObservation(
                        readWatch.ElapsedMilliseconds,
                        countingForProbe?.RowsRead ?? -1,
                        countingForProbe?.PayloadBytes ?? -1,
                        countingForProbe?.LastReadElapsedMs ?? -1),
                    fire: observation => StallWaitProbeRunner.RunAsync(
                        _postgres,
                        server,
                        definition.Name,
                        definition.PerItemWallClockBudget!.Value,
                        observation,
                        _logger,
                        cancellationToken),
                    onDecision: decision => _logger?.LogDebug(
                        "Stall probe for {Collector} on '{Server}': {Verdict} — {Reason} (#2880)",
                        definition.Name, server.Config.DisplayName,
                        decision.Fire ? "firing" : "not firing", decision.Reason),
                    cancellationToken: cancellationToken);

                try
                {
                    using var command = CreateCollectorCommand(targetProvider, plan, targetConnection, definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds);

                    /* Both phases stamp from finally rather than after the await (#2816). A throwing open or
                       a drain cut short by the budget must still report the time it burned; the alternative
                       is that its milliseconds land in the residual, and the residual is the one term whose
                       whole job is to be small and unattributed. 97% of a day's residual budget was one such
                       misattribution the last time this was got wrong. */
                    var openWatch = Stopwatch.StartNew();
                    DbDataReader opened;
                    try
                    {
                        opened = await command.ExecuteReaderAsync(itemToken);
                    }
                    finally
                    {
                        context.ServerScopeOpenMs = openWatch.ElapsedMilliseconds;
                    }

                    using var reader = opened;

                    /* A transparent reconnect during the open gives a new physical connection (new
                       ClientConnectionId, new session); the id resolved at open then belongs to the old
                       one. Property comparison only. */
                    if (targetConnection is SqlConnection openedSql
                        && TargetSessionIdCache.Shared.DiscardIfReplaced(sessionIdKey, openedSql.ClientConnectionId))
                    {
                        context.TargetSessionId = null;
                    }

                    var drainWatch = Stopwatch.StartNew();

                    /* #2864: the collector reads through a counting decorator rather than the provider reader
                       directly, so what the drain DELIVERED is recorded whether or not the cycle survives to
                       store it. An abandoned cycle ships nothing, so rows_collected is 0 either way and could
                       never separate "the target never sent row 1" from "it sent 149 and stopped" — the first
                       production capture with V108's phases read open:104ms drain:119,945ms rows=0 and could
                       go no further. Decorating rather than editing 66 collectors' read loops means a
                       collector cannot forget to count, and the counters ride the SAME drainWatch so the
                       last-read reading and the drain figure it is subtracted from share one clock. */
                    var counting = new DrainCountingDataReader(reader, drainWatch);

                    /* #2880: hand the arm the live counters. Assigned here rather than passed in because the
                       arm is started before the open, where this reader does not exist yet — until then the
                       arm observes -1, which Decide reads as "still executing" and fires on, since a read
                       still inside ExecuteReaderAsync at a quarter of its budget is exactly what is worth a
                       sample. */
                    countingForProbe = counting;

                    try
                    {
                        rows = await definition.ReadAsync(counting, context, itemToken);
                    }
                    finally
                    {
                        context.ServerScopeDrainMs = drainWatch.ElapsedMilliseconds;

                        /* Stamped from finally with the drain itself, and for the same #2816 reason: the run
                           that matters most here is the one cut short, so a count only recorded on the
                           success path would be absent from exactly the cycles it exists to explain. */
                        context.ServerScopeRowsRead = counting.RowsRead;
                        context.ServerScopeBytesRead = counting.PayloadBytes;
                        context.ServerScopeLastReadMs = counting.LastReadElapsedMs;
                    }

                    /* #1851: a definition that declares it may hand back an OPTIONAL trailing
                       (item_name, error_text) result set naming items its own server-side cursor
                       reached but could not probe — database_size_stats' mid-restore / inaccessible
                       databases, which used to vanish into an empty CATCH. Read through the SAME
                       shared machinery as the enumeration path's failures (#1837), so the note wording
                       and the log cap cannot drift between the two channels or between the two hosts.
                       Read HERE, still inside the reader, and before the storage phase below: it
                       touches only the note, never `rows`, so the payload and its delta ordering are
                       exactly what they were. */
                    if (definition.EmitsProbeFailures)
                    {
                        var probes = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, itemToken, context.Target.IsAwsRds);
                        collectionNote = EnumeratedCollectorDriver.MergeNotes(collectionNote, probes.Note);
                        LogEnumerationProbeFailures(definition, server, probes.ProbeFailures);
                    }

                    /* #2880: disarm the moment the READ is over rather than when this branch ends.
                       The `using` above guarantees disposal on the exception paths, but it would not
                       run until after the storage and watermark phases — long enough for a probe to
                       fire against a target the sweep has already stopped reading from, and to record
                       trigger evidence that included time no reader was waiting on. Dispose is
                       idempotent for exactly this pairing. */
                    stallProbeArm.Dispose();

                    /* #5449: procedure_stats' plan phase. The main query is numbers only, so a host that captures plans or
                       reads identities asks the target for them in a second query over the first MaxPlansPerRun kept rows,
                       here: on this same target connection and inside the same wall-clock budget (itemToken), before the
                       #5158 reuse pass below reads the identities it fills. Lite never reaches this: it captures no plans. */
                    var procedureStatsPlanPhaseMs = 0L;
                    if ((object)rows is List<ProcedureStatsCollector.Row> phaseRows
                        && ProcedureStatsCollector.PlanPhaseApplies(context))
                    {
                        procedureStatsPlanPhaseMs = await RunProcedureStatsPlanPhaseAsync(
                            targetProvider, reader, targetConnection, server, context, phaseRows, itemToken);
                    }

                    /* #6009: the module phase. For the modules a pending deploy follow-up watches and nowhere else, a second small
                       query reads modify_date on this same target connection and inside the same budget. With nothing watched it
                       sends nothing, and the plan reuse pass below runs exactly as before. */
                    if ((object)rows is List<ProcedureStatsCollector.Row> moduleRows && moduleRows.Count > 0)
                    {
                        await RunProcedureStatsModulePhaseAsync(
                            targetProvider, reader, targetConnection, server, context, moduleRows, null, itemToken);
                    }

                    /* #5158: the deferred plan fetch. After the main read is over and on this same target connection,
                       inside the same wall-clock budget (itemToken), so a slow fetch is cut by the budget like the read. */
                    if (context.DeferPlanXmlFetch && (object)rows is List<QueryStatsCollector.Row> deferredRows)
                    {
                        await reader.CloseAsync();
                        pendingPlanKeys = await FetchDeferredQueryStatsPlansAsync(
                            targetProvider, targetConnection, server, context, deferredRows, itemToken);
                    }
                    else if (context.PlanIdentityColumns && (object)rows is List<ProcedureStatsCollector.Row> identityRows)
                    {
                        /* The run's mode, read back from the stamp: the knob and the capture setting are not asked again. */
                        var procedureStatsMode = ProcedureStatsPlanFetchModes.OfRun(context);
                        if (procedureStatsMode == ProcedureStatsPlanFetchMode.On)
                        {
                            await reader.CloseAsync();
                        }

                        pendingProcedureKeys = await ApplyProcedureStatsPlanReuseAsync(
                            targetProvider, targetConnection, server, context, identityRows, procedureStatsMode, itemToken,
                            procedureStatsPlanPhaseMs);
                    }
                    else if (procedureStatsPlanPhaseMs > 0 && (object)rows is List<ProcedureStatsCollector.Row> offRows)
                    {
                        /* Off mode renders the plans in the plan phase and has no reuse pass: stamp what it rendered. */
                        StampProcedureStatsOffPlanPhase(context, offRows, procedureStatsPlanPhaseMs);
                    }
                }
                catch (Exception ex) when (EnumeratedCollectorDriver.ItemBudgetExpired(itemBudget, cancellationToken))
                {
                    /* #2673: this server-scoped collector blew its wall-clock budget mid read/drain. Abandon
                       the whole cycle WITHOUT advancing any watermark — ship nothing, retry next cycle with
                       the same cutoff — so no single collector runs minutes on a target. Returning here skips
                       the storage phase AND the state-persistence block below, which is what keeps the
                       watermark from moving. The provider's cancellation artifact (ex) is dropped in favour
                       of the budget message, the same as the per-item arm. */
                    _ = ex;
                    var budgetSeconds = (int)definition.PerItemWallClockBudget!.Value.TotalSeconds;
                    _logger?.LogWarning(
                        "{Collector} on '{Server}' reached its {Budget}s wall-clock budget mid-collection — abandoned this cycle, will retry next (#2673).",
                        definition.Name, server.Config.DisplayName, budgetSeconds);
                    return new CollectorRunResult(
                        0,
                        sqlSlice.ElapsedMilliseconds,
                        0,
                        /* Deliberately NOT context.Measurements: this cycle stored nothing and advanced no
                           watermark, so a partial count from the slice that ran before the budget fired
                           would describe a read that was thrown away. "12 read, 0 stored" on an abandoned
                           run reads as a collection fault, and the cause is a timeout. */
                        CollectorContext.NoMeasurements,
                        EnumeratedCollectorDriver.WholeCycleBudgetNote(budgetSeconds),
                        Abandoned: true,
                        /* #2851: the abandoned cycle reports its phases too. A collector that blew a
                           wall-clock budget is precisely the one worth asking "on what" — and because the
                           stamps come from finally above, the phase it died IN is reported rather than lost. */
                        ServerPhasesMeasured: true,
                        ServerOpenMs: context.ServerScopeOpenMs,
                        ServerDrainMs: context.ServerScopeDrainMs,
                        ServerWatermarkMs: serverWatermarkMs,
                        /* #2864, and THIS is the arm the counters were added for: an abandoned cycle stores
                           nothing, so rows_collected is 0 either way and cannot say whether the target sent
                           nothing or sent rows and then stopped. The counting reader stamps from finally, so
                           what arrived before the budget fired survives the abandon. */
                        ServerRowsRead: context.ServerScopeRowsRead,
                        ServerBytesRead: context.ServerScopeBytesRead,
                        ServerLastReadMs: context.ServerScopeLastReadMs,
                        TargetSessionId: context.TargetSessionId);
                }

                /* Optional best-effort second query on the same connection (server_properties'
                   health probe). Failure-isolated; skipped on an empty primary, matching Lite. */
                var supplementalPlan = definition.BuildSupplementalQuery(context);
                if (supplementalPlan is not null && rows.Count > 0)
                {
                    try
                    {
                        using var supplementalCommand = CreateCollectorCommand(targetProvider, supplementalPlan, targetConnection, CommandTimeoutSeconds);
                        using var supplementalReader = await supplementalCommand.ExecuteReaderAsync(cancellationToken);
                        await definition.ApplySupplementalAsync(rows, supplementalReader, context, cancellationToken);
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogDebug(ex, "Supplemental query for {Collector} failed; continuing without it", definition.Name);
                    }
                }
                sqlMs += sqlSlice.ElapsedMilliseconds;

                var storageSlice = Stopwatch.StartNew();
                await using var pgConnection = await _postgres.OpenConnectionAsync(cancellationToken);
                var committed = false;
                try
                {
                    rowsWritten = await WriteBatchAsync(pgConnection, definition, rows, server, collectionTime, context, cancellationToken);
                    committed = true;
                }
                finally
                {
                    /* #5158: a plan this run rendered counts as stored only once its batch committed (a successful
                       StoreWriteReattempt included). A batch that failed leaves nothing cached, so the next run
                       renders those plans again rather than sending a digest the store never received. */
                    if (pendingPlanKeys is not null
                        && _queryStatsPlanCaches.TryGetValue(server.ServerId, out var planCache))
                    {
                        if (committed)
                        {
                            planCache.ConfirmPending(pendingPlanKeys, DateTime.UtcNow);
                        }
                        else
                        {
                            planCache.DiscardPending(pendingPlanKeys);
                        }
                    }

                    /* Two runs of one server's procedure_stats never overlap, so no other run can overwrite a pending entry
                       between this run's add and this confirm (ConfirmPending trusts the entry it finds under the key):
                       - a non-daily run is awaited inside the server's sweep body, under the per-server CollectionGate
                         (DarlingWorker.cs:12146), and a snapshot_now takes the same gate (DarlingWorker.cs:12430);
                       - the sweep launches one body per server at a time (DarlingWorker.cs:3358);
                       - a run an override makes daily goes through the per-(server, collector) DetachedCollectorGate
                         (DarlingWorker.cs:13799), and the at-connect and snapshot runs take its slot first
                         (TryTakeInlineDailySlot, DarlingWorker.cs:5639). */
                    if (pendingProcedureKeys is not null
                        && _procedureStatsPlanCaches.TryGetValue(server.ServerId, out var procedurePlanCache))
                    {
                        if (committed)
                        {
                            procedurePlanCache.ConfirmPending(pendingProcedureKeys, DateTime.UtcNow);
                        }
                        else
                        {
                            procedurePlanCache.DiscardPending(pendingProcedureKeys);
                        }
                    }
                }

                storageMs += storageSlice.ElapsedMilliseconds;

                /* The single item's write returned: land what the definition staged for it. A throw above
                   never reaches here, and a cycle that throws saves no state at all. */
                context.LandStagedItemState();

                /* #4197 part b: advance the cache only AFTER WriteBatchAsync's COPY has returned — this
                   plain (non-fan-out) path's write is the batch commit itself, so "after the write
                   returns" IS "after commit" here, never before it. A run that wrote zero rows takes
                   neither branch below and leaves the cache exactly as the seed/hit above left it,
                   matching the definition's own "no row-derived watermark from an empty batch" rule
                   (#1962) instead of inventing one. Only eligible definitions (a live WatermarkValueAccessor,
                   not a fan-out path) advance — see watermarkCacheEligible above. */
                if (watermarkCacheEligible && rowsWritten > 0)
                {
                    CommitServerWatermark(server, definition, rows, watermarkFromUtcColumn, context.Measurements);
                }
            }
        }

        /* Persist what the definition observed, AFTER the cycle completed — including a cycle that wrote
           zero rows, which is exactly the case a row-derived watermark cannot cover (#1962). A cycle that
           threw never reaches here, so the older state survives and the next run takes its conservative
           path. Outside the storage-phase timer: this is host bookkeeping, not collected data. */
        if (context.PendingState.Count > 0)
        {
            /* #2312: query_store's pending state is down to ONE family — the open-interval refresh stamps
               (qsowm:), which belong to the host's own state owner rather than the definition's name (the
               definition declares no state keys, so a row written under "query_store" would never be read
               back). The plan/text watermark families that used to be split out here retired with the
               watermarks themselves; the split-by-prefix survives only as the qsowm: extraction, so a
               future fourth family cannot silently land under the wrong owner and become unprunable. */
            var openIntervalKeys = context.PendingState
                .Where(entry => entry.Key.StartsWith(QueryStoreOpenIntervalState.WatermarkKeyPrefix, StringComparison.Ordinal))
                .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal);

            if (openIntervalKeys.Count > 0)
            {
                await SaveCollectorStateAsync(
                    server.ServerId, QueryStoreOpenIntervalState.StateCollectorName, openIntervalKeys, cancellationToken);

                var others = context.PendingState
                    .Where(entry => !openIntervalKeys.ContainsKey(entry.Key))
                    .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal);
                if (others.Count > 0)
                {
                    await SaveCollectorStateAsync(server.ServerId, definition.Name, others, cancellationToken);
                }
            }
            else
            {
                await SaveCollectorStateAsync(server.ServerId, definition.Name, context.PendingState, cancellationToken);
            }
        }

        _logger?.LogDebug("Collected {RowCount} {Collector} rows for server '{Server}'",
            rowsWritten, definition.Name, server.Config.DisplayName);
        /* #2864: the drain forensics are recorded on the SUCCESS path too, not only on abandon. The
           abandoned row is the one being explained, but a ratio needs a denominator - "149 rows, last read
           at 500 ms" only reads as pathological against what this collector on this server normally
           delivers, and that baseline has to come from the same column on ordinary rows. Storing it only on
           failures would reproduce exactly the cross-referencing this change exists to remove.

           Kept ABOVE the return rather than inside the argument list, so the call reads as one run. The note
           reaching it is covered by DarlingRunnerCollectionNoteLiveTests, which drives RunAsync (#6356). */
        /* #3099: merged rather than assigned, and read HERE — one place, after every write on all three
           dispatch paths — because the count is a per-cycle total and the fan-out paths write once per
           item. Merging keeps a re-attempt from displacing the probe-failure or partial-database note it
           can legitimately co-occur with; MergeNotes composes null away, so an ordinary cycle carries
           exactly the note it carried before. */
        collectionNote = EnumeratedCollectorDriver.MergeNotes(
            collectionNote, StoreWriteReattemptNote(context.StoreWriteReattempts));
        collectionNote = EnumeratedCollectorDriver.MergeNotes(
            collectionNote, QueryStoreIntervalMissNote(context.QueryStoreIntervalMisses));

        return new CollectorRunResult(
            rowsWritten, sqlMs, storageMs, context.Measurements, collectionNote, fanout.Result,
            ServerPhasesMeasured: serverPhasesMeasured,
            ServerOpenMs: context.ServerScopeOpenMs,
            ServerDrainMs: context.ServerScopeDrainMs,
            ServerWatermarkMs: serverWatermarkMs,
            ServerRowsRead: context.ServerScopeRowsRead,
            ServerBytesRead: context.ServerScopeBytesRead,
            ServerLastReadMs: context.ServerScopeLastReadMs,
            TargetSessionId: context.TargetSessionId,
            FetchPhases: fetchPhases.Result,
            ReadSource: queryStoreReadSource);
    }
}
