/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /// <summary>
    /// Runs a shared collector definition (PerformanceMonitor.Collectors) against one server:
    /// SQL phase (definition reads/filters rows) and storage phase (appender write with the
    /// standard prefix columns) are timed separately, preserving the #1180 fetch-side metrics.
    /// Collectors migrate onto this runner one PR at a time (headless plan v5.1); it reproduces
    /// the hand-rolled per-collector loop byte-for-byte at the storage layer.
    /// </summary>
    private async Task<int> RunCollectorDefinitionAsync<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerConnection server,
        CancellationToken cancellationToken)
    {
        var serverId = GetServerId(server);
        var collectionTime = DateTime.UtcNow;

        /* This server's slot, not a shared field: servers collect in parallel (see RunTelemetry). */
        var telemetry = TelemetryFor(serverId);
        telemetry.SqlMs = 0;
        telemetry.StorageMs = 0;
        telemetry.Note = null;
        telemetry.Fanout = null;

        /* The per-database rollup (#2472), fed by both fan-out shapes — the Azure per-database connection
           loop and the enumeration driver's onItemComplete hook — through the same shared accumulator the
           Darling runner uses, so the two SKUs cannot come to disagree about what a slow database is. */
        var fanout = new FanoutCostAccumulator();

        var status = _serverManager.GetConnectionStatus(server.Id);
        var target = new CollectorTargetInfo
        {
            IsAzureSqlDb = status.SqlEngineEdition == 5,
            IsAzureManagedInstance = status.SqlEngineEdition == 8,
            IsAwsRds = status.IsAwsRds,
            SqlMajorVersion = status.SqlMajorVersion,
            HasMsdbAccess = status.HasMsdbAccess,
        };

        /* Some collectors don't exist on some targets (e.g. ring buffers on Azure SQL DB) —
           skip the cycle entirely, matching the original hand-rolled collectors. */
        if (!CollectorCatalog.AppliesTo(definition, target))
        {
            return 0;
        }

        /* Watermark = the host store's latest already-collected value of the definition's time
           column (Darling reads Postgres here instead) — feeds server-side filters + client dedup. */
        DateTime? watermark = definition.WatermarkColumn is null
            ? null
            : await GetLastCollectedTimeAsync(serverId, definition.TargetTable, definition.WatermarkColumn, cancellationToken);

        /* Numeric (bigint) watermark = the host store's latest already-collected value of the definition's
           monotonic identity column (job_history's instance_id) — the bigint twin of the timestamp watermark
           above, for exact-and-complete dedup that survives server-side purges. Null for every collector that
           declares no numeric watermark (the common case), so no extra query runs for them. */
        long? numericWatermark = definition.NumericWatermarkColumn is null
            ? null
            : await GetLastCollectedInstanceIdAsync(serverId, definition.TargetTable, definition.NumericWatermarkColumn, cancellationToken);

        /* Only when the watermark came back null (hot store empty): tell a TRUE first run from a store merely
           emptied by archival, so a definition like default_trace_events doesn't re-scan source data already
           in the parquet archive (CollectorContext.HasCollectedBefore). Skipped in the common (non-null
           watermark) path — no extra query. */
        bool hasCollectedBefore = definition.WatermarkColumn is not null
            && watermark is null
            && await HasPriorCollectorSuccessAsync(serverId, definition.Name, cancellationToken);

        /* Per-server state the definition declared keys for — the watermark's sibling for facts no MAX()
           over the collected rows can produce (default_trace_events' last-seen trace FILE, #1962). No
           declared keys (every other collector) means no query runs. */
        var collectorState = definition.StateKeys.Count == 0
            ? null
            : await GetCollectorStateAsync(serverId, definition.Name, cancellationToken);

        /* #2312: the open-interval refresh stamps, HOST-owned under their own state owner — the same
           pattern as Darling's plan/text watermarks: the definition cannot declare these keys (one per
           DATABASE, only known at runtime). Read unconditionally for query_store and merged into the
           same flat State; a store predating this owner has no rows, and absent keys read as "include
           the open interval", which is today's behavior exactly. */
        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
        {
            var openIntervalState = await GetCollectorStateAsync(
                serverId, QueryStoreOpenIntervalState.StateCollectorName, cancellationToken);

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

        /* #2188: retire the per-database state rows of databases that no longer exist. Lite's backfill
           worker writes done: and hole: per database and only ever deletes a hole it SERVICES or expires,
           so a dropped database's markers were kept forever — the same defect as Darling's watermark rows,
           in Lite's own collector_state. Same trigger and same placement as Darling's, before the state
           load, so the two hosts cannot drift on when they prune.

           Gated on the SAME AppliesTo that decides whether database_states is collected at all: on Azure
           SQL DB there is no snapshot by design, so this would otherwise be a guaranteed no-op every cycle
           and #2191's boundary would be emergent rather than stated. */
        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
            && DatabaseStateCollector.Instance.AppliesTo(target))
        {
            await PruneOrphanedQueryStoreDatabaseStateAsync(serverId, cancellationToken);
        }
        else if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                 && target.IsAzureSqlDb)
        {
            /* #2191's boundary, now crossable. Azure SQL DB has no database_states snapshot by design, which
               is why the arm above states it as a no-op — but after #2220 a registration that names a database
               sweeps only that database, so its one legitimate key is the connection string's own catalog. A
               registration naming NO database is still skipped: it is a registration of the logical SERVER,
               and a single-name prune there would delete every live watermark it has. */
            var ownDatabase = new SqlConnectionStringBuilder(
                _serverManager.CredentialResolver.GetConnectionString(server)).InitialCatalog;
            if (AzureSweepScope.OwnDatabaseOrEmpty(ownDatabase).Count > 0)
            {
                await PruneForeignQueryStoreDatabaseStateAsync(serverId, ownDatabase, cancellationToken);
            }
        }

        var context = new CollectorContext
        {
            ServerId = serverId,
            ServerName = GetServerNameForStorage(server),
            CollectionTime = collectionTime,
            Deltas = _deltaCalculator,
            Target = target,
            Watermark = watermark,
            NumericWatermark = numericWatermark,
            HasCollectedBefore = hasCollectedBefore,
            State = collectorState ?? CollectorContext.NoState,
            IgnoredWaitTypes = _ignoredWaitTypes.Value,
            ExcludedDatabases = server.ExcludedDatabases?.ToArray() ?? Array.Empty<string>(),
            PerfmonCounterOverride = GetPerfmonCounterOverride(),
        };

        /* Two accumulators, not one contiguous read-then-write pair: the enumeration and Azure paths now
           FLUSH each database's rows before reading the next (#1556), so SQL and storage slices interleave.
           The telemetry slot's SqlMs / StorageMs stay the #1180 fetch/store split — now sums of
           interleaved slices. */
        long sqlMs = 0;
        long storageMs = 0;
        var rowsWritten = 0;

        if (definition.RunsPerDatabase(context.Target))
        {
            /* Azure SQL DB scopes some DMVs to the connected database — run the query once per
               database, skipping (and debug-logging) databases that error, matching the original
               hand-rolled collectors.

               Definitions with a database-scoped watermark (the XE ring-buffer collectors, whose
               per-database sessions dispatch independently) get the query rebuilt per database
               against that database's own newest already-collected value — the single server-wide
               watermark would let one busy database's newer event silence another database's older
               event still sitting in its ring buffer. Everything else keeps the build-once plan. */
            var plan = definition.PerDatabaseWatermarkColumn is null || definition.WatermarkColumn is null
                ? definition.BuildQuery(context)
                : null;
            var commandTimeout = definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds;
            var databases = await GetAzureDatabaseListAsync(server, cancellationToken);

            var attempted = 0;
            var failed = 0;
            Exception? firstFailure = null;

            /* #2623: the names, not just the count. A partial loss composes a note naming which databases
               were skipped, because the count alone does not tell an operator whether the ONE database
               that matters is in the collected set or the skipped one. Mirrors Darling. */
            var failedDatabases = new List<string>();

            /* #1875: this path reads the trailing probe-failure set once PER DATABASE, so the note and the
               log cap are decided for the cycle after the loop rather than inside it — see
               CycleProbeFailures for why neither generalizes from the single-read plain path. */
            var cycleProbeFailures = new CycleProbeFailures();

            /* One DuckDB connection for the whole body; one appender per database on it (disposing an
               appender flushes that database — commit-1..N-1 semantics on abort). */
            using var duckConnection = _duckDb.CreateConnection();
            await duckConnection.OpenAsync(cancellationToken);

            foreach (var databaseName in databases)
            {
                cancellationToken.ThrowIfCancellationRequested();
                attempted++;

                /* #2150: THE path the field report is on — Azure SQL DB collects query_store per database
                   here, not through the enumerated driver, so the wall-clock ceiling has to be applied on
                   both. Null for every collector that declares none, in which case dbToken IS
                   cancellationToken and this loop is byte-for-byte what it was. Darling's twin is the same
                   shape in DarlingCollectorRunner. */
                using var dbBudget = EnumeratedCollectorDriver.StartItemBudget(
                    definition.PerItemWallClockBudget, cancellationToken);
                var dbToken = dbBudget?.Token ?? cancellationToken;

                /* #2312: this database's open-interval stamp, staged at decision time and landed only
                   after its read and flush succeed — per iteration, so a fault cannot leak a stamp
                   into a sibling database's landing. Mirrors Darling. */
                string? stagedOpenIntervalStamp = null;
                try
                {
                    /* The authoritative database_name for XE rows read on this path — see
                       CollectorContext.CurrentDatabaseName. */
                    context.CurrentDatabaseName = databaseName;

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
                        /* #2344: same bound as the enumerated arm, safe by the other route — this
                           branch does not clamp itself, but query_store's BuildCutoffParameters does. */
                        var azureReadFloor = string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                            ? WatermarkPolicy.ReadFloor(collectionTime)
                            : null;
                        context.Watermark = await GetLastCollectedTimeForDatabaseAsync(
                            serverId, definition.TargetTable, definition.WatermarkColumn!,
                            definition.PerDatabaseWatermarkColumn!, databaseName, dbToken, azureReadFloor);

                        /* #2111 adaptive shrink, Azure arm — tighten BEFORE BuildQuery: the
                           definition's own clamp only floors OLDER watermarks, so a tighter one
                           passes through untouched; the skipped range rides the backfill hole. */
                        var azureFailures = ConsecutiveQueryStoreItemFailures(serverId, databaseName);
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
                                        server.DisplayName, databaseName, azureFailures, adaptiveSpan.TotalMinutes);
                                    await RecordQueryStoreBackfillHoleAsync(serverId, databaseName, azureRaw, tighterFloor, dbToken);
                                    context.Watermark = tighterFloor;
                                }
                            }
                            else
                            {
                                /* Never-succeeded database: tighten the first-run fallback too (the
                                   review catch); no hole — pre-watermark history is the tail's job. */
                                _logger?.LogWarning(
                                    "query_store on '{Server}' database [{Database}] adaptive first-contact shrink: {Failures} consecutive failed cycles — first-run window narrowed to {Minutes:F0}m.",
                                    server.DisplayName, databaseName, azureFailures, adaptiveSpan.TotalMinutes);
                                context.Watermark = tighterFloor;
                            }
                        }

                        /* #2312, Azure arm: same per-database open-interval decision as the enumerated
                           delegate, BEFORE BuildQuery bakes the predicate. Staged into the local, landed
                           only in the post-flush success block below — a per-database fault this loop
                           tolerates must re-include next cycle, not spend the refresh window. Mirrors
                           Darling. */
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
                           is visible. */
                        if (context.CatchupClampApplied)
                        {
                            _logger?.LogWarning(
                                "{Collector} on '{Server}' database [{Database}] catch-up clamped to {Hours}h (stored watermark {Raw:o} is older) — a bounded, logged history hole.",
                                definition.Name, server.DisplayName, databaseName, WatermarkPolicy.MaxCatchup.TotalHours, context.Watermark);

                            /* #2058: record the hole for the backfill worker — context.Watermark still
                               holds the RAW value here (the definition clamped only its own cutoff), so
                               the hole is (raw, re-derived clamp floor); merged wider on repeat. The
                               name guard keeps the XE collectors sharing this branch from growing
                               backfill state they have no worker for. */
                            if (context.Watermark.HasValue
                                && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                                && WatermarkPolicy.ClampCatchup(context.Watermark, collectionTime) is DateTime azureClampedFloor)
                            {
                                await RecordQueryStoreBackfillHoleAsync(serverId, databaseName, context.Watermark.Value, azureClampedFloor, dbToken);
                            }
                        }
                    }

                    var sqlSlice = Stopwatch.StartNew();
                    List<TRow> batch;
                    /* dbToken, not cancellationToken (#2150): connect, execute and drain are the phases the
                       budget bounds. The FLUSH below deliberately stays on cancellationToken — abandoning a
                       write already in flight would trade a slow cycle for a partially-written one. */
                    using (var dbConnection = await OpenAzureDatabaseConnectionAsync(server, databaseName, dbToken))
                    using (var dbCommand = CreateCollectorCommand(dbPlan, dbConnection, commandTimeout))
                    using (var dbReader = await dbCommand.ExecuteReaderAsync(dbToken))
                    {
                        batch = await definition.ReadAsync(dbReader, context, dbToken);

                        /* #1875: the payload path's probe-failure contract, on the path that used to
                           ignore it. blocked_process_report is the declaring collector that also runs per
                           database (Azure SQL DB, #1535), so before this its batch produced the trailing
                           set and the loop simply never advanced the reader to it — the rows were built
                           and dropped. Read HERE, still inside the reader and inside the per-database
                           try, so a diagnostics fault stays a one-database skip like any other. */
                        if (definition.EmitsProbeFailures)
                        {
                            cycleProbeFailures.Add(
                                await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(dbReader, dbToken));
                        }
                    }
                    /* Read ONCE: the stopwatch is still running, so a second read a few statements later
                       returns a larger number and the per-item total would exceed the blended total it is
                       a ratio against (#2472). */
                    var dbSqlMs = sqlSlice.ElapsedMilliseconds;
                    sqlMs += dbSqlMs;

                    /* Flush this database before reading the next — peak memory is one database's rows. */
                    long dbStorageMs = 0;
                    if (batch.Count > 0)
                    {
                        var storageSlice = Stopwatch.StartNew();
                        rowsWritten += WriteBatch(duckConnection, definition, batch, serverId, context.ServerName, collectionTime, context);
                        dbStorageMs = storageSlice.ElapsedMilliseconds;
                        storageMs += dbStorageMs;
                    }

                    /* #2472: this database's slice, counted even when its batch was empty — an empty batch
                       still paid for its read, and that read is in the blended total the rollup is a ratio
                       against. */
                    fanout.Observe(databaseName, dbSqlMs + dbStorageMs);

                    /* Same per-database bounded-cycle WARNING the enumeration path emits from
                       onItemComplete. Reachable here since #1836 put query_store — the only collector
                       that declares either bound — on this branch for Azure SQL DB; without it a
                       database whose cycle was cut at the bound would look like a clean collection.
                       Since #1960 a bound DEFERS the backlog to the next cycle's resume from the
                       shipped boundary rather than dropping it — this log is how a long catch-up
                       stays observable. Read after the flush, as on the other path: the context
                       signal stays this database's until the next read resets it. */
                    /* #2111: success resets the adaptive-shrink count on the Azure arm too. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        OnQueryStoreItemSucceeded(serverId, databaseName);

                        /* #2312: read and flush both landed — the staged open-interval stamp may too. */
                        if (stagedOpenIntervalStamp is not null)
                        {
                            context.PendingState[QueryStoreOpenIntervalState.KeyFor(databaseName)] = stagedOpenIntervalStamp;
                        }
                    }

                    var capHit = definition.PerItemRowCountWarnThreshold is int cap && batch.Count >= cap;
                    if (capHit || context.PerItemTextBudgetExceeded)
                    {
                        _logger?.LogWarning(
                            "{Collector} on '{Server}' database [{Database}] hit its per-database collection bound ({Reason}) — shipped {ShippedMB:F1}MB up to {Boundary}; the backlog resumes from that boundary next cycle.",
                            definition.Name, server.DisplayName, databaseName,
                            capHit ? $"row cap {definition.PerItemRowCountWarnThreshold}" : "text byte budget",
                            context.PerItemTextBytesShipped / (1024.0 * 1024.0),
                            context.PerItemShippedBoundary?.ToString("o") ?? "n/a");
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
                       the field report describes, and it bites hardest on Lite because its live collectors
                       run strictly one after another. Ahead of the generic catch because a cancelled command
                       does not reliably arrive as an OperationCanceledException, so that filter cannot be
                       trusted to claim it; the token check is what keeps a real shutdown out of this arm. */
                    _ = ex;
                    var budgetFailure = EnumeratedCollectorDriver.ItemBudgetException(
                        definition.PerItemWallClockBudget!.Value);
                    failed++;
                    failedDatabases.Add(databaseName);
                    firstFailure ??= budgetFailure;

                    /* Same #2111 stamp the generic arm makes, and it MATTERS more here: this is what turns
                       the bound from a cut that repeats forever into one that converges. The consecutive
                       count narrows this database's next catch-up window, so a database that cannot finish
                       in the budget keeps halving until it can. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        OnQueryStoreItemFailed(serverId, databaseName);
                    }

                    /* WARNING, not Debug, unlike the routine per-database skip beside it: an offline
                       database is ordinary and this is a collector that could not finish its work. */
                    _logger?.LogWarning(
                        "{Collector} on '{Server}' database [{Database}] {Message}",
                        definition.Name, server.DisplayName, databaseName, budgetFailure.Message);
                }
                catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException)
                {
                    /* OOM is filtered OUT of this per-database skip and propagates: it is fatal, not a
                       routine one-database miss. */
                    failed++;
                    failedDatabases.Add(databaseName);
                    firstFailure ??= ex;

                    /* #2111: the yield-to-live stamp + adaptive-shrink count for the Azure SQL DB
                       arm — query_store reaches THIS per-database loop there, not the enumeration
                       path's onItemError, and without the stamp the backfill worker would never
                       yield on an Azure target (the review catch on #2112). Same query_store-only
                       guard as the hole recording above. */
                    if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                    {
                        OnQueryStoreItemFailed(serverId, databaseName);
                    }

                    _logger?.LogDebug("Skipping database '{Database}' for {Collector}: {Error}", databaseName, definition.Name, ex.Message);
                }
            }

            context.CurrentDatabaseName = null;

            /* #1875: ONE note for the cycle and ONE capped log burst, composed from every database's
               failures together. Assigned unconditionally — a cycle where nothing failed composes null,
               which is exactly what this path carried before. */
            telemetry.Note = EnumeratedCollectorDriver.MergeNotes(
                cycleProbeFailures.Note,
                EnumeratedCollectorDriver.BuildPartialFailureNote(
                    failed, attempted, failedDatabases, firstFailure?.Message));
            LogEnumerationProbeFailures(definition, server, cycleProbeFailures.Failures);

            /* One database failing is routine (offline, mid-restore, a permissions oddity) and stays a
               debug-logged skip. EVERY database failing is a systemic fault — before this check the
               cycle recorded SUCCESS with zero rows, the silent-empty shape this codebase keeps paying
               for (#1506's empty-list finding, #1535's invisible sessions). Rethrow the first failure so
               RunCollectorAsync classifies it (PERMISSIONS / transient / ERROR) instead. */
            if (attempted > 0 && failed == attempted && firstFailure is not null)
            {
                _logger?.LogWarning("{Collector} failed in all {Count} database(s) on '{Server}'; surfacing the first failure",
                    definition.Name, attempted, server.DisplayName);
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(firstFailure).Throw();
            }
        }
        else
        {
            using var sqlConnection = await CreateConnectionAsync(server, cancellationToken);

            var enumerationPlan = definition.BuildEnumerationQuery(context);
            if (enumerationPlan is not null)
            {
                /* Enumeration shape (the [db].sys.sp_executesql idiom): list items first, then
                   run one query per item ON THE SAME CONNECTION; an item that fails with a
                   SqlException is skipped with a warning, matching the original collectors. */
                var listSlice = Stopwatch.StartNew();
                EnumerationOutcome enumeration;
                /* Enumeration always uses the host default timeout, matching the originals —
                   the per-collector override applies only to the heavy per-item commands. */
                using (var enumerationCommand = CreateCollectorCommand(enumerationPlan, sqlConnection, CommandTimeoutSeconds))
                using (var enumerationReader = await enumerationCommand.ExecuteReaderAsync(cancellationToken))
                {
                    /* Shared read (#1837): the item list, then the OPTIONAL second result set of items the
                       enumeration could not probe. Both hosts route through it so the item read, the
                       failure read, and the note wording cannot drift. */
                    enumeration = await EnumeratedCollectorDriver.ReadEnumerationAsync(enumerationReader, cancellationToken);
                }
                sqlMs += listSlice.ElapsedMilliseconds;

                var items = enumeration.Items;

                /* Null on the ordinary path; the empty-enumeration breadcrumb, the probe-failure summary,
                   or both otherwise. Assigned BEFORE the zero-item early return so that cycle — the one
                   that used to log a bare SUCCESS indistinguishable from healthy — carries it too. */
                telemetry.Note = enumeration.Note;
                LogEnumerationProbeFailures(definition, server, enumeration.ProbeFailures);

                if (items.Count == 0)
                {
                    /* No items → no storage phase, matching the original's early return. The cycle still
                       records SUCCESS/0 rows (nothing failed outright), and the note above is what makes
                       that row distinguishable from a healthy collector whose databases were just quiet —
                       the silent-empty shape this codebase keeps paying for (#1837). */
                    return 0;
                }

                /* Optional quick scalar probe (e.g. query_store's live PRODUCTVERSION check,
                   deliberately probed per cycle rather than trusting cached status). Best-effort
                   on a 10-second budget, matching the original; failure leaves the definition on
                   its documented default via a null EnumerationProbeResult. */
                var probeSlice = Stopwatch.StartNew();
                var probePlan = definition.BuildEnumerationProbe(context);
                if (probePlan is not null)
                {
                    try
                    {
                        using var probeCommand = CreateCollectorCommand(probePlan, sqlConnection, 10);
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

                var itemTimeout = definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds;

                /* One DuckDB connection for the whole body; the driver writes one appender per database
                   on it, flushing each before reading the next. */
                using var duckConnection = _duckDb.CreateConnection();
                await duckConnection.OpenAsync(cancellationToken);

                /* #2312: open-interval stamps STAGED at decision time (perItemWatermark, below), landed
                   into PendingState only from onItemComplete — after the item's read AND flush succeeded.
                   A per-item fault the driver tolerates must re-include next cycle, not spend the
                   15-minute refresh window on a cycle that captured nothing. Mirrors Darling. */
                var stagedOpenIntervalStamps = new Dictionary<string, string>(StringComparer.Ordinal);

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
                            /* #2344: bound the read for the ONE collector whose value is clamped on the
                               next line. Name-guarded rather than applied to every enumerating definition:
                               a ring-buffer source whose legitimate catch-up spans days must keep reading
                               its whole history, so the clamp and the bound travel together. */
                            var readFloor = string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
                                ? WatermarkPolicy.ReadFloor(collectionTime)
                                : null;
                            var raw = await GetLastCollectedTimeForDatabaseAsync(
                                serverId, definition.TargetTable, definition.WatermarkColumn!,
                                definition.PerDatabaseWatermarkColumn!, item, ct, readFloor);
                            var clamped = WatermarkPolicy.ClampCatchup(raw, collectionTime);
                            if (raw.HasValue && clamped != raw)
                            {
                                _logger?.LogWarning(
                                    "{Collector} on '{Server}' database [{Database}] catch-up clamped to {Hours}h (stored watermark {Raw:o} is older) — a bounded, logged history hole.",
                                    definition.Name, server.DisplayName, item, WatermarkPolicy.MaxCatchup.TotalHours, raw.Value);

                                /* #2058: the clamp opens a hole (raw, clamped) the live path never
                                   revisits — record it for the backfill worker, merged wider with any
                                   hole already pending. Name-guarded like the Azure site. */
                                if (clamped.HasValue
                                    && string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                                {
                                    await RecordQueryStoreBackfillHoleAsync(serverId, item, raw.Value, clamped.Value, ct);
                                }
                            }

                            /* #2111 adaptive shrink — see Darling's twin; the skipped range rides the
                               same hole records the clamp writes, deferred to the trickle, never
                               dropped. Success resets the count via onItemComplete. */
                            var failures = ConsecutiveQueryStoreItemFailures(serverId, item);
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
                                            server.DisplayName, item, failures, span.TotalMinutes);
                                        await RecordQueryStoreBackfillHoleAsync(serverId, item, current, tighterFloor, ct);
                                        clamped = tighterFloor;
                                    }
                                }
                                else
                                {
                                    /* Never-succeeded database (null watermark): tighten the 60-minute
                                       first-run fallback the same way — the review catch; see Darling's
                                       twin. No hole: pre-watermark history is the tail's job. */
                                    _logger?.LogWarning(
                                        "query_store on '{Server}' database [{Database}] adaptive first-contact shrink: {Failures} consecutive failed cycles — first-run window narrowed to {Minutes:F0}m.",
                                        server.DisplayName, item, failures, span.TotalMinutes);
                                    clamped = tighterFloor;
                                }
                            }

                            context.Watermark = clamped;

                            /* #2312: decide per database whether this cycle reads the OPEN interval. The
                               stamp is only STAGED here — it lands in PendingState from onItemComplete,
                               after this item's read and flush actually succeeded, so a per-item fault
                               (which this driver swallows by design) re-includes next time instead of
                               spending the refresh window on a cycle that captured nothing. Mirrors
                               Darling. */
                            if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                            {
                                var includeOpen = QueryStoreOpenIntervalState.ShouldIncludeOpenInterval(
                                    context.State, item, collectionTime);
                                context.IncludeOpenInterval = includeOpen;
                                if (includeOpen)
                                {
                                    stagedOpenIntervalStamps[QueryStoreOpenIntervalState.KeyFor(item)] =
                                        QueryStoreOpenIntervalState.Format(collectionTime);
                                }
                            }
                        },
                    readItem: async (item, ct) =>
                    {
                        var batch = new List<TRow>();
                        using var itemCommand = CreateCollectorCommand(definition.BuildPerItemQuery(item, context), sqlConnection, itemTimeout);
                        using var itemReader = await itemCommand.ExecuteReaderAsync(ct);
                        await definition.ReadItemAsync(item, itemReader, batch, context, ct);
                        return batch;
                    },
                    writeBatch: (batch, ct) => Task.FromResult(WriteBatch(duckConnection, definition, batch, serverId, context.ServerName, collectionTime, context)),
                    onItemComplete: (item, batchCount, itemSqlMs, itemStorageMs) =>
                    {
                        /* #2472: the per-database cost the blended collection_log row cannot carry.
                           Counted for every completed item, including the quiet ones the log line below
                           skips — their read time is in the blended total too. */
                        fanout.Observe(item, itemSqlMs + itemStorageMs);

                        /* #2111: a completed item resets the adaptive-shrink count — recovery returns
                           the member to the full catch-up width on its next cycle. */
                        if (string.Equals(definition.Name, QueryStoreCollector.Instance.Name, StringComparison.Ordinal))
                        {
                            OnQueryStoreItemSucceeded(serverId, item);

                            /* #2312: NOW the open-interval stamp may land — this hook only fires after
                               the item's read and flush both succeeded. Remove, not read: a stamp left
                               staged (read faulted) must not leak into a later run's landing. */
                            if (stagedOpenIntervalStamps.Remove(QueryStoreOpenIntervalState.KeyFor(item), out var landedStamp))
                            {
                                context.PendingState[QueryStoreOpenIntervalState.KeyFor(item)] = landedStamp;
                            }
                        }

                        /* Per-DATABASE line for non-empty batches (#1565): the per-server summary blends
                           every database into one number, hiding a single busy database's burst behind
                           quiet siblings. Quiet databases (0 rows) stay silent. */
                        if (batchCount > 0)
                        {
                            _logger?.LogInformation("  [{Server}] {Collector} [{Database}] => {Rows} rows (sql:{SqlMs}ms, duckdb:{DuckMs}ms)",
                                server.DisplayName, definition.Name, item, batchCount, itemSqlMs, itemStorageMs);
                        }

                        var capHit = definition.PerItemRowCountWarnThreshold is int cap && batchCount >= cap;
                        if (capHit || context.PerItemTextBudgetExceeded)
                        {
                            _logger?.LogWarning(
                                "{Collector} on '{Server}' database [{Database}] hit its per-database collection bound ({Reason}) — shipped {ShippedMB:F1}MB up to {Boundary}; the backlog resumes from that boundary next cycle.",
                                definition.Name, server.DisplayName, item,
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
                            OnQueryStoreItemFailed(serverId, item);
                        }

                        _logger?.LogWarning("Failed to collect {Collector} from [{Database}] on '{Server}': {Message}",
                            definition.Name, item, server.DisplayName, ex.Message);
                    },
                    cancellationToken,
                    /* #2150: the per-database wall-clock ceiling. Null for every collector but
                       query_store, so this argument leaves every other cycle untouched. */
                    perItemBudget: definition.PerItemWallClockBudget);

                rowsWritten = driverResult.Rows;
                sqlMs += driverResult.SqlMs;
                storageMs += driverResult.StorageMs;
            }
            else
            {
                /* Plain single-query path (server-scoped): read all rows, then write them in one batch
                   (supplemental never runs for per-database collectors). Routed through WriteBatch so
                   all three paths share one writer.

                   #2673: the primary read + DRAIN is bounded by the collector's PerItemWallClockBudget (one
                   item = the whole server). The 60s per-command timeout covers only execution, not the drain
                   of a large result set, so a heavy server-scoped collector (procedure_stats, query_stats)
                   could occupy the monitored server for minutes. Bites hardest on Lite, whose live collectors
                   run strictly one after another. Null budget = itemToken IS cancellationToken and this block
                   is byte-for-byte what it was. */
                var sqlSlice = Stopwatch.StartNew();
                var plan = definition.BuildQuery(context);
                List<TRow> rows;
                using var itemBudget = EnumeratedCollectorDriver.StartItemBudget(definition.PerItemWallClockBudget, cancellationToken);
                var itemToken = itemBudget?.Token ?? cancellationToken;
                try
                {
                    using var command = CreateCollectorCommand(plan, sqlConnection, definition.CommandTimeoutSecondsOverride ?? CommandTimeoutSeconds);
                    using var reader = await command.ExecuteReaderAsync(itemToken);
                    rows = await definition.ReadAsync(reader, context, itemToken);

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
                        var probes = await EnumeratedCollectorDriver.ReadPayloadProbeFailuresAsync(reader, itemToken);
                        telemetry.Note = probes.Note;
                        LogEnumerationProbeFailures(definition, server, probes.ProbeFailures);
                    }
                }
                catch (Exception ex) when (EnumeratedCollectorDriver.ItemBudgetExpired(itemBudget, cancellationToken))
                {
                    /* #2673: this server-scoped collector blew its wall-clock budget mid read/drain. Abandon
                       the cycle WITHOUT advancing any watermark — ship nothing, retry next — so no single
                       collector runs minutes on a monitored server. Returning skips the storage phase and the
                       state-persistence at the method tail, which is what keeps the watermark from moving. */
                    _ = ex;
                    var budgetSeconds = (int)definition.PerItemWallClockBudget!.Value.TotalSeconds;
                    telemetry.SqlMs = sqlSlice.ElapsedMilliseconds;
                    telemetry.Note = EnumeratedCollectorDriver.WholeCycleBudgetNote(budgetSeconds);
                    telemetry.Abandoned = true;
                    _logger?.LogWarning(
                        "{Collector} on '{Server}' reached its {Budget}s wall-clock budget mid-collection — abandoned this cycle, will retry next (#2673).",
                        definition.Name, context.ServerName, budgetSeconds);
                    return 0;
                }

                /* Optional best-effort second query on the same connection (e.g. server_properties'
                   WS5 health probe). Failure-isolated: it can never fail the primary rows. Skipped
                   when the primary produced no rows, matching the originals (which only ran their
                   second query after a successful primary read). */
                var supplementalPlan = definition.BuildSupplementalQuery(context);
                if (supplementalPlan is not null && rows.Count > 0)
                {
                    try
                    {
                        using var supplementalCommand = CreateCollectorCommand(supplementalPlan, sqlConnection, CommandTimeoutSeconds);
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
                using var duckConnection = _duckDb.CreateConnection();
                await duckConnection.OpenAsync(cancellationToken);
                rowsWritten = WriteBatch(duckConnection, definition, rows, serverId, context.ServerName, collectionTime, context);
                storageMs += storageSlice.ElapsedMilliseconds;
            }
        }

        /* Persist what the definition observed, AFTER the cycle completed — including a cycle that wrote
           zero rows, which is exactly the case a row-derived watermark cannot cover (#1962). A cycle that
           threw never reaches here, so the older state survives and the next run takes its conservative
           path. Outside the storage-phase timer: this is host bookkeeping, not collected data. */
        if (context.PendingState.Count > 0)
        {
            /* #2312: the open-interval stamps belong to their OWN state owner, not the definition's name
               — a row written under "query_store" would load back (nothing reads that owner here) but the
               shared prune set pairs qsowm: with query_store_open_interval, and a prefix pruned under the
               wrong owner deletes nothing. Split by prefix on the way out, like Darling's runner. */
            var openIntervalKeys = context.PendingState
                .Where(entry => entry.Key.StartsWith(QueryStoreOpenIntervalState.WatermarkKeyPrefix, StringComparison.Ordinal))
                .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal);

            if (openIntervalKeys.Count > 0)
            {
                var others = context.PendingState
                    .Where(entry => !openIntervalKeys.ContainsKey(entry.Key))
                    .ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.Ordinal);

                await SaveCollectorStateAsync(
                    serverId, QueryStoreOpenIntervalState.StateCollectorName, openIntervalKeys, cancellationToken);

                if (others.Count > 0)
                {
                    await SaveCollectorStateAsync(serverId, definition.Name, others, cancellationToken);
                }
            }
            else
            {
                await SaveCollectorStateAsync(serverId, definition.Name, context.PendingState, cancellationToken);
            }
        }

        telemetry.SqlMs = sqlMs;
        telemetry.StorageMs = storageMs;
        telemetry.Fanout = fanout.Result;

        _logger?.LogDebug("Collected {RowCount} {Collector} rows for server '{Server}'", rowsWritten, definition.Name, server.DisplayName);
        return rowsWritten;
    }

    /// <summary>
    /// Writes the per-item app-log lines for probe failures, capped at
    /// <see cref="EnumeratedCollectorDriver.MaxLoggedProbeFailures"/> with the suppressed remainder
    /// reported as a count. The collection_log row already carries the summary note; this is where the
    /// actual per-database error text lands, and it is why that note says "see the app log". Darling's
    /// twin is <c>DarlingCollectorRunner.LogEnumerationProbeFailures</c> — same shared templates.
    ///
    /// <para>Serves BOTH channels: an enumeration's second result set (#1837) and a payload collector's
    /// trailing one (#1851). Named for the shared template it writes, which reports the failing step as
    /// an enumeration probe — accurate for both, since a payload collector reaches this only by
    /// enumerating and probing databases inside its own server-side cursor.</para>
    /// </summary>
    private void LogEnumerationProbeFailures<TRow>(
        ICollectorDefinition<TRow> definition,
        ServerConnection server,
        IReadOnlyList<EnumerationProbeFailure> probeFailures)
    {
        if (probeFailures.Count == 0)
        {
            return;
        }

        var shown = Math.Min(probeFailures.Count, EnumeratedCollectorDriver.MaxLoggedProbeFailures);
        for (var i = 0; i < shown; i++)
        {
            _logger?.LogWarning(EnumeratedCollectorDriver.ProbeFailureLogTemplate,
                definition.Name, server.DisplayName, probeFailures[i].Item, probeFailures[i].Error);
        }

        if (probeFailures.Count > shown)
        {
            _logger?.LogWarning(EnumeratedCollectorDriver.ProbeFailureOverflowLogTemplate,
                definition.Name, server.DisplayName, probeFailures.Count, probeFailures.Count - shown, shown);
        }
    }

    /// <summary>
    /// Writes ONE batch (one enumerated item / one database, or the whole result set for a plain
    /// collector) to DuckDB via a single appender on the caller's already-open connection (#1556). The
    /// three collection paths route through here so the storage logic — the prefix columns, the positional
    /// payload — lives once. Disposing the appender FLUSHES the batch, so on a mid-run abort the batches
    /// already written stay committed (commit-1..N-1). An empty batch opens no appender and returns 0
    /// (rows_collected = Σ non-empty batch counts). Synchronous (the DuckDB appender API is), returning the
    /// count so the driver can await it as a completed task.
    /// </summary>
    private static int WriteBatch<TRow>(
        DuckDBConnection duckConnection,
        ICollectorDefinition<TRow> definition,
        List<TRow> rows,
        int serverId,
        string serverName,
        DateTime collectionTime,
        CollectorContext context)
    {
        if (rows.Count == 0)
        {
            return 0;
        }

        var rowsWritten = 0;
        using (var appender = duckConnection.CreateAppender(definition.TargetTable))
        {
            var writer = new AppenderCollectorRowWriter();

            foreach (var item in rows)
            {
                var row = appender.CreateRow();

                if (definition.IncludesCollectionId)
                {
                    row.AppendValue(GenerateCollectionId()); /* collection_id BIGINT */
                }

                row.AppendValue(collectionTime)              /* collection_time TIMESTAMP */
                   .AppendValue(serverId)                    /* server_id INTEGER */
                   .AppendValue(serverName);                 /* server_name VARCHAR */

                writer.CurrentRow = row;
                definition.WritePayload(item, writer, context);
                row.EndRow();

                rowsWritten++;
            }
        }

        return rowsWritten;
    }

    private static SqlCommand CreateCollectorCommand(CollectorQuery plan, SqlConnection connection, int commandTimeoutSeconds)
    {
        var command = new SqlCommand(plan.Text, connection) { CommandTimeout = commandTimeoutSeconds };

        foreach (var parameter in plan.Parameters)
        {
            command.Parameters.Add(ToSqlParameter(parameter));
        }

        return command;
    }

    private static SqlParameter ToSqlParameter(CollectorParameter parameter) => parameter.Type switch
    {
        CollectorParameterType.DateTime2 => new SqlParameter(parameter.Name, SqlDbType.DateTime2) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.NVarChar128 => new SqlParameter(parameter.Name, SqlDbType.NVarChar, 128) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.NVarChar260 => new SqlParameter(parameter.Name, SqlDbType.NVarChar, 260) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.Int32 => new SqlParameter(parameter.Name, SqlDbType.Int) { Value = parameter.Value ?? DBNull.Value },
        CollectorParameterType.BigInt => new SqlParameter(parameter.Name, SqlDbType.BigInt) { Value = parameter.Value ?? DBNull.Value },
        _ => throw new ArgumentOutOfRangeException(nameof(parameter), parameter.Type, "Unmapped collector parameter type"),
    };
}
