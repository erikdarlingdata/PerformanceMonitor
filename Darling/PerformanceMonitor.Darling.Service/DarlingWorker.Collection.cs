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
using System.Diagnostics;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.Versioning;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Hosting.WindowsServices;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using ServerClock = PerformanceMonitor.Analysis.Baselines.ServerClock;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingWorker
{
    /// <summary>
    /// Everything after the (optional) managed-Postgres bootstrap: store connection, migration,
    /// Timescale adoption, delta seeding, and the collection/alert/analysis loop. Split from
    /// <see cref="ExecuteAsync"/> so the bootstrap's finally can stop the bundled server after
    /// this method's data source is disposed.
    /// </summary>
    private async Task RunCollectionLoopAsync(
        DarlingConfig config,
        string storeConnectionString,
        DarlingSelfAlertEvaluator.StoreUpgradeReport? storeUpgradeReport,
        DarlingSelfAlertEvaluator.StoreTimescaleReport? storeTimescaleReport,
        string? managedDataDirectory,
        ManagedConfWriteResult? managedConfWriteResult,
        bool managedUsedLastGoodConf,
        ManagedConfMigrationOutcome? managedConfVerification,
        Func<CancellationToken, Task>? reconcileNetworkAfterRoleProvisioning,
        CancellationToken stoppingToken)
    {
        /* Carry the collect/config search path on the store connection string BEFORE the data
           source (and its pool) is created, so every pooled physical connection resolves the
           shared SQL's bare table names to the V8 schemas from its very first use — deterministic
           and independent of the pool's connection-open timing relative to PgMigrations'
           best-effort ALTER DATABASE ... SET search_path. Without this a FRESH bring-your-own
           store silently collects nothing until the service is restarted; see
           EnsureStoreSearchPath for the pool-timing root cause. Managed mode already sets it, so
           this is a no-op there. The pin has to sit OUTSIDE EnsureStoreSearchPath (wrapping its
           result rather than the two being one call), so the census below only has to look for the
           pin on the Create line itself — no reassignment in between for a future edit to slip
           an unpinned read behind. */

        /* A string Npgsql cannot use throws from the Create below, leaves this method and ExecuteAsync, and the
           host logs that exception to the console, the Event Log and the file log with its message and its inner
           exception. Npgsql's text for that can quote part of the string (a password with an unquoted ';' makes the
           text after it the "keyword", and a password tail that becomes a Host value with a ':' in it makes the text
           after the ':' a bad port). So prove the string first, with Npgsql's own construction step (the guard builds
           and disposes a data source, and opens nothing): the exception that leaves is the formatter's text, with no
           inner exception, and the host still stops exactly as it did. The keywords added below (search path,
           application name, session time zone) take part in none of Npgsql's construction checks, so a string that
           passes here does not throw from the Create below for a parse or a validation reason. */
        if (DarlingStoreConnection.TryDescribeParseFailure(storeConnectionString) is { } unusableStoreString)
        {
            throw new InvalidOperationException("postgres.connectionString could not be used: " + unusableStoreString);
        }

        DarlingSelfAlertEvaluator.CollectionGapReport? collectionGapReport = null;
        await using var postgres = NpgsqlDataSource.Create(
            DarlingStoreConnection.PinSessionTimeZoneUtc(
                DarlingStoreConnection.WithApplicationName(
                    DarlingStoreConnection.EnsureStoreSearchPath(storeConnectionString), DarlingManagedPostgres.ServiceApplicationName)));
        _postgres = postgres;
        _ownerSource?.Publish(postgres);
        /* #2936: a failure here is triaged rather than uniformly terminal. A store that is unreachable for
           a moment — restarting, failing over, still coming up alongside this service — and a sibling
           instance holding the migration advisory lock both succeed seconds later; a rung that cannot
           apply against this store never will. StartupFailureTriage decides which arrived and carries
           the reasoning for where that boundary sits, including why anything it cannot place positively
           stays terminal. This is the LAST of the three startup steps that can kill collection — the config
           load and the managed bootstrap above are the other two, triaged the same way through the same
           predicate — so the degrade-vs-kill question the next block answers out loud is answered here
           too: a transient store failure now degrades to a delayed first cycle, and everything else keeps
           the critical line and the stand-down byte for byte. Both caps are load-bearing: an attempt is not
           quick just because a refused connect is — one that blocks behind a peer's advisory lock can
           spend MigrationLockWaitTimeoutSeconds — so the wall-clock budget is what stops 25 attempts from
           becoming ten hours, and the attempt count is what the warning line reports.
           A FRESH connection per attempt, not a reuse of the old one — its connector is dead after a
           transport failure, the same reason DarlingManagedPostgres.OpenProbedMaintenanceConnectionAsync retries
           the connect and its first query as one unit rather than just the open. Re-entering MigrateAsync is safe because the applier commits
           each rung's DDL and its darling_schema_version stamp in ONE transaction: a rung that failed
           part-way left nothing applied and nothing stamped, and rungs at or below the stamp are skipped,
           so a retry resumes at the rung that failed instead of redoing the ladder. That rests on the
           rungs being transactional, which was measured rather than assumed, because the ladder's
           expensive ones are not plain DDL: V23's own statement set — create_hypertable with
           migrate_data => true, ALTER TABLE SET (timescaledb.compress ...), add_compression_policy — run
           against 200,000 rows on PostgreSQL 17.11 / TimescaleDB 2.29.2 built 139 chunks and a policy job
           inside the transaction and left, after ROLLBACK, a plain table with every row, no chunks, no
           job and no reloptions. No rung uses CREATE INDEX CONCURRENTLY or any other statement that
           cannot be transacted. */
        var storeRetryBudget = System.Diagnostics.Stopwatch.StartNew();
        /* #4508: set once, the first time the fast budget above runs out on a failure that is still
           retryable — see the arm below. Gates CRITICAL-once/WARNING-per-retry the same way the config
           and bootstrap sites' single terminal LogCritical already is "once": this is the sustained arm's
           one moment where the diagnosis is new, and every later attempt on the same still-down store is
           not. */
        var storeSustainedRetryCriticalLogged = false;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var migrateConnection = await postgres.OpenConnectionAsync(stoppingToken);
                /* MigrateAsync (logger overload) also best-effort sets the database-default search_path to
                   collect/config for every future connection (V8 security split); a least-privilege BYO
                   login that cannot ALTER DATABASE is warned, not failed — the managed connection strings
                   carry Search Path regardless. */
                var applied = await PgMigrations.MigrateAsync(migrateConnection, _logger, stoppingToken);
                _logger.LogInformation("Postgres store ready (schema v{Version}, {Applied} migration(s) applied)",
                    StorageVersion.SchemaVersion, applied);
                /* #4961: this install's id, made (or read back) now that the schema is current and before any worker
                   starts. Inside this try, on the same connection, so a failure to make it is retried and triaged exactly
                   like a failed migration. The CLI and the Viewer only read the row. */
                _installId = await StoreInstallId.EnsureAsync(migrateConnection, _logger, stoppingToken);
                _logger.LogInformation("Install id {InstallId}", _installId);
                /* #5450: the newest collection time already in the store, read NOW — the schema is current and no
                   collector has written yet — so the gap the service was down for can be raised once the alert
                   engine exists. Best effort: a failed read loses one alert and must not fail or retry the start. */
                collectionGapReport = await ReadCollectionGapAsync(migrateConnection, stoppingToken);
                break;
            }
            catch (Exception ex) when (ex is not OperationCanceledException
                && attempt < StartupFailureTriage.Attempts
                && storeRetryBudget.Elapsed < StartupFailureTriage.RetryBudget
                && StartupFailureTriage.IsRetryable(ex))
            {
                /* Every placeholder appears ONCE. A repeated name is not a duplicate here, it is an extra
                   positional slot: LogValuesFormatter numbers placeholders by occurrence, so a template
                   naming four things in five slots throws FormatException out of the logger, and out of
                   this BackgroundService, and StopHost then takes the process down - a retry path that
                   kills the service harder than the failure it was retrying. */
                _logger.LogWarning(
                    ex,
                    "Cannot reach or migrate the Postgres store yet ({Message}) — attempt {Attempt} of " +
                    "{Total}, retrying in {Delay}s. A store that is restarting, failing over or still " +
                    "coming up recovers on its own; after the last attempt this keeps retrying more slowly " +
                    "instead of giving up.",
                    ex.Message, attempt, StartupFailureTriage.Attempts,
                    (int)StartupFailureTriage.RetryDelay.TotalSeconds);
                _collectorState.PublishRetrying(
                    CollectorRuntimeState.StartupStep.Store, attempt, StartupFailureTriage.Attempts);
                await Task.Delay(StartupFailureTriage.RetryDelay, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException
                && StartupFailureTriage.IsRetryable(ex))
            {
                /* #4508: reached only once the fast arm above has stopped matching — its own attempt/budget
                   conjuncts are false, so this is a store outage that has already outlasted the two-minute
                   fast budget while still being a failure StartupFailureTriage.IsRetryable accepts. The old
                   code had no third arm here: the fast filter going false let this fall straight through to
                   the terminal catch below, so a store still coming back got the identical CRITICAL /
                   PublishStopped / return a rung that can never apply gets, and collection never started
                   again for the life of the process. NextAction is the SAME decision the fast arm's filter
                   re-derives inline (attempt/budget/classifier) — calling it here, rather than reaching for
                   SustainedRetryDelay directly, is what routes this arm's delay through the seam instead of
                   beside it. */
                var next = StartupFailureTriage.NextAction(attempt, storeRetryBudget.Elapsed, ex);
                if (!storeSustainedRetryCriticalLogged)
                {
                    // install-darling.ps1 waits for this text (#6470): reword both together.
                    _logger.LogCritical(
                        ex,
                        "Cannot reach or migrate the Postgres store within the {Budget}s retry budget " +
                        "({Message}); this is still a recoverable failure, so it keeps retrying every " +
                        "{Delay}s instead of standing down. Collection will not start until it succeeds.",
                        (int)StartupFailureTriage.RetryBudget.TotalSeconds, ex.Message,
                        (int)StartupFailureTriage.SustainedRetryDelay.TotalSeconds);
                    storeSustainedRetryCriticalLogged = true;
                }
                else
                {
                    _logger.LogWarning(
                        ex,
                        "Still cannot reach or migrate the Postgres store ({Message}) — attempt {Attempt}, " +
                        "retrying in {Delay}s.",
                        ex.Message, attempt, (int)StartupFailureTriage.SustainedRetryDelay.TotalSeconds);
                }

                _collectorState.PublishRetrying(
                    CollectorRuntimeState.StartupStep.Store, attempt, StartupFailureTriage.Attempts, sustained: true);
                await Task.Delay(next.Delay, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogCritical(ex, "Cannot reach or migrate the Postgres store: {Message}", ex.Message); // install-darling.ps1 waits for this text (#6470): reword both together.
                /* #2953: AFTER the critical line, deliberately. The log line is the diagnosis of record and
                   predates this seam; publishing first would put a new call between the failure and the one
                   message an operator greps for. */
                _collectorState.PublishStopped(CollectorRuntimeState.StartupStep.Store);
                return;
            }
        }

        /* #4348 S1b: the one-time scrub of secrets an older collector build stored in plain text (a
           standby's replication password inside primary_conninfo, most notably). Launched here, right
           after migrations confirm collect.pg_server_config and collect.collector_state exist, and NOT
           gated on TimescaleDB — the scrub works the same on plain PostgreSQL, it just has fewer chunks to
           reason about. Drained with the other background startup work below. */
        var settingScrub = RunPgSettingScrubAsync(postgres, stoppingToken);

        /* #5097: the slow-read writer, drained at shutdown. Failure-isolated inside; it never ends before shutdown. */
        var slowReadWriter = _slowReads is null ? Task.CompletedTask : _slowReads.RunAsync(postgres, _logger, stoppingToken);

        /* #5506 part 3: the long-read table, built once the store is migrated. Never throws; until it finishes, and if it
           fails, the window-size rule decides which reads are long. */
        var readAdmissionTable = _readAdmission is null ? Task.CompletedTask : _readAdmission.RefreshAsync(postgres, _readLatency, stoppingToken);

        /* #4348: the one-time scrub of collected statement text (collect.pg_statement_text,
           collect.pg_blocking_edges) an older build stored before the shared sensitive-statement filter
           existed. Same launch discipline as the setting scrub immediately above — after migrations
           confirm both target tables exist, not gated on TimescaleDB, drained with the rest of startup
           below. */
        var statementTextScrub = RunPgStatementTextScrubAsync(postgres, stoppingToken);

        /* The one-time removal of exact duplicate rows already stored in collect.deadlocks (one Azure deadlock
           stored twice: the database's own session and the server's telemetry), keeping the earliest. Same launch
           discipline as the scrubs above: its own connection, its own catch, drained with the other background
           startup work below. */
        var deadlockDuplicateCleanup = RunDeadlockDuplicateCleanupAsync(postgres, stoppingToken);

        /* #4346: the one-time scrub of the legacy plan_force_actions.detail state_unavailable line
           #4326/#4363/#4376 stop new rows from ever carrying. Same launch shape as settingScrub above —
           its own connection, its own catch, drained with the other background startup work below. */
        var planForceDetailScrub = RunPlanForceActionDetailScrubAsync(postgres, stoppingToken);

        /* #4605, #4952, #5507, #5807, #5957: the Query Store read indexes - the BRIN on collect.query_store_interval_wide
           (collection_time), the btrees on (server_id, first_execution_time) of collect.query_store_interval_wide
           and collect.query_store_interval_latest, and the btrees on (server_id, database_name, query_id, first_execution_time) of
           collect.query_store_interval_wide (WideDatabaseQueryFirstExec) and collect.query_store_interval_latest
           (LatestDatabaseQueryFirstExec) - built in the background
           QueryStoreBackgroundIndexes.StartDelay after start so their heap reads stay off the post-restart IO burst,
           one after another, each failure-isolated. Launched after migrations confirm the tables exist, never awaited
           on the startup path, one attempt per start, and RunDelayedAsync never throws. Drained with the other
           background work.
           #5571: the same task first runs Phase A of the day partitions (arm, validate, promote, then ANALYZE) for each
           of the two interval tables, then these index ensures, because the VALIDATE and a CREATE INDEX CONCURRENTLY on
           the legacy table conflict. It then LOOPS every hour until shutdown, so a VALIDATE that lost a lock or a promote
           that did not get one is tried again without a restart, and it runs the once-a-day ANALYZE of each promoted
           parent off the sweep loop. It never blocks collectors or startup. Phase B (create ahead, drop expired, and the
           cheap parts of Phase A for a table that is not promoted) is the "query store interval partitions" convergence
           step, which runs before the collectors and every hour. */
        var queryStoreIndexes = QueryStoreIntervalPartitions.RunDelayedAsync(
            postgres, _logger, QueryStoreBackgroundIndexes.StartDelay, QueryStoreBackgroundIndexes.All, stoppingToken);

        /* #6750 part 3: the keys of the monitor's own Query Store statements stored before V219, one scan of the text table, in the
           background QueryStoreBackgroundIndexes.StartDelay after start. Until it has run the regressions read keeps the text probe
           (QueryStoreSelfKeys explains the gate). Once per start, a no-op once the state row exists, and it never throws. */
        var queryStoreSelfKeys = QueryStoreSelfKeys.RunDelayedAsync(postgres, _logger, QueryStoreBackgroundIndexes.StartDelay, stoppingToken);

        /* #4957: one rollup-coverage probe in the background RollupCoverageWarmup.ServiceStartDelay after start, so
           the first MCP or web call finds each rollup's floor already measured and does not wait ~10 s on the
           large stores for the oldest compressed chunk's min(bucket). Launched after migrations, never awaited on
           the startup path, and RunDelayedAsync never throws: a failed warm logs once at Debug and changes
           nothing else. Drained with the other background work. */
        var rollupCoverageWarm = RollupCoverageWarmup.RunDelayedAsync(
            postgres, _logger, RollupCoverageWarmup.ServiceStartDelay, stoppingToken);

        /* #4214 ruling 9: the once-per-start store host/settings profile log — host facts, pg_settings and
           the managed conf files only, never the store-size/chunk-total reads --check-settings and the MCP
           read own. Its own short deadline and its own catch: a bad conf file, a permission problem or a
           slow store here must never fail or delay the collection start immediately below it. */
        await LogStoreHostProfileAsync(config, postgres, managedDataDirectory, managedConfWriteResult, stoppingToken);

        /* Least-privilege role provisioning (V8 security hardening), managed mode only: create /
           refresh the admin + viewer login roles and their per-role DPAPI credentials, and grant the
           collect/config privileges — idempotent and self-healing, the conf-append discipline applied
           to roles. Windows-only (DPAPI credential files); a failure degrades (the Viewer cannot
           connect as admin/viewer until a later start succeeds) but never kills collection, which
           connects as the owner. The compose store is the branch below; any other BYO store provisions roles
           out-of-band via tools/provision-roles.sql. */
        if (config.Postgres.Managed && OperatingSystem.IsWindows())
        {
            string? provisioningDirectory = null;
            try
            {
                var dataDirectory = DarlingManagedPostgres.ResolveDataDirectory(config.Postgres);
                provisioningDirectory = dataDirectory;
                /* #2918: record what provisioning actually WROTE onto the roles, not what the store holds
                   afterwards. This runs BEFORE SeedIfEmptyAsync, so on a brand-new store there is no
                   config_service row to read and the roles get the 15 s default while the seed then inserts
                   darling.json's value — seeding the reload baseline from the post-seed view would claim a
                   value the roles never received, and the gate only fires on a difference, so that first-run
                   mismatch would never be corrected. Left at -1 if provisioning throws, so the first reload
                   re-asserts rather than trusting a write that did not land. */
                _appliedComposeStatementTimeoutSeconds =
                    await DarlingManagedRoles.EnsureProvisionedAsync(postgres, dataDirectory, _logger, stoppingToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* The hourly store-maintenance tick retries it (RetryRoleProvisioningAsync), and every start does. */
                _roleProvisioningRetryDirectory = provisioningDirectory;
                _logger.LogError(
                    "Least-privilege role provisioning failed — the Viewer's admin/viewer roles may be stale, and " +
                    "a role credential file the web and MCP hosts wait for is not written until an attempt " +
                    "succeeds; it is retried on the hourly store-maintenance tick and on the next start: {Message}",
                    ex.Message);
                /* Provisioning stopped before its own rules section: a store that has never had the password rules
                   would keep none while the viewer and mcp roles keep their earlier credentials. Best effort; the
                   call warns for itself and never throws. */
                await DarlingManagedRoles.EnsureServerPasswordRulesAsync(postgres, _logger, stoppingToken);
            }

            /* #5654: the pg_hba pass in the bootstrap leaves the alert sources line out while the group has members
               beyond the registered source logins. Provisioning above revokes the extras it can, so a pass that left
               the line out for that reason runs once more now, and the line lands on this start. */
            if (reconcileNetworkAfterRoleProvisioning is not null)
            {
                await reconcileNetworkAfterRoleProvisioning(stoppingToken);
            }
        }
        else if (!config.Postgres.Managed && Hosting.DarlingHostBinding.IsRunningInContainer)
        {
            /* #3914: the Linux compose distribution's store is the service's own, so it provisions the same roles
               there and the web and MCP hosts connect as viewer and mcp. It decides from the store whether this
               IS that store, publishes the verdict the hosts wait on, and never throws: a refusal or failure
               leaves each host on its role's credential from an earlier start, or on the owner when there is none,
               with the reason. Same reload baseline as managed, and only from a start that provisioned. */
            var verdict = await DarlingStoreLogins.ProvisionComposeStoreAsync(
                postgres, config.Postgres.ConnectionString, _logger, stoppingToken);
            if (verdict.Provisioned)
            {
                _composeStoreRolesProvisioned = true;
                _appliedComposeStatementTimeoutSeconds = verdict.AppliedComposeStatementTimeoutSeconds;
            }
            else
            {
                await DarlingManagedRoles.EnsureServerPasswordRulesAsync(postgres, _logger, stoppingToken);
            }
        }
        else if (!config.Postgres.Managed)
        {
            /* A self-managed store gets the store's password rules from tools/provision-roles.sql; a store upgraded
               without re-running it has none. Best-effort on every start as the role that owns the tables, the way the
               database-default search_path is: one warning naming the script when the login may not create them. */
            await DarlingManagedRoles.EnsureServerPasswordRulesAsync(postgres, _logger, stoppingToken);
        }

        /* Optional TimescaleDB adoption — runtime setup, deliberately NOT a versioned migration
           (the store must work with or without the extension; migrations stay engine-plain).
           Detected once at startup; when present, the collector tables become hypertables with
           a compression policy (Darling's archival tier) and the daily retention purge below
           switches to drop_chunks. All idempotent, so every restart re-converges. In its own
           try/catch OUTSIDE the critical migrate block: an optional feature failing must degrade
           to plain-PostgreSQL mode, never kill the service.
           This block runs AFTER CREATE EXTENSION (TryEnableAsync), which is why the AUTHORITATIVE
           collection_log conversion (EnsureCollectionLogHypertableAsync) lives here, not in the V23
           migration: MigrateAsync above runs BEFORE the extension exists, so a fresh store's V23
           guard skips the conversion and this heals it (collection_log is outside the collector
           catalog, so the loop calls above never touch it). */

        /* Handle for the background baseline backfill launched inside the block below (#1757); stays null in
           plain-PostgreSQL mode or if the TimescaleDB block faults before reaching it. Drained at shutdown. */
        Task? baselineBackfill = null;

        /* And the materialization-hole repair's (#3653, Q10), launched the same way right after the ensure
           sweep; same lifetime, same drain. */
        Task? holeRepair = null;

        /* And the one-time refresh of the rebuilt collection-health rollup (#4812), launched the same way right
           after the ensure sweep; same lifetime, same drain. */
        Task? collectionHealthWarm = null;

        /* #3817: the convergence pass's clock and tally, both declared HERE rather than in the TimescaleDB
           block, because the pass spans four segments across three connections and two of those segments run
           on a plain-PostgreSQL store where the block below never opens its gate. The clock covers every
           segment including the connection opens, which is where a slow store actually spends this pass; the
           tally is what makes the four segments one line instead of four. */
        var startupConvergenceClock = Stopwatch.StartNew();
        var startupConvergence = new StoreObjectConvergenceTally();

        try
        {
            await using var timescaleConnection = await postgres.OpenConnectionAsync(stoppingToken);
            _timescaleAvailable = await TimescaleSupport.TryEnableAsync(timescaleConnection, _logger, stoppingToken);
            if (_timescaleAvailable)
            {
                /* #3817: the store-object ensures that the start path and the hourly store-maintenance tick
                   BOTH run now come from ONE list (s_storeObjectConvergence), walked here segment by segment
                   so the three non-convergence steps interleaved below keep the positions their own issues
                   argued for. Every step, the four load-bearing ordering constraints among them, each step's
                   idempotence verdict and the five things deliberately left start-path-only are documented on
                   that list. What stood here was eleven await lines; this is the same eleven calls in the same
                   order, READ off the list rather than restated, because the defect #3817 closes is precisely
                   a second caller whose order drifts from this one's — and the only way two callers cannot
                   drift is for there to be one order.

                   The tally accumulates across all four segments and all three connections, so a start writes
                   ONE "Store object convergence:" line (after the tuning block below) rather than one per
                   segment, and the hourly pass writes the same line in the same shape. The per-object
                   INFORMATION lines each ensure writes are unchanged and still land here on the start path,
                   which is where an operator reads them. */
                await RunStoreObjectConvergenceSegmentAsync(
                    timescaleConnection, StoreObjectConvergenceStage.Timescale, startupConvergence, stoppingToken);

                /* #3653 (Q12): one line per superseded hourly rollup — where the interval-honest successor's
                   materialized floor stands against the legacy's and against the hourly tier's horizon — so the
                   hand-over from legacy to successor is visible in the log while it happens. An instrument,
                   not a mechanism (nothing here changes a policy or drops a relation; SupersededHourlyRollups
                   states why the legacy trio stays). AFTER the ensure so both relations exist on the start that
                   creates the successors; two min(bucket) reads per pair, failure-isolated, never fatal. */
                await TimescaleSupport.LogSupersededHourlyRollupCoverageAsync(timescaleConnection, _logger, DateTime.UtcNow, stoppingToken);

                /* #3653 (Q10): the materialization-hole repair — for every continuous aggregate, the bucket ranges
                   inside its materialized span where the source holds rows and the aggregate holds none (the
                   pre-outage tail a refresh policy can never reach back to once its window has moved past it),
                   each closed with ONE forced refresh over exactly its bounds, oldest first, capped per aggregate
                   at one policy window's worth of buckets. AFTER the ensure so every aggregate exists, and BEFORE
                   the compression and retention ensures below, on its own connection and NOT awaited — the scan
                   is a few hundred index probes per aggregate and starts at once, but a capped repair on the
                   heaviest aggregate is a policy run's worth of work, and holding a restarted service dark for
                   it is the trade #1757 already declined for the baseline backfill. Ordering against the
                   retention re-arm is not load-bearing (TimescaleSupport.MaterializationHoles states why in
                   full): under #4299's design the raw purge no longer runs on its own schedule at all, so this
                   scan and EnsureRetentionPoliciesAsync can run in either order on the same start without a
                   race — the only trigger for an actual purge is the service's OWN hourly Periodic pass, which
                   cannot fire before the next hourly tick. The first start after the upgrade, which still runs
                   whichever raw job the old scheduled-based code had already armed once, and a DBA's own
                   alter_job/run_job against a raw job, are the two named exceptions to that gate; both are
                   unaffected by this ordering either way. Drained with the command loop at shutdown. */
                holeRepair = RunMaterializationHoleRepairAsync(postgres, stoppingToken);

                /* #4812: the reshape sweep above rebuilt collection_health_hourly WITH NO DATA if it predated the
                   note column. Its refresh policy has no initial_start, so the scheduler launches the policy's first
                   run AT ONCE, not up to an hour later; until that run lands, every fleet read (the Overview cards,
                   get_fleet_overview) scans a week of raw. ONE refresh of the policy's window from here can meet
                   that run: TimescaleDB raises 55P03 rather than waiting, and the refresh logs it at Information
                   and steps aside, because the policy is doing the same refresh. DELIBERATELY LAUNCHED, NOT
                   AWAITED, on its own connection: it reads about a gigabyte on a 44-server store and a restarted
                   service must not go dark for it. A no-op once the rollup holds anything. Drained with the
                   command loop at shutdown. */
                collectionHealthWarm = RunCollectionHealthRollupWarmAsync(postgres, stoppingToken);

                /* #3817 segment two: the ensures that must run AFTER the hole repair is LAUNCHED — #3597's
                   dedup-index sweep and #3581's aggregate compression (which carries #3620's chunk-width
                   ensure inside it). Both need the aggregates to exist, and the repair's comment above places
                   itself ahead of both deliberately; splitting the walk here is what preserves that without
                   giving the hole repair a position in a list of idempotent ensures it is not one of. Same
                   list, same tally, next slice. */
                await RunStoreObjectConvergenceSegmentAsync(
                    timescaleConnection, StoreObjectConvergenceStage.TimescaleAfterRepairLaunch, startupConvergence, stoppingToken);

                /* AFTER the CAGGs exist: the tiered retention (raw 4d, hourly HISTORY CAGGs 90d per #1937, daily
                   history kept indefinitely; the interval-dedup and baseline tiers carry their own, #1958). The
                   START-PATH pass: it creates what is missing, converges horizons and judges every policy's
                   coverage gate, and writes one WARNING per held policy plus the "Retention evaluation at
                   startup:" tally. KEPT as-is under #3812, which ADDED the hourly re-evaluation on the
                   compression tick (ReevaluateRetentionPoliciesAsync) rather than moving this; a fresh store
                   still gets its policies before the first collection lands, and a restart is still when an
                   operator is reading the log. */
                await TimescaleSupport.EnsureRetentionPoliciesAsync(timescaleConnection, _logger, stoppingToken);

                /* #1757: the baseline aggregates ship WITH NO DATA and their refresh policy only ever covers
                   the trailing 1-day refresh window, so without this they would answer a 30-day question with
                   a day of supply. DELIBERATELY LAUNCHED, NOT AWAITED: it is a bulk materialization whose
                   cost scales with however much history the store already had, and every step below — the
                   composer tuning, the delta re-seed, the collection loop itself — is sequenced after it.
                   Awaiting it here would take a restarted service dark for as long as the backfill runs,
                   which is exactly when an operator is most likely to be restarting it. Coverage-gated, so it
                   is a no-op on every start after the first and resumes where it left off if cut short.
                   Drained with the command loop at shutdown. */
                baselineBackfill = RunBaselineBackfillAsync(postgres, stoppingToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A partially-converted store is fine: DELETE-based retention works on hypertables
               too, so falling back to plain-PG mode is always safe. */
            _timescaleAvailable = false;
            _logger.LogWarning("TimescaleDB setup failed — continuing in plain-PostgreSQL mode: {Message}", ex.Message);
        }

        /* #1757: the provider reads the seven baseline relations BY NAME — a missing one throws,
           ComputeBaselinesAsync swallows it, and that family silently returns an empty baseline. So every
           relation is guaranteed to EXIST here, with a plain view over the same select filling any gap.

           DELIBERATELY UNGATED on _timescaleAvailable. Three ways a gap appears and only one of them is "no
           TimescaleDB": the extension is absent, the TimescaleDB block threw partway, or the block ran fine
           and EnsureContinuousAggregatesAsync's per-aggregate failure isolation left one aggregate unbuilt.
           That last one is the easiest to miss and would take exactly one family down on an otherwise healthy
           store. The call is per-view and probes for an existing relation first, so it never touches a real
           aggregate. Its own connection — the block's is already disposed by here.

           The retirement drop (#2007) rides the same ungated block for the same reason: the retired CPU/IO
           baseline relations exist as CAGGs on TimescaleDB stores and as plain fallback views on
           plain-PostgreSQL stores, and both shapes must go. BEFORE the ensure call, though order is not
           load-bearing — the retired names left BaselineAggregates, so the ensure sweep can never recreate
           them. */
        try
        {
            await using var fallbackConnection = await postgres.OpenConnectionAsync(stoppingToken);
            /* #3817: segment three of the one convergence list — the two baseline steps, in the same order,
               off the same list, so the hourly pass runs them too. The UNGATED property is a property of this
               CALL SITE (after the TimescaleDB block's catch, on every path), not of the list: the steps are
               tagged Ungated and the hourly pass runs them on its own connection for the same reason. */
            await RunStoreObjectConvergenceSegmentAsync(
                fallbackConnection, StoreObjectConvergenceStage.Ungated, startupConvergence, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Baseline relations could not be checked or backed by plain views — some anomaly baselines may silently return nothing: {Message}",
                ex.Message);
        }

        /* Composer + analyze_*_plan performance tuning (covering indexes + per-table autovacuum-insert override) —
           idempotent RUNTIME setup, NOT a versioned migration: results-invariant perf, so it must not bump
           StorageVersion and gate the Viewer's schema check on indexes it does not need (the role-GUC provisioning
           reasoning). AFTER the TimescaleDB block so the collector tables are already hypertables when indexed
           (CREATE INDEX / ALTER TABLE SET propagate to all chunks), BEFORE collectors so a first build never
           contends with live inserts. Its own try/catch: a failure degrades to un-tuned (slower) queries, never
           fatal (the same optional-feature discipline as the TimescaleDB block above). */
        try
        {
            await using var tuningConnection = await postgres.OpenConnectionAsync(stoppingToken);
            /* #3817: segment four — the tuning pass, off the same list. The whole-block wrap the issue names
               (one failure costing every index in the pass) is unchanged HERE and unchanged on the hourly
               pass, because it is inside PgTableTuning.ApplyAsync's own per-statement isolation rather than
               at this call site; what changes is that a pass which failed is retried within the hour instead
               of at the next restart. */
            await RunStoreObjectConvergenceSegmentAsync(
                tuningConnection, StoreObjectConvergenceStage.Tuning, startupConvergence, stoppingToken);
            // The retained sql_handle->module map (#1568 object_name for OLD query_stats windows the CAGG serves,
            // after procedure_stats raw drops at 4d): create the table here, because readers need it to exist.
            /* #5578: ONLY the cheap table DDL runs on this path. The refresh that fills the map reads procedure_stats
               (55.6 s at start on a large store, and a flat two days when there is no watermark) and used to be
               awaited here, which held every collector back by that much on every restart. No collector reads the
               map, and the map keeps every name it has, so a map a little behind costs nothing. The refresh runs
               AFTER the collectors start, from the sweep loop's first pass: the hourly store-maintenance tick
               (RefreshModuleMapRecentAsync) and the daily purge (RefreshAsync) both fire in that first pass and both
               are launched fire-and-track, so neither can delay a collector. */
            /* NOT a convergence-list step (#3817): the table ensure is a cheap idempotent DDL, and the map's data
               refresh is not a store object at all. */
            await DarlingModuleMap.EnsureTableAsync(tuningConnection, _logger, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning("Composer performance tuning failed — queries fall back to un-indexed scans: {Message}", ex.Message);
        }

        /* #3817: the ONE line this start's convergence pass writes, UNCONDITIONALLY — the #3756 discipline,
           and the same line the hourly pass writes (LogStoreObjectConvergence is the single owner of the
           template, so the two cadences cannot drift into two shapes). A start that changed nothing still
           writes it, with zeros, because a pass whose negative outcome is indistinguishable from its
           non-execution has not reported: before this, "did the aggregates get ensured on this start" was
           answered by reading eleven per-object lines and inferring the absence of a twelfth. Written here
           rather than inside the try blocks so it reports every segment including the ones that threw —
           a segment's catch above is what makes its steps' absence visible in this count. */
        LogStoreObjectConvergence(startupConvergence, startupConvergenceClock.ElapsedMilliseconds, startup: true);

        /* Restart continuity: re-seed delta baselines from the store (the Postgres twin of Lite's
           DuckDB seeding) so the first cycle after a service restart produces real deltas instead
           of zeroes. A seed failure logs a warning and collection proceeds with first-cycle-zero. */
        var deltas = new DarlingDeltaCalculator();
        await deltas.SeedFromStoreAsync(postgres, _logger, stoppingToken);
        _deltas = deltas;

        /* Control-plane Stage 1: SEED the config store from darling.json once (idempotent; only empty
           sections), then read the store view and make it authoritative — the held DarlingConfig is
           mutated in place so the alert-settings/capture-plans/analysis seams below all reflect the
           store. Store-unreachable degrades to the darling.json-loaded config (never worse than before).
           The initial server set comes from config_monitored_servers WHERE is_enabled = TRUE (post-seed
           equivalent to darling.json, but store-authoritative); the config_version read here is the
           reload baseline, so the seed's own version bumps do not trigger a spurious first-sweep reload. */
        /* #5655: say in the store which version this service host runs and when the process started, so the viewers can show the
           SERVICE's version (they used to show their own). Its own table, never config_service (that row's self-bump trigger would
           reload every service). A failed write logs a warning and the start goes on; the sweep below tries it again until it lands. */
        var serviceVersionStart = ProcessStartUtc();
        var serviceVersionWriter = new DarlingServiceVersionWriter((retry, ct) =>
            DarlingServiceVersionRecorder.RecordAsync(postgres, _logger, serviceVersionStart, ct, retry));
        await serviceVersionWriter.TryAsync(stoppingToken);

        var configProvider = new StoreConfigProvider(postgres, _logger);
        await configProvider.SeedIfEmptyAsync(config, stoppingToken);
        /* #5366: the service's password key, after migrations, role provisioning and the seed, and before the first config view (#5455:
           that view reads the legacy pins this start takes, and a view built before them leaves every saved old-format password
           unpinned for the whole run). It reads only config.Postgres, which the view never changes. Before any collection or reload
           can need it: loaded (or made and published) here, with the legacy pin snapshot, and the ring every writer and
           the resolver seal and open through is set. Until this returns the ring refuses with the "still loading" reason.
           Never throws; a key that cannot be used is a refusing ring with the reason logged and recorded in the store. */
        _passwordKeyRuntime = await DarlingPasswordKeyRuntime.StartForServiceAsync(
            config, DarlingConfig.ResolveConfigPath(), postgres, _logger, stoppingToken);
        /* #6715: the seed above copies the webhook values from darling.json as plain text, because the ring it would seal them with
           needs the seeded store and loads only now. Seal those (and any an older build left) here, before the first view reads the
           row, so this host's reload baseline already includes the write's config_version bump. Never stops the start. */
        await configProvider.SealPlainWebhookSettingsAsync(_passwordKeyRuntime.Ring, stoppingToken);
        /* #5456: a pin step that could not run is tried again on every sweep, and until it does the re-enter line says to wait.
           Set before the first view so that view's line already knows; the pins it takes bump config_version, which every
           host's reload beacon picks up. */
        configProvider.LegacyPinsWaiting = () => _passwordKeyRuntime?.PinsWaiting == true;
        var initialView = await configProvider.LoadViewAsync(config, stoppingToken);
        IReadOnlyList<MonitoredServer> initialServers = config.Servers;
        if (initialView is not null)
        {
            StoreConfigProvider.ApplyToConfig(config, initialView);
            _collectorState.SigningStatus.Publish(config.Webhooks, config.NotificationRoutes, DateTime.UtcNow); // #5998: presence and dates for the web page, no key text
            _scheduleOverrides = initialView.ScheduleOverrides;
            _lastConfigVersion = initialView.ConfigVersion;
            /* Stage 2: honor config_service.paused from the very first sweep (a service that was paused
               before a restart comes back up paused). */
            _paused = initialView.Paused;
            /* Reconcile the observed collect.servers.is_enabled to the desired state up front, so a server
               disabled in the store while the service was down shows disabled even though it never connects
               (never upserts) this run. */
            await DarlingObservability.SyncServerEnabledStatesAsync(postgres, _logger, stoppingToken);
            /* Post-seed the store carries darling.json's servers; fall back to the file only if the store
               read is empty (a partially-seeded store), so the service never starts up monitoring nothing. */
            if (initialView.EnabledServers.Count > 0)
            {
                initialServers = initialView.EnabledServers;
            }
        }

        /* #1560/#1562: publish the effective MCP + web enable/port (store-authoritative when the view loaded,
           else the darling.json values) so the two hosts' supervisors start from the same truth the worker
           holds — including on a store-unreachable boot. Re-published on every reload below. */
        _mcpState.Publish(config.Mcp.Enabled, config.Mcp.Port);
        _webState.Publish(config.Web.Enabled, config.Web.Port);
        /* The SMTP settings in force (the store's when the view loaded, else darling.json's): a setup that sends its
           login without encryption is named in the log, because no settings window is open for it. */
        DarlingSmtpWarning.Log(config.Smtp, _logger);
        /* #2298: publish the effective server set the same way — store-authoritative when the view loaded,
           else the darling.json servers, which is what this run will actually collect from either way. The
           MCP host's plan-fetch resolver reads this instead of re-reading the store as the mcp role. */
        _registryState.Publish(initialServers);
        /* #6468: an install that has no servers yet is a normal first start, so say so once, at Information, and name the ways to add
           one. The service keeps running: it picks up the first server the Viewer or the web dashboard registers, with no restart. */
        if (initialServers.Count == 0)
        {
            _logger.LogInformation(
                "No monitored servers are configured yet. Add one in the Viewer (Add Server) or with " +
                "PerformanceMonitor.Darling.Service.exe --add-server; the service starts collecting from it without a restart.");
        }
        /* #5812 (L1): the PLAN_REGRESSION candidate cache evicts the entries of servers outside this set on its next store. */
        PerformanceMonitor.Darling.Analysis.PgFactCollector.PlanRegressionCandidateCache.Shared.RetainServers(initialServers.Select(s => s.ServerId));

        /* Capture-plans is read live (() => config.CapturePlans) so a store reload of
           config_service.capture_plans is honored on the next collector cycle without rebuilding.
           CollectSchemaChangeEvents is a file-only knob (darling.json), read the same way for symmetry —
           default true keeps every SKU collecting Object DDL; set false to silence a benchmark box's flood. */
        /* #4004: the store's log-hash key, loaded ONCE here and shared by every run that hashes log text (pg_log_events
           on the pg_read_file and RDS routes), so raw_line_hash and statement_fingerprint are keyed with a secret the
           store never holds. Generated when none exists, and replaced only when its directory was found open to other
           users, by whichever look found it so (role provisioning above, a host, or this load), which removes the key
           there and then: null means the file could not be used, the reason is already logged, and those runs refuse
           rather than hash without it. */
        var logHashKeyLoad = DarlingLogHashKeyFile.LoadForService(config, DarlingConfig.ResolveConfigPath(), _logger);
        var logHashKey = logHashKeyLoad.Key;
        /* #4004 review, round 3: a key that replaced one the directory check discarded is noted on the collection-log
           row of the first pg_log_events run after this, and only that run (RunOneAsync takes it). */
        _logHashKeyRotation.Arm(DarlingLogHashKeyFile.RotationNote(logHashKeyLoad));
        /* #5452: which AWS roles this run may assume, set once here and read by the RDS path, the way the password key's
           ring is. darling.json's allowedAwsRoles plus every role a darling.json server names; config.Servers is the
           file's list (the store view never replaces it), and the file is read at start, so an edit applies on restart. */
        Targets.AwsRoleAllowlist.Current = Targets.AwsRoleAllowlist.FromConfig(config);
        var runner = new DarlingCollectorRunner(postgres, deltas, _logger, () => config.CapturePlans, () => config.CollectSchemaChangeEvents,
            () => StoreConfigProvider.ClampTextBudgetMb(config.QueryStoreTextBudgetMb),
            /* #2171: live provider like its siblings — a store reload flipping plan_xml_compression
               takes effect on the next write batch, no restart. */
            compressPlanContent: () => !string.Equals(config.PlanXmlCompression, "none", StringComparison.OrdinalIgnoreCase),
            /* #2862: the procedure_stats plan-capture cadence, clamped at the provider like the V59 budget
               above so the runner never sees an out-of-range interval. A file-only knob today, but read
               live like its siblings, so setting it to 1 restores every-cycle plan capture and promoting
               it to a store column later needs no change here. */
            procedureStatsPlanCycleInterval: () => StoreConfigProvider.ClampProcedureStatsPlanCycleInterval(config.ProcedureStatsPlanCycleInterval),
            /* #5158: query_stats fetches plan XML only for plans this host has not committed. A file-only knob:
               read through a provider each cycle, but darling.json is loaded once, so an edit needs a restart.
               false restores the inline capture. */
            queryStatsDeferredPlanFetch: () => config.QueryStatsDeferredPlanFetch,
            /* #5158: procedure_stats' deferred plan fetch: off, shadow or on. A file-only knob: read through a provider each
               cycle, but darling.json is loaded once, so an edit needs a restart. The runner treats an unrecognized value
               as off. */
            procedureStatsDeferredPlanFetch: () => config.ProcedureStatsDeferredPlanFetch,
            /* #3477: the per-collector database scope, resolved live against the SAME _scheduleOverrides
               the cadence gate reads — one source, so the scope a run collects under and the schedule it
               was dispatched under can never come from two different reloads. */
            databaseScope: (collectorName, serverId) => StoreConfigProvider.ResolveDatabaseScope(collectorName, serverId, _scheduleOverrides),
            logHashKey: logHashKey,
            /* #4659: shared with the alert read adapter (BuildAlertEngine). */
            queryStoreWriteFence: _queryStoreWriteFence,
            /* The databases monitored as their own servers, from the same live store set the alert sweep uses:
               the long-query trace leaves them to their own registrations, in its lifecycle and its read. */
            separatelyMonitoredDatabases: runtime => AzureMasterScope.SeparatelyMonitoredDatabases(
                runtime.Target.IsAzureSqlDb,
                runtime.ServerId.ToString(CultureInfo.InvariantCulture),
                runtime.Config.Host,
                runtime.Config.Database,
                LiveAlertTargets(_registryState.Read()?.Servers)),
            /* #4961: this install's id, made at start before any worker runs. */
            installId: () => _installId,
            /* #6211: live provider like its siblings, so a store change to query_store_fetch_top_n takes effect on the next cycle, no restart. */
            queryStoreFetchTopN: () => StoreConfigProvider.ClampQueryStoreFetchTopN(config.QueryStoreFetchTopN));
        /* #7067: one gate, one clock, for the worker's and the runner's repeated warnings. */
        runner.RepeatLog = RepeatLog;
        _runner = runner;
        var servers = new List<ServerLoopState>();
        /* #1581 cold-start stagger: capture ONE startup instant so every initial server's first-sweep offset is
           measured from the same base — the deterministic per-server ColdStartFirstSweepDue then spreads the
           fleet's FIRST catch-up bodies across ColdStartSpreadSeconds instead of launching all of them in a
           single sweep tick and slamming the N=4 gate (the field herd). Reconcile-ADDED servers below keep the
           MinValue default and launch promptly. */
        var coldStartInstant = DateTime.UtcNow;
        foreach (var server in initialServers)
        {
            servers.Add(new ServerLoopState
            {
                Config = server,
                FirstSweepDueUtc = ColdStartFirstSweepDue(
                    coldStartInstant, server.ServerId),
            });
        }

        /* Phase-5 slice D: the shared alert engine, wired to the PG-backed stores (V3) and the
           shared email/webhook delivery. Constructed once — the engine holds the per-server
           edge-trigger state for the service's lifetime. The settings/history/webhook pieces
           are hoisted here because the AN3 analysis-notification path below shares them. The mute
           service is hoisted too so a reload can re-LoadAsync() it (closes F16 — the engine holds
           its IsAlertMuted delegate, so refreshing the same instance's cache mutes the next sweep). */
        var alertSettings = new DarlingAlertSettings(config, _logger);
        var historyStore = new PgAlertHistoryStore(postgres, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, _logger);
        var webhookAlertService = new WebhookAlertService(
            alertSettings, DarlingAlertDeliverer.Branding,
            _loggerFactory.CreateLogger<WebhookAlertService>(), historyStore);
        var muteRuleService = new MuteRuleService(
            new PgMuteRuleStore(postgres), _loggerFactory.CreateLogger<MuteRuleService>());
        await LoadMuteRulesAsync(muteRuleService);
        await LoadThresholdOverridesAsync();

        /* #5632 Phase A: Level 1 investigations. The dispatcher opens or joins a request after an alert row is written (the
           deliverer and the finding sender below call it), and the sweep loop expires lapsed leases and old requests. Both
           use a data source of their own whose connections carry a 5-second lock_timeout, so a held lock on the
           investigation tables costs an alert pass at most that long and touches no other connection's settings. */
        await using var investigationPostgres = NpgsqlDataSource.Create(
            InvestigationDispatcher.WithLockTimeout(
                DarlingStoreConnection.PinSessionTimeZoneUtc(
                    DarlingStoreConnection.WithApplicationName(
                        DarlingStoreConnection.EnsureStoreSearchPath(storeConnectionString), DarlingManagedPostgres.ServiceApplicationName))));
        var investigationStore = new InvestigationRequestStore(
            investigationPostgres, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds);
        /* #5821: the generic webhook's {{investigation_json}} masks the stored brief at the level the MCP tools serve; an
           unrecognized value runs at the strictest level, as the MCP host does (it logs that once, so this does not). */
        /* #6005: the deadlock rollup (written by the alert pass), the agent gate it feeds and the 4-hour digest. */
        var deadlockRollup = new PerformanceMonitor.Darling.Service.Investigations.DarlingDeadlockRollup(
            new DeadlockSignatureStore(investigationPostgres, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds));
        _deadlockRollup = deadlockRollup;
        var investigationDispatcher = new InvestigationDispatcher(
            investigationStore, alertSettings, _logger,
            McpDataDetailModes.TryParse(config.Mcp.DataDetail, out McpDataDetail dataDetail)
                ? dataDetail
                : McpDataDetail.MetadataOnly,
            /* #5822: the long-poll's wake-up, fired when a request opens. */
            openSignal: PerformanceMonitor.Darling.Service.Investigations.InvestigationOpenSignal.Shared,
            agentGate: deadlockRollup.AllowsAgentRequestAsync,
            keys: _pseudonymKeys);

        /* The shared record-and-send deliverer — hoisted so BOTH the shared alert engine and the Stage 4
           service self-alerts (DarlingSelfAlertEvaluator) fire through the SAME instance: identical
           email/webhook delivery, per-fingerprint delivery cooldown, and history-seeded restart replay. */
        var deliverer = new DarlingAlertDeliverer(
            alertSettings, historyStore, webhookAlertService, _logger,
            /* #1236: map a fired alert's ServerKey (the storage-name hash the engine keys on) back to the live
               server's per-server delivery override, under the servers lock (mirrors FetchFailedJobsAsync). A
               store reload swaps ServerLoopState.Config, so this always reads the current override. */
            resolveServerOverride: serverKey =>
            {
                if (!int.TryParse(serverKey, NumberStyles.Integer, CultureInfo.InvariantCulture, out var id))
                {
                    return null;
                }

                lock (_serversLock)
                {
                    return servers
                        .Find(s => s.Config.ServerId == id)
                        ?.Config.AlertDeliveryModeOverride;
                }
            },
            investigationDispatcher: investigationDispatcher,
            /* #5825: the same mute rules the fires obey stop an alert's clear. */
            isAlertMuted: muteRuleService.IsAlertMuted);
        var engine = BuildAlertEngine(config, servers, alertSettings, historyStore, muteRuleService, deliverer, deadlockRollup);

        /* Held for the PostgreSQL predictors, which deliver alongside the shared engine rather than
           through it (see EvaluatePostgresAlertsAsync). Same deliverer instance, so a PostgreSQL alert
           lands in the same history and obeys the same mute rules as an engine-emitted one — the point
           of reusing it rather than building a second delivery path. */
        _alertDeliverer = deliverer;
        /* #2711: the Postgres Deadlocks/Blocking resolution path writes history directly (see
           _historyStore's doc comment) - same instance the engine's own resolutionCallback closure
           over historyStore uses, so a restart-time history read sees both engines' rows regardless of
           which wrote them. */
        _historyStore = historyStore;
        /* Same instance the engine binds, so a mute-rule reload mutes the PostgreSQL predictors on the next
           sweep exactly as it mutes every SQL Server family. */
        _isAlertMuted = muteRuleService.IsAlertMuted;
        _alertCooldownMinutes = alertSettings.CooldownMinutes;

        /* #3285: the user-authored custom-alert evaluator. A rule's compose metric runs on a dedicated
           VIEWER-role pool (which carries the statement_timeout cap and the least-privilege ACL) — never the
           owner pool. #3970: two deployments can supply that pool — a managed Windows store's provisioned
           viewer credential (written by EnsureProvisionedAsync above), or the Linux compose store's own viewer
           role (DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync: this start's, provisioned above, or a
           trusted credential an earlier start left, accepted before use — never the owner login, unlike a
           host's own fallback). A bring-your-own store stays out either way: its viewer role comes from
           tools/provision-roles.sql, which does not create config.record_custom_alert_resolution. */
        string? customAlertViewerConnString = null;
        if (OperatingSystem.IsWindows() && config.Postgres.Managed)
        {
            /* The evaluator runs on the web login: it writes the resolution row through record_custom_alert_resolution, which the read-only viewer login cannot execute. */
            customAlertViewerConnString = DarlingManagedPostgres.TryBuildWebConnectionStringFromStoredCredential(config.Postgres);
        }
        else if (!config.Postgres.Managed && Hosting.DarlingHostBinding.IsRunningInContainer)
        {
            customAlertViewerConnString = await DarlingStoreLogins.ResolveComposeCustomAlertViewerAsync(
                config.Postgres.ConnectionString, _logger, stoppingToken);
        }

        await using var customAlertViewerSource =
            customAlertViewerConnString is not null
                ? NpgsqlDataSource.Create(DarlingStoreConnection.PinSessionTimeZoneUtc(customAlertViewerConnString))
                : null;
        if (customAlertViewerSource is not null)
        {
            _customAlertEvaluator = new CustomAlertEvaluator(
                new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds),
                new CustomAlertStateStore(postgres, McpCommandDeadlines.ReadSeconds),
                customAlertViewerSource,
                deliverer,
                muteRuleService.IsAlertMuted,
                /* #3464: the same live master switch every other family consults — a closure over the
                   settings adapter's pass-through read, so a control-plane flip gates the very next sweep. */
                alertsEnabled: () => alertSettings.AlertsEnabled,
                historyStore,
                (int)s_customAlertSweepInterval.TotalSeconds,
                s_customAlertSweepInterval,
                _loggerFactory.CreateLogger<CustomAlertEvaluator>());
            _logger.LogInformation(
                "Custom alert evaluator ready (#3285) — evaluating user rules on the viewer-role pool every {Seconds}s.",
                (int)s_customAlertSweepInterval.TotalSeconds);
        }
        else
        {
            _logger.LogInformation(
                "Custom alert evaluator not started: this deployment has no viewer-role login to evaluate custom alerts with — a managed Windows store or the Linux compose store's own provisioned viewer role are required; a bring-your-own store is not (#3285, #3970).");
        }

        /* Stage 4: the service self-alerts, over the SAME deliverer + history + mute check the engine uses.
           collection-stopped / capture-down are polled from collection_log on the alert cadence below;
           connection lost/restored fire on the connect edges in TryConnectAsync. */
        _selfAlerts = new DarlingSelfAlertEvaluator(
            alertSettings, deliverer, historyStore, muteRuleService.IsAlertMuted, _logger,
            /* V20: the connect-edge Server-Unreachable/Restored delivery honors the notify toggle, read live
               through the same by-reference alertSettings seam a store reload hot-swaps. */
            notifyConnectionChanges: () => alertSettings.NotifyConnectionChanges,
            notifyConnectionDownAtStartup: () => alertSettings.NotifyConnectionDownAtStartup,
            connectionRefireMinutes: () => alertSettings.ConnectionRefireMinutes,
            /* #991: the Availability Group alert family reads its master switch and both thresholds live
               through the same by-reference alertSettings seam, so a store edit takes effect on the next sweep
               without a restart (and the clamps live on the settings properties, not here). */
            notifyAgHealth: () => alertSettings.NotifyAgHealth,
            /* #6875: the Identity Capacity switch and bars, read live, and the per-server and per-database override resolution. Off until an operator turns it on. */
            identityCapacityEnabled: () => alertSettings.IdentityCapacityEnabled,
            identityCapacityWarningPercent: () => alertSettings.IdentityCapacityWarningPercent,
            identityCapacityCriticalPercent: () => alertSettings.IdentityCapacityCriticalPercent,
            resolveThreshold: (wire, serverKey, database, global) => _thresholdOverrides.ResolveWire(wire, serverKey, database, global),
            /* #6874: the Agent Alert Gaps switch, read live. Off until an operator turns it on. */
            agentAlertSetupEnabled: () => alertSettings.AgentAlertGapsEnabled,
            /* #6873: the three database protection switches, read live. Off until an operator turns each on. */
            backupOverdueEnabled: () => alertSettings.BackupOverdueEnabled,
            logBackupOverdueEnabled: () => alertSettings.LogBackupOverdueEnabled,
            checkDbOverdueEnabled: () => alertSettings.CheckDbOverdueEnabled,
            agLagAlertSeconds: () => alertSettings.AgLagAlertSeconds,
            agRedoQueueAlertKb: () => alertSettings.AgRedoQueueAlertKb,
            agDisconnectRefireMinutes: () => alertSettings.AgDisconnectRefireMinutes,
            /* #2136: the cadence warning threshold, read live like the AG seams (clamped on the property). */
            storeJobCadenceWarnPercent: () => alertSettings.StoreJobCadenceWarnPercent,
            /* #3297 (V119): the Retention Held tiers, read live like the cadence knob above — which is what
               makes them tunable at all. Until this rung they were compile-time constants, so #3296's
               operator got an hourly CRITICAL with nothing in Settings to reach. Clamped on the properties. */
            retentionHoldWarnRatio: () => alertSettings.RetentionHoldWarnRatio,
            retentionHoldCriticalRatio: () => alertSettings.RetentionHoldCriticalRatio,
            /* #3013: the same process counter the shared engine tallies on, so one number covers both
               halves of a server's alert work. */
            readFailures: AlertReadFailureCounter.Shared,
            /* #3500: the opt-in store label, straight from the file's peers block — NOT a Func like the
               store-backed knobs above, because the peers block is file-only and restart-only, so a live
               read would claim a hot-reload the config cannot deliver. Null/blank means the evaluator
               fires under the shipped "Monitor Store" constant, byte-identical to every release before
               the field existed. */
            storeName: config.Peers?.StoreName,
            /* #3580: the two daily documents' delivered-today stamps, in the store's own key/value state
               table, so a restart of this process does not re-announce a digest or rollup the previous
               process delivered an hour ago — and does re-attempt one whose delivery failed. */
            deliveryStamps: new PgSelfAlertDeliveryStampStore(postgres, _logger),
            /* #4750: the webhook channels' failures in a row, read from the SAME service the deliverer sends
               through, so "Notification Channel Failing" judges the counts the sends actually move. Counts
               only: a webhook error can carry the endpoint URL, and the URL is the credential. */
            webhookChannelFailures: webhookAlertService.GetChannelFailureCounts);

        /* #7041: the Daily Health Report. The settings are read live (a store edit lands on the next 15-second check) and the
           builder is the one source the scheduled send and send_daily_health_report_now share. */
        _selfAlerts.DailyReportSchedule = () => new PerformanceMonitor.Darling.Service.Reports.DailyHealthReportSchedule(
            alertSettings.DailyReportEnabled, alertSettings.DailyReportSendAt, alertSettings.DailyReportTimeZone, alertSettings.DailyReportTitle);
        _selfAlerts.DailyReportSource = CreateDailyHealthReportSource(postgres);
        if (_dailyReportHub is not null)
        {
            _dailyReportHub.Evaluator = _selfAlerts;
        }

        /* #1706: report this start's store runtime upgrade, now that there IS an alert engine to report it
           through. Fired once, here, and never re-evaluated — the store is down while an upgrade runs, so
           its start could only ever be a log line, and by the time this line is reached both terminal states
           (upgraded, or reverted and still running) have a live store to alert from. */
        if (storeUpgradeReport is not null)
        {
            await _selfAlerts.EvaluateStoreUpgradeAsync(storeUpgradeReport, stoppingToken);
        }

        /* #5450: the gap before this start, the same once-per-start event. The evaluator holds the 15-minute gate. */
        await _selfAlerts.EvaluateCollectionGapAtStartAsync(collectionGapReport, stoppingToken);

        /* #3908: the store's TimescaleDB extension, the same once-per-start event. */
        if (storeTimescaleReport is not null)
        {
            await _selfAlerts.EvaluateStoreTimescaleAsync(storeTimescaleReport, stoppingToken);
        }

        /* Phase-5 analysis slice AN3: the analysis pipeline's shared pieces, constructed once.
           The plan fetcher resolves a finding's serverId to the CONNECTED runtime's connection
           string (the PgPlanFetcher seam — null for an unknown/disconnected server degrades the
           fetch like Lite's ServerManager miss). The shared AnalysisNotificationService routes
           high-severity findings through DarlingFindingAlertSender (email + webhook + history,
           Lite's cadence); the serverId resolver is Lite's shape (the finding's int id as a
           string), no silencing predicate and no tray sink (headless). */
        var planFetcher = new PgPlanFetcher(
            /* Enumerated from the command loop (analyze_now) as well as the main loop, so guard the read
               against a concurrent reconcile add/remove. Held only for the lookup, never across the fetch. */
            serverId =>
            {
                lock (_serversLock)
                {
                    return servers
                        .Select(s => s.Runtime)
                        .FirstOrDefault(r => r is not null && r.ServerId == serverId)?.ConnectionString;
                }
            },
            _logger);
        /* #3916 PR B: pages wait out a hold-back window, so the flush re-reads the mute registry — a mute
           written inside the window drops the queued page. The read fails OPEN (logged, "not muted"): the
           finding already passed the queue-time mute filter in PgFindingStore. */
        var muteReadStore = new PgFindingStore(postgres, _logger);
        var notificationService = new AnalysisNotificationService(
            new DarlingFindingAlertSender(alertSettings, historyStore, webhookAlertService, _logger, investigationDispatcher),
            alertSettings,
            finding => finding.ServerId.ToString(CultureInfo.InvariantCulture),
            _loggerFactory.CreateLogger<AnalysisNotificationService>(),
            isStoryMuted: async (serverId, storyPathHash) =>
                (await muteReadStore.GetMutedStoryHashesAsync(serverId)).Contains(storyPathHash));

        /* #2138 phase 1: the auto force-plan bot, hooked onto the SCHEDULED analysis pass only (the
           interactive analyze_now command deliberately does not trigger it — an operator poking a
           server should not spend the bot's blast-radius budget). Settings are file-level and OFF by
           default. The bot is constructed with the JOURNAL and nothing else — no executor, no
           connection factory — because phase 1 has no write path at all; the store it writes to is
           the monitoring store, never a monitored server. */
        _planForceBot = new PlanForceBot(
            new PgPlanForceActionStore(postgres, _loggerFactory.CreateLogger<PgPlanForceActionStore>()),
            config.ForcePlanBot.ToSettings(),
            _loggerFactory.CreateLogger<PlanForceBot>());

        /* Command plane (Stage 2): the executor claims/executes/reports config_command rows on its OWN
           5-second loop, concurrent with the collection sweep, so a slow command never stalls collection.
           The host lets snapshot_now/analyze_now reach the LIVE loop (the running server set + runner +
           analysis pieces) without the executor touching that mutable state directly; every other command
           only writes the config.* tables and rides the reload beacon. Launched here and awaited after the
           collection loop stops so both drain cleanly on shutdown. */
        var commandHost = new WorkerCommandHost(this, servers, runner, planFetcher, notificationService, config, stoppingToken);
        var serviceInstance = $"{Environment.MachineName}:{Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}";
        var commandExecutor = new DarlingCommandExecutor(postgres, commandHost, serviceInstance, _logger);
        var commandLoop = RunCommandLoopAsync(commandExecutor, stoppingToken);

        /* #5654: the drain for alerts other services queue through config.submit_external_alert. The SAME deliverer, mute
           check and history store as the custom alerts above, so an external alert obeys every route and mute rule and
           lands in the same history. A registered server name maps to its id under the servers lock; an unregistered
           name stays a label in the server_id 0 bucket. */
        var externalAlertStore = new ExternalAlertStore(postgres, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds);
        var externalAlertDrain = new ExternalAlertDrain(
            externalAlertStore,
            alertsEnabled: () => alertSettings.AlertsEnabled,
            (outcome, token) => deliverer.DeliverAndReportAsync(outcome, token),
            (metric, serverName, serverKey, detail, severity, token) => deliverer.TryResolvePagerDutyAsync(metric, serverName, serverKey, detail, severity, token),
            historyStore,
            muteRuleService.IsAlertMuted,
            name =>
            {
                lock (_serversLock)
                {
                    var match = servers.Find(s => string.Equals(s.Config.DisplayName, name, StringComparison.OrdinalIgnoreCase));
                    return match is null ? null : new ExternalAlertServer(match.Config.ServerId, match.Config.DisplayName);
                }
            },
            serviceInstance,
            _loggerFactory.CreateLogger<ExternalAlertDrain>(),
            /* #5825: an external alert's clear goes where its fire went, under the same setting as every clear. */
            deliverResolution: (resolution, options, token) => deliverer.DeliverResolutionAsync(resolution, options, token));

        /* #5654: the drain runs on its own 15-second loop, like the command loop, and is never awaited inside the fleet
           loop, so collection, the custom alerts and the self alerts do not wait on a channel send or a store lock. The
           loop honors the service pause and the master switch itself. Awaited with the command loop at shutdown. */
        var externalAlertLoop = RunExternalAlertLoopAsync(externalAlertDrain, stoppingToken);

        /* #5632 Phase A (ruling R4): the investigation expiry sweep, beside the external alert loop on its own 60-second
           tick. It only expires: it journals a lapsed lease once per claim and moves a request past its expiry, or out of
           claims, to expired. It never reopens a lease; a claim's own WHERE takes a lapsed lease over. */
        /* #6003: after each sweep, the request events go out: the D8 fallback of a request that expired unclaimed, and the
           signed POST of every new event to its agent route; the pass wakes the MCP long-poll for what it found. */
        var investigationEvents = new PerformanceMonitor.Darling.Service.Investigations.InvestigationEventDelivery(
            investigationStore, () => alertSettings.NotificationRoutes,
            new PerformanceMonitor.Darling.Service.Investigations.ServiceInvestigationEventSends(
                webhookAlertService, deliverer, () => _selfAlerts),
            PerformanceMonitor.Darling.Service.Investigations.InvestigationOpenSignal.Shared, _logger,
            /* #6979: the report notice goes to the route's Slack and Teams destinations (the settings' channel when the route has none),
               masked at the level the MCP tools serve, with links only while the web dashboard serves (TriageBaseUrl is empty otherwise). */
            reportNotices: webhookAlertService, defaults: alertSettings, dataDetail: dataDetail,
            baseUrl: () => alertSettings.TriageBaseUrl);
        /* #6234: the change watch rides the sweep's tick; its first pass runs at start, so a watch whose window ended while the service was down is evaluated then. */
        var changeWatchPass = new PerformanceMonitor.Darling.Service.ChangeWatch.ChangeWatchPass(
            postgres,
            new ChangeWatchStore(investigationPostgres, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds),
            investigationDispatcher.Core, _logger, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds);
        var investigationSweepLoop = RunInvestigationSweepLoopAsync(investigationStore, investigationEvents, deadlockRollup, changeWatchPass, stoppingToken);

        /* #6001: the evidence capture, on its own tick. It reads the store only and opens no connection to a monitored server. */
        var investigationEvidenceLoop = RunInvestigationEvidenceLoopAsync(
            new PerformanceMonitor.Darling.Service.Investigations.InvestigationEvidenceCapture(
                investigationPostgres, investigationStore, _logger,
                collectorIntervalMinutes: (collector, serverId) => PerformanceMonitor.Collectors.CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(
                    StoreConfigProvider.ResolveSchedule(collector, serverId, _scheduleOverrides).FrequencyMinutes)), stoppingToken);

        /* #2022 phase 2: the Query Store backfill worker, on its own tick (see s_queryStoreBackfillInterval).
           Fills the two windows the live path discards by design — the 60-minute first-contact tail and
           clamp-bounded outage holes — newest-first, byte-budgeted, strictly BELOW the live path's floor, and
           never past the raw tier's horizon. Plan capture reads the same live provider the runner does. */
        /* The extension flag reaches the backfill because its HORIZON depends on it (#3012): on a plain
           PostgreSQL store there are no hourly rollups for a backdated row to fall out of, so the refresh
           term does not apply and that deployment mode keeps the full raw-tier depth. Passed as a provider
           rather than a value so it cannot capture a stale reading. */
        var queryStoreBackfill = new QueryStoreBackfill(postgres, runner, deltas, _logger,
            () => StoreConfigProvider.ClampTextBudgetMb(config.QueryStoreTextBudgetMb),
            () => _timescaleAvailable,
            /* #5483: the server's effective database_states cadence, resolved live like the alert adapter's, so the
               gone-database check knows how old a snapshot may be. */
            serverId => StoreConfigProvider.ResolveSchedule("database_states", serverId, _scheduleOverrides).FrequencyMinutes,
            /* #5518: the same fence the runner's Query Store writes go through, so the candidate list is read from the
               store only when a write named a database it does not hold, the cut chunk moved or it aged out. */
            _queryStoreWriteFence);
        _queryStoreBackfill = queryStoreBackfill;
        var backfillLoop = RunQueryStoreBackfillLoopAsync(queryStoreBackfill, servers, () => config.QueryStoreBackfillEnabled, stoppingToken);

        /* #5450 proposal 2: the outbound heartbeat, on its own task and connection so a slow or dead URL never touches
           collection. Returns at once when heartbeat.url is not set (the default). The URL is a secret: see
           DarlingHeartbeat for what may reach the log. */
        var heartbeatLoop = new DarlingHeartbeat(_logger).RunAsync(config.Heartbeat, postgres, stoppingToken);

        /* The fleet concurrency gate (#1553 D2): at most N=4 per-server collection bodies open a SQL connection
           at once, so one slow or hung server cannot head-of-line-block the fleet the way the old strictly
           sequential foreach did. Deliberately NOT disposed (CA2000 suppressed, not "fixed" back by an analyzer
           or a later builder): a body still running past the shutdown drain — or a reconcile-removed server's
           body detached from the drain list — would otherwise reach its finally { gate.Release() } on a disposed
           SemaphoreSlim and throw ObjectDisposedException, faulting an unobserved Task. A SemaphoreSlim needs no
           deterministic disposal unless its AvailableWaitHandle is used, which it never is here. */
        /* #2170: the width is now an operator knob, and a SemaphoreSlim cannot be resized — so the gate's
           MAX is the clamp ceiling while its INITIAL count is the configured width. Later changes move
           between the two: widening Releases permits, narrowing absorbs them as in-flight bodies finish
           (see ReconcileSweepGate), which converges without ever blocking this loop or interrupting a
           running collection.

           Starting AT the configured width rather than at the ceiling matters (review catch): reconciling
           down only STARTS the absorber, so a gate born wide would offer ceiling-many permits for the
           window before it retires them — and a restart with many servers simultaneously due (before the
           #1581 cold-start stagger spreads them) is exactly when that window would be spent. Born narrow,
           the window does not exist. */
        var initialSweepWidth = StoreConfigProvider.ClampConcurrentSweeps(config.MaxConcurrentSweeps);
#pragma warning disable CA2000
        var serverSweepGate = new SemaphoreSlim(initialSweepWidth, SweepGateCeiling);
#pragma warning restore CA2000
        SweepGate = serverSweepGate;
        lock (_gateLock)
        {
            /* The permits the gate was never given ARE the absorbed ones — seed both counts so the first
               reconcile computes its delta from reality instead of re-absorbing what was never issued. */
            _gateAbsorbed = SweepGateCeiling - initialSweepWidth;
            _gateDesiredAbsorb = _gateAbsorbed;
        }

        /* #4999: the daily-run cap comes from this store's pool and the sweep width just set, and says what it is
           once, here. No daily run can be going yet, so nothing has to give a permit back. */
        ApplyDailyRunCap(StorePoolMaxSize(postgres), initialSweepWidth);

        // install-darling.ps1 waits for this text (#6470): reword both together.
        _logger.LogInformation("PerformanceMonitor Darling collection loop started");
        /* #2953: the one publish that clears the failure phases. Set HERE — the last statement before the
           sweep loop's first iteration — and not re-published per cycle: this seam answers "did collection
           start", which is the question no other surface could answer without the store. Whether the CURRENT
           sweep is succeeding is collection_log's question, and by this point collection_log exists to be
           asked. */
        _collectorState.PublishCollecting();

        /* #4732: true from a pass that found collection paused until the first pass that runs it again. */
        var pausedSinceLastRun = false;

        /* #5479: true from a pass that the memory launch guard held off until the first pass that launches bodies again. */
        var heldSinceLastLaunch = false;

        /* #5595: the retention drain's settle window starts here, where the loop starts collecting, not when the worker
           was built: the store retries and migrations before this point can take minutes. */
        _collectionPressure.MarkCollectionStarted();

        /* A stop that lands where a pass awaits the store (the config beacon, the password key check, a version stamp)
           raises OperationCanceledException from the pass itself. It must end the loop and fall to the drain below, as the
           two delay arms do: leaving by the exception disposes the store sources while the command, external alert,
           investigation, backfill and heartbeat loops are still running, and a pass of theirs in flight then fails on a
           disposed source and logs a false warning. The try's braces sit at the loop's own indent and the pass is not
           re-indented under them, so the lines that tests and other branches read by column stay where they were. */
        try
        {
        while (!stoppingToken.IsCancellationRequested)
        {
            /* #4732: tells the skipped-slot count when this loop was not running. First thing in the pass and on the
               wall clock the collectors' due stamps are written on, so a sleep, a stall or a clock step of either sign
               since the previous pass raises the floor before this pass launches any body. */
            _skipCreditFloor.Tick(DateTime.UtcNow);

            /* #5366: the password key check, once per pass: the store still publishes the key this service holds and the key
               tables still have all their triggers. A change turns the ring to refusing until the start fixes it; the check
               never throws and costs two small reads. */
            if (_passwordKeyRuntime is not null)
            {
                await _passwordKeyRuntime.SweepCheckAsync(stoppingToken);
            }

            /* #5655: a service version write that failed at the start is tried again here on every pass until it succeeds, the way
               the pin step above is, so the viewers do not say "unknown (older than 3.11)" about this service until the next restart. */
            if (serviceVersionWriter.Pending)
            {
                await serviceVersionWriter.TryAsync(stoppingToken);
            }

            /* Control-plane reload beacon: poll config_version at a SAFE point (top of the sweep, never
               mid-collection). On change, re-read the store and hot-swap the live config: the alert /
               SMTP / webhook / capture / analysis settings (via the by-reference DarlingAlertSettings
               seam + the runner's capture provider), the monitored-server set (add/remove/replace
               ServerLoopState), the per-collector schedule overrides + NextDue, and the mute-rule cache. */
            var configVersion = await configProvider.ReadConfigVersionAsync(stoppingToken);
            if (configVersion.HasValue && configVersion.Value != _lastConfigVersion)
            {
                /* The watermark advances AFTER the reload applies, not on detecting the bump. Assigning it
                   first meant a reload whose store read failed had already recorded the version as applied:
                   the operator's change stayed live in the store, absent from this process, and unreported
                   until some LATER write bumped config_version again. Nothing retried it, because the
                   beacon's next tick compared equal.

                   That ordering is why a deadline on this thread is not merely a delay — it DISCARDS the
                   change — which is the asymmetry ServiceCommandDeadlines.SerialLoopSeconds is floored
                   against. Fixing the bound without fixing the ordering would have left the silent-loss
                   path intact and only made it rarer.

                   Failing to apply now leaves the watermark behind, so the very next tick re-detects the
                   same bump and retries. That is right for the transient case this guards (a store restart,
                   a failover, a blip) and is bounded work — one single-row beacon read plus one view read
                   per 15 s tick. StoreConfigProvider rate-limits its own failure log across the streak so a
                   PERSISTENTLY unreachable store does not turn the retry into a log flood.

                   Deliberately NOT a retry POLICY: #2936 owns that question for the adjacent migrate path,
                   where it is a real design decision (which failures are retryable, bounded or forever,
                   re-enter or resume) because that path has no loop of its own and its failure is terminal.
                   This path already re-runs every tick by construction and its failure is recoverable, so
                   the only defect here was the ordering. */
                /* Stamped from the version the reload ACTUALLY applied, not from the beacon's own earlier
                   read. ReloadFromStoreAsync re-reads config_version inside LoadViewAsync, so a write
                   landing in the window between the two makes the applied view NEWER than
                   configVersion.Value — and recording the older number would leave the watermark behind a
                   config that is already live, costing one redundant idempotent re-apply on the next tick.
                   Self-healing rather than lossy, but it is the same class of imprecision as the defect
                   above ("the version recorded as applied must be the version that was applied"), and the
                   startup path at :1179 already does it this way. */
                var appliedVersion = await ReloadFromStoreAsync(configProvider, config, servers, muteRuleService, runner, stoppingToken);
                if (appliedVersion.HasValue)
                {
                    _lastConfigVersion = appliedVersion.Value;

                    /* #2170: the reload swapped the knob into the live config; move the gate to match. Safe
                       here by construction — top of the sweep, and narrowing never preempts a running body.
                       Inside the success branch because a reload that applied nothing changed no knob. */
                    var reloadedSweepWidth = StoreConfigProvider.ClampConcurrentSweeps(config.MaxConcurrentSweeps);
                    ReconcileSweepGate(serverSweepGate, reloadedSweepWidth, stoppingToken);

                    /* #4999: the daily-run cap is derived from the sweep width, so every reload recomputes it from the
                       width just applied. A width that moves the cap changes the log once, and one that does not
                       leaves both alone. */
                    ApplyDailyRunCap(StorePoolMaxSize(postgres), reloadedSweepWidth);
                }
            }

            /* Stage 2 pause gate (Lite's IsPaused): while paused, skip ALL collection/alert/analysis/purge
               work but keep looping — the reload beacon above still runs (so a resume, applied via the same
               reload, un-pauses on the very next tick) and the command loop keeps draining commands. A
               resume that reloaded THIS iteration already flipped _paused to false above, so it takes effect
               immediately, not a sweep later. */
            if (!ShouldRunCollection(_paused))
            {
                pausedSinceLastRun = true;
                try
                {
                    await Task.Delay(s_sweepInterval, stoppingToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                continue;
            }

            /* #4732: the first pass to run collection after a pause. The ticks kept coming through the pause, so Tick
               saw no gap, but nothing was collected and no due stamp moved (a reload never moves one forward): left
               alone, the first run would count every slot that came due during the pause as skipped. */
            if (pausedSinceLastRun)
            {
                _skipCreditFloor.Resume(DateTime.UtcNow);
                pausedSinceLastRun = false;
            }

            /* P1: snapshot the server list under the lock so the fire-and-track launch loop below iterates a
               STABLE array while the command loop reconciles the list concurrently (this also keeps the
               deliverer's / plan-fetcher's own locked Find/Select provably race-free). Taken AFTER the reload
               above so this sweep launches against the freshly reconciled set. */
            ServerLoopState[] sweepTargets;
            lock (_serversLock)
            {
                sweepTargets = servers.ToArray();
            }

            /* #5479: the per-server self-alerts (Collection Stopped and its siblings) and the custom-alert rules, evaluated
               by ONE tracked task per pass over this snapshot, launched without awaiting it and skipped while the previous
               pass still runs. They live here, after the operator-pause `continue` above (a deliberate pause stays silent,
               as it always did) and BEFORE the launch loop below, so they run whether or not any collection body launches:
               a launch guard that holds the bodies off, or a body that never finishes, no longer silences them. Each
               server keeps its own cadence stamps, which only this pass reads and writes, so no server is evaluated by two
               threads at once. */
            TryStartSelfAlertPass(
                sweepTargets, stoppingToken, StoreConfigProvider.ClampConcurrentSweeps(config.MaxConcurrentSweeps),
                engine, config, notificationService);

            /* Fire-and-track launch loop (#1553 D2/D2b): LAUNCH each server's collection body WITHOUT awaiting
               it, so one slow or hung server can no longer stall the fleet or the fleet-level steps below (the
               old foreach awaited every step inline — the 24-server field incident). At most N=4 bodies open a
               connection at once (serverSweepGate, acquired inside the body). All tracking / skip-log state
               (InFlightSweep, SweepStartedUtc, WarnedThisEpisode) is written ONLY here on the outer sweep thread
               — the body never touches it, so there is no cross-thread tear on these fields. */

            /* Memory launch guard (#1556, #5479): before launching ANY new bodies this pass, ask the guard. Over the
               line it launches NOTHING this pass so the in-flight bodies drain, and it runs a decommitting full
               garbage collection itself (a drained process allocates nothing, so none would run otherwise), then
               measures again in the same pass and releases when the figure is back under the line. The purge /
               disk-pressure / delay steps below keep running. The guard logs the trip, each collection, the hold
               and the release. */
            var inFlightBodies = 0;
            foreach (var target in sweepTargets)
            {
                if (target.InFlightSweep is { IsCompleted: false })
                {
                    inFlightBodies++;
                }
            }

            var mayLaunchSweeps = _launchMemoryGuard.MayLaunch(inFlightBodies);

            foreach (var server in sweepTargets)
            {
                /* D3: the per-sweep cancellation check HOISTS here from the old inline body — a bare `break`
                   inside the extracted async body would not compile (CS0139), and this launch loop is the loop
                   it belongs to. */
                if (stoppingToken.IsCancellationRequested)
                {
                    break;
                }

                /* Working-set guard tripped this tick: stop launching new bodies (evaluated once, before the
                   loop, so the decision applies uniformly to every server this sweep). */
                if (!mayLaunchSweeps)
                {
                    /* #5479: a held pass records no slot, so the slots that come due while the guard holds are counted
                       here, once each within a pass of their due time (a held slot is a skipped slot). Without it the
                       hour's count read "0 ran, 0 skipped" and Collection Falling Behind cleared an hour into an outage. */
                    heldSinceLastLaunch = true;
                    CountHeldSlots(sweepTargets, DateTime.UtcNow);
                    break;
                }

                /* #5479: the first launch after a hold. The held slots were counted above as they came due, and no due
                   stamp moved, so the first bodies would step over every one of them again (101,774 of 101,881 in the
                   field). Raise the floor to now, exactly as the first pass after an operator pause does (#4732). Before
                   any body launches: this is the first statement the launch loop runs once the guard lets it. */
                if (heldSinceLastLaunch)
                {
                    _skipCreditFloor.Resume(DateTime.UtcNow);
                    heldSinceLastLaunch = false;
                }

                /* #1581 cold-start stagger: hold this server's FIRST sweep body (InFlightSweep still null) until
                   its deterministic per-server offset elapses, so a service restart does not launch all N
                   servers' initial catch-up bodies in ONE tick — the field herd where 24 servers stamped
                   SweepStartedUtc together, queued behind the N=4 gate, and every one that waited past 60s logged
                   "collection body has not completed after 60s". The offset is bounded by ColdStartSpreadSeconds,
                   so no first launch is deferred beyond that window; once a body has launched (InFlightSweep
                   non-null) this no longer applies, so the relaunch path below and steady-state cadence are
                   untouched. A reconcile-added server has FirstSweepDueUtc == MinValue and launches immediately.
                   #4732: the stamp is the start plus at most ColdStartSpreadSeconds, so it is read through StampIsDue
                   with that span: a wall clock corrected backwards inside the window (a fast clock fixed at boot)
                   leaves the stamp more than one span ahead, and a raw compare would hold every server that has not
                   launched yet for as long as the step, with no connect, collection or alert. */
                if (server.InFlightSweep is null
                    && !StampIsDue(server.FirstSweepDueUtc, TimeSpan.FromSeconds(ColdStartSpreadSeconds), DateTime.UtcNow))
                {
                    continue;
                }

                if (server.InFlightSweep is { IsCompleted: false })
                {
                    /* Still in flight: do NOT relaunch (INV-2, one body per server). The field incident was
                       HANGS, not throws, and the body's catch-all (D3) only covers throws — so the stall is
                       surfaced HERE. The EPISODE (SweepStartedUtc, stamped at launch) still spans queue + run,
                       because a body queued behind the gate genuinely is unserved. But the two halves get
                       DIFFERENT channels: RunStartedTicks is 0 until the body acquires the concurrency gate, so
                       a body merely waiting its turn is CAPACITY pressure, never a hang. Attributing queue time
                       to the hang watchdog fired it ~82 times/hour at 24 servers behind the N=4 gate on a
                       demonstrably healthy fleet, which buried the one signal this warning exists to raise. */
                    var episodeSeconds = (DateTime.UtcNow - server.SweepStartedUtc).TotalSeconds;
                    var runStartedTicks = Interlocked.Read(ref server.RunStartedTicks);
                    var running = runStartedTicks != 0;
                    var runningSeconds = running
                        ? (DateTime.UtcNow - new DateTime(runStartedTicks, DateTimeKind.Utc)).TotalSeconds
                        : 0;

                    /* #4710: a body that has not started running is either inside its connect attempt, which runs
                       BEFORE the fleet permit, or waiting for that permit. Only the first is not the fleet
                       limit's doing, so its wording must not blame the limit. Read only when the body is not
                       running: once the permit is held the connect stage is over. */
                    var connectStartedTicks = running ? 0 : Interlocked.Read(ref server.ConnectStartedTicks);
                    var connecting = connectStartedTicks != 0;
                    var connectSeconds = connecting
                        ? (DateTime.UtcNow - new DateTime(connectStartedTicks, DateTimeKind.Utc)).TotalSeconds
                        : 0;

                    _logger.LogDebug(
                        "[{Server}] collection body still in flight after {Elapsed:F0}s ({State}) — skipping this sweep",
                        server.Config.DisplayName,
                        episodeSeconds,
                        SweepInFlightWording.DebugState(running, runningSeconds, connecting, connectSeconds));

                    switch (ClassifySweepEpisode(
                        episodeSeconds, running, runningSeconds, server.WarnedThisEpisode, server.QueuedInfoThisEpisode))
                    {
                        /* HANG — the body is actually EXECUTING and has not finished. ONE Warning per episode.
                           This is the channel that must stay quiet on a healthy fleet so a real stall is seen. */
                        case SweepEpisodeSignal.Hang:
                            server.WarnedThisEpisode = true;
                            _logger.LogWarning(
                                "[{Server}] collection body has not completed after {Elapsed:F0}s of execution — skipping relaunch",
                                server.Config.DisplayName, runningSeconds);
                            break;

                        /* CAPACITY — still QUEUED behind the gate, so nothing is wrong with this server: the
                           fleet is simply wider than the configured sweep width at this moment. Info, once.
                           Reports the EFFECTIVE width, not the compile-time default (#2170 review catch):
                           this line is what an operator reads while deciding whether to raise the knob, so
                           printing 4 after they raised it to 12 would send them chasing a limit that is no
                           longer in force. #4710: a body still inside its connect attempt has not asked for
                           a slot yet, so it gets the connect stage's wording instead, with the connect
                           attempt's own clock and the connect gate's width. */
                        case SweepEpisodeSignal.Queued:
                            server.QueuedInfoThisEpisode = true;
                            _logger.LogInformation(
                                SweepInFlightWording.NotStartedInfoTemplate(connecting),
                                server.Config.DisplayName,
                                connecting ? connectSeconds : episodeSeconds,
                                connecting ? ServerConnectProbe.GateWidth : EffectiveSweepWidth);
                            break;
                    }

                    continue;
                }

                /* The body is null (never launched) or has completed since the last sweep. If we surfaced EITHER
                   channel for this episode, log its resolution with the total elapsed and re-arm before
                   relaunching — the re-arm happens HERE on observing IsCompleted, never in the body's finally
                   (which would race this launch loop). A body that finished under both thresholds was never
                   surfaced, so it simply relaunches. */
                if (server.WarnedThisEpisode || server.QueuedInfoThisEpisode)
                {
                    var ranForSeconds = (DateTime.UtcNow - server.SweepStartedUtc).TotalSeconds;
                    _logger.LogInformation(
                        "[{Server}] collection body completed after {Elapsed:F0}s (episode, including any queue wait)",
                        server.Config.DisplayName, ranForSeconds);
                    server.WarnedThisEpisode = false;
                    server.QueuedInfoThisEpisode = false;
                }

                /* Stamp the launch time BEFORE launching — time spent QUEUED on the gate is part of this
                   episode — and clear the run and connect stamps so this episode starts as QUEUED. The reset must
                   precede the call: ProcessServerSweepAsync runs synchronously up to its first await, so with a
                   free permit the body may stamp RunStartedTicks (or, on its way into a connect attempt,
                   ConnectStartedTicks) before this statement returns, and resetting after would erase it. Then
                   fire-and-track: assign the Task to InFlightSweep (so it is observed and
                   the next sweep + the shutdown drain can see it) but do NOT await it here. */
                server.SweepStartedUtc = DateTime.UtcNow;
                Interlocked.Exchange(ref server.RunStartedTicks, 0);
                Interlocked.Exchange(ref server.ConnectStartedTicks, 0);
                server.InFlightSweep = ProcessServerSweepAsync(
                    server, engine, runner, planFetcher, notificationService, config, serverSweepGate, stoppingToken);
            }

            /* #4999: the hang watchdog above reads only the per-server bodies, and a daily run no longer runs in
               one: it is detached and can go for hours, so a stuck one showed up only as stale data. The same goes
               for the three runs detached by name (query_store, plan_correction, pg_wait_sampling). Same tick, the
               same kind of Warning, one per run, naming the server and collector; the threshold is its own, 15
               minutes (DailyRunWatchdogSeconds), where the bodies' is 60 seconds. After the launch loop and outside
               it, because a detached run belongs to no single body and the loop may leave early. */
            WatchDailyRuns(DateTime.UtcNow);

            /* #4130: fire-and-track, exactly like the per-server sweeps just above and the oversized-plan
               backlog just below — see TryStartScheduledPurge's doc for why the inline await was the
               defect. */
            TryStartScheduledPurge(
                DateTime.UtcNow,
                token => RunScheduledPurgeAsync(postgres, config, token),
                stoppingToken);

            /* #3392: drain the oversized-plan backlog. Its own low-frequency cadence off this loop, beside
               the purge, and deliberately NOT inside the per-server collector rotation: fetching a plan the
               capture cap declined is worth doing eventually and worth nothing if it competes with a live
               collection cycle for that server's wall-clock budget.

               FIRE-AND-TRACK, not awaited — the #1553 launch-loop shape rather than the purge's. The purge
               is awaited because it talks only to the store with bounded statements; this pass opens a
               connection to every monitored server, and its worst case is
               fleet width * MaxPlansPerServerPerTick * PerPlanBudget, which on a 42-server fleet whose
               targets are all timing out is 105 minutes. Awaited, that is 105 minutes in which this loop
               launches no collection bodies at all — the 24-server field incident's exact shape, one
               maintenance step over. Launched and tracked, a slow pass costs only its own next slots.

               sweepTargets is this tick's snapshot, already taken under the servers lock, so the pass
               iterates a stable set. RunAsync is failure-isolated per server and returns quietly on
               cancellation, so the task can complete unobserved without an unhandled fault. */
            /* #4732: the span is the longest delay NextSweepDelay returns, which is the delay the stamp below is written with. */
            if (StampIsDue(_nextOversizedPlanSweepUtc, OversizedPlanBacklogSweep.LongestSweepDelay, DateTime.UtcNow)
                && (_oversizedPlanSweep is null || _oversizedPlanSweep.IsCompleted))
            {
                /* #3405: count BOTH populations, because an empty target list has two causes that mean
                   opposite things and the pass itself cannot tell them apart. sweepableTargets is the
                   registrations that could ever carry a runtime this pass would visit — read off the
                   registration, since Runtime is null both mid-connect and forever. backlogTargets is the
                   ones carrying one now. Counted in one walk rather than two so the pair describes the same
                   snapshot. */
                var backlogTargets = new List<ServerRuntime>(sweepTargets.Length);
                var sweepableTargets = 0;
                foreach (var target in sweepTargets)
                {
                    if (!OversizedPlanBacklogSweep.IsSweepableTarget(target.Config))
                    {
                        continue;
                    }

                    sweepableTargets++;

                    if (target.Runtime is { } runtime)
                    {
                        backlogTargets.Add(runtime);
                    }
                }

                /* Stamped BEFORE the launch, like every other cadence on this loop: the IsCompleted guard
                   above is what keeps a slow pass from stacking, and it only works against a stamp that has
                   already moved. The delay itself is the sweep's decision, not this loop's. */
                var (sweepDelay, connectWaits) = OversizedPlanBacklogSweep.NextSweepDelay(
                    sweepableTargets, backlogTargets.Count, _oversizedPlanSweepConnectWaits);
                _oversizedPlanSweepConnectWaits = connectWaits;
                _nextOversizedPlanSweepUtc = DateTime.UtcNow.Add(sweepDelay);

                _oversizedPlanSweep = OversizedPlanBacklogSweep.RunAsync(
                    postgres, backlogTargets, _logger, stoppingToken);
            }

            /* #5985: the query-tag map step. Rebuilds every listed server's map when a tag, assignment or rule changed
               (rules_version ahead of built_version), otherwise extends it for the keys sighted since the last pass,
               capped per server per pass, and flushes the runner's in-memory sightings first. Fire-and-track like the
               sweep above: it only talks to the store, but a rebuild over a large sighting history can run long and must not
               hold the launch loop. The builder is failure-isolated per server; only a cancellation (shutdown) escapes it, and
               the tracked task is never awaited, so that ends the task quietly. */
            if (StampIsDue(_nextQueryTagMapPassUtc, s_queryTagMapPassInterval, DateTime.UtcNow)
                && (_queryTagMapPass is null || _queryTagMapPass.IsCompleted))
            {
                _nextQueryTagMapPassUtc = DateTime.UtcNow.Add(s_queryTagMapPassInterval);
                var mapServerIds = new List<int>(sweepTargets.Length);
                foreach (var target in sweepTargets)
                {
                    mapServerIds.Add(target.Config.ServerId);
                }

                _queryTagMapBuilder ??= new QueryTagMapBuilder(postgres, _logger, sightings: runner.QueryTagSightings);
                _queryTagMapPass = _queryTagMapBuilder.RunMapPassAsync(mapServerIds, stoppingToken);
            }

            /* Stage 4 fleet-level self-alert: the store disk-pressure backstop. The daily purge is the ONLY
               other maintenance cadence and it is purely time-based (no disk-free check), so on its own the
               store can still fill between purges — this edge-fired condition is the flagship-appropriate
               backstop. Own slow cadence; the master alerts gate + edge-trigger live inside the evaluator. */
            /* #4732: the stamp below is written as now + s_diskCheckInterval, so that is the span. */
            if (StampIsDue(_nextDiskCheckUtc, s_diskCheckInterval, DateTime.UtcNow))
            {
                _nextDiskCheckUtc = DateTime.UtcNow.Add(s_diskCheckInterval);
                await EvaluateStoreDiskPressureAsync(config, stoppingToken);
            }

            /* #3304: the custom-alert-rule integrity check. A stored rule silently stops compiling when a
               measure drifts out of the catalog (the incremental pg_* buildout), and a rule can compile yet
               never fire (scope resolves to no monitored server, or its measure is always-NULL). Nobody
               "opens" a rule, so a silent gap = an alert an operator believes is armed. Fleet-level, own slow
               cadence; both evaluators are null on deployments without a viewer pool, and each Evaluate* half
               is failure-isolated so a throw never stops the fleet loop. */
            /* #4732: the stamp below is written as now + s_customAlertHealthInterval, so that is the span. */
            if (_customAlertEvaluator is not null && _selfAlerts is not null
                && StampIsDue(_nextCustomAlertHealthCheckUtc, s_customAlertHealthInterval, DateTime.UtcNow))
            {
                _nextCustomAlertHealthCheckUtc = DateTime.UtcNow.Add(s_customAlertHealthInterval);
                await EvaluateCustomAlertRuleHealthAsync(servers, stoppingToken);

                /* #3305: on the same cadence, reconcile persisted state — force-resolve + clean the state of
                   disabled rules (which the enabled-only sweep would orphan) and of servers that have left a
                   rule's scope. Deleted rules are resolved on the delete path (their state cascades away). */
                await ReconcileCustomAlertStateAsync(servers, stoppingToken);
            }

            /* #3306: a mute rule that has outlived its reason. A mute is a deliberate blind spot, and this is
               the only surface that reports one exists, how old it is, and whether it will ever expire —
               otherwise the only way to find one is to already suspect it and call get_mute_rules. Without it
               a rule created to stop a false-positive flood keeps suppressing the alert after the fix ships,
               and the symptom is silence. Judged on the CONJUNCTION (still in force, no expiry, past every expiry the
               product offers), never on permanence alone, which the dialog offers on purpose. Reads the live
               MuteRuleService cache the engine matches against — so it sees exactly what is suppressing
               alerts right now, and needs no store read of its own. Fleet-level, own slow cadence; the
               Evaluate* wrapper is failure-isolated so a throw never stops the fleet loop. */
            /* #4732: the stamp below is written as now + s_staleMuteCheckInterval, so that is the span. */
            if (_selfAlerts is not null && StampIsDue(_nextStaleMuteCheckUtc, s_staleMuteCheckInterval, DateTime.UtcNow))
            {
                _nextStaleMuteCheckUtc = DateTime.UtcNow.Add(s_staleMuteCheckInterval);
                await _selfAlerts.EvaluateStaleMuteRulesAsync(muteRuleService.GetRules(), stoppingToken);
            }

            /* #5654: external alerts the drain gave up on (expired, interrupted or failed every send). A source that queues
               an alert believes it was delivered, and nothing else reports that it was not. The count is one store read per
               tick; a failed read holds the standing alert rather than resolving it. Fleet-level, own slow cadence; the
               Evaluate* wrapper is failure-isolated so a throw never stops the fleet loop. */
            /* #4732: the stamp below is written as now + s_externalAlertsCheckInterval, so that is the span. */
            if (_selfAlerts is not null && StampIsDue(_nextExternalAlertsCheckUtc, s_externalAlertsCheckInterval, DateTime.UtcNow))
            {
                _nextExternalAlertsCheckUtc = DateTime.UtcNow.Add(s_externalAlertsCheckInterval);
                await EvaluateExternalAlertsAsync(externalAlertStore, stoppingToken);
            }

            /* #4750: a webhook channel that has failed three times in a row. A channel can fail for weeks while
               another one delivers every alert, and nothing else reports it. The counts are the webhook
               service's own in-memory tallies, so the read costs nothing and it rides every sweep tick with no
               cadence of its own; the evaluator is an edge (one alert when a count reaches three, one
               resolution when it is back at 0), so a channel that stays broken does not repeat. Fleet-level,
               master-gated inside, and the Evaluate* wrapper is failure-isolated so a throw never stops the
               fleet loop. */
            if (_selfAlerts is not null)
            {
                await _selfAlerts.EvaluateNotificationChannelsAsync(stoppingToken);
            }

            /* #7041: the Daily Health Report's due check. Every 15-second tick pays one settings read and a clock compare; the report is
               built only once it is due and not yet delivered today. A throw here must not stop the fleet loop. */
            if (_selfAlerts is not null)
            {
                try
                {
                    await _selfAlerts.EvaluateDailyHealthReportAsync(stoppingToken);
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _logger.LogWarning(ex, "Daily health report check failed; it will be tried again on the next tick");
                }
            }

            /* #3514: the web-dashboard TLS certificate expiry self-alert. The web host loads the certificate
               once at start and publishes its served expiry to WebTlsCertificateState; a headless service can
               run for months without a restart, so the worker re-evaluates that fixed expiry against the clock
               on its own slow cadence and the evaluator warns 30 days out / Critical once lapsed — and Critical
               at once when the snapshot carries the host's not-yet-valid refusal (#3517: the dashboard is
               loopback-only from the start, and the host does not re-decide when the date passes). A null
               snapshot means no LAN TLS certificate to watch. Fleet-level, and the Evaluate* wrapper is
               failure-isolated so a throw never stops the fleet loop. */
            /* #5288: the MCP endpoint's certificate rides the same hourly gate (one stamp, no new field): the
               MCP host publishes its served expiry to McpTlsCertificateState exactly as the web host does, and
               the evaluator keeps the two alerts apart by key and metric, so the call sits right beside the web
               one. Each Evaluate* wrapper is failure-isolated on its own, so a throw in the web half cannot
               skip the MCP half. */
            /* #4732: the stamp below is written as now + s_webTlsCheckInterval, so that is the span. */
            if (_selfAlerts is not null && StampIsDue(_nextWebTlsCheckUtc, s_webTlsCheckInterval, DateTime.UtcNow))
            {
                _nextWebTlsCheckUtc = DateTime.UtcNow.Add(s_webTlsCheckInterval);
                await _selfAlerts.EvaluateWebTlsCertificateAsync(
                    BuildWebTlsCertReport(_webTlsCertState.Read()), stoppingToken);
                if (_mcpTlsCertState is not null)
                {
                    await _selfAlerts.EvaluateMcpTlsCertificateAsync(
                        BuildWebTlsCertReport(_mcpTlsCertState.Read()), stoppingToken);
                }
            }

            /* #4732: the fleet gate's last-hour counts, once a minute: the "Collection Falling Behind" self-alert
               (fleet-level, master-gated inside, failure-isolated inside) and the service log's hourly summary line,
               which also runs with no alert engine. Before this a collector slot that came due while its run was
               late was skipped with one Info line, and how long a body queued for a gate slot was never measured. */
            /* #4732: the stamp below is written as now + s_fleetGateCheckInterval, so that is the span. */
            if (StampIsDue(_nextFleetGateCheckUtc, s_fleetGateCheckInterval, DateTime.UtcNow))
            {
                _nextFleetGateCheckUtc = DateTime.UtcNow.Add(s_fleetGateCheckInterval);
                await CheckFleetGateAsync(stoppingToken);
            }

            /* #4215: the managed-store settings self-alert — darling-managed.conf fell back to the
               last-good copy, is hand-edited and kept in force, or PostgreSQL rejected one or more owned
               settings outright. Every
               fact but the rejected-setting list is this start's own in-process state, carried down from
               ExecuteAsync exactly like managedConfWriteResult already is — never re-read, because the writer
               runs on every start and the state is re-derived after a restart. The rejected list is the one
               fact that is store-backed (collect.managed_conf_verdicts, V146), read fresh each tick so a value
               fixed by a later start clears without this process restarting. Fleet-level, own slow cadence;
               the Evaluate* wrapper is failure-isolated so a throw never stops the fleet loop. */
            /* #4732: the stamp below is written as now + s_storeSettingsCheckInterval, so that is the span. */
            if (_selfAlerts is not null && StampIsDue(_nextStoreSettingsCheckUtc, s_storeSettingsCheckInterval, DateTime.UtcNow))
            {
                _nextStoreSettingsCheckUtc = DateTime.UtcNow.Add(s_storeSettingsCheckInterval);
                await EvaluateStoreSettingsAsync(config, managedConfWriteResult, managedUsedLastGoodConf, managedConfVerification, stoppingToken);
            }

            /* #1581: the compression-job self-heal backstop. TimescaleDB compression policy jobs can silently
               die (next_start = -infinity) or hang, halting the store's archival tier so uncompressed data grows
               without bound until the disk fills and collection stops for the WHOLE fleet (the field incident).
               Timescale-only — gated one level in since #3815 put the availability re-probe ahead of it on
               this same tick; own hourly cadence; failure-isolated inside EvaluateCompressionJobHealthAsync.

               The next due time is SNAPPED to the wall clock rather than taken from this fire (#3575). The
               dead-job arm the check judges reads next_start = -infinity, which is also what the scheduler
               writes for the few milliseconds at either edge of every healthy run before the worker is, or
               after it stops being, visible as Running — and the policies run at :MM:00 on a fixed schedule,
               so a check that re-anchored itself as "UtcNow + 1 h" on every fire crept a few seconds per hour
               across those instants until, on a production store, it sampled one 53 ms into a 63 ms run and
               paged. NextCompressionCheckUtc puts every steady-state sample at :30 past its minute instead,
               half the grid step from every policy's start in both directions. The read itself now confirms
               a -infinity trip with a second read five seconds later (ReadStuckPolicyJobsAsync), so the
               phase is hardening on top of the fix, not the fix. */
            /* #4732: NextCompressionCheckUtc writes at most one interval plus its 30 s phase ahead (CompressionCheckSpan). */
            if (StampIsDue(_nextCompressionCheckUtc, CompressionCheckSpan, DateTime.UtcNow))
            {
                _nextCompressionCheckUtc = TimescaleSupport.NextCompressionCheckUtc(DateTime.UtcNow, s_compressionCheckInterval);
                TryStartStoreMaintenanceTick(token => RunStoreMaintenanceTickAsync(token), stoppingToken);
            }

            /* #2068: the store self-metrics sweep. Capacity forecasting previously required ad-hoc
               archaeology over the TimescaleDB chunk catalog, whose raw window is 4 days — a measured 3x
               daily-ingest jump was only visible because the catalog still held both eras. This persists
               the store's own size/compression/growth series (per-hypertable, per-dimension, whole-store)
               into the plain collect.store_metrics table hourly, retention bounded by the sweep's own
               DELETE. Every store shape (the hypertable arm gates on _timescaleAvailable INSIDE);
               failure-isolated inside SweepStoreSelfMetricsAsync like the two checks above. */
            /* #3466 (lane 2): the fleet sweep — the scheduled, stateful whole-fleet report. FIRE-AND-TRACK
               like the oversized-plan backlog, not awaited like the purge: the sweep reads only the loopback
               store, but it reads it once per monitored server, and a stalled store would otherwise hold
               this loop for minutes — the field-herd shape one maintenance step over. Launched and tracked,
               a slow sweep costs only its own next slots (the IsCompleted guard), and RunAsync never faults
               (catch-all inside, quiet on cancellation), so the task can complete unobserved.

               Gated on the sweep's OWN switch and deliberately NOT on config.Alerts.Enabled: sweeps under
               master-off are the muted-mode contract's whole point — the report keeps publishing, carries
               the mute in its header, and writes the would-have-paged ledger. The engine makes no delivery
               call anywhere (delivery is lane 4's daily rollup), so the master-switch census has nothing of
               this path's to count. The cadence is read ONCE into the local that both the stamp and the
               engine's span math use, so a store reload swapping config mid-tick cannot make the document
               claim a span the schedule never used — the retention-hold read-once discipline. Stamped
               BEFORE the launch, like every cadence on this loop. */
            if (config.Alerts.FleetSweepEnabled
                && StampIsDue(_nextFleetSweepUtc, FleetSweepStampSpan, DateTime.UtcNow)
                && (_fleetSweep is null || _fleetSweep.IsCompleted))
            {
                /* #4732: this interval is read at the fire and can differ from the one the stamp was last written with, so the
                   guard above spans the largest interval ClampIntervalMinutes allows, never this fire's value. */
                var fleetSweepMinutes = FleetSweepEngine.ClampIntervalMinutes(config.Alerts.FleetSweepIntervalMinutes);
                _nextFleetSweepUtc = NextGridStamp(_nextFleetSweepUtc, DateTime.UtcNow, TimeSpan.FromMinutes(fleetSweepMinutes));

                /* The whole registered population, connected or not — a sweep is about the fleet, and a
                   server that cannot be reached is exactly the kind of fact it must carry (its store rows
                   go quiet, which the engine bands and explains) rather than skip. */
                var fleetSweepServers = new List<(int ServerId, string ServerName)>(sweepTargets.Length);
                foreach (var target in sweepTargets)
                {
                    fleetSweepServers.Add((target.Config.ServerId, target.Config.DisplayName));
                }

                /* #4925: an Azure SQL Database master's signals count only its own blocking and deadlocks. A target
                   whose list cannot be built reads unscoped (null), never fails the sweep. */
                var fleetSweepSeparate = new Dictionary<int, IReadOnlyList<string>?>();
                foreach (var target in sweepTargets)
                {
                    try
                    {
                        /* A master that is disconnected when the sweep fires has no runtime to ask, so its list comes from the
                           stored edition and the registry, the same way the MCP and web hosts get it; reading it unscoped
                           would count its earlier events twice in the sweep band. */
                        fleetSweepSeparate[target.Config.ServerId] = target.Runtime is null
                            ? await AnalysisSeparatelyMonitoredDatabasesAsync(target.Config.ServerId, _registryState?.Read(), postgres, stoppingToken)
                            : AnalysisSeparatelyMonitoredDatabases(target.Runtime);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogWarning(ex, "Fleet sweep could not list the separately monitored databases for server {ServerId}; reading it unscoped", target.Config.ServerId);
                        fleetSweepSeparate[target.Config.ServerId] = null;
                    }
                }

                _fleetSweep = FleetSweepEngine.RunAsync(
                    postgres, fleetSweepServers, TimeSpan.FromMinutes(fleetSweepMinutes),
                    config.Alerts.Enabled, _logger, fleetSweepSeparate, stoppingToken);
            }

            /* #4823: the once-a-minute checkpointer sync-time sample, AHEAD of the hourly tick below so the sample taken
               on a tick's own pass is inside the window that tick takes. #4834: whether or not a self-alert evaluator
               exists, because the hourly checkpointer row stores the window's maximum either way. A failed read is a
               Debug line and a skipped minute inside SampleCheckpointSyncAsync, never a stopped loop. */
            /* #4732: NextGridStamp writes at most one interval ahead, and s_checkpointSyncSampleInterval is the one it uses. */
            if (StampIsDue(_nextCheckpointSyncSampleUtc, s_checkpointSyncSampleInterval, DateTime.UtcNow))
            {
                _nextCheckpointSyncSampleUtc = NextGridStamp(_nextCheckpointSyncSampleUtc, DateTime.UtcNow, s_checkpointSyncSampleInterval);
                await SampleCheckpointSyncAsync(stoppingToken);
            }

            /* #4732: NextGridStamp writes at most one interval ahead, and s_storeMetricsInterval is the one it uses. */
            if (StampIsDue(_nextStoreMetricsUtc, s_storeMetricsInterval, DateTime.UtcNow))
            {
                /* #4012's review: the deadlock re-mask inside the sweep keys an alert whose report is gone under the
                   same log-hash key the runner's log-event runs share. */
                _pgDeadlockRemaskKey = runner.LogHashKey;
                TryStartStoreMetricsTick(DateTime.UtcNow, _checkpointSyncSampler.TakeWindowMax, (window, token) => RunStoreMetricsTickAsync(window, token), stoppingToken);
            }

            try
            {
                await Task.Delay(s_sweepInterval, stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Expected on shutdown. */
        }

        /* Shutdown drain for the fire-and-track bodies (#1553): the launch loop does NOT await the per-server
           bodies, so on cancellation some may still be running (or queued on the gate). Collect the live ones
           under the servers lock (the command loop reconciles concurrently) and wait up to the drain budget for
           them to finish, bounded so a genuinely hung body cannot hold shutdown open indefinitely. Bodies never
           fault (ProcessServerSweepAsync's catch-all), so Task.WhenAll completes cleanly rather than throwing.
           A reconcile-removed server's body is not in `servers` and so is not awaited here — it detaches, but
           its own catch-all + the never-disposed gate keep it non-fatal. The delay uses CancellationToken.None
           because stoppingToken is already cancelled at this point. */
        List<Task> inFlightSweeps;
        lock (_serversLock)
        {
            inFlightSweeps = servers
                .Select(s => s.InFlightSweep)
                .Where(t => t is not null)
                .Select(t => t!)
                .ToList();
        }

        if (_storeMaintenanceTick is { IsCompleted: false })
        {
            inFlightSweeps.Add(_storeMaintenanceTick);
        }

        if (_storeMetricsTick is { IsCompleted: false })
        {
            inFlightSweeps.Add(_storeMetricsTick);
        }

        /* #5479: the self-alert pass is tracked like the ticks above, and ends on the same cancellation. */
        if (_selfAlertPass is { IsCompleted: false })
        {
            inFlightSweeps.Add(_selfAlertPass);
        }

        /* #5642: and so is the engine pass. */
        if (_alertEnginePass is { IsCompleted: false })
        {
            inFlightSweeps.Add(_alertEnginePass);
        }

        /* #5574: the perfmon_stats re-group loop ends on the same cancellation; its chunk in flight rolls back or is left
           for the compression policy, and the loop never faults. */
        if (_perfmonRegroupDrain?.Completion is { IsCompleted: false } regroupRun)
        {
            inFlightSweeps.Add(regroupRun);
        }

        /* #4938: the daily runs detached from those bodies join the wait inside DrainInFlightAsync. */
        await DrainInFlightAsync(inFlightSweeps);

        /* #5985: the last query-tag sightings flush. The collection bodies have drained, so nothing appends any more; what
           they sighted since the map step's last pass is still in memory and would be lost with the process. */
        await DrainQueryTagSightingsAsync(_queryTagMapPass, runner.QueryTagSightings);

        /* #4130: drain the in-flight daily purge the same way as the per-server sweeps above, rather than
           abandoning it mid-DELETE. RunTrackedAsync's own try/catch swallows the OperationCanceledException
           that stoppingToken's cancellation raises inside PurgeAsync, so this await completes cleanly
           without throwing — same shape as the sweeps' catch-all. */
        if (_purgeTask is { IsCompleted: false })
        {
            await Task.WhenAny(_purgeTask, Task.Delay(s_shutdownDrainBudget, CancellationToken.None));
        }

        /* Drain the concurrent command loop on shutdown (it observes the same token). */
        try
        {
            await commandLoop;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* #5654: the external alert loop observes the same token; its pass in flight gives back the rows it had not reached. */
        try
        {
            await externalAlertLoop;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* #5632 Phase A: the investigation sweep observes the same token. */
        try
        {
            await investigationSweepLoop;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* #6001: the evidence capture observes it too. */
        try
        {
            await investigationEvidenceLoop;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* #3916 PR B: once the sweeps and the command loop (analyze_now) have drained, nothing enqueues a
           page any more; drop the analysis hold-back queue unstamped. A page queued at stop is not a
           delivery, so nothing holds its story and the next start re-attempts it. */
        notificationService.Dispose();

        /* Drain the Query Store backfill loop the same way (#2022): a slice abandoned mid-shutdown is
           fine — its boundary is derived (or hole-recorded) from what actually landed, so the next
           start resumes exactly where the COPY committed. */
        try
        {
            await backfillLoop;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* The heartbeat observes the same token and swallows everything but cancellation (#5450). */
        try
        {
            await heartbeatLoop;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* Drain the background baseline backfill the same way (#1757). It observes the same token, and its
           own body already swallows everything but cancellation, so this is about not leaving an unobserved
           Task behind — a backfill still running at shutdown is fine to abandon: TimescaleDB commits it in
           per-batch transactions and the coverage gate picks it up from there on the next start. */
        if (baselineBackfill is not null)
        {
            try
            {
                await baselineBackfill;
            }
            catch (OperationCanceledException)
            {
                /* Expected on shutdown. */
            }
        }

        /* And the hole repair (#3653, Q10), for the same reason: a forced refresh cut short at shutdown is
           re-found by the next start's scan, because the scan reads the materialization rather than a ledger
           of what this start meant to do. */
        if (holeRepair is not null)
        {
            try
            {
                await holeRepair;
            }
            catch (OperationCanceledException)
            {
                /* Expected on shutdown. */
            }
        }

        /* And the collection-health rollup's one-time refresh (#4812): cut short at shutdown, the view is left
           partly materialized and the refresh policy finishes it; the read's continuity guard covers the gap. */
        if (collectionHealthWarm is not null)
        {
            try
            {
                await collectionHealthWarm;
            }
            catch (OperationCanceledException)
            {
                /* Expected on shutdown. */
            }
        }

        /* #4299/#4391: the Periodic pass's own repair launch, same drain as the start-path one above —
           a repair the Periodic tick started is awaited here too, so shutdown does not race it and any fault
           it throws is observed rather than lost with the task. */
        if (_periodicHoleRepair is not null)
        {
            try
            {
                await _periodicHoleRepair;
            }
            catch (OperationCanceledException)
            {
                /* Expected on shutdown. */
            }
        }

        /* And the setting scrub (#4348 S1b): a run cut short by shutdown left its marker unwritten (the
           marker is only written after every batch completes), so the next start tries again from the top. */
        try
        {
            await settingScrub;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* And the long-read table's first build (#5506 part 3). */
        try
        {
            await readAdmissionTable;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* And the slow-read writer (#5097): a record still queued at shutdown is dropped. */
        try
        {
            await slowReadWriter;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* And the statement-text scrub (#4348), for the same reason. */
        try
        {
            await statementTextScrub;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* And the deadlock duplicate cleanup, in its own try so a cancelled scrub above never leaves it unobserved. */
        try
        {
            await deadlockDuplicateCleanup;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* And the plan-force-actions detail scrub (#4346), for the same reason. */
        try
        {
            await planForceDetailScrub;
        }
        catch (OperationCanceledException)
        {
            /* Expected on shutdown. */
        }

        /* And the Query Store read index ensures (#4605, #4952, #5507), which absorb their own failures. */
        await queryStoreIndexes;

        /* And the self-statement key backfill (#6750), which absorbs its own failures. */
        await queryStoreSelfKeys;

        /* And the rollup-coverage warm (#4957), which also absorbs its own failures. */
        await rollupCoverageWarm;

        _logger.LogInformation("PerformanceMonitor Darling collection loop stopped");
    }

    /// <summary>
    /// One server's collection body (#1553 D3), extracted from the old inline sweep loop for the fire-and-track
    /// model (self-alert call rebased to the _postgres field, the loop's continue/break reshaped to a body
    /// return + a hoisted cancellation check): self-alert eval -> connect-or-collect -> alert eval -> analysis, in that order,
    /// SEQUENTIAL within this one server (Lite's RemoteCollectorService shape — parallel across servers,
    /// sequential collectors per server). Runs on a pool thread; the outer launch loop tracks the returned Task
    /// in <see cref="ServerLoopState.InFlightSweep"/> and never awaits it inline, so one slow/hung server cannot
    /// head-of-line-block the fleet. The fleet gate is acquired here (bounding fleet-wide concurrency to
    /// <see cref="MaxConcurrentServerSweeps"/>), and the whole body is wrapped so a hung server cannot starve
    /// the fleet and one server's unexpected throw is CONTAINED (Design Goal 4) rather than killing the loop or
    /// surfacing as an unobserved fire-and-track fault.
    /// </summary>
    private async Task ProcessServerSweepAsync(
        ServerLoopState server,
        AlertEngine engine,
        DarlingCollectorRunner runner,
        PgPlanFetcher planFetcher,
        AnalysisNotificationService notificationService,
        DarlingConfig config,
        SemaphoreSlim gate,
        CancellationToken stoppingToken)
    {
        /* Acquire the fleet concurrency gate OUTSIDE the try (the never-faulting-probe idiom): WaitAsync either
           returns having TAKEN a permit — matched by the finally's Release — or THROWS owning nothing (a cancel
           while queued on shutdown), so the finally can never over-release a permit we do not hold. */
        /* #4710: a disconnected server's connect attempt runs BEFORE the fleet gate, in the small connect gate.
           Inside the fleet gate it held a collection slot for the 15 s SqlClient needs to fail against any dead
           server. Only the store-facing work after the attempt (the failure edge, or the on-load snapshots after
           a success) takes the fleet permit below. Not due, retired or already connected: no attempt. */
        ConnectAttempt? connectAttempt = null;
        if (server.Runtime is null && !server.Retired && StampIsDue(server.NextConnectAttempt, s_connectAttemptStampSpan, DateTime.UtcNow))
        {
            /* The attempt runs before the fleet permit, so RunStartedTicks stays 0 through it. Stamp the connect
               stage on its own field so the in-flight check can say "connecting" rather than blame the fleet
               limit for a slow server, and clear it in a finally so a shutdown cancel clears it too. */
            try
            {
                Interlocked.Exchange(ref server.ConnectStartedTicks, DateTime.UtcNow.Ticks);
                connectAttempt = await ServerConnectProbe.AttemptAsync(
                    server.Config,
                    ConnectOverride ?? ((target, token) => DarlingServerConnector.ConnectAsync(target, _logger, token)),
                    _connectProbeGate,
                    stoppingToken);
            }
            finally
            {
                Interlocked.Exchange(ref server.ConnectStartedTicks, 0);
            }
        }

        /* #4732: the time this body queued for a fleet slot, kept per minute in _fleetGateStats. A wait that ends in a
           cancel records nothing (the throw skips the line below), like the permit it never took. */
        var gateWaitStarted = Stopwatch.GetTimestamp();
        await gate.WaitAsync(stoppingToken);
        var gateWaited = Stopwatch.GetElapsedTime(gateWaitStarted);
        _fleetGateStats?.RecordQueueWait(gateWaited);
        server.LastGateWaitMs = (long)gateWaited.TotalMilliseconds;
        server.LastGateWaitAnalysisHolders = Volatile.Read(ref _analysisPassesInSlot);

        /* The permit is held: this body has STOPPED queueing and STARTED running. Stamp the run start so the
           outer launch loop's watchdog can tell a genuine hang from a body that was merely waiting its turn.
           This is one of the TWO sweep-bookkeeping fields the body writes, via Interlocked (see RunStartedTicks;
           the other is the connect stamp above) — the three outer-thread-only fields are deliberately left
           alone. Placed before the Retired check so a
           retired body still reports as "running" for the instant it takes to no-op out, rather than looking
           permanently queued. */
        Interlocked.Exchange(ref server.RunStartedTicks, DateTime.UtcNow.Ticks);

        try
        {
            /* Retired containment (#1553 D1), the AUTHORITATIVE check: a reconcile-remove may have retired this
               server AFTER this body was launched or while it sat QUEUED on the gate. An async method runs
               synchronously only to its first await, so a check BEFORE WaitAsync would evaluate at LAUNCH time
               (Retired still false) and a gate-queued body would never re-check on dequeue — so the check lives
               HERE, as the first statement after acquiring the permit. A retired body no-ops entirely: it never
               connects, never runs XE DDL, and never re-writes self-alert edge state after Forget removed it.
               (The connect path adds a SECOND re-check for the narrow window where removal lands DURING the
               connect I/O; this entry check covers the dominant queued-dequeue path.) */
            if (server.Retired)
            {
                return;
            }

            /* #5479: the Stage 4 store-polled self-alerts (Collection Stopped, capture down, ...) and the custom-alert
               rules (#3285) no longer run here. They ran at the top of this body, so a body the memory launch guard held
               off, or one that never finished, silenced them: a fleet that had stopped collecting paged nobody for 25
               hours. They are evaluated by the fleet-level pass the sweep loop launches every tick whether or not any body
               launches (RunSelfAlertPassAsync), which is now the ONLY place that reads or writes a server's two alert
               cadence stamps and calls the two evaluators for a server. Connection lost/restored still fire on the
               connect edges in TryConnectAsync. */
            if (server.Runtime is null)
            {
                await TryConnectAsync(server, runner, config, connectAttempt, stoppingToken);
                return;
            }

            /* Reconcile the opt-in long-query completion XE session (#1496) to its enabled flag before the
               collector sweep. State-tracked (ServerLoopState.LongQueryTraceApplied), so it only opens a
               connection when the desired state changes — enabling creates the session, disabling DROPS it.
               Runs regardless of whether the collector is due or enabled, because a disabled collector is
               never dispatched by RunDueCollectorsAsync and so the DROP-on-disable has nowhere else to run. */
            await ReconcileLongQueryTraceAsync(server, runner, stoppingToken);

            /* #4961: the always-on deadlock and blocked-process sessions are ensured at connect and then once an hour, so a
               session dropped from outside comes back within the hour. */
            await EnsureAlwaysOnXeSessionsAsync(
                server, runner, DateTime.UtcNow, _logger, stoppingToken,
                attentionEnabled: StoreConfigProvider.ResolveSchedule("attention", server.Config.ServerId, _scheduleOverrides).Enabled);

            await RunDueCollectorsAsync(server, runner, stoppingToken);

            /* #5642: the alert evaluation and the same-statement-pileup finding (#3467) no longer run here. They ran
               after the collector sweep, so a body that never finished (or a slow collector) silenced every alert on
               its server, the same defect #5479 fixed for the self-alerts. They run in the fleet's
               engine pass (RunAlertEnginePassAsync) on the same 30 s stamps, in a single-flight slot of its own chained
               after the self-alert step, so alert reads never delay Collection Stopped. The pass makes store reads only,
               so the two msdb facts the alert engine wants are read HERE, after the collectors, into a per-server cache
               the pass serves. */
            await RefreshMsdbCacheAsync(server, stoppingToken);

            /* AN3: the scheduled analysis pipeline, per-server. The cadence, the enabled gate, and the notify
               gate are now control-plane knobs read LIVE from config.Analysis (a reload takes effect on the next
               tick). When analysis is disabled the pass is skipped and NextAnalysisDue is left in the past, so
               re-enabling runs immediately. The next-due stamp advances up front (Lite's scheduler shape), so a
               timed-out pass is skipped, not retried immediately. Delivery is gated on the master switch
               AND analysis_notifications_enabled (ShouldNotifyAnalysisFindings — #3464; Lite's D0 split
               stands: production unconditional, delivery gated); the interval is clamped to Lite's 5-360
               range. */
            /* #5824: the pass runs here, inside this body, so it holds this server's fleet collection slot for its whole
               length (up to the 120 s budget plus the 5 s unwind grace, then the notify and force-plan steps). The next
               body reads its length as a candidate cause of any slot this server steps over. */
            server.LastAnalysisMs = 0;
            var analysisIntervalMinutes = Math.Clamp(config.Analysis.IntervalMinutes, MinAnalysisIntervalMinutes, MaxAnalysisIntervalMinutes);
            /* #5812 (ruling R3): an MCP or viewer call reuses PLAN_REGRESSION candidates for one analysis cadence, never
               longer than the scheduled pass that replaces them. */
            PerformanceMonitor.Darling.Analysis.PgFactCollector.PlanRegressionCandidateCache.Shared.RefreshAfter = TimeSpan.FromMinutes(analysisIntervalMinutes);
            if (config.Analysis.Enabled && StampIsDue(server.NextAnalysisDue, TimeSpan.FromMinutes(analysisIntervalMinutes), DateTime.UtcNow))
            {
                server.NextAnalysisDue = DateTime.UtcNow.AddMinutes(analysisIntervalMinutes);

                /* Every engine takes the pass (#3542). Until the PostgreSQL-target analysis engine existed,
                   this site gated on the target engine and wrote a tombstone into analysis_state for a
                   PostgreSQL server instead of running: the pipeline was SQL-Server-shaped, its data-span
                   gate read wait_stats, and a PostgreSQL server_id would have sat at "0 hours, still
                   collecting" for the life of the deployment. That gate is gone, and the invariant that
                   replaced it lives one layer down: the pass ROUTES BY THE REGISTRY'S engine_kind INSIDE
                   DarlingAnalysisService (its ResolveEngineAsync picks the PostgreSQL-target component set
                   and the pg_database_stats span gate for a postgres / aurora-postgres row), so the worker
                   no longer knows or cares which engine it is scheduling. The first real pass then
                   overwrites any tombstone row still standing with the honest state (D7 — the lazy
                   overwrite, no migration). */
                var analysisStarted = Stopwatch.GetTimestamp();
                Interlocked.Increment(ref _analysisPassesInSlot);
                try
                {
                    await RunScheduledAnalysisAsync(
                        server, planFetcher, notificationService, ShouldNotifyAnalysisFindings(config), stoppingToken);
                }
                finally
                {
                    Interlocked.Decrement(ref _analysisPassesInSlot);
                    server.LastAnalysisMs = (long)Stopwatch.GetElapsedTime(analysisStarted).TotalMilliseconds;
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            /* Shutdown (a body-internal await observed the token) — quiet and expected. */
        }
        catch (OperationCanceledException ex)
        {
            /* #5642: a cancellation that is NOT the service stopping (an inner timeout token that escaped) used to be
               swallowed here without a line, which is one of the silent ways a server's sweep ended. */
            _logger.LogWarning("[{Server}] Collection sweep ended on a cancellation that was not the service stopping: {Message}",
                server.Config.DisplayName, ex.Message);
        }
        catch (Exception ex)
        {
            /* Design Goal 4 (exception containment — the gap the sweep loop previously acknowledged it had no
               catch-all for): one server's unexpected throw must never kill the collection loop, and a faulted
               fire-and-track Task must never surface as an unobserved exception. Log and isolate; the server
               retries on its next sweep exactly like a failed collector. */
            _logger.LogError("[{Server}] Collection sweep failed: {Message}", server.Config.DisplayName, ex.Message);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Reconciles the OPT-IN long-query completion XE session (#1496) to its resolved enabled flag, once
    /// the desired state differs from what was last applied to this server (tracked in
    /// <see cref="ServerLoopState.LongQueryTraceApplied"/>, and on Azure SQL Database in
    /// <see cref="ServerLoopState.LongQueryTraceAppliedKey"/> too, so steady state — a default-off
    /// collector — opens no connection at all). Enabling creates the session; disabling drops it. While
    /// the trace is on, the create side runs again once an hour, so a session dropped from outside comes
    /// back. A failure leaves the applied state unchanged so the next sweep retries (a failed drop on Azure
    /// SQL Database up to <see cref="LongQueryTraceDatabases.DropAttemptCap"/> times in a row, then once
    /// an hour), and never breaks the sweep.
    /// </summary>
    private async Task ReconcileLongQueryTraceAsync(ServerLoopState server, DarlingCollectorRunner runner, CancellationToken cancellationToken)
    {
        if (server.Runtime is null)
        {
            return;
        }

        /* XE is a SQL Server concept: there is nothing to create or drop on a PostgreSQL target, and the
           un-gated form was the round-2 live catch — ReconcileLongQueryCompletionsAsync builds a
           SqlConnection, the ctor throws "Keyword not supported: 'host'", the catch below skips the latch
           assignment, and because LongQueryTraceApplied resets to null on every connect the failure retried
           EVERY sweep forever (~1,440 warnings/day/server, the same order as the defect this PR fixed).
           Same gate as EnsureAllAsync and FetchFailedJobsAsync, the two doors this class already closed. */
        if (server.Runtime.Target.Engine != CollectorTargetEngine.SqlServer)
        {
            return;
        }

        var serverId = server.Config.ServerId;
        var enabled = StoreConfigProvider.ResolveSchedule("long_query_completions", serverId, _scheduleOverrides).Enabled;

        /* Azure SQL Database: the other registrations of this logical server, so a drop leaves a database where one
           of them keeps the session. Read from the same live registry the separately monitored list comes from. */
        IReadOnlyList<LongQueryTraceRegistration> registrations = Array.Empty<LongQueryTraceRegistration>();
        IReadOnlyList<string> serverSeparatelyMonitored = Array.Empty<string>();
        if (server.Runtime.Target.IsAzureSqlDb)
        {
            var live = _registryState.Read()?.Servers;
            registrations = LongQueryTraceRegistrations(
                server.Runtime.Config.Host,
                live,
                otherId => StoreConfigProvider.ResolveSchedule("long_query_completions", otherId, _scheduleOverrides).Enabled,
                otherId => runner.DatabaseScopeFor(LongQueryCompletionsCollector.Instance.Name, otherId));
            serverSeparatelyMonitored = LongQueryTraceServerSeparatelyMonitored(server.Runtime.Config.Host, live);
        }

        /* #4961: where the session is the server's own (every engine but Azure SQL Database), the drop while the trace is off
           would stop the trace another registration of this install keeps on the same instance. The guard that says so is a
           function, resolved only when a drop is about to run, so a server whose trace is on, or already reconciled off,
           reads no registry and no state. Each registration's setting is its own override, else the install's default. */
        Func<Task<LongQueryTraceInstanceGuard>>? instanceGuard = null;
        if (!server.Runtime.Target.IsAzureSqlDb)
        {
            instanceGuard = () => LongQueryTraceInstanceGuardFor(
                serverId,
                _registryState.Read()?.Servers,
                otherId => StoreConfigProvider.ResolveSchedule("long_query_completions", otherId, _scheduleOverrides).Enabled,
                (id, carrier) => runner.GetCollectorStateAsync(id, carrier, cancellationToken));
        }

        await ReconcileLongQueryTraceAsync(server, runner, enabled, registrations, serverSeparatelyMonitored, DateTime.UtcNow, _logger, cancellationToken, instanceGuard);
    }

    /// <summary>
    /// The registrations of one logical server, for <see cref="LongQueryTraceDatabases.KeptElsewhere"/>: each live
    /// registration on <paramref name="host"/>, with whether its long-query trace is on, its exclusions and the
    /// trace's database scope (empty = every database). The registry holds only monitored servers, so each one is
    /// enabled.
    /// </summary>
    internal static IReadOnlyList<LongQueryTraceRegistration> LongQueryTraceRegistrations(
        string host,
        IReadOnlyList<MonitoredServer>? live,
        Func<int, bool> traceOn,
        Func<int, IReadOnlyList<string>> databaseScope)
    {
        if (live is null)
        {
            return Array.Empty<LongQueryTraceRegistration>();
        }

        var hostKey = host.Trim();
        return live
            .Where(other => string.Equals(other.Host.Trim(), hostKey, StringComparison.OrdinalIgnoreCase))
            .Select(other =>
            {
                var scope = databaseScope(other.ServerId);
                return new LongQueryTraceRegistration(
                    other.ServerId.ToString(CultureInfo.InvariantCulture),
                    other.Host,
                    other.Database,
                    Enabled: true,
                    TraceOn: traceOn(other.ServerId),
                    other.ExcludedDatabases.ToList(),
                    scope.Count == 0 ? null : scope);
            })
            .ToList();
    }

    /// <summary>
    /// The databases monitored as their own servers on <paramref name="host"/>'s logical server, as that server's
    /// registration sees them (<see cref="AzureMasterScope.SeparatelyMonitoredDatabases"/>). The same list for
    /// every registration of the server, including one that names a database: it says which databases a logical
    /// server's registration leaves out, for <see cref="LongQueryTraceDatabases.KeptElsewhere"/>. No registration's
    /// id is empty, so the empty self id leaves none of them out.
    /// </summary>
    internal static IReadOnlyList<string> LongQueryTraceServerSeparatelyMonitored(string host, IReadOnlyList<MonitoredServer>? live) =>
        AzureMasterScope.SeparatelyMonitoredDatabases(
            isAzureSqlDb: true, selfId: string.Empty, host, database: null, LiveAlertTargets(live));

    /// <summary>
    /// #4961: where the session is the server's own (every engine but Azure SQL Database), this registration's last-known
    /// <c>@@SERVERNAME</c> beside the other registrations of this install that could keep the session on the same instance
    /// (<see cref="LongQueryTraceInstanceGuard"/>): each SQL Server registration in the live registry, which holds only the
    /// monitored servers, whose long-query trace is on. A PostgreSQL registration holds no Extended Events session, so it is
    /// none. <paramref name="traceOn"/> is the effective setting, a registration's own override or else the install's
    /// default. The names come from the identity row a wait_stats or cpu_utilization run persisted
    /// (<see cref="ServerEpoch.LastKnownNameAsync"/>), and are read only when another registration could keep the session,
    /// so an install with no other trace on reads none. With no name of its own this registration matches nothing, so the
    /// names of the others are not read either, and the drop runs as before. Lite's twin is
    /// <c>RemoteCollectorService.LongQueryTraceInstanceGuardFor</c>.
    /// </summary>
    /// <param name="serverId">This registration's store id.</param>
    /// <param name="live">The live registry, or null when none has been published yet.</param>
    /// <param name="traceOn">Whether a registration's long-query trace is on, by its store id.</param>
    /// <param name="readCarrierState">Reads one carrier collector's persisted state for a registration, by its store id.</param>
    internal static async Task<LongQueryTraceInstanceGuard> LongQueryTraceInstanceGuardFor(
        int serverId,
        IReadOnlyList<MonitoredServer>? live,
        Func<int, bool> traceOn,
        Func<int, string, Task<Dictionary<string, string>>> readCarrierState)
    {
        ArgumentNullException.ThrowIfNull(traceOn);
        ArgumentNullException.ThrowIfNull(readCarrierState);

        var candidates = (live ?? Array.Empty<MonitoredServer>())
            .Where(other => other.ServerId != serverId && !other.IsPostgres && traceOn(other.ServerId))
            .ToList();
        if (candidates.Count == 0)
        {
            return LongQueryTraceInstanceGuard.NoKeepers;
        }

        var ownName = await ServerEpoch.LastKnownNameAsync(carrier => readCarrierState(serverId, carrier));
        var keepers = new List<LongQueryTraceInstance>(candidates.Count);
        foreach (var other in candidates)
        {
            var otherId = other.ServerId;
            var name = ownName is null ? null : await ServerEpoch.LastKnownNameAsync(carrier => readCarrierState(otherId, carrier));
            keepers.Add(new LongQueryTraceInstance(Enabled: true, TraceOn: true, name));
        }

        return new LongQueryTraceInstanceGuard(ownName, keepers);
    }

    /// <summary>
    /// #4961: a SQL Server restart stops the long-query trace's session, because the per-install session is created
    /// with <c>STARTUP_STATE = OFF</c>, and a stopped session reads as a quiet one. When a collector run saw the
    /// instance's identity move (<see cref="ServerEpoch.IdentityChangesMeasurement"/>: the start time, or the name
    /// after a failover), this clears the long-query latch and its hourly create clock, so the next sweep runs the
    /// whole reconcile and starts the session, instead of waiting for the hourly pass. Returns true when it cleared.
    /// </summary>
    internal static bool ForgetLongQueryTraceLatchOnRestart(ServerLoopState server, IReadOnlyList<CollectorMeasurement> measurements)
    {
        ArgumentNullException.ThrowIfNull(server);
        ArgumentNullException.ThrowIfNull(measurements);

        if (!measurements.Any(m => string.Equals(m.Label, ServerEpoch.IdentityChangesMeasurement, StringComparison.Ordinal) && m.Value > 0))
        {
            return false;
        }

        server.ForgetLongQueryTraceLatch();
        return true;
    }

    /// <summary>
    /// The half of <see cref="ReconcileLongQueryTraceAsync(ServerLoopState, DarlingCollectorRunner, CancellationToken)"/> that
    /// runs after the engine gate and the schedule: it decides whether the trace needs reconciling, runs it, and records
    /// the outcome on the loop state. Static, with the enabled flag and the time passed in, so a test can drive it.
    /// </summary>
    internal static async Task ReconcileLongQueryTraceAsync(
        ServerLoopState server,
        DarlingCollectorRunner runner,
        bool enabled,
        IReadOnlyList<LongQueryTraceRegistration> registrations,
        IReadOnlyList<string> serverSeparatelyMonitored,
        DateTime utcNow,
        ILogger logger,
        CancellationToken cancellationToken,
        Func<Task<LongQueryTraceInstanceGuard>>? instanceGuard = null)
    {
        /* #4961: a removed server's sweep that was already running reaches here after the removal retired the server and
           dropped its session, and a create now would outlive the server. */
        if (server.Runtime is null || server.Retired)
        {
            return;
        }

        /* #6893: Amazon RDS Web and Express refuse every session create, and write each refusal into the instance's own error log. The
           collector does not apply there, so neither the create nor the drop is sent, on any sweep. */
        if (!CollectorTargetInfo.XeSessionSupported(server.Runtime.Target))
        {
            server.LongQueryTraceFault = null;
            server.LongQueryTracePartialNote = null;
            return;
        }

        /* On Azure SQL Database the trace follows the monitored set, so the latch also holds what that set
           depended on: the scope the read loop resolves (the runner's own accessor, not a copy), the exclusions,
           and the databases monitored as their own servers. Any change re-runs the reconcile, which drops the
           session from a database newly left out. The co-owners are there too: a drop skipped because another
           registration kept the session runs once that one turns its trace off. */
        var stateKey = server.Runtime.Target.IsAzureSqlDb
            ? LongQueryTraceDatabases.StateKey(
                enabled,
                runner.DatabaseScopeFor(LongQueryCompletionsCollector.Instance.Name, server.Runtime.ServerId),
                server.Runtime.Config.ExcludedDatabases,
                runner.SeparatelyMonitoredDatabasesFor(server.Runtime),
                LongQueryTraceDatabases.CoOwners(
                    server.Runtime.ServerId.ToString(CultureInfo.InvariantCulture),
                    server.Runtime.Config.Host,
                    registrations,
                    serverSeparatelyMonitored))
            : null;

        /* Latched, the reconcile runs again only on the hour. The whole pass runs when the cleanup's attempt after the
           cap is due. Otherwise, while the trace is on, the create side runs alone, so a session dropped from outside
           comes back: the read returns no rows for an absent session, the same as for a quiet one. The drop side
           runs once per connect, per change of the state key, or per attempt after the cap. */
        LongQueryTracePass pass;
        if (server.LongQueryTraceApplied != enabled
            || !string.Equals(server.LongQueryTraceAppliedKey, stateKey, StringComparison.Ordinal))
        {
            pass = LongQueryTracePass.Full;
        }
        else if (server.LongQueryTraceDropRetry.RetryDue(stateKey ?? string.Empty, utcNow))
        {
            pass = LongQueryTracePass.RetryAfterCap;
        }
        else if (enabled
                 && server.LongQueryTraceAppliedAtUtc is { } appliedAt
                 && utcNow - appliedAt >= LongQueryTraceDatabases.RetryInterval)
        {
            pass = LongQueryTracePass.CreateOnly;
        }
        else
        {
            return;
        }

        /* #4964: the pass decides its log level once, from what the passes before it did. A create that has already
           warned logs its failure again at Debug, in the reconcile's own lines and in the catch below. */
        var createFailureWarned = enabled && server.LongQueryTraceCreateWarned;

        try
        {
            var outcome = new LongQueryTraceReconcileOutcome();
            var partialNote = await DarlingXeSessions.ReconcileLongQueryCompletionsAsync(
                server.Runtime, runner, enabled, pass, registrations, serverSeparatelyMonitored, createFailureWarned, logger, cancellationToken, instanceGuard, outcome);

            /* #4961: a start the replica refused just after this reconcile created the definition (it does not show it yet) is
               not applied: the latch stays where it was, so the next sweep tries again and starts the session, instead of the
               retry waiting for the hourly create pass. */
            if (!outcome.StartPending)
            {
                server.LongQueryTraceApplied = enabled;
                server.LongQueryTraceAppliedKey = stateKey;
                server.LongQueryTraceAppliedAtUtc = utcNow;
            }

            /* Only a pass that ran the cleanup ends its retries: the create side alone leaves the hourly attempt. */
            if (pass != LongQueryTracePass.CreateOnly)
            {
                server.LongQueryTraceDropRetry.Reset();
            }

            /* #3754: a reconcile that returned is one the session exists after - everywhere, or (Azure)
               everywhere it could. Clear the fault, and carry the partial note if there was one. Nulled
               rather than left alone on the DISABLED arm: the collector is not dispatched while disabled,
               so a stale note from an earlier enabled reconcile must not be waiting when it is re-enabled
               and the next reconcile (which runs first, in this same sweep) has already replaced it. */
            server.LongQueryTraceFault = null;
            server.LongQueryTracePartialNote = enabled ? partialNote : null;

            /* #4964: a create that succeeded ends the run of failures, so the next one warns again. */
            server.LongQueryTraceCreateWarned = false;
        }
        catch (LongQueryTraceDropException ex)
        {
            /* #4964: only the drop failed, so the create side finished: its run of failures is over. */
            server.LongQueryTraceCreateWarned = false;

            /* A drop failed: on Azure SQL Database, in some databases or in listing them, and on every other engine, the
               server's own (#4964). While enabling, the
               create side had finished, so the fault clears and its partial note stands. The latch stays unset so
               the next sweep tries again, until the cap: then the reconcile counts as applied, and one warning
               names the databases where the session may remain. After that, one attempt an hour, logged at Debug,
               so the warning is not repeated. */
            if (enabled)
            {
                server.LongQueryTraceFault = null;
                server.LongQueryTracePartialNote = ex.CreateNote;
                server.LongQueryTraceAppliedAtUtc = utcNow;
            }

            switch (server.LongQueryTraceDropRetry.RecordFailure(stateKey ?? string.Empty, utcNow))
            {
                case LongQueryTraceDropOutcome.GaveUp:
                    server.LongQueryTraceApplied = enabled;
                    server.LongQueryTraceAppliedKey = stateKey;
                    logger.LogWarning("[{Server}] {Message}", server.Config.DisplayName,
                        LongQueryTraceDatabases.GiveUpWarning(ex, " It also tries again after it reconnects."));
                    break;
                case LongQueryTraceDropOutcome.TryAgainInAnHour:
                    logger.LogDebug("[{Server}] {Message} The next attempt is in an hour.", server.Config.DisplayName, ex.Message);
                    break;
                default:
                    logger.LogWarning("[{Server}] {Message} The next sweep tries again.", server.Config.DisplayName, ex.Message);
                    break;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* #4964: the first failure of a create that cannot succeed logs at Warning. The sweeps after it retry the
               create just the same, and record the fault just the same below, but log at Debug until a create succeeds
               or the server reconnects. */
            /* #4961: a read-only database's refusal was already logged where it happened, with why and what to change. */
            var readOnlyRefusal = enabled && DarlingXeSessions.IsReadOnlyDatabaseRefusal(ex);
            /* #5378: a login that was told no (error 15247, the other denial numbers) stays told no until it is granted
               something, so the create is not re-run on every sweep: it backs off exactly like the read-only refusal above,
               to the hourly create pass, and a reconnect or a change of the state key tries again at once. */
            var permissionDenied = enabled && DarlingXeSessions.ErrorNumbersOf(ex).Any(SqlServerPermissionErrors.IsPermissionDenied);
            logger.Log(createFailureWarned || readOnlyRefusal ? LogLevel.Debug : LogLevel.Warning,
                "[{Server}] Failed to reconcile the long-query completion XE session: {Message}",
                server.Config.DisplayName, AlwaysOnXeSessions.DescribeFailure(ex));
            server.LongQueryTraceCreateWarned = enabled;

            /* #4961: a read-only database stays read-only until the registration or the database changes, so a create it
               refused is not tried again on every sweep. The latch counts the reconcile as applied for this state: the next
               attempt is the hourly create pass, logged at Debug, and a reconnect or a change of the state key runs the
               whole reconcile again. The fault below stays set, so every run still records it. */
            if (readOnlyRefusal || permissionDenied)
            {
                server.LongQueryTraceApplied = enabled;
                server.LongQueryTraceAppliedKey = stateKey;
                server.LongQueryTraceAppliedAtUtc = utcNow;
            }

            /* A pass that ran and threw must move its hourly clock, or every later sweep is another pass and another
               warning, with no cap. The full pass keeps its retry on each sweep: the latch is still unset. The hourly
               create pass waits an hour. The attempt after the cap waits an hour too, and the create side it ran
               counts as the hourly create pass, as in the drop-failure arm above. The fault below stays set until a
               pass succeeds. */
            if (pass == LongQueryTracePass.CreateOnly)
            {
                server.LongQueryTraceAppliedAtUtc = utcNow;
            }
            else if (pass == LongQueryTracePass.RetryAfterCap)
            {
                server.LongQueryTraceDropRetry.RecordFailure(stateKey ?? string.Empty, utcNow);
                if (enabled)
                {
                    server.LongQueryTraceAppliedAtUtc = utcNow;
                }
            }

            /* #3754: while ENABLING, a throw means the session could not be created where the collector
               will read - the one CREATE on-prem, or every database on Azure SQL DB (the Azure arm throws
               only for all-refused). Record it so this sweep's run of the collector is classified
               SESSION_MISSING rather than dispatched to read an absent session as zero rows. The database
               name rides on the exception where the Azure arm stamped one (#2997); on-prem there is no
               database to name. A DISABLING failure records nothing here: the collector is not dispatched
               while disabled, so there is no run to be honest on, and the retry the unchanged latch already
               buys is the whole of the remedy. */
            if (enabled)
            {
                var refusedIn = CollectorFaultDatabase.For(ex, fallback: null);
                /* #4961: this install's own session, named from its id. With no id there is no name to give. */
                var sessionLabel = runner.LongQuerySessionName() ?? "(unnamed: this install has no id)";

                /* #4961: the sentence about Azure SQL Database's caps rides on a failed create or start there, and on nothing else. */
                var refusal = AlwaysOnXeSessions.DescribeFailure(ex);
                var where = refusedIn is null ? string.Empty : $" in any monitored database (first refusal in [{refusedIn}])";
                server.LongQueryTraceFault = permissionDenied
                    ? $"XE session {sessionLabel} could not be created{where} because the monitoring login lacks the permission to create Extended Events sessions (ALTER ANY EVENT SESSION on-premises, CREATE ANY DATABASE EVENT SESSION on Azure SQL Database), so no completions can be captured until it is granted: {refusal}"
                    : $"XE session {sessionLabel} could not be created{where}, so no completions can be captured until it is: {refusal}";
                server.LongQueryTraceFaultIsPermission = permissionDenied;
                server.LongQueryTracePartialNote = null;
            }
        }
    }

    /// <summary>
    /// Materializes the baseline aggregates over the history the store already had (#1757), concurrently with
    /// the collection sweep rather than ahead of it. On a fresh store the coverage gate makes this a no-op; on
    /// an upgraded store it is the one-time pass that turns a refresh-window-deep supply into the full
    /// baseline window.
    ///
    /// <para>Takes its OWN connection rather than borrowing the startup one: the caller's connection is scoped
    /// to the TimescaleDB setup block and is disposed the moment that block exits, which is long before this
    /// finishes. Everything is swallowed but cancellation — a store that cannot be backfilled must degrade to
    /// short baselines, never take down collection — and <see cref="TimescaleSupport.BackfillBaselineAggregatesAsync"/>
    /// is itself failure-isolated per aggregate, so this catch is only for the connection-open path.</para>
    /// </summary>
    private async Task RunBaselineBackfillAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(stoppingToken);
            var backfilled = await TimescaleSupport.BackfillBaselineAggregatesAsync(connection, _logger, stoppingToken);
            if (backfilled > 0)
            {
                _logger.LogInformation(
                    "TimescaleDB: baseline backfill complete — {Backfilled} aggregate(s) materialized over pre-existing history.",
                    backfilled);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Baseline aggregate backfill could not run — baselines are computed from however much history the aggregates already hold: {Message}",
                ex.Message);
        }
    }

    /// <summary>
    /// Refreshes the rebuilt collection-health rollup once (#4812, <see cref="TimescaleSupport.WarmCollectionHealthHourlyAsync"/>),
    /// concurrently with the rest of startup, for the reason <see cref="RunBaselineBackfillAsync"/> gives. Its
    /// OWN connection, since the startup one is scoped to the TimescaleDB block; everything but cancellation is
    /// swallowed, because a rollup that could not be warmed just answers from raw until its policy runs.
    /// </summary>
    private async Task RunCollectionHealthRollupWarmAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(stoppingToken);
            await TimescaleSupport.WarmCollectionHealthHourlyAsync(connection, _logger, DateTime.UtcNow, stoppingToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Could not open a connection to refresh the collection-health rollup (#4812) - it fills on its refresh policy's first run: {Message}",
                ex.Message);
        }
    }

    /// <summary>
    /// #4391: whether a materialization-hole repair pass's summary is clean enough to stamp the repair
    /// epoch — true only when the pass had zero isolated per-aggregate failures. Deferred holes
    /// (<see cref="TimescaleSupport.MaterializationHoleRepairSummary.HolesDeferred"/>) and holes still
    /// remaining after repair do not disqualify a stamp: those are re-measured fresh by the Periodic raw
    /// purge trigger's own hole scan at drop time, so a deferred-but-otherwise-clean pass may still stamp.
    /// </summary>
    internal static bool RepairEpochStampAllowed(TimescaleSupport.MaterializationHoleRepairSummary summary)
        => summary.Failures == 0;

    /// <summary>
    /// Scans every continuous aggregate for materialization holes and closes each with one targeted forced
    /// refresh (#3653, Q10 — <see cref="TimescaleSupport.RepairMaterializationHolesAsync"/>), concurrently with
    /// the rest of startup rather than ahead of it, for the reason <see cref="RunBaselineBackfillAsync"/> gives:
    /// the work is bounded but not small on the largest store, and a restarted service must not go dark for it.
    ///
    /// <para>Its OWN connection, like the backfill's — the startup connection is scoped to the TimescaleDB
    /// block. Everything but cancellation is swallowed here: the pass is failure-isolated per aggregate
    /// already, so this catch is for the connection-open path, and a store whose holes cannot be repaired
    /// degrades to the holes it had, never to a service that did not start.</para>
    ///
    /// <para><b>One INFORMATION line per start, whatever the scan found (#3756).</b> The pass returns its
    /// tally and this method writes it, UNCONDITIONALLY. It is the self-proving-flag class (#3574's
    /// <c>visibility</c>, #3735's <c>collection_health_age_seconds</c>): a check whose negative outcome is
    /// indistinguishable from its non-execution has not reported. The first store to carry the scan showed
    /// the failure: the pass wrote a summary only when it had repaired, deferred or failed something, so a
    /// start that found nothing, a start whose scan threw before its first probe and was swallowed by the
    /// per-aggregate isolation, and a start that never reached the scan all left the same absence, and the
    /// countersign had to accept "zero needed" on inference. Now every outcome writes a DIFFERENT line, all
    /// from this one method so a reader has one place to look: the pass returned — this summary, with zeros
    /// when there were zeros and the isolated-failure count when isolation caught anything (the per-aggregate
    /// WARNING already names the aggregate and the message; the summary's job is the census, so it stays
    /// INFORMATION rather than repeating the failure at WARNING and counting it twice in any warning-rate
    /// read); the pass threw outside its isolation or the connection would not open — the WARNING below;
    /// shutdown cancelled it before it could report — the cancellation line, so the next start's reader knows
    /// this one's scan is not a verdict. A start that never launches it (plain-PostgreSQL mode, or the
    /// TimescaleDB block faulting first) already writes its own line at the launch site's catch; nothing here
    /// can or should speak for that case.</para>
    /// </summary>
    private async Task RunMaterializationHoleRepairAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        _materializationHoleRepairRunning = true;
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(stoppingToken);

            /* #4299: read BEFORE the repair runs, so the value stamped on a clean finish is the postmaster
               start this repair actually ran under, not whatever it is by the time the repair returns. A
               connection-open failure here throws out to the catch below with nothing stamped, which is right —
               a repair that never ran must not make the trigger's epoch check pass. */
            long postmasterEpoch;
            await using (var epochRead = new NpgsqlCommand(TimescaleSupport.PostmasterStartEpochMicrosecondsSql, connection))
            {
                postmasterEpoch = (long)(await epochRead.ExecuteScalarAsync(stoppingToken))!;
            }

            var summary = await TimescaleSupport.RepairMaterializationHolesAsync(connection, _logger, DateTime.UtcNow, stoppingToken);

            /* #3756: not gated on any count — the zero-hole start is the one this line exists for. Every figure
               is the pass's own (TimescaleSupport.MaterializationHoleRepairSummary states the arithmetic:
               buckets found = repaired + deferred; a range the cap split counts once as found and once on each
               side), and the elapsed is the pass's clock, not this method's connection open. */
            _logger.LogInformation(
                "TimescaleDB: materialization hole scan (#3653 Q10) — {Scanned} aggregate(s) walked, {Skipped} skipped, {HolesFound} hole(s) found spanning {BucketsFound} bucket(s), {HolesRepaired} hole(s) / {BucketsRepaired} bucket(s) repaired ({Forced} needed the forced refresh), {HolesDeferred} hole(s) / {BucketsDeferred} bucket(s) deferred past the cap, {Remaining} bucket(s) still reading as holes after repair, {DailyChained} successor-daily bucket(s) chained (#4300), {Failures} isolated failure(s), in {ElapsedMs} ms.",
                summary.AggregatesScanned, summary.AggregatesSkipped, summary.HolesFound, summary.BucketsFound,
                summary.HolesRepaired, summary.BucketsRepaired, summary.HolesForced, summary.HolesDeferred, summary.BucketsDeferred,
                summary.HolesRemaining, summary.DailyBucketsChained, summary.Failures, (long)summary.Elapsed.TotalMilliseconds);

            /* #4299/#4391: the completion stamp — reached ONLY here, past both the epoch read and the
               repair call, neither of which threw or was cancelled. Stamped on every one of the three raw jobs
               (TimescaleSupport.RawRelations), one statement per job, each guarded by its own
               IS DISTINCT FROM so a repeat stamp of the same value writes nothing. A stamp failure here is
               logged and swallowed per job — the repair itself already succeeded and reporting that success
               is not conditional on the stamp also landing; a job whose stamp did not take simply stays
               ungated for the Periodic trigger until the next repair tries again.

               The stamp itself is gated on RepairEpochStampAllowed(summary): a repair that ran with one or
               more isolated per-aggregate failures (summary.Failures > 0) must NOT stamp, because the epoch
               is the trigger's promise that the repair covering it finished clean — TimescaleSupport's own
               doc on the stamp says an isolated failure must not make that check pass. Deferred holes and
               holes still remaining after repair (summary.HolesDeferred, summary.HolesRemaining) do NOT block
               the stamp: those are read fresh by the trigger's own hole scan at purge time, which is the gate
               for them; only a failure that means this pass never finished cleanly blocks it here. */
            if (RepairEpochStampAllowed(summary))
            {
                foreach (var relation in TimescaleSupport.RawRelations)
                {
                    try
                    {
                        await using var stamp = new NpgsqlCommand(TimescaleSupport.RawRepairEpochStampSql(relation), connection);
                        stamp.Parameters.AddWithValue(postmasterEpoch);
                        await stamp.ExecuteNonQueryAsync(stoppingToken);
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        _logger.LogWarning(
                            "Materialization-hole repair finished, but stamping the repair epoch on {Relation}'s raw retention job failed — the Periodic trigger will not purge it until a later repair stamps it: {Message}",
                            relation, ex.Message);
                    }
                }
            }
            else
            {
                _logger.LogWarning(
                    "Materialization-hole repair finished with {Failures} failure(s); the repair epoch stays unstamped, so the Periodic raw purge keeps holding until a repair completes cleanly.",
                    summary.Failures);
            }

            /* #4716: the one-time heal of the daily days an earlier hourly repair left short — AFTER the stamp,
               so nothing the heal does can change the stamp, and on this same connection while
               _materializationHoleRepairRunning is still true, so no other repair refreshes the same aggregate
               at the same time and collection never waits on it. Isolated in its own method: a heal failure
               never changes the stamp above, the summary line above, or the start. */
            await RunPartialDailyHealAsync(connection, stoppingToken);

            /* #6070: then the days a daily-parented daily is missing although its parent daily has them, which
               neither the hole scan (it skips a daily that has materialized nothing) nor the heal (it leaves a
               source-with-rows, daily-without-row day to the scan) reaches. Same connection and flag, same
               isolation: a failure here changes nothing above. */
            await RunDailyChildHoleRepairAsync(connection, stoppingToken);
        }
        catch (OperationCanceledException)
        {
            /* #3756: the absence the issue's three did not name. Without this, a scan cut short by shutdown leaves
               exactly the nothing a scan that never ran leaves. Rethrown so the drain's contract is untouched (it
               swallows this as "expected on shutdown"); the line is the whole of the change. */
            _logger.LogInformation(
                "TimescaleDB: materialization hole scan (#3653 Q10) was cancelled before it could report — at shutdown that is expected, and the next start's scan re-finds anything this one left, because the scan reads the materialization rather than a ledger of what this start meant to do.");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Materialization-hole repair could not run — any pre-outage tail the refresh policies skipped stays unmaterialized until the next start retries: {Message}",
                ex.Message);
        }
        finally
        {
            _materializationHoleRepairRunning = false;
        }
    }

    /// <summary>
    /// Runs <see cref="PartialDailyHeal.RunAsync"/> (#4716) on the hole repair's own connection, after that
    /// repair's completion stamp, and writes ONE INFORMATION line whatever it found — a run with nothing to do
    /// must not look like a run that never happened (#3756). A failure here is logged at WARNING and stops
    /// nowhere else: the stamp, the repair's summary line and the start are all behind it. A cancelled run says
    /// so and writes no marker for a daily it had not finished, so the next start walks that daily again.
    /// </summary>
    private async Task RunPartialDailyHealAsync(NpgsqlConnection connection, CancellationToken stoppingToken)
    {
        try
        {
            var summary = await PartialDailyHeal.RunAsync(connection, _logger, DateTime.UtcNow, stoppingToken);
            _logger.LogInformation(
                "Partial-daily heal (#4716): {Checked} daily rollup(s) checked, {AlreadyDone} already done, {Compared} day(s) compared, {Partial} partial, {Refreshed} refreshed ({Chained} more chained to dependent dailies), {Failures} failure(s), in {Seconds:F1} s.",
                summary.DailiesChecked, summary.DailiesAlreadyDone, summary.DaysCompared, summary.DaysPartial,
                summary.DaysRefreshed, summary.DaysChained, summary.Failures, summary.Elapsed.TotalSeconds);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Partial-daily heal (#4716) was cancelled before it could report — at shutdown that is expected, and a daily it had not finished has no marker, so the next start walks it again.");
        }
        catch (Exception ex)
        {
            var sqlState = ex is NpgsqlException { SqlState: { Length: > 0 } state } ? state : null;
            _logger.LogWarning(
                "Partial-daily heal (#4716) could not run — a daily it had not finished has no marker, so the next start walks it again: {ExceptionType}{SqlState}",
                ex.GetType().Name, sqlState is null ? "" : $", SQLSTATE {sqlState}");
        }
    }

    /// <summary>
    /// Runs <see cref="TimescaleSupport.RepairDailyChildHolesAsync"/> (#6070) on the hole repair's own connection and
    /// writes ONE INFORMATION line whatever it found, so "nothing to do" differs from "never ran" (#3756). A
    /// failure is one WARNING and stops nowhere else.
    /// </summary>
    private async Task RunDailyChildHoleRepairAsync(NpgsqlConnection connection, CancellationToken stoppingToken)
    {
        try
        {
            var summary = await TimescaleSupport.RepairDailyChildHolesAsync(connection, _logger, DateTime.UtcNow, stoppingToken);
            _logger.LogInformation(
                "Daily-child hole repair (#6070): {Scanned} daily-parented rollup(s) scanned, {Found} day(s) missing that the parent holds, {Refreshed} refreshed, {Failures} failure(s){More}, in {Seconds:F1} s",
                summary.DailiesScanned, summary.DaysFound, summary.DaysRefreshed, summary.Failures,
                summary.DaysRemaining ? " - more remain for the next start" : string.Empty, summary.Elapsed.TotalSeconds);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogInformation(
                "Daily-child hole repair (#6070) was cancelled before it could report - at shutdown that is expected, and the next start scans again.");
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                "Daily-child hole repair (#6070) could not run - the next start scans again: {ExceptionType}",
                ex.GetType().Name);
        }
    }

    /// <summary>
    /// Runs <see cref="PgSettingScrub.RunAsync"/> once (#4348 S1b), concurrently with the rest of startup.
    /// Its own connection and its own catch, the same isolation as <see cref="RunMaterializationHoleRepairAsync"/>:
    /// a store this cannot reach degrades to whatever plaintext it already had, never to a service that did
    /// not start. One INFORMATION line whatever the run found, so "nothing to do" (already scrubbed) is
    /// visibly different from "never ran" in the log.
    /// </summary>
    private async Task RunPgSettingScrubAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            var summary = await PgSettingScrub.RunAsync(postgres, _logger, stoppingToken);
            if (summary.AlreadyDone)
            {
                _logger.LogInformation("Postgres setting scrub (#4348): already scrubbed at the current rules version — nothing to do.");
            }
            else
            {
                _logger.LogInformation(
                    "Postgres setting scrub (#4348): {Candidates} candidate row(s) read, {Updated} row(s) redacted across {Days} day(s).",
                    summary.CandidatesRead, summary.RowsUpdated, summary.DaysTouched);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "Postgres setting scrub (#4348) was cancelled before it could report — at shutdown that is expected, and the next start retries from the top because the marker is only written after every batch completes.");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "Postgres setting scrub (#4348) could not run — any row an older collector build stored unredacted stays as it is until the next start retries: {Message}",
                ex.Message);
        }
    }

    /// <summary>
    /// Runs <see cref="PgStatementTextScrub.RunAsync"/> once (#4348), concurrently with the rest of startup.
    /// Same isolation as <see cref="RunPgSettingScrubAsync"/>: its own connection, its own catch, and a
    /// store this cannot reach retries the scrub at the next start, never blocking the service from
    /// starting.
    /// </summary>
    private async Task RunPgStatementTextScrubAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            var summary = await PgStatementTextScrub.RunAsync(postgres, _logger, stoppingToken);
            if (summary.AlreadyDone)
            {
                _logger.LogInformation("Postgres statement-text scrub (#4348): already scrubbed at the current scrub version — nothing to do.");
            }
            else
            {
                _logger.LogInformation(
                    "Postgres statement-text scrub (#4348): {StatementTextUpdated} pg_statement_text row(s) and {BlockingEdgesUpdated} pg_blocking_edges row(s) updated.",
                    summary.StatementTextRowsUpdated, summary.BlockingEdgesRowsUpdated);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "Postgres statement-text scrub (#4348) was cancelled before it could report — at shutdown that is expected, and the next start retries from the top because the marker is only written after every batch completes.");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Never the exception TEXT — just the exception type and SQLSTATE (when it is an NpgsqlException),
               the same discipline PgStatementTextScrub's own per-server/per-day catches apply. */
            _logger.LogWarning(
                "Postgres statement-text scrub (#4348) could not run ({ExceptionType}{SqlState}) — the scrub retries at the next start.",
                ex.GetType().Name, ex is NpgsqlException npgsqlEx ? $", SQLSTATE {npgsqlEx.SqlState}" : string.Empty);
        }
    }

    /// <summary>
    /// Runs <see cref="DeadlockDuplicateCleanup.RunAsync"/> once, concurrently with the rest of startup. Same
    /// isolation as <see cref="RunPgStatementTextScrubAsync"/>: its own connection, its own catch, and a store this
    /// cannot reach retries the cleanup at the next start, never blocking the service from starting.
    /// </summary>
    private async Task RunDeadlockDuplicateCleanupAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            var summary = await DeadlockDuplicateCleanup.RunAsync(postgres, _logger, stoppingToken);
            if (summary.AlreadyDone)
            {
                _logger.LogInformation("Deadlock duplicate cleanup: already done at the current cleanup version — nothing to do.");
            }
            else
            {
                _logger.LogInformation(
                    "Deadlock duplicate cleanup: {Removed} exact duplicate row(s) removed across {Days} (server, day) batch(es).",
                    summary.RowsRemoved, summary.DaysVisited);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "Deadlock duplicate cleanup was cancelled before it could report — at shutdown that is expected, and the next start retries because the marker is only written after every batch completes.");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Never the exception TEXT — just the exception type and SQLSTATE (when it is an NpgsqlException). */
            _logger.LogWarning(
                "Deadlock duplicate cleanup could not run ({ExceptionType}{SqlState}) — the cleanup retries at the next start.",
                ex.GetType().Name, ex is NpgsqlException npgsqlEx ? $", SQLSTATE {npgsqlEx.SqlState}" : string.Empty);
        }
    }

    /// <summary>
    /// Runs <see cref="PlanForceActionDetailScrub.RunAsync"/> once (#4346), concurrently with the rest of
    /// startup. Its own connection and its own catch, the same isolation as
    /// <see cref="RunPgSettingScrubAsync"/>: a store this cannot reach keeps whatever legacy text it
    /// already had, never a service that did not start.
    /// </summary>
    private async Task RunPlanForceActionDetailScrubAsync(NpgsqlDataSource postgres, CancellationToken stoppingToken)
    {
        try
        {
            var summary = await PlanForceActionDetailScrub.RunAsync(postgres, _logger, stoppingToken);
            if (summary.AlreadyDone)
            {
                _logger.LogInformation("Plan-force-action detail scrub (#4346): already scrubbed — nothing to do.");
            }
            else
            {
                _logger.LogInformation(
                    "Plan-force-action detail scrub (#4346): {Candidates} candidate row(s) read, {Updated} row(s) redacted.",
                    summary.CandidatesRead, summary.RowsUpdated);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation(
                "Plan-force-action detail scrub (#4346) was cancelled before it could report — at shutdown that is expected, and the next start retries from the top because the marker is only written after every batch completes.");
            throw;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Never ex.Message here — this scrub's whole reason for existing is a row that carried
               unredacted exception text, and its own failure path must not repeat that mistake. Log the
               exception's type and SQLSTATE only, the same shape SanitizeDetailForAudit's own matched
               templates and CollectionFailure.Describe use. */
            var sqlState = ex is Npgsql.NpgsqlException { SqlState: { Length: > 0 } state } ? state : null;
            _logger.LogWarning(
                "Plan-force-action detail scrub (#4346) could not run — any legacy row stays as it is until the next start retries: {ExceptionType}{SqlState}",
                ex.GetType().Name, sqlState is null ? "" : $", SQLSTATE {sqlState}");
        }
    }
}
