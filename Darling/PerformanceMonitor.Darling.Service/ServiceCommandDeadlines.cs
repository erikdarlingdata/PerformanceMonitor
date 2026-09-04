/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Explicit command deadlines for this project's store access (#2874), one constant per BUDGET REGIME.
///
/// <para><c>.Service</c> is roughly fifteen regimes, not one, and they are being closed a regime at a
/// time; this file is where each one's constant lands. Only the collection sweep is here so far.
/// <b>Do not reuse a constant across regimes</b> — the four numbers this sweep has already produced
/// (60 s in <c>.Analysis</c> from a 120 s <c>CancelAfter</c>, 10 s for the alert pass from its 30 s
/// cadence, <c>.Storage</c>'s five, <c>.Viewer</c>'s 15/5/10 from a connection permit) each came from a
/// different enclosing constraint and none of them transferred.</para>
/// </summary>
public static class ServiceCommandDeadlines
{
    /// <summary>
    /// The per-server collection body's store reads and writes — every collector's state read, its
    /// watermark read, its <c>collection_log</c> row, and the binary COPY that is the collector's
    /// actual store write on all four transports.
    ///
    /// <para><b>The regime.</b> Nothing encloses these: <c>ProcessServerSweepAsync</c> runs on the plain
    /// stopping token, with no <c>CancelAfter</c> anywhere between the 15 s launch tick and the
    /// collector call. What the body DOES hold is scarce — one of <c>max_concurrent_sweeps</c> permits
    /// and, post-#2822, one borrowed store connection out of <c>MaxPoolSize = 24</c>, both for its whole
    /// duration — and the 60 s <c>DarlingWorker.SweepWatchdogSeconds</c> only LOGS, so nothing
    /// interrupts a stalled body. The per-command deadline is the only bound that exists.</para>
    ///
    /// <para><b>ABOVE the measured worst case.</b> The store-write half of every collector run is
    /// already instrumented as <c>collection_log.store_duration_ms</c>. Over 24 h on the busiest server
    /// of the larger production fleet (43 servers, a 305 GB store ingesting ~15-20 GB/day) the worst of
    /// 200 runs was <b>1.53 s</b> — <c>query_store</c> writing 11,614 rows as a per-database batch
    /// sequence. Fleet-wide, the per-run store-write average across all 39 collectors on both
    /// production stores tops out at <b>1,511 ms</b> (<c>index_object_stats</c>, ~21,090 rows/run) with
    /// most collectors in the single-digit-to-low-tens ms. On top of that sits store connection
    /// acquisition, measured at <b>673-893 ms</b> in #2819. So ~2.4 s covers the worst thing actually
    /// observed, and 10 s keeps ~4x headroom over it.</para>
    ///
    /// <para><b>BELOW the point where the deadline costs more than it saves</b>, and the bound is the
    /// 60 s watchdog rather than a cadence, because the body runs its due collectors SEQUENTIALLY. Real
    /// cycles on that same server span 13 s (a light tick) to 48 s (a tick including
    /// <c>query_store</c>). One blown deadline on a 48 s cycle lands at 58 s and stays under the
    /// watchdog; one blown 30 s DEFAULT lands at 78 s and manufactures a "collection body has not
    /// completed after 60s" warning on a body that is merely slow. Keeping a single overshoot inside the
    /// watchdog is what stops this change from re-creating the #1581/#2170 warning herd.</para>
    ///
    /// <para><b>Why it is derived at <c>max_concurrent_sweeps</c> = 8, not the seeded 4.</b> The knob
    /// (V59, clamped 1-16) is going from 4 to 8, which doubles the bodies in flight and therefore the
    /// concurrent store connections the sweep holds — the pool is NOT the binding constraint at either
    /// width (borrowing put peak demand at the sweep width itself, and
    /// <c>DarlingManagedPostgres</c>'s own note establishes even 16-wide fits inside 24 with the
    /// seams), but the DURATION of a stalled hold is: C bodies each stalled T seconds withhold C x T
    /// connection-seconds from retention, alerting and observability. At the 30 s default and C = 8 that
    /// is 8 of 24 connections held for 30 s, a third of the pool, against a sixth at C = 4. So the
    /// floor above was doubled before the headroom was taken — 1.53 s of measured store write projected
    /// to ~3.1 s if write latency scaled linearly with twice the concurrent writers, plus ~0.9 s of
    /// acquisition, is the ~4.0 s that 10 s is set to clear. <b>The value is correct at 8 and at 16;
    /// it was chosen so that raising the knob does not invalidate it.</b></para>
    ///
    /// <para><b>What this is NOT derived from.</b> Not the 120 s <c>PerItemWallClockBudget</c>
    /// abandonment tail — that was root-caused to external contention on the monitored target
    /// (confined to 2 of 43 servers, load-invariant across a 6.1x cadence increase: runs x6.1,
    /// abandonments x1.11), so a store-side deadline is not the lever for it. And not
    /// <c>collector_cost.max_sql_ms</c>, which on a budgeted collector is CENSORED by that same 120 s
    /// budget and measures how long an abandon took to unwind rather than stall severity.</para>
    ///
    /// <para>It happens to land on the same 10 s the alert pass uses. That is two derivations meeting,
    /// not a number being reused: the alert pass is bounded above by its own 30 s sweep interval and
    /// below by a 1,744.9 ms forced-plan read, neither of which appears anywhere above.</para>
    /// </summary>
    public const int CollectionSweepSeconds = 10;

    /// <summary>
    /// The once-per-process STARTUP / BOOTSTRAP path's store commands: the managed-Postgres network
    /// reconcile's catalog checks, the in-place major-upgrade path's identity / extension / sentinel
    /// reads, least-privilege role provisioning, the delta-baseline re-seed, and the config store's
    /// first-run seed.
    ///
    /// <para><b>The regime.</b> Every one of these runs exactly once per process start, on the plain
    /// stopping token, before the collection loop exists. Nothing encloses them, and this was verified
    /// rather than assumed — the same verification <c>PgMigrations.MigrateAsync</c> records for the
    /// migration immediately ahead of them: no <c>CancelAfter</c> anywhere on the path, no
    /// <c>HostOptions.StartupTimeout</c> configured (so the framework default is infinite), no health
    /// check or readiness probe in the repo, no <c>HEALTHCHECK</c> on the container image, and no
    /// orchestrator manifest. The installers' 60 s / 2 min <c>WaitForStatus('Running')</c> do not bound
    /// it either, because <c>DarlingWorker</c> is a <c>BackgroundService</c> that reports Running before
    /// the first statement runs.</para>
    ///
    /// <para><b>This is the one regime in #2874 whose value goes UP, and the asymmetry is why.</b> Two of
    /// the four failure handlers on this path are <c>LogCritical</c> followed by <c>return</c>: a managed
    /// bootstrap throw exits the service, and a migrate-path throw ends collection for the life of the
    /// process with no retry and no triage. So a deadline that fires during a slow-but-HEALTHY start
    /// converts a recoverable delay into a dead service until a human restarts it, while a deadline that
    /// fires late only delays this instance's first collection cycle — the MCP and web hosts are separate
    /// <c>BackgroundService</c>s on their own data sources and are already serving. Waiting is the cheap
    /// direction here, which is the opposite of every other regime in this sweep, where the risk was
    /// holding a scarce pooled connection. The defect #2874 exists for is not that 30 s is too long; it is
    /// that 30 s is a number nobody chose and that renders as "Exception while reading from stream" when
    /// it fires. On this path that misreads a reachable-but-slow store as an unreachable one, in the very
    /// log line an operator acts on.</para>
    ///
    /// <para><b>ABOVE the measured cold worst case.</b> Measured on a store built by the product's own
    /// <c>MigrateAsync</c> (schema 110, PostgreSQL 17.11 / TimescaleDB 2.29.2), seeded to the live
    /// 42-server store's own recorded sizes, with shared buffers dropped before each first run: the
    /// worst site in the group is <c>VerifySentinelReadAsync</c>'s <c>count(*)</c> over
    /// <c>collect.collection_log</c> at <b>151.6 ms</b> (733 MB / 3.18 M rows / 31 chunks, against the
    /// 0.69 GB that table holds live). Everything else is smaller: the four delta seeds at <b>117 ms</b>
    /// worst over a 3.69 GB / 30.96 M-row <c>wait_stats</c> at production per-chunk density, the 63-statement
    /// provisioning DDL batch at <b>79 ms</b> (which grows only ~0.04 ms per chunk, so a far chunkier store
    /// stays in the low hundreds), <c>pg_hba_file_rules</c> at 2 ms, and the identity, <c>SHOW</c> and
    /// <c>to_regclass</c> reads at or below 1 ms. So 60 s is roughly 400x the worst thing observed — sized
    /// that way deliberately, because the measurement is local NVMe on Linux while the path it defends runs
    /// on Windows storage, against a store at its least warm, and possibly one recovering from an unclean
    /// shutdown or its own post-<c>pg_upgrade</c> statistics pass.</para>
    ///
    /// <para><b>BELOW the budget the same startup path gives its heaviest statements.</b>
    /// <c>PgMigrations.MigrationCommandTimeoutSeconds</c> is 300 s per rung, and its lock wait is five
    /// multiples of that. Those bound data-MOVING DDL on a cold busy store. The sites here are catalog
    /// reads, single-row <c>config</c> seeds, one <c>count(*)</c> and one idempotent grant batch — strictly
    /// cheaper by construction, so granting them a migration rung's budget would be borrowing a number
    /// rather than deriving one. 60 s is also exactly 2x the default it replaces, which keeps the change
    /// legible: it widens by one multiple and names why.</para>
    ///
    /// <para><b>What this is NOT derived from.</b> Not #1772, which is this group's justification but not
    /// its bound. That field failure — a 276 GB store where the delta seed could not finish inside the 30 s
    /// default, so restart continuity silently degraded to first-cycle-zero on every start — was fixed by
    /// binding <c>$1</c> on BOTH halves of the seed query, outer read and inner <c>MAX()</c>, which
    /// <c>DarlingDeltaCalculator</c>'s own comment records. A deadline was never that fix, and measuring the
    /// two shapes side by side says why: bounded 117 ms against 566 ms for the unbounded inner
    /// <c>MAX()</c> on the same store. What #1772 establishes is that the inherited default DOES fire on
    /// this path in the field, and that when it does the failure is silent — which argues for a generous
    /// deadline, not a tight one.</para>
    ///
    /// <para>Not the pool either. These sites do not compete for the collection pool the way the sweep body
    /// does: the bootstrap ones run on non-pooled connections built from a connection-string builder
    /// (<c>Pooling = false</c> on the upgrade path), before any data source exists, and the seeding ones run
    /// one at a time on a single borrowed connection while nothing else in the process is collecting.</para>
    /// </summary>
    public const int BootstrapSeconds = 60;

    /// <summary>
    /// The two commands in <c>DarlingManagedPostgres.EnsureDatabaseOnceAsync</c> — the
    /// <c>pg_database</c> probe and <c>CREATE DATABASE</c> — which are the bootstrap's first real
    /// interaction with the freshly started server, and the ONLY sites in this group with a retry above
    /// them.
    ///
    /// <para><b>Why they cannot take <see cref="BootstrapSeconds"/>.</b> <c>EnsureDatabaseAsync</c> wraps
    /// the whole unit in six attempts separated by 2 s, and an Npgsql command deadline is inside what that
    /// loop retries. Measured against Npgsql 10.0.3: a command that exceeds its <c>CommandTimeout</c>
    /// throws <c>NpgsqlException("Exception while reading from stream")</c> wrapping a
    /// <c>TimeoutException</c>, and <c>IsTransientConnectionFault</c> walks the inner chain and returns
    /// true for exactly that. (A SERVER-side <c>statement_timeout</c> is the mirror image — it arrives as
    /// <c>PostgresException 57014</c>, which that same test rejects as "the server replied", so it is not
    /// retried. The same wall-clock event, named two ways, and only one of them gets six chances.) So the
    /// deadline here multiplies: the worst-case bootstrap delay this pair can contribute is
    /// <c>6 x deadline + 10 s</c> of pauses before the throw reaches <c>LogCritical</c>-and-exit. At the
    /// inherited 30 s that is 190 s; at <see cref="BootstrapSeconds"/> it would be 370 s. At 10 s it is
    /// 70 s, which sits just past the installers' 60 s <c>WaitForStatus('Running')</c> and inside the
    /// 2-minute variant — i.e. inside the window an operator is already waiting through, rather than
    /// several minutes beyond it.</para>
    ///
    /// <para><b>ABOVE the measured worst case with room to spare.</b> The probe measured 12-14 ms and
    /// <c>CREATE DATABASE</c> 19-68 ms, so 10 s is ~147x the slower of the two. And it is generous for the
    /// fault the retry actually exists for, which is not slowness at all: a Windows backend that loses the
    /// shared-memory reservation race authenticates and then DIES on its first query, which arrives as a
    /// reset in milliseconds. Waiting a full <see cref="BootstrapSeconds"/> for a backend that is already
    /// gone spends the retry's whole purpose — getting a fresh one quickly — on a corpse.</para>
    ///
    /// <para>These two are also the only sites in the group that run against the MAINTENANCE database
    /// (<c>postgres</c>) rather than the store, which is what makes their floor a connection probe's floor
    /// rather than a query's.</para>
    /// </summary>
    public const int BootstrapConnectProbeSeconds = 10;

    /// <summary>
    /// The store commands awaited INLINE on the serial collection-loop thread, ahead of every per-server
    /// launch: the control-plane reload body — <c>StoreConfigProvider.LoadViewAsync</c>'s five config
    /// reads, <c>DarlingObservability.SyncServerEnabledStatesAsync</c>'s two registry statements and the
    /// managed-role <c>statement_timeout</c> re-assert — plus the store-size read behind the disk-pressure
    /// check.
    ///
    /// <para><b>The regime, by the token test.</b> Every one of these is awaited directly inside
    /// <c>DarlingWorker</c>'s <c>while (!stoppingToken.IsCancellationRequested)</c> body, on the plain
    /// stopping token, BEFORE the per-server launches fan out. So unlike the sweep body — which holds one
    /// of <c>max_concurrent_sweeps</c> permits while the rest of the fleet proceeds — a stall here delays
    /// the whole fleet's cycle, and unlike the bootstrap path it happens repeatedly for as long as the
    /// service runs. That combination is what makes it a regime of its own rather than an extension of
    /// either neighbour.</para>
    ///
    /// <para><b>Seven of the nine ALSO run once on the bootstrap path</b> (<c>LoadViewAsync</c> at
    /// <c>DarlingWorker.cs:1179</c>, <c>SyncServerEnabledStatesAsync</c> at <c>:1192</c>), which is
    /// precisely why they take this constant and not <see cref="BootstrapSeconds"/>: a deadline is a
    /// property of the command, so a dual-caller site has to take the TIGHTER of its two bounds, and this
    /// is it.</para>
    ///
    /// <para><b>ABOVE the measured cold worst case.</b> Measured against a 4.05 GB store built by the
    /// product's own <c>MigrateAsync</c> and seeded through <c>SeedIfEmptyAsync</c>, shared buffers
    /// dropped before the first run: <c>LoadViewAsync</c>'s five commands together took <b>16.1 ms</b>
    /// cold (3.4-4.8 ms warm), <c>SyncServerEnabledStatesAsync</c>'s two <b>3.8 ms</b>, and
    /// <c>pg_database_size</c> <b>6.2 ms</b> — so the worst SINGLE command on this thread is a
    /// single-digit-millisecond read and 5 s is roughly three orders of magnitude above it. The
    /// <c>ALTER ROLE</c> re-assert is one statement out of the 63-statement provisioning batch that
    /// measured 79 ms in total, so it is in the same class.</para>
    ///
    /// <para><b>BELOW the point where a stalled chain reports as a hang.</b> The nine run SEQUENTIALLY on
    /// one thread, so the bound that matters is the chain's, not one command's: 9 x 5 s = 45 s stays
    /// inside <c>DarlingWorker.SweepWatchdogSeconds</c> (60 s), while 9 x the inherited 30 s default is
    /// 270 s and would put a merely-slow reload four and a half minutes past it. That is the same
    /// chain-length reasoning #2928 applied to the sweep body, on a different chain.</para>
    ///
    /// <para><b>Why it is LOOSER than the reload beacon's own bound and must stay so.</b>
    /// <c>ReadConfigVersionAsync</c> runs on EVERY 15 s tick and is a single-row lookup, so it is the
    /// tightest thing on this thread and is bounded separately. The nine here run only when the beacon
    /// has already seen a version change, or on the 5-minute disk-check cadence — rare, and each one a
    /// heavier read than the beacon.</para>
    ///
    /// <para><b>The failure asymmetry points the same way as the bootstrap's, for a different reason, and
    /// it is the binding one.</b> The beacon advances <c>_lastConfigVersion</c> BEFORE calling the reload
    /// body, so a read that dies mid-reload does not merely fail open to the live config — it fails open
    /// having already consumed the version bump that would have retried it, and the operator's change is
    /// silently lost until something bumps the version again. An over-eager deadline therefore swallows a
    /// setting change without a word, while a late one delays one tick of an already-rare event. The
    /// disk-check read's own failure is milder (null store size at Debug, retried in five minutes), so it
    /// rides the reload body's number rather than setting it.</para>
    ///
    /// <para><b>Not derived from store connection acquisition.</b> #2819's 673-893 ms has been folded
    /// into floors elsewhere in this sweep; #2940 measured that the connect phase tracks Npgsql's
    /// <c>Timeout</c> rather than <c>CommandTimeout</c>, so a <c>CommandTimeout</c> floor must not carry
    /// it. None of the four numbers in this file's group C entries includes it.</para>
    /// </summary>
    public const int SerialLoopSeconds = 5;
}
