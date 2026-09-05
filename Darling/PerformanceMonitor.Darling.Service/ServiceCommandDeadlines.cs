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
/// time; this file is where each one's constant lands. Eleven are here: the collection sweep (#2928),
/// the post-analysis force-plan hook and the two CLI verbs (group E), the startup/bootstrap path and
/// its connect probe and the serial collection-loop thread (#2946), and the command plane, the
/// actual-plan store resolve, the Query Store backfill's reads and the control-plane reload beacon
/// (group D).
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
    /// most collectors in the single-digit-to-low-tens ms. So <b>1.53 s</b> is the worst thing actually
    /// observed, and 10 s keeps ~6.5x headroom over it.</para>
    ///
    /// <para><b>Store connection acquisition is NOT in this floor</b>, and the reason is what
    /// <c>CommandTimeout</c> actually bounds. #2819 measured acquisition at 673-893 ms, and an earlier
    /// derivation of this constant added it to the store write to reach a ~2.4 s floor. It does not
    /// belong there: <c>CommandTimeout</c> starts when the command EXECUTES on an already-open
    /// connection, while the connect phase is tracked by the connection string's <c>Timeout</c>. That is
    /// measured rather than reasoned — with <c>Timeout=2</c> a connect failure lands at 2.0 s whether
    /// <c>CommandTimeout</c> is 1 or 60. Folding an acquisition figure into a <c>CommandTimeout</c>
    /// floor credits this knob with bounding a phase it cannot reach. The 673-893 ms is real and still
    /// binds where it belongs — pool pressure, in <c>DarlingManagedPostgres</c> and
    /// <c>DarlingCollectorRunner</c> — just not here.</para>
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
    /// to <b>~3.1 s</b> if write latency scaled linearly with twice the concurrent writers is what 10 s
    /// is set to clear, and it clears it by ~3.2x. <b>The value is correct at 8 and at 16;
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
    /// <c>PgPlanForceActionStore</c>'s four commands — the force-plan bot's journal write and its windowed
    /// history read, plus the two reads #2731's write path will consume.
    ///
    /// <para><b>The regime, and why #2882 deferred it.</b> The alert pass excluded these deliberately: they
    /// look like members of that family and are not. <c>PlanForceBot.RunAfterAnalysisAsync</c> is built at
    /// <c>DarlingWorker.cs</c>'s <c>postPassHook</c> closing over the plain <c>stoppingToken</c>, so it runs
    /// OUTSIDE <c>passCts.CancelAfter(s_analysisTimeout)</c> — whose token is threaded only into
    /// <c>analysisService.AnalyzeAsync</c> — despite being invoked lexically inside the pass. Being
    /// lexically inside a budgeted method is not the same as being under its budget. So nothing encloses
    /// these, exactly as in #2882; what differs is the CADENCE, which is the analysis interval clamped
    /// 5-360 minutes rather than the 30 s alert sweep the 10 s was bounded against.</para>
    ///
    /// <para><b>The ceiling is the analysis pass's own budget, not a cadence and not the watchdog.</b> The
    /// hook runs inside <c>ProcessServerSweepAsync</c>, holding one of <c>max_concurrent_sweeps</c> permits
    /// for its whole duration, and that server's collection cannot relaunch while the body is in flight. The
    /// 60 s <c>DarlingWorker.SweepWatchdogSeconds</c> is NOT the bound here the way it is for the collection
    /// sweep: a body that ran an analysis pass has already been eligible to trip it, since the pass alone is
    /// budgeted at 120 s. What the pass's 120 s IS, is the product's own statement of how long a server's
    /// collection may be suspended for analysis — so a hook that rides the pass must not exceed it. The bot
    /// performs at most <c>PlanForceBot.MaxTargetsPerPass</c> (10) history reads plus one journal write each,
    /// sequentially, on separate store connections: <b>20 commands</b>. 20 x 5 s = 100 s, inside the pass's
    /// 120 s. At Npgsql's inherited 30 s default it is 600 s — <b>five times the budget of the pass it rides
    /// on</b>, for a hook that is not part of it.</para>
    ///
    /// <para><b>ABOVE the worst case, and the floor is NOT connection acquisition.</b> #2940 measured that
    /// the connect phase tracks the connection string's <c>Timeout</c> and not <c>CommandTimeout</c> — at
    /// <c>Timeout=2</c> it fails at 2.0 s with <c>CommandTimeout</c> set to either 1 or 60 — so the 673-893 ms
    /// acquisition of #2819 is outside what this constant bounds and is deliberately NOT in the arithmetic
    /// below. What is left is the statement itself. Measured on PostgreSQL 17 against a
    /// <c>collect.plan_force_actions</c> seeded to 5.0 M
    /// rows / 1,382 MB — 43 servers x 8 databases x 1,000 distinct queries over a 1,095-day horizon, because
    /// this table has NO retention path and grows for the life of the deployment — <c>GetQueryHistoryAsync</c>
    /// runs in <b>3.59 ms cold</b> and 0.84-1.01 ms warm, and the journal <c>INSERT ... RETURNING</c> in
    /// <b>0.346 ms</b>. It is flat in table size: both indexes V107 creates are used, and the only subquery
    /// that scales with volume is the trailing-24 h server count, which stayed at 0.47 ms with a
    /// deliberately pathological 2,925-row window (the volume a per-query cooldown of zero would produce).
    /// Those figures are from a local container, so the floor is anchored on the one COLD store read this
    /// sweep has measured in production instead: #2882's forced-plan read at <b>1,744.9 ms</b> over ~6.0 GB.
    /// A value under ~2 s could fire on a cold read; 5 s carries ~2.9x over it.</para>
    ///
    /// <para><b>So the CEILING is what fixes this number, not the floor.</b> The pass budget puts it at
    /// 6 s or less and the floor only rules out the bottom two, which is worth saying plainly rather than
    /// presenting a two-sided squeeze that is really one-sided.</para>
    ///
    /// <para><b>The two reads with no caller yet.</b> <c>GetPendingReviewsAsync</c> and
    /// <c>GetRecentActionsAsync</c> are specced ahead of #2731 and have no production caller in this build,
    /// so they take this constant because the class they sit on is the bot's. When
    /// <c>GetRecentActionsAsync</c> gains the <c>get_plan_force_actions</c> tool its doc comment names, it
    /// becomes an MCP read and belongs on <c>Mcp.McpCommandDeadlines.ReadSeconds</c> instead — flagged here
    /// rather than pre-empted, because moving it now would bound a caller that does not exist against a
    /// regime it is not yet in.</para>
    /// </summary>
    public const int PostAnalysisForcePlanSeconds = 5;

    /// <summary>
    /// The store reads and writes the CLI verbs perform — <c>--enable-mcp</c>/<c>--disable-mcp</c>/
    /// <c>--enable-web</c>/<c>--disable-web</c>'s <c>config_service</c> update, <c>--configure-firewall</c>'s
    /// endpoint-toggle read, and <c>--recompress-plan-dim</c>'s plan-codec preflight.
    ///
    /// <para><b>This number is not new: the product already derived it, and two of the three sites did not
    /// get it.</b> <c>DarlingCliCommands.TryReadEndpointTogglesAsync</c> wraps its read in a linked
    /// <c>CancellationTokenSource</c> at ten seconds, with the reason written down — <i>"the store is down"
    /// and "the store is slow" must not differ in how long an installer hangs</i> — and it prints that number
    /// back to the operator ("the store did not answer within 10 seconds"). That argument is a property of
    /// the SURFACE, not of that one verb: every one of these runs with a person or an installer script
    /// waiting on a console, with no enclosing budget, no retry and no next tick. The other two sites simply
    /// never had it, which is the #2786 shape — a bound that names the arm it was written for.</para>
    ///
    /// <para><b>So the ceiling is the promise in that message.</b> A <c>CommandTimeout</c> above ten seconds
    /// would let a command outlive the budget whose expiry the operator is told about, and Npgsql's inherited
    /// 30 s does exactly that: the CTS fires at 10 s while the command believes it has 20 s left, so the two
    /// bounds disagree about the same wait. Making the command agree with the budget is what stops the
    /// message from being a lie. The floor is a single-row read or a single-row primary-key update of a
    /// one-row <c>config_service</c> table — sub-millisecond work — against #2882's 1,744.9 ms production
    /// cold-read anchor, so ten seconds is ~5.7x the worst thing that can legitimately happen.</para>
    ///
    /// <para><b>The CTS and the <c>CommandTimeout</c> bound different spans, and that is why both exist.</b>
    /// #2940 measured that the connect phase tracks <c>Timeout</c>, not <c>CommandTimeout</c>. So
    /// <c>TryReadEndpointTogglesAsync</c>'s linked <c>CancellationTokenSource</c> is the only one of the two
    /// that covers <c>OpenAsync</c> — which is the half an installer actually hangs on — while this constant
    /// bounds the statement. Both are set to the same number because the operator is told one number; they
    /// are not redundant, and the store-connection acquisition cost belongs to the CTS's span rather than to
    /// this constant's.</para>
    ///
    /// <para>It lands on the same ten seconds as <see cref="CollectionSweepSeconds"/> and #2882's alert pass.
    /// That is a third derivation meeting them rather than a number reused: nothing above mentions a 30 s
    /// sweep interval, a 60 s watchdog or <c>max_concurrent_sweeps</c>, and this one is bounded by a sentence
    /// printed to an operator.</para>
    /// </summary>
    public const int CliStoreReadSeconds = 10;

    /// <summary>
    /// The command deadline at the ONE CLI site that already has a budget of its own —
    /// <c>TryReadEndpointTogglesAsync</c>'s linked <c>CancellationTokenSource</c> — set deliberately ABOVE
    /// <see cref="CliStoreReadSeconds"/> so the budget wins the race rather than tying it.
    ///
    /// <para><b>Review caught this, and it undercut the justification for the constant above.</b> Setting the
    /// command to the same duration as the CTS makes which one fires first a race, and the two arms report
    /// differently: the CTS surfaces as <c>OperationCanceledException</c> and is caught by the arm that says
    /// "the store did not answer within N seconds", while an Npgsql <c>CommandTimeout</c> expiry is an
    /// <c>NpgsqlException</c> wrapping a <c>TimeoutException</c>, falls through to the general arm, and
    /// renders <c>ex.Message</c> — which is <c>Exception while reading from stream</c>, the network-fault
    /// costume #2826 exists to stop this product wearing. So a tie does not merely reword the failure; under
    /// half the timing window it hands an installer the exact misdiagnosis, from the site whose whole purpose
    /// is to say plainly why the store did not answer.</para>
    ///
    /// <para><b>It is the same rule as the HypoPG forward path, and that is why the margin is the same 5 s.</b>
    /// <c>HypotheticalIndexExperiment.ForwardCommandTimeoutSeconds</c> sits above the server-side
    /// <c>SET LOCAL statement_timeout</c> for exactly this reason — a client-side deadline BELOW the
    /// authoritative one converts a diagnosable error into an undiagnosable one. Here the authoritative bound
    /// is a client-side CTS rather than a server GUC, but the ordering requirement is identical: whichever
    /// bound produces the better message must be the one that fires, so the other is a backstop strictly
    /// above it. The backstop still matters — it is what bounds the statement if the token is never observed —
    /// and it stays far below Npgsql's inherited 30 s.</para>
    ///
    /// <para>The other two CLI sites have no budget around them, so for them
    /// <see cref="CliStoreReadSeconds"/> IS the bound and there is nothing to order it against.</para>
    /// </summary>
    public const int CliBudgetBackstopSeconds = CliStoreReadSeconds + 5;

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
    /// check and the daily retention sweep's own run-record.
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
    /// <para><b>BELOW the point where a stalled chain reports as a hang.</b> The ten run SEQUENTIALLY on
    /// one thread, so the bound that matters is the chain's, not one command's: 10 x 5 s = 50 s stays
    /// inside <c>DarlingWorker.SweepWatchdogSeconds</c> (60 s), while 10 x the inherited 30 s default is
    /// 300 s and would put a merely-slow reload five minutes past it. That is the same chain-length
    /// reasoning #2928 applied to the sweep body, on a different chain. Ten is the count on the worst
    /// tick rather than a bound on the longest single path: a tick where the reload beacon fires AND the
    /// 24 h purge comes due runs the reload body's nine and the purge's run-record back to back.</para>
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

    /// <summary>
    /// The store&lt;-&gt;service COMMAND plane's own bookkeeping — the stale-command reaper, the atomic
    /// claim, the desired-state store write a claimed command dispatches, the terminal result report,
    /// and the <c>pg_statement_text</c> lookup <c>test_hypothetical_index</c> resolves its statement
    /// from.
    ///
    /// <para><b>The regime.</b> Nothing encloses these: <c>RunCommandLoopAsync</c> runs on the plain
    /// stopping token with no <c>CancelAfter</c> anywhere, and nothing re-runs a command — measured
    /// against the shipped SQL, the reaper marks an abandoned row terminal <c>failed</c> and never
    /// re-queues it to <c>pending</c>, so a second instance cannot claim work still in flight. What
    /// the loop holds is its own SERIAL thread: <c>DarlingCommandExecutor.ReclaimStaleCommandsSql</c>'s command runs
    /// FIRST on every tick, before the drain, so a stalled reaper is added tick period for the whole
    /// plane.</para>
    ///
    /// <para><b>ABOVE the measured worst case, by three orders of magnitude and deliberately.</b>
    /// Every statement here is a single row on a keyed, non-hypertable relation:
    /// <c>config.config_command</c> by its identity PK, <c>config.config_service</c> by
    /// <c>id = 1</c>, <c>config.config_monitored_servers</c> by <c>server_id</c>, and
    /// <c>collect.pg_statement_text</c> by its <c>(server_id, queryid)</c> PK. #2901 measured the
    /// viewer end of this same plane — the same table, the same shape — at <b>3.9 ms cold and 0.1 ms
    /// warm</b>. The headroom is spent on the report write specifically, which is what ENDS the claim
    /// lease: a report that gives up leaves the row <c>in_progress</c>, and the reaper then reports a
    /// command that SUCCEEDED as reclaimed-failed five minutes later. Store connection acquisition
    /// (673-893 ms, #2819) is NOT in this floor — <c>CommandTimeout</c> bounds statement execution,
    /// not <c>OpenConnectionAsync</c>, which the connection string's own <c>Timeout</c> governs.</para>
    ///
    /// <para><b>BELOW the cadence the plane exists to guarantee.</b>
    /// <c>DarlingWorker.s_commandPollInterval</c> is 5 s, chosen so "an operator command is picked up
    /// within ~5s". The loop is single-threaded, so one stalled statement is added tick period: at 5 s
    /// the worst tick is 10 s, at Npgsql's inherited 30 s it is 35 s — seven times the cadence. And the
    /// executor's four statements chain inside the wait the viewer STATES:
    /// <c>ViewerDataService.DefaultCommandTimeout</c> is 45 s for pause/resume, which 4 x 5 = 20 s fits
    /// and 4 x 30 = 120 s does not. This is the third derivation in this sweep to land on 5, and none
    /// of them copied another: the viewer's came from a 400 ms poll interval, this one from a 5 s
    /// server-side tick and a 45 s stated wait.</para>
    ///
    /// <para><b>It NARROWS the lease overrun rather than closing it</b>, following #2888's lock wait
    /// and #2901's own command plane. <c>DarlingCommandExecutor.StaleCommandTimeout</c> is five
    /// minutes with no heartbeat, and what a command spends is dominated by work this deadline does not
    /// bound — a full collector sweep for <c>snapshot_now</c>, a fleet-wide retention purge for
    /// <c>purge_now</c>, 120 s of re-execution for <c>execute_actual_plan</c>. So the lease is not the
    /// instrument this constant moves; it is the instrument that MISREPORTS when the report write
    /// fails, and the report failing fast is strictly better than it failing slow.</para>
    /// </summary>
    public const int CommandPlaneSeconds = 5;

    /// <summary>
    /// The <c>execute_actual_plan</c> store resolve — <c>DarlingWorker.RunExecuteActualPlanAsync</c>'s
    /// one read, which turns the command's identifier-only payload into the query text, estimated plan
    /// and isolation level it re-executes.
    ///
    /// <para><b>Its own regime, because its FLOOR is larger than the whole of
    /// <see cref="CommandPlaneSeconds"/>.</b> It receives the same token as the plane's bookkeeping and
    /// nothing re-runs it either, but none of the three resolvers carries a predicate on
    /// <c>collection_time</c> — the partitioning column — so each is a <c>LIMIT 1</c> over every chunk
    /// in retention, and the <c>query_store_stats</c> resolver sorts rows carrying
    /// <c>query_plan_text</c> before taking one. On the larger production store that table is
    /// <b>62.5 GB across 19 chunks</b> (#2795), and the store's own PostgreSQL log for a single day
    /// records reads of it cancelled at Npgsql's 30 s default 2,092 and 631 times. So the default is
    /// demonstrably beneath this table's cold cost, and giving this site the plane's 5 s would fail a
    /// button that works today — the "a wrong bound is worse than no bound" case #2874 opens with.</para>
    ///
    /// <para><b>Derived from what is left of the command's budget, not chosen.</b> The resolve is not
    /// the last thing the command does: <c>ActualPlanCaptureTimeoutSeconds</c> (120) bounds the
    /// re-execution that follows it inside the same claim, and the viewer waits
    /// <c>ViewerDataService.ImperativeCommandTimeout</c> (180 s). 180 - 120 leaves 60 s for everything
    /// else, of which the claim costs up to one 5 s poll tick and the report up to
    /// <see cref="CommandPlaneSeconds"/>; 45 s is the remainder. Landing inside it is what makes a
    /// timeout surface as a legible "store error" outcome rather than as a viewer poll miss — the same
    /// pair of bounds <c>ActiveQueriesFetchTimeoutSeconds</c>'s own comment derives from.</para>
    ///
    /// <para><b>Not measured, and said rather than implied.</b> There is no cold timing of this
    /// specific read: the 62.5 GB / 19-chunk figure and the two cancellation counts are #2795's, taken
    /// on a store this session had no credentials for, and the reads it measured were unbounded
    /// AGGREGATES while this one seeks
    /// <c>idx_query_store_stats_server_db_query_plan_time</c>'s leading three columns. The measurement
    /// bounds this read's cost from above rather than establishing it.</para>
    /// </summary>
    public const int ActualPlanResolveSeconds = 45;

    /// <summary>
    /// The Query Store backfill worker's two store reads — the candidate-database scan and the
    /// derived-ceiling <c>MIN(last_execution_time)</c>.
    ///
    /// <para><b>The enclosing budget does not bound them, and that is measured rather than read.</b>
    /// <c>DarlingWorker.BackfillSliceDeadline</c> is 300 s, but <c>AbandonableStep</c> ABANDONS: it
    /// races the work against a <c>Task.Delay</c> and returns <c>Abandoned</c> without signalling
    /// anything. Against a live store, a 20 s statement with <c>CommandTimeout = 0</c> under a 3 s
    /// deadline returned <c>Abandoned</c> at 3.0 s while <c>pg_stat_activity</c> still showed that
    /// backend <c>state = 'active'</c> running it; the same statement with <c>CommandTimeout = 5</c>
    /// faulted at 5.0 s with the backend gone. <b>The command deadline is the only thing that kills the
    /// statement.</b> And with the shipped shape — no deadline, 300 s budget — a 40 s statement faulted
    /// at <b>30.0 s</b> with <c>Exception while reading from stream</c>: the 300 s was never reached,
    /// and the value that decided was the undocumented one.</para>
    ///
    /// <para><b>ABOVE the worst case, which for this regime means ABOVE Npgsql's default.</b> Both
    /// reads are unbounded across retention on <c>query_store_stats</c>, and both are in #2795's
    /// production cancellation census: the candidate scan's own form <b>631 times in one day</b>, and
    /// the <c>MIN</c>'s shape-twin <c>MAX</c> <b>2,092 times</b>, measured at <b>40,743-50,560 ms
    /// cold</b> and 9,279 ms warm on the 62.5 GB / 19-chunk table. The candidate scan's
    /// <c>collection_time &gt; now() - CandidateWindow</c> predicate is INERT: <c>CandidateWindow</c>
    /// is 7 days while <c>TimescaleSupport.RawRetentionSpan</c> is 4, so no chunk that exists is ever
    /// excluded — which is why a nominally bounded read is in that census at all. And the <c>MIN</c>
    /// cannot be bounded the way #2344 and #2795 bounded their <c>MAX</c> siblings: it exists to find
    /// the OLDEST stored row, so a <c>collection_time</c> floor would hide exactly what it looks for.
    /// 120 s clears the twin's 50.6 s cold worst with 2.4x headroom. Both failures are swallowed at
    /// <c>LogDebug</c> and return "no candidates" / "skip this database", which reads as no backfill
    /// work — the silent-degradation shape of #2795 and #2796, one loop over.</para>
    ///
    /// <para><b>BELOW the point where the loop walks away from a live statement.</b> Strictly under the
    /// 300 s <c>BackfillSliceDeadline</c>, so the statement dies before the step abandons it and the
    /// <c>Abandoned</c> outcome is unreachable for these two reads; an abandoned read would keep
    /// burning a pooled store connection while the step's in-flight guard quarantines that server's
    /// backfill until the task truly ends. Also under <c>s_queryStoreBackfillInterval</c> (300 s), so a
    /// read alone can never consume a whole tick.</para>
    /// </summary>
    public const int QueryStoreBackfillReadSeconds = 120;

    /// <summary>
    /// The control-plane reload beacon — <c>StoreConfigProvider.ReadConfigVersionAsync</c>'s
    /// <c>SELECT config_version FROM config_service WHERE id = 1</c>.
    ///
    /// <para><b>Claimed here rather than with the startup group, on this project's own
    /// classification rule</b>: which token does the site receive, and what re-runs it. This one takes
    /// the plain stopping token and is re-run by the collection sweep <b>every 15 s, for the life of
    /// the process</b>. The other twelve sites in its file are seeding and reconcile reads that run
    /// ONCE per process start and belong with startup. A file boundary is not a regime boundary, which
    /// is the lesson #2928 recorded in both directions.</para>
    ///
    /// <para><b>ABOVE the measured worst case</b>, which is the same 3.9 ms cold #2901 measured for a
    /// single-row <c>config.*</c> read; <c>config_service</c> holds exactly one row.</para>
    ///
    /// <para><b>BELOW the tick, and tighter than <see cref="CommandPlaneSeconds"/> because the
    /// asymmetry is more lopsided.</b> The read is awaited at the TOP of the sweep loop on the serial
    /// loop thread, ahead of the fire-and-track launch of every server, so a stall is fleet-wide
    /// collection latency rather than one server's: at Npgsql's inherited 30 s a single stall makes the
    /// 15 s <c>s_sweepInterval</c> period 45 s for the whole fleet at once. And it fails OPEN — the
    /// catch takes everything, warns, returns null and keeps the live config — so an exceeded deadline
    /// costs at most one 15 s tick of delay in applying a config change, against 42 servers' collection
    /// for an overrun. 3 s is 750x the measured floor and a fifth of the tick, so even a full overshoot
    /// leaves the sweep inside two ticks.</para>
    /// </summary>
    public const int ConfigReloadBeaconSeconds = 3;
}
