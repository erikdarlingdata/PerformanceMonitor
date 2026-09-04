/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Whether a failure of one of <c>DarlingWorker</c>'s three collection-blocking startup steps is worth
/// trying again, and how long that is worth doing for (#2936).
///
/// <para><b>The three sites, which are one defect.</b> Loading <c>darling.json</c>, bootstrapping the
/// managed Postgres, and opening the store connection to run <c>PgMigrations.MigrateAsync</c>. Each sat
/// behind a single <c>catch (Exception)</c>, a single <c>LogCritical</c> and a single <c>return</c>, so a
/// file another process held open for a moment, a store that was unreachable for two seconds, and a
/// migration rung that can never apply all ended collection for the life of the process, identically. A
/// bare <c>catch (Exception)</c> cannot say which arrived.</para>
///
/// <para><b>And the process does not exit on that <c>return</c>.</b> The worker task completes
/// SUCCESSFULLY, so the host stays up, the MCP and web hosts keep serving, and the Windows service keeps
/// reporting Running — SCM recovery never fires, because nothing crashed. So the failures that most
/// deserve a retry are exactly the ones whose symptom looks least like a small problem.</para>
///
/// <para><b>Three sites, ONE classifier, deliberately.</b> The discrimination each site needs is the same
/// question — exception type chain and, where the server answered, <c>PostgresException.SqlState</c> — and
/// a file-sharing violation is a type-chain question just as much as a refused socket is. Forking the
/// logic per site would give three places for the boundary to drift; the one per-site difference that
/// does exist is named in <see cref="IsRetryable"/> as a carve-out rather than a second predicate.</para>
///
/// <para><b>Default-deny, and that is the load-bearing decision.</b> <see cref="IsRetryable"/> is an
/// ALLOWLIST. Anything it does not recognise keeps the old behaviour byte for byte: the critical line and
/// the stand-down. That direction is chosen because the two errors are not symmetric. Refusing to retry
/// something transient costs an operator a restart, and they get a loud, literal, searchable line telling
/// them to perform it. Retrying something permanent costs them the line — a rung that can never apply
/// would be re-attempted on a cadence, logging warnings, and the one message that says what is actually
/// wrong would never be emitted. A retry policy that retries everything is indistinguishable from a
/// working one right up to the point where it matters, so the unclassifiable case is terminal.</para>
///
/// <para><b>Why the existing classifier could not be reused.</b> <c>DarlingManagedPostgres</c> already has
/// <c>IsTransientConnectionFault</c>, driving a 6-attempt / 2-second retry around the bundled store's
/// first post-start interaction, and its rule is "a <see cref="PostgresException"/> means the server
/// replied, so it is never transient". That rule is right for its own site — the server there was started
/// by this process moments earlier with a credential this process derived, so a reply really is a verdict —
/// and it is wrong here. Measured against PostgreSQL 17.11, TWO of the three transient cases #2936 names
/// arrive as a <see cref="PostgresException"/>: an unclean store restart answers <c>57P01</c>
/// ("terminating connection due to unexpected postmaster exit") and a store still in crash recovery
/// answers <c>57P03</c> ("the database system is not yet accepting connections") for as long as recovery
/// takes. Both are the server replying, and both mean "not yet" rather than "no". That class is also
/// <c>DarlingManagedPostgres</c>-shaped in a second way: the type is
/// <c>[SupportedOSPlatform("windows")]</c>, and this path runs on Linux containers too.</para>
///
/// <para><b>Retryable, with what each one actually looks like.</b> Every shape below was observed against
/// PostgreSQL 17.11 / TimescaleDB 2.29.2 rather than reasoned about:</para>
/// <list type="bullet">
/// <item><description><b>Transport — the server never delivered a verdict.</b> No
/// <see cref="PostgresException"/> anywhere in the chain, and a <see cref="SocketException"/>,
/// <see cref="IOException"/> or <see cref="TimeoutException"/> in it (or the exception is an
/// <see cref="NpgsqlException"/> itself). Nothing listening presents as
/// <c>NpgsqlException -> SocketException</c>; an unroutable host as
/// <c>NpgsqlException -> TimeoutException</c>; the store dying mid-statement as
/// <c>NpgsqlException -> IOException -> SocketException</c>.</description></item>
/// <item><description><b>A plain <see cref="IOException"/>, which is how a file another process is
/// holding arrives.</b> This is what the config-load site is for: <c>DarlingConfig.Load</c> is
/// <c>File.Exists</c> then <c>File.ReadAllText</c> then deserialize, and a sharing violation on a
/// <c>darling.json</c> that an installer, the Viewer's Settings save or a config-management tool is
/// mid-write is as transient as a store that is not up yet. The managed bootstrap reaches the same shape
/// unpacking <c>pg-runtime.zip</c> over a transiently locked file. <b>But NOT the not-found subtypes</b> —
/// see the carve-out below, which is the one place the three sites genuinely differ.</description></item>
/// <item><description><b>A bare <see cref="TimeoutException"/>, which is how the migration lock-wait
/// budget expires.</b> A sibling instance holding the advisory lock is transient by definition — it is
/// applying rungs and will release. Note the two spellings this arrives in: a blocking
/// <c>pg_advisory_lock</c> hitting its <c>CommandTimeout</c> surfaces as
/// <c>NpgsqlException -> TimeoutException</c>, which is the transport bullet above and is byte-identical
/// to a RUNG overrunning its own command timeout; the polling waiter that replaced it (#2935) throws a
/// plain <see cref="TimeoutException"/> carrying both schema versions, and only against a store BELOW
/// this build's version — a peer holding the lock on an already-current store now returns zero applied
/// rather than throwing at all. Both spellings are retried, so this classification does not depend on
/// which one the applier is using.</description></item>
/// <item><description><b><c>57P01</c>, <c>57P02</c>, <c>57P03</c> — the server going down, or coming
/// up.</b> Observed directly: SIGKILL the store and the in-flight session gets <c>57P01</c>; connect
/// during the crash recovery that follows and every attempt gets <c>57P03</c> until recovery finishes.
/// These are the issue's "a restart, a failover, the store still coming up alongside the service", and
/// they are the whole reason this class exists rather than a call to the classifier next
/// door.</description></item>
/// <item><description><b>Class <c>08</c> — <c>08000</c>, <c>08001</c>, <c>08003</c>, <c>08004</c>,
/// <c>08006</c>.</b> The connection itself failed. Not the whole class: <c>08P01</c> is a protocol
/// violation, which is a defect that will recur identically, and <c>08007</c> leaves the transaction's
/// outcome unknown, which is a thing to say out loud rather than paper over.</description></item>
/// <item><description><b><c>40001</c> and <c>40P01</c> — serialization failure and deadlock.</b> Safe for
/// a reason specific to this applier rather than a general one: each rung's DDL and its
/// <c>darling_schema_version</c> stamp commit in ONE transaction, so a rung that loses a deadlock rolls
/// back with nothing applied and nothing stamped, and re-entering re-attempts that same rung from
/// scratch.</description></item>
/// <item><description><b><c>55P03</c> and <c>55006</c> — lock not available, object in use.</b> A rung's
/// <c>ALTER TABLE</c> losing a race with live traffic, reachable on any store carrying a role- or
/// database-level <c>lock_timeout</c>. The blocker is by definition a session that finishes, and the
/// rung's transaction rolled back whole.</description></item>
/// </list>
///
/// <para><b>Terminal, including the ones that are close calls.</b> Class <c>42</c> is a rung that cannot
/// apply against this store — wrong syntax, missing object, insufficient privilege — and is the case the
/// old code was right about. Class <c>23</c> is a rung whose constraint is violated by data already in
/// the store, which is V62's exact shape, and no amount of waiting changes the rows. <c>28P01</c> /
/// <c>28000</c> is a credential, <c>3D000</c> a database that does not exist, <c>55000</c> a database
/// somebody set <c>datallowconn = false</c> on. Class <c>53</c> — disk full, out of memory, too many
/// connections — is a capacity finding an operator has to see now, and two minutes does not clear any of
/// them; class <c>58</c> is the filesystem underneath the store. Two deserve naming because a
/// class-level rule would have swept them in with their neighbours: <c>57014</c> is somebody else's
/// <c>statement_timeout</c> cancelling a rung, which will cancel the identical rung identically on every
/// attempt, and <c>57P04</c> is the database having been dropped — both sit in the same class <c>57</c>
/// as the three retryable states above, which is why this classifies on individual states and not on
/// the two-character class. And an exception that is neither <see cref="NpgsqlException"/> nor carrying a
/// transport fault is terminal by the default-deny rule: the <c>ArgumentException</c> a malformed
/// connection string produces while Npgsql is still parsing it belongs there, and got there without
/// being enumerated.</para>
///
/// <para><b>The carve-out: <see cref="FileNotFoundException"/> and
/// <see cref="DirectoryNotFoundException"/> are terminal even though both derive from
/// <see cref="IOException"/>.</b> This is the one asymmetry between the three sites, and without it the
/// retry would be worse than no retry. <c>DarlingConfig.Load</c> throws <see cref="FileNotFoundException"/>
/// for a config that is not there — the single most likely reason it ever fails, on a first install — and
/// the managed bootstrap throws it for a missing <c>pg-runtime.zip</c>, a broken package. Neither fixes
/// itself, and both are exactly the case default-deny exists to keep loud: retried, they would spend the
/// budget emitting warnings and then produce the one line that names the missing path two minutes late.
/// Classified by TYPE rather than by site, so the store path — where these are unreachable — needs no
/// special case and the two file-touching sites cannot drift apart.</para>
///
/// <para><b><see cref="OperationCanceledException"/> is not classified here at all.</b> The call site's
/// filter excludes it ahead of this, because it means the service is stopping.</para>
/// </summary>
internal static class StartupFailureTriage
{
    /// <summary>
    /// How many times the collection loop's first store interaction may be tried before a failure
    /// <see cref="IsRetryable"/> accepts becomes terminal anyway.
    ///
    /// <para>Paired with <see cref="RetryBudget"/>, which is the real bound — see that constant
    /// for why an attempt cap alone is not one. Twenty-five tries <see cref="RetryDelay"/> apart is
    /// 24 waits of 5 s, so the two caps expire together on a failure that returns immediately, which is
    /// the ordinary case; the attempt count exists because it is what the warning line reports and what an
    /// operator counts in.</para>
    ///
    /// <para><b>Bounded rather than a supervisor loop, deliberately.</b> The repo holds both shapes.
    /// <c>DarlingManagedPostgres.EnsureDatabaseAsync</c> is bounded — 6 attempts, 2 s apart, classified,
    /// then it gives up — and it is the structural match: straight-line startup code around a store
    /// interaction that must succeed before the caller can continue. <c>DarlingMcpHostService</c> and
    /// <c>DarlingWebHostService</c> retry a failed config load forever on a 30 s
    /// <c>FailedStartBackoff</c> (#2038), which is right THERE because those hosts are supervisor loops
    /// that already existed and had nothing else to do with the tick. Making this one unbounded means
    /// giving the collection loop's whole start a supervisor of its own, which is a different change than
    /// classifying its failures; this bounded form leaves the long-outage case behaving exactly as it does
    /// today rather than half-solving it.</para>
    ///
    /// <para>Attempts and delay are separate from every other budget in
    /// <c>ServiceCommandDeadlines</c> and from the 6 / 2 s above per that file's rule that a constant is
    /// not reused across regimes. Nothing about a per-command deadline or a Windows shared-memory
    /// reservation race says how long a store takes to come back.</para>
    /// </summary>
    internal const int Attempts = 25;

    /// <summary>
    /// Pause between the attempts <see cref="Attempts"/> allows.
    ///
    /// <para>A connect against a store that is not listening fails immediately — measured sub-millisecond,
    /// <see cref="SocketException"/> straight back — so this delay is the entire cost of a poll and the
    /// entire notice latency too. Five seconds keeps that latency an order below the tens of seconds of
    /// store start it exists to cover, while 25 connection attempts spread over two minutes is nothing
    /// against a store that is either not listening or busy replaying WAL.</para>
    /// </summary>
    internal static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Wall clock from the first attempt after which no further attempt is made, whatever
    /// <see cref="Attempts"/> has left.
    ///
    /// <para><b>Why an attempt cap alone is not a bound, measured rather than assumed.</b> An attempt is
    /// not instantaneous just because a refused connect is: a peer instance holding the migration advisory
    /// lock makes <c>PgMigrations.MigrateAsync</c> BLOCK inside <c>pg_advisory_lock</c> for as long as
    /// <c>MigrationLockWaitTimeoutSeconds</c> allows, which is 1500 s. Observed against a live store: a
    /// second session took that lock and the service waited 38 s inside a SINGLE attempt, silently, and
    /// never retried — correct behaviour, and exactly the shape that makes an attempt cap misleading.
    /// Twenty-five attempts each free to block for 25 minutes is over ten hours of retrying, which is not
    /// what "retry briefly, then say so" means and which would push the one definitive line most of a day
    /// past the failure it describes. So this bounds the WALL CLOCK, <see cref="Attempts"/> bounds
    /// the pauses, and whichever expires first ends the retrying.</para>
    ///
    /// <para><b>What each cap therefore covers.</b> A store that is not listening fails an attempt in
    /// under a millisecond, so all 25 attempts fit inside this budget and the attempt count is what ends
    /// it. A store that accepts the connection and then makes the attempt WAIT — a peer migrator, a rung
    /// outrunning its own command timeout — spends the whole budget inside attempt one, so a wait that
    /// was going to be waited out anyway is not multiplied by 25. Both paths land on the same terminal
    /// line.</para>
    ///
    /// <para><b>Two minutes, derived.</b> BELOW: twice the 60 s this repo already allows a PostgreSQL
    /// start (<c>DarlingManagedPostgres.PgCtlWaitSeconds</c>), against a measured 2.0 s for an unclean
    /// kill of a 1.7 GB store carrying 701 MB of unreplayed WAL (26 transport failures, then 74
    /// <c>57P03</c>s, then service) — so the ordinary blip is gone inside the first wait and the rest is
    /// for the cold, large, contended restart. ABOVE: it stays finite because the terminal line is the
    /// only diagnosis this failure produces and the process does NOT exit when it lands, so an unbounded
    /// retry would turn a wrong host or port in <c>darling.json</c> — which presents as a retryable
    /// <see cref="SocketException"/> forever — from one loud documented line into a service that reports
    /// Running and never concludes anything. And it sits inside the 150 s
    /// <c>DarlingWorker.ColdStartSpreadSeconds</c> the fleet's first sweep is already staggered across, so
    /// even a fully spent budget delivers the first collection cycle within the span a normal cold start
    /// occupies anyway.</para>
    /// </summary>
    internal static readonly TimeSpan RetryBudget = TimeSpan.FromSeconds(120);

    /// <summary>
    /// <c>SqlState</c>s that mean "not yet" rather than "no" — see the class remarks for what each one was
    /// observed doing. An allowlist: a state absent from it is terminal, which is what keeps a rung that
    /// can never apply from being retried into silence.
    /// </summary>
    private static readonly HashSet<string> s_retryableSqlStates = new(StringComparer.Ordinal)
    {
        /* The server is going down, or is not up yet. */
        PostgresErrorCodes.AdminShutdown,
        PostgresErrorCodes.CrashShutdown,
        PostgresErrorCodes.CannotConnectNow,

        /* The connection itself failed. 08P01 (protocol violation) and 08007 (transaction resolution
           unknown) are deliberately absent — see the class remarks. */
        PostgresErrorCodes.ConnectionException,
        PostgresErrorCodes.SqlClientUnableToEstablishSqlConnection,
        PostgresErrorCodes.ConnectionDoesNotExist,
        PostgresErrorCodes.SqlServerRejectedEstablishmentOfSqlConnection,
        PostgresErrorCodes.ConnectionFailure,

        /* The rung's transaction lost a concurrency race and rolled back whole, stamp included. */
        PostgresErrorCodes.SerializationFailure,
        PostgresErrorCodes.DeadlockDetected,
        PostgresErrorCodes.LockNotAvailable,
        PostgresErrorCodes.ObjectInUse,
    };

    /// <summary>
    /// Whether the collection loop's first store interaction should be tried again after this failure.
    /// FALSE for anything not positively recognised, including every <see cref="PostgresException"/> whose
    /// <c>SqlState</c> is not in <see cref="s_retryableSqlStates"/> — see the class remarks for why the
    /// unclassifiable case is terminal rather than retried.
    /// </summary>
    /// <param name="exception">The failure. <see cref="OperationCanceledException"/> is the caller's to
    /// filter out first; it means shutdown, not a store problem.</param>
    internal static bool IsRetryable(Exception? exception)
    {
        if (exception is null)
        {
            return false;
        }

        /* Walk the chain rather than testing the outermost type: Npgsql reports a refused connect as
           NpgsqlException wrapping SocketException, and a mid-statement death as NpgsqlException wrapping
           IOException wrapping SocketException, so the outermost type alone cannot tell a transport
           failure from anything else Npgsql raises.

           TWO passes, not one, and the order is the point. A PostgresException anywhere in the chain is
           the server's own verdict on the thing that failed, and nothing wrapping it adds to that — so it
           decides regardless of how deeply it sits, rather than losing to whichever transport type a
           single pass happened to reach first. Only a chain carrying no verdict at all is read as a
           transport failure. */
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is PostgresException postgres)
            {
                return postgres.SqlState is not null && s_retryableSqlStates.Contains(postgres.SqlState);
            }
        }

        /* The not-found subtypes are IOExceptions and are NOT transient at any of the three sites: a
           config that is not there, or a pg-runtime.zip that is not there, is an install to fix, not a
           moment to wait out. Checked ahead of the transport pass so the IOException arm below cannot
           swallow them. See the class remarks for why this is a typed carve-out rather than a per-site
           predicate. */
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is FileNotFoundException or DirectoryNotFoundException)
            {
                return false;
            }
        }

        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is SocketException or IOException or TimeoutException)
            {
                return true;
            }
        }

        return exception is NpgsqlException;
    }
}
