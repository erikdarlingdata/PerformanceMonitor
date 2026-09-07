/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The INSTANCE half of #3095's phase contract: what the source pins in <see cref="StoreCopyPhaseTests"/>
/// structurally cannot see (#3099).
///
/// <para><b>Split out rather than serializing its pure sibling.</b> The data-phase check reads the shared
/// <c>DARLING_TEST_PG</c> store, which makes its whole class a live class —
/// <c>LivePostgresCollectionHygieneTests</c> requires that, and its preferred remedy is exactly this shape:
/// a mostly-pure class keeps its purity and the live test moves to its own <c>...LivePostgresTests</c>.
/// Putting <c>[Collection("live-postgres")]</c> on <see cref="StoreCopyPhaseTests"/> instead would serialize
/// seven source-text pins against every other live class in the suite for no reason.</para>
///
/// <para>The start-phase check needs no store and is kept here anyway: it and the data-phase check are a
/// test and its negative control, they share the probe plumbing below, and separating them would leave the
/// control one file away from the thing it controls. The cost is that one fast, storeless test is serialized
/// with the live collection, which is cheaper than either alternative.</para>
///
/// <para><b>Which leg of <see cref="StoreWriteReattempt.IsSafeToReattempt"/> each test here covers, stated
/// because it is not what the names suggest (#3111).</b> That gate is
/// <c>IsTransportFault(fault) &amp;&amp; For(fault) == StoreCopyPhase.Start</c>, and the two legs are
/// covered by different tests on different faults:</para>
///
/// <list type="bullet">
/// <item><see cref="AFaultOutOfBeginBinaryImportCarriesStart_OnTheRealPath"/> covers the PHASE leg only.
/// Its fault is measured, not assumed: an unopened connection makes the real
/// <c>BeginBinaryImportAsync</c> raise <c>InvalidOperationException("Connection is not open")</c>, which is
/// a start-phase fault that is NOT a transport fault — <see cref="PostgresTransportFault"/> walks the chain,
/// finds no socket/stream/timeout link, and the outermost type is not an
/// <see cref="NpgsqlException"/> either. So the full gate DECLINES that fault, and the test asserts exactly
/// that rather than leaving the reader to infer a retry from it.</item>
/// <item><see cref="ARealStartPhaseTimeoutIsATransportFaultAndTheGateAcceptsIt"/> covers BOTH legs, on the
/// real path, on the population the gate exists for: a start-phase COPY timeout under store-side
/// contention. It is the only test in the repository where a fault the runner itself produced is carried
/// end to end into an accepted re-attempt decision.</item>
/// <item><see cref="AFaultInsideTheRowLoopCarriesData_OnTheRealPath"/> is the phase leg's negative
/// control.</item>
/// </list>
///
/// <para><c>StoreWriteReattemptTests</c> covers the predicate's whole truth table, but it STAMPS its own
/// faults and constructs its own transport shapes — so it pins the policy given a fault and deliberately
/// owns nothing about which faults the runner really raises. That is the gap this class closes, and the
/// reason it is worth naming: the transport leg was reachable only through hand-built exceptions, so
/// nothing established that the faults the COPY actually produces satisfy it.</para>
/// </summary>
[Collection("live-postgres")]
public class StoreCopyPhaseLivePostgresTests
{
    /// <summary>
    /// A fault out of the REAL <c>BeginBinaryImportAsync</c>, on the real write path, carries
    /// <see cref="StoreCopyPhase.Start"/>.
    ///
    /// <para><b>Why this exists alongside the source pin above, not instead of it.</b> The two fail
    /// differently and neither covers the other's blind spot. The pin catches every edit of a shape it
    /// anticipated — a moved transition, a second one, an explicit <c>Stamp(ex, Start)</c> — and is blind
    /// to constructions nobody thought of; the gap that let a bare post-loop
    /// <c>copyPhase = StoreCopyPhase.Start;</c> pass all five of its assertions was exactly that. This
    /// test is indifferent to how the source got there: it runs the path and reads the stamp off a real
    /// exception, so an unanticipated construction that produces the wrong phase fails here even when
    /// every regex still matches.</para>
    ///
    /// <para>No live store is needed and that is the point of using an UNOPENED connection: the real
    /// <c>BeginBinaryImportAsync</c> refuses before it reaches a socket, which is precisely a start-phase
    /// fault — the importer never returned. The definition's <c>WritePayload</c> throws a distinctive
    /// exception, so if the row loop were ever reached this would surface as that fault instead of the
    /// connection's, and the assertion would name which.</para>
    ///
    /// <para><b>It covers the PHASE leg of the re-attempt gate and NOT the transport leg</b>, and the last
    /// two assertions say so in the test rather than in prose. The fault this path really raises is an
    /// <c>InvalidOperationException</c>, which is start-phase and is not transport — so
    /// <see cref="StoreWriteReattempt.IsSafeToReattempt"/> declines it. Asserting that keeps the reader from
    /// taking "a real start-phase fault on the real path" as evidence about a re-attempt, which is the
    /// inference the class summary exists to head off, and it fails if either half of the gate is ever
    /// loosened into accepting this shape.</para>
    /// </summary>
    [Fact]
    public async Task AFaultOutOfBeginBinaryImportCarriesStart_OnTheRealPath()
    {
        await using var dataSource = NpgsqlDataSource.Create(UnreachableStore);
        /* Deliberately NOT opened: that is what makes the real BeginBinaryImportAsync refuse before it
           reaches a socket, which is a start-phase fault and needs no store. */
        await using var connection = new NpgsqlConnection(UnreachableStore);

        var fault = await CopyFaultAsync(new PhaseProbeDefinition(), dataSource, connection);

        Assert.False(
            fault is PhaseProbeReachedTheRowLoopException,
            "the row loop must not be reachable when Begin refuses: WritePayload ran, so this test is no "
            + "longer exercising the start phase.");

        Assert.Equal(StoreCopyPhase.Start, CollectorFaultCopyPhase.For(fault));

        /* The scope of what this fault establishes, asserted. It is start-phase and it is not transport,
           so the conjunction declines it — measured on the fault the path produces rather than assumed
           from its type name. */
        Assert.False(PostgresTransportFault.IsTransportFault(fault));
        Assert.False(StoreWriteReattempt.IsSafeToReattempt(fault));
    }

    /// <summary>
    /// The transport leg of <see cref="StoreWriteReattempt.IsSafeToReattempt"/>, on the real path, on a real
    /// fault (#3111) — and with it the first end-to-end demonstration that the gate ACCEPTS something.
    ///
    /// <para><b>Why the sibling above does not cover this.</b> Its fault is an
    /// <c>InvalidOperationException</c>: start-phase, but not a transport fault, so the conjunction
    /// declines it. Every accepting case in the suite was a hand-stamped, hand-constructed exception. That
    /// left the load-bearing claim — that a COPY start-phase stall really is a transport fault and
    /// therefore really is re-attempted — resting on fixtures asserting their own shape, which is the
    /// failure mode where a predicate passes its tests while being wrong about its subject.</para>
    ///
    /// <para><b>How the condition is induced, and why this shape.</b> A collector for an exceptional state
    /// proves only that it parses when the state is absent, so the stall is produced rather than waited
    /// for: a second session holds <c>ACCESS EXCLUSIVE</c> on the COPY's target table, which makes the
    /// backend block acquiring its own lock BEFORE it can answer <c>CopyInResponse</c> — exactly the
    /// regime <see cref="StoreCopyPhase.Start"/> describes, and exactly what a store-contention hypothesis
    /// predicts. <c>CommandTimeout</c> is cut to a few seconds so that the CONNECTION's bound is the one
    /// that fires: the start phase now also carries <see cref="StoreCopyStartDeadline"/>, and letting that
    /// fire instead would spend the whole sweep deadline here and produce a different fault shape. Which of
    /// the two ends the stall is asserted below rather than left to the arithmetic. The connection string is
    /// DERIVED from the live one rather than rebuilt, so the harness's host, port and credentials cannot
    /// drift from it.</para>
    ///
    /// <para><see cref="TheStartPhaseIsBoundedByTheSweepDeadlineNotTheConnectionsCommandTimeout"/> is this
    /// test's twin from the other side, and the pair is what establishes that both bounds exist.</para>
    ///
    /// <para>Nothing is written and nothing needs cleaning: the lock is taken in a transaction that rolls
    /// back, and the COPY never completes a row. Serialization with the rest of the live collection is what
    /// makes a table lock safe to take at all, and this class already carries that attribute.</para>
    /// </summary>
    [Fact]
    public async Task ARealStartPhaseTimeoutIsATransportFaultAndTheGateAcceptsIt()
    {
        var pg = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(pg), "Set DARLING_TEST_PG to run the transport-leg instance check.");

        /* Which bound this test measures, asserted rather than inferred from two numbers a reader has to
           go and compare. If the sweep deadline were ever re-derived below this, StoreCopyStartDeadline
           would end the stall first and every assertion below about the fault's SHAPE would fail with
           nothing saying why. */
        Assert.True(
            StartPhaseTimeoutSeconds < StoreCopyStartDeadline.Deadline.TotalSeconds,
            $"this test needs the connection's {StartPhaseTimeoutSeconds}s CommandTimeout to fire before "
            + $"the {StoreCopyStartDeadline.Deadline.TotalSeconds}s start-phase deadline, because it asserts "
            + "Npgsql's own timeout shape; lower it, or move the assertion to the twin");

        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = NpgsqlDataSource.Create(pg!);
        await using (var migrateConnection = await dataSource.OpenConnectionAsync(ct))
        {
            await PerformanceMonitor.Darling.Storage.PgMigrations.MigrateAsync(migrateConnection, ct);
        }

        /* The blocker. ACCESS EXCLUSIVE conflicts with the ROW EXCLUSIVE a COPY takes, so the COPY's
           backend waits on the lock manager and never reaches CopyInResponse. Rolled back, so the lock is
           the whole of its effect. */
        await using var blocker = await dataSource.OpenConnectionAsync(ct);
        await using var blockerTransaction = await blocker.BeginTransactionAsync(ct);
        await using (var takeLock = new NpgsqlCommand(
            $"LOCK TABLE collect.{PhaseProbeTable} IN ACCESS EXCLUSIVE MODE", blocker, blockerTransaction))
        {
            await takeLock.ExecuteNonQueryAsync(ct);
        }

        /* Derived, not rebuilt: only the deadline differs from the live connection string. Its own data
           source, so the physical connection is new and cannot be a pooled one that predates the store's
           database-level search_path. */
        var bounded = new NpgsqlConnectionStringBuilder(pg!)
        {
            CommandTimeout = StartPhaseTimeoutSeconds,
        }.ConnectionString;

        await using var boundedSource = NpgsqlDataSource.Create(bounded);
        await using var connection = await boundedSource.OpenConnectionAsync(ct);

        var fault = await CopyFaultAsync(
            new PhaseProbeDefinition(PhaseProbeTable), boundedSource, connection, ct);

        Assert.False(
            fault is PhaseProbeReachedTheRowLoopException,
            "the row loop must not be reachable while the COPY target is locked: WritePayload ran, so the "
            + "blocker did not block and this test is measuring the data phase.");

        /* The phase leg. */
        Assert.Equal(StoreCopyPhase.Start, CollectorFaultCopyPhase.For(fault));

        /* THE LEG THIS TEST EXISTS FOR — and the premise underneath it, so a pass cannot come from the
           chain walk's NpgsqlException fallback while the fault is really something else. */
        Assert.IsAssignableFrom<NpgsqlException>(fault);
        Assert.IsType<TimeoutException>(fault.GetBaseException());
        Assert.True(PostgresTransportFault.IsTransportFault(fault));

        /* Both legs, so the gate accepts — the one claim no other test in the repository makes about a
           fault the runner itself raised. */
        Assert.True(StoreWriteReattempt.IsSafeToReattempt(fault));

        await blockerTransaction.RollbackAsync(ct);
    }

    /// <summary>
    /// The start phase is bounded by <see cref="StoreCopyStartDeadline"/> and not by whatever the store
    /// connection's <c>CommandTimeout</c> happens to be — measured on the shipped COPY body, against a
    /// real lock wait.
    ///
    /// <para><b>Why this cannot pass for the wrong reason.</b> <c>CommandTimeout</c> is set well ABOVE the
    /// sweep deadline, so the connection's own bound cannot be what ends the stall, and the two bounds
    /// produce DIFFERENT fault shapes: Npgsql's is an <c>NpgsqlException</c> wrapping a
    /// <see cref="TimeoutException"/> — the shape the sibling above asserts — while a breached start-phase
    /// deadline is a bare <see cref="TimeoutException"/> carrying
    /// <see cref="StoreCopyStartDeadline.BreachMessage"/>. Remove the bound and this test does not fail
    /// slowly or ambiguously: it waits for the connection's timeout and then fails on the type. The
    /// elapsed range is asserted on both sides as well, so a deadline accidentally written in
    /// milliseconds cannot satisfy the type and message alone.</para>
    ///
    /// <para><b>It really does cost the sweep deadline in wall clock</b>, and that is the price of driving
    /// the SHIPPED private body rather than a copy: the production call site takes
    /// <see cref="StoreCopyStartDeadline.Deadline"/> and there is nothing to inject a shorter one through.
    /// A copy of the method with a test-sized deadline would prove the transcription works, which is not
    /// the claim.</para>
    ///
    /// <para>Nothing is written and nothing needs cleaning, for the sibling's reasons: the lock is taken in
    /// a transaction that rolls back, and the COPY never starts a row.</para>
    /// </summary>
    [Fact]
    public async Task TheStartPhaseIsBoundedByTheSweepDeadlineNotTheConnectionsCommandTimeout()
    {
        var pg = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(pg), "Set DARLING_TEST_PG to run the start-phase bound check.");

        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = NpgsqlDataSource.Create(pg!);
        await using (var migrateConnection = await dataSource.OpenConnectionAsync(ct))
        {
            await PerformanceMonitor.Darling.Storage.PgMigrations.MigrateAsync(migrateConnection, ct);
        }

        await using var blocker = await dataSource.OpenConnectionAsync(ct);
        await using var blockerTransaction = await blocker.BeginTransactionAsync(ct);
        await using (var takeLock = new NpgsqlCommand(
            $"LOCK TABLE collect.{PhaseProbeTable} IN ACCESS EXCLUSIVE MODE", blocker, blockerTransaction))
        {
            await takeLock.ExecuteNonQueryAsync(ct);
        }

        /* DERIVED from the live string with only the deadline changed, like the sibling — and deliberately
           raised rather than left at Npgsql's default, so the connection's bound is out of reach of the
           whole test rather than merely unlikely to fire first. Relational to the constant, so a
           re-derivation of the sweep deadline moves this with it instead of stranding it. */
        var commandTimeoutSeconds = (int)StoreCopyStartDeadline.Deadline.TotalSeconds * 3;
        var wellAboveTheDeadline = new NpgsqlConnectionStringBuilder(pg!)
        {
            CommandTimeout = commandTimeoutSeconds,
        }.ConnectionString;

        await using var boundedSource = NpgsqlDataSource.Create(wellAboveTheDeadline);
        await using var connection = await boundedSource.OpenConnectionAsync(ct);

        var clock = Stopwatch.StartNew();
        var fault = await CopyFaultAsync(
            new PhaseProbeDefinition(PhaseProbeTable), boundedSource, connection, ct);
        clock.Stop();

        Assert.False(
            fault is PhaseProbeReachedTheRowLoopException,
            "the row loop must not be reachable while the COPY target is locked: WritePayload ran, so the "
            + "blocker did not block and this test is measuring the data phase.");

        /* The shape that only the start-phase deadline produces. Exact type, because Npgsql's own timeout
           arrives as an NpgsqlException wrapping one of these and would satisfy an assignability check. */
        Assert.IsType<TimeoutException>(fault);
        Assert.Equal(StoreCopyStartDeadline.BreachMessage, fault.Message);

        /* And it is not readable as a shutdown, which is the whole reason the breach is translated at all.
           Left as the cancellation Npgsql threw, this fault would be excluded from the phase stamp and
           from the re-attempt gate, and would be reported as an orderly stop. */
        Assert.IsNotAssignableFrom<OperationCanceledException>(fault);

        /* It fired on its own clock rather than the connection's. Both sides: the upper bound is what says
           Npgsql's timeout did not do this, and the lower bound is what a deadline written in
           milliseconds would fail. */
        Assert.InRange(
            clock.Elapsed,
            StoreCopyStartDeadline.Deadline - TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(commandTimeoutSeconds));

        /* And the consequences the phase axis carries, on this fault: it is a transport fault, it is
           start-phase, so the gate accepts it — a re-attempt after a start-phase stall is exactly-once and
           delta-safe because no row was started and no baseline moved. */
        Assert.True(PostgresTransportFault.IsTransportFault(fault));
        Assert.Equal(StoreCopyPhase.Start, CollectorFaultCopyPhase.For(fault));
        Assert.True(StoreWriteReattempt.IsSafeToReattempt(fault));

        await blockerTransaction.RollbackAsync(ct);
    }

    /// <summary>
    /// The negative control, and without it the test above proves much less: it would pass just as well if
    /// <c>Start</c> were stamped unconditionally on every COPY fault. Here the row loop IS reached — the
    /// definition's <c>WritePayload</c> throws once Begin has returned — and the stamp must read
    /// <see cref="StoreCopyPhase.Data"/>.
    ///
    /// <para>Begin has to succeed for the loop to be entered, so this one needs a real store and is gated
    /// on <c>DARLING_TEST_PG</c> like the rest of the live suite. The COPY targets a table that does not
    /// exist only in the negative case above; here it targets a real one so Begin returns.</para>
    /// </summary>
    [Fact]
    public async Task AFaultInsideTheRowLoopCarriesData_OnTheRealPath()
    {
        var pg = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(pg), "Set DARLING_TEST_PG to run the data-phase instance check.");

        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = NpgsqlDataSource.Create(pg!);
        await using (var migrateConnection = await dataSource.OpenConnectionAsync(ct))
        {
            await PerformanceMonitor.Darling.Storage.PgMigrations.MigrateAsync(migrateConnection, ct);
        }

        /* The COPY connection comes from a data source built AFTER the migration, and that is load-bearing
           rather than tidy. PgMigrations sets the store's search_path at DATABASE level, which a session
           picks up only when its physical connection is opened — and Npgsql pools, so a connection taken
           from the source above can be the very one the migrate ran on, carrying the search_path from
           before the ALTER. The definition's TargetTable is bare, so on a store being migrated for the
           FIRST time the COPY then fails 42P01 "relation wait_stats does not exist": Begin never returns,
           the row loop is not entered, and this test fails claiming the data phase was unreachable. It
           survives in a suite where some earlier live class has already migrated, which is the worst
           version of the bug — measured on a fresh store, on PostgreSQL 16.15 and 17.11. */
        await using var copySource = NpgsqlDataSource.Create(pg!);
        await using var connection = await copySource.OpenConnectionAsync(ct);
        var fault = await CopyFaultAsync(
            new PhaseProbeDefinition(PhaseProbeTable), copySource, connection, ct);

        Assert.True(
            fault is PhaseProbeReachedTheRowLoopException,
            $"Begin must have returned so the row loop is entered; got {fault.GetType().Name}: {fault.Message}");

        Assert.Equal(StoreCopyPhase.Data, CollectorFaultCopyPhase.For(fault));
    }

    /* ── probe plumbing ── */

    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=probe;Database=probe";

    /// <summary>
    /// The real store table both live tests COPY into, named once because the transport-leg test also has
    /// to LOCK it and the two spellings must be the same table — a lock on a different one would let the
    /// COPY through and the test would silently measure the data phase instead. The store's own
    /// database-level <c>search_path</c> puts <c>collect</c> first, so the definition's bare
    /// <c>TargetTable</c> resolves to it while the <c>LOCK</c> qualifies it explicitly.
    /// </summary>
    private const string PhaseProbeTable = "wait_stats";

    /// <summary>
    /// The CONNECTION deadline <see cref="ARealStartPhaseTimeoutIsATransportFaultAndTheGateAcceptsIt"/>
    /// blocks against. Small for two reasons: Npgsql's default there is an undocumented 30 s, which is dead
    /// time in every CI run, and it has to fire ahead of <see cref="StoreCopyStartDeadline.Deadline"/> for
    /// that test to see Npgsql's own timeout shape rather than a translated breach. That ordering is
    /// asserted in the test rather than left to two numbers sitting in different files.
    /// </summary>
    private const int StartPhaseTimeoutSeconds = 3;

    /// <summary>
    /// Drives the SHIPPED private COPY body and returns the fault it raised. Reflection because the method
    /// is private and <c>InternalsVisibleTo</c> does not reach private members; a rename breaks this
    /// loudly, which is the correct failure for a test whose whole subject is that method.
    ///
    /// <para><b>Takes its connection and data source rather than owning them, and has no <c>finally</c>.</b>
    /// Every caller holds both under <c>await using</c>, so disposal is the language's job — and the reason
    /// is ORDERING, which turns on the fact that this helper RETURNS the fault instead of letting it
    /// propagate. By the time a <c>finally</c> here would run, the <c>catch</c> below has already converted
    /// the fault into this method's return value, so there is no in-flight exception for a throwing
    /// <c>DisposeAsync</c> to replace; what it would replace is the returned exception object itself, and it
    /// would do so BEFORE any caller had read a single assertion off it. Leaving disposal to each caller's
    /// <c>await using</c> puts it at the end of the test method, after every assertion about the fault has
    /// run — so a dispose that throws can only fail a test whose measurement is already complete, rather
    /// than destroy the measurement. Same family of hazard as the one
    /// <c>LiveCleanupConversionRatchetTests</c> exists to stamp out, differing in what is at risk.
    /// <c>LiveStoreCleanup</c> is not the remedy here because there is nothing to clean: the COPY always
    /// faults, so no attempt commits a row.</para>
    /// </summary>
    private static async Task<Exception> CopyFaultAsync(
        PhaseProbeDefinition definition,
        NpgsqlDataSource dataSource,
        NpgsqlConnection connection,
        CancellationToken cancellationToken = default)
    {
        var runner = new DarlingCollectorRunner(dataSource, new CollectorDeltaCalculator());
        var method = typeof(DarlingCollectorRunner)
            .GetMethod("CopyBatchOnceAsync", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.NotNull(method);

        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "phase-probe", Host = "phase-probe-host" },
            ConnectionString = "Server=phase-probe-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "phase-probe-host",
            ServerId = -3099,
            EngineEdition = 3,
        };

        var context = new CollectorContext
        {
            ServerId = server.ServerId,
            ServerName = server.StorageName,
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
            Target = server.Target,
        };

        var task = (Task)method!.MakeGenericMethod(typeof(int)).Invoke(
            runner,
            new object?[]
            {
                connection, definition, new List<int> { 1 }, server,
                DateTime.UtcNow, context, cancellationToken,
            })!;

        try
        {
            await task;
        }
        catch (Exception ex)
        {
            return ex;
        }

        Assert.Fail("the COPY body was expected to fault");
        throw new InvalidOperationException("unreachable");
    }

    private sealed class PhaseProbeReachedTheRowLoopException : Exception
    {
        public PhaseProbeReachedTheRowLoopException() : base("WritePayload was reached") { }
    }

    /// <summary>
    /// A minimal non-diverting definition. Non-diverting matters: a diverting collector opens a
    /// transaction BEFORE the try, which on an unopened connection would throw outside the stamped region
    /// and the probe would read Unknown for a reason that has nothing to do with the phase.
    /// </summary>
    private sealed class PhaseProbeDefinition : CollectorDefinitionBase<int>
    {
        private readonly string _table;

        public PhaseProbeDefinition(string table = "phase_probe_no_such_table") => _table = table;

        public override string Name => "phase_probe";

        public override string TargetTable => _table;

        public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } =
            new[] { new CollectorColumn("wait_type", CollectorColumnType.Varchar) };

        public override CollectorQuery BuildQuery(CollectorContext context)
            => throw new NotSupportedException("the probe never fetches");

        public override ValueTask<List<int>> ReadAsync(
            DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
            => throw new NotSupportedException("the probe never fetches");

        /* Throws so the row loop is DETECTABLE: in the start-phase case it must never run, and in the
           data-phase case it is how the loop is entered and then faulted. */
        public override void WritePayload(int row, ICollectorRowWriter writer, CollectorContext context)
            => throw new PhaseProbeReachedTheRowLoopException();
    }
}
