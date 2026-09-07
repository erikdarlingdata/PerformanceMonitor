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

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var fault = await CopyFaultAsync(new PhaseProbeDefinition("wait_stats"), dataSource, connection, ct);

        Assert.True(
            fault is PhaseProbeReachedTheRowLoopException,
            $"Begin must have returned so the row loop is entered; got {fault.GetType().Name}: {fault.Message}");

        Assert.Equal(StoreCopyPhase.Data, CollectorFaultCopyPhase.For(fault));
    }

    /* ── probe plumbing ── */

    private const string UnreachableStore = "Host=127.0.0.1;Port=1;Username=probe;Database=probe";

    /// <summary>
    /// Drives the SHIPPED private COPY body and returns the fault it raised. Reflection because the method
    /// is private and <c>InternalsVisibleTo</c> does not reach private members; a rename breaks this
    /// loudly, which is the correct failure for a test whose whole subject is that method.
    ///
    /// <para><b>Takes its connection and data source rather than owning them, and has no <c>finally</c>.</b>
    /// Every caller holds both under <c>await using</c>, so disposal is the language's job. That is not
    /// stylistic: a <c>DisposeAsync</c> in a <c>finally</c> can throw and REPLACE the body's exception,
    /// which on this helper is the entire result being measured — and it is the same hazard
    /// <c>LiveCleanupConversionRatchetTests</c> exists to stamp out. <c>LiveStoreCleanup</c> is not the
    /// remedy here because there is nothing to clean: the COPY always faults, so no attempt commits a row.
    /// </para>
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
