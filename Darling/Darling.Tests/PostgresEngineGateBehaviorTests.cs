/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// BEHAVIOURAL coverage for the PostgreSQL engine gate on the <c>analyze_now</c> operator door (#2230).
///
/// <para><b>What was missing.</b> The three gates added by #2213's round-3 fix were covered by
/// source-scanning pins (<c>TheScheduledAnalysisPassIsGatedByEngine</c> greps the call site) and by a live
/// rig run, but nothing drove a PostgreSQL runtime through a gate and asserted the short-circuit. A source
/// scan cannot tell a gate that returns early from one that falls through and happens to write the same
/// text.</para>
///
/// <para><b>All three doors.</b> The analyze_now gate's observable is a PRESENCE — a row in
/// <c>analysis_state</c> carrying the engine tombstone — so it is asserted directly. The reconcile gate
/// looked like it needed a counting seam to observe an absence, and that was wrong: the belt gate inside
/// <see cref="DarlingXeSessions.ReconcileLongQueryCompletionsAsync"/> is public and its own precondition,
/// so an ungated call THROWS and a gated one returns — a difference an assertion can see with no seam, no
/// live store, and no network. snapshot_now needed neither trick: its dispatch loop already reports how many
/// collectors it ran, and every run it makes writes itself to <c>collection_log</c>, so the gate's effect is
/// a count and a set of rows rather than something that has to be inferred.</para>
///
/// <para><b>The regression it guards is specific and was real.</b> Clicking "Generate now" against a
/// PostgreSQL target used to run the full SQL-Server-shaped pass, find nothing, and persist the GENERIC
/// <c>insufficient_data</c> message — OVERWRITING the honest engine tombstone the scheduled arm had already
/// written. The Recommendations tab regressed from "does not apply, use the PG reads" back to "still
/// collecting" the moment an operator pressed the button. So the assertion that matters is not just
/// "insufficient_data is true", it is that the MESSAGE is the engine one.</para>
///
/// <para>Live-store gated on <c>DARLING_TEST_PG</c>, which CI's "Darling PostgreSQL tests" job sets — the
/// gate's whole effect is a write through <c>_postgres</c>, so there is nothing to observe without one.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PostgresEngineGateBehaviorTests
{
    /// <summary>
    /// The HOST is what identity derives from, which is the trap this test tripped over first:
    /// <c>MonitoredServer.StorageName</c> is <c>BuildStorageName(Host, Database, ReadOnlyIntent, Engine, Port)</c>
    /// (#2218 added the last two) — NOT
    /// <c>Name</c> — and <c>RunAnalyzeNowAsync</c> finds a server by hashing that. A server_id hashed from
    /// anything else simply is not found, and the gate then returns "server not monitored" rather than the
    /// arm under test. Unique hosts so neither case can collide with a real server's analysis_state row.
    /// </summary>
    private const string PgHost = "pg-engine-gate-behavior-2230.invalid";

    private const string SqlHost = "sql-engine-gate-behavior-2230.invalid";

    /* snapshot_now writes to collection_log rather than analysis_state, and it asserts on EMPTINESS — so it
       needs hosts of its own, or the analyze_now cases sharing a server_id would make the absence assertion
       depend on test ordering. */
    private const string PgSnapshotHost = "pg-snapshot-gate-behavior-2230.invalid";

    private const string SqlSnapshotHost = "sql-snapshot-gate-behavior-2230.invalid";

    /// <summary>The one collector both snapshot arms leave enabled: SQL-Server-only, so the engine gate is
    /// the ONLY thing that can decide whether it is dispatched.</summary>
    private const string SnapshotCollector = "wait_stats";

    /// <summary>
    /// Derived through the SAME helper the worker uses, so the test cannot drift from the lookup.
    ///
    /// <para>#2218 is that drift happening: the ENGINE joined the derivation, and because this helper hashed
    /// without it, a PostgreSQL host produced an id the product never uses — so the gate returned "server not
    /// monitored" rather than the arm under test, which is the same trap the class comment above describes for
    /// <c>Name</c> vs <c>Host</c>. The engine is defaulted to null so the SQL Server call sites stay unchanged,
    /// exactly as the shared helper does it.</para>
    /// </summary>
    private static int ServerIdFor(string host, string? engine = null) =>
        ServerIdHelper.GetDeterministicHashCode(ServerIdHelper.BuildStorageName(host, null, false, engine, 0));

    [Fact]
    public async Task AnalyzeNow_AgainstAPostgresTarget_WritesTheEngineTombstone_AndDoesNotRunThePass()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the analyze_now engine-gate test.");

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var serverId = ServerIdFor(PgHost, "postgres");

        /* Fabricated worker, the CollectorMemoryKnobTests.SweepGate idiom: the real ctor wants a host's worth
           of dependencies, and the gate under test reads exactly three fields. Reflection because pinning the
           BEHAVIOUR beats widening the surface just to observe it. */
        var worker = (DarlingWorker)System.Runtime.CompilerServices.RuntimeHelpers
            .GetUninitializedObject(typeof(DarlingWorker));
        SetField(worker, "_serversLock", new object());
        SetField(worker, "_logger", NullLogger<DarlingWorker>.Instance);
        SetField(worker, "_postgres", postgres);

        var server = PostgresLoopState(serverId);
        var servers = NewLoopStateList(server);

        var bodySucceeded = false;
        try
        {
            var outcome = await InvokeAnalyzeNowAsync(worker, servers, serverId);

            /* 1. The gate returned the success shape, not a failure and not the analysis result. */
            /* Assert the STATUS first: if the lookup missed, the status is "server not monitored" and says
               so, where a bare Assert.True on Success only reports Expected/Actual booleans. */
            Assert.Equal("analysis not applicable", GetOutcomeStatus(outcome));
            Assert.True(GetOutcomeSuccess(outcome));

            /* 2. The once-latch is set, so the scheduled tick will not re-write what this just wrote —
                  the two arms share the tombstone rather than racing to overwrite it. */
            Assert.True(AnalysisStateWritten(server));

            /* 3. THE REGRESSION GUARD: the persisted message is the ENGINE tombstone, not the generic
                  insufficient-data text the SQL-Server-shaped pass would have left. */
            var state = await ReadAnalysisStateAsync(postgres, serverId);
            var (found, insufficient, message) = (state.Found, state.Insufficient, state.Message);
            Assert.True(found, "the gate must PERSIST a row, or the Recommendations tab has nothing to show");
            Assert.True(insufficient);
            Assert.Equal(DarlingWorker.PostgresAnalysisNotApplicable, message);

            /* And the specific words that make it honest rather than merely non-empty. */
            Assert.Contains("does not apply to a PostgreSQL target", message, StringComparison.Ordinal);
            Assert.Contains("get_pg_blocking", message, StringComparison.Ordinal);
            /* And it DISCLAIMS the still-collecting reading rather than avoiding the words: the message
               quotes the phrase in order to contrast with it ("This is not \"still collecting\""), so a
               DoesNotContain on those words can never pass and asserting it was my error, not the
               product's. The property worth pinning is that the disclaimer is present. */
            Assert.Contains("This is not \"still collecting\"", message, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) =>
                DeleteAnalysisStateAsync(cleanup, cleanupCt, serverId));
        }
    }

    /// <summary>
    /// The same door against a SQL Server target must NOT take the gate — otherwise the test above would
    /// pass on a gate that fires unconditionally, which is the failure mode a presence-assertion is blind to.
    /// <para>Asserted by the outcome status alone: a SQL Server target falls through to the real pass, which
    /// on a store with no data for this server_id reports insufficient data. Either way it is NOT
    /// "analysis not applicable", and that is the discriminator.</para>
    /// </summary>
    [Fact]
    public async Task AnalyzeNow_AgainstASqlServerTarget_DoesNotTakeTheEngineGate()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the analyze_now engine-gate test.");

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var serverId = ServerIdFor(SqlHost);

        var worker = (DarlingWorker)System.Runtime.CompilerServices.RuntimeHelpers
            .GetUninitializedObject(typeof(DarlingWorker));
        SetField(worker, "_serversLock", new object());
        SetField(worker, "_logger", NullLogger<DarlingWorker>.Instance);
        SetField(worker, "_postgres", postgres);

        var server = SqlServerLoopState(serverId);
        var servers = NewLoopStateList(server);

        var bodySucceeded = false;
        try
        {
            /* The SQL Server path runs the real analysis pass, which needs collaborators the fabricated
               worker does not have — so the assertion is that it did NOT short-circuit as the PG arm, which
               is observable either as a different status or as a throw from the pass itself. Both prove the
               gate is engine-conditional; only "analysis not applicable" would disprove it. */
            string? status = null;
            try
            {
                status = GetOutcomeStatus(await InvokeAnalyzeNowAsync(worker, servers, serverId));
            }
            catch (Exception ex) when (ex is not Xunit.Sdk.XunitException)
            {
                /* Fell through into the pass and hit a missing collaborator — which is itself the proof. */
                Assert.NotNull(ex);
            }

            Assert.NotEqual("analysis not applicable", status);
            Assert.False(AnalysisStateWritten(server),
                "the PostgreSQL once-latch must not be set for a SQL Server target");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) =>
                DeleteAnalysisStateAsync(cleanup, cleanupCt, serverId));
        }
    }

    /// <summary>
    /// The reconcile door, gated (#2230). <see cref="DarlingXeSessions.ReconcileLongQueryCompletionsAsync"/>
    /// carries its own engine precondition — "belt to the worker's braces" — and it is the belt that
    /// actually stopped the field failure, so it is the one worth pinning.
    ///
    /// <para>The regression was measured, not theoretical: ungated, this method built a
    /// <c>SqlConnection</c> from a PostgreSQL connection string, the ctor threw
    /// <c>Keyword not supported: 'host'</c>, the caller's catch skipped the latch assignment, and because
    /// <c>LongQueryTraceApplied</c> resets to null on every connect it retried EVERY sweep forever —
    /// ~1,440 warnings/day/server (#2213 round 2).</para>
    ///
    /// <para>Both <c>enabled</c> values, because the gate sits ahead of that branch: ungated, the false arm
    /// would try to DROP a session over the same impossible connection.</para>
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ReconcileLongQueryCompletions_AgainstAPostgresTarget_ReturnsBeforeBuildingASqlConnection(bool enabled)
    {
        /* runner is null on purpose: reaching it would mean the gate did not fire. */
        await DarlingXeSessions.ReconcileLongQueryCompletionsAsync(
            PostgresRuntime(), runner: null!, enabled, NullLogger<DarlingWorker>.Instance, CancellationToken.None);
    }

    /// <summary>
    /// The proof the test above is not vacuous. Same connection string, ENGINE flipped to SQL Server: the
    /// gate no longer applies, the ctor rejects the string, and the exact field exception surfaces. Without
    /// this arm, the pin above would pass just as happily against a method that had stopped connecting for
    /// some unrelated reason.
    /// </summary>
    [Fact]
    public async Task ReconcileLongQueryCompletions_SameStringButSqlServerEngine_ThrowsTheFieldFailure()
    {
        /* The engine is the ONLY difference from the gated case — same host, same connection string. */
        var ungated = PostgresRuntime(CollectorTargetEngine.SqlServer);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            DarlingXeSessions.ReconcileLongQueryCompletionsAsync(
                ungated, runner: null!, enabled: true, NullLogger<DarlingWorker>.Instance, CancellationToken.None));

        /* The words from the sweep log, so a future reader can match this pin to that incident. */
        Assert.Contains("Keyword not supported", ex.Message, StringComparison.Ordinal);
        Assert.Contains("host", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The snapshot_now door, the THIRD dispatch loop and the one that got neither engine gate in #2213's
    /// first round (#2230).
    ///
    /// <para><b>The regression is phantom success, not a crash.</b> An operator snapshot against a
    /// PostgreSQL target dispatched every SQL Server collector. Those collectors do not fail loudly — their
    /// own <c>AppliesTo</c> early-returns, yielding zero rows, and <c>RunOneAsync</c> then writes
    /// <c>SUCCESS</c> to <c>collection_log</c>. So one click produced a burst of ~40 rows saying collection
    /// worked, on an engine where those collectors cannot mean anything. That reads as health, which is
    /// strictly worse than an error.</para>
    ///
    /// <para><b>Why one collector rather than all of them.</b> The schedule overrides disable everything
    /// except <c>wait_stats</c> — SQL-Server-only, and dispatched through the same loop — so the two arms
    /// below differ in the ENGINE and nothing else: same collector, same overrides, same store. That keeps
    /// the test to a single dispatch decision instead of 49, and makes the SQL Server arm cheap enough to be
    /// the non-vacuity proof rather than a second slow test.</para>
    /// </summary>
    [Fact]
    public async Task SnapshotNow_AgainstAPostgresTarget_DispatchesNoSqlServerCollector()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the snapshot_now engine-gate test.");

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var serverId = ServerIdFor(PgSnapshotHost, "postgres");

        var bodySucceeded = false;
        try
        {
            var (collectorsRun, _) = await InvokeSnapshotAsync(
                postgres,
                NewLoopState(
                    new MonitoredServer { Name = "pg-snapshot-gate", Host = PgSnapshotHost, Engine = "postgres" },
                    SnapshotRuntime(PgSnapshotHost, serverId, CollectorTargetEngine.PostgreSql)),
                serverId);

            /* 1. THE GATE: the only enabled collector is SQL-Server-only, so a gated loop runs nothing. */
            Assert.Equal(0, collectorsRun);

            /* 2. And it left no trace claiming otherwise. This is the assertion that would have caught the
                  original defect: ungated, collection_log carries a wait_stats row here, and its status is
                  SUCCESS — the phantom-success class. Asserting on the ABSENCE of the row rather than on its
                  status is deliberate, because a gate that dispatched and then failed would also avoid
                  SUCCESS while still having connected to a PostgreSQL host as SQL Server. */
            Assert.Empty(await ReadLoggedCollectorsAsync(postgres, serverId));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) =>
                DeleteCollectionLogAsync(cleanup, cleanupCt, serverId));
        }
    }

    /// <summary>
    /// The proof the test above is not vacuous. Same collector, same overrides, engine flipped to SQL
    /// Server: the loop dispatches it, and the run writes itself to <c>collection_log</c>.
    ///
    /// <para>Without this arm the gate assertion would pass just as well against a snapshot that had stopped
    /// dispatching anything at all — a loop broken for some unrelated reason looks identical to a loop
    /// gated correctly, and "ran zero collectors" is exactly what a broken snapshot also reports.</para>
    ///
    /// <para>The connection goes to a loopback port with no listener, so the collector fails immediately
    /// rather than waiting out a default timeout. The STATUS is not asserted: whether the attempt lands
    /// ERROR or a zero-row SUCCESS depends on how far the collector gets before the connection dies, and the
    /// fact under test is that it was dispatched at all.</para>
    /// </summary>
    [Fact]
    public async Task SnapshotNow_AgainstASqlServerTarget_DispatchesTheSameCollector()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the snapshot_now engine-gate test.");

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var serverId = ServerIdFor(SqlSnapshotHost);

        var bodySucceeded = false;
        try
        {
            var (collectorsRun, success) = await InvokeSnapshotAsync(
                postgres,
                NewLoopState(
                    new MonitoredServer { Name = "sql-snapshot-gate", Host = SqlSnapshotHost },
                    SnapshotRuntime(SqlSnapshotHost, serverId, CollectorTargetEngine.SqlServer)),
                serverId);

            Assert.True(success);
            Assert.Equal(1, collectorsRun);

            /* The row the PostgreSQL arm must not have, under the name that identifies it. */
            Assert.Equal(new[] { "wait_stats" }, await ReadLoggedCollectorsAsync(postgres, serverId));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (cleanup, cleanupCt) =>
                DeleteCollectionLogAsync(cleanup, cleanupCt, serverId));
        }
    }

    /// <summary>A PostgreSQL runtime whose connection string is the PostgreSQL shape that <c>SqlConnection</c>
    /// cannot parse — the combination that produced the field failure.</summary>
    private static ServerRuntime PostgresRuntime(
        CollectorTargetEngine engine = CollectorTargetEngine.PostgreSql) => new()
    {
        Config = new MonitoredServer { Name = "pg-reconcile-gate", Host = PgHost, Engine = "postgres" },
        ConnectionString = $"Host={PgHost};Database=postgres;Username=monitor",
        Target = new CollectorTargetInfo { Engine = engine },
        StorageName = PgHost,
        ServerId = ServerIdFor(PgHost, "postgres"),
    };

    /// <summary>
    /// #2579: the within-engine pre-dispatch skip covers EVERY engine, not just PostgreSQL.
    ///
    /// <para>It was scoped to PostgreSQL when it landed, deliberately, because dropping it on SQL Server
    /// changes a shipping SKU's log semantics for the Azure-gated collectors — left as "its own decision".
    /// This is that decision. What settled it: on an AWS RDS fleet the SQL Server gates are not a handful.
    /// 84 instances x <c>agent_status</c> and <c>running_jobs</c> x a 5-minute cadence is ~24,000
    /// <c>collection_log</c> rows a day reporting SUCCESS for collectors that deliberately do not run.</para>
    ///
    /// <para>And a gated-off run recorded as SUCCESS is byte-identical to a real one — same status, zero
    /// rows, no note — so nothing downstream can tell them apart. That is the shape the miss vocabulary
    /// exists to prevent, and it convincingly read as evidence of working collection: it produced a filed
    /// issue and an opened PR before the 0ms durations gave it away.</para>
    ///
    /// <para>Asserted against the shipped source rather than by running a sweep, because the condition is
    /// one line inside the dispatch loop and the alternative is standing up a whole worker. The pin is that
    /// the engine discriminator is GONE from this gate — if it comes back, SQL Server silently resumes
    /// logging fake successes.</para>
    /// </summary>
    [Fact]
    public void ThePreDispatchGate_AppliesToEveryEngine_NotOnlyPostgres()
    {
        var source = File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        var gate = source.IndexOf("if (!CollectorCatalog.AppliesTo(name, runtime.Target))", StringComparison.Ordinal);
        Assert.True(gate > 0, "the within-engine pre-dispatch gate is gone — a gated-off collector will log a fake SUCCESS again (#2579)");

        /* The engine discriminator must not be back on this gate. Scanning the 400 characters before it
           rather than the whole file, because CollectorTargetEngine.PostgreSql legitimately appears all over
           this class — it is only wrong HERE. */
        var window = source[Math.Max(0, gate - 400)..gate];
        Assert.DoesNotContain("runtime.Target.Engine == CollectorTargetEngine.PostgreSql", window, StringComparison.Ordinal);

        /* The wrong-DIALECT drop above it is a separate gate and must survive. */
        Assert.Contains("if (!CollectorCatalog.EngineMatches(name, runtime.Target))", source, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    private static void SetField(DarlingWorker worker, string name, object value) =>
        typeof(DarlingWorker)
            .GetField(name, BindingFlags.NonPublic | BindingFlags.Instance)!
            .SetValue(worker, value);

    private static async Task<object> InvokeAnalyzeNowAsync(
        DarlingWorker worker, object servers, int serverId)
    {
        var method = typeof(DarlingWorker).GetMethod(
            "RunAnalyzeNowAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

        /* planFetcher / notificationService / config are only touched on the SQL Server path, so the gate
           can be driven with nulls — which is itself part of what "short-circuits" means here. */
        var task = (Task)method.Invoke(worker, new object?[]
        {
            servers, null, null, null, serverId, CancellationToken.None,
        })!;
        await task;
        return task.GetType().GetProperty("Result")!.GetValue(task)!;
    }

    /* CommandOutcome is public (DarlingCommandExecutor), so no reflection is needed for the result —
       only for ServerLoopState, which is a private nested type. */
    private static bool GetOutcomeSuccess(object outcome) => ((CommandOutcome)outcome).Success;

    private static string? GetOutcomeStatus(object? outcome) => (outcome as CommandOutcome)?.ResultStatus;

    /// <summary>
    /// <c>DarlingWorker.ServerLoopState</c> is a PRIVATE nested class, so the test cannot name the type and
    /// builds it reflectively — the same trade <c>CollectorMemoryKnobTests</c> makes for private gate state.
    /// Widening it to <c>internal</c> purely for a test would be a production change to observe behaviour
    /// that reflection can already reach.
    /// </summary>
    private static readonly Type LoopStateType = typeof(DarlingWorker)
        .GetNestedType("ServerLoopState", BindingFlags.NonPublic)!;

    private static object NewLoopState(MonitoredServer config, ServerRuntime runtime)
    {
        var state = Activator.CreateInstance(LoopStateType)!;
        LoopStateType.GetProperty("Config")!.SetValue(state, config);
        LoopStateType.GetProperty("Runtime")!.SetValue(state, runtime);
        return state;
    }

    private static bool AnalysisStateWritten(object loopState) =>
        (bool)LoopStateType.GetProperty("PostgresAnalysisStateWritten")!.GetValue(loopState)!;

    /// <summary>The parameter is <c>List&lt;ServerLoopState&gt;</c>, so the list is reflective too.</summary>
    private static object NewLoopStateList(object single)
    {
        var list = Activator.CreateInstance(typeof(List<>).MakeGenericType(LoopStateType))!;
        list.GetType().GetMethod("Add")!.Invoke(list, new[] { single });
        return list;
    }

    private static object PostgresLoopState(int serverId) => NewLoopState(
        new MonitoredServer { Name = "pg-gate", Host = PgHost, Engine = "postgres" },
        new ServerRuntime
        {
            Config = new MonitoredServer { Name = "pg-gate", Host = PgHost, Engine = "postgres" },
            ConnectionString = $"Host={PgHost};Database=postgres;Username=monitor",
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql },
            StorageName = PgHost,
            ServerId = serverId,
        });

    private static object SqlServerLoopState(int serverId) => NewLoopState(
        new MonitoredServer { Name = "sql-gate", Host = SqlHost },
        new ServerRuntime
        {
            Config = new MonitoredServer { Name = "sql-gate", Host = SqlHost },
            ConnectionString = $"Server={SqlHost};Integrated Security=true",
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
            StorageName = SqlHost,
            ServerId = serverId,
        });

    private static async Task<(bool Found, bool Insufficient, string Message)> ReadAnalysisStateAsync(
        NpgsqlDataSource postgres, int serverId)
    {
        await using var command = postgres.CreateCommand(
            "SELECT insufficient_data, message FROM analysis_state WHERE server_id = $1 " +
            "ORDER BY analysis_time DESC LIMIT 1");
        command.Parameters.AddWithValue(serverId);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        if (!await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            return (false, false, string.Empty);
        }

        return (true,
            !reader.IsDBNull(0) && reader.GetBoolean(0),
            reader.IsDBNull(1) ? string.Empty : reader.GetString(1));
    }

    /// <summary>
    /// Deletes only this test's own synthetic server_id, through <c>LiveStoreCleanup</c> so the teardown runs
    /// on its OWN connection rather than the body's (#1902). A finally that tears down on the body's
    /// connection throws out of the finally and REPLACES the body's exception with the teardown's — and it is
    /// the body's failure that closed the connection in the first place, so the teardown fails because of the
    /// thing it then hides. Opening a fresh connection by hand is explicitly not accepted either: it is half
    /// the fix and still throws from the finally.
    /// </summary>
    private static async Task DeleteAnalysisStateAsync(
        NpgsqlConnection cleanup, CancellationToken cleanupCt, int serverId)
    {
        await using var command = new NpgsqlCommand(
            "DELETE FROM analysis_state WHERE server_id = $1", cleanup);
        command.Parameters.AddWithValue(serverId);
        await command.ExecuteNonQueryAsync(cleanupCt);
    }

    /// <summary>
    /// A runtime for the snapshot arms. A dispatched collector must fail FAST, because the SQL Server arm's
    /// whole purpose is to prove dispatch happened, not to collect anything.
    ///
    /// <para><b>The connection host is deliberately NOT the identity host.</b> Identity comes from
    /// <c>host</c> — a synthetic <c>.invalid</c> name, so the lookup and the store rows cannot collide with a
    /// real server — while the connection goes to <c>127.0.0.1</c> port 1, which is the suite's existing
    /// unreachable-endpoint idiom (<c>ViewerControlPlaneStage3bTests</c>, the MCP <c>DeadStore</c>
    /// constants). Connection-refused from a loopback port with no listener is immediate and depends on
    /// nothing outside the runner; resolving a <c>.invalid</c> name instead makes the timing a property of
    /// CI's resolver, which is not a thing this test should be measuring.</para>
    /// </summary>
    private static ServerRuntime SnapshotRuntime(string host, int serverId, CollectorTargetEngine engine) => new()
    {
        Config = new MonitoredServer { Name = host, Host = host },
        ConnectionString = engine == CollectorTargetEngine.PostgreSql
            ? "Host=127.0.0.1;Port=1;Database=postgres;Username=monitor;Timeout=1"
            /* SQL auth rather than integrated: the failure under test is the connect, and integrated auth on
               a Linux runner fails for a platform reason instead, which is a different thing to assert on. */
            : "Server=127.0.0.1,1;User ID=x;Password=x;Connect Timeout=1;Encrypt=false",
        Target = new CollectorTargetInfo { Engine = engine },
        StorageName = host,
        ServerId = serverId,
    };

    /// <summary>
    /// Every collector disabled except <see cref="SnapshotCollector"/>, as per-server overrides. This is what
    /// keeps the two arms to a single dispatch decision — and it goes through the shipped
    /// <see cref="StoreConfigProvider.ResolveSchedule"/>, which the loop consults, rather than reaching past
    /// it.
    /// </summary>
    private static IReadOnlyList<ScheduleOverride> SingleEnabledCollectorOverrides(int serverId) =>
        CollectorScheduleDefaults.All.Keys
            .Select(name => new ScheduleOverride(
                serverId, name, null, null,
                Enabled: string.Equals(name, SnapshotCollector, StringComparison.OrdinalIgnoreCase)))
            .ToList();

    /// <summary>
    /// Drives the real <c>RunSnapshotAsync</c> and returns what its outcome JSON reports. The runner is a
    /// REAL <see cref="DarlingCollectorRunner"/> rather than a stand-in: a fake would have to reimplement the
    /// dispatch it exists to observe, and the collection_log write that carries the assertion happens inside
    /// the real path.
    /// </summary>
    private static async Task<(int CollectorsRun, bool Success)> InvokeSnapshotAsync(
        NpgsqlDataSource postgres, object loopState, int serverId)
    {
        var worker = (DarlingWorker)System.Runtime.CompilerServices.RuntimeHelpers
            .GetUninitializedObject(typeof(DarlingWorker));
        SetField(worker, "_serversLock", new object());
        SetField(worker, "_logger", NullLogger<DarlingWorker>.Instance);
        SetField(worker, "_postgres", postgres);
        SetField(worker, "_scheduleOverrides", SingleEnabledCollectorOverrides(serverId));

        var runner = new DarlingCollectorRunner(
            postgres, new CollectorDeltaCalculator(), NullLogger<DarlingCollectorRunner>.Instance);

        var method = typeof(DarlingWorker).GetMethod(
            "RunSnapshotAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var task = (Task)method.Invoke(worker, new object?[]
        {
            NewLoopStateList(loopState), runner, serverId, TestContext.Current.CancellationToken,
        })!;
        await task;

        var outcome = (CommandOutcome)task.GetType().GetProperty("Result")!.GetValue(task)!;

        /* collectorsRun is only in the JSON — the outcome record carries the status text, not the count. A
           failure shape has no such property, so a missing one is reported as a failed snapshot rather than
           silently read as zero. */
        using var json = System.Text.Json.JsonDocument.Parse(outcome.ResultJson!);
        var ran = json.RootElement.TryGetProperty("collectorsRun", out var value) ? value.GetInt32() : -1;
        return (ran, outcome.Success);
    }

    /// <summary>The distinct collector names this snapshot logged, ordered so the assertion is stable.</summary>
    private static async Task<List<string>> ReadLoggedCollectorsAsync(NpgsqlDataSource postgres, int serverId)
    {
        var names = new List<string>();
        await using var command = postgres.CreateCommand(
            "SELECT DISTINCT collector_name FROM collection_log WHERE server_id = $1 ORDER BY collector_name");
        command.Parameters.AddWithValue(serverId);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private static async Task DeleteCollectionLogAsync(
        NpgsqlConnection cleanup, CancellationToken cleanupCt, int serverId)
    {
        await using var command = new NpgsqlCommand(
            "DELETE FROM collection_log WHERE server_id = $1", cleanup);
        command.Parameters.AddWithValue(serverId);
        await command.ExecuteNonQueryAsync(cleanupCt);
    }
}
