using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Mints an isolated scratch DATABASE on the DARLING_TEST_PG server for tests that seed the
/// singleton config rows. <c>StoreConfigProvider.SeedIfEmptyAsync</c> deliberately no-ops on a
/// store any earlier test already seeded, so running these tests against the one shared CI
/// database made them order-dependent (caught live: the V20 round-trip read the V3 defaults an
/// earlier test seeded). Each such test gets its own database instead — created here, dropped on
/// dispose with <c>WITH (FORCE)</c> so a lingering connection can never wedge the drop.
/// <c>Pooling=false</c> on the scratch connection string keeps in-process pooled connections from
/// pinning the database in the first place.
///
/// <para><b>A scratch database must not outlive its run (#4981).</b> Each one that has the TimescaleDB extension
/// holds a background-worker slot for its scheduler, and a long-lived local cluster has only a few, so databases
/// left behind eventually fail unrelated tests with "limit of 8 exceeded". Three layers keep that from happening.
/// <list type="number">
/// <item>The drop on dispose is retried and a failure is reported, no longer swallowed.</item>
/// <item>Every database this process creates is remembered until its drop succeeds. Whatever is still remembered
/// when the process exits (a test that failed before it could dispose, a drop that kept failing) is dropped then,
/// and named on stderr together with the test that created it.</item>
/// <item>The first create against a cluster sweeps that cluster for databases a killed run left behind
/// (<see cref="ScratchDatabaseSweep"/>), because a killed process runs neither of the layers above. The sweep runs on
/// whatever cluster <c>DARLING_TEST_PG</c> names, and that is safe because it drops only a database whose name matches
/// the factory's anchored pattern, that is at least 30 minutes old and idle, and that is not its own, with a plain
/// <c>DROP DATABASE</c>.</item>
/// </list></para>
/// </summary>
internal sealed class ScratchPostgres : IAsyncDisposable
{
    private readonly string _adminConnectionString;

    public string DatabaseName { get; }

    public string ConnectionString { get; }

    /// <summary>
    /// The same store WITHOUT the session time-zone pin, so a session opened through it reports the database's own
    /// default zone. It exists for tests that must observe that default (e.g. to prove the product pin overrides it).
    /// </summary>
    public string UnpinnedConnectionString { get; }

    private ScratchPostgres(string adminConnectionString, string databaseName, string connectionString, string unpinnedConnectionString)
    {
        UnpinnedConnectionString = unpinnedConnectionString;
        _adminConnectionString = adminConnectionString;
        DatabaseName = databaseName;
        ConnectionString = connectionString;
    }

    /// <summary>
    /// Every database this process created and has not yet dropped: name to the connection string that drops it, and
    /// the test that created it. An entry leaves only when its drop succeeds.
    /// </summary>
    private static readonly ConcurrentDictionary<string, (string AdminConnectionString, string Creator)> Remembered = new();

    /// <summary>One start-of-run sweep per cluster per process, keyed by host and port.</summary>
    private static readonly ConcurrentDictionary<string, Lazy<Task>> Sweeps = new();

    private static int _exitDrainHooked;

    /// <summary>Drop attempts on dispose: a transient refusal (a session that has not finished closing) should not leak.</summary>
    private const int DropAttempts = 3;

    public static async Task<ScratchPostgres> CreateAsync(string baseConnectionString, CancellationToken cancellationToken)
    {
        HookExitDrain();
        await SweepOncePerClusterAsync(baseConnectionString, cancellationToken);

        /* The factory's pattern (prefix, creation stamp, 12 hex): safe to interpolate as a quoted identifier. */
        var databaseName = ScratchDatabaseSweep.NewName(DateTime.UtcNow);

        /* Remembered BEFORE the create, so a failure or cancellation anywhere after it still has a drop queued; the
           exit drain uses IF EXISTS, so a name whose create never ran costs nothing. */
        Remembered[databaseName] = (baseConnectionString, CurrentTestName());

        try
        {
            await using var admin = new NpgsqlConnection(baseConnectionString);
            await admin.OpenAsync(cancellationToken);
            await using var create = new NpgsqlCommand($"CREATE DATABASE \"{databaseName}\"", admin);
            await create.ExecuteNonQueryAsync(cancellationToken);
        }
        catch
        {
            await TryDropAsync(baseConnectionString, databaseName);
            throw;
        }

        var builder = new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Database = databaseName,
            Pooling = false,
        };

        /* The base string can already carry the session pin (LiveStoreSessionTimeZone adds it to DARLING_TEST_PG),
           so the unpinned string drops it. */
        builder.Remove("Timezone");

        /* Pinned to UTC the way every product store connection is, so a cluster whose default zone is behind UTC
           cannot skew timestamp round trips (a chunk's range_end read back through a ::timestamp cast). */
        return new ScratchPostgres(
            baseConnectionString,
            databaseName,
            PerformanceMonitor.Darling.Storage.DarlingStoreConnection.PinSessionTimeZoneUtc(builder.ConnectionString),
            builder.ConnectionString);
    }

    /// <summary>
    /// Drops the database, retrying a transient refusal. A drop that still fails is reported and the database stays
    /// remembered for the exit drain; it does not fail the test, because failing a passing test in its cleanup would
    /// invert the signal, and the layers behind this one still remove it.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        for (var attempt = 1; attempt <= DropAttempts; attempt++)
        {
            try
            {
                await DropAsync(_adminConnectionString, DatabaseName);
                return;
            }
            catch (Exception ex) when (attempt < DropAttempts)
            {
                Report($"Scratch database {DatabaseName}: drop attempt {attempt} of {DropAttempts} failed ({ex.GetType().Name}: {ex.Message}); retrying.");
                await Task.Delay(TimeSpan.FromMilliseconds(250 * attempt));
            }
            catch (Exception ex)
            {
                Report($"Scratch database {DatabaseName}: drop failed after {DropAttempts} attempts ({ex.GetType().Name}: {ex.Message}); it stays queued for the exit drain.");
            }
        }
    }

    /// <summary>True while this process still has a drop pending for <paramref name="databaseName"/>.</summary>
    internal static bool IsRemembered(string databaseName) => Remembered.ContainsKey(databaseName);

    /// <summary>
    /// Drops the named databases if this process still remembers them, as the exit drain does for everything left.
    /// Exposed by name so a test can prove the drain on a database it leaked on purpose without dropping the databases
    /// that other tests, running in parallel, are still using.
    /// </summary>
    internal static async Task DropRememberedAsync(params string[] databaseNames)
    {
        foreach (var name in databaseNames)
        {
            if (Remembered.TryGetValue(name, out var entry))
            {
                await DropAsync(entry.AdminConnectionString, name);
            }
        }
    }

    /// <summary>Total time <see cref="QuiesceTimescaleJobsAsync"/> may spend before it lets the drop go ahead.</summary>
    internal static readonly TimeSpan QuiesceCap = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Unschedules every TimescaleDB job in <paramref name="databaseName"/>, then waits (up to
    /// <see cref="QuiesceCap"/>) for the job workers already running there to finish, so the
    /// <c>DROP DATABASE ... WITH (FORCE)</c> that follows does not SIGTERM a policy worker mid-run. CI saw the
    /// PostgreSQL postmaster log "terminated by exception 0xC0000005" for a TimescaleDB worker, twice, both times
    /// with a scratch-database FORCE drop in flight (PR #5480); no worker is running in the dropped database
    /// once this returns, so the drop has nothing to kill.
    /// <para>Best-effort and bounded: it never throws, never fails a test, and never skips the drop. A caller's
    /// <paramref name="cancellationToken"/> ends it early too, without a message (the exit drain passes its deadline). A database
    /// without the extension, or one already gone, returns at once. It does NOT call
    /// <c>_timescaledb_functions.stop_background_workers()</c>, which SIGTERMs running jobs, the very kill this
    /// avoids.</para>
    /// </summary>
    internal static async Task QuiesceTimescaleJobsAsync(string adminConnectionString, string databaseName, CancellationToken cancellationToken = default)
    {
        using var cap = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cap.CancelAfter(QuiesceCap);
        try
        {
            var scratchBuilder = new NpgsqlConnectionStringBuilder(adminConnectionString)
            {
                Database = databaseName,
                Pooling = false,
                Timeout = 5,
            };
            var adminBuilder = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false, Timeout = 5 };

            await using (var scratch = new NpgsqlConnection(scratchBuilder.ConnectionString))
            {
                await scratch.OpenAsync(cap.Token);
                await using var probe = new NpgsqlCommand("SELECT to_regclass('_timescaledb_config.bgw_job') IS NOT NULL", scratch);
                if (!(bool)(await probe.ExecuteScalarAsync(cap.Token))!)
                {
                    return;
                }

                await using var unschedule = new NpgsqlCommand(
                    "SELECT alter_job(job_id, scheduled => false) FROM timescaledb_information.jobs", scratch);
                await unschedule.ExecuteNonQueryAsync(cap.Token);
            }

            await using var admin = new NpgsqlConnection(adminBuilder.ConnectionString);
            await admin.OpenAsync(cap.Token);
            /* Two empty polls in a row: a worker the scheduler registered just before the unschedule can still be
               starting up and not yet show in pg_stat_activity on the first look. */
            var emptyPolls = 0;
            while (!cap.IsCancellationRequested)
            {
                emptyPolls = await JobWorkerCountAsync(admin, databaseName, cap.Token) == 0 ? emptyPolls + 1 : 0;
                if (emptyPolls >= 2)
                {
                    return;
                }

                await Task.Delay(TimeSpan.FromMilliseconds(100), cap.Token);
            }
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InvalidCatalogName)
        {
            /* The database is already gone: nothing to quiesce, and the drop below is IF EXISTS. */
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* The caller's own deadline (the exit drain's), not the cap: the caller decides whether the drop still runs. */
        }
        catch (Exception ex)
        {
            /* The cap, a missing database or a refused connect all land here. The drop that follows is the
               test's cleanup and it still runs; the retries behind it are unchanged. */
            Console.Error.WriteLine($"Scratch database {databaseName}: TimescaleDB jobs not quiesced before the drop ({ex.GetType().Name}: {ex.Message}).");
        }
    }

    /// <summary>
    /// How many TimescaleDB job workers (and any other non-client, non-scheduler, non-autovacuum backend) are
    /// attached to <paramref name="databaseName"/>. Job workers show up as e.g. <c>Retention Policy [1038]</c>.
    /// </summary>
    internal static async Task<long> JobWorkerCountAsync(NpgsqlConnection admin, string databaseName, CancellationToken cancellationToken)
    {
        await using var count = new NpgsqlCommand(
            @"SELECT count(*) FROM pg_stat_activity
              WHERE datname = $1
                AND backend_type NOT IN ('client backend', 'TimescaleDB Background Worker Scheduler', 'autovacuum worker')",
            admin);
        count.Parameters.AddWithValue(databaseName);
        return (long)(await count.ExecuteScalarAsync(cancellationToken))!;
    }

    private static async Task DropAsync(string adminConnectionString, string databaseName)
    {
        await QuiesceTimescaleJobsAsync(adminConnectionString, databaseName);
        await using var admin = new NpgsqlConnection(adminConnectionString);
        await admin.OpenAsync();
        await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{databaseName}\" WITH (FORCE)", admin);
        await drop.ExecuteNonQueryAsync();
        Remembered.TryRemove(databaseName, out _);
    }

    private static async Task TryDropAsync(string adminConnectionString, string databaseName)
    {
        try
        {
            await DropAsync(adminConnectionString, databaseName);
        }
        catch
        {
            /* The create itself failed, so the original exception is the story; the exit drain retries this drop. */
        }
    }

    /// <summary>
    /// Subscribes the exit drain once. A process-exit handler is the one place that runs for a failed test, a test
    /// that never disposed, and an interrupted run alike; it cannot run for a killed process, which is what the
    /// start-of-run sweep is for.
    /// </summary>
    private static void HookExitDrain()
    {
        if (Interlocked.Exchange(ref _exitDrainHooked, 1) == 0)
        {
            AppDomain.CurrentDomain.ProcessExit += (_, _) => DropEverythingRemembered();
        }
    }

    private static void DropEverythingRemembered()
    {
        var pending = new List<RememberedDatabase>();
        foreach (var (name, entry) in Remembered.ToArray())
        {
            pending.Add(new RememberedDatabase(name, entry.AdminConnectionString, entry.Creator));
        }

        DropRememberedAtExit(pending);
    }

    /// <summary>One database the exit drain was asked to drop: its name, the connection string that drops it, and the test that created it.</summary>
    internal readonly record struct RememberedDatabase(string Name, string AdminConnectionString, string Creator);

    /// <summary>
    /// What an exit drain did: connections it tried to open, databases it dropped, drops that failed, databases it left
    /// alone because their cluster did not answer, and databases it had not finished when its deadline passed.
    /// </summary>
    internal readonly record struct ExitDrainOutcome(int ConnectAttempts, int Dropped, int Failed, int Skipped, int Unfinished);

    /// <summary>
    /// How long the exit drain waits to connect to a cluster, in seconds. The default is 15, and the drain ran once per
    /// remembered database, so a cluster stopped before the process exited cost 15 seconds for each database the run
    /// still held (#4981).
    /// </summary>
    internal const int ExitDrainConnectTimeoutSeconds = 3;

    /// <summary>
    /// How long one drop at exit may run, in seconds. This stays at the library default on purpose: a dead cluster
    /// never gets as far as a command (its connect fails first), and a drop on a live cluster ends with an immediate
    /// checkpoint that took 11 seconds on a loaded local cluster, so a shorter limit cancelled a drop that was working
    /// and left the very database the drain exists to remove.
    /// </summary>
    internal const int ExitDrainCommandTimeoutSeconds = 30;

    /// <summary>The admin connection string with the exit drain's own connect and command timeouts, and no pooling: one connection per drop.</summary>
    internal static string ExitDrainConnectionString(string adminConnectionString) =>
        new NpgsqlConnectionStringBuilder(adminConnectionString)
        {
            Timeout = ExitDrainConnectTimeoutSeconds,
            CommandTimeout = ExitDrainCommandTimeoutSeconds,
            Pooling = false,
        }.ConnectionString;

    /// <summary>
    /// How long the whole exit drain may take, in seconds, in-flight drops included. The drain is a ProcessExit handler,
    /// and xUnit v3 under Microsoft.Testing.Platform gives that stage 11 seconds after the run returns (one second, then
    /// its <c>shutdownForegroundThreadWaitSeconds</c> default of 10) before it prints "[FATAL ERROR] Foreground threads
    /// were left running, forcing process exit" and exits with code 1. No thread was left running: the 2026-10-08
    /// nightly passed every test and still failed because this drain, then dropping 39 leaked databases one at a time
    /// with a TimescaleDB quiesce before each (#5480), was still at work when that window closed. The drain returns when
    /// this passes even with a drop in flight (one DROP DATABASE waited 26 seconds for its checkpoint on a local rig), and
    /// names every database it did not finish; the next run's start-of-run sweep removes those, or they go with the
    /// throwaway cluster. Tests that mint a database must still drop it themselves (see
    /// <c>ScratchDatabaseDisposalCensusTests</c>); this budget only keeps a leak from failing a run.
    /// </summary>
    internal const int ExitDrainBudgetSeconds = 6;

    /// <summary>How many databases the exit drain quiesces and drops at once. Each holds at most two connections at a time.</summary>
    internal const int ExitDrainParallelism = 16;

    /// <summary>
    /// The exit drain's body, taking its list as a parameter so a test can run it without a process exit and without
    /// touching the databases that other tests, running in parallel, still hold.
    /// </summary>
    /// <remarks>
    /// <para>Clusters drain side by side, and within a live cluster up to <see cref="ExitDrainParallelism"/> databases
    /// drop at once, all under one <paramref name="budget"/> (default <see cref="ExitDrainBudgetSeconds"/> seconds).
    /// When it passes, the drain returns and names every
    /// database it did not finish. A drop already sent is not cancelled (an interrupted DROP DATABASE can leave an
    /// invalid database behind), but no new connect, quiesce or drop starts.</para>
    /// <para>The first database on each cluster goes alone and probes it. A connect that fails without the server
    /// answering (a stopped cluster, a refused or timed-out connection) marks the cluster dead, so N databases on it
    /// cost one short wait, not N, and the rest are skipped by name. A server that answers with an error (a bad
    /// password, a missing database) is not a dead cluster, and a failed drop on a live cluster never stops the
    /// others.</para>
    /// </remarks>
    internal static ExitDrainOutcome DropRememberedAtExit(IEnumerable<RememberedDatabase> remembered, TimeSpan? budget = null)
    {
        var deadline = budget ?? TimeSpan.FromSeconds(ExitDrainBudgetSeconds);
        var databases = remembered.ToList();
        var drain = new ExitDrain(databases.Count);
        using var stop = new CancellationTokenSource(deadline);
        using var gate = new SemaphoreSlim(ExitDrainParallelism);

        var clusters = databases
            .Select((db, index) => (Db: db, Index: index))
            .GroupBy(entry => ClusterKeyOrOwn(entry.Db), StringComparer.Ordinal)
            .Select(cluster => Task.Run(() => DrainClusterAsync(cluster.ToList(), drain, gate, stop.Token)))
            .ToArray();

        try
        {
            Task.WaitAll(clusters, deadline);
        }
        catch (AggregateException)
        {
            /* Every per-database step catches its own failure; nothing here should throw, and nothing may stop the exit. */
        }

        /* One snapshot: a drop sent before the deadline can still finish after this line, and the counts and the names
           printed below must agree. */
        var states = drain.Snapshot();
        for (var i = 0; i < databases.Count; i++)
        {
            if (states[i] == ExitDrain.Unsettled)
            {
                var db = databases[i];
                Console.Error.WriteLine($"Scratch database {db.Name} outlived its test ({db.Creator}) and was not dropped within the exit drain's {deadline.TotalSeconds:0.#}-second limit; the next run's start-of-run sweep removes it.");
            }
        }

        return new ExitDrainOutcome(
            Volatile.Read(ref drain.ConnectAttempts),
            states.Count(state => state == ExitDrain.Dropped),
            states.Count(state => state == ExitDrain.Failed),
            states.Count(state => state == ExitDrain.Skipped),
            states.Count(state => state == ExitDrain.Unsettled));
    }

    /// <summary>The database's cluster by host and port, or a key of its own when its connection string does not parse, so it fails alone.</summary>
    private static string ClusterKeyOrOwn(RememberedDatabase db)
    {
        try
        {
            return ClusterKey(db.AdminConnectionString);
        }
        catch (Exception)
        {
            return "unparsed:" + db.Name;
        }
    }

    private static async Task DrainClusterAsync(List<(RememberedDatabase Db, int Index)> cluster, ExitDrain drain, SemaphoreSlim gate, CancellationToken stop)
    {
        if (await DropOneAtExitAsync(cluster[0].Db, cluster[0].Index, drain, gate, stop) == ExitDrop.ClusterDead)
        {
            foreach (var (db, index) in cluster.Skip(1))
            {
                drain.Settle(index, ExitDrain.Skipped);
                Console.Error.WriteLine($"Scratch database {db.Name} outlived its test ({db.Creator}) and was not dropped at exit: its cluster did not answer an earlier connect.");
            }

            return;
        }

        await Task.WhenAll(cluster.Skip(1).Select(entry => DropOneAtExitAsync(entry.Db, entry.Index, drain, gate, stop)));
    }

    private static async Task<ExitDrop> DropOneAtExitAsync(RememberedDatabase db, int index, ExitDrain drain, SemaphoreSlim gate, CancellationToken stop)
    {
        try
        {
            await gate.WaitAsync(stop);
        }
        catch (OperationCanceledException)
        {
            return ExitDrop.OutOfTime;
        }

        try
        {
            Interlocked.Increment(ref drain.ConnectAttempts);
            var connectionString = ExitDrainConnectionString(db.AdminConnectionString);
            await using var admin = new NpgsqlConnection(connectionString);
            try
            {
                await admin.OpenAsync(stop);
            }
            catch (OperationCanceledException) when (stop.IsCancellationRequested)
            {
                return ExitDrop.OutOfTime;
            }
            catch (Exception ex) when (ex is not PostgresException)
            {
                drain.Settle(index, ExitDrain.Failed);
                Console.Error.WriteLine($"Scratch database {db.Name} outlived its test ({db.Creator}) and could not be dropped at exit: {ex.Message}");
                return ExitDrop.ClusterDead;
            }

            /* Bounded by its own cap and by the drain's deadline, and it never throws. */
            await QuiesceTimescaleJobsAsync(connectionString, db.Name, stop);
            if (stop.IsCancellationRequested)
            {
                return ExitDrop.OutOfTime;
            }

            await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{db.Name}\" WITH (FORCE)", admin);
            await drop.ExecuteNonQueryAsync(CancellationToken.None);
            Remembered.TryRemove(db.Name, out _);
            drain.Settle(index, ExitDrain.Dropped);
            Console.Error.WriteLine($"Scratch database {db.Name} outlived its test ({db.Creator}); dropped at process exit.");
            return ExitDrop.Done;
        }
        catch (Exception ex)
        {
            drain.Settle(index, ExitDrain.Failed);
            Console.Error.WriteLine($"Scratch database {db.Name} outlived its test ({db.Creator}) and could not be dropped at exit: {ex.Message}");
            return ExitDrop.Done;
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>How one database's exit drop ended, as far as the rest of its cluster is concerned.</summary>
    private enum ExitDrop
    {
        Done,
        ClusterDead,
        OutOfTime,
    }

    /// <summary>Each database's outcome in the drain, set once, and the connect count, safe to update from every drop at once.</summary>
    private sealed class ExitDrain(int count)
    {
        public const int Unsettled = 0;
        public const int Dropped = 1;
        public const int Failed = 2;
        public const int Skipped = 3;

        private readonly int[] _states = new int[count];

        public int ConnectAttempts;

        public void Settle(int index, int state) => Interlocked.CompareExchange(ref _states[index], state, Unsettled);

        public int[] Snapshot()
        {
            var copy = new int[_states.Length];
            for (var i = 0; i < copy.Length; i++)
            {
                copy[i] = Volatile.Read(ref _states[i]);
            }

            return copy;
        }
    }

    /// <summary>The cluster a connection string names, by host and port: what the start-of-run sweep and the exit drain each treat as one cluster.</summary>
    private static string ClusterKey(string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        return $"{builder.Host}:{builder.Port}".ToUpperInvariant();
    }

    /// <summary>
    /// The first create against a cluster sweeps it once. Every later caller waits on the same task, so no scratch
    /// database is created mid-sweep, but a failed sweep never fails a test: it only means debris stays for the next run.
    /// </summary>
    private static async Task SweepOncePerClusterAsync(string baseConnectionString, CancellationToken cancellationToken)
    {
        var key = ClusterKey(baseConnectionString);
        var sweep = Sweeps.GetOrAdd(key, _ => new Lazy<Task>(() => RunSweepAsync(baseConnectionString)));
        await sweep.Value.WaitAsync(cancellationToken);
    }

    private static async Task RunSweepAsync(string baseConnectionString)
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));
            await ScratchDatabaseSweep.SweepAsync(
                baseConnectionString, DateTime.UtcNow, ScratchDatabaseSweep.AbandonedAfter, Report, timeout.Token);
        }
        catch (Exception ex)
        {
            Report($"The start-of-run sweep for abandoned scratch databases failed and was skipped: {ex.Message}");
        }
    }

    private static string CurrentTestName()
    {
        try
        {
            return TestContext.Current.Test?.TestDisplayName ?? "no test context";
        }
        catch
        {
            return "no test context";
        }
    }

    private static void Report(string message)
    {
        try
        {
            TestContext.Current.SendDiagnosticMessage(message);
        }
        catch
        {
            /* No test context to report to; the text is advisory. */
        }

        Console.Error.WriteLine(message);
    }
}
