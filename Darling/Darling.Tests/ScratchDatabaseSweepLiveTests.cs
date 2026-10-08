using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4981 against a real cluster: the start-of-run sweep drops a scratch database a killed run left behind, and
/// nothing else; and a scratch database that was never disposed does not outlive the process.
///
/// <para>The sweep cases plant databases with names that are exact factory names and names that only look like
/// them, one of each reason the sweep must refuse (too young, a client session connected, a different shape), runs
/// the sweep, and reads <c>pg_database</c> back. The sweep may also drop abandoned databases that other runs left on
/// the same cluster, which is its job, so the assertions are about the planted names and about every dropped name
/// being a factory name.</para>
/// </summary>
/* #1776 own-store: every database this class creates carries a name unique to the run, and the only rows it reads
   are the cluster's own database list, so it does not race the shared store's tables. */
public sealed class ScratchDatabaseSweepLiveTests
{
    private const string OldStamp = "20010101000000";

    [Fact]
    public async Task TheSweep_DropsAnOldIdleFactoryDatabase_AndLeavesEveryOtherNameAlone()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live scratch sweep test.");

        var ct = TestContext.Current.CancellationToken;
        var now = DateTime.UtcNow;
        /* 12 lowercase hex characters, fresh per call, so concurrent runs never collide on a name. */
        static string Id() => Guid.NewGuid().ToString("N")[..12];

        var abandoned = $"darling_scratch_{OldStamp}_{Id()}";
        var busy = $"darling_scratch_{OldStamp}_{Id()}";
        var young = $"darling_scratch_{now.AddMinutes(-5):yyyyMMddHHmmss}_{Id()}";
        var nearMisses = new[]
        {
            $"darling_scratch_{Id()}",                           // the earlier shape, no stamp
            $"darling_scratch_{OldStamp}_{Id()}0",               // 13 hex
            $"darling_scratch_{OldStamp}_{Id()[..11]}",          // 11 hex
            $"darling_scratch_{OldStamp}_ABCDEF{Id()[..6]}",     // uppercase hex
            $"darling_scratch_{OldStamp}_{Id()}_x",              // trailing extra
            $"darling_scratch_20011301000000_{Id()}",            // month 13
            $"xdarling_scratch_{OldStamp}_{Id()}",               // prefixed
            $"darlingtest_scratch_{OldStamp}_{Id()}",            // another family's prefix
        };

        var planted = new List<string> { abandoned, busy, young };
        planted.AddRange(nearMisses);

        /* The once-per-cluster sweep that ScratchPostgres.CreateAsync waits on runs from whichever test in this process
           creates a scratch database first, in parallel with this class. A database stamped 2001 with no session is
           exactly what that sweep drops, so planting one while it is still listing could lose it before the checks
           below run. Creating and disposing one scratch database here waits for that sweep to finish, and it never
           runs a second time in the process, so nothing planted below can be taken by it. */
        await (await ScratchPostgres.CreateAsync(baseConnectionString!, ct)).DisposeAsync();

        NpgsqlConnection? busySession = null;
        var bodySucceeded = false;
        try
        {
            /* #5549: unpooled, like every connection in this class that creates or drops a database. */
            await using (var admin = new NpgsqlConnection(ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString)))
            {
                await admin.OpenAsync(ct);

                /* A client session on an old database: a run that is still using it, whatever its name says. It is
                   connected the moment the database exists, because old with no session is what any sweep drops,
                   including one from another process on the same cluster, which this class cannot wait for. */
                await CreateDatabaseAsync(admin, busy, ct);
                var busyBuilder = new NpgsqlConnectionStringBuilder(baseConnectionString) { Database = busy, Pooling = false };
                busySession = new NpgsqlConnection(busyBuilder.ConnectionString);
                await busySession.OpenAsync(ct);

                foreach (var name in planted.Where(name => name != busy))
                {
                    await CreateDatabaseAsync(admin, name, ct);
                }
            }

            var log = new List<string>();
            var dropped = await ScratchDatabaseSweep.SweepAsync(
                baseConnectionString!, now, ScratchDatabaseSweep.AbandonedAfter, log.Add, ct);

            /* Nothing here depends on THIS sweep being the one that dropped the abandoned database: a sweep from another
               process on the same cluster may have done it first. What must hold either way is that it is gone, that
               every drop this sweep made was a factory name and was logged, and that it refused the rest. */
            Assert.All(dropped, name => Assert.True(ScratchDatabaseSweep.IsFactoryName(name), name));
            Assert.All(dropped, name => Assert.Contains(log, line => line.Contains(name, StringComparison.Ordinal)));
            Assert.DoesNotContain(busy, dropped);
            Assert.DoesNotContain(young, dropped);
            Assert.Empty(dropped.Intersect(nearMisses, StringComparer.Ordinal));

            var remaining = await ExistingAsync(baseConnectionString!, planted, ct);
            Assert.DoesNotContain(abandoned, remaining);
            Assert.Equal(
                planted.Where(name => name != abandoned).OrderBy(name => name, StringComparer.Ordinal),
                remaining.OrderBy(name => name, StringComparer.Ordinal));

            bodySucceeded = true;
        }
        finally
        {
            if (busySession is not null)
            {
                await busySession.DisposeAsync();
            }

            await LiveStoreCleanup.RunAsync(ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString!), bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var name in planted)
                {
                    /* No TimescaleDB job worker is left in the database the FORCE drop below kills (#5480). */
                    await ScratchPostgres.QuiesceTimescaleJobsAsync(baseConnectionString!, name);
                    await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)", cleanup);
                    await drop.ExecuteNonQueryAsync(cleanupCt);
                }
            });
        }
    }

    /// <summary>
    /// #5549: an abandoned database with TimescaleDB jobs is quiesced before the sweep drops it. A custom job that
    /// sleeps 8 seconds is running in the database when the sweep starts. The quiesce unschedules the jobs and waits for
    /// the running worker to leave (its cap is 10 seconds). TimescaleDB's scheduler ends an unscheduled job's running
    /// worker itself, a few seconds after the stop (faster on some machines), so a correct sweep can finish well before
    /// the job's 8 seconds are up. Without the quiesce, a plain <c>DROP DATABASE</c> does not wait for the worker:
    /// TimescaleDB ends it as part of the drop, which is the kill the quiesce exists to avoid, and the drop returns in a
    /// fraction of a second (0.1 to 0.3 seconds on a rig). The two differ in ORDER, not in elapsed time (a clock floor
    /// failed a correct 1.6 second sweep on CI): with the quiesce the worker is gone before the sweep's
    /// <c>DROP DATABASE</c> starts, without it the drop runs while the worker is alive. So a monitor session polls
    /// <c>pg_stat_activity</c> during the sweep and the test fails if any one look sees both at once. It also fails, rather
    /// than passing silently, if the monitor never saw the worker alive after the sweep began.
    ///</summary>
    [Fact]
    public async Task TheSweep_QuiescesAnAbandonedDatabasesTimescaleJobs_BeforeDroppingIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live scratch sweep test.");

        var ct = TestContext.Current.CancellationToken;
        var now = DateTime.UtcNow;
        var name = $"darling_scratch_{OldStamp}_{Guid.NewGuid().ToString("N")[..12]}";

        /* Same wait as the first sweep test: the once-per-cluster sweep must have finished before an old, idle database
           is planted, or it could be the one to drop it before the checks below run. */
        await (await ScratchPostgres.CreateAsync(baseConnectionString!, ct)).DisposeAsync();

        var bodySucceeded = false;
        var workerAge = 0.0;
        try
        {
            var unpooledAdmin = ScratchPostgres.UnpooledAdminConnectionString(baseConnectionString);
            await using var admin = new NpgsqlConnection(unpooledAdmin);
            await admin.OpenAsync(ct);
            await CreateDatabaseAsync(admin, name, ct);

            var inDatabase = new NpgsqlConnectionStringBuilder(baseConnectionString) { Database = name, Pooling = false }.ConnectionString;
            await using (var connection = new NpgsqlConnection(inDatabase))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                Assert.SkipUnless(await LiveTimescaleProbe.TryEnableAsync(inDatabase, ct),
                    "TimescaleDB is not available on this PostgreSQL, so there are no jobs for the sweep to quiesce.");

                /* The product's own policies, then one job that is still running when the sweep arrives. */
                await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
                foreach (var sql in new[]
                {
                    "CREATE PROCEDURE public.sweep_quiesce_slow_job(job_id int, config jsonb) LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(8); END $$",
                    "SELECT add_job('public.sweep_quiesce_slow_job', interval '1 hour', initial_start => now())",
                })
                {
                    await using var command = new NpgsqlCommand(sql, connection);
                    await command.ExecuteNonQueryAsync(ct);
                }
            }

            /* The slow job's worker (a "User-Defined Action", unlike the quick policy workers that run first) is attached to
               the database, and no client session is: that is what the sweep looks for. */
            var workers = 0L;
            for (var poll = 0; poll < 100 && workers == 0; poll++)
            {
                workers = await SlowJobWorkerCountAsync(admin, name, ct);
                if (workers == 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(100), ct);
                }
            }

            Assert.True(workers > 0, "the slow job never started, so this test would prove nothing about the quiesce.");
            for (var poll = 0; poll < 50 && await ClientSessionCountAsync(admin, name, ct) > 0; poll++)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(100), ct);
            }

            Assert.Equal(0L, await ClientSessionCountAsync(admin, name, ct));

            /* How long the worker has run, by the server's clock: it has 8 seconds minus this left. */
            await using (var age = new NpgsqlCommand(
                @"SELECT extract(epoch FROM now() - min(backend_start))::float8 FROM pg_stat_activity
                  WHERE datname = $1 AND backend_type LIKE 'User-Defined Action%'", admin))
            {
                age.Parameters.AddWithValue(name);
                workerAge = (double)(await age.ExecuteScalarAsync(ct))!;
            }

            Assert.True(workerAge < 5, $"the job worker is already {workerAge:F1} s into its 8 s sleep, too little left of it for the sweep to find the worker still alive.");

            var workerPid = await WorkerPidAsync(admin, name, ct);

            /* Watch the order, not the clock: a monitor session polls pg_stat_activity (about every 20 ms) for the whole
               sweep. Each look is one statement, so it sees the worker and the sweeper's DROP DATABASE together or not
               at all. A correct sweep never has both alive at one look: the worker is gone before the DROP starts. */
            var watch = new SweepWatch();
            using var watchStop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            await using var monitor = new NpgsqlConnection(unpooledAdmin);
            await monitor.OpenAsync(ct);
            var watcher = WatchAsync(monitor, workerPid, name, watch, watchStop.Token);

            var log = new List<string>();
            IReadOnlyList<string> dropped;
            try
            {
                watch.SweepStarted = true;
                dropped = await ScratchDatabaseSweep.SweepAsync(
                    baseConnectionString!, now, ScratchDatabaseSweep.AbandonedAfter, log.Add, ct);
            }
            finally
            {
                await watchStop.CancelAsync();
                await watcher;
            }

            Assert.Contains(name, dropped);
            Assert.True(watch.WorkerAliveAfterStart > 0,
                $"the monitor never saw the job worker (pid {workerPid}) alive after the sweep started ({watch.Looks} looks), so this run proves nothing about the quiesce: the scheduler ended the worker before the sweep began.");
            Assert.True(watch.Overlaps == 0,
                $"the sweep dropped the database while the job worker (pid {workerPid}) was still alive, at {watch.Overlaps} of {watch.Looks} looks: it did not wait the worker out.");
            Assert.Contains(log, line => line.Contains(name, StringComparison.Ordinal) && line.StartsWith("Dropped", StringComparison.Ordinal));
            Assert.Empty(await ExistingAsync(baseConnectionString!, new[] { name }, ct));
            Assert.Equal(0L, await ScratchPostgres.JobWorkerCountAsync(admin, name, ct));
            bodySucceeded = true;
        }
        finally
        {
            await DropIfStillThereAsync(baseConnectionString!, name, bodySucceeded);
        }
    }

    /// <summary>What the monitor session saw while the sweep ran. The test sets <see cref="SweepStarted"/>; only the watcher writes the counts.</summary>
    private sealed class SweepWatch
    {
        public volatile bool SweepStarted;
        public int Looks;
        public int WorkerAliveAfterStart;
        public int Overlaps;
    }

    private static async Task<int> WorkerPidAsync(NpgsqlConnection admin, string name, CancellationToken ct)
    {
        await using var pid = new NpgsqlCommand(
            "SELECT min(pid) FROM pg_stat_activity WHERE datname = $1 AND backend_type LIKE 'User-Defined Action%'", admin);
        pid.Parameters.AddWithValue(name);
        return await pid.ExecuteScalarAsync(ct) is int found ? found : 0;
    }

    /// <summary>
    /// Polls until <paramref name="stop"/> fires. One statement per look, so both facts come from one snapshot: the job
    /// worker with this pid is alive, and another backend is running the sweep's <c>DROP DATABASE</c> for this name.
    /// </summary>
    private static async Task WatchAsync(NpgsqlConnection monitor, int workerPid, string name, SweepWatch watch, CancellationToken stop)
    {
        /* The pattern is written DROP%DATABASE% so the drop-site census (UnpooledDropConnectionCensusTests) does not take this look-up for a drop. */
        const string Look = @"SELECT
              EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND backend_type LIKE 'User-Defined Action%'),
              EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND state = 'active'
                      AND query LIKE 'DROP%DATABASE%' AND strpos(query, $2) > 0)";
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var started = watch.SweepStarted;
                await using (var look = new NpgsqlCommand(Look, monitor))
                {
                    look.Parameters.AddWithValue(workerPid);
                    look.Parameters.AddWithValue(name);
                    await using var reader = await look.ExecuteReaderAsync(stop);
                    await reader.ReadAsync(stop);
                    var workerAlive = reader.GetBoolean(0);
                    var dropRunning = reader.GetBoolean(1);
                    watch.Looks++;
                    if (started && workerAlive)
                    {
                        watch.WorkerAliveAfterStart++;
                    }

                    if (workerAlive && dropRunning)
                    {
                        watch.Overlaps++;
                    }
                }

                /* No pause between looks: without the quiesce, the stretch where the DROP is running and the worker is not yet
                   gone lasts only the milliseconds TimescaleDB takes to end the worker, and a 20 ms pause missed it. */
                await Task.Yield();
            }
        }
        catch (OperationCanceledException)
        {
            /* The sweep returned; the last look was cut short. */
        }
    }

    private static async Task<long> SlowJobWorkerCountAsync(NpgsqlConnection admin, string name, CancellationToken ct)
    {
        await using var count = new NpgsqlCommand(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND backend_type LIKE 'User-Defined Action%'", admin);
        count.Parameters.AddWithValue(name);
        return (long)(await count.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> ClientSessionCountAsync(NpgsqlConnection admin, string name, CancellationToken ct)
    {
        await using var count = new NpgsqlCommand(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = $1 AND backend_type = 'client backend'", admin);
        count.Parameters.AddWithValue(name);
        return (long)(await count.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task ADisposedScratchDatabase_IsGone_AndNoLongerRemembered()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live scratch sweep test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var name = scratch.DatabaseName;
        var bodySucceeded = false;
        try
        {
            Assert.True(ScratchDatabaseSweep.IsFactoryName(name), name);
            Assert.True(ScratchPostgres.IsRemembered(name));
            Assert.Single(await ExistingAsync(baseConnectionString!, new[] { name }, ct));

            await scratch.DisposeAsync();

            Assert.Empty(await ExistingAsync(baseConnectionString!, new[] { name }, ct));
            Assert.False(ScratchPostgres.IsRemembered(name));
            bodySucceeded = true;
        }
        finally
        {
            await DropIfStillThereAsync(baseConnectionString!, name, bodySucceeded);
        }
    }

    /// <summary>
    /// A test that fails before it can dispose, or a setup helper that throws between the create and the return, leaves
    /// its database undisposed. The process-exit drain exists for exactly that; this leaves one undisposed on purpose
    /// and runs the drain's own drop for just that name (the drain itself would also drop the databases other tests,
    /// running in parallel, still use).
    /// </summary>
    [Fact]
    public async Task ADatabaseNobodyDisposed_IsDroppedByTheDrain_AndForgottenAfterwards()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live scratch sweep test.");

        var ct = TestContext.Current.CancellationToken;
        var leaked = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);   // deliberately never disposed
        var name = leaked.DatabaseName;
        var bodySucceeded = false;
        try
        {
            Assert.True(ScratchPostgres.IsRemembered(name), "a database nobody disposed must still be queued for the drain");
            Assert.Single(await ExistingAsync(baseConnectionString!, new[] { name }, ct));

            await ScratchPostgres.DropRememberedAsync(name);

            Assert.Empty(await ExistingAsync(baseConnectionString!, new[] { name }, ct));
            Assert.False(ScratchPostgres.IsRemembered(name));
            bodySucceeded = true;
        }
        finally
        {
            await DropIfStillThereAsync(baseConnectionString!, name, bodySucceeded);
        }
    }

    /* The names are built in this class from digits and lowercase hex (or are deliberate near misses of that shape),
       never from input, so they are safe as quoted identifiers. */
    private static async Task CreateDatabaseAsync(NpgsqlConnection admin, string name, CancellationToken ct)
    {
        await using var create = new NpgsqlCommand($"CREATE DATABASE \"{name}\"", admin);
        await create.ExecuteNonQueryAsync(ct);
    }

    private static async Task<List<string>> ExistingAsync(string connectionString, IEnumerable<string> names, CancellationToken ct)
    {
        var found = new List<string>();
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand("SELECT datname FROM pg_database WHERE datname = ANY($1)", connection);
        command.Parameters.AddWithValue(names.ToArray());
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            found.Add(reader.GetString(0));
        }

        return found;
    }

    private static Task DropIfStillThereAsync(string connectionString, string name, bool bodySucceeded) =>
        LiveStoreCleanup.RunAsync(ScratchPostgres.UnpooledAdminConnectionString(connectionString), bodySucceeded, async (cleanup, cleanupCt) =>
        {
            /* No TimescaleDB job worker is left in the database the FORCE drop below kills (#5480). */
            await ScratchPostgres.QuiesceTimescaleJobsAsync(connectionString, name);
            await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)", cleanup);
            await drop.ExecuteNonQueryAsync(cleanupCt);
        });
}
