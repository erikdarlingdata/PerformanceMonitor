using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4981: the exit drain must not stall the end of a test run when the cluster it was asked to clean is gone. It is
/// the last thing a run does, so with the default 15-second connect wait per remembered database, a cluster that was
/// stopped before the process exited cost 15 seconds for each database the run still held.
///
/// <para>The drain body takes its list as a parameter, so none of these tests needs a process exit, and none
/// touches the databases that other tests in the same process are still using.</para>
/// </summary>
/* #1776 own-store: the dead-cluster cases never connect to a store, and the live case creates one scratch database
   through ScratchPostgres, so it does not read or write the shared store's tables. */
[Collection("timing")]
public sealed class ScratchPostgresExitDrainTests
{
    /// <summary>A port nothing listens on: bind to port 0, read the port the OS chose, release it.</summary>
    private static int ClosedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    private static string StoppedClusterConnectionString(int port) =>
        new NpgsqlConnectionStringBuilder
        {
            Host = "127.0.0.1",
            Port = port,
            Username = "darling",
            Database = "postgres",
            Pooling = false,
        }.ConnectionString;

    private static ScratchPostgres.RememberedDatabase Remembered(string connectionString, string creator) =>
        new($"darling_scratch_20010101000000_{Guid.NewGuid():N}"[..43], connectionString, creator);

    /// <summary>
    /// A cluster that hangs: a listener that never accepts, so the OS completes each TCP handshake and the startup
    /// message then waits for an answer that never comes, until the drain's connect timeout. Stop it when done.
    /// </summary>
    private static (TcpListener Listener, string ConnectionString) HangingCluster()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return (listener, StoppedClusterConnectionString(((IPEndPoint)listener.LocalEndpoint).Port));
    }

    /// <summary>
    /// The 2026-10-08 nightly passed all 28,763 tests and still failed with "[FATAL ERROR] Foreground threads were
    /// left running": this drain, a ProcessExit handler, was still dropping 39 leaked databases one at a time when
    /// xUnit's 11-second end-of-run window closed. Four clusters that each hang for the full connect timeout cost
    /// four timeouts in a row on the old drain (about 12 seconds, past that window) and one when they drain side by side.
    /// </summary>
    [Fact]
    public void TheDrain_OfSeveralHangingClusters_WaitsOutTheirConnectsSideBySide()
    {
        var clusters = new[] { HangingCluster(), HangingCluster(), HangingCluster(), HangingCluster() };
        try
        {
            var databases = Array.ConvertAll(clusters, cluster => Remembered(cluster.ConnectionString, "a hanging one"));

            var clock = Stopwatch.StartNew();
            var outcome = ScratchPostgres.DropRememberedAtExit(databases);
            clock.Stop();

            Assert.True(
                clock.Elapsed < TimeSpan.FromSeconds(ScratchPostgres.ExitDrainConnectTimeoutSeconds * 2),
                $"the drain of 4 hanging clusters took {clock.Elapsed.TotalSeconds:0.0} seconds; one at a time costs 4 connect timeouts of {ScratchPostgres.ExitDrainConnectTimeoutSeconds} seconds");
            Assert.Equal(4, outcome.ConnectAttempts);
            Assert.Equal(4, outcome.Failed);
            Assert.Equal(0, outcome.Dropped);
        }
        finally
        {
            foreach (var cluster in clusters)
            {
                cluster.Listener.Stop();
            }
        }
    }

    /// <summary>
    /// Whatever the clusters do, the drain returns by its deadline and names each database it did not finish, so a
    /// leak, a slow drop or a hung cluster can never again run the exit past xUnit's window and fail a green run.
    /// </summary>
    [Fact]
    public void TheDrain_ReturnsAtItsDeadline_AndCountsWhatItDidNotFinish()
    {
        var (listener, hanging) = HangingCluster();
        try
        {
            var deadline = TimeSpan.FromSeconds(1);
            var clock = Stopwatch.StartNew();
            var outcome = ScratchPostgres.DropRememberedAtExit(
                new[] { Remembered(hanging, "hung one"), Remembered(hanging, "hung two"), Remembered(hanging, "hung three") },
                deadline);
            clock.Stop();

            /* Under the 3-second connect timeout, so it was the deadline, not the timeout, that ended the drain. */
            Assert.True(
                clock.Elapsed < TimeSpan.FromSeconds(ScratchPostgres.ExitDrainConnectTimeoutSeconds) - TimeSpan.FromMilliseconds(500),
                $"the drain took {clock.Elapsed.TotalSeconds:0.0} seconds against a {deadline.TotalSeconds:0}-second deadline");
            Assert.Equal(3, outcome.Unfinished);
            Assert.Equal(0, outcome.Dropped);
            Assert.Equal(0, outcome.Failed);
            Assert.Equal(0, outcome.Skipped);
        }
        finally
        {
            listener.Stop();
        }
    }

    [Fact]
    public void TheDrain_OfThreeDatabases_OnAStoppedCluster_TriesTheClusterOnce_AndEndsQuickly()
    {
        var stopped = StoppedClusterConnectionString(ClosedPort());
        var databases = new[] { Remembered(stopped, "test one"), Remembered(stopped, "test two"), Remembered(stopped, "test three") };

        var clock = Stopwatch.StartNew();
        var outcome = ScratchPostgres.DropRememberedAtExit(databases);
        clock.Stop();

        /* One short wait, not three. A refused connect on Windows takes about 2 seconds, so the old drain took about 6
           here (and 45 where the connect waited out its default), against one attempt capped at the 3-second timeout. */
        Assert.True(
            clock.Elapsed < TimeSpan.FromSeconds(ScratchPostgres.ExitDrainConnectTimeoutSeconds + 2),
            $"the drain of 3 databases on a stopped cluster took {clock.Elapsed.TotalSeconds:0.0} seconds");

        /* Three databases on one dead cluster cost one failed connect, not three: the other two are named and left. */
        Assert.Equal(1, outcome.ConnectAttempts);
        Assert.Equal(1, outcome.Failed);
        Assert.Equal(2, outcome.Skipped);
        Assert.Equal(0, outcome.Dropped);
    }

    [Fact]
    public void TheDrain_OutOfBudget_StartsNoDrop_AndNamesEveryDatabaseItLeaves()
    {
        /* The test runner gives the process 10 seconds to exit once the run returns, then forces exit code 1. The
           nightly of 2026-10-08 lost its publish to that with 28,763 tests green. A drain whose budget is spent
           starts no connect at all: nothing is attempted, nothing is dropped, and every database is counted as
           unfinished, so the exit ends on time. */
        var stopped = StoppedClusterConnectionString(ClosedPort());
        var databases = new[] { Remembered(stopped, "budget one"), Remembered(stopped, "budget two"), Remembered(stopped, "budget three") };

        var outcome = ScratchPostgres.DropRememberedAtExit(databases, TimeSpan.Zero);

        Assert.Equal(0, outcome.ConnectAttempts);
        Assert.Equal(0, outcome.Dropped);
        Assert.Equal(0, outcome.Failed);
        Assert.Equal(0, outcome.Skipped);
        Assert.Equal(3, outcome.Unfinished);
    }

    [Fact]
    public void TheDrainBudget_StaysInsideTheRunnersExitWait_WithRoomForTheProcessToLeave()
    {
        /* The runner waits 10 seconds (shutdownForegroundThreadWaitSeconds) and the wait starts when the run returns.
           The budget bounds the whole drain, a drop in flight included, and still leaves headroom for the process to
           leave. */
        Assert.InRange(ScratchPostgres.ExitDrainBudgetSeconds, 1, 7);
    }

    [Fact]
    public void TheDrain_ConnectsWithTheShortTimeouts_AndKeepsTheRestOfTheAdminConnectionString()
    {
        var admin = new NpgsqlConnectionStringBuilder
        {
            Host = "db.example.test",
            Port = 5544,
            Username = "darling",
            Database = "postgres",
            Pooling = false,
        }.ConnectionString;

        var drain = new NpgsqlConnectionStringBuilder(ScratchPostgres.ExitDrainConnectionString(admin));

        /* The connect wait is the one that stalled the exit: 15 seconds by default, 3 for the drain. Both values are
           named in one place, and the drain sets them whatever the base string carries. */
        Assert.Equal(ScratchPostgres.ExitDrainConnectTimeoutSeconds, drain.Timeout);
        Assert.Equal(ScratchPostgres.ExitDrainCommandTimeoutSeconds, drain.CommandTimeout);
        Assert.True(drain.Timeout < new NpgsqlConnectionStringBuilder().Timeout);
        Assert.False(drain.Pooling);
        Assert.Equal("db.example.test", drain.Host);
        Assert.Equal(5544, drain.Port);
        Assert.Equal("darling", drain.Username);
        Assert.Equal("postgres", drain.Database);
    }

    [Fact]
    public void TheDrain_StopsOnlyTheClusterThatDidNotAnswer_NotAnotherOne()
    {
        /* Two different ports, so the two stopped clusters are two clusters even if the OS hands the same port out twice. */
        var firstPort = ClosedPort();
        int secondPort;
        do
        {
            secondPort = ClosedPort();
        }
        while (secondPort == firstPort);

        var firstStopped = StoppedClusterConnectionString(firstPort);
        var secondStopped = StoppedClusterConnectionString(secondPort);

        var outcome = ScratchPostgres.DropRememberedAtExit(new[]
        {
            Remembered(firstStopped, "first one"),
            Remembered(secondStopped, "second one"),
            Remembered(firstStopped, "first two"),
            Remembered(secondStopped, "second two"),
        });

        /* Two clusters, each tried once: a dead cluster does not cause the other to be skipped. */
        Assert.Equal(2, outcome.ConnectAttempts);
        Assert.Equal(2, outcome.Failed);
        Assert.Equal(2, outcome.Skipped);
    }

    [Fact]
    public async Task TheDrain_OnALiveCluster_DropsWhatItWasGiven_EvenWhenAStoppedClusterIsInTheSameList()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live exit-drain test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var stopped = StoppedClusterConnectionString(ClosedPort());

        /* A deadline as long as one drop's command timeout: this pins the dead-and-live mix, not the deadline, and a drop
           mid-suite waits for a checkpoint that a busy cluster can take seconds to finish. */
        var outcome = ScratchPostgres.DropRememberedAtExit(
            new[]
            {
                Remembered(stopped, "dead one"),
                new ScratchPostgres.RememberedDatabase(scratch.DatabaseName, baseConnectionString!, "the live test"),
                Remembered(stopped, "dead two"),
            },
            TimeSpan.FromSeconds(ScratchPostgres.ExitDrainConnectTimeoutSeconds + ScratchPostgres.ExitDrainCommandTimeoutSeconds) + ScratchPostgres.QuiesceCap);

        /* One attempt on the stopped cluster, one on the live one; the second database on the stopped cluster is left. */
        Assert.Equal(2, outcome.ConnectAttempts);
        Assert.Equal(1, outcome.Dropped);
        Assert.Equal(1, outcome.Failed);
        Assert.Equal(1, outcome.Skipped);
        Assert.False(ScratchPostgres.IsRemembered(scratch.DatabaseName), "a database the drain dropped must leave the queue");

        await using var probe = new NpgsqlConnection(baseConnectionString);
        await probe.OpenAsync(ct);
        await using var exists = new NpgsqlCommand("SELECT count(*) FROM pg_database WHERE datname = $1", probe);
        exists.Parameters.AddWithValue(scratch.DatabaseName);
        Assert.Equal(0L, Convert.ToInt64(await exists.ExecuteScalarAsync(ct)));
    }

    /// <summary>
    /// A run that leaks many databases on a live cluster: the drain returns by its deadline and accounts for every one,
    /// dropped or not finished, with none failed. The 2026-10-08 nightly leaked 39, and dropping them one at a time
    /// outran xUnit's end-of-run window.
    /// <para>How many it drops in time is not pinned. Each DROP DATABASE waits for an immediate checkpoint, and on a
    /// cluster busy with other tests' writes one checkpoint took 30 seconds on a local rig (2026-10-08), so a drop can
    /// legitimately miss the deadline mid-suite; at the real process exit the cluster is idle.</para>
    /// </summary>
    [Fact]
    public async Task TheDrain_OfManyLeakedDatabases_OnALiveCluster_ReturnsByItsDeadline_AndAccountsForEveryOne()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live exit-drain test.");

        var ct = TestContext.Current.CancellationToken;
        const int Leaked = 6;
        var scratches = new ScratchPostgres[Leaked];
        try
        {
            for (var i = 0; i < Leaked; i++)
            {
                scratches[i] = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
            }

            var clock = Stopwatch.StartNew();
            var outcome = ScratchPostgres.DropRememberedAtExit(
                Array.ConvertAll(scratches, scratch => new ScratchPostgres.RememberedDatabase(scratch.DatabaseName, baseConnectionString!, "the leak test")));
            clock.Stop();

            Assert.True(
                clock.Elapsed < TimeSpan.FromSeconds(ScratchPostgres.ExitDrainBudgetSeconds) + TimeSpan.FromSeconds(1),
                $"the drain of {Leaked} databases took {clock.Elapsed.TotalSeconds:0.0} seconds against its {TimeSpan.FromSeconds(ScratchPostgres.ExitDrainBudgetSeconds).TotalSeconds:0}-second deadline");
            Assert.Equal(Leaked, outcome.Dropped + outcome.Unfinished);
            Assert.Equal(0, outcome.Failed);
            Assert.Equal(0, outcome.Skipped);
            Assert.True(outcome.ConnectAttempts >= 1, "the drain never tried the live cluster");

            /* A database the drain dropped is gone from the cluster and from the queue. A drop it sent before the
               deadline may finish after it returned, so the queue can hold fewer than the outcome's unfinished count. */
            var dropped = Array.FindAll(scratches, scratch => !ScratchPostgres.IsRemembered(scratch.DatabaseName));
            Assert.True(dropped.Length >= outcome.Dropped, $"the drain reported {outcome.Dropped} dropped, and only {dropped.Length} left the queue");
            await using var probe = new NpgsqlConnection(baseConnectionString);
            await probe.OpenAsync(ct);
            await using var exists = new NpgsqlCommand("SELECT count(*) FROM pg_database WHERE datname = ANY($1)", probe);
            exists.Parameters.AddWithValue(Array.ConvertAll(dropped, scratch => scratch.DatabaseName));
            Assert.Equal(0L, Convert.ToInt64(await exists.ExecuteScalarAsync(ct)));
        }
        finally
        {
            /* A drop the drain already made makes this a no-op; one it missed (a failing run) is dropped here, not at exit. */
            foreach (var scratch in scratches)
            {
                if (scratch is not null)
                {
                    await scratch.DisposeAsync();
                }
            }
        }
    }
}
