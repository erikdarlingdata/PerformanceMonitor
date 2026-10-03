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

        var outcome = ScratchPostgres.DropRememberedAtExit(new[]
        {
            Remembered(stopped, "dead one"),
            new ScratchPostgres.RememberedDatabase(scratch.DatabaseName, baseConnectionString!, "the live test"),
            Remembered(stopped, "dead two"),
        });

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
}
