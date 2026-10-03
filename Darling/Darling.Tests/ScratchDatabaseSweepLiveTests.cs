using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
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
            await using (var admin = new NpgsqlConnection(baseConnectionString))
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

            await LiveStoreCleanup.RunAsync(baseConnectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                foreach (var name in planted)
                {
                    await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)", cleanup);
                    await drop.ExecuteNonQueryAsync(cleanupCt);
                }
            });
        }
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
        LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
        {
            await using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)", cleanup);
            await drop.ExecuteNonQueryAsync(cleanupCt);
        });
}
