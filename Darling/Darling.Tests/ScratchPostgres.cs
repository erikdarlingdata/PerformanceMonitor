using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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

    private static async Task DropAsync(string adminConnectionString, string databaseName)
    {
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

    /// <summary>What an exit drain did: connections it tried to open, databases it dropped, drops that failed, and databases it left alone.</summary>
    internal readonly record struct ExitDrainOutcome(int ConnectAttempts, int Dropped, int Failed, int Skipped);

    /// <summary>
    /// How long the exit drain waits to connect to a cluster, in seconds. The default is 15, and the drain ran once per
    /// remembered database, so a cluster stopped before the process exited cost 15 seconds for each database the run
    /// still held (#4981).
    /// </summary>
    internal const int ExitDrainConnectTimeoutSeconds = 3;

    /// <summary>
    /// How long one drop at exit may run, in seconds. This stays at the library default on purpose: a dead cluster
    /// never gets as far as a command (its connect fails first), and a drop on a live cluster ends with an immediate
    /// checkpoint that took 11 seconds on a busy local cluster, so a shorter limit cancelled a drop that was working
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
    /// The exit drain's body, taking its list as a parameter so a test can run it without a process exit and without
    /// touching the databases that other tests, running in parallel, still hold.
    /// </summary>
    /// <remarks>
    /// Connects with the short timeouts above, and stops trying a cluster once a connect to it fails without the
    /// server answering (a stopped cluster, a refused or timed-out connection), so N databases on a dead cluster cost
    /// one short wait, not N. A server that answers with an error (a bad password, a missing database) is not a dead
    /// cluster, and a failed drop on a live cluster never stops the others; each skipped database is still named.
    /// </remarks>
    internal static ExitDrainOutcome DropRememberedAtExit(IEnumerable<RememberedDatabase> remembered)
    {
        var unreachable = new HashSet<string>(StringComparer.Ordinal);
        int attempts = 0, dropped = 0, failed = 0, skipped = 0;
        foreach (var db in remembered)
        {
            try
            {
                var cluster = ClusterKey(db.AdminConnectionString);
                if (unreachable.Contains(cluster))
                {
                    skipped++;
                    Console.Error.WriteLine($"Scratch database {db.Name} outlived its test ({db.Creator}) and was not dropped at exit: its cluster did not answer an earlier connect.");
                    continue;
                }

                attempts++;
                using var admin = new NpgsqlConnection(ExitDrainConnectionString(db.AdminConnectionString));
                try
                {
                    admin.Open();
                }
                catch (Exception ex) when (ex is not PostgresException)
                {
                    unreachable.Add(cluster);
                    throw;
                }

                using var drop = new NpgsqlCommand($"DROP DATABASE IF EXISTS \"{db.Name}\" WITH (FORCE)", admin);
                drop.ExecuteNonQuery();
                Remembered.TryRemove(db.Name, out _);
                dropped++;
                Console.Error.WriteLine($"Scratch database {db.Name} outlived its test ({db.Creator}); dropped at process exit.");
            }
            catch (Exception ex)
            {
                failed++;
                Console.Error.WriteLine($"Scratch database {db.Name} outlived its test ({db.Creator}) and could not be dropped at exit: {ex.Message}");
            }
        }

        return new ExitDrainOutcome(attempts, dropped, failed, skipped);
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
