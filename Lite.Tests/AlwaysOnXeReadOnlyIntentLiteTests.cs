/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Which connection each step of the deadlock and blocked-process sessions' ensure uses on Azure SQL Database, for a
/// registration with read-only intent and for one without (#4961). A session cannot be created or dropped over a connection
/// to a read-only replica, and a definition made on the primary replicates to it, so a registration with the intent creates
/// over a connection without it. The start, the probes and the read stay on the registration's own connection, because run
/// state is per replica. A session that runs on the replica is stopped there before it is dropped on the primary, on the
/// switch back and when the server is removed. Each test drives the real ensure against a scripted primary and replica,
/// which see the connection string each open would use.
///
/// <para>In <c>app-logger-statics</c> because the tests read <see cref="AppLogger"/>'s process-wide buffer, which another
/// reader would drain from under them.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class AlwaysOnXeReadOnlyIntentLiteTests : IDisposable
{
    private readonly List<DuckDbInitializer> _initializers = [];

    private const string Host = "intent.database.windows.net";

    private readonly string _tempDir;
    private readonly string _configDir;
    private readonly string _dbPath;

    public AlwaysOnXeReadOnlyIntentLiteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private static string Shared(AlwaysOnXeSessionKind kind) => AlwaysOnXeSessions.SharedNameFor(kind);

    private sealed class AlreadyThere : Exception
    {
        public AlreadyThere() : base("already there") { }
    }

    /// <summary>
    /// One Azure SQL Database as its primary and its read-only replica see it. A definition is made on the primary and
    /// replicates at once; a session runs on one side or the other, and is started and stopped on its own. The replica
    /// refuses a create or a drop with error 3906, and the primary refuses a drop while the session still runs on the replica
    /// (the documented order: stop it there, then drop it on the primary).
    /// </summary>
    private sealed class Pair
    {
        public HashSet<string> Definitions { get; } = new(StringComparer.Ordinal);
        public HashSet<string> WrongEvent { get; } = new(StringComparer.Ordinal);
        public HashSet<string> RunningOnPrimary { get; } = new(StringComparer.Ordinal);
        public HashSet<string> RunningOnReplica { get; } = new(StringComparer.Ordinal);

        /* Every open, and every call over a connection: its intent, then what it was. */
        public List<ApplicationIntent> Opens { get; } = new();
        public List<(ApplicationIntent Intent, string What)> Log { get; } = new();

        /* A database that is read-only itself, reached without the intent: the primary refuses creates too. */
        public bool PrimaryIsReadOnly { get; set; }

        /// <summary>The (intent, verb) of each statement that names one session, in the order sent.</summary>
        public List<(ApplicationIntent Intent, string Verb)> Statements(string name) =>
            Log.Where(e => e.What.StartsWith("DDL ", StringComparison.Ordinal) && e.What.Contains("[" + name + "]", StringComparison.Ordinal))
               .Select(e => (e.Intent, Verb: e.What["DDL ".Length..e.What.IndexOf(' ', "DDL ".Length)]))
               .ToList();

        public List<ApplicationIntent> ReadsOf(string prefix) =>
            Log.Where(e => e.What.StartsWith(prefix, StringComparison.Ordinal)).Select(e => e.Intent).ToList();
    }

    private sealed class Channel : IAlwaysOnXeDatabase
    {
        private readonly Pair _pair;
        private readonly ApplicationIntent _intent;

        public Channel(Pair pair, ApplicationIntent intent)
        {
            _pair = pair;
            _intent = intent;
        }

        public Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken)
        {
            _pair.Log.Add((_intent, "catalog " + sessionName));
            return Task.FromResult(
                !_pair.Definitions.Contains(sessionName) ? AlwaysOnXeCatalog.Missing
                : _pair.WrongEvent.Contains(sessionName) && kind == AlwaysOnXeSessionKind.Deadlock ? AlwaysOnXeCatalog.WrongEvent
                : AlwaysOnXeCatalog.Present);
        }

        public Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken)
        {
            _pair.Log.Add((_intent, "started " + sessionName));
            return Task.FromResult(Running.Contains(sessionName));
        }

        private HashSet<string> Running => _intent == ApplicationIntent.ReadOnly ? _pair.RunningOnReplica : _pair.RunningOnPrimary;

        public Task ExecuteAsync(string statement, CancellationToken cancellationToken)
        {
            var text = statement.TrimStart();
            var name = text[(text.IndexOf('[') + 1)..text.IndexOf(']')];
            var creates = text.StartsWith("CREATE EVENT SESSION", StringComparison.Ordinal);
            var starts = text.Contains("STATE = START", StringComparison.Ordinal);
            var stops = text.Contains("STATE = STOP", StringComparison.Ordinal);
            var drops = text.StartsWith("DROP EVENT SESSION", StringComparison.Ordinal);
            var verb = creates && starts ? "CREATE+START" : creates ? "CREATE" : starts ? "START" : stops ? "STOP" : "DROP";
            _pair.Log.Add((_intent, "DDL " + verb + " [" + name + "]"));

            if ((creates || drops) && (_intent == ApplicationIntent.ReadOnly || _pair.PrimaryIsReadOnly))
            {
                throw SqlExceptionFactory.Create(
                    LongQueryTraceDatabases.ReadOnlyDatabaseErrorNumber, errorClass: 16, message: "Failed to update database because the database is read-only.");
            }

            if (creates)
            {
                if (_pair.Definitions.Contains(name))
                {
                    throw new AlreadyThere();
                }

                _pair.Definitions.Add(name);
                if (starts)
                {
                    Running.Add(name);
                }
            }
            else if (drops)
            {
                if (_pair.RunningOnReplica.Contains(name))
                {
                    throw new InvalidOperationException("The session still runs on a read-only replica: stop it there first.");
                }

                _pair.Definitions.Remove(name);
                _pair.RunningOnPrimary.Remove(name);
            }
            else if (starts)
            {
                if (!_pair.Definitions.Contains(name))
                {
                    throw new InvalidOperationException("The session does not exist.");
                }

                if (!Running.Add(name))
                {
                    throw new AlreadyThere();
                }
            }
            else if (stops && !Running.Remove(name))
            {
                throw new InvalidOperationException("The session is not running here.");
            }

            return Task.CompletedTask;
        }

        public bool IsAlreadyPresent(Exception exception) => exception is AlreadyThere;

        /* As the app's own connection reads them, so the shared marking sees a read-only database's refusal (error 3906). */
        public IEnumerable<int> ErrorNumbers(Exception exception) => RemoteCollectorService.ErrorNumbersOf(exception);
    }

    private sealed class Rig
    {
        public required RemoteCollectorService Service { get; init; }
        public required ServerConnection Server { get; init; }
        public Pair Alpha { get; } = new();

        public DateTime Clock { get; set; } = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);

        /// <summary>This rig's lines in the log buffer since the last drain.</summary>
        public List<string> Lines() =>
            AppLogger.DrainBufferedLines()
                .Where(line => line.Contains(Server.DisplayName, StringComparison.Ordinal))
                .ToList();

        /// <summary>The rig's lines at Warning or above since the last drain: every one of them, whatever it says.</summary>
        public List<string> Loud() =>
            Lines().Where(line => line.Contains("[WARN ]", StringComparison.Ordinal) || line.Contains("[ERROR]", StringComparison.Ordinal)).ToList();

        public async Task EnsureBothAsync()
        {
            await Service.EnsureDeadlockXeSessionAsync(Server, engineEdition: 5, CancellationToken.None);
            await Service.EnsureBlockedProcessXeSessionAsync(Server, engineEdition: 5, CancellationToken.None);
        }

        public string Own(AlwaysOnXeSessionKind kind) =>
            AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.LiteProduct, Service.GetInstallId(), kind);
    }

    private async Task<Rig> BuildRigAsync(bool readOnlyIntent)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        _initializers.Add(duckDb);
        await duckDb.InitializeAsync();

        var server = new ServerConnection
        {
            ServerName = Host,
            DisplayName = "intent-" + Guid.NewGuid().ToString("N")[..8],
            ReadOnlyIntent = readOnlyIntent,
        };
        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = 5;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("deadlocks", enabled: true);
        schedules.UpdateSchedule("blocked_process_report", enabled: true);

        var service = new RemoteCollectorService(
            duckDb, servers, schedules, installIdStore: new InstallIdStore(_configDir, "test-machine", null));
        var rig = new Rig { Service = service, Server = server };

        service.LongQueryTraceUtcNowForTests = () => rig.Clock;
        service.XeSessionDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "master", "alpha" });
        service.AlwaysOnXeConnectionForTests = (_, database, connectionString, _) =>
        {
            /* The removal reads the database name back from the choice's key, which keeps it in upper case. */
            Assert.Equal("alpha", database, ignoreCase: true);
            Assert.Equal("alpha", new SqlConnectionStringBuilder(connectionString).InitialCatalog, ignoreCase: true);
            var intent = new SqlConnectionStringBuilder(connectionString).ApplicationIntent;
            rig.Alpha.Opens.Add(intent);
            return Task.FromResult<IAlwaysOnXeDatabase>(new Channel(rig.Alpha, intent));
        };

        return rig;
    }

    private static readonly AlwaysOnXeSessionKind[] Kinds = { AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeSessionKind.BlockedProcess };

    /* ── A registration with read-only intent ── */

    [Fact]
    public async Task ReadOnlyIntent_ASharedSessionThatIsMissing_IsCreatedOverAConnectionWithoutTheIntent_AndStartedOverTheOwnOne()
    {
        var rig = await BuildRigAsync(readOnlyIntent: true);

        await rig.EnsureBothAsync();

        foreach (var kind in Kinds)
        {
            var shared = Shared(kind);
            Assert.Equal(
                new[] { (ApplicationIntent.ReadWrite, "CREATE"), (ApplicationIntent.ReadOnly, "START") },
                rig.Alpha.Statements(shared));
            Assert.Contains(shared, rig.Alpha.RunningOnReplica);
            Assert.DoesNotContain(shared, rig.Alpha.RunningOnPrimary);
        }

        /* The probes ran over the registration's own connection, and a read-write one was opened for each create only. */
        Assert.All(rig.Alpha.ReadsOf("catalog "), intent => Assert.Equal(ApplicationIntent.ReadOnly, intent));
        Assert.All(rig.Alpha.ReadsOf("started "), intent => Assert.Equal(ApplicationIntent.ReadOnly, intent));
        Assert.Equal(2, rig.Alpha.Opens.Count(intent => intent == ApplicationIntent.ReadWrite));
        Assert.Equal(2, rig.Alpha.Opens.Count(intent => intent == ApplicationIntent.ReadOnly));
    }

    [Fact]
    public async Task ReadOnlyIntent_AnOwnFallback_IsCreatedOverAConnectionWithoutTheIntent_AndStartedOverTheOwnOne()
    {
        var rig = await BuildRigAsync(readOnlyIntent: true);
        var shared = Shared(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Definitions.Add(shared);
        rig.Alpha.WrongEvent.Add(shared);
        rig.Alpha.RunningOnReplica.Add(shared);

        await rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None);

        var own = rig.Own(AlwaysOnXeSessionKind.Deadlock);
        Assert.Equal(
            new[] { (ApplicationIntent.ReadWrite, "CREATE"), (ApplicationIntent.ReadOnly, "START") },
            rig.Alpha.Statements(own));
        Assert.Contains(own, rig.Alpha.RunningOnReplica);
        Assert.Equal(own, rig.Service.AlwaysOnReadSessionName(rig.Server, "alpha", AlwaysOnXeSessionKind.Deadlock));
        Assert.Empty(rig.Alpha.Statements(shared));
    }

    [Fact]
    public async Task ReadOnlyIntent_SwitchingBack_StopsTheOwnSessionOverTheOwnConnection_ThenDropsItOverAConnectionWithoutTheIntent()
    {
        var rig = await BuildRigAsync(readOnlyIntent: true);
        var shared = Shared(AlwaysOnXeSessionKind.Deadlock);
        var own = rig.Own(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Definitions.Add(shared);
        rig.Alpha.WrongEvent.Add(shared);
        rig.Alpha.RunningOnReplica.Add(shared);
        await rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None);
        Assert.Contains(own, rig.Alpha.RunningOnReplica);
        rig.Alpha.Log.Clear();

        rig.Alpha.WrongEvent.Clear();
        await rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None);

        Assert.Equal(
            new[] { (ApplicationIntent.ReadOnly, "STOP"), (ApplicationIntent.ReadWrite, "DROP") },
            rig.Alpha.Statements(own));
        Assert.DoesNotContain(own, rig.Alpha.Definitions);
        Assert.Equal(shared, rig.Service.AlwaysOnReadSessionName(rig.Server, "alpha", AlwaysOnXeSessionKind.Deadlock));
        Assert.DoesNotContain(rig.Alpha.Statements(shared), s => s.Verb is "CREATE" or "CREATE+START" or "DROP");
    }

    [Fact]
    public async Task ReadOnlyIntent_ARemovedServer_StopsItsOwnSessionOverTheOwnConnection_ThenDropsItOverAConnectionWithoutTheIntent()
    {
        var rig = await BuildRigAsync(readOnlyIntent: true);
        var own = rig.Own(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Definitions.Add(own);
        rig.Alpha.RunningOnReplica.Add(own);
        rig.Service.AlwaysOnChoices.Set(rig.Server.Id, "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);

        await rig.Service.DropAlwaysOnOwnSessionsOfRemovedServerAsync(rig.Server, CancellationToken.None);

        Assert.Equal(
            new[] { (ApplicationIntent.ReadOnly, "STOP"), (ApplicationIntent.ReadWrite, "DROP") },
            rig.Alpha.Statements(own));
        Assert.DoesNotContain(own, rig.Alpha.Definitions);
    }

    [Fact]
    public async Task ReadOnlyIntent_ARemovedServer_WhoseOwnSessionDoesNotRunOnTheReplica_DropsItWithoutAStop()
    {
        var rig = await BuildRigAsync(readOnlyIntent: true);
        var own = rig.Own(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Definitions.Add(own);
        rig.Service.AlwaysOnChoices.Set(rig.Server.Id, "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);

        await rig.Service.DropAlwaysOnOwnSessionsOfRemovedServerAsync(rig.Server, CancellationToken.None);

        Assert.Equal(new[] { (ApplicationIntent.ReadWrite, "DROP") }, rig.Alpha.Statements(own));
    }

    [Fact]
    public async Task ReadOnlyIntent_AHealthyDatabase_OpensNoConnectionWithoutTheIntent()
    {
        var rig = await BuildRigAsync(readOnlyIntent: true);
        foreach (var kind in Kinds)
        {
            rig.Alpha.Definitions.Add(Shared(kind));
            rig.Alpha.RunningOnReplica.Add(Shared(kind));
        }

        await rig.EnsureBothAsync();

        Assert.All(rig.Alpha.Opens, intent => Assert.Equal(ApplicationIntent.ReadOnly, intent));
        Assert.DoesNotContain(rig.Alpha.Log, e => e.What.StartsWith("DDL ", StringComparison.Ordinal));
    }

    /* ── A registration without it is unchanged ── */

    [Fact]
    public async Task NoIntent_EverythingGoesOverTheOwnConnection_AsItAlwaysHas()
    {
        var rig = await BuildRigAsync(readOnlyIntent: false);
        var shared = Shared(AlwaysOnXeSessionKind.Deadlock);
        var own = rig.Own(AlwaysOnXeSessionKind.Deadlock);

        /* The shared create is one batch, with its start. */
        await rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None);
        Assert.Equal(new[] { (ApplicationIntent.ReadWrite, "CREATE+START") }, rig.Alpha.Statements(shared));

        /* A fallback is the same, and its drop on the switch back is the one statement, with no stop first. */
        rig.Alpha.WrongEvent.Add(shared);
        rig.Alpha.Log.Clear();
        await rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None);
        Assert.Equal(new[] { (ApplicationIntent.ReadWrite, "CREATE+START") }, rig.Alpha.Statements(own));
        rig.Alpha.WrongEvent.Clear();
        rig.Alpha.Log.Clear();
        await rig.Service.EnsureDeadlockXeSessionAsync(rig.Server, engineEdition: 5, CancellationToken.None);
        Assert.Equal(new[] { (ApplicationIntent.ReadWrite, "DROP") }, rig.Alpha.Statements(own));

        Assert.All(rig.Alpha.Opens, intent => Assert.Equal(ApplicationIntent.ReadWrite, intent));
        Assert.Equal(3, rig.Alpha.Opens.Count);
    }

    /* ── A read-only database reached without the intent (an Azure geo-secondary) ── */

    /* Runs the body with the log at Debug, so the lines that must stay quiet are on the buffer to be counted, and puts the level back. */
    private static async Task WithDebugLoggingAsync(Func<Task> body)
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();
            await body();
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    private static int CreatesSent(Pair pair) =>
        pair.Log.Count(e => e.What.StartsWith("DDL CREATE", StringComparison.Ordinal));

    [Fact]
    public async Task AReadOnlyDatabaseWithoutTheIntent_LogsTheReadOnlyExplanationOnce_NotTheCapsSentence_AndTheRetriesStayQuiet()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync(readOnlyIntent: false);
            rig.Alpha.PrimaryIsReadOnly = true;

            await rig.EnsureBothAsync();

            /* One line a capture at Warning or above, each saying why and what to change. The server's own text and the caps
               sentence, which does not apply to a read-only database, are not among them. */
            var loud = rig.Loud();
            Assert.Equal(2, loud.Count);
            Assert.All(loud, line =>
            {
                Assert.Contains("read-only", line, StringComparison.Ordinal);
                Assert.Contains("3906", line, StringComparison.Ordinal);
                Assert.Contains("Register the primary database instead", line, StringComparison.Ordinal);
                Assert.DoesNotContain(AlwaysOnXeSessions.AzureCapsSentence, line, StringComparison.Ordinal);
            });
            Assert.Contains(loud, line => line.Contains("deadlock", StringComparison.OrdinalIgnoreCase));
            Assert.Contains(loud, line => line.Contains("blocked process", StringComparison.OrdinalIgnoreCase));

            /* The pass an hour later asks again, and says nothing new at Warning or above. */
            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.EnsureBothAsync();
            await rig.EnsureBothAsync();
            Assert.Empty(rig.Loud());
        });
    }

    [Fact]
    public async Task AReadOnlyDatabaseWithoutTheIntent_IsNotCreatedAgainWithinAnHour_ThenIsTriedAgain()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync(readOnlyIntent: false);
            rig.Alpha.PrimaryIsReadOnly = true;

            await rig.EnsureBothAsync();
            Assert.Equal(2, CreatesSent(rig.Alpha));

            /* Inside the hour a cycle sends no create, reads nothing, and opens no connection. */
            rig.Alpha.Log.Clear();
            var opens = rig.Alpha.Opens.Count;
            rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
            await rig.EnsureBothAsync();
            await rig.EnsureBothAsync();
            Assert.Empty(rig.Alpha.Log);
            Assert.Equal(opens, rig.Alpha.Opens.Count);

            /* An hour after the refusal the create is sent again, once a capture, and refused again. */
            rig.Clock += TimeSpan.FromMinutes(1);
            await rig.EnsureBothAsync();
            Assert.Equal(2, CreatesSent(rig.Alpha));

            /* The hold starts again from that attempt. */
            rig.Alpha.Log.Clear();
            await rig.EnsureBothAsync();
            Assert.Empty(rig.Alpha.Log);
        });
    }

    [Fact]
    public async Task AReadOnlyDatabaseThatBecomesWritable_IsCreatedAtTheNextTryAfterTheHour_AndAnotherRefusalWarnsAgain()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync(readOnlyIntent: false);
            rig.Alpha.PrimaryIsReadOnly = true;
            await rig.EnsureBothAsync();

            rig.Alpha.PrimaryIsReadOnly = false;
            rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
            await rig.EnsureBothAsync();
            Assert.Empty(rig.Alpha.Definitions);

            rig.Clock += TimeSpan.FromMinutes(1);
            await rig.EnsureBothAsync();
            Assert.Contains(Shared(AlwaysOnXeSessionKind.Deadlock), rig.Alpha.Definitions);
            Assert.Contains(Shared(AlwaysOnXeSessionKind.BlockedProcess), rig.Alpha.Definitions);

            /* A pass that got through forgets the refusal: the next one is told at Warning again, with no hour to wait. */
            rig.Lines();
            rig.Alpha.PrimaryIsReadOnly = true;
            rig.Alpha.Definitions.Clear();
            rig.Alpha.RunningOnPrimary.Clear();
            await rig.EnsureBothAsync();
            Assert.Equal(2, rig.Loud().Count);
        });
    }
}
