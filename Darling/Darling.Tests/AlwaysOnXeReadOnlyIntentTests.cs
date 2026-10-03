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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Which connection each step of the deadlock and blocked-process sessions' ensure uses on Azure SQL Database, for a
/// registration with read-only intent and for one without (#4961). A session cannot be created or dropped over a connection
/// to a read-only replica, and a definition made on the primary replicates to it, so a registration with the intent creates
/// over a connection without it. The start, the probes and the read stay on the registration's own connection, because run
/// state is per replica. A session that runs on the replica is stopped there before it is dropped on the primary, on the
/// switch back and when the server is removed. Each test drives the real ensure against a scripted primary and replica, which
/// see the connection string each open would use.
/// </summary>
public sealed class AlwaysOnXeReadOnlyIntentTests : IAsyncDisposable
{
    private const string Host = "intent.database.windows.net";
    private const string InstallIdValue = "0a1b2c3d";

    /* Never opened: the runner reaches the store only to write collected rows, and these tests collect nothing. */
    private readonly NpgsqlDataSource _store = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused;Username=unused");

    public ValueTask DisposeAsync() => _store.DisposeAsync();

    private static string Own(AlwaysOnXeSessionKind kind) =>
        AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.DarlingProduct, InstallIdValue, kind);

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
                throw ReadOnlyRefusal();
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
    }

    private sealed class Rig
    {
        public required DarlingCollectorRunner Runner { get; init; }
        public required ServerRuntime Runtime { get; init; }
        public Pair Alpha { get; } = new();
        public DarlingSelfAlertTests.CapturingLogger Logger { get; } = new();

        /* The connection string of every open, in order. */
        public List<string> OpenedStrings { get; } = new();

        public Task EnsureAsync() => DarlingXeSessions.EnsureAllAsync(Runtime, Runner, Logger, CancellationToken.None);

        /// <summary>
        /// The server's removal: its long-query trace was never on (nothing to drop there), and no other registration
        /// reads a session of its own, so every own fallback the choices hold is dropped.
        /// </summary>
        public Task RemoveAsync() =>
            DarlingRemovedServerSessions.DropAsync(
                new RemovedLongQueryServer(Runtime.Config, Runtime, LongQueryTraceApplied: false),
                Runner, remaining: null, traceOn: _ => false,
                instanceGuard: _ => Task.FromResult(LongQueryTraceInstanceGuard.NoKeepers), Logger, CancellationToken.None);

        /// <summary>
        /// Every line at Warning or above that this routine wrote, whatever it says. Nothing narrows it by a word, so a Warning a
        /// test does not expect, such as a failed stop or a failed drop, is among them.
        /// </summary>
        public List<string> Loud() => Logger.Entries
            .Where(e => e.Level >= LogLevel.Warning)
            .Select(e => e.Message)
            .ToList();

        /// <summary>
        /// The one line a create that a read-only database refused logs at Warning for a capture: the registration, the database
        /// and the message that session's kind is worded with.
        /// </summary>
        public string ReadOnlyLine(string database, AlwaysOnXeSessionKind kind) =>
            $"[{Runtime.Config.DisplayName}] [{database}] {AlwaysOnXeSessions.ReadOnlyDatabaseMessage(kind)}";
    }

    private Rig BuildRig(bool readOnlyIntent)
    {
        var config = new MonitoredServer { Name = "alwayson-intent", Host = Host, ReadOnlyIntent = readOnlyIntent };
        var runtime = new ServerRuntime
        {
            Config = config,
            ConnectionString = $"Server=tcp:{Host},1433;Initial Catalog=master;Encrypt=True;ApplicationIntent={(readOnlyIntent ? "ReadOnly" : "ReadWrite")}",
            Target = new CollectorTargetInfo { IsAzureSqlDb = true },
            StorageName = Host,
            ServerId = 4961,
        };
        var runner = new DarlingCollectorRunner(
            _store,
            new CollectorDeltaCalculator(),
            databaseScope: (_, _) => new List<string>(),
            separatelyMonitoredDatabases: _ => new List<string>(),
            installId: () => InstallIdValue);

        var rig = new Rig { Runner = runner, Runtime = runtime };
        runner.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "master", "alpha" });
        runner.AlwaysOnXeConnectionForTests = (_, database, connectionString, _) =>
        {
            /* The removal reads the database name back from the choice's key, which keeps it in upper case. */
            Assert.Equal("alpha", database, ignoreCase: true);
            Assert.Equal("alpha", new SqlConnectionStringBuilder(connectionString).InitialCatalog, ignoreCase: true);
            var intent = new SqlConnectionStringBuilder(connectionString).ApplicationIntent;
            rig.OpenedStrings.Add(connectionString);
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
        var rig = BuildRig(readOnlyIntent: true);

        await rig.EnsureAsync();

        foreach (var kind in Kinds)
        {
            var shared = Shared(kind);
            Assert.Equal(
                new[] { (ApplicationIntent.ReadWrite, "CREATE"), (ApplicationIntent.ReadOnly, "START") },
                rig.Alpha.Statements(shared));
            Assert.Contains(shared, rig.Alpha.RunningOnReplica);
            Assert.DoesNotContain(shared, rig.Alpha.RunningOnPrimary);
        }

        /* The probes ran over the registration's own connection, and the read-write one was opened once, for the creates. */
        Assert.All(rig.Alpha.ReadsOf("catalog "), intent => Assert.Equal(ApplicationIntent.ReadOnly, intent));
        Assert.All(rig.Alpha.ReadsOf("started "), intent => Assert.Equal(ApplicationIntent.ReadOnly, intent));
        Assert.Equal(new[] { ApplicationIntent.ReadOnly, ApplicationIntent.ReadWrite }, rig.Alpha.Opens);
        Assert.Empty(rig.Loud());
    }

    [Fact]
    public async Task ReadOnlyIntent_AnOwnFallback_IsCreatedOverAConnectionWithoutTheIntent_AndStartedOverTheOwnOne()
    {
        var rig = BuildRig(readOnlyIntent: true);
        var shared = Shared(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Definitions.Add(shared);
        rig.Alpha.WrongEvent.Add(shared);
        rig.Alpha.RunningOnReplica.Add(shared);

        await rig.EnsureAsync();

        var own = Own(AlwaysOnXeSessionKind.Deadlock);
        Assert.Equal(
            new[] { (ApplicationIntent.ReadWrite, "CREATE"), (ApplicationIntent.ReadOnly, "START") },
            rig.Alpha.Statements(own));
        Assert.Contains(own, rig.Alpha.RunningOnReplica);
        Assert.Equal(own, rig.Runner.AlwaysOnReadSessionName(rig.Runtime, "alpha", AlwaysOnXeSessionKind.Deadlock));
        Assert.Empty(rig.Alpha.Statements(shared));
    }

    [Fact]
    public async Task ReadOnlyIntent_SwitchingBack_StopsTheOwnSessionOverTheOwnConnection_ThenDropsItOverAConnectionWithoutTheIntent()
    {
        var rig = BuildRig(readOnlyIntent: true);
        var shared = Shared(AlwaysOnXeSessionKind.Deadlock);
        var own = Own(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Definitions.Add(shared);
        rig.Alpha.WrongEvent.Add(shared);
        rig.Alpha.RunningOnReplica.Add(shared);
        await rig.EnsureAsync();
        Assert.Contains(own, rig.Alpha.RunningOnReplica);
        rig.Alpha.Log.Clear();

        rig.Alpha.WrongEvent.Clear();
        await rig.EnsureAsync();

        Assert.Equal(
            new[] { (ApplicationIntent.ReadOnly, "STOP"), (ApplicationIntent.ReadWrite, "DROP") },
            rig.Alpha.Statements(own));
        Assert.DoesNotContain(own, rig.Alpha.Definitions);
        Assert.Equal(shared, rig.Runner.AlwaysOnReadSessionName(rig.Runtime, "alpha", AlwaysOnXeSessionKind.Deadlock));
        Assert.DoesNotContain(rig.Alpha.Statements(shared), s => s.Verb is "CREATE" or "CREATE+START" or "DROP");
    }

    [Fact]
    public async Task ReadOnlyIntent_ARemovedServer_StopsItsOwnSessionOverTheOwnConnection_ThenDropsItOverAConnectionWithoutTheIntent()
    {
        var rig = BuildRig(readOnlyIntent: true);
        var own = Own(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Definitions.Add(own);
        rig.Alpha.RunningOnReplica.Add(own);
        rig.Runner.AlwaysOnChoices.Set(DarlingAlwaysOnXeSessions.ServerKey(rig.Runtime), "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);

        await rig.RemoveAsync();

        Assert.Equal(
            new[] { (ApplicationIntent.ReadOnly, "STOP"), (ApplicationIntent.ReadWrite, "DROP") },
            rig.Alpha.Statements(own));
        Assert.DoesNotContain(own, rig.Alpha.Definitions);
        Assert.Empty(rig.Loud());
    }

    [Fact]
    public async Task ReadOnlyIntent_ARemovedServer_WhoseOwnSessionDoesNotRunOnTheReplica_DropsItWithoutAStop()
    {
        var rig = BuildRig(readOnlyIntent: true);
        var own = Own(AlwaysOnXeSessionKind.Deadlock);
        rig.Alpha.Definitions.Add(own);
        rig.Runner.AlwaysOnChoices.Set(DarlingAlwaysOnXeSessions.ServerKey(rig.Runtime), "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);

        await rig.RemoveAsync();

        Assert.Equal(new[] { (ApplicationIntent.ReadWrite, "DROP") }, rig.Alpha.Statements(own));
        Assert.DoesNotContain(own, rig.Alpha.Definitions);
        Assert.Empty(rig.Loud());
    }

    [Fact]
    public async Task ReadOnlyIntent_AHealthyDatabase_OpensNoConnectionWithoutTheIntent()
    {
        var rig = BuildRig(readOnlyIntent: true);
        foreach (var kind in Kinds)
        {
            rig.Alpha.Definitions.Add(Shared(kind));
            rig.Alpha.RunningOnReplica.Add(Shared(kind));
        }

        await rig.EnsureAsync();

        Assert.Equal(new[] { ApplicationIntent.ReadOnly }, rig.Alpha.Opens);
        Assert.DoesNotContain(rig.Alpha.Log, e => e.What.StartsWith("DDL ", StringComparison.Ordinal));
    }

    /* ── A registration without it, and on-premises, are unchanged ── */

    [Fact]
    public async Task NoIntent_EverythingGoesOverTheOwnConnection_AsItAlwaysHas()
    {
        var rig = BuildRig(readOnlyIntent: false);
        var shared = Shared(AlwaysOnXeSessionKind.Deadlock);
        var own = Own(AlwaysOnXeSessionKind.Deadlock);

        /* The shared create is one batch, with its start. */
        await rig.EnsureAsync();
        Assert.Equal(new[] { (ApplicationIntent.ReadWrite, "CREATE+START") }, rig.Alpha.Statements(shared));

        /* A fallback is the same, and its drop on the switch back is the one statement, with no stop first. */
        rig.Alpha.WrongEvent.Add(shared);
        rig.Alpha.Log.Clear();
        await rig.EnsureAsync();
        Assert.Equal(new[] { (ApplicationIntent.ReadWrite, "CREATE+START") }, rig.Alpha.Statements(own));
        rig.Alpha.WrongEvent.Clear();
        rig.Alpha.Log.Clear();
        await rig.EnsureAsync();
        Assert.Equal(new[] { (ApplicationIntent.ReadWrite, "DROP") }, rig.Alpha.Statements(own));

        Assert.All(rig.Alpha.Opens, intent => Assert.Equal(ApplicationIntent.ReadWrite, intent));
        Assert.Equal(3, rig.Alpha.Opens.Count);
    }

    /* ── A read-only database reached without the intent (an Azure geo-secondary) ── */

    [Fact]
    public async Task AReadOnlyDatabaseWithoutTheIntent_LogsTheReadOnlyExplanationOnce_NotTheCapsSentence_AndTheRetriesStayQuiet()
    {
        var rig = BuildRig(readOnlyIntent: false);
        rig.Alpha.PrimaryIsReadOnly = true;

        await rig.EnsureAsync();

        /* One line a capture at Warning or above, whatever it says, and each is the read-only message: why, and what to change. */
        var expected = Kinds.Select(kind => rig.ReadOnlyLine("alpha", kind)).OrderBy(line => line, StringComparer.Ordinal).ToList();
        Assert.Equal(expected, rig.Loud().OrderBy(line => line, StringComparer.Ordinal));
        Assert.All(rig.Loud(), line =>
        {
            Assert.Contains("read-only", line, StringComparison.Ordinal);
            Assert.Contains("3906", line, StringComparison.Ordinal);
            Assert.Contains("Register the primary database instead", line, StringComparison.Ordinal);
            Assert.DoesNotContain(AlwaysOnXeSessions.AzureCapsSentence, line, StringComparison.Ordinal);
        });

        /* The pass an hour later asks again, and says nothing new at Warning. */
        await rig.EnsureAsync();
        await rig.EnsureAsync();
        Assert.Equal(expected, rig.Loud().OrderBy(line => line, StringComparer.Ordinal));
    }

    private static SqlException ReadOnlyRefusal() =>
        LongQueryTraceReadOnlyIntentTests.SqlExceptionFactory.Create(
            LongQueryTraceDatabases.ReadOnlyDatabaseErrorNumber, 16, "Failed to update database because the database is read-only.");
}
