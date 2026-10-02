/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The long-query trace's session lifecycle on Azure SQL Database, where each database holds its own session: which
/// databases a reconcile creates the session in and drops it from, and how a failed drop is retried. Each test drives
/// the real reconcile with the database list and the per-database work replaced, so no server is needed.
///
/// <para>In <c>app-logger-statics</c> because two tests read <see cref="AppLogger"/>'s process-wide buffer, which
/// another reader would drain from under them.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class LongQueryTraceLifecycleLiteTests : IDisposable
{
    private const string Host = "lqtrace.database.windows.net";

    private readonly string _tempDir;
    private readonly string _configDir;
    private readonly string _dbPath;

    public LongQueryTraceLifecycleLiteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(_configDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
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

    private sealed class Rig
    {
        public required RemoteCollectorService Service { get; init; }
        public required ServerManager Servers { get; init; }
        public required ScheduleManager Schedules { get; init; }
        public required ServerConnection Server { get; init; }
        public required DuckDbInitializer DuckDb { get; init; }
        public required int ServerId { get; init; }

        /* The SQL error number a refusing database says, or null to refuse with a plain error that is no SQL error. */
        public int? RefusalNumber { get; set; }

        /* This install's id (null when the service was built without an id store), and every session name the long-query
           work named: the name a create or a drop would have put in its statement (#4961). */
        public string? InstallId { get; set; }
        public List<string> Names { get; } = new();

        /* What the logical server's master lists, before the registration's exclusions. */
        public List<string> Listed { get; set; } = new() { "master", "alpha", "beta", "gamma" };
        public List<(string Database, bool Create)> Calls { get; } = new();
        public HashSet<string> Refuse { get; } = new(StringComparer.OrdinalIgnoreCase);

        /* The drops of the session that versions before #4961 shared between installs (#4961), kept apart from the calls
           above: the database each one named (empty on server scope), the databases that refuse it, and every call of
           either kind in the order it ran, as (database, "legacy" | "create" | "drop"). */
        public List<string> LegacyCalls { get; } = new();
        public HashSet<string> RefuseLegacy { get; } = new(StringComparer.OrdinalIgnoreCase);
        public List<(string Database, string Kind)> Sequence { get; } = new();
        public Exception? ListFailure { get; set; }
        public int ListCalls { get; set; }

        /* The time each reconcile runs at. */
        public DateTime Clock { get; set; } = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);

        public Task ReconcileAsync() => Service.ReconcileLongQueryCompletionsXeSessionAsync(Server, CancellationToken.None);

        public bool? Applied => Service.LongQueryTraceAppliedState(Server.Id);

        public IEnumerable<string> Dropped => Calls.Where(c => !c.Create).Select(c => c.Database);

        public IEnumerable<string> Created => Calls.Where(c => c.Create).Select(c => c.Database);
    }

    /// <summary>A logical-server registration (no database named) on an Azure SQL Database server.</summary>
    private Task<Rig> BuildRigAsync(bool traceOn, params string[] excluded) =>
        BuildRigAsync(
            new ServerConnection
            {
                ServerName = Host,
                DisplayName = "lqtrace-" + Guid.NewGuid().ToString("N")[..8],
                ExcludedDatabases = excluded.ToList(),
            },
            traceOn);

    /// <summary>A registration of one database of the same logical server, with read-only intent unless told otherwise.</summary>
    private async Task<Rig> BuildDatabaseRigAsync(string database, bool traceOn, bool readOnlyIntent = true)
    {
        var rig = await BuildRigAsync(
            new ServerConnection
            {
                ServerName = Host,
                DisplayName = "lqtrace-" + database,
                DatabaseName = database,
                ReadOnlyIntent = readOnlyIntent,
            },
            traceOn);
        rig.Listed = new List<string> { database };
        return rig;
    }

    /// <summary>A server on an engine with no per-database sessions (edition 3, an on-premises server): its session is the server's.</summary>
    private Task<Rig> BuildOnPremRigAsync(bool traceOn) =>
        BuildRigAsync(
            new ServerConnection { ServerName = "lqtrace-sql", DisplayName = "lqtrace-onprem-" + Guid.NewGuid().ToString("N")[..8] },
            traceOn,
            engineEdition: 3);

    private async Task<Rig> BuildRigAsync(ServerConnection server, bool traceOn, int engineEdition = 5, bool withInstallId = true)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();

        var servers = new ServerManager(_configDir);
        servers.AddServer(server);
        servers.GetConnectionStatus(server.Id).SqlEngineEdition = engineEdition;

        var schedules = new ScheduleManager(_configDir);
        schedules.UpdateSchedule("long_query_completions", enabled: traceOn);

        return WireRig(duckDb, servers, schedules, server, withInstallId);
    }

    /// <summary>
    /// A new process over the same store and the same configuration: a new service, new in-memory state, and fresh call
    /// lists. The record of a drop that an earlier service kept in the store is the only thing the new one inherits.
    /// </summary>
    private async Task<Rig> RestartAsync(Rig before)
    {
        var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();

        var after = WireRig(duckDb, before.Servers, before.Schedules, before.Server, withInstallId: true);
        after.Listed = before.Listed.ToList();
        return after;
    }

    private Rig WireRig(DuckDbInitializer duckDb, ServerManager servers, ScheduleManager schedules, ServerConnection server, bool withInstallId)
    {
        var rig = new Rig
        {
            Service = new RemoteCollectorService(
                duckDb, servers, schedules,
                installIdStore: withInstallId ? new InstallIdStore(_configDir, "test-machine", null) : null),
            Servers = servers,
            Schedules = schedules,
            Server = server,
            DuckDb = duckDb,
            ServerId = RemoteCollectorService.GetServerId(server),
        };
        rig.InstallId = rig.Service.GetInstallId();

        rig.Service.LongQueryTraceUtcNowForTests = () => rig.Clock;
        rig.Service.LongQueryTraceListOverrideForTests = (registration, allDatabases, _) =>
        {
            rig.ListCalls++;
            if (rig.ListFailure is { } failure)
            {
                throw failure;
            }

            return Task.FromResult(allDatabases
                ? rig.Listed.ToList()
                : rig.Listed.Where(d => !registration.ExcludedDatabases.Contains(d, StringComparer.OrdinalIgnoreCase)).ToList());
        };

        rig.Service.LongQueryTraceDatabaseOverrideForTests = (_, database, create, sessionName, _) =>
        {
            /* The legacy session's drop is its own list, so every assertion about this install's session stays as it was. */
            if (string.Equals(sessionName, LongQueryCompletionsCollector.LegacyXeSessionName, StringComparison.Ordinal))
            {
                /* A create of it would show as a malformed entry, so every comparison against the databases fails. */
                rig.LegacyCalls.Add(create ? database + " (create)" : database);
                rig.Sequence.Add((database, create ? "legacy-create" : "legacy"));
                return rig.RefuseLegacy.Contains(database)
                    ? Task.FromException(new InvalidOperationException($"The legacy drop was refused in '{database}'."))
                    : Task.CompletedTask;
            }

            rig.Sequence.Add((database, create ? "create" : "drop"));
            rig.Calls.Add((database, create));
            rig.Names.Add(sessionName);
            var refusal = $"The {(create ? "create" : "drop")} was refused in {database}.";
            return rig.Refuse.Contains(database)
                ? Task.FromException(rig.RefusalNumber is { } number
                    ? SqlExceptionFactory.Create(number, errorClass: 14, message: refusal)
                    : new InvalidOperationException(refusal))
                : Task.CompletedTask;
        };

        return rig;
    }

    /// <summary>Registers one database of the same logical server as its own server.</summary>
    private static void RegisterSeparately(Rig rig, string database) =>
        rig.Servers.AddServer(new ServerConnection { ServerName = Host, DisplayName = $"{Host} {database}", DatabaseName = database });

    /// <summary>
    /// Registers another server of the same logical server with its own trace setting: one database when
    /// <paramref name="database"/> is given, otherwise a second registration of the logical server.
    /// </summary>
    private static ServerConnection RegisterOther(Rig rig, string? database, bool traceOn, bool readOnlyIntent, params string[] excluded)
    {
        var other = new ServerConnection
        {
            ServerName = Host,
            DisplayName = $"{Host} {database ?? "server"} {(readOnlyIntent ? "read-only" : "read-write")}",
            DatabaseName = database,
            ReadOnlyIntent = readOnlyIntent,
            ExcludedDatabases = excluded.ToList(),
        };
        rig.Servers.AddServer(other);
        SetTrace(rig, other, traceOn);
        return other;
    }

    private static void SetTrace(Rig rig, ServerConnection server, bool traceOn) =>
        rig.Schedules.SetScheduleForServer(
            server.Id,
            new List<CollectorSchedule> { new() { Name = "long_query_completions", Enabled = traceOn } });

    /* ── M1: a failed drop is retried, with a cap ── */

    [Fact]
    public async Task Off_AFailedListing_IsNotMarkedDone_AndTheNextCycleListsAgain()
    {
        var rig = await BuildRigAsync(traceOn: false);
        rig.ListFailure = SqlExceptionFactory.Create(40613, 20, "Database 'master' is not currently available.");

        await rig.ReconcileAsync();

        Assert.Null(rig.Applied);

        rig.ListFailure = null;
        await rig.ReconcileAsync();

        Assert.Equal(2, rig.ListCalls);
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
        Assert.False(rig.Applied);
    }

    [Fact]
    public async Task Off_OneDatabaseRefusesTheDrop_TheOthersAreStillDropped_AndTheNextCycleRetries()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Warning);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: false);
            rig.Refuse.Add("beta");

            await rig.ReconcileAsync();

            Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
            Assert.Null(rig.Applied);
            Assert.Contains(AppLogger.DrainBufferedLines(), line =>
                line.Contains(rig.Server.DisplayName, StringComparison.Ordinal)
                && line.Contains("[beta]", StringComparison.Ordinal)
                && line.Contains("WARN", StringComparison.Ordinal));

            rig.Calls.Clear();
            await rig.ReconcileAsync();

            Assert.Contains("beta", rig.Dropped);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Off_TheFifthFailedPassInARow_IsMarkedDone_WithOneWarningThatNamesTheDatabase()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Warning);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: false);
            rig.Refuse.Add("beta");

            for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
                Assert.Null(rig.Applied);
            }

            await rig.ReconcileAsync();
            Assert.False(rig.Applied);

            /* Done: the next cycle does not try again. */
            rig.Calls.Clear();
            await rig.ReconcileAsync();
            Assert.Empty(rig.Calls);

            var giveUps = AppLogger.DrainBufferedLines()
                .Where(line => line.Contains(rig.Server.DisplayName, StringComparison.Ordinal)
                    && line.Contains("Stopped retrying", StringComparison.Ordinal))
                .ToList();
            var giveUp = Assert.Single(giveUps);
            Assert.Contains("beta", giveUp, StringComparison.Ordinal);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /* ── After the cap: one attempt an hour ── */

    [Fact]
    public async Task Off_AfterTheCap_TriesAgainOnceAnHour_AtDebug_WithNoSecondWarning()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: false);
            rig.Refuse.Add("beta");

            for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
            }

            Assert.False(rig.Applied);
            var giveUp = Assert.Single(Lines(rig), line => line.Contains("Stopped retrying", StringComparison.Ordinal));
            Assert.Contains(
                "It tries again once an hour, and right away after a restart, a change to the trace's settings, or a change to"
                + " another registration of these databases.",
                giveUp,
                StringComparison.Ordinal);
            Assert.DoesNotContain("reconnects", giveUp, StringComparison.Ordinal);

            /* Within the hour: nothing. */
            rig.Calls.Clear();
            rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            Assert.Empty(rig.Calls);

            /* An hour after the cap: one attempt, and the cycle after it runs nothing. */
            rig.Clock += TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);

            /* It fails again an hour later. Both failures log at Debug, and the cap's warning stays the only one. */
            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            Assert.Equal(new[] { "alpha", "beta", "gamma", "alpha", "beta", "gamma" }, rig.Dropped);

            var afterTheCap = Lines(rig);
            Assert.DoesNotContain(afterTheCap, line => line.Contains("WARN", StringComparison.Ordinal));
            Assert.Equal(2, afterTheCap.Count(line =>
                line.Contains("DEBUG", StringComparison.Ordinal)
                && line.Contains("The next attempt is in an hour.", StringComparison.Ordinal)));
            Assert.Equal(2, afterTheCap.Count(line =>
                line.Contains("DEBUG", StringComparison.Ordinal)
                && line.Contains("[beta]", StringComparison.Ordinal)));
            Assert.False(rig.Applied);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task AfterTheCap_AnAttemptThatSucceeds_EndsTheHourlyAttempts()
    {
        var rig = await BuildRigAsync(traceOn: false);
        rig.Refuse.Add("beta");

        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync();
        }

        rig.Refuse.Clear();
        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);

        /* Done, with nothing left to retry: an hour later nothing runs. */
        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync();
        Assert.Empty(rig.Calls);
    }

    [Fact]
    public async Task On_AfterTheCap_TheCyclesInBetween_KeepTheHourlyAttempt()
    {
        /* gamma is excluded, so the drop outside the set tries it, and gamma refuses. */
        var rig = await BuildRigAsync(traceOn: true, "gamma");
        rig.Refuse.Add("gamma");

        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync();
        }

        Assert.True(rig.Applied);

        /* Each cycle in the hour creates the session where it is missing and leaves the drop alone. A cycle that
           skips the drop must not end the hourly attempt. */
        rig.Calls.Clear();
        for (var cycle = 1; cycle <= 3; cycle++)
        {
            rig.Clock += TimeSpan.FromMinutes(15);
            await rig.ReconcileAsync();
        }

        Assert.Empty(rig.Dropped);
        Assert.Equal(new[] { "alpha", "beta", "alpha", "beta", "alpha", "beta" }, rig.Created);

        rig.Clock += TimeSpan.FromMinutes(15);
        await rig.ReconcileAsync();
        Assert.Equal(new[] { "gamma" }, rig.Dropped);
    }

    /// <summary>This rig's lines in the log buffer since the last drain.</summary>
    private static List<string> Lines(Rig rig) =>
        AppLogger.DrainBufferedLines()
            .Where(line => line.Contains(rig.Server.DisplayName, StringComparison.Ordinal))
            .ToList();

    /* ── #4964: a create that fails again and again logs its first failure at Warning, the repeats at Debug ── */

    private const string ReconcileLine = "Failed to reconcile long-query completion XE session";

    /// <summary>The failure the reconcile kept for the collector's run, read from the service's own slot.</summary>
    private static Exception? KeptFault(Rig rig)
    {
        var slot = typeof(RemoteCollectorService).GetField("_longQueryTraceFault", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var faults = (ConcurrentDictionary<string, Exception>)slot.GetValue(rig.Service)!;
        return faults.TryGetValue(rig.Server.Id, out var fault) ? fault : null;
    }

    private static int Count(IEnumerable<string> lines, string level, string text) =>
        lines.Count(line => line.Contains(level, StringComparison.Ordinal) && line.Contains(text, StringComparison.Ordinal));

    [Fact]
    public async Task On_ACreateThatFailsOnEveryCycle_LogsOneWarning_ThenDebug_AndRecordsTheFaultEachTime()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            var lines = new List<string>();

            rig.ListFailure = new InvalidOperationException("master is not readable, first.");
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "WARN", ReconcileLine));
            Assert.Contains("first.", KeptFault(rig)!.Message, StringComparison.Ordinal);

            /* The same failure on the next two cycles: no new Warning, one Debug line each, and the fault is kept
               again with the newest message, so the run is still classified from it. */
            foreach (var attempt in new[] { "second.", "third." })
            {
                rig.ListFailure = new InvalidOperationException("master is not readable, " + attempt);
                await rig.ReconcileAsync();
                lines.AddRange(Lines(rig));
                Assert.Contains(attempt, KeptFault(rig)!.Message, StringComparison.Ordinal);
            }

            Assert.Equal(1, Count(lines, "WARN", ReconcileLine));
            Assert.Equal(2, Count(lines, "DEBUG", ReconcileLine));

            /* The retry did not change: every cycle listed again. */
            Assert.Equal(3, rig.ListCalls);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task On_ACreateThatSucceedsAfterFailures_WarnsAgainWhenItFailsAfterwards()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            var lines = new List<string>();

            rig.ListFailure = new InvalidOperationException("master is not readable.");
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "WARN", ReconcileLine));
            Assert.Equal(1, Count(lines, "DEBUG", ReconcileLine));

            /* A create that succeeds ends the run of failures, and the fault it kept. */
            rig.ListFailure = null;
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Null(KeptFault(rig));
            Assert.True(rig.Applied);

            /* The next failure is a new one: a Warning, and its repeat is Debug. */
            rig.ListFailure = new InvalidOperationException("master is not readable again.");
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(2, Count(lines, "WARN", ReconcileLine));
            Assert.Equal(2, Count(lines, "DEBUG", ReconcileLine));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Off_ADropFailureAfterCreateFailures_IsNotCountedAsARepeat()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            var lines = new List<string>();

            rig.ListFailure = new InvalidOperationException("master is not readable.");
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "WARN", ReconcileLine));

            /* Turned off, the drop is a different pass with its own failure: its first line is a Warning. The cap on the
               drop side is its own (#4944). */
            rig.Schedules.UpdateSchedule("long_query_completions", enabled: false);
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "WARN", "Could not list the databases to drop the long-query trace session from"));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /* ── #4964: the per-database create lines of the shared Azure ensure ── */

    private const string EnsureLine = "Failed to ensure long query completions XE session";

    /// <summary>
    /// The create is refused in every database on every cycle. The shared ensure logs a Warning for each refusing database
    /// and an Error for the all-refused summary on the first cycle; the cycles after it log the same lines at Debug until a
    /// create succeeds. The retry does not change: every cycle tries every database, and the fault stays set.
    /// </summary>
    [Fact]
    public async Task On_ACreateRefusedInEveryDatabase_LogsOneWarningSetAndOneError_ThenDebug()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            foreach (var database in new[] { "alpha", "beta", "gamma" })
            {
                rig.Refuse.Add(database);
            }

            const string allRefused = "Failed to ensure the long query completions XE session in all 3 database(s)";
            var lines = new List<string>();

            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(3, Count(lines, "WARN", EnsureLine));
            Assert.Equal(1, Count(lines, "ERROR", allRefused));

            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));

            Assert.Equal(3, Count(lines, "WARN", EnsureLine));
            Assert.Equal(1, Count(lines, "ERROR", allRefused));
            Assert.Equal(6, Count(lines, "DEBUG", EnsureLine));
            Assert.Equal(2, Count(lines, "DEBUG", allRefused));

            /* The retry and the fault did not change: three cycles tried all three databases, and the run is still told. */
            Assert.Equal(9, rig.Created.Count());
            Assert.NotNull(KeptFault(rig));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// A create that succeeds ends the run of refusals, so the next refusal warns again: the Warning set and the Error
    /// come back once, and their repeats are Debug.
    /// </summary>
    [Fact]
    public async Task On_ACreateRefusedAgainAfterItSucceeded_WarnsAgain()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            foreach (var database in new[] { "alpha", "beta", "gamma" })
            {
                rig.Refuse.Add(database);
            }

            const string allRefused = "Failed to ensure the long query completions XE session in all 3 database(s)";
            var lines = new List<string>();

            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(3, Count(lines, "WARN", EnsureLine));
            Assert.Equal(3, Count(lines, "DEBUG", EnsureLine));

            rig.Refuse.Clear();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Null(KeptFault(rig));

            foreach (var database in new[] { "alpha", "beta", "gamma" })
            {
                rig.Refuse.Add(database);
            }

            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            lines.AddRange(Lines(rig));
            Assert.Equal(6, Count(lines, "WARN", EnsureLine));
            Assert.Equal(2, Count(lines, "ERROR", allRefused));
            Assert.Equal(6, Count(lines, "DEBUG", EnsureLine));
            Assert.Equal(2, Count(lines, "DEBUG", allRefused));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// The create's own listing of the monitored databases fails with a SQL error on every cycle: its Error line is the
    /// first failure's, and the cycles after it log Debug.
    /// </summary>
    [Fact]
    public async Task On_AListingThatFailsWithASqlError_LogsOneError_ThenDebug()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: true);
            const string listing = "Failed to enumerate databases for long query completions XE sessions";
            var lines = new List<string>();

            for (var cycle = 0; cycle < 3; cycle++)
            {
                rig.ListFailure = SqlExceptionFactory.Create(4060, errorClass: 11, message: "master is not readable.");
                await rig.ReconcileAsync();
                lines.AddRange(Lines(rig));
            }

            Assert.Equal(1, Count(lines, "ERROR", listing));
            Assert.Equal(2, Count(lines, "DEBUG", listing));
            Assert.NotNull(KeptFault(rig));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /* ── #4964: the drop of a server-scoped session follows the same cap as the Azure arm's ── */

    /* The server-scoped session reaches the test replacement with no database name. */
    private const string TheServer = "";

    private const string DropLine = "Could not drop the long-query trace session:";

    /// <summary>
    /// On an engine with no per-database sessions, a disabled trace whose drop fails on every cycle follows the cadence of
    /// the Azure arm: the first four failures are Warnings that the next cycle tries again, the fifth is the one Warning that
    /// gives up, the cycles after it run nothing, and an hour later one attempt logs at Debug. Before, the failure reached
    /// the general catch, which warned on every cycle with no end.
    /// </summary>
    [Fact]
    public async Task Off_OnPremises_ADropThatFailsOnEveryCycle_WarnsToTheCap_ThenTriesOnceAnHourAtDebug()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildOnPremRigAsync(traceOn: false);
            rig.Refuse.Add(TheServer);
            var lines = new List<string>();

            for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
                Assert.Null(rig.Applied);
            }

            await rig.ReconcileAsync();
            Assert.False(rig.Applied);
            lines.AddRange(Lines(rig));
            Assert.Equal(LongQueryTraceDatabases.DropAttemptCap - 1, Count(lines, "WARN", DropLine));
            var giveUp = Assert.Single(lines, line => line.Contains("Stopped retrying", StringComparison.Ordinal));
            Assert.Contains("may remain on the server", giveUp, StringComparison.Ordinal);
            Assert.DoesNotContain("databases could not be listed", giveUp, StringComparison.Ordinal);
            Assert.Equal(0, Count(lines, "WARN", ReconcileLine));

            /* Done: the cycles after it run nothing, until the hour is up. */
            rig.Calls.Clear();
            await rig.ReconcileAsync();
            rig.Clock += LongQueryTraceDatabases.RetryInterval - TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            Assert.Empty(rig.Calls);

            /* An hour after the cap: one attempt, at Debug, and the cycle after it runs nothing. */
            rig.Clock += TimeSpan.FromMinutes(1);
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            Assert.Single(rig.Calls);
            lines.Clear();
            lines.AddRange(Lines(rig));
            Assert.Equal(0, Count(lines, "WARN", DropLine));
            Assert.Equal(1, Count(lines, "DEBUG", "The next attempt is in an hour."));
            Assert.False(rig.Applied);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /// <summary>
    /// A drop that fails, then succeeds, is done: the count and the hourly attempt end, and nothing more is tried.
    /// </summary>
    [Fact]
    public async Task Off_OnPremises_ADropThatSucceedsAfterFailures_IsDone()
    {
        var rig = await BuildOnPremRigAsync(traceOn: false);
        rig.Refuse.Add(TheServer);

        await rig.ReconcileAsync();
        await rig.ReconcileAsync();
        Assert.Null(rig.Applied);

        rig.Refuse.Clear();
        await rig.ReconcileAsync();
        Assert.False(rig.Applied);

        rig.Calls.Clear();
        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync();
        Assert.Empty(rig.Calls);
    }

    /// <summary>
    /// Turning the trace on is a create that succeeds, and it ends the run of drop failures: after the cap gave up, on and
    /// off again, the next failures count from one, and warn again.
    /// </summary>
    [Fact]
    public async Task Off_OnPremises_AfterTheCap_TurnedOnAndOffAgain_CountsTheFailuresAgain()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildOnPremRigAsync(traceOn: false);
            rig.Refuse.Add(TheServer);
            for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
            }

            Assert.False(rig.Applied);

            rig.Refuse.Clear();
            rig.Schedules.UpdateSchedule("long_query_completions", enabled: true);
            await rig.ReconcileAsync();
            Assert.True(rig.Applied);

            AppLogger.DrainBufferedLines();
            rig.Refuse.Add(TheServer);
            rig.Schedules.UpdateSchedule("long_query_completions", enabled: false);
            var lines = new List<string>();
            for (var pass = 1; pass < LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
            }

            lines.AddRange(Lines(rig));
            Assert.Equal(LongQueryTraceDatabases.DropAttemptCap - 1, Count(lines, "WARN", DropLine));

            /* The failed passes were not marked done: the last reconcile that finished is the one that turned it on. */
            Assert.True(rig.Applied);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    /* ── M2: the trace follows the monitored set ── */

    [Fact]
    public async Task On_AnExclusionChange_DropsTheSessionFromTheNewlyExcludedDatabase()
    {
        var rig = await BuildRigAsync(traceOn: true);

        await rig.ReconcileAsync();
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Created);

        rig.Server.ExcludedDatabases.Add("gamma");
        rig.Calls.Clear();
        await rig.ReconcileAsync();

        Assert.Equal(new[] { "gamma" }, rig.Dropped);
        Assert.DoesNotContain("gamma", rig.Created);
    }

    [Fact]
    public async Task Off_DropsTheSessionEverywhere_ExcludedDatabasesIncluded()
    {
        var rig = await BuildRigAsync(traceOn: false, "gamma");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
    }

    /* ── L3: a database monitored as its own server belongs to that registration ── */

    [Fact]
    public async Task Off_LeavesTheSessionOfADatabaseMonitoredAsItsOwnServer()
    {
        var rig = await BuildRigAsync(traceOn: false);
        RegisterSeparately(rig, "beta");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task On_DoesNotCreateTheSessionInADatabaseMonitoredAsItsOwnServer()
    {
        var rig = await BuildRigAsync(traceOn: true);
        RegisterSeparately(rig, "beta");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Created);
        Assert.DoesNotContain("beta", rig.Dropped);
    }

    /// <summary>
    /// The trace ON leaves a database monitored as its own server even when this registration excludes it, which
    /// would otherwise put it in the drop outside the set. Only the plan's own-server rule keeps it: the other
    /// registration's trace is off, so no other registration keeps the session there.
    /// </summary>
    [Fact]
    public async Task On_ADatabaseMonitoredAsItsOwnServer_AndExcluded_IsNotDropped()
    {
        var rig = await BuildRigAsync(traceOn: true, "beta");
        RegisterOther(rig, "beta", traceOn: false, readOnlyIntent: false);

        await rig.ReconcileAsync();

        Assert.DoesNotContain("beta", rig.Dropped);
    }

    /* ── M1: a drop leaves a database where another registration of the server keeps the session ── */

    [Fact]
    public async Task Off_LeavesTheDatabaseOfAReadOnlyRegistration_WhileItsTraceIsOn()
    {
        var rig = await BuildRigAsync(traceOn: false);
        RegisterOther(rig, "beta", traceOn: true, readOnlyIntent: true);

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task ReadOnlyRegistration_Off_LeavesItsDatabase_WhileTheLogicalServersTraceIsOnAndCoversIt()
    {
        var rig = await BuildDatabaseRigAsync("beta", traceOn: false);
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: false);

        await rig.ReconcileAsync();

        Assert.Empty(rig.Dropped);
        Assert.False(rig.Applied);
    }

    [Fact]
    public async Task ReadOnlyRegistration_Off_DropsItsDatabase_WhenTheLogicalServerExcludesIt()
    {
        var rig = await BuildDatabaseRigAsync("beta", traceOn: false);
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: false, "beta");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "beta" }, rig.Dropped);
    }

    /// <summary>
    /// A registration of one database without read-only intent is monitored as its own server, so the logical
    /// server's registration never creates the session there and does not keep it. Turning that database's own
    /// trace off still drops it.
    /// </summary>
    [Fact]
    public async Task DatabaseRegistration_Off_DropsItsDatabase_WhileTheLogicalServerLeavesItToIt()
    {
        var rig = await BuildDatabaseRigAsync("beta", traceOn: false, readOnlyIntent: false);
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: false);

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "beta" }, rig.Dropped);
    }

    [Fact]
    public async Task Off_LeavesTheDatabasesOfASecondLogicalServerRegistration_WhileItsTraceIsOn()
    {
        var rig = await BuildRigAsync(traceOn: false);
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: true);

        await rig.ReconcileAsync();

        Assert.Empty(rig.Dropped);
    }

    [Fact]
    public async Task On_TheOutsideDrop_LeavesAnExcludedDatabase_ThatASecondRegistrationsTraceCovers()
    {
        var rig = await BuildRigAsync(traceOn: true, "gamma");
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: true);

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "alpha", "beta" }, rig.Created);
        Assert.Empty(rig.Dropped);
    }

    [Fact]
    public async Task On_TheOutsideDrop_StillDropsADatabase_ThatBothLogicalServerRegistrationsExclude()
    {
        var rig = await BuildRigAsync(traceOn: true, "gamma");
        RegisterOther(rig, database: null, traceOn: true, readOnlyIntent: true, "gamma");

        await rig.ReconcileAsync();

        Assert.Equal(new[] { "gamma" }, rig.Dropped);
    }

    [Fact]
    public async Task Off_OnceTheOtherRegistrationTurnsItsTraceOff_TheNextPassDrops()
    {
        var rig = await BuildRigAsync(traceOn: false);
        var other = RegisterOther(rig, "beta", traceOn: true, readOnlyIntent: true);

        await rig.ReconcileAsync();
        Assert.DoesNotContain("beta", rig.Dropped);

        /* Nothing changed: the reconcile is done and does not run again. */
        rig.Calls.Clear();
        await rig.ReconcileAsync();
        Assert.Empty(rig.Calls);

        SetTrace(rig, other, traceOn: false);
        await rig.ReconcileAsync();

        Assert.Contains("beta", rig.Dropped);
    }

    /* ── the read follows the lifecycle; the always-on sessions do not change ── */

    /// <summary>
    /// The logical server's long-query read skips the databases monitored as their own servers, the same ones its
    /// lifecycle leaves alone. Reading one would fail every cycle when that database's own trace is off (a missing
    /// session is a read failure since #4731), or store its events twice when it is on. Pinned in the source: the
    /// read loop needs a live connection per database.
    /// </summary>
    [Fact]
    public void TheLongQueryRead_SkipsTheDatabasesMonitoredAsTheirOwnServers()
    {
        var definition = ReadLf(Path.Combine("PerformanceMonitor.Collectors", "LongQueryCompletionsCollector.cs"));
        Assert.Contains("public override bool SkipsSeparatelyMonitoredDatabases => true;", definition, StringComparison.Ordinal);

        var runner = ReadLf(Path.Combine("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs"));
        var list = runner.IndexOf("await GetAzureDatabaseListAsync(server, cancellationToken);", StringComparison.Ordinal);
        var skip = runner.IndexOf("if (definition.SkipsSeparatelyMonitoredDatabases)", StringComparison.Ordinal);
        Assert.True(list > 0 && skip > list, "the per-database read filters its list after listing it");
        Assert.Contains("databases = WithoutSeparatelyMonitoredDatabases(server, databases);", runner, StringComparison.Ordinal);
    }

    /// <summary>
    /// The read leaves out master too, by the trace's own rule (<see cref="LongQueryTraceDatabases.CanHoldSession"/>), and
    /// counts the databases it lists without master, so a logical server whose user databases are all monitored
    /// separately is not left reading master alone, and a list of master alone is not blamed on them (#4961). The
    /// behaviour is driven in <see cref="EmptyDatabaseListNoteLiteTests"/>; pinned here in the source as well, so the rule
    /// stays the trace's own and not a second copy in the runner.
    /// </summary>
    [Fact]
    public void TheLongQueryRead_AlsoSkipsMaster_ByTheTracesOwnRule()
    {
        var runner = ReadLf(Path.Combine("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs"));
        var skip = runner.IndexOf("if (definition.SkipsSeparatelyMonitoredDatabases)", StringComparison.Ordinal);
        var note = runner.IndexOf("EmptyDatabaseListNote.For(", skip, StringComparison.Ordinal);
        Assert.True(skip > 0 && note > skip, "the skip comes before the note's inputs");

        var block = runner[skip..note];
        var master = block.IndexOf("databases = databases.FindAll(LongQueryTraceDatabases.CanHoldSession);", StringComparison.Ordinal);
        var counted = block.IndexOf("listedDatabaseCount = databases.Count;", StringComparison.Ordinal);
        var separately = block.IndexOf("databases = WithoutSeparatelyMonitoredDatabases(server, databases);", StringComparison.Ordinal);
        Assert.True(master > 0 && counted > master && separately > counted,
            "master leaves the list, then the list is counted, then the separately monitored databases leave");
        Assert.DoesNotContain("\"master\"", block, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("master", false)]
    [InlineData("MASTER", false)]
    [InlineData("alpha", true)]
    [InlineData("masters", true)]
    public void TheTrace_CanHoldASessionInEveryDatabaseButMaster(string database, bool expected)
    {
        Assert.Equal(expected, LongQueryTraceDatabases.CanHoldSession(database));
    }

    [Fact]
    public void ThePlan_CreatesTheSessionWhereTheReadReads_NeverInMaster()
    {
        var listed = new[] { "master", "alpha", "zeta" };

        var plan = LongQueryTraceDatabases.Plan(
            enabled: true, listed, listed, Array.Empty<string>(), Array.Empty<string>());

        Assert.Equal(listed.Where(LongQueryTraceDatabases.CanHoldSession), plan.Create);
        Assert.DoesNotContain("master", plan.Create, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Guard, not RED first: the always-on deadlock and blocked-process sessions keep their inventory lifecycle. Their
    /// ensures still list the databases themselves, with the server's exclusions and nothing else, so an exclusion or
    /// ownership change that moves the long-query trace does not move them.
    /// </summary>
    [Fact]
    public void TheAlwaysOnSessions_StillFollowTheInventory()
    {
        var shared = ReadLf(Path.Combine("Lite", "Services", "RemoteCollectorService.BlockedProcessReport.cs"));
        Assert.Contains("databases = await GetAzureDatabaseListAsync(server, cancellationToken);", shared, StringComparison.Ordinal);

        foreach (var (file, capture) in new[] { ("RemoteCollectorService.BlockedProcessReport.cs", "\"blocked process\""), ("RemoteCollectorService.Deadlocks.cs", "\"deadlock\"") })
        {
            var source = ReadLf(Path.Combine("Lite", "Services", file));
            var call = source.IndexOf("await EnsureDatabaseScopedXeSessionsAsync(", StringComparison.Ordinal);
            Assert.True(call > 0, $"{file} lost its database-scoped ensure");
            var args = source[call..source.IndexOf(';', call)];
            Assert.Contains(capture, args, StringComparison.Ordinal);
            /* #4961: the arms also hand the shared driver their per-database routine, which is no list of databases. */
            Assert.EndsWith("databaseName, token))", args, StringComparison.Ordinal);
            Assert.DoesNotContain("SeparatelyMonitored", args, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The edition is read once for a reconcile. A connection check that writes a blank edition and then 5 again, between the
    /// reconcile's first read and its drop, must not send the drop down the Azure branch with no registrations: that drop
    /// listed nothing as kept and dropped the session from a database another registration still uses.
    /// </summary>
    [Fact]
    public async Task Off_AnEditionThatChangesDuringTheReconcile_StillLeavesTheDatabasesAnotherRegistrationKeeps()
    {
        var rig = await BuildRigAsync(traceOn: false);
        var other = RegisterOther(rig, database: null, traceOn: false, readOnlyIntent: true);

        /* An earlier cycle read the edition. With both traces off nothing is kept, so the drop takes every database. */
        await rig.ReconcileAsync();
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);

        /* The other registration turns its trace on, so the plan runs again. A failed check wrote a blank edition, and the
           next check wrote 5 between this reconcile's first read of it and its drop (the clock is read in between). */
        SetTrace(rig, other, traceOn: true);
        var status = rig.Servers.GetConnectionStatus(rig.Server.Id);
        status.SqlEngineEdition = 0;
        rig.Service.LongQueryTraceUtcNowForTests = () =>
        {
            status.SqlEngineEdition = 5;
            return rig.Clock;
        };
        rig.Calls.Clear();

        await rig.ReconcileAsync();

        Assert.Empty(rig.Dropped);
    }

    /// <summary>
    /// The enumeration behind the Azure database list has the server's exclusion clause and parameters only when asked for.
    /// The trace's drops ask for none, so a session created before a database was excluded is still dropped there. The
    /// lifecycle rigs replace the listing, so this reads the query the real listing runs.
    /// </summary>
    [Fact]
    public void TheAzureDatabaseList_AppliesTheServersExclusionsOnlyWhenAsked()
    {
        var server = new ServerConnection { ServerName = Host, ExcludedDatabases = new List<string> { "scratch", "tempdb_clone" } };

        var (everySql, everyParameters) = RemoteCollectorService.BuildAzureDatabaseListQuery(server, applyExclusions: false);
        Assert.DoesNotContain("NOT IN", everySql, StringComparison.Ordinal);
        Assert.Empty(everyParameters);

        var (monitoredSql, monitoredParameters) = RemoteCollectorService.BuildAzureDatabaseListQuery(server, applyExclusions: true);
        Assert.Contains("name NOT IN (@excl_db_0, @excl_db_1)", monitoredSql, StringComparison.Ordinal);
        Assert.Equal(2, monitoredParameters.Count);
    }

    private static string ReadLf(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, relativePath)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relativePath)).Replace("\r\n", "\n");
    }

    /* ── #4961: the session is this install's own, named from its id ── */

    private static string OwnSession(string? installId) =>
        LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.LiteProduct, installId);

    [Fact]
    public async Task Azure_EveryPass_NamesOnlyThisInstallsSession()
    {
        /* "gamma" is excluded, so the create pass drops the session outside the monitored set, and it refuses that drop until
           the cap gives up, so the hourly attempt after the cap runs too. */
        var rig = await BuildRigAsync(traceOn: true, "gamma");
        rig.Refuse.Add("gamma");
        for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
        {
            await rig.ReconcileAsync();
        }

        rig.Clock += LongQueryTraceDatabases.RetryInterval;
        await rig.ReconcileAsync();

        /* Off: the drop in each listed database. */
        rig.Schedules.UpdateSchedule("long_query_completions", enabled: false);
        rig.Refuse.Clear();
        await rig.ReconcileAsync();

        Assert.Contains(rig.Calls, c => c.Create);
        Assert.Contains(rig.Calls, c => !c.Create);
        Assert.Equal(rig.Calls.Count, rig.Names.Count);
        Assert.All(rig.Names, name => Assert.Equal(OwnSession(rig.InstallId), name));
        Assert.DoesNotContain(rig.Names, name => name == LongQueryCompletionsCollector.LegacyXeSessionName);

        /* The legacy session is dropped once in each listed database but master, the excluded one included, and never again. */
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.LegacyCalls);
    }

    [Fact]
    public async Task OnPrem_TheEnsureRunsOnEachCycle_AndTheDrop_NameThisInstallsSession()
    {
        var rig = await BuildOnPremRigAsync(traceOn: true);

        /* Lite ensures on every cycle, so a stopped session is started again on the next one (STARTUP_STATE = OFF). */
        await rig.ReconcileAsync();
        await rig.ReconcileAsync();
        Assert.Equal(2, rig.Calls.Count(c => c.Create));

        rig.Schedules.UpdateSchedule("long_query_completions", enabled: false);
        await rig.ReconcileAsync();

        Assert.Single(rig.Calls, c => !c.Create);
        Assert.Equal(3, rig.Names.Count);
        Assert.All(rig.Names, name => Assert.Equal(OwnSession(rig.InstallId), name));

        /* The legacy session is dropped once on the server, with no database named, across all three cycles. */
        Assert.Equal(new[] { string.Empty }, rig.LegacyCalls);
    }

    [Theory]
    [InlineData(5)]
    [InlineData(3)]
    public async Task NoInstallId_NothingIsCreated_AndTheRunRecordsWhy(int engineEdition)
    {
        var rig = await BuildRigAsync(
            new ServerConnection { ServerName = Host, DisplayName = "lqtrace-noid" }, traceOn: true, engineEdition, withInstallId: false);
        Assert.Null(rig.InstallId);

        await rig.ReconcileAsync();

        Assert.Empty(rig.Calls);
        Assert.Empty(rig.Names);
        Assert.Null(rig.Applied);
        var fault = rig.Service.LongQueryTraceFaultState(rig.Server.Id);
        Assert.NotNull(fault);
        Assert.Contains("no id", fault!.Message, StringComparison.Ordinal);

        /* Off: there is no session of this install's to drop, and no fault is left for a run that is not dispatched. */
        rig.Schedules.UpdateSchedule("long_query_completions", enabled: false);
        await rig.ReconcileAsync();

        Assert.Empty(rig.Calls);
        Assert.Null(rig.Service.LongQueryTraceFaultState(rig.Server.Id));
    }

    /* ── #4964: the collector's own line for the trace's failed create follows the create's lines ── */

    private const string TraceCollector = "long_query_completions";

    /// <summary>The collector's own line for a trace fault that is an ensure failure, as <c>RunCollectorAsync</c> writes it.</summary>
    private const string CollectorEnsureLine = "long_query_completions Failed to ensure long query completions XE session";

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

    /// <summary>One collection cycle for the trace: the reconcile the per-server loop runs first, then the collector's run.</summary>
    private static async Task ReconcileAndRunAsync(Rig rig)
    {
        await rig.ReconcileAsync();
        await rig.Service.RunCollectorAsync(rig.Server, TraceCollector, CancellationToken.None);
    }

    private static void RefuseEveryDatabase(Rig rig, int? number)
    {
        rig.RefusalNumber = number;
        foreach (var database in new[] { "alpha", "beta", "gamma" })
        {
            rig.Refuse.Add(database);
        }
    }

    /// <summary>The collection_log rows the test's server has for the long-query collector, oldest first.</summary>
    private static async Task<List<(string Status, string? Error)>> ReadRunsAsync(Rig rig)
    {
        using var connection = rig.DuckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT status, error_message FROM collection_log WHERE server_id = {rig.ServerId} AND collector_name = '{TraceCollector}' ORDER BY log_id";
        using var reader = await command.ExecuteReaderAsync();
        var runs = new List<(string, string?)>();
        while (await reader.ReadAsync())
        {
            runs.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        return runs;
    }

    /// <summary>
    /// A create refused in every database on every cycle. The collector's run rethrows the fault the reconcile kept, so its
    /// own line follows the create's lines: the first failing cycle logs it at Error, and the cycles after it log it at
    /// Debug. Every run still records ERROR with the same message, counts toward the collector's consecutive errors, and
    /// flags the XE session unavailable.
    /// </summary>
    [Fact]
    public async Task ACollectorWhoseTraceCreateFailsOnEveryCycle_LogsItsOwnLineOnce_ThenDebug()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync(traceOn: true);
            RefuseEveryDatabase(rig, number: 4060);
            var lines = new List<string>();

            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "ERROR", CollectorEnsureLine));

            await ReconcileAndRunAsync(rig);
            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));

            Assert.Equal(1, Count(lines, "ERROR", CollectorEnsureLine));
            Assert.Equal(2, Count(lines, "DEBUG", CollectorEnsureLine));

            /* The run row and its classification are the same on every cycle, and so is the retry. */
            var runs = await ReadRunsAsync(rig);
            Assert.Equal(3, runs.Count);
            Assert.All(runs, run =>
            {
                Assert.Equal("ERROR", run.Status);
                Assert.Contains("Failed to ensure long query completions XE session", run.Error, StringComparison.Ordinal);
            });
            Assert.Equal(9, rig.Created.Count());

            var failure = Assert.Single(rig.Service.GetHealthSummary(rig.ServerId).XeSessionFailures);
            Assert.Equal(3, failure.ConsecutiveErrors);
        });
    }

    /// <summary>
    /// A permission refusal is classified PERMISSIONS, and its collector line is a Warning the first time and Debug after it.
    /// The scheduler skips a collector that was refused for permission, so the second run happens only once the server's
    /// health is cleared, as removing the server does.
    /// </summary>
    [Fact]
    public async Task ACollectorRefusedForPermission_LogsItsOwnLineAtWarning_ThenDebug()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync(traceOn: true);
            RefuseEveryDatabase(rig, number: 262);
            var lines = new List<string>();

            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "WARN", CollectorEnsureLine));

            rig.Service.ClearHealthForServer(rig.ServerId);
            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));

            Assert.Equal(1, Count(lines, "WARN", CollectorEnsureLine));
            Assert.Equal(1, Count(lines, "DEBUG", CollectorEnsureLine));
            Assert.Equal(0, Count(lines, "ERROR", CollectorEnsureLine));

            var runs = await ReadRunsAsync(rig);
            Assert.Equal(2, runs.Count);
            Assert.All(runs, run => Assert.Equal("PERMISSIONS", run.Status));
        });
    }

    /// <summary>
    /// A create that succeeds ends the run of failures, so the next failing run logs its own line at Error again, and its
    /// repeat at Debug.
    /// </summary>
    [Fact]
    public async Task ACollectorWhoseTraceCreateFailsAgainAfterItSucceeded_LogsItsOwnLineAtErrorAgain()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync(traceOn: true);
            RefuseEveryDatabase(rig, number: 4060);
            var lines = new List<string>();

            await ReconcileAndRunAsync(rig);
            await ReconcileAndRunAsync(rig);

            /* The create succeeds in every database. (A run would go on to read the server, so only the reconcile runs.) */
            rig.Refuse.Clear();
            await rig.ReconcileAsync();
            Assert.Null(KeptFault(rig));

            RefuseEveryDatabase(rig, number: 4060);
            await ReconcileAndRunAsync(rig);
            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));

            Assert.Equal(2, Count(lines, "ERROR", CollectorEnsureLine));
            Assert.Equal(2, Count(lines, "DEBUG", CollectorEnsureLine));
            Assert.Equal(4, (await ReadRunsAsync(rig)).Count(run => run.Status == "ERROR"));
        });
    }

    /// <summary>
    /// An install with no id has no session to create, and the reconcile keeps that as the fault the run records on every
    /// cycle. The run's two lines for it (a plain error, not a SQL error) follow the same rule as the create's.
    /// </summary>
    [Fact]
    public async Task ACollectorWithNoInstallId_LogsItsOwnLinesOnce_ThenDebug()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync(
                new ServerConnection { ServerName = Host, DisplayName = "lqtrace-noid-" + Guid.NewGuid().ToString("N")[..8] },
                traceOn: true, engineEdition: 5, withInstallId: false);
            const string typeLine = "long_query_completions InvalidOperationException: The long-query trace was not created";
            const string failedLine = "Collector 'long_query_completions' failed for server";
            var lines = new List<string>();

            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "ERROR", typeLine));
            Assert.Equal(1, Count(lines, "ERROR", failedLine));

            await ReconcileAndRunAsync(rig);
            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));

            Assert.Equal(1, Count(lines, "ERROR", typeLine));
            Assert.Equal(1, Count(lines, "ERROR", failedLine));
            Assert.Equal(2, Count(lines, "DEBUG", typeLine));
            Assert.Equal(2, Count(lines, "DEBUG", failedLine));

            var runs = await ReadRunsAsync(rig);
            Assert.Equal(3, runs.Count);
            Assert.All(runs, run =>
            {
                Assert.Equal("ERROR", run.Status);
                Assert.Contains("no id", run.Error, StringComparison.Ordinal);
            });
        });
    }

    /// <summary>
    /// A server-scoped create (on-premises, Managed Instance, RDS) that fails with a SQL error keeps that error as it is,
    /// and the run reaches its SQL error arm. That arm's two lines follow the create's rule too.
    /// </summary>
    [Fact]
    public async Task ACollectorWhoseServerScopedCreateFailsWithASqlError_LogsItsOwnLinesOnce_ThenDebug()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildOnPremRigAsync(traceOn: true);

            /* The server-scoped create names no database. */
            rig.Refuse.Add(string.Empty);
            rig.RefusalNumber = 1105;
            const string sqlErrorLine = "long_query_completions SQL Error #1105";
            const string failedLine = "Collector 'long_query_completions' SQL error #1105 for server";
            var lines = new List<string>();

            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));
            Assert.Equal(1, Count(lines, "ERROR", sqlErrorLine));
            Assert.Equal(1, Count(lines, "ERROR", failedLine));

            await ReconcileAndRunAsync(rig);
            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));

            Assert.Equal(1, Count(lines, "ERROR", sqlErrorLine));
            Assert.Equal(1, Count(lines, "ERROR", failedLine));
            Assert.Equal(2, Count(lines, "DEBUG", sqlErrorLine));
            Assert.Equal(2, Count(lines, "DEBUG", failedLine));

            var runs = await ReadRunsAsync(rig);
            Assert.Equal(3, runs.Count);
            Assert.All(runs, run => Assert.Equal("ERROR", run.Status));
        });
    }

    /// <summary>
    /// A ring-buffer read that fails while the session exists is not a failed create: nothing was kept by the reconcile,
    /// so every cycle's run logs its line for the failed read at Error, the level it always had. Only a kept create
    /// failure repeats at Debug.
    /// </summary>
    [Fact]
    public async Task AFailedRingBufferRead_KeepsItsLevelOnEveryCycle()
    {
        await WithDebugLoggingAsync(async () =>
        {
            var rig = await BuildRigAsync(traceOn: true);
            rig.Service.AzureDatabaseListOverrideForTests = (_, _) => Task.FromResult(new List<string> { "alpha", "beta" });
            rig.Service.AzureDatabaseReaderOverrideForTests = (database, _) =>
                throw SqlExceptionFactory.Create(50001, errorClass: 16, message: $"The long-query XE session cannot be read in {database}.");
            const string readLine = "long_query_completions Failed to read long query completions XE session";
            var lines = new List<string>();

            await ReconcileAndRunAsync(rig);
            await ReconcileAndRunAsync(rig);
            await ReconcileAndRunAsync(rig);
            lines.AddRange(Lines(rig));

            /* The create succeeded each time, so no fault was kept; each run's read failed at its own level. */
            Assert.Null(KeptFault(rig));
            Assert.Equal(3, Count(lines, "ERROR", readLine));
            Assert.Equal(0, Count(lines, "DEBUG", readLine));

            var runs = await ReadRunsAsync(rig);
            Assert.Equal(3, runs.Count);
            Assert.All(runs, run =>
            {
                Assert.Equal("ERROR", run.Status);
                Assert.Contains("Failed to read long query completions XE session", run.Error, StringComparison.Ordinal);
            });
        });
    }

    /* ── #4961: another registration of the same instance keeps the session (L6), and a removed server drops its own (R4) ── */

    private const string InstanceName = "SQL01";

    /// <summary>Seeds the identity row a wait_stats (or cpu_utilization) run persists for one registration.</summary>
    private static async Task SeedNameAsync(Rig rig, ServerConnection server, string? name, string collector = "wait_stats")
    {
        using var conn = rig.DuckDb.CreateConnection();
        await conn.OpenAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT OR REPLACE INTO collector_state (server_id, collector_name, state_key, state_value, updated_at) VALUES ($1,$2,$3,$4,$5)";
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = RemoteCollectorService.GetServerId(server) });
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = ServerEpoch.IdentityStateKey });
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter
        {
            Value = ServerEpoch.Serialize(new ServerEpoch.Stamp(new DateTime(2026, 10, 2, 8, 0, 0, DateTimeKind.Utc), name)),
        });
        cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = DateTime.UtcNow });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Another on-premises registration of this install, under another host name, with its own trace setting and the
    /// name its instance last reported (null: it has reported none).
    /// </summary>
    private static async Task<ServerConnection> RegisterOnPremOtherAsync(Rig rig, bool traceOn, string? name, bool enabled = true, string collector = "wait_stats")
    {
        var other = new ServerConnection
        {
            ServerName = "lqtrace-sql-alias-" + Guid.NewGuid().ToString("N")[..8],
            DisplayName = "lqtrace-alias-" + Guid.NewGuid().ToString("N")[..8],
            IsEnabled = enabled,
        };
        rig.Servers.AddServer(other);
        SetTrace(rig, other, traceOn);
        if (name is not null)
        {
            await SeedNameAsync(rig, other, name, collector);
        }

        return other;
    }

    /// <summary>
    /// Test 21: a registration whose trace is off does not drop the session another registration of the same instance
    /// keeps. The match is positive: both registrations' last-known names are known and agree, ignoring case, and the other
    /// registration is monitored with its trace on. Anything less drops, as before.
    /// </summary>
    [Theory]
    [InlineData("SQL01", "sql01", true, true, false)]
    [InlineData("SQL01", "SQL01", true, true, false)]
    [InlineData("SQL01", "SQL01", true, false, true)]
    [InlineData("SQL01", "SQL01", false, true, true)]
    [InlineData("SQL01", "SQL02", true, true, true)]
    [InlineData("SQL01", null, true, true, true)]
    [InlineData(null, "SQL01", true, true, true)]
    public async Task Off_OnPremises_LeavesTheSession_WhileAnotherRegistrationOfTheSameInstanceKeepsIt(
        string? ownName, string? otherName, bool otherEnabled, bool otherTraceOn, bool expectDrop)
    {
        var rig = await BuildOnPremRigAsync(traceOn: false);
        if (ownName is not null)
        {
            await SeedNameAsync(rig, rig.Server, ownName);
        }

        await RegisterOnPremOtherAsync(rig, otherTraceOn, otherName, otherEnabled);

        await rig.ReconcileAsync();

        Assert.Equal(expectDrop ? 1 : 0, rig.Calls.Count(c => !c.Create));
        Assert.False(rig.Applied);

        /* Done either way: the next cycle runs nothing. */
        rig.Calls.Clear();
        await rig.ReconcileAsync();
        Assert.Empty(rig.Calls);
    }

    [Fact]
    public async Task Off_OnPremises_ANameOnlyTheSecondCarrierHolds_StillMatches()
    {
        var rig = await BuildOnPremRigAsync(traceOn: false);
        await SeedNameAsync(rig, rig.Server, InstanceName, collector: "cpu_utilization");
        await RegisterOnPremOtherAsync(rig, traceOn: true, InstanceName, collector: "cpu_utilization");

        await rig.ReconcileAsync();

        Assert.Empty(rig.Calls);
    }

    /// <summary>The install's default is on and the other registration overrides it to off: its effective setting is off, so it keeps nothing.</summary>
    [Fact]
    public async Task Off_OnPremises_AnotherRegistrationWithItsOwnSettingOff_KeepsNothing_WhateverTheInstallsDefault()
    {
        var rig = await BuildOnPremRigAsync(traceOn: true);
        SetTrace(rig, rig.Server, traceOn: false);
        await SeedNameAsync(rig, rig.Server, InstanceName);
        await RegisterOnPremOtherAsync(rig, traceOn: false, InstanceName);

        await rig.ReconcileAsync();

        Assert.Single(rig.Calls, c => !c.Create);
    }

    /// <summary>The install's default is off and the other registration turns its own on: its effective setting is on, so it keeps the session.</summary>
    [Fact]
    public async Task Off_OnPremises_AnotherRegistrationWithItsOwnSettingOn_KeepsTheSession_WhateverTheInstallsDefault()
    {
        var rig = await BuildOnPremRigAsync(traceOn: false);
        await SeedNameAsync(rig, rig.Server, InstanceName);
        await RegisterOnPremOtherAsync(rig, traceOn: true, InstanceName);

        await rig.ReconcileAsync();

        Assert.Empty(rig.Calls);
    }

    /// <summary>The removal begins, and a reconcile that comes after it does not create the session again.</summary>
    [Fact]
    public async Task AfterTheRemovalsDrop_TheReconcile_DoesNotCreateTheSessionAgain()
    {
        var rig = await BuildRigAsync(traceOn: true);
        await rig.ReconcileAsync();
        await RemoveAsync(rig);
        rig.Calls.Clear();

        await rig.ReconcileAsync();

        Assert.Empty(rig.Calls);
    }

    /// <summary>The service's session drop for a removed server, the way the removal calls it.</summary>
    private static Task RemoveAsync(Rig rig) =>
        rig.Service.DropLongQueryTraceOfRemovedServerAsync(rig.Server, CancellationToken.None);

    /// <summary>Test 20, Azure: the removal drops this install's session in each listed database but master, and leaves the databases another registration keeps.</summary>
    [Fact]
    public async Task Removal_Azure_DropsThisInstallsSessionEverywhere_ExceptWhereAnotherRegistrationKeepsIt()
    {
        var rig = await BuildRigAsync(traceOn: true);
        await rig.ReconcileAsync();
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Created);
        /* A second registration of the logical server, with its trace on, keeps the session in the one database it does not exclude. */
        RegisterOther(rig, null, traceOn: true, readOnlyIntent: true, "alpha", "gamma");
        rig.Calls.Clear();
        rig.Names.Clear();

        await RemoveAsync(rig);

        Assert.Equal(new[] { "alpha", "gamma" }, rig.Dropped);
        Assert.Equal(rig.Calls.Count, rig.Names.Count);
        Assert.All(rig.Names, name => Assert.Equal(LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.LiteProduct, rig.InstallId!), name));
        Assert.DoesNotContain(rig.Names, name => name == LongQueryCompletionsCollector.LegacyXeSessionName);
    }

    [Fact]
    public async Task Removal_OnPremises_DropsThisInstallsServerScopedSession_Once()
    {
        var rig = await BuildOnPremRigAsync(traceOn: true);
        await SeedNameAsync(rig, rig.Server, InstanceName);
        await rig.ReconcileAsync();
        rig.Calls.Clear();
        rig.Names.Clear();

        await RemoveAsync(rig);

        var drop = Assert.Single(rig.Calls);
        Assert.False(drop.Create);
        Assert.Equal(string.Empty, drop.Database);
        Assert.Equal(LongQueryCompletionsCollector.XeSessionNameFor(LongQueryCompletionsCollector.LiteProduct, rig.InstallId!), Assert.Single(rig.Names));
    }

    [Fact]
    public async Task Removal_OnPremises_LeavesTheSession_WhileAnotherRegistrationOfTheSameInstanceKeepsIt_AndSaysSo()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Information);
            var rig = await BuildOnPremRigAsync(traceOn: true);
            await SeedNameAsync(rig, rig.Server, InstanceName);
            await RegisterOnPremOtherAsync(rig, traceOn: true, "sql01");
            await rig.ReconcileAsync();
            rig.Calls.Clear();
            AppLogger.DrainBufferedLines();

            await RemoveAsync(rig);

            Assert.Empty(rig.Calls);
            Assert.Contains(AppLogger.DrainBufferedLines(), line =>
                line.Contains(rig.Server.DisplayName, StringComparison.Ordinal)
                && line.Contains("INFO", StringComparison.Ordinal)
                && line.Contains("keeps it on the same instance", StringComparison.Ordinal));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Removal_OnPremises_WithNoNameKnown_LeavesTheSession_WhenAnotherRegistrationCouldKeepIt_AndSaysWhy()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Information);
            var rig = await BuildOnPremRigAsync(traceOn: true);
            await RegisterOnPremOtherAsync(rig, traceOn: true, InstanceName);
            await rig.ReconcileAsync();
            rig.Calls.Clear();
            AppLogger.DrainBufferedLines();

            await RemoveAsync(rig);

            Assert.Empty(rig.Calls);
            Assert.Contains(AppLogger.DrainBufferedLines(), line =>
                line.Contains(rig.Server.DisplayName, StringComparison.Ordinal)
                && line.Contains("INFO", StringComparison.Ordinal)
                && line.Contains("instance name is not known", StringComparison.Ordinal));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Removal_OnPremises_WithNoNameKnown_StillDrops_WhenNoOtherRegistrationCouldKeepIt()
    {
        var rig = await BuildOnPremRigAsync(traceOn: true);
        await RegisterOnPremOtherAsync(rig, traceOn: false, InstanceName);
        await rig.ReconcileAsync();
        rig.Calls.Clear();

        await RemoveAsync(rig);

        Assert.Single(rig.Calls, c => !c.Create);
    }

    [Fact]
    public async Task Removal_OnPremises_DropsTheSession_WhenTheOtherRegistrationOfTheInstanceHasItsTraceOff()
    {
        var rig = await BuildOnPremRigAsync(traceOn: true);
        await SeedNameAsync(rig, rig.Server, InstanceName);
        await RegisterOnPremOtherAsync(rig, traceOn: false, InstanceName);
        await rig.ReconcileAsync();
        rig.Calls.Clear();

        await RemoveAsync(rig);

        Assert.Single(rig.Calls, c => !c.Create);
    }

    /// <summary>A server whose last finished reconcile dropped the session has none to remove, so the removal opens no connection to it.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Removal_OfAServerWhoseTraceIsOff_TouchesNothing(bool azureSqlDatabase)
    {
        var rig = azureSqlDatabase ? await BuildRigAsync(traceOn: false) : await BuildOnPremRigAsync(traceOn: false);
        await rig.ReconcileAsync();
        rig.Calls.Clear();
        var listed = rig.ListCalls;

        await RemoveAsync(rig);

        Assert.Empty(rig.Calls);
        Assert.Equal(listed, rig.ListCalls);
    }

    /// <summary>One attempt, and nothing it hits stops the removal: a refused drop and a token that has run out both return.</summary>
    [Fact]
    public async Task Removal_ADropThatFails_OrTimesOut_NeverThrows_AndTriesOnce()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Warning);
            var rig = await BuildRigAsync(traceOn: true);
            await rig.ReconcileAsync();
            rig.Calls.Clear();
            rig.Refuse.Add("beta");
            AppLogger.DrainBufferedLines();

            await RemoveAsync(rig);

            Assert.Equal(new[] { "alpha", "beta", "gamma" }, rig.Dropped);
            Assert.Contains(AppLogger.DrainBufferedLines(), line =>
                line.Contains(rig.Server.DisplayName, StringComparison.Ordinal)
                && line.Contains("WARN", StringComparison.Ordinal)
                && line.Contains("removed", StringComparison.OrdinalIgnoreCase));

            rig.Calls.Clear();
            using var spent = new CancellationTokenSource();
            spent.Cancel();
            await rig.Service.DropLongQueryTraceOfRemovedServerAsync(rig.Server, spent.Token);
            Assert.Empty(rig.Calls);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Removal_WithNoInstallId_DropsNothing()
    {
        var rig = await BuildRigAsync(
            new ServerConnection { ServerName = "lqtrace-sql", DisplayName = "lqtrace-noid-" + Guid.NewGuid().ToString("N")[..8] },
            traceOn: true, engineEdition: 3, withInstallId: false);

        await RemoveAsync(rig);

        Assert.Empty(rig.Calls);
    }

    private sealed class RecordingDatabase : IAlwaysOnXeDatabase
    {
        private readonly List<string> _statements;

        public RecordingDatabase(List<string> statements) => _statements = statements;

        public Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken) =>
            Task.FromResult(AlwaysOnXeCatalog.Present);

        public Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken) => Task.FromResult(true);

        public Task ExecuteAsync(string statement, CancellationToken cancellationToken)
        {
            _statements.Add(statement);
            return Task.CompletedTask;
        }

        public bool IsAlreadyPresent(Exception exception) => false;
    }

    /// <summary>
    /// Test 20, Azure's fallbacks: the deadlock and blocked-process sessions this install chose for itself in the removed
    /// server's databases are dropped by their own names, except in a database where another registration of this install
    /// reads its own session. A database where the install reads the shared session is left alone: the shared session is never dropped.
    /// </summary>
    [Fact]
    public async Task Removal_Azure_DropsTheDeadlockAndBlockedProcessSessionsThisInstallChose_NeverTheSharedOnes()
    {
        var rig = await BuildRigAsync(traceOn: false);
        await rig.ReconcileAsync();
        var other = RegisterOther(rig, null, traceOn: false, readOnlyIntent: true);
        var choices = rig.Service.AlwaysOnChoices;
        choices.Set(rig.Server.Id, "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(rig.Server.Id, "beta", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(rig.Server.Id, "beta", AlwaysOnXeSessionKind.BlockedProcess, AlwaysOnXeChoice.Own);
        choices.Set(rig.Server.Id, "gamma", AlwaysOnXeSessionKind.BlockedProcess, AlwaysOnXeChoice.Shared);
        choices.Set(other.Id, "beta", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);

        var statements = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        rig.Service.AlwaysOnXeDatabaseForTests = (_, database, _) =>
        {
            if (!statements.TryGetValue(database, out var list))
            {
                statements[database] = list = new List<string>();
            }

            return Task.FromResult<IAlwaysOnXeDatabase>(new RecordingDatabase(list));
        };

        await RemoveAsync(rig);

        string Drop(AlwaysOnXeSessionKind kind) =>
            AlwaysOnXeSessions.BuildAzureDropSql(kind, AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.LiteProduct, rig.InstallId, kind));
        Assert.Equal(new[] { Drop(AlwaysOnXeSessionKind.Deadlock) }, statements["alpha"]);
        Assert.Equal(new[] { Drop(AlwaysOnXeSessionKind.BlockedProcess) }, statements["beta"]);
        Assert.False(statements.ContainsKey("gamma"));
        Assert.DoesNotContain(statements.Values.SelectMany(s => s), s => s.Contains("[" + AlwaysOnXeSessions.SharedNameFor(AlwaysOnXeSessionKind.Deadlock) + "]", StringComparison.Ordinal));
        Assert.Empty(choices.OwnDatabases(rig.Server.Id, AlwaysOnXeSessionKind.Deadlock));
        Assert.Empty(choices.OwnDatabases(rig.Server.Id, AlwaysOnXeSessionKind.BlockedProcess));
    }

    /// <summary>
    /// A database of the same name on another logical server is another database, so it holds nothing back: only another
    /// registration of the removed server's own logical server reads its own session in a database of this server. The server
    /// name compares ignoring case and padding.
    /// </summary>
    [Fact]
    public async Task Removal_Azure_TheFallbackIsHeldBackOnlyByARegistrationOfTheSameServer_NotByADatabaseOfTheSameNameElsewhere()
    {
        var rig = await BuildRigAsync(traceOn: false);
        await rig.ReconcileAsync();
        var sameServer = new ServerConnection
        {
            ServerName = " " + Host.ToUpperInvariant() + " ",
            DisplayName = "lqtrace-same-" + Guid.NewGuid().ToString("N")[..8],
            ReadOnlyIntent = true,
        };
        var otherServer = new ServerConnection
        {
            ServerName = "lqtrace-other.database.windows.net",
            DisplayName = "lqtrace-elsewhere-" + Guid.NewGuid().ToString("N")[..8],
        };
        rig.Servers.AddServer(sameServer);
        rig.Servers.AddServer(otherServer);
        var choices = rig.Service.AlwaysOnChoices;
        choices.Set(rig.Server.Id, "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(rig.Server.Id, "beta", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(sameServer.Id, "BETA", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);
        choices.Set(otherServer.Id, "alpha", AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeChoice.Own);

        var statements = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);
        rig.Service.AlwaysOnXeDatabaseForTests = (_, database, _) =>
        {
            if (!statements.TryGetValue(database, out var list))
            {
                statements[database] = list = new List<string>();
            }

            return Task.FromResult<IAlwaysOnXeDatabase>(new RecordingDatabase(list));
        };

        await RemoveAsync(rig);

        var drop = AlwaysOnXeSessions.BuildAzureDropSql(
            AlwaysOnXeSessionKind.Deadlock,
            AlwaysOnXeSessions.OwnNameFor(LongQueryCompletionsCollector.LiteProduct, rig.InstallId, AlwaysOnXeSessionKind.Deadlock));
        Assert.Equal(new[] { drop }, statements["alpha"]);
        Assert.False(statements.ContainsKey("beta"));

        /* The other server's own choice is its own to keep or forget. */
        Assert.Single(choices.OwnDatabases(otherServer.Id, AlwaysOnXeSessionKind.Deadlock), database => string.Equals(database, "alpha", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// The removal drops the session after the tag clear and before the block that drops the server's state, and gives the
    /// whole step 15 seconds. The block that follows still awaits nothing (ConnectionAlertRetryInFlightTests).
    /// </summary>
    [Fact]
    public void TheRemoval_DropsTheSession_AfterTheTagClear_BeforeTheStateDrops_WithinFifteenSeconds()
    {
        var window = ReadLf("Lite/MainWindow.xaml.cs");
        var start = window.IndexOf("private async Task RemoveServerAsync(ServerConnection server)", StringComparison.Ordinal);
        Assert.True(start >= 0, "the window has no RemoveServerAsync");
        var removal = window[start..];

        var tags = removal.IndexOf("ClearServerTagsForServerAsync(removedServerId)", StringComparison.Ordinal);
        var drop = removal.IndexOf("DropLongQueryTraceOfRemovedServerAsync(server", StringComparison.Ordinal);
        var state = removal.IndexOf("_collectorService?.ClearHealthForServer(removedServerId);", StringComparison.Ordinal);
        Assert.True(tags >= 0 && drop > tags && state > drop, "the drop must sit between the tag clear and the state drops");
        Assert.Contains("RemoteCollectorService.LongQueryTraceRemovalTimeout", removal[tags..state], StringComparison.Ordinal);
        Assert.Equal(TimeSpan.FromSeconds(15), RemoteCollectorService.LongQueryTraceRemovalTimeout);
    }

    [Fact]
    public void TheEnsures_StartASessionTheyFindStopped_ByThisInstallsName_AndNothingNamesTheLegacySession()
    {
        var source = ReadLf("Lite/Services/RemoteCollectorService.LongQueryCompletions.cs");

        /* On-prem: the existence check also reports whether it runs, and a stopped one is started by name. */
        Assert.Contains("is_running = CASE WHEN dxs.name IS NOT NULL THEN 1 ELSE 0 END", source, StringComparison.Ordinal);
        Assert.Contains("isRunning == 0", source, StringComparison.Ordinal);
        Assert.Contains("BuildStartSessionSql(sessionName, databaseScoped: false)", source, StringComparison.Ordinal);

        /* Azure SQL Database: a session that exists and does not run is started by name. */
        Assert.Contains("FROM sys.dm_xe_database_sessions AS xes", source, StringComparison.Ordinal);
        Assert.Contains("ALTER EVENT SESSION [{sessionName}] ON DATABASE STATE = START;", source, StringComparison.Ordinal);

        Assert.DoesNotContain("LegacyXeSessionName", source, StringComparison.Ordinal);
        Assert.DoesNotContain("LongQueryCompletionsCollector.XeSessionName", source, StringComparison.Ordinal);
    }

    /* ── #4961: the session that versions before this one shared between installs is dropped once, then recorded ── */

    private static readonly string[] AllDatabases = { "alpha", "beta", "gamma" };

    /// <summary>The drop records the store holds for this registration: (state key, state value), by key.</summary>
    private async Task<List<(string Key, string Value)>> RecordsAsync(Rig rig)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT state_key, state_value
FROM collector_state
WHERE server_id = $1
AND   collector_name = $2
AND   starts_with(state_key, $3)
ORDER BY state_key";
        command.Parameters.Add(new DuckDBParameter { Value = RemoteCollectorService.GetServerId(rig.Server) });
        command.Parameters.Add(new DuckDBParameter { Value = LegacyLongQuerySession.StateCollector });
        command.Parameters.Add(new DuckDBParameter { Value = LegacyLongQuerySession.StateKeyPrefix });

        var rows = new List<(string, string)>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.GetString(1)));
        }

        return rows;
    }

    private async Task ExecuteStoreAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private static string[] KeysFor(params string[] databases) =>
        databases.Select(LegacyLongQuerySession.StateKey).OrderBy(key => key, StringComparer.Ordinal).ToArray();

    private static List<string> FoundLines(IEnumerable<string> lines) =>
        lines
            .Where(line => line.Contains("INFO", StringComparison.Ordinal)
                && line.Contains(LongQueryCompletionsCollector.LegacyXeSessionName, StringComparison.Ordinal)
                && line.Contains("older", StringComparison.OrdinalIgnoreCase))
            .ToList();

    private Task<Rig> BuildForAsync(bool azure, bool traceOn) =>
        azure ? BuildRigAsync(traceOn) : BuildOnPremRigAsync(traceOn);

    [Theory]
    [InlineData(false, true)]
    [InlineData(false, false)]
    [InlineData(true, true)]
    [InlineData(true, false)]
    public async Task Legacy_IsDroppedOncePerRegistrationAndDatabase_WhetherTheTraceIsOnOrOff(bool azure, bool traceOn)
    {
        var rig = await BuildForAsync(azure, traceOn);

        for (var cycle = 1; cycle <= 3; cycle++)
        {
            await rig.ReconcileAsync();
        }

        /* Server scope has no database, so its one drop names none. On Azure it is every listed database but master. */
        Assert.Equal(azure ? AllDatabases : new[] { string.Empty }, rig.LegacyCalls);

        var records = await RecordsAsync(rig);
        Assert.Equal(KeysFor(azure ? AllDatabases : new[] { string.Empty }), records.Select(r => r.Key));
        Assert.All(records, record =>
            Assert.Equal(rig.Clock, DateTime.Parse(record.Value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)));
    }

    [Fact]
    public async Task Legacy_Azure_ReachesADatabaseTheTraceExcludes_AndOneMonitoredAsItsOwnServer()
    {
        /* Nothing owns the legacy session any more, so no exclusion and no other registration keeps it from being dropped. */
        var rig = await BuildRigAsync(traceOn: true, "gamma");
        RegisterSeparately(rig, "beta");

        await rig.ReconcileAsync();

        Assert.Equal(AllDatabases, rig.LegacyCalls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Legacy_TheRecordSurvivesARestart_AndTheNewProcessDoesNotDropAgain(bool azure)
    {
        var rig = await BuildForAsync(azure, traceOn: true);
        await rig.ReconcileAsync();
        Assert.Equal(azure ? AllDatabases : new[] { string.Empty }, rig.LegacyCalls);

        var restarted = await RestartAsync(rig);
        await restarted.ReconcileAsync();
        await restarted.ReconcileAsync();

        Assert.Empty(restarted.LegacyCalls);
        Assert.NotEmpty(restarted.Created);
    }

    [Fact]
    public async Task Legacy_AFailedRecordRead_NeverBecomesASecondDrop()
    {
        var rig = await BuildOnPremRigAsync(traceOn: true);
        await rig.ReconcileAsync();
        Assert.Single(rig.LegacyCalls);

        /* A new process whose store cannot be read for the record: it skips the legacy step, and the install's own
           session is still ensured. */
        var restarted = await RestartAsync(rig);
        await ExecuteStoreAsync("ALTER TABLE collector_state RENAME TO collector_state_away");
        try
        {
            await restarted.ReconcileAsync();

            Assert.Empty(restarted.LegacyCalls);
            Assert.NotEmpty(restarted.Created);
        }
        finally
        {
            await ExecuteStoreAsync("ALTER TABLE collector_state_away RENAME TO collector_state");
        }

        /* The next cycle reads the record, and it says the session was dropped. */
        await restarted.ReconcileAsync();
        Assert.Empty(restarted.LegacyCalls);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Legacy_ASessionAnOlderInstallCreatesAgain_IsNeverDroppedAgain_AndOneInformationLineNamesIt(bool azure)
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Information);
            AppLogger.DrainBufferedLines();

            /* An older install keeps creating the legacy session on this server, so every look for it finds one. */
            var rig = await BuildForAsync(azure, traceOn: true);
            rig.Service.LegacyLongQuerySessionExistsForTests = (_, _) => true;
            var lines = new List<string>();

            await rig.ReconcileAsync();
            var dropped = rig.LegacyCalls.ToList();
            Assert.Equal(azure ? AllDatabases : new[] { string.Empty }, dropped);

            /* On, on again, off, and the cleanup of this install's own session failing to the cap and then once an hour. */
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            rig.Schedules.UpdateSchedule("long_query_completions", enabled: false);
            rig.Refuse.Add(azure ? "alpha" : string.Empty);
            for (var pass = 1; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
            }

            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            rig.Refuse.Clear();
            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            rig.Schedules.UpdateSchedule("long_query_completions", enabled: true);
            await rig.ReconcileAsync();

            Assert.Equal(dropped, rig.LegacyCalls);
            lines.AddRange(Lines(rig));
            Assert.Single(FoundLines(lines));

            /* A new start looks again, finds it again, says so once, and still leaves it alone. */
            var restarted = await RestartAsync(rig);
            restarted.Service.LegacyLongQuerySessionExistsForTests = (_, _) => true;
            await restarted.ReconcileAsync();
            await restarted.ReconcileAsync();

            Assert.Empty(restarted.LegacyCalls);
            Assert.Single(FoundLines(Lines(restarted)));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Legacy_AnOlderInstallsSession_IsNotReportedWhileItsDropIsStillFailing()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Information);
            AppLogger.DrainBufferedLines();

            var rig = await BuildOnPremRigAsync(traceOn: true);
            rig.Service.LegacyLongQuerySessionExistsForTests = (_, _) => true;
            rig.RefuseLegacy.Add(string.Empty);

            await rig.ReconcileAsync();
            await rig.ReconcileAsync();

            /* No record yet, so the session is this install's to drop, not an older install's to report. */
            Assert.Equal(2, rig.LegacyCalls.Count);
            Assert.Empty(FoundLines(Lines(rig)));

            rig.RefuseLegacy.Clear();
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();

            Assert.Equal(3, rig.LegacyCalls.Count);
            Assert.Single(FoundLines(Lines(rig)));
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public void Legacy_TheDropNamesOnlyTheLegacySession_NeverDeadlockBlockedProcessOrAnotherId()
    {
        var source = ReadLf("Lite/Services/RemoteCollectorService.LegacyLongQuerySession.cs");

        Assert.Contains("LongQueryCompletionsCollector.LegacyXeSessionName", source, StringComparison.Ordinal);
        Assert.DoesNotContain("DeadlocksCollector", source, StringComparison.Ordinal);
        Assert.DoesNotContain("BlockedProcessReportCollector", source, StringComparison.Ordinal);
        Assert.DoesNotContain("XeSessionNameFor", source, StringComparison.Ordinal);
        Assert.DoesNotContain("LongQuerySessionName()", source, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Legacy_Azure_AFailedDropIsNotRecorded_AndRetriesToTheCapThenOnceAnHour()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildRigAsync(traceOn: false);
            rig.RefuseLegacy.Add("beta");

            await rig.ReconcileAsync();

            /* Every database was tried, the two that succeeded are recorded, and the one that refused is not. */
            Assert.Equal(AllDatabases, rig.LegacyCalls);
            Assert.Equal(KeysFor("alpha", "gamma"), (await RecordsAsync(rig)).Select(r => r.Key));
            Assert.Null(rig.Applied);

            /* The next passes try only the database that is not recorded, until the fifth failed pass in a row gives up. */
            for (var pass = 2; pass <= LongQueryTraceDatabases.DropAttemptCap; pass++)
            {
                await rig.ReconcileAsync();
            }

            Assert.Equal(new[] { "alpha", "beta", "gamma", "beta", "beta", "beta", "beta" }, rig.LegacyCalls);
            Assert.False(rig.Applied);
            var lines = Lines(rig);
            Assert.Single(lines, line => line.Contains("Stopped retrying", StringComparison.Ordinal) && line.Contains("beta", StringComparison.Ordinal));

            /* Given up: the cycles in between leave it alone, and an hour later one attempt runs, at Debug, with no second warning. */
            rig.LegacyCalls.Clear();
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            Assert.Empty(rig.LegacyCalls);

            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            Assert.Equal(new[] { "beta" }, rig.LegacyCalls);
            lines = Lines(rig);
            Assert.DoesNotContain(lines, line => line.Contains("Stopped retrying", StringComparison.Ordinal));
            Assert.DoesNotContain(lines, line => line.Contains("WARN", StringComparison.Ordinal) && line.Contains("[beta]", StringComparison.Ordinal));

            /* The refusal ends: the next attempt drops it, records it, and nothing is left to try. */
            rig.RefuseLegacy.Clear();
            rig.LegacyCalls.Clear();
            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            Assert.Equal(new[] { "beta" }, rig.LegacyCalls);
            Assert.Equal(KeysFor(AllDatabases), (await RecordsAsync(rig)).Select(r => r.Key));

            rig.LegacyCalls.Clear();
            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            Assert.Empty(rig.LegacyCalls);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Legacy_OnPremises_AFailedDropWhileTheTraceIsOn_TakesTheCap_BecauseTheCreateDoesNotResetIt()
    {
        var level = AppLogger.MinimumLevel;
        try
        {
            AppLogger.SetMinimumLevel(LogLevel.Debug);
            AppLogger.DrainBufferedLines();

            var rig = await BuildOnPremRigAsync(traceOn: true);
            rig.RefuseLegacy.Add(string.Empty);

            /* This install's own session is created on every cycle, and each create succeeds. Without a cap that
               counts the failed legacy drops across them, the warning that gives up never comes. */
            for (var cycle = 1; cycle <= LongQueryTraceDatabases.DropAttemptCap; cycle++)
            {
                await rig.ReconcileAsync();
            }

            Assert.Equal(LongQueryTraceDatabases.DropAttemptCap, rig.LegacyCalls.Count);
            Assert.Equal(LongQueryTraceDatabases.DropAttemptCap, rig.Created.Count());
            Assert.Empty(await RecordsAsync(rig));
            Assert.Single(Lines(rig), line => line.Contains("Stopped retrying", StringComparison.Ordinal));

            /* Given up: the cycles in between create the session and leave the legacy drop alone, and an hour later it runs once. */
            rig.LegacyCalls.Clear();
            await rig.ReconcileAsync();
            await rig.ReconcileAsync();
            Assert.Empty(rig.LegacyCalls);

            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            Assert.Single(rig.LegacyCalls);

            /* Done once it succeeds: recorded, and never tried again. */
            rig.RefuseLegacy.Clear();
            rig.LegacyCalls.Clear();
            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            Assert.Single(rig.LegacyCalls);
            Assert.Single(await RecordsAsync(rig));

            rig.LegacyCalls.Clear();
            await rig.ReconcileAsync();
            rig.Clock += LongQueryTraceDatabases.RetryInterval;
            await rig.ReconcileAsync();
            Assert.Empty(rig.LegacyCalls);
        }
        finally
        {
            AppLogger.SetMinimumLevel(level);
        }
    }

    [Fact]
    public async Task Legacy_Azure_RunsBeforeThisInstallsCreate_InEachDatabase_AndAFailureDoesNotStopTheCreate()
    {
        var rig = await BuildRigAsync(traceOn: true);
        rig.RefuseLegacy.Add("alpha");

        await rig.ReconcileAsync();

        foreach (var database in new[] { "alpha", "beta", "gamma" })
        {
            var legacyAt = rig.Sequence.IndexOf((database, "legacy"));
            var createAt = rig.Sequence.IndexOf((database, "create"));
            Assert.True(legacyAt >= 0, $"{database}: the legacy session was not dropped");
            Assert.True(createAt > legacyAt, $"{database}: the create ran before the legacy drop");
        }

        /* The refused drop in alpha did not keep this install's session from being created there, and it is the only one
           left to retry. The run's fault is clear: the session exists. */
        Assert.Equal(AllDatabases, rig.Created);
        Assert.Null(rig.Applied);
        Assert.Null(rig.Service.LongQueryTraceFaultState(rig.Server.Id));
        Assert.Equal(KeysFor("beta", "gamma"), (await RecordsAsync(rig)).Select(r => r.Key));

        rig.RefuseLegacy.Clear();
        rig.LegacyCalls.Clear();
        await rig.ReconcileAsync();
        Assert.Equal(new[] { "alpha" }, rig.LegacyCalls);
    }
}
