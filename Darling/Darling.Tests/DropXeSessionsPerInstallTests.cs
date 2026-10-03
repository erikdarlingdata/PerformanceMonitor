/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4961: each install makes its own long-query session, and on Azure SQL Database its own deadlock and blocked-process
/// fallbacks, all named from the install's id. <c>--drop-xe-sessions</c> drops this install's sessions with the shared ones,
/// and lists the sessions of other installs with a guarded DROP for the operator to run, which it never runs itself.
/// </summary>
public sealed class DropXeSessionsPerInstallTests
{
    private const string Id = "1a2b3c4d";
    private const string OwnLongQuery = "PerformanceMonitor_Darling_1a2b3c4d_LongQueryCompletions";
    private const string OwnDeadlock = "PerformanceMonitor_Darling_1a2b3c4d_Deadlock";
    private const string OwnBlocked = "PerformanceMonitor_Darling_1a2b3c4d_BlockedProcess";
    private const string OtherLiteLongQuery = "PerformanceMonitor_Lite_ffeeddcc_LongQueryCompletions";
    private const string OtherDarlingDeadlock = "PerformanceMonitor_Darling_ffeeddcc_Deadlock";
    private const string OtherDarlingBlocked = "PerformanceMonitor_Darling_ffeeddcc_BlockedProcess";
    private const string SharedDeadlock = "PerformanceMonitor_Deadlock";
    private const string SharedBlocked = "PerformanceMonitor_BlockedProcess";
    private const string Legacy = "PerformanceMonitor_LongQueryCompletions";

    // ---- the names ------------------------------------------------------------------------------------------------------

    [Fact]
    public void TheInstallsNames_AreItsLongQuerySessionAndTheTwoAzureFallbacks()
    {
        Assert.Equal(new[] { OwnLongQuery, OwnDeadlock, OwnBlocked }, DarlingXeSessionCleanup.InstallSessionNames(Id));
        Assert.Equal(
            new[] { SharedDeadlock, SharedBlocked, Legacy, OwnLongQuery, OwnDeadlock, OwnBlocked },
            DarlingXeSessionCleanup.NamesToDrop(Id));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("1A2B3C4D")]
    [InlineData("1a2b3c4")]
    [InlineData("1a2b3c4d ")]
    public void WithNoUsableId_TheVerbHandlesNoInstallName(string? id)
    {
        Assert.Empty(DarlingXeSessionCleanup.InstallSessionNames(id));
        Assert.Equal(DarlingXeSessionCleanup.SessionNames.ToArray(), DarlingXeSessionCleanup.NamesToDrop(id));
        Assert.False(DarlingXeSessionCleanup.IsOtherInstallSessionName(OtherLiteLongQuery, id));
    }

    [Fact]
    public void PlanDrops_KeepsThisInstallsNamesAndTheSharedOnes_AndIgnoresAnotherInstalls()
    {
        var plan = DarlingXeSessionCleanup.PlanDrops(
            [
                new ExistingXeSession(OtherLiteLongQuery, XeSessionScope.Server),
                new ExistingXeSession(OwnLongQuery, XeSessionScope.Server),
                new ExistingXeSession(Legacy, XeSessionScope.Server),
                new ExistingXeSession(SharedDeadlock, XeSessionScope.Server),
                new ExistingXeSession(OtherDarlingDeadlock, XeSessionScope.Database, "alpha"),
                new ExistingXeSession(OwnDeadlock, XeSessionScope.Database, "alpha"),
            ],
            Id);

        Assert.Equal(
            new[]
            {
                "DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_Darling_1a2b3c4d_LongQueryCompletions] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_Darling_1a2b3c4d_Deadlock] ON DATABASE;",
            },
            plan.Select(d => d.Statement).ToArray());
    }

    [Fact]
    public void PlanDrops_WithNoId_KeepsOnlyTheSharedNames()
    {
        var plan = DarlingXeSessionCleanup.PlanDrops(
            [
                new ExistingXeSession(OwnLongQuery, XeSessionScope.Server),
                new ExistingXeSession(SharedBlocked, XeSessionScope.Server),
            ]);

        Assert.Equal(new[] { "DROP EVENT SESSION [PerformanceMonitor_BlockedProcess] ON SERVER;" }, plan.Select(d => d.Statement).ToArray());
    }

    [Theory]
    [InlineData(XeSessionScope.Server, "SERVER")]
    [InlineData(XeSessionScope.Database, "DATABASE")]
    public void DropStatement_AcceptsThisInstallsNames_AndRefusesEveryOtherInstalls(XeSessionScope scope, string on)
    {
        foreach (var own in new[] { OwnLongQuery, OwnDeadlock, OwnBlocked })
        {
            Assert.Equal($"DROP EVENT SESSION [{own}] ON {on};", DarlingXeSessionCleanup.DropStatement(own, scope, Id));

            /* With no id the same name is nobody's to drop, and a different id's name is another install's. */
            Assert.Throws<ArgumentException>(() => DarlingXeSessionCleanup.DropStatement(own, scope));
            Assert.Throws<ArgumentException>(() => DarlingXeSessionCleanup.DropStatement(own, scope, "ffeeddcc"));
            Assert.Throws<ArgumentException>(() => DarlingXeSessionCleanup.DropStatement(own.ToUpperInvariant(), scope, Id));
        }

        foreach (var other in new[] { OtherLiteLongQuery, OtherDarlingDeadlock, OtherDarlingBlocked, "system_health" })
        {
            Assert.Throws<ArgumentException>(() => DarlingXeSessionCleanup.DropStatement(other, scope, Id));
        }
    }

    // ---- the listing of other installs' sessions ------------------------------------------------------------------------

    [Theory]
    [InlineData(OtherLiteLongQuery, true)]
    [InlineData(OtherDarlingDeadlock, true)]
    [InlineData(OtherDarlingBlocked, true)]
    [InlineData(OwnLongQuery, false)]
    [InlineData(OwnDeadlock, false)]
    [InlineData(OwnBlocked, false)]
    [InlineData(SharedDeadlock, false)]
    [InlineData(SharedBlocked, false)]
    [InlineData(Legacy, false)]
    [InlineData("PerformanceMonitor_Darling_notanid_Deadlock", false)]
    [InlineData("PerformanceMonitor_Other_ffeeddcc_Deadlock", false)]
    [InlineData("PerformanceMonitor_Darling_ffeeddcc_Deadlock]; DROP DATABASE x; --", false)]
    [InlineData("system_health", false)]
    public void AnotherInstallsSession_IsRecognizedByItsExactShape_AndNeverThisInstallsOwn(string name, bool expected) =>
        Assert.Equal(expected, DarlingXeSessionCleanup.IsOtherInstallSessionName(name, Id));

    [Theory]
    [InlineData(XeSessionScope.Server, "FROM sys.server_event_sessions AS ses", "ses")]
    [InlineData(XeSessionScope.Database, "FROM sys.database_event_sessions AS des", "des")]
    public void TheFindOfOtherInstalls_MatchesTheThreePerInstallShapes_WithTheUnderscoresEscaped(XeSessionScope scope, string from, string alias)
    {
        var sql = DarlingXeSessionCleanup.ComposeFindOthersSql(scope);

        Assert.Contains(from, sql, StringComparison.Ordinal);
        Assert.StartsWith(Environment.NewLine + "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;", sql.ReplaceLineEndings(Environment.NewLine), StringComparison.Ordinal);
        Assert.Contains("SELECT /* PerformanceMonitorDarling */", sql, StringComparison.Ordinal);
        var patterns = Regex.Matches(sql, "N'([^']*)'").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(
            new[] { "PerformanceMonitor[_]%[_]LongQueryCompletions", "PerformanceMonitor[_]%[_]Deadlock", "PerformanceMonitor[_]%[_]BlockedProcess" },
            patterns);
        foreach (var pattern in patterns)
        {
            Assert.Contains($"{alias}.name LIKE N'{pattern}'", sql, StringComparison.Ordinal);
        }

        /* The shapes need a middle segment, so neither the shared names nor the old shared long-query name can match. */
        var regexes = patterns.Select(p => new Regex("^" + Regex.Escape(p.Replace("[_]", "_", StringComparison.Ordinal)).Replace("%", ".*", StringComparison.Ordinal) + "$")).ToArray();
        bool Matches(string name) => regexes.Any(r => r.IsMatch(name));
        Assert.All(new[] { OtherLiteLongQuery, OtherDarlingDeadlock, OtherDarlingBlocked, OwnLongQuery }, name => Assert.True(Matches(name), name));
        Assert.All(new[] { SharedDeadlock, SharedBlocked, Legacy }, name => Assert.False(Matches(name), name));
    }

    // ---- the verb: drops this install's sessions and lists the others' ---------------------------------------------------

    private sealed class FakeTarget : IXeSessionCleanupTarget
    {
        public List<ExistingXeSession> Found { get; } = [];

        public List<ExistingXeSession> Others { get; } = [];

        public List<XeSessionDrop> Dropped { get; } = [];

        public Task<XeSessionSearch> FindSessionsAsync(CancellationToken cancellationToken) =>
            Task.FromResult(new XeSessionSearch(Found.ToList(), []) { Others = Others.ToList() });

        public Task DropAsync(XeSessionDrop drop, CancellationToken cancellationToken)
        {
            Dropped.Add(drop);
            return Task.CompletedTask;
        }
    }

    private static async Task<(int Exit, string Output, string Error)> RunAsync(FakeTarget target, bool dryRun, string? installId = Id)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await DarlingXeSessionCleanup.RunAsync("sql01", dryRun, target, output, error, CancellationToken.None, installId);
        return (exit, output.ToString(), error.ToString());
    }

    private static string GuardedDropText(string name, string view, string alias, string on) =>
        string.Join(
            Environment.NewLine,
            "    IF EXISTS",
            "    (",
            "        SELECT",
            "            1/0",
            $"        FROM sys.{view} AS {alias}",
            $"        WHERE {alias}.name = N'{name}'",
            "    )",
            "    BEGIN",
            $"        DROP EVENT SESSION [{name}] ON {on};",
            "    END;");

    [Fact]
    public async Task TheVerb_DropsThisInstallsSessionsAndTheSharedOnes_AndListsOtherInstallsWithoutDroppingThem_OnServerScope()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(OwnLongQuery, XeSessionScope.Server));
        target.Found.Add(new ExistingXeSession(Legacy, XeSessionScope.Server));
        target.Found.Add(new ExistingXeSession(SharedDeadlock, XeSessionScope.Server));
        target.Others.Add(new ExistingXeSession(OtherLiteLongQuery, XeSessionScope.Server));
        target.Others.Add(new ExistingXeSession(OtherDarlingDeadlock, XeSessionScope.Server));

        var (exit, output, error) = await RunAsync(target, dryRun: false);

        Assert.Equal(0, exit);
        Assert.Equal(string.Empty, error);
        Assert.Equal(
            new[]
            {
                "DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_Darling_1a2b3c4d_LongQueryCompletions] ON SERVER;",
            },
            target.Dropped.Select(d => d.Statement).ToArray());
        Assert.Contains($"[DROPPED] {OwnLongQuery} (server scope)", output, StringComparison.Ordinal);

        Assert.Contains(DarlingXeSessionCleanup.OtherInstallsHeading, output, StringComparison.Ordinal);
        Assert.Contains($"{OtherLiteLongQuery} (server scope)", output, StringComparison.Ordinal);
        Assert.Contains(GuardedDropText(OtherLiteLongQuery, "server_event_sessions", "ses", "SERVER"), output, StringComparison.Ordinal);
        Assert.Contains(GuardedDropText(OtherDarlingDeadlock, "server_event_sessions", "ses", "SERVER"), output, StringComparison.Ordinal);
        Assert.DoesNotContain("[DROPPED] " + OtherLiteLongQuery, output, StringComparison.Ordinal);
        Assert.DoesNotContain("[DROPPED] " + OtherDarlingDeadlock, output, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(output, "^WARNING: ", RegexOptions.Multiline));
    }

    [Fact]
    public async Task TheVerb_DropsAndListsTheSameWay_OnAzureSqlDatabase()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(OwnLongQuery, XeSessionScope.Database, "alpha"));
        target.Found.Add(new ExistingXeSession(OwnDeadlock, XeSessionScope.Database, "alpha"));
        target.Found.Add(new ExistingXeSession(SharedBlocked, XeSessionScope.Database, "beta"));
        target.Others.Add(new ExistingXeSession(OtherLiteLongQuery, XeSessionScope.Database, "alpha"));
        target.Others.Add(new ExistingXeSession(OtherDarlingBlocked, XeSessionScope.Database, "beta"));

        var (exit, output, error) = await RunAsync(target, dryRun: false);

        Assert.Equal(0, exit);
        Assert.Equal(string.Empty, error);
        Assert.Equal(
            new[]
            {
                "DROP EVENT SESSION [PerformanceMonitor_Darling_1a2b3c4d_LongQueryCompletions] ON DATABASE;",
                "DROP EVENT SESSION [PerformanceMonitor_Darling_1a2b3c4d_Deadlock] ON DATABASE;",
                "DROP EVENT SESSION [PerformanceMonitor_BlockedProcess] ON DATABASE;",
            },
            target.Dropped.Select(d => d.Statement).ToArray());
        Assert.Contains($"[DROPPED] {OwnDeadlock} (database scope, in database alpha)", output, StringComparison.Ordinal);

        Assert.Contains(DarlingXeSessionCleanup.OtherInstallsHeading, output, StringComparison.Ordinal);
        Assert.Contains($"{OtherLiteLongQuery} (database scope, in database alpha)", output, StringComparison.Ordinal);
        Assert.Contains(GuardedDropText(OtherLiteLongQuery, "database_event_sessions", "des", "DATABASE"), output, StringComparison.Ordinal);
        Assert.Contains(GuardedDropText(OtherDarlingBlocked, "database_event_sessions", "des", "DATABASE"), output, StringComparison.Ordinal);
        Assert.DoesNotContain("[DROPPED] " + OtherLiteLongQuery, output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADryRun_ListsThisInstallsDropsAndTheOthersSessions_AndDropsNothing()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(OwnLongQuery, XeSessionScope.Server));
        target.Others.Add(new ExistingXeSession(OtherLiteLongQuery, XeSessionScope.Server));

        var (exit, output, _) = await RunAsync(target, dryRun: true);

        Assert.Equal(0, exit);
        Assert.Empty(target.Dropped);
        Assert.Contains($"[WOULD DROP] DROP EVENT SESSION [{OwnLongQuery}] ON SERVER;", output, StringComparison.Ordinal);
        Assert.Contains(DarlingXeSessionCleanup.OtherInstallsHeading, output, StringComparison.Ordinal);
        Assert.Contains(GuardedDropText(OtherLiteLongQuery, "server_event_sessions", "ses", "SERVER"), output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WithNothingOfAnotherInstalls_TheVerbPrintsNoListing()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(OwnLongQuery, XeSessionScope.Server));

        var (_, output, _) = await RunAsync(target, dryRun: false);

        Assert.DoesNotContain(DarlingXeSessionCleanup.OtherInstallsHeading, output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WithNoId_TheVerbSaysSo_AndHandlesNoInstallName()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(OwnLongQuery, XeSessionScope.Server));
        target.Found.Add(new ExistingXeSession(SharedDeadlock, XeSessionScope.Server));
        target.Others.Add(new ExistingXeSession(OtherLiteLongQuery, XeSessionScope.Server));

        var (exit, output, _) = await RunAsync(target, dryRun: false, installId: null);

        Assert.Equal(0, exit);
        Assert.Contains(DarlingXeSessionCleanup.NoInstallIdNote, output, StringComparison.Ordinal);
        Assert.Equal(new[] { "DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;" }, target.Dropped.Select(d => d.Statement).ToArray());
        Assert.DoesNotContain(DarlingXeSessionCleanup.OtherInstallsHeading, output, StringComparison.Ordinal);
        Assert.DoesNotContain(OtherLiteLongQuery, output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheNoteAfterADrop_NamesTheCaptureOfAnInstallsSessionToo_OncePerCapture()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(OwnLongQuery, XeSessionScope.Database, "alpha"));
        target.Found.Add(new ExistingXeSession(Legacy, XeSessionScope.Database, "alpha"));
        target.Found.Add(new ExistingXeSession(OwnDeadlock, XeSessionScope.Database, "alpha"));

        var (_, output, _) = await RunAsync(target, dryRun: false);

        Assert.Contains(
            "NOTE: capture of deadlocks and long query completions on that server stops until this service reconnects to it, so remove the server next.",
            output,
            StringComparison.Ordinal);
    }

    // ---- the CLI hands the target this install's id ----------------------------------------------------------------------

    private static string WriteConfig(DirectoryInfo root)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();

        var path = Path.Combine(root.FullName, "darling.json");
        File.WriteAllText(
            path,
            "{ \"postgres\": { \"connectionString\": "
            + JsonSerializer.Serialize($"Host=127.0.0.1;Port={port};Username=darling;Password=x;Database=darlingtest;Timeout=2")
            + " }, \"servers\": [ { \"name\": \"SQL2022\", \"host\": \"SQL2022\" } ] }");
        return path;
    }

    private static async Task<(int Exit, string Output, string Error, XeCleanupStoreFacts? Seen)> RunVerbAsync(
        FakeTarget target, XeCleanupStoreFacts facts, params string[] extra)
    {
        var root = Directory.CreateTempSubdirectory("drop-xe-install-");
        try
        {
            var output = new StringWriter();
            var error = new StringWriter();
            XeCleanupStoreFacts? seen = null;
            var exit = await DarlingCliCommands.DropXeSessionsAsync(
                ["SQL2022", "--config", WriteConfig(root), .. extra],
                (_, _, handed, _) =>
                {
                    seen = handed;
                    return Task.FromResult<IXeSessionCleanupTarget>(target);
                },
                output,
                error,
                CancellationToken.None,
                readStoreFacts: (_, _) => Task.FromResult(facts));
            return (exit, output.ToString(), error.ToString(), seen);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task TheVerb_ReadsTheInstallIdFromTheStore_AndHandsItToTheTarget_AndDropsWithIt()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(OwnLongQuery, XeSessionScope.Server));
        target.Others.Add(new ExistingXeSession(OtherLiteLongQuery, XeSessionScope.Server));

        var (exit, output, _, seen) = await RunVerbAsync(target, new XeCleanupStoreFacts(Id));

        Assert.Equal(0, exit);
        Assert.Equal(Id, seen?.InstallId);
        Assert.Equal(new[] { $"DROP EVENT SESSION [{OwnLongQuery}] ON SERVER;" }, target.Dropped.Select(d => d.Statement).ToArray());
        Assert.Contains(DarlingXeSessionCleanup.OtherInstallsHeading, output, StringComparison.Ordinal);
        Assert.DoesNotContain(DarlingXeSessionCleanup.NoInstallIdNote, output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheVerb_WhenTheStoreHasNoId_SaysSo_AndTheTargetGetsNone()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(OwnLongQuery, XeSessionScope.Server));

        var (exit, output, _, seen) = await RunVerbAsync(target, XeCleanupStoreFacts.NoId);

        Assert.Equal(0, exit);
        Assert.Null(seen?.InstallId);
        Assert.Empty(target.Dropped);
        Assert.Contains(DarlingXeSessionCleanup.NoInstallIdNote, output, StringComparison.Ordinal);
    }
}
