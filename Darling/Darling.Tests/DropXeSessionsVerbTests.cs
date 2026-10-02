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
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4732, #4961: removing a server drops this install's own Extended Events sessions on it, and <c>--drop-xe-sessions</c> is
/// the operator's explicit way to drop the shared ones and what a removal could not drop. The pure halves (the grammar, the statements, the plan) pin as text; the executor runs over a
/// fake target so the find-print-drop path needs no SQL Server.
/// </summary>
public sealed class DropXeSessionsVerbTests
{
    private const string Deadlock = "PerformanceMonitor_Deadlock";
    private const string Blocked = "PerformanceMonitor_BlockedProcess";
    private const string LongQuery = "PerformanceMonitor_LongQueryCompletions";

    /// <summary>The words that tell an operator the service stops capturing until it reconnects: printed only by a run that dropped
    /// a session (#4732).</summary>
    private const string CaptureStopsMarker = "stops until this service reconnects to it, so remove the server next.";

    // ---- Azure SQL Database: which databases the verb searches for which session -------------------------------------------

    private const string AzureHost = "dropxe.database.windows.net";

    private static SqlServerXeSessionCleanupTarget AzureTarget(MonitoredServer self, params MonitoredServer[] registry) =>
        AzureTarget(facts: null, self, registry);

    private static SqlServerXeSessionCleanupTarget AzureTarget(XeCleanupStoreFacts? facts, MonitoredServer self, params MonitoredServer[] registry) =>
        new(
            new ServerRuntime
            {
                Config = self,
                ConnectionString = $"Server=tcp:{AzureHost},1433;Initial Catalog=master;Encrypt=True",
                Target = new CollectorTargetInfo { IsAzureSqlDb = true },
                StorageName = AzureHost,
                ServerId = self.ServerId,
            },
            sessionNames: null,
            registry,
            facts);

    private const int SelfId = 11;
    private const int SecondId = 12;
    private const string StoreInstallId = "1a2b3c4d";
    private const string OwnLongQuery = "PerformanceMonitor_Darling_1a2b3c4d_LongQueryCompletions";

    /// <summary>One row of the store's collector schedules for the long-query collector: a registration's own override, or the
    /// install's default when <paramref name="serverId"/> is null.</summary>
    private static ScheduleOverride LongQueryRow(int? serverId, bool enabled) =>
        new(serverId, "long_query_completions", FrequencyMinutes: null, RetentionDays: null, enabled);

    private static XeCleanupStoreFacts FactsWith(IReadOnlyList<ScheduleOverride>? rows, IReadOnlyDictionary<int, string>? names = null) =>
        new(StoreInstallId, rows, names);

    /// <summary>Two registrations of one logical server: this one, which excludes gamma, and a second with read-only intent,
    /// which excludes delta.</summary>
    private static (MonitoredServer Self, MonitoredServer Second) TwoRegistrations() =>
        (new MonitoredServer { Name = "dropxe", Host = AzureHost, ExcludedDatabases = ["gamma"], StoredServerId = SelfId },
         new MonitoredServer { Name = "dropxe-ro", Host = AzureHost, ReadOnlyIntent = true, ExcludedDatabases = ["delta"], StoredServerId = SecondId });

    private static SqlServerXeSessionCleanupTarget.AzureSearchPlan PlanWith(XeCleanupStoreFacts? facts)
    {
        var (self, second) = TwoRegistrations();
        return AzureTarget(facts, self, self, second).PlanAzureSearch(
            monitored: ["master", "alpha", "delta"],
            every: ["master", "alpha", "gamma", "delta"]);
    }

    /// <summary>
    /// The long-query session is searched in the databases the registration excludes, because a session created before the
    /// database was excluded stays there with nothing that drops it. It is not searched in a database monitored as its own
    /// server, which that server's registration owns, and never in master. The other sessions keep the monitored list.
    /// </summary>
    [Fact]
    public void TheLongQuerySearch_VisitsAnExcludedDatabase_AndSkipsOneMonitoredAsItsOwnServer()
    {
        var self = new MonitoredServer { Name = "dropxe", Host = AzureHost, ExcludedDatabases = ["gamma"] };
        var alphaOwnServer = new MonitoredServer { Name = "dropxe-alpha", Host = AzureHost, Database = "alpha" };
        var target = AzureTarget(self, self, alphaOwnServer);

        var plan = target.PlanAzureSearch(
            monitored: ["master", "alpha", "beta"],
            every: ["master", "alpha", "beta", "gamma"]);

        Assert.Equal(new[] { "beta", "gamma" }, plan.LongQueryDatabases);
        Assert.Equal(new[] { "alpha", "beta" }, plan.AlwaysOnDatabases);
        Assert.True(plan.Reports("gamma", LongQuery));
        Assert.False(plan.Reports("gamma", Deadlock));
        Assert.False(plan.Reports("alpha", LongQuery));
        Assert.True(plan.Reports("alpha", Blocked));
        Assert.False(plan.Reports("master", LongQuery));
        Assert.Equal(new[] { "alpha", "beta", "gamma" }, plan.Visited);
    }

    /// <summary>
    /// #4961: another registration of the logical server keeps the session when its EFFECTIVE long-query setting is on, its own
    /// override or else the install's default: the verb leaves each database that registration would create the session in.
    /// </summary>
    [Fact]
    public void TheLongQuerySearch_SkipsADatabaseAnotherRegistrationOfTheServerKeeps()
    {
        var plan = PlanWith(FactsWith([LongQueryRow(SecondId, enabled: true)]));

        Assert.Equal(new[] { "delta" }, plan.LongQueryDatabases);
    }

    /// <summary>The install's default can turn a registration's trace on: it keeps the session with no row of its own.</summary>
    [Fact]
    public void TheLongQuerySearch_SkipsADatabaseWhenTheInstallsDefaultTurnsTheOtherRegistrationOn()
    {
        var plan = PlanWith(FactsWith([LongQueryRow(serverId: null, enabled: true)]));

        Assert.Equal(new[] { "delta" }, plan.LongQueryDatabases);
    }

    /// <summary>
    /// #4961: a registration whose long-query trace is off keeps nothing, so the verb drops the session in every database it
    /// finds it in, the ones that registration would have created it in too. Its own override beats the install's default,
    /// and with no row at all the collector's default, which is off, applies.
    /// </summary>
    [Fact]
    public void TheLongQuerySearch_DropsInADatabaseTheOtherRegistrationLeavesOff()
    {
        var everyDatabase = new[] { "alpha", "gamma", "delta" };

        /* Its own override is off. */
        Assert.Equal(everyDatabase, PlanWith(FactsWith([LongQueryRow(SecondId, enabled: false)])).LongQueryDatabases);

        /* Its own override beats an install's default that is on. */
        Assert.Equal(
            everyDatabase,
            PlanWith(FactsWith([LongQueryRow(serverId: null, enabled: true), LongQueryRow(SecondId, enabled: false)])).LongQueryDatabases);

        /* No row at all: the collector's own default applies, which is off. */
        Assert.Equal(everyDatabase, PlanWith(FactsWith([])).LongQueryDatabases);
    }

    /// <summary>A store whose schedule rows could not be read gives the verb no setting to go by, so it keeps counting every
    /// other registration of the logical server as keeping the session, as it did before it read them.</summary>
    [Fact]
    public void TheLongQuerySearch_WhenTheScheduleRowsCouldNotBeRead_CountsEveryOtherRegistrationAsKeeping()
    {
        Assert.Equal(new[] { "delta" }, PlanWith(FactsWith(rows: null)).LongQueryDatabases);
        Assert.Equal(new[] { "delta" }, PlanWith(facts: null).LongQueryDatabases);
    }

    [Fact]
    public void ATargetWithItsOwnNames_SearchesNoDatabaseForTheLongQuerySession()
    {
        var self = new MonitoredServer { Name = "dropxe", Host = AzureHost, ExcludedDatabases = ["gamma"] };
        var runtime = new ServerRuntime
        {
            Config = self,
            ConnectionString = $"Server=tcp:{AzureHost},1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo { IsAzureSqlDb = true },
            StorageName = AzureHost,
            ServerId = self.ServerId,
        };
        var target = new SqlServerXeSessionCleanupTarget(runtime, new[] { "dropxe_test_only" }, [self]);

        var plan = target.PlanAzureSearch(["master", "alpha"], ["master", "alpha", "gamma"]);

        Assert.Empty(plan.LongQueryDatabases);
        Assert.Equal(new[] { "alpha" }, plan.AlwaysOnDatabases);
    }

    // ---- on-premises: this install's long-query session when another registration of this install keeps it (#4961) ---------------

    private static SqlServerXeSessionCleanupTarget OnPremisesTarget(XeCleanupStoreFacts facts, MonitoredServer self, params MonitoredServer[] registry) =>
        new(
            new ServerRuntime
            {
                Config = self,
                ConnectionString = "Server=sql01;Encrypt=True",
                Target = new CollectorTargetInfo(),
                StorageName = "sql01",
                ServerId = self.ServerId,
            },
            sessionNames: null,
            registry,
            facts);

    private static (MonitoredServer Self, MonitoredServer Second) TwoOnPremisesRegistrations() =>
        (new MonitoredServer { Name = "sql01-a", Host = "sql01-a", StoredServerId = SelfId },
         new MonitoredServer { Name = "sql01-b", Host = "sql01-b", StoredServerId = SecondId });

    private static Dictionary<int, string> InstanceNames(string? own, string? second)
    {
        var names = new Dictionary<int, string>();
        if (own is not null)
        {
            names[SelfId] = own;
        }

        if (second is not null)
        {
            names[SecondId] = second;
        }

        return names;
    }

    /// <summary>The server-scope search of a server that holds this install's long-query session, the old shared one and the
    /// shared deadlock session.</summary>
    private static Task<XeSessionSearch> OnPremisesSearchAsync(
        IReadOnlyList<ScheduleOverride>? rows, IReadOnlyDictionary<int, string>? names, params string[] found)
    {
        var (self, second) = TwoOnPremisesRegistrations();
        return OnPremisesTarget(FactsWith(rows, names), self, self, second)
            .ServerScopeSearchAsync(found.Length == 0 ? [Deadlock, OwnLongQuery, LongQuery] : found, []);
    }

    private static readonly string[] SharedAndLegacy = [Deadlock, LongQuery];

    /// <summary>
    /// A server's session is the instance's own, so another registration of this install on the same instance, with its
    /// long-query trace on, keeps this install's session: the verb leaves it and says why. The shared sessions and the old
    /// shared long-query session are nobody's keeper session, so they stay in the plan.
    /// </summary>
    [Fact]
    public async Task OnPremises_AKeeperOnTheSameInstance_LeavesThisInstallsLongQuerySession_AndSaysWhy()
    {
        var search = await OnPremisesSearchAsync([LongQueryRow(SecondId, enabled: true)], InstanceNames("SQL01", "sql01"));

        Assert.Equal(SharedAndLegacy, search.Sessions.Select(session => session.Name).ToArray());
        var note = Assert.Single(search.Notes);
        Assert.Contains(OwnLongQuery, note, StringComparison.Ordinal);
        Assert.Contains("Another registration of this install keeps it on the same instance", note, StringComparison.Ordinal);
    }

    /// <summary>A name that is not known cannot match: this registration's own, or the keeper's. The session is dropped.</summary>
    [Fact]
    public async Task OnPremises_AnUnknownInstanceName_DropsThisInstallsLongQuerySession()
    {
        var keeperOn = new[] { LongQueryRow(SecondId, enabled: true) };

        foreach (var names in new[] { InstanceNames(null, "SQL01"), InstanceNames("SQL01", null), InstanceNames(null, null) })
        {
            var search = await OnPremisesSearchAsync(keeperOn, names);

            Assert.Equal(new[] { Deadlock, OwnLongQuery, LongQuery }, search.Sessions.Select(session => session.Name).ToArray());
            Assert.Empty(search.Notes);
        }
    }

    [Fact]
    public async Task OnPremises_AKeeperOnAnotherInstance_DropsThisInstallsLongQuerySession()
    {
        var search = await OnPremisesSearchAsync([LongQueryRow(SecondId, enabled: true)], InstanceNames("SQL01", "SQL02"));

        Assert.Equal(new[] { Deadlock, OwnLongQuery, LongQuery }, search.Sessions.Select(session => session.Name).ToArray());
        Assert.Empty(search.Notes);
    }

    /// <summary>A registration whose effective trace setting is off keeps nothing, however its instance name reads: its own
    /// override, or with no row at all the collector's default, which is off.</summary>
    [Fact]
    public async Task OnPremises_AKeeperWithItsTraceOff_DoesNotCount()
    {
        var sameInstance = InstanceNames("SQL01", "SQL01");

        foreach (var rows in new[] { new[] { LongQueryRow(SecondId, enabled: false) }, Array.Empty<ScheduleOverride>() })
        {
            var search = await OnPremisesSearchAsync(rows, sameInstance);

            Assert.Equal(new[] { Deadlock, OwnLongQuery, LongQuery }, search.Sessions.Select(session => session.Name).ToArray());
            Assert.Empty(search.Notes);
        }
    }

    /// <summary>With the schedule rows unreadable, every other registration counts as having its trace on, as on Azure SQL
    /// Database; the instance names still have to match.</summary>
    [Fact]
    public async Task OnPremises_WhenTheScheduleRowsCouldNotBeRead_AKeeperOnTheSameInstanceStillCounts()
    {
        var search = await OnPremisesSearchAsync(rows: null, InstanceNames("SQL01", "SQL01"));

        Assert.Equal(SharedAndLegacy, search.Sessions.Select(session => session.Name).ToArray());
        Assert.Single(search.Notes);
    }

    /// <summary>The guard is for this install's long-query session alone: with that session not on the server, nothing is left
    /// and nothing is said.</summary>
    [Fact]
    public async Task OnPremises_WithNoLongQuerySessionOfThisInstallOnTheServer_SaysNothing()
    {
        var search = await OnPremisesSearchAsync([LongQueryRow(SecondId, enabled: true)], InstanceNames("SQL01", "SQL01"), Deadlock, LongQuery);

        Assert.Equal(SharedAndLegacy, search.Sessions.Select(session => session.Name).ToArray());
        Assert.Empty(search.Notes);
    }

    /// <summary>
    /// Under the real executor: the session left in place is never dropped, the reason is on stderr, and it is a note, not a
    /// failure, so the exit code stays 0 for a script that removes the server next.
    /// </summary>
    [Fact]
    public async Task OnPremises_TheVerbDoesNotDropTheSessionItLeft_AndPrintsWhyOnStderr()
    {
        var (self, second) = TwoOnPremisesRegistrations();
        var real = OnPremisesTarget(FactsWith([LongQueryRow(SecondId, enabled: true)], InstanceNames("SQL01", "SQL01")), self, self, second);
        var target = new OnPremisesSearchTarget(real, [Deadlock, OwnLongQuery, LongQuery]);
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = await DarlingXeSessionCleanup.RunAsync("sql01", dryRun: false, target, output, error, CancellationToken.None, StoreInstallId);

        Assert.Equal(0, exit);
        Assert.Equal(SharedAndLegacy, target.Dropped.Select(drop => drop.Session.Name).ToArray());
        Assert.Contains(OwnLongQuery, error.ToString(), StringComparison.Ordinal);
        Assert.Contains("keeps it on the same instance", error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(OwnLongQuery, output.ToString(), StringComparison.Ordinal);
    }

    private sealed class OnPremisesSearchTarget : IXeSessionCleanupTarget
    {
        private readonly SqlServerXeSessionCleanupTarget _real;
        private readonly string[] _found;

        public OnPremisesSearchTarget(SqlServerXeSessionCleanupTarget real, string[] found)
        {
            _real = real;
            _found = found;
        }

        public List<XeSessionDrop> Dropped { get; } = [];

        public Task<XeSessionSearch> FindSessionsAsync(CancellationToken cancellationToken) => _real.ServerScopeSearchAsync(_found, []);

        public Task DropAsync(XeSessionDrop drop, CancellationToken cancellationToken)
        {
            Dropped.Add(drop);
            return Task.CompletedTask;
        }
    }

    // ---- a database the search cannot open: a problem when it is monitored, a note when the registration excludes it ----------

    /// <summary>The registration excludes gamma and monitors alpha as its own server: the plan visits alpha and beta for the
    /// always-on sessions and beta and gamma for the long-query one, so gamma is searched for that session alone.</summary>
    private static SqlServerXeSessionCleanupTarget.AzureSearchPlan ExcludedGammaPlan()
    {
        var self = new MonitoredServer { Name = "dropxe", Host = AzureHost, ExcludedDatabases = ["gamma"] };
        var alphaOwnServer = new MonitoredServer { Name = "dropxe-alpha", Host = AzureHost, Database = "alpha" };
        return AzureTarget(self, self, alphaOwnServer).PlanAzureSearch(
            monitored: ["master", "alpha", "beta"],
            every: ["master", "alpha", "beta", "gamma"]);
    }

    /// <summary>The target's real Azure search loop under the real executor, with the read of one database replaced: the
    /// <paramref name="refusing"/> database refuses the connection, alpha holds the deadlock session and beta the long-query
    /// one.</summary>
    private sealed class AzureSearchTarget : IXeSessionCleanupTarget
    {
        private readonly SqlServerXeSessionCleanupTarget.AzureSearchPlan _plan;
        private readonly string _refusing;

        public AzureSearchTarget(SqlServerXeSessionCleanupTarget.AzureSearchPlan plan, string refusing)
        {
            _plan = plan;
            _refusing = refusing;
        }

        public List<XeSessionDrop> Dropped { get; } = [];

        public Task<XeSessionSearch> FindSessionsAsync(CancellationToken cancellationToken) =>
            SqlServerXeSessionCleanupTarget.SearchAzureDatabasesAsync(_plan, NamesInDatabaseAsync, cancellationToken);

        public Task DropAsync(XeSessionDrop drop, CancellationToken cancellationToken)
        {
            Dropped.Add(drop);
            return Task.CompletedTask;
        }

        private Task<IReadOnlyList<string>> NamesInDatabaseAsync(string database, CancellationToken cancellationToken)
        {
            if (string.Equals(database, _refusing, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("login failed");
            }

            return Task.FromResult<IReadOnlyList<string>>(database switch
            {
                "alpha" => new[] { Deadlock },
                "beta" => new[] { LongQuery },
                _ => Array.Empty<string>(),
            });
        }
    }

    private static async Task<(int Exit, string Error, AzureSearchTarget Target)> RunAzureSearchAsync(string refusing)
    {
        var target = new AzureSearchTarget(ExcludedGammaPlan(), refusing);
        var error = new StringWriter();
        var exit = await DarlingXeSessionCleanup.RunAsync("sql01", dryRun: false, target, new StringWriter(), error, CancellationToken.None);
        return (exit, error.ToString(), target);
    }

    /// <summary>
    /// The long-query search opens databases the registration excludes, which the verb never opened before it. An excluded
    /// database the login cannot open (no user there, a paused serverless database) must not fail a verb that dropped every
    /// session it found: a script that runs the verb and then removes the server would stop there. It is a note on stderr that
    /// names the database, says it is excluded and says a session left there needs a manual drop.
    /// </summary>
    [Fact]
    public async Task AnExcludedDatabaseTheLoginCannotOpen_IsANoteOnStderr_AndTheRunStillSucceeds()
    {
        var (exit, error, target) = await RunAzureSearchAsync(refusing: "gamma");

        Assert.Equal(0, exit);
        Assert.Equal(
            [
                ("alpha", Deadlock),
                ("beta", LongQuery),
            ],
            target.Dropped.Select(drop => (drop.Session.Database!, drop.Session.Name)).ToArray());
        Assert.Equal(
            $"Database gamma is excluded from monitoring and could not be searched for the long-query completion session (login failed), so a {LongQuery} session left there needs a manual drop."
                + Environment.NewLine,
            error);
    }

    /// <summary>A database the registration monitors is searched for every session, so one the login cannot open still fails
    /// the run with the unavailable code, as it did before the search reached excluded databases. This passes without the
    /// exclusion rule too: it is the guard that the rule did not loosen the monitored case.</summary>
    [Fact]
    public async Task AMonitoredDatabaseTheLoginCannotOpen_StillFailsTheRun_WithTheUnavailableCode()
    {
        var (exit, error, target) = await RunAzureSearchAsync(refusing: "beta");

        Assert.Equal(2, exit);
        Assert.Equal(
            "Could not search database beta for Extended Events sessions: login failed" + Environment.NewLine,
            error);
        Assert.DoesNotContain("excluded from monitoring", error, StringComparison.Ordinal);
        Assert.Equal(new[] { ("alpha", Deadlock) }, target.Dropped.Select(drop => (drop.Session.Database!, drop.Session.Name)).ToArray());
    }

    [Fact]
    public void ADatabaseSearchedOnlyForTheLongQuerySession_IsFiledAsANote_AndAMonitoredOneAsAProblem()
    {
        var plan = ExcludedGammaPlan();

        var excluded = plan.Unsearched("gamma", "login failed");
        Assert.False(excluded.IsProblem);
        Assert.Contains("Database gamma is excluded from monitoring", excluded.Text, StringComparison.Ordinal);
        Assert.Contains("could not be searched for the long-query completion session (login failed)", excluded.Text, StringComparison.Ordinal);
        Assert.Contains($"{LongQuery} session left there needs a manual drop", excluded.Text, StringComparison.Ordinal);

        var monitored = plan.Unsearched("beta", "login failed");
        Assert.True(monitored.IsProblem);
        Assert.Equal("Could not search database beta for Extended Events sessions: login failed", monitored.Text);

        // Database names compare without regard to case, as the rest of the plan does.
        Assert.True(plan.Unsearched("BETA", "login failed").IsProblem);
        Assert.False(plan.Unsearched("GAMMA", "login failed").IsProblem);
    }

    // ---- classification, help and dispatch -----------------------------------------------------------------------------

    [Theory]
    [InlineData("--drop-xe-sessions")]
    [InlineData("--DROP-XE-SESSIONS")]
    public void TheVerbIsKnown_AndNeverStartsAHost(string arg)
    {
        Assert.True(DarlingCliCommands.IsDropXeSessionsVerb(arg));
        Assert.True(DarlingCliCommands.IsKnownVerb(arg));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs([arg]));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs([arg, "sql01", "--dry-run"]));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs([arg, "--print-sql"]));
    }

    [Theory]
    [InlineData("--drop-xe-session")]
    [InlineData("--drop-xe-sessions-now")]
    [InlineData("drop-xe-sessions")]
    public void ANearMissIsAnUnknownOption(string arg)
    {
        Assert.False(DarlingCliCommands.IsDropXeSessionsVerb(arg));
        Assert.Equal(StartupAction.UnknownOption, DarlingCliCommands.ClassifyStartupArgs([arg]));
    }

    [Fact]
    public void HelpListsBothForms_InAscii()
    {
        var usage = DarlingCliCommands.UsageText();
        Assert.Contains("--drop-xe-sessions <server-name> [--dry-run] [--config <path>]", usage, StringComparison.Ordinal);
        Assert.Contains("--drop-xe-sessions --print-sql", usage, StringComparison.Ordinal);
        Assert.All(usage, ch => Assert.True(ch < 128, $"usage text must be ASCII; found U+{(int)ch:X4}"));
        Assert.All(DarlingCliCommands.DropXeSessionsUsageText(), ch => Assert.True(ch < 128));
    }

    [Theory]
    [InlineData(Deadlock)]
    [InlineData(Blocked)]
    [InlineData(LongQuery)]
    public void HelpNamesEverySessionTheVerbDrops(string name)
    {
        Assert.Contains(name, DarlingCliCommands.UsageText(), StringComparison.Ordinal);
        Assert.Contains(name, DarlingCliCommands.DropXeSessionsUsageText(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheNamesReadAsOnePhrase() =>
        Assert.Equal(
            "PerformanceMonitor_Deadlock, PerformanceMonitor_BlockedProcess and PerformanceMonitor_LongQueryCompletions",
            DarlingXeSessionCleanup.SessionNamesPhrase());

    [Fact]
    public void ProgramDispatchesTheVerb_WithoutAWindowsGuard()
    {
        var program = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Program.cs");
        var at = program.IndexOf("DarlingCliCommands.IsDropXeSessionsVerb(args[0])", StringComparison.Ordinal);
        Assert.True(at >= 0, "Program.cs no longer dispatches --drop-xe-sessions (#4732)");

        var block = program[at..Math.Min(program.Length, at + 300)];
        Assert.Contains("DarlingCliCommands.DropXeSessionsAsync(args[1..]", block, StringComparison.Ordinal);
        Assert.DoesNotContain("IsWindows", block, StringComparison.Ordinal);
    }

    // ---- argument handling ---------------------------------------------------------------------------------------------

    [Theory]
    [InlineData("sql01", "sql01", false, false, null)]
    [InlineData("sql01 --dry-run", "sql01", true, false, null)]
    [InlineData("--dry-run sql01", "sql01", true, false, null)]
    [InlineData("sql01 --config C:\\etc\\darling.json", "sql01", false, false, "C:\\etc\\darling.json")]
    [InlineData("--config C:\\etc\\darling.json sql01 --dry-run", "sql01", true, false, "C:\\etc\\darling.json")]
    [InlineData("--print-sql", null, false, true, null)]
    [InlineData("--PRINT-SQL", null, false, true, null)]
    public void TheGrammarAcceptsTheDocumentedForms(string args, string? server, bool dryRun, bool printSql, string? config)
    {
        Assert.True(DarlingCliCommands.TryParseDropXeSessionsArgs(
            args.Split(' '), out var gotServer, out var gotDryRun, out var gotPrintSql, out var gotConfig, out var error), error);
        Assert.Equal(server, gotServer);
        Assert.Equal(dryRun, gotDryRun);
        Assert.Equal(printSql, gotPrintSql);
        Assert.Equal(config, gotConfig);
    }

    [Theory]
    [InlineData("", "needs a server name")]
    [InlineData("--dry-run", "needs a server name")]
    [InlineData("sql01 sql02", "takes ONE server name")]
    [InlineData("sql01 --force", "Unknown option")]
    [InlineData("sql01 --config", "--config needs a path")]
    [InlineData("sql01 --config --dry-run", "--config needs a path")]
    [InlineData("--print-sql sql01", "takes no server name")]
    [InlineData("--print-sql --dry-run", "takes no --dry-run")]
    [InlineData("--print-sql --config x.json", "takes no --config")]
    public void TheGrammarRefusesTheRest_WithAMessage(string args, string expected)
    {
        var rest = args.Length == 0 ? Array.Empty<string>() : args.Split(' ');
        Assert.False(DarlingCliCommands.TryParseDropXeSessionsArgs(
            rest, out _, out _, out _, out _, out var error));
        Assert.Contains(expected, error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NoServerName_PrintsUsage_AndExitsNonZero()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = await DarlingCliCommands.DropXeSessionsAsync([], ThrowingConnect, output, error, CancellationToken.None);

        Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.UsageOrConfig, exit);
        Assert.NotEqual(0, exit);
        Assert.Contains("needs a server name", error.ToString(), StringComparison.Ordinal);
        Assert.Contains("--drop-xe-sessions <server-name> [--dry-run] [--config <path>]", output.ToString(), StringComparison.Ordinal);
    }

    // ---- the SQL text --------------------------------------------------------------------------------------------------

    [Fact]
    public async Task PrintSql_NeedsNoServer_AndMakesNoConnection()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var exit = await DarlingCliCommands.DropXeSessionsAsync(["--print-sql"], ThrowingConnect, output, error, CancellationToken.None);

        Assert.Equal(0, exit);
        Assert.Equal(string.Empty, error.ToString());
        Assert.Equal(DarlingXeSessionCleanup.GuardedDropScript() + Environment.NewLine, output.ToString());
        Assert.DoesNotContain(CaptureStopsMarker, output.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(Deadlock, "server_event_sessions", "ses", "SERVER")]
    [InlineData(Blocked, "server_event_sessions", "ses", "SERVER")]
    [InlineData(LongQuery, "server_event_sessions", "ses", "SERVER")]
    [InlineData(Deadlock, "database_event_sessions", "des", "DATABASE")]
    [InlineData(Blocked, "database_event_sessions", "des", "DATABASE")]
    [InlineData(LongQuery, "database_event_sessions", "des", "DATABASE")]
    public void EveryDropIsGuardedByIfExists_AgainstTheCatalogOfItsScope(string name, string view, string alias, string scope)
    {
        var script = DarlingXeSessionCleanup.GuardedDropScript();
        var guarded = new Regex(
            @"IF EXISTS\s*\(\s*SELECT\s+1/0\s+FROM sys\." + view + " AS " + alias + @"\s+WHERE " + alias + @"\.name = N'" + name + @"'\s*\)\s*"
            + @"BEGIN\s+DROP EVENT SESSION \[" + name + @"\] ON " + scope + @";\s+END;",
            RegexOptions.CultureInvariant);

        Assert.Matches(guarded, script);
    }

    [Fact]
    public void TheScriptDropsThreeNamesInTwoScopes_AndNothingElse()
    {
        var script = DarlingXeSessionCleanup.GuardedDropScript();
        var code = string.Join('\n', script.Split('\n').Where(l => !l.TrimStart().StartsWith("--", StringComparison.Ordinal)));

        /* Six guarded drops, and the two per-install queries (one for each scope) that print a DROP statement as a column for
           the operator to run (#4961). */
        Assert.Equal(8, Regex.Matches(code, "DROP EVENT SESSION", RegexOptions.CultureInvariant).Count);
        Assert.Equal(6, Regex.Matches(code, @"IF EXISTS", RegexOptions.CultureInvariant).Count);
        Assert.Equal(
            new[] { Blocked, Deadlock, LongQuery },
            Regex.Matches(code, @"PerformanceMonitor_\w+").Select(m => m.Value).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal(
            new[] { Deadlock, Blocked, LongQuery },
            DarlingXeSessionCleanup.SessionNames.ToArray());
        Assert.Equal(
            new[] { DeadlocksCollector.XeSessionName, BlockedProcessReportCollector.XeSessionName, LongQueryCompletionsCollector.LegacyXeSessionName },
            DarlingXeSessionCleanup.SessionNames.ToArray());
        foreach (var other in new[] { "CREATE", "ALTER", "DELETE", "TRUNCATE", "EXEC", "sp_", "DROP DATABASE", "DROP TABLE", "ON ALL SERVER" })
        {
            Assert.DoesNotContain(other, code, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheScriptCarriesTheSharedNamesWarning_AsAComment()
    {
        var script = DarlingXeSessionCleanup.GuardedDropScript();
        Assert.Single(Regex.Matches(script, @"^-- WARNING: ", RegexOptions.Multiline));
        Assert.Contains(DarlingXeSessionCleanup.SharedNamesWarning, script, StringComparison.Ordinal);
    }

    [Fact]
    public void TheWarningNamesEveryMonitorThatCreatesTheSessionsAgain()
    {
        var warning = DarlingXeSessionCleanup.SharedNamesWarning;

        Assert.Contains("a Lite app", warning, StringComparison.Ordinal);
        Assert.Contains("a deprecated Full Dashboard install", warning, StringComparison.Ordinal);
        Assert.Contains("another Darling service", warning, StringComparison.Ordinal);
        Assert.Contains("never by the Dashboard", warning, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(Deadlock)]
    [InlineData(Blocked)]
    [InlineData(LongQuery)]
    public void TheFindQueriesLookForEveryName_InTheCatalogOfTheirScope(string name)
    {
        Assert.Matches(
            new Regex(@"FROM sys\.server_event_sessions AS ses\s+WHERE ses\.name IN \([^)]*N'" + name + @"'[^)]*\);", RegexOptions.CultureInvariant),
            DarlingXeSessionCleanup.FindServerSessionsSql);
        Assert.Matches(
            new Regex(@"FROM sys\.database_event_sessions AS des\s+WHERE des\.name IN \([^)]*N'" + name + @"'[^)]*\);", RegexOptions.CultureInvariant),
            DarlingXeSessionCleanup.FindDatabaseSessionsSql);
    }

    [Fact]
    public void TheFindQueriesLookForNothingElse()
    {
        foreach (var sql in new[] { DarlingXeSessionCleanup.FindServerSessionsSql, DarlingXeSessionCleanup.FindDatabaseSessionsSql })
        {
            Assert.Equal(
                new[] { Blocked, Deadlock, LongQuery },
                Regex.Matches(sql, "N'([^']*)'").Select(m => m.Groups[1].Value).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        }
    }

    [Theory]
    [InlineData(Deadlock, XeSessionScope.Server, "DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;")]
    [InlineData(Blocked, XeSessionScope.Server, "DROP EVENT SESSION [PerformanceMonitor_BlockedProcess] ON SERVER;")]
    [InlineData(LongQuery, XeSessionScope.Server, "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;")]
    [InlineData(Deadlock, XeSessionScope.Database, "DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON DATABASE;")]
    [InlineData(Blocked, XeSessionScope.Database, "DROP EVENT SESSION [PerformanceMonitor_BlockedProcess] ON DATABASE;")]
    [InlineData(LongQuery, XeSessionScope.Database, "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON DATABASE;")]
    public void TheStatementIsBracketQuoted(string name, XeSessionScope scope, string expected) =>
        Assert.Equal(expected, DarlingXeSessionCleanup.DropStatement(name, scope));

    [Theory]
    [InlineData("system_health")]
    [InlineData("PerformanceMonitor_LongQueryCompletion")]
    [InlineData("performancemonitor_longquerycompletions")]
    [InlineData("performancemonitor_deadlock")]
    [InlineData("PerformanceMonitor_Deadlock]; DROP DATABASE x; --")]
    [InlineData("")]
    public void AnyOtherNameIsRefused(string name) =>
        Assert.Throws<ArgumentException>(() => DarlingXeSessionCleanup.DropStatement(name, XeSessionScope.Server));

    [Fact]
    public void AClosingBracketIsDoubled_SoAnIdentifierCannotEndEarly() =>
        Assert.Equal("[a]]b]", DarlingXeSessionCleanup.BracketQuote("a]b"));

    // ---- the plan: which statements for these existing sessions ---------------------------------------------------------

    [Fact]
    public void PlanDrops_KeepsOnlyDarlingsNames_InAFixedOrder_AndCollapsesDuplicates()
    {
        var plan = DarlingXeSessionCleanup.PlanDrops(
        [
            new ExistingXeSession("system_health", XeSessionScope.Server),
            new ExistingXeSession(Blocked, XeSessionScope.Database, "zeta"),
            new ExistingXeSession(LongQuery, XeSessionScope.Database, "zeta"),
            new ExistingXeSession(Blocked, XeSessionScope.Server),
            new ExistingXeSession(LongQuery, XeSessionScope.Server),
            new ExistingXeSession("performancemonitor_deadlock", XeSessionScope.Server),
            new ExistingXeSession(Deadlock, XeSessionScope.Database, "alpha"),
            new ExistingXeSession(Deadlock, XeSessionScope.Server),
            new ExistingXeSession(Deadlock, XeSessionScope.Database, "ALPHA"),
            new ExistingXeSession("performancemonitor_longquerycompletions", XeSessionScope.Database, "alpha"),
            new ExistingXeSession("'; DROP DATABASE x; --", XeSessionScope.Server),
        ]);

        Assert.Equal(
            [
                "DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_BlockedProcess] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON DATABASE;",
                "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON DATABASE;",
                "DROP EVENT SESSION [PerformanceMonitor_BlockedProcess] ON DATABASE;",
                "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON DATABASE;",
            ],
            plan.Select(p => p.Statement).ToArray());
        Assert.Equal(new string?[] { null, null, null, "alpha", "alpha", "zeta", "zeta" }, plan.Select(p => p.Session.Database).ToArray());
    }

    [Fact]
    public void PlanDrops_OfNothing_IsNothing() =>
        Assert.Empty(DarlingXeSessionCleanup.PlanDrops([]));

    [Fact]
    public void PlanDrops_RefusesADatabaseScopedSessionWithNoDatabase() =>
        Assert.Throws<ArgumentException>(() =>
            DarlingXeSessionCleanup.PlanDrops([new ExistingXeSession(Deadlock, XeSessionScope.Database)]));

    // ---- the executor over a fake target -------------------------------------------------------------------------------

    private sealed class FakeTarget : IXeSessionCleanupTarget
    {
        public List<ExistingXeSession> Found { get; } = [];

        public List<string> Problems { get; } = [];

        public List<string> Notes { get; } = [];

        public Exception? SearchFails { get; set; }

        public HashSet<string> Refused { get; } = [];

        public List<XeSessionDrop> Dropped { get; } = [];

        public Task<XeSessionSearch> FindSessionsAsync(CancellationToken cancellationToken) =>
            SearchFails is null
                ? Task.FromResult(new XeSessionSearch(Found.ToList(), Problems.ToList()) { Notes = Notes.ToList() })
                : throw SearchFails;

        public Task DropAsync(XeSessionDrop drop, CancellationToken cancellationToken)
        {
            if (Refused.Contains(drop.Statement))
            {
                throw new InvalidOperationException("permission denied");
            }

            Dropped.Add(drop);
            return Task.CompletedTask;
        }
    }

    private static async Task<(int Exit, string Output, string Error)> RunAsync(FakeTarget target, bool dryRun)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await DarlingXeSessionCleanup.RunAsync("sql01", dryRun, target, output, error, CancellationToken.None);
        return (exit, output.ToString(), error.ToString());
    }

    [Fact]
    public async Task ItPrintsEachSessionItFinds_AndDropsIt()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(LongQuery, XeSessionScope.Server));
        target.Found.Add(new ExistingXeSession(Blocked, XeSessionScope.Server));
        target.Found.Add(new ExistingXeSession(Deadlock, XeSessionScope.Server));

        var (exit, output, error) = await RunAsync(target, dryRun: false);

        Assert.Equal(0, exit);
        Assert.Equal(string.Empty, error);
        Assert.Equal(
            [
                "DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_BlockedProcess] ON SERVER;",
                "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;",
            ],
            target.Dropped.Select(d => d.Statement).ToArray());
        Assert.Contains($"Found {Deadlock} (server scope)", output, StringComparison.Ordinal);
        Assert.Contains($"[DROPPED] {Blocked} (server scope)", output, StringComparison.Ordinal);
        Assert.Contains($"Found {LongQuery} (server scope)", output, StringComparison.Ordinal);
        Assert.Contains($"[DROPPED] {LongQuery} (server scope)", output, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(output, "^WARNING: ", RegexOptions.Multiline));
    }

    [Fact]
    public async Task ARealDrop_SaysWhatStopsUntilTheServiceReconnects_OnceAndBeforeTheWarning()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(Deadlock, XeSessionScope.Server));
        target.Found.Add(new ExistingXeSession(Blocked, XeSessionScope.Server));

        var (_, output, _) = await RunAsync(target, dryRun: false);

        const string note = "NOTE: capture of deadlocks and blocked processes on that server stops until this service reconnects to it, so remove the server next.";
        Assert.Single(Regex.Matches(output, "^NOTE: ", RegexOptions.Multiline));
        Assert.Contains(note, output, StringComparison.Ordinal);
        Assert.True(
            output.IndexOf(note, StringComparison.Ordinal) < output.IndexOf("WARNING: ", StringComparison.Ordinal),
            "the note comes before the shared-names warning");
    }

    [Theory]
    [InlineData(new[] { Deadlock, Blocked }, "deadlocks and blocked processes")]
    [InlineData(new[] { Deadlock, Blocked, LongQuery }, "deadlocks, blocked processes and long query completions")]
    [InlineData(new[] { Blocked, LongQuery }, "blocked processes and long query completions")]
    [InlineData(new[] { Deadlock }, "deadlocks")]
    [InlineData(new[] { LongQuery }, "long query completions")]
    public void TheNoteNamesOnlyTheCapturesWhoseSessionWasDropped(string[] dropped, string captures) =>
        Assert.Equal(
            $"NOTE: capture of {captures} on that server stops until this service reconnects to it, so remove the server next.",
            DarlingXeSessionCleanup.CaptureStopsNote(dropped.Select(name => new ExistingXeSession(name, XeSessionScope.Server))));

    [Fact]
    public void ADropOnManyDatabases_NamesEachCaptureOnce() =>
        Assert.Equal(
            "NOTE: capture of deadlocks on that server stops until this service reconnects to it, so remove the server next.",
            DarlingXeSessionCleanup.CaptureStopsNote(
            [
                new ExistingXeSession(Deadlock, XeSessionScope.Database, "sales"),
                new ExistingXeSession(Deadlock, XeSessionScope.Database, "hr"),
            ]));

    [Fact]
    public void NothingDropped_HasNoNote() =>
        Assert.Null(DarlingXeSessionCleanup.CaptureStopsNote([]));

    [Fact]
    public async Task WhenEveryDropIsRefused_NothingStopped_SoNoNoteIsPrinted()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(Deadlock, XeSessionScope.Server));
        target.Refused.Add("DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;");

        var (exit, output, _) = await RunAsync(target, dryRun: false);

        Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable, exit);
        Assert.Empty(target.Dropped);
        Assert.DoesNotContain(CaptureStopsMarker, output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DryRun_ListsTheDrops_AndRunsNone()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(Deadlock, XeSessionScope.Database, "sales"));
        target.Found.Add(new ExistingXeSession(LongQuery, XeSessionScope.Database, "sales"));

        var (exit, output, _) = await RunAsync(target, dryRun: true);

        Assert.Equal(0, exit);
        Assert.Empty(target.Dropped);
        Assert.Contains("[WOULD DROP] DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON DATABASE;", output, StringComparison.Ordinal);
        Assert.Contains("[WOULD DROP] DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON DATABASE;", output, StringComparison.Ordinal);
        Assert.Contains($"Found {LongQuery} (database scope, in database sales)", output, StringComparison.Ordinal);
        Assert.Contains("in database sales", output, StringComparison.Ordinal);
        Assert.Contains("Dry run: nothing was dropped.", output, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(output, "^WARNING: ", RegexOptions.Multiline));
        Assert.DoesNotContain(CaptureStopsMarker, output, StringComparison.Ordinal);
        Assert.DoesNotContain("NOTE:", output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NothingThere_IsSuccess_AndSaysSo()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession("system_health", XeSessionScope.Server));

        var (exit, output, _) = await RunAsync(target, dryRun: false);

        Assert.Equal(0, exit);
        Assert.Empty(target.Dropped);
        Assert.Contains("nothing to drop in the places searched", output, StringComparison.Ordinal);
        Assert.DoesNotContain(CaptureStopsMarker, output, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WhenADatabaseCouldNotBeSearched_NothingFound_DoesNotClaimTheServerIsClean()
    {
        var target = new FakeTarget();
        target.Problems.Add("Could not search database hr for Extended Events sessions: login failed");

        var (exit, output, error) = await RunAsync(target, dryRun: false);

        Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable, exit);
        Assert.Contains("nothing to drop in the places searched", output, StringComparison.Ordinal);
        Assert.Contains("database hr", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ANoteFromTheSearch_IsPrintedOnStderr_AndLeavesTheExitCodeAndTheDropsAlone()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(Deadlock, XeSessionScope.Server));
        target.Notes.Add("Database hr is excluded from monitoring and could not be searched for the long-query completion session (login failed), so a session left there needs a manual drop.");

        var (exit, output, error) = await RunAsync(target, dryRun: false);

        Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.Success, exit);
        Assert.Single(target.Dropped);
        Assert.Contains($"[DROPPED] {Deadlock}", output, StringComparison.Ordinal);
        Assert.Equal(target.Notes[0] + Environment.NewLine, error);
        Assert.DoesNotContain("[FAILED]", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ANoteWithNothingFound_StillSaysNothingWasDroppedOnlyInThePlacesSearched()
    {
        var target = new FakeTarget();
        target.Notes.Add("Database hr is excluded from monitoring and could not be searched for the long-query completion session (login failed), so a session left there needs a manual drop.");

        var (exit, output, error) = await RunAsync(target, dryRun: false);

        Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.Success, exit);
        Assert.Contains("nothing to drop in the places searched", output, StringComparison.Ordinal);
        Assert.Contains("database hr", error, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ARefusedDrop_DoesNotStopTheNext_AndExitsWithTheUnavailableCode()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(Deadlock, XeSessionScope.Server));
        target.Found.Add(new ExistingXeSession(Blocked, XeSessionScope.Server));
        target.Found.Add(new ExistingXeSession(LongQuery, XeSessionScope.Server));
        target.Refused.Add("DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;");

        var (exit, _, error) = await RunAsync(target, dryRun: false);

        Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable, exit);
        Assert.Contains("[FAILED]", error, StringComparison.Ordinal);
        Assert.Contains("permission denied", error, StringComparison.Ordinal);
        Assert.Equal(
            ["DROP EVENT SESSION [PerformanceMonitor_BlockedProcess] ON SERVER;", "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;"],
            target.Dropped.Select(d => d.Statement).ToArray());
    }

    [Fact]
    public async Task ADatabaseThatCouldNotBeSearched_StillDropsTheRest_AndExitsWithTheUnavailableCode()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession(Blocked, XeSessionScope.Database, "sales"));
        target.Problems.Add("Could not search database hr for Extended Events sessions: login failed");

        var (exit, _, error) = await RunAsync(target, dryRun: false);

        Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable, exit);
        Assert.Contains("database hr", error, StringComparison.Ordinal);
        Assert.Single(target.Dropped);
    }

    [Fact]
    public async Task ATargetThatCannotBeSearched_ExitsWithTheUnavailableCode_AndNamesPrintSql()
    {
        var target = new FakeTarget { SearchFails = new InvalidOperationException("timeout") };

        var (exit, _, error) = await RunAsync(target, dryRun: false);

        Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable, exit);
        Assert.Contains("'sql01'", error, StringComparison.Ordinal);
        Assert.Contains("--print-sql", error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ANameTheTargetReturnsThatIsNotDarlings_IsNeverDropped()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession("system_health", XeSessionScope.Server));
        target.Found.Add(new ExistingXeSession("x]; DROP DATABASE prod; --", XeSessionScope.Server));

        await RunAsync(target, dryRun: false);

        Assert.Empty(target.Dropped);
    }

    // ---- the whole verb through the configuration, with the connection injected ------------------------------------------

    private static Task<IXeSessionCleanupTarget> ThrowingConnect(MonitoredServer server, IReadOnlyList<MonitoredServer> registry, XeCleanupStoreFacts facts, CancellationToken cancellationToken) =>
        throw new InvalidOperationException("the verb connected when it should not have");

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

    private static async Task<(int Exit, string Output, string Error, List<string> Connected)> RunVerbAsync(
        string[] rest, IXeSessionCleanupTarget? target = null, Exception? connectFails = null)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var connected = new List<string>();

        var exit = await DarlingCliCommands.DropXeSessionsAsync(
            rest,
            (server, _, _, _) =>
            {
                connected.Add(server.DisplayName);
                return connectFails is not null
                    ? throw connectFails
                    : Task.FromResult(target ?? throw new InvalidOperationException("no target"));
            },
            output,
            error,
            CancellationToken.None);

        return (exit, output.ToString(), error.ToString(), connected);
    }

    [Fact]
    public async Task AnUnknownServer_ExitsNonZero_AndNamesPrintSql_WithoutConnecting()
    {
        var root = Directory.CreateTempSubdirectory("drop-xe-unknown-");
        try
        {
            var (exit, _, error, connected) = await RunVerbAsync(["NOPE", "--config", WriteConfig(root)]);

            Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.UsageOrConfig, exit);
            Assert.Contains("No monitored server named 'NOPE'", error, StringComparison.Ordinal);
            Assert.Contains("--drop-xe-sessions --print-sql", error, StringComparison.Ordinal);
            Assert.Contains("SQL2022", error, StringComparison.Ordinal);
            Assert.Empty(connected);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task AMissingConfiguration_ExitsWithTheConfigurationCode()
    {
        var (exit, _, error, connected) = await RunVerbAsync(["SQL2022", "--config", Path.Combine(Path.GetTempPath(), "drop-xe-missing", "darling.json")]);

        Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.UsageOrConfig, exit);
        Assert.Contains("Could not load configuration", error, StringComparison.Ordinal);
        Assert.Empty(connected);
    }

    [Fact]
    public async Task AKnownServer_IsConnectedTo_AndItsSessionsAreDropped()
    {
        var root = Directory.CreateTempSubdirectory("drop-xe-known-");
        try
        {
            var target = new FakeTarget();
            target.Found.Add(new ExistingXeSession(Deadlock, XeSessionScope.Server));
            target.Found.Add(new ExistingXeSession(LongQuery, XeSessionScope.Server));

            var (exit, output, _, connected) = await RunVerbAsync(["sql2022", "--config", WriteConfig(root)], target);

            Assert.Equal(0, exit);
            Assert.Equal(["SQL2022"], connected);
            Assert.Equal(
                ["DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;", "DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;"],
                target.Dropped.Select(d => d.Statement).ToArray());
            Assert.Contains("[DROPPED]", output, StringComparison.Ordinal);
            Assert.Contains(
                "NOTE: capture of deadlocks and long query completions on that server stops until this service reconnects to it, so remove the server next.",
                output,
                StringComparison.Ordinal);

            /* The store here is unreachable, so the verb falls back to darling.json's list. It says what IT does with that list, not
               --validate-config's "validating" (whose own wording DarlingCliCommandsHostCheckTests pins). */
            Assert.Contains("so matching the server name against darling.json's own server list instead.", output, StringComparison.Ordinal);
            Assert.DoesNotContain("validating", output, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task DryRun_ThroughTheVerb_DropsNothing()
    {
        var root = Directory.CreateTempSubdirectory("drop-xe-dry-");
        try
        {
            var target = new FakeTarget();
            target.Found.Add(new ExistingXeSession(Deadlock, XeSessionScope.Server));

            var (exit, output, _, _) = await RunVerbAsync(["SQL2022", "--dry-run", "--config", WriteConfig(root)], target);

            Assert.Equal(0, exit);
            Assert.Empty(target.Dropped);
            Assert.Contains("[WOULD DROP]", output, StringComparison.Ordinal);
            Assert.DoesNotContain(CaptureStopsMarker, output, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task AServerThatCannotBeConnectedTo_ExitsWithTheUnavailableCode_AndNamesPrintSql()
    {
        var root = Directory.CreateTempSubdirectory("drop-xe-down-");
        try
        {
            var (exit, _, error, connected) = await RunVerbAsync(
                ["SQL2022", "--config", WriteConfig(root)], connectFails: new InvalidOperationException("network path not found"));

            Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.TargetUnavailable, exit);
            Assert.Equal(["SQL2022"], connected);
            Assert.Contains("Could not connect to 'SQL2022': network path not found", error, StringComparison.Ordinal);
            Assert.Contains("--print-sql", error, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    // ---- which server a name picks ---------------------------------------------------------------------------------------

    [Fact]
    public void ANameIsMatchedExactly_NeverByPartOfIt()
    {
        var a = new MonitoredServer { Name = "Sales", Host = "sql01", Database = "sales" };
        var b = new MonitoredServer { Name = "Hr", Host = "sql01", Database = "hr" };
        var servers = new[] { a, b };

        Assert.Equal([a], DarlingCliCommands.MatchDropXeSessionsTarget(servers, "sales"));
        Assert.Equal([b], DarlingCliCommands.MatchDropXeSessionsTarget(servers, b.StorageName));
        Assert.Empty(DarlingCliCommands.MatchDropXeSessionsTarget(servers, "sale"));
        Assert.Empty(DarlingCliCommands.MatchDropXeSessionsTarget(servers, "sql01"));
        Assert.Equal(2, DarlingCliCommands.MatchDropXeSessionsTarget([a, new MonitoredServer { Name = "sales", Host = "sql02" }], "SALES").Count);
    }

    // ---- what a removed server's answer tells the operator ---------------------------------------------------------------

    [Fact]
    public void RemoveServer_NamesTheVerb_ForAServerThatHadConnected()
    {
        var connected = DarlingMcpServerAdminTools.RemovedAnswer(
            new DarlingMcpServerAdminTools.ServerDefinition(new DarlingServerResolver.RegisteredServer(1, "sql01", "sql01"), true, "plain"), "exact");
        var never = DarlingMcpServerAdminTools.RemovedAnswer(
            new DarlingMcpServerAdminTools.ServerDefinition(new DarlingServerResolver.RegisteredServer(2, "sql02", "sql02"), false, "plain"), "exact");

        using var connectedDoc = JsonDocument.Parse(connected);
        using var neverDoc = JsonDocument.Parse(never);
        var note = connectedDoc.RootElement.GetProperty("note").GetString();
        Assert.Contains("--drop-xe-sessions", note, StringComparison.Ordinal);
        Assert.Contains("--print-sql", note, StringComparison.Ordinal);
        Assert.Contains("history are kept", note, StringComparison.Ordinal);

        /* #4961: the removal drops this install's own sessions, so the note no longer says every session stays. */
        Assert.Contains("also drops this install's own Extended Events sessions on the server", note, StringComparison.Ordinal);
        Assert.Contains("The shared sessions (", note, StringComparison.Ordinal);
        Assert.DoesNotContain("are not dropped and stay on the server", note, StringComparison.Ordinal);

        /* The server is gone by the time the answer is read, so the named form cannot find it: the first form the note points at is
           --print-sql, and the named form is mentioned only to say it had to run before the removal. */
        Assert.Equal(
            note!.IndexOf("--drop-xe-sessions", StringComparison.Ordinal),
            note.IndexOf("--drop-xe-sessions --print-sql", StringComparison.Ordinal));
        Assert.Contains("--drop-xe-sessions <server>) works only for a server this service still monitors, so it had to run before this removal", note, StringComparison.Ordinal);
        foreach (var name in new[] { Deadlock, Blocked, LongQuery })
        {
            Assert.Contains(name, note, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("--drop-xe-sessions", neverDoc.RootElement.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheReadmeNamesEverySessionTheVerbDrops_InItsSectionAndInTheRemoveServerBullet()
    {
        var readme = RepoFile.ReadRepoFile("Darling", "README.md").Replace("\r\n", "\n", StringComparison.Ordinal);

        var start = readme.IndexOf("### Drop the Extended Events sessions a removed server left behind", StringComparison.Ordinal);
        Assert.True(start >= 0, "the README no longer has the --drop-xe-sessions section (#4732)");
        var end = readme.IndexOf("\n---", start, StringComparison.Ordinal);
        Assert.True(end > start, "the --drop-xe-sessions section no longer ends at a horizontal rule");
        var section = readme[start..end];

        var bulletAt = readme.IndexOf("`remove_server` (which drops this install's own Extended Events sessions on the server", StringComparison.Ordinal);
        Assert.True(bulletAt >= 0, "the remove_server bullet no longer says what it leaves on the server (#4732)");
        var bullet = readme[bulletAt..Math.Min(readme.Length, bulletAt + 500)];

        foreach (var name in DarlingXeSessionCleanup.SessionNames)
        {
            Assert.Contains(name, section, StringComparison.Ordinal);
            Assert.Contains(name, bullet, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheReadmeSaysWhenToRunTheNamedForm_WhatItStops_WhatToRunAfterTheRemoval_AndWhichGrantDropsASession()
    {
        var readme = RepoFile.ReadRepoFile("Darling", "README.md").Replace("\r\n", "\n", StringComparison.Ordinal);

        var start = readme.IndexOf("### Drop the Extended Events sessions a removed server left behind", StringComparison.Ordinal);
        Assert.True(start >= 0, "the README no longer has the --drop-xe-sessions section (#4732)");
        var end = readme.IndexOf("\n---", start, StringComparison.Ordinal);
        Assert.True(end > start, "the --drop-xe-sessions section no longer ends at a horizontal rule");
        var section = readme[start..end];

        // When to run the named form, and what it stops on a server the service still monitors.
        Assert.Contains("just before you remove the server", section, StringComparison.Ordinal);
        Assert.Contains("stops its deadlock and blocked-process capture until this service reconnects to it", section, StringComparison.Ordinal);
        Assert.Contains("the named form works only until `remove_server`", section, StringComparison.Ordinal);
        Assert.Contains("prints one `NOTE:` line", section, StringComparison.Ordinal);

        // Who else creates the sessions again: the deprecated Full Dashboard installer as well as Lite and another Darling service.
        Assert.Contains("a Lite app, a deprecated Full Dashboard install or another Darling service", section, StringComparison.Ordinal);

        // The grant that drops a session is the one the DROP EVENT SESSION page lists; a create grant does not cover it.
        Assert.Contains("https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-event-session-transact-sql", section, StringComparison.Ordinal);
        Assert.Contains("`DROP ANY EVENT SESSION` (SQL Server 2022 and later) or `ALTER ANY EVENT SESSION`", section, StringComparison.Ordinal);
        Assert.Contains("`DROP ANY DATABASE EVENT SESSION` in each monitored database", section, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER ANY DATABASE EVENT SESSION", section, StringComparison.Ordinal);
        Assert.DoesNotContain("could create the sessions can drop them", section, StringComparison.Ordinal);
        Assert.Contains("`CREATE ANY DATABASE EVENT SESSION` on Azure SQL Database, for one) does not cover the drop", section, StringComparison.Ordinal);

        // The opening sentence carries one colon, not two.
        Assert.DoesNotContain("monitored server: the Extended Events sessions", section, StringComparison.Ordinal);

        // The other places that point at the verb say the same: after the removal it is --print-sql.
        var bulletAt = readme.IndexOf("`remove_server` (which drops this install's own Extended Events sessions on the server", StringComparison.Ordinal);
        Assert.True(bulletAt >= 0, "the remove_server bullet no longer says what it leaves on the server (#4732)");
        var bullet = readme[bulletAt..Math.Min(readme.Length, bulletAt + 1000)];
        Assert.Contains("`--drop-xe-sessions --print-sql`", bullet, StringComparison.Ordinal);
        Assert.Contains("works only before the removal", bullet, StringComparison.Ordinal);

        /* #4961: removing the server drops this install's long-query session; the verb's script is for an attempt that failed. */
        Assert.DoesNotContain("a server removed while the collector was on keeps the session", readme, StringComparison.Ordinal);
        var collectorAt = readme.IndexOf("Removing the server also drops this install's session on it", StringComparison.Ordinal);
        Assert.True(collectorAt >= 0, "the long_query_completions paragraph no longer says removing a server drops this install's session (#4961)");
        var collector = readme[collectorAt..Math.Min(readme.Length, collectorAt + 500)];
        Assert.Contains("If that attempt failed or ran out of time", collector, StringComparison.Ordinal);
        Assert.Contains("`--drop-xe-sessions --print-sql`", collector, StringComparison.Ordinal);

        // The list of verbs that survive an unusable store connection names this one.
        var listAt = readme.IndexOf("Every other verb that opens the store (", StringComparison.Ordinal);
        Assert.True(listAt >= 0, "the README no longer lists the verbs that open the store");
        Assert.Contains("`--drop-xe-sessions <server-name>`", readme[listAt..Math.Min(readme.Length, listAt + 400)], StringComparison.Ordinal);
    }

    [Fact]
    public void TheUsageAndTheReadme_SayAnExcludedDatabaseThatCannotBeOpenedLeavesTheExitCodeAlone_AndAMonitoredOneExitsTwo()
    {
        var usage = DarlingCliCommands.DropXeSessionsUsageText();
        Assert.Contains("An excluded database that cannot be opened is reported as a note and does not change the exit code", usage, StringComparison.Ordinal);
        Assert.Contains("a monitored database that cannot be opened exits 2", usage, StringComparison.Ordinal);

        var readme = RepoFile.ReadRepoFile("Darling", "README.md").Replace("\r\n", "\n", StringComparison.Ordinal);
        var start = readme.IndexOf("### Drop the Extended Events sessions a removed server left behind", StringComparison.Ordinal);
        Assert.True(start >= 0, "the README no longer has the --drop-xe-sessions section (#4732)");
        var end = readme.IndexOf("\n---", start, StringComparison.Ordinal);
        Assert.True(end > start, "the --drop-xe-sessions section no longer ends at a horizontal rule");
        var section = readme[start..end];

        Assert.Contains("is reported in a note on stderr and does not change the exit code", section, StringComparison.Ordinal);
        Assert.Contains("a monitored database it cannot open makes the verb exit `2`", section, StringComparison.Ordinal);
    }
}
