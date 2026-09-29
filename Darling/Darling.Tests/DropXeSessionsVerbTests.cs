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
/// #4732: removing a server leaves its Extended Events sessions on it, and <c>--drop-xe-sessions</c> is the operator's
/// explicit way to remove them. The pure halves (the grammar, the statements, the plan) pin as text; the executor runs over a
/// fake target so the find-print-drop path needs no SQL Server.
/// </summary>
public sealed class DropXeSessionsVerbTests
{
    private const string Deadlock = "PerformanceMonitor_Deadlock";
    private const string Blocked = "PerformanceMonitor_BlockedProcess";
    private const string LongQuery = "PerformanceMonitor_LongQueryCompletions";

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

        Assert.Equal(6, Regex.Matches(code, "DROP EVENT SESSION", RegexOptions.CultureInvariant).Count);
        Assert.Equal(6, Regex.Matches(code, @"IF EXISTS", RegexOptions.CultureInvariant).Count);
        Assert.Equal(
            new[] { Blocked, Deadlock, LongQuery },
            Regex.Matches(code, @"PerformanceMonitor_\w+").Select(m => m.Value).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal(
            new[] { Deadlock, Blocked, LongQuery },
            DarlingXeSessionCleanup.SessionNames.ToArray());
        Assert.Equal(
            new[] { DeadlocksCollector.XeSessionName, BlockedProcessReportCollector.XeSessionName, LongQueryCompletionsCollector.XeSessionName },
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

        public Exception? SearchFails { get; set; }

        public HashSet<string> Refused { get; } = [];

        public List<XeSessionDrop> Dropped { get; } = [];

        public Task<XeSessionSearch> FindSessionsAsync(CancellationToken cancellationToken) =>
            SearchFails is null
                ? Task.FromResult(new XeSessionSearch(Found.ToList(), Problems.ToList()))
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
    }

    [Fact]
    public async Task NothingThere_IsSuccess_AndSaysSo()
    {
        var target = new FakeTarget();
        target.Found.Add(new ExistingXeSession("system_health", XeSessionScope.Server));

        var (exit, output, _) = await RunAsync(target, dryRun: false);

        Assert.Equal(0, exit);
        Assert.Empty(target.Dropped);
        Assert.Contains("nothing to drop", output, StringComparison.Ordinal);
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

    private static Task<IXeSessionCleanupTarget> ThrowingConnect(MonitoredServer server, CancellationToken cancellationToken) =>
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
            (server, _) =>
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
        foreach (var name in new[] { Deadlock, Blocked, LongQuery })
        {
            Assert.Contains(name, note, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("--drop-xe-sessions", neverDoc.RootElement.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheReadmeNamesEverySessionTheVerbDrops_InItsSectionAndInTheRemoveServerBullet()
    {
        var readme = RepoFile.ReadRepoFileLf("Darling", "README.md");

        var start = readme.IndexOf("### Drop the Extended Events sessions a removed server left behind", StringComparison.Ordinal);
        Assert.True(start >= 0, "the README no longer has the --drop-xe-sessions section (#4732)");
        var end = readme.IndexOf("\n---", start, StringComparison.Ordinal);
        Assert.True(end > start, "the --drop-xe-sessions section no longer ends at a horizontal rule");
        var section = readme[start..end];

        var bulletAt = readme.IndexOf("`remove_server` (which leaves the server's Extended Events sessions on it", StringComparison.Ordinal);
        Assert.True(bulletAt >= 0, "the remove_server bullet no longer says what it leaves on the server (#4732)");
        var bullet = readme[bulletAt..Math.Min(readme.Length, bulletAt + 500)];

        foreach (var name in DarlingXeSessionCleanup.SessionNames)
        {
            Assert.Contains(name, section, StringComparison.Ordinal);
            Assert.Contains(name, bullet, StringComparison.Ordinal);
        }
    }
}
