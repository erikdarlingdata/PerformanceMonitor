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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4961: on Azure SQL Database a session cannot be dropped over a read-only connection. For a registration with read-only
/// intent, <c>--drop-xe-sessions</c> stops each session over the registration's own connection when it runs there, then drops
/// it over a connection with the intent forced off. Every other drop stays one statement over the registration's own
/// connection. The target opens its connections through a test seam here, so the statements and the connection each one runs
/// over pin without a SQL Server.
/// </summary>
public sealed class DropXeSessionsReadOnlyIntentTests
{
    private const string Host = "dropxe-ro.database.windows.net";
    private const string InstallId = "1a2b3c4d";
    private const string Database = "beta";

    /// <summary>One thing the target did: the connection it did it over, what it did, and the statement or the name.</summary>
    private sealed record Call(string ConnectionString, string Kind, string Text);

    /// <summary>A stand-in for one open connection: it records what it is asked and answers whether the session runs there.</summary>
    private sealed class FakeDatabase : IAlwaysOnXeDatabase
    {
        private readonly string _connectionString;
        private readonly List<Call> _calls;
        private readonly bool _running;

        public FakeDatabase(string connectionString, List<Call> calls, bool running)
        {
            _connectionString = connectionString;
            _calls = calls;
            _running = running;
        }

        public Task<AlwaysOnXeCatalog> ReadCatalogAsync(AlwaysOnXeSessionKind kind, string sessionName, CancellationToken cancellationToken) =>
            throw new NotSupportedException("A drop reads no catalog.");

        public Task<bool> IsStartedAsync(string sessionName, CancellationToken cancellationToken)
        {
            _calls.Add(new Call(_connectionString, "Runs", sessionName));
            return Task.FromResult(_running);
        }

        public Task ExecuteAsync(string statement, CancellationToken cancellationToken)
        {
            _calls.Add(new Call(_connectionString, "Execute", statement));
            return Task.CompletedTask;
        }

        public bool IsAlreadyPresent(Exception exception) => false;
    }

    private static string AzureConnectionString(bool readOnlyIntent) =>
        $"Server=tcp:{Host},1433;Initial Catalog=master;Encrypt=True" + (readOnlyIntent ? ";ApplicationIntent=ReadOnly" : string.Empty);

    /// <summary>A product target (no names of its own) for a registration, with every connection it opens recorded.</summary>
    private static (SqlServerXeSessionCleanupTarget Target, List<Call> Calls) Rig(string connectionString, bool azure, bool running)
    {
        var calls = new List<Call>();
        var target = new SqlServerXeSessionCleanupTarget(
            new ServerRuntime
            {
                Config = new MonitoredServer { Name = "dropxe", Host = Host },
                ConnectionString = connectionString,
                Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
                StorageName = Host,
                ServerId = 1,
            },
            sessionNames: null,
            registry: null,
            facts: new XeCleanupStoreFacts(InstallId))
        {
            OpenDatabaseForTests = (opened, _) => Task.FromResult<IAlwaysOnXeDatabase>(new FakeDatabase(opened, calls, running)),
        };
        return (target, calls);
    }

    private static ApplicationIntent IntentOf(string connectionString) =>
        new SqlConnectionStringBuilder(connectionString).ApplicationIntent;

    private static string DatabaseOf(string connectionString) =>
        new SqlConnectionStringBuilder(connectionString).InitialCatalog;

    private static XeSessionDrop DatabaseDrop(string name) =>
        new(new ExistingXeSession(name, XeSessionScope.Database, Database), InstallId);

    /// <summary>Every name the verb may drop for this install, so each one is a name a stop must reach too.</summary>
    public static TheoryData<string> AllowedNames()
    {
        var names = new TheoryData<string>();
        foreach (var name in DarlingXeSessionCleanup.NamesToDrop(InstallId))
        {
            names.Add(name);
        }

        return names;
    }

    // ---- the drop itself ------------------------------------------------------------------------------------------------

    [Theory]
    [MemberData(nameof(AllowedNames))]
    public async Task AReadOnlyIntentDrop_OfASessionThatRunsOnTheReplica_StopsOverTheOwnConnection_ThenDropsOverOneWithoutTheIntent(string name)
    {
        var (target, calls) = Rig(AzureConnectionString(readOnlyIntent: true), azure: true, running: true);
        var drop = DatabaseDrop(name);

        await target.DropAsync(drop, CancellationToken.None);

        Assert.Equal(new[] { "Runs", "Execute", "Execute" }, calls.Select(c => c.Kind));
        Assert.Equal(name, calls[0].Text);
        Assert.Equal(DarlingXeSessionCleanup.StopStatement(name, InstallId), calls[1].Text);
        Assert.Equal(drop.Statement, calls[2].Text);

        /* The check and the stop go over the registration's own read-only connection, to the session's database. */
        Assert.All(calls.Take(2), c =>
        {
            Assert.Equal(ApplicationIntent.ReadOnly, IntentOf(c.ConnectionString));
            Assert.Equal(Database, DatabaseOf(c.ConnectionString));
        });

        /* The drop goes over the same database with the intent forced off. */
        Assert.Equal(ApplicationIntent.ReadWrite, IntentOf(calls[2].ConnectionString));
        Assert.Equal(Database, DatabaseOf(calls[2].ConnectionString));
    }

    [Theory]
    [MemberData(nameof(AllowedNames))]
    public async Task AReadOnlyIntentDrop_OfASessionThatDoesNotRunOnTheReplica_SendsOnlyTheDropOverOneWithoutTheIntent(string name)
    {
        var (target, calls) = Rig(AzureConnectionString(readOnlyIntent: true), azure: true, running: false);
        var drop = DatabaseDrop(name);

        await target.DropAsync(drop, CancellationToken.None);

        Assert.Equal(new[] { "Runs", "Execute" }, calls.Select(c => c.Kind));
        Assert.Equal(drop.Statement, calls[1].Text);
        Assert.DoesNotContain(calls, c => c.Text.Contains("STATE = STOP", StringComparison.Ordinal));
        Assert.Equal(ApplicationIntent.ReadWrite, IntentOf(calls[1].ConnectionString));
        Assert.Equal(Database, DatabaseOf(calls[1].ConnectionString));
    }

    [Theory]
    [MemberData(nameof(AllowedNames))]
    public async Task ADropWithoutReadOnlyIntent_IsOneDropOverTheRegistrationsOwnConnection(string name)
    {
        var (target, calls) = Rig(AzureConnectionString(readOnlyIntent: false), azure: true, running: true);
        var drop = DatabaseDrop(name);

        await target.DropAsync(drop, CancellationToken.None);

        var only = Assert.Single(calls);
        Assert.Equal("Execute", only.Kind);
        Assert.Equal(drop.Statement, only.Text);
        Assert.Equal(ApplicationIntent.ReadWrite, IntentOf(only.ConnectionString));
        Assert.Equal(Database, DatabaseOf(only.ConnectionString));
    }

    /// <summary>A server-scoped target (SQL Server, Managed Instance, RDS) never stops anything first: its drop is one statement
    /// over the registration's own connection, intent and all.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AnOnPremisesDrop_IsOneDropOverTheRegistrationsOwnConnection_WhateverItsIntent(bool readOnlyIntent)
    {
        var own = "Server=sql01,1433;Encrypt=True" + (readOnlyIntent ? ";ApplicationIntent=ReadOnly" : string.Empty);
        var (target, calls) = Rig(own, azure: false, running: true);
        var drop = new XeSessionDrop(new ExistingXeSession(DeadlocksCollector.XeSessionName, XeSessionScope.Server), InstallId);

        await target.DropAsync(drop, CancellationToken.None);

        var only = Assert.Single(calls);
        Assert.Equal("Execute", only.Kind);
        Assert.Equal(drop.Statement, only.Text);
        Assert.Equal(readOnlyIntent ? ApplicationIntent.ReadOnly : ApplicationIntent.ReadWrite, IntentOf(only.ConnectionString));
    }

    /// <summary>A name the target may not drop is refused for the stop as for the drop, before any connection opens.</summary>
    [Theory]
    [InlineData("system_health")]
    [InlineData("performancemonitor_deadlock")]
    [InlineData("PerformanceMonitor_Deadlock]; DROP DATABASE x; --")]
    [InlineData("PerformanceMonitor_Darling_ffffffff_LongQueryCompletions")]
    public async Task AReadOnlyIntentDrop_OfANameTheTargetMayNotDrop_IsRefusedBeforeAnyConnectionOpens(string name)
    {
        var (target, calls) = Rig(AzureConnectionString(readOnlyIntent: true), azure: true, running: true);

        await Assert.ThrowsAsync<ArgumentException>(() => target.DropAsync(DatabaseDrop(name), CancellationToken.None));

        Assert.Empty(calls);
    }

    // ---- the stop's text and its allow-list -------------------------------------------------------------------------

    /// <summary>The stop reaches exactly the names the drop does: one list decides both, spelled exactly, for this install's id.
    /// A name outside it, another install's, a case variant, and an own name with no install id are refused by both.</summary>
    [Fact]
    public void TheStop_AcceptsExactlyTheNamesTheDropAccepts()
    {
        var allowed = DarlingXeSessionCleanup.NamesToDrop(InstallId);
        var candidates = allowed
            .Concat(allowed.Select(n => n.ToUpperInvariant()))
            .Concat(allowed.Select(n => n.ToLowerInvariant()))
            .Concat(new[]
            {
                "system_health",
                string.Empty,
                "PerformanceMonitor_Darling_ffffffff_LongQueryCompletions",
                "PerformanceMonitor_Darling_ffffffff_Deadlock",
                "PerformanceMonitor_Lite_1a2b3c4d_BlockedProcess",
                "PerformanceMonitor_Deadlock]; DROP DATABASE x; --",
                "master'; DROP DATABASE x;--",
            })
            .Distinct(StringComparer.Ordinal)
            .ToList();

        foreach (var installId in new string?[] { InstallId, null, "ffffffff" })
        {
            foreach (var name in candidates)
            {
                var dropAccepts = Accepts(() => DarlingXeSessionCleanup.DropStatement(name, XeSessionScope.Database, installId));
                var stopAccepts = Accepts(() => DarlingXeSessionCleanup.StopStatement(name, installId));
                Assert.True(dropAccepts == stopAccepts, $"'{name}' for install '{installId}': drop accepts {dropAccepts}, stop accepts {stopAccepts}");
                Assert.Equal(dropAccepts, DarlingXeSessionCleanup.NamesToDrop(installId).Contains(name, StringComparer.Ordinal));
            }
        }
    }

    private static bool Accepts(Func<string> build)
    {
        try
        {
            build();
            return true;
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    /// <summary>A long-query name, the install's own or the old shared one, is stopped by the collector's builder, which guards
    /// on the sessions that run in the database. The own deadlock and blocked-process names use the always-on builder.</summary>
    [Fact]
    public void TheStop_OfANameABuilderTakes_IsThatBuildersText()
    {
        var own = DarlingXeSessionCleanup.InstallSessionNames(InstallId);
        var ownLongQuery = own.Single(LongQueryCompletionsCollector.IsInstallSessionName);

        Assert.Equal(LongQueryCompletionsCollector.BuildStopSessionSql(ownLongQuery), DarlingXeSessionCleanup.StopStatement(ownLongQuery, InstallId));
        Assert.Equal(
            LongQueryCompletionsCollector.BuildStopSessionSql(LongQueryCompletionsCollector.LegacyXeSessionName),
            DarlingXeSessionCleanup.StopStatement(LongQueryCompletionsCollector.LegacyXeSessionName, InstallId));
        foreach (var kind in new[] { AlwaysOnXeSessionKind.Deadlock, AlwaysOnXeSessionKind.BlockedProcess })
        {
            var ownName = own.Single(n => AlwaysOnXeSessions.IsOwnName(n, kind));
            Assert.Equal(AlwaysOnXeSessions.BuildAzureStopSql(kind, ownName), DarlingXeSessionCleanup.StopStatement(ownName, InstallId));
        }
    }

    /// <summary>The shared deadlock and blocked-process names are ones no builder takes, so the verb builds their stop the way the
    /// long-query one is built: a guard on <c>sys.dm_xe_database_sessions</c>, which lists the sessions that run on the replica
    /// the connection reaches, around the one STOP.</summary>
    [Theory]
    [InlineData("PerformanceMonitor_Deadlock")]
    [InlineData("PerformanceMonitor_BlockedProcess")]
    public void TheStop_OfASharedName_IsGuardedOnTheSessionsThatRunInTheDatabase(string name)
    {
        var stop = DarlingXeSessionCleanup.StopStatement(name, InstallId);

        Assert.Equal(
            "\nIF EXISTS\n(\n    SELECT\n        1/0\n    FROM sys.dm_xe_database_sessions\n"
            + $"    WHERE name = N'{name}'\n)\nBEGIN\n    ALTER EVENT SESSION [{name}] ON DATABASE STATE = STOP;\nEND;",
            stop.Replace("\r\n", "\n", StringComparison.Ordinal));
    }
}
