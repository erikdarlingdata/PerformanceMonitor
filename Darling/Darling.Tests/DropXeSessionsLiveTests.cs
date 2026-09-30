/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4732: <c>SqlServerXeSessionCleanupTarget</c> is the only code that drops anything on a monitored server, and every other
/// test of the verb drives a fake target. The live test creates a server-scoped Extended Events session with a name of its own,
/// runs the target's real find and drop for that name alone, and checks the server itself: the find reports the session, a
/// dry run leaves it in place, the drop removes it, and a second drop is refused by the server. It runs only when
/// DARLING_TEST_SQL (SQL Server host; optional DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD for sql auth) is set, as the
/// collector runner's end-to-end tests do, and it never names one of Darling's three sessions: a server's real sessions may
/// be in use, so the target is built with the test-only name and drops only that. The other tests here need no server; they
/// pin how the target treats the names it is given, and that a target given Darling's names sends the plan's own find and
/// DROP text, so a green live run stands for the text the product sends.
/// </summary>
public sealed class DropXeSessionsLiveTests
{
    private const string TestOnlyPrefix = "PerformanceMonitor_TestOnly_";

    /// <summary>A runtime that connects to nothing: enough to build a target whose text and guards are read, not run.</summary>
    private static ServerRuntime UnreachableRuntime() => new()
    {
        Config = new MonitoredServer { Name = "unreachable", Host = "unreachable" },
        ConnectionString = "Server=127.0.0.1,1;Connect Timeout=1;Encrypt=False",
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = "unreachable",
        ServerId = 1,
    };

    private static string NewTestOnlyName() => TestOnlyPrefix + Guid.NewGuid().ToString("N")[..8];

    private static string[] QuotedNames(string sql) =>
        Regex.Matches(sql, "N'([^']*)'").Select(m => m.Groups[1].Value).ToArray();

    // ---- no server needed ---------------------------------------------------------------------------------------------

    /// <summary>
    /// The text a product target sends, held to the text the plan pins, byte for byte. The live case builds its target with a
    /// test-only name, so what it proves about SQL is what this proves about the product's names: the target composes its find
    /// and DROP text through the same methods the plan's constants and statements are built with, and a target that holds
    /// Darling's names sends exactly the plan's find constants and, for every name in both scopes, exactly the DROP the plan
    /// built for it (#4732). <c>Assert.Equal</c> on two strings ignores nothing, so a difference in one character, or in a
    /// line ending, fails.
    /// </summary>
    private static void AssertSendsThePlansText(SqlServerXeSessionCleanupTarget target)
    {
        Assert.Equal(DarlingXeSessionCleanup.FindServerSessionsSql, target.FindServerSql);
        Assert.Equal(DarlingXeSessionCleanup.FindDatabaseSessionsSql, target.FindDatabaseSql);

        var existing = DarlingXeSessionCleanup.SessionNames
            .SelectMany(n => new[]
            {
                new ExistingXeSession(n, XeSessionScope.Server),
                new ExistingXeSession(n, XeSessionScope.Database, "AnyDatabase"),
            })
            .ToList();
        var plan = DarlingXeSessionCleanup.PlanDrops(existing);
        Assert.Equal(existing.Count, plan.Count);

        foreach (var name in DarlingXeSessionCleanup.SessionNames)
        {
            foreach (var scope in new[] { XeSessionScope.Server, XeSessionScope.Database })
            {
                var planned = Assert.Single(plan, d => d.Session.Name == name && d.Session.Scope == scope);
                Assert.Equal(planned.Statement, target.StatementFor(planned));
            }
        }
    }

    [Fact]
    public void ATargetGivenNoNames_SendsTheFindAndDropTextThePlanPins() =>
        AssertSendsThePlansText(new SqlServerXeSessionCleanupTarget(UnreachableRuntime()));

    /// <summary>A copy of the names is a different reference from <c>SessionNames</c>: no shortcut on the reference can be
    /// what makes this pass.</summary>
    [Fact]
    public void ATargetGivenACopyOfDarlingsNames_SendsTheFindAndDropTextThePlanPins() =>
        AssertSendsThePlansText(new SqlServerXeSessionCleanupTarget(UnreachableRuntime(), DarlingXeSessionCleanup.SessionNames.ToArray()));

    /// <summary>The product's own target has no other gate on a DROP than its list of names, which is Darling's, spelled exactly
    /// (#4732).</summary>
    [Theory]
    [InlineData("system_health")]
    [InlineData("performancemonitor_deadlock")]
    [InlineData("PerformanceMonitor_Deadlock]; DROP DATABASE x; --")]
    public async Task ATargetGivenNoNames_RefusesAnyNameButDarlingsOwnSpelling_BeforeItConnects(string other)
    {
        var target = new SqlServerXeSessionCleanupTarget(UnreachableRuntime());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            target.DropAsync(new XeSessionDrop(new ExistingXeSession(other, XeSessionScope.Server)), CancellationToken.None));
    }

    [Fact]
    public void ATargetGivenItsOwnName_SearchesForThatNameAlone_InBothScopes()
    {
        var name = NewTestOnlyName();
        var target = new SqlServerXeSessionCleanupTarget(UnreachableRuntime(), new[] { name });

        Assert.Equal(new[] { name }, QuotedNames(target.FindServerSql));
        Assert.Equal(new[] { name }, QuotedNames(target.FindDatabaseSql));
        Assert.Contains("FROM sys.server_event_sessions AS ses", target.FindServerSql, StringComparison.Ordinal);
        Assert.Contains("FROM sys.database_event_sessions AS des", target.FindDatabaseSql, StringComparison.Ordinal);
        foreach (var darlingName in DarlingXeSessionCleanup.SessionNames)
        {
            Assert.DoesNotContain(darlingName, target.FindServerSql, StringComparison.Ordinal);
            Assert.DoesNotContain(darlingName, target.FindDatabaseSql, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("PerformanceMonitor_Deadlock")]
    [InlineData("PerformanceMonitor_BlockedProcess")]
    [InlineData("PerformanceMonitor_LongQueryCompletions")]
    [InlineData("SomeoneElsesSession")]
    public async Task ATargetGivenItsOwnName_RefusesAnyOtherName_BeforeItConnects(string other)
    {
        var target = new SqlServerXeSessionCleanupTarget(UnreachableRuntime(), new[] { NewTestOnlyName() });

        /* An ArgumentException, not the SqlException an attempt to connect to the unreachable server would give. */
        await Assert.ThrowsAsync<ArgumentException>(() =>
            target.DropAsync(new XeSessionDrop(new ExistingXeSession(other, XeSessionScope.Server)), CancellationToken.None));
    }

    [Fact]
    public async Task ATargetGivenItsOwnName_RefusesADifferentSpellingOfIt_BeforeItConnects()
    {
        var name = NewTestOnlyName();
        var target = new SqlServerXeSessionCleanupTarget(UnreachableRuntime(), new[] { name });

        await Assert.ThrowsAsync<ArgumentException>(() =>
            target.DropAsync(new XeSessionDrop(new ExistingXeSession(name.ToUpperInvariant(), XeSessionScope.Server)), CancellationToken.None));
    }

    [Fact]
    public void ATargetNeedsAtLeastOneName_AndNoBlankOne()
    {
        Assert.Throws<ArgumentException>(() => new SqlServerXeSessionCleanupTarget(UnreachableRuntime(), Array.Empty<string>()));
        Assert.Throws<ArgumentException>(() => new SqlServerXeSessionCleanupTarget(UnreachableRuntime(), new[] { " " }));
    }

    // ---- the live SQL Server -------------------------------------------------------------------------------------------

    [Fact]
    public async Task TheRealFindAndDrop_RemoveATestOnlySession_AndADryRunLeavesItInPlace()
    {
        var sqlHost = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        Assert.SkipWhen(string.IsNullOrEmpty(sqlHost),
            "Set DARLING_TEST_SQL to run the live drop-xe-sessions drop test.");

        var ct = TestContext.Current.CancellationToken;
        var sqlUser = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var config = new MonitoredServer
        {
            Name = "darling-drop-xe-e2e",
            Host = sqlHost!,
            Auth = string.IsNullOrEmpty(sqlUser) ? "integrated" : "sql",
            Username = sqlUser,
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
        };

        var runtime = await DarlingServerConnector.ConnectAsync(config, logger: null, ct);
        Assert.SkipWhen(runtime.Target.IsAzureSqlDb, "Azure SQL Database has no server-scoped Extended Events sessions to create.");

        var name = NewTestOnlyName();
        Assert.DoesNotContain(DarlingXeSessionCleanup.SessionNames, n => string.Equals(n, name, StringComparison.OrdinalIgnoreCase));

        try
        {
            await ExecuteAsync(runtime.ConnectionString,
                $"CREATE EVENT SESSION {DarlingXeSessionCleanup.BracketQuote(name)} ON SERVER ADD EVENT sqlserver.error_reported "
                + "ADD TARGET package0.ring_buffer (SET max_memory = 1024) WITH (STARTUP_STATE = OFF);",
                ct);
            await ExecuteAsync(runtime.ConnectionString,
                $"ALTER EVENT SESSION {DarlingXeSessionCleanup.BracketQuote(name)} ON SERVER STATE = START;", ct);
            Assert.True(await SessionExistsAsync(runtime.ConnectionString, name, ct), "the test session should exist after it is created");

            var target = new SqlServerXeSessionCleanupTarget(runtime, new[] { name });

            /* The find reports the session, at server scope, and nothing else. */
            var before = await target.FindSessionsAsync(ct);
            Assert.Empty(before.Problems);
            var found = Assert.Single(before.Sessions);
            Assert.Equal(new ExistingXeSession(name, XeSessionScope.Server), found);

            /* The dry run: the executor's own path over the real target. The plan only ever plans Darling's three names,
               so for this name it reports nothing to drop; what it shows is that the dry-run path searches a real server
               without an error, drops nothing, and leaves the session where it was. */
            var output = new StringWriter();
            var error = new StringWriter();
            var dryRunExit = await DarlingXeSessionCleanup.RunAsync("live-test", dryRun: true, target, output, error, ct);
            Assert.Equal(DarlingCliCommands.DropXeSessionsExitCode.Success, dryRunExit);
            Assert.Equal(string.Empty, error.ToString());
            Assert.DoesNotContain("[DROPPED]", output.ToString(), StringComparison.Ordinal);
            Assert.True(await SessionExistsAsync(runtime.ConnectionString, name, ct), "a dry run must leave the session in place");

            /* The drop removes it, and the server agrees. */
            await target.DropAsync(new XeSessionDrop(found), ct);
            Assert.False(await SessionExistsAsync(runtime.ConnectionString, name, ct), "the drop should have removed the session");
            Assert.Empty((await target.FindSessionsAsync(ct)).Sessions);

            /* A drop the server refuses (the session is gone) reaches the caller as an exception, which is what the
               executor turns into its [FAILED] line and exit code 2. */
            await Assert.ThrowsAsync<SqlException>(() => target.DropAsync(new XeSessionDrop(found), ct));
        }
        finally
        {
            await DropTestSessionIfExistsAsync(runtime.ConnectionString, name);
        }
    }

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Read straight from the catalog, not through the target under test.</summary>
    private static async Task<bool> SessionExistsAsync(string connectionString, string name, CancellationToken ct)
    {
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        using var command = new SqlCommand("SELECT COUNT(*) FROM sys.server_event_sessions WHERE name = @name;", connection) { CommandTimeout = 60 };
        command.Parameters.AddWithValue("@name", name);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) > 0;
    }

    /// <summary>The cleanup that runs even when the test fails. It drops the test-only name and refuses any other, so it can
    /// never reach one of Darling's sessions.</summary>
    private static async Task DropTestSessionIfExistsAsync(string connectionString, string name)
    {
        if (!name.StartsWith(TestOnlyPrefix, StringComparison.Ordinal))
        {
            throw new InvalidOperationException("The cleanup only drops the session this test created.");
        }

        if (await SessionExistsAsync(connectionString, name, CancellationToken.None))
        {
            await ExecuteAsync(connectionString, $"DROP EVENT SESSION {DarlingXeSessionCleanup.BracketQuote(name)} ON SERVER;", CancellationToken.None);
        }
    }
}
