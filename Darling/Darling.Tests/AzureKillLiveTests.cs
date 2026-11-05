/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Azure SQL Database kill against real Azure SQL Databases (#5632): <see cref="SqlServerRemediationExecutor.KillAzureSessionAsync"/>,
/// run as a remediation user that holds only the documented grants. Each database named in <c>DARLING_TEST_AZURE_DATABASES</c> (semicolon
/// separated, for example a General Purpose and a Hyperscale database on one logical server) must hold these contained users, all with the
/// password in <c>DARLING_TEST_AZURE_PASSWORD</c>, and the table <c>dbo.kill5632 (id int PRIMARY KEY, v int)</c> with one row:
/// <list type="bullet">
/// <item><c>pm5632_fix</c>: the remediation user, with VIEW DATABASE STATE and KILL DATABASE CONNECTION.</item>
/// <item><c>pm5632_nokill</c>: VIEW DATABASE STATE only, for the missing-grant refusal.</item>
/// <item><c>pm5632_mon</c>: the monitoring user, with VIEW DATABASE STATE.</item>
/// <item><c>pm5632_app</c>: the session that is killed, with SELECT and UPDATE on dbo.kill5632.</item>
/// </list>
/// Skips unless <c>DARLING_TEST_AZURE_SQL</c> (the logical server's host name) is set; CI never sets it.
/// </summary>
public sealed class AzureKillLiveTests
{
    private const string Fix = "pm5632_fix";
    private const string NoKill = "pm5632_nokill";
    private const string Monitor = "pm5632_mon";
    private const string App = "pm5632_app";
    private const string SecretEnv = "DARLING_TEST_AZURE_PASSWORD";

    private static string? Host => Environment.GetEnvironmentVariable("DARLING_TEST_AZURE_SQL");

    private static List<string> DatabaseNames() =>
        [.. (Environment.GetEnvironmentVariable("DARLING_TEST_AZURE_DATABASES") ?? "").Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)];

    public static TheoryData<string> Databases()
    {
        var data = new TheoryData<string>();
        foreach (var name in DatabaseNames())
            data.Add(name);
        if (data.Count == 0)
            data.Add("(none)");
        return data;
    }

    private static void SkipUnlessSet(string database) =>
        Assert.SkipWhen(string.IsNullOrEmpty(Host) || database == "(none)" || string.IsNullOrEmpty(Environment.GetEnvironmentVariable(SecretEnv)),
            "Set DARLING_TEST_AZURE_SQL, DARLING_TEST_AZURE_DATABASES and DARLING_TEST_AZURE_PASSWORD to run the Azure SQL Database kill live tests.");

    private static SqlConnectionStringBuilder Builder(string database, string user, string program = "pm5632_victim") => new()
    {
        DataSource = "tcp:" + Host + ",1433",
        InitialCatalog = database,
        UserID = user,
        Password = Environment.GetEnvironmentVariable(SecretEnv),
        Encrypt = SqlConnectionEncryptOption.Mandatory,
        ConnectTimeout = 60,
        /* No connection resiliency: with it the driver quietly reconnects after a kill, and the test could not see the session end. */
        ConnectRetryCount = 0,
        Pooling = false,
        ApplicationName = program,
    };

    private static SqlServerRemediationExecutor Executor(string user = Fix, string? monitoringUser = null) =>
        new(1, new RemediationServer(Host!, EncryptMode: "Mandatory"), user, "env:" + SecretEnv, monitoringUserName: monitoringUser);

    private static ServerActionPermit Permit(int sessionId, string? database) =>
        new(ServerActionNames.KillSession, 1, "azure-test", sessionId.ToString(CultureInfo.InvariantCulture), database, Guid.NewGuid(), "person:test");

    /// <summary>A session of <paramref name="user"/> in <paramref name="database"/> that holds an exclusive row lock in an open transaction.</summary>
    private sealed record Victim(SqlConnection Connection, SqlTransaction? Transaction, int SessionId, DateTime LoginTime) : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            try { if (Transaction is not null && Connection.State == ConnectionState.Open) await Transaction.RollbackAsync(); } catch (Exception) { /* killed */ }
            await Connection.DisposeAsync();
        }
    }

    private static async Task<Victim> OpenVictimAsync(string database, string user = App, bool holdLock = true, string program = "pm5632_victim")
    {
        var ct = TestContext.Current.CancellationToken;
        var connection = new SqlConnection(Builder(database, user, program).ConnectionString);
        await connection.OpenAsync(ct);
        SqlTransaction? transaction = null;
        if (holdLock)
        {
            transaction = (SqlTransaction)await connection.BeginTransactionAsync(ct);
            await using var update = new SqlCommand("UPDATE dbo.kill5632 SET v = v + 1 WHERE id = 1;", connection, transaction);
            await update.ExecuteNonQueryAsync(ct);
        }

        await using var read = new SqlCommand("SELECT @@SPID, login_time FROM sys.dm_exec_sessions WHERE session_id = @@SPID;", connection, transaction);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return new Victim(connection, transaction, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture), reader.GetDateTime(1));
    }

    /// <summary>Whether the victim's session is still there with its login time, read as the remediation user.</summary>
    private static async Task<bool> AliveAsync(string database, Victim victim)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new SqlConnection(Builder(database, Fix).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand("SELECT COUNT_BIG(*) FROM sys.dm_exec_sessions WHERE session_id = @id AND login_time = @login;", connection);
        command.Parameters.Add(new SqlParameter("@id", SqlDbType.Int) { Value = victim.SessionId });
        command.Parameters.Add(new SqlParameter("@login", SqlDbType.DateTime) { Value = victim.LoginTime });
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) > 0;
    }

    [Theory]
    [MemberData(nameof(Databases))]
    public async Task ADryRun_ReadsTheSessionsFacts_AndKillsNothing(string database)
    {
        SkipUnlessSet(database);
        await using var victim = await OpenVictimAsync(database);
        var result = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime, dryRun: true,
            TestContext.Current.CancellationToken);
        Assert.Equal(RemediationOutcome.Done, result.Outcome);
        Assert.False(result.Wrote);
        Assert.Equal(App, result.Session?.LoginName);
        Assert.Equal(1, result.Session?.OpenTransactionCount);
        Assert.True(await AliveAsync(database, victim));
    }

    [Theory]
    [MemberData(nameof(Databases))]
    public async Task AKill_EndsTheBlockingSession_ReleasesItsLock_AndReadsTheRequestAfter(string database)
    {
        SkipUnlessSet(database);
        var ct = TestContext.Current.CancellationToken;
        await using var victim = await OpenVictimAsync(database);

        /* A second session waits on the victim's lock. */
        await using var waiter = new SqlConnection(Builder(database, App).ConnectionString);
        await waiter.OpenAsync(ct);
        await using var blocked = new SqlCommand("UPDATE dbo.kill5632 SET v = v + 1 WHERE id = 1;", waiter) { CommandTimeout = 120 };
        var waiting = blocked.ExecuteNonQueryAsync(ct);
        await Task.Delay(TimeSpan.FromSeconds(2), ct);
        Assert.False(waiting.IsCompleted);

        var result = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime, dryRun: false, ct);
        Assert.Equal(RemediationOutcome.Done, result.Outcome);
        Assert.True(result.Wrote);
        Assert.NotNull(result.Rollback);
        Assert.Equal(1, await waiting.WaitAsync(TimeSpan.FromSeconds(60), ct));
        Assert.False(await AliveAsync(database, victim));
    }

    [Theory]
    [MemberData(nameof(Databases))]
    public async Task ALoginTimeThatDiffers_IsAReusedId_AndNothingIsKilled(string database)
    {
        SkipUnlessSet(database);
        await using var victim = await OpenVictimAsync(database);
        var result = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime.AddSeconds(-3),
            dryRun: false, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationResult.Reasons.SessionReused, result.Reason);
        Assert.Null(result.Session);
        Assert.True(await AliveAsync(database, victim));
    }

    [Theory]
    [MemberData(nameof(Databases))]
    public async Task AUserWithoutKillDatabaseConnection_IsRefusedWithTheGrantNamed(string database)
    {
        SkipUnlessSet(database);
        await using var victim = await OpenVictimAsync(database);
        var result = await Executor(NoKill).KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime,
            dryRun: false, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationResult.Reasons.KillPermissionDenied, result.Reason);
        Assert.Contains("GRANT KILL DATABASE CONNECTION", result.Message, StringComparison.Ordinal);
        Assert.True(await AliveAsync(database, victim));
    }

    [Theory]
    [MemberData(nameof(Databases))]
    public async Task ADatabaseNameTheConnectionDoesNotLandIn_IsTheWrongDatabase(string database)
    {
        SkipUnlessSet(database);
        await using var victim = await OpenVictimAsync(database);

        /* The server opens a database whatever the case of its name, but the batch compares DB_NAME() with the name exactly. */
        var other = database.ToUpperInvariant() == database ? database.ToLowerInvariant() : database.ToUpperInvariant();
        var result = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, other), victim.SessionId, victim.LoginTime,
            dryRun: false, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationResult.Reasons.WrongDatabase, result.Reason);
        Assert.True(await AliveAsync(database, victim));
    }

    [Fact]
    public async Task ASessionInTheOtherDatabase_IsNotReachedFromThisOne()
    {
        var names = DatabaseNames();
        Assert.SkipWhen(names.Count < 2, "Set DARLING_TEST_AZURE_DATABASES to two databases on one logical server to run this test.");
        SkipUnlessSet(names[0]);

        await using var victim = await OpenVictimAsync(names[0]);
        var result = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, names[1]), victim.SessionId, victim.LoginTime,
            dryRun: false, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationOutcome.Refused, result.Outcome);
        Assert.Contains(result.Reason, new[] { RemediationResult.Reasons.SessionGone, RemediationResult.Reasons.SessionReused, RemediationResult.Reasons.SessionInOtherDatabase });
        Assert.True(await AliveAsync(names[0], victim));
    }

    /// <summary>
    /// #5632: a session is Darling's own when its program name starts with PerformanceMonitorDarling AND it signed in as the monitoring login
    /// (in either sign-in form), and it is never killed; its facts are not returned.
    /// </summary>
    [Theory]
    [MemberData(nameof(Databases))]
    public async Task ASessionOfADarlingProgram_SignedInAsTheMonitoringLogin_IsNeverKilled_AndItsFactsAreNotReturned(string database)
    {
        SkipUnlessSet(database);
        foreach (var user in new[] { Monitor, Monitor + "@" + Host!.Split('.')[0] })
        {
            await using var victim = await OpenVictimAsync(database, user, holdLock: false, program: "PerformanceMonitorDarling-Collector");
            foreach (var dry in new[] { true, false })
            {
                var result = await Executor(monitoringUser: Monitor).KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime,
                    dry, TestContext.Current.CancellationToken);
                Assert.Equal(RemediationResult.Reasons.ToolLoginSession, result.Reason);
                Assert.Null(result.Session);
                Assert.True(await AliveAsync(database, victim));
            }
        }
    }

    /// <summary>#5632: any client can name its own program, so an ordinary login with Darling's program name is not Darling's own and is killable.</summary>
    [Theory]
    [MemberData(nameof(Databases))]
    public async Task AnOrdinaryLoginWithADarlingProgramName_IsNotRefusedAsDarlings(string database)
    {
        SkipUnlessSet(database);
        await using var victim = await OpenVictimAsync(database, App, holdLock: false, program: "PerformanceMonitorDarling-Collector");
        var result = await Executor(monitoringUser: Monitor).KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime,
            dryRun: true, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationOutcome.Done, result.Outcome);
        Assert.NotEqual(RemediationResult.Reasons.ToolLoginSession, result.Reason);
        Assert.False(result.Wrote);
        Assert.NotNull(result.Session);
        Assert.True(await AliveAsync(database, victim));
    }

    /// <summary>#5632: another session of the remediation login is the tool's own, whatever its program, and also when it signed in as user@server.</summary>
    [Theory]
    [MemberData(nameof(Databases))]
    public async Task AnotherSessionOfTheRemediationLogin_IsNeverKilled_InEitherSignInForm(string database)
    {
        SkipUnlessSet(database);
        foreach (var user in new[] { Fix, Fix + "@" + Host!.Split('.')[0] })
        {
            await using var victim = await OpenVictimAsync(database, user, holdLock: false);
            var result = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime,
                dryRun: false, TestContext.Current.CancellationToken);
            Assert.Equal(RemediationResult.Reasons.ToolLoginSession, result.Reason);
            Assert.True(await AliveAsync(database, victim));
        }
    }

    /// <summary>
    /// #5632: the monitoring login's name alone is not a reason to refuse. A server monitored as its admin login would otherwise have a real blocker
    /// nobody could end; Darling's own sessions are told apart by their program name together with the login.
    /// </summary>
    [Theory]
    [MemberData(nameof(Databases))]
    public async Task ASessionOfTheMonitoringLoginWithAnotherProgram_IsAnOrdinarySession(string database)
    {
        SkipUnlessSet(database);
        await using var victim = await OpenVictimAsync(database, Monitor, holdLock: false);
        var result = await Executor(monitoringUser: Monitor).KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime,
            dryRun: true, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationOutcome.Done, result.Outcome);
        Assert.False(result.Wrote);
        Assert.True(await AliveAsync(database, victim));
    }

    /// <summary>#5632: a session in another database answers no facts about it, and is not killed.</summary>
    [Fact]
    public async Task ASessionInTheOtherDatabase_ReturnsNoFactsAboutIt()
    {
        var names = DatabaseNames();
        Assert.SkipWhen(names.Count < 2, "Set DARLING_TEST_AZURE_DATABASES to two databases on one logical server to run this test.");
        SkipUnlessSet(names[0]);

        await using var victim = await OpenVictimAsync(names[0]);
        var result = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, names[1]), victim.SessionId, victim.LoginTime,
            dryRun: false, TestContext.Current.CancellationToken);
        Assert.Equal(RemediationOutcome.Refused, result.Outcome);
        Assert.Contains(result.Reason, new[] { RemediationResult.Reasons.SessionGone, RemediationResult.Reasons.SessionReused, RemediationResult.Reasons.SessionInOtherDatabase });
        Assert.Null(result.Session);
        Assert.True(await AliveAsync(names[0], victim));
    }

    [Theory]
    [MemberData(nameof(Databases))]
    public async Task AKillWhoseReplyIsLost_IsOutcomeUnknown_NotFailed(string database)
    {
        SkipUnlessSet(database);
        await using var victim = await OpenVictimAsync(database);
        SqlServerRemediationExecutor.KillBatchSeam.Value = async (command, token) =>
        {
            await using (await command.ExecuteReaderAsync(token)) { }
            throw new System.IO.IOException("The connection was reset after the batch was sent.");
        };
        try
        {
            var result = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime,
                dryRun: false, TestContext.Current.CancellationToken);
            Assert.Equal(RemediationOutcome.Unknown, result.Outcome);
            Assert.Equal(RemediationResult.Reasons.OutcomeUnknown, result.Reason);
            Assert.False(result.Wrote);
        }
        finally
        {
            SqlServerRemediationExecutor.KillBatchSeam.Value = null;
        }

        /* The batch did run: the session is gone, which is why the answer may not say "failed". */
        Assert.False(await AliveAsync(database, victim));
    }

    /// <summary>A connection with read-only intent: on Hyperscale (or a database with a readable secondary) it lands on a replica.</summary>
    private static async Task<SqlConnection?> OpenReplicaAsync(string database, string user)
    {
        var ct = TestContext.Current.CancellationToken;
        var builder = Builder(database, user);
        builder.ApplicationIntent = ApplicationIntent.ReadOnly;
        var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);
        await using var read = new SqlCommand("SELECT CONVERT(nvarchar(128), DATABASEPROPERTYEX(DB_NAME(), 'Updateability'));", connection);
        if ((string?)await read.ExecuteScalarAsync(ct) == "READ_ONLY")
            return connection;
        await connection.DisposeAsync();
        return null;
    }

    [Theory]
    [MemberData(nameof(Databases))]
    public async Task TheKillBatch_OnAReplica_IsNotThePrimary_AndKillsNothing(string database)
    {
        SkipUnlessSet(database);
        var ct = TestContext.Current.CancellationToken;
        await using var victim = await OpenVictimAsync(database);
        await using var replica = await OpenReplicaAsync(database, Fix);
        Assert.SkipWhen(replica is null, $"{database} has no readable replica to land on.");

        /* The executor always connects read-write; this runs its batch where a read-only connection lands, to show the batch itself refuses there. */
        await using var command = new SqlCommand(SqlServerRemediationExecutor.AzureKillSql, replica);
        command.Parameters.Add(new SqlParameter("@session_id", SqlDbType.Int) { Value = victim.SessionId });
        command.Parameters.Add(new SqlParameter("@login_time", SqlDbType.DateTime) { Value = victim.LoginTime });
        command.Parameters.Add(new SqlParameter("@dry", SqlDbType.Bit) { Value = false });
        command.Parameters.Add(new SqlParameter("@database", SqlDbType.NVarChar, 128) { Value = database });
        command.Parameters.Add(new SqlParameter("@monitoring_login", SqlDbType.NVarChar, 128) { Value = DBNull.Value });
        SqlServerRemediationExecutor.AddKillActivityParameters(command, null);
        Assert.Equal("not_primary", (string?)await command.ExecuteScalarAsync(ct));
        Assert.True(await AliveAsync(database, victim));
    }

    /// <summary>The victim's <c>last_request_end_time</c>, read as the remediation user.</summary>
    private static async Task<DateTime?> LastRequestEndAsync(string database, Victim victim)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = new SqlConnection(Builder(database, Fix).ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand("SELECT last_request_end_time FROM sys.dm_exec_sessions WHERE session_id = @id AND login_time = @login;", connection);
        command.Parameters.Add(new SqlParameter("@id", SqlDbType.Int) { Value = victim.SessionId });
        command.Parameters.Add(new SqlParameter("@login", SqlDbType.DateTime) { Value = victim.LoginTime });
        return await command.ExecuteScalarAsync(ct) as DateTime?;
    }

    /// <summary>
    /// #7109: a kill that carries the <c>last_request_end_time</c> read before the session finished another batch is refused <c>session_changed</c>,
    /// dry run and write alike, and the session lives. With the current value the same kill goes through.
    /// </summary>
    [Theory]
    [MemberData(nameof(Databases))]
    public async Task AKillWithAStaleLastRequestEnd_IsRefusedSessionChanged_AndTheSessionLives_ButTheCurrentValueKills(string database)
    {
        SkipUnlessSet(database);
        var ct = TestContext.Current.CancellationToken;
        await using var victim = await OpenVictimAsync(database, holdLock: false);
        var before = await LastRequestEndAsync(database, victim);
        Assert.NotNull(before);

        /* The session finishes a new batch: its last_request_end_time moves on (datetime resolution is about 3 ms). */
        await Task.Delay(50, ct);
        await using (var again = new SqlCommand("SELECT 1;", victim.Connection))
            Assert.Equal(1, (int)(await again.ExecuteScalarAsync(ct))!);
        var after = await LastRequestEndAsync(database, victim);
        Assert.NotEqual(before, after);

        foreach (var dry in new[] { true, false })
        {
            var refused = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime, dry, ct, new KillExpectedActivity(before));
            Assert.Equal(RemediationOutcome.Refused, refused.Outcome);
            Assert.Equal(RemediationResult.Reasons.SessionChanged, refused.Reason);
            Assert.False(refused.Wrote);
            Assert.True(await AliveAsync(database, victim));
        }

        var killed = await Executor().KillAzureSessionAsync(Permit(victim.SessionId, database), victim.SessionId, victim.LoginTime, dryRun: false, ct, new KillExpectedActivity(after));
        Assert.True(killed is { Outcome: RemediationOutcome.Done, Wrote: true }, killed.Message);
        Assert.False(await AliveAsync(database, victim));
    }

    [Theory]
    [MemberData(nameof(Databases))]
    public async Task ASessionOnAReplica_IsNotFoundOnThePrimary_AndIsNotKilled(string database)
    {
        SkipUnlessSet(database);
        var ct = TestContext.Current.CancellationToken;
        await using var replica = await OpenReplicaAsync(database, App);
        Assert.SkipWhen(replica is null, $"{database} has no readable replica to land on.");
        await using var read = new SqlCommand("SELECT @@SPID, login_time FROM sys.dm_exec_sessions WHERE session_id = @@SPID;", replica);
        int sessionId;
        DateTime loginTime;
        await using (var reader = await read.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct));
            sessionId = Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture);
            loginTime = reader.GetDateTime(1);
        }

        var result = await Executor().KillAzureSessionAsync(Permit(sessionId, database), sessionId, loginTime, dryRun: false, ct);
        Assert.Equal(RemediationOutcome.Refused, result.Outcome);
        /* A replica session can share its id with this call's own connection on the primary (Hyperscale numbers them separately): own_session is then the first refusal, and nothing is killed either way. */
        Assert.Contains(result.Reason, new[] { RemediationResult.Reasons.SessionGone, RemediationResult.Reasons.SessionReused, RemediationResult.Reasons.OwnSession });
        await using var still = new SqlCommand("SELECT 1;", replica);
        Assert.Equal(1, (int)(await still.ExecuteScalarAsync(ct))!);
    }
}
