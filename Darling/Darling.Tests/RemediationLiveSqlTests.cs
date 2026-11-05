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
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The server write code against a real SQL Server (#5632, step 6). Gated on <c>DARLING_TEST_SQL</c> (and <c>DARLING_TEST_SQL_USER</c> /
/// <c>DARLING_TEST_SQL_PASSWORD</c>), the variables the other live SQL tests read, so it skips where the runner has no test server. It runs
/// only against that server, and never against a monitored one. Each test makes its own database with Query Store on and its own remediation
/// login, hands the login to the code as an <c>env:</c> reference set in this process, and drops both in its own <c>finally</c>.
/// </summary>
[Collection("remediation-live-sql")]
public sealed class RemediationLiveSqlTests
{
    private static string? Host => Environment.GetEnvironmentVariable("DARLING_TEST_SQL");

    private static SqlConnectionStringBuilder Builder(string? user, string? password, string database = "master", string app = "PerformanceMonitorDarling-RemediationTest")
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = Host,
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = app,
            Pooling = false,
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };
        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = password ?? string.Empty;
        }

        return builder;
    }

    private static SqlConnectionStringBuilder Admin(string database = "master") =>
        Builder(Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"), Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"), database);

    private static async Task<SqlConnection> OpenAsync(SqlConnectionStringBuilder builder, CancellationToken ct)
    {
        var connection = new SqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task<object?> ScalarAsync(SqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(SqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One test's own database (Query Store on) and its own remediation login. The caller drops it in its own finally.</summary>
    private sealed class Scratch
    {
        public string Database { get; } = "pm_rem_" + Guid.NewGuid().ToString("N")[..16];

        public string Login => Database;

        public string Password { get; } = "Aa1!" + Convert.ToHexString(RandomNumberGenerator.GetBytes(12));

        public string EnvName { get; } = "PM_REM_TEST_" + Guid.NewGuid().ToString("N");

        public string SecretSlot => "env:" + EnvName;

        public async Task CreateAsync(CancellationToken ct)
        {
            await using var admin = await OpenAsync(Admin(), ct);
            Environment.SetEnvironmentVariable(EnvName, Password);
            await ExecAsync(admin, $"CREATE DATABASE [{Database}];", ct);
            await ExecAsync(admin, $"ALTER DATABASE [{Database}] SET QUERY_STORE = ON (OPERATION_MODE = READ_WRITE, QUERY_CAPTURE_MODE = ALL, DATA_FLUSH_INTERVAL_SECONDS = 60);", ct);
            await ExecAsync(admin, $"CREATE LOGIN [{Login}] WITH PASSWORD = N'{Password}', CHECK_POLICY = OFF, DEFAULT_DATABASE = [master];", ct);
            await ExecAsync(admin, $"GRANT VIEW SERVER STATE, ALTER ANY CONNECTION, ALTER SERVER STATE TO [{Login}];", ct);
            await ExecAsync(admin, $"USE [{Database}]; CREATE USER [{Login}] FOR LOGIN [{Login}]; ALTER ROLE db_owner ADD MEMBER [{Login}];", ct);
            await ExecAsync(admin, $"USE [{Database}]; CREATE TABLE dbo.t (a int NOT NULL, b int NOT NULL); INSERT dbo.t (a, b) VALUES (1, 1), (2, 2), (3, 3);", ct);
        }

        public async Task DropAsync()
        {
            Environment.SetEnvironmentVariable(EnvName, null);
            try
            {
                await using var admin = await OpenAsync(Admin(), CancellationToken.None);
                await ExecAsync(admin, $"IF DB_ID(N'{Database}') IS NOT NULL BEGIN ALTER DATABASE [{Database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{Database}]; END;", CancellationToken.None);
                await ExecAsync(admin, $"IF SUSER_ID(N'{Login}') IS NOT NULL DROP LOGIN [{Login}];", CancellationToken.None);
            }
            catch (SqlException)
            {
                /* The test's own assertions report a real failure; cleanup that cannot reach the server leaves a uniquely named scratch database. */
            }
        }

        public SqlServerRemediationExecutor Executor(CapturingLogger logger, int serverId = 7, string? monitoringUser = null) =>
            new(serverId, new RemediationServer(Host!, null, "Optional", TrustServerCertificate: true), Login, SecretSlot, logger, monitoringUser);
    }

    private static ServerActionPermit Permit(string serverAction, string? id, string? database, int serverId = 7) =>
        new(new ServerActionCall("tool", serverAction,
            new AgentAction(Guid.NewGuid(), Guid.NewGuid(), "agent-a", "why", "[]"),
            new AgentActionTarget(serverId, "srv", id, database)));

    private static void NeedServer() =>
        Assert.SkipWhen(string.IsNullOrEmpty(Host), "Set DARLING_TEST_SQL (and DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live remediation tests.");

    /// <summary>Runs a statement in the scratch database as the administrator and returns the query and plan ids Query Store holds for it.</summary>
    private static async Task<(long QueryId, long PlanId)> CaptureQueryAsync(Scratch scratch, string marker, CancellationToken ct)
    {
        await using var admin = await OpenAsync(Admin(scratch.Database), ct);
        for (var i = 0; i < 3; i++)
            await ExecAsync(admin, $"SELECT marker = '{marker}', n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1;", ct);
        await ExecAsync(admin, "EXEC sys.sp_query_store_flush_db;", ct);
        await using var command = new SqlCommand(
            "SELECT TOP (1) q.query_id, p.plan_id FROM sys.query_store_query AS q JOIN sys.query_store_query_text AS t ON t.query_text_id = q.query_text_id "
            + "JOIN sys.query_store_plan AS p ON p.query_id = q.query_id WHERE t.query_sql_text LIKE @like AND t.query_sql_text NOT LIKE N'%query_store_query%' ORDER BY p.plan_id;", admin);
        command.Parameters.Add(new SqlParameter("@like", SqlDbType.NVarChar, 200) { Value = "%'" + marker + "'%" });
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "Query Store captured no plan for the test query.");
        return (reader.GetInt64(0), reader.GetInt64(1));
    }

    private static async Task<bool> IsForcedAsync(Scratch scratch, long planId, CancellationToken ct)
    {
        await using var admin = await OpenAsync(Admin(scratch.Database), ct);
        return Convert.ToBoolean(await ScalarAsync(admin, $"SELECT is_forced_plan FROM sys.query_store_plan WHERE plan_id = {planId};", ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task AForce_PlacesThePlan_AnUnforceRemovesIt_AndADryRunWritesNothing()
    {
        NeedServer();
        var ct = TestContext.Current.CancellationToken;
        var scratch = new Scratch();
        try
        {
            await scratch.CreateAsync(ct);
            var (queryId, planId) = await CaptureQueryAsync(scratch, "force1", ct);
            var logger = new CapturingLogger();
            var executor = scratch.Executor(logger);
            var permit = Permit(ServerActionNames.ForcePlan, planId.ToString(CultureInfo.InvariantCulture), scratch.Database);

            var dry = await executor.ForcePlanAsync(permit, queryId, planId, dryRun: true, ct);
            Assert.True(dry is { Outcome: RemediationOutcome.Done, Wrote: false }, dry.Message);
            Assert.False(await IsForcedAsync(scratch, planId, ct));

            var forced = await executor.ForcePlanAsync(permit, queryId, planId, dryRun: false, ct);
            Assert.True(forced is { Outcome: RemediationOutcome.Done, Wrote: true }, forced.Message);
            Assert.True(await IsForcedAsync(scratch, planId, ct));

            /* Forced already: refused, and the plan stays as it was. */
            var again = await executor.ForcePlanAsync(permit, queryId, planId, dryRun: false, ct);
            Assert.Equal(RemediationResult.Reasons.AlreadyForced, again.Reason);
            Assert.False(again.Wrote);
            Assert.True(await IsForcedAsync(scratch, planId, ct));

            var dryUnforce = await executor.UnforcePlanAsync(permit, queryId, planId, dryRun: true, ct);
            Assert.True(dryUnforce is { Outcome: RemediationOutcome.Done, Wrote: false }, dryUnforce.Message);
            Assert.True(await IsForcedAsync(scratch, planId, ct));

            var unforced = await executor.UnforcePlanAsync(permit, queryId, planId, dryRun: false, ct);
            Assert.True(unforced is { Outcome: RemediationOutcome.Done, Wrote: true }, unforced.Message);
            Assert.False(await IsForcedAsync(scratch, planId, ct));

            var unforceAgain = await executor.UnforcePlanAsync(permit, queryId, planId, dryRun: false, ct);
            Assert.Equal(RemediationResult.Reasons.NotForced, unforceAgain.Reason);

            AssertNoSecret(scratch, logger, dry, forced, again, dryUnforce, unforced, unforceAgain);
        }
        finally
        {
            await scratch.DropAsync();
        }
    }

    [Fact]
    public async Task AForceOfAMissingPlan_OrAPlanOfAnotherQuery_IsRefusedWithoutAWrite()
    {
        NeedServer();
        var ct = TestContext.Current.CancellationToken;
        var scratch = new Scratch();
        try
        {
            await scratch.CreateAsync(ct);
            var (queryId, planId) = await CaptureQueryAsync(scratch, "force2", ct);
            var executor = scratch.Executor(new CapturingLogger());

            var missingId = planId + 100000;
            var missing = await executor.ForcePlanAsync(Permit(ServerActionNames.ForcePlan, missingId.ToString(CultureInfo.InvariantCulture), scratch.Database),
                queryId, missingId, dryRun: false, ct);
            Assert.Equal(RemediationOutcome.Refused, missing.Outcome);
            Assert.Equal(RemediationResult.Reasons.PlanNotFound, missing.Reason);
            Assert.False(missing.Wrote);

            /* A real plan under the wrong query id is not that query's plan. */
            var wrongQuery = await executor.ForcePlanAsync(Permit(ServerActionNames.ForcePlan, planId.ToString(CultureInfo.InvariantCulture), scratch.Database),
                queryId + 100000, planId, dryRun: false, ct);
            Assert.Equal(RemediationResult.Reasons.PlanNotFound, wrongQuery.Reason);
            Assert.False(await IsForcedAsync(scratch, planId, ct));

            /* The permit's database is where the plan is looked up: a database that does not hold the plan finds none. */
            var elsewhere = await executor.ForcePlanAsync(Permit(ServerActionNames.ForcePlan, planId.ToString(CultureInfo.InvariantCulture), "tempdb"),
                queryId, planId, dryRun: false, ct);
            Assert.True(elsewhere.Outcome != RemediationOutcome.Done, elsewhere.Message);
            Assert.False(await IsForcedAsync(scratch, planId, ct));
        }
        finally
        {
            await scratch.DropAsync();
        }
    }

    private static async Task<(string Hex, string Marker)> CachePlanAsync(Scratch scratch, string marker, CancellationToken ct)
    {
        await using var admin = await OpenAsync(Admin(scratch.Database), ct);
        await ExecAsync(admin, $"SELECT marker = '{marker}', n = COUNT_BIG(*) FROM dbo.t WHERE b >= 1;", ct);
        var hex = await HandleAsync(marker, ct);
        Assert.NotNull(hex);
        return (hex!, marker);
    }

    private static async Task<bool> HandleCachedAsync(string hex, CancellationToken ct)
    {
        await using var admin = await OpenAsync(Admin(), ct);
        await using var command = new SqlCommand("SELECT COUNT_BIG(*) FROM sys.dm_exec_cached_plans WHERE plan_handle = CONVERT(varbinary(64), @hex, 1);", admin);
        command.Parameters.Add(new SqlParameter("@hex", SqlDbType.VarChar, 130) { Value = hex });
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) > 0;
    }

    private static async Task<string?> HandleAsync(string marker, CancellationToken ct)
    {
        await using var admin = await OpenAsync(Admin(), ct);
        await using var command = new SqlCommand(
            "SELECT TOP (1) plan_handle = CONVERT(varchar(130), c.plan_handle, 1) FROM sys.dm_exec_cached_plans AS c CROSS APPLY sys.dm_exec_sql_text(c.plan_handle) AS t "
            + "WHERE t.text LIKE @like AND t.text NOT LIKE N'%dm_exec_cached_plans%';", admin);
        command.Parameters.Add(new SqlParameter("@like", SqlDbType.NVarChar, 200) { Value = "%'" + marker + "'%" });
        return await command.ExecuteScalarAsync(ct) as string;
    }

    [Fact]
    public async Task AnEviction_RemovesThatOneHandle_AndADryRunRemovesNothing()
    {
        NeedServer();
        var ct = TestContext.Current.CancellationToken;
        var scratch = new Scratch();
        try
        {
            await scratch.CreateAsync(ct);
            var markerA = "evictA" + Guid.NewGuid().ToString("N")[..8];
            var markerB = "evictB" + Guid.NewGuid().ToString("N")[..8];
            var (handleA, _) = await CachePlanAsync(scratch, markerA, ct);
            var (handleB, _) = await CachePlanAsync(scratch, markerB, ct);
            var logger = new CapturingLogger();
            var executor = scratch.Executor(logger);
            var permit = Permit(ServerActionNames.EvictPlan, handleA, null);

            var dry = await executor.EvictPlanAsync(permit, handleA, dryRun: true, ct);
            Assert.True(dry is { Outcome: RemediationOutcome.Done, Wrote: false }, dry.Message);
            Assert.True(await HandleCachedAsync(handleA, ct));

            var evicted = await executor.EvictPlanAsync(permit, handleA, dryRun: false, ct);
            Assert.True(evicted is { Outcome: RemediationOutcome.Done, Wrote: true }, evicted.Message);
            Assert.False(await HandleCachedAsync(handleA, ct));
            Assert.True(await HandleCachedAsync(handleB, ct));

            /* Gone now: a dry run and a real run both say so, and neither runs anything. */
            var dryGone = await executor.EvictPlanAsync(permit, handleA, dryRun: true, ct);
            var gone = await executor.EvictPlanAsync(permit, handleA, dryRun: false, ct);
            Assert.Equal(RemediationResult.Reasons.NotCached, dryGone.Reason);
            Assert.Equal(RemediationResult.Reasons.NotCached, gone.Reason);
            Assert.True(await HandleCachedAsync(handleB, ct));

            AssertNoSecret(scratch, logger, dry, evicted, dryGone, gone);
        }
        finally
        {
            await scratch.DropAsync();
        }
    }

    [Fact]
    public async Task AMalformedHandle_IsRefusedBeforeAnyBatchRuns_SoNothingReachesTheServer()
    {
        NeedServer();
        var ct = TestContext.Current.CancellationToken;
        var scratch = new Scratch();
        try
        {
            await scratch.CreateAsync(ct);
            var marker = "evictC" + Guid.NewGuid().ToString("N")[..8];
            var (handle, _) = await CachePlanAsync(scratch, marker, ct);
            var executor = scratch.Executor(new CapturingLogger());
            await using var admin = await OpenAsync(Admin(), ct);
            var before = Convert.ToInt64(await ScalarAsync(admin, $"SELECT COUNT_BIG(*) FROM sys.dm_exec_sessions WHERE program_name = N'{RemediationConnection.ApplicationName}';", ct), CultureInfo.InvariantCulture);

            foreach (var bad in new[] { handle + "; SELECT 1", handle[..^1], "0x" + new string('A', 130), "zz" + handle })
            {
                var result = await executor.EvictPlanAsync(Permit(ServerActionNames.EvictPlan, bad, null), bad, dryRun: false, ct);
                Assert.Equal(RemediationResult.Reasons.BadHandle, result.Reason);
            }

            Assert.True(await HandleCachedAsync(handle, ct));
            Assert.Equal(before, Convert.ToInt64(await ScalarAsync(admin, $"SELECT COUNT_BIG(*) FROM sys.dm_exec_sessions WHERE program_name = N'{RemediationConnection.ApplicationName}';", ct), CultureInfo.InvariantCulture));
        }
        finally
        {
            await scratch.DropAsync();
        }
    }

    /// <summary>A second session, opened by the test as the scratch login, with its session id and login time read from the server.</summary>
    private static async Task<(SqlConnection Victim, int SessionId, DateTime LoginTime)> OpenVictimAsync(Scratch scratch, CancellationToken ct)
    {
        /* No connection resiliency: with it the driver quietly reconnects after a kill, and the test could not see the session end. */
        /* The test server's admin login with an ordinary program name: the remediation login is the scratch one, and Darling's own sessions
           (its program name, its remediation login) are never killed (#5632). */
        var victimBuilder = Admin();
        victimBuilder.ApplicationName = "pm5632_victim";
        victimBuilder.ConnectRetryCount = 0;
        var victim = await OpenAsync(victimBuilder, ct);
        var sessionId = Convert.ToInt32(await ScalarAsync(victim, "SELECT @@SPID;", ct), CultureInfo.InvariantCulture);
        await using var admin = await OpenAsync(Admin(), ct);
        var loginTime = (DateTime)(await ScalarAsync(admin, $"SELECT login_time FROM sys.dm_exec_sessions WHERE session_id = {sessionId};", ct))!;
        return (victim, sessionId, loginTime);
    }

    /// <summary>Is that very session still there? A session id is reused, so the login time is part of the question.</summary>
    private static async Task<bool> SessionExistsAsync(int sessionId, DateTime loginTime, CancellationToken ct)
    {
        await using var admin = await OpenAsync(Admin(), ct);
        await using var command = new SqlCommand("SELECT COUNT_BIG(*) FROM sys.dm_exec_sessions WHERE session_id = @id AND login_time = @login_time;", admin);
        command.Parameters.Add(new SqlParameter("@id", SqlDbType.Int) { Value = sessionId });
        command.Parameters.Add(new SqlParameter("@login_time", SqlDbType.DateTime) { Value = loginTime });
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture) > 0;
    }

    [Fact]
    public async Task AKill_EndsTheSession_ADryRunDoesNot_AndAWrongLoginTimeIsRefusedAndTheSessionLives()
    {
        NeedServer();
        var ct = TestContext.Current.CancellationToken;
        var scratch = new Scratch();
        SqlConnection? victim = null;
        try
        {
            await scratch.CreateAsync(ct);
            int sessionId;
            DateTime loginTime;
            (victim, sessionId, loginTime) = await OpenVictimAsync(scratch, ct);
            var logger = new CapturingLogger();
            var executor = scratch.Executor(logger);
            var permit = Permit(ServerActionNames.KillSession, sessionId.ToString(CultureInfo.InvariantCulture), null);

            var dry = await executor.KillSessionAsync(permit, sessionId, loginTime, dryRun: true, ct);
            Assert.True(dry is { Outcome: RemediationOutcome.Done, Wrote: false }, dry.Message);
            Assert.True(await SessionExistsAsync(sessionId, loginTime, ct));

            var wrong = await executor.KillSessionAsync(permit, sessionId, loginTime.AddSeconds(1), dryRun: false, ct);
            Assert.Equal(RemediationOutcome.Refused, wrong.Outcome);
            Assert.Equal(RemediationResult.Reasons.SessionReused, wrong.Reason);
            Assert.False(wrong.Wrote);
            var wrongDry = await executor.KillSessionAsync(permit, sessionId, loginTime.AddSeconds(1), dryRun: true, ct);
            Assert.Equal(RemediationResult.Reasons.SessionReused, wrongDry.Reason);
            Assert.True(await SessionExistsAsync(sessionId, loginTime, ct));
            Assert.Equal(1, Convert.ToInt32(await ScalarAsync(victim, "SELECT 1;", ct), CultureInfo.InvariantCulture));

            var killed = await executor.KillSessionAsync(permit, sessionId, loginTime, dryRun: false, ct);
            Assert.True(killed is { Outcome: RemediationOutcome.Done, Wrote: true }, killed.Message);
            /* The victim can no longer use its connection. An idle killed session leaves sys.dm_exec_sessions when its client next
               touches the connection, so that touch comes first and the catalog is read after it. */
            await Assert.ThrowsAnyAsync<Exception>(() => ScalarAsync(victim, "SELECT 1;", ct));
            for (var i = 0; i < 50 && await SessionExistsAsync(sessionId, loginTime, ct); i++)
                await Task.Delay(100, ct);
            Assert.False(await SessionExistsAsync(sessionId, loginTime, ct));

            /* The session is gone: the same kill is refused rather than failing. */
            var gone = await executor.KillSessionAsync(permit, sessionId, loginTime, dryRun: false, ct);
            /* The server hands a freed id to the next sign-in, and the executor's own connection can be that sign-in: then the id is its own
               session. Either way the kill is refused and wrote nothing. */
            Assert.Contains(gone.Reason, new[] { RemediationResult.Reasons.SessionGone, RemediationResult.Reasons.OwnSession, RemediationResult.Reasons.SessionReused });
            Assert.False(gone.Wrote);
            /* An id no session holds is session_gone for certain. */
            await using var admin = await OpenAsync(Admin(), ct);
            var free = Convert.ToInt32(await ScalarAsync(admin, "SELECT MAX(session_id) + 1000 FROM sys.dm_exec_sessions;", ct), CultureInfo.InvariantCulture);
            var absent = await executor.KillSessionAsync(Permit(ServerActionNames.KillSession, free.ToString(CultureInfo.InvariantCulture), null), free, loginTime, dryRun: false, ct);
            Assert.Equal(RemediationResult.Reasons.SessionGone, absent.Reason);


            AssertNoSecret(scratch, logger, dry, wrong, wrongDry, killed, gone);
        }
        finally
        {
            victim?.Dispose();
            await scratch.DropAsync();
        }
    }

    /// <summary>
    /// #7109: a kill that carries the session's <c>last_request_end_time</c> from the dry run is refused <c>session_changed</c> when the session has
    /// finished another batch since, and the session stays alive, dry run and write alike. With the current value the same kill goes through.
    /// </summary>
    [Fact]
    public async Task AKillWithAStaleLastRequestEnd_IsRefusedSessionChanged_AndTheSessionLives_ButTheCurrentValueKills()
    {
        NeedServer();
        var ct = TestContext.Current.CancellationToken;
        var scratch = new Scratch();
        SqlConnection? victim = null;
        try
        {
            await scratch.CreateAsync(ct);
            int sessionId;
            DateTime loginTime;
            (victim, sessionId, loginTime) = await OpenVictimAsync(scratch, ct);
            var executor = scratch.Executor(new CapturingLogger());
            var permit = Permit(ServerActionNames.KillSession, sessionId.ToString(CultureInfo.InvariantCulture), null);
            await using var admin = await OpenAsync(Admin(), ct);
            var lastEndSql = $"SELECT last_request_end_time FROM sys.dm_exec_sessions WHERE session_id = {sessionId};";

            var before = (DateTime)(await ScalarAsync(admin, lastEndSql, ct))!;
            /* The session finishes a new batch: its last_request_end_time moves on (datetime resolution is about 3 ms). */
            await Task.Delay(50, ct);
            Assert.Equal(1, Convert.ToInt32(await ScalarAsync(victim, "SELECT 1;", ct), CultureInfo.InvariantCulture));
            var after = (DateTime)(await ScalarAsync(admin, lastEndSql, ct))!;
            Assert.NotEqual(before, after);

            foreach (var dry in new[] { true, false })
            {
                var refused = await executor.KillSessionAsync(permit, sessionId, loginTime, dry, ct, new KillExpectedActivity(before));
                Assert.Equal(RemediationOutcome.Refused, refused.Outcome);
                Assert.Equal(RemediationResult.Reasons.SessionChanged, refused.Reason);
                Assert.False(refused.Wrote);
                Assert.True(await SessionExistsAsync(sessionId, loginTime, ct));
            }

            /* The refusals did not touch the victim, so its last_request_end_time is still the value read after its second batch. */
            var killed = await executor.KillSessionAsync(permit, sessionId, loginTime, dryRun: false, ct, new KillExpectedActivity(after));
            Assert.True(killed is { Outcome: RemediationOutcome.Done, Wrote: true }, killed.Message);
        }
        finally
        {
            victim?.Dispose();
            await scratch.DropAsync();
        }
    }

    /// <summary>
    /// A session of a given login and program name on the test server, opened by the test: the administrator login or the scratch (remediation)
    /// login. <see cref="OpenVictimAsync"/> is the ordinary one.
    /// </summary>
    private static async Task<(SqlConnection Victim, int SessionId, DateTime LoginTime)> OpenSessionAsync(Scratch scratch, bool asRemediationLogin, string program, CancellationToken ct)
    {
        var builder = asRemediationLogin ? Builder(scratch.Login, scratch.Password, scratch.Database, program) : Admin();
        builder.ApplicationName = program;
        builder.ConnectRetryCount = 0;
        var victim = await OpenAsync(builder, ct);
        var sessionId = Convert.ToInt32(await ScalarAsync(victim, "SELECT @@SPID;", ct), CultureInfo.InvariantCulture);
        await using var admin = await OpenAsync(Admin(), ct);
        var loginTime = (DateTime)(await ScalarAsync(admin, $"SELECT login_time FROM sys.dm_exec_sessions WHERE session_id = {sessionId};", ct))!;
        return (victim, sessionId, loginTime);
    }

    /// <summary>
    /// #5632: a session counts as Darling's own when its program name starts with PerformanceMonitorDarling AND its login is the monitoring login,
    /// or when it is the remediation login with any program name. An ordinary login with Darling's program name is not Darling's.
    /// </summary>
    [Fact]
    public async Task ASessionIsDarlings_OnlyByProgramNameTogetherWithTheMonitoringLogin_OrByTheRemediationLogin()
    {
        NeedServer();
        var ct = TestContext.Current.CancellationToken;
        var scratch = new Scratch();
        var opened = new List<SqlConnection>();
        try
        {
            await scratch.CreateAsync(ct);
            var admin = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER")!;
            const string darlingProgram = "PerformanceMonitorDarling-Collector";
            var permitFor = (int id) => Permit(ServerActionNames.KillSession, id.ToString(CultureInfo.InvariantCulture), null);

            /* The monitoring login with the Darling program name: refused, with no facts, dry run and real. */
            var (monitoring, monitoringId, monitoringTime) = await OpenSessionAsync(scratch, false, darlingProgram, ct);
            opened.Add(monitoring);
            foreach (var dry in new[] { true, false })
            {
                var refused = await scratch.Executor(new CapturingLogger(), monitoringUser: admin).KillSessionAsync(permitFor(monitoringId), monitoringId, monitoringTime, dry, ct);
                Assert.Equal(RemediationResult.Reasons.ToolLoginSession, refused.Reason);
                Assert.Null(refused.Session);
                Assert.True(await SessionExistsAsync(monitoringId, monitoringTime, ct));
            }

            /* An ordinary login with the Darling program name is not Darling's: it is a session this tool may end. */
            var (ordinary, ordinaryId, ordinaryTime) = await OpenSessionAsync(scratch, false, darlingProgram, ct);
            opened.Add(ordinary);
            var ordinaryDry = await scratch.Executor(new CapturingLogger(), monitoringUser: "pm5632_someone_else").KillSessionAsync(permitFor(ordinaryId), ordinaryId, ordinaryTime, dryRun: true, ct);
            Assert.True(ordinaryDry is { Outcome: RemediationOutcome.Done, Wrote: false }, ordinaryDry.Message);
            Assert.NotNull(ordinaryDry.Session);

            /* The remediation login with any program name: refused. */
            var (own, ownId, ownTime) = await OpenSessionAsync(scratch, true, "pm5632_victim", ct);
            opened.Add(own);
            var ownRefused = await scratch.Executor(new CapturingLogger(), monitoringUser: "pm5632_someone_else").KillSessionAsync(permitFor(ownId), ownId, ownTime, dryRun: false, ct);
            Assert.Equal(RemediationResult.Reasons.ToolLoginSession, ownRefused.Reason);
            Assert.Null(ownRefused.Session);
            Assert.True(await SessionExistsAsync(ownId, ownTime, ct));
        }
        finally
        {
            foreach (var connection in opened)
                connection.Dispose();
            await scratch.DropAsync();
        }
    }

    /// <summary>The kill batch run on a connection that names its OWN session: it refuses before anything else, and the connection survives. Run as the administrator, so no grant is in the way.</summary>
    [Fact]
    public async Task TheKillBatch_RefusesItsOwnSession_BeforeAnythingElse_AndDoesNotKillIt()
    {
        NeedServer();
        var ct = TestContext.Current.CancellationToken;
        await using var own = await OpenAsync(Admin(), ct);
        var spid = Convert.ToInt32(await ScalarAsync(own, "SELECT @@SPID;", ct), CultureInfo.InvariantCulture);
        foreach (var dry in new[] { true, false })
        {
            await using var command = new SqlCommand(SqlServerRemediationExecutor.KillSql, own) { CommandTimeout = 30 };
            command.Parameters.Add(new SqlParameter("@session_id", SqlDbType.Int) { Value = spid });
            command.Parameters.Add(new SqlParameter("@login_time", SqlDbType.DateTime) { Value = new DateTime(2000, 1, 1) });
            command.Parameters.Add(new SqlParameter("@dry", SqlDbType.Bit) { Value = dry });
            command.Parameters.Add(new SqlParameter("@monitoring_login", SqlDbType.NVarChar, 128) { Value = DBNull.Value });
            SqlServerRemediationExecutor.AddKillActivityParameters(command, null);
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("own_session", reader.GetString(0));
        }

        Assert.Equal(spid, Convert.ToInt32(await ScalarAsync(own, "SELECT @@SPID;", ct), CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task TheSecret_AppearsInNoLogLineOrResult_AndAFailedSignInIsReportedWithItsOwnReason()
    {
        NeedServer();
        var ct = TestContext.Current.CancellationToken;
        var scratch = new Scratch();
        try
        {
            await scratch.CreateAsync(ct);
            var logger = new CapturingLogger();

            /* A real login with the wrong secret: the server says the sign-in failed. */
            var wrongName = "PM_REM_WRONG_" + Guid.NewGuid().ToString("N");
            Environment.SetEnvironmentVariable(wrongName, "Not-The-Password-" + Guid.NewGuid().ToString("N"));
            try
            {
                var wrongSecret = new SqlServerRemediationExecutor(7, new RemediationServer(Host!, null, "Optional", TrustServerCertificate: true), scratch.Login, "env:" + wrongName, logger);
                var failed = await wrongSecret.KillSessionAsync(Permit(ServerActionNames.KillSession, "55", null), 55, DateTime.UtcNow, dryRun: false, ct);
                Assert.Equal(RemediationOutcome.Failed, failed.Outcome);
                Assert.Equal(RemediationResult.Reasons.SignInFailed, failed.Reason);
                Assert.False(failed.Wrote);
                Assert.DoesNotContain(Environment.GetEnvironmentVariable(wrongName)!, failed.Message, StringComparison.Ordinal);
                AssertNoSecret(scratch, logger, failed);
            }
            finally
            {
                Environment.SetEnvironmentVariable(wrongName, null);
            }

            /* The server echoes the login NAME in its failure message. Make the name equal to the secret: the only way the secret can reach a
               message is through the server's own text, and it must be scrubbed from that. */
            var echoName = "Echo-" + Guid.NewGuid().ToString("N");
            Environment.SetEnvironmentVariable(scratch.EnvName, echoName);
            try
            {
                var echo = new SqlServerRemediationExecutor(7, new RemediationServer(Host!, null, "Optional", TrustServerCertificate: true), echoName, scratch.SecretSlot, logger);
                var result = await echo.KillSessionAsync(Permit(ServerActionNames.KillSession, "55", null), 55, DateTime.UtcNow, dryRun: false, ct);
                Assert.Equal(RemediationOutcome.Failed, result.Outcome);
                Assert.Equal(RemediationResult.Reasons.SignInFailed, result.Reason);
                Assert.DoesNotContain(echoName, result.Message, StringComparison.Ordinal);
                Assert.DoesNotContain(logger.Lines, line => line.Contains(echoName, StringComparison.Ordinal));
                Assert.Contains("***", result.Message, StringComparison.Ordinal);
            }
            finally
            {
                Environment.SetEnvironmentVariable(scratch.EnvName, scratch.Password);
            }
        }
        finally
        {
            await scratch.DropAsync();
        }
    }

    private static void AssertNoSecret(Scratch scratch, CapturingLogger logger, params RemediationResult[] results)
    {
        Assert.NotEmpty(logger.Lines);
        Assert.DoesNotContain(logger.Lines, line => line.Contains(scratch.Password, StringComparison.Ordinal));
        foreach (var result in results)
            Assert.DoesNotContain(scratch.Password, result.ToString(), StringComparison.Ordinal);
    }
}
