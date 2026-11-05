/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* The kill half of the plan force live tests (#5632, step 12): the same store and SQL Server setup (a partial class, so the private setup is
   shared). Each test makes its own scratch store and SQL Server database, and opens its own victim session as the scratch login. */
public sealed partial class PlanForceToolLiveTests
{
    private const string KillKind = "session_kill";

    /// <summary>A second session on the test SQL Server, with the id and login time the server itself holds for it.</summary>
    private sealed record Victim(SqlConnection Connection, int SessionId, DateTime LoginLocal);

    /// <summary>
    /// A session to kill. By default it is the test server's admin login with an ordinary program name: the remediation login is the scratch one, and
    /// Darling's own sessions (its program name, its remediation login) are refused (#5632). <paramref name="asRemediationLogin"/> signs in as that login.
    /// </summary>
    private static async Task<Victim> OpenVictimAsync(Ctx c, string program = VictimProgram, bool asRemediationLogin = false)
    {
        /* No connection resiliency: with it the driver quietly reconnects after a kill, and the test could not see the session end. */
        var builder = new SqlConnectionStringBuilder(c.Sql.Admin(c.Sql.Database).ConnectionString)
        {
            ConnectRetryCount = 0,
            ApplicationName = program,
        };
        if (asRemediationLogin)
        {
            builder.UserID = c.Sql.Login;
            builder.Password = c.Sql.Password;
        }
        var connection = await OpenAsync(builder, c.Ct);
        var id = Convert.ToInt32(await ScalarAsync(connection, "SELECT @@SPID;", c.Ct), CultureInfo.InvariantCulture);
        await using var admin = await OpenAsync(c.Sql.Admin(), c.Ct);
        var login = (DateTime)(await ScalarAsync(admin, $"SELECT login_time FROM sys.dm_exec_sessions WHERE session_id = {id};", c.Ct))!;
        return new Victim(connection, id, login);
    }

    /// <summary>The minutes the test SQL Server's clock is ahead of UTC, as the server says it now; the store's clock row is seeded with it.</summary>
    private static async Task<int> SeedClockAsync(Ctx c)
    {
        await using var admin = await OpenAsync(c.Sql.Admin(), c.Ct);
        var offset = Convert.ToInt32(await ScalarAsync(admin, "SELECT DATEDIFF(MINUTE, SYSUTCDATETIME(), SYSDATETIME());", c.Ct), CultureInfo.InvariantCulture);
        await using var insert = new NpgsqlCommand(
            "INSERT INTO collect.server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) VALUES ($1, $2, 7, 'srv-7', $3)", c.Connection) { CommandTimeout = 60 };
        insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = DateTime.UtcNow.Ticks, NpgsqlDbType = NpgsqlDbType.Bigint });
        insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
        insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = offset, NpgsqlDbType = NpgsqlDbType.Integer });
        await insert.ExecuteNonQueryAsync(c.Ct);
        return offset;
    }

    /// <summary>The login time as <c>get_blocking</c> would hand it back: UTC, ISO 8601.</summary>
    private static string UtcText(Victim v, int offsetMinutes) => v.LoginLocal.AddMinutes(-offsetMinutes).ToString("o", CultureInfo.InvariantCulture);

    private static async Task<bool> SessionAliveAsync(Ctx c, Victim v)
    {
        await using var admin = await OpenAsync(c.Sql.Admin(), c.Ct);
        await using var command = new SqlCommand("SELECT COUNT_BIG(*) FROM sys.dm_exec_sessions WHERE session_id = @id AND login_time = @login;", admin);
        command.Parameters.Add(new SqlParameter("@id", SqlDbType.Int) { Value = v.SessionId });
        command.Parameters.Add(new SqlParameter("@login", SqlDbType.DateTime) { Value = v.LoginLocal });
        return Convert.ToInt64(await command.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture) > 0;
    }

    private const string VictimProgram = "pm5632_victim";

    private static Task<string> KillAsync(Ctx c, int session, string? loginTime, bool dryRun, string server = "srv-7") =>
        DarlingMcpPlanForceTools.RunKillAsync(c.Source, c.Source, c.Log, server, session, loginTime, null, c.Arg, dryRun, c.Ct);

    [Fact]
    public async Task ADryRunKill_ShowsTheSessionsFacts_KillsNothing_AndJournalsNothing_AndTheKillIsDryByDefault()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            var answer = await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun: true);
            var json = Parse(answer);
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.Equal(Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"), (string?)json["session"]!["login_name"]);
            Assert.Equal(VictimProgram, (string?)json["session"]!["program_name"]);
            Assert.Equal(victim.LoginLocal.AddMinutes(-offset).ToString("o", CultureInfo.InvariantCulture), (string?)json["session"]!["login_time"]);
            Assert.Empty(json["warnings"]!.AsArray());
            Assert.True(await SessionAliveAsync(c, victim));
            Assert.Equal(0, await JournalActionRowsAsync(c));
            var parameter = typeof(DarlingMcpPlanForceTools).GetMethod(nameof(DarlingMcpPlanForceTools.KillSession))!.GetParameters().Single(p => p.Name == "dry_run");
            Assert.True((bool)parameter.DefaultValue!);
            AssertNoSecret(c, answer);
        });
    }

    /// <summary>
    /// #7108: the prepare step returns the facts that decide the kill, with the last request's end time among them, kills nothing, and the write step
    /// then ends the session it names.
    /// </summary>
    [Fact]
    public async Task ThePrepareStep_ReturnsTheBindingFacts_KillsNothing_AndTheWriteStepEndsTheSession()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            await using var admin = await OpenAsync(c.Sql.Admin(), c.Ct);
            var lastEnd = (DateTime)(await ScalarAsync(admin, $"SELECT last_request_end_time FROM sys.dm_exec_sessions WHERE session_id = {victim.SessionId};", c.Ct))!;

            var store = new InvestigationRequestStore(c.Source, 30, agentActionSource: c.Source);
            var decision = await ServerActionGate.AuthorizeAsync(store, c.Source, c.Log, DarlingMcpPlanForceTools.KillToolName, ServerActionNames.KillSession, c.Arg,
                new AgentActionTarget(7, "srv-7", victim.SessionId.ToString(CultureInfo.InvariantCulture), null), c.Ct);
            var permit = decision.Permit!;

            var prepared = await DarlingMcpPlanForceTools.PrepareKillSessionAsync(c.Source, c.Source, c.Log, permit, 7, "srv-7", azure: false, victim.SessionId,
                victim.LoginLocal.AddMinutes(-offset), null, c.Ct);
            Assert.True(prepared.Proceed);
            var binding = prepared.Binding!;
            Assert.Equal(victim.SessionId, binding.SessionId);
            Assert.Equal(victim.LoginLocal.AddMinutes(-offset), binding.LoginTimeUtc);
            Assert.Equal(victim.LoginLocal, binding.LoginTimeServerLocal);
            Assert.Equal(lastEnd, binding.LastRequestEndTimeServerLocal);
            Assert.Equal(lastEnd.AddMinutes(-offset), binding.LastRequestEndTimeUtc);
            Assert.Equal("sleeping", binding.Status);
            Assert.Equal(0, binding.OpenTransactionCount);
            Assert.True(await SessionAliveAsync(c, victim));
            Assert.Equal(0, await JournalActionRowsAsync(c));

            var written = await DarlingMcpPlanForceTools.WriteKillSessionAsync(prepared.Executor!, permit, azure: false, binding, c.Ct);
            Assert.Equal(RemediationOutcome.Done, written.Outcome);
            Assert.True(written.Wrote);
            Assert.False(await SessionAliveAsync(c, victim));
        });
    }

    /// <summary>
    /// #5632: a session of Darling itself is never killed, dry run or real: one whose program name starts with PerformanceMonitorDarling AND whose
    /// login is the monitoring login, and another session of the remediation login with any program name. The refusal returns no facts about the
    /// session, and journals nothing. The monitoring login's name alone is not a reason to refuse, so an ordinary session of the same login stays
    /// killable; and an ordinary login with Darling's program name is not Darling's either.
    /// </summary>
    [Fact]
    public async Task ASessionOfDarlingsProgram_OrOfTheRemediationLogin_IsRefused_WithNoFacts_AndNothingIsKilled()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var adminLogin = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER")!;

            /* The server entry's monitoring login is the test server's admin login, the one the victims sign in as. */
            await PgExecAsync(c.Connection, $"UPDATE config.config_monitored_servers SET username = '{adminLogin.Replace("'", "''", StringComparison.Ordinal)}' WHERE server_id = 7", c.Ct);
            foreach (var (program, remediation) in new[] { ("PerformanceMonitorDarling-Collector", false), (VictimProgram, true) })
            {
                var victim = await OpenVictimAsync(c, program, remediation);
                using var _ = victim.Connection;
                foreach (var dry in new[] { true, false })
                {
                    var json = Parse(await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun: dry));
                    Assert.Equal("refused", (string?)json["status"]);
                    Assert.Equal(RemediationResult.Reasons.ToolLoginSession, (string?)json["reason"]);
                    Assert.Null(json["session"]);
                    Assert.True(await SessionAliveAsync(c, victim));
                }
            }

            /* The same program name from a login that is not the monitoring login is an ordinary session: the dry run reaches the end. */
            await PgExecAsync(c.Connection, "UPDATE config.config_monitored_servers SET username = 'pm5632_someone_else' WHERE server_id = 7", c.Ct);
            var ordinary = await OpenVictimAsync(c, "PerformanceMonitorDarling-Collector");
            using var _ordinary = ordinary.Connection;
            var ordinaryJson = Parse(await KillAsync(c, ordinary.SessionId, UtcText(ordinary, offset), dryRun: true));
            Assert.Equal("dry_run", (string?)ordinaryJson["status"]);
            Assert.NotNull(ordinaryJson["session"]);
            Assert.True(await SessionAliveAsync(c, ordinary));

            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task ARealKill_EndsTheSession_WarnsOfTheRollbackItCost_JournalsCalledAndDone_AndHasNoUndo()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            /* An open transaction that wrote: it has used log, so the kill rolls work back. */
            await ExecAsync(victim.Connection, "BEGIN TRANSACTION; UPDATE dbo.t SET b = b + 1 WHERE a = 1;", c.Ct);

            var dry = Parse(await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun: true));
            Assert.Equal("dry_run", (string?)dry["status"]);
            Assert.True((long)dry["session"]!["log_bytes_used"]! > 0);
            Assert.Equal(1, (int)dry["session"]!["open_transaction_count"]!);
            Assert.Contains("rolls that work back", (string?)Assert.Single(dry["warnings"]!.AsArray()), StringComparison.Ordinal);

            var answer = await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun: false);
            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.NotNull(json["rollback"]);
            Assert.NotNull(json["rollback"]!["in_progress"]);
            Assert.Contains("There is no undo", (string?)json["message"], StringComparison.Ordinal);

            /* The victim can no longer use its connection; the catalog is read after that touch. */
            await Assert.ThrowsAnyAsync<Exception>(() => ScalarAsync(victim.Connection, "SELECT 1;", c.Ct));
            for (var i = 0; i < 50 && await SessionAliveAsync(c, victim); i++)
                await Task.Delay(100, c.Ct);
            Assert.False(await SessionAliveAsync(c, victim));

            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(KillKind, (string?)called["kind"]);
            Assert.Equal("kill_session", (string?)called["tool"]);
            Assert.Equal(victim.SessionId.ToString(CultureInfo.InvariantCulture), (string?)called["target"]!["id"]);
            Assert.Equal(7, (int)called["target"]!["server_id"]!);
            Assert.Equal(Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"), (string?)called["before"]!["session"]!["login_name"]);
            Assert.True((long)called["before"]!["session"]!["log_bytes_used"]! > 0);
            var done = Assert.Single(await RowsAsync(c, "action_done"));
            Assert.Equal((long)json["journal_id"]!, (long)done["called_id"]!);
            Assert.NotNull(done["after"]!["rollback"]);

            var list = await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct);
            var change = Assert.Single(list.Changes);
            Assert.Equal(KillKind, change.Kind);
            Assert.False(change.CanTryUndo(DateTime.UtcNow), "a kill has no undo");
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task AReusedSessionId_IsRefusedSessionReused_OnADryRunAndARealCall_AndTheSessionLives()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            /* The same id with another login time: the shape of an id the server gave to a new session. */
            var other = victim with { LoginLocal = victim.LoginLocal.AddSeconds(5) };
            foreach (var dryRun in new[] { true, false })
            {
                var answer = await KillAsync(c, victim.SessionId, UtcText(other, offset), dryRun);
                var json = Parse(answer);
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("session_reused", (string?)json["reason"]);
                /* Nothing about the session that has the id now: not its login time, not its facts. */
                Assert.Null(json["actual_login_time"]);
                Assert.Null(json["session"]);
                Assert.DoesNotContain(UtcText(victim, offset), answer, StringComparison.Ordinal);
            }

            Assert.True(await SessionAliveAsync(c, victim));
            Assert.Equal(1, await ScalarAsync(victim.Connection, "SELECT 1;", c.Ct) is int one ? one : 0);
            Assert.Equal(0, await JournalActionRowsAsync(c));
            /* A refusal is journaled once per request, tool, code and hour, so two refused calls leave one row. */
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'session_reused'", c.Ct));
        });
    }

    [Fact]
    public async Task AKillWhoseReplyIsLost_AnswersUnknown_JournalsNoFailure_AndTheListSaysResultUnknown()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            /* The real batch runs and kills the session; then the reply is lost, the way a dropped connection loses it. */
            PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.KillBatchSeam.Value = async (command, token) =>
            {
                await using (var reader = await command.ExecuteReaderAsync(token))
                    while (await reader.ReadAsync(token))
                    {
                    }

                throw new System.IO.IOException("The connection was reset.");
            };
            string answer;
            try
            {
                answer = await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun: false);
            }
            finally
            {
                PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.KillBatchSeam.Value = null;
            }

            var json = Parse(answer);
            Assert.Equal("outcome_unknown", (string?)json["status"]);
            Assert.Contains("may have been killed", (string?)json["message"], StringComparison.Ordinal);
            for (var i = 0; i < 50 && await SessionAliveAsync(c, victim); i++)
                await Task.Delay(100, c.Ct);
            Assert.False(await SessionAliveAsync(c, victim));
            Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Empty(await RowsAsync(c, "action_failed"));
            Assert.Empty(await RowsAsync(c, "action_done"));
            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct)).Changes);
            Assert.Equal(AgentChangeResult.Unknown, change.Result);
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task AKillWhoseRollbackReadFails_IsStillDone_JournalsDone_AndSaysTheRollbackWasNotRead()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            /* The real batch, with an error raised by the read after the KILL: the KILL ran, so this is a kill, not a failure. */
            PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.KillBatchSeam.Value = (command, token) =>
            {
                const string read = "SELECT @command = r.command";
                Assert.Contains(read, command.CommandText, StringComparison.Ordinal);
                command.CommandText = command.CommandText.Replace(read, "RAISERROR(N'the read after the kill failed', 16, 1); " + read, StringComparison.Ordinal);
                return command.ExecuteReaderAsync(token);
            };
            string answer;
            try
            {
                answer = await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun: false);
            }
            finally
            {
                PerformanceMonitor.Darling.Remediation.SqlServerRemediationExecutor.KillBatchSeam.Value = null;
            }

            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.Null(json["rollback"]);
            Assert.Contains("was not read", (string?)json["message"], StringComparison.Ordinal);
            for (var i = 0; i < 50 && await SessionAliveAsync(c, victim); i++)
                await Task.Delay(100, c.Ct);
            Assert.False(await SessionAliveAsync(c, victim));
            Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Single(await RowsAsync(c, "action_done"));
            Assert.Empty(await RowsAsync(c, "action_failed"));
        });
    }

    [Fact]
    public async Task ASessionThatIsGone_IsRefusedSessionGone_NotSessionReused()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            var login = UtcText(victim, offset);
            victim.Connection.Dispose();
            for (var i = 0; i < 50 && await SessionAliveAsync(c, victim); i++)
                await Task.Delay(100, c.Ct);
            /* The server hands a freed id to the next sign-in, which can be the remediation connection itself (that is own_session, tested by the
               executor). So the gone session is one whose id no session holds now. */
            await using var admin = await OpenAsync(c.Sql.Admin(), c.Ct);
            var free = Convert.ToInt32(await ScalarAsync(admin, "SELECT MAX(session_id) + 1000 FROM sys.dm_exec_sessions;", c.Ct), CultureInfo.InvariantCulture);
            var json = Parse(await KillAsync(c, free, login, dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("session_gone", (string?)json["reason"]);
        });
    }

    [Fact]
    public async Task WithoutAServerClock_TheKillIsRefusedServerClockUnknown_AndNothingIsKilled()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await KillAsync(c, victim.SessionId, UtcText(victim, 0), dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("server_clock_unknown", (string?)json["reason"]);
            }

            Assert.True(await SessionAliveAsync(c, victim));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    /// <summary>
    /// #7108: the refusal for a server whose clock was never collected comes from the prepare step, after the gate passed. The tool journals it as a refusal with its
    /// reason on the agent's request. The store is enough: nothing reaches the monitored server.
    /// </summary>
    [Fact]
    public async Task WithoutAServerClock_TheRefusalIsJournaledOnTheRequestWithItsReason()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var json = Parse(await KillAsync(c, 55, "2026-10-19T12:00:00Z", dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("server_clock_unknown", (string?)json["reason"]);
            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'server_clock_unknown' AND actor = 'agent:agent-a' "
                + "AND request_id = (SELECT request_id FROM config.investigation_requests LIMIT 1)", c.Ct));
        }, storeOnly: true);
    }

    [Fact]
    public async Task ALoginTimeThatIsNotATime_AndABadSessionId_AreRefusedBeforeTheGate()
    {
        await RunAsync(Without.KillSwitch, async c =>
        {
            /* The switch is off, yet the answer is the argument's own refusal: a call that cannot do anything is refused before the gate. */
            var json = Parse(await KillAsync(c, 55, "last tuesday", dryRun: false));
            Assert.Equal("bad_login_time", (string?)json["reason"]);
            var bad = Parse(await KillAsync(c, 0, "2026-10-19T12:00:00Z", dryRun: false));
            Assert.Equal("bad_session_id", (string?)bad["reason"]);
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public async Task AKillOnAPostgreSqlTarget_WithSqlServerArguments_IsRefusedBadBackendId_AndNothingReachesAnyTarget()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            await SeedClockAsync(c);
            foreach (var dryRun in new[] { true, false })
            {
                /* A PostgreSQL target is named by backend_id and pid; a session id and a login time alone name nothing there. */
                var answer = await KillAsync(c, victim.SessionId, UtcText(victim, 0), dryRun, server: "srv-8");
                var json = Parse(answer);
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("bad_backend_id", (string?)json["reason"]);
                Assert.Contains("Nothing was run", (string?)json["message"], StringComparison.Ordinal);
            }

            Assert.True(await SessionAliveAsync(c, victim));
            Assert.Equal(0, await JournalActionRowsAsync(c));
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'bad_backend_id'", c.Ct));
        });
    }

    /// <summary>
    /// #5632: a target the registry records as an Azure SQL Database (engine edition 5) takes the Azure arm. The database is required, never
    /// master, never one the entry excludes, and must be the entry's database or one Darling collected a blocking row from; each refusal comes
    /// before any connection, kills nothing and journals no action. A database that passes is opened, and the batch checks the engine again:
    /// this test server is not an Azure SQL Database, so it answers not_azure_sql_database and kills nothing.
    /// </summary>
    [Fact]
    public async Task AnAzureTarget_NeedsADatabaseItKnows_AndTheBatchChecksTheEngineAgain()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            await PgExecAsync(c.Connection, "UPDATE collect.servers SET sql_engine_edition = 5 WHERE server_id = 7", c.Ct);
            await PgExecAsync(c.Connection, "UPDATE config.config_monitored_servers SET username = 'monitor_login', excluded_databases = ARRAY['Archive'] WHERE server_id = 7", c.Ct);

            async Task<JsonNode> Azure(string? database) =>
                Parse(await DarlingMcpPlanForceTools.RunKillAsync(c.Source, c.Source, c.Log, "srv-7", victim.SessionId, UtcText(victim, offset), null, c.Arg,
                    dry_run: false, c.Ct, database));

            Assert.Equal(RemediationResult.Reasons.DatabaseRequired, (string?)(await Azure(null))["reason"]);
            Assert.Equal(RemediationResult.Reasons.DatabaseRequired, (string?)(await Azure("master"))["reason"]);
            Assert.Equal(DarlingMcpPlanForceTools.AzureDatabaseExcluded, (string?)(await Azure("archive"))["reason"]);
            Assert.Equal(DarlingMcpPlanForceTools.AzureDatabaseUnknown, (string?)(await Azure(c.Sql.Database))["reason"]);

            /* A blocking row from the database names it as one the target holds. */
            await using (var insert = new NpgsqlCommand(
                "INSERT INTO collect.dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, "
                + "wait_time_ms, lock_mode, blocking_status, blocked_sql_text, blocking_sql_text, contentious_object) "
                + "VALUES ($1, $2, 7, 'srv-7', $2, $3, 999, $4, 10, '', 'sleeping', '', '', '')", c.Connection) { CommandTimeout = 60 })
            {
                insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = DateTime.UtcNow.Ticks, NpgsqlDbType = NpgsqlDbType.Bigint });
                insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-1), DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = c.Sql.Database, NpgsqlDbType = NpgsqlDbType.Text });
                insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = victim.SessionId, NpgsqlDbType = NpgsqlDbType.Integer });
                await insert.ExecuteNonQueryAsync(c.Ct);
            }

            var reached = await Azure(c.Sql.Database);
            Assert.Equal("refused", (string?)reached["status"]);
            Assert.Equal(RemediationResult.Reasons.NotAzureSqlDatabase, (string?)reached["reason"]);
            Assert.True(await SessionAliveAsync(c, victim));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    private static async Task AssertKillRefusedAsync(Without without, string reason)
    {
        await RunAsync(without, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            foreach (var dryRun in new[] { true, false })
            {
                var answer = await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun);
                var json = Parse(answer);
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal(reason, (string?)json["reason"]);
                AssertNoSecret(c, answer);
            }

            Assert.True(await SessionAliveAsync(c, victim));
            Assert.Equal(0, await JournalActionRowsAsync(c));
        });
    }

    [Fact]
    public Task WithoutTheKillSwitch_TheCallIsRefused_AndTheSessionLives() => AssertKillRefusedAsync(Without.KillSwitch, "server_action_off");

    [Fact]
    public Task WithoutALevelThreeRoute_TheKillIsRefused_AndTheSessionLives() => AssertKillRefusedAsync(Without.Level, "not_level_3");

    [Fact]
    public Task WithoutTheCredentialSlot_TheKillIsRefused_AndTheSessionLives() => AssertKillRefusedAsync(Without.CredentialSlot, "no_remediation_credential");

    [Fact]
    public async Task AKillByALoginWithoutAlterAnyConnection_IsRefusedKillPermissionDenied_AndTheSessionLives()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var victim = await OpenVictimAsync(c);
            using var _ = victim.Connection;
            await using (var admin = await OpenAsync(c.Sql.Admin(), c.Ct))
                await ExecAsync(admin, $"REVOKE ALTER ANY CONNECTION FROM [{c.Sql.Login}];", c.Ct);
            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await KillAsync(c, victim.SessionId, UtcText(victim, offset), dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("kill_permission_denied", (string?)json["reason"]);
            }

            Assert.True(await SessionAliveAsync(c, victim));
        });
    }

    /// <summary>
    /// The round trip the login time takes: a head blocker is captured by the REAL blocking capture, stored, read back by the REAL
    /// <c>get_blocking</c>, and the UTC <c>blocking_login_time</c> it returns is what the kill takes. A sign-in time that did not survive any step
    /// (the zone shift, the engine's <c>datetime</c> steps) would come back <c>session_reused</c>.
    /// </summary>
    [Fact]
    public async Task TheLoginTimeGetBlockingReturns_KillsTheHeadBlocker_ItWasReadFrom()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            var head = await OpenVictimAsync(c);
            using var _ = head.Connection;
            await ExecAsync(head.Connection, "BEGIN TRANSACTION; SELECT b FROM dbo.t WITH (HOLDLOCK, ROWLOCK) WHERE a = 1;", c.Ct);
            await using var waiter = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct);
            var waiting = ExecAsync(waiter, "UPDATE dbo.t SET b = 9 WHERE a = 1;", c.Ct);
            try
            {
                var context = new CollectorContext { ServerId = 7, ServerName = "srv-7", CollectionTime = DateTime.UtcNow, Deltas = null!, State = CollectorContext.NoState };
                var query = DmvBlockingSnapshotCollector.Instance.BuildQuery(context);
                DmvBlockingSnapshotCollector.Row? captured = null;
                for (var i = 0; i < 60 && captured is null; i++)
                {
                    await Task.Delay(TimeSpan.FromSeconds(1), c.Ct);
                    await using var connection = await OpenAsync(c.Sql.Admin(), c.Ct);
                    await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
                    await using var reader = await command.ExecuteReaderAsync(c.Ct);
                    var rows = await DmvBlockingSnapshotCollector.Instance.ReadAsync(reader, context, c.Ct);
                    var match = rows.Where(r => r.BlockingSpid == head.SessionId).ToList();
                    if (match.Count > 0)
                        captured = match[0];
                }

                Assert.True(captured is not null, "the capture never saw the head blocker");
                Assert.Equal(head.LoginLocal, captured!.Value.BlockingLoginTime);

                var seenAt = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-1), DateTimeKind.Unspecified);
                await using (var insert = new NpgsqlCommand(
                    "INSERT INTO collect.dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, database_name, blocked_spid, blocking_spid, "
                    + "wait_time_ms, lock_mode, blocking_status, blocked_sql_text, blocking_sql_text, contentious_object, blocking_login_time) "
                    + "VALUES ($1, $2, 7, 'srv-7', $2, $3, $4, $5, $6, '', 'sleeping', '', '', '', $7)", c.Connection) { CommandTimeout = 60 })
                {
                    insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = DateTime.UtcNow.Ticks, NpgsqlDbType = NpgsqlDbType.Bigint });
                    insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = seenAt, NpgsqlDbType = NpgsqlDbType.Timestamp });
                    insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = captured.Value.DatabaseName ?? c.Sql.Database, NpgsqlDbType = NpgsqlDbType.Text });
                    insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = captured.Value.BlockedSpid, NpgsqlDbType = NpgsqlDbType.Integer });
                    insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = captured.Value.BlockingSpid, NpgsqlDbType = NpgsqlDbType.Integer });
                    insert.Parameters.Add(new NpgsqlParameter<long> { TypedValue = captured.Value.WaitTimeMs, NpgsqlDbType = NpgsqlDbType.Bigint });
                    insert.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(captured.Value.BlockingLoginTime!.Value, DateTimeKind.Unspecified), NpgsqlDbType = NpgsqlDbType.Timestamp });
                    await insert.ExecuteNonQueryAsync(c.Ct);
                }

                using var blocking = JsonDocument.Parse(await DarlingMcpBlockingTools.GetBlocking(c.Source, "srv-7", 1, 50, null, false, null, DatabaseFilter.All, 150, cancellationToken: c.Ct));
                var read = blocking.RootElement.GetProperty("events").EnumerateArray()
                    .Select(e => e.GetProperty("blocking_login_time").GetString()).First(t => t is not null)!;
                Assert.Equal(UtcText(head, offset), read);

                var dry = Parse(await KillAsync(c, head.SessionId, read, dryRun: true));
                Assert.Equal("dry_run", (string?)dry["status"]);
                var killed = Parse(await KillAsync(c, head.SessionId, read, dryRun: false));
                Assert.Equal("done", (string?)killed["status"]);
                await Assert.ThrowsAnyAsync<Exception>(() => ScalarAsync(head.Connection, "SELECT 1;", c.Ct));
            }
            finally
            {
                try
                {
                    await waiting;
                }
                catch (SqlException)
                {
                    /* The waiter's update ends when the head's lock goes, or when its own connection closes at the end of the test. */
                }
            }
        });
    }

    /// <summary>
    /// The 120-byte question (#5632, step 12): a read-only transaction that holds a lock (<c>HOLDLOCK</c>) and writes nothing. The first transaction to
    /// touch a database just created records 120 bytes of log even for a read; once warmed, a read-only transaction records none, in the scratch database
    /// and in an existing one (<c>msdb</c>, read only, no object created). The kill sets no threshold, so a session with 0 bytes is the one with no work to
    /// roll back, and this holds that the answer's <c>log_bytes_used</c> reads exactly that.
    /// </summary>
    [Fact]
    public async Task AReadOnlyTransactionThatHoldsALock_ReadsZeroLogBytes_AfterTheDatabaseIsWarm_InTheScratchDatabaseAndInMsdb()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var offset = await SeedClockAsync(c);
            /* Warm the scratch database: the first transaction there is the one that records the 120 bytes. */
            await using (var warm = await OpenAsync(c.Sql.Admin(c.Sql.Database), c.Ct))
                await ExecAsync(warm, "BEGIN TRANSACTION; SELECT b FROM dbo.t WITH (HOLDLOCK, ROWLOCK) WHERE a = 1; ROLLBACK;", c.Ct);

            foreach (var (database, sql) in new[]
            {
                (c.Sql.Database, "BEGIN TRANSACTION; SELECT b FROM dbo.t WITH (HOLDLOCK, ROWLOCK) WHERE a = 1;"),
                ("msdb", "BEGIN TRANSACTION; SELECT TOP (1) job_id FROM dbo.sysjobs WITH (HOLDLOCK);"),
            })
            {
                var builder = new SqlConnectionStringBuilder(c.Sql.Admin(database).ConnectionString) { ConnectRetryCount = 0 };
                await using var session = await OpenAsync(builder, c.Ct);
                await ExecAsync(session, sql, c.Ct);
                var id = Convert.ToInt32(await ScalarAsync(session, "SELECT @@SPID;", c.Ct), CultureInfo.InvariantCulture);
                await using var admin = await OpenAsync(c.Sql.Admin(), c.Ct);
                var login = (DateTime)(await ScalarAsync(admin, $"SELECT login_time FROM sys.dm_exec_sessions WHERE session_id = {id};", c.Ct))!;
                var answer = Parse(await KillAsync(c, id, login.AddMinutes(-offset).ToString("o", CultureInfo.InvariantCulture), dryRun: true));
                Assert.Equal("dry_run", (string?)answer["status"]);
                Assert.Equal(1, (int)answer["session"]!["open_transaction_count"]!);
                var bytes = answer["session"]!["log_bytes_used"];
                Assert.True(bytes is null || (long)bytes == 0, $"a warmed read-only transaction in {database} reported {bytes} log bytes");
                Assert.Empty(answer["warnings"]!.AsArray());
            }
        });
    }
}
