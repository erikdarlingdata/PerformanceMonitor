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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and its own SQL Server database, and never touches
   another test's rows, so the class is deliberately NOT [Collection("live-postgres")]. It shares the "remediation-live-sql" collection with
   RemediationLiveSqlTests instead: that class counts the sessions of the remediation application name on the one test SQL Server, and a
   force running beside it would change the count. */

/// <summary>
/// <c>force_query_store_plan</c> and <c>unforce_query_store_plan</c> against a live store AND a live SQL Server (#5632, step 7). Needs
/// <c>DARLING_TEST_PG</c> and <c>DARLING_TEST_SQL</c> (with <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c>); skips without them.
/// Each test makes its own store database, migrated, and its own SQL Server database with Query Store on and its own remediation login, hands the login to
/// the tool as an <c>env:</c> reference set in this process, and drops both in its own <c>finally</c>. The server row's host is the test SQL Server.
/// </summary>
[Collection("remediation-live-sql")]
public sealed partial class PlanForceToolLiveTests
{
    private static string? StoreConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");
    private static string? SqlHost => Environment.GetEnvironmentVariable("DARLING_TEST_SQL");

    private const string ForceKind = "plan_force";
    private const string UnforceKind = "plan_unforce";

    private sealed class Lines : ILogger
    {
        public List<string> All { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            All.Add(formatter(state, exception) + (exception is null ? "" : " " + exception));
    }

    /// <summary>One SQL Server database with Query Store on, with its own login. The caller drops it in its own finally.</summary>
    private sealed class SqlScratch
    {
        public string Database { get; } = "pm_pf_" + Guid.NewGuid().ToString("N")[..16];

        public string Password { get; } = "Aa1!" + Convert.ToHexString(RandomNumberGenerator.GetBytes(12));

        public string EnvName { get; } = "PM_PF_TEST_" + Guid.NewGuid().ToString("N");

        public string Login => Database;

        public SqlConnectionStringBuilder Admin(string database = "master") => new()
        {
            DataSource = SqlHost,
            InitialCatalog = database,
            UserID = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER"),
            Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD"),
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            Pooling = false,
        };

        public async Task CreateAsync(CancellationToken ct)
        {
            Environment.SetEnvironmentVariable(EnvName, Password);
            await using var admin = await OpenAsync(Admin(), ct);
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
                /* The test's own assertions report a real failure; a cleanup that cannot reach the server leaves a uniquely named scratch database. */
            }
        }

        /// <summary>Runs the marker query, makes a second plan for it (an index changes the plan), runs it again, and answers the query id and both plan ids.</summary>
        public async Task<(long QueryId, long PlanA, long PlanB)> TwoPlansAsync(string marker, CancellationToken ct)
        {
            await using var admin = await OpenAsync(Admin(Database), ct);
            for (var i = 0; i < 3; i++)
                await ExecAsync(admin, $"SELECT marker = '{marker}', n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1;", ct);
            await ExecAsync(admin, "CREATE INDEX ix_t_a ON dbo.t (a);", ct);
            for (var i = 0; i < 3; i++)
                await ExecAsync(admin, $"SELECT marker = '{marker}', n = COUNT_BIG(*) FROM dbo.t WHERE a >= 1;", ct);
            await ExecAsync(admin, "EXEC sys.sp_query_store_flush_db;", ct);
            await using var command = new SqlCommand(
                "SELECT q.query_id, p.plan_id FROM sys.query_store_query AS q JOIN sys.query_store_query_text AS t ON t.query_text_id = q.query_text_id "
                + "JOIN sys.query_store_plan AS p ON p.query_id = q.query_id WHERE t.query_sql_text LIKE @like AND t.query_sql_text NOT LIKE N'%query_store_query%' ORDER BY p.plan_id;", admin);
            command.Parameters.Add(new SqlParameter("@like", SqlDbType.NVarChar, 200) { Value = "%'" + marker + "'%" });
            var rows = new List<(long Query, long Plan)>();
            await using (var reader = await command.ExecuteReaderAsync(ct))
                while (await reader.ReadAsync(ct))
                    rows.Add((reader.GetInt64(0), reader.GetInt64(1)));
            Assert.True(rows.Count >= 2 && rows.Select(r => r.Query).Distinct().Count() == 1,
                $"Query Store should hold two plans of one query; it holds {rows.Count} row(s) over {rows.Select(r => r.Query).Distinct().Count()} query id(s).");
            return (rows[0].Query, rows[0].Plan, rows[1].Plan);
        }

        public async Task<long?> ForcedPlanAsync(long queryId, CancellationToken ct)
        {
            await using var admin = await OpenAsync(Admin(Database), ct);
            var value = await ScalarAsync(admin, $"SELECT TOP (1) plan_id FROM sys.query_store_plan WHERE query_id = {queryId} AND is_forced_plan = 1 ORDER BY plan_id;", ct);
            return value is null or DBNull ? null : Convert.ToInt64(value, CultureInfo.InvariantCulture);
        }

        public async Task<bool> HasForcingTypeColumnAsync(CancellationToken ct)
        {
            await using var admin = await OpenAsync(Admin(Database), ct);
            return await ScalarAsync(admin, "SELECT COL_LENGTH(N'sys.query_store_plan', N'plan_forcing_type_desc');", ct) is not (null or DBNull);
        }

        public async Task ForceDirectlyAsync(long queryId, long planId, CancellationToken ct)
        {
            await using var admin = await OpenAsync(Admin(Database), ct);
            await ExecAsync(admin, $"EXEC sys.sp_query_store_force_plan @query_id = {queryId}, @plan_id = {planId};", ct);
        }
    }

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

    private static async Task PgExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> PgScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>Which of the four conditions to leave out of the setup.</summary>
    private enum Without { Nothing, Switch, Level, ActionsEnabled, CredentialSlot, EvictSwitch, KillSwitch }

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Connection, SqlScratch Sql, string Arg, Lines Log, CancellationToken Ct)
    {
        public Task<string> Tool(bool force, long queryId, long planId, bool dryRun, string server = "srv-7") =>
            DarlingMcpPlanForceTools.RunAsync(Source, Source, Log, force, server, Sql.Database, queryId, planId, Arg, dryRun, Ct);
    }

    /// <param name="storeOnly">A test whose tool refuses before it reaches the monitored server needs only the store: no SQL Server database is made, and the server row's host is a closed local port.</param>
    private static async Task RunAsync(Without without, Func<Ctx, Task> body, bool storeOnly = false)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(StoreConnectionString), "Set DARLING_TEST_PG to run the plan force tool live tests.");
        Assert.SkipWhen(!storeOnly && string.IsNullOrEmpty(SqlHost), "Set DARLING_TEST_SQL (and DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the plan force tool live tests.");
        var ct = TestContext.Current.CancellationToken;
        var sql = new SqlScratch();
        await using var scratch = await ScratchPostgres.CreateAsync(StoreConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            if (storeOnly)
                Environment.SetEnvironmentVariable(sql.EnvName, sql.Password);
            else
                await sql.CreateAsync(ct);
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            var slot = without == Without.CredentialSlot ? "NULL" : "'env:" + sql.EnvName + "'";
            var user = without == Without.CredentialSlot ? "NULL" : $"'{sql.Login}'";
            await PgExecAsync(connection,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, encrypt_mode, trust_server_certificate, remediation_username, remediation_encrypted_password) "
                + $"VALUES (7, 'srv-7', '{(storeOnly ? "127.0.0.1,1" : SqlHost)}', 'Optional', TRUE, {user}, {slot}), (8, 'srv-8', 'pg-host', 'Optional', TRUE, NULL, NULL)", ct);
            await PgExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, engine_kind, created_date, modified_date) "
                + "VALUES (7, 'srv-7', 'srv-7', TRUE, 16, 'sqlserver', now(), now()), (8, 'srv-8', 'srv-8', TRUE, NULL, 'postgres', now(), now())", ct);
            await PgExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            if (without == Without.ActionsEnabled)
                await PgExecAsync(connection, "UPDATE config.config_service SET agent_actions_enabled = false", ct);
            var level = without == Without.Level ? 2 : 3;
            await PgExecAsync(connection, $"INSERT INTO config.config_notification_routes (metric_match, agent_level, action_approval) VALUES ('Blocking', {level}, 'off')", ct);
            if (without != Without.Switch)
                await PgExecAsync(connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'force_plan', true, 'test')", ct);
            if (without is not (Without.Switch or Without.KillSwitch))
                await PgExecAsync(connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'kill_session', true, 'test')", ct);
            if (without is not (Without.Switch or Without.EvictSwitch))
                await PgExecAsync(connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'evict_plan', true, 'test')", ct);

            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
            await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
            var store = new InvestigationRequestStore(source, 30);
            var alert = new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100,
                LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = level };
            var opened = await store.OpenOrJoinAsync(alert, ct);
            var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
            var arg = AgentActionArgumentTests.Arg(opened.RequestId.Value.ToString(), claim.ClaimId!.Value.ToString(), reason: "plan 2 regressed; peer agreed");
            await body(new Ctx(source, connection, sql, arg, new Lines(), ct));
            bodySucceeded = true;
        }
        finally
        {
            if (storeOnly)
                Environment.SetEnvironmentVariable(sql.EnvName, null);
            else
                await sql.DropAsync();
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    private static void AssertNoSecret(Ctx c, params string[] texts)
    {
        var secret = c.Sql.Password;
        Assert.DoesNotContain(c.Log.All, line => line.Contains(secret, StringComparison.Ordinal));
        foreach (var text in texts)
            Assert.DoesNotContain(secret, text, StringComparison.Ordinal);
    }

    private static async Task<string> JournalTextAsync(Ctx c) =>
        await PgScalarAsync(c.Connection, "SELECT coalesce(string_agg(event || ' ' || coalesce(reason, '') || ' ' || coalesce(detail::text, ''), ' | '), '') FROM config.investigation_journal", c.Ct) ?? "";

    [Fact]
    public async Task AForce_PlacesThePlan_JournalsTheBeforeAndAfterState_AndShowsInTheAgentChangesList()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf1", c.Ct);
            var answer = await c.Tool(force: true, query, planA, dryRun: false);
            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Null(json["before"]!["forced_plan"]);
            Assert.Equal(planA, (long)json["after"]!["forced_plan"]!["plan_id"]!);
            Assert.Equal("unforce_query_store_plan", (string?)json["undo"]!["tool"]);
            Assert.Equal(planA, (long)json["undo"]!["arguments"]!["plan_id"]!);
            Assert.Equal(query, (long)json["undo"]!["arguments"]!["query_id"]!);
            Assert.Equal((string?)json["before"]!["database"], (string?)json["undo"]!["arguments"]!["database_name"]);
            Assert.False(string.IsNullOrWhiteSpace((string?)json["undo"]!["arguments"]!["server_name"]));

            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(ForceKind, (string?)called["kind"]);
            Assert.Equal("force_query_store_plan", (string?)called["tool"]);
            Assert.Null(called["before"]!["forced_plan"]);
            var done = Assert.Single(await RowsAsync(c, "action_done"));
            Assert.Equal(planA, (long)done["after"]!["state"]!["forced_plan"]!["plan_id"]!);
            Assert.Equal((long)json["journal_id"]!, long.Parse(await PgScalarAsync(c.Connection, "SELECT journal_id FROM config.investigation_journal WHERE event = 'action_called'", c.Ct) ?? "0", CultureInfo.InvariantCulture));

            var list = await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct);
            var change = Assert.Single(list.Changes);
            Assert.Equal(ForceKind, change.Kind);
            Assert.Equal(AgentChangeResult.Done, change.Result);
            Assert.True(change.CanTryUndo(DateTime.UtcNow), "a done plan_force can be undone from the list (step 9)");
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task AnUnforce_RemovesTheForce_AndJournalsPlanUnforce()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf2", c.Ct);
            await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));

            var answer = await c.Tool(force: false, query, planA, dryRun: false);
            var json = Parse(answer);
            Assert.Equal("done", (string?)json["status"]);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal(planA, (long)json["before"]!["forced_plan"]!["plan_id"]!);
            Assert.Null(json["after"]!["forced_plan"]);
            Assert.Equal("force_query_store_plan", (string?)json["undo"]!["tool"]);
            Assert.Equal(planA, (long)json["undo"]!["arguments"]!["plan_id"]!);

            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(UnforceKind, (string?)called["kind"]);
            Assert.Equal(planA, (long)called["before"]!["forced_plan"]!["plan_id"]!);
            var list = await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct);
            Assert.Equal(UnforceKind, Assert.Single(list.Changes).Kind);
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    [Fact]
    public async Task AForce_WhileAnotherPlanIsForced_ReplacesIt_AndTheBeforeStateNamesTheOldPlanAndHowItWasForced()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, planB) = await c.Sql.TwoPlansAsync("pf3", c.Ct);
            await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);

            var json = Parse(await c.Tool(force: true, query, planB, dryRun: false));
            Assert.Equal("done", (string?)json["status"]);
            Assert.Equal(planB, await c.Sql.ForcedPlanAsync(query, c.Ct));
            var old = json["before"]!["forced_plan"]!;
            Assert.Equal(planA, (long)old["plan_id"]!);
            Assert.Equal(await c.Sql.HasForcingTypeColumnAsync(c.Ct) ? "MANUAL" : "unknown", (string?)old["forcing_type"]);
            Assert.Equal("force_query_store_plan", (string?)json["undo"]!["tool"]);
            Assert.Equal(planA, (long)json["undo"]!["arguments"]!["plan_id"]!);
            Assert.Null(json["undo"]!["note"]);
            var called = Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Equal(planA, (long)called["before"]!["forced_plan"]!["plan_id"]!);
        });
    }

    [Fact]
    public async Task ADryRun_IsTheDefault_ReturnsTheExactStatement_WritesNothing_AndJournalsNothing()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf4", c.Ct);
            var answer = await c.Tool(force: true, query, planA, dryRun: true);
            var json = Parse(answer);
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.Equal($"EXEC [{c.Sql.Database}].sys.sp_query_store_force_plan @query_id = {query}, @plan_id = {planA};", (string?)json["statement"]);
            Assert.Null(json["before"]!["forced_plan"]);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal("0", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'", c.Ct));

            /* The tool's own default is a dry run: the parameter's default is true. */
            var parameter = typeof(DarlingMcpPlanForceTools).GetMethod(nameof(DarlingMcpPlanForceTools.ForceQueryStorePlan))!.GetParameters().Single(p => p.Name == "dry_run");
            Assert.True((bool)parameter.DefaultValue!);
            var unforceParameter = typeof(DarlingMcpPlanForceTools).GetMethod(nameof(DarlingMcpPlanForceTools.UnforceQueryStorePlan))!.GetParameters().Single(p => p.Name == "dry_run");
            Assert.True((bool)unforceParameter.DefaultValue!);
            AssertNoSecret(c, answer);
        });
    }

    [Fact]
    public async Task APlanThatIsNotTheQuerys_OrIsAlreadyForced_IsRefusedWithoutAWrite_AndAnUnforceOfAnUnforcedPlanToo()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf5", c.Ct);
            Assert.Equal("plan_not_found", (string?)Parse(await c.Tool(force: true, query, planA + 100000, dryRun: false))["reason"]);
            Assert.Equal("plan_not_found", (string?)Parse(await c.Tool(force: true, query + 100000, planA, dryRun: false))["reason"]);
            Assert.Equal("plan_not_forced", (string?)Parse(await c.Tool(force: false, query, planA, dryRun: false))["reason"]);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            await c.Sql.ForceDirectlyAsync(query, planA, c.Ct);
            Assert.Equal("plan_already_forced", (string?)Parse(await c.Tool(force: true, query, planA, dryRun: false))["reason"]);
            Assert.Equal(planA, await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal("0", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'", c.Ct));
        });
    }

    private static async Task AssertRefusedAsync(Without without, string reason)
    {
        await RunAsync(without, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pfg", c.Ct);
            foreach (var dryRun in new[] { true, false })
            {
                var answer = await c.Tool(force: true, query, planA, dryRun);
                var json = Parse(answer);
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal(reason, (string?)json["reason"]);
                AssertNoSecret(c, answer);
            }

            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
            Assert.Equal("0", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'", c.Ct));
            Assert.Equal("1", await PgScalarAsync(c.Connection, $"SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = '{reason}'", c.Ct));
            AssertNoSecret(c, await JournalTextAsync(c));
        });
    }

    [Fact]
    public Task WithoutTheServersSwitch_TheCallIsRefused_Journaled_AndWritesNothing() => AssertRefusedAsync(Without.Switch, "server_action_off");

    [Fact]
    public Task WithoutALevelThreeRoute_TheCallIsRefused_Journaled_AndWritesNothing() => AssertRefusedAsync(Without.Level, "not_level_3");

    [Fact]
    public Task WithAgentActionsOff_TheCallIsRefused_Journaled_AndWritesNothing() => AssertRefusedAsync(Without.ActionsEnabled, "actions_off");

    [Fact]
    public Task WithoutTheCredentialSlot_TheCallIsRefused_Journaled_AndWritesNothing() => AssertRefusedAsync(Without.CredentialSlot, "no_remediation_credential");

    [Fact]
    public Task ADryRunWithNoAgentClaim_IsRefusedAsRequired_AndSaysWhyAndThatTheSwitchIsOff() => AssertDryRunRefusalAsync(Without.Switch, "is currently off");

    [Fact]
    public Task ADryRunWithNoAgentClaim_IsRefusedAsRequired_AndSaysWhyAndThatTheSwitchIsOn() => AssertDryRunRefusalAsync(Without.Nothing, "is currently on.");

    private static async Task AssertDryRunRefusalAsync(Without without, string expected)
    {
        await RunAsync(without, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pfd", c.Ct);
            var answer = await DarlingMcpPlanForceTools.RunAsync(c.Source, c.Source, c.Log, true, "srv-7", c.Sql.Database, query, planA, null, true, c.Ct);
            var json = Parse(answer);
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("agent_action_required", (string?)json["reason"]);
            var message = (string?)json["message"];
            Assert.Contains("A dry run needs it too", message, StringComparison.Ordinal);
            Assert.Contains(expected, message, StringComparison.Ordinal);
            Assert.Null(await c.Sql.ForcedPlanAsync(query, c.Ct));
        });
    }

    [Fact]
    public async Task APostgreSqlTarget_IsRefusedWithItsOwnReason_AndJournaled()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var answer = await c.Tool(force: true, 1, 1, dryRun: false, server: "srv-8");
            var json = Parse(answer);
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("postgres_target", (string?)json["reason"]);
            Assert.Equal("1", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event = 'refused' AND reason = 'postgres_target'", c.Ct));
            Assert.Equal("0", await PgScalarAsync(c.Connection, "SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%'", c.Ct));
        });
    }

    [Fact]
    public async Task AWriteThatTheServerRejects_IsJournaledActionFailed_WithoutTheSecret()
    {
        await RunAsync(Without.Nothing, async c =>
        {
            var (query, planA, _) = await c.Sql.TwoPlansAsync("pf8", c.Ct);
            /* The server refuses to force a plan while the database is read-only: the write fails after the gate and the before read passed. */
            await using (var admin = await OpenAsync(c.Sql.Admin(), c.Ct))
                await ExecAsync(admin, $"ALTER DATABASE [{c.Sql.Database}] SET READ_ONLY WITH ROLLBACK IMMEDIATE;", c.Ct);
            var answer = await c.Tool(force: true, query, planA, dryRun: false);
            var json = Parse(answer);
            Assert.NotEqual("done", (string?)json["status"]);
            Assert.Single(await RowsAsync(c, "action_called"));
            Assert.Empty(await RowsAsync(c, "action_done"));
            AssertNoSecret(c, answer, await JournalTextAsync(c));
        });
    }

    /// <summary>The detail objects of every journal row of an event.</summary>
    private static async Task<List<JsonObject>> RowsAsync(Ctx c, string journalEvent)
    {
        var rows = new List<JsonObject>();
        await using var command = new NpgsqlCommand("SELECT detail::text FROM config.investigation_journal WHERE event = $1 ORDER BY journal_id", c.Connection);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = journalEvent, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        await using var reader = await command.ExecuteReaderAsync(c.Ct);
        while (await reader.ReadAsync(c.Ct))
            rows.Add(JsonNode.Parse(reader.GetString(0))!.AsObject());
        return rows;
    }
}
