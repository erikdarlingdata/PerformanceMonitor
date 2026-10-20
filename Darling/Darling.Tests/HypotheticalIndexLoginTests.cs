using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6170: the hypothetical-index check plans statements taken from pg_stat_statements, and planning can evaluate a function another
/// role created when its arguments are constants. The check therefore runs under its own login, never under the monitoring login.
/// </summary>
public sealed class HypotheticalIndexLoginTests
{
    private const string MonitoringConnection =
        "Host=db.example;Port=5433;Database=app;Username=monitoring;Password=monitoring-secret;SSL Mode=Require;Options=-c search_path=pg_catalog,pg_temp";

    [Theory]
    [InlineData(null, null)]
    [InlineData("check_login", null)]
    [InlineData(null, "env:PM6170_UNUSED")]
    [InlineData("  ", "env:PM6170_UNUSED")]
    public void WithNoLoginSetUp_TheCheckIsRefused_AndTheRefusalSaysWhatToSetUp(string? user, string? reference)
    {
        var (connection, refusal) = HypotheticalIndexLogin.Resolve("pg-one", user, reference, MonitoringConnection);

        Assert.Null(connection);
        Assert.NotNull(refusal);
        Assert.Contains("hypotheticalIndexUsername", refusal, StringComparison.Ordinal);
        Assert.Contains("hypotheticalIndexEncryptedPassword", refusal, StringComparison.Ordinal);
        Assert.Contains("CONNECT", refusal, StringComparison.Ordinal);
        Assert.Contains("USAGE", refusal, StringComparison.Ordinal);
        Assert.Contains("SELECT", refusal, StringComparison.Ordinal);
        Assert.Contains("never runs under the monitoring login", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("monitoring-secret", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void AStoredPassword_IsRefused_NotUsed()
    {
        var (connection, refusal) = HypotheticalIndexLogin.Resolve("pg-one", "check_login", "plain-password", MonitoringConnection);

        Assert.Null(connection);
        Assert.Contains("env:NAME or file:path", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("plain-password", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnsetVariable_IsRefused_WithoutRepeatingTheReference()
    {
        var (connection, refusal) = HypotheticalIndexLogin.Resolve(
            "pg-one", "check_login", "env:PM6170_DEFINITELY_NOT_SET_ANYWHERE", MonitoringConnection);

        Assert.Null(connection);
        Assert.Contains("could not be read", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("PM6170_DEFINITELY_NOT_SET_ANYWHERE", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void AFileReference_SignsInAsTheCheckLogin_OnTheMonitoringConnectionsServer()
    {
        var path = Path.Combine(Path.GetTempPath(), "pm6170-" + Guid.NewGuid().ToString("N") + ".txt");
        File.WriteAllText(path, "check-secret\n");
        try
        {
            var (connection, refusal) = HypotheticalIndexLogin.Resolve("pg-one", " check_login ", "file:" + path, MonitoringConnection);

            Assert.Null(refusal);
            var parsed = new NpgsqlConnectionStringBuilder(connection);
            Assert.Equal("check_login", parsed.Username);
            Assert.Equal("check-secret", parsed.Password);
            Assert.Equal(HypotheticalIndexLogin.ApplicationName, parsed.ApplicationName);
            Assert.False(parsed.Pooling);
            Assert.Equal("db.example", parsed.Host);
            Assert.Equal(5433, parsed.Port);
            Assert.Equal("app", parsed.Database);
            Assert.DoesNotContain("monitoring-secret", connection, StringComparison.Ordinal);
            Assert.DoesNotContain("Username=monitoring;", connection, StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData("SELECT 1", true)]
    [InlineData("  select * from t where id = $1", true)]
    [InlineData("/* trace */ -- note\nSELECT a FROM t", true)]
    [InlineData("WITH x AS (SELECT 1) SELECT * FROM x", true)]
    [InlineData("(SELECT 1) UNION (SELECT 2)", true)]
    [InlineData("INSERT INTO t VALUES ($1)", false)]
    [InlineData("UPDATE t SET a = $1", false)]
    [InlineData("DELETE FROM t WHERE id = $1", false)]
    [InlineData("/* SELECT */ DELETE FROM t", false)]
    [InlineData("-- SELECT\nUPDATE t SET a = 1", false)]
    [InlineData("selecting_is_not_select()", false)]
    [InlineData("", false)]
    public void OnlySelectStatementsAreTested(string text, bool expected)
    {
        Assert.Equal(expected, HypotheticalIndexExperiment.IsSelectStatement(text));
    }

    [Fact]
    public async Task AWriteStatement_IsRefusedWithAPlainSentence_BeforeAnythingIsSent()
    {
        await using var connection = new NpgsqlConnection("Host=127.0.0.1;Port=1;Timeout=1");

        var result = await HypotheticalIndexExperiment.RunAsync(
            connection, "UPDATE t SET a = $1 WHERE id = $2", "CREATE INDEX ON t (a)", 17,
            TestContext.Current.CancellationToken);

        Assert.False(result.PlannerWouldUseIt);
        Assert.Contains("Only SELECT statements are tested", result.Explanation, StringComparison.Ordinal);
    }
}

/// <summary>
/// #6170: against a real cluster, a function another role created and a statement that calls it with constant arguments runs under the
/// check's own login, not under the monitoring login, and the grant set the docs list (CONNECT, USAGE, SELECT) is enough to plan.
/// </summary>
[Collection("live-postgres")]
public sealed class HypotheticalIndexLoginLiveTests
{
    private const string Database = "pm6170_app";
    private const string Owner = "pm6170_owner";
    private const string Monitor = "pm6170_monitor";
    private const string Check = "pm6170_check";
    private const string PasswordVariable = "PM6170_CHECK_PASSWORD";

    [Fact]
    public async Task AFunctionAnotherRoleCreated_RunsUnderTheChecksOwnLogin_AndNotTheMonitoringLogin()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the hypothetical-index login live test.");

        var ct = TestContext.Current.CancellationToken;
        var admin = new NpgsqlConnectionStringBuilder(adminConnectionString) { Pooling = false };
        var app = new NpgsqlConnectionStringBuilder(admin.ConnectionString) { Database = Database };

        var bodySucceeded = false;
        try
        {
            await ResetAsync(admin.ConnectionString, ct);
            var major = (int)(await ScalarAsync(admin.ConnectionString, "SELECT pg_catalog.current_setting('server_version_num')::int / 10000", ct));
            Assert.SkipWhen(major < HypotheticalIndexExperiment.MinimumPostgresMajorForGenericPlan, "EXPLAIN (GENERIC_PLAN) needs PostgreSQL 16.");

            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Owner} LOGIN", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Monitor} LOGIN", ct);
            await ExecAsync(admin.ConnectionString, $"GRANT pg_monitor TO {Monitor}", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE ROLE {Check} LOGIN", ct);
            await ExecAsync(admin.ConnectionString, $"CREATE DATABASE {Database}", ct);

            /* The grant set the docs list for the check's login, and nothing else: CONNECT, USAGE on the schema, SELECT on one table. */
            await ExecAsync(app.ConnectionString, $"""
                REVOKE CONNECT ON DATABASE {Database} FROM PUBLIC;
                GRANT CONNECT ON DATABASE {Database} TO {Owner}, {Monitor}, {Check};
                CREATE SCHEMA app AUTHORIZATION {Owner};
                SET ROLE {Owner};
                CREATE TABLE app.t (id int PRIMARY KEY, v text);
                CREATE TABLE app.u (id int PRIMARY KEY, w text);
                /* Planning folds this constant call, so it runs while the statement is planned. It reports who called it. */
                CREATE FUNCTION app.who() RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
                BEGIN RAISE EXCEPTION 'who=%', session_user; END $$;
                RESET ROLE;
                GRANT USAGE ON SCHEMA app TO {Monitor}, {Check};
                GRANT SELECT ON app.t TO {Monitor}, {Check};
                """, ct);

            var monitoringConnectionString = new NpgsqlConnectionStringBuilder(app.ConnectionString) { Username = Monitor, Pooling = false }.ConnectionString;
            Environment.SetEnvironmentVariable(PasswordVariable, "unused-on-a-trust-rig");
            var (checkConnectionString, refusal) = HypotheticalIndexLogin.Resolve("pg-live", Check, "env:" + PasswordVariable, monitoringConnectionString);
            Assert.Null(refusal);

            const string callsTheFunction = "SELECT id FROM app.t WHERE id = $1 AND v = app.who()";

            /* Under the check's login the function runs as that login. */
            var underCheck = await Assert.ThrowsAsync<PostgresException>(() => RunAsync(checkConnectionString!, callsTheFunction, major, ct));
            Assert.Contains($"who={Check}", underCheck.MessageText, StringComparison.Ordinal);
            Assert.DoesNotContain(Monitor, underCheck.MessageText, StringComparison.Ordinal);

            /* The same statement over the monitoring login would run it as the monitoring login: the shape the separate login removes. */
            var underMonitor = await Assert.ThrowsAsync<PostgresException>(() => RunAsync(monitoringConnectionString, callsTheFunction, major, ct));
            Assert.Contains($"who={Monitor}", underMonitor.MessageText, StringComparison.Ordinal);

            /* The grant set is what planning needs: SELECT on the table the statement reads. A table it was not given is refused at planning. */
            var noSelect = await Assert.ThrowsAsync<PostgresException>(() => RunAsync(checkConnectionString!, "SELECT id FROM app.u WHERE id = $1", major, ct));
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, noSelect.SqlState);

            /* The login holds no read-all role and no write grant. */
            Assert.Equal(0L, await ScalarAsync(admin.ConnectionString,
                $"SELECT pg_catalog.count(*) FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r ON r.oid = m.member WHERE r.rolname = '{Check}'", ct));
            Assert.Equal(0L, await ScalarAsync(app.ConnectionString,
                $"SELECT (pg_catalog.has_table_privilege('{Check}', 'app.t', 'INSERT')::int + pg_catalog.has_table_privilege('{Check}', 'app.t', 'UPDATE')::int "
                + $"+ pg_catalog.has_table_privilege('{Check}', 'app.t', 'DELETE')::int)::bigint", ct));

            bodySucceeded = true;
        }
        finally
        {
            Environment.SetEnvironmentVariable(PasswordVariable, null);
            await LiveStoreCleanup.RunAsync(admin.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await DropAsync(cleanup, cleanupCt));
        }
    }

    private static async Task RunAsync(string connectionString, string statement, int major, CancellationToken ct)
    {
        await using var connection = PostgresTargetProvider.CreatePinnedConnection(connectionString);
        await connection.OpenAsync(ct);
        await HypotheticalIndexExperiment.RunAsync(connection, statement, "CREATE INDEX ON app.t (v)", major, ct);
    }

    private static async Task ResetAsync(string adminConnectionString, CancellationToken ct)
    {
        NpgsqlConnection.ClearAllPools();
        PostgresTargetProvider.ClearPools();
        await using var connection = new NpgsqlConnection(adminConnectionString);
        await connection.OpenAsync(ct);
        await DropAsync(connection, ct);
    }

    private static async Task DropAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        NpgsqlConnection.ClearAllPools();
        PostgresTargetProvider.ClearPools();
        if (await DatabaseExistsAsync(connection, ct))
        {
            await ScratchPostgres.QuiesceTimescaleJobsAsync(connection.ConnectionString, Database, ct);
        }

        foreach (var sql in new[]
        {
            $"SELECT pg_catalog.pg_terminate_backend(pid) FROM pg_catalog.pg_stat_activity WHERE datname = '{Database}'",
            $"DROP DATABASE IF EXISTS {Database}",
            $"DROP ROLE IF EXISTS {Owner}",
            $"DROP ROLE IF EXISTS {Monitor}",
            $"DROP ROLE IF EXISTS {Check}",
        })
        {
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<bool> DatabaseExistsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT pg_catalog.count(*) FROM pg_catalog.pg_database WHERE datname = '{Database}'", connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) > 0;
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }
}
