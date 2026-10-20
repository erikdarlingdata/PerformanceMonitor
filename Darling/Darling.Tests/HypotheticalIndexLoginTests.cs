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
[Collection("darling-owned-secrets")]
public sealed class HypotheticalIndexLoginTests : IDisposable
{
    /* The password reference is read through the owned-secret check (#5240), which refuses every reference until a configuration has
       loaded. An empty owned set stands for a loaded configuration that owns nothing; the process-wide value is put back. */
    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    public HypotheticalIndexLoginTests() =>
        DarlingOwnedSecrets.Set(new DarlingOwnedSet(Array.Empty<string>(), Array.Empty<string>()));

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    private const string MonitoringConnection =
        "Host=db.example;Port=5433;Database=app;Username=monitoring;Password=monitoring-secret;SSL Mode=Require;Options=-c search_path=pg_catalog,pg_temp";

    internal static MonitoredServer Server(string? user, string? reference) => new()
    {
        Name = "pg-one",
        Host = "db.example",
        Auth = "sql",
        Username = "monitoring",
        HypotheticalIndexUsername = user,
        HypotheticalIndexEncryptedPassword = reference,
    };

    [Theory]
    [InlineData(null, null)]
    [InlineData("check_login", null)]
    [InlineData(null, "env:PM6170_UNUSED")]
    [InlineData("  ", "env:PM6170_UNUSED")]
    public void WithNoLoginSetUp_TheCheckIsRefused_AndTheRefusalSaysWhatToSetUp(string? user, string? reference)
    {
        var (connection, refusal) = HypotheticalIndexLogin.Resolve(Server(user, reference), MonitoringConnection);

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
        var (connection, refusal) = HypotheticalIndexLogin.Resolve(Server("check_login", "plain-password"), MonitoringConnection);

        Assert.Null(connection);
        Assert.Contains("env:NAME or file:path", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("plain-password", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void AnUnsetVariable_IsRefused_WithoutRepeatingTheReference()
    {
        var (connection, refusal) = HypotheticalIndexLogin.Resolve(
            Server("check_login", "env:PM6170_DEFINITELY_NOT_SET_ANYWHERE"), MonitoringConnection);

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
            var (connection, refusal) = HypotheticalIndexLogin.Resolve(Server(" check_login ", "file:" + path), MonitoringConnection);

            Assert.Null(refusal);
            var parsed = new NpgsqlConnectionStringBuilder(connection);
            Assert.Equal("check_login", parsed.Username);
            Assert.Equal("check-secret", parsed.Password);
            Assert.Equal(HypotheticalIndexLogin.ApplicationName, parsed.ApplicationName);
            Assert.False(parsed.Pooling);
            Assert.Equal("db.example", parsed.Host);
            Assert.Equal(5433, parsed.Port);
            Assert.Equal("app", parsed.Database);
            /* #6170: the startup option that pins search_path is kept, so CreatePinnedConnection's check still applies to this connection. */
            Assert.True(MonitoredServerConnection.CarriesPin(parsed.Options));
            Assert.DoesNotContain("monitoring-secret", connection, StringComparison.Ordinal);
            Assert.DoesNotContain("Username=monitoring;", connection, StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData("Prefer", "Require")]
    [InlineData("Require", "Require")]
    [InlineData("VerifyFull", "Prefer")]
    public void TheCheckSignsInWithScramOnly_AndRequiresChannelBindingUnlessTheCertificateIsVerifiedInFull(string sslMode, string channelBinding)
    {
        const string variable = "PM6170_SCRAM_RULE_PASSWORD";
        Environment.SetEnvironmentVariable(variable, "check-secret");
        try
        {
            var monitoring = $"Host=db.example;Database=app;Username=monitoring;Password=monitoring-secret;SSL Mode={sslMode};"
                + "Options=-c search_path=pg_catalog,pg_temp;Require Auth=Password,MD5,ScramSHA256;Channel Binding=Disable";

            var (connection, refusal) = HypotheticalIndexLogin.Resolve(Server("check_login", "env:" + variable), monitoring);

            Assert.Null(refusal);
            var parsed = new NpgsqlConnectionStringBuilder(connection);
            /* Whatever the monitoring string carried (here: clear text and MD5 allowed, channel binding off), the check's own sign-in is SCRAM-SHA-256 only. */
            Assert.Equal("ScramSHA256", parsed.RequireAuth);
            Assert.Equal(Enum.Parse<ChannelBinding>(channelBinding), parsed.ChannelBinding);
            Assert.Equal(Enum.Parse<SslMode>(sslMode), parsed.SslMode);
        }
        finally
        {
            Environment.SetEnvironmentVariable(variable, null);
        }
    }

    [Fact]
    public void ThePasswordIsUsedAsTheReferenceGivesIt_NotTrimmed()
    {
        const string variable = "PM6170_PADDED_PASSWORD";
        Environment.SetEnvironmentVariable(variable, " pad ded ");
        try
        {
            var (connection, refusal) = HypotheticalIndexLogin.Resolve(Server("check_login", "env:" + variable), MonitoringConnection);

            Assert.Null(refusal);
            Assert.Equal(" pad ded ", new NpgsqlConnectionStringBuilder(connection).Password);
        }
        finally
        {
            Environment.SetEnvironmentVariable(variable, null);
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
/// #6170: the password reference of the hypothetical-index login is checked where it resolves, like every other server secret (#5240). A
/// reference held in the store that names one of this service's own configuration files or secrets is refused before it is read, unless
/// darling.json declares that same reference for the same server. The owned set is process-wide state, so the class is in the
/// <c>darling-owned-secrets</c> collection and puts back what it found.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class HypotheticalIndexPasswordReferenceTests : IDisposable
{
    private const string OwnedVariable = "DARLING_TEST_OWNED_VARIABLE_6170";
    private const string OwnedVariableValue = "owned-variable-value-Q7";
    private const string OtherVariable = "DARLING_TEST_OTHER_VARIABLE_6170";
    private const string OtherVariableValue = "other-variable-value-Q7";

    private readonly string _root = Path.Combine(Path.GetTempPath(), "darling-hypo-ref-" + Guid.NewGuid().ToString("N"));
    private readonly DarlingOwnedSet _before = DarlingOwnedSecrets.Current;
    private readonly string _ownedFile;

    public HypotheticalIndexPasswordReferenceTests()
    {
        var ownedDirectory = Path.Combine(_root, "own");
        Directory.CreateDirectory(ownedDirectory);
        _ownedFile = Path.Combine(ownedDirectory, "secret.txt");
        File.WriteAllText(_ownedFile, "owned-file-value-Q7");
        Environment.SetEnvironmentVariable(OwnedVariable, OwnedVariableValue);
        Environment.SetEnvironmentVariable(OtherVariable, OtherVariableValue);
        DarlingOwnedSecrets.Set(new DarlingOwnedSet(new[] { ownedDirectory }, new[] { OwnedVariable }));
    }

    public void Dispose()
    {
        DarlingOwnedSecrets.Set(_before);
        Environment.SetEnvironmentVariable(OwnedVariable, null);
        Environment.SetEnvironmentVariable(OtherVariable, null);
        try
        {
            Directory.Delete(_root, true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* best effort; a leftover scratch directory under the temp path harms nothing */
        }
    }

    [Fact]
    public void AnOwnedEnvReference_IsRefusedBeforeItResolves()
    {
        var server = HypotheticalIndexLoginTests.Server("check_login", "env:" + OwnedVariable);

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolveHypotheticalIndexPassword(server));

        Assert.Contains("hypotheticalIndexEncryptedPassword", ex.Message, StringComparison.Ordinal);
        Assert.Contains(DarlingOwnedSecrets.ReferenceRefusalText, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(OwnedVariable, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(OwnedVariableValue, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOwnedFileReference_IsRefusedBeforeItIsRead_AndTheCheckSaysSoWithoutThePath()
    {
        var server = HypotheticalIndexLoginTests.Server("check_login", "file:" + _ownedFile);

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolveHypotheticalIndexPassword(server));
        Assert.Contains(DarlingOwnedSecrets.ReferenceRefusalText, ex.Message, StringComparison.Ordinal);

        var (connection, refusal) = HypotheticalIndexLogin.Resolve(server, "Host=db.example;Database=app");
        Assert.Null(connection);
        Assert.Contains("could not be read", refusal, StringComparison.Ordinal);
        Assert.DoesNotContain(_ownedFile, refusal, StringComparison.Ordinal);
        Assert.DoesNotContain("owned-file-value-Q7", refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOwnedReferenceTheFileDeclaresForThisServer_StillResolves()
    {
        var server = HypotheticalIndexLoginTests.Server("check_login", "env:" + OwnedVariable);
        server.HypotheticalIndexEncryptedPasswordDeclaredByFile = true;

        Assert.Equal(OwnedVariableValue, DarlingSecrets.ResolveHypotheticalIndexPassword(server));
    }

    [Fact]
    public void AReferenceToSomethingThatIsNotOwned_Resolves()
    {
        var server = HypotheticalIndexLoginTests.Server("check_login", "env:" + OtherVariable);

        Assert.Equal(OtherVariableValue, DarlingSecrets.ResolveHypotheticalIndexPassword(server));
    }

    [Fact]
    public void AValueThatIsNotAReference_IsRefused_WithoutRepeatingIt()
    {
        var server = HypotheticalIndexLoginTests.Server("check_login", "a-stored-password");

        var ex = Assert.Throws<InvalidOperationException>(() => DarlingSecrets.ResolveHypotheticalIndexPassword(server));

        Assert.DoesNotContain("a-stored-password", ex.Message, StringComparison.Ordinal);
    }
}

/// <summary>
/// #6170: against a real cluster, a function another role created and a statement that calls it with constant arguments runs under the
/// check's own login, not under the monitoring login, in a read-only transaction, and the grant set the docs list (CONNECT, USAGE, SELECT)
/// is enough to plan. The cluster is the SCRAM-and-TLS one <see cref="PostgresKillLiveTests.ScramTlsCluster"/> boots, because the check signs in
/// with SCRAM-SHA-256 only, as the remediation login does: a server that asks for MD5 is refused before the password is sent.
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class HypotheticalIndexLoginLiveTests : IClassFixture<PostgresKillLiveTests.ScramTlsCluster>, IDisposable
{
    private readonly PostgresKillLiveTests.ScramTlsCluster cluster;
    private readonly DarlingOwnedSet _ownedBefore = DarlingOwnedSecrets.Current;

    public HypotheticalIndexLoginLiveTests(PostgresKillLiveTests.ScramTlsCluster cluster)
    {
        this.cluster = cluster;
        /* A loaded configuration that owns nothing, so the password reference is not refused for want of a configuration (#5240). */
        DarlingOwnedSecrets.Set(new DarlingOwnedSet(Array.Empty<string>(), Array.Empty<string>()));
    }

    public void Dispose() => DarlingOwnedSecrets.Set(_ownedBefore);

    private const string Database = "pm6170_app";
    private const string PasswordVariable = "PM6170_CHECK_PASSWORD";

    private readonly string _tag = Guid.NewGuid().ToString("N")[..10];
    private string Owner => "pm6170_owner_" + _tag;
    private string Monitor => "pm6170_monitor_" + _tag;
    private string Check => "pm6170_check_" + _tag;
    private string Md5Login => "pmk_md5_" + _tag;

    private static string NewPassword() => "Aa1" + Guid.NewGuid().ToString("N");

    private static MonitoredServer ServerWithLogin(string user, string variable) => new()
    {
        Name = "pg-live",
        Host = "127.0.0.1",
        Auth = "sql",
        Username = "monitoring",
        HypotheticalIndexUsername = user,
        HypotheticalIndexEncryptedPassword = "env:" + variable,
    };

    [Fact]
    public async Task AFunctionAnotherRoleCreated_RunsUnderTheChecksOwnLogin_InAReadOnlyTransaction_AndNotUnderTheMonitoringLogin()
    {
        Assert.SkipWhen(cluster.SkipReason is not null, cluster.SkipReason ?? string.Empty);

        var ct = TestContext.Current.CancellationToken;
        var admin = cluster.SuperuserConnectionString;
        var app = new NpgsqlConnectionStringBuilder(admin) { Database = Database }.ConnectionString;
        var ownerPassword = NewPassword();
        var monitorPassword = NewPassword();
        var checkPassword = NewPassword();

        try
        {
            var major = (int)(await ScalarAsync(admin, "SELECT pg_catalog.current_setting('server_version_num')::int / 10000", ct));
            Assert.SkipWhen(major < HypotheticalIndexExperiment.MinimumPostgresMajorForGenericPlan, "EXPLAIN (GENERIC_PLAN) needs PostgreSQL 16.");

            await ExecAsync(admin, $"CREATE ROLE \"{Owner}\" LOGIN PASSWORD '{ownerPassword}'", ct);
            await ExecAsync(admin, $"CREATE ROLE \"{Monitor}\" LOGIN PASSWORD '{monitorPassword}'", ct);
            await ExecAsync(admin, $"GRANT pg_monitor TO \"{Monitor}\"", ct);
            await ExecAsync(admin, $"CREATE ROLE \"{Check}\" LOGIN PASSWORD '{checkPassword}'", ct);
            await ExecAsync(admin, $"CREATE DATABASE {Database}", ct);

            /* The grant set the docs list for the check's login, and nothing else: CONNECT, USAGE on the schema, SELECT on one table. */
            await ExecAsync(app, $"""
                REVOKE CONNECT ON DATABASE {Database} FROM PUBLIC;
                GRANT CONNECT ON DATABASE {Database} TO "{Owner}", "{Monitor}", "{Check}";
                CREATE SCHEMA app AUTHORIZATION "{Owner}";
                SET ROLE "{Owner}";
                CREATE TABLE app.t (id int PRIMARY KEY, v text);
                CREATE TABLE app.u (id int PRIMARY KEY, w text);
                /* Planning folds this constant call, so it runs while the statement is planned. It reports who called it. */
                CREATE FUNCTION app.who() RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
                BEGIN RAISE EXCEPTION 'who=%', session_user; END $$;
                /* The same for a STABLE function, which planning runs while it estimates selectivity. */
                CREATE FUNCTION app.who_stable() RETURNS text LANGUAGE plpgsql STABLE AS $$
                BEGIN RAISE EXCEPTION 'who_stable=%', session_user; END $$;
                /* It reports whether the transaction it runs in is read-only. */
                CREATE FUNCTION app.ro() RETURNS text LANGUAGE plpgsql IMMUTABLE AS $$
                BEGIN RAISE EXCEPTION 'ro=%', pg_catalog.current_setting('transaction_read_only'); END $$;
                RESET ROLE;
                GRANT USAGE ON SCHEMA app TO "{Monitor}", "{Check}";
                GRANT SELECT ON app.t TO "{Monitor}", "{Check}";
                """, ct);

            var monitoringConnectionString = new NpgsqlConnectionStringBuilder(cluster.ConnectionString(Monitor, monitorPassword))
            {
                Database = Database,
                Options = MonitoredServerConnection.PinnedSearchPathOption,
                Pooling = false,
            }.ConnectionString;
            Environment.SetEnvironmentVariable(PasswordVariable, checkPassword);
            var (checkConnectionString, refusal) = HypotheticalIndexLogin.Resolve(ServerWithLogin(Check, PasswordVariable), monitoringConnectionString);
            Assert.Null(refusal);

            const string callsTheFunction = "SELECT id FROM app.t WHERE id = $1 AND v = app.who()";

            /* Under the check's login the function runs as that login. */
            var underCheck = await Assert.ThrowsAsync<PostgresException>(() => RunAsync(checkConnectionString!, callsTheFunction, major, ct));
            Assert.Contains($"who={Check}", underCheck.MessageText, StringComparison.Ordinal);
            Assert.DoesNotContain(Monitor, underCheck.MessageText, StringComparison.Ordinal);

            /* A STABLE function is run by planning too, and runs as the check's login. */
            var stable = await Assert.ThrowsAsync<PostgresException>(
                () => RunAsync(checkConnectionString!, "SELECT id FROM app.t WHERE id = $1 AND v = app.who_stable()", major, ct));
            Assert.Contains($"who_stable={Check}", stable.MessageText, StringComparison.Ordinal);

            /* The same statement over the monitoring login would run it as the monitoring login: the shape the separate login removes. */
            var underMonitor = await Assert.ThrowsAsync<PostgresException>(() => RunAsync(monitoringConnectionString, callsTheFunction, major, ct));
            Assert.Contains($"who={Monitor}", underMonitor.MessageText, StringComparison.Ordinal);

            /* The experiment's transaction is read-only, so a function planning runs cannot write. */
            var readOnly = await Assert.ThrowsAsync<PostgresException>(
                () => RunAsync(checkConnectionString!, "SELECT id FROM app.t WHERE id = $1 AND v = app.ro()", major, ct));
            Assert.Contains("ro=on", readOnly.MessageText, StringComparison.Ordinal);

            /* The grant set is what planning needs: SELECT on the table the statement reads. A table it was not given is refused at planning. */
            var noSelect = await Assert.ThrowsAsync<PostgresException>(() => RunAsync(checkConnectionString!, "SELECT id FROM app.u WHERE id = $1", major, ct));
            Assert.Equal(PostgresErrorCodes.InsufficientPrivilege, noSelect.SqlState);

            /* The login holds no read-all role and no write grant. */
            Assert.Equal(0L, await ScalarAsync(admin,
                $"SELECT pg_catalog.count(*) FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles r ON r.oid = m.member WHERE r.rolname = '{Check}'", ct));
            Assert.Equal(0L, await ScalarAsync(app,
                $"SELECT (pg_catalog.has_table_privilege('{Check}', 'app.t', 'INSERT')::int + pg_catalog.has_table_privilege('{Check}', 'app.t', 'UPDATE')::int "
                + $"+ pg_catalog.has_table_privilege('{Check}', 'app.t', 'DELETE')::int)::bigint", ct));
        }
        finally
        {
            Environment.SetEnvironmentVariable(PasswordVariable, null);
            await DropAsync(admin);
        }
    }

    [Fact]
    public async Task AServerThatAsksForMd5_IsRefused_BeforeThePasswordIsSent()
    {
        Assert.SkipWhen(cluster.SkipReason is not null, cluster.SkipReason ?? string.Empty);

        var ct = TestContext.Current.CancellationToken;
        var admin = cluster.SuperuserConnectionString;
        var md5Password = NewPassword();
        const string variable = "PM6170_MD5_PASSWORD";

        try
        {
            /* A login whose password is stored as MD5, and a pg_hba line (TLS, 127.0.0.1) that asks it for MD5. The password is right: the
               sign-in would succeed if the check accepted MD5. */
            await ExecAsync(admin, $"SET password_encryption = 'md5'; CREATE ROLE \"{Md5Login}\" LOGIN PASSWORD '{md5Password}'; RESET password_encryption;", ct);

            var monitoringConnectionString = new NpgsqlConnectionStringBuilder(cluster.ConnectionString(Md5Login, md5Password))
            {
                Database = "postgres",
                Options = MonitoredServerConnection.PinnedSearchPathOption,
            }.ConnectionString;
            Environment.SetEnvironmentVariable(variable, md5Password);
            var (checkConnectionString, refusal) = HypotheticalIndexLogin.Resolve(ServerWithLogin(Md5Login, variable), monitoringConnectionString);
            Assert.Null(refusal);

            var ex = await Assert.ThrowsAsync<NpgsqlException>(async () =>
            {
                await using var connection = PostgresTargetProvider.CreatePinnedConnection(checkConnectionString!);
                await connection.OpenAsync(ct);
            });

            Assert.True(HypotheticalIndexLogin.IsSignInRuleRefusal(ex), ex.Message);
            Assert.Contains("MD5", ex.Message, StringComparison.Ordinal);
            Assert.Contains("authentication method is not allowed", ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(variable, null);
            await DropAsync(admin);
        }
    }

    private static async Task RunAsync(string connectionString, string statement, int major, CancellationToken ct)
    {
        await using var connection = PostgresTargetProvider.CreatePinnedConnection(connectionString);
        await connection.OpenAsync(ct);
        await HypotheticalIndexExperiment.RunAsync(connection, statement, "CREATE INDEX ON app.t (v)", major, ct);
    }

    private async Task DropAsync(string admin)
    {
        NpgsqlConnection.ClearAllPools();
        PostgresTargetProvider.ClearPools();
        /* No TimescaleDB job worker is left running in the database when the drop goes ahead (#5480). The helper returns at once
           when the database carries no TimescaleDB jobs, or is already gone. */
        await ScratchPostgres.QuiesceTimescaleJobsAsync(admin, Database);
        foreach (var sql in new[]
        {
            $"SELECT pg_catalog.pg_terminate_backend(pid) FROM pg_catalog.pg_stat_activity WHERE datname = '{Database}'",
            $"DROP DATABASE IF EXISTS {Database}",
            $"DROP ROLE IF EXISTS \"{Owner}\"",
            $"DROP ROLE IF EXISTS \"{Monitor}\"",
            $"DROP ROLE IF EXISTS \"{Check}\"",
            $"DROP ROLE IF EXISTS \"{Md5Login}\"",
        })
        {
            try
            {
                await ExecAsync(admin, sql, CancellationToken.None);
            }
            catch (PostgresException)
            {
                /* Best effort: the cluster is deleted when the class ends. */
            }
        }
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
