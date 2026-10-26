/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6472 on a real SQL Server: the generated script creates a login and grants, and running it twice is clean; the connection test's grant
/// check names exactly the grants a login lacks, with the statement that adds each, and says nothing for a login that has them.
/// <para>Gated on <c>DARLING_TEST_SQL</c>, <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> (a sysadmin on a dev box, the
/// sql2019 one; never a monitored server). It creates throwaway logins named <c>pm_grant_test_&lt;random&gt;</c> and drops them, and the
/// msdb users the script made, in a finally. It never alters an existing login.</para>
/// </summary>
public sealed class MonitoringGrantScriptLiveSqlTests
{
    private static readonly string[] s_envNames = { "DARLING_TEST_SQL", "DARLING_TEST_SQL_USER", "DARLING_TEST_SQL_PASSWORD" };

    private static (string Host, string User, string Password)? Admin()
    {
        var values = Array.ConvertAll(s_envNames, Environment.GetEnvironmentVariable);
        return Array.Exists(values, string.IsNullOrEmpty) ? null : (values[0]!, values[1]!, values[2]!);
    }

    private static string Cs(string host, string user, string password) =>
        new SqlConnectionStringBuilder
        {
            DataSource = host, UserID = user, Password = password, TrustServerCertificate = true, Encrypt = SqlConnectionEncryptOption.Mandatory,
            InitialCatalog = "master", ConnectTimeout = 15,
        }.ConnectionString;

    private static string NewName() => "pm_grant_test_" + Guid.NewGuid().ToString("N")[..10];

    /* A password that meets Windows policy and holds no quote, with the random part from a guid. */
    private static string NewPassword() => "Aa1!" + Guid.NewGuid().ToString("N");

    private static async Task Exec(string connectionString, string sql, System.Threading.CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task Cleanup(string adminCs, string login, System.Threading.CancellationToken ct)
    {
        /* The probes above left pooled connections open as the throwaway login, and SQL Server will not drop a login that has a session. */
        SqlConnection.ClearAllPools();
        try
        {
            await Exec(adminCs, $"USE [msdb]; IF DATABASE_PRINCIPAL_ID({Lit(login)}) IS NOT NULL DROP USER {MonitoringGrantScript.QuoteName(login)};", ct);
            await Exec(adminCs, $"IF SUSER_ID({Lit(login)}) IS NOT NULL DROP LOGIN {MonitoringGrantScript.QuoteName(login)};", ct);
        }
        catch (SqlException)
        {
            /* The login is a throwaway named for this run; a failed drop is reported by the next run's leftovers, not by hiding the test result. */
        }
    }

    [Fact]
    public async Task ScriptRunsTwice_AndTheConnectionTestNamesOnlyWhatIsMissing()
    {
        var admin = Admin();
        Assert.SkipWhen(admin is null, "Set DARLING_TEST_SQL, DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD (a sysadmin on the sql2019 dev box) to run the live grant script test.");
        var (host, user, password) = admin!.Value;
        var adminCs = Cs(host, user, password);
        var ct = TestContext.Current.CancellationToken;

        var onlyViewServerState = NewName();
        var fromScript = NewName();
        var scriptPassword = NewPassword();
        var partialPassword = NewPassword();
        try
        {
            /* A login with one of the four required grants: the check names the other three, with the statement for each. */
            await Exec(adminCs, $"CREATE LOGIN [{onlyViewServerState}] WITH PASSWORD = N'{partialPassword}', CHECK_POLICY = OFF; GRANT VIEW SERVER STATE TO [{onlyViewServerState}];", ct);
            var partial = await DarlingServerConnector.ProbeMissingGrantsAsync(
                Cs(host, onlyViewServerState, partialPassword), isAzureSqlDatabase: false, null, ct);
            Assert.NotNull(partial);
            Assert.DoesNotContain("VIEW SERVER STATE is missing", partial);
            Assert.Contains($"CONNECT ANY DATABASE is missing (GRANT CONNECT ANY DATABASE TO [{onlyViewServerState}];)", partial);
            Assert.Contains($"VIEW ANY DEFINITION is missing (GRANT VIEW ANY DEFINITION TO [{onlyViewServerState}];)", partial);
            Assert.Contains($"ALTER ANY EVENT SESSION is missing (GRANT ALTER ANY EVENT SESSION TO [{onlyViewServerState}];)", partial);

            /* The same login through the whole connection test: it still connects, and the answer carries the warning. */
            var probe = await DarlingServerConnector.ProbeAsync(
                new MonitoredServer { Name = "grant-6472", Host = host, Auth = "sql", Username = onlyViewServerState, Password = partialPassword, TrustServerCertificate = true },
                null, ct);
            Assert.True(probe.Success, probe.Error);
            Assert.Equal(partial, probe.MissingGrantsNote);

            /* The generated script, run twice: the second run creates nothing and fails nothing. */
            var script = MonitoringGrantScript
                .Build(MonitoringGrantScript.ChoosePrincipal(new GrantPrincipalInputs("sql", fromScript, host, null, "NOMACHINE", null)), finOps: false, azureSqlDatabase: false)
                .Replace("<strong password>", scriptPassword, StringComparison.Ordinal);
            await Exec(adminCs, script, ct);
            await Exec(adminCs, script, ct);

            Assert.Null(await DarlingServerConnector.ProbeMissingGrantsAsync(
                Cs(host, fromScript, scriptPassword), isAzureSqlDatabase: false, null, ct));
            var full = await DarlingServerConnector.ProbeAsync(
                new MonitoredServer { Name = "grant-6472b", Host = host, Auth = "sql", Username = fromScript, Password = scriptPassword, TrustServerCertificate = true },
                null, ct);
            Assert.True(full.Success, full.Error);
            Assert.Null(full.MissingGrantsNote);
            Assert.True(full.HasMsdbAccess);
        }
        finally
        {
            await Cleanup(adminCs, onlyViewServerState, default(System.Threading.CancellationToken));
            await Cleanup(adminCs, fromScript, default(System.Threading.CancellationToken));
        }
    }

    [Fact]
    public async Task FinOpsScript_RunsTwice_AndGrantsEachOnlineReadWriteUserDatabaseAndNothingElse()
    {
        var admin = Admin();
        Assert.SkipWhen(admin is null, "Set DARLING_TEST_SQL, DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD (a sysadmin on the sql2019 dev box) to run the live grant script test.");
        var (host, user, password) = admin!.Value;
        var adminCs = Cs(host, user, password);
        var ct = TestContext.Current.CancellationToken;

        /* A login name holding a question mark, a quote and a closing bracket, and database names holding a quote and a closing bracket:
           the cases sp_MSforeachdb broke (it replaced every question mark in the batch, and put the database name in unescaped). */
        var login = NewName() + "?'x]y";
        var suffix = Guid.NewGuid().ToString("N")[..8];
        /* Created in this order, so the held and the failing database sit BEFORE the writable one by database_id: a loop that stops at
           either gives the writable one nothing. */
        var heldDatabase = "pm_grant_held_" + suffix + "'g]h";
        var triggerDatabase = "pm_grant_trg_" + suffix + "'i]j";
        var database = "pm_grant_db_" + suffix + "'a]b";
        var readOnlyDatabase = "pm_grant_ro_" + suffix + "'c]d";
        var offlineDatabase = "pm_grant_off_" + suffix + "'e]f";
        var allDatabases = new[] { heldDatabase, triggerDatabase, database, readOnlyDatabase, offlineDatabase };
        var quotedLogin = MonitoringGrantScript.QuoteName(login);
        SqlConnection? held = null;
        try
        {
            /* Five scratch databases beside whatever the box already holds: one SINGLE_USER and held by a second session (the script must
               skip it without stopping), one whose CREATE USER fails (the script must print why and go on), one writable (it must grant in
               it), one read-only and one offline (it must leave both alone). */
            foreach (var name in allDatabases)
            {
                await Exec(adminCs, $"CREATE DATABASE {MonitoringGrantScript.QuoteName(name)};", ct);
            }

            /* The second session makes the database single-user and then takes its one slot itself: a new login would race for it. */
            held = new SqlConnection(new SqlConnectionStringBuilder(adminCs) { Pooling = false }.ConnectionString);
            await held.OpenAsync(ct);
            await using (var hold = new SqlCommand(
                $"ALTER DATABASE {MonitoringGrantScript.QuoteName(heldDatabase)} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; USE {MonitoringGrantScript.QuoteName(heldDatabase)};", held))
            {
                await hold.ExecuteNonQueryAsync(ct);
            }

            await Exec(InDatabase(adminCs, triggerDatabase),
                "CREATE TRIGGER pm_grant_block ON DATABASE FOR CREATE_USER AS THROW 50001, N'blocked by the test trigger', 1;", ct);
            await Exec(adminCs, $"ALTER DATABASE {MonitoringGrantScript.QuoteName(readOnlyDatabase)} SET READ_ONLY WITH ROLLBACK IMMEDIATE;", ct);
            await Exec(adminCs, $"ALTER DATABASE {MonitoringGrantScript.QuoteName(offlineDatabase)} SET OFFLINE WITH ROLLBACK IMMEDIATE;", ct);

            /* The databases the script is expected to grant in, read just before it runs: other test classes create and drop databases on
               this instance at the same time, so the check afterwards covers the ones that existed when the script ran. */
            var existedBefore = await Names(adminCs, WritableDatabasesSql, ct);

            var script = MonitoringGrantScript
                .Build(MonitoringGrantScript.ChoosePrincipal(new GrantPrincipalInputs("sql", login, host, null, "NOMACHINE", null)), finOps: true, azureSqlDatabase: false)
                .Replace("<strong password>", NewPassword(), StringComparison.Ordinal);
            var messages = await ExecWithMessages(adminCs, script, ct);
            await Exec(adminCs, script, ct);

            /* The failing database is named with its error, and the loop went on: the held database is filtered out, not an error. */
            Assert.Contains("Skipped " + MonitoringGrantScript.QuoteName(triggerDatabase) + ": blocked by the test trigger", messages);
            Assert.Contains("Granting in " + MonitoringGrantScript.QuoteName(database), messages);
            Assert.DoesNotContain(MonitoringGrantScript.QuoteName(heldDatabase), messages);

            /* The login exists under its exact name, and the user in the scratch database carries the same name. */
            Assert.Equal(login, await Scalar(adminCs, "master", $"SELECT sp.name FROM sys.server_principals AS sp WHERE sp.name = {Lit(login)};", ct));
            Assert.Equal(login, await Scalar(adminCs, database, $"SELECT dp.name FROM sys.database_principals AS dp WHERE dp.name = {Lit(login)};", ct));

            /* The three FinOps grants as rows in the scratch database's own permission catalog, for the user: the script's server-level
               grants already give a login the effective permissions, so reading effective permissions would pass with the block missing. */
            var scratchCs = InDatabase(adminCs, database);
            await using (var connection = new SqlConnection(scratchCs))
            {
                await connection.OpenAsync(ct);
                await using var check = new SqlCommand(
                    "SELECT " +
                    "(SELECT COUNT(*) FROM sys.database_permissions AS p JOIN sys.database_principals AS dp ON dp.principal_id = p.grantee_principal_id " +
                    "WHERE dp.name = @login AND p.class = 0 AND p.state = N'G' AND p.permission_name = N'VIEW DEFINITION'), " +
                    "(SELECT COUNT(*) FROM sys.database_permissions AS p JOIN sys.database_principals AS dp ON dp.principal_id = p.grantee_principal_id " +
                    "WHERE dp.name = @login AND p.class = 0 AND p.state = N'G' AND p.permission_name = N'VIEW DATABASE STATE'), " +
                    "(SELECT COUNT(*) FROM sys.database_permissions AS p JOIN sys.database_principals AS dp ON dp.principal_id = p.grantee_principal_id " +
                    "WHERE dp.name = @login AND p.class = 1 AND p.major_id = OBJECT_ID(N'sys.sql_expression_dependencies') AND p.minor_id = 0 " +
                    "AND p.state = N'G' AND p.permission_name = N'SELECT');", connection);
                check.Parameters.AddWithValue("@login", login);
                await using var reader = await check.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                var names = new[] { "VIEW DEFINITION", "VIEW DATABASE STATE", "SELECT on sys.sql_expression_dependencies" };
                for (var i = 0; i < names.Length; i++)
                {
                    Assert.True(!reader.IsDBNull(i) && Convert.ToInt32(reader.GetValue(i)) == 1, $"The FinOps script left no grant row for {names[i]} in the scratch database (answer: {(reader.IsDBNull(i) ? "NULL" : reader.GetValue(i))}).");
                }
            }

            /* Every database that was online, read-write and open when the script ran (and still is) holds the user, apart from the one
               made to fail. The scratch writable database must be among them. */
            var stillWritable = await Names(adminCs, WritableDatabasesSql, ct);
            Assert.Contains(database, existedBefore);
            foreach (var name in existedBefore.Where(n => stillWritable.Contains(n) && n != triggerDatabase))
            {
                Assert.True(await HasUser(adminCs, name, login, ct), $"The FinOps script left no user in the writable database {name}.");
            }

            /* No user where the script must not make one: the failing database (its CREATE USER was refused), the system databases the
               loop skips (msdb holds the user from the base grants, so it is left out), the read-only database, the offline one (brought
               back online to look) and the single-user one (looked at through the session that holds it). */
            Assert.False(await HasUser(adminCs, triggerDatabase, login, ct), $"The failing database {triggerDatabase} holds a user.");
            foreach (var name in new[] { readOnlyDatabase, "master", "model", "tempdb" })
            {
                Assert.False(await HasUser(adminCs, name, login, ct), $"The FinOps script made a user in {name}, which it must skip.");
            }

            await Exec(adminCs, $"ALTER DATABASE {MonitoringGrantScript.QuoteName(offlineDatabase)} SET ONLINE;", ct);
            Assert.False(await HasUser(adminCs, offlineDatabase, login, ct), $"The FinOps script made a user in the offline database {offlineDatabase}.");
            await using (var heldCheck = new SqlCommand($"SELECT dp.name FROM sys.database_principals AS dp WHERE dp.name = {Lit(login)};", held))
            {
                Assert.Null(await heldCheck.ExecuteScalarAsync(ct));
            }
        }
        finally
        {
            System.Threading.CancellationToken none = default;
            held?.Dispose();
            SqlConnection.ClearAllPools();

            /* Each step on its own, so a database that cannot be reached does not stop the cleanup of the rest. The user goes from every
               online read-write database that holds it, whatever made it (the script, or a half-finished run); an offline scratch
               database comes online first, or its files stay on the server's disk after the DROP. */
            System.Collections.Generic.List<string> reachable;
            try
            {
                reachable = await Names(adminCs, "SELECT d.name FROM sys.databases AS d WHERE d.database_id > 4 AND d.state = 0 AND DATABASEPROPERTYEX(d.name, N'Updateability') = N'READ_WRITE';", none);
            }
            catch (SqlException)
            {
                reachable = new System.Collections.Generic.List<string>(allDatabases);
            }

            foreach (var name in reachable)
            {
                await TryExec(adminCs, $"USE {MonitoringGrantScript.QuoteName(name)}; IF DATABASE_PRINCIPAL_ID({Lit(login)}) IS NOT NULL DROP USER {quotedLogin};", none);
            }

            foreach (var name in allDatabases)
            {
                var quoted = MonitoringGrantScript.QuoteName(name);
                await TryExec(adminCs, $"IF DB_ID({Lit(name)}) IS NOT NULL AND DATABASEPROPERTYEX({Lit(name)}, N'Status') = N'OFFLINE' ALTER DATABASE {quoted} SET ONLINE;", none);
                await TryExec(adminCs,
                    $"IF DB_ID({Lit(name)}) IS NOT NULL BEGIN IF DATABASEPROPERTYEX({Lit(name)}, N'Status') = N'ONLINE' ALTER DATABASE {quoted} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {quoted}; END;", none);
            }

            await Cleanup(adminCs, login, none);
        }
    }

    /* The databases the FinOps loop grants in: the filter in its cursor, so the snapshot and the script agree. */
    private const string WritableDatabasesSql =
        "SELECT d.name FROM sys.databases AS d WHERE d.database_id > 4 AND d.state = 0 AND DATABASEPROPERTYEX(d.name, N'Updateability') = N'READ_WRITE' " +
        "AND d.user_access <> 1 AND HAS_DBACCESS(d.name) = 1;";

    private static string InDatabase(string adminCs, string database) =>
        new SqlConnectionStringBuilder(adminCs) { InitialCatalog = database }.ConnectionString;

    private static async Task TryExec(string connectionString, string sql, System.Threading.CancellationToken ct)
    {
        try
        {
            await Exec(connectionString, sql, ct);
        }
        catch (SqlException)
        {
            /* A throwaway database or user that cannot be reached is left for the next run's leftovers, not hidden behind the test result. */
        }
    }

    /* Runs a batch and returns every message it printed (PRINT and low-severity errors). */
    private static async Task<string> ExecWithMessages(string connectionString, string sql, System.Threading.CancellationToken ct)
    {
        var messages = new StringBuilder();
        await using var connection = new SqlConnection(connectionString);
        connection.InfoMessage += (_, e) => messages.AppendLine(e.Message);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
        return messages.ToString();
    }

    private static string Lit(string value) => "N'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";

    private static async Task<string?> Scalar(string adminCs, string database, string sql, System.Threading.CancellationToken ct)
    {
        await using var connection = new SqlConnection(new SqlConnectionStringBuilder(adminCs) { InitialCatalog = database }.ConnectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection);
        return (await command.ExecuteScalarAsync(ct)) as string;
    }

    private static async Task<System.Collections.Generic.List<string>> Names(string adminCs, string sql, System.Threading.CancellationToken ct)
    {
        var names = new System.Collections.Generic.List<string>();
        await using var connection = new SqlConnection(adminCs);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private static async Task<bool> HasUser(string adminCs, string database, string login, System.Threading.CancellationToken ct) =>
        await Scalar(adminCs, database, $"SELECT dp.name FROM sys.database_principals AS dp WHERE dp.name = {Lit(login)};", ct) is not null;
}
