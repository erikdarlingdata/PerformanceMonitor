/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
            await Exec(adminCs, $"USE [msdb]; IF DATABASE_PRINCIPAL_ID(N'{login}') IS NOT NULL DROP USER [{login}];", ct);
            await Exec(adminCs, $"IF SUSER_ID(N'{login}') IS NOT NULL DROP LOGIN [{login}];", ct);
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
    public async Task FinOpsScript_RunsTwice_AndGrantsEachUserDatabase()
    {
        var admin = Admin();
        Assert.SkipWhen(admin is null, "Set DARLING_TEST_SQL, DARLING_TEST_SQL_USER and DARLING_TEST_SQL_PASSWORD (a sysadmin on the sql2019 dev box) to run the live grant script test.");
        var (host, user, password) = admin!.Value;
        var adminCs = Cs(host, user, password);
        var ct = TestContext.Current.CancellationToken;

        var login = NewName();
        var database = "pm_grant_db_" + Guid.NewGuid().ToString("N")[..8];
        try
        {
            /* One scratch database, so the FinOps loop has a writable user database to grant in without touching the box's own databases' users:
               the loop reaches every user database, so the cleanup below drops the login's user from each. */
            await Exec(adminCs, $"CREATE DATABASE [{database}];", ct);
            var script = MonitoringGrantScript
                .Build(MonitoringGrantScript.ChoosePrincipal(new GrantPrincipalInputs("sql", login, host, null, "NOMACHINE", null)), finOps: true, azureSqlDatabase: false)
                .Replace("<strong password>", NewPassword(), StringComparison.Ordinal);
            await Exec(adminCs, script, ct);
            await Exec(adminCs, script, ct);

            /* The login holds the FinOps grants in the scratch database (an effective-permission check as the login itself). */
            /* All three FinOps grants, checked from inside the scratch database so each securable resolves (an object name in another
               database answers NULL, which says nothing). A NULL fails with its position, not as a cast error. */
            var scratchCs = new SqlConnectionStringBuilder(adminCs) { InitialCatalog = database }.ConnectionString;
            await using var connection = new SqlConnection(scratchCs);
            await connection.OpenAsync(ct);
            await using var check = new SqlCommand(
                $"EXECUTE AS LOGIN = N'{login}'; SELECT HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DEFINITION'), " +
                "HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DATABASE STATE'), " +
                "HAS_PERMS_BY_NAME(N'sys.sql_expression_dependencies', N'OBJECT', N'SELECT'); REVERT;", connection);
            await using var reader = await check.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            var names = new[] { "VIEW DEFINITION", "VIEW DATABASE STATE", "SELECT on sys.sql_expression_dependencies" };
            for (var i = 0; i < names.Length; i++)
            {
                Assert.True(!reader.IsDBNull(i) && Convert.ToInt32(reader.GetValue(i)) == 1, $"The FinOps script left {names[i]} ungranted in the scratch database (answer: {(reader.IsDBNull(i) ? "NULL" : reader.GetValue(i))}).");
            }
        }
        finally
        {
            System.Threading.CancellationToken none = default;
            try
            {
                await Exec(adminCs, $"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}]; END;", none);
                /* The loop made the login's user in every writable user database of the box; remove each before the login goes. */
                await Exec(adminCs,
                    $"EXECUTE sys.sp_MSforeachdb N'USE [?]; IF DB_ID() > 4 AND DATABASE_PRINCIPAL_ID(N''{login}'') IS NOT NULL AND DATABASEPROPERTYEX(DB_NAME(), ''Updateability'') = ''READ_WRITE'' DROP USER [{login}];';", none);
            }
            catch (SqlException)
            {
            }

            await Cleanup(adminCs, login, none);
        }
    }
}
