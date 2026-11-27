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
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6472: the monitoring login and grant script (<see cref="MonitoringGrantScript"/>), the <c>--grant-script</c> verb that prints it,
/// and the missing-grant warning the connection test adds. Pure: no SQL Server. The live half is MonitoringGrantScriptLiveSqlTests.
/// </summary>
public sealed class MonitoringGrantScriptTests
{
    private static GrantPrincipalInputs Inputs(
        string? auth = "integrated", string? username = null, string host = "sql01", string? serviceAccount = null,
        string machine = "DARLING01", string? domain = "CORP") =>
        new(auth, username, host, serviceAccount, machine, domain);

    [Fact]
    public void SqlAuth_IsTheNamedLogin_WithAPasswordPlaceholder_AndNeverARealPassword()
    {
        var principal = MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor"));
        var script = MonitoringGrantScript.Build(principal, finOps: false, azureSqlDatabase: false);

        Assert.Equal(GrantPrincipalKind.SqlLogin, principal.Kind);
        Assert.Equal("pm_monitor", principal.Name);
        Assert.Contains("CREATE LOGIN [pm_monitor] WITH PASSWORD = N'<strong password>'", script);
        Assert.Contains("GRANT VIEW SERVER STATE TO [pm_monitor];", script);
        Assert.Contains("GRANT CONNECT ANY DATABASE TO [pm_monitor];", script);
        Assert.Contains("GRANT VIEW ANY DEFINITION TO [pm_monitor];", script);
        Assert.Contains("GRANT ALTER ANY EVENT SESSION TO [pm_monitor];", script);
        Assert.Contains("GRANT ALTER TRACE TO [pm_monitor];", script);
        Assert.Contains("GRANT ALTER SETTINGS TO [pm_monitor];", script);
        Assert.Contains("USE [msdb];", script);
        Assert.Contains("CREATE USER [pm_monitor] FOR LOGIN [pm_monitor];", script);
        Assert.Contains("GRANT SELECT ON dbo.sysjobs TO [pm_monitor];", script);
        Assert.Contains("GRANT EXECUTE ON dbo.agent_datetime TO [pm_monitor];", script);
    }

    [Fact]
    public void SqlAuthWithNoUsername_UsesAPlaceholderLoginAndSaysSo()
    {
        var principal = MonitoringGrantScript.ChoosePrincipal(Inputs("sql", null));

        Assert.Equal(MonitoringGrantScript.PlaceholderLogin, principal.Name);
        Assert.Contains(principal.Notes, n => n.Contains("names no SQL login", StringComparison.Ordinal));
    }

    [Fact]
    public void WindowsAuth_OnTheSameMachine_IsTheServiceVirtualAccount()
    {
        foreach (var host in new[] { "darling01", "DARLING01.corp.local", "localhost", ".", "(local)", @"darling01\INST1", "tcp:darling01,1433" })
        {
            var principal = MonitoringGrantScript.ChoosePrincipal(Inputs(host: host));

            Assert.Equal(GrantPrincipalKind.WindowsLogin, principal.Kind);
            Assert.Equal(@"NT SERVICE\PerformanceMonitor Darling", principal.Name);
        }

        var script = MonitoringGrantScript.Build(
            MonitoringGrantScript.ChoosePrincipal(Inputs(host: "darling01")), finOps: false, azureSqlDatabase: false);
        Assert.Contains(@"CREATE LOGIN [NT SERVICE\PerformanceMonitor Darling] FROM WINDOWS;", script);
        Assert.Contains(@"GRANT VIEW SERVER STATE TO [NT SERVICE\PerformanceMonitor Darling];", script);
    }

    [Fact]
    public void WindowsAuth_OnAnotherMachine_IsTheMachineAccount_AndTheChoicesAreListed()
    {
        var principal = MonitoringGrantScript.ChoosePrincipal(Inputs(host: "sql01"));

        Assert.Equal(@"CORP\DARLING01$", principal.Name);
        Assert.Contains(principal.Notes, n => n.Contains(@"NT SERVICE\PerformanceMonitor Darling", StringComparison.Ordinal));
        Assert.Contains(principal.Notes, n => n.Contains("gmsa$", StringComparison.Ordinal));
    }

    [Fact]
    public void WindowsAuth_OnAWorkgroupMachine_LeavesTheDomainAPlaceholder()
    {
        /* The user domain of a machine outside any domain is the machine name: no domain to put in front of the account. */
        var principal = MonitoringGrantScript.ChoosePrincipal(Inputs(host: "sql01", domain: "DARLING01"));

        Assert.Equal(@"DOMAIN\DARLING01$", principal.Name);
    }

    [Theory]
    [InlineData(@"CORP\gmsa-darling$", "gMSA")]
    [InlineData(@"CORP\svc-darling", "account")]
    public void WhenTheServiceRunsAsADomainAccountOrAGmsa_ThatIsThePrincipal_OnAnyMachine(string account, string kind)
    {
        foreach (var host in new[] { "darling01", "sql01" })
        {
            var principal = MonitoringGrantScript.ChoosePrincipal(Inputs(host: host, serviceAccount: account));

            Assert.Equal(account, principal.Name);
            Assert.Contains(principal.Notes, n => n.Contains(kind, StringComparison.Ordinal));
        }
    }

    [Theory]
    [InlineData("darling01")]
    [InlineData("sql01")]
    public void WhenTheServiceIsNotOnThisMachine_WindowsAuthNamesAPlaceholder_NotTheCallersMachineAccount(string host)
    {
        /* The Viewer on a workstation: the service's registry key is absent, so its account is unknown. The old script named the
           workstation's own account (CORP\DARLING01$) or the virtual account as fact. */
        var principal = MonitoringGrantScript.ChoosePrincipal(new GrantPrincipalInputs("integrated", null, host, null, "DARLING01", "CORP", ServiceNotOnThisMachine: true));

        Assert.Equal(MonitoringGrantScript.PlaceholderServiceAccount, principal.Name);
        Assert.True(principal.IsPlaceholder);
        Assert.DoesNotContain("DARLING01", principal.Name, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(principal.Notes, n => n.Contains("PLACEHOLDER", StringComparison.Ordinal));
        Assert.Contains(principal.Notes, n => n.Contains("--grant-script --host", StringComparison.Ordinal));
    }

    [Fact]
    public void WhenTheServiceAccountWasRead_ItWinsOverTheNotOnThisMachineFlag_AndIsNotAPlaceholder()
    {
        var principal = MonitoringGrantScript.ChoosePrincipal(new GrantPrincipalInputs("integrated", null, "sql01", @"CORP\gmsa-darling$", "WORKSTATION", "CORP", ServiceNotOnThisMachine: true));

        Assert.Equal(@"CORP\gmsa-darling$", principal.Name);
        Assert.False(principal.IsPlaceholder);
    }

    [Fact]
    public void TheNotOnThisMachineFlag_DoesNotChangeSqlOrEntraPrincipals()
    {
        var sql = MonitoringGrantScript.ChoosePrincipal(new GrantPrincipalInputs("sql", "pm_monitor", "sql01", null, "WORKSTATION", "CORP", ServiceNotOnThisMachine: true));
        var entra = MonitoringGrantScript.ChoosePrincipal(new GrantPrincipalInputs("managedidentity", null, "x.database.windows.net", null, "WORKSTATION", "CORP", ServiceNotOnThisMachine: true));

        Assert.Equal("pm_monitor", sql.Name);
        Assert.False(sql.IsPlaceholder);
        Assert.Equal("your-managed-identity-name", entra.Name);
    }

    [Fact]
    public void ThePlaceholderScript_QuotesThePlaceholderAndKeepsTheNotesInsideTheHeaderComment()
    {
        var principal = MonitoringGrantScript.ChoosePrincipal(new GrantPrincipalInputs("integrated", null, "sql01", null, "WORKSTATION", "CORP", ServiceNotOnThisMachine: true));
        var script = MonitoringGrantScript.Build(principal, finOps: false, azureSqlDatabase: false);

        Assert.Contains("[" + MonitoringGrantScript.PlaceholderServiceAccount + "]", script, StringComparison.Ordinal);
        Assert.DoesNotContain("WORKSTATION", script, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData(@"NT SERVICE\PerformanceMonitor Darling")]
    [InlineData("LocalSystem")]
    public void WhenTheServiceRunsAsItsVirtualAccount_TheMachineDecidesLikeNoAccountWasRead(string account)
    {
        Assert.Equal(@"NT SERVICE\PerformanceMonitor Darling", MonitoringGrantScript.ChoosePrincipal(Inputs(host: "darling01", serviceAccount: account)).Name);
        Assert.Equal(@"CORP\DARLING01$", MonitoringGrantScript.ChoosePrincipal(Inputs(host: "sql01", serviceAccount: account)).Name);
    }

    [Theory]
    [InlineData("managedidentity", "your-managed-identity-name")]
    [InlineData("serviceprincipal", "your-app-registration-name")]
    public void EntraAuth_IsAnExternalProviderUser_InTheAzureScript(string auth, string placeholder)
    {
        var principal = MonitoringGrantScript.ChoosePrincipal(Inputs(auth, host: "x.database.windows.net"));
        var script = MonitoringGrantScript.Build(principal, finOps: false, azureSqlDatabase: true);

        Assert.Equal(GrantPrincipalKind.ExternalProvider, principal.Kind);
        Assert.Contains($"CREATE USER [{placeholder}] FROM EXTERNAL PROVIDER;", script);
        Assert.Contains($"GRANT VIEW DATABASE STATE TO [{placeholder}];", script);
        Assert.Contains($"GRANT VIEW DEFINITION TO [{placeholder}];", script);
        Assert.Contains($"GRANT ALTER ANY DATABASE EVENT SESSION TO [{placeholder}];", script);
    }

    [Fact]
    public void AzureSqlDatabase_SqlLogin_IsAContainedUser_WithNoServerGrantsOrMaster()
    {
        var script = MonitoringGrantScript.Build(
            MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor", "x.database.windows.net")), finOps: false, azureSqlDatabase: true);

        Assert.Contains("CREATE USER [pm_monitor] WITH PASSWORD = N'<strong password>';", script);
        Assert.DoesNotContain("CREATE LOGIN", script);
        Assert.DoesNotContain("USE [master]", script);
        Assert.DoesNotContain("VIEW SERVER STATE", script);
        Assert.DoesNotContain("msdb", script);
        Assert.DoesNotContain("sql_expression_dependencies", script);
    }

    [Fact]
    public void AzureSqlDatabase_WithFinOps_AddsTheOneGrantTheBaseSetLacks()
    {
        var script = MonitoringGrantScript.Build(
            MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor", "x.database.windows.net")), finOps: true, azureSqlDatabase: true);

        Assert.Contains("GRANT SELECT ON sys.sql_expression_dependencies TO [pm_monitor];", script);
        Assert.Equal(1, script.Split("GRANT VIEW DATABASE STATE", StringSplitOptions.None).Length - 1);   /* granted once, not twice */
    }

    [Fact]
    public void OnPremises_WithFinOps_GrantsEachUserDatabase_AndWithoutItDoesNot()
    {
        var principal = MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor"));
        var with = MonitoringGrantScript.Build(principal, finOps: true, azureSqlDatabase: false);
        var without = MonitoringGrantScript.Build(principal, finOps: false, azureSqlDatabase: false);

        /* A cursor over sys.databases, with the login handed to each database as a parameter and quoted there. */
        Assert.DoesNotContain("sp_MSforeach", with);
        Assert.Contains("CURSOR LOCAL FAST_FORWARD", with);
        Assert.Contains("FROM sys.databases AS d", with);
        Assert.Contains("d.state = 0", with);
        Assert.Contains("DATABASEPROPERTYEX(d.name, N'Updateability') = N'READ_WRITE'", with);
        Assert.Contains("@login sysname = N'pm_monitor'", with);
        Assert.Contains("N'@login sysname',", with);
        Assert.Contains("@login = @login;", with);
        Assert.Contains("N''GRANT VIEW DATABASE STATE TO '' + QUOTENAME(@login)", with);
        Assert.Contains("N''GRANT VIEW DEFINITION TO '' + QUOTENAME(@login)", with);
        Assert.Contains("N''GRANT SELECT ON sys.sql_expression_dependencies TO '' + QUOTENAME(@login)", with);
        Assert.DoesNotContain("TO [pm_monitor]", with[with.IndexOf("FinOps Index Analysis", StringComparison.Ordinal)..]);   /* no name in the per-database text */
        Assert.DoesNotContain("database_cursor", without);
        Assert.DoesNotContain("sql_expression_dependencies", without);
    }

    [Fact]
    public void ObjectCreation_IsGuarded_SoTheScriptRunsTwice()
    {
        var script = MonitoringGrantScript.Build(
            MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor")), finOps: true, azureSqlDatabase: false);

        Assert.Contains("sys.server_principals", script);
        Assert.Contains("sys.database_principals", script);
        /* The login, the msdb user and the FinOps user: three guards, and the live test runs the whole script twice. */
        Assert.Equal(3, script.Split("IF NOT EXISTS", StringSplitOptions.None).Length - 1);
    }

    [Fact]
    public void ANameWithQuotesAndBrackets_IsEscapedInEveryPlaceItAppears()
    {
        var principal = new GrantPrincipal(GrantPrincipalKind.SqlLogin, "o'b]d", Array.Empty<string>());
        var script = MonitoringGrantScript.Build(principal, finOps: true, azureSqlDatabase: false);

        Assert.Contains("[o'b]]d]", script);
        Assert.Contains("N'o''b]d'", script);
        Assert.Contains("@login sysname = N'o''b]d'", script);   /* the FinOps cursor takes the name as one literal, doubled once */
        Assert.DoesNotContain("N''o''''b]d''", script);
    }

    [Theory]
    [InlineData("\n")]
    [InlineData("\r\n")]
    public void ANameWithANewline_StaysInsideEveryBracketAndLiteralItIsWrittenIn(string newline)
    {
        /* The name is "x", a newline, then text that would be a statement if the newline ended the line it sits in. It is one identifier
           and one string literal in each place; the only other place a name is written is a block comment, which flattens the newline. */
        var name = "x" + newline + "DROP LOGIN sa;--";
        var principal = new GrantPrincipal(GrantPrincipalKind.SqlLogin, name, Array.Empty<string>());
        var script = MonitoringGrantScript.Build(principal, finOps: true, azureSqlDatabase: false, serverLabel: name);

        Assert.Contains("CREATE LOGIN [" + name + "] WITH PASSWORD", script);
        Assert.Contains("GRANT VIEW SERVER STATE TO [" + name + "];", script);
        Assert.Contains("@login sysname = N'" + name + "',", script);
        Assert.DoesNotContain("\nDROP LOGIN sa;--\n", script);   /* never alone on a line outside the brackets and the literal */
        Assert.DoesNotContain("/* Performance Monitor Darling: the login and grants for x" + newline, script);
    }

    [Theory]
    [InlineData("contoso.database.windows.net", true)]
    [InlineData("tcp:contoso.database.windows.net,1433", true)]
    [InlineData("sql01", false)]
    [InlineData("database.windows.net.example.com", false)]
    [InlineData("mi01.abc123def.database.windows.net", false)]             /* a Managed Instance works like on-premises */
    [InlineData("mi01.public.abc123def.database.windows.net,3342", false)] /* its public endpoint */
    public void AzureSqlDatabaseHost_IsRecognised(string host, bool expected) =>
        Assert.Equal(expected, MonitoringGrantScript.IsAzureSqlDatabaseHost(host));

    [Theory]
    [InlineData("DARLING01", true)]
    [InlineData("darling01,1433", true)]
    [InlineData(@"darling01\SQL2", true)]
    [InlineData("darling010", false)]
    [InlineData("other.darling01.example", false)]
    [InlineData("", false)]
    public void SameMachine_ComparesTheHostNotASubstring(string host, bool expected) =>
        Assert.Equal(expected, MonitoringGrantScript.IsSameMachine(host, "DARLING01"));

    [Fact]
    public void PermissionCheck_NamesWhoConnected_AndReadsOnlyExactlyZeroAsMissing()
    {
        var entries = MonitoringGrantScript.RequiredServerGrants;
        var sql = MonitoringGrantScript.BuildPermissionCheckSql(entries);

        Assert.Contains("HAS_PERMS_BY_NAME(NULL, NULL, N'VIEW SERVER STATE') AS g0", sql);
        Assert.Contains("SUSER_SNAME() AS who", sql);

        /* NULL is "this version does not know the name", which is no claim; 1 is held; only 0 is missing. */
        var missing = MonitoringGrantScript.MissingGrants(entries, new int?[] { 0, 1, null, 0 }, "pm_monitor");
        Assert.Equal(new[] { "VIEW SERVER STATE", "ALTER ANY EVENT SESSION" }, missing.Select(m => m.Entry.Permission).ToArray());
        Assert.Equal("GRANT VIEW SERVER STATE TO [pm_monitor];", missing[0].Statement);
        Assert.Empty(MonitoringGrantScript.MissingGrants(entries, new int?[] { 1, 1, 1, 1 }, "pm_monitor"));
        Assert.Null(MonitoringGrantScript.DescribeMissingGrants(Array.Empty<(GrantEntry, string)>()));
    }

    /* #6889: Amazon RDS for SQL Server has no sysadmin and refuses CONNECT ANY DATABASE, ALTER SETTINGS and eight msdb grants to its
       master user. The RDS form leaves those out and gives the login a user in each user database instead. */
    private static string RdsScript(bool finOps = false) => MonitoringGrantScript.Build(
        MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor")), finOps, azureSqlDatabase: false, serverLabel: "db1.abc.us-east-1.rds.amazonaws.com", awsRds: true);

    private static string[] StatementLines(string script) =>
        script.Split('\n').Select(l => l.Trim()).Where(l => l.StartsWith("GRANT ", StringComparison.Ordinal)).ToArray();

    [Fact]
    public void Rds_LeavesOutTheTwoServerGrantsRdsRefuses_AndSaysWhy()
    {
        var script = RdsScript();
        var grants = StatementLines(script);

        Assert.DoesNotContain(grants, g => g.Contains("CONNECT ANY DATABASE", StringComparison.Ordinal));
        Assert.DoesNotContain(grants, g => g.Contains("ALTER SETTINGS", StringComparison.Ordinal));
        Assert.Contains("GRANT VIEW SERVER STATE TO [pm_monitor];", grants);
        Assert.Contains("GRANT VIEW ANY DEFINITION TO [pm_monitor];", grants);
        Assert.Contains("GRANT ALTER ANY EVENT SESSION TO [pm_monitor];", grants);
        Assert.Contains("GRANT ALTER TRACE TO [pm_monitor];", grants);
        Assert.Contains("RDS refuses the CONNECT ANY DATABASE grant", script);
        Assert.Contains("RDS refuses the ALTER SETTINGS grant", script);
        Assert.Contains("the service does not set 'blocked process threshold (s)'", script);

        /* The same call without the RDS form still grants both. */
        var onPremises = StatementLines(MonitoringGrantScript.Build(
            MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor")), finOps: false, azureSqlDatabase: false));
        Assert.Contains("GRANT CONNECT ANY DATABASE TO [pm_monitor];", onPremises);
        Assert.Contains("GRANT ALTER SETTINGS TO [pm_monitor];", onPremises);
    }

    [Fact]
    public void Rds_Msdb_KeepsTheUserAndTheThreeJobReadsRdsAccepts_AndNothingElse()
    {
        var script = RdsScript();
        var msdb = script[script.IndexOf("/* Optional: SQL Agent job history.", StringComparison.Ordinal)..script.LastIndexOf("USE [master];", StringComparison.Ordinal)];

        Assert.Contains("CREATE USER [pm_monitor] FOR LOGIN [pm_monitor];", msdb);
        Assert.Equal(
            new[]
            {
                "GRANT SELECT ON dbo.sysjobs TO [pm_monitor];",
                "GRANT SELECT ON dbo.sysjobactivity TO [pm_monitor];",
                "GRANT SELECT ON dbo.sysjobhistory TO [pm_monitor];",
            },
            StatementLines(msdb));
        /* #6882: the backup history grant is the eighth RDS refuses, so no GRANT names backupset (the list above has none) and the
           comment says the backups and CHECKDB view still fills without it. */
        Assert.DoesNotContain("GRANT SELECT ON dbo.backupset", msdb);
        Assert.Contains("can read msdb.dbo.backupset without it", msdb);
        Assert.Contains("Running Jobs and Agent Status do not collect on RDS", msdb);
    }

    [Fact]
    public void Rds_AddsAUserToEachUserDatabase_InTheFinOpsCursorForm_SkippingRdsadmin_AndSafeToRunTwice()
    {
        var script = RdsScript();

        Assert.DoesNotContain("sp_MSforeach", script);
        Assert.Contains("CURSOR LOCAL FAST_FORWARD", script);
        Assert.Contains("WHERE d.database_id > 4", script);
        Assert.Contains("AND   d.name <> N'rdsadmin'", script);
        Assert.Contains("DATABASEPROPERTYEX(d.name, N'Updateability') = N'READ_WRITE'", script);
        Assert.Contains("@login sysname = N'pm_monitor'", script);
        Assert.Contains("N'CREATE USER ' + QUOTENAME(@login) + N' FOR LOGIN ' + QUOTENAME(@login)", script.Replace("''", "'", StringComparison.Ordinal));
        Assert.Contains("WHERE dp.name = @login", script);   /* the per-database user is created only when missing */
        Assert.Contains("A database created later needs the same CREATE USER", script);

        /* Without FinOps the block adds the user and no FinOps grant. */
        Assert.DoesNotContain("GRANT VIEW DATABASE STATE TO", script);
        Assert.DoesNotContain("sql_expression_dependencies", script);

        /* With FinOps the one block carries both: one cursor, the user, then the FinOps grants. */
        var withFinOps = RdsScript(finOps: true);
        Assert.Equal(1, withFinOps.Split("CURSOR LOCAL FAST_FORWARD", StringSplitOptions.None).Length - 1);
        Assert.Contains("d.name <> N'rdsadmin'", withFinOps);
        Assert.Contains("GRANT VIEW DATABASE STATE TO ", withFinOps);

        /* The login, the msdb user and the per-database user: three guards. */
        Assert.Equal(3, script.Split("IF NOT EXISTS", StringSplitOptions.None).Length - 1);
    }

    [Fact]
    public void Rds_HeaderTellsTheOperatorToUseTheMasterUser_NotASysadmin()
    {
        var header = RdsScript().Split("*/", 2, StringSplitOptions.None)[0];

        Assert.Contains("run it as the RDS master user", header);
        Assert.DoesNotContain("as a sysadmin", header);

        var onPremises = MonitoringGrantScript.Build(
            MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor")), finOps: false, azureSqlDatabase: false)
            .Split("*/", 2, StringSplitOptions.None)[0];
        Assert.Contains("as a sysadmin", onPremises);
    }

    [Fact]
    public void Rds_DoesNotChangeTheAzureSqlDatabaseScript()
    {
        var principal = MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor"));
        Assert.Equal(
            MonitoringGrantScript.Build(principal, finOps: true, azureSqlDatabase: true),
            MonitoringGrantScript.Build(principal, finOps: true, azureSqlDatabase: true, awsRds: true));
    }

    [Theory]
    [InlineData("db1.cabcdef.us-east-1.rds.amazonaws.com", true)]
    [InlineData("DB1.CABCDEF.US-EAST-1.RDS.AMAZONAWS.COM,1433", true)]
    [InlineData("tcp:db1.cabcdef.us-east-1.rds.amazonaws.com,1433", true)]
    [InlineData("db1.cabcdef.cn-north-1.rds.amazonaws.com.cn", true)]
    [InlineData("sql01", false)]
    [InlineData("rds.amazonaws.com.example.com", false)]
    [InlineData("contoso.database.windows.net", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void IsAwsRdsHost_RecognizesAnRdsEndpoint(string? host, bool expected) =>
        Assert.Equal(expected, MonitoringGrantScript.IsAwsRdsHost(host));

    [Fact]
    public void RdsPermissionCheck_DoesNotAskForTheGrantRdsRefuses()
    {
        var rds = MonitoringGrantScript.RequiredRdsServerGrants.Select(e => e.Permission).ToArray();

        Assert.Equal(new[] { "VIEW SERVER STATE", "VIEW ANY DEFINITION", "ALTER ANY EVENT SESSION" }, rds);
        Assert.DoesNotContain("CONNECT ANY DATABASE", MonitoringGrantScript.BuildPermissionCheckSql(MonitoringGrantScript.RequiredRdsServerGrants));
        Assert.Contains("CONNECT ANY DATABASE", MonitoringGrantScript.BuildPermissionCheckSql(MonitoringGrantScript.RequiredServerGrants));
    }

    [Theory]
    [InlineData("--host db1.abc.us-east-1.rds.amazonaws.com --auth sql --username pm", true)]
    [InlineData("--host sql01 --rds", true)]
    [InlineData("sql01 --rds --finops", true)]
    [InlineData("--host sql01 --rds --azure", true)]
    public void ArgumentParsing_TakesTheRdsFlag(string line, bool ok)
    {
        Assert.Equal(ok, DarlingCliCommands.TryParseGrantScriptArgs(
            line.Split(' ', StringSplitOptions.RemoveEmptyEntries), out var parsed, out _));
        Assert.Equal(line.Contains("--rds", StringComparison.Ordinal), parsed!.Rds);
    }

    [Fact]
    public async Task Verb_ForAnRdsFlagOrAnRdsHost_PrintsTheRdsForm_AndAnOtherHostKeepsTheSysadminForm()
    {
        async Task<string> Run(params string[] args)
        {
            var output = new StringWriter();
            var code = await DarlingCliCommands.GrantScriptAsync(
                args, output, new StringWriter(), TestContext.Current.CancellationToken, readServiceAccount: () => null, machineName: "DARLING01", domainName: "CORP");
            Assert.Equal(0, code);
            return output.ToString();
        }

        var byFlag = await Run("--host", "sql01", "--auth", "sql", "--username", "pm_monitor", "--rds");
        var byHost = await Run("--host", "db1.abc.us-east-1.rds.amazonaws.com", "--auth", "sql", "--username", "pm_monitor");
        var plain = await Run("--host", "sql01", "--auth", "sql", "--username", "pm_monitor");

        foreach (var rds in new[] { byFlag, byHost })
        {
            Assert.DoesNotContain("GRANT CONNECT ANY DATABASE", rds);
            Assert.DoesNotContain("GRANT ALTER SETTINGS", rds);
            Assert.Contains("run it as the RDS master user", rds);
            Assert.Contains("d.name <> N'rdsadmin'", rds);
        }

        Assert.Contains("GRANT CONNECT ANY DATABASE TO [pm_monitor];", plain);
        Assert.DoesNotContain("rdsadmin", plain);
        Assert.Contains("--rds", DarlingCliCommands.GrantScriptUsageText());
    }

    [Fact]
    public void AzureSqlDatabase_ChecksTheDatabaseGrants_NotTheServerOnes()
    {
        var sql = MonitoringGrantScript.BuildPermissionCheckSql(MonitoringGrantScript.RequiredAzureGrants);

        Assert.Contains("HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DATABASE STATE')", sql);
        Assert.Contains("USER_NAME() AS who", sql);
        Assert.DoesNotContain("VIEW SERVER STATE", sql);
    }

    [Fact]
    public void MissingGrantsWarning_NamesEachGrantWithItsStatement()
    {
        var missing = MonitoringGrantScript.MissingGrants(
            MonitoringGrantScript.RequiredServerGrants, new int?[] { 0, 0, 1, 1 }, "pm_monitor");
        var text = MonitoringGrantScript.DescribeMissingGrants(missing);

        Assert.NotNull(text);
        Assert.Contains("VIEW SERVER STATE is missing (GRANT VIEW SERVER STATE TO [pm_monitor];)", text);
        Assert.Contains("CONNECT ANY DATABASE is missing (GRANT CONNECT ANY DATABASE TO [pm_monitor];)", text);
        Assert.DoesNotContain("VIEW ANY DEFINITION", text);
    }

    [Fact]
    public void ConnectionTestLine_ShowsAMissingGrantAsAWarning_NotAFailure()
    {
        var ok = new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null);
        var warned = ok with { MissingGrantsNote = "Grants missing for the connecting login: VIEW SERVER STATE is missing (GRANT VIEW SERVER STATE TO [x];)." };

        var plain = DarlingCliCommands.FormatProbeLine("sql01", ok);
        var line = DarlingCliCommands.FormatProbeLine("sql01", warned);

        Assert.DoesNotContain("[WARN]", plain);
        Assert.StartsWith("  [PASS] sql01:", line);
        Assert.Contains("  [WARN] sql01: Grants missing for the connecting login: VIEW SERVER STATE is missing", line);
        Assert.StartsWith(plain, line);
    }

    [Fact]
    public void MissingGrantsNote_TravelsThroughTheServiceReply_AndTheViewerReadsIt()
    {
        var probe = new ConnectionProbeResult(true, 15, 3, "Enterprise", false, false, false, true, null, MissingGrantsNote: "VIEW SERVER STATE is missing (GRANT VIEW SERVER STATE TO [x];)");

        var (_, json) = DarlingCommandExecutor.MapProbeResult(probe);
        var parsed = DarlingServiceConnectionTest.ParseReply("connected", json);
        using var doc = JsonDocument.Parse(json);

        Assert.Equal(probe.MissingGrantsNote, parsed.MissingGrantsNote);
        Assert.Equal(probe.MissingGrantsNote, ViewerDataService.ProbeMissingGrantsNote(doc.RootElement));

        var clean = DarlingCommandExecutor.MapProbeResult(probe with { MissingGrantsNote = null }).ResultJson;
        using var cleanDoc = JsonDocument.Parse(clean);
        Assert.Null(DarlingServiceConnectionTest.ParseReply("connected", clean).MissingGrantsNote);
        Assert.Null(ViewerDataService.ProbeMissingGrantsNote(cleanDoc.RootElement));
    }

    [Theory]
    [InlineData("--host sql01", true)]
    [InlineData("sql01", true)]
    [InlineData("sql01 --finops --azure --config c.json", true)]
    [InlineData("--host sql01 --auth sql --username pm", true)]
    [InlineData("", false)]
    [InlineData("sql01 --host sql02", false)]
    [InlineData("sql01 sql02", false)]
    [InlineData("--host sql01 --auth bogus", false)]
    [InlineData("sql01 --username pm", false)]
    [InlineData("--host sql01 --nope", false)]
    [InlineData("--host", false)]
    public void ArgumentParsing_IsStrict(string line, bool ok) =>
        Assert.Equal(ok, DarlingCliCommands.TryParseGrantScriptArgs(
            line.Split(' ', StringSplitOptions.RemoveEmptyEntries), out _, out _));

    /* --add-server takes "windows", --grant-script took only "integrated": the same word now works on both. The registry stores
       "integrated", and the principal rules compare against that. */
    [Theory]
    [InlineData("windows", "integrated")]
    [InlineData("Windows", "integrated")]
    [InlineData("integrated", "integrated")]
    [InlineData("INTEGRATED", "integrated")]
    [InlineData("sql", "sql")]
    public void TheAuthWords_WindowsAndIntegrated_AreTheSameOnTheGrantScriptVerb(string word, string expected)
    {
        Assert.True(DarlingCliCommands.TryParseGrantScriptArgs(new[] { "--host", "sql01", "--auth", word }, out var parsed, out var error));
        Assert.Null(error);
        Assert.Equal(expected, parsed!.Auth);
    }

    [Fact]
    public async Task Verb_ForAHostNotYetAdded_PrintsTheScript_AndConnectsToNothing()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var code = await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "sql01", "--auth", "sql", "--username", "pm_monitor", "--finops" },
            output, error, TestContext.Current.CancellationToken, readServiceAccount: () => null, machineName: "DARLING01", domainName: "CORP");

        Assert.Equal(0, code);
        Assert.Equal("", error.ToString());
        Assert.Contains("CREATE LOGIN [pm_monitor] WITH PASSWORD = N'<strong password>'", output.ToString());
        Assert.Contains("database_cursor", output.ToString());
        Assert.DoesNotContain("sp_MSforeach", output.ToString());
        Assert.DoesNotContain("encryptedPassword", output.ToString());
    }

    [Fact]
    public async Task Verb_ForAnAzureHost_PrintsTheAzureForm_WithoutTheAzureFlag()
    {
        var output = new StringWriter();

        var code = await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "contoso.database.windows.net", "--auth", "managedidentity" },
            output, new StringWriter(), TestContext.Current.CancellationToken, readServiceAccount: () => null, machineName: "DARLING01", domainName: "CORP");

        Assert.Equal(0, code);
        Assert.Contains("FROM EXTERNAL PROVIDER", output.ToString());
        Assert.DoesNotContain("USE [master]", output.ToString());
    }

    [Fact]
    public async Task Verb_ForAManagedInstanceHost_PrintsTheServerLoginForm_WithAnExternalProviderLogin()
    {
        var output = new StringWriter();

        var code = await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "mi01.abc123def.database.windows.net", "--auth", "managedidentity" },
            output, new StringWriter(), TestContext.Current.CancellationToken, readServiceAccount: () => null, machineName: "DARLING01", domainName: "CORP");

        Assert.Equal(0, code);
        Assert.Contains("USE [master]", output.ToString());
        Assert.Contains("CREATE LOGIN [your-managed-identity-name] FROM EXTERNAL PROVIDER;", output.ToString());
        Assert.Contains("VIEW SERVER STATE", output.ToString());
    }

    [Fact]
    public void Build_ACommentCloserInANameOrNote_CannotEndTheHeaderComment()
    {
        var principal = new GrantPrincipal(
            GrantPrincipalKind.SqlLogin, "pm", new[] { "Principal: svc */ ALTER SERVER ROLE sysadmin ADD MEMBER [x]; /*" });

        var script = MonitoringGrantScript.Build(principal, finOps: false, azureSqlDatabase: false, serverLabel: "sql01 */ DROP LOGIN [a]; /*\nnext");

        // The first comment closer in the script is the header's own, after its last sentence; nothing the caller supplied ended it earlier.
        var firstCloser = script.IndexOf("*/", StringComparison.Ordinal);
        Assert.True(script.IndexOf("It is safe to run twice.", StringComparison.Ordinal) < firstCloser);
        Assert.DoesNotContain("*/ ALTER", script);
        Assert.DoesNotContain("*/ DROP", script);
    }

    [Fact]
    public void Build_ACommentOpenerInANameOrNote_LeavesTheStatementsOutsideEveryComment()
    {
        var principal = new GrantPrincipal(
            GrantPrincipalKind.SqlLogin, "pm", new[] { "Principal: svc /* still a note" });

        var script = MonitoringGrantScript.Build(principal, finOps: false, azureSqlDatabase: false, serverLabel: "sql01 /* open\nnext");

        // T-SQL block comments nest: an unescaped "/*" in the label or a note would leave the header comment open and swallow the statements.
        var depth = 0;
        var createLoginDepth = -1;
        for (var i = 0; i < script.Length - 1; i++)
        {
            if (script[i] == '/' && script[i + 1] == '*') { depth++; i++; }
            else if (script[i] == '*' && script[i + 1] == '/') { depth--; i++; }
            else if (createLoginDepth < 0 && string.CompareOrdinal(script, i, "CREATE LOGIN", 0, 12) == 0) { createLoginDepth = depth; }
        }

        Assert.Equal(0, createLoginDepth);
        Assert.Equal(0, depth);
    }

    [Fact]
    public async Task Verb_ForARegisteredServer_KeepsTheStoreNotesOffTheScriptStream()
    {
        var root = Directory.CreateTempSubdirectory("grant-script-");
        try
        {
            /* A store nobody listens on: the registry read fails, the verb says so on the error stream, and uses the file's own list. */
            var path = Path.Combine(root.FullName, "darling.json");
            File.WriteAllText(path,
                "{ \"postgres\": { \"connectionString\": \"Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=2\" }, "
                + "\"servers\": [ { \"name\": \"SQL2022\", \"host\": \"SQL2022\", \"auth\": \"sql\", \"username\": \"pm_monitor\" } ] }");
            var output = new StringWriter();
            var error = new StringWriter();

            var code = await DarlingCliCommands.GrantScriptAsync(
                new[] { "SQL2022", "--config", path }, output, error, TestContext.Current.CancellationToken,
                readServiceAccount: () => null, machineName: "DARLING01", domainName: "CORP");

            Assert.Equal(0, code);
            Assert.StartsWith("/* Performance Monitor Darling", output.ToString());
            Assert.DoesNotContain("NOTE:", output.ToString());
            Assert.Contains("NOTE: the store's registry is not reachable", error.ToString());
            Assert.Contains("CREATE LOGIN [pm_monitor] WITH PASSWORD", output.ToString());
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task Verb_ForAHostWithNoAuth_OffWindows_PrintsTheSqlLoginScript()
    {
        async Task<(int Code, string Out, string Err)> Run(bool isWindows, params string[] args)
        {
            var output = new StringWriter();
            var error = new StringWriter();
            var code = await DarlingCliCommands.GrantScriptAsync(
                args, output, error, TestContext.Current.CancellationToken,
                readServiceAccount: () => null, machineName: "DARLING01", domainName: "CORP", isWindows: isWindows);
            return (code, output.ToString(), error.ToString());
        }

        // #9844: a Linux or Compose service connects with a SQL login, so the host form without --auth prints the SQL login script.
        var named = await Run(false, "--host", "sql01", "--username", "DarlingMonitor");
        Assert.Equal(0, named.Code);
        Assert.Equal("", named.Err);
        Assert.Contains("CREATE LOGIN [DarlingMonitor] WITH PASSWORD = N'<strong password>'", named.Out);
        Assert.DoesNotContain("FROM WINDOWS", named.Out);
        Assert.DoesNotContain("NT SERVICE", named.Out);

        // No --username: the placeholder login and its note.
        var placeholder = await Run(false, "--host", "sql01");
        Assert.Equal(0, placeholder.Code);
        Assert.Contains($"CREATE LOGIN [{MonitoringGrantScript.PlaceholderLogin}] WITH PASSWORD = N'<strong password>'", placeholder.Out);
        Assert.Contains("The server entry names no SQL login", placeholder.Out);
        Assert.DoesNotContain("FROM WINDOWS", placeholder.Out);

        // The same line on Windows prints the Windows account script, and an explicit --auth wins on either platform.
        var windows = await Run(true, "--host", "sql01");
        Assert.Equal(0, windows.Code);
        Assert.Contains("FROM WINDOWS", windows.Out);
        Assert.DoesNotContain("WITH PASSWORD", windows.Out);
        Assert.Equal(windows.Out, (await Run(true, "--host", "sql01", "--auth", "integrated")).Out);
        Assert.Equal(windows.Out, (await Run(false, "--host", "sql01", "--auth", "integrated")).Out);
        Assert.Equal(named.Out, (await Run(true, "--host", "sql01", "--auth", "sql", "--username", "DarlingMonitor")).Out);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Verb_ForAHostWithAServiceAccountAndNoAuth_PrintsTheWindowsScriptForIt(bool isWindows)
    {
        var output = new StringWriter();
        var error = new StringWriter();

        // A typed --service-account names the Windows account to grant, so with no --auth it keeps integrated on every platform.
        var code = await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "sql01", "--service-account", @"CORP\gmsa-darling$" }, output, error, TestContext.Current.CancellationToken,
            readServiceAccount: () => null, machineName: "DARLING01", domainName: "CORP", isWindows: isWindows);

        Assert.Equal(0, code);
        Assert.Equal("", error.ToString());
        Assert.Contains(@"CREATE LOGIN [CORP\gmsa-darling$] FROM WINDOWS;", output.ToString());
        Assert.DoesNotContain("WITH PASSWORD", output.ToString());
        Assert.DoesNotContain(MonitoringGrantScript.PlaceholderLogin, output.ToString());
    }

    [Fact]
    public async Task Verb_ReadsTheServiceAccountFromTheCaller_ForWindowsAuth()
    {
        var output = new StringWriter();

        var code = await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "sql01" }, output, new StringWriter(), TestContext.Current.CancellationToken,
            readServiceAccount: () => @"CORP\gmsa-darling$", machineName: "DARLING01", domainName: "CORP", isWindows: true);

        Assert.Equal(0, code);
        Assert.Contains(@"CREATE LOGIN [CORP\gmsa-darling$] FROM WINDOWS;", output.ToString());
    }

    [Fact]
    public async Task Verb_WhenNoServiceIsInstalledOnThisMachine_NamesAPlaceholder_NotThisMachinesAccount()
    {
        var output = new StringWriter();

        var code = await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "sql01" }, output, new StringWriter(), TestContext.Current.CancellationToken,
            readServiceAccount: () => null, machineName: "DARLING01", domainName: "CORP", isWindows: true);

        Assert.Equal(0, code);
        Assert.Contains("PLACEHOLDER", output.ToString());
        Assert.DoesNotContain("DARLING01$", output.ToString());
        Assert.DoesNotContain(@"NT SERVICE\PerformanceMonitor Darling] FROM WINDOWS", output.ToString());
    }

    [Fact]
    public async Task Verb_WithAServiceAccountOnTheCommandLine_UsesIt_EvenWhenTheRegistryHasNone()
    {
        var output = new StringWriter();

        var code = await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "sql01", "--service-account", @"CORP\gmsa-darling$" }, output, new StringWriter(), TestContext.Current.CancellationToken,
            readServiceAccount: () => null, machineName: "DARLING01", domainName: "CORP");

        Assert.Equal(0, code);
        Assert.Contains(@"CREATE LOGIN [CORP\gmsa-darling$] FROM WINDOWS;", output.ToString());
        Assert.DoesNotContain("PLACEHOLDER", output.ToString());
    }

    [Fact]
    public async Task Verb_WithBadArguments_PrintsTheGrammar_AndExitsOne()
    {
        var output = new StringWriter();
        var error = new StringWriter();

        var code = await DarlingCliCommands.GrantScriptAsync(new[] { "--bogus" }, output, error, TestContext.Current.CancellationToken);

        Assert.Equal(1, code);
        Assert.Contains("Unknown option --bogus", error.ToString());
        Assert.Contains("--grant-script", output.ToString());
    }

    [Fact]
    public void TheVerb_IsKnown_SoTheExeDispatchesIt()
    {
        Assert.True(DarlingCliCommands.IsGrantScriptVerb("--GRANT-SCRIPT"));
        Assert.True(DarlingCliCommands.IsKnownVerb("--grant-script"));
        Assert.False(DarlingCliCommands.IsGrantScriptVerb("--grant"));
    }

    [Fact]
    public void OnPremises_Msdb_GrantsSelectOnCdcJobs_GuardedForAServerThatNeverConfiguredCdc()
    {
        var script = MonitoringGrantScript.Build(
            MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor")), finOps: false, azureSqlDatabase: false);
        var msdb = script[script.IndexOf("USE [msdb];", StringComparison.Ordinal)..script.LastIndexOf("USE [master];", StringComparison.Ordinal)];
        var lines = msdb.Split('\n').Select(l => l.Trim()).ToArray();

        /* The query snapshots collector reads msdb.dbo.cdc_jobs to leave CDC capture jobs out of the Long-Running Query alert, and msdb
           grants public no SELECT on it, so the login needs the grant. msdb creates the table the first time CDC is configured, and a
           GRANT on a missing object raises error 15151, so the grant sits inside an existence guard and the script runs clean on a
           server that has never used CDC. */
        var grant = Array.IndexOf(lines, "GRANT SELECT ON dbo.cdc_jobs TO [pm_monitor];");
        Assert.True(grant >= 2, msdb);
        Assert.Equal("IF OBJECT_ID(N'dbo.cdc_jobs') IS NOT NULL", lines[grant - 2]);
        Assert.Equal("BEGIN", lines[grant - 1]);
        Assert.Equal("END;", lines[grant + 1]);
        Assert.Contains("GRANT SELECT ON dbo.cdc_jobs TO [pm_monitor];", StatementLines(script));
    }

    [Fact]
    public void TheCdcJobsGrant_IsOptional_AndLeftOutOfTheRdsForm()
    {
        var entry = Assert.Single(MonitoringGrantScript.All, e => e.Scope == GrantScope.Msdb && e.OnObject == "dbo.cdc_jobs");

        Assert.Equal(GrantSet.Base, entry.Set);
        Assert.Equal("SELECT", entry.Permission);
        /* Optional like the other msdb reads: the connection test does not warn when the login lacks it. */
        Assert.False(entry.Required);
        /* RDS lets its master user grant only the three job reads in msdb, so the RDS form carries no GRANT on cdc_jobs. */
        Assert.DoesNotContain(StatementLines(RdsScript()), g => g.Contains("cdc_jobs", StringComparison.Ordinal));
    }

    /* ---------------- #8798: the optional per-database SELECT for the column_stats collector ---------------- */

    private const string ColumnStatsText =
        "Optional, for the column_stats collector (column selectivity). This grant lets the monitoring login read every row of the databases listed here. "
        + "Darling copies into its store only statistic and column names, densities, row and sample counts and update times; it never copies column values, and it never reads histograms or filter expressions.";

    private static string ColumnStatsScript(string[]? databases, bool finOps = false) =>
        MonitoringGrantScript.Build(
            MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor")), finOps, azureSqlDatabase: false, columnStatsDatabases: databases);

    [Fact]
    public void ColumnStatsBlock_OnlyForListedDatabases()
    {
        var script = ColumnStatsScript(new[] { "SalesDb", "Hr" });

        Assert.Contains("EXECUTE [SalesDb].sys.sp_executesql", script);
        Assert.Contains("EXECUTE [Hr].sys.sp_executesql", script);
        Assert.Contains("GRANT SELECT TO ", script);
        /* One block per listed database and no cursor over sys.databases: the grant never defaults to every database. */
        Assert.DoesNotContain("database_cursor", script);
        Assert.DoesNotContain("EXECUTE [Other]", script);
        /* The login is created in the database when missing, as the FinOps block does. */
        Assert.Contains("CREATE USER '' + QUOTENAME(@login)", script);
        /* A listed database that does not exist is skipped with a message, never run in the previous database. */
        Assert.Contains("IF DB_ID(N'SalesDb') IS NULL", script);
    }

    [Fact]
    public void AzureSqlDatabase_ListedGivesTheDatabaseLevelSelect_NoListGivesNone()
    {
        var principal = MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor", "x.database.windows.net"));
        var plain = MonitoringGrantScript.Build(principal, finOps: false, azureSqlDatabase: true);
        var listed = MonitoringGrantScript.Build(principal, finOps: false, azureSqlDatabase: true, columnStatsDatabases: new[] { "SalesDb", "Hr*/Db" });

        Assert.DoesNotContain("GRANT SELECT TO ", plain);
        Assert.Equal(plain, MonitoringGrantScript.Build(principal, finOps: false, azureSqlDatabase: true, columnStatsDatabases: new[] { " " }));
        Assert.StartsWith(plain, listed, StringComparison.Ordinal);
        Assert.Contains("GRANT SELECT TO [pm_monitor];", listed);
        Assert.Contains("SalesDb", listed);
        /* A database name cannot end the comment it is named in. */
        Assert.DoesNotContain("Hr*/Db", listed);
        Assert.DoesNotContain("sp_executesql", listed);
    }

    [Fact]
    public void MissingSelectWarning_MakesTheUserBeforeTheGrant()
    {
        var note = MonitoringGrantScript.DescribeMissingColumnStatsSelect(new[] { "SalesDb" }, "pm_monitor")!;

        var create = note.IndexOf("CREATE USER [pm_monitor] FOR LOGIN [pm_monitor]", StringComparison.Ordinal);
        Assert.True(create >= 0, note);
        Assert.True(create < note.IndexOf("GRANT SELECT TO [pm_monitor]", StringComparison.Ordinal), note);
        Assert.Null(MonitoringGrantScript.DescribeMissingColumnStatsSelect(Array.Empty<string>(), "pm_monitor"));
    }

    [Fact]
    public void NoList_NoBlock()
    {
        var plain = MonitoringGrantScript.Build(MonitoringGrantScript.ChoosePrincipal(Inputs("sql", "pm_monitor")), false, false);
        foreach (var databases in new string[]?[] { null, Array.Empty<string>(), new[] { "", "  " } })
        {
            var script = ColumnStatsScript(databases);

            Assert.DoesNotContain("column_stats", script);
            Assert.DoesNotContain("GRANT SELECT TO ", script);
            Assert.Equal(plain, script);
        }
    }

    [Fact]
    public void Block_SaysBothSentencesPlainly()
    {
        var script = ColumnStatsScript(new[] { "SalesDb" });

        Assert.Contains("/* " + ColumnStatsText + " */", script);
        Assert.Contains("This grant lets the monitoring login read every row of the databases listed here.", script);
        Assert.Contains("never copies column values, and it never reads histograms or filter expressions.", script);
    }

    [Fact]
    public void NamesAreQuoted()
    {
        var script = ColumnStatsScript(new[] { "We]ird'Db", "dup", "DUP" });

        Assert.Contains("EXECUTE [We]]ird'Db].sys.sp_executesql", script);
        Assert.Contains("DB_ID(N'We]ird''Db')", script);
        Assert.Single(script.Split("EXECUTE [dup].sys.sp_executesql").Skip(1).ToArray());
        Assert.DoesNotContain("EXECUTE [DUP]", script);
    }

    [Fact]
    public void ColumnStatsEntry_IsTheDatabaseLevelSelect_AndTheConnectionTestAsksNothingServerWide()
    {
        var entry = Assert.Single(MonitoringGrantScript.All, e => e.Set == GrantSet.ColumnStats);

        Assert.Equal(GrantScope.ListedDatabases, entry.Scope);
        Assert.Equal("GRANT SELECT TO ", entry.GrantPrefix);
        Assert.DoesNotContain(entry, MonitoringGrantScript.RequiredServerGrants);
    }

    [Fact]
    public void Cli_ParsesColumnStatsDatabases()
    {
        /* One name per occurrence, taken whole: a name may hold a comma or a space; a repeat is kept once. */
        Assert.True(DarlingCliCommands.TryParseGrantScriptArgs(
            new[] { "--host", "sql01", "--column-stats-databases", "SalesDb", "--column-stats-databases", "Sales, Archive", "--column-stats-databases", "salesdb" },
            out var parsed, out _));
        Assert.Equal(new[] { "SalesDb", "Sales, Archive" }, parsed!.ColumnStatsDatabases);

        Assert.True(DarlingCliCommands.TryParseGrantScriptArgs(new[] { "--host", "sql01" }, out var none, out _));
        Assert.Null(none!.ColumnStatsDatabases);

        Assert.False(DarlingCliCommands.TryParseGrantScriptArgs(new[] { "--host", "sql01", "--column-stats-databases" }, out _, out var error));
        Assert.Contains("needs a value", error);
        Assert.False(DarlingCliCommands.TryParseGrantScriptArgs(new[] { "--host", "sql01", "--column-stats-databases", " " }, out _, out var empty));
        Assert.Contains("--column-stats-databases", empty);
        Assert.Contains("--column-stats-databases", DarlingCliCommands.GrantScriptUsageText());
    }

    [Fact]
    public async Task Verb_PrintsTheColumnStatsBlock_OnlyWhenTheOptionIsGiven()
    {
        var withList = new StringWriter();
        var without = new StringWriter();
        Assert.Equal(0, await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "sql01", "--auth", "sql", "--username", "pm", "--column-stats-databases", "SalesDb" }, withList, new StringWriter(), default));
        Assert.Equal(0, await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "sql01", "--auth", "sql", "--username", "pm" }, without, new StringWriter(), default));

        Assert.Contains("EXECUTE [SalesDb].sys.sp_executesql", withList.ToString());
        Assert.DoesNotContain("column_stats", without.ToString());
    }

    [Fact]
    public async Task ConnectionTest_NoColumnStatsEntries_WhenListNull()
    {
        var commands = new System.Collections.Generic.List<string>();
        var connection = new FakeGrantConnection(commands, text => AllHeld());

        var note = await DarlingServerConnector.ProbeMissingGrantsAsync("x", false, null, default, _ => connection);

        Assert.Null(note);
        Assert.Single(commands);
        Assert.DoesNotContain("N'SELECT'", commands[0]);
    }

    [Fact]
    public async Task ConnectionTest_ReportsMissingSelect_ForListedDatabase()
    {
        var commands = new System.Collections.Generic.List<string>();
        var connection = new FakeGrantConnection(commands, text => text.Contains("N'DATABASE', N'SELECT'", StringComparison.Ordinal)
            ? Row(("d0", 0), ("d1", 1))
            : AllHeld());

        var note = await DarlingServerConnector.ProbeMissingGrantsAsync(
            "x", false, null, default, _ => connection, columnStatsDatabases: new[] { "SalesDb", "OkDb" });

        Assert.NotNull(note);
        Assert.Contains("column_stats", note);
        Assert.Contains("[SalesDb]", note);
        Assert.DoesNotContain("OkDb", note);
        Assert.Contains("SELECT", note);
        Assert.Contains("HAS_PERMS_BY_NAME(N'SalesDb', N'DATABASE', N'SELECT')", commands[1]);
        Assert.Contains("HAS_PERMS_BY_NAME(N'OkDb', N'DATABASE', N'SELECT')", commands[1]);
    }

    [Fact]
    public async Task ConnectionTest_NothingToSay_WhenEveryListedDatabaseHoldsSelect()
    {
        var connection = new FakeGrantConnection(new System.Collections.Generic.List<string>(), text => text.Contains("N'DATABASE', N'SELECT'", StringComparison.Ordinal)
            ? Row(("d0", 1))
            : AllHeld());

        Assert.Null(await DarlingServerConnector.ProbeMissingGrantsAsync(
            "x", false, null, default, _ => connection, columnStatsDatabases: new[] { "SalesDb" }));
    }

    [Fact]
    public void TheCaller_PassesTheListedDatabases_OnlyWhenColumnStatsIsEnabledWithAnExplicitScope()
    {
        var on = new ScheduleOverride(null, "column_stats", null, null, true, new[] { "SalesDb" });
        var onNoScope = new ScheduleOverride(null, "column_stats", null, null, true, Array.Empty<string>());
        var offWithScope = new ScheduleOverride(null, "column_stats", null, null, false, new[] { "SalesDb" });

        Assert.Equal(new[] { "SalesDb" }, DarlingCommandExecutor.ResolveColumnStatsGrantDatabases(7, new[] { on }));
        /* An empty scope collects every database; the connection test never checks every database. */
        Assert.Null(DarlingCommandExecutor.ResolveColumnStatsGrantDatabases(7, new[] { onNoScope }));
        Assert.Null(DarlingCommandExecutor.ResolveColumnStatsGrantDatabases(7, new[] { offWithScope }));
        /* Off by default: no schedule rows at all. */
        Assert.Null(DarlingCommandExecutor.ResolveColumnStatsGrantDatabases(7, Array.Empty<ScheduleOverride>()));
    }

    private static System.Data.DataTable AllHeld()
    {
        var columns = MonitoringGrantScript.RequiredServerGrants.Count;
        var table = new System.Data.DataTable();
        for (var i = 0; i < columns; i++)
        {
            table.Columns.Add("g" + i, typeof(int));
        }

        table.Columns.Add("who", typeof(string));
        table.Columns.Add("no_user_dbs", typeof(long));
        var row = table.NewRow();
        for (var i = 0; i < columns; i++)
        {
            row[i] = 1;
        }

        row[columns] = "pm_monitor";
        row[columns + 1] = 0L;
        table.Rows.Add(row);
        return table;
    }

    private static System.Data.DataTable Row(params (string Name, int Value)[] values)
    {
        var table = new System.Data.DataTable();
        foreach (var (name, _) in values)
        {
            table.Columns.Add(name, typeof(int));
        }

        var row = table.NewRow();
        for (var i = 0; i < values.Length; i++)
        {
            row[i] = values[i].Value;
        }

        table.Rows.Add(row);
        return table;
    }

#pragma warning disable CS8765
    private sealed class FakeGrantConnection(System.Collections.Generic.List<string> commands, Func<string, System.Data.DataTable> answer) : System.Data.Common.DbConnection
    {
        public override string ConnectionString { get; set; } = "";
        public override string Database => "";
        public override string DataSource => "";
        public override string ServerVersion => "";
        public override System.Data.ConnectionState State => System.Data.ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        public override Task OpenAsync(System.Threading.CancellationToken cancellationToken) => Task.CompletedTask;
        protected override System.Data.Common.DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override System.Data.Common.DbCommand CreateDbCommand() => new FakeGrantCommand(this, commands, answer);
    }

    private sealed class FakeGrantCommand(System.Data.Common.DbConnection connection, System.Collections.Generic.List<string> commands, Func<string, System.Data.DataTable> answer) : System.Data.Common.DbCommand
    {
        public override string CommandText { get; set; } = "";
        public override int CommandTimeout { get; set; }
        public override System.Data.CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override System.Data.UpdateRowSource UpdatedRowSource { get; set; }
        protected override System.Data.Common.DbConnection? DbConnection { get; set; } = connection;
        protected override System.Data.Common.DbParameterCollection DbParameterCollection { get; } = new FakeGrantParameters();
        protected override System.Data.Common.DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => throw new NotSupportedException();
        public override void Prepare() { }
        protected override System.Data.Common.DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override System.Data.Common.DbDataReader ExecuteDbDataReader(System.Data.CommandBehavior behavior)
        {
            commands.Add(CommandText);
            return answer(CommandText).CreateDataReader();
        }
    }

    private sealed class FakeGrantParameters : System.Data.Common.DbParameterCollection
    {
        public override int Count => 0;
        public override object SyncRoot => this;
        public override int Add(object value) => throw new NotSupportedException();
        public override void AddRange(Array values) => throw new NotSupportedException();
        public override void Clear() { }
        public override bool Contains(object? value) => false;
        public override bool Contains(string value) => false;
        public override void CopyTo(Array array, int index) { }
        public override System.Collections.IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
        public override int IndexOf(object? value) => -1;
        public override int IndexOf(string parameterName) => -1;
        public override void Insert(int index, object value) => throw new NotSupportedException();
        public override void Remove(object value) { }
        public override void RemoveAt(int index) { }
        public override void RemoveAt(string parameterName) { }
        protected override System.Data.Common.DbParameter GetParameter(int index) => throw new NotSupportedException();
        protected override System.Data.Common.DbParameter GetParameter(string parameterName) => throw new NotSupportedException();
        protected override void SetParameter(int index, System.Data.Common.DbParameter value) { }
        protected override void SetParameter(string parameterName, System.Data.Common.DbParameter value) { }
    }
#pragma warning restore CS8765
}
