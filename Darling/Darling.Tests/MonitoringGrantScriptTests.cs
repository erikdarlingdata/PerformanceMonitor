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

        Assert.Contains("sp_MSforeachdb", with);
        Assert.Contains("GRANT VIEW DATABASE STATE TO [pm_monitor];", with);
        Assert.Contains("GRANT VIEW DEFINITION TO [pm_monitor];", with);
        Assert.Contains("GRANT SELECT ON sys.sql_expression_dependencies TO [pm_monitor];", with);
        Assert.Contains("''pm_monitor''", with);
        Assert.DoesNotContain("sp_MSforeachdb", without);
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
        Assert.Contains("N''o''''b]d''", script);   /* inside the FinOps string, the literal's own quote doubles again */
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
        Assert.Contains("sp_MSforeachdb", output.ToString());
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
    public async Task Verb_ReadsTheServiceAccountFromTheCaller_ForWindowsAuth()
    {
        var output = new StringWriter();

        var code = await DarlingCliCommands.GrantScriptAsync(
            new[] { "--host", "sql01" }, output, new StringWriter(), TestContext.Current.CancellationToken,
            readServiceAccount: () => @"CORP\gmsa-darling$", machineName: "DARLING01", domainName: "CORP");

        Assert.Equal(0, code);
        Assert.Contains(@"CREATE LOGIN [CORP\gmsa-darling$] FROM WINDOWS;", output.ToString());
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
}
