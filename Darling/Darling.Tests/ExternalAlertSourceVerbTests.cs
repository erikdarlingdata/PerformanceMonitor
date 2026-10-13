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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The four external alert source verbs (#5654): <c>--add-alert-source</c>, <c>--rotate-alert-source</c>,
/// <c>--remove-alert-source</c> and <c>--list-alert-sources</c>. These are the parse, classification and text tests; the live
/// proof against a store is <see cref="ExternalAlertSourceVerbLiveTests"/>.
/// </summary>
public sealed class ExternalAlertSourceVerbTests
{
    [Theory]
    [InlineData("--add-alert-source")]
    [InlineData("--rotate-alert-source")]
    [InlineData("--remove-alert-source")]
    [InlineData("--list-alert-sources")]
    public void EachVerb_IsKnown_AndStartsAKnownVerbRun(string verb)
    {
        Assert.True(DarlingCliCommands.IsKnownVerb(verb));
        Assert.Equal(StartupAction.RunKnownVerb, DarlingCliCommands.ClassifyStartupArgs(new[] { verb, "orders" }));
    }

    [Fact]
    public void EachVerb_IsRecognisedByItsOwnPredicateOnly()
    {
        Assert.True(DarlingCliCommands.IsAddAlertSourceVerb("--add-alert-source"));
        Assert.True(DarlingCliCommands.IsRotateAlertSourceVerb("--ROTATE-ALERT-SOURCE"));
        Assert.True(DarlingCliCommands.IsRemoveAlertSourceVerb("--remove-alert-source"));
        Assert.True(DarlingCliCommands.IsListAlertSourcesVerb("--list-alert-sources"));
        Assert.False(DarlingCliCommands.IsAddAlertSourceVerb("--add-server"));
        Assert.False(DarlingCliCommands.IsAddServerVerb("--add-alert-source"));
        Assert.False(DarlingCliCommands.IsListAlertSourcesVerb("--list-alert-source"));
    }

    [Fact]
    public void TheUsageText_ListsEveryVerb()
    {
        var usage = DarlingCliCommands.UsageText();
        foreach (var verb in new[] { "--add-alert-source", "--rotate-alert-source", "--remove-alert-source", "--list-alert-sources" })
        {
            Assert.Contains(verb, usage);
        }
    }

    [Theory]
    [InlineData("orders", null)]
    [InlineData("ab", null)]
    public void TheNameVerbs_TakeOneValidName(string name, string? config)
    {
        Assert.True(DarlingCliCommands.TryParseAlertSourceArgs("--add-alert-source", new[] { name }, true, out var parsed, out var configPath, out var error));
        Assert.Equal(name, parsed);
        Assert.Equal(config, configPath);
        Assert.Null(error);
    }

    [Fact]
    public void TheNameVerbs_TakeAConfigPath_BeforeOrAfterTheName()
    {
        Assert.True(DarlingCliCommands.TryParseAlertSourceArgs("--rotate-alert-source", new[] { "orders", "--config", "c:/x/darling.json" }, true, out var name, out var path, out _));
        Assert.Equal("orders", name);
        Assert.Equal("c:/x/darling.json", path);

        Assert.True(DarlingCliCommands.TryParseAlertSourceArgs("--remove-alert-source", new[] { "--config", "d.json", "orders" }, true, out name, out path, out _));
        Assert.Equal("orders", name);
        Assert.Equal("d.json", path);
    }

    [Theory]
    [InlineData(new string[0], "needs a source name")]
    [InlineData(new[] { "orders", "billing" }, "ONE source name")]
    [InlineData(new[] { "orders", "--bogus" }, "Unknown option")]
    [InlineData(new[] { "orders", "--config" }, "--config needs a path")]
    [InlineData(new[] { "orders", "--config", "--other" }, "--config needs a path")]
    [InlineData(new[] { "Orders" }, "not a valid source name")]
    [InlineData(new[] { "a" }, "not a valid source name")]
    [InlineData(new[] { "1orders" }, "not a valid source name")]
    [InlineData(new[] { "or-ders" }, "not a valid source name")]
    [InlineData(new[] { "abcdefghijklmnopqrstuvwxyz0123456" }, "not a valid source name")]
    [InlineData(new[] { "orders\n" }, "not a valid source name")]
    public void TheNameVerbs_RefuseABadCommandLine(string[] rest, string expected)
    {
        Assert.False(DarlingCliCommands.TryParseAlertSourceArgs("--add-alert-source", rest, true, out _, out _, out var error));
        Assert.Contains(expected, error);
    }

    [Fact]
    public void TheListVerb_TakesNoName_AndAConfigPath()
    {
        Assert.True(DarlingCliCommands.TryParseAlertSourceArgs("--list-alert-sources", Array.Empty<string>(), false, out var name, out var path, out _));
        Assert.Null(name);
        Assert.Null(path);

        Assert.True(DarlingCliCommands.TryParseAlertSourceArgs("--list-alert-sources", new[] { "--config", "x.json" }, false, out _, out path, out _));
        Assert.Equal("x.json", path);

        Assert.False(DarlingCliCommands.TryParseAlertSourceArgs("--list-alert-sources", new[] { "orders" }, false, out _, out _, out var error));
        Assert.Contains("takes no name", error);
    }

    [Fact]
    public void ALongestValidName_FitsInALoginName()
    {
        var longest = "a" + new string('b', 30);
        Assert.True(ExternalAlertSourceAdmin.IsValidName(longest));
        Assert.True(ExternalAlertSourceAdmin.RoleNameFor(longest).Length <= 63);
    }

    /// <summary>#5654: the name check runs to the string's true end, so a trailing line feed (which a <c>$</c>-anchored .NET
    /// pattern lets through) is refused, and the verbs and the mute rule surfaces share one rule.</summary>
    [Theory]
    [InlineData("orders\n")]
    [InlineData("orders\r\n")]
    [InlineData("orders\0")]
    public void TheNameRule_RefusesATrailingLineFeed(string name)
    {
        Assert.False(ExternalAlertSourceAdmin.IsValidName(name));
        Assert.True(ExternalAlertSourceAdmin.IsValidName(name.TrimEnd('\n', '\r', '\0')));
    }

    private static PostgresConfig ManagedWithRole(string role) => new()
    {
        Managed = true,
        Port = 5641,
        DataDirectory = Path.Combine(Path.GetTempPath(), "darling-store-test", "data"),
        Network = new PostgresNetworkConfig { Listen = "192.0.2.10", AllowFrom = "192.0.2.0/24", Role = role },
    };

    /// <summary>#5654: a source on another machine gets the remote viewer's string shape: the viewer's host, VerifyFull and the
    /// store's own certificate, from the same builder, with the source's login.</summary>
    [Fact]
    public void TheRemoteSourceString_VerifiesTheServer_InTheViewersForm()
    {
        var postgres = ManagedWithRole("viewer,sources");
        var remote = DarlingCliCommands.BuildRemoteAlertSourceConnectionString(postgres, "darling_src_orders", "SourcePassword123");
        Assert.NotNull(remote);

        var builder = new NpgsqlConnectionStringBuilder(remote!.ConnectionString);
        Assert.Equal(SslMode.VerifyFull, builder.SslMode);
        Assert.Equal(remote.CertificatePath, builder.RootCertificate);
        Assert.EndsWith("server.crt", remote.CertificatePath, StringComparison.Ordinal);
        Assert.Equal("192.0.2.10", builder.Host);
        Assert.Equal("darling_src_orders", builder.Username);
        Assert.Equal("SourcePassword123", builder.Password);

        var viewer = DarlingCliCommands.BuildViewerConnectionString(
            DarlingCliCommands.ResolveViewerHost(postgres.Network!.Listen), postgres.Port, "viewer", "ViewerPassword123", remote.CertificatePath);
        Assert.Equal(
            viewer.Replace("Username=viewer;Password=ViewerPassword123", string.Empty, StringComparison.Ordinal),
            remote.ConnectionString.Replace("Username=darling_src_orders;Password=SourcePassword123", string.Empty, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("viewer")]
    [InlineData("admin,viewer")]
    public void TheRemoteSourceString_IsPrintedOnlyWhenSourcesAreAdmitted(string role)
    {
        Assert.Null(DarlingCliCommands.BuildRemoteAlertSourceConnectionString(ManagedWithRole(role), "darling_src_orders", "pw"));
        Assert.Null(DarlingCliCommands.BuildRemoteAlertSourceConnectionString(new PostgresConfig { Managed = false }, "darling_src_orders", "pw"));
        Assert.Null(DarlingCliCommands.BuildRemoteAlertSourceConnectionString(null, "darling_src_orders", "pw"));
    }

    [Fact]
    public void TheResultText_WhenSourcesAreAdmitted_PrintsTheLocalAndTheVerifyingString()
    {
        var output = new StringWriter();
        var exit = DarlingCliCommands.PrintAlertSourceResult(
            "--add-alert-source",
            new ExternalAlertSourceAdmin.Result(ExternalAlertSourceAdmin.Outcome.Done, "darling_src_orders", "PwForTheTest0123456789abcdefgh"),
            "orders", "Host=127.0.0.1;Port=5641;Database=darling;Username=darling;Password=ownerpw", output, new StringWriter(),
            ManagedWithRole("viewer,sources"));

        Assert.Equal(0, exit);
        var text = output.ToString();
        Assert.Equal(2, Regex.Matches(text, "PwForTheTest0123456789abcdefgh").Count);
        Assert.Single(Regex.Matches(text, "SSL Mode=VerifyFull;Root Certificate="));
        Assert.Contains("Copy " + Path.Combine(Path.GetTempPath(), "darling-store-test", "server.crt"), text, StringComparison.Ordinal);
        Assert.DoesNotContain("ownerpw", text, StringComparison.Ordinal);
    }

    /// <summary>#5654: rotate and remove say how many open sessions of the login were ended, or print the statement a database
    /// administrator runs when the owner may not end them.</summary>
    [Fact]
    public void TheResultText_SaysWhichSessionsWereEnded_OrPrintsTheStatementForADba()
    {
        var ended = new StringWriter();
        DarlingCliCommands.PrintAlertSourceResult("--rotate-alert-source",
            new ExternalAlertSourceAdmin.Result(ExternalAlertSourceAdmin.Outcome.Done, "darling_src_orders", "Pw0123456789", SessionsEnded: 3),
            "orders", "Host=127.0.0.1", ended, new StringWriter());
        Assert.Contains("Ended 3 open session(s) of login 'darling_src_orders'", ended.ToString(), StringComparison.Ordinal);
        Assert.Contains("no longer works for a new connection", ended.ToString(), StringComparison.Ordinal);

        var refused = new StringWriter();
        DarlingCliCommands.PrintAlertSourceResult("--remove-alert-source",
            new ExternalAlertSourceAdmin.Result(ExternalAlertSourceAdmin.Outcome.Done, "darling_src_orders", LoginDropped: true,
                EndSessionsSqlForDba: ExternalAlertSourceAdmin.BuildEndSessionsSqlForDba("darling_src_orders")),
            "orders", "Host=127.0.0.1", refused, new StringWriter());
        Assert.Contains("may not end the open sessions", refused.ToString(), StringComparison.Ordinal);
        Assert.Contains(
            "SELECT pg_catalog.pg_terminate_backend(a.pid) FROM pg_catalog.pg_stat_activity AS a WHERE a.usename = 'darling_src_orders';",
            refused.ToString(), StringComparison.Ordinal);
        Assert.Contains(ExternalAlertSourceAdmin.BuildEndSessionsSqlForDba("darling_src_orders"),
            ExternalAlertSourceAdmin.BuildRemoveSqlForDba("darling_src_orders", "darling"), StringComparison.Ordinal);
    }

    /// <summary>#5654: when the owner cannot turn utility statement tracking off, the text says the statistics may hold the
    /// verifier and prints the reset statement for a database administrator.</summary>
    [Fact]
    public void TheResultText_PrintsTheStatisticsReset_WhenTrackingCouldNotBeTurnedOff()
    {
        var output = new StringWriter();
        DarlingCliCommands.PrintAlertSourceResult("--add-alert-source",
            new ExternalAlertSourceAdmin.Result(ExternalAlertSourceAdmin.Outcome.Done, "darling_src_orders", "Pw0123456789",
                StatementStatisticsResetSql: "SELECT pg_stat_statements_reset();"),
            "orders", "Host=127.0.0.1", output, new StringWriter());
        Assert.Contains("may hold", output.ToString(), StringComparison.Ordinal);
        Assert.Contains("SELECT pg_stat_statements_reset();", output.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// #5654: every statement the verbs run as the store owner names its catalog functions through <c>pg_catalog.</c>, and every
    /// verb transaction begins with <c>SET LOCAL search_path = pg_catalog, pg_temp</c>, so the shared provisioning step the add
    /// verb runs inside it resolves the same way.
    /// </summary>
    [Fact]
    public void TheVerbsOwnerSql_NamesCatalogFunctionsThroughPgCatalog_AndPinsTheTransactionSearchPath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        var source = File.ReadAllText(Path.Combine(dir!.FullName, "Darling", "PerformanceMonitor.Darling.Service", "ExternalAlertSourceAdmin.cs"));
        var code = string.Join("\n", source.Split('\n').Where(l => !l.TrimStart().StartsWith("///", StringComparison.Ordinal)
            && !l.TrimStart().StartsWith("/*", StringComparison.Ordinal) && !l.TrimStart().StartsWith("*", StringComparison.Ordinal)));

        var unqualified = Regex.Matches(code,
                @"(?<![\w.])(count|max|min|shobj_description|current_database|current_setting|to_regclass|format|lower|pg_terminate_backend|has_\w+_privilege)\s*\(")
            .Select(m => m.Value).ToArray();
        Assert.Empty(unqualified);
        Assert.Matches(new Regex(@"\bFROM pg_catalog\.pg_roles\b"), code);
        Assert.DoesNotMatch(new Regex(@"\bFROM (pg_roles|pg_stat_activity|pg_authid)\b"), code);

        /* One raw BeginTransactionAsync, inside the helper that pins the search path; every verb calls the helper. */
        Assert.Single(Regex.Matches(code, @"\.BeginTransactionAsync\("));
        Assert.Contains("\"SET LOCAL search_path = pg_catalog, pg_temp\"", code, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(code, @"await BeginOwnerTransactionAsync\(owner, ct\)").Count);
    }

    [Fact]
    public void TheSourceConnectionString_CarriesWhereTheStoreIs_AndNothingOfTheOwners()
    {
        var text = ExternalAlertSourceAdmin.BuildSourceConnectionString(
            "Host=db.example;Port=5433;Database=darling;Username=darling;Password=OwnerSecretValue;Search Path=collect,config;Application Name=x",
            "darling_src_orders", "SourcePassword123");
        var builder = new NpgsqlConnectionStringBuilder(text);
        Assert.Equal("db.example", builder.Host);
        Assert.Equal(5433, builder.Port);
        Assert.Equal("darling", builder.Database);
        Assert.Equal("darling_src_orders", builder.Username);
        Assert.Equal("SourcePassword123", builder.Password);
        Assert.DoesNotContain("OwnerSecretValue", text);
        Assert.DoesNotContain("Search Path", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Application Name", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheStatementsForADatabaseAdministrator_CarryTheVerifier_NotThePassword()
    {
        var password = DarlingManagedPostgres.GeneratePassword();
        var verifier = ScramSha256Verifier.Create(password);
        var add = ExternalAlertSourceAdmin.BuildAddSqlForDba("darling_src_orders", verifier, "darling", 16);

        Assert.Contains("CREATE ROLE \"darling_src_orders\"", add);
        Assert.Contains($"PASSWORD '{verifier}'", add);
        Assert.Contains($"COMMENT ON ROLE \"darling_src_orders\" IS '{DarlingManagedRoles.AlertSourceRoleMarker}'", add);
        Assert.Contains("GRANT EXECUTE ON FUNCTION config.submit_external_alert(", add);
        Assert.Contains("NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT", add);
        Assert.DoesNotContain(password, add);

        /* #5654: the membership in the group the pg_hba "sources" line names, last, with the PostgreSQL 16 options only where
           the server takes them. */
        Assert.EndsWith("GRANT \"darling_alert_sources\" TO \"darling_src_orders\" WITH INHERIT FALSE, SET FALSE;", add);
        Assert.EndsWith("GRANT \"darling_alert_sources\" TO \"darling_src_orders\";",
            ExternalAlertSourceAdmin.BuildAddSqlForDba("darling_src_orders", verifier, "darling", 15));

        var remove = ExternalAlertSourceAdmin.BuildRemoveSqlForDba("darling_src_orders", "darling");
        Assert.Contains("DROP ROLE \"darling_src_orders\"", remove);
    }

    [Theory]
    [InlineData(null, "192.0.2.10", "not list \"sources\"", "\"role\": \"viewer,sources\"")]
    [InlineData("", "192.0.2.10", "not list \"sources\"", "\"role\": \"viewer,sources\"")]
    [InlineData("admin", "192.0.2.10", "not list \"sources\"", "\"role\": \"admin,sources\"")]
    [InlineData("admin,viewer", null, "not list \"sources\"", "\"role\": \"admin,viewer,sources\"")]
    [InlineData("viewer,sources", null, "not a network address", "listen and allowFrom must also be set")]
    [InlineData("viewr", "192.0.2.10", "is not valid", "\"role\": \"viewer,sources\"")]
    public void TheNetworkNote_SaysWhatToChange_WhenSourcesAreNotAdmitted(string? role, string? listen, string first, string change)
    {
        var postgres = new PostgresConfig { Managed = true, Network = new PostgresNetworkConfig { Listen = listen, AllowFrom = "192.0.2.0/24", Role = role } };
        var note = string.Join("\n", DarlingCliCommands.BuildAlertSourceNetworkNote(postgres));

        Assert.Contains(first, note, StringComparison.Ordinal);
        Assert.Contains(change, note, StringComparison.Ordinal);
        Assert.Contains("restart", note, StringComparison.Ordinal);
        Assert.Contains("TLS", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNetworkNote_SaysSourcesAreAdmitted_WhenTheRoleListsThem()
    {
        var postgres = new PostgresConfig { Managed = true, Network = new PostgresNetworkConfig { Listen = "192.0.2.10", AllowFrom = "192.0.2.0/24", Role = "viewer+sources" } };
        var note = Assert.Single(DarlingCliCommands.BuildAlertSourceNetworkNote(postgres));

        Assert.Contains("lists \"sources\"", note, StringComparison.Ordinal);
        Assert.Contains("TLS from 192.0.2.0/24", note, StringComparison.Ordinal);
        Assert.DoesNotContain("restart", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNetworkNote_ForABringYourOwnStore_NamesThePgHbaLine()
    {
        var note = string.Join("\n", DarlingCliCommands.BuildAlertSourceNetworkNote(new PostgresConfig { Managed = false }));
        Assert.Contains("hostssl <database> +darling_alert_sources <range> scram-sha-256", note, StringComparison.Ordinal);
        Assert.DoesNotContain("no other role", note, StringComparison.Ordinal);
        Assert.DoesNotContain("reject", note, StringComparison.Ordinal);
    }

    /* #5654: a member of the group that is not a source login (here the owner, which PostgreSQL 16 makes a member of the group
       and of each login it creates) is named; a bring-your-own store gets a reject line for it, to go above the group line. */
    [Fact]
    public void TheNetworkNote_ForABringYourOwnStore_NamesEachExtraMember_WithARejectLine()
    {
        var note = string.Join("\n", DarlingCliCommands.BuildAlertSourceNetworkNote(new PostgresConfig { Managed = false }, ["store_owner", "Mixed Case"]));
        Assert.Contains("'store_owner', 'Mixed Case'", note, StringComparison.Ordinal);
        Assert.Contains("ABOVE", note, StringComparison.Ordinal);
        Assert.Contains("  hostssl <database> store_owner <range> reject", note, StringComparison.Ordinal);
        Assert.Contains("  hostssl <database> \"Mixed Case\" <range> reject", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNetworkNote_ForTheManagedStore_SaysTheLineIsLeftOut_WhileAnExtraMemberRemains()
    {
        var postgres = new PostgresConfig { Managed = true, Network = new PostgresNetworkConfig { Listen = "192.0.2.10", AllowFrom = "192.0.2.0/24", Role = "viewer,sources" } };
        var note = string.Join("\n", DarlingCliCommands.BuildAlertSourceNetworkNote(postgres, ["store_owner"]));
        Assert.Contains("'store_owner'", note, StringComparison.Ordinal);
        Assert.Contains("leaves out the +darling_alert_sources line", note, StringComparison.Ordinal);
        Assert.DoesNotContain("reject", note, StringComparison.Ordinal);
        Assert.Single(DarlingCliCommands.BuildAlertSourceNetworkNote(postgres, []));
    }

    /* #5654: the managed pg_hba block writes the alert source line only when the group's members were read and are exactly the
       source logins; the other roles' lines stay either way. */
    [Fact]
    public void TheManagedPgHbaBlock_LeavesOutTheSourcesLine_UnlessTheGroupHoldsOnlySourceLogins()
    {
        string[] roles = ["viewer", "sources"];
        Assert.Equal(roles, DarlingManagedPostgres.AdmittedNetworkRoles(roles, []));
        Assert.Equal(["viewer"], DarlingManagedPostgres.AdmittedNetworkRoles(roles, ["store_owner"]));
        Assert.Equal(["viewer"], DarlingManagedPostgres.AdmittedNetworkRoles(roles, null));
        Assert.Empty(DarlingManagedPostgres.AdmittedNetworkRoles(["sources"], ["store_owner"]));
        Assert.DoesNotContain("+darling_alert_sources",
            DarlingManagedPostgres.BuildNetworkPgHbaLines(DarlingManagedPostgres.AdmittedNetworkRoles(roles, ["store_owner"]), "10.0.0.0/24"), StringComparison.Ordinal);
    }

    /* #5654: when a database administrator still has to run the statements, add and rotate say nothing is applied yet, never
       that the password changed or the login exists. */
    [Theory]
    [InlineData("--rotate-alert-source", "nothing is changed yet", "no longer works")]
    [InlineData("--add-alert-source", "does not exist yet", "created (login")]
    public void ThePendingResultText_SaysNothingIsAppliedYet(string verb, string expected, string absent)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = DarlingCliCommands.PrintAlertSourceResult(
            verb,
            new ExternalAlertSourceAdmin.Result(ExternalAlertSourceAdmin.Outcome.NeedsDbaSql, "darling_src_orders", "PwForTheTest0123456789abcdefgh",
                "ALTER ROLE \"darling_src_orders\" PASSWORD 'SCRAM-SHA-256$x';", DbaReason: "permission denied to alter role"),
            "orders", "Host=127.0.0.1;Port=5432;Database=darling;Username=darling;Password=ownerpw", output, error);

        Assert.Equal(0, exit);
        Assert.Contains(expected, output.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(absent, output.ToString(), StringComparison.Ordinal);
        Assert.Contains("permission denied to alter role", output.ToString(), StringComparison.Ordinal);
        Assert.Contains("nothing", output.ToString()[output.ToString().IndexOf("permission denied", StringComparison.Ordinal)..], StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheResultText_ShowsThePasswordOnlyInsideTheConnectionString_AndOnce()
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = DarlingCliCommands.PrintAlertSourceResult(
            "--add-alert-source",
            new ExternalAlertSourceAdmin.Result(ExternalAlertSourceAdmin.Outcome.Done, "darling_src_orders", "PwForTheTest0123456789abcdefgh"),
            "orders", "Host=127.0.0.1;Port=5432;Database=darling;Username=darling;Password=ownerpw", output, error);

        Assert.Equal(0, exit);
        Assert.Single(Regex.Matches(output.ToString(), "PwForTheTest0123456789abcdefgh"));
        Assert.DoesNotContain("ownerpw", output.ToString());
        Assert.Contains("shown ONCE", output.ToString());
        Assert.Equal(string.Empty, error.ToString());
    }

    [Theory]
    [InlineData("InvalidName", "not a valid source name")]
    [InlineData("NoTables", "no external alert tables")]
    [InlineData("AlreadyRegistered", "already exists")]
    [InlineData("LoginExists", "left alone")]
    [InlineData("NotRegistered", "No source named")]
    [InlineData("LoginNotManaged", "was not made by")]
    public void ARefusal_GoesToStderr_WithExitCode1_AndPrintsNoConnectionString(string outcomeName, string expected)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = DarlingCliCommands.PrintAlertSourceResult(
            "--rotate-alert-source", new ExternalAlertSourceAdmin.Result(Enum.Parse<ExternalAlertSourceAdmin.Outcome>(outcomeName), "darling_src_orders"), "orders", "Host=x", output, error);

        Assert.Equal(1, exit);
        Assert.Contains(expected, error.ToString());
        Assert.DoesNotContain("Password", output.ToString());
    }

    [Fact]
    public void TheList_NeverPrintsALoginPassword_AndSaysWhenNothingWasSent()
    {
        var output = new StringWriter();
        var rows = new[]
        {
            new ExternalAlertSourceAdmin.SourceRow("orders", "darling_src_orders", true, DateTimeOffset.Parse("2026-10-01T12:00:00Z"), null, 0),
            new ExternalAlertSourceAdmin.SourceRow("billing", "darling_src_billing", false, DateTimeOffset.Parse("2026-10-02T08:30:00Z"), DateTimeOffset.Parse("2026-10-03T09:15:20Z"), 7),
        };

        Assert.Equal(0, DarlingCliCommands.PrintAlertSourceList(rows, output, new StringWriter()));
        var text = output.ToString();
        Assert.Contains("orders", text);
        Assert.Contains("never", text);
        Assert.Contains("2026-10-03 09:15:20", text);
        Assert.Contains("disabled", text);
        Assert.DoesNotContain("assword", text);
    }

    [Fact]
    public void TheList_OnAStoreWithoutTheTables_IsARefusal()
    {
        var error = new StringWriter();
        Assert.Equal(1, DarlingCliCommands.PrintAlertSourceList(null, new StringWriter(), error));
        Assert.Contains("no external alert tables", error.ToString());
    }
}
