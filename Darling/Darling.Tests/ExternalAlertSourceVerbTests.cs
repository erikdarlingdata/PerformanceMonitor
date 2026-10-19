/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
[Trait("Stage", "Guard")]
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

    private static PostgresConfig ManagedWithRole(string? role) => new()
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
    [InlineData("")]
    public void TheRemoteSourceString_OfAManagedStore_VerifiesTheServer_WhetherOrNotSourcesAreListed(string role)
    {
        var remote = DarlingCliCommands.BuildRemoteAlertSourceConnectionString(ManagedWithRole(role), "darling_src_orders", "pw");
        Assert.NotNull(remote);
        Assert.False(remote!.SourcesListed);
        var builder = new NpgsqlConnectionStringBuilder(remote.ConnectionString);
        Assert.Equal(SslMode.VerifyFull, builder.SslMode);
        Assert.EndsWith("server.crt", builder.RootCertificate, StringComparison.Ordinal);
        Assert.True(DarlingCliCommands.BuildRemoteAlertSourceConnectionString(ManagedWithRole("viewer,sources"), "darling_src_orders", "pw")!.SourcesListed);
    }

    [Fact]
    public void TheRemoteSourceString_OfAStoreDarlingDoesNotManage_IsLeftToTheBringYourOwnBuilder()
    {
        Assert.Null(DarlingCliCommands.BuildRemoteAlertSourceConnectionString(new PostgresConfig { Managed = false }, "darling_src_orders", "pw"));
        Assert.Null(DarlingCliCommands.BuildRemoteAlertSourceConnectionString(null, "darling_src_orders", "pw"));
    }

    /// <summary>#5654: the string for a source on another machine, on a store Darling does not manage, verifies the server and names where
    /// the CA certificate goes; nothing of the owner's string but where the store is comes along.</summary>
    [Theory]
    [InlineData("Host=db.example;Port=5433;Database=darling;Username=darling;Password=OwnerSecretValue;Search Path=collect,config;SSL Mode=Disable")]
    [InlineData("Host=db.example;Port=5433;Database=darling;Username=darling;Password=OwnerSecretValue")]
    [InlineData("Host=db.example;Port=5433;Database=darling;Username=darling;Password=OwnerSecretValue;SSL Mode=VerifyFull;Root Certificate=/etc/ssl/ca.pem")]
    public void TheRemoteSourceString_OfABringYourOwnStore_IsVerifyFull_WithARootCertificatePlaceholder(string owner)
    {
        var text = ExternalAlertSourceAdmin.BuildRemoteSourceConnectionString(owner, "darling_src_orders", "SourcePassword123");
        var builder = new NpgsqlConnectionStringBuilder(text);
        Assert.Equal(SslMode.VerifyFull, builder.SslMode);
        Assert.Equal(ExternalAlertSourceAdmin.RootCertificatePlaceholder, builder.RootCertificate);
        Assert.Contains("CA certificate that signed the store server certificate", builder.RootCertificate, StringComparison.Ordinal);
        Assert.Equal("db.example", builder.Host);
        Assert.Equal(5433, builder.Port);
        Assert.Equal("darling", builder.Database);
        Assert.Equal("darling_src_orders", builder.Username);
        Assert.Equal("SourcePassword123", builder.Password);
        Assert.DoesNotContain("OwnerSecretValue", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Search Path", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("ca.pem", text, StringComparison.Ordinal);
    }

    /// <summary>The password-bearing lines the verb prints, in order: the local string first, then the one for another machine.</summary>
    private static List<string> PrintedConnectionStrings(string verb, ExternalAlertSourceAdmin.Outcome outcome, PostgresConfig? postgres, out string text)
    {
        var output = new StringWriter();
        var exit = DarlingCliCommands.PrintAlertSourceResult(
            verb,
            new ExternalAlertSourceAdmin.Result(outcome, "darling_src_orders", "PwForTheTest0123456789abcdefgh", outcome == ExternalAlertSourceAdmin.Outcome.NeedsDbaSql ? "SELECT 1;" : null),
            "orders", "Host=db.example;Port=5641;Database=darling;Username=darling;Password=ownerpw", output, new StringWriter(), postgres);
        Assert.Equal(0, exit);
        text = output.ToString();
        return text.Split('\n').Select(l => l.Trim()).Where(l => l.Contains("Password=PwForTheTest0123456789abcdefgh", StringComparison.Ordinal)).ToList();
    }

    public static IEnumerable<object?[]> EachStoreKind() => new[]
    {
        new object?[] { "managed, sources listed", ManagedWithRole("viewer,sources") },
        new object?[] { "managed, sources not listed", ManagedWithRole("viewer") },
        new object?[] { "managed, no roles", ManagedWithRole(null) },
        new object?[] { "bring your own", new PostgresConfig { Managed = false } },
        new object?[] { "no config", null },
    };

    /// <summary>#5654: on every kind of store, add and rotate print the local string and then one for another machine that is
    /// SSL Mode=VerifyFull with a root certificate (the managed store's server.crt, or the placeholder), so the string they print for
    /// another machine is never less strict than the remote viewer's.</summary>
    [Theory]
    [MemberData(nameof(EachStoreKind))]
    public void AddAndRotate_PrintAVerifyingStringForAnotherMachine_OnEveryKindOfStore(string kind, PostgresConfig? postgres)
    {
        var viewer = new NpgsqlConnectionStringBuilder(DarlingCliCommands.BuildViewerConnectionString("h", 5432, "viewer", "pw", "/c/server.crt"));
        foreach (var verb in new[] { "--add-alert-source", "--rotate-alert-source" })
        foreach (var outcome in new[] { ExternalAlertSourceAdmin.Outcome.Done, ExternalAlertSourceAdmin.Outcome.NeedsDbaSql })
        {
            var strings = PrintedConnectionStrings(verb, outcome, postgres, out var text);
            Assert.True(strings.Count == 2, kind + ": " + verb + " printed " + strings.Count + " strings");

            var remote = new NpgsqlConnectionStringBuilder(strings[1]);
            Assert.Equal(viewer.SslMode, remote.SslMode);
            Assert.Equal(SslMode.VerifyFull, remote.SslMode);
            Assert.False(string.IsNullOrWhiteSpace(remote.RootCertificate), kind);
            Assert.Equal("darling_src_orders", remote.Username);

            var managed = postgres is { Managed: true };
            Assert.Equal(managed, remote.RootCertificate!.EndsWith("server.crt", StringComparison.Ordinal));
            Assert.Equal(!managed, remote.RootCertificate == ExternalAlertSourceAdmin.RootCertificatePlaceholder);
            Assert.DoesNotContain("ownerpw", text, StringComparison.Ordinal);
            Assert.DoesNotContain("uses the address it reaches the store at", text, StringComparison.Ordinal);
        }
    }

    /// <summary>#5654: a managed store whose network roles do not list <c>sources</c> still gets the verifying string, with one line
    /// saying it works once <c>sources</c> is listed; a store that lists it gets no such line.</summary>
    [Theory]
    [InlineData("viewer", true)]
    [InlineData("viewer,sources", false)]
    public void TheManagedStore_WithoutSources_SaysTheStringWorksOnceSourcesIsListed(string role, bool line)
    {
        PrintedConnectionStrings("--add-alert-source", ExternalAlertSourceAdmin.Outcome.Done, ManagedWithRole(role), out var text);
        Assert.Equal(line, text.Contains("It works once \"sources\" is listed in the store's network roles", StringComparison.Ordinal));
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

    /// <summary>#5654: when the server log still records the owner's statements, the text says the password statement was written
    /// to it and names the settings, as the script's step 0 warning does.</summary>
    [Fact]
    public void TheResultText_NamesTheServerLogSettings_ThatStillRecordTheStatement()
    {
        var output = new StringWriter();
        DarlingCliCommands.PrintAlertSourceResult("--add-alert-source",
            new ExternalAlertSourceAdmin.Result(ExternalAlertSourceAdmin.Outcome.Done, "darling_src_orders", "Pw0123456789",
                ServerLogSettingsOn: "log_statement = ddl"),
            "orders", "Host=127.0.0.1", output, new StringWriter());
        Assert.Contains("(log_statement = ddl)", output.ToString(), StringComparison.Ordinal);
        Assert.Contains("written to the server log", output.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// #5654: a step after the commit that did not complete is reported after the committed result: the password is printed,
    /// the exit code is success, and the text never says the sources could not be changed.
    /// </summary>
    [Theory]
    [InlineData("--add-alert-source")]
    [InlineData("--rotate-alert-source")]
    public void AFollowUpFailure_IsReportedAfterTheCommittedResult(string verb)
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = DarlingCliCommands.PrintAlertSourceResult(verb,
            new ExternalAlertSourceAdmin.Result(ExternalAlertSourceAdmin.Outcome.Done, "darling_src_orders", "PwForTheTest0123456789abcdefgh",
                FollowUpFailure: "The change above is committed, but ending the open sessions failed (boom)."),
            "orders", "Host=127.0.0.1;Port=5432;Database=darling", output, error);

        Assert.Equal(0, exit);
        Assert.Contains("PwForTheTest0123456789abcdefgh", output.ToString(), StringComparison.Ordinal);
        Assert.Contains("committed, but ending the open sessions failed (boom)", error.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain("Could not change", output.ToString() + error.ToString(), StringComparison.Ordinal);
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
        /* The whole file, comments included: no comment in it writes a catalog function as a call, so nothing is filtered out. */
        var code = source;

        var unqualified = Regex.Matches(code,
                @"(?<![\w.])(count|max|min|shobj_description|current_database|current_setting|to_regclass|format|lower|upper|replace|length|"
                + @"array_\w+|string_\w+|set_config|quote_\w+|regexp_\w+|now|pg_\w+|has_\w+_privilege)\s*\(")
            .Select(m => m.Value).ToArray();
        Assert.Empty(unqualified);
        Assert.Matches(new Regex(@"\bFROM pg_catalog\.pg_roles\b"), code);
        Assert.DoesNotMatch(new Regex(@"\bFROM (pg_roles|pg_stat_activity|pg_authid|pg_settings)\b"), code);

        /* One raw BeginTransactionAsync, inside the helper that pins the search path; every verb calls the helper. */
        Assert.Single(Regex.Matches(code, @"\.BeginTransactionAsync\("));
        Assert.Contains("\"SET LOCAL search_path = pg_catalog, pg_temp\"", code, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(code, @"await BeginOwnerTransactionAsync\(owner, ct\)").Count);
    }

    /// <summary>
    /// #5654: the SQL the store owner runs from the shared role code (the provisioning block the add verb runs, its group sweep,
    /// and the group read the add and rotate verbs make after their commit) and the statement guards of the verbs call no
    /// function by a bare name. String literals and comments are taken out first; what is left is SQL, and every
    /// <c>name(</c> in it is a keyword or a column list, or is written <c>pg_catalog.name(</c> or <c>config.name(</c>. A new
    /// call without the schema fails this.
    /// </summary>
    [Fact]
    public void TheOwnerRunRoleSql_CallsNoFunctionByABareName()
    {
        string[] sql =
        [
            DarlingManagedRoles.BuildExternalAlertSourceRolesSql("config"),
            DarlingManagedRoles.AlertSourceGroupSweepSql("config", "group_role.oid"),
            DarlingManagedRoles.AlertSourceGroupExtrasSql("config", registryExists: true),
            DarlingManagedRoles.AlertSourceGroupExtrasSql("config", registryExists: false),
            DarlingManagedRoles.AlertSourceRegistryProbeSql,
            ExternalAlertSourceAdmin.BuildSettingsBlockSql(ExternalAlertSourceAdmin.ServerLogSettings, local: true),
            ExternalAlertSourceAdmin.DbaStatementTextGuardSql,
        ];
        var allowed = new System.Collections.Generic.HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "in", "exists", "coalesce", "as", "not", "or", "and", "from", "join", "on", "where", "when", "then", "else", "filter", "values",
            "any", "all", "walk", "closure", "v", "t",
        };

        foreach (var text in sql)
        {
            /* Comments first (their prose may hold an apostrophe), then literals. */
            var code = Regex.Replace(text, @"/\*.*?\*/", " ", RegexOptions.Singleline);
            code = Regex.Replace(code, @"(?m)^\s*--.*$", " ");
            code = Regex.Replace(code, @"'(?:[^']|'')*'", "''");
            var bare = Regex.Matches(code, @"(?<![\w.$""])([A-Za-z_]\w*)\s*\(")
                .Select(m => m.Groups[1].Value)
                .Where(name => !allowed.Contains(name))
                .ToArray();
            Assert.True(bare.Length == 0, "Called by a bare name: " + string.Join(", ", bare) + Environment.NewLine + text);
        }

        /* The group read after the commit runs in its own transaction with the same search path as the verbs'. */
        var roles = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedRoles.cs"));
        var read = roles[roles.IndexOf("ReadAlertSourceGroupExtrasAsync(NpgsqlConnection connection", StringComparison.Ordinal)..];
        read = read[..read.IndexOf("return extras;", StringComparison.Ordinal)];
        Assert.Contains("BeginTransactionAsync(ct)", read, StringComparison.Ordinal);
        Assert.Contains("\"SET LOCAL search_path = pg_catalog, pg_temp\"", read, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(read, @"new NpgsqlCommand\([^;]*connection, transaction\)").Count);
    }

    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        return dir!.FullName;
    }

    /// <summary>
    /// #5654: the statements printed for a database administrator that carry a verifier (add and rotate) begin with the guards:
    /// utility statement tracking off, the server log settings off, and the check that stops while tracking is still on, all
    /// before the password statement.
    /// </summary>
    [Fact]
    public void TheDbaStatementsWithAVerifier_BeginWithTheStatementTextGuards()
    {
        foreach (var dba in new[]
                 {
                     ExternalAlertSourceAdmin.BuildAddSqlForDba("darling_src_orders", "SCRAM-SHA-256$v", "darling", 17),
                     ExternalAlertSourceAdmin.BuildRotateSqlForDba("darling_src_orders", "SCRAM-SHA-256$v"),
                 })
        {
            var password = dba.IndexOf("PASSWORD 'SCRAM-SHA-256$v'", StringComparison.Ordinal);
            Assert.True(password > 0);
            var guards = dba[..password];
            Assert.StartsWith("-- ", dba, StringComparison.Ordinal);
            Assert.Contains("SET pg_stat_statements.track_utility = off;", guards, StringComparison.Ordinal);
            Assert.Contains("SET pg_stat_monitor.pgsm_track_utility = off;", guards, StringComparison.Ordinal);
            foreach (var (setting, value) in ExternalAlertSourceAdmin.ServerLogSettings)
            {
                Assert.Contains($"('{setting}', '{value}')", guards, StringComparison.Ordinal);
            }

            Assert.Contains("('log_statement', 'none')", guards, StringComparison.Ordinal);
            Assert.Contains("pg_catalog.set_config(one.name, one.value, false)", guards, StringComparison.Ordinal);
            Assert.Contains("RAISE EXCEPTION", guards, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("SET pg_stat", ExternalAlertSourceAdmin.BuildRemoveSqlForDba("darling_src_orders", "darling"), StringComparison.Ordinal);
    }

    /// <summary>#5654: the server log settings the verbs turn off are the list step 0 of the shipped script turns off.</summary>
    [Fact]
    public void TheServerLogSettings_AreTheScriptsStepZeroList()
    {
        var script = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "tools", "provision-roles.sql"));
        foreach (var (setting, value) in ExternalAlertSourceAdmin.ServerLogSettings)
        {
            Assert.Contains($"('{setting}', '{value}')", script, StringComparison.Ordinal);
        }

        foreach (var setting in ExternalAlertSourceAdmin.StatementTrackingSettings)
        {
            Assert.Contains($"SET {setting} = off;", script, StringComparison.Ordinal);
        }
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
    public void TheNetworkNote_ForTheManagedStore_SaysTheLineIsOutNow_OrWhenItStopsBeingAdmitted()
    {
        var postgres = new PostgresConfig { Managed = true, Network = new PostgresNetworkConfig { Listen = "192.0.2.10", AllowFrom = "192.0.2.0/24", Role = "viewer,sources" } };
        var note = string.Join("\n", DarlingCliCommands.BuildAlertSourceNetworkNote(postgres, ["store_owner"]));
        Assert.Contains("'store_owner'", note, StringComparison.Ordinal);
        Assert.Contains("now holds no +darling_alert_sources line", note, StringComparison.Ordinal);
        Assert.DoesNotContain("reject", note, StringComparison.Ordinal);
        Assert.Single(DarlingCliCommands.BuildAlertSourceNetworkNote(postgres, []));

        /* When the verb could not take the line out, the note says it stays live until the service restarts. */
        var failed = string.Join("\n", DarlingCliCommands.BuildAlertSourceNetworkNote(postgres, ["store_owner"], "permission denied for function pg_reload_conf"));
        Assert.Contains("(permission denied for function pg_reload_conf)", failed, StringComparison.Ordinal);
        Assert.Contains("stays live, and admits these roles from the allowed range, until the service restarts", failed, StringComparison.Ordinal);
        Assert.DoesNotContain("now holds no", failed, StringComparison.Ordinal);
    }

    /* #5654: pg_hba reads all, sameuser, samerole, samegroup and replication as keywords unless they are quoted, so every name
       the service writes or prints into a pg_hba line is quoted when it is one of them, or is not plain lower case. */
    [Theory]
    [InlineData("viewer", "viewer")]
    [InlineData("store_owner2", "store_owner2")]
    [InlineData("all", "\"all\"")]
    [InlineData("sameuser", "\"sameuser\"")]
    [InlineData("samerole", "\"samerole\"")]
    [InlineData("samegroup", "\"samegroup\"")]
    [InlineData("replication", "\"replication\"")]
    [InlineData("Mixed Case", "\"Mixed Case\"")]
    [InlineData("+group", "\"+group\"")]
    public void APgHbaName_IsQuoted_WhenItIsAKeywordOrNotPlain(string name, string expected)
        => Assert.Equal(expected, DarlingManagedPostgres.QuotePgHbaName(name));

    [Fact]
    public void TheNetworkNote_ForABringYourOwnStore_QuotesARoleNamedAll_InItsRejectLine()
    {
        var note = string.Join("\n", DarlingCliCommands.BuildAlertSourceNetworkNote(new PostgresConfig { Managed = false }, ["all"]));
        Assert.Contains("  hostssl <database> \"all\" <range> reject", note, StringComparison.Ordinal);
        Assert.DoesNotContain("<database> all <range>", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheManagedLines_KeepTheirFixedNames()
    {
        Assert.Equal("hostssl darling viewer 192.0.2.0/24 scram-sha-256", DarlingManagedPostgres.BuildNetworkPgHbaLine("viewer", "192.0.2.0/24"));
        Assert.Equal("hostssl darling admin 192.0.2.0/24 scram-sha-256", DarlingManagedPostgres.BuildNetworkPgHbaLine("admin", "192.0.2.0/24"));
        Assert.Equal("hostssl darling +darling_alert_sources 192.0.2.0/24 scram-sha-256", DarlingManagedPostgres.BuildNetworkPgHbaLine("sources", "192.0.2.0/24"));
    }

    /* #5654: the add and rotate verbs take the alert sources line out of the managed block at once; the other lines stay, and a
       block left with no line is removed. */
    [Fact]
    public void RemovingTheAlertSourceLine_KeepsTheOtherManagedLines_AndRemovesAnEmptyBlock()
    {
        var both = DarlingManagedPostgres.ReconcilePgHba("local all all trust\n",
            DarlingManagedPostgres.BuildNetworkPgHbaLines(["viewer", "sources"], "192.0.2.0/24"));
        var (text, removed) = DarlingManagedPostgres.RemoveAlertSourcePgHbaLine(both);
        Assert.True(removed);
        Assert.Contains("hostssl darling viewer 192.0.2.0/24 scram-sha-256", text, StringComparison.Ordinal);
        Assert.DoesNotContain("+darling_alert_sources", text, StringComparison.Ordinal);
        Assert.Contains("local all all trust", text, StringComparison.Ordinal);
        Assert.Equal(DarlingManagedPostgres.ReconcilePgHba(both, DarlingManagedPostgres.BuildNetworkPgHbaLines(["viewer"], "192.0.2.0/24")), text);

        var only = DarlingManagedPostgres.ReconcilePgHba("local all all trust\n",
            DarlingManagedPostgres.BuildNetworkPgHbaLines(["sources"], "192.0.2.0/24"));
        var (emptied, removedOnly) = DarlingManagedPostgres.RemoveAlertSourcePgHbaLine(only);
        Assert.True(removedOnly);
        Assert.DoesNotContain(DarlingManagedPostgres.PgHbaBeginMarker, emptied, StringComparison.Ordinal);
        Assert.Equal("local all all trust\n", emptied);

        var (unchanged, none) = DarlingManagedPostgres.RemoveAlertSourcePgHbaLine(text);
        Assert.False(none);
        Assert.Equal(text, unchanged);
    }

    /* #5654: with only "sources" configured and another member in the group, the start admits no role, so the managed block is
       removed; the check of the live rules then looks for the left-out line instead of passing on zero of zero. */
    [Fact]
    public void ASourcesOnlyStore_WithAnExtraMember_RemovesTheBlock_AndTheCheckFailsWhileTheLineIsLive()
    {
        string[] configured = [DarlingManagedPostgres.AlertSourcesNetworkToken];
        var admitted = DarlingManagedPostgres.AdmittedNetworkRoles(configured, ["store_owner"]);
        Assert.Empty(admitted);
        var current = DarlingManagedPostgres.ReconcilePgHba("local all all trust\n", DarlingManagedPostgres.BuildNetworkPgHbaLines(configured, "192.0.2.0/24"));
        var updated = DarlingManagedPostgres.ReconcilePgHba(current, DarlingManagedPostgres.BuildNetworkPgHbaLines(admitted, "192.0.2.0/24"));
        Assert.DoesNotContain(DarlingManagedPostgres.PgHbaBeginMarker, updated, StringComparison.Ordinal);

        Assert.Equal(DarlingManagedPostgres.PgHbaVerdict.LeftOutRoleLive, DarlingManagedPostgres.ExposedPgHbaVerdict(admitted.Count, 0, 1, 1));
        Assert.Equal(DarlingManagedPostgres.PgHbaVerdict.Ok, DarlingManagedPostgres.ExposedPgHbaVerdict(admitted.Count, 0, 1, 0));
        Assert.Equal(DarlingManagedPostgres.PgHbaVerdict.AdmittedRoleMissing, DarlingManagedPostgres.ExposedPgHbaVerdict(2, 1, 0, 0));
        Assert.Equal(DarlingManagedPostgres.PgHbaVerdict.Ok, DarlingManagedPostgres.ExposedPgHbaVerdict(2, 2, 0, 0));
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
    public void TheResultText_ShowsThePasswordOnlyInsideTheConnectionStrings_AndOnceEach()
    {
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = DarlingCliCommands.PrintAlertSourceResult(
            "--add-alert-source",
            new ExternalAlertSourceAdmin.Result(ExternalAlertSourceAdmin.Outcome.Done, "darling_src_orders", "PwForTheTest0123456789abcdefgh"),
            "orders", "Host=127.0.0.1;Port=5432;Database=darling;Username=darling;Password=ownerpw", output, error);

        Assert.Equal(0, exit);
        /* #5654: the local string and the one for another machine, each with the password once. */
        Assert.Equal(2, Regex.Matches(output.ToString(), "PwForTheTest0123456789abcdefgh").Count);
        Assert.Equal(2, Regex.Matches(output.ToString(), "Password=PwForTheTest0123456789abcdefgh").Count);
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
