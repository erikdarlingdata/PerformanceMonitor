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
        var add = ExternalAlertSourceAdmin.BuildAddSqlForDba("darling_src_orders", verifier, "darling");

        Assert.Contains("CREATE ROLE \"darling_src_orders\"", add);
        Assert.Contains($"PASSWORD '{verifier}'", add);
        Assert.Contains($"COMMENT ON ROLE \"darling_src_orders\" IS '{DarlingManagedRoles.AlertSourceRoleMarker}'", add);
        Assert.Contains("GRANT EXECUTE ON FUNCTION config.submit_external_alert(", add);
        Assert.Contains("NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS NOINHERIT", add);
        Assert.DoesNotContain(password, add);

        var remove = ExternalAlertSourceAdmin.BuildRemoveSqlForDba("darling_src_orders", "darling");
        Assert.Contains("DROP ROLE \"darling_src_orders\"", remove);
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
