/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#9641: who is served the alert source tools. The three writes need the server admin switch as add_servers does, and the list is a read.</summary>
public sealed class AlertSourceToolsPolicyTests
{
    private static readonly string[] Writes = ["add_alert_source", "rotate_alert_source", "remove_alert_source"];

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(true, false, false)] /* writes alone: the tools create and drop logins, so they are not served */
    [InlineData(true, true, true)]
    public void TheWritesAreServedOnlyWithWritesAndServerAdminBothOn(bool writes, bool serverAdmin, bool served)
    {
        var policy = new DarlingMcpWritePolicy.Effective(writes, serverAdmin && writes);

        foreach (var tool in Writes)
        {
            Assert.Equal(served, policy.Offers(tool));
            Assert.True(DarlingMcpWritePolicy.IsWrite(tool));
            Assert.True(DarlingMcpWritePolicy.IsServerAdmin(tool));
        }
    }

    [Fact]
    public void TheListIsServedUnderEveryPolicy()
    {
        Assert.True(DarlingMcpWritePolicy.Effective.Off.Offers("list_alert_sources"));
        Assert.False(DarlingMcpWritePolicy.IsWrite("list_alert_sources"));
    }
}

/// <summary>
/// #9641 against a live store, through the real tool bodies and the owner data source they run on. The provisioning batch creates the fixed
/// <c>admin</c>, <c>viewer</c> and <c>mcp</c> roles, which are cluster-wide, so the class is in the <c>pg-cluster-roles</c> collection.
/// </summary>
[Collection("pg-cluster-roles")]
public sealed class AlertSourceToolsLiveTests
{
    /* #1776 own-store: the test mints its own scratch database through ScratchPostgres and a per-run source name; the fixed
       admin/viewer/mcp roles are why the class sits in the pg-cluster-roles collection. */
    private readonly string _runSuffix = Guid.NewGuid().ToString("N")[..8];

    private sealed class CaptureLogger : ILogger
    {
        public List<string> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            Lines.Add(formatter(state, exception));
            if (state is IEnumerable<KeyValuePair<string, object?>> values)
                Lines.AddRange(values.Select(v => $"{v.Key}={v.Value}"));
            if (exception is not null)
                Lines.Add(exception.ToString());
        }
    }

    private static async Task<string> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture) ?? string.Empty;
    }

    private static string PasswordIn(string report)
    {
        var match = Regex.Match(report, @"Password=([^;\r\n]+)");
        Assert.True(match.Success, "The report carries a connection string with a password.");
        return match.Groups[1].Value;
    }

    [Fact]
    public async Task AddRotateListRemove_RunThroughTheTools_AndTheAnswerHoldsThePasswordOnceAndNothingElseDoes()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source tool tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var source = "tool_" + _runSuffix;
        var login = ExternalAlertSourceAdmin.RoleNameFor(source);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            Assert.SkipWhen(
                await ScalarAsync(owner, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'mcp'))", ct) == "True",
                "A cluster-wide admin/viewer/mcp role already exists on this rig; the managed provisioning batch would adopt it.");
            var ownerRole = new NpgsqlConnectionStringBuilder(scratch.ConnectionString).Username ?? "darling";
            await using (var provision = new NpgsqlCommand(
                DarlingManagedRoles.BuildProvisioningSql(
                    ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp,
                    15, PasswordReassert.All, ProvisioningTarget.ComposeStore(ownerRole, scratch.DatabaseName)), owner))
            {
                await provision.ExecuteNonQueryAsync(ct);
            }

            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var logs = new CaptureLogger();
            var postgres = new PostgresConfig();

            Task<string> Change(string tool, string verb, string name) =>
                DarlingMcpAlertSourceTools.ChangeAsync(tool, verb, dataSource, postgres, name, logs, ct);

            Assert.Equal("empty", (string?)JsonNode.Parse(await DarlingMcpAlertSourceTools.ListAlertSources(dataSource, ct))!["status"]);

            var added = JsonNode.Parse(await Change("add_alert_source", "--add-alert-source", source))!.AsObject();
            Assert.Equal("added", (string?)added["status"]);
            Assert.Equal(login, (string?)added["login"]);
            var report = (string)added["report"]!;
            var firstPassword = PasswordIn(report);
            Assert.Equal("1", await ScalarAsync(owner, $"SELECT count(*) FROM pg_roles WHERE rolname = '{login}'", ct));

            var again = JsonNode.Parse(await Change("add_alert_source", "--add-alert-source", source))!.AsObject();
            Assert.Equal("refused", (string?)again["status"]);
            Assert.Equal("already_registered", (string?)again["reason"]);
            Assert.DoesNotContain(firstPassword, again.ToJsonString(), StringComparison.Ordinal);

            var listed = JsonNode.Parse(await DarlingMcpAlertSourceTools.ListAlertSources(dataSource, ct))!.AsObject();
            Assert.Equal("ok", (string?)listed["status"]);
            Assert.Equal(source, (string?)listed["sources"]![0]!["source"]);
            Assert.DoesNotContain(firstPassword, listed.ToJsonString(), StringComparison.Ordinal);

            var rotated = JsonNode.Parse(await Change("rotate_alert_source", "--rotate-alert-source", source))!.AsObject();
            Assert.Equal("rotated", (string?)rotated["status"]);
            var secondPassword = PasswordIn((string)rotated["report"]!);
            Assert.NotEqual(firstPassword, secondPassword);

            /* The password is in the answer and in nothing else: not a log line, and not a row the store keeps. */
            foreach (var password in new[] { firstPassword, secondPassword })
            {
                Assert.NotEmpty(logs.Lines);
                Assert.DoesNotContain(logs.Lines, line => line.Contains(password, StringComparison.Ordinal));
                Assert.Equal("0", await ScalarAsync(owner,
                    $"SELECT count(*) FROM config.external_alert_source AS s WHERE s::text LIKE '%' || $${password}$$ || '%'", ct));
            }

            var removed = JsonNode.Parse(await Change("remove_alert_source", "--remove-alert-source", source))!.AsObject();
            Assert.Equal("removed", (string?)removed["status"]);
            Assert.Equal("0", await ScalarAsync(owner, $"SELECT count(*) FROM pg_roles WHERE rolname = '{login}'", ct));
            Assert.Equal("empty", (string?)JsonNode.Parse(await DarlingMcpAlertSourceTools.ListAlertSources(dataSource, ct))!["status"]);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(
                    ["admin", "viewer", "web", "mcp", login, DarlingManagedRoles.AlertSourceGroupRoleName], _runSuffix, cleanupCt));
        }
    }

    [Fact]
    public async Task ARefusal_NamesWhyAndChangesNothing_AndNoPasswordIsMade()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres (+TimescaleDB) connection string, owner/superuser, to run the alert source tool tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var logs = new CaptureLogger();

            async Task<JsonObject> Call(string tool, string verb, string? name) =>
                JsonNode.Parse(await DarlingMcpAlertSourceTools.ChangeAsync(tool, verb, dataSource, new PostgresConfig(), name, logs, ct))!.AsObject();

            /* A bad name never reaches the store, whatever the verb. */
            foreach (var bad in new[] { "X", "Bad Name", "1abc", "has-dash", "line\n", string.Empty })
            {
                var answer = await Call("add_alert_source", "--add-alert-source", bad);
                Assert.Equal("refused", (string?)answer["status"]);
                Assert.Equal("invalid_name", (string?)answer["reason"]);
            }

            var unknownRotate = await Call("rotate_alert_source", "--rotate-alert-source", "nosuch");
            Assert.Equal("not_registered", (string?)unknownRotate["reason"]);
            var unknownRemove = await Call("remove_alert_source", "--remove-alert-source", "nosuch");
            Assert.Equal("not_registered", (string?)unknownRemove["reason"]);

            /* An unconfirmed call for a name no source has ends at the check and never reaches the change: the change logs a line when
               it finishes, so a source added under that name a moment later would not be rotated or removed without a confirm. */
            var unconfirmedLogs = new CaptureLogger();
            foreach (var (tool, verb) in new[] { ("rotate_alert_source", "--rotate-alert-source"), ("remove_alert_source", "--remove-alert-source") })
            {
                var unconfirmed = JsonNode.Parse(await DarlingMcpAlertSourceTools.ChangeAsync(
                    tool, verb, dataSource, new PostgresConfig(), "nosuch", unconfirmedLogs, ct, confirm: false))!.AsObject();
                Assert.Equal("refused", (string?)unconfirmed["status"]);
                Assert.Equal("not_registered", (string?)unconfirmed["reason"]);
            }

            Assert.DoesNotContain(unconfirmedLogs.Lines, line => line.Contains("finished as", StringComparison.Ordinal));

            /* Before the worker has published the owner connection, nothing is attempted. */
            var missing = JsonNode.Parse(await DarlingMcpAlertSourceTools.ChangeAsync(
                "add_alert_source", "--add-alert-source", null, new PostgresConfig(), "netmon2", logs, ct))!.AsObject();
            Assert.Equal("no_owner_connection", (string?)missing["reason"]);
            Assert.Equal("0", await ScalarAsync(owner, "SELECT count(*) FROM config.external_alert_source", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
                await new LiveCleanupBatch(cleanup).RemoveRolesAsync(
                    ["admin", "viewer", "web", "mcp", DarlingManagedRoles.AlertSourceGroupRoleName], _runSuffix, cleanupCt));
        }
    }
}
