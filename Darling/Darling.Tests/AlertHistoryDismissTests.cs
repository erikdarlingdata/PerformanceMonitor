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
using System.Net;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web Alert History dismiss (#4843): the pure body parser, the role grant that lets the web host's
/// <c>viewer</c> identity write it, and the route itself. The live half connects AS a role holding exactly the
/// managed <c>viewer</c> grant on <c>config_alert_log</c> (the statement is lifted out of the managed
/// provisioning SQL, so a drift in it changes what these tests run), never as the owner.
/// </summary>
[Collection("live-postgres")]
public sealed class AlertHistoryDismissTests
{
    private static readonly string RunSuffix = Guid.NewGuid().ToString("N")[..8];
    private static readonly string DismissRole = "dismiss_only_" + RunSuffix;
    private const string RolePassword = "DismissTestPw0123456789abcdef01";

    private static string Cs()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the alert dismiss live tests.");
        return cs!;
    }

    /* ═══════════════════════════ the pure parser ═══════════════════════════ */

    [Fact]
    public void ParseDismissBody_DedupesAndKeepsFirstSeenOrder()
    {
        var ok = DarlingWebEndpoints.ParseDismissBody(
            """{"alerts":[{"alert_time":"2026-10-04T10:00:00","server_id":1,"metric_name":"High CPU"},{"alert_time":"2026-10-04T10:00:00Z","server_id":1,"metric_name":"High CPU"},{"alert_time":"2026-10-04T11:00:00","server_id":2,"metric_name":"Blocking"}]}""",
            out var keys, out var refusal);

        Assert.True(ok, refusal);
        Assert.Equal(2, keys.Count);
        Assert.Equal(1, keys[0].ServerId);
        Assert.Equal(2, keys[1].ServerId);
        Assert.Equal(DateTimeKind.Unspecified, keys[0].AlertTime.Kind);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("[]")]
    [InlineData("{}")]
    [InlineData("""{"alerts":[]}""")]
    [InlineData("""{"alerts":"x"}""")]
    [InlineData("""{"alerts":[{"alert_time":"2026-10-04T10:00:00","server_id":"1","metric_name":"m"}]}""")]
    [InlineData("""{"alerts":[{"alert_time":"nope","server_id":1,"metric_name":"m"}]}""")]
    [InlineData("""{"alerts":[{"alert_time":"2026-10-04T10:00:00","server_id":1,"metric_name":""}]}""")]
    [InlineData("""{"alerts":[{"alert_time":"2026-10-04T10:00:00","server_id":1}]}""")]
    [InlineData("""{"alerts":[7]}""")]
    [InlineData("""{"alerts":[],"alerts":[]}""")]
    public void ParseDismissBody_RefusesABadBody(string body)
    {
        Assert.False(DarlingWebEndpoints.ParseDismissBody(body, out _, out var refusal));
        Assert.False(string.IsNullOrEmpty(refusal));
    }

    [Fact]
    public void ParseDismissBody_CapsTheKeyCount()
    {
        string Body(int n) => "{\"alerts\":[" + string.Join(",", Enumerable.Range(0, n)
            .Select(i => $"{{\"alert_time\":\"2026-10-04T10:00:00\",\"server_id\":{i},\"metric_name\":\"m\"}}")) + "]}";

        Assert.True(DarlingWebEndpoints.ParseDismissBody(Body(DarlingWebEndpoints.MaxDismissKeys), out var keys, out _));
        Assert.Equal(DarlingWebEndpoints.MaxDismissKeys, keys.Count);
        Assert.False(DarlingWebEndpoints.ParseDismissBody(Body(DarlingWebEndpoints.MaxDismissKeys + 1), out _, out _));
    }

    /* ═══════════════════════════ the write gate ═══════════════════════════ */

    [Theory]
    [InlineData(true, "POST", true)]
    [InlineData(false, "POST", false)]
    [InlineData(false, "PUT", false)]
    [InlineData(false, "DELETE", false)]
    public void TheWriteGate_RefusesAReadOnlySeat_OnTheDismissRoute(bool canEdit, string method, bool expected) =>
        Assert.Equal(expected, DarlingWebSeat.IsRequestAllowed(
            new DarlingWebSeat("who", canEdit), method, "/api/alert-history/dismiss"));

    /* ═══════════════════════════ the grant text ═══════════════════════════ */

    [Fact]
    public void ManagedProvisioning_GrantsViewerExactlyTheDismissedColumn_AndMcpNothing()
    {
        var sql = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 15);

        Assert.Contains("GRANT UPDATE (dismissed) ON config.config_alert_log TO viewer;", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config.config_alert_log TO mcp", sql, StringComparison.Ordinal);

        /* No table-level UPDATE, INSERT or DELETE on the alert log for viewer, in any spelling. */
        var tableLevel = Regex.Matches(
            Regex.Replace(sql, @"(?m)^\s*--.*$", ""),
            @"GRANT\s+(?<spec>[^;(]*?)\s+ON\s+config\.config_alert_log\s+TO\s+[^;]*viewer[^;]*;");
        Assert.Empty(tableLevel);
    }

    /* ═══════════════════════════ live: AS the grant-only role ═══════════════════════════ */

    [Fact]
    public async Task TheColumnGrant_LetsTheRoleDismiss_AndNothingElse_AndLeavesTheReadOnlyProbeFalse()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Cs(), ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await CreateDismissOnlyRoleAsync(owner, ct);
            await Exec(owner, SeedSql, ct);
        }

        var bodySucceeded = false;
        try
        {
            await using var role = new NpgsqlConnection(RoleConnectionString(scratch.ConnectionString));
            await role.OpenAsync(ct);

            /* Negative control: the same role, same statement, BEFORE the shipped grant, is refused. This is what
               makes the success below attributable to the grant and not to a role that could always write. */
            var dismissKey = new AlertDismissKey(new DateTime(2026, 10, 4, 10, 0, 0), -7001, "High CPU");
            await using (var before = AlertDismissStore.CreateDismissCommand(
                NpgsqlDataSource.Create(RoleConnectionString(scratch.ConnectionString)), new[] { dismissKey }, 30))
            {
                var refused = await Assert.ThrowsAsync<PostgresException>(() => before.ExecuteNonQueryAsync(ct));
                Assert.Equal("42501", refused.SqlState);
            }

            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);
                await GrantDismissedColumnAsync(owner, ct);
            }

            /* The WPF read-only probe, verbatim, as the grant-only role: a column grant is not a table grant. */
            Assert.False(await Scalar<bool>(role, "SELECT has_table_privilege('config_alert_log', 'UPDATE')", ct));
            Assert.True(await Scalar<bool>(role, "SELECT has_column_privilege('config_alert_log', 'dismissed', 'UPDATE')", ct));
            Assert.False(await Scalar<bool>(role, "SELECT has_column_privilege('config_alert_log', 'muted', 'UPDATE')", ct));

            /* The shared write core, as that role. */
            await using (var command = AlertDismissStore.CreateDismissCommand(
                NpgsqlDataSource.Create(RoleConnectionString(scratch.ConnectionString)), new[] { dismissKey }, 30))
            {
                Assert.Equal(1, await command.ExecuteNonQueryAsync(ct));
            }

            /* Every other column, INSERT and DELETE: 42501. */
            foreach (var statement in new[]
            {
                "UPDATE config_alert_log SET muted = TRUE",
                "UPDATE config_alert_log SET metric_name = 'x'",
                "UPDATE config_alert_log SET dismissed = FALSE, send_error = 'x'",
                "INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value) VALUES (now(), -1, 's', 'm', 1, 1)",
                "DELETE FROM config_alert_log",
            })
            {
                var ex = await Assert.ThrowsAsync<PostgresException>(() => Exec(role, statement, ct));
                Assert.Equal("42501", ex.SqlState);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DropRoleAsync);
        }
    }

    [Fact]
    public async Task TheRoute_Answers200_400_415_AsTheGrantOnlyRole_AndReportsUnknownAndAlreadyDismissed()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Cs(), ct);
        await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
        {
            await owner.OpenAsync(ct);
            await PgMigrations.MigrateAsync(owner, ct);
            await ProvisionDismissOnlyRoleAsync(owner, ct);
            await Exec(owner, SeedSql, ct);
        }

        var bodySucceeded = false;
        try
        {
            await using var ds = NpgsqlDataSource.Create(RoleConnectionString(scratch.ConnectionString));
            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
                WebRootPath = "wwwroot",
            });
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(ds);
            await using var app = builder.Build();
            DarlingWebEndpoints.MapAll(app, ds, new CollectorRuntimeState(), new CapturingTestLogger());
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<(int Status, string Body)> Post(string? contentType, string body)
            {
                var context = await server.SendAsync(request =>
                {
                    request.Request.Method = "POST";
                    request.Request.Path = "/api/alert-history/dismiss";
                    request.Request.Headers.Host = "localhost";
                    if (contentType is not null)
                    {
                        request.Request.ContentType = contentType;
                    }

                    request.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(body));
                });
                using var reader = new StreamReader(context.Response.Body);
                return (context.Response.StatusCode, await reader.ReadToEndAsync(ct));
            }

            static string Alert(string time, int server, string metric) =>
                $"{{\"alert_time\":\"{time}\",\"server_id\":{server},\"metric_name\":\"{metric}\"}}";

            /* 415 and 400 never reach the store. */
            Assert.Equal(StatusCodes.Status415UnsupportedMediaType, (await Post("text/plain", "{}")).Status);
            Assert.Equal(StatusCodes.Status415UnsupportedMediaType, (await Post(null, "{}")).Status);
            Assert.Equal(StatusCodes.Status400BadRequest, (await Post("application/json", "nope")).Status);
            Assert.Equal(StatusCodes.Status400BadRequest, (await Post("application/json", "{\"alerts\":[]}")).Status);
            Assert.Equal(StatusCodes.Status400BadRequest, (await Post("application/json", "{\"alerts\":[],\"alerts\":[]}")).Status);

            /* Two real rows (one duplicated), one unknown key. */
            var first = await Post("application/json; charset=utf-8", "{\"alerts\":[" + string.Join(",",
                Alert("2026-10-04T10:00:00", -7001, "High CPU"),
                Alert("2026-10-04T10:00:00", -7001, "High CPU"),
                Alert("2026-10-04T10:05:00", -7001, "Blocking"),
                Alert("2026-10-04T10:00:00", -7999, "No Such")) + "]}");
            Assert.Equal(StatusCodes.Status200OK, first.Status);
            using (var doc = JsonDocument.Parse(first.Body))
            {
                var root = doc.RootElement;
                Assert.Equal(3, root.GetProperty("requested").GetInt32());
                Assert.Equal(2, root.GetProperty("dismissed").GetInt32());
                Assert.Equal(0, root.GetProperty("already_dismissed").GetInt32());
                Assert.Equal(1, root.GetProperty("unknown").GetInt32());
            }

            /* Repeating it dismisses nothing new: both rows are already dismissed. */
            var again = await Post("application/json", "{\"alerts\":[" + Alert("2026-10-04T10:00:00", -7001, "High CPU") + "]}");
            using (var doc = JsonDocument.Parse(again.Body))
            {
                Assert.Equal(0, doc.RootElement.GetProperty("dismissed").GetInt32());
                Assert.Equal(1, doc.RootElement.GetProperty("already_dismissed").GetInt32());
            }

            await using var verify = new NpgsqlConnection(scratch.ConnectionString);
            await verify.OpenAsync(ct);
            Assert.Equal(2L, await Scalar<long>(verify, "SELECT count(*) FROM config_alert_log WHERE server_id = -7001 AND dismissed", ct));
            Assert.Equal(1L, await Scalar<long>(verify, "SELECT count(*) FROM config_alert_log WHERE server_id = -7001 AND NOT dismissed", ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DropRoleAsync);
        }
    }

    /* ═══════════════════════════ helpers ═══════════════════════════ */

    private const string SeedSql = @"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value) VALUES
 ('2026-10-04 10:00:00', -7001, 'srv', 'High CPU', 90, 80),
 ('2026-10-04 10:05:00', -7001, 'srv', 'Blocking', 5, 1),
 ('2026-10-04 10:10:00', -7001, 'srv', 'Deadlocks', 2, 1)";

    private static Task CreateDismissOnlyRoleAsync(NpgsqlConnection owner, CancellationToken ct) =>
        Exec(owner, $@"
CREATE ROLE {DismissRole} LOGIN NOSUPERUSER PASSWORD '{RolePassword}';
GRANT USAGE ON SCHEMA config TO {DismissRole};
GRANT SELECT ON config.config_alert_log TO {DismissRole};", ct);

    /// <summary>Gives the role ONLY the managed viewer's alert-log grant, lifted from the managed provisioning SQL
    /// so the test runs the shipped statement.</summary>
    private static async Task GrantDismissedColumnAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        var managed = DarlingManagedRoles.BuildProvisioningSql(
            ProvisioningTestSecrets.Admin, ProvisioningTestSecrets.Viewer, ProvisioningTestSecrets.Mcp, 15);
        var grant = Regex.Match(managed, @"GRANT UPDATE \(dismissed\) ON config\.config_alert_log TO viewer;");
        Assert.True(grant.Success, "the managed provisioning SQL no longer grants viewer the dismissed column");

        await Exec(owner, grant.Value.Replace(" TO viewer;", " TO " + DismissRole + ";", StringComparison.Ordinal), ct);
    }

    private static async Task ProvisionDismissOnlyRoleAsync(NpgsqlConnection owner, CancellationToken ct)
    {
        await CreateDismissOnlyRoleAsync(owner, ct);
        await GrantDismissedColumnAsync(owner, ct);
    }

    private static Task DropRoleAsync(NpgsqlConnection owner, CancellationToken ct) =>
        Exec(owner, $"DROP OWNED BY {DismissRole}; DROP ROLE IF EXISTS {DismissRole}", ct);

    private static string RoleConnectionString(string baseConnectionString) =>
        new NpgsqlConnectionStringBuilder(baseConnectionString)
        {
            Username = DismissRole,
            Password = RolePassword,
            SearchPath = "config,public",
            Pooling = false,
        }.ConnectionString;

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> Scalar<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }
}
