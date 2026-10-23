/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The threshold-overrides web routes (#5916 part 4) on a live store, run as the managed <c>admin</c> role (a change is accepted,
/// stamped with the seat's name, and a bad key or bound is a 400 before any write) and as the managed <c>viewer</c> role (the
/// list works, a change answers 403 naming the admin login and leaves the row alone), the grants replayed from the shipped
/// provisioning SQL.
/// </summary>
public sealed class ThresholdOverrideWebLiveTests
{
    private const string RolePassword = "threshold-web-live-pw";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<string?> ScalarAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Runs <paramref name="body"/> against the web routes with the web host's data source logging in as a role holding
    /// exactly what the managed <paramref name="managedRole"/> holds, on a scratch store with two servers and one stored override
    /// (srv-8 cpu.threshold_percent = 77, set by "seed").</summary>
    private static async Task WithRoutesAsync(
        string managedRole, Func<NpgsqlDataSource, Func<string, string?, string?, Task<(int Status, JsonNode? Body)>>, CancellationToken, Task> body,
        bool armed = false)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the threshold override web live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "thr_seat_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var owner = await SeededAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, managedRole))
            {
                await ExecAsync(owner, statement, ct);
            }

            var asSeat = new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var webLogin = NpgsqlDataSource.Create(asSeat);

            /* #6600: the managed-shaped store. The web reads as the role above; a signed-in admin's change and its access check use the arm, a
               second small pool on a role that holds the admin role's grants. */
            var adminArm = await AdminArmLive.BuildAsync(owner, scratch, roleName, armed ? AdminArmShape.Admin : AdminArmShape.None, ct);

            using var armToDispose = adminArm;

            var builder = WebApplication.CreateBuilder(new WebApplicationOptions
            {
                ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
                WebRootPath = "wwwroot",
            });
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            builder.Services.AddSingleton(webLogin);
            await using var app = builder.Build();
            if (armed)
            {
                app.Use(async (context, next) =>
                {
                    context.Items[DarlingWebSeat.HttpContextItemKey] = new DarlingWebSeat("alice", true);
                    await next(context);
                });
            }

            DarlingWebEndpoints.MapAll(app, webLogin, new CollectorRuntimeState(), new CapturingTestLogger(),
                postgresConfig: armed ? new PostgresConfig { Managed = true } : null, adminArm: adminArm);
            await app.StartAsync(ct);
            using var server = app.GetTestServer();

            async Task<(int Status, JsonNode? Body)> Send(string method, string? contentType, string? json)
            {
                var context = await server.SendAsync(request =>
                {
                    request.Request.Method = method;
                    request.Request.Path = "/api/threshold-overrides";
                    request.Request.Headers.Host = "localhost";
                    if (contentType is not null)
                    {
                        request.Request.ContentType = contentType;
                    }

                    request.Request.Body = new MemoryStream(Encoding.UTF8.GetBytes(json ?? ""));
                });
                using var reader = new StreamReader(context.Response.Body);
                var text = await reader.ReadToEndAsync(ct);
                return (context.Response.StatusCode, string.IsNullOrWhiteSpace(text) ? null : JsonNode.Parse(text));
            }

            await body(owner, Send, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (connection, token) =>
            {
                NpgsqlConnection.ClearAllPools();
                await using var drop = new NpgsqlCommand(AdminArmLive.DropRolesSql(roleName), connection);
                await drop.ExecuteNonQueryAsync(token);
            });
        }
    }

    private static async Task<NpgsqlDataSource> SeededAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        var source = NpgsqlDataSource.Create(builder.ConnectionString);
        await ExecAsync(source, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h7'), (8, 'srv-8', 'h8')", ct);
        await ExecAsync(source, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
            + "VALUES (7, 'srv-7', 'srv-7', TRUE, 15, now(), now()), (8, 'srv-8', 'srv-8', TRUE, 15, now(), now())", ct);
        await ExecAsync(source, "INSERT INTO config.config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING", ct);
        await ExecAsync(source, "INSERT INTO config.config_threshold_overrides (server_id, threshold_key, value, set_by) VALUES (8, 'cpu.threshold_percent', 77, 'seed')", ct);
        return source;
    }

    private const string Json = "application/json";

    [Fact]
    public async Task AsTheAdminRole_TheListShowsTheOverrideAndItsGlobalValue_AndAChangeIsAcceptedAndStamped()
    {
        await WithRoutesAsync("admin", async (owner, send, ct) =>
        {
            var list = await send("GET", null, null);
            Assert.Equal(200, list.Status);
            Assert.True(list.Body!["can_edit"]!.GetValue<bool>());
            var row = Assert.Single(list.Body["overrides"]!.AsArray())!;
            Assert.Equal("srv-8", row["server_name"]!.GetValue<string>());
            Assert.Equal("cpu.threshold_percent", row["key"]!.GetValue<string>());
            Assert.Equal(77, row["value"]!.GetValue<double>());
            Assert.Equal("seed", row["set_by"]!.GetValue<string>());
            Assert.Equal(80, row["global_value"]!.GetValue<double>());
            Assert.Contains(list.Body["servers"]!.AsArray(), s => s!["server_name"]!.GetValue<string>() == "srv-7");
            Assert.Contains(list.Body["keys"]!.AsArray(), k => k!["key"]!.GetValue<string>() == "pvs.threshold_percent" && k["database_scope"]!.GetValue<bool>());

            /* A new override and a change to the seeded one. */
            var added = await send("PUT", Json, """{"server_name":"srv-7","key":"cpu.threshold_percent","value":95}""");
            Assert.Equal(200, added.Status);
            Assert.Equal("ok", added.Body!["status"]!.GetValue<string>());
            Assert.Equal("95", await ScalarAsync(owner, "SELECT value FROM config.config_threshold_overrides WHERE server_id = 7", ct));
            var by = await ScalarAsync(owner, "SELECT set_by FROM config.config_threshold_overrides WHERE server_id = 7", ct);
            Assert.Matches("^[A-Za-z0-9._:-]{1,64}$", by!);
            Assert.Equal("ok", (await send("PUT", Json, """{"server_name":"srv-8","key":"cpu.threshold_percent","value":60}""")).Body!["status"]!.GetValue<string>());
            Assert.Equal("60", await ScalarAsync(owner, "SELECT value FROM config.config_threshold_overrides WHERE server_id = 8", ct));
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.config_threshold_overrides", ct));

            /* A database override is its own row; clear removes exactly the row named. */
            Assert.Equal(200, (await send("PUT", Json, """{"server_name":"srv-7","key":"pvs.threshold_percent","value":70,"database_name":"shop"}""")).Status);
            Assert.Equal("3", await ScalarAsync(owner, "SELECT count(*) FROM config.config_threshold_overrides", ct));
            var cleared = await send("PUT", Json, """{"server_name":"srv-7","key":"pvs.threshold_percent","database_name":"shop","clear":true}""");
            Assert.Equal(200, cleared.Status);
            Assert.Equal("cleared", cleared.Body!["status"]!.GetValue<string>());
            Assert.Equal("2", await ScalarAsync(owner, "SELECT count(*) FROM config.config_threshold_overrides", ct));
        });
    }

    [Fact]
    public async Task AsTheAdminRole_ABadKeyOrBoundOrBody_IsRefusedBeforeAnyWrite()
    {
        await WithRoutesAsync("admin", async (owner, send, ct) =>
        {
            var before = await ScalarAsync(owner, "SELECT string_agg(server_id || ':' || threshold_key || ':' || value, ',' ORDER BY id) FROM config.config_threshold_overrides", ct);

            foreach (var (json, reason) in new[]
            {
                ("""{"server_name":"srv-7","key":"cpu.nonsense","value":5}""", "not_in_registry"),
                ("""{"server_name":"srv-7","key":"self_alerts.enabled","value":1}""", "not_tunable"),
                ("""{"server_name":"srv-7","key":"cpu.threshold_percent","value":101}""", "out_of_bounds"),
                ("""{"server_name":"srv-7","key":"cpu.threshold_percent","value":0}""", "out_of_bounds"),
                ("""{"server_name":"srv-7","key":"cpu.threshold_percent","value":50,"database_name":"shop"}""", "database_scope"),
                ("""{"server_name":"srv-7","key":"cpu.threshold_percent"}""", "value_required"),
            })
            {
                var answer = await send("PUT", Json, json);
                Assert.Equal(400, answer.Status);
                Assert.Equal(reason, answer.Body!["reason"]!.GetValue<string>());
                Assert.False(string.IsNullOrWhiteSpace(answer.Body["error"]!.GetValue<string>()));
            }

            Assert.Equal(400, (await send("PUT", Json, """{"server_name":"srv-7","key":"cpu.threshold_percent","value":"high"}""")).Status);
            Assert.Equal(400, (await send("PUT", Json, """{"server_name":"srv-7","key":"cpu.threshold_percent","value":1.5}""")).Status);
            Assert.Equal(400, (await send("PUT", Json, """{"key":"cpu.threshold_percent","value":50}""")).Status);
            Assert.Equal(400, (await send("PUT", Json, "[1]")).Status);
            Assert.Equal(400, (await send("PUT", Json, "not json")).Status);
            Assert.Equal(415, (await send("PUT", "text/plain", """{"server_name":"srv-7","key":"cpu.threshold_percent","value":50}""")).Status);

            Assert.Equal(before, await ScalarAsync(owner, "SELECT string_agg(server_id || ':' || threshold_key || ':' || value, ',' ORDER BY id) FROM config.config_threshold_overrides", ct));
        });
    }

    /* #6600: the managed store's shape. The list reads as the viewer role, a signed-in admin's change goes through the arm, and the access
       flag is true; the row records the person. */
    [Fact]
    public async Task OnTheManagedStore_ASignedInAdminSeat_ChangesThroughTheArm_AndCanEditIsTrue()
    {
        await WithRoutesAsync("viewer", async (owner, send, ct) =>
        {
            var list = await send("GET", null, null);
            Assert.Equal(200, list.Status);
            Assert.True(list.Body!["can_edit"]!.GetValue<bool>());

            var answer = await send("PUT", Json, """{"server_name":"srv-7","key":"cpu.threshold_percent","value":95}""");
            Assert.Equal(200, answer.Status);
            Assert.Contains("alice", await ScalarAsync(owner, "SELECT set_by FROM config.config_threshold_overrides WHERE server_id = 7", ct), StringComparison.Ordinal);
        }, armed: true);
    }

    [Fact]
    public async Task AsTheViewerRole_TheListWorks_ButAChangeAnswers403NamingTheAdminLogin_AndChangesNothing()
    {
        await WithRoutesAsync("viewer", async (owner, send, ct) =>
        {
            var list = await send("GET", null, null);
            Assert.Equal(200, list.Status);
            Assert.False(list.Body!["can_edit"]!.GetValue<bool>());
            Assert.Equal("srv-8", Assert.Single(list.Body["overrides"]!.AsArray())!["server_name"]!.GetValue<string>());

            foreach (var json in new[]
            {
                """{"server_name":"srv-7","key":"cpu.threshold_percent","value":95}""",
                """{"server_name":"srv-8","key":"cpu.threshold_percent","value":60}""",
                """{"server_name":"srv-8","key":"cpu.threshold_percent","clear":true}""",
            })
            {
                var answer = await send("PUT", Json, json);
                Assert.Equal(403, answer.Status);
                Assert.Equal(DarlingWebThresholdOverrides.RefusedMessage(false), answer.Body!["error"]!.GetValue<string>());
                Assert.Contains("admin", answer.Body["error"]!.GetValue<string>());
            }

            Assert.Equal("77", await ScalarAsync(owner, "SELECT value FROM config.config_threshold_overrides WHERE server_id = 8", ct));
            Assert.Equal("1", await ScalarAsync(owner, "SELECT count(*) FROM config.config_threshold_overrides", ct));
        });
    }
}
