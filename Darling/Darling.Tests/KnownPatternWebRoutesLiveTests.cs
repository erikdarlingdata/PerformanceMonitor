/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The known-pattern web routes (#6334) through a real host over a scratch store, running as the managed <c>viewer</c> role (the
/// grants replayed from the shipped provisioning SQL, so the EXECUTE grants on the three store functions are what lets a write through):
/// an add, an edit and a delete round trip with the signed-in user stamped as the creator, the refusal text for each bad input with
/// nothing written, and a read-only seat refused every write by the host's real method gate.
/// </summary>
public sealed class KnownPatternWebRoutesLiveTests
{
    private const int ServerId = 7_633_401;

    /// <summary>Defined (<c>config_monitored_servers</c>, what the page's picker lists) with no <c>servers</c> row: added, not yet connected.</summary>
    private const int DefinedOnlyServerId = 7_633_402;

    /// <summary>Has a <c>servers</c> row and no definition: removed, its history kept.</summary>
    private const int RegistryOnlyServerId = 7_633_403;
    private const string ServerName = "LcKnownPatternSrv";
    private const string RolePassword = "known-pattern-live-pw";
    private static readonly CapturingTestLogger Log = new();

    private static async Task WithRoutesAsync(DarlingWebSeat seat, Func<HttpClient, PgKnownPatternStore, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the known-pattern web route live tests.");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "kp_seat_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            /* The write functions refuse a server id the registry (config_monitored_servers) does not hold. */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({ServerId}, 'kp-a', 'kp-a', TRUE) ON CONFLICT DO NOTHING");
            await DarlingMcpTestData.ExecAsync(connection, ct, $"INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ({DefinedOnlyServerId}, 'kp-b', 'kp-b', TRUE) ON CONFLICT DO NOTHING");
            await DarlingMcpTestData.RegisterServerAsync(connection, RegistryOnlyServerId, "kp-gone", ct);
            /* The three store functions are provisioning SQL, not a migration: create them as the store owner, before the viewer replay grants EXECUTE on them. */
            await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
            await DarlingMcpTestData.ExecAsync(connection, ct, DarlingManagedRoles.BuildKnownPatternFunctionsSql("config"));
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, "web"))
            {
                await ExecAsync(owner, statement, ct);
            }

            /* The replay leaves out every EXECUTE grant (the batch creates its functions itself), so the three this feature's batch grants the viewer are replayed by name. */
            await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.insert_known_pattern({DarlingManagedRoles.InsertKnownPatternSignature}) TO {roleName}", ct);
            await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.update_known_pattern({DarlingManagedRoles.UpdateKnownPatternSignature}) TO {roleName}", ct);
            await ExecAsync(owner, $"GRANT EXECUTE ON FUNCTION config.delete_known_pattern(bigint) TO {roleName}", ct);

            var asViewer =new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                SearchPath = PgSchemaGenerator.SearchPath,
                Pooling = false,
            }.ConnectionString;
            await using var webLogin = NpgsqlDataSource.Create(asViewer);

            var builder = WebApplication.CreateBuilder();
            builder.WebHost.UseTestServer();
            builder.Logging.ClearProviders();
            await using var app = builder.Build();

            /* The host's seat step, as DarlingWebHostService runs it: the resolved seat rides HttpContext.Items and a read-only seat is
               refused every mutating request by the real DarlingWebSeat.IsRequestAllowed. */
            app.Use(async (context, next) =>
            {
                context.Items[DarlingWebSeat.HttpContextItemKey] = seat;
                if (!DarlingWebSeat.IsRequestAllowed(seat, context.Request.Method, context.Request.Path.Value ?? "/"))
                {
                    context.Response.StatusCode = StatusCodes.Status403Forbidden;
                    context.Response.ContentType = "application/json; charset=utf-8";
                    await context.Response.WriteAsync(JsonSerializer.Serialize(new { error = DarlingWebEndpoints.ReadOnlyAccountText }));
                    return;
                }

                await next(context);
            });
            DarlingKnownPatternRoutes.Map(app, webLogin, Log);
            await app.StartAsync(ct);

            using var client = app.GetTestClient();
            await body(client, new PgKnownPatternStore(owner), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (connection, token) =>
            {
                NpgsqlConnection.ClearAllPools();
                await using var drop = new NpgsqlCommand($"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName}", connection);
                await drop.ExecuteNonQueryAsync(token);
            });
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(HttpStatusCode Status, JsonElement Body)> SendAsync(
        HttpClient client, HttpMethod method, string url, string? body, string mediaType = "application/json")
    {
        var ct = TestContext.Current.CancellationToken;
        using var request = new HttpRequestMessage(method, url) { Content = new StringContent(body ?? "", Encoding.UTF8, mediaType) };
        using var response = await client.SendAsync(request, ct);
        var text = await response.Content.ReadAsStringAsync(ct);
        return (response.StatusCode, JsonDocument.Parse(text).RootElement.Clone());
    }

    [Fact]
    public async Task AddEditDelete_RoundTrip_StampsTheSignedInUser_AndAnUnknownIdIs404()
    {
        var seat = new DarlingWebSeat("alice@example.com", true);
        await WithRoutesAsync(seat, async (client, store, ct) =>
        {
            var added = await SendAsync(client, HttpMethod.Post, "/api/known-patterns",
                $"{{\"server_id\":{ServerId},\"metric_name\":\"High CPU\",\"time_of_day\":\"02:00\",\"time_zone\":\"UTC\",\"window_minutes\":45,\"note\":\"Nightly backup.\",\"expires_at_utc\":\"2099-01-01T00:00:00Z\"}}");
            Assert.True(added.Status == HttpStatusCode.Created, added.Body + " " + Log.Joined);
            Assert.Equal("created", added.Body.GetProperty("status").GetString());
            var id = added.Body.GetProperty("id").GetInt64();

            var stored = Assert.Single(await store.LoadAllAsync(ct));
            Assert.Equal(id, stored.Id);
            Assert.Equal(ServerId, stored.ServerId);
            Assert.Equal("High CPU", stored.MetricName);
            Assert.Equal(new TimeOnly(2, 0), stored.TimeOfDay);
            Assert.Equal(45, stored.WindowMinutes);
            Assert.Equal("Nightly backup.", stored.Note);
            Assert.Equal(seat.EditorPrincipal, stored.CreatedBy);
            Assert.Equal(new DateTime(2099, 1, 1), stored.ExpiresAtUtc);

            /* PUT replaces the whole entry: the fields left out go back to any, and the creator stays. */
            var edited = await SendAsync(client, HttpMethod.Put, $"/api/known-patterns/{id}", "{\"note\":\"Moved to the weekend.\"}");
            Assert.Equal(HttpStatusCode.OK, edited.Status);
            Assert.Equal("updated", edited.Body.GetProperty("status").GetString());
            stored = Assert.Single(await store.LoadAllAsync(ct));
            Assert.Equal("Moved to the weekend.", stored.Note);
            Assert.Null(stored.ServerId);
            Assert.Null(stored.MetricName);
            Assert.Null(stored.TimeOfDay);
            Assert.Null(stored.ExpiresAtUtc);
            Assert.Equal(seat.EditorPrincipal, stored.CreatedBy);

            var deleted = await SendAsync(client, HttpMethod.Delete, $"/api/known-patterns/{id}", null);
            Assert.Equal(HttpStatusCode.OK, deleted.Status);
            Assert.Equal("deleted", deleted.Body.GetProperty("status").GetString());
            Assert.Empty(await store.LoadAllAsync(ct));

            Assert.Equal(HttpStatusCode.NotFound, (await SendAsync(client, HttpMethod.Delete, $"/api/known-patterns/{id}", null)).Status);
            Assert.Equal(HttpStatusCode.NotFound, (await SendAsync(client, HttpMethod.Put, $"/api/known-patterns/{id}", "{\"note\":\"x\"}")).Status);
        });
    }

    [Theory]
    [InlineData("{\"metric_name\":\"High CPU\"}", "A known pattern needs a note")]
    [InlineData("{\"note\":\"   \"}", "A known pattern needs a note")]
    [InlineData("{\"note\":\"__LONG__\"}", "the most a known pattern holds is 500")]
    [InlineData("{\"note\":\"n\",\"time_of_day\":\"02:00\",\"window_minutes\":721}", "window_minutes must be between 0 and 720")]
    [InlineData("{\"note\":\"n\",\"time_of_day\":\"02:00\",\"time_zone\":\"Not/AZone\"}", "is not known on this machine")]
    [InlineData("{\"note\":\"n\",\"server_id\":0}", "server_id must be a registered server's id")]
    [InlineData("{\"note\":\"n\",\"server_id\":4242}", "server_id 4242 is not a registered server")]
    [InlineData("{\"note\":\"n\",\"time_of_day\":\"soon\"}", "time_of_day must be a time")]
    [InlineData("{\"note\":\"n\",\"window_minutes\":\"x\"}", "window_minutes must be a whole number or null")]
    [InlineData("{\"note\":\"n\",\"created_by\":\"root\"}", "'created_by' is not a known-pattern field")]
    [InlineData("{\"note\":", "not valid JSON")]
    [InlineData("[1]", "must be a JSON object")]
    public async Task ABadBody_IsRefusedWithTheReason_OnAddAndOnEdit_AndWritesNothing(string body, string expected)
    {
        body = body.Replace("__LONG__", new string('x', 501), StringComparison.Ordinal);
        await WithRoutesAsync(new DarlingWebSeat("alice@example.com", true), async (client, store, ct) =>
        {
            var added = await SendAsync(client, HttpMethod.Post, "/api/known-patterns", body);
            Assert.Equal(HttpStatusCode.BadRequest, added.Status);
            Assert.Equal("invalid", added.Body.GetProperty("status").GetString());
            Assert.Contains(expected, added.Body.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain("(Parameter", added.Body.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.Empty(await store.LoadAllAsync(ct));

            var seeded = await store.InsertAsync(new PerformanceMonitor.Common.KnownPatterns.KnownPatternInput(null, null, null, null, null, "Keep me.", null), "seed", ct);
            var edited = await SendAsync(client, HttpMethod.Put, $"/api/known-patterns/{seeded}", body);
            Assert.Equal(HttpStatusCode.BadRequest, edited.Status);
            Assert.Contains(expected, edited.Body.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.DoesNotContain("(Parameter", edited.Body.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.Equal("Keep me.", Assert.Single(await store.LoadAllAsync(ct)).Note);
        });
    }

    [Fact]
    public async Task AServerTheEditorLists_IsAccepted_EvenBeforeItsFirstConnect_AndARemovedServerIsRefused()
    {
        await WithRoutesAsync(new DarlingWebSeat("alice@example.com", true), async (client, store, ct) =>
        {
            var added = await SendAsync(client, HttpMethod.Post, "/api/known-patterns", $"{{\"server_id\":{DefinedOnlyServerId},\"note\":\"Just added.\"}}");
            Assert.True(added.Status == HttpStatusCode.Created, added.Body + " " + Log.Joined);
            Assert.Equal(DefinedOnlyServerId, Assert.Single(await store.LoadAllAsync(ct)).ServerId);

            var gone = await SendAsync(client, HttpMethod.Post, "/api/known-patterns", $"{{\"server_id\":{RegistryOnlyServerId},\"note\":\"Removed.\"}}");
            Assert.Equal(HttpStatusCode.BadRequest, gone.Status);
            Assert.Contains($"server_id {RegistryOnlyServerId} is not a registered server", gone.Body.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.Single(await store.LoadAllAsync(ct));
        });
    }

    [Fact]
    public async Task AWriteWithoutAJsonContentType_Is415_AndChangesNothing()
    {
        await WithRoutesAsync(new DarlingWebSeat("alice@example.com", true), async (client, store, ct) =>
        {
            Assert.Equal(HttpStatusCode.UnsupportedMediaType, (await SendAsync(client, HttpMethod.Post, "/api/known-patterns", "{\"note\":\"x\"}", "text/plain")).Status);
            Assert.Empty(await store.LoadAllAsync(ct));

            var seeded = await store.InsertAsync(new PerformanceMonitor.Common.KnownPatterns.KnownPatternInput(null, null, null, null, null, "Keep me.", null), "seed", ct);
            Assert.Equal(HttpStatusCode.UnsupportedMediaType, (await SendAsync(client, HttpMethod.Put, $"/api/known-patterns/{seeded}", "{\"note\":\"x\"}", "text/plain")).Status);
            Assert.Equal(HttpStatusCode.UnsupportedMediaType, (await SendAsync(client, HttpMethod.Delete, $"/api/known-patterns/{seeded}", null, "text/plain")).Status);
            Assert.Equal("Keep me.", Assert.Single(await store.LoadAllAsync(ct)).Note);
        });
    }

    [Fact]
    public async Task AReadOnlySeat_IsRefusedEveryWrite_ByTheHostsGate_AndChangesNothing()
    {
        await WithRoutesAsync(new DarlingWebSeat("reader@example.com", false), async (client, store, ct) =>
        {
            var seeded = await store.InsertAsync(new PerformanceMonitor.Common.KnownPatterns.KnownPatternInput(null, null, null, null, null, "Keep me.", null), "seed", ct);

            Assert.Equal(HttpStatusCode.Forbidden, (await SendAsync(client, HttpMethod.Post, "/api/known-patterns", "{\"note\":\"x\"}")).Status);
            Assert.Equal(HttpStatusCode.Forbidden, (await SendAsync(client, HttpMethod.Put, $"/api/known-patterns/{seeded}", "{\"note\":\"changed\"}")).Status);
            Assert.Equal(HttpStatusCode.Forbidden, (await SendAsync(client, HttpMethod.Delete, $"/api/known-patterns/{seeded}", null)).Status);

            var entry = Assert.Single(await store.LoadAllAsync(ct));
            Assert.Equal("Keep me.", entry.Note);
        });
    }
}
