/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one, so it is not serialized against the live-postgres collection. */

/// <summary>The server-tag web routes through a real host over a scratch store (#5085).</summary>
public sealed class ServerTagWebRoutesLiveTests
{
    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source)> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server-tag route tests (they mint their own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        return (scratch, NpgsqlDataSource.Create(connectionString));
    }

    private static async Task<WebApplication> StartAsync(NpgsqlDataSource source)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        var app = builder.Build();
        DarlingWebEndpoints.MapServerTags(app, source, NullLogger.Instance);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return app;
    }

    private static StringContent Json(string body, string mediaType = "application/json") =>
        new(body, Encoding.UTF8, mediaType);

    private static async Task<(HttpStatusCode Status, JsonElement Body)> SendAsync(HttpClient client, HttpMethod method, string url, string? body, string mediaType = "application/json")
    {
        var ct = TestContext.Current.CancellationToken;
        using var request = new HttpRequestMessage(method, url) { Content = body is null ? Json("", mediaType) : Json(body, mediaType) };
        using var response = await client.SendAsync(request, ct);
        var text = await response.Content.ReadAsStringAsync(ct);
        return (response.StatusCode, JsonDocument.Parse(text).RootElement.Clone());
    }

    [Fact]
    public async Task TheRoutes_AnswerCreateUpdateAssignConflictConfirmAndDelete()
    {
        var (scratch, source) = await OpenAsync();
        await using var _ = scratch;
        await using var __ = source;
        var ct = TestContext.Current.CancellationToken;
        await using var app = await StartAsync(source);
        using var client = app.GetTestClient();

        var created = await SendAsync(client, HttpMethod.Post, "/api/server-tags", "{\"name\":\"Prod\"}");
        Assert.Equal(HttpStatusCode.Created, created.Status);
        var tagId = created.Body.GetProperty("tag").GetProperty("tag_id").GetInt32();

        Assert.Equal(HttpStatusCode.Conflict, (await SendAsync(client, HttpMethod.Post, "/api/server-tags", "{\"name\":\"prod\"}")).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Post, "/api/server-tags", "{\"name\":\"\"}")).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Post, "/api/server-tags", "{\"nope\":1}")).Status);
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, (await SendAsync(client, HttpMethod.Post, "/api/server-tags", "{\"name\":\"X\"}", "text/plain")).Status);

        var updated = await SendAsync(client, new HttpMethod("PATCH"), $"/api/server-tags/{tagId}", "{\"name\":\"Production\"}");
        Assert.Equal(HttpStatusCode.OK, updated.Status);
        Assert.Equal(HttpStatusCode.NotFound, (await SendAsync(client, new HttpMethod("PATCH"), "/api/server-tags/9999", "{\"name\":\"Z\"}")).Status);

        await using (var connection = await source.OpenConnectionAsync(ct))
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, 41, "route-test-a", ct);
        }

        var assigned = await SendAsync(client, HttpMethod.Post, $"/api/server-tags/{tagId}/servers", "{\"server_ids\":[41]}");
        Assert.Equal(HttpStatusCode.OK, assigned.Status);
        Assert.Equal(HttpStatusCode.NotFound, (await SendAsync(client, HttpMethod.Post, $"/api/server-tags/{tagId}/servers", "{\"server_ids\":[42]}")).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Post, $"/api/server-tags/{tagId}/servers", "{\"server_ids\":\"x\"}")).Status);

        /* A rule scoped to the tag turns an unconfirmed delete into a 409 that carries the rule. */
        await using (var command = source.CreateCommand("INSERT INTO custom_alert_rules (name, definition, enabled, version, created_at, updated_at, updated_by) VALUES "
            + $"('tag-rule', '{{\"scope\":{{\"mode\":\"tag\",\"tagId\":{tagId}}}}}', true, 1, now(), now(), 'test')"))
        {
            await command.ExecuteNonQueryAsync(ct);
        }

        var needsConfirm = await SendAsync(client, HttpMethod.Delete, $"/api/server-tags/{tagId}", null);
        Assert.Equal(HttpStatusCode.Conflict, needsConfirm.Status);
        Assert.Equal("confirm_required", needsConfirm.Body.GetProperty("status").GetString());
        Assert.True(needsConfirm.Body.GetProperty("affected_rules").GetArrayLength() > 0);

        var unassigned = await SendAsync(client, HttpMethod.Delete, $"/api/server-tags/{tagId}/servers", "{\"server_ids\":[41]}");
        Assert.Equal(HttpStatusCode.OK, unassigned.Status);

        var deleted = await SendAsync(client, HttpMethod.Delete, $"/api/server-tags/{tagId}?confirm=true", null);
        Assert.Equal(HttpStatusCode.OK, deleted.Status);
        Assert.Equal("deleted", deleted.Body.GetProperty("status").GetString());
    }

    [Fact]
    public async Task TheRoutes_AnswerMalformedBodiesAndEdgesWith4xxNeverA500()
    {
        var (scratch, source) = await OpenAsync();
        await using var _ = scratch;
        await using var __ = source;
        await using var app = await StartAsync(source);
        using var client = app.GetTestClient();
        var patch = new HttpMethod("PATCH");

        var created = await SendAsync(client, HttpMethod.Post, "/api/server-tags", "{\"name\":\"Prod\"}");
        var tagId = created.Body.GetProperty("tag").GetProperty("tag_id").GetInt32();
        Assert.Equal(HttpStatusCode.Created, (await SendAsync(client, HttpMethod.Post, "/api/server-tags", "{\"name\":\"Other\"}")).Status);

        /* M1: a duplicate JSON key is the caller's malformed body. */
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Post, "/api/server-tags", "{\"name\":\"a\",\"name\":\"b\"}")).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, patch, $"/api/server-tags/{tagId}", "{\"name\":\"a\",\"name\":\"b\"}")).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Post, $"/api/server-tags/{tagId}/servers", "{\"server_ids\":[1],\"server_ids\":[2]}")).Status);

        /* M2: a body past the 64 KB cap is refused, on every body-reading route. */
        var pad = new string(' ', 70 * 1024);   // valid JSON either way, so only the cap can refuse it
        var big = "{\"name\":\"Padded\"}" + pad;
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Post, "/api/server-tags", big)).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, patch, $"/api/server-tags/{tagId}", "{\"name\":\"Prod\"}" + pad)).Status);

        /* L4: 415 on both DELETE routes, a non-"true" confirm, and a PATCH to a sibling's name. */
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, (await SendAsync(client, HttpMethod.Delete, $"/api/server-tags/{tagId}", null, "text/plain")).Status);
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, (await SendAsync(client, HttpMethod.Delete, $"/api/server-tags/{tagId}/servers", "{\"server_ids\":[1]}", "text/plain")).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Delete, $"/api/server-tags/{tagId}?confirm=1", null)).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Delete, $"/api/server-tags/{tagId}?confirm=TRUE", null)).Status);
        Assert.Equal(HttpStatusCode.Conflict, (await SendAsync(client, patch, $"/api/server-tags/{tagId}", "{\"name\":\"other\"}")).Status);
    }
}
