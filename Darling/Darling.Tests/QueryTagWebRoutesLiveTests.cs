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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one, so it is not serialized against the live-postgres collection. */

/// <summary>The query-tag web routes through a real host over a scratch store (#5986).</summary>
public sealed class QueryTagWebRoutesLiveTests
{
    private static async Task<(ScratchPostgres Scratch, NpgsqlDataSource Source)> OpenAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live query-tag route tests (they mint their own scratch database).");
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
        DarlingWebEndpoints.MapQueryTags(app, source, NullLogger.Instance);
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

    private static async Task<int> CreateTagAsync(HttpClient client, string name)
    {
        var created = await SendAsync(client, HttpMethod.Post, "/api/query-tags", "{\"name\":\"" + name + "\"}");
        Assert.Equal(HttpStatusCode.Created, created.Status);
        return created.Body.GetProperty("tag").GetProperty("tag_id").GetInt32();
    }

    [Fact]
    public async Task ACreateWithABadName_AnswersTheSharedRefusalCode_AndWritesNothing()
    {
        var (scratch, source) = await OpenAsync();
        await using var scratchHold = scratch;
        await using var sourceHold = source;
        await using var app = await StartAsync(source);
        using var client = app.GetTestClient();

        var refused = await SendAsync(client, HttpMethod.Post, "/api/query-tags", "{\"name\":\"   \"}");
        Assert.Equal(HttpStatusCode.BadRequest, refused.Status);
        Assert.Equal("invalid", refused.Body.GetProperty("status").GetString());
        Assert.True(refused.Body.TryGetProperty("refusal", out var code) && !string.IsNullOrEmpty(code.GetString()));
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, (await SendAsync(client, HttpMethod.Post, "/api/query-tags", "{\"name\":\"X\"}", "text/plain")).Status);
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Post, "/api/query-tags", "{\"nope\":1}")).Status);

        var list = JsonDocument.Parse(await DarlingMcpQueryTagTools.ListQueryTagsCore(new QueryTagStore(source, 30), TestContext.Current.CancellationToken)).RootElement;
        Assert.Equal(0, list.GetProperty("tag_count").GetInt32());
    }

    [Fact]
    public async Task ADeleteWithoutConfirm_Answers409ConfirmRequired_AndChangesNothing_ThenConfirmDeletes()
    {
        var (scratch, source) = await OpenAsync();
        await using var scratchHold = scratch;
        await using var sourceHold = source;
        await using var app = await StartAsync(source);
        using var client = app.GetTestClient();
        var tagId = await CreateTagAsync(client, "Reports");
        Assert.Equal(HttpStatusCode.OK, (await SendAsync(client, HttpMethod.Post, $"/api/query-tags/{tagId}/assignments", "{\"kind\":\"hash\",\"query_hash\":\"0xAB12\"}")).Status);

        var needs = await SendAsync(client, HttpMethod.Delete, $"/api/query-tags/{tagId}", null);
        Assert.Equal(HttpStatusCode.Conflict, needs.Status);
        Assert.Equal("confirm_required", needs.Body.GetProperty("status").GetString());
        Assert.Equal(1, needs.Body.GetProperty("assignments").GetInt32());

        var list = JsonDocument.Parse(await DarlingMcpQueryTagTools.ListQueryTagsCore(new QueryTagStore(source, 30), TestContext.Current.CancellationToken)).RootElement;
        Assert.Equal(1, list.GetProperty("tag_count").GetInt32());

        var deleted = await SendAsync(client, HttpMethod.Delete, $"/api/query-tags/{tagId}?confirm=true", null);
        Assert.Equal(HttpStatusCode.OK, deleted.Status);
        Assert.Equal("deleted", deleted.Body.GetProperty("status").GetString());
    }

    [Fact]
    public async Task AnAssignByHash_ThroughTheRoute_ShowsInListQueryTags_StampedWithTheWebSeat_AndUnassignRemovesIt()
    {
        var (scratch, source) = await OpenAsync();
        await using var scratchHold = scratch;
        await using var sourceHold = source;
        var ct = TestContext.Current.CancellationToken;
        await using var app = await StartAsync(source);
        using var client = app.GetTestClient();
        var tagId = await CreateTagAsync(client, "Reports");

        var assigned = await SendAsync(client, HttpMethod.Post, $"/api/query-tags/{tagId}/assignments", "{\"kind\":\"hash\",\"query_hash\":\"0xAB12\"}");
        Assert.Equal(HttpStatusCode.OK, assigned.Status);
        Assert.Equal("assigned", assigned.Body.GetProperty("status").GetString());

        var list = JsonDocument.Parse(await DarlingMcpQueryTagTools.ListQueryTagsCore(new QueryTagStore(source, 30), ct)).RootElement;
        var assignment = list.GetProperty("tags")[0].GetProperty("assignments")[0];
        Assert.Equal("0xAB12", assignment.GetProperty("query_hash").GetString());
        Assert.NotEqual("mcp", assignment.GetProperty("created_by").GetString());

        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Post, $"/api/query-tags/{tagId}/assignments", "{\"kind\":\"hash\",\"server_id\":\"x\"}")).Status);
        var removed = await SendAsync(client, HttpMethod.Delete, $"/api/query-tags/{tagId}/assignments", "{\"kind\":\"hash\",\"query_hash\":\"0xAB12\"}");
        Assert.Equal(HttpStatusCode.OK, removed.Status);
        list = JsonDocument.Parse(await DarlingMcpQueryTagTools.ListQueryTagsCore(new QueryTagStore(source, 30), ct)).RootElement;
        Assert.Equal(0, list.GetProperty("tags")[0].GetProperty("assignments").GetArrayLength());
    }

    [Fact]
    public async Task ARuleCreatedThroughTheRoute_AppearsInTheRuleList_CanBeTestedEditedAndDeleted()
    {
        var (scratch, source) = await OpenAsync();
        await using var scratchHold = scratch;
        await using var sourceHold = source;
        await using var app = await StartAsync(source);
        using var client = app.GetTestClient();
        var tagId = await CreateTagAsync(client, "Reports");

        var created = await SendAsync(client, HttpMethod.Post, "/api/query-tag-rules", "{\"field\":\"application\",\"pattern\":\"report*\",\"tag_id\":" + tagId + ",\"priority\":50}");
        Assert.Equal(HttpStatusCode.Created, created.Status);
        var ruleId = created.Body.GetProperty("rule").GetProperty("id").GetInt64();

        var listed = await SendAsync(client, HttpMethod.Get, "/api/query-tag-rules", null);
        Assert.Equal(HttpStatusCode.OK, listed.Status);
        var rule = listed.Body.GetProperty("rules")[0];
        Assert.Equal(ruleId, rule.GetProperty("id").GetInt64());
        Assert.Equal("report*", rule.GetProperty("pattern").GetString());
        Assert.Equal(50, rule.GetProperty("priority").GetInt32());
        Assert.NotEqual("mcp", rule.GetProperty("created_by").GetString());

        var tested = await SendAsync(client, HttpMethod.Get, $"/api/query-tag-rules/test?field=application&pattern=report*&tag_id={tagId}", null);
        Assert.Equal(HttpStatusCode.OK, tested.Status);
        Assert.True(tested.Body.TryGetProperty("keys_examined", out _) || tested.Body.GetProperty("status").GetString() == "empty");
        Assert.Equal(HttpStatusCode.BadRequest, (await SendAsync(client, HttpMethod.Get, "/api/query-tag-rules/test?field=application&tag_id=x", null)).Status);

        Assert.Equal(HttpStatusCode.OK, (await SendAsync(client, new HttpMethod("PATCH"), $"/api/query-tag-rules/{ruleId}", "{\"enabled\":false}")).Status);
        Assert.False((await SendAsync(client, HttpMethod.Get, "/api/query-tag-rules", null)).Body.GetProperty("rules")[0].GetProperty("enabled").GetBoolean());
        Assert.Equal(HttpStatusCode.NotFound, (await SendAsync(client, new HttpMethod("PATCH"), "/api/query-tag-rules/9999", "{\"enabled\":false}")).Status);

        Assert.Equal(HttpStatusCode.OK, (await SendAsync(client, HttpMethod.Delete, $"/api/query-tag-rules/{ruleId}", null)).Status);
        Assert.Equal(0, (await SendAsync(client, HttpMethod.Get, "/api/query-tag-rules", null)).Body.GetProperty("rules").GetArrayLength());
    }
}
