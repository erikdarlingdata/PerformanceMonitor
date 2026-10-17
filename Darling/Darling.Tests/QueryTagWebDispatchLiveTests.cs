/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5987: the web read dispatch (<c>/api/read</c>) passes the page's <c>tag</c> parameter to the five query reads the grids use, so the filter
/// reaches the server and is applied before the top-N cut. Seeded and read like <see cref="QueryTagReadsLiveTests"/>, but through the dispatch the
/// page calls. On the base the dispatch dropped <c>tag</c>, so every filtered read answered with every row.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryTagWebDispatchLiveTests
{
    private static async Task<JsonElement> WebReadAsync(NpgsqlDataSource postgres, string read, string member, string? tag)
    {
        var query = new List<KeyValuePair<string, string?>> { new("server_name", QueryTagReadsLiveTests.ServerName), new("hours_back", "1") };
        if (tag != null) query.Add(new("tag", tag));
        var context = new DefaultHttpContext();
        context.Request.QueryString = QueryString.Create(query);
        var json = await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
        return JsonDocument.Parse(json).RootElement.Clone().GetProperty(member);
    }

    private static string?[] Tags(JsonElement rows) =>
        rows.EnumerateArray().Select(r => r.TryGetProperty("tag", out var t) ? t.GetString() : null).ToArray();

    [Theory]
    [InlineData("get_top_queries_by_cpu", "queries")]
    [InlineData("get_top_procedures_by_cpu", "procedures")]
    [InlineData("get_query_store_top", "queries")]
    [InlineData("get_active_queries", "queries")]
    [InlineData("get_long_query_completions", "completions")]
    public Task TheWebDispatch_PassesTheTag_SoTheFilterIsAppliedByTheRead(string read, string member) =>
        QueryTagReadsLiveTests.RunAsync(async (pg, ct) =>
        {
            var all = await WebReadAsync(pg, read, member, null);
            var onTag = await WebReadAsync(pg, read, member, "TagReadsProd");
            var untagged = await WebReadAsync(pg, read, member, "untagged");
            Assert.True(all.GetArrayLength() > onTag.GetArrayLength(), read + " ignored the tag: " + all.GetArrayLength() + " rows with and without it");
            Assert.NotEqual(0, onTag.GetArrayLength());
            Assert.All(Tags(onTag), t => Assert.StartsWith("TagReadsProd", t));
            Assert.NotEqual(0, untagged.GetArrayLength());
            Assert.All(Tags(untagged), Assert.Null);
        });
}
