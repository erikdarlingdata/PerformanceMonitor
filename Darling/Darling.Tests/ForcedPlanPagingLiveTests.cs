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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6560: <c>get_forced_plans</c> listed at most 200 forces with no way to reach the rest or to ask for one plan. Over a server
/// with 250 forces, the list pages (<c>limit</c>, <c>offset</c>, <c>has_more</c>, <c>next_offset</c>) and <c>query_id</c> /
/// <c>plan_id</c> name the forces wanted, and narrow the read itself, before the reader's cap of 2000 forces, so a force past the cap is still found.
/// </summary>
[Collection("live-postgres")]
public sealed class ForcedPlanPagingLiveTests
{
    private const string ServerName = "darling-forced-paging";
    private const string Db = "BigDb";
    private const int Forces = 250;
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<T> WithStoreAsync<T>(Func<NpgsqlDataSource, CancellationToken, Task<T>> body, int forces = Forces)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live forced-plan paging tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* One capture a minute ago: the marker row, then 250 forced plans (plan_id 1..250, query_id plan_id + 1000). */
        var at = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-1));
        await DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.query_store_forced_plans
(collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, query_plan_hash, plan_forcing_type, force_failure_count, last_force_failure_reason)
VALUES ($1,$2,$3,$4,$5,NULL,NULL,NULL,NULL,NULL,NULL)", CollectionIdGenerator.Next(), at, ServerId, ServerName, Db);
        await DarlingMcpTestData.ExecAsync(connection, ct, @"INSERT INTO collect.query_store_forced_plans
(collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, query_plan_hash, plan_forcing_type, force_failure_count, last_force_failure_reason)
SELECT $1 + g, $2, $3, $4, $5, g, g + 1000, '0x' || g::text, 'MANUAL', 0, NULL FROM generate_series(1, $6::int) AS g",
            CollectionIdGenerator.Next() + 1_000_000, at, ServerId, ServerName, Db, forces);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await body(postgres, ct);
    }

    private static Task<string> Call(NpgsqlDataSource postgres, long? queryId, long? planId, int limit, int offset, CancellationToken ct) =>
        DarlingMcpForcedPlanTools.GetForcedPlans(postgres, ServerName, 24, null, DatabaseFilter.All, queryId, planId, limit, offset, ct);

    private static JsonElement Parse(string json) => JsonDocument.Parse(json).RootElement;

    /// <summary>
    /// #6834: a slow <c>get_forced_plans</c> call's slow-read row named no server and kept no server id. A server id is a hash of the name, so
    /// about half of them are negative, and the scope took a negative id for "none noted". The server here has a negative id on purpose.
    /// </summary>
    [Fact]
    public async Task ASlowGetForcedPlans_IsRecordedWithItsServerAndArguments_ForAServerWhoseIdIsNegative()
    {
        Assert.True(ServerId < 0, "the pin needs a server whose id is negative");
        await WithStoreAsync(async (postgres, ct) =>
        {
            using var opened = ReadScope.Open(null);
            await Call(postgres, null, null, 200, 0, ct);

            var slowReads = new SlowReadLog(0);
            var arguments = McpToolLatencyFilter.SlowReadArguments("get_forced_plans", new Dictionary<string, JsonElement>
            {
                ["server_name"] = JsonDocument.Parse("\"" + ServerName + "\"").RootElement,
                ["hours_back"] = JsonDocument.Parse("12").RootElement,
            });
            slowReads.Offer(opened.Scope, ReadSurface.Mcp, "get_forced_plans", ReadOutcome.Ok, 6_000, arguments, null, null);

            Assert.True(slowReads.TryRead(out var record));
            Assert.Equal(ServerId, record!.ServerId);
            var stored = JsonDocument.Parse(record.ArgumentsJson).RootElement;
            Assert.Equal(ServerId, stored.GetProperty("server_id").GetInt32());
            Assert.Equal(12, stored.GetProperty("hours_back").GetInt32());
            return 0;
        });
    }

    [Fact]
    public async Task ThePlanFilter_ReturnsAPlanThatSortsPastTheFirst200()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, null, 250, 200, 0, ct));

            var plan = Assert.Single(root.GetProperty("forced_plans").EnumerateArray());
            Assert.Equal(250, plan.GetProperty("plan_id").GetInt64());
            Assert.Equal(1250, plan.GetProperty("query_id").GetInt64());
            Assert.Equal(1, root.GetProperty("matched_forces").GetInt32());
            Assert.Equal(1, root.GetProperty("total_forces").GetInt32());
            Assert.False(root.GetProperty("has_more").GetBoolean());
            return 0;
        });
    }

    [Fact]
    public async Task TheQueryFilter_ReturnsThatQuerysForce()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, 1240, null, 200, 0, ct));

            var plan = Assert.Single(root.GetProperty("forced_plans").EnumerateArray());
            Assert.Equal(240, plan.GetProperty("plan_id").GetInt64());
            return 0;
        });
    }

    [Fact]
    public async Task ThePages_CoverAllForcesOnce_AndSayWhereTheNextOneStarts()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            var first = Parse(await Call(postgres, null, null, 200, 0, ct));
            Assert.Equal(200, first.GetProperty("forced_plans").GetArrayLength());
            Assert.True(first.GetProperty("has_more").GetBoolean());
            Assert.True(first.GetProperty("truncated").GetBoolean());
            Assert.Equal(200, first.GetProperty("next_offset").GetInt32());
            Assert.Contains("offset 200", first.GetProperty("page_note").GetString(), StringComparison.Ordinal);

            var second = Parse(await Call(postgres, null, null, 200, 200, ct));
            Assert.Equal(50, second.GetProperty("forced_plans").GetArrayLength());
            Assert.False(second.GetProperty("has_more").GetBoolean());
            Assert.Equal(JsonValueKind.Null, second.GetProperty("next_offset").ValueKind);

            var ids = first.GetProperty("forced_plans").EnumerateArray().Concat(second.GetProperty("forced_plans").EnumerateArray())
                .Select(p => p.GetProperty("plan_id").GetInt64()).ToList();
            Assert.Equal(Forces, ids.Distinct().Count());
            Assert.Equal(Forces, first.GetProperty("total_forces").GetInt32());
            return 0;
        });
    }

    /// <summary>Plans sort by database then plan_id and the reader keeps the first <see cref="DarlingForcedPlanStateReader.MaxForces"/>; plan 2100 is past it.</summary>
    private const int PastTheCap = DarlingForcedPlanStateReader.MaxForces + 100;

    [Fact]
    public async Task TheQueryFilter_FindsAForcePastTheReadCap()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, PastTheCap + 1000, null, 200, 0, ct));

            var plan = Assert.Single(root.GetProperty("forced_plans").EnumerateArray());
            Assert.Equal(PastTheCap, plan.GetProperty("plan_id").GetInt64());
            Assert.Equal(1, root.GetProperty("matched_forces").GetInt32());
            return 0;
        }, PastTheCap);
    }

    [Fact]
    public async Task ThePlanFilter_FindsAForcePastTheReadCap()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, null, PastTheCap, 200, 0, ct));

            var plan = Assert.Single(root.GetProperty("forced_plans").EnumerateArray());
            Assert.Equal(PastTheCap + 1000, plan.GetProperty("query_id").GetInt64());
            return 0;
        }, PastTheCap);
    }

    [Fact]
    public async Task AFilterThatMatchesNothing_SaysSo()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            var root = Parse(await Call(postgres, 999_999, null, 200, 0, ct));

            Assert.Equal("empty", root.GetProperty("status").GetString());
            Assert.Contains("matches query_id/plan_id", root.GetProperty("message").GetString(), StringComparison.Ordinal);
            return 0;
        });
    }

    [Fact]
    public async Task ABadLimitOrOffset_IsRefused()
    {
        await WithStoreAsync(async (postgres, ct) =>
        {
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await Call(postgres, null, null, 0, 0, ct)));
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await Call(postgres, null, null, 201, 0, ct)));
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await Call(postgres, null, null, 10, -1, ct)));
            return 0;
        });
    }
}
