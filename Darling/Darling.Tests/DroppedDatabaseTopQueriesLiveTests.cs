/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #7725: <c>get_top_queries_by_cpu</c> ranks the databases the server still lists first, then fills what the limit leaves from the
/// databases the server dropped, ranked the same way, and marks each of those rows <c>database_dropped</c>. A dropped database that
/// ran heavier work must never push a listed database's row out of the page.
/// </summary>
[Collection("live-postgres")]
public sealed class DroppedDatabaseTopQueriesLiveTests
{
    private static Task PlantQueryAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, DateTime at, string db, string tag, long weight) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats
                  (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                   query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                   delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                   min_dop, max_dop, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 10, $12, $12, 100, 0, 0, 1, $12, 1, $12, 1, 1, 3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, db,
            "0xQ" + tag, "0xP" + tag, "0xS" + tag, "0xL" + tag, "SELECT " + tag, SHA256.HashData(Encoding.UTF8.GetBytes("SELECT " + tag)), weight);

    [Fact]
    public async Task TopQueries_RankTheListedDatabasesFirst_ThenFillFromTheDroppedOnes_MarkedDropped()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live dropped-database Top Queries test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        const string serverName = "dropped-topq";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);

        /* GoneDb is the heaviest database, then LiveDb, then OtherDb: before the fix GoneDb ranked first among them unmarked. */
        await PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2), "GoneDb", "Gone1", 900_000);
        await PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2), "LiveDb", "Live1", 500_000);
        await PlantQueryAsync(connection, ct, serverId, serverName, now.AddHours(-2), "OtherDb", "Other1", 100_000);

        /* An earlier snapshot named all three; the newest names LiveDb and OtherDb, so GoneDb was dropped in between. */
        foreach (var (age, names) in new[] { (TimeSpan.FromHours(1), new[] { "GoneDb", "LiveDb", "OtherDb" }), (TimeSpan.FromMinutes(5), new[] { "LiveDb", "OtherDb" }) })
        {
            var at = DateTime.SpecifyKind(DateTime.UtcNow - age, DateTimeKind.Unspecified);
            var id = 1;
            foreach (var name in names)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1,$2,$3,$4,$5,$6,'ONLINE',FALSE)",
                    CollectionIdGenerator.Next(), at, serverId, serverName, name, id++);
            }
        }

        /* The fixture produced rows: nothing below may pass on an empty page. */
        await using var count = new NpgsqlCommand("SELECT count(*) FROM query_stats WHERE server_id = $1", connection);
        count.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        Assert.Equal(3L, (long)(await count.ExecuteScalarAsync(ct))!);

        static JsonElement[] Page(string json, out JsonDocument document)
        {
            document = JsonDocument.Parse(json);
            return document.RootElement.GetProperty("queries").EnumerateArray().Select(q => q.Clone()).ToArray();
        }

        static bool Dropped(JsonElement row) => row.TryGetProperty("database_dropped", out var flag) && flag.GetBoolean();

        /* Room for all three: the listed databases first, in rank order, with no mark; then GoneDb, marked. */
        var all = Page(await DarlingMcpDataTools.GetTopQueriesByCpu(dataSource, serverName, hours_back: 168, top: 20, cancellationToken: ct), out var allDocument);
        using (allDocument)
        {
            Assert.NotEmpty(all);
            Assert.Equal(new[] { "LiveDb", "OtherDb", "GoneDb" }, all.Select(q => q.GetProperty("database_name").GetString()!).ToArray());
            Assert.Equal(new[] { false, false, true }, all.Select(Dropped).ToArray());
            Assert.False(all[0].TryGetProperty("database_dropped", out _), "a listed database's row carries no database_dropped key");
            /* #7725: every left-out database has a row in the reply, so there is nothing to disclose. */
            Assert.Equal(JsonValueKind.Null, allDocument.RootElement.GetProperty("databases_left_out").ValueKind);
        }

        /* A limit the listed rows fill leaves GoneDb out, heavier as it is. */
        var two = Page(await DarlingMcpDataTools.GetTopQueriesByCpu(dataSource, serverName, hours_back: 168, top: 2, cancellationToken: ct), out var twoDocument);
        using (twoDocument)
        {
            Assert.Equal(new[] { "LiveDb", "OtherDb" }, two.Select(q => q.GetProperty("database_name").GetString()!).ToArray());
            Assert.DoesNotContain(two, Dropped);
            /* #7725: GoneDb has no row in this reply, so the reply names it, as get_top_procedures_by_cpu does. */
            Assert.Equal(new[] { "GoneDb" }, twoDocument.RootElement.GetProperty("databases_left_out").EnumerateArray().Select(n => n.GetString()!).ToArray());
            Assert.Contains("no longer lists", twoDocument.RootElement.GetProperty("databases_left_out_note").GetString(), StringComparison.Ordinal);
            Assert.Equal(1, twoDocument.RootElement.GetProperty("databases_left_out_count").GetInt32());
        }

        /* One slot left over: the fill takes the dropped database's row. */
        var three = Page(await DarlingMcpDataTools.GetTopQueriesByCpu(dataSource, serverName, hours_back: 168, top: 3, cancellationToken: ct), out var threeDocument);
        using (threeDocument)
        {
            Assert.Equal(new[] { "LiveDb", "OtherDb", "GoneDb" }, three.Select(q => q.GetProperty("database_name").GetString()!).ToArray());
        }

        /* A named database is read as asked, and is not marked. */
        var named = Page(await DarlingMcpDataTools.GetTopQueriesByCpu(dataSource, serverName, hours_back: 168, top: 20, database_name: "GoneDb", cancellationToken: ct), out var namedDocument);
        using (namedDocument)
        {
            Assert.Equal(new[] { "GoneDb" }, named.Select(q => q.GetProperty("database_name").GetString()!).ToArray());
            Assert.DoesNotContain(named, Dropped);
        }
    }
}
