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
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: a scratch database through ScratchPostgres, so no other class's rows shape the averages under test. */

/// <summary>
/// #8572 row 1: <c>get_top_queries_by_cpu</c> and <c>get_top_procedures_by_cpu</c> write their averages (CPU, elapsed, reads) to two
/// places, the way <c>get_query_trend</c> does, where the reply is built. One statement that ran three times, spending 1,000,007 microseconds
/// of CPU, 2,000,011 of elapsed time and 100 logical reads, averages 333.3356666666667 ms of CPU: that was the reply's figure before.
/// </summary>
public sealed class McpReplyAverageRoundingLiveTests
{
    private const string ServerName = "darling-reply-averages";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task TopQueriesAndProcedures_WriteTheirAveragesToTwoPlaces()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live reply-average test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        var at = DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-10));
        var text = "SELECT Averages FROM Numbers";
        var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(text));
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, query_plan_hash, sql_handle, plan_handle, query_text,
                                       query_text_digest, delta_execution_count, delta_worker_time,
                                       delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, "appdb",
            "0xAVG", "0xPLANHASH", "0xSQLH", "0xPLANH", text, digest, 3L, 1_000_007L, 2_000_011L, 100L, 1, 1);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name,
                                           schema_name, object_name, object_type, delta_execution_count,
                                           delta_worker_time, delta_elapsed_time, delta_logical_reads)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, "appdb", "dbo", "usp_Averages", "P", 3L, 1_000_007L, 2_000_011L, 100L);

        foreach (var read in new[] { "get_top_queries_by_cpu", "get_top_procedures_by_cpu" })
        {
            var context = new DefaultHttpContext();
            context.Request.QueryString = QueryString.Create("server_name", ServerName);
            var reply = await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
            using var doc = JsonDocument.Parse(reply);
            var row = FirstObjectWith(doc.RootElement, "avg_cpu_ms");
            Assert.True(row is not null, read + " answered no row: " + reply);
            Assert.Equal("333.34", row.Value.GetProperty("avg_cpu_ms").GetRawText());
            Assert.Equal("666.67", row.Value.GetProperty("avg_elapsed_ms").GetRawText());
            Assert.Equal("33.33", row.Value.GetProperty("avg_reads").GetRawText());
        }
    }

    private static JsonElement? FirstObjectWith(JsonElement element, string property)
    {
        if (element.ValueKind == JsonValueKind.Object)
        {
            if (element.TryGetProperty(property, out _))
            {
                return element;
            }

            foreach (var child in element.EnumerateObject())
            {
                if (FirstObjectWith(child.Value, property) is { } found)
                {
                    return found;
                }
            }
        }
        else if (element.ValueKind == JsonValueKind.Array)
        {
            foreach (var child in element.EnumerateArray())
            {
                if (FirstObjectWith(child, property) is { } found)
                {
                    return found;
                }
            }
        }

        return null;
    }
}
