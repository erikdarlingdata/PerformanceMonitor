/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: a scratch database made through ScratchPostgres. */

/// <summary>
/// #8915: an index seen once has no windowed scan count, so the read says "unknown" (null, a dash in the Viewer) and never
/// "an index with no scans". An index seen twice with no scans in between keeps its zero and its wording.
/// </summary>
public sealed class PgIndexUsageOneSampleLiveTests
{
    private const int ServerId = -891501;
    private const string ServerName = "index-one-sample-e2e";

    private static async Task SeedSampleAsync(
        NpgsqlConnection store, DateTime at, string index, long scans, bool unique, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(store, ct,
            @"INSERT INTO pg_index_usage_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, index_name, index_scans,
     tuples_read, tuples_fetched, blocks_read, blocks_hit, index_bytes, table_bytes, is_unique, is_primary_key, is_valid, is_ready,
     is_replica_identity, is_partial, is_expression, supports_constraint, index_method, column_count, index_definition, stats_reset)
VALUES ($1,$2,$3,$4,'postgres','public','widget',$5, $6, 0,0,0,5000, 1000000, 20000000, $7,false,true,true, false,false,false,false, 'btree', 1,
        'CREATE INDEX x ON public.widget USING btree (grp)', NULL)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, index, scans, unique);
    }

    [Fact]
    public async Task OneSampleIsUnknown_TwoSamplesWithNoScansIsZero()
    {
        var ct = TestContext.Current.CancellationToken;
        var storeConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(storeConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live test.");

        await using var scratch = await ScratchPostgres.CreateAsync(storeConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var store = new NpgsqlConnection(scratch.ConnectionString);
            await store.OpenAsync(ct);
            await PgMigrations.MigrateAsync(store, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await DarlingMcpTestData.RegisterServerAsync(store, ServerId, ServerName, ct);

            var later = DateTime.UtcNow.AddHours(-1);
            var earlier = later.AddHours(-2);
            await SeedSampleAsync(store, later, "plain_once", 281_830_117, false, ct);
            await SeedSampleAsync(store, later, "unique_once", 281_830_117, true, ct);
            await SeedSampleAsync(store, earlier, "plain_twice", 0, false, ct);
            await SeedSampleAsync(store, later, "plain_twice", 0, false, ct);

            using var doc = JsonDocument.Parse(await DarlingMcpPgIndexUsageTools.GetPgIndexUsage(dataSource, ServerName, 24, 25, null, null, ct));
            var indexes = doc.RootElement.GetProperty("indexes").EnumerateArray().ToDictionary(e => e.GetProperty("index").GetString()!);

            foreach (var once in new[] { "plain_once", "unique_once" })
            {
                var row = indexes[once];
                Assert.Equal(JsonValueKind.Null, row.GetProperty("scans_in_window").ValueKind);
                Assert.Equal(281_830_117, row.GetProperty("total_scans_since_stats_reset").GetInt64());
                Assert.Contains("Unknown", row.GetProperty("scans_in_window_note").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain("no scans", row.GetProperty("cost_finding").GetString()!, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("Zero scans", row.GetProperty("droppability_finding").GetString()!, StringComparison.OrdinalIgnoreCase);
                Assert.Equal("Healthy", row.GetProperty("severity").GetString());
            }

            var twice = indexes["plain_twice"];
            Assert.Equal(0, twice.GetProperty("scans_in_window").GetInt64());
            Assert.Equal(JsonValueKind.Null, twice.GetProperty("scans_in_window_note").ValueKind);
            Assert.Contains("On an index with no scans", twice.GetProperty("cost_finding").GetString(), StringComparison.Ordinal);

            var end = DateTime.UtcNow;
            var rows = await DarlingPgIndexUsageReader.GetPgIndexUsageAsync(dataSource, ServerId, end.AddHours(-24), end, 100, ct);
            var onceDisplay = PgDisplay.IndexUsage(rows.Single(r => r.IndexName == "plain_once"));
            Assert.Equal(PgDisplay.NotApplicableText, onceDisplay.ScansInWindow);
            Assert.Null(onceDisplay.ScansInWindowSort);
            Assert.Contains("Needs two samples", onceDisplay.ScansInWindowNote, StringComparison.Ordinal);
            Assert.False(onceDisplay.IsUnscanned);

            var twiceDisplay = PgDisplay.IndexUsage(rows.Single(r => r.IndexName == "plain_twice"));
            Assert.Equal("0", twiceDisplay.ScansInWindow);
            Assert.Equal(0L, twiceDisplay.ScansInWindowSort);
            Assert.Equal(string.Empty, twiceDisplay.ScansInWindowNote);
            Assert.True(twiceDisplay.IsUnscanned);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
