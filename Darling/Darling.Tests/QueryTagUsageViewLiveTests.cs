/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// #5988 part B: the viewer's "Resource use by tag" view, through <see cref="ViewerDataService.GetQueryTagRollupDataAsync"/> and
/// <see cref="QueryTagUsageView"/> over seeded plan-cache and blocking facts. One test per behaviour.
/// </summary>
public sealed class QueryTagUsageViewLiveTests
{
    /// <summary>The sentence each section must say when its source is missing: the same words in Lite and the Viewer (#6850).</summary>
    private static readonly IReadOnlyDictionary<string, string> Expected = new Dictionary<string, string>
    {
        ["plan_cache"] = "This store has no query stats data yet.",
        ["query_store"] = "This store has no Query Store data yet.",
        ["procedures"] = "This store has no procedure stats data yet.",
        ["blocking_reports"] = "This store has no blocked process report data yet.",
        ["blocking_snapshots"] = "This store has no blocking snapshot data yet.",
        ["deadlocks"] = "This store has no deadlock data yet.",
    };

    private const int ServerOne = 7801;
    private const int ServerTwo = 7802;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object?[] args)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = arg ?? DBNull.Value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> TagAsync(NpgsqlConnection store, CancellationToken ct, string name)
    {
        await using var command = new NpgsqlCommand("INSERT INTO config.query_tags (name) VALUES ($1) RETURNING id", store);
        command.Parameters.Add(new NpgsqlParameter { Value = name });
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static Task MapAsync(NpgsqlConnection store, CancellationToken ct, int server, string hash, int tagId) =>
        ExecAsync(store, "INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ($1, 'hash', $2, $3, 'hand_hash')", ct, server, hash, tagId);

    private static Task PlanCacheAsync(NpgsqlConnection store, CancellationToken ct, int server, string hash, long cpuUs) =>
        ExecAsync(store,
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count, delta_elapsed_time, delta_worker_time, delta_logical_reads, delta_logical_writes) VALUES ($1, $2, $3, 'x', 'db1', $4, 1, $5, $5, 10, 1)",
            ct, CollectionIdGenerator.Next(), DateTime.UtcNow.AddMinutes(-30), server, hash, cpuUs);

    private static async Task WithViewAsync(Func<NpgsqlConnection, ViewerDataService, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the viewer's by-tag pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var store = new NpgsqlConnection(connectionString);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);
        foreach (var (id, name) in new[] { (ServerOne, "srv-one"), (ServerTwo, "srv-two") })
        {
            await ExecAsync(store, $"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ({id}, '{name}', '{name}', TRUE, 15, now(), now())", ct);
        }

        var bodySucceeded = false;
        try
        {
            await using var viewer = new ViewerDataService(connectionString);
            await body(store, viewer, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static double Cell(QueryTagUsageRow row, QueryTagUsageTable table, string column) =>
        double.Parse(row.Cells[table.Columns.ToList().IndexOf(column) - 1], NumberStyles.Any, CultureInfo.InvariantCulture);

    [Fact]
    public async Task TheTagRowsPlusUntagged_EqualTheSectionTotal_AndASectionIsNeverAddedToAnother()
    {
        await WithViewAsync(async (store, viewer, ct) =>
        {
            var a = await TagAsync(store, ct, "A");
            var b = await TagAsync(store, ct, "B");
            await MapAsync(store, ct, ServerOne, "0xA1", a);
            await MapAsync(store, ct, ServerOne, "0xB1", b);
            await PlanCacheAsync(store, ct, ServerOne, "0xA1", 3_000_000);
            await PlanCacheAsync(store, ct, ServerOne, "0xB1", 5_000_000);
            await PlanCacheAsync(store, ct, ServerOne, "0xZZ", 2_000_000);

            var end = DateTime.UtcNow;
            var data = await viewer.GetQueryTagRollupDataAsync([ServerOne], end.AddHours(-24), end, ct);
            var result = QueryTagUsageView.Build(data, [(ServerOne, "srv-one")], allServers: false, tagFilter: null);

            var plan = result.Tables.Single(t => t.Section == "plan_cache");
            var tagSum = plan.Rows.Where(r => r.Kind == "tag").Sum(r => Cell(r, plan, "CPU (ms)"));
            var untagged = Cell(plan.Rows.Single(r => r.Kind == "untagged"), plan, "CPU (ms)");
            var total = Cell(plan.Rows.Single(r => r.Kind == "total"), plan, "CPU (ms)");
            Assert.Equal(["A", "B"], plan.Rows.Where(r => r.Kind == "tag").Select(r => r.Label).Order().ToArray());
            Assert.Equal(2_000, untagged);
            Assert.Equal(10_000, total);
            Assert.Equal(total, tagSum + untagged);

            /* Query Store holds nothing here: its table stays its own, so the plan cache's figures are not carried into it. */
            var queryStore = result.Tables.Single(t => t.Section == "query_store");
            Assert.Equal(0, Cell(queryStore.Rows.Single(r => r.Kind == "total"), queryStore, "CPU (ms)"));
        });
    }

    [Fact]
    public async Task TheFleetView_ListsEachServerAndAFleetTotal()
    {
        await WithViewAsync(async (store, viewer, ct) =>
        {
            var a = await TagAsync(store, ct, "A");
            await MapAsync(store, ct, ServerOne, "0xA1", a);
            await PlanCacheAsync(store, ct, ServerOne, "0xA1", 4_000_000);
            await PlanCacheAsync(store, ct, ServerTwo, "0xA1", 6_000_000);

            var end = DateTime.UtcNow;
            var scope = new[] { (ServerOne, "srv-one"), (ServerTwo, "srv-two") };
            var data = await viewer.GetQueryTagRollupDataAsync([ServerOne, ServerTwo], end.AddHours(-24), end, ct);
            var result = QueryTagUsageView.Build(data, scope, allServers: true, tagFilter: null);

            var plan = result.Tables.Where(t => t.Section == "plan_cache").ToList();
            Assert.Equal(["Plan cache - srv-one", "Plan cache - srv-two", "Plan cache - all servers"], plan.Select(t => t.Title).ToArray());
            var one = plan[0];
            var two = plan[1];
            var fleet = plan[2];
            Assert.Equal(4_000, Cell(one.Rows.Single(r => r.Kind == "total"), one, "CPU (ms)"));
            /* A key tagged on one server is untagged on the other. */
            Assert.Equal(6_000, Cell(two.Rows.Single(r => r.Kind == "untagged"), two, "CPU (ms)"));
            Assert.Equal(10_000, Cell(fleet.Rows.Single(), fleet, "CPU (ms)"));
        });
    }

    /// <summary>#6850: with every source table dropped, the Viewer and the get_resource_use_by_tag tool each say each section's missing source in the
    /// same words Lite uses, never the table's name.</summary>
    [Fact]
    public async Task EverySection_WithItsSourceMissing_IsSaidInWords_InTheViewerAndTheTool()
    {
        await WithViewAsync(async (store, viewer, ct) =>
        {
            foreach (var table in new[]
            {
                "query_stats", "query_store_interval_wide", "procedure_stats", "blocked_process_reports", "dmv_blocking_snapshots", "deadlocks",
            })
            {
                await ExecAsync(store, $"DROP TABLE IF EXISTS collect.{table} CASCADE", ct);
            }

            var end = DateTime.UtcNow;
            var data = await viewer.GetQueryTagRollupDataAsync([ServerOne], end.AddHours(-24), end, ct);
            /* Query Store is the one section the gate answers first: with its table gone no server passes the gate, so the reason is the gate's
               refusal, which names no table either. The "no ... data yet" sentence there is only for a table that vanishes after the gate passed. */
            var queryStore = QueryTagRollupSection.QueryStore.Name;
            foreach (var (section, sentence) in Expected.Where(e => e.Key != queryStore))
            {
                Assert.True(data.Unavailable.TryGetValue(section, out var reason), $"the viewer served {section} with its source dropped");
                Assert.Equal(sentence, reason);
            }

            Assert.DoesNotContain("query_store_interval_wide", data.Unavailable[queryStore], StringComparison.Ordinal);

            await using var source = NpgsqlDataSource.Create(store.ConnectionString);
            var tool = await PerformanceMonitor.Darling.Service.Mcp.DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(source, null, 24, null, 25, null, null, ct);
            foreach (var (section, sentence) in Expected.Where(e => e.Key != queryStore))
            {
                Assert.Contains($"{section} is unavailable: {sentence}", tool, StringComparison.Ordinal);
            }

            Assert.DoesNotContain("query_store_interval_wide", tool, StringComparison.Ordinal);

            Assert.DoesNotContain("does not exist", tool, StringComparison.OrdinalIgnoreCase);
        });
    }
}
