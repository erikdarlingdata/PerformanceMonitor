/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, read-time census for the query-text family (inventory rows 1-7): get_active_queries, get_top_queries_by_cpu,
/// get_query_store_top, get_query_store_regressions, get_query_heatmap, get_plan_corrections and both get_finops views that
/// print statement text (high_impact's sample_query_text, optimization's query_preview). Each case plants rows DIRECTLY in
/// the store tables, so the rows look like ones stored before the collection hook existed: a canary statement beside a plain
/// one, and a second server holding only plain rows. The read runs RAW (the tool method alone: it must still hold the
/// canary, the control that proves the plant reached the output) and FILTERED (the same text through the host's registered
/// filter list). The filtered answer holds no secret needle, carries the marker, and keeps the plain statement. A page with
/// no canary comes back byte-identical to the raw read (whenever the output format variable is unset, which is the
/// production default). Each case reads the preview form and the <c>full_text=true</c> form where the tool has one.
/// </summary>
[Collection("live-postgres")]
public sealed class StatementFilterQueryReadsLiveTests
{
    private const string Db = "SsfQueryDb";
    private const string CanaryServer = "darling-ssf-query-reads";
    private const string CleanServer = "darling-ssf-query-reads-clean";
    private static readonly int CanaryId = ServerIdHelper.GetDeterministicHashCode(CanaryServer);
    private static readonly int CleanId = ServerIdHelper.GetDeterministicHashCode(CleanServer);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private readonly ITestOutputHelper _output;

    public StatementFilterQueryReadsLiveTests(ITestOutputHelper output) => _output = output;

    public static IEnumerable<TheoryDataRow<string>> Cases() => new[]
    {
        new TheoryDataRow<string>("get_active_queries"),
        new TheoryDataRow<string>("get_top_queries_by_cpu"),
        new TheoryDataRow<string>("get_query_store_top"),
        new TheoryDataRow<string>("get_query_store_regressions"),
        new TheoryDataRow<string>("get_query_heatmap"),
        new TheoryDataRow<string>("get_plan_corrections"),
        new TheoryDataRow<string>("get_finops high_impact"),
        new TheoryDataRow<string>("get_finops optimization"),
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task EveryQueryTextRead_WithTheCanaryStoredBeforeTheHook_WithholdsItThroughTheHost_AndLeavesACleanPageUntouched(string tool)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live query-text filter census.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);
        using var host = await StatementFilterCensus.BuildHostAsync();

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, CanaryId, CanaryServer, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, CleanId, CleanServer, ct);
            await SeedAsync(connection, tool, CanaryId, CanaryServer, withCanary: true, ct);
            await SeedAsync(connection, tool, CleanId, CleanServer, withCanary: false, ct);

            var reads = ReadsFor(postgres, tool, ct);
            Assert.NotEmpty(reads);
            foreach (var (form, read) in reads)
            {
                string label = tool + " (" + form + ")";

                string raw = await read(CanaryServer);
                Assert.True(raw.Contains("S3cret-canary-ssf", StringComparison.Ordinal), label + ": the raw read lost the canary, so the plant did not reach the output");
                string filtered = (await StatementFilterCensus.FilterThroughHostAsync(host, raw)).Text;
                try
                {
                    StatementFilterCensus.AssertFilteredWithholdsTheCanary(raw, filtered);
                }
                catch (Exception ex)
                {
                    throw new Xunit.Sdk.XunitException(label + ": " + ex.Message);
                }

                /* The no-hit page: nothing to withhold, so the answer is the raw read, byte for byte. The host is built with GCF
                   pinned off (#5320), so no other class's DARLING_OUTPUT_FORMAT reaches it and the identity holds every run. */
                string clean = await read(CleanServer);
                Assert.DoesNotContain("S3cret-canary-ssf", clean, StringComparison.Ordinal);
                string cleanFiltered = (await StatementFilterCensus.FilterThroughHostAsync(host, clean)).Text;
                Assert.True(string.Equals(clean, cleanFiltered, StringComparison.Ordinal), label + ": a page with no canary changed under the filter");

                _output.WriteLine($"{label}: no-hit page {Encoding.UTF8.GetByteCount(clean):N0} bytes raw, {Encoding.UTF8.GetByteCount(cleanFiltered):N0} bytes filtered; canary page {Encoding.UTF8.GetByteCount(raw):N0} raw, {Encoding.UTF8.GetByteCount(filtered):N0} filtered.");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>Every form of the tool the census reads: the default (preview) and the <c>full_text</c> form where the tool has one.</summary>
    private static List<(string Form, Func<string, Task<string>> Read)> ReadsFor(NpgsqlDataSource postgres, string tool, CancellationToken ct)
    {
        var reads = new List<(string, Func<string, Task<string>>)>();
        switch (tool)
        {
            case "get_active_queries":
                reads.Add(("preview", s => DarlingMcpSessionTools.GetActiveQueries(postgres, s, cancellationToken: ct)));
                reads.Add(("full_text", s => DarlingMcpSessionTools.GetActiveQueries(postgres, s, full_text: true, cancellationToken: ct)));
                break;
            case "get_top_queries_by_cpu":
                reads.Add(("default", s => DarlingMcpDataTools.GetTopQueriesByCpu(postgres, s, cancellationToken: ct)));
                reads.Add(("detail=full", s => DarlingMcpDataTools.GetTopQueriesByCpu(postgres, s, detail: "full", cancellationToken: ct)));
                reads.Add(("group_by=host_object", s => DarlingMcpDataTools.GetTopQueriesByCpu(postgres, s, group_by: "host_object", cancellationToken: ct)));
                break;
            case "get_query_store_top":
                reads.Add(("preview", s => DarlingMcpDataTools.GetQueryStoreTop(postgres, s, cancellationToken: ct)));
                reads.Add(("full_text", s => DarlingMcpDataTools.GetQueryStoreTop(postgres, s, full_text: true, cancellationToken: ct)));
                break;
            case "get_query_store_regressions":
                reads.Add(("preview", s => DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, s, cancellationToken: ct)));
                reads.Add(("full_text", s => DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(postgres, s, full_text: true, cancellationToken: ct)));
                break;
            case "get_query_heatmap":
                reads.Add(("preview", s => DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, s, cancellationToken: ct)));
                reads.Add(("full_text", s => DarlingMcpQueryHeatmapTools.GetQueryHeatmap(postgres, s, full_text: true, cancellationToken: ct)));
                break;
            case "get_plan_corrections":
                reads.Add(("preview", s => DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, s, cancellationToken: ct)));
                reads.Add(("full_text", s => DarlingMcpPlanCorrectionTools.GetPlanCorrections(postgres, s, full_text: true, cancellationToken: ct)));
                break;
            case "get_finops high_impact":
                reads.Add(("high_impact", s => DarlingMcpFinOpsTools.GetFinOps(postgres, "high_impact", s, cancellationToken: ct)));
                break;
            case "get_finops optimization":
                reads.Add(("optimization", s => DarlingMcpFinOpsTools.GetFinOps(postgres, "optimization", s, cancellationToken: ct)));
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(tool), tool, null);
        }

        return reads;
    }

    private static async Task SeedAsync(NpgsqlConnection connection, string tool, int serverId, string serverName, bool withCanary, CancellationToken ct)
    {
        var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        var texts = new List<(string Text, string Tag)>();
        if (withCanary) texts.Add((StatementScrubCanary.CanaryStatement, "C"));
        texts.Add((StatementScrubCanary.PlainStatement, "P"));

        switch (tool)
        {
            case "get_active_queries":
                for (int i = 0; i < texts.Count; i++)
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id, wait_resource, percent_complete, query_hash)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-5 - i)), serverId, serverName, 100 + i, Db,
                        "00 00:00:05.125", texts[i].Text, "running", 0, null, 0L, 1000L, 1500L, 10_000L, 50L, 7L, 0m,
                        "Read Committed", 1, 0, "app_svc", "APPSRV01", "MyOrderService.Worker", 0, 0,
                        "KEY: 5:72057594043432960 (a1b2c3d4e5f6)", 12.5m, "0x" + (0x1A2B3C4D5E6F7080L + i).ToString("X16"));
                }

                break;

            case "get_top_queries_by_cpu":
            case "get_finops high_impact":
            case "get_finops optimization":
                for (int i = 0; i < texts.Count; i++)
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle, query_text,
                                       delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, min_dop, max_dop, sample_interval_seconds)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-30 - i)), serverId, serverName, Db,
                        "0xQH" + texts[i].Tag, "0xPLAN" + texts[i].Tag, "0xSQLH" + texts[i].Tag, "0xPLANH" + texts[i].Tag, texts[i].Text,
                        10L, 1_000_000L, 2_000_000L, 5_000L, 1, 1, 60);
                }

                break;

            case "get_query_heatmap":
            {
                var t0 = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerHour), now.Kind).AddHours(-1);
                long[] elapsedMicros = { 500, 5_000_000 };
                for (int i = 0; i < texts.Count; i++)
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sample_interval_seconds, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes, query_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(t0), serverId, serverName, Db, "0xHM" + texts[i].Tag,
                        60, 1L, 0L, elapsedMicros[i % 2], 0L, 0L, texts[i].Text);
                }

                break;
            }

            case "get_query_store_top":
                for (int i = 0; i < texts.Count; i++)
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, last_execution_time, module_name, query_text, query_hash, query_plan_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, avg_rowcount)
VALUES ($1,$2,$3,$4,$5,$6,$7,'Regular',$2,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-30 - i)), serverId, serverName, Db,
                        100L + i, 200L + i, null, texts[i].Text, "0xQSH" + texts[i].Tag, "0xQSP" + texts[i].Tag,
                        100L, 5000L, 4000L, 100L, 10L, 5L, 10L);
                }

                break;

            case "get_query_store_regressions":
                for (int i = 0; i < texts.Count; i++)
                {
                    long queryId = 1000 + i;
                    foreach (var (at, executions, avg, interval, text) in new[]
                    {
                        (now.AddHours(-40), 50L, 1000L, 2L * i + 1, "SELECT 1"),
                        (now.AddMinutes(-30), 200L, 4000L, 2L * i + 2, texts[i].Text),
                    })
                    {
                        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, runtime_stats_interval_id, query_text, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, Db, queryId, 9L, "Regular",
                            executions, avg, avg, 100L, interval, text, DarlingMcpTestData.Naive(at));
                    }
                }

                break;

            case "get_plan_corrections":
                for (int i = 0; i < texts.Count; i++)
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO plan_correction (collection_id, collection_time, server_id, server_name, database_name, recommendation_name, recommendation_state, query_id, query_text, regressed_plan_id, last_good_plan_id, score)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
                        CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(now.AddMinutes(-20 - i)), serverId, serverName, Db,
                        "PlanRegression_ssf_" + texts[i].Tag, "Active", 5000L + i, texts[i].Text, 6000L + i, 7000L + i, 80);
                }

                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(tool), tool, null);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (int id in new[] { CanaryId, CleanId })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                $"DELETE FROM query_snapshots WHERE server_id = {id}; DELETE FROM query_stats WHERE server_id = {id}; " +
                $"DELETE FROM query_store_stats WHERE server_id = {id}; DELETE FROM plan_correction WHERE server_id = {id}; " +
                $"DELETE FROM servers WHERE server_id = {id}; DELETE FROM config_monitored_servers WHERE server_id = {id};");
        }
    }
}
