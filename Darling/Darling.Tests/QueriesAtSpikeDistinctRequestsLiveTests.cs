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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7896: the CPU spike drill-down's <c>queries_at_spike</c> lists each request once. A request alive across several snapshots in the
/// four minutes around the peak is sampled once per snapshot with a climbing cumulative CPU, so one long request could fill every slot
/// and hide the second and third consumers. The seed is one request (session 70) in four snapshots plus two other requests: three
/// entries come back, the long one at its longest sample. Skips without <c>DARLING_TEST_PG</c>, like its siblings.
/// </summary>
[Collection("live-postgres")]
public sealed class QueriesAtSpikeDistinctRequestsLiveTests
{
    private const string ServerName = "darling-queries-at-spike-distinct-requests-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #7896 queries-at-spike drill-down test.";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task OneLongRequestInFourSnapshots_TakesOneSlot_AtItsLongestSample()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), Skip);
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            DateTime peak;
            using (var connection = new NpgsqlConnection(cs))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await DeleteRowsAsync(connection, ct);
                await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

                peak = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-10);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO collect.cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, 95, 0)",
                    CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(peak), ServerId, ServerName, DarlingMcpTestData.Naive(peak));

                // Session 70 runs one request across four snapshots, 90 s and 30 s either side of the peak, its cumulative CPU climbing.
                var cpu = 1000L;
                foreach (var offset in new[] { -90, -30, 30, 90 })
                {
                    await SnapshotAsync(connection, peak.AddSeconds(offset), sessionId: 70, cpuTimeMs: cpu, "SELECT long", ct);
                    cpu += 1000;
                }
                // Two other requests, each sampled once.
                await SnapshotAsync(connection, peak.AddSeconds(-30), sessionId: 71, cpuTimeMs: 500, "SELECT second", ct);
                await SnapshotAsync(connection, peak.AddSeconds(30), sessionId: 72, cpuTimeMs: 300, "SELECT third", ct);
            }

            var context = new PerformanceMonitor.Analysis.AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = peak.AddMinutes(-5),
                TimeRangeEnd = peak.AddMinutes(5),
                ServerUtcOffset = TimeSpan.Zero,
            };
            var finding = new PerformanceMonitor.Analysis.AnalysisFinding
            {
                ServerId = ServerId,
                RootFactKey = "CPU_SPIKE",
                StoryPath = "CPU_SPIKE",
                PathKeys = ["CPU_SPIKE"],
                Severity = 0.6,
            };
            await new PerformanceMonitor.Darling.Analysis.PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

            Assert.NotNull(finding.DrillDown);
            Assert.True(finding.DrillDown!.TryGetValue("queries_at_spike", out var raw), "no queries_at_spike drill-down");
            var entries = JsonSerializer.SerializeToElement(raw).EnumerateArray()
                .Select(e => (Session: e.GetProperty("session_id").GetInt32(), Cpu: e.GetProperty("cpu_time_ms").GetInt64()))
                .ToList();

            Assert.Equal(3, entries.Count);
            Assert.Equal(new[] { 70, 71, 72 }, entries.Select(e => e.Session).ToArray());
            Assert.Equal(4000L, entries[0].Cpu);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, $"DELETE FROM collect.cpu_utilization_stats WHERE server_id = {ServerId}");
                await DeleteRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    private static Task SnapshotAsync(NpgsqlConnection connection, DateTime at, int sessionId, long cpuTimeMs, string text, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, elapsed_time_formatted, query_text, status, blocking_session_id, wait_type, wait_time_ms, cpu_time_ms, total_elapsed_time_ms, reads, writes, logical_reads, granted_query_memory_gb, transaction_isolation_level, dop, parallel_worker_count, login_name, host_name, program_name, open_transaction_count, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), ServerId, ServerName, sessionId, "AppDb", "00 00:00:05.000",
            text, "running", 0, null, 0L, cpuTimeMs, cpuTimeMs * 60, 100L, 0L, 2000L, 0.5m, "Read Committed", 1, 0, "sa", "APP01", "Orders.Api", 1, 0);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collect.query_snapshots WHERE server_id = {ServerId}; DELETE FROM collect.servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
