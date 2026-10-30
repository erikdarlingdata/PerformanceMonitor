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

/* #1776 own-store: the live test mints its own scratch database and every row it plants goes through DeleteRowsAsync. */

/// <summary>
/// #6384: the Viewer's Sweep Pressure and Heaviest Collectors panels read the same figures the web's <c>get_collection_health</c>
/// returns for the same server and window. The store is seeded with collectors that run in the sweep body, one that runs daily and one
/// that runs detached by name (neither may reach either panel), and the Viewer's read is compared field for field with the tool's
/// <c>sweep_pressure</c> block.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerSweepPressureLiveTests
{
    private const int ServerId = -6384001;
    private const int EmptyServerId = -6384002;
    private const string ServerName = "viewer-sweep-pressure";
    private const string EmptyServerName = "viewer-sweep-pressure-empty";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheViewersSweepPressureAndHeaviestCollectors_AreTheFiguresTheWebToolReturns_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer sweep-pressure test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, EmptyServerId, EmptyServerName, ct);
            var now = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Utc);

            /* Amortized ms/min (average over cadence): query_stats 5,000 (every minute), memory_clerks 2,400 (every 5), wait_stats 2,000,
               file_io_stats 300, perfmon_stats 100. The top three are the Heaviest Collectors; the two below them are cut. */
            await PlantAsync(connection, now, "wait_stats", ct, Enumerable.Repeat(2_000, 20).ToArray());
            await PlantAsync(connection, now, "query_stats", ct, Enumerable.Repeat(5_000, 9).Append(50_000).Append(5_000).ToArray());
            await PlantAsync(connection, now, "file_io_stats", ct, Enumerable.Repeat(300, 10).ToArray());
            await PlantAsync(connection, now, "memory_clerks", ct, Enumerable.Repeat(12_000, 6).ToArray());
            await PlantAsync(connection, now, "perfmon_stats", ct, Enumerable.Repeat(100, 10).ToArray());
            /* Out of the sweep body: a daily collector (by cadence) and query_store (detached by name). Their long runs must not reach
               either panel, or the Viewer would call a body overrun what the web does not. */
            await PlantAsync(connection, now, "index_object_stats", ct, Enumerable.Repeat(80_000, 3).ToArray());
            await PlantAsync(connection, now, "query_store", ct, Enumerable.Repeat(70_000, 3).ToArray());

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            using var doc = JsonDocument.Parse(await DarlingMcpDataTools.GetCollectionHealth(postgres, ServerName, cancellationToken: ct));
            var web = doc.RootElement.GetProperty("sweep_pressure");

            await using var viewer = new ViewerDataService(scratch.ConnectionString);
            var report = await viewer.GetSweepPressureAsync(ServerId, cancellationToken: ct);
            Assert.NotNull(report);

            var pressure = report.Pressure;
            Assert.Equal(web.GetProperty("verdict").GetString(), pressure.Verdict);
            Assert.Equal(web.GetProperty("busy_percent").GetDouble(), Math.Round(pressure.BusyPercent, 1));
            Assert.Equal(web.GetProperty("busy_ms_per_minute").GetDouble(), Math.Round(pressure.BusyMsPerMinute, 0));
            Assert.Equal(web.GetProperty("peak_cycle_ms").GetDouble(), Math.Round(pressure.PeakCycleMs, 0));
            Assert.Equal(web.GetProperty("peak_cycle_percent").GetDouble(), Math.Round(pressure.PeakCyclePercent, 1));
            Assert.Equal(web.GetProperty("peak_cycle_risk").GetString(), pressure.PeakCycleRisk);

            var webHeaviest = web.GetProperty("heaviest_collectors").EnumerateArray().ToList();
            Assert.Equal(webHeaviest.Count, report.Heaviest.Count);
            for (var i = 0; i < webHeaviest.Count; i++)
            {
                var webRow = webHeaviest[i];
                var viewerRow = report.Heaviest[i];
                Assert.Equal(webRow.GetProperty("collector").GetString(), viewerRow.Collector);
                Assert.Equal(webRow.GetProperty("avg_duration_ms").GetDouble(), viewerRow.AvgDurationMs);
                Assert.Equal(webRow.GetProperty("p95_duration_ms").GetDouble(), viewerRow.P95DurationMs);
                Assert.Equal(webRow.GetProperty("max_duration_ms").GetDouble(), viewerRow.MaxDurationMs);
                Assert.Equal(webRow.GetProperty("frequency_minutes").GetInt32(), viewerRow.FrequencyMinutes);
                Assert.Equal(webRow.GetProperty("amortized_ms_per_minute").GetDouble(), viewerRow.AmortizedMsPerMinute);
                Assert.Equal(webRow.GetProperty("pct_of_sweep_budget_per_run").GetDouble(), viewerRow.PctOfSweepBudgetPerRun);
            }

            /* The plant means what the comments say: ranked by amortized cost, three rows, the two collectors outside the body absent. */
            Assert.Equal(new[] { "query_stats", "memory_clerks", "wait_stats" }, report.Heaviest.Select(r => r.Collector));
            Assert.DoesNotContain(report.Heaviest, r => r.Collector is "index_object_stats" or "query_store");

            /* The panel's own wording of those figures: the same six lines, in the web's order. */
            Assert.Equal(
                new[] { "Verdict", "Sweep busy %", "Busy ms/min", "Peak cycle", "Peak cycle %", "Peak risk" },
                SweepPressurePanel.StatRows(pressure).Select(r => r.Label));

            /* A server with no collection log rows has no report, as the tool answers it "unavailable". */
            Assert.Null(await viewer.GetSweepPressureAsync(EmptyServerId, cancellationToken: ct));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static Task PlantAsync(NpgsqlConnection connection, DateTime now, string collector, CancellationToken ct, int[] durations) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
SELECT $1 + u.ord, $2, $3, $4, $5::timestamp - u.ord * interval '30 minutes', u.duration_ms, 'SUCCESS', 1
FROM unnest($6::integer[]) WITH ORDINALITY AS u (duration_ms, ord)",
            CollectionIdGenerator.Next(), ServerId, ServerName, collector, DarlingMcpTestData.Naive(now), durations);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.collection_log_hourly WHERE server_id = ANY($1)", new[] { ServerId, EmptyServerId });
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = ANY($1)", new[] { ServerId, EmptyServerId });
    }
}
