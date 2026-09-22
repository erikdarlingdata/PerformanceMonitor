/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live-Postgres pin for #3648, Darling's twin of Lite's <c>DrillDownDopProvenanceTests</c>: the
/// <c>top_cpu_queries</c> and <c>bad_actor_query</c> drill-downs must headline the NEWEST plan's
/// <c>max_dop</c> and carry the cross-plan maximum as a history with provenance, not fold every plan the
/// hash ever had inside the window into one provenance-free number.
///
/// <para><b>The fixture is the live page.</b> One <c>query_hash</c>, two plans: an old parallel plan whose
/// per-plan high-water mark reads 16 and was last seen three hours before the window's end, and a new serial
/// plan reading 1 that spent the CPU at the newest snapshot. The old read said <c>max_dop = 16</c> for this
/// shape on a MAXDOP-1 instance whose stored plan was serial, and a tuning recommendation was made from it
/// and retracted.</para>
///
/// <para>Live rather than a string pin because the claim is the ENGINE's answer to <c>ROW_NUMBER() OVER</c>
/// with explicit <c>NULLS LAST</c> tie-breakers combined with a partition-wide <c>MAX() OVER</c> — which row
/// the newest-plan CASE picks on Postgres, and that a NULL reading reaches the reader as NULL. The text half
/// (byte-identity with Lite's SQL) lives in <c>Lite.Tests.DrillDownDopProvenanceParityTests</c>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class DrillDownDopProvenanceLiveTests
{
    private const int TestServerId = -364800;
    private const string TestServerName = "DopProvSrv";
    private const string Db = "DopProvDb";
    private const string Hash = "0x3648HASH";
    private const string OldParallelPlan = "0x3648PLANPARALLEL";
    private const string NewSerialPlan = "0x3648PLANSERIAL";

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    private static async Task SeedAsync(
        NpgsqlConnection c, string planHash, DateTime collectionTime, int? maxDop, long workerTimeUs,
        DateTime? creationTime, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, creation_time, query_text, delta_execution_count, delta_worker_time,
     delta_elapsed_time, delta_logical_reads, delta_spills, min_dop, max_dop)
VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)", c);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(Hash);
        command.Parameters.AddWithValue(planHash);
        command.Parameters.AddWithValue("0x3648SQLH");
        command.Parameters.AddWithValue(planHash + "H");
        command.Parameters.AddWithValue(DateTime.SpecifyKind(creationTime ?? collectionTime.AddDays(-1), DateTimeKind.Unspecified));
        command.Parameters.AddWithValue("SELECT * FROM DopProvTable");
        command.Parameters.AddWithValue(10L);
        command.Parameters.AddWithValue(workerTimeUs);
        command.Parameters.AddWithValue(workerTimeUs * 2);
        command.Parameters.AddWithValue(1000L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(1);
        command.Parameters.AddWithValue(maxDop.HasValue ? maxDop.Value : DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("DELETE FROM query_stats WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(TestServerId);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<JsonElement> CollectAsync(
        NpgsqlDataSource postgres, AnalysisContext context, string factKey, string drillDownKey)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = factKey,
            StoryPath = factKey,
            /* #3859: the collector matches on PathKeys, not on a split of the rendered path. */
            PathKeys = [factKey],
            /* Past the display gate — below it the expensive drill-downs are skipped wholesale and this
               collector never runs at all. */
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown.TryGetValue(drillDownKey, out var raw), $"{drillDownKey} was not collected");
        return JsonSerializer.SerializeToElement(raw);
    }

    [Fact]
    public async Task TopCpuAndBadActor_HeadlineTheNewestPlansDop_AndCarryTheParallelHistory()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3648 DOP-provenance test.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
        {
            await DeleteTestRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = TruncateToSeconds(DateTime.UtcNow);
            var periodStart = periodEnd.AddHours(-4);
            var parallelLastSeen = periodEnd.AddHours(-3);
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = periodStart,
                TimeRangeEnd = periodEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            var expectedNote = $"DOP 1 (a parallel plan ran at 16 until {parallelLastSeen:yyyy-MM-dd}; 2 plans in window)";

            /* ── The live page, RED before #3648 (max_dop read 16): three parallel snapshots ending three
                  hours before the window's end, then the serial plan at the newest snapshot. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await SeedAsync(connection, OldParallelPlan, parallelLastSeen.AddMinutes(-20), 16, 100_000, null, ct);
                await SeedAsync(connection, OldParallelPlan, parallelLastSeen.AddMinutes(-10), 16, 100_000, null, ct);
                await SeedAsync(connection, OldParallelPlan, parallelLastSeen, 16, 100_000, null, ct);
                await SeedAsync(connection, NewSerialPlan, periodEnd.AddMinutes(-30), 1, 400_000, null, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var top = Assert.Single((await CollectAsync(postgres, context, "CPU_SQL_PERCENT", "top_cpu_queries")).EnumerateArray());
                Assert.Equal(1, top.GetProperty("max_dop").GetInt32());
                Assert.Equal(16, top.GetProperty("max_dop_any_plan").GetInt32());
                Assert.Equal(2, top.GetProperty("plan_count").GetInt64());
                Assert.Equal(parallelLastSeen, DateTime.Parse(top.GetProperty("max_dop_any_plan_last_seen").GetString()!, null,
                    System.Globalization.DateTimeStyles.RoundtripKind));
                Assert.Equal(expectedNote, top.GetProperty("dop_note").GetString());
                /* The windowed total still spans BOTH plans: 3 x 100000 + 400000 us = 700 ms. */
                Assert.Equal(700.0, top.GetProperty("total_cpu_ms").GetDouble());

                var bad = await CollectAsync(postgres, context, "BAD_ACTOR_" + Hash, "bad_actor_query");
                Assert.Equal(1, bad.GetProperty("max_dop").GetInt32());
                Assert.Equal(16, bad.GetProperty("max_dop_any_plan").GetInt32());
                Assert.Equal(2, bad.GetProperty("plan_count").GetInt64());
                Assert.Equal(expectedNote, bad.GetProperty("dop_note").GetString());
            }

            /* ── 0 is unknown: a NULL reading arrives as JSON null on every DOP field and no history is
                  invented from nothing. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedAsync(connection, NewSerialPlan, periodEnd.AddMinutes(-30), null, 400_000, null, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var top = Assert.Single((await CollectAsync(postgres, context, "CPU_SQL_PERCENT", "top_cpu_queries")).EnumerateArray());
                Assert.Equal(JsonValueKind.Null, top.GetProperty("max_dop").ValueKind);
                Assert.Equal(JsonValueKind.Null, top.GetProperty("max_dop_any_plan").ValueKind);
                Assert.Equal(JsonValueKind.Null, top.GetProperty("max_dop_any_plan_last_seen").ValueKind);
                Assert.Equal(JsonValueKind.Null, top.GetProperty("dop_note").ValueKind);
            }

            /* ── Both plans at the newest snapshot (the stale parallel plan still cached beside the serial
                  one that replaced it): collection_time ties, so compile time decides, and the plan
                  compiled LATER is the headline whatever its counter says. Postgres would default a DESC
                  sort to NULLS FIRST where DuckDB defaults NULLS LAST — the explicit NULLS LAST is what
                  keeps this row choice identical across the SKUs. ── */
            var newest = periodEnd.AddMinutes(-30);
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedAsync(connection, OldParallelPlan, newest, 16, 500_000, periodEnd.AddDays(-20), ct);
                await SeedAsync(connection, NewSerialPlan, newest, 1, 100_000, periodEnd.AddDays(-1), ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var top = Assert.Single((await CollectAsync(postgres, context, "CPU_SQL_PERCENT", "top_cpu_queries")).EnumerateArray());
                Assert.Equal(1, top.GetProperty("max_dop").GetInt32());
                Assert.Equal(16, top.GetProperty("max_dop_any_plan").GetInt32());
                Assert.Equal(newest, DateTime.Parse(top.GetProperty("max_dop_any_plan_last_seen").GetString()!, null,
                    System.Globalization.DateTimeStyles.RoundtripKind));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }
}
