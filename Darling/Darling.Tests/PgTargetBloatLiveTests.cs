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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The bloat family's exit criterion (#3691 lane 13), against a real store: fourteen days of hourly
/// <c>pg_table_bloat_stats</c> for one 1 GiB table whose bloat estimate grows 100 MB → 500 MB, beside twenty
/// 2 MB tables sitting at a 90 % spot "bloat" the whole time → exactly one <c>PG_BLOAT_TREND</c> naming the
/// big table and none of the small ones; ten daily <c>pg_index_bloat</c> samples for the big table's index
/// growing 50 MB → 950 MB of reclaimable space beside five 16 kB indexes skipped with two distinct reasons →
/// one <c>PG_INDEX_BLOAT_TREND</c> naming the index with the skipped share; a planted backlog on the SAME
/// table → the edge forms, and through the REAL <c>analyze_server</c> tool the three chain into ONE story,
/// <c>PG_INDEX_BLOAT_TREND → PG_BLOAT_TREND → PG_AUTOVACUUM_BACKLOG</c>.
///
/// <para>Three honesty traps are planted on purpose: a row BEFORE the lookback with a zero estimate (must not
/// become the earliest sample), a row at the lookback's first hour flagged <c>estimate_unavailable</c> with a
/// 5 GB garbage estimate (must be excluded — the #2542 trap), and the small tables' 90 % spot figure (must
/// never be graded). Gated on <c>DARLING_TEST_PG</c>; the planting shape is <see cref="PgTargetFactCollectorTests"/>'
/// with this family's two source tables added; cleanup runs through <see cref="LiveStoreCleanup"/> (the #1902
/// ratchet).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetBloatLiveTests
{
    private const string ServerName = "darling-pg-target-bloat-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const long MiB = 1024L * 1024;

    [Fact]
    public async Task APlantedFortnightOfBloat_ProducesOneTrendNamingTheBigTable_AndOneChainedStoryThroughAnalyzeServer()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the bloat-family e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 18, ct);

            /* Whole-minute bounds, window ending a minute ago (the plumbing e2e's reasoning). */
            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);
            var lookbackStart = windowEnd.AddDays(-PgTargetScorer.BloatLookbackDays);

            /* The gate and the coverage witness: 25 h of span, one row a minute across the window. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* ── pg_table_bloat_stats, hourly across the lookback.
               big   — 1 GiB heap; a zero-estimate row a day BEFORE the lookback (must not be the earliest); hour 0
                       flagged estimate_unavailable with a 5 GB garbage estimate (must be excluded); hours 1..336
                       usable, the estimate rising linearly 100 MB → 500 MB.
               small_N — twenty 2 MB heaps at a 90 % spot "bloat" every six hours, flat: under the floor, never
                       graded, never named. */
            const int hours = 14 * 24;
            await PlantTableBloatAsync(connection, lookbackStart.AddDays(-1), "public", "big", heapBytes: 1024 * MiB, bloatBytes: 0, bloatPct: 0m, dead: 0, unavailable: false, ct);
            await PlantTableBloatAsync(connection, lookbackStart, "public", "big", heapBytes: 1024 * MiB, bloatBytes: 5 * 1024 * MiB, bloatPct: 88.59m, dead: 0, unavailable: true, ct);
            for (var h = 1; h <= hours; h++)
            {
                var at = lookbackStart.AddHours(h);
                var bloat = 100 * MiB + (long)((h - 1) / (double)(hours - 1) * 400 * MiB);
                await PlantTableBloatAsync(connection, at, "public", "big", heapBytes: 1024 * MiB, bloatBytes: bloat, bloatPct: 40m, dead: 1_000_000 + 2_000L * h, unavailable: false, ct);
                if (h % 6 == 0)
                {
                    for (var n = 1; n <= 20; n++)
                        await PlantTableBloatAsync(connection, at, "public", $"small_{n}", heapBytes: 2 * MiB, bloatBytes: (long)(1.8 * MiB), bloatPct: 90m, dead: 500, unavailable: false, ct);
                }
            }

            /* ── pg_index_bloat, daily, ten samples inside the lookback:
               big_pkey — 1.5 GiB, estimated, reclaimable 50 MB → 950 MB (growth 900 MiB: 0.92 base, so the index leads).
               tiny_1..5 — 16 kB, skipped: three with the partial-index reason, two with the never-analysed one. */
            for (var d = 0; d < 10; d++)
            {
                var at = windowEnd.AddDays(-9 + d).AddHours(-1);
                var reclaimable = 50 * MiB + d * 100 * MiB;
                await PlantIndexBloatAsync(connection, at, "public", "big", "big_pkey", indexBytes: 1536 * MiB, reclaimable, estPct: 60.0, skippedReason: null, ct);
                for (var n = 1; n <= 5; n++)
                {
                    var reason = n <= 3
                        ? "partial index: the model scales the parent reltuples, but the index holds only the rows matching its predicate."
                        : "the parent table has never been analyzed (reltuples = -1), so there is no row count to model from.";
                    await PlantIndexBloatAsync(connection, at, "public", $"tiny_{n}", $"tiny_{n}_idx", indexBytes: 16 * 1024, reclaimable: null, estPct: null, skippedReason: reason, ct);
                }
            }

            /* ── pg_autovacuum_stats, hourly across the window, the big table past its line for four samples. */
            var bigDead = new long[] { 900, 2_000, 3_000, 4_000, 5_250 };
            for (var h = 0; h < 5; h++)
            {
                await PlantAutovacuumAsync(connection, windowStart.AddHours(h), "public", "big", live: 10_000, dead: bigDead[h], vacuumThreshold: 1_050, ct);
            }

            /* ── the collector alone. */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);
            Assert.False(context.Coverage!.IsPartial);

            var trend = Assert.Single(facts, f => f.Key == PgTargetFactKeys.BloatTrend);
            Assert.Equal(PgTargetSources.BloatSource, trend.Source);
            Assert.Equal("appdb", trend.DatabaseName);
            Assert.Equal("public.big", trend.ObjectName);
            Assert.Equal(400 * MiB, trend.Value);
            Assert.Equal(1, trend.Metadata[PgTargetScorer.BloatEstimateAvailableKey]);
            Assert.Equal(400 * MiB, trend.Metadata[PgTargetScorer.BloatGrowthBytesKey]);
            /* The earliest USABLE sample is hour 1 (100 MB) — not the pre-lookback zero, not the flagged 5 GB row. */
            Assert.Equal(100 * MiB, trend.Metadata[PgTargetScorer.BloatEarlierBytesKey]);
            Assert.Equal(500 * MiB, trend.Metadata[PgTargetScorer.BloatLatestBytesKey]);
            Assert.Equal(400.0, trend.Metadata[PgTargetScorer.BloatGrowthPctKey], precision: 6);
            Assert.Equal(1, trend.Metadata[PgTargetScorer.BloatGrowthPctComputableKey]);
            Assert.Equal(1024 * MiB, trend.Metadata[PgTargetScorer.BloatObjectBytesKey]);
            Assert.Equal(hours, trend.Metadata[PgTargetScorer.BloatSamplesKey]);
            Assert.Equal(hours - 1, trend.Metadata[PgTargetScorer.BloatSpanHoursKey], precision: 6);
            Assert.Equal(PgTargetScorer.BloatLookbackDays, trend.Metadata[PgTargetScorer.BloatLookbackDaysKey]);
            Assert.Equal(40.0, trend.Metadata[PgTargetScorer.BloatSpotPctKey], precision: 6);
            Assert.Equal(1_000_000 + 2_000L * hours, trend.Metadata[PgTargetScorer.BloatDeadTuplesKey]);
            /* Population: 21 tables seen, all 21 estimable at some sample, ONE over the floor, one over the line. */
            Assert.Equal(21, trend.Metadata[PgTargetScorer.BloatObjectsSeenKey]);
            Assert.Equal(21, trend.Metadata[PgTargetScorer.BloatObjectsEstimableKey]);
            Assert.Equal(1, trend.Metadata[PgTargetScorer.BloatObjectsConsideredKey]);
            Assert.Equal(1, trend.Metadata[PgTargetScorer.BloatObjectsOverLineKey]);
            /* None of the small tables is named, at 90 % or any percentage. */
            Assert.Equal(["public.big"], PgTargetScorer.BloatNamedObjects(trend).ToList());
            Assert.DoesNotContain(trend.Metadata.Keys, k => k.Contains("small_", StringComparison.Ordinal));

            var indexTrend = Assert.Single(facts, f => f.Key == PgTargetFactKeys.IndexBloatTrend);
            Assert.Equal("public.big.big_pkey", indexTrend.ObjectName);
            Assert.Equal(900 * MiB, indexTrend.Value);
            Assert.Equal(50 * MiB, indexTrend.Metadata[PgTargetScorer.BloatEarlierBytesKey]);
            Assert.Equal(950 * MiB, indexTrend.Metadata[PgTargetScorer.BloatLatestBytesKey]);
            Assert.Equal(1536 * MiB, indexTrend.Metadata[PgTargetScorer.BloatObjectBytesKey]);
            Assert.Equal(10, indexTrend.Metadata[PgTargetScorer.BloatSamplesKey]);
            Assert.Equal(9 * 24, indexTrend.Metadata[PgTargetScorer.BloatSpanHoursKey], precision: 6);
            Assert.Equal(6, indexTrend.Metadata[PgTargetScorer.BloatObjectsSeenKey]);
            Assert.Equal(1, indexTrend.Metadata[PgTargetScorer.BloatObjectsEstimableKey]);
            Assert.Equal(1, indexTrend.Metadata[PgTargetScorer.BloatObjectsConsideredKey]);
            Assert.Equal(1, indexTrend.Metadata[PgTargetScorer.BloatObjectsOverLineKey]);
            Assert.Equal(5.0 / 6.0, indexTrend.Metadata[PgTargetScorer.IndexBloatSkippedShareKey], precision: 9);
            Assert.Equal(2, indexTrend.Metadata[PgTargetScorer.IndexBloatSkipReasonsKey]);
            Assert.Equal(1, indexTrend.Metadata[PgTargetScorer.IndexBloatPgstattupleAvailableKey]);
            Assert.Equal(60.0, indexTrend.Metadata[PgTargetScorer.BloatSpotPctKey], precision: 6);

            var backlog = Assert.Single(facts, f => f.Key == PgTargetFactKeys.AutovacuumBacklog);
            Assert.Equal("public.big", backlog.ObjectName);

            /* Scored: the table at 400 MiB on the 256 MiB → 1 GiB ramp = 0.59375, lifted 1.3× by the same-table
               backlog; the index at 900 MiB = 0.919, lifted 1.3× by the table co-fire; the backlog at 5× = 0.72.
               Every measured bar stamps threshold_lineage = 1. */
            new FactScorer().ScoreAll(facts);
            Assert.Equal(0.59375, trend.BaseSeverity, precision: 9);
            Assert.Equal(1, trend.Metadata["threshold_lineage"]);
            Assert.InRange(indexTrend.BaseSeverity, 0.919, 0.920);
            Assert.Equal(1, indexTrend.Metadata["threshold_lineage"]);
            Assert.Contains(trend.AmplifierResults, a => a.Matched && a.Description.Contains("PG_AUTOVACUUM_BACKLOG co-fired", StringComparison.Ordinal));
            Assert.Contains(indexTrend.AmplifierResults, a => a.Matched && a.Description.Contains("PG_BLOAT_TREND co-fired", StringComparison.Ordinal));
            Assert.True(indexTrend.Severity > trend.Severity && trend.Severity > backlog.Severity, $"{indexTrend.Severity} {trend.Severity} {backlog.Severity}");

            /* ── THE EXIT CRITERION: the real analyze_server, ONE story for the three, index-led. */
            var service = new DarlingAnalysisService(postgres);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var chain = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.IndexBloatTrend);
                Assert.Equal(
                    $"{PgTargetFactKeys.IndexBloatTrend} → {PgTargetFactKeys.BloatTrend} → {PgTargetFactKeys.AutovacuumBacklog}",
                    chain.GetProperty("story_path").GetString());
                Assert.Equal(3, chain.GetProperty("fact_count").GetInt32());
                Assert.Equal(PgTargetSources.BloatSource, chain.GetProperty("category").GetString());
                Assert.Equal(900.0 * MiB, chain.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 3);

                /* Consumed by the chain: neither the table trend nor the backlog roots a second story. */
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() is PgTargetFactKeys.BloatTrend or PgTargetFactKeys.AutovacuumBacklog);

                var advice = chain.GetProperty("advice");
                Assert.Equal("public.big.big_pkey in appdb grew 900 MB of estimated reclaimable space (1800% of the earlier estimate) across 10 daily samples spanning 9 days", advice.GetProperty("headline").GetString());
                Assert.Contains("83.3% of the indexes seen were skipped at their latest sample — a majority, across 2 distinct reasons", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("PG_BLOAT_TREND co-fired on this index's table", advice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("The trend is the finding; the percentage is not.", advice.GetProperty("remediation").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain("CREATE INDEX", advice.GetProperty("remediation").GetString(), StringComparison.OrdinalIgnoreCase);

                /* The next_tools are PostgreSQL reads for every hop, never a SQL Server one. */
                var tools = chain.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains("get_pg_index_bloat", tools);
                Assert.Contains("get_pg_table_bloat", tools);
                Assert.Contains("get_pg_autovacuum_health", tools);
                Assert.DoesNotContain(tools, t => t.StartsWith("get_wait", StringComparison.Ordinal) || t.StartsWith("get_index", StringComparison.Ordinal));
            }

            /* The facts read shows the family under its source with the measured-lineage flag. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.BloatSource);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var root = doc.RootElement;
                Assert.Equal(2, root.GetProperty("shown").GetInt32());
                var keys = root.GetProperty("facts").EnumerateArray().Select(f => f.GetProperty("key").GetString()).ToList();
                Assert.Contains(PgTargetFactKeys.BloatTrend, keys);
                Assert.Contains(PgTargetFactKeys.IndexBloatTrend, keys);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static async Task PlantTableBloatAsync(
        NpgsqlConnection connection, DateTime at, string schema, string table, long heapBytes, long bloatBytes, decimal bloatPct, long dead, bool unavailable, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_table_bloat_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name,
     heap_bytes, heap_pages, toast_bytes, index_bytes, live_tuples, dead_tuples, mods_since_analyze, last_analyzed,
     estimated_tuple_bytes, estimated_heap_pages, fillfactor, bloat_bytes_estimate, bloat_pct_estimate, estimate_unavailable,
     alignment_bytes, pgstattuple_available)
VALUES ($1, $2, $3, $4, 'appdb', $5, $6, $7, $7 / 8192, 0, 0, 5000000, $8, 0, NULL, 120.0, ($7 - $9) / 8192, 100, $9, $10, $11, 8, FALSE)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(heapBytes);
        command.Parameters.AddWithValue(dead);
        command.Parameters.AddWithValue(bloatBytes);
        command.Parameters.AddWithValue(bloatPct);
        command.Parameters.AddWithValue(unavailable);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantIndexBloatAsync(
        NpgsqlConnection connection, DateTime at, string schema, string table, string index, long indexBytes, long? reclaimable, double? estPct, string? skippedReason, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_index_bloat
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name, index_name,
     index_bytes, skipped_reason, index_pages, table_rows, fillfactor, est_tuple_bytes, est_leaf_pages,
     est_bloat_pct, est_reclaimable_bytes, pgstattuple_available)
VALUES ($1, $2, $3, $4, 'appdb', $5, $6, $7, $8, $9, $8 / 8192, 5000000, 90, 40, ($8 - COALESCE($10, 0)) / 8192, $11, $10, TRUE)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(index);
        command.Parameters.AddWithValue(indexBytes);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)skippedReason ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = reclaimable.HasValue ? reclaimable.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = estPct.HasValue ? estPct.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantAutovacuumAsync(
        NpgsqlConnection connection, DateTime at, string schema, string table, long live, long dead, long vacuumThreshold, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_autovacuum_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, table_name,
     live_tuples, dead_tuples, mods_since_analyze, inserts_since_vacuum, vacuum_threshold, insert_vacuum_threshold,
     analyze_threshold, autovacuum_disabled, total_bytes, last_vacuum, last_autovacuum, last_analyze, last_autoanalyze,
     vacuum_count, autovacuum_count, analyze_count, autoanalyze_count)
VALUES ($1, $2, $3, $4, 'appdb', $5, $6, $7, $8, 0, 0, $9, -1, 0, FALSE, 1073741824, NULL, NULL, NULL, NULL, 0, 0, 0, 0)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        command.Parameters.AddWithValue(live);
        command.Parameters.AddWithValue(dead);
        command.Parameters.AddWithValue(vacuumThreshold);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_table_bloat_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_index_bloat WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_autovacuum_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
