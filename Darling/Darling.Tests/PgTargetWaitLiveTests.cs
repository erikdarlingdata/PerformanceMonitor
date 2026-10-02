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
/// The wait family's exit criterion (#3542 lane 5), against a real store, on TWO servers: an Aurora-stamped one
/// whose <c>pg_wait_stats</c> rows carry the engine's measured deltas — <c>Lock:relation</c> at a stated fraction
/// of the wait source's observed time with NO <c>is_sampled</c> — and a stock one whose <c>pg_wait_sampling</c>
/// rows carry cumulative sample counts — the SAME key with <c>is_sampled = 1</c>, <c>estimate_resolution_ms</c>
/// and "estimated from sampling" in the prose, reached through the FALLBACK because its <c>pg_wait_stats</c> is
/// empty. The Aurora series plants the three interval states: stored (60), a restart collection (every row 0 →
/// not a sample, its deltas excluded) and pre-V128 NULL rows (→ <c>LAG</c>), so the three-state idiom is pinned
/// on live SQL, not by text alone. Driven through the REAL <c>analyze_server</c> so the story, the advice and
/// the facts read are the product's, not the collector's.
///
/// <para>Gated on <c>DARLING_TEST_PG</c>; the planting shape is <see cref="PgTargetFactCollectorTests"/>' (the
/// plumbing e2e) with the two wait tables added; rows are exactly what the collectors write. Cleanup runs
/// through <see cref="LiveStoreCleanup"/> (the #1902 ratchet).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetWaitLiveTests
{
    private const string AuroraName = "darling-pg-target-wait-aurora-e2e";
    private const string StockName = "darling-pg-target-wait-stock-e2e";
    private static readonly int AuroraId = ServerIdHelper.GetDeterministicHashCode(AuroraName);
    private static readonly int StockId = ServerIdHelper.GetDeterministicHashCode(StockName);

    /* Aurora event ids: the collector keys deltas on the numeric id (type_id << 24 | event). Names are what the
       lookup returned on the planted major; the fact key normalises case, so "Relation" here and "relation" on
       another major land on one key. */
    private const int LockTypeId = 3, LWLockTypeId = 1, IoTypeId = 10, CpuTypeId = 0;

    [Fact]
    public async Task AnAuroraTargetMeasuresRelationLockWaits_AStockTargetEstimatesThemFromSampling_AndTheSameKeyCarriesTheGrade()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the wait-family e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, AuroraId, AuroraName, MonitoredEngineKind.AuroraPostgres, 17, ct);
            /* Both are PostgreSQL to the ENGINE resolution (the registry selects the engine set; an unstamped row
               takes the SQL Server set, per the service's own rule). Inside the set the wait partial never reads
               the kind: the stock target reaches its sampled facts through the READ-then-fallback, because its
               pg_wait_stats is empty — the routing is the data's (adversarial item B). */
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, StockId, StockName, MonitoredEngineKind.Postgres, 16, ct);

            /* Whole-minute bounds, window ending a minute ago (the plumbing e2e's reasoning). */
            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            /* The gate and the coverage witness for both: 25 h of span, one row a minute across the window. */
            foreach (var (id, name) in new[] { (AuroraId, AuroraName), (StockId, StockName) })
            {
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, windowEnd.AddHours(-25), ct);
                for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                    await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, id, name, windowStart.AddMinutes(minute - 1), ct);
            }

            /* ── Aurora: pg_wait_stats, one collection a minute, 241 collections in the window (T-4h .. T).
                 Per steady minute: Lock:Relation 12 s of wait (0.20 of a backend), Lock:transactionid 3 s,
                 LWLock:WALWrite 6 s, IO:DataFileRead 30 s, IO:WALSync 6 s, CPU 40 s (never a fact, never in
                 the share). Three interval states across the series:
                   minutes  0..9  → pre-V128 rows: sample_interval_seconds NULL, so the LAG derives 60 (the first
                                    of them has no predecessor and is not a sample);
                   minute   120   → a RESTART collection: every row's interval 0 and delta 0 → not a sample, and
                                    its (zero) deltas excluded;
                   all others     → stored 60.
                 Countable: 241 − 1 (first, LAG) − 1 (restart) = 239 samples = 14,340 s of wait-source time,
                 against the witness's 14,400 s. Lock:Relation = 239 × 12 s = 2,868 s → 2,868 / 14,340 = 0.20. */
            for (var minute = 0; minute <= 240; minute++)
            {
                var at = windowStart.AddMinutes(minute);
                int? interval = minute < 10 ? null : minute == 120 ? 0 : 60;
                var restart = minute == 120;
                await PlantAuroraCollectionAsync(connection, at, interval, restart, ct);
            }

            /* ── Stock: pg_wait_sampling, one collection every five minutes, 49 collections (T-4h .. T), 10 ms
                 period, cumulative counts. Per five-minute interval Lock:relation gains 6,000 samples (60 s of
                 estimated waiting = 0.20 of a backend), IO:DataFileRead 15,000 (150 s), CPU/Running 30,000; a
                 second Lock:relation series (another query_id) gains 300 (3 s) and RESETS at collection 30 (its
                 count drops to 100 — taken whole, per the reader's rule). 48 intervals = 14,400 s observed.
                 Lock:relation = 48 × 60 s + 47 × 3 s + 1 s (the reset interval's whole count) = 3,022 s. */
            for (var step = 0; step <= 48; step++)
            {
                var at = windowStart.AddMinutes(step * 5);
                var secondSeries = step < 30 ? 1_000L + step * 300 : 100L + (step - 30) * 300;
                await PlantSamplingCollectionAsync(connection, at, relationSamples: 50_000L + step * 6_000, relationSecondSeries: secondSeries,
                    readSamples: 200_000L + step * 15_000, cpuSamples: 900_000L + step * 30_000, ct);
            }

            /* ── the collector alone, Aurora. */
            var collector = new PgTargetFactCollector(postgres);
            var auroraFacts = await collector.CollectFactsAsync(Context(AuroraId, AuroraName, windowStart, windowEnd));
            var auroraWaits = auroraFacts.Where(f => f.Source == PgTargetSources.WaitsSource).ToList();
            Assert.NotEmpty(auroraWaits);

            var relation = Assert.Single(auroraWaits, f => f.Key == PgTargetFactKeys.WaitKey("Lock", "relation"));
            Assert.Equal("Lock:relation", relation.ObjectName);
            Assert.Equal(0.20, relation.Value, precision: 6);
            Assert.Equal(2_868_000, relation.Metadata[PgTargetScorer.WaitMsKey], precision: 3);
            Assert.Equal(14_340_000, relation.Metadata[PgTargetScorer.WaitSourceObservedMsKey], precision: 3);
            Assert.Equal(239, relation.Metadata[PgTargetScorer.WaitSampleCountKey]);
            Assert.Equal(241, relation.Metadata[PgTargetScorer.WaitCollectionCountKey]);
            Assert.Equal(1, relation.Metadata[PgTargetScorer.WaitRestartCollectionsKey]);
            Assert.Equal(14_400_000, relation.Metadata[PgTargetScorer.WaitObservedMsKey], precision: 3);
            Assert.Equal(2_868_000 / 14_400_000.0, relation.Metadata[PgTargetScorer.WaitWitnessFractionKey], precision: 9);
            Assert.Equal(239 * 12, relation.Metadata[PgTargetScorer.WaitCountKey]);
            /* Share of all waiting: (12 + 3 + 6 + 30 + 6) s per minute, CPU excluded → 12 / 57. */
            Assert.Equal(12 / 57.0, relation.Metadata[PgTargetScorer.WaitShareOfWaitTimeKey], precision: 9);
            Assert.Equal(12 / 15.0, relation.Metadata[PgTargetScorer.WaitShareOfTypeKey], precision: 9);
            /* THE GRADE: an Aurora fact carries neither sampled key. */
            Assert.False(relation.Metadata.ContainsKey(PgTargetScorer.WaitIsSampledKey));
            Assert.False(relation.Metadata.ContainsKey(PgTargetScorer.WaitEstimateResolutionMsKey));

            var lockRollup = Assert.Single(auroraWaits, f => f.Key == PgTargetFactKeys.WaitKey("Lock", null));
            Assert.Equal(0.25, lockRollup.Value, precision: 6);
            Assert.Equal(2, lockRollup.Metadata[PgTargetScorer.WaitEventsInTypeKey]);
            Assert.Equal(0.50, Assert.Single(auroraWaits, f => f.Key == PgTargetFactKeys.WaitKey("IO", "DataFileRead")).Value, precision: 6);
            Assert.Equal(0.60, Assert.Single(auroraWaits, f => f.Key == PgTargetFactKeys.WaitKey("IO", null)).Value, precision: 6);
            Assert.Equal(0.10, Assert.Single(auroraWaits, f => f.Key == PgTargetFactKeys.WaitKey("IO", "WALSync")).Value, precision: 6);
            Assert.Equal(0.10, Assert.Single(auroraWaits, f => f.Key == PgTargetFactKeys.WaitKey("LWLock", "WALWrite")).Value, precision: 6);
            Assert.DoesNotContain(auroraWaits, f => f.Key == PgTargetFactKeys.WaitKey("IPC", null));
            Assert.DoesNotContain(auroraWaits, f => f.ObjectName == "CPU");

            /* ── the collector alone, stock: the FALLBACK fired (pg_wait_stats has no rows for this server). */
            var stockFacts = await collector.CollectFactsAsync(Context(StockId, StockName, windowStart, windowEnd));
            var stockWaits = stockFacts.Where(f => f.Source == PgTargetSources.WaitsSource).ToList();
            Assert.NotEmpty(stockWaits);

            var estimated = Assert.Single(stockWaits, f => f.Key == PgTargetFactKeys.WaitKey("Lock", "relation"));
            Assert.Equal("Lock:relation", estimated.ObjectName);
            Assert.Equal(1, estimated.Metadata[PgTargetScorer.WaitIsSampledKey]);
            Assert.Equal(10, estimated.Metadata[PgTargetScorer.WaitEstimateResolutionMsKey]);
            Assert.Equal(14_400_000, estimated.Metadata[PgTargetScorer.WaitSourceObservedMsKey], precision: 3);
            Assert.Equal(48, estimated.Metadata[PgTargetScorer.WaitSampleCountKey]);
            Assert.Equal(49, estimated.Metadata[PgTargetScorer.WaitCollectionCountKey]);
            /* 48 × 6,000 + 47 × 300 + 100 (the reset interval, taken whole) = 302,200 samples × 10 ms. */
            Assert.Equal(302_200, estimated.Metadata[PgTargetScorer.WaitDeltaSamplesKey]);
            Assert.Equal(3_022_000, estimated.Metadata[PgTargetScorer.WaitMsKey], precision: 3);
            Assert.Equal(3_022_000 / 14_400_000.0, estimated.Value, precision: 9);
            Assert.Equal(1, estimated.Metadata[PgTargetScorer.WaitCounterResetsKey]);
            Assert.Equal(3, estimated.Metadata[PgTargetScorer.WaitPeakBackendsKey]);
            Assert.False(estimated.Metadata.ContainsKey(PgTargetScorer.WaitRestartCollectionsKey));
            /* CPU/Running is not waiting: share = 3,022 / (3,022 + 7,200). */
            Assert.Equal(3_022_000 / (3_022_000 + 7_200_000.0), estimated.Metadata[PgTargetScorer.WaitShareOfWaitTimeKey], precision: 9);
            Assert.All(stockWaits, f => Assert.Equal(1, f.Metadata[PgTargetScorer.WaitIsSampledKey]));
            Assert.All(stockWaits, f => Assert.Equal(10, f.Metadata[PgTargetScorer.WaitEstimateResolutionMsKey]));
            Assert.Equal(0.50, Assert.Single(stockWaits, f => f.Key == PgTargetFactKeys.WaitKey("IO", "DataFileRead")).Value, precision: 6);

            /* ── THE EXIT CRITERION, through the real analyze_server. Aurora: IO:DataFileRead (0.50 on a
                 0.15→1.0 ramp = 0.706) and Lock:relation (0.20 → 0.53) each root a story; the IO and Lock
                 ROLLUPS, whose named standouts fired, score 0 and root nothing (one wait is graded once); nothing
                 in this planting fires a cause for either standout to lead to, so both are one-fact stories.
                 The relation card's advice states the measured seconds and never calls itself an estimate. */
            var service = new DarlingAnalysisService(postgres);
            var aurora = await DarlingMcpTools.AnalyzeServer(service, postgres, AuroraName, 4);
            using (var doc = JsonDocument.Parse(aurora))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                var roots = findings.Select(f => f.GetProperty("root_fact").GetProperty("key").GetString()).ToList();

                Assert.Contains(PgTargetFactKeys.WaitKey("IO", "DataFileRead"), roots);
                Assert.DoesNotContain(PgTargetFactKeys.WaitKey("IO", null), roots);
                Assert.DoesNotContain(PgTargetFactKeys.WaitKey("Lock", null), roots);
                Assert.DoesNotContain(PgTargetFactKeys.WaitKey("IO", "WALSync"), roots);      /* 0.10: under the standout bar */

                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.WaitKey("Lock", "relation"));
                Assert.Equal(PgTargetSources.WaitsSource, card.GetProperty("category").GetString());
                Assert.Equal(PgTargetFactKeys.WaitKey("Lock", "relation"), card.GetProperty("story_path").GetString());
                Assert.Equal(0.20, card.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 6);
                var advice = card.GetProperty("advice");
                var text = advice.GetProperty("headline").GetString() + advice.GetProperty("investigation").GetString() + advice.GetProperty("remediation").GetString();
                /* The tool's window is anchored at NOW, not at the minute-truncated end the collector-alone pass
                   used, so the collection count shifts by a minute or two; the per-minute planting makes the
                   FRACTION exact regardless, and the prose is pinned on it. */
                Assert.Contains("the engine measured", text, StringComparison.Ordinal);
                Assert.Contains(" s of Lock:relation waiting across ", text, StringComparison.Ordinal);
                Assert.Contains("0.2 of one backend", text, StringComparison.Ordinal);
                Assert.Contains("80 % of its own wait type", text, StringComparison.Ordinal);
                Assert.Contains("restart or first sighting and contributed no sample", text, StringComparison.Ordinal);
                Assert.DoesNotContain("estimated from sampling", text, StringComparison.Ordinal);
                Assert.DoesNotContain("spent", text, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("RCSI", text, StringComparison.Ordinal);
                Assert.Contains("lock_timeout", text, StringComparison.Ordinal);
                Assert.Contains("idle_in_transaction_session_timeout", text, StringComparison.Ordinal);

                var tools = card.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains(tools, t => t.StartsWith("get_pg_wait", StringComparison.Ordinal));
                Assert.DoesNotContain(tools, t => t.StartsWith("get_wait", StringComparison.Ordinal));
            }

            /* Stock: the same key, estimated. Lock:relation at 0.21 (0.15→1.0 = 0.535) roots; the Lock rollup
               yields to it; the prose says so. */
            var stock = await DarlingMcpTools.AnalyzeServer(service, postgres, StockName, 4);
            using (var doc = JsonDocument.Parse(stock))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                Assert.DoesNotContain(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.WaitKey("Lock", null));
                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.WaitKey("Lock", "relation"));
                var advice = card.GetProperty("advice");
                var text = advice.GetProperty("headline").GetString() + advice.GetProperty("investigation").GetString() + advice.GetProperty("remediation").GetString();
                Assert.Contains("estimated from sampling", text, StringComparison.Ordinal);
                Assert.Contains(" backend-samples at a 10 ms sampling period", text, StringComparison.Ordinal);
                Assert.Contains("cannot see a wait shorter than 10 ms", text, StringComparison.Ordinal);
                Assert.Contains("reset 1 time(s)", text, StringComparison.Ordinal);
                Assert.DoesNotContain("spent", text, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain("the engine measured", text, StringComparison.Ordinal);
            }

            /* The facts read shows the family under its source, with the lineage flag on every graded fact and
               the grade keys where they belong. */
            var auroraJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, AuroraName, 4, PgTargetSources.WaitsSource);
            using (var doc = JsonDocument.Parse(auroraJson))
            {
                var shown = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.Equal(7, shown.Count);   /* Lock, LWLock, IO + the four standouts (no IPC planted) */
                foreach (var fact in shown)
                {
                    var metadata = fact.GetProperty("metadata");
                    /* #3691 lineage, round 2: every Aurora fact says 1 — the LWLock rollup graded on its fleet-measured
                       type bars (2026-09-19); the Lock and IO rollups yielded to a standout past 0.15 and the four
                       standouts graded on the per-event bars, both measured by the 2026-09-20 per-event read (§C3).
                       Round 1 had the standouts and the yielded rollups at 0; the stock side below still does. */
                    Assert.True(metadata.GetProperty("threshold_lineage").GetDouble() == 1, fact.GetProperty("key").GetString());
                    Assert.False(metadata.TryGetProperty(PgTargetScorer.WaitIsSampledKey, out _), fact.GetProperty("key").GetString());
                }
            }
            var stockJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, StockName, 4, PgTargetSources.WaitsSource);
            using (var doc = JsonDocument.Parse(stockJson))
            {
                var shown = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.Equal(4, shown.Count);   /* Lock, Lock:relation, IO, IO:DataFileRead */
                foreach (var fact in shown)
                {
                    var metadata = fact.GetProperty("metadata");
                    Assert.Equal(1, metadata.GetProperty(PgTargetScorer.WaitIsSampledKey).GetDouble());
                    /* A stock sampled estimate is another instrument than the Aurora population measured 2026-09-19. */
                    Assert.Equal(0, metadata.GetProperty("threshold_lineage").GetDouble());
                    Assert.Equal(10, metadata.GetProperty(PgTargetScorer.WaitEstimateResolutionMsKey).GetDouble());
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static AnalysisContext Context(int serverId, string serverName, DateTime start, DateTime end) => new()
    {
        ServerId = serverId,
        ServerName = serverName,
        TimeRangeStart = start,
        TimeRangeEnd = end,
        ServerUtcOffset = TimeSpan.Zero,
    };

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One Aurora collection: six rows as <c>PgWaitStatsCollector</c> writes them — cumulative counters
    /// (arbitrary here; the read uses the deltas), the collect-time deltas, and the interval in one of its three
    /// states. A restart collection writes delta 0 / interval 0 on every row.</summary>
    private static async Task PlantAuroraCollectionAsync(NpgsqlConnection connection, DateTime at, int? interval, bool restart, CancellationToken ct)
    {
        var collectionId = CollectionIdGenerator.Next();
        await PlantAuroraRowAsync(connection, collectionId, at, LockTypeId, "Lock", "Relation", restart ? 0 : 12, restart ? 0 : 12_000_000, interval, ct);
        await PlantAuroraRowAsync(connection, collectionId, at, LockTypeId, "Lock", "transactionid", restart ? 0 : 3, restart ? 0 : 3_000_000, interval, ct);
        await PlantAuroraRowAsync(connection, collectionId, at, LWLockTypeId, "LWLock", "WALWrite", restart ? 0 : 600, restart ? 0 : 6_000_000, interval, ct);
        await PlantAuroraRowAsync(connection, collectionId, at, IoTypeId, "IO", "DataFileRead", restart ? 0 : 3_000, restart ? 0 : 30_000_000, interval, ct);
        await PlantAuroraRowAsync(connection, collectionId, at, IoTypeId, "IO", "WALSync", restart ? 0 : 200, restart ? 0 : 6_000_000, interval, ct);
        await PlantAuroraRowAsync(connection, collectionId, at, CpuTypeId, "CPU", "CPU", restart ? 0 : 1, restart ? 0 : 40_000_000, interval, ct);
    }

    private static async Task PlantAuroraRowAsync(
        NpgsqlConnection connection, long collectionId, DateTime at, int typeId, string type, string waitEvent,
        long deltaWaits, long deltaWaitTimeUs, int? interval, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event,
     waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1000000, 1000000000000, $9, $10, $11)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(AuroraId);
        command.Parameters.AddWithValue(AuroraName);
        command.Parameters.AddWithValue(typeId);
        command.Parameters.AddWithValue(((long)typeId << 24) | (long)(waitEvent.Length * 131 % 0xFFFF));
        command.Parameters.AddWithValue(type);
        command.Parameters.AddWithValue(waitEvent);
        command.Parameters.AddWithValue(deltaWaits);
        command.Parameters.AddWithValue(deltaWaitTimeUs);
        command.Parameters.Add(new NpgsqlParameter { Value = interval.HasValue ? interval.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One stock collection: four series as <c>PgWaitSamplingCollector</c> writes them — CUMULATIVE
    /// sample counts with the period beside them, keyed (event_type, event, query_id).</summary>
    private static async Task PlantSamplingCollectionAsync(
        NpgsqlConnection connection, DateTime at, long relationSamples, long relationSecondSeries, long readSamples, long cpuSamples, CancellationToken ct)
    {
        var collectionId = CollectionIdGenerator.Next();
        await PlantSamplingRowAsync(connection, collectionId, at, "Lock", "relation", 1001, relationSamples, 3, ct);
        await PlantSamplingRowAsync(connection, collectionId, at, "Lock", "relation", 1002, relationSecondSeries, 1, ct);
        await PlantSamplingRowAsync(connection, collectionId, at, "IO", "DataFileRead", 1003, readSamples, 2, ct);
        await PlantSamplingRowAsync(connection, collectionId, at, "CPU", "Running", 0, cpuSamples, 8, ct);
    }

    private static async Task PlantSamplingRowAsync(
        NpgsqlConnection connection, long collectionId, DateTime at, string eventType, string waitEvent, long queryId, long samples, int backends, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_wait_sampling
    (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 10, $9)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(StockId);
        command.Parameters.AddWithValue(StockName);
        command.Parameters.AddWithValue(eventType);
        command.Parameters.AddWithValue(waitEvent);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(samples);
        command.Parameters.AddWithValue(backends);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({AuroraId}, {StockId}); " +
            $"DELETE FROM pg_wait_stats WHERE server_id IN ({AuroraId}, {StockId}); " +
            $"DELETE FROM pg_wait_sampling WHERE server_id IN ({AuroraId}, {StockId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({AuroraId}, {StockId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({AuroraId}, {StockId}); " +
            $"DELETE FROM servers WHERE server_id IN ({AuroraId}, {StockId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
