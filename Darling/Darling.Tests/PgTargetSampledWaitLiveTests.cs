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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Lane 24's exit criterion (#3691), end to end on a real store: a stock-stamped server with 31 days of
/// service-sampler rows (<c>sampled_ms = 30000</c> — the sampler watched 30 s of each 300 s cycle) that were quiet
/// for 30 days and heavy for the last four hours. The collector's wait facts state the HONEST sampled rate — the
/// wait over the time the sampler watched, ten times what lane 5's over-the-interval arithmetic read for the same
/// rows, asserted to the exact number — with <c>sampled_ms_known = 1</c> and the wall interval beside it; the
/// <c>pg_sampled_wait_ms_per_sec</c> baseline is a robust bucket of the same rate; the detector fires
/// <c>ANOMALY_PG_SAMPLED_WAIT_PROFILE</c> on the peak AND the mean; and through the REAL <c>analyze_server</c> the
/// anomaly folds onto the dominant sampled wait's own card (<c>Lock:relation</c>) as one incident, with advice in the
/// sampled grade's words. A second server that carries BOTH sources in the window shows the exact source winning:
/// its facts are the Aurora deltas stamped <c>sampled_suppressed_by_exact = 1</c>, and no sampled anomaly is
/// emitted for it. A third, YOUNG stock server (lane 35 of #3691: six hours of sampler history, so every baseline
/// tier is under its three-distinct-day floor) carries ONE hot cycle in an otherwise quiet window: the detector
/// fires <c>is_new</c> on the peak's bar alone (peak 1,700, mean about 362, under the 500 bar) — the first-occurrence
/// gating the Aurora and SQL Server twins have (#3741), which lane 24's pair-gated arm withheld.
///
/// <para>Gated on <c>DARLING_TEST_PG</c>; <c>[Collection("live-postgres")]</c>; cleanup through
/// <see cref="LiveStoreCleanup"/> (the #1902 ratchet). The planting shape is <c>PgTargetAnomalyTests</c>' e2e
/// (<c>generate_series</c> for the month) and <c>PgTargetWaitLiveTests</c>' (the two wait tables' row shapes).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetSampledWaitLiveTests
{
    private const string StockName = "darling-pg-target-sampled-wait-stock-e2e";
    private const string BothName = "darling-pg-target-sampled-wait-both-e2e";
    private const string YoungName = "darling-pg-target-sampled-wait-young-e2e";
    private static readonly int StockId = ServerIdHelper.GetDeterministicHashCode(StockName);
    private static readonly int BothId = ServerIdHelper.GetDeterministicHashCode(BothName);
    private static readonly int YoungId = ServerIdHelper.GetDeterministicHashCode(YoungName);

    /* Per five-minute cycle, 30 s watched (sampled_ms = 30000), 1,000 ms period. Quiet: Lock:relation 3–5 samples
       (3–5 s of sampled waiting in 30 s watched → 0.10–0.17 of a backend; a deterministic ripple so the bucket has a
       MAD), IO:DataFileRead 6 (0.20), CPU/Running 30 (never waiting). Heavy (the last 250 minutes = 50 cycles):
       Lock:relation 45 → 45 s in 30 s watched = 1.5 backends; IO:DataFileRead stays 6. */
    private const int CycleMinutes = 5, SampledMs = 30_000, PeriodMs = 1_000;
    private const int QuietRelationBase = 3, HeavyRelation = 45, ReadPerCycle = 6, CpuPerCycle = 30;

    [Fact]
    public async Task ThirtyDaysOfSamplerRows_YieldTheHonestRate_ARobustBaseline_AndAProfileAnomalyFoldedOntoTheDominantWait()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the sampled-wait e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, StockId, StockName, MonitoredEngineKind.Postgres, 17, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, BothId, BothName, MonitoredEngineKind.Postgres, 17, ct);
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, YoungId, YoungName, MonitoredEngineKind.Postgres, 17, ct);

            /* 31 days ending a minute ago, whole minutes; the heavy stretch is the last 250 minutes so the tool's own
               four-hour window (anchored at NOW) sits wholly inside it. */
            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            const int minutes = 31 * 24 * 60;
            var start = end.AddMinutes(-minutes);
            const int heavyFromMinute = minutes - 250;
            var windowEnd = end;
            var windowStart = end.AddHours(-4);

            foreach (var (id, name) in new[] { (StockId, StockName), (BothId, BothName) })
            {
                /* The gate and the coverage witness: one pg_database_stats row a minute for the month (constant
                   counters — no TPS, no deadlocks, so no load anomaly muddies the picture). */
                await PlantAsync(connection, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb', 1000, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, $5) AS n", ct, CollectionIdGenerator.Next() + 2_000_000L, start, id, name, minutes);

                /* The sampler's rows: cumulative counts per series, sampled_ms on every row, one collection per cycle. */
                await PlantAsync(connection, @"
WITH cycles AS (
    SELECT n,
           CASE WHEN n >= $6 THEN " + HeavyRelation + " ELSE " + QuietRelationBase + @" + ((n / " + CycleMinutes + @") % 3) END AS relation_inc
    FROM generate_series(0, $5, " + CycleMinutes + @") AS n
),
running AS (
    SELECT n,
           SUM(relation_inc) OVER (ORDER BY n) AS relation_total,
           (n / " + CycleMinutes + @") * " + ReadPerCycle + @" AS read_total,
           (n / " + CycleMinutes + @") * " + CpuPerCycle + @" AS cpu_total
    FROM cycles
)
INSERT INTO pg_wait_sampling
    (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, s.event_type, s.event, s.query_id,
       CASE s.query_id WHEN 1001 THEN 100000 + relation_total WHEN 1003 THEN 200000 + read_total ELSE 900000 + cpu_total END,
       " + PeriodMs + @", CASE s.query_id WHEN 1001 THEN 3 WHEN 1003 THEN 2 ELSE 8 END, " + SampledMs + @"
FROM running
CROSS JOIN (VALUES ('Lock', 'relation', 1001::bigint), ('IO', 'DataFileRead', 1003::bigint), ('CPU', 'Running', 0::bigint)) AS s(event_type, event, query_id)",
                    ct, CollectionIdGenerator.Next() + 3_000_000L, start, id, name, minutes, heavyFromMinute);
            }

            /* The YOUNG server: the same sampler shape over only six hours (two hours of history before the window,
               the window itself), quiet throughout but for ONE heavy cycle two hours into the window. Six hours
               touch at most two calendar dates, so no tier of the sampled bucket reaches its three-distinct-day
               floor and the detector takes its first-occurrence arm; the bucket still has samples (the arm's
               SampleCount == 0 sit-out does not apply). The same six hours of pg_database_stats, because the
               detector's root gate (HasBaselineDataSql) asks that table for ANY row in 30 days before it runs one
               detector — a server with no database stats has no anomaly pass at all, first occurrence or not. */
            const int youngMinutes = 6 * 60;
            var youngStart = end.AddMinutes(-youngMinutes);
            const int youngHotMinute = youngMinutes - 120;
            await PlantAsync(connection, @"
INSERT INTO pg_database_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     xact_commit, xact_rollback, blks_read, blks_hit, temp_files, temp_bytes, deadlocks, stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, 'appdb', 1000, 10, 100, 9000, 0, 0, 0, NULL
FROM generate_series(0, $5) AS n", ct, CollectionIdGenerator.Next() + 5_000_000L, youngStart, YoungId, YoungName, youngMinutes);
            await PlantAsync(connection, @"
WITH cycles AS (
    SELECT n,
           CASE WHEN n = $6 THEN " + HeavyRelation + " ELSE " + QuietRelationBase + @" + ((n / " + CycleMinutes + @") % 3) END AS relation_inc
    FROM generate_series(0, $5, " + CycleMinutes + @") AS n
),
running AS (
    SELECT n,
           SUM(relation_inc) OVER (ORDER BY n) AS relation_total,
           (n / " + CycleMinutes + @") * " + ReadPerCycle + @" AS read_total,
           (n / " + CycleMinutes + @") * " + CpuPerCycle + @" AS cpu_total
    FROM cycles
)
INSERT INTO pg_wait_sampling
    (collection_id, collection_time, server_id, server_name, event_type, event, query_id, sample_count, profile_period_ms, backend_count, sampled_ms)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4, s.event_type, s.event, s.query_id,
       CASE s.query_id WHEN 1001 THEN 100000 + relation_total WHEN 1003 THEN 200000 + read_total ELSE 900000 + cpu_total END,
       " + PeriodMs + @", CASE s.query_id WHEN 1001 THEN 3 WHEN 1003 THEN 2 ELSE 8 END, " + SampledMs + @"
FROM running
CROSS JOIN (VALUES ('Lock', 'relation', 1001::bigint), ('IO', 'DataFileRead', 1003::bigint), ('CPU', 'Running', 0::bigint)) AS s(event_type, event, query_id)",
                ct, CollectionIdGenerator.Next() + 4_000_000L, youngStart, YoungId, YoungName, youngMinutes, youngHotMinute);

            /* The BOTH server also carries the engine's exact deltas for the window: 241 one-minute pg_wait_stats
               collections with the stored interval 60 (Lock:Relation 12 s a minute = 0.20 of a backend). */
            for (var minute = 0; minute <= 240; minute++)
                await PlantAuroraCollectionAsync(connection, windowStart.AddMinutes(minute), ct);

            /* ── The collector alone, stock: the HONEST sampled rate. 49 collections in the closed 4 h window, 48
                 countable; each countable cycle: 45 samples × 1,000 ms = 45 s of sampled Lock:relation waiting over
                 30 s watched → 1.5 of a backend, where over the 300 s interval it read 0.15. */
            var collector = new PgTargetFactCollector(postgres);
            var stockFacts = await collector.CollectFactsAsync(Context(StockId, StockName, windowStart, windowEnd));
            var stockWaits = stockFacts.Where(f => f.Source == PgTargetSources.WaitsSource).ToList();
            Assert.NotEmpty(stockWaits);
            var relation = Assert.Single(stockWaits, f => f.Key == PgTargetFactKeys.WaitKey("Lock", "relation"));
            Assert.Equal(1, relation.Metadata[PgTargetScorer.WaitIsSampledKey]);
            Assert.Equal(1, relation.Metadata[PgTargetScorer.WaitSampledMsKnownKey]);
            Assert.Equal(48, relation.Metadata[PgTargetScorer.WaitSampleCountKey]);
            Assert.Equal(49, relation.Metadata[PgTargetScorer.WaitCollectionCountKey]);
            Assert.Equal(48 * SampledMs, relation.Metadata[PgTargetScorer.WaitSourceObservedMsKey], precision: 3);
            Assert.Equal(48 * CycleMinutes * 60_000, relation.Metadata[PgTargetScorer.WaitSourceIntervalMsKey], precision: 3);
            Assert.Equal(48 * HeavyRelation * PeriodMs, relation.Metadata[PgTargetScorer.WaitMsKey], precision: 3);
            Assert.Equal(1.5, relation.Value, precision: 9);
            /* THE 10×: the same seconds over the wall interval — lane 5's figure, still on the fact as the witness fraction. */
            Assert.Equal(0.15, relation.Metadata[PgTargetScorer.WaitWitnessFractionKey], precision: 9);
            Assert.Equal(relation.Value, relation.Metadata[PgTargetScorer.WaitWitnessFractionKey] * 10, precision: 9);
            Assert.Equal(PeriodMs, relation.Metadata[PgTargetScorer.WaitEstimateResolutionMsKey]);
            Assert.Equal(0.20, Assert.Single(stockWaits, f => f.Key == PgTargetFactKeys.WaitKey("IO", "DataFileRead")).Value, precision: 9);
            Assert.All(stockWaits, f => Assert.Equal(1, f.Metadata[PgTargetScorer.WaitSampledMsKnownKey]));
            Assert.All(stockWaits, f => Assert.False(f.Metadata.ContainsKey(PgTargetScorer.WaitSampledSuppressedByExactKey)));

            /* ── The collector alone, BOTH: the exact source wins and says so; nothing sampled is emitted. */
            var bothFacts = await collector.CollectFactsAsync(Context(BothId, BothName, windowStart, windowEnd));
            var bothWaits = bothFacts.Where(f => f.Source == PgTargetSources.WaitsSource).ToList();
            Assert.NotEmpty(bothWaits);
            Assert.All(bothWaits, f => Assert.Equal(1, f.Metadata[PgTargetScorer.WaitSampledSuppressedByExactKey]));
            Assert.All(bothWaits, f => Assert.False(f.Metadata.ContainsKey(PgTargetScorer.WaitIsSampledKey)));
            Assert.Equal(0.20, Assert.Single(bothWaits, f => f.Key == PgTargetFactKeys.WaitKey("Lock", "relation")).Value, precision: 6);

            /* ── The baseline alone: a robust bucket of the SAMPLED rate, in ms per second watched. Quiet cycles:
                 (3..5 + 6) s over 30 s → 300 / 333.3 / 366.7 ms/s; median 333.3, MAD above 0. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(StockId, MetricNames.PgSampledWaitMsPerSec, windowStart, ct);
            Assert.True(bucket.IsTrustworthy, "the 30-day sampled-wait bucket is not trustworthy");
            Assert.InRange(bucket.Median, 300.0, 367.0);
            Assert.True(bucket.EffectiveRobustSigma > 0, "the ripple should give the bucket a MAD");
            Assert.Equal(0, (await baselines.GetBaselineAsync(StockId, MetricNames.PgWaitMsPerSec, windowStart, ct)).SampleCount);
            /* The BOTH server's sampler rows are a supply for the sampled bucket too; it is the DETECTOR that sits out
               there (below), on the window's exact rows, not the arm. */
            Assert.True((await baselines.GetBaselineAsync(BothId, MetricNames.PgSampledWaitMsPerSec, windowStart, ct)).SampleCount > 0);

            /* ── The detector alone: the sampled profile fires on stock (peak AND mean 1,700 ms/s: (45 + 6) s over
                 30 s), with the contributors; and sits out on BOTH (the exact source wrote the window). */
            var detector = new PgTargetAnomalyDetector(postgres, baselines);
            var stockAnomalies = await detector.DetectAnomaliesAsync(Context(StockId, StockName, windowStart, windowEnd));
            var anomaly = Assert.Single(stockAnomalies, a => a.Key == PgTargetFactKeys.AnomalySampledWaitProfile);
            Assert.DoesNotContain(stockAnomalies, a => a.Key == PgTargetFactKeys.AnomalyWaitProfile);
            Assert.Equal(1_700.0, anomaly.Metadata["current_ms_per_sec"], precision: 6);
            Assert.Equal(1_700.0, anomaly.Metadata["mean_ms_per_sec"], precision: 6);
            Assert.Equal(0, anomaly.Metadata["is_new"]);
            Assert.True(anomaly.Metadata["modified_z"] >= 3.0 * AnomalyThresholds.HeavyTailModifiedZThreshold, $"modified_z {anomaly.Metadata["modified_z"]} — the escape must be reachable on this planting");
            Assert.True(anomaly.Metadata["mean_modified_z"] >= AnomalyThresholds.HeavyTailModifiedZThreshold);
            /* #3653 B: under tiles, window_samples is the WORST HOUR TILE's count (one hour at the
               5-min sampler cadence = 12); the whole window's count moved to window_samples_total. */
            Assert.Equal(48, anomaly.Metadata["window_samples_total"]);
            Assert.Equal(12, anomaly.Metadata["window_samples"]); // the worst hour tile (#3653 B)
            Assert.Equal(48 * SampledMs, anomaly.Metadata[PgTargetScorer.WaitSourceObservedMsKey], precision: 3);
            Assert.Equal(48 * CycleMinutes * 60_000, anomaly.Metadata[PgTargetScorer.WaitSourceIntervalMsKey], precision: 3);
            Assert.Equal(1, anomaly.Metadata[PgTargetScorer.WaitSampledMsKnownKey]);
            Assert.Equal(1, anomaly.Metadata[PgTargetScorer.WaitIsSampledKey]);
            Assert.Equal(0, anomaly.Metadata["threshold_lineage"]);
            Assert.Equal(48 * HeavyRelation * PeriodMs, anomaly.Metadata["contrib_Lock:relation"], precision: 3);
            Assert.Equal(48 * ReadPerCycle * PeriodMs, anomaly.Metadata["contrib_IO:DataFileRead"], precision: 3);
            Assert.False(anomaly.Metadata.ContainsKey("contrib_CPU:Running"));
            Assert.Equal(new[] { PgTargetFactKeys.WaitKey("Lock", "relation"), PgTargetFactKeys.WaitKey("Lock", null) }, PgTargetFactKeys.WaitProfileFamilies(anomaly.Metadata));

            var bothAnomalies = await detector.DetectAnomaliesAsync(Context(BothId, BothName, windowStart, windowEnd));
            Assert.DoesNotContain(bothAnomalies, a => a.Key == PgTargetFactKeys.AnomalySampledWaitProfile);

            /* ── Lane 35: the first-occurrence arm on the YOUNG server. The bucket is untrustworthy (≤ 2 distinct days)
                 yet populated; the window's peak cycle is (45 + 6) s over 30 s = 1,700 ms/s, 3.4× the 500 bar; the
                 window mean is 47 quiet cycles (300 / 333 / 367) and the one hot one — about 362, UNDER the bar. The
                 twins fire this on the peak; lane 24's pair-gated arm held it. */
            var youngBucket = await baselines.GetBaselineAsync(YoungId, MetricNames.PgSampledWaitMsPerSec, windowStart, ct);
            Assert.True(youngBucket.SampleCount > 0, "the young server's bucket must have samples, or the detector sits out for another reason");
            Assert.False(youngBucket.IsTrustworthy, $"six hours of history must not make a trustworthy bucket (distinct days {youngBucket.DistinctDays})");
            var youngAnomalies = await detector.DetectAnomaliesAsync(Context(YoungId, YoungName, windowStart, windowEnd));
            var first = Assert.Single(youngAnomalies, a => a.Key == PgTargetFactKeys.AnomalySampledWaitProfile);
            Assert.Equal(1, first.Metadata["is_new"]);
            Assert.Equal(1_700.0, first.Metadata["current_ms_per_sec"], precision: 6);
            Assert.InRange(first.Metadata["mean_ms_per_sec"], 355.0, 370.0);
            Assert.True(first.Metadata["mean_ms_per_sec"] < AnomalyThresholds.PgSampledWaitProfileFallbackMsPerSec, "the fixture's mean must sit under the bar, or the pin proves nothing");
            Assert.Equal(3.4, first.Metadata["fallback_exceedance"], precision: 6);
            Assert.Equal(0, first.Metadata["ratio"]);
            Assert.Equal(0, first.Metadata["mean_ratio"]);
            Assert.Equal(0, first.Metadata["fire_threshold"]);
            Assert.Equal(48, first.Metadata["window_samples"]);
            Assert.Equal(1, first.Metadata[PgTargetScorer.WaitIsSampledKey]);
            Assert.Equal(1, first.Metadata[PgTargetScorer.WaitSampledMsKnownKey]);
            Assert.Equal(0, first.Metadata["threshold_lineage"]);
            /* Lock:relation over the 48 countable cycles (cycle indices 25..72, hot at 48): 47 quiet on the 3/4/5
               ripple sum to 189, plus the one heavy 45 = 234 samples × the period. IO:DataFileRead is the steady 6 a
               cycle — the LARGER contributor here: one hot cycle does not make Lock the window's leader, and the
               fact says so honestly (the dominant-wait fold would land on IO:DataFileRead). */
            Assert.Equal(234 * PeriodMs, first.Metadata["contrib_Lock:relation"], precision: 3);
            Assert.Equal(48 * ReadPerCycle * PeriodMs, first.Metadata["contrib_IO:DataFileRead"], precision: 3);
            Assert.False(first.Metadata.ContainsKey("contrib_CPU:Running"));
            /* The first-occurrence advice: the absolute-level rendering, no sigma, the sampled grade's words. */
            var firstAdvice = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalySampledWaitProfile, new[] { first }.ToFactLookup())!;
            Assert.Contains("no baseline yet", firstAdvice.Headline, StringComparison.Ordinal);
            Assert.Contains("fired on its absolute level", firstAdvice.Investigation, StringComparison.Ordinal);
            Assert.Contains("estimated from sampling", firstAdvice.Investigation, StringComparison.Ordinal);
            Assert.DoesNotContain("sigma", firstAdvice.Investigation, StringComparison.OrdinalIgnoreCase);

            /* ── THE EXIT CRITERION, through the real analyze_server on the stock server: Lock:relation (sampled, 1.5 on
                 the 0.15 → 1.0 ramp = 1.0) roots; the Lock rollup yields to it; the sampled profile anomaly folds onto
                 the relation card's incident and its advice speaks the sampled grade. */
            var service = new DarlingAnalysisService(postgres);
            var stock = await DarlingMcpTools.AnalyzeServer(service, postgres, StockName, 4);
            using (var doc = JsonDocument.Parse(stock))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                string RootKey(JsonElement f) => f.GetProperty("root_fact").GetProperty("key").GetString()!;

                Assert.DoesNotContain(findings, f => RootKey(f) == PgTargetFactKeys.WaitKey("Lock", null));
                var relationCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.WaitKey("Lock", "relation"));
                Assert.Equal(1.5, relationCard.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 6);
                var relationAdvice = relationCard.GetProperty("advice");
                var relationText = relationAdvice.GetProperty("headline").GetString() + relationAdvice.GetProperty("investigation").GetString() + relationAdvice.GetProperty("remediation").GetString();
                Assert.Contains("estimated from sampling", relationText, StringComparison.Ordinal);
                Assert.Contains("the sampler was watching", relationText, StringComparison.Ordinal);
                Assert.Contains("of wall clock", relationText, StringComparison.Ordinal);
                Assert.Contains("per second watched", relationText, StringComparison.Ordinal);
                Assert.Contains("1.5 of one backend", relationText, StringComparison.Ordinal);
                Assert.DoesNotContain("did not record how long the sampler watched", relationText, StringComparison.Ordinal);
                Assert.DoesNotContain("the engine measured", relationText, StringComparison.Ordinal);
                Assert.DoesNotContain("spent", relationText, StringComparison.OrdinalIgnoreCase);

                var profileCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalySampledWaitProfile);
                Assert.Equal(relationCard.GetProperty("incident_id").GetString(), profileCard.GetProperty("incident_id").GetString());
                Assert.DoesNotContain(findings, f => RootKey(f) == PgTargetFactKeys.AnomalyWaitProfile);
                var profileAdvice = profileCard.GetProperty("advice");
                var profileText = profileAdvice.GetProperty("headline").GetString() + profileAdvice.GetProperty("investigation").GetString() + profileAdvice.GetProperty("remediation").GetString();
                Assert.Contains("estimated from sampling", profileText, StringComparison.Ordinal);
                Assert.Contains("led by Lock:relation", profileText, StringComparison.Ordinal);
                Assert.Contains("The sampler watched", profileText, StringComparison.Ordinal);
                Assert.Contains("mean 1700 ms/sec", profileText, StringComparison.Ordinal);
                Assert.DoesNotContain("the engine measured", profileText, StringComparison.Ordinal);
                Assert.DoesNotContain("spent", profileText, StringComparison.OrdinalIgnoreCase);
                /* One confirmer on stock (the fired sampled standout; no load family moved, no CPU fact exists):
                   1.0 + 0.3, released from the cap by the extremity escape yet under the 1.5 notify floor — the
                   stock ceiling PREAMBLE-3691 states. */
                Assert.Equal(1.3, profileCard.GetProperty("severity").GetDouble(), precision: 6);

                var tools = profileCard.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains("get_pg_wait_sampling", tools);
            }

            /* The facts read: every stock sampled fact carries the honest denominator's keys and lineage 0. */
            var stockJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, StockName, 4, PgTargetSources.WaitsSource);
            using (var doc = JsonDocument.Parse(stockJson))
            {
                var shown = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.Equal(4, shown.Count);   /* Lock, Lock:relation, IO, IO:DataFileRead */
                foreach (var fact in shown)
                {
                    var metadata = fact.GetProperty("metadata");
                    Assert.Equal(1, metadata.GetProperty(PgTargetScorer.WaitIsSampledKey).GetDouble());
                    Assert.Equal(1, metadata.GetProperty(PgTargetScorer.WaitSampledMsKnownKey).GetDouble());
                    Assert.Equal(0, metadata.GetProperty("threshold_lineage").GetDouble());
                    Assert.True(metadata.GetProperty(PgTargetScorer.WaitSourceIntervalMsKey).GetDouble() > metadata.GetProperty(PgTargetScorer.WaitSourceObservedMsKey).GetDouble());
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
        Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 241 },
    };

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One planting statement with its positional parameters (<c>$1</c> collection-id base, <c>$2</c> start,
    /// <c>$3</c>/<c>$4</c> server, <c>$5</c> minutes, and for the sampler <c>$6</c> the first heavy minute) — every bound
    /// parameter is referenced by its statement, as PostgreSQL requires.</summary>
    private static async Task PlantAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        foreach (var value in values)
            command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One exact (engine-differenced) collection for the BOTH server, <c>PgTargetWaitLiveTests</c>' row shape:
    /// Lock:Relation 12 s and CPU 40 s a minute, stored interval 60.</summary>
    private static async Task PlantAuroraCollectionAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct)
    {
        var collectionId = CollectionIdGenerator.Next();
        foreach (var (typeId, type, waitEvent, waits, us) in new[] { (3, "Lock", "Relation", 12L, 12_000_000L), (0, "CPU", "CPU", 1L, 40_000_000L) })
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type_id, wait_event_id, wait_type, wait_event,
     waits, wait_time_us, delta_waits, delta_wait_time_us, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 1000000, 1000000000000, $9, $10, 60)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(BothId);
            command.Parameters.AddWithValue(BothName);
            command.Parameters.AddWithValue(typeId);
            command.Parameters.AddWithValue(((long)typeId << 24) | (long)(waitEvent.Length * 131 % 0xFFFF));
            command.Parameters.AddWithValue(type);
            command.Parameters.AddWithValue(waitEvent);
            command.Parameters.AddWithValue(waits);
            command.Parameters.AddWithValue(us);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({StockId}, {BothId}, {YoungId}); " +
            $"DELETE FROM pg_wait_stats WHERE server_id IN ({StockId}, {BothId}, {YoungId}); " +
            $"DELETE FROM pg_wait_sampling WHERE server_id IN ({StockId}, {BothId}, {YoungId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({StockId}, {BothId}, {YoungId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({StockId}, {BothId}, {YoungId}); " +
            $"DELETE FROM servers WHERE server_id IN ({StockId}, {BothId}, {YoungId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
