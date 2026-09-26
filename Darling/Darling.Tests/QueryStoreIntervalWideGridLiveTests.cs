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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3953's live pass for the Queries grid: <see cref="QueryStoreIntervalWide.ReadsTableAsync"/>'s gate and
/// <see cref="ViewerDataService.QueryStoreTopTableSql"/>, seeded through the real write path on a real
/// PostgreSQL store. Mirrors <c>PlanRegressionIntervalTableEquivalenceTests</c> (V143's own equivalence test)
/// and reuses <c>QueryStoreIntervalWideWriterTests</c>' Row/WriteAsync shape for the seed.
///
/// <para><see cref="SeedGridAsync"/> is the shared seed: several intervals, every outcome (Regular, Aborted,
/// Exception), an open interval re-fetched across three collections, and two databases. A later lane (B3, the
/// MCP and slicer reads) can call it directly rather than writing its own.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres and then works entirely inside it (the clamp test
   converts query_store_stats to a real hypertable and drops one of its chunks), so it cannot race live
   collection, and serializing it would be pure slowdown. */
public sealed class QueryStoreIntervalWideGridLiveTests
{
    private const int ServerId = -3953930;
    private const int TestTop = 50;

    private static readonly DateTime WindowStart = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddDays(3);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task TheTableRead_EqualsRaw_OpenEndAndLiteralEndAtAppliedThrough_EndToEnd_AndTheLiveGate()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 grid live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await SeedGridAsync(runner, ServerId, WindowStart, ct);
        /* WriteBackfillBatchAsync's own coverage bookkeeping raises filled_since to the WALL-CLOCK "now" it ran
           at (EnsureCoverageSql: GREATEST(clock, MAX(collection_time) seen in the last day) — none of this
           historical seed is within a day of real "now"), not to this seed's own historical span. Force it back
           so the gate can see this as a long-covered store, exactly like PlanRegressionIntervalTableEquivalenceTests'
           own gate test does via a direct UPDATE. */
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);

        var appliedThrough = await ScalarDateTimeAsync(connection,
            "SELECT applied_through FROM collect.query_store_interval_wide_coverage WHERE server_id = @server_id", ct);

        /* Open end (a WPF preset): the gate picks the table, and the table read at the gate's own clamp equals
           raw over every returned column, both ways. */
        var (useTableOpen, clampOpen) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.True(useTableOpen, "expected the gate to pick the table for the seeded, forced-covered window");
        await AssertRawEqualsTableAsync(connection, WindowStart, WindowEnd, clampOpen, null, TestTop, ct);

        /* A literal end exactly at applied_through (an MCP as_of / custom range whose end matches the store's own
           claim): clause 4 does not refuse it (NOT strictly less than), so the table is still picked and still
           equal. */
        var (useTableLiteral, clampLiteral) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, appliedThrough, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.True(useTableLiteral, "expected the gate to pick the table when the literal end exactly matches applied_through");
        await AssertRawEqualsTableAsync(connection, WindowStart, WindowEnd, clampLiteral, appliedThrough, TestTop, ct);

        /* End to end: ViewerDataService.GetQueryStoreTopQueriesAsync, through its own schema-version probe, lands
           on the table (V145 is present) and returns exactly the identities/outcomes/totals raw's own SQL does —
           including the open interval's final running-max execution count (20, after three re-fetches). */
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var endToEndRows = await viewer.GetQueryStoreTopQueriesAsync(ServerId, WindowStart, WindowEnd);
        var rawKeys = await RawTopKeysAsync(connection, WindowStart, WindowEnd, TestTop, ct);
        var endToEndKeys = endToEndRows
            .Select(r => (r.DatabaseName, r.QueryId, r.PlanId, r.ExecutionTypeDesc, r.ReplicaRole, r.TotalExecutions))
            .OrderBy(k => k)
            .ToList();
        Assert.True(rawKeys.Count > 0, "the seed produced no raw rows; the end-to-end comparison would be vacuous");
        Assert.Equal(rawKeys.OrderBy(k => k).ToList(), endToEndKeys);
        Assert.Equal(20, endToEndRows.Single(r => r.DatabaseName == "qsB" && r.QueryId == 4).TotalExecutions);
        Assert.Equal(2, endToEndRows.Select(r => r.DatabaseName).Distinct().Count());

        /* ---- the live gate: each refusal clause on a real connection, then the passing case restored ---- */

        Assert.False((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowEnd.AddHours(-1), WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct)).UseTable,
            "a window under 12 hours must read raw");

        Assert.False((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, appliedThrough.AddMinutes(-1), QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct)).UseTable,
            "a literal end before applied_through must read raw");

        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_wide_pending (server_id, collection_time, database_name, recorded_at) VALUES (@server_id, @cutoff, 'qsA', @cutoff)",
            WindowStart, ct);
        Assert.False((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct)).UseTable,
            "a pending batch must read raw regardless of coverage");
        await ExecAsync(connection, "DELETE FROM collect.query_store_interval_wide_pending WHERE server_id = @server_id", ct);

        Assert.True((await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct)).UseTable,
            "the passing case, restored, must read the table again");
    }

    /// <summary>
    /// Review D4R H1: a window under <see cref="QueryStoreIntervalWide.GridWideMinWindow"/> must issue ZERO
    /// round trips against <c>collect.query_store_interval_wide</c> — no <c>ReadSourceInputsSql</c>, no
    /// <c>ChunkFloorsSql</c>, and above all no <c>PlainTableFloorSql</c> (the unindexed
    /// <c>MIN(first_execution_time) WHERE server_id = $1</c> scan) — counted directly off
    /// <c>pg_stat_user_tables.seq_scan</c>/<c>idx_scan</c> before and after the call, since PostgreSQL's own
    /// catalog is the only seam that tells "no statement ran" apart from "a statement ran and returned
    /// nothing". Ungated at <c>a7fdde9f</c> (pre-fix): fails RED there because every grid read, short window or
    /// not, always ran the gate's own round trips including the floor scan.
    ///
    /// <para><b>Flushed, not merely cleared (2026-09-26 de-flake).</b> This test's earlier shape captured
    /// <c>pg_stat_user_tables.seq_scan</c> immediately before and after the read under test, on the test's own
    /// backend. PostgreSQL throttles a backend's cumulative-stats report to at most once per second
    /// (<c>PGSTAT_MIN_INTERVAL</c>); the seed's own scans against <c>query_store_interval_wide</c>, issued
    /// moments earlier over the runner's pooled data source, can still be sitting PENDING on that other backend
    /// when the "before" snapshot is taken, then land AFTER it — <c>Expected (0, 9), Actual (1, 9)</c> with
    /// nothing wrong in the code under test. <c>pg_stat_force_next_flush()</c> only forces the CALLING
    /// backend's own pending counters to report immediately, so it is issued here on the SAME two connections
    /// that did the seeding and the coverage force (the runner's pooled data source and this test's own
    /// connection) before the "before" snapshot, and again before the "after" snapshot so a real regression
    /// (the mutation below) still shows up rather than sitting pending itself. Ungated on the SAME product code:
    /// this is purely about when the counter becomes visible, not what it counts.</para>
    /// </summary>
    [Fact]
    public async Task ShortWindowRead_IssuesNoRoundTripAgainstTheWideTable()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 H1 round-trip test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await SeedGridAsync(runner, ServerId, WindowStart, ct);
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);

        async Task<(long SeqScan, long IdxScan)> ScanCountsAsync()
        {
            await using var command = new NpgsqlCommand(
                "SELECT COALESCE(seq_scan, 0), COALESCE(idx_scan, 0) FROM pg_stat_user_tables WHERE relname = 'query_store_interval_wide'", connection);
            await using var reader = await command.ExecuteReaderAsync(ct);
            if (!await reader.ReadAsync(ct))
            {
                return (0, 0);
            }

            return (reader.GetInt64(0), reader.GetInt64(1));
        }

        /* Force EVERY backend that touched the wide table during setup to report its pending stats now,
           rather than waiting on the once-per-second throttle, so "before" reflects setup's scans and not a
           partial, still-pending view of them. */
        await ForceStatsFlushAsync(connection, ct);
        await using (var setupFlush = await postgres.OpenConnectionAsync(ct))
        {
            await ForceStatsFlushAsync(setupFlush, ct);
        }

        var shortWindowEnd = WindowStart.AddHours(6);
        var before = await ScanCountsAsync();

        /* The gate itself, under the ruled minimum window (12h): must return "raw" with no round trip at all. */
        var (useTable, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connection, ServerId, WindowStart, shortWindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.False(useTable, "a 6h window is under the grid's 12h minimum and must read raw");

        /* End to end through the viewer, the same surface the review named: no extra connection or transaction
           against the wide table for a short-window grid read. */
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        await viewer.GetQueryStoreTopQueriesAsync(ServerId, WindowStart, shortWindowEnd);

        /* The viewer reads through its OWN pooled NpgsqlDataSource, a backend this test cannot call
           pg_stat_force_next_flush() on directly, so a short settle stands in for it here (the same fallback wait
           used when a backend's pending statistics cannot be flushed directly) — long enough to clear PostgreSQL's once-per-second pending-stats throttle
           (PGSTAT_MIN_INTERVAL) so a real regression (the mutation below) is visible in THIS read rather than
           sitting pending on that backend. Then force THIS test's own backend's stats to report before
           re-reading. */
        await Task.Delay(TimeSpan.FromSeconds(1.1), ct);
        await ForceStatsFlushAsync(connection, ct);
        var after = await ScanCountsAsync();
        Assert.Equal(before, after);
    }

    /// <summary>
    /// #4310 site 3 clause 6's own pin: (a) a store the gate would otherwise route to the table
    /// (<see cref="SeedGridAsync"/>'s tier-2 seed, forced long-covered) still lands on the table —
    /// <c>UseTable == true</c> — and the table read equals raw exactly, over the SAME window
    /// <see cref="QueryStoreIntervalWide.HasLegacyRowSql"/> covers; (b) the legacy twin of the SAME seed
    /// (<see cref="SeedGridLegacyAsync"/>, every row's <c>interval_start_time_utc</c> null) makes
    /// <see cref="QueryStoreIntervalWide.ReadsTableAsync"/> refuse — <c>UseTable == false</c> — over a window
    /// clauses 1-5 alone would otherwise route to the table.
    /// </summary>
    [Fact]
    public async Task LegacyRowInWindow_RefusesTheTable_AndTheTier2SeedDoesNot()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4310 site 3 clause 6 legacy-row live test.");
        var ct = TestContext.Current.CancellationToken;

        /* (a) tier-2 seed: clause 6's own probe finds no legacy row, so the gate picks the table and the table
           read equals raw exactly, exactly like the file's own end-to-end test above. */
        await using var scratchTier2 = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connectionTier2 = await OpenMigratedAsync(scratchTier2, ct);
        await using var postgresTier2 = NpgsqlDataSource.Create(scratchTier2.ConnectionString);
        var runnerTier2 = new DarlingCollectorRunner(postgresTier2, new CollectorDeltaCalculator());
        await SeedGridAsync(runnerTier2, ServerId, WindowStart, ct);
        await ForceFilledSinceAsync(connectionTier2, WindowStart.AddDays(-1), ct);
        var (useTableTier2, clampTier2) = await QueryStoreIntervalWide.ReadsTableAsync(
            connectionTier2, ServerId, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.True(useTableTier2, "the tier-2 seed carries no legacy row; clause 6 must not refuse it");
        await AssertRawEqualsTableAsync(connectionTier2, WindowStart, WindowEnd, clampTier2, null, TestTop, ct);

        /* (b) legacy seed: the IDENTICAL rows, but every one pre-tier-2 (no interval_start_time_utc), over the
           SAME window and coverage forcing. Clauses 1-5 alone would route this to the table (same shape as (a)
           passed); clause 6 must refuse it instead. */
        await using var scratchLegacy = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connectionLegacy = await OpenMigratedAsync(scratchLegacy, ct);
        await using var postgresLegacy = NpgsqlDataSource.Create(scratchLegacy.ConnectionString);
        var runnerLegacy = new DarlingCollectorRunner(postgresLegacy, new CollectorDeltaCalculator());
        await SeedGridLegacyAsync(runnerLegacy, ServerId, WindowStart, ct);
        await ForceFilledSinceAsync(connectionLegacy, WindowStart.AddDays(-1), ct);
        var (useTableLegacy, _) = await QueryStoreIntervalWide.ReadsTableAsync(
            connectionLegacy, ServerId, WindowStart, WindowEnd, null, QueryStoreIntervalWide.GridWideMinWindow, 30, null, ct);
        Assert.False(useTableLegacy, "every row in this seed is legacy (interval_start_time_utc IS NULL); clause 6 must refuse the table");
    }

    /// <summary>
    /// The clamp (review D4R H3): once raw's oldest chunk (this window's own day 0) is dropped, the table still
    /// holds those rows — its own retention is independent of raw's — so an UNCLAMPED table read would show rows
    /// raw no longer has. Bounding the table read at <see cref="QueryStoreIntervalWide.ClampedStart"/> instead
    /// returns exactly what raw itself can still show.
    /// </summary>
    [Fact]
    public async Task Clamp_MatchesRawOnceRawsOldestChunkIsDropped_AndFailsWithoutTheClamp()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #3953 grid clamp test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenMigratedAsync(scratch, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "TimescaleDB must be enabled on the test cluster for the clamp test's chunk drop");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await ScalarBoolAsync(connection,
            "SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_schema = 'collect' AND hypertable_name = 'query_store_stats')", ct),
            "query_store_stats did not convert to a hypertable; the chunk-drop below cannot run");

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        await SeedGridAsync(runner, ServerId, WindowStart, ct);
        await ForceFilledSinceAsync(connection, WindowStart.AddDays(-1), ct);

        /* Baseline: raw still holds everything (including the anchor, 45 days back), so its floor sits well
           before the window and the clamp is a no-op. */
        var rawFloorBefore = await ScalarDateTimeOrNullAsync(connection, ChunkFloorSql, ct);
        Assert.Equal(WindowStart, QueryStoreIntervalWide.ClampedStart(rawFloorBefore, WindowStart));

        /* Drop every chunk wholly before window day 1 — the anchor's chunk (45 days back) and day 0's own chunk.
           Day 1 and day 2 survive. */
        await ExecAsync(connection, "SELECT drop_chunks('collect.query_store_stats', older_than => @cutoff)", WindowStart.AddDays(1), ct);

        var rawFloorAfter = await ScalarDateTimeOrNullAsync(connection, ChunkFloorSql, ct);
        Assert.True(rawFloorAfter is DateTime f && f > WindowStart, $"expected the drop to move raw's floor past {WindowStart:o}; got {rawFloorAfter:o}");
        var clampedStart = QueryStoreIntervalWide.ClampedStart(rawFloorAfter, WindowStart);
        Assert.True(clampedStart > WindowStart, "expected the clamp to move forward once raw's floor rose above the window start");

        /* Without the clamp — reading the table from windowStart, raw's own (pre-drop) bound — the table still
           shows day 0's rows and raw does not any more: they differ. This is the one place this file proves a
           mismatch on purpose, to show the clamp is load-bearing rather than decorative. */
        var (_, unclampedTableOnly, _, _) = await CompareRawAndTableAsync(connection, WindowStart, WindowEnd, WindowStart, null, TestTop, ct);
        Assert.True(unclampedTableOnly > 0, "expected the unclamped table read to show rows raw no longer has, proving the clamp matters");

        /* With the clamp restored: equal again, over the shrunken span raw and the table can both still answer. */
        await AssertRawEqualsTableAsync(connection, WindowStart, WindowEnd, clampedStart, null, TestTop, ct);
    }

    private const string ChunkFloorSql = @"
SELECT MIN(range_start) AT TIME ZONE 'UTC'
FROM timescaledb_information.chunks
WHERE hypertable_schema = 'collect'
AND   hypertable_name = 'query_store_stats';";

    /* ---- the seed (shared with lane B3's MCP/slicer live tests) ------------------------------------------- */

    /// <summary>
    /// Seeds <paramref name="serverId"/> through the real write path: an anchor interval 45 days before
    /// <paramref name="windowStart"/> (pushes the wide table's own floor comfortably past every clause-3 margin
    /// this file's tests need, so windowStart itself can sit exactly on the seeded span); day 0 with two
    /// databases and every outcome (Regular, Aborted, Exception); day 1 with a named replica role; and day 2
    /// with one interval re-fetched across three collections while still open (execution_count and
    /// last_execution_time both growing — Query Store's own collector cadence) alongside one more Regular row in
    /// the other database. Six distinct (database, query, plan, outcome, role) identities in all. Does not touch
    /// the coverage row — a caller that needs the gate to pick the table over historical data must force
    /// <c>filled_since</c> itself (<see cref="ForceFilledSinceAsync"/>'s shape), because a real apply raises it
    /// only as far as the wall clock it ran at.
    ///
    /// <para><b>#4310 site 3 clause 6:</b> every row here carries a real <c>RuntimeStatsIntervalId</c> AND
    /// <c>IntervalStartTimeUtc</c> (tier 2), so <see cref="QueryStoreIntervalWide.HasLegacyRowSql"/> finds
    /// nothing and clause 6 never refuses the table over this seed — the table-path tests below genuinely
    /// exercise the table. <see cref="SeedGridLegacyAsync"/> is the SAME shape with
    /// <c>IntervalStartTimeUtc</c> left null (pre-tier-2), for the refusal pins.</para>
    /// </summary>
    internal static Task SeedGridAsync(DarlingCollectorRunner runner, int serverId, DateTime windowStart, CancellationToken ct) =>
        SeedGridCoreAsync(runner, serverId, windowStart, tier2: true, ct);

    /// <summary>The legacy twin of <see cref="SeedGridAsync"/> (#4310 site 3 clause 6's own pin): the IDENTICAL
    /// rows, but with <c>IntervalStartTimeUtc</c> left null on every one — <c>HasLegacyRowSql</c> finds every
    /// row in this seed, so a gate that would otherwise pick the table over it must refuse instead.</summary>
    internal static Task SeedGridLegacyAsync(DarlingCollectorRunner runner, int serverId, DateTime windowStart, CancellationToken ct) =>
        SeedGridCoreAsync(runner, serverId, windowStart, tier2: false, ct);

    private static async Task SeedGridCoreAsync(DarlingCollectorRunner runner, int serverId, DateTime windowStart, bool tier2, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            ServerId = serverId,
            ServerName = "qsiw-grid-host",
            CollectionTime = DateTime.UtcNow,
            Deltas = new CollectorDeltaCalculator(),
        };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "qsiw-grid", Host = "qsiw-grid-host" },
            ConnectionString = "Server=qsiw-grid-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "qsiw-grid-host",
            ServerId = serverId,
            EngineEdition = 3,
        };

        async Task WriteAsync(DateTime collectionTime, params QueryStoreCollector.Row[] rows)
        {
            foreach (var batch in rows.GroupBy(r => r.DatabaseName))
            {
                await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, batch.ToList(), server, collectionTime, context, ct);
            }
        }

        QueryStoreCollector.Row Row(
            string database, long queryId, long planId, long intervalId, DateTime first, DateTime last, long executions,
            long cpuUs, string? role = null, string type = "Regular") => new()
            {
                DatabaseName = database,
                QueryId = queryId,
                PlanId = planId,
                ExecutionTypeDesc = type,
                FirstExecutionTime = first,
                LastExecutionTime = last,
                QueryHash = "0x" + queryId.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
                QueryPlanHash = "0x" + planId.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
                ExecutionCount = executions,
                AvgCpuTimeUs = cpuUs,
                AvgDurationUs = cpuUs * 2,
                IsForcedPlan = false,
                ForceFailureCount = 0,
                ReplicaRole = role,
                RuntimeStatsIntervalId = intervalId,
                IntervalStartTimeUtc = tier2 ? first : null,
            };

        var anchor = windowStart.AddDays(-45);
        await WriteAsync(anchor.AddMinutes(10), Row("qsA", 900, 9001, 9000, anchor, anchor.AddMinutes(5), 1, 50));

        var day0 = windowStart.AddHours(1);
        await WriteAsync(day0.AddMinutes(10),
            Row("qsA", 1, 11, 100, day0, day0.AddMinutes(9), 10, 500),
            Row("qsB", 2, 21, 200, day0.AddHours(1), day0.AddHours(1).AddMinutes(8), 4, 700, type: "Aborted"),
            Row("qsB", 2, 21, 200, day0.AddHours(1), day0.AddHours(1).AddMinutes(8), 1, 700, type: "Exception"));

        var day1 = windowStart.AddDays(1).AddHours(1);
        await WriteAsync(day1.AddMinutes(10),
            Row("qsA", 3, 31, 300, day1, day1.AddMinutes(9), 8, 300, role: "secondary1"));

        var day2 = windowStart.AddDays(2).AddHours(1);
        await WriteAsync(day2.AddMinutes(10), Row("qsB", 4, 41, 400, day2, day2.AddMinutes(9), 5, 200));
        await WriteAsync(day2.AddMinutes(25), Row("qsB", 4, 41, 400, day2, day2.AddMinutes(24), 12, 220));
        await WriteAsync(day2.AddMinutes(40),
            Row("qsB", 4, 41, 400, day2, day2.AddMinutes(39), 20, 250),
            Row("qsA", 5, 51, 401, day2.AddMinutes(30), day2.AddMinutes(39), 6, 150));
    }

    /* ---- raw vs. table, over every returned column ------------------------------------------------------- */

    private static async Task AssertRawEqualsTableAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, DateTime tableStart, DateTime? literalEndUtc, int top, CancellationToken ct)
    {
        var (rawOnly, tableOnly, rawCount, tableCount) = await CompareRawAndTableAsync(connection, windowStart, windowEnd, tableStart, literalEndUtc, top, ct);
        Assert.True(rawCount > 0, "the seed produced no raw rows; the comparison would be vacuous");
        Assert.Equal(rawCount, tableCount);
        Assert.Equal(0, rawOnly);
        Assert.Equal(0, tableOnly);
    }

    /// <summary>
    /// Materialises <see cref="ViewerDataService.QueryStoreTopSql"/> and <see cref="ViewerDataService.QueryStoreTopTableSql"/>
    /// into temp tables (so their differently-bound positional parameters never collide in one command) and
    /// diffs them both ways with <c>EXCEPT ALL</c> over every returned column. <paramref name="tableStart"/> is
    /// deliberately a caller-supplied value rather than always <see cref="QueryStoreIntervalWide.ClampedStart"/>,
    /// so the clamp test can pass the UNCLAMPED window start and observe the mismatch it causes.
    /// </summary>
    private static async Task<(long RawOnly, long TableOnly, long RawCount, long TableCount)> CompareRawAndTableAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, DateTime tableStart, DateTime? literalEndUtc, int top, CancellationToken ct)
    {
        await ExecAsync(connection, "DROP TABLE IF EXISTS tmp_raw_top, tmp_table_top", ct);

        await using (var raw = new NpgsqlCommand("CREATE TEMP TABLE tmp_raw_top AS " + ViewerDataService.QueryStoreTopSql, connection))
        {
            ViewerDataService.AddServerWindowParameters(raw, ServerId, windowStart, windowEnd);
            raw.Parameters.Add(new NpgsqlParameter<int> { TypedValue = top });
            raw.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
            await raw.ExecuteNonQueryAsync(ct);
        }

        await using (var table = new NpgsqlCommand("CREATE TEMP TABLE tmp_table_top AS " + ViewerDataService.QueryStoreTopTableSql, connection))
        {
            table.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            table.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(tableStart, DateTimeKind.Unspecified) });
            table.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Timestamp,
                Value = literalEndUtc.HasValue ? DateTime.SpecifyKind(literalEndUtc.Value, DateTimeKind.Unspecified) : DBNull.Value,
            });
            table.Parameters.Add(new NpgsqlParameter<int> { TypedValue = top });
            table.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
            await table.ExecuteNonQueryAsync(ct);
        }

        var rawCount = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM tmp_raw_top", ct);
        var tableCount = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM tmp_table_top", ct);
        var rawOnly = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM ((SELECT * FROM tmp_raw_top) EXCEPT ALL (SELECT * FROM tmp_table_top)) AS d", ct);
        var tableOnly = await ScalarLongAsync(connection, "SELECT COUNT(*) FROM ((SELECT * FROM tmp_table_top) EXCEPT ALL (SELECT * FROM tmp_raw_top)) AS d", ct);
        return (rawOnly, tableOnly, rawCount, tableCount);
    }

    /// <summary>The end-to-end oracle: <see cref="ViewerDataService.QueryStoreTopSql"/> itself (raw, unchanged),
    /// read directly rather than through <see cref="ViewerDataService.GetQueryStoreTopQueriesAsync"/> (which
    /// would pick the table for this seed), projected to the identity + outcome + total this file compares
    /// end-to-end. Column ordinals match <c>ranked</c>'s final SELECT list exactly (0 database_name, 1 query_id,
    /// 2 plan_id, 6 total_executions, 19 execution_type_desc, 52 replica_role).</summary>
    private static async Task<List<(string DatabaseName, long QueryId, long PlanId, string ExecutionTypeDesc, string? ReplicaRole, long TotalExecutions)>> RawTopKeysAsync(
        NpgsqlConnection connection, DateTime windowStart, DateTime windowEnd, int top, CancellationToken ct)
    {
        var keys = new List<(string, long, long, string, string?, long)>();
        await using var command = new NpgsqlCommand(ViewerDataService.QueryStoreTopSql, connection);
        ViewerDataService.AddServerWindowParameters(command, ServerId, windowStart, windowEnd);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = top });
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            keys.Add((
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2),
                reader.GetString(19),
                reader.IsDBNull(52) ? null : reader.GetString(52),
                reader.GetInt64(6)));
        }

        return keys;
    }

    /* ---- helpers ------------------------------------------------------------------------------------------ */

    private static async Task<NpgsqlConnection> OpenMigratedAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return connection;
    }

    /// <summary>Forces THIS connection's own pending cumulative-stats counters to report immediately
    /// (<c>pg_stat_force_next_flush()</c>, PostgreSQL 15+), rather than waiting on the once-per-second
    /// throttle every backend is otherwise subject to. Part of the round-trip-count de-flake above.</summary>
    private static async Task ForceStatsFlushAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT pg_stat_force_next_flush()", connection);
        await command.ExecuteScalarAsync(ct);
    }

    private static async Task ForceFilledSinceAsync(NpgsqlConnection connection, DateTime value, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "UPDATE collect.query_store_interval_wide_coverage SET filled_since = @value WHERE server_id = @server_id", connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        command.Parameters.AddWithValue("value", DateTime.SpecifyKind(value, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@server_id", StringComparison.Ordinal))
        {
            command.Parameters.AddWithValue("server_id", ServerId);
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, DateTime cutoff, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        if (sql.Contains("@server_id", StringComparison.Ordinal))
        {
            command.Parameters.AddWithValue("server_id", ServerId);
        }

        command.Parameters.AddWithValue("cutoff", DateTime.SpecifyKind(cutoff, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<bool> ScalarBoolAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<DateTime> ScalarDateTimeAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("server_id", ServerId);
        return (DateTime)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<DateTime?> ScalarDateTimeOrNullAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync(ct);
        return result is DateTime dt ? dt : null;
    }
}
