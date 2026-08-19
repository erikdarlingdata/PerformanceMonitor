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
/// Live-Postgres pins for #1850, the Darling twin of Lite's
/// <c>QueryStoreReplicaSplitAnalysisTests</c>: the two ANALYSIS-layer Query Store dedups must key on
/// <c>replica_role</c>, like the read side already does (#1845).
///
/// <para>On a SQL Server 2022+ Availability Group with Query Store for secondary replicas enabled, the
/// primary holds ONE shared Query Store carrying every replica's rows, and
/// <c>sys.query_store_runtime_stats</c> is keyed by (plan_id, interval, execution_type, replica_group).
/// Two rows differing only in <c>replica_role</c> are distinct legitimate work. The old partition —
/// (database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time) — is narrower
/// than that identity, so the <c>rn = 1</c> filter did not de-duplicate them: it DISCARDED one
/// replica's row. An under-count, and a silent one.</para>
///
/// <para>Live rather than a string pin because the two things that could go wrong are the ENGINE's
/// answers, not ours: whether <c>IS NOT DISTINCT FROM</c> in a join actually matches NULL to NULL, and
/// whether <c>DISTINCT ON</c> groups NULL <c>replica_role</c> rows together. A test that asserts we
/// emit a partition clause cannot answer either, and both are what stands between this change and
/// silently emptying plan-regression detection on every non-AG server.</para>
///
/// <para>The seed makes the old key fail LOUDLY: both replicas run the same two plans over the same two
/// intervals, but the secondary is always collected one second later, so under the old partition the
/// secondary always wins <c>ORDER BY collection_time DESC</c> and the PRIMARY — the replica carrying the
/// 12x regression — vanishes. The old key reports one offender at 3x against a true two offenders and
/// 12x. Watched RED at exactly those numbers in the Lite twin, whose SQL is the same shape.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStoreReplicaSplitAnalysisLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -918550;
    private const string TestServerName = "AgPrimarySrv";
    private const string Db = "AgDb";
    private const long QueryId = 101;

    private const string GoodPlanHash = "0xGOODPLAN";
    private const string BadPlanHash = "0xBADPLAN";

    /* Per-execution CPU. The bad plan costs 12x the good one on the PRIMARY and only 3x on the
       SECONDARY, so which replica survives the dedup is visible in the worst-factor number itself. */
    private const long GoodCpuUs = 100_000;
    private const long BadCpuUsPrimary = 1_200_000;
    private const long BadCpuUsSecondary = 300_000;

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    /// <summary>
    /// Opens a session with the store's search_path SET explicitly rather than inherited.
    ///
    /// <para>Npgsql pools PHYSICAL sessions, and a session opened before this store's first migration
    /// keeps the pre-ALTER default <c>"$user", public</c> for its whole life — so the bare table name
    /// below resolves to nothing (42P01) on a first run against a fresh database, and then passes on
    /// every rerun once the migration is a no-op. That is a first-run-only failure, i.e. exactly the
    /// kind of flake that gets re-diagnosed instead of fixed. <see cref="LiveStoreCleanup"/> carries the
    /// same SET for the same reason.</para>
    /// </summary>
    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    [Fact]
    public async Task AnalysisDedups_SplitByReplica_AndStillWorkOffAnAg()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #1850 replica-split test.");

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
            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = periodStart,
                TimeRangeEnd = periodEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            /* ── The AG case: two replicas, same query, same two intervals. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await SeedReplicaAsync(connection, periodStart, periodEnd, "PRIMARY", BadCpuUsPrimary, offsetSeconds: 0, ct);
                await SeedReplicaAsync(connection, periodStart, periodEnd, "SECONDARY", BadCpuUsSecondary, offsetSeconds: 1, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var fact = await CollectPlanRegressionFactAsync(postgres, context);
                Assert.NotNull(fact);

                /* RED under the old key: 1. The primary's rows lost ORDER BY collection_time DESC to the
                   secondary's and were discarded, so only one replica's regression was ever counted. */
                Assert.Equal(2.0, fact!.Metadata["offender_count"]);

                /* RED under the old key: 3. Dropping the primary did not merely lose a row — it reported
                   the SECONDARY's much milder regression as the server's worst, understating the real
                   problem by 4x. The under-count being worse than a double-count, in one number. */
                Assert.Equal(12.0, fact.Metadata["worst_regression_factor"], precision: 1);

                var rows = await CollectRegressedQueriesDrillDownAsync(postgres, context);

                /* RED under the old key: 1 row. */
                Assert.Equal(2, rows.Count);

                var byRole = rows.ToDictionary(
                    r => r.GetProperty("replica_role").GetString()!,
                    r => r.GetProperty("regression_factor").GetDouble());

                /* The row shape carries the role so the operator can tell WHICH replica regressed —
                   without it two rows for the same query_id would be indistinguishable noise. */
                Assert.Equal(["PRIMARY", "SECONDARY"], byRole.Keys.OrderBy(k => k, StringComparer.Ordinal));
                Assert.Equal(12.0, byRole["PRIMARY"], precision: 1);
                Assert.Equal(3.0, byRole["SECONDARY"], precision: 1);
            }

            /* ── The 99% case: replica_role NULL, which is every standalone server, every non-AG server
                  and everything below SQL Server 2022. This is the arm that would have caught an
                  equi-join: NULL = NULL is UNKNOWN, so `b.replica_role = l.replica_role` matches nothing
                  and plan-regression detection returns empty for almost every install. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedReplicaAsync(connection, periodStart, periodEnd, role: null, BadCpuUsPrimary, offsetSeconds: 0, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var fact = await CollectPlanRegressionFactAsync(postgres, context);
                Assert.NotNull(fact);
                Assert.Equal(1.0, fact!.Metadata["offender_count"]);
                Assert.Equal(12.0, fact.Metadata["worst_regression_factor"], precision: 1);

                var rows = await CollectRegressedQueriesDrillDownAsync(postgres, context);
                var row = Assert.Single(rows);
                /* NULL reads back as the empty string, not as a dropped property or a literal "NULL". */
                Assert.Equal("", row.GetProperty("replica_role").GetString());
            }

            /* ── #2138 CPU-primary scoring, against the REAL Postgres SQL. Duration-only: CPU flat,
                  duration 5x worse. Duration alone is confounded by blocking, IO waits and machine
                  contention that no plan choice caused — under the old GREATEST this fired at 5.0; now
                  the CPU path (1x < 2) and the corroboration gate (1x < 1.25) both decline it. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedSplitSignalsAsync(connection, periodStart, periodEnd,
                    goodCpuUs: 100_000, goodDurUs: 120_000, badCpuUs: 100_000, badDurUs: 600_000, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                Assert.Null(await CollectPlanRegressionFactAsync(postgres, context));
                /* The drill-down runs the same scoring — a row here that the fact never counted would be
                   incoherent in the report. */
                Assert.Empty(await CollectRegressedQueriesDrillDownAsync(postgres, context));
            }

            /* ── #2138: extreme duration (6x) with mild CPU corroboration (1.5x) DOES fire, at HALF the
                  duration ratio — 3.0, not 6.0 — so it competes honestly with CPU-detected rows. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedSplitSignalsAsync(connection, periodStart, periodEnd,
                    goodCpuUs: 100_000, goodDurUs: 100_000, badCpuUs: 150_000, badDurUs: 600_000, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var fact = await CollectPlanRegressionFactAsync(postgres, context);
                Assert.NotNull(fact);
                Assert.Equal(3.0, fact!.Metadata["worst_regression_factor"], precision: 1);
                /* A duration-fired row reports the duration dimension. */
                Assert.Equal(2.0, fact.Metadata["regressed_dimension"]);
            }

            /* ── #2138 review catch: CPU has PRECEDENCE, so cpu 2.5x with duration 10x (a genuine CPU
                  regression that also picked up blocking) fires the CPU branch at 2.5 — and must be
                  LABELED cpu. Comparing raw ratio magnitudes, correct under the old GREATEST, would
                  call this duration-caused. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedSplitSignalsAsync(connection, periodStart, periodEnd,
                    goodCpuUs: 100_000, goodDurUs: 100_000, badCpuUs: 250_000, badDurUs: 1_000_000, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var fact = await CollectPlanRegressionFactAsync(postgres, context);
                Assert.NotNull(fact);
                Assert.Equal(2.5, fact!.Metadata["worst_regression_factor"], precision: 1);
                Assert.Equal(1.0, fact.Metadata["regressed_dimension"]);
            }

            /* ── #2138: below the spend floor. A 12x CPU ratio on a query burning 1.2 CPU-seconds across
                  the whole window (100 execs x 12ms) is sampling jitter, not a finding. Same 12x ratio as
                  the arms above — the only difference is absolute spend, so this pins the 10-CPU-second
                  noise floor and nothing else. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedSplitSignalsAsync(connection, periodStart, periodEnd,
                    goodCpuUs: 1_000, goodDurUs: 21_000, badCpuUs: 12_000, badDurUs: 32_000, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                Assert.Null(await CollectPlanRegressionFactAsync(postgres, context));
                Assert.Empty(await CollectRegressedQueriesDrillDownAsync(postgres, context));
            }

            /* ── #2138 gap 3: the regressed query's hash ALSO shows the parameter-sensitivity signature
                  in the plan cache (ratio 20x, past every detector floor) — the drill-down row must
                  carry the flag, because the force-plan caution and the future bot's never-auto-force
                  gate both read it. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedReplicaAsync(connection, periodStart, periodEnd, role: null, BadCpuUsPrimary, offsetSeconds: 0, ct);
                await SeedPlanCacheRowAsync(connection, periodStart, periodEnd, minWorkerUs: 15_000, maxWorkerUs: 300_000, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var row = Assert.Single(await CollectRegressedQueriesDrillDownAsync(postgres, context));
                Assert.True(row.GetProperty("parameter_sensitivity_cofired").GetBoolean());
            }

            /* ── A 2x worker-time spread is ordinary variance, not the >= 10x signature: the flag stays
                  false. Pins that the flag uses the detector's own threshold, not mere cache presence. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await DeleteTestRowsAsync(connection, ct);
                await SeedReplicaAsync(connection, periodStart, periodEnd, role: null, BadCpuUsPrimary, offsetSeconds: 0, ct);
                await SeedPlanCacheRowAsync(connection, periodStart, periodEnd, minWorkerUs: 150_000, maxWorkerUs: 300_000, ct);
            }

            await using (var postgres = NpgsqlDataSource.Create(connectionString!))
            {
                var row = Assert.Single(await CollectRegressedQueriesDrillDownAsync(postgres, context));
                Assert.False(row.GetProperty("parameter_sensitivity_cofired").GetBoolean());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    private static async Task<Fact?> CollectPlanRegressionFactAsync(NpgsqlDataSource postgres, AnalysisContext context)
    {
        var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
        return facts.FirstOrDefault(f => f.Key == "PLAN_REGRESSION");
    }

    private static async Task<List<JsonElement>> CollectRegressedQueriesDrillDownAsync(
        NpgsqlDataSource postgres, AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            /* Past the display gate — below it the expensive drill-downs are skipped wholesale and this
               collector never runs at all. */
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("regressed_queries", out var raw))
            return [];

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    /// <summary>
    /// One replica's rows: two plans over two intervals, each interval collected TWICE — the incremental
    /// re-collection the dedup exists to fold. The cheap baseline plan last ran 5 days ago; the regressed
    /// plan is still running at the end of the window.
    /// </summary>
    private static async Task SeedReplicaAsync(
        NpgsqlConnection connection, DateTime periodStart, DateTime periodEnd,
        string? role, long badCpuUs, int offsetSeconds, CancellationToken ct)
    {
        var goodFirstExec = periodStart.AddDays(-6);
        var goodLastExec = periodStart.AddDays(-5);
        var badFirstExec = periodStart.AddDays(-1);
        var badLastExec = periodEnd;

        for (var collection = 0; collection < 2; collection++)
        {
            var collectionTime = periodEnd.AddMinutes(-10 + collection).AddSeconds(offsetSeconds);

            await SeedRowAsync(connection, collectionTime, planId: 1, GoodPlanHash, intervalId: 1,
                goodFirstExec, goodLastExec, GoodCpuUs, role, avgDurUs: null, ct);
            await SeedRowAsync(connection, collectionTime, planId: 2, BadPlanHash, intervalId: 2,
                badFirstExec, badLastExec, badCpuUs, role, avgDurUs: null, ct);
        }
    }

    /// <summary>
    /// One replica-less query, two plans, CPU and duration controlled INDEPENDENTLY — the seed shape for
    /// the #2138 CPU-primary scoring arms, where which signal moved is the entire test.
    /// </summary>
    private static async Task SeedSplitSignalsAsync(
        NpgsqlConnection connection, DateTime periodStart, DateTime periodEnd,
        long goodCpuUs, long goodDurUs, long badCpuUs, long badDurUs, CancellationToken ct)
    {
        var collectionTime = periodEnd.AddMinutes(-10);

        await SeedRowAsync(connection, collectionTime, planId: 1, GoodPlanHash, intervalId: 1,
            periodStart.AddDays(-6), periodStart.AddDays(-5), goodCpuUs, role: null, goodDurUs, ct);
        await SeedRowAsync(connection, collectionTime, planId: 2, BadPlanHash, intervalId: 2,
            periodStart.AddDays(-1), periodEnd, badCpuUs, role: null, badDurUs, ct);
    }

    private static async Task SeedRowAsync(
        NpgsqlConnection connection, DateTime collectionTime, long planId, string planHash, long intervalId,
        DateTime firstExecutionTime, DateTime lastExecutionTime, long avgCpuUs, string? role,
        long? avgDurUs, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     query_text, query_hash, query_plan_hash, execution_count,
     avg_cpu_time_us, avg_duration_us, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $8, $9, $10, $11, $12, $13, '0xREGRESSQH', $14, $15, $16, $17, false, 0)";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(QueryId);
        command.Parameters.AddWithValue(planId);
        command.Parameters.AddWithValue(role is null ? DBNull.Value : role);
        command.Parameters.AddWithValue(intervalId);
        command.Parameters.AddWithValue(firstExecutionTime);
        command.Parameters.AddWithValue(firstExecutionTime);
        command.Parameters.AddWithValue(lastExecutionTime);
        command.Parameters.AddWithValue("SELECT * FROM dbo.Orders WHERE CustomerId = @id");
        command.Parameters.AddWithValue(planHash);
        /* 100 executions per (plan, replica) collection — comfortably past the HAVING >= 25 floor even
           after the dedup collapses the repeat collections down to one row. */
        command.Parameters.AddWithValue(100L);
        command.Parameters.AddWithValue(avgCpuUs);
        /* Duration defaults to CPU + 20ms, so the classic seeds regress on BOTH signals and fire the
           CPU-primary path (#2138); the split-signal arms pass avgDurUs to move one without the other. */
        command.Parameters.AddWithValue(avgDurUs ?? avgCpuUs + 20_000);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// One plan-cache row for THE regressed query's hash ('0xREGRESSQH'), with the worker-time spread
    /// as the only dial — the #2138 gap-3 PSP-signature seed. Grants flat, no spills.
    /// </summary>
    private static async Task SeedPlanCacheRowAsync(
        NpgsqlConnection connection, DateTime periodStart, DateTime periodEnd,
        long minWorkerUs, long maxWorkerUs, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, query_plan_hash, creation_time, execution_count,
     min_worker_time, max_worker_time, min_grant_kb, max_grant_kb,
     min_spills, max_spills, query_text, delta_execution_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)";

        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(periodEnd.AddMinutes(-5));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue("0xREGRESSQH");
        command.Parameters.AddWithValue(BadPlanHash);
        command.Parameters.AddWithValue(periodStart.AddDays(-3));
        command.Parameters.AddWithValue(100L);
        command.Parameters.AddWithValue(minWorkerUs);
        command.Parameters.AddWithValue(maxWorkerUs);
        command.Parameters.AddWithValue(1024L);
        command.Parameters.AddWithValue(1024L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue(0L);
        command.Parameters.AddWithValue("SELECT * FROM dbo.Orders WHERE CustomerId = @id");
        command.Parameters.AddWithValue(50L);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        /* Two commands, not one multi-statement string: Npgsql does not allow parameters in
           multi-statement commands. query_stats carries the #2138 gap-3 plan-cache seeds. */
        await using (var command = new NpgsqlCommand(
            "DELETE FROM query_store_stats WHERE server_id = $1", connection))
        {
            command.Parameters.AddWithValue(TestServerId);
            await command.ExecuteNonQueryAsync(ct);
        }

        await using (var command = new NpgsqlCommand(
            "DELETE FROM query_stats WHERE server_id = $1", connection))
        {
            command.Parameters.AddWithValue(TestServerId);
            await command.ExecuteNonQueryAsync(ct);
        }
    }
}
