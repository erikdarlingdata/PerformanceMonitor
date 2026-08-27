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
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Phase-5 analysis slice AN2a fact-collector port. Ungated: PgFactCollector exposes
/// EXACTLY the collect-method surface of Lite's DuckDbFactCollector (the literal name list below
/// — a method ported without updating the list, or renamed, fails loudly; when Lite grows a new
/// collect method the port PR must extend both); every query passes the PG-dialect hygiene pins
/// (no QUALIFY, no bare NOW()/CURRENT_TIMESTAMP, no read_parquet, no DuckDB UNION ALL BY NAME,
/// $N positional parameters only, no N'' literals); every FROM/JOIN target resolves to a V4
/// passthrough view or a shared-catalog collector table; and the single any_value() use (PG 16+,
/// below the supported floor: BYO PG 16+, bundled PG 18) stays confined to the plan-regression query. Gated on DARLING_TEST_PG:
/// migrate, plant rows for two representative collectors (wait-stats and CPU facts), run the full
/// CollectFactsAsync, assert the expected fact keys/values and Lite's emission order, and assert
/// an EMPTY store yields zero facts with NO exception (the swallow posture).
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class PgFactCollectorTests
{
    /// <summary>Distinctive fake ids — a real server_id is a storage-name hash, never these.</summary>
    private const int TestServerId = -626262;
    private const int EmptyServerId = -626263;
    private const string TestServerName = "fact-collector-e2e";

    /// <summary>
    /// Lite's DuckDbFactCollector collect-method surface (ordinal-sorted), enumerated literally:
    /// the port must carry every one of these — same names — and nothing extra. A future Lite
    /// addition must be ported AND added here, so drift fails this test rather than silently
    /// narrowing Darling's analysis.
    /// </summary>
    private static readonly string[] LiteCollectMethodSurface =
    {
        "CollectActiveQueryFactsAsync",
        "CollectBadActorFactsAsync",
        "CollectBlockingChainFactsAsync",
        "CollectBlockingFactsAsync",
        "CollectCpuUtilizationFactsAsync",
        "CollectDatabaseConfigFactsAsync",
        "CollectDatabaseSizeFactAsync",
        "CollectDeadlockFactsAsync",
        "CollectDiskSpaceFactsAsync",
        "CollectFileAutogrowthFactsAsync",
        "CollectIoLatencyFactsAsync",
        "CollectMemoryClerkFactsAsync",
        "CollectMemoryFactsAsync",
        "CollectMemoryGrantFactsAsync",
        "CollectMemoryPressureEventFactsAsync",
        "CollectParameterSensitivityFactsAsync",
        "CollectPerfmonFactsAsync",
        "CollectPlanAdvisoryFactsAsync",
        "CollectPlanCacheFactsAsync",
        "CollectPlanRegressionFactsAsync",
        "CollectProcedureStatsFactsAsync",
        "CollectQueryStatsFactsAsync",
        "CollectRunnableTaskFactsAsync",
        "CollectRunningJobFactsAsync",
        "CollectServerConfigFactsAsync",
        "CollectServerMetadataFactsAsync",
        "CollectServerPropertiesFactsAsync",
        "CollectSessionFactsAsync",
        "CollectTempDbFactsAsync",
        "CollectTraceFlagFactsAsync",
        "CollectWaitStatsFactsAsync"
    };

    /* ---------------- ungated: surface + dialect pins ---------------- */

    [Fact]
    public void ImplementsTheSharedSeam_WithLitesCollectMethodSurface()
    {
        Assert.True(typeof(IFactCollector).IsAssignableFrom(typeof(PgFactCollector)));

        var ported = typeof(PgFactCollector)
            .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
            .Where(m => m.Name.StartsWith("Collect", StringComparison.Ordinal)
                     && m.Name.EndsWith("Async", StringComparison.Ordinal))
            .Select(m => m.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(LiteCollectMethodSurface, ported);
    }

    [Fact]
    public void AllSql_CoversEveryQuery_OnePerCollectMethodPlusTheDmvFallback()
    {
        /* 31 collect methods, one query each, plus the DMV-snapshot fallback the blocking-chain
           method appends through PgBlockingPairRowQuery. */
        Assert.Equal(LiteCollectMethodSurface.Length + 1, PgFactCollector.AllSql.Count);
        Assert.Contains(PgBlockingPairRowQuery.DmvSnapshotSql, PgFactCollector.AllSql);
    }

    [Fact]
    public void AllSql_PgDialect_NoDuckDbOnlyConstructs_NoBareNow_PositionalParams()
    {
        foreach (var sql in PgFactCollector.AllSql)
        {
            var upper = sql.ToUpperInvariant();

            /* QUALIFY is DuckDB-only (Lite keeps it in BaselineProvider, a different slice —
               it must never appear here). */
            Assert.DoesNotContain("QUALIFY", upper, StringComparison.Ordinal);

            /* Bare now()/CURRENT_TIMESTAMP is timestamptz — it would compare the naive-UTC
               columns in the PG server's time zone. Every "now"/window bound is a bound
               Kind-Unspecified parameter. */
            Assert.DoesNotContain("NOW(", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("CURRENT_TIMESTAMP", upper, StringComparison.Ordinal);

            /* Lite's v_* views union the parquet archive; Darling has no parquet tier. */
            Assert.DoesNotContain("READ_PARQUET", upper, StringComparison.Ordinal);
            Assert.DoesNotContain("UNION ALL BY NAME", upper, StringComparison.Ordinal);

            /* Postgres has no N'' literals and no @named parameters — $N positional only. */
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.Contains("$1", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AllSql_AnyValue_OnlyInThePlanRegressionQuery()
    {
        /* any_value() is standard SQL:2023, in Postgres since 16 (product minimum PG is 17).
           It is deliberate in the plan-regression aggregation and nowhere else. */
        Assert.Contains("any_value(query_plan_hash)", PgFactCollector.PlanRegressionSql, StringComparison.Ordinal);
        foreach (var sql in PgFactCollector.AllSql)
        {
            if (sql.Contains("any_value", StringComparison.OrdinalIgnoreCase))
            {
                Assert.Equal(PgFactCollector.PlanRegressionSql, sql);
            }
        }
    }

    [Fact]
    public void AllSql_EveryFromJoinTarget_ResolvesToAV4ViewOrACollectorTable()
    {
        /* The V4 passthrough views, parsed from the migration itself so this can't drift. */
        var v4 = PgMigrations.Scripts.Single(m => m.Version == 4);
        var views = Regex.Matches(v4.Sql, @"CREATE OR REPLACE VIEW (v_\w+)")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);
        Assert.Equal(17, views.Count);

        var tables = CollectorCatalog.All.Select(s => s.TargetTable).ToHashSet(StringComparer.Ordinal);

        foreach (var sql in PgFactCollector.AllSql)
        {
            /* Scan the SQL the SERVER sees, not the comments explaining it. Two things in the raw text
               are not relation references and would be read as one:

               1. `--` comments. These queries carry long rationale comments, and English prose about a
                  join contains the word "join" followed by a word ("an equi-join here would ...") —
                  which a bare FROM/JOIN scan reads as a relation named `here`. A guard that fails
                  because someone explained a join in a comment is a broken guard, not a strict one.
               2. `IS [NOT] DISTINCT FROM`, a COMPARISON OPERATOR that happens to end in the word FROM,
                  so its right-hand operand parses as a relation name.

               Stripping both rather than loosening the assertion: the point of this guard is that every
               real relation reference resolves, and that must stay exact. */
            var scanSql = Regex.Replace(sql, @"--[^\n]*", " ");
            scanSql = Regex.Replace(scanSql, @"\bIS\s+(?:NOT\s+)?DISTINCT\s+FROM\b", " ", RegexOptions.IgnoreCase);

            /* CTE names defined by this query are legal FROM targets too (WITH x AS ( / , y AS (). */
            var ctes = Regex.Matches(scanSql, @"(?:WITH|,)\s*(\w+)\s+AS\s*\(", RegexOptions.Singleline)
                .Select(m => m.Groups[1].Value)
                .ToHashSet(StringComparer.Ordinal);

            foreach (Match m in Regex.Matches(scanSql, @"\b(?:FROM|JOIN)\s+(\w+)", RegexOptions.IgnoreCase))
            {
                var target = m.Groups[1].Value;
                Assert.True(
                    views.Contains(target) || tables.Contains(target) || ctes.Contains(target),
                    $"FROM/JOIN target '{target}' resolves to no V4 view, collector table, or CTE in:\n{sql}");
            }
        }
    }

    [Fact]
    public void BlockingChainSql_KeepsThePairRowShape_AndThePhantomSpidFilter()
    {
        /* The SPID-0 phantom-root filter and the pair cap are behavior, not style — see
           PgBlockingPairRowQuery (Lite's BlockingPairRowQuery carried verbatim). */
        foreach (var sql in new[] { PgFactCollector.BlockingChainSql, PgBlockingPairRowQuery.DmvSnapshotSql })
        {
            Assert.Contains("blocking_spid IS NOT NULL", sql, StringComparison.Ordinal);
            Assert.Contains("blocking_spid <> 0", sql, StringComparison.Ordinal);
            Assert.Contains("LIMIT 5000", sql, StringComparison.Ordinal);
            Assert.Contains("monitor_loop", sql, StringComparison.Ordinal);
            Assert.Contains("contentious_object", sql, StringComparison.Ordinal);
        }
    }

    /* ---------------- gated: live end-to-end collection ---------------- */

    [Fact]
    public async Task EndToEnd_CollectFacts_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fact-collector test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        /* Migrations are idempotent — an older store comes up to current, a current store no-ops. */
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var collector = new PgFactCollector(postgres);

        var bodySucceeded = false;
        try
        {
            /* Whole-second window bounds so the PG microsecond timestamp comparisons are exact.
               Host-UTC window semantics — AnalysisContext times are used as-is. */
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var windowStart = windowEnd.AddHours(-1);
            var sampleTime = windowStart.AddMinutes(30);

            /* Plant the two representative collectors' inputs:
               1. wait_stats — a wait with delta_wait_time_ms > 0 (the WaitStatsSql emission
                  gate) that is neither LCK_M_* nor CX* so the shared grouping helpers no-op:
                  900,000 ms over a 3,600,000 ms window = 0.25 fraction, 3,000 tasks = 300 ms avg. */
            using (var plant = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name,
     wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection))
            {
                plant.Parameters.AddWithValue(CollectionIdGenerator.Next());
                plant.Parameters.AddWithValue(sampleTime);
                plant.Parameters.AddWithValue(TestServerId);
                plant.Parameters.AddWithValue(TestServerName);
                plant.Parameters.AddWithValue("SOS_SCHEDULER_YIELD");
                plant.Parameters.AddWithValue(3000L);
                plant.Parameters.AddWithValue(900000L);
                plant.Parameters.AddWithValue(100000L);
                await plant.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }

            /* 2. cpu_utilization_stats — two samples (80%, 90%): avg 85 → CPU_SQL_PERCENT
                  Value 85; max 90 stays under the CPU_SPIKE gate (max >= 80 AND (avg < 20 OR
                  max/avg >= 3) — 90/85 < 3), so exactly one CPU fact. */
            foreach (var cpuPercent in new[] { 80, 90 })
            {
                using var plant = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
                plant.Parameters.AddWithValue(CollectionIdGenerator.Next());
                plant.Parameters.AddWithValue(sampleTime);
                plant.Parameters.AddWithValue(TestServerId);
                plant.Parameters.AddWithValue(TestServerName);
                plant.Parameters.AddWithValue(cpuPercent);
                plant.Parameters.AddWithValue(5);
                await plant.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }

            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };

            var facts = await collector.CollectFactsAsync(context);

            /* The wait fact: Key = the wait type, Value = fraction of the examined period. */
            var wait = Assert.Single(facts, f => f.Source == "waits");
            Assert.Equal("SOS_SCHEDULER_YIELD", wait.Key);
            Assert.Equal(0.25, wait.Value, precision: 10);
            Assert.Equal(900000, wait.Metadata["wait_time_ms"]);
            Assert.Equal(3000, wait.Metadata["waiting_tasks_count"]);
            Assert.Equal(100000, wait.Metadata["signal_wait_time_ms"]);
            Assert.Equal(800000, wait.Metadata["resource_wait_time_ms"]);
            Assert.Equal(300.0, wait.Metadata["avg_ms_per_wait"], precision: 10);

            /* The CPU fact: Value = average SQL CPU %, and no spurious CPU_SPIKE. */
            var cpu = Assert.Single(facts, f => f.Source == "cpu");
            Assert.Equal("CPU_SQL_PERCENT", cpu.Key);
            Assert.Equal(85.0, cpu.Value, precision: 10);
            Assert.Equal(90.0, cpu.Metadata["max_sql_cpu"], precision: 10);
            Assert.Equal(5.0, cpu.Metadata["avg_other_cpu"], precision: 10);
            Assert.Equal(2, (int)cpu.Metadata["sample_count"]);
            Assert.DoesNotContain(facts, f => f.Key == "CPU_SPIKE");

            /* Lite's emission order: wait facts before CPU facts, and nothing else invented
               from the otherwise-empty store. */
            Assert.True(facts.IndexOf(wait) < facts.IndexOf(cpu),
                "wait facts must be emitted before cpu facts (Lite's collection order)");
            Assert.Equal(2, facts.Count);

            /* An EMPTY (but migrated) store: zero facts, NO exception — the collectors'
               swallow/degrade posture. */
            var emptyContext = new AnalysisContext
            {
                ServerId = EmptyServerId,
                ServerName = "fact-collector-empty",
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };
            Assert.Empty(await collector.CollectFactsAsync(emptyContext));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id IN ({TestServerId}, {EmptyServerId}); " +
            $"DELETE FROM cpu_utilization_stats WHERE server_id IN ({TestServerId}, {EmptyServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    /* ---------------- #2387: PLAN_REGRESSION must prune on the partitioning column ---------------- */

    /// <summary>
    /// The comparison window is on <c>last_execution_time</c> — a plan regression is about when the query
    /// last RAN — but <c>query_store_stats</c> is partitioned and compressed on <c>collection_time</c>, so a
    /// predicate on <c>last_execution_time</c> alone prunes nothing. Reported by @ethosthalsell: every
    /// analysis cycle read the server's whole history and decompressed whatever was compressed, PER SERVER,
    /// with cost scaling on STORE SIZE rather than on the configured window — which is why no VM size fixed
    /// it. Same class as #2344's unbounded watermark <c>MAX</c>, one query over.
    ///
    /// <para><b>Why the extra predicate is safe.</b> It cannot change the result: a row cannot be collected
    /// before the execution it reports, so <c>last_execution_time &gt;= X</c> already implies
    /// <c>collection_time &gt;= X</c>. It is redundant to the ANSWER and load-bearing for the PLANNER —
    /// #2344's "provably free" argument exactly.</para>
    ///
    /// <para><b>Measured on the live use1 store</b> (44 GB, 6 chunks, 4 compressed): a
    /// <c>collection_time</c> bound tight enough to bite reports <c>Chunks excluded during startup: 4</c>,
    /// against <c>0</c> without one. The magnitude depends on how much history the store holds BEYOND the
    /// window — on a store whose raw tier is trimmed to 4 days this prunes nothing and costs nothing, and on
    /// the reporter's store carrying months it is the whole fix.</para>
    /// </summary>
    [Fact]
    public void PlanRegressionSql_BoundsThePartitioningColumn_NotOnlyLastExecutionTime()
    {
        var sql = PgFactCollector.PlanRegressionSql;

        /* The semantic window stays where it belongs. */
        Assert.Contains("last_execution_time >= $2", sql, StringComparison.Ordinal);

        /* And the partitioning column is bounded too, or TimescaleDB cannot exclude a single chunk. */
        Assert.Contains("collection_time >= $3", sql, StringComparison.Ordinal);

        /* Its own parameter, NOT "$2 - INTERVAL '1 day'": a bare parameter comparison is the form runtime
           chunk exclusion handles most reliably, and it keeps the skew margin visible in C# where the
           reason for it is written down. */
        Assert.DoesNotContain("$2 - INTERVAL", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The chunk-exclusion bound sits BELOW the comparison window by a clock-skew margin.
    /// <c>last_execution_time</c> comes from the monitored server's clock and <c>collection_time</c> from the
    /// store's, so a monitored server running fast can report an execution later than the collection that
    /// carried it. Without the margin those newest rows would be excluded — a silent under-count, and a
    /// worse bug than the one being fixed. A margin ABOVE the window would be the wrong direction and prune
    /// nothing extra, so the ordering is asserted rather than the literals.
    /// </summary>
    [Fact]
    public void TheExclusionBound_SitsBelowTheComparisonWindow()
    {
        Assert.True(
            PgFactCollector.PlanRegressionSkewMarginDays > 0,
            "a zero margin would exclude rows whose monitored-server clock runs ahead of the store's");

        Assert.Equal(14, PgFactCollector.PlanRegressionWindowDays);
        Assert.True(
            PgFactCollector.PlanRegressionWindowDays + PgFactCollector.PlanRegressionSkewMarginDays
                > PgFactCollector.PlanRegressionWindowDays,
            "the exclusion bound must reach further back than the comparison window, never less far");
    }
}
