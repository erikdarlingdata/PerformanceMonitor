/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
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
        "CollectObservedCoverageAsync",
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
        /* 32 collect methods (31 fact readers plus the #3538 coverage witness), one query each, plus
           the DMV-snapshot fallback the blocking-chain method appends through PgBlockingPairRowQuery, plus
           PLAN_REGRESSION's #3953 table twin: the plan-regression method runs one of two reads per server,
           and Lite has no store for the second (its DuckDB keeps no latest-snapshot interval table). */
        Assert.Equal(LiteCollectMethodSurface.Length + 2, PgFactCollector.AllSql.Count);
        Assert.Contains(PgFactCollector.PlanRegressionTableSql, PgFactCollector.AllSql);
        Assert.Contains(PgBlockingPairRowQuery.DmvSnapshotSql, PgFactCollector.AllSql);
        Assert.Contains(PgFactCollector.CoverageSql, PgFactCollector.AllSql);
    }

    /* ---------------- #3538 A2: the coverage witness ---------------- */

    /// <summary>
    /// #3538 A2: the coverage witness reads the SAME series the wait fractions are summed from, finds
    /// the first in-window row's predecessor by scanning one gap policy back ($4) and clips its
    /// interval to the window, and credits an interval past the policy ($5, bound — never a literal, so
    /// it cannot drift from the calculator that discarded the delta) as zero. Pinned on the text
    /// because each of those is a behaviour a well-meaning simplification would remove: drop the
    /// lookback and every window under-credits one cadence; inline 3600 and the next policy change
    /// deflates rates again; drop the policy branch and a three-hour outage credits an hour of
    /// observation to a delta the calculator threw away.
    /// </summary>
    [Fact]
    public void CoverageSql_ReadsTheWaitSeries_LooksBackOnePolicy_ClipsToTheWindow_AndDiscardsPastThePolicy()
    {
        var sql = PgFactCollector.CoverageSql;

        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT DISTINCT collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time) OVER (ORDER BY collection_time)", sql, StringComparison.Ordinal);

        /* The lookback bound and the policy are PARAMETERS. */
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.Contains("> $5 THEN 0", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("3600", sql, StringComparison.Ordinal);

        /* Clipped to the window start, so observed time can never exceed the nominal window. */
        Assert.Contains("GREATEST(previous_time, $2)", sql, StringComparison.Ordinal);

        /* The edge columns the C# finishes the lead-in and tail gaps from. */
        Assert.Contains("AS orphan_count", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_sample", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(collection_time) AS last_sample", sql, StringComparison.Ordinal);

        /* Byte-identical to Lite's inline text — the two collectors are a method-for-method port and
           the shared dialect is the whole reason this query could be written once. */
        var lite = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Analysis", "DuckDbFactCollector.Waits.cs"));
        Assert.Contains(sql.Trim(), lite, StringComparison.Ordinal);
    }

    /// <summary>
    /// The three rate-fact sites divide by the OBSERVED duration and bail on an unobserved window, in
    /// both SKUs — a wait fact emitted against the nominal window on either side would silently
    /// re-open the defect for that SKU only, and the census above cannot see a divisor.
    /// </summary>
    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Analysis/PgFactCollector.Waits.cs")]
    [InlineData("Lite/Analysis/DuckDbFactCollector.Waits.cs")]
    public void RateFacts_DivideByObservedDuration_AndBailWhenUnobserved(string relativePath)
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(), relativePath));

        Assert.Contains("waitTimeMs / context.ObservedDurationMs", source, StringComparison.Ordinal);
        Assert.DoesNotContain("waitTimeMs / context.PeriodDurationMs", source, StringComparison.Ordinal);
        Assert.Equal(2, CountOf(source, "var observedHours = context.ObservedDurationMs / 3_600_000.0;"));
        Assert.Equal(3, CountOf(source, "if (context.ObservedDurationMs <= 0) return;"));

        /* Every use of the nominal window left in the file is a metadata statement of what was asked
           for, never a divisor. */
        foreach (var line in source.Split('\n').Where(l => l.Contains("context.PeriodDurationMs", StringComparison.Ordinal)))
        {
            Assert.True(
                line.Contains("[\"period_duration_ms\"]", StringComparison.Ordinal)
                    || line.Contains("var periodHours = ", StringComparison.Ordinal)
                    || line.Contains("var nominalMs = ", StringComparison.Ordinal),
                $"{relativePath} still uses the nominal window somewhere other than metadata: {line.Trim()}");
        }
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0; i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
            count++;
        return count;
    }

    /* The house idiom for source-anchored pins (AnalysisPassCommandTimeoutTests et al.): walk up from
       THIS file, so the pin reads the tree it was compiled from rather than wherever the binary runs. */
    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
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
        Assert.Contains("any_value(query_plan_hash)", PgFactCollector.PlanRegressionTableSql, StringComparison.Ordinal);
        foreach (var sql in PgFactCollector.AllSql)
        {
            if (sql.Contains("any_value", StringComparison.OrdinalIgnoreCase))
            {
                /* The two PLAN_REGRESSION reads (#3953: raw, and its interval-table twin), and nothing else. */
                Assert.True(
                    sql == PgFactCollector.PlanRegressionSql || sql == PgFactCollector.PlanRegressionTableSql,
                    "any_value() outside the PLAN_REGRESSION reads:\n" + sql);
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

                /* #3953: the latest-snapshot interval table is the one non-collector relation a fact reads,
                   sourced from its Storage class rather than restated, as #2150 did for query_store_text in
                   the drill-down guard. */
                Assert.True(
                    views.Contains(target) || tables.Contains(target) || ctes.Contains(target)
                        || target == QueryStoreIntervalLatest.TableName,
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
                  900,000 ms over a 3,600,000 ms window = 0.25 fraction, 3,000 tasks = 300 ms avg.

                  Two rows, not one (#3538 A2): a baseline reading at the window start with no
                  knowable delta, and the delta row at the window end whose change accrued over the
                  hour between them. The fraction now divides by the OBSERVED collection time — the
                  interval between consecutive readings — and a lone delta row with nothing before it
                  observes no time at all (the calculator's first sighting), so the single-row fixture
                  this used to plant would correctly yield no wait fact. The series covers the window
                  exactly, so the 0.25 the scenario documents is unchanged. */
            foreach (var (time, deltaTasks, deltaWaitMs, deltaSignalMs) in new[]
            {
                (windowStart, 0L, 0L, 0L),
                (windowEnd, 3000L, 900000L, 100000L)
            })
            {
                using var plant = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name,
     wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
                plant.Parameters.AddWithValue(CollectionIdGenerator.Next());
                plant.Parameters.AddWithValue(time);
                plant.Parameters.AddWithValue(TestServerId);
                plant.Parameters.AddWithValue(TestServerName);
                plant.Parameters.AddWithValue("SOS_SCHEDULER_YIELD");
                plant.Parameters.AddWithValue(deltaTasks);
                plant.Parameters.AddWithValue(deltaWaitMs);
                plant.Parameters.AddWithValue(deltaSignalMs);
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

            /* #3538 A2: the series covered the window exactly, so coverage is full, the wait fact
               carries a coverage_fraction of 1, and no COLLECTION_GAP fact is emitted. */
            Assert.NotNull(context.Coverage);
            Assert.Equal(1.0, context.Coverage!.Fraction, precision: 6);
            Assert.False(context.Coverage.IsPartial);
            Assert.Equal(2, context.Coverage.SampleCount);
            Assert.Equal(1.0, wait.Metadata["coverage_fraction"], precision: 6);
            Assert.Equal(3_600_000, wait.Metadata["period_duration_ms"]);
            Assert.DoesNotContain(facts, f => f.Key == WindowCoverage.FactKey);

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
            Assert.NotNull(emptyContext.Coverage);
            Assert.False(emptyContext.Coverage!.IsObserved);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ---------------- #3527: perfmon facts are per-second rates ---------------- */

    /// <summary>
    /// #3527: delta_cntr_value spans one COLLECTION INTERVAL, not one second — read raw, the
    /// PERFMON_*_SEC facts overstate by the cadence (60x at 60s, 300x at 5min). The query must
    /// select the row's measured sample_interval_seconds (#2234) for the division and filter
    /// interval &lt;= 0 rows (no delta was knowable: first sighting, reset, gap) so rn = 1 lands on
    /// the newest row a rate can honestly be derived from.
    /// </summary>
    [Fact]
    public void PerfmonSql_SelectsTheMeasuredInterval_AndFiltersUnknowableRows()
    {
        var sql = PgFactCollector.PerfmonSql;

        Assert.Contains("delta_cntr_value, sample_interval_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("sample_interval_seconds > 0", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3527 live fixture: a 'Batch Requests/sec' row with delta 6000 over a measured 60s interval
    /// must emit PERFMON_BATCH_REQ_SEC = 100 (not 6000), with the raw delta and the divisor in the
    /// metadata. A NEWER interval-0 row (unknowable delta) must be skipped — the fact still comes
    /// from the older usable row — and a counter with ONLY interval-0 rows emits no fact at all,
    /// never a fact of 0.
    /// </summary>
    [Fact]
    public async Task EndToEnd_PerfmonFacts_DivideDeltaByMeasuredInterval_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live perfmon fact test.");

        var ct = TestContext.Current.CancellationToken;
        const int perfmonServerId = TestServerId - 2; // own id — this test cleans its own rows

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using (var cleanup = new NpgsqlCommand(
            $"DELETE FROM perfmon_stats WHERE server_id = {perfmonServerId};", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var collector = new PgFactCollector(postgres);

        var bodySucceeded = false;
        try
        {
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var windowStart = windowEnd.AddHours(-1);

            async Task PlantAsync(long id, DateTime time, string counter, long delta, int intervalSeconds)
            {
                using var plant = new NpgsqlCommand(@"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name,
     object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
                plant.Parameters.AddWithValue(id);
                plant.Parameters.AddWithValue(time);
                plant.Parameters.AddWithValue(perfmonServerId);
                plant.Parameters.AddWithValue("perfmon-per-second-e2e");
                plant.Parameters.AddWithValue("SQLServer:SQL Statistics");
                plant.Parameters.AddWithValue(counter);
                plant.Parameters.AddWithValue("");
                plant.Parameters.AddWithValue(delta * 2);
                plant.Parameters.AddWithValue(delta);
                plant.Parameters.AddWithValue(intervalSeconds);
                await plant.ExecuteNonQueryAsync(ct);
            }

            /* Batch requests: an older USABLE row (delta 6000 / 60s = 100/sec), then a NEWER
               interval-0 row that must not become the fact. */
            await PlantAsync(1, windowStart.AddMinutes(20), "Batch Requests/sec", 6000, 60);
            await PlantAsync(2, windowStart.AddMinutes(25), "Batch Requests/sec", 0, 0);

            /* Compilations: one usable row, 300 / 60s = 5/sec. */
            await PlantAsync(3, windowStart.AddMinutes(20), "SQL Compilations/sec", 300, 60);

            /* Re-compilations: ONLY an interval-0 row — no rate is knowable, so no fact. */
            await PlantAsync(4, windowStart.AddMinutes(20), "SQL Re-Compilations/sec", 0, 0);

            var context = new AnalysisContext
            {
                ServerId = perfmonServerId,
                ServerName = "perfmon-per-second-e2e",
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };

            var facts = await collector.CollectFactsAsync(context);

            var batch = Assert.Single(facts, f => f.Key == "PERFMON_BATCH_REQ_SEC");
            Assert.Equal(100.0, batch.Value, precision: 10);
            Assert.Equal(6000, batch.Metadata["delta_cntr_value"]);
            Assert.Equal(60, batch.Metadata["sample_interval_seconds"]);

            var compilations = Assert.Single(facts, f => f.Key == "PERFMON_COMPILATIONS_SEC");
            Assert.Equal(5.0, compilations.Value, precision: 10);

            Assert.DoesNotContain(facts, f => f.Key == "PERFMON_RECOMPILATIONS_SEC");
            Assert.Equal(2, facts.Count);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var command = new NpgsqlCommand(
                    $"DELETE FROM perfmon_stats WHERE server_id = {perfmonServerId};", cleanup);
                await command.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }

    /* ---------------- #3653: RUNNING_JOBS names the job ---------------- */

    /// <summary>
    /// #3653 (A9): the RUNNING_JOBS aggregate names ONE job — the one furthest past its own history among
    /// the rows that were RUNNING LONG — and the emission carries it on <c>Fact.ObjectName</c>, never in the
    /// doubles-only metadata. Pinned on the text because each clause is a behaviour a simplification would
    /// remove: drop the FILTER and the longest merely-running job is named; put duration before percent and
    /// a slow-but-normal job outranks a fast one at 4× its average; drop the job_name tail and two equal rows
    /// pick nondeterministically, rewriting the frozen finding text between passes over the same window.
    /// Both SKUs: Lite's inline query carries the clause verbatim and maps the same ordinal to the same
    /// slot, so a change to one side fails here before the parity review has to find it.
    /// </summary>
    [Fact]
    public void RunningJobsSql_NamesTheJobFurthestPastItsOwnHistory_AmongLongRowsOnly_OnObjectName_BothSkus()
    {
        var sql = PgFactCollector.RunningJobsSql;

        const string clause =
            "(ARRAY_AGG(job_name ORDER BY percent_of_average DESC NULLS LAST, current_duration_seconds DESC, job_name)";
        Assert.Contains(clause, sql, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE is_running_long))[1] AS worst_long_job_name", sql, StringComparison.Ordinal);

        /* The four pre-existing columns keep their positions (the C# reads ordinals 0–3 as before) and the
           name is the FIFTH — the C# reads ordinal 4. */
        var columns = Regex.Matches(sql, @"\bAS\s+(\w+)", RegexOptions.IgnoreCase).Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(
            new[] { "running_count", "running_long_count", "max_percent_of_avg", "max_duration_seconds", "worst_long_job_name" },
            columns);

        /* Any_value would be the wrong tool here twice over: it is unordered, and the dialect pin above
           confines it to the plan-regression query. */
        Assert.DoesNotContain("any_value", sql, StringComparison.OrdinalIgnoreCase);

        foreach (var relativePath in new[]
        {
            "Darling/PerformanceMonitor.Darling.Analysis/PgFactCollector.Activity.cs",
            "Lite/Analysis/DuckDbFactCollector.Activity.cs"
        })
        {
            var source = File.ReadAllText(Path.Combine(RepoRoot(), relativePath));
            Assert.Contains(clause, source, StringComparison.Ordinal);
            Assert.Contains("FILTER (WHERE is_running_long))[1] AS worst_long_job_name", source, StringComparison.Ordinal);

            /* The ordinal-4 read keeps NULL as null (not ""), and the value lands on ObjectName. */
            Assert.Contains("var worstLongJobName = reader.IsDBNull(4) ? null : reader.GetString(4);", source, StringComparison.Ordinal);
            Assert.Contains("ObjectName = worstLongJobName,", source, StringComparison.Ordinal);

            /* Never into Metadata: the dictionary is Dictionary<string, double>, so a string there would not
               compile — this guards the next-cheapest drift, a numeric "has_name" stand-in that a reader would
               then have to reverse-map. The RUNNING_JOBS metadata block stays the four figures. */
            var emission = source.IndexOf("Key = \"RUNNING_JOBS\",", StringComparison.Ordinal);
            Assert.True(emission >= 0, $"{relativePath}: RUNNING_JOBS emission not found");
            var block = source[emission..source.IndexOf("});", emission, StringComparison.Ordinal)];
            var metadataKeys = Regex.Matches(block, @"\[""(\w+)""\]\s*=").Select(m => m.Groups[1].Value).ToArray();
            Assert.Equal(new[] { "running_count", "running_long_count", "max_percent_of_average", "max_duration_seconds" }, metadataKeys);
        }
    }

    /// <summary>
    /// #3653 live fixture: three running_jobs rows at one tick — two running long (400% at 7,200 s and 250%
    /// at 9,000 s) and one not (the longest runtime and the highest percent of the three, so a dropped
    /// FILTER would name it) — must emit RUNNING_JOBS with the 400% job on ObjectName, the running-long
    /// count as the value, and the pre-#3653 per-row counts and window maxima untouched beside it. A second
    /// tick where nothing runs long must emit the fact with a NULL name, not an empty string and not the
    /// longest job present.
    /// </summary>
    [Fact]
    public async Task EndToEnd_RunningJobs_NamesTheWorstLongJob_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live running-jobs fact test.");

        var ct = TestContext.Current.CancellationToken;
        const int jobsServerId = TestServerId - 3; // own id — this test cleans its own rows
        const int quietServerId = TestServerId - 4;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using (var cleanup = new NpgsqlCommand(
            $"DELETE FROM running_jobs WHERE server_id IN ({jobsServerId}, {quietServerId});", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var collector = new PgFactCollector(postgres);

        var bodySucceeded = false;
        try
        {
            var windowEnd = TruncateToSeconds(DateTime.UtcNow);
            var windowStart = windowEnd.AddHours(-1);
            var tick = windowStart.AddMinutes(30);

            async Task PlantAsync(int serverId, string jobName, long currentSeconds, bool isLong, decimal? percentOfAverage)
            {
                /* avg/p95 derived so is_running_long agrees with the collector's own definition (current > p95
                   when long); percent_of_average is stored verbatim because the ORDER BY reads the column. */
                using var plant = new NpgsqlCommand(@"
INSERT INTO running_jobs
    (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time,
     current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count,
     is_running_long, percent_of_average)
VALUES ($1, $2, $3, $4, $5, true, $6, $7, $8, $9, 100, $10, $11)", connection);
                plant.Parameters.AddWithValue(tick);
                plant.Parameters.AddWithValue(serverId);
                plant.Parameters.AddWithValue("running-jobs-name-e2e");
                plant.Parameters.AddWithValue(jobName);
                plant.Parameters.AddWithValue(Guid.NewGuid().ToString());
                plant.Parameters.AddWithValue(tick.AddSeconds(-currentSeconds));
                plant.Parameters.AddWithValue(currentSeconds);
                plant.Parameters.AddWithValue(isLong ? currentSeconds / 3 : currentSeconds);
                plant.Parameters.AddWithValue(isLong ? currentSeconds / 2 : currentSeconds * 2);
                plant.Parameters.AddWithValue(isLong);
                plant.Parameters.AddWithValue((object?)percentOfAverage ?? DBNull.Value);
                await plant.ExecuteNonQueryAsync(ct);
            }

            await PlantAsync(jobsServerId, "Weekly CHECKDB", 9_000, isLong: true, percentOfAverage: 250.0m);
            await PlantAsync(jobsServerId, "Nightly Index Maintenance", 7_200, isLong: true, percentOfAverage: 400.0m);
            await PlantAsync(jobsServerId, "Long Steady ETL", 99_999, isLong: false, percentOfAverage: 900.0m);

            await PlantAsync(quietServerId, "Log Backup", 300, isLong: false, percentOfAverage: 100.0m);
            await PlantAsync(quietServerId, "Long Steady ETL", 99_999, isLong: false, percentOfAverage: null);

            AnalysisContext Context(int serverId) => new()
            {
                ServerId = serverId,
                ServerName = "running-jobs-name-e2e",
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero
            };

            var jobs = Assert.Single(await collector.CollectFactsAsync(Context(jobsServerId)), f => f.Key == "RUNNING_JOBS");
            Assert.Equal("Nightly Index Maintenance", jobs.ObjectName);
            Assert.Equal(2, jobs.Value);
            Assert.Equal(3, jobs.Metadata["running_count"]);
            Assert.Equal(2, jobs.Metadata["running_long_count"]);
            Assert.Equal(900, jobs.Metadata["max_percent_of_average"]);
            Assert.Equal(99_999, jobs.Metadata["max_duration_seconds"]);

            var quiet = Assert.Single(await collector.CollectFactsAsync(Context(quietServerId)), f => f.Key == "RUNNING_JOBS");
            Assert.Null(quiet.ObjectName);
            Assert.Equal(0, quiet.Value);
            Assert.Equal(2, quiet.Metadata["running_count"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using var command = new NpgsqlCommand(
                    $"DELETE FROM running_jobs WHERE server_id IN ({jobsServerId}, {quietServerId});", cleanup);
                await command.ExecuteNonQueryAsync(cleanupCt);
            });
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
    /// #2827: every ordered aggregate in the interval-grain dedup must carry the SAME
    /// <c>ORDER BY</c>, or the "keep the latest row" contract silently breaks per column.
    ///
    /// <para>The dedup used to be <c>ROW_NUMBER() OVER (...) ... WHERE rn = 1</c>, which takes one row
    /// and therefore cannot mix. Expressing it as <c>GROUP BY</c> + <c>array_agg(col ORDER BY ...)[1]</c>
    /// is measurably faster — it lets the planner hash-aggregate at the interval grain instead of forcing
    /// one global sort of the server's whole Query Store slice on all eight keys (23.4s to 9.6s in
    /// isolation on the busiest use1 server; byte-identical results across six servers) — but it buys that
    /// with a NEW failure mode the window function did not have: each aggregate sorts independently, so a
    /// single drifted <c>ORDER BY</c> would take <c>avg_cpu_time_us</c> from one collection and
    /// <c>execution_count</c> from another and blend two different observations into one row. That is a
    /// silent wrong ANSWER, not a slowdown, and nothing downstream could detect it.</para>
    ///
    /// <para>Asserted as a property over EVERY ordered aggregate in the CTE rather than by naming the
    /// seven columns: a pin that enumerates what exists today passes unchanged when an eighth column is
    /// added with the wrong ordering, which is exactly how a broken sibling survived a green suite in
    /// #2344 and again in PgStatementText.</para>
    /// </summary>
    [Fact]
    public void PlanRegressionDedup_OrdersEveryAggregateIdentically()
    {
        var sql = PgFactCollector.PlanRegressionSql;

        /* The dedup CTE runs from "WITH deduped AS" to the CTE that consumes it. */
        var start = sql.IndexOf("WITH deduped AS", StringComparison.Ordinal);
        var end = sql.IndexOf("plan_agg AS", StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the deduped CTE should still open the query");

        /* Comments stripped first. The CTE's own prose names the forms it rejects, and a pin that scans
           raw text would match those and "pass" on the strength of a comment — the same trap as a coverage
           ratchet counting a table named in prose as read. Measure the SQL, not what it says about itself. */
        var dedup = Regex.Replace(sql[start..end], @"--[^\n]*", string.Empty);

        var orderings = Regex
            .Matches(dedup, @"array_agg\([^)]*?\s+ORDER\s+BY\s+(?<ord>[^)]+)\)")
            .Select(m => m.Groups["ord"].Value.Trim())
            .ToList();

        /* If this is zero the dedup has been rewritten again and this pin is no longer measuring it. */
        Assert.True(orderings.Count > 0, "expected ordered aggregates in the dedup CTE");
        Assert.Single(orderings.Distinct(StringComparer.Ordinal));

        /* The tie-break itself is the #1850/#1845 contract: latest collection wins, and where two
           collections share a timestamp the one that saw more executions wins. Both halves matter. */
        Assert.Equal("collection_time DESC, execution_count DESC", orderings[0]);

        /* replica_role stays in the grouping key. Dropping it does not de-duplicate — it DISCARDS one
           replica's row, an under-count that is strictly worse than the double-count this CTE exists to
           fix, because a double-count is visible in the number and a dropped row is silent (#1850). */
        Assert.Contains(
            "GROUP BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time",
            dedup,
            StringComparison.Ordinal);

        /* Do not go back to either measured-slower form. DISTINCT ON needs the same global sort the
           window function did (49.8s against the window function's 23.4s), and raising work_mem is not a
           substitute either — 512MB took the whole query from 25.6s to 59.3s. */
        Assert.DoesNotContain("DISTINCT ON", dedup, StringComparison.OrdinalIgnoreCase);
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
