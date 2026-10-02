/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3902: the regressed-queries drill-down follows the PLAN_REGRESSION fact instead of re-running the
/// whole detection behind it.
///
/// <para>Both reads deduplicated the server's entire 14-day Query Store slice on every pass — measured at
/// 20 s and 17 s on a rig seeded to the production funnel — and the drill-down's five rows are the head of
/// the ranking the fact had just computed. The fact now stamps the queries it reported on the pass's
/// <see cref="AnalysisContext"/>, and the drill-down computes its rows for those queries only.</para>
///
/// <para>Live, and driven through the two collectors in the order the analysis service runs them, because
/// what could go wrong is in the wiring: the stamp not reaching the drill-down, the drill-down ignoring it,
/// or the restriction dropping a row the unrestricted read returns. The seed carries more regressions than
/// either read keeps (thirty, against the fact's twenty and the drill-down's five), one of them on two
/// replicas, and a crowd of one-plan queries whose rows only the unrestricted read has to wade through.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PlanRegressionDrillDownReuseLiveTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -390202;
    private const string TestServerName = "RegrReuseSrv";

    /// <summary>Queries with a regression, more than the fact keeps.</summary>
    private const int RegressedQueries = 30;

    /// <summary>One-plan queries: rows in the slice that can never regress.</summary>
    private const int SteadyQueries = 60;

    /// <summary>The fact's own cap (<c>LIMIT 20</c>).</summary>
    private const int FactCap = 20;

    /// <summary>The query that regressed on both replicas of an AG (two fact rows, one offender).</summary>
    private const long TwoReplicaQueryId = RegressedQueries;

    private const int LiveTimeoutSeconds = 60;

    private static DateTime TruncateToSeconds(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    [Fact]
    public async Task TheDrillDown_ReadsTheFactsOffendersOnly_AndReturnsWhatTheWholeSliceReturns_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #3902 drill-down reuse test.");

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

            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                await SeedAsync(connection, periodStart, periodEnd, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);

            /* ── The pass: facts, then the drill-down, on ONE context — DarlingAnalysisService's order. ── */
            var pass = NewContext(periodStart, periodEnd);
            var fact = (await new PgFactCollector(postgres).CollectFactsAsync(pass))
                .Single(f => f.Key == "PLAN_REGRESSION");

            Assert.Equal(FactCap, fact.Metadata["offender_count"]);

            /* The stamp names the fact's twenty rows: nineteen queries on one replica and the two-replica
               query once — two rows, one offender. */
            var expectedOffenders = Enumerable.Range(RegressedQueries - FactCap + 2, FactCap - 1)
                .Select(q => new PlanRegressionOffender(DatabaseFor(q), q))
                .ToHashSet();
            Assert.NotNull(pass.PlanRegressionOffenders);
            Assert.Equal(expectedOffenders, pass.PlanRegressionOffenders!.ToHashSet());
            Assert.Equal(expectedOffenders.Count, pass.PlanRegressionOffenders!.Count);

            var followed = await DrillDownRowsAsync(postgres, pass);

            /* ── The same drill-down with no fact to follow: the whole slice, as it always read. ── */
            var standalone = await DrillDownRowsAsync(postgres, NewContext(periodStart, periodEnd));

            Assert.Equal(5, standalone.Count);
            Assert.Equal(
                standalone.Select(r => r.GetRawText()),
                followed.Select(r => r.GetRawText()));

            /* The head of the ranking is the two-replica query, once per replica — the rows the restriction
               must not collapse. */
            Assert.Equal(
                ["PRIMARY", "SECONDARY"],
                followed.Take(2).Select(r => r.GetProperty("replica_role").GetString()).OrderBy(r => r, StringComparer.Ordinal));
            Assert.All(followed.Take(2), r => Assert.Equal(TwoReplicaQueryId, r.GetProperty("query_id").GetInt64()));

            /* ── The stamp is what the drill-down reads. The agreement above holds whether it follows the fact
                  or ignores it, so name two queries from the MIDDLE of the ranking and the drill-down must
                  return those two and nothing else — a collector that dropped the stamp returns the top five. ── */
            var pinned = NewContext(periodStart, periodEnd);
            pinned.PlanRegressionOffenders = [new(DatabaseFor(27), 27), new(DatabaseFor(28), 28)];
            Assert.Equal([28L, 27L], (await DrillDownRowsAsync(postgres, pinned)).Select(r => r.GetProperty("query_id").GetInt64()));

            /* An empty stamp — a fact that found nothing, so no finding to follow — reads the whole slice. */
            var emptied = NewContext(periodStart, periodEnd);
            emptied.PlanRegressionOffenders = [];
            Assert.Equal(
                standalone.Select(r => r.GetRawText()),
                (await DrillDownRowsAsync(postgres, emptied)).Select(r => r.GetRawText()));

            /* ── The read shape: rows the drill-down takes from query_store_stats, following the fact versus
                  reading the whole slice. The unrestricted count is the positive control — a measurement
                  that could not see the scan would report the same number twice. ── */
            await using (var connection = await OpenWithSearchPathAsync(connectionString!, ct))
            {
                var restricted = await QueryStoreRowsReadAsync(connection, periodStart, periodEnd, pass.PlanRegressionOffenders, ct);
                var whole = await QueryStoreRowsReadAsync(connection, periodStart, periodEnd, offenders: null, ct);

                Assert.Equal(RowsFor(expectedOffenders), restricted);
                Assert.Equal(TotalRows, whole);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteTestRowsAsync);
        }
    }

    private static AnalysisContext NewContext(DateTime periodStart, DateTime periodEnd) => new()
    {
        ServerId = TestServerId,
        ServerName = TestServerName,
        TimeRangeStart = periodStart,
        TimeRangeEnd = periodEnd,
        ServerUtcOffset = TimeSpan.Zero,
    };

    private static async Task<List<JsonElement>> DrillDownRowsAsync(NpgsqlDataSource postgres, AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_REGRESSION",
            StoryPath = "PLAN_REGRESSION",
            /* #3859: the collector matches on PathKeys, not on a split of the rendered path. */
            PathKeys = ["PLAN_REGRESSION"],
            /* Past the display gate — below it the expensive drill-downs are skipped wholesale. */
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("regressed_queries", out var raw))
            return [];

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    /// <summary>
    /// Rows the drill-down's plan takes out of <c>query_store_stats</c> — the table itself or, on a store
    /// where it is a hypertable, its chunks — summed across loops, from the executed plan.
    /// </summary>
    private static async Task<double> QueryStoreRowsReadAsync(
        NpgsqlConnection connection, DateTime periodStart, DateTime periodEnd,
        IReadOnlyList<PlanRegressionOffender>? offenders, CancellationToken ct)
    {
        var relations = new HashSet<string>(StringComparer.Ordinal) { "query_store_stats" };
        await using (var chunks = new NpgsqlCommand(@"
SELECT c.relname
FROM pg_inherits AS i
JOIN pg_class AS c ON c.oid = i.inhrelid
WHERE i.inhparent = 'collect.query_store_stats'::regclass", connection) { CommandTimeout = LiveTimeoutSeconds })
        await using (var reader = await chunks.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                relations.Add(reader.GetString(0));
            }
        }

        await using var cmd = new NpgsqlCommand(
            "EXPLAIN (ANALYZE, FORMAT JSON) " + PgDrillDownCollector.RegressedQueriesSql, connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        };
        cmd.Parameters.AddWithValue(TestServerId);
        cmd.Parameters.AddWithValue(periodStart.AddDays(-14));
        cmd.Parameters.AddWithValue(periodStart);
        cmd.Parameters.AddWithValue(periodEnd);
        cmd.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text,
            Value = offenders is null ? DBNull.Value : offenders.Select(o => o.DatabaseName).ToArray()
        });
        cmd.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            Value = offenders is null ? DBNull.Value : offenders.Select(o => o.QueryId).ToArray()
        });

        var json = (string)(await cmd.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);

        var rows = 0.0;
        Walk(document.RootElement[0].GetProperty("Plan"));
        return rows;

        /* The innermost node that reads the table: a hypertable's ChunkAppend names the table too, and
           counting it as well as the chunk scans under it would count every row twice. */
        void Walk(JsonElement node)
        {
            if (Reads(node) && !Children(node).Any(ReadsBelow))
            {
                rows += node.GetProperty("Actual Rows").GetDouble() * node.GetProperty("Actual Loops").GetDouble();
            }

            foreach (var child in Children(node))
            {
                Walk(child);
            }
        }

        bool Reads(JsonElement node) =>
            node.TryGetProperty("Relation Name", out var relation) && relations.Contains(relation.GetString()!);

        bool ReadsBelow(JsonElement node) => Reads(node) || Children(node).Any(ReadsBelow);

        static IEnumerable<JsonElement> Children(JsonElement node) =>
            node.TryGetProperty("Plans", out var children) ? children.EnumerateArray() : [];
    }

    /* ── The seed ──────────────────────────────────────────────────────────────────────────────────────
       Regressed query q (1..30): a cheap plan that ran six-to-five days back and a costlier plan still
       running at the end of the window, two intervals each, every interval collected TWICE (the cumulative
       re-collection the dedup folds). The costly plan is (2 + q/2)x the cheap one, so the ranking by factor
       is exactly q descending and every cut (the fact's 20, the drill-down's 5) is unambiguous. Query 30
       regresses on both replicas of an AG, its secondary a shade milder (16.9x against 17x) so the two rows
       cannot tie. Steady query s: one plan, the same shape, never a regression. */

    private const int IntervalsPerPlan = 2;
    private const int CollectionsPerInterval = 2;

    private static int RowsPerRegressedQueryReplica => 2 * IntervalsPerPlan * CollectionsPerInterval;

    private static int ReplicasFor(long queryId) => queryId == TwoReplicaQueryId ? 2 : 1;

    private static double RowsFor(IEnumerable<PlanRegressionOffender> offenders) =>
        offenders.Sum(o => RowsPerRegressedQueryReplica * ReplicasFor(o.QueryId));

    private static double TotalRows =>
        Enumerable.Range(1, RegressedQueries).Sum(q => RowsPerRegressedQueryReplica * ReplicasFor(q))
        + (SteadyQueries * IntervalsPerPlan * CollectionsPerInterval);

    private static string DatabaseFor(long queryId) =>
        "RegrReuseDb" + (queryId % 3).ToString(CultureInfo.InvariantCulture);

    private static async Task SeedAsync(
        NpgsqlConnection connection, DateTime periodStart, DateTime periodEnd, CancellationToken ct)
    {
        for (long q = 1; q <= RegressedQueries; q++)
        {
            foreach (var role in q == TwoReplicaQueryId ? new[] { "PRIMARY", "SECONDARY" } : new string?[] { null })
            {
                await SeedPlanAsync(connection, q, planId: (q * 10) + 1, "0xCHEAP" + q, cpuUs: 100_000,
                    firstIntervalId: 1, lastExec: periodStart.AddDays(-5), role, periodEnd, ct);
                var factor = 2 + (q / 2.0) - (role == "SECONDARY" ? 0.1 : 0);
                await SeedPlanAsync(connection, q, planId: (q * 10) + 2, "0xCOSTLY" + q, cpuUs: 100_000 * factor,
                    firstIntervalId: 11, lastExec: periodEnd, role, periodEnd, ct);
            }
        }

        for (long s = 1; s <= SteadyQueries; s++)
        {
            await SeedPlanAsync(connection, 1_000 + s, planId: 50_000 + s, "0xSTEADY" + s, cpuUs: 200_000,
                firstIntervalId: 21, lastExec: periodEnd.AddHours(-1), role: null, periodEnd, ct);
        }
    }

    private static async Task SeedPlanAsync(
        NpgsqlConnection connection, long queryId, long planId, string planHash, double cpuUs,
        long firstIntervalId, DateTime lastExec, string? role, DateTime periodEnd, CancellationToken ct)
    {
        for (var interval = 0; interval < IntervalsPerPlan; interval++)
        {
            var firstExec = lastExec.AddHours(-(IntervalsPerPlan - interval));
            for (var collection = 1; collection <= CollectionsPerInterval; collection++)
            {
                await using var cmd = new NpgsqlCommand(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     query_hash, query_plan_hash, execution_count,
     avg_cpu_time_us, avg_duration_us, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Regular', $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, false, 0)", connection)
                {
                    CommandTimeout = LiveTimeoutSeconds
                };
                cmd.Parameters.AddWithValue(CollectionIdGenerator.Next());
                /* Collected after the window's last execution, the second collection a minute after the first
                   and carrying the interval's final (cumulative) count. */
                cmd.Parameters.AddWithValue(periodEnd.AddMinutes(-10 + collection));
                cmd.Parameters.AddWithValue(TestServerId);
                cmd.Parameters.AddWithValue(TestServerName);
                cmd.Parameters.AddWithValue(DatabaseFor(queryId));
                cmd.Parameters.AddWithValue(queryId);
                cmd.Parameters.AddWithValue(planId);
                cmd.Parameters.AddWithValue(role is null ? DBNull.Value : role);
                cmd.Parameters.AddWithValue(firstIntervalId + interval);
                cmd.Parameters.AddWithValue(firstExec);
                cmd.Parameters.AddWithValue(firstExec);
                cmd.Parameters.AddWithValue(lastExec.AddHours(-(IntervalsPerPlan - 1 - interval)));
                cmd.Parameters.AddWithValue("0xQH" + queryId.ToString(CultureInfo.InvariantCulture));
                cmd.Parameters.AddWithValue(planHash);
                cmd.Parameters.AddWithValue(50L * collection);
                cmd.Parameters.AddWithValue((long)cpuUs);
                cmd.Parameters.AddWithValue((long)cpuUs + 20_000);
                await cmd.ExecuteNonQueryAsync(ct);
            }
        }
    }

    private static async Task<NpgsqlConnection> OpenWithSearchPathAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection);
        await setPath.ExecuteNonQueryAsync(ct);
        return connection;
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("DELETE FROM query_store_stats WHERE server_id = $1", connection)
        {
            CommandTimeout = LiveTimeoutSeconds
        };
        cmd.Parameters.AddWithValue(TestServerId);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
