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
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// #6808: the seed, the expected answer and the EXPLAIN runner behind <see cref="QueryStoreClutterChurnPlanLiveTests"/>.
/// No test framework in here, so the same file also runs from a console harness on a machine that cannot host the
/// Windows-only test project. The expected rows come from a plain C# model of the seed, not from SQL.
/// </summary>
internal static partial class QueryStoreClutterChurnProbe
{
    internal const int ServerId = 6808;
    internal const string ServerName = "churn-6808";
    internal const string Big = "db_big";
    internal const string Small = "db_small";
    internal const int Collections = 40;
    internal static readonly TimeSpan Spacing = TimeSpan.FromMinutes(15);

    /// <summary>The work_mem the statement runs under. Low enough that sorting every raw row spills, high enough that one hash entry per plan does not.</summary>
    internal const string WorkMem = "1MB";

    internal sealed record Expected(
        string Database, DateTime FirstCollection, DateTime LastCollection, int CollectionsObserved, int DistinctQueries,
        int DistinctPlans, int P95, int Max, int SeenOnce, int FirstSeenAfterFirst);

    /// <summary>One raw row: (database, query, plan, collection index).</summary>
    private readonly record struct Raw(string Database, long Query, long Plan, int Index);

    /// <summary>The seed. db_big: 4000 plans, three to a query. Every fourth plan is seen in one collection only, the rest in 25 consecutive ones.
    /// db_small: 6 plans of one query, collection indexes 3 to 5, two of them seen once.</summary>
    private static List<Raw> Model()
    {
        var rows = new List<Raw>();
        for (var p = 0; p < 4000; p++)
        {
            var first = p % 16;
            var length = p % 4 == 0 ? 1 : 25;
            for (var i = 0; i < length; i++)
            {
                rows.Add(new Raw(Big, 1000 + (p / 3), 50000 + p, first + i));
            }
        }

        for (var p = 0; p < 6; p++)
        {
            var first = 3 + (p % 3);
            var length = p == 5 ? 1 : 2;
            for (var i = 0; i < length && first + i <= 5; i++)
            {
                rows.Add(new Raw(Small, 7, 90 + p, first + i));
            }
        }

        return rows;
    }

    /// <summary>The newest collection is 2 hours before <paramref name="anchor"/>, 40 collections 15 minutes apart.</summary>
    internal static DateTime CollectionTime(DateTime anchor, int index) => anchor.AddHours(-2).Subtract(Spacing * (Collections - 1 - index));

    internal static int RowCount => Model().Count;

    /// <summary>What the statement must return for the window and the database choice, by the model.</summary>
    internal static List<Expected> ExpectedFor(DateTime anchor, DateTime start, DateTime end, string[]? databases)
    {
        var inWindow = Model()
            .Where(r => CollectionTime(anchor, r.Index) >= start && CollectionTime(anchor, r.Index) <= end)
            .Where(r => databases is null || databases.Contains(r.Database))
            .ToList();
        var result = new List<Expected>();
        foreach (var db in inWindow.GroupBy(r => r.Database).OrderBy(g => g.Key, StringComparer.Ordinal))
        {
            var firstCollection = db.Min(r => r.Index);
            var perPlan = db.GroupBy(r => r.Plan).Select(g => (Query: g.First().Query, Min: g.Min(r => r.Index), Max: g.Max(r => r.Index))).ToList();
            var plansPerQuery = perPlan.GroupBy(x => x.Query).Select(g => g.Count()).Order().ToList();
            /* percentile_disc(0.95): the first value whose cumulative share reaches 0.95 */
            var p95 = plansPerQuery[(int)Math.Ceiling(0.95 * plansPerQuery.Count) - 1];
            result.Add(new Expected(
                db.Key,
                CollectionTime(anchor, firstCollection),
                CollectionTime(anchor, db.Max(r => r.Index)),
                db.Select(r => r.Index).Distinct().Count(),
                plansPerQuery.Count,
                perPlan.Count,
                p95,
                plansPerQuery.Max(),
                perPlan.Count(x => x.Min == x.Max),
                perPlan.Count(x => x.Min > firstCollection)));
        }

        return result;
    }

    internal static async Task SeedAsync(NpgsqlConnection connection, DateTime anchor, CancellationToken ct)
    {
        await using (var server = new NpgsqlCommand(
            @"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
              VALUES ($1, $2, $2, TRUE, 15, $3, $3) ON CONFLICT (server_id) DO NOTHING", connection))
        {
            server.Parameters.AddWithValue(ServerId);
            server.Parameters.AddWithValue(ServerName);
            server.Parameters.AddWithValue(DateTime.SpecifyKind(anchor, DateTimeKind.Unspecified));
            await server.ExecuteNonQueryAsync(ct);
        }

        var rows = Model();
        var times = rows.Select(r => DateTime.SpecifyKind(CollectionTime(anchor, r.Index), DateTimeKind.Unspecified)).ToArray();
        await using (var insert = new NpgsqlCommand(
            @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
              SELECT 900000000 + t.n, t.at, $1, $2, t.db, t.q, t.p, '0xQ' || t.q, '0xP' || t.p, 'SELECT 1', 10, 1000, 800, t.at
              FROM unnest($3::timestamp[], $4::text[], $5::bigint[], $6::bigint[]) WITH ORDINALITY AS t(at, db, q, p, n)", connection))
        {
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(ServerName);
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = times });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = rows.Select(r => r.Database).ToArray() });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = rows.Select(r => r.Query).ToArray() });
            insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = rows.Select(r => r.Plan).ToArray() });
            await insert.ExecuteNonQueryAsync(ct);
        }

        /* VACUUM sets the visibility map, which production's autovacuum keeps set and an index-only walk needs to skip the heap. */
        await using var analyze = new NpgsqlCommand("VACUUM (ANALYZE) query_store_stats", connection);
        await analyze.ExecuteNonQueryAsync(ct);
    }

    internal sealed record Run(List<DarlingQueryStoreClutterReader.PlanChurnRow> Rows, string PlanText);

    /// <summary>
    /// Runs the shipped statement under <see cref="WorkMem"/> (SET LOCAL, so it ends with the transaction): once for its rows, once
    /// under EXPLAIN (ANALYZE) for the plan it actually ran. <paramref name="planSettings"/> are more SET LOCAL settings (for example
    /// <c>enable_seqscan = off</c>) applied to both.
    /// </summary>
    internal static async Task<Run> RunAsync(NpgsqlConnection connection, DateTime start, DateTime end, string[]? databases, CancellationToken ct, params string[] planSettings)
    {
        await using var tx = await connection.BeginTransactionAsync(ct);
        await using (var set = new NpgsqlCommand("SET LOCAL work_mem = '" + WorkMem + "'", connection, tx))
        {
            await set.ExecuteNonQueryAsync(ct);
        }

        foreach (var planSetting in planSettings)
        {
            await using var force = new NpgsqlCommand("SET LOCAL " + planSetting, connection, tx);
            await force.ExecuteNonQueryAsync(ct);
        }

        NpgsqlCommand Build(string sql)
        {
            var command = new NpgsqlCommand(sql, connection, tx);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = new[] { ServerId } });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
            command.Parameters.Add((databases is null ? DatabaseFilter.All : DatabaseFilter.Of(databases)).Parameter());
            return command;
        }

        var rows = new List<DarlingQueryStoreClutterReader.PlanChurnRow>();
        await using (var command = Build(DarlingQueryStoreClutterReader.PlanChurnSql))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                rows.Add(new DarlingQueryStoreClutterReader.PlanChurnRow(
                    reader.GetInt32(0), reader.GetString(1), reader.GetDateTime(2), reader.GetDateTime(3), reader.GetInt32(4),
                    reader.GetInt32(5), reader.GetInt32(6), reader.GetInt32(7), reader.GetInt32(8), reader.GetInt32(9), reader.GetInt32(10)));
            }
        }

        var plan = new StringBuilder();
        await using (var explain = Build("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) " + DarlingQueryStoreClutterReader.PlanChurnSql))
        await using (var reader = await explain.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                plan.AppendLine(reader.GetString(0));
            }
        }

        await tx.RollbackAsync(ct);
        return new Run(rows, plan.ToString());
    }

    /// <summary>The index <c>PlanChurnSql</c>'s projection is cut to: <c>PgTableTuning</c> creates it, <c>MigrateAsync</c> does not.</summary>
    internal const string CoveringIndex = "idx_query_store_stats_server_db_query_plan_time";

    /// <summary>
    /// The plan lines that scan a chunk of <c>query_store_stats</c> (or the table itself when it is not a hypertable), found by what
    /// they do: a scan node whose relation is such a chunk. A bitmap heap scan counts, its bitmap index scan names no relation and does not.
    /// </summary>
    internal static List<string> StatsTableScans(string planText) =>
        planText.Split('\n')
            .Select(line => line.Trim())
            .Where(t => t.Contains("Scan", StringComparison.Ordinal)
                && !t.Contains("Custom Scan", StringComparison.Ordinal)
                && StatsRelation().IsMatch(t))
            .ToList();

    [GeneratedRegex(@"\bon\s+(\w+\.)?(_hyper_\d+_\d+_chunk|query_store_stats)\b")]
    private static partial Regex StatsRelation();

    /// <summary>
    /// True when EVERY scan of the stats table's chunks is an index-only scan of <see cref="CoveringIndex"/> (a chunk index is named
    /// <c>&lt;chunk&gt;_&lt;index&gt;</c> cut to 63 characters, so the match is on the first 40 characters of the index name). The
    /// statement reads its <c>NOT MATERIALIZED</c> CTE twice, so at least two scans are expected, and no scan at all fails (#6816).
    /// </summary>
    internal static bool WalksCoveringIndexOnly(string planText)
    {
        var scans = StatsTableScans(planText);
        return scans.Count >= 2
            && scans.All(t => t.Contains("Index Only Scan", StringComparison.Ordinal) && t.Contains(CoveringIndex[..40], StringComparison.Ordinal));
    }

    [GeneratedRegex(@"\bBatches:\s+(\d+)")]
    private static partial Regex HashBatches();

    /// <summary>Why a plan spilled to disk, or null when it did not: an external sort, a hash aggregate that wrote batches, a materialized CTE that went to temp files, or a hash join split into more than one batch (#6816).</summary>
    internal static string? SpillIn(string planText)
    {
        foreach (var line in planText.Split('\n'))
        {
            var t = line.Trim();
            if (t.Contains("Sort Method: external", StringComparison.Ordinal) || t.Contains("Disk Usage:", StringComparison.Ordinal) || t.Contains("Storage: Disk", StringComparison.Ordinal))
            {
                return t;
            }

            var batches = HashBatches().Match(t);
            if (batches.Success && int.Parse(batches.Groups[1].Value, CultureInfo.InvariantCulture) > 1)
            {
                return t;
            }
        }

        return null;
    }

    /// <summary>The first difference between the rows the statement returned and the expected ones, or null when they agree.</summary>
    internal static string? FirstDifference(List<DarlingQueryStoreClutterReader.PlanChurnRow> rows, List<Expected> expected)
    {
        if (rows.Count != expected.Count)
        {
            return string.Create(CultureInfo.InvariantCulture, $"row count {rows.Count}, expected {expected.Count}");
        }

        for (var i = 0; i < rows.Count; i++)
        {
            var r = rows[i];
            var e = expected[i];
            var got = FormattableString.Invariant($"{r.ServerId}|{r.DatabaseName}|{r.FirstCollection:o}|{r.LastCollection:o}|{r.CollectionsObserved}|{r.DistinctQueries}|{r.DistinctPlans}|{r.PlansPerQueryP95}|{r.PlansPerQueryMax}|{r.PlansSeenOnce}|{r.PlansFirstSeenAfterFirstCollection}");
            var want = FormattableString.Invariant($"{ServerId}|{e.Database}|{e.FirstCollection:o}|{e.LastCollection:o}|{e.CollectionsObserved}|{e.DistinctQueries}|{e.DistinctPlans}|{e.P95}|{e.Max}|{e.SeenOnce}|{e.FirstSeenAfterFirst}");
            if (got != want)
            {
                return $"got      {got}\nexpected {want}";
            }
        }

        return null;
    }
}
