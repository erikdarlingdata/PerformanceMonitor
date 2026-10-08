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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5582: the reusable half of the Query Store RankedTimeSeries exactness proof. It runs ONE panel through two compile
/// switches of <see cref="ComposeCompiler.CompileCore"/> (the candidate text and the oracle text) on the same stored rows
/// and says whether the result sets are equal. Part 3 of #5582 (an exact rollup) calls <see cref="CompareAsync"/> with its
/// own candidate delegate and the same oracle, over the same <see cref="SeedAsync"/> rows.
///
/// <para><b>What "equal" means.</b> The same (bucket, group...) keys, and for every key the same value: exactly (bit for bit
/// after the final cast to double) when <c>relativeTolerance</c> is 0, within that relative tolerance otherwise. Every Query
/// Store fact column is <c>bigint</c> (V145), so every partial sum, count, minimum and maximum a candidate keeps is an exact
/// integer or numeric and the tolerance is 0; the parameter exists for a candidate that keeps a float partial.</para>
///
/// <para><b>Ties at the top-N cutoff.</b> Neither text orders the rank beyond <c>value DESC NULLS LAST</c>, so which of
/// several groups tied at the cutoff takes the last slot is the plan's choice. When the two member sets differ the harness
/// reads every group's true total from the ranked (non-time) panel, which neither switch changes, and accepts the difference
/// only if each side is a valid top N under those totals and the groups that differ all sit exactly at the cutoff value. The
/// rows of the groups both sides chose must still be equal. <see cref="Comparison.MemberSetsDiffered"/> reports whether it
/// happened, so a test that expects the texts to agree can assert it.</para>
/// </summary>
internal static class QueryStoreRankedHarness
{
    internal static readonly DateTime WindowStart = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
    internal static readonly DateTime WindowEnd = WindowStart.AddDays(3);

    /// <summary>The registered servers the seed writes, as (id, name).</summary>
    internal static readonly (int Id, string Name)[] Servers =
    {
        (-58201, "rank-srv-1"),
        (-58202, "rank-srv-2"),
        (-58203, "rank-srv-3"),
    };

    /// <summary>Compiles a plan; the shape of <see cref="ComposeCompiler.CompileCore"/> with its switch bound.</summary>
    internal delegate (ComposeCompiled? Compiled, string? Error) Compiler(PanelPlan plan, ComposeRunContext context);

    /// <summary>The compiler as the product runs it.</summary>
    internal static readonly Compiler Product = (plan, context) => ComposeCompiler.CompileCore(plan, context, singleScanRankedTimeSeries: true);

    /// <summary>The compiler for the text every route emitted before #5582 (two scans of the fact rows).</summary>
    internal static readonly Compiler Oracle = (plan, context) => ComposeCompiler.CompileCore(plan, context, singleScanRankedTimeSeries: false);

    /// <summary>The outcome of one comparison.</summary>
    internal sealed record Comparison(
        bool Equal, int CandidateRows, int OracleRows, bool MemberSetsDiffered, string? Difference, string CandidateSql, string OracleSql);

    /// <summary>The wide-route context for a window: Query Store reads the wide table, no rollups.</summary>
    internal static ComposeRunContext WideContext(DateTime start, DateTime end, IReadOnlyList<string>? servers = null) =>
        new(servers, start, end, ComposeRunContext.NoVariables, RollupAvailability.None, end, RollupCoverage.Unknown, QueryStoreWideEligible: true);

    internal static PanelPlan Parse(string planJson)
    {
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(planJson)!, Array.Empty<string>());
        Assert.True(error is null, $"{planJson}: {error}");
        return plan!;
    }

    /// <summary>Runs <paramref name="planJson"/> through both compilers over the same context and compares the rows.</summary>
    internal static async Task<Comparison> CompareAsync(
        NpgsqlConnection connection, string planJson, ComposeRunContext context, Compiler candidate, Compiler oracle,
        double relativeTolerance, CancellationToken ct)
    {
        var plan = Parse(planJson);
        var (candidateSql, candidateRows) = await RunAsync(connection, plan, context, candidate, ct);
        var (oracleSql, oracleRows) = await RunAsync(connection, plan, context, oracle, ct);
        var width = plan.GroupBy.Count + 2;
        var candidateByKey = ByKey(candidateRows, width, out var candidateDuplicate);
        var oracleByKey = ByKey(oracleRows, width, out var oracleDuplicate);

        Comparison Make(bool equal, bool differed, string? difference) =>
            new(equal, candidateRows.Count, oracleRows.Count, differed, difference, candidateSql, oracleSql);

        if (candidateDuplicate is not null || oracleDuplicate is not null)
        {
            return Make(false, false, $"a result set repeats the key {candidateDuplicate ?? oracleDuplicate}");
        }

        var sameKeys = candidateByKey.Keys.ToHashSet(StringComparer.Ordinal).SetEquals(oracleByKey.Keys);
        if (sameKeys)
        {
            var wrong = candidateByKey.FirstOrDefault(kv => !ValuesEqual(kv.Value, oracleByKey[kv.Key], relativeTolerance));
            return wrong.Key is null
                ? Make(true, false, null)
                : Make(false, false, $"key {wrong.Key}: candidate {Show(wrong.Value)}, oracle {Show(oracleByKey[wrong.Key])}");
        }

        /* The key sets differ. For a panel with no top-N rank that is a plain mismatch; for a ranked one it may be a tie at
           the cutoff, which the totals decide. */
        if (plan.TopN is not > 0 || plan.Mode != PanelMode.RankedTimeSeries)
        {
            return Make(false, false, "the key sets differ and the panel has no rank to explain it");
        }

        var totals = await TotalsAsync(connection, planJson, context, oracle, ct);
        var candidateMembers = GroupsOf(candidateByKey);
        var oracleMembers = GroupsOf(oracleByKey);
        var verdict = TiesExplain(totals, plan.TopN, candidateMembers, oracleMembers);
        if (verdict is not null)
        {
            return Make(false, true, verdict);
        }

        var common = candidateMembers.Intersect(oracleMembers, StringComparer.Ordinal).ToHashSet(StringComparer.Ordinal);
        foreach (var (key, value) in candidateByKey.Where(kv => common.Contains(GroupOf(kv.Key))))
        {
            if (!oracleByKey.TryGetValue(key, out var other) || !ValuesEqual(value, other, relativeTolerance))
            {
                return Make(false, true, $"a group both sides chose differs at key {key}: candidate {Show(value)}, oracle {Show(other)}");
            }
        }

        return Make(true, true, null);
    }

    private static string? TiesExplain(Dictionary<string, double?> totals, int topN, HashSet<string> candidate, HashSet<string> oracle)
    {
        /* Best first: larger totals first, NULL last, as the rank orders them. */
        var ordered = totals.OrderByDescending(kv => kv.Value.HasValue).ThenByDescending(kv => kv.Value ?? 0d).ToList();
        var take = Math.Min(topN, ordered.Count);
        if (take == 0)
        {
            return "no totals";
        }

        var cutoff = ordered[take - 1].Value;
        bool Better(double? a, double? b) => a.HasValue && (!b.HasValue || a.Value > b.Value);
        var mustHave = ordered.Where(kv => Better(kv.Value, cutoff)).Select(kv => kv.Key).ToHashSet(StringComparer.Ordinal);
        foreach (var (name, members) in new[] { ("candidate", candidate), ("oracle", oracle) })
        {
            if (members.Count != take)
            {
                return $"{name} chose {members.Count} groups, expected {take}";
            }

            if (!mustHave.IsSubsetOf(members))
            {
                return $"{name} left out a group that beats the cutoff: {string.Join(", ", mustHave.Except(members))}";
            }

            var below = members.FirstOrDefault(m => totals.TryGetValue(m, out var t) && Better(cutoff, t));
            if (below is not null)
            {
                return $"{name} chose a group below the cutoff: {below}";
            }
        }

        return null;
    }

    private static async Task<Dictionary<string, double?>> TotalsAsync(
        NpgsqlConnection connection, string planJson, ComposeRunContext context, Compiler compiler, CancellationToken ct)
    {
        var json = (JsonObject)JsonNode.Parse(planJson)!;
        json.Remove("timeBucket");
        json.Remove("includeOther");
        json["topN"] = ComposeLimits.MaxTopN;
        json["viz"] = "table";
        var plan = Parse(json.ToJsonString());
        var (_, rows) = await RunAsync(connection, plan, context, compiler, ct);
        var totals = new Dictionary<string, double?>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            totals[string.Join("|", row.Take(row.Length - 1).Select(Cell))] = row[^1] as double?;
        }

        return totals;
    }

    private static async Task<(string Sql, List<object?[]> Rows)> RunAsync(
        NpgsqlConnection connection, PanelPlan plan, ComposeRunContext context, Compiler compiler, CancellationToken ct)
    {
        var (compiled, error) = compiler(plan, context);
        Assert.True(error is null, error);
        Assert.NotNull(compiled);
        var rows = new List<object?[]>();
        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new object?[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(values);
        }

        return (compiled.Sql, rows);
    }

    private static string Cell(object? value) => value is null ? "<null>" : Convert.ToString(value, CultureInfo.InvariantCulture)!;

    private static Dictionary<string, double?> ByKey(List<object?[]> rows, int width, out string? duplicate)
    {
        duplicate = null;
        var byKey = new Dictionary<string, double?>(StringComparer.Ordinal);
        foreach (var row in rows)
        {
            Assert.Equal(width, row.Length);
            var key = string.Join("|", row.Take(width - 1).Select(Cell));
            if (!byKey.TryAdd(key, row[^1] is null ? null : Convert.ToDouble(row[^1], CultureInfo.InvariantCulture)))
            {
                duplicate = key;
            }
        }

        return byKey;
    }

    /// <summary>The group part of a result key (everything after the bucket); null for a key with no group.</summary>
    private static string GroupOf(string key) => key[(key.IndexOf('|', StringComparison.Ordinal) + 1)..];

    private static HashSet<string> GroupsOf(Dictionary<string, double?> byKey) =>
        byKey.Keys.Select(GroupOf).Where(g => !g.Contains(ComposeCompiler.OtherSeriesLabel, StringComparison.Ordinal)).ToHashSet(StringComparer.Ordinal);

    private static string Show(double? value) => value.HasValue ? value.Value.ToString("R", CultureInfo.InvariantCulture) : "<null>";

    /// <summary>Equal when both are NULL, or both are numbers within <paramref name="relativeTolerance"/> of each other (0: bit for bit).</summary>
    internal static bool ValuesEqual(double? a, double? b, double relativeTolerance)
    {
        if (a is null || b is null)
        {
            return a is null && b is null;
        }

        if (a.Value.Equals(b.Value))
        {
            return true;
        }

        if (relativeTolerance <= 0 || double.IsNaN(a.Value) || double.IsNaN(b.Value))
        {
            return false;
        }

        return Math.Abs(a.Value - b.Value) <= relativeTolerance * Math.Max(Math.Abs(a.Value), Math.Abs(b.Value));
    }

    /// <summary>
    /// Writes the exactness rows into <c>collect.query_store_interval_wide</c> of a freshly migrated database and registers
    /// <see cref="Servers"/>. Deterministic (no random). 6,000 generated rows over three days and three servers, built to hold
    /// every case the proof needs:
    /// <list type="bullet">
    /// <item>hours 3, 9, 15, ... of the window hold no row (empty buckets);</item>
    /// <item><c>module_name</c> and <c>database_name</c> are NULL on some rows (a NULL group key), <c>mod0</c> to <c>mod16</c>
    /// have many rows, <c>only_one</c> has one row, <c>allnull</c> has NULL in every measured column;</item>
    /// <item>execution_count, avg_duration_us and avg_cpu_time_us are NULL on some rows; the duration and CPU columns reach 4e12,
    /// so a per-group sum of their products exceeds a bigint (the numeric path);</item>
    /// <item>the <c>tb_</c> modules: two big ones, six that tie exactly, two small ones, each in one bucket, for the cutoff tie.</item>
    /// </list>
    /// </summary>
    internal static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var (id, name) in Servers)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        }

        await using var generated = new NpgsqlCommand(
            @"
INSERT INTO collect.query_store_interval_wide
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     module_name, query_hash, execution_count, avg_duration_us, max_duration_us, avg_cpu_time_us, max_cpu_time_us,
     query_plan_hash, runtime_stats_interval_id, interval_start_time_utc)
SELECT
    $1::timestamp + make_interval(hours => s.h, mins => (s.i * 7) % 60, secs => (s.i % 13)),
    CASE s.i % 3 WHEN 0 THEN $2 WHEN 1 THEN $3 ELSE $4 END,
    CASE WHEN s.i % 29 = 0 THEN NULL ELSE 'db' || (s.i % 4) END,
    s.i, s.i, 'Regular', $1::timestamp - interval '1 hour', $1::timestamp,
    CASE WHEN s.i = 4242 THEN 'only_one' WHEN s.i % 101 = 7 THEN 'allnull' WHEN s.i % 5 = 0 THEN NULL ELSE 'mod' || (s.i % 17) END,
    '0x' || lpad(to_hex(s.i % 400), 16, '0'),
    CASE WHEN s.i % 11 = 0 OR s.i % 101 = 7 THEN NULL ELSE 1 + (s.i * 31) % 997 END,
    CASE WHEN s.i % 9 = 0 OR s.i % 101 = 7 THEN NULL ELSE (s.i::bigint * 982451653) % 4000000000000 END,
    CASE WHEN s.i % 13 = 0 OR s.i % 101 = 7 THEN NULL ELSE (s.i::bigint * 15485863) % 9000000 END,
    CASE WHEN s.i % 7 = 0 OR s.i % 101 = 7 THEN NULL ELSE (s.i::bigint * 179424673) % 4000000000000 END,
    CASE WHEN s.i % 17 = 0 OR s.i % 101 = 7 THEN NULL ELSE (s.i::bigint * 32452843) % 9000000 END,
    '0x' || lpad(to_hex(s.i), 16, '0'), s.i, $1::timestamp - interval '1 hour'
FROM (SELECT i, (i * 13) % 72 AS h FROM generate_series(1, 6000) AS i) AS s
WHERE s.h % 6 <> 3;", connection);
        generated.Parameters.AddWithValue(DateTime.SpecifyKind(WindowStart, DateTimeKind.Unspecified));
        generated.Parameters.AddWithValue(Servers[0].Id);
        generated.Parameters.AddWithValue(Servers[1].Id);
        generated.Parameters.AddWithValue(Servers[2].Id);
        await generated.ExecuteNonQueryAsync(ct);

        /* The cutoff tie: tb_big1 and tb_big2 beat everything, tb_tie0..5 are identical in every measure, tb_small1/2 are below.
           One row each, in the same hour, so every aggregate of a tied group is the same number. */
        await using var ties = new NpgsqlCommand(
            @"
INSERT INTO collect.query_store_interval_wide
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     module_name, query_hash, execution_count, avg_duration_us, max_duration_us, avg_cpu_time_us, max_cpu_time_us,
     query_plan_hash, runtime_stats_interval_id, interval_start_time_utc)
SELECT $1::timestamp + interval '10 hours 5 minutes', $2, 'dbTie', 900000 + t.n, 900000 + t.n, 'Regular',
       $1::timestamp - interval '1 hour', $1::timestamp, t.module, '0x' || lpad(to_hex(900000 + t.n), 16, '0'),
       t.executions, t.avg_us, t.max_us, t.avg_us, t.max_us, '0x' || lpad(to_hex(900000 + t.n), 16, '0'), 900000 + t.n, $1::timestamp - interval '1 hour'
FROM (VALUES
    (1, 'tb_big1', 500, 900000, 950000), (2, 'tb_big2', 400, 800000, 850000),
    (3, 'tb_tie0', 100, 1000, 2000), (4, 'tb_tie1', 100, 1000, 2000), (5, 'tb_tie2', 100, 1000, 2000),
    (6, 'tb_tie3', 100, 1000, 2000), (7, 'tb_tie4', 100, 1000, 2000), (8, 'tb_tie5', 100, 1000, 2000),
    (9, 'tb_small1', 1, 10, 20), (10, 'tb_small2', 1, 20, 30)
) AS t(n, module, executions, avg_us, max_us);", connection);
        ties.Parameters.AddWithValue(DateTime.SpecifyKind(WindowStart, DateTimeKind.Unspecified));
        ties.Parameters.AddWithValue(Servers[0].Id);
        await ties.ExecuteNonQueryAsync(ct);

        await using var analyze = new NpgsqlCommand("ANALYZE collect.query_store_interval_wide; ANALYZE collect.servers;", connection);
        await analyze.ExecuteNonQueryAsync(ct);
    }
}
