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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #8084: the high-impact aggregate ships the scorer only the hashes it can keep (the top N on each of its six
/// measures), reads the base table rather than the payload-resolving view, and runs its window functions over grouped
/// rows rather than over every raw row in the window. The scored result is the one scoring every hash gives.
/// #8117: the text and plan lookup that follows picks each kept hash's busiest row on the base table and resolves the
/// payload dimension once per pick, rather than joining every row of the hash to the dimension before the pick.
/// </summary>
public sealed class FinOpsHighImpactBoundedReadLiveTests
{
    private const string ServerName = "darling-finops-high-impact-bounded";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>The scorer's per-measure keep count for these facts: small, so the window holds many more hashes than six times it.</summary>
    private const int TopN = 3;

    /// <summary>Hashes seeded with distinct figures. Two more (<see cref="TiedHashA"/>, <see cref="TiedHashB"/>) share every figure.</summary>
    private const int HashCount = 60;
    private const string TiedHashA = "0xBTIE1";
    private const string TiedHashB = "0xBTIE2";

    /// <summary>
    /// The window's per-hash aggregate as the seed wrote it, before any scoring: the rows the scorer saw when the
    /// statement shipped every hash. Hash i runs in Db(i % 4); every tenth hash also ran, later and for less CPU, in
    /// Db9, so its figures sum across two databases, its database is the one with the most CPU and its memory is the
    /// newer row's grant (#7166, #7695). Ordered as the statement orders: CPU descending, then hash.
    /// </summary>
    private static List<HighImpactQuery> ExpectedAggregate()
    {
        var rows = new List<HighImpactQuery>();
        for (var i = 1; i <= HashCount; i++)
        {
            var f = Figures(i);
            var row = new HighImpactQuery
            {
                QueryHash = Hash(i),
                DatabaseName = "Db" + (i % 4),
                DatabaseCount = 1,
                TotalExecutions = f.Exec,
                TotalCpuMs = f.CpuUs / 1000.0m,
                TotalDurationMs = f.ElapsedUs / 1000.0m,
                TotalReads = f.Reads,
                TotalWrites = f.Writes,
                PeakMemoryMb = f.GrantKb / 1024.0m,
            };
            if (i % 10 == 0)
            {
                var s = SecondRow(i);
                row.DatabaseCount = 2;
                row.TotalExecutions += s.Exec;
                row.TotalCpuMs += s.CpuUs / 1000.0m;
                row.TotalDurationMs += s.ElapsedUs / 1000.0m;
                row.TotalReads += s.Reads;
                row.TotalWrites += s.Writes;
                row.PeakMemoryMb = s.GrantKb / 1024.0m;
            }
            rows.Add(row);
        }

        var tie = TieFigures();
        foreach (var hash in new[] { TiedHashA, TiedHashB })
        {
            rows.Add(new HighImpactQuery
            {
                QueryHash = hash, DatabaseName = "Db1", DatabaseCount = 1, TotalExecutions = tie.Exec, TotalCpuMs = tie.CpuUs / 1000.0m,
                TotalDurationMs = tie.ElapsedUs / 1000.0m, TotalReads = tie.Reads, TotalWrites = tie.Writes, PeakMemoryMb = tie.GrantKb / 1024.0m,
            });
        }

        return rows.OrderByDescending(r => r.TotalCpuMs).ThenBy(r => r.QueryHash, StringComparer.Ordinal).ToList();
    }

    private static string Hash(int i) => "0xB" + i.ToString("D3");

    /// <summary>
    /// Six measures that each put a different hash on top. A third of the hashes wrote nothing, four share the largest
    /// write count (one more than the keep count, so the cut falls inside a tie), and two share the largest execution
    /// count.
    /// </summary>
    private static (long Exec, long CpuUs, long ElapsedUs, long Reads, long Writes, long GrantKb) Figures(int i) =>
        (Exec: (i * 17) % 47 + 1,
         CpuUs: ((i * 37) % 61 + 1) * 100_000L,
         ElapsedUs: ((i * 53) % 59 + 1) * 150_000L,
         Reads: ((i * 29) % 67 + 1) * 1_000L,
         Writes: i is 5 or 25 or 35 or 55 ? 3_600L : Math.Max(0, (i * 41) % 71 - 30) * 50L,
         GrantKb: ((i * 23) % 53 + 1) * 2_048L);

    /// <summary>The two tied hashes' figures: the largest grant of all, so both are kept and only the hash orders them.</summary>
    private static (long Exec, long CpuUs, long ElapsedUs, long Reads, long Writes, long GrantKb) TieFigures()
    {
        var tie = Figures(HashCount + 1);
        tie.GrantKb = 120_000L;
        return tie;
    }

    /// <summary>The later, smaller row every tenth hash also has in Db9; its grant is the one the hash reports.</summary>
    private static (long Exec, long CpuUs, long ElapsedUs, long Reads, long Writes, long GrantKb) SecondRow(int i) =>
        (Exec: 2, CpuUs: 50_000L, ElapsedUs: 75_000L, Reads: 500L, Writes: 25L, GrantKb: (i % 7 + 1) * 4_096L);

    private static async Task<ScratchPostgres> SeedAsync(string cs, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-2);
        for (var i = 1; i <= HashCount; i++)
        {
            var f = Figures(i);
            await Plant(connection, ct, at, Hash(i), "Db" + (i % 4), f.Exec, f.CpuUs, f.ElapsedUs, f.Reads, f.Writes, f.GrantKb);
            if (i % 10 == 0)
            {
                var s = SecondRow(i);
                await Plant(connection, ct, at.AddMinutes(30), Hash(i), "Db9", s.Exec, s.CpuUs, s.ElapsedUs, s.Reads, s.Writes, s.GrantKb);
            }
        }

        var tie = TieFigures();
        await Plant(connection, ct, at, TiedHashA, "Db1", tie.Exec, tie.CpuUs, tie.ElapsedUs, tie.Reads, tie.Writes, tie.GrantKb);
        await Plant(connection, ct, at, TiedHashB, "Db1", tie.Exec, tie.CpuUs, tie.ElapsedUs, tie.Reads, tie.Writes, tie.GrantKb);

        /* Outside the window: never aggregated, never a candidate. */
        await Plant(connection, ct, at.AddDays(-3), "0xBOLD", "Db0", 99, 900_000_000L, 900_000_000L, 9_000_000L, 9_000L, 99_999L);
        return scratch;
    }

    /// <summary>
    /// The read's result over a window of many hashes is the scorer's result over every hash in the window: the same
    /// rows, in the same order, with the same shares and scores, including the tie that only the hash orders and the
    /// hash whose figures sum across two databases.
    /// </summary>
    [Fact]
    public async Task ReadAsync_ScoresAsIfEveryHashHadBeenShipped()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live high-impact bounded-read test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(cs!, ct);

        var expected = HighImpactScorer.Score(ExpectedAggregate(), TopN);
        Assert.True(expected.Count > TopN && expected.Count <= 6 * TopN, $"the seed keeps {expected.Count} of {HashCount + 2} hashes; it must keep more than {TopN} and at most {6 * TopN}");
        Assert.Contains(expected, r => r.DatabaseCount == 2);
        Assert.Contains(expected, r => r.TotalWrites == 0);
        Assert.Contains(expected, r => r.QueryHash == TiedHashA);
        Assert.Contains(expected, r => r.QueryHash == TiedHashB);
        /* Four hashes tie on the largest write count; the keep count is three, so the one with the least CPU is cut. */
        Assert.DoesNotContain(expected, r => r.QueryHash == Hash(5));

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var actual = await DarlingFinOpsHighImpactReader.ReadAsync(dataSource, ServerId, 24, 60, ct, TopN);

        Assert.Equal(expected.Select(r => r.QueryHash).ToList(), actual.Select(r => r.QueryHash).ToList());
        for (var i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var a = actual[i];
            Assert.Equal(e.DatabaseName, a.DatabaseName);
            Assert.Equal(e.DatabaseCount, a.DatabaseCount);
            Assert.Equal(e.TotalExecutions, a.TotalExecutions);
            Assert.Equal(e.TotalCpuMs, a.TotalCpuMs);
            Assert.Equal(e.TotalDurationMs, a.TotalDurationMs);
            Assert.Equal(e.TotalReads, a.TotalReads);
            Assert.Equal(e.TotalWrites, a.TotalWrites);
            Assert.Equal(e.PeakMemoryMb, a.PeakMemoryMb);
            Assert.Equal(e.CpuShare, a.CpuShare);
            Assert.Equal(e.DurationShare, a.DurationShare);
            Assert.Equal(e.ReadsShare, a.ReadsShare);
            Assert.Equal(e.WritesShare, a.WritesShare);
            Assert.Equal(e.MemoryShare, a.MemoryShare);
            Assert.Equal(e.ExecutionsShare, a.ExecutionsShare);
            Assert.Equal(e.ImpactScore, a.ImpactScore);
            Assert.Equal("SELECT " + e.QueryHash + ";", a.SampleQueryText);
        }
    }

    /// <summary>
    /// The statement ships the scorer at most six times its keep count, however many hashes the window holds, and what
    /// it ships scores to the same result as every hash would. The candidate read is a public static method on the
    /// reader, <c>ReadCandidatesAsync(NpgsqlDataSource, int serverId, DateTime cutoffUtc, int topN, int commandTimeoutSeconds, CancellationToken)</c>,
    /// reached by name so the fact states what is missing when it is.
    /// </summary>
    [Fact]
    public async Task Statement_ShipsAtMostSixTimesTopN_AndTheyScoreTheSame()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live high-impact bounded-read test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(cs!, ct);

        var method = typeof(DarlingFinOpsHighImpactReader).GetMethod("ReadCandidatesAsync", BindingFlags.Public | BindingFlags.Static);
        Assert.True(method is not null,
            "DarlingFinOpsHighImpactReader.ReadCandidatesAsync is missing: the aggregate statement has no per-measure cap, so every query_hash in the window is shipped to the scorer.");

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var cutoff = DateTime.UtcNow.AddHours(-24);
        var candidates = await (Task<List<HighImpactQuery>>)method!.Invoke(null, [dataSource, ServerId, cutoff, TopN, 60, ct])!;

        Assert.True(candidates.Count <= 6 * TopN, $"{candidates.Count} candidates shipped for topN {TopN}; at most {6 * TopN} can be kept");
        Assert.True(candidates.Count < HashCount, $"{candidates.Count} candidates shipped from a window of {HashCount + 2} hashes");

        var expected = HighImpactScorer.Score(ExpectedAggregate(), TopN);
        var scored = HighImpactScorer.Score(candidates, TopN);
        Assert.Equal(expected.Select(r => r.QueryHash).ToList(), scored.Select(r => r.QueryHash).ToList());
        Assert.Equal(expected.Select(r => r.ImpactScore).ToList(), scored.Select(r => r.ImpactScore).ToList());
        Assert.Equal(expected.Select(r => r.CpuShare).ToList(), scored.Select(r => r.CpuShare).ToList());
        Assert.Equal(expected.Select(r => r.MemoryShare).ToList(), scored.Select(r => r.MemoryShare).ToList());
    }

    /// <summary>
    /// Plan facts for the aggregate statement, from its generic plan over a migrated store: no scan reads a payload
    /// dimension table (the aggregate needs no text or plan, so it must not go through the resolving view), and every
    /// window function runs over already-grouped rows (an Aggregate sits below each WindowAgg), never over the raw rows
    /// of the window. The statement is prepared and executed with a typed NULL per parameter the server reports for
    /// it, so the fact holds whatever the statement's parameter list is.
    /// </summary>
    [Fact]
    public async Task AggregatePlan_ReadsNoPayloadDimension_AndWindowsOverGroupedRowsOnly()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live high-impact plan test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await DarlingMcpTestData.ExecAsync(connection, ct, "PREPARE high_impact AS " + DarlingFinOpsHighImpactReader.HighImpactQueriesSql);
        using var types = new NpgsqlCommand("SELECT parameter_types::text[] FROM pg_prepared_statements WHERE name = 'high_impact'", connection);
        var parameterTypes = (string[])(await types.ExecuteScalarAsync(ct))!;
        await DarlingMcpTestData.ExecAsync(connection, ct, "SET plan_cache_mode = force_generic_plan");
        using var explain = new NpgsqlCommand(
            "EXPLAIN (FORMAT JSON) EXECUTE high_impact(" + string.Join(", ", parameterTypes.Select(t => "NULL::" + t)) + ")", connection);
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        var nodes = Flatten(document.RootElement[0].GetProperty("Plan")).ToList();

        var relations = nodes
            .Where(n => n.TryGetProperty("Relation Name", out _))
            .Select(n => n.GetProperty("Relation Name").GetString())
            .ToList();
        Assert.NotEmpty(relations);
        Assert.DoesNotContain(PayloadDimensions.QueryTextDimTable, relations);
        Assert.DoesNotContain(PayloadDimensions.QueryPlanDimTable, relations);

        var windows = nodes.Where(n => n.GetProperty("Node Type").GetString() == "WindowAgg").ToList();
        Assert.NotEmpty(windows);
        foreach (var window in windows)
        {
            Assert.True(Flatten(window).Skip(1).Any(n => n.GetProperty("Node Type").GetString() == "Aggregate"),
                "a WindowAgg runs over the window's raw rows: no Aggregate sits below it");
        }
    }

    /* ── #8117: the text and plan lookup for the kept hashes ── */

    private const string DimensionHash = "0xPDIM";
    private const string InlineHash = "0xPINL";
    private const string SplitHash = "0xPSPLIT";
    private const string PlanlessHash = "0xPNONE";

    private const string DimensionBusiestText = "SELECT 'busiest' FROM dbo.PDim;";
    private const string DimensionQuietText = "SELECT 'quiet' FROM dbo.PDim;";
    private const string DimensionBusiestPlan = "<ShowPlanXML plan=\"PDim busiest\" />";
    private const string DimensionQuietPlan = "<ShowPlanXML plan=\"PDim quiet\" />";
    private const string InlineBusiestText = "SELECT 'inline busiest' FROM dbo.PInl;";
    private const string InlineQuietText = "SELECT 'inline quiet' FROM dbo.PInl;";
    private const string InlineBusiestPlan = "<ShowPlanXML plan=\"PInl busiest\" />";
    private const string SplitText = "SELECT 'split' FROM dbo.PSplit;";
    private const string SplitQuietPlan = "<ShowPlanXML plan=\"PSplit quiet, the only plan\" />";
    private const string PlanlessText = "SELECT 'no plan' FROM dbo.PNone;";

    /// <summary>
    /// Four hashes whose rows carry their payload in every shape the store holds. <see cref="DimensionHash"/>: rows
    /// written since #1767, inline columns NULL, text and plan (gzip bytes, #2069) in the dimension tables by digest;
    /// the busiest row's text and plan differ from the others'. <see cref="InlineHash"/>: rows written before #1767,
    /// text and plan inline. <see cref="SplitHash"/>: the busiest row has text and no plan; a quieter row has the only
    /// plan. <see cref="PlanlessHash"/>: text and no plan on any row. The figures put each hash on top of one measure,
    /// so a keep count of one keeps all four.
    /// </summary>
    private static async Task<ScratchPostgres> SeedPayloadShapesAsync(string cs, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-2);

        foreach (var text in new[] { DimensionBusiestText, DimensionQuietText, SplitText, PlanlessText })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO query_text_dim (digest, query_text, last_seen) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                PayloadDimensions.Digest(text), text, at);
        }
        foreach (var plan in new[] { DimensionBusiestPlan, DimensionQuietPlan, SplitQuietPlan })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO query_plan_dim (digest, query_plan_xml, query_plan_gz, last_seen) VALUES ($1, NULL, $2, $3) ON CONFLICT DO NOTHING",
                PayloadDimensions.Digest(plan), PayloadDimensions.CompressContent(plan), at);
        }

        /* Top CPU. Three rows; the middle one is the busiest and carries the other text and plan. */
        await PlantPayload(connection, ct, at, DimensionHash, exec: 5, cpuUs: 900_000L, reads: 10L, writes: 0L,
            textDigest: PayloadDimensions.Digest(DimensionQuietText), planDigest: PayloadDimensions.Digest(DimensionQuietPlan));
        await PlantPayload(connection, ct, at.AddMinutes(1), DimensionHash, exec: 50, cpuUs: 900_000L, reads: 10L, writes: 0L,
            textDigest: PayloadDimensions.Digest(DimensionBusiestText), planDigest: PayloadDimensions.Digest(DimensionBusiestPlan));
        await PlantPayload(connection, ct, at.AddMinutes(2), DimensionHash, exec: 20, cpuUs: 900_000L, reads: 10L, writes: 0L,
            textDigest: PayloadDimensions.Digest(DimensionQuietText), planDigest: PayloadDimensions.Digest(DimensionQuietPlan));

        /* Top reads. Two rows written before the dimensions existed. */
        await PlantPayload(connection, ct, at, InlineHash, exec: 3, cpuUs: 1_000L, reads: 5_000_000L, writes: 0L,
            inlineText: InlineQuietText, inlinePlan: null);
        await PlantPayload(connection, ct, at.AddMinutes(1), InlineHash, exec: 30, cpuUs: 1_000L, reads: 5_000_000L, writes: 0L,
            inlineText: InlineBusiestText, inlinePlan: InlineBusiestPlan);

        /* Top writes. The busiest row has no plan in either shape; the quieter row has the only one. */
        await PlantPayload(connection, ct, at, SplitHash, exec: 90, cpuUs: 1_000L, reads: 10L, writes: 7_000L,
            textDigest: PayloadDimensions.Digest(SplitText), planDigest: null);
        await PlantPayload(connection, ct, at.AddMinutes(1), SplitHash, exec: 10, cpuUs: 1_000L, reads: 10L, writes: 7_000L,
            textDigest: PayloadDimensions.Digest(SplitText), planDigest: PayloadDimensions.Digest(SplitQuietPlan));

        /* Top executions. No plan anywhere. */
        await PlantPayload(connection, ct, at, PlanlessHash, exec: 400, cpuUs: 1_000L, reads: 10L, writes: 0L,
            textDigest: PayloadDimensions.Digest(PlanlessText), planDigest: null);
        await PlantPayload(connection, ct, at.AddMinutes(1), PlanlessHash, exec: 300, cpuUs: 1_000L, reads: 10L, writes: 0L,
            textDigest: PayloadDimensions.Digest(PlanlessText), planDigest: null);

        return scratch;
    }

    /// <summary>
    /// Each kept hash's text and plan come from its busiest row in the window, per form, in whichever shape that row
    /// holds them: the dimension row its digest names (gzip bytes for the plan), or the inline column of a row written
    /// before the dimensions existed. A hash whose busiest row has no plan takes the plan of the busiest row that has
    /// one; a hash with no plan on any row has none.
    /// </summary>
    [Fact]
    public async Task ReadAsync_ResolvesEachKeptHashesTextAndPlanFromItsBusiestRow()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live high-impact lookup test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedPayloadShapesAsync(cs!, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var actual = await DarlingFinOpsHighImpactReader.ReadAsync(dataSource, ServerId, 24, 60, ct, topN: 1);
        var byHash = actual.ToDictionary(r => r.QueryHash, StringComparer.Ordinal);
        Assert.Equal(new[] { DimensionHash, InlineHash, SplitHash, PlanlessHash }.OrderBy(h => h, StringComparer.Ordinal), byHash.Keys.OrderBy(h => h, StringComparer.Ordinal));

        Assert.Equal(DimensionBusiestText, byHash[DimensionHash].FullQueryText);
        Assert.Equal(DimensionBusiestText, byHash[DimensionHash].SampleQueryText);
        Assert.Equal(DimensionBusiestPlan, byHash[DimensionHash].QueryPlanXml);

        Assert.Equal(InlineBusiestText, byHash[InlineHash].FullQueryText);
        Assert.Equal(InlineBusiestPlan, byHash[InlineHash].QueryPlanXml);

        Assert.Equal(SplitText, byHash[SplitHash].FullQueryText);
        Assert.Equal(SplitQuietPlan, byHash[SplitHash].QueryPlanXml);

        Assert.Equal(PlanlessText, byHash[PlanlessHash].FullQueryText);
        Assert.Null(byHash[PlanlessHash].QueryPlanXml);
    }

    /// <summary>
    /// Plan facts for the lookup statement, from its generic plan over a migrated store: every per-hash pick (a Limit
    /// node) runs over rows of the base table alone, with no join below it, so a payload dimension is read after the
    /// pick, once per kept hash, rather than joined to every row of the hash before it. The statement is prepared and
    /// executed with a typed NULL per parameter the server reports for it, so the fact holds whatever the statement's
    /// parameter list is.
    /// </summary>
    [Fact]
    public async Task LookupPlan_PicksEachHashesRowOnTheBaseTable_WithNoJoinBelowThePick()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live high-impact lookup plan test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await DarlingMcpTestData.ExecAsync(connection, ct, "PREPARE high_impact_lookup AS " + DarlingFinOpsHighImpactReader.HighImpactSampleLookupSql);
        using var types = new NpgsqlCommand("SELECT parameter_types::text[] FROM pg_prepared_statements WHERE name = 'high_impact_lookup'", connection);
        var parameterTypes = (string[])(await types.ExecuteScalarAsync(ct))!;
        await DarlingMcpTestData.ExecAsync(connection, ct, "SET plan_cache_mode = force_generic_plan");
        using var explain = new NpgsqlCommand(
            "EXPLAIN (FORMAT JSON) EXECUTE high_impact_lookup(" + string.Join(", ", parameterTypes.Select(t => "NULL::" + t)) + ")", connection);
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        var nodes = Flatten(document.RootElement[0].GetProperty("Plan")).ToList();

        var relations = nodes
            .Where(n => n.TryGetProperty("Relation Name", out _))
            .Select(n => n.GetProperty("Relation Name").GetString())
            .ToList();
        Assert.Contains(QueryStatsCollector.Instance.TargetTable, relations);
        Assert.Contains(PayloadDimensions.QueryTextDimTable, relations);
        Assert.Contains(PayloadDimensions.QueryPlanDimTable, relations);

        var picks = nodes.Where(n => n.GetProperty("Node Type").GetString() == "Limit").ToList();
        Assert.NotEmpty(picks);
        foreach (var pick in picks)
        {
            var below = Flatten(pick).Skip(1).ToList();
            var joins = below.Where(n => n.GetProperty("Node Type").GetString() is "Hash Join" or "Merge Join" or "Nested Loop").ToList();
            Assert.True(joins.Count == 0,
                "a per-hash pick joins every row of the hash to a payload dimension before keeping one: " +
                string.Join(", ", joins.Select(j => j.GetProperty("Node Type").GetString())));
            var scanned = below.Where(n => n.TryGetProperty("Relation Name", out _)).Select(n => n.GetProperty("Relation Name").GetString()).Distinct().ToList();
            Assert.Equal(new[] { QueryStatsCollector.Instance.TargetTable }, scanned);
        }
    }

    private static async Task PlantPayload(NpgsqlConnection c, CancellationToken ct, DateTime at, string hash,
        long exec, long cpuUs, long reads, long writes,
        byte[]? textDigest = null, byte[]? planDigest = null, string? inlineText = null, string? inlinePlan = null)
    {
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                query_hash, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                delta_logical_writes, max_grant_kb, sample_interval_seconds, last_execution_time,
                query_text, query_text_digest, query_plan_xml, query_plan_digest)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, "Db0", hash, exec, cpuUs, cpuUs * 2, reads, writes, 1_024L, 60, at,
            inlineText, textDigest, inlinePlan, planDigest);
    }

    private static IEnumerable<JsonElement> Flatten(JsonElement node)
    {
        yield return node;
        if (!node.TryGetProperty("Plans", out var children)) yield break;
        foreach (var child in children.EnumerateArray())
        {
            foreach (var descendant in Flatten(child)) yield return descendant;
        }
    }

    private static async Task Plant(NpgsqlConnection c, CancellationToken ct, DateTime at, string hash, string db,
        long exec, long cpuUs, long elapsedUs, long reads, long writes, long grantKb)
    {
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                query_hash, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                delta_logical_writes, max_grant_kb, sample_interval_seconds, last_execution_time, query_text)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, db, hash, exec, cpuUs, elapsedUs, reads, writes, grantKb, 60, at, "SELECT " + hash + ";");
    }
}
