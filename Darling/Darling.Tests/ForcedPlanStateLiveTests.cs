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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5632 step 1b: the state of each Query Store force over seeded <c>collect.query_store_forced_plans</c> and <c>query_store_stats</c> rows.
/// Every time is a fixed instant (<see cref="T0"/>), so no clock decides a state and the cases hold at any hour. One scratch store holds every
/// case; each case is its own database, so the cases cannot see one another.
/// </summary>
[Collection("live-postgres")]
public sealed class ForcedPlanStateLiveTests
{
    private const string ServerName = "darling-forced-plans";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly DateTime T0 = new(2026, 3, 10, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime AsOf = T0.AddHours(3);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task Exec(NpgsqlConnection c, CancellationToken ct, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(c, ct, sql, values.Select(v => v is DateTime d ? DateTime.SpecifyKind(d, DateTimeKind.Unspecified) : v).ToArray());

    /// <summary>One collector pass of one database: the marker row, then one row per forced plan.</summary>
    private static async Task CaptureAsync(NpgsqlConnection c, CancellationToken ct, string db, DateTime at, params (long Plan, long Query, long Failures, string? Reason)[] forced)
    {
        const string sql = @"INSERT INTO collect.query_store_forced_plans
(collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, query_plan_hash, plan_forcing_type, force_failure_count, last_force_failure_reason)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)";
        await Exec(c, ct, sql, CollectionIdGenerator.Next(), at, ServerId, ServerName, db, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);
        foreach (var f in forced)
            await Exec(c, ct, sql, CollectionIdGenerator.Next(), at, ServerId, ServerName, db, f.Plan, f.Query, "0x" + f.Plan.ToString("X", CultureInfo.InvariantCulture), "MANUAL", f.Failures, (object?)f.Reason ?? DBNull.Value);
    }

    /// <summary>One Query Store interval of one plan: it ran <paramref name="executions"/> times and last ran at <paramref name="lastRun"/>.</summary>
    private static Task RanAsync(NpgsqlConnection c, CancellationToken ct, string db, long query, long plan, DateTime lastRun, long executions, bool forcedWhenSampled = false) =>
        Exec(c, ct, @"INSERT INTO query_store_stats
(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, runtime_stats_interval_id,
 interval_start_time_utc, first_execution_time, last_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, avg_rowcount, is_forced_plan, force_failure_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,'Regular',$8,$9,$9,$10,$11,5000,7000,1,$12,0)",
            CollectionIdGenerator.Next(), lastRun.AddMinutes(1), ServerId, ServerName, db, query, plan, plan * 1000 + lastRun.Minute, lastRun.AddMinutes(-30), lastRun, executions, forcedWhenSampled);

    private sealed record Rig(NpgsqlDataSource Postgres, NpgsqlConnection Connection);

    private static async Task<T> WithStoreAsync<T>(Func<Rig, CancellationToken, Task<T>> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live forced-plan state tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        return await body(new Rig(postgres, connection), ct);
    }

    private static async Task<Dictionary<(string Db, long Plan), ForcedPlanFact>> ReadAsync(
        Rig rig, CancellationToken ct, DatabaseFilter? databases = null, IReadOnlyDictionary<(string, long), DateTime>? forcedAt = null)
    {
        var read = await DarlingForcedPlanStateReader.ReadAsync(rig.Postgres, ServerId, databases ?? DatabaseFilter.All, null, T0.AddHours(-1), AsOf, forcedAt, 30, cancellationToken: ct);
        return read.Plans.ToDictionary(p => (p.DatabaseName, p.PlanId));
    }

    /// <summary>The seed every state case reads: database <c>StateDb</c> carries one plan per state, <c>OlderDb</c> a capture that ended earlier.</summary>
    private static async Task SeedStatesAsync(NpgsqlConnection c, CancellationToken ct)
    {
        const string db = "StateDb";
        foreach (var minutes in new[] { 0, 30, 60, 90 })
        {
            var at = T0.AddMinutes(minutes);
            var forced = new List<(long, long, long, string?)>
            {
                (11, 1, 0, null),     /* WORKING: it ran, no other plan did after the force */
                (21, 2, 0, null),     /* NOT_EXERCISED: nothing of the query ran */
                (31, 3, 0, null),     /* IGNORED: another plan ran after the force */
                (41, 4, 2, "GENERAL_FAILURE"), /* FAILING: the engine counts failures, and the plan also ran */
            };
            if (minutes < 60) forced.Add((51, 5, 0, null)); /* REMOVED: forced at T0 and T0+30, gone from the later captures */
            await CaptureAsync(c, ct, db, at, forced.ToArray());
        }

        /* WORKING: plan 12 of the same query ran at T0-30m, BEFORE the force was first seen, so it does not make the force ignored. */
        await RanAsync(c, ct, db, 1, 12, T0.AddMinutes(-30), 99);
        await RanAsync(c, ct, db, 1, 11, T0.AddMinutes(20), 10);
        await RanAsync(c, ct, db, 3, 31, T0.AddMinutes(20), 4);
        await RanAsync(c, ct, db, 3, 32, T0.AddMinutes(40), 7);
        /* #6221: plan 32 is a LATER plan of the query, so it is another plan by its stored XML (an ordinary plan: no UsePlan). */
        await StoredPlanSeed.AddAsync(c, ct, ServerId, db, 32, StoredPlanSeed.Plain);
        await RanAsync(c, ct, db, 4, 41, T0.AddMinutes(20), 3);
        await RanAsync(c, ct, db, 5, 51, T0.AddMinutes(10), 3, forcedWhenSampled: true);

        /* OlderDb's newest capture is at T0+30m, 60 minutes before StateDb's. Plan 61 is in it, so it is still forced; plan 62 was only in the first. */
        await CaptureAsync(c, ct, "OlderDb", T0, (61, 6, 0, null), (62, 7, 0, null));
        await CaptureAsync(c, ct, "OlderDb", T0.AddMinutes(30), (61, 6, 0, null));
    }

    /// <summary>The read's row cap cuts the last rows of its order: a failing force first-kept, then the rest of each database's newest capture, then removed
    /// ones. Before, the order was database and plan id, so a standing failing force in a later database could be cut with nothing said.</summary>
    [Fact]
    public async Task WhenTheReadHitsItsCap_FailingForcesAreKept_AndTheReadSaysItWasCapped()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            await SeedStatesAsync(rig.Connection, ct);

            var capped = await DarlingForcedPlanStateReader.ReadAsync(rig.Postgres, ServerId, DatabaseFilter.All, null, T0.AddHours(-1), AsOf, 3, null, 30, null, ct);
            Assert.True(capped.ReadCapped);
            Assert.Equal(3, capped.Plans.Count);
            var kept = capped.Plans.Select(p => (p.DatabaseName, p.PlanId)).ToHashSet();
            Assert.Contains(("StateDb", 41L), kept);
            Assert.DoesNotContain(("StateDb", 51L), kept);
            Assert.DoesNotContain(("OlderDb", 62L), kept);

            var whole = await DarlingForcedPlanStateReader.ReadAsync(rig.Postgres, ServerId, DatabaseFilter.All, null, T0.AddHours(-1), AsOf, 7, null, 30, null, ct);
            Assert.False(whole.ReadCapped);
            Assert.Equal(7, whole.Plans.Count);

            /* captures[].forced_plan_count counts the whole capture, so a cut read does not under-report a database (as hint_count does). */
            var wholeCounts = whole.Captures.ToDictionary(c => c.DatabaseName, c => c.ForcedPlanCount);
            Assert.Equal(wholeCounts, capped.Captures.ToDictionary(c => c.DatabaseName, c => c.ForcedPlanCount));
            Assert.True(capped.Captures.Sum(c => c.ForcedPlanCount) > capped.Plans.Count(p => p.State != ForcedPlanState.Removed));
            return 0;
        });
    }

    /// <summary>
    /// #6833: the forced-plan list over the field shape (many forces captured every cycle). The order is failing forces first, then the other
    /// forces of the newest capture, then the removed ones, each by plan id, the first seen of a force is the start of its current run of
    /// captures, and a cap cuts the last rows of that order and says so.
    /// </summary>
    [Fact]
    public async Task TheListOverManyForcesCapturedEveryCycle_KeepsItsOrderFirstSeenAndCap()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            const string db = "FieldDb";
            const int captures = 24, forces = 40;
            for (var c = 0; c < captures; c++)
            {
                var forced = new List<(long Plan, long Query, long Failures, string? Reason)>();
                for (long plan = 1; plan <= forces; plan++)
                {
                    if (plan is >= 10 and <= 12 && c is >= 5 and <= 8) continue; /* absent for four captures: first seen is capture 9 */
                    if (plan == 39 && c >= 12) continue; /* in the first twelve captures only: REMOVED */
                    var failing = plan is 7 or 30;
                    forced.Add((plan, plan + 100, failing ? 2 : 0, failing ? "GENERAL_FAILURE" : null));
                }

                await CaptureAsync(rig.Connection, ct, db, T0.AddMinutes(5 * c), forced.ToArray());
            }

            var read = await DarlingForcedPlanStateReader.ReadAsync(rig.Postgres, ServerId, DatabaseFilter.All, null, T0.AddHours(-1), AsOf, 50, null, 30, null, ct);
            Assert.False(read.ReadCapped);
            var expected = new List<long> { 7, 30 };
            expected.AddRange(Enumerable.Range(1, forces).Select(p => (long)p).Where(p => p is not (7 or 30 or 39)));
            expected.Add(39);
            Assert.Equal(expected, read.Plans.Select(p => p.PlanId).ToList());
            var facts = read.Plans.ToDictionary(p => p.PlanId);
            Assert.Equal(T0, facts[1].FirstSeen);
            Assert.Equal(T0.AddMinutes(5 * 9), facts[10].FirstSeen);
            Assert.Equal(T0.AddMinutes(5 * 9), facts[12].FirstSeen);
            Assert.Equal(T0.AddMinutes(5 * (captures - 1)), facts[1].LastSeen);
            Assert.Equal(ForcedPlanState.Failing, facts[7].State);
            Assert.Equal(ForcedPlanState.Removed, facts[39].State);

            /* A cap of 5 keeps the two failing forces and the first three others, and reads one row past it to know it cut. */
            var capped = await DarlingForcedPlanStateReader.ReadAsync(rig.Postgres, ServerId, DatabaseFilter.All, null, T0.AddHours(-1), AsOf, 5, null, 30, null, ct);
            Assert.True(capped.ReadCapped);
            Assert.Equal(new long[] { 7, 30, 1, 2, 3 }, capped.Plans.Select(p => p.PlanId).ToArray());
            return 0;
        });
    }

    [Fact]
    public async Task EachState_IsReadFromItsOwnSeededRows_AndADatabaseReadsItsOwnNewestCapture()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            await SeedStatesAsync(rig.Connection, ct);
            var forces = await ReadAsync(rig, ct);

            Assert.Equal(ForcedPlanState.Working, forces[("StateDb", 11)].State);
            Assert.Equal(10, forces[("StateDb", 11)].ForcedExecutions);
            Assert.Empty(forces[("StateDb", 11)].OtherPlans);
            Assert.Equal(ForcedPlanState.NotExercised, forces[("StateDb", 21)].State);
            Assert.Equal(ForcedPlanState.Ignored, forces[("StateDb", 31)].State);
            Assert.Equal(32, Assert.Single(forces[("StateDb", 31)].OtherPlans).PlanId);
            Assert.Equal(7, forces[("StateDb", 31)].OtherPlans[0].Executions);
            Assert.Equal(ForcedPlanState.Failing, forces[("StateDb", 41)].State);
            Assert.Equal(2, forces[("StateDb", 41)].FailureCount);
            Assert.Equal("GENERAL_FAILURE", forces[("StateDb", 41)].FailureReason);
            Assert.Equal(ForcedPlanState.Removed, forces[("StateDb", 51)].State);

            /* The older database is judged against its own newest capture (T0+30m), not StateDb's (T0+90m). */
            Assert.Equal(ForcedPlanState.NotExercised, forces[("OlderDb", 61)].State);
            Assert.Equal(ForcedPlanState.Removed, forces[("OlderDb", 62)].State);
            return 0;
        });
    }

    [Fact]
    public async Task FirstSeen_IsTheFirstCaptureOfTheCurrentRun_AndAForceSetAgainStartsANewRun()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            /* Plan 71: shown at T0 and T0+5m, absent at T0+10m, shown again from T0+15m. First seen is T0+15m. */
            await CaptureAsync(rig.Connection, ct, "RunDb", T0, (71, 8, 0, null));
            await CaptureAsync(rig.Connection, ct, "RunDb", T0.AddMinutes(5), (71, 8, 0, null));
            await CaptureAsync(rig.Connection, ct, "RunDb", T0.AddMinutes(10));
            await CaptureAsync(rig.Connection, ct, "RunDb", T0.AddMinutes(15), (71, 8, 0, null));
            await CaptureAsync(rig.Connection, ct, "RunDb", T0.AddMinutes(20), (71, 8, 0, null));
            /* It ran at T0+3m (the earlier run) and at T0+30m (after the new run began). */
            await RanAsync(rig.Connection, ct, "RunDb", 8, 71, T0.AddMinutes(3), 5);
            await RanAsync(rig.Connection, ct, "RunDb", 8, 71, T0.AddMinutes(30), 2);

            var fact = (await ReadAsync(rig, ct))[("RunDb", 71)];
            Assert.Equal(T0.AddMinutes(15), fact.FirstSeen);
            Assert.Equal(ForcedPlanState.Working, fact.State);
            Assert.Equal(2, fact.ForcedExecutions);
            return 0;
        });
    }

    [Fact]
    public async Task ForcedAt_ReplacesFirstSeen_InTheIgnoredAndWorkingRules()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            foreach (var minutes in new[] { 0, 30, 60 })
                await CaptureAsync(rig.Connection, ct, "SeamDb", T0.AddMinutes(minutes), (81, 9, 0, null), (91, 10, 0, null));
            /* Query 9: the other plan ran at T0+10m, the forced plan last ran at T0+40m. */
            await RanAsync(rig.Connection, ct, "SeamDb", 9, 82, T0.AddMinutes(10), 4);
            await StoredPlanSeed.AddAsync(rig.Connection, ct, ServerId, "SeamDb", 82, StoredPlanSeed.Plain);
            await RanAsync(rig.Connection, ct, "SeamDb", 9, 81, T0.AddMinutes(40), 4);
            /* Query 10: the forced plan ran at T0+20m only. */
            await RanAsync(rig.Connection, ct, "SeamDb", 10, 91, T0.AddMinutes(20), 4);

            var firstSeen = await ReadAsync(rig, ct);
            Assert.Equal(ForcedPlanState.Ignored, firstSeen[("SeamDb", 81)].State);
            Assert.Equal(ForcedPlanState.Working, firstSeen[("SeamDb", 91)].State);

            var forcedAt = new Dictionary<(string, long), DateTime> { [("SeamDb", 81)] = T0.AddMinutes(20), [("SeamDb", 91)] = T0.AddMinutes(50) };
            var seam = await ReadAsync(rig, ct, null, forcedAt);
            /* The other plan's run (T0+10m) is before the real force, so it no longer counts; the forced plan's run (T0+40m) is after it. */
            Assert.Equal(ForcedPlanState.Working, seam[("SeamDb", 81)].State);
            Assert.Equal(T0.AddMinutes(20), seam[("SeamDb", 81)].Since);
            Assert.Equal(T0, seam[("SeamDb", 81)].FirstSeen);
            /* The forced plan's one run (T0+20m) is before the real force, so nothing ran after it. */
            Assert.Equal(ForcedPlanState.NotExercised, seam[("SeamDb", 91)].State);
            return 0;
        });
    }

    [Fact]
    public async Task TheTool_NamesEachState_AndAnEmptyAnswerSaysWhy()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            var asOf = AsOf.ToString("o", CultureInfo.InvariantCulture);

            /* No capture at all in the window. */
            using (var none = JsonDocument.Parse(await DarlingMcpForcedPlanTools.GetForcedPlans(rig.Postgres, ServerName, 24, asOf, DatabaseFilter.All, ct)))
            {
                Assert.Equal("unavailable", none.RootElement.GetProperty("status").GetString());
                Assert.Contains("No forced-plan capture", none.RootElement.GetProperty("message").GetString());
            }

            /* A capture with no forced plan: the answer lists the captured database and says nothing is forced. */
            await CaptureAsync(rig.Connection, ct, "QuietDb", T0.AddMinutes(5));
            using (var quiet = JsonDocument.Parse(await DarlingMcpForcedPlanTools.GetForcedPlans(rig.Postgres, ServerName, 24, asOf, DatabaseFilter.All, ct)))
            {
                Assert.Equal("empty", quiet.RootElement.GetProperty("status").GetString());
                Assert.Contains("No plan is forced", quiet.RootElement.GetProperty("message").GetString());
                Assert.Equal("QuietDb", quiet.RootElement.GetProperty("captures")[0].GetProperty("database_name").GetString());
            }

            await SeedStatesAsync(rig.Connection, ct);
            using var doc = JsonDocument.Parse(await DarlingMcpForcedPlanTools.GetForcedPlans(rig.Postgres, ServerName, 24, asOf, DatabaseFilter.One("StateDb"), ct));
            var states = doc.RootElement.GetProperty("forced_plans").EnumerateArray()
                .ToDictionary(e => e.GetProperty("plan_id").GetInt64(), e => e.GetProperty("state").GetString());
            Assert.Equal(
                new Dictionary<long, string?> { [11] = "WORKING", [21] = "NOT_EXERCISED", [31] = "IGNORED", [41] = "FAILING", [51] = "REMOVED" },
                states);
            Assert.Equal("FAILING", doc.RootElement.GetProperty("forced_plans")[0].GetProperty("state").GetString());
            Assert.Contains("up to one collector pass earlier", doc.RootElement.GetProperty("note").GetString());
            Assert.Equal(5, doc.RootElement.GetProperty("total_forces").GetInt32());
            return 0;
        });
    }

    [Fact]
    public async Task TheHistoryTool_ListsAForcedPlanWithNoExecutionsInTheWindow_AndARemovedOneWithItsState()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            await SeedStatesAsync(rig.Connection, ct);
            var asOf = AsOf.ToString("o", CultureInfo.InvariantCulture);

            /* Query 2's forced plan 21 never ran: the old read had no row for it and answered "empty". */
            using (var never = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(rig.Postgres, "StateDb", 2, ServerName, 24, asOf, ct)))
            {
                var plan = Assert.Single(never.RootElement.GetProperty("plans").EnumerateArray());
                Assert.Equal(21, plan.GetProperty("plan_id").GetInt64());
                Assert.Equal(0, plan.GetProperty("execution_count").GetInt64());
                Assert.True(plan.GetProperty("is_forced_plan").GetBoolean());
                Assert.Equal("NOT_EXERCISED", plan.GetProperty("forced_state").GetString());
                Assert.Equal("NOT_EXERCISED", Assert.Single(never.RootElement.GetProperty("forced_plans").EnumerateArray()).GetProperty("state").GetString());
            }

            /* Query 3: both its plans ran, and the forced one carries its state. */
            using (var ignored = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(rig.Postgres, "StateDb", 3, ServerName, 24, asOf, ct)))
            {
                var byPlan = ignored.RootElement.GetProperty("plans").EnumerateArray().ToDictionary(e => e.GetProperty("plan_id").GetInt64());
                Assert.Equal("IGNORED", byPlan[31].GetProperty("forced_state").GetString());
                Assert.Equal(JsonValueKind.Null, byPlan[32].GetProperty("forced_state").ValueKind);
            }

            /* Query 5 (#6598): plan 51 was forced when sampled and its force was removed. It says so: forced, state REMOVED, and the force is listed. */
            using (var removed = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(rig.Postgres, "StateDb", 5, ServerName, 24, asOf, ct)))
            {
                var plan = Assert.Single(removed.RootElement.GetProperty("plans").EnumerateArray());
                Assert.True(plan.GetProperty("is_forced_plan").GetBoolean());
                Assert.Equal("REMOVED", plan.GetProperty("forced_state").GetString());
                var force = Assert.Single(removed.RootElement.GetProperty("forced_plans").EnumerateArray());
                Assert.Equal(51, force.GetProperty("plan_id").GetInt64());
                Assert.Equal("REMOVED", force.GetProperty("state").GetString());
                Assert.Equal("ok", removed.RootElement.GetProperty("forced_plans_status").GetString());
                /* The plan ran 3 times, and the force's own runs were not read, so the force does not say it ran zero times. */
                Assert.Equal(3, plan.GetProperty("execution_count").GetInt64());
                Assert.False(force.GetProperty("runs_read").GetBoolean());
                Assert.Equal(JsonValueKind.Null, force.GetProperty("forced_plan").ValueKind);
                Assert.Equal(JsonValueKind.Null, force.GetProperty("other_plans_total").ValueKind);
            }

            return 0;
        });
    }

    [Fact]
    public void Classify_AppliesTheRulesInOrder()
    {
        Assert.Equal(ForcedPlanState.Removed, DarlingForcedPlanStateReader.Classify(false, 3, "X", true, true));
        Assert.Equal(ForcedPlanState.Failing, DarlingForcedPlanStateReader.Classify(true, 1, null, true, true));
        Assert.Equal(ForcedPlanState.Failing, DarlingForcedPlanStateReader.Classify(true, 0, "GENERAL_FAILURE", false, true));
        Assert.Equal(ForcedPlanState.Ignored, DarlingForcedPlanStateReader.Classify(true, 0, null, true, true));
        Assert.Equal(ForcedPlanState.Working, DarlingForcedPlanStateReader.Classify(true, 0, null, false, true));
        Assert.Equal(ForcedPlanState.NotExercised, DarlingForcedPlanStateReader.Classify(true, 0, null, false, false));
    }

    /// <summary>#6160: a variant of the forced plan running is the force running; it neither makes a force IGNORED nor hides a failure.</summary>
    [Fact]
    public void Classify_ACountedVariant_IsTheForceRunning()
    {
        Assert.Equal(ForcedPlanState.Working, DarlingForcedPlanStateReader.Classify(true, 0, null, false, false, variantPlanRan: true));
        Assert.Equal(ForcedPlanState.Ignored, DarlingForcedPlanStateReader.Classify(true, 0, null, true, false, variantPlanRan: true));
        Assert.Equal(ForcedPlanState.Failing, DarlingForcedPlanStateReader.Classify(true, 1, "NO_INDEX", false, false, variantPlanRan: true));
        Assert.Equal(ForcedPlanState.Removed, DarlingForcedPlanStateReader.Classify(false, 0, null, false, false, variantPlanRan: true));
    }

    [Fact]
    public async Task AVariantPlan_OfTheForcedPlan_IsListedApart_AndAFailingForceWithAGuide_StaysFailingAndNamesTheGuide()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            const string db = "VariantDb";
            const string sql = @"INSERT INTO collect.query_store_forced_plans
(collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, plan_forcing_type, force_failure_count, last_force_failure_reason,
 variant_plan_ids, plan_guide_name, plan_guide_enabled, plan_guide_scope)
VALUES ($1,$2,$3,$4,$5,$6,$7,'MANUAL',$8,$9,$10,$11,$12,$13)";
            foreach (var minutes in new[] { 0, 30, 60 })
            {
                var at = T0.AddMinutes(minutes);
                await Exec(rig.Connection, ct, @"INSERT INTO collect.query_store_forced_plans (collection_id, collection_time, server_id, server_name, database_name)
VALUES ($1,$2,$3,$4,$5)", CollectionIdGenerator.Next(), at, ServerId, ServerName, db);
                /* Plan 11 of query 1: the engine runs it as variant 12 (and 13 is a real other plan). Plan 21 of query 2: failing, with a guide. */
                await Exec(rig.Connection, ct, sql, CollectionIdGenerator.Next(), at, ServerId, ServerName, db, 11L, 1L, 0L, DBNull.Value, "12", DBNull.Value, DBNull.Value, DBNull.Value);
                await Exec(rig.Connection, ct, sql, CollectionIdGenerator.Next(), at, ServerId, ServerName, db, 21L, 2L, 3L, "NO_INDEX", DBNull.Value, "pg_obj", false, "OBJECT");
            }

            await RanAsync(rig.Connection, ct, db, 1, 12, T0.AddMinutes(20), 8);
            await RanAsync(rig.Connection, ct, db, 2, 22, T0.AddMinutes(20), 5);
            /* #6221: plan 22 is a later plan of query 2, another plan by its stored XML. Plan 12 is named a variant by the older collector's row (still honored), so it is not looked up. */
            await StoredPlanSeed.AddAsync(rig.Connection, ct, ServerId, db, 22, StoredPlanSeed.Plain);

            var forces = await ReadAsync(rig, ct);
            var working = forces[(db, 11)];
            Assert.Equal(ForcedPlanState.Working, working.State);
            Assert.Empty(working.OtherPlans);
            var variant = Assert.Single(working.VariantPlans);
            Assert.Equal(12, variant.PlanId);
            Assert.Equal(8, variant.Executions);
            Assert.Null(working.PlanGuide);

            var failing = forces[(db, 21)];
            Assert.Equal(ForcedPlanState.Failing, failing.State);
            Assert.Equal(22, Assert.Single(failing.OtherPlans).PlanId);
            Assert.Equal(new ForcedPlanGuide("pg_obj", false, "OBJECT"), failing.PlanGuide);

            using var doc = JsonDocument.Parse(JsonSerializer.Serialize(DarlingMcpForcedPlanTools.ForceNode(failing)));
            Assert.Equal("FAILING", doc.RootElement.GetProperty("state").GetString());
            Assert.Equal("pg_obj", doc.RootElement.GetProperty("plan_guide").GetProperty("name").GetString());
            Assert.False(doc.RootElement.GetProperty("plan_guide").GetProperty("enabled").GetBoolean());

            /* The Query Store history tool lists the same force with its variant and its guide. */
            var asOf = AsOf.ToString("o", CultureInfo.InvariantCulture);
            using (var history = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(rig.Postgres, db, 1, ServerName, 24, asOf, ct)))
            {
                var forced = Assert.Single(history.RootElement.GetProperty("forced_plans").EnumerateArray());
                Assert.Equal("WORKING", forced.GetProperty("state").GetString());
                Assert.Equal(12, Assert.Single(forced.GetProperty("variant_plans").EnumerateArray()).GetProperty("plan_id").GetInt64());
            }

            using (var history = JsonDocument.Parse(await DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(rig.Postgres, db, 2, ServerName, 24, asOf, ct)))
            {
                var forced = Assert.Single(history.RootElement.GetProperty("forced_plans").EnumerateArray());
                Assert.Equal("FAILING", forced.GetProperty("state").GetString());
                Assert.Equal("pg_obj", forced.GetProperty("plan_guide").GetProperty("name").GetString());
            }

            return 0;
        });
    }
}
