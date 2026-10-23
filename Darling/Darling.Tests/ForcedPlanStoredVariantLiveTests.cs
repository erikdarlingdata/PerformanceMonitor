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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The plan XML a store-side read finds in <c>collect.query_store_plan_map</c> and <c>query_plan_dim</c>, as the Query Store collector writes it
/// (#6221): the text column for old rows, gzip bytes for new ones, and a map row with a NULL digest for a plan the engine could not persist.
/// </summary>
internal static class StoredPlanSeed
{
    internal const string Variant = "<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementType=\"SELECT\" UsePlan=\"1\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    internal const string Plain = "<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementType=\"SELECT\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    internal static string Guided(string guide) =>
        "<ShowPlanXML><BatchSequence><Batch><Statements><StmtSimple StatementType=\"SELECT\" UsePlan=\"1\" PlanGuideName=\"" + guide + "\" /></Statements></Batch></BatchSequence></ShowPlanXML>";

    /// <summary>Stores <paramref name="xml"/> as the plan of (server, database, plan). Null XML with <paramref name="nullDigest"/> is a map row with no content.</summary>
    internal static async Task AddAsync(NpgsqlConnection c, CancellationToken ct, int serverId, string db, long plan, string? xml, bool gzip = false, bool nullDigest = false)
    {
        var at = DateTime.SpecifyKind(new DateTime(2026, 3, 10, 6, 0, 0), DateTimeKind.Unspecified);
        byte[]? digest = null;
        if (!nullDigest && xml is not null)
        {
            /* One dimension row per plan: the id in a comment keeps two plans with the same text from sharing a row. */
            var stored = xml + "<!-- plan " + plan.ToString(CultureInfo.InvariantCulture) + " -->";
            digest = PayloadDimensions.Digest(stored);
            await using var dim = new NpgsqlCommand(gzip
                ? "INSERT INTO query_plan_dim (digest, query_plan_gz, last_seen) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING"
                : "INSERT INTO query_plan_dim (digest, query_plan_xml, last_seen) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING", c);
            dim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = digest });
            if (gzip) dim.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = PayloadDimensions.CompressContent(stored) });
            else dim.Parameters.AddWithValue(stored);
            dim.Parameters.AddWithValue(NpgsqlDbType.Timestamp, at);
            await dim.ExecuteNonQueryAsync(ct);
        }

        await using var map = new NpgsqlCommand("INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) VALUES ($1,$2,$3,$4,$5,$6)", c);
        map.Parameters.AddWithValue(serverId);
        map.Parameters.AddWithValue(db);
        map.Parameters.AddWithValue(plan);
        map.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bytea, Value = (object?)digest ?? DBNull.Value });
        map.Parameters.AddWithValue("0x6221");
        map.Parameters.AddWithValue(NpgsqlDbType.Timestamp, at);
        await map.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>
/// #6221, the store-side half. The collector reads no plan XML on the monitored server, so a force whose runs moved to a LATER plan id of the
/// same query is judged from the XML the Query Store collector already stored (<c>collect.query_store_plan_map</c>, then <c>query_plan_dim</c>):
/// <c>UsePlan="1"</c> with no <c>PlanGuideName</c> is a variant and counts as the force running; a plan guide name is named; a plan with no stored
/// XML makes the state UNKNOWN_PLAN_NOT_COLLECTED, never IGNORED. Every case is a fixed instant, so no clock decides a state.
/// </summary>
[Collection("live-postgres")]
public sealed class ForcedPlanStoredVariantLiveTests
{
    private const string ServerName = "darling-forced-stored";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly DateTime T0 = new(2026, 3, 10, 6, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime AsOf = T0.AddHours(3);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task Exec(NpgsqlConnection c, CancellationToken ct, string sql, params object?[] values) =>
        DarlingMcpTestData.ExecAsync(c, ct, sql, values.Select(v => v is DateTime d ? DateTime.SpecifyKind(d, DateTimeKind.Unspecified) : v).ToArray());

    /// <summary>Three collector passes of one database: the marker row, then the forced plans. As #6221's collector writes them: no variant ids.</summary>
    private static async Task CaptureAsync(NpgsqlConnection c, CancellationToken ct, string db, params (long Plan, long Query)[] forced)
    {
        const string sql = @"INSERT INTO collect.query_store_forced_plans
(collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, query_plan_hash, plan_forcing_type, force_failure_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)";
        foreach (var minutes in new[] { 0, 30, 60 })
        {
            var at = T0.AddMinutes(minutes);
            await Exec(c, ct, sql, CollectionIdGenerator.Next(), at, ServerId, ServerName, db, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);
            foreach (var f in forced)
                await Exec(c, ct, sql, CollectionIdGenerator.Next(), at, ServerId, ServerName, db, f.Plan, f.Query, "0x" + f.Plan.ToString("X", CultureInfo.InvariantCulture), "MANUAL", 0L);
        }
    }

    private static Task RanAsync(NpgsqlConnection c, CancellationToken ct, string db, long query, long plan, DateTime lastRun, long executions) =>
        Exec(c, ct, @"INSERT INTO query_store_stats
(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, runtime_stats_interval_id,
 interval_start_time_utc, first_execution_time, last_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, avg_rowcount, is_forced_plan, force_failure_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,'Regular',$8,$9,$9,$10,$11,5000,7000,1,FALSE,0)",
            CollectionIdGenerator.Next(), lastRun.AddMinutes(1), ServerId, ServerName, db, query, plan, plan * 1000 + lastRun.Minute, lastRun.AddMinutes(-30), lastRun, executions);

    private sealed record Rig(NpgsqlDataSource Postgres, NpgsqlConnection Connection);

    private static async Task WithStoreAsync(Func<Rig, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the stored-variant live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(new Rig(postgres, connection), ct);
    }

    private static async Task<ForcedPlanFact> ForceAsync(Rig rig, string db, long plan, CancellationToken ct)
    {
        var read = await DarlingForcedPlanStateReader.ReadAsync(rig.Postgres, ServerId, DatabaseFilter.One(db), null, T0.AddHours(-1), AsOf, null, 30, cancellationToken: ct);
        return read.Plans.Single(p => p.PlanId == plan);
    }

    /// <summary>The forced plan 11 of query 1 ran once; its runs then moved to plan <paramref name="later"/>, which ran after the force was first seen.</summary>
    private static async Task SeedAsync(Rig rig, string db, long later, string? xml, CancellationToken ct, bool storePlan = true, bool gzip = false, bool nullDigest = false)
    {
        await CaptureAsync(rig.Connection, ct, db, (11, 1));
        await RanAsync(rig.Connection, ct, db, 1, later, T0.AddMinutes(40), 8);
        if (storePlan) await StoredPlanSeed.AddAsync(rig.Connection, ct, ServerId, db, later, xml, gzip, nullDigest);
    }

    [Fact]
    public async Task ALaterPlanWhoseStoredXmlIsAVariant_IsTheForceRunning_NotIgnored_AndTheToolSaysSo()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            await SeedAsync(rig, "VariantDb", 12, StoredPlanSeed.Variant, ct);
            var fact = await ForceAsync(rig, "VariantDb", 11, ct);

            Assert.Equal(ForcedPlanState.Working, fact.State);
            var variant = Assert.Single(fact.VariantPlans);
            Assert.Equal(12, variant.PlanId);
            Assert.Equal(8, variant.Executions);
            Assert.Empty(fact.OtherPlans);
            Assert.Empty(fact.UnknownPlans);
            Assert.Null(fact.PlanGuide);

            using var doc = JsonDocument.Parse(await DarlingMcpForcedPlanTools.GetForcedPlans(rig.Postgres, ServerName, 24, AsOf.ToString("o", CultureInfo.InvariantCulture), DatabaseFilter.One("VariantDb"), ct));
            var force = Assert.Single(doc.RootElement.GetProperty("forced_plans").EnumerateArray());
            Assert.Equal("WORKING", force.GetProperty("state").GetString());
            Assert.Equal(12, Assert.Single(force.GetProperty("variant_plans").EnumerateArray()).GetProperty("plan_id").GetInt64());
            Assert.Equal(0, force.GetProperty("other_plans_total").GetInt32());
        });
    }

    [Fact]
    public async Task AVariantStoredAsGzip_IsFoundToo()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            await SeedAsync(rig, "GzipDb", 12, StoredPlanSeed.Variant, ct, gzip: true);
            Assert.Equal(ForcedPlanState.Working, (await ForceAsync(rig, "GzipDb", 11, ct)).State);
        });
    }

    [Fact]
    public async Task ALaterPlanThatCarriesAPlanGuideName_NamesTheGuide_AndIsAnotherPlanNotAVariant()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            await SeedAsync(rig, "GuideDb", 12, StoredPlanSeed.Guided("pm_guide_6221"), ct);
            var fact = await ForceAsync(rig, "GuideDb", 11, ct);

            Assert.Equal("pm_guide_6221", fact.PlanGuide?.Name);
            Assert.Empty(fact.VariantPlans);
            Assert.Equal(12, Assert.Single(fact.OtherPlans).PlanId);
            Assert.Equal(ForcedPlanState.Ignored, fact.State);

            using var doc = JsonDocument.Parse(JsonSerializer.Serialize(DarlingMcpForcedPlanTools.ForceNode(fact)));
            Assert.Equal("pm_guide_6221", doc.RootElement.GetProperty("plan_guide").GetProperty("name").GetString());
        });
    }

    [Fact]
    public async Task ALaterPlanWithoutUsePlan_KeepsTodaysIgnored()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            await SeedAsync(rig, "PlainDb", 12, StoredPlanSeed.Plain, ct);
            var fact = await ForceAsync(rig, "PlainDb", 11, ct);

            Assert.Equal(ForcedPlanState.Ignored, fact.State);
            Assert.Equal(12, Assert.Single(fact.OtherPlans).PlanId);
            Assert.Empty(fact.VariantPlans);
            Assert.Empty(fact.UnknownPlans);
            Assert.Null(fact.PlanGuide);
        });
    }

    [Fact]
    public async Task ALaterPlanWithNoStoredXml_IsUnknown_NotIgnored_AndTheAnswerGivesTheReason()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            /* Plan 12: no map row at all (not fetched yet). Plan 22 (query 2): a map row with a NULL digest (the engine could not persist it). */
            await SeedAsync(rig, "NoXmlDb", 12, null, ct, storePlan: false);
            await CaptureAsync(rig.Connection, ct, "NullDigestDb", (21, 2));
            await RanAsync(rig.Connection, ct, "NullDigestDb", 2, 22, T0.AddMinutes(40), 5);
            await StoredPlanSeed.AddAsync(rig.Connection, ct, ServerId, "NullDigestDb", 22, null, nullDigest: true);

            foreach (var (db, forced, later) in new[] { ("NoXmlDb", 11L, 12L), ("NullDigestDb", 21L, 22L) })
            {
                var fact = await ForceAsync(rig, db, forced, ct);
                Assert.Equal(ForcedPlanState.UnknownPlanNotCollected, fact.State);
                Assert.Equal(later, Assert.Single(fact.UnknownPlans).PlanId);
                Assert.Empty(fact.OtherPlans);
                Assert.Empty(fact.VariantPlans);
                Assert.Contains(later.ToString(CultureInfo.InvariantCulture), fact.UnknownReason, StringComparison.Ordinal);
                Assert.Contains("no plan XML in the store", fact.UnknownReason, StringComparison.Ordinal);
            }

            using var doc = JsonDocument.Parse(await DarlingMcpForcedPlanTools.GetForcedPlans(rig.Postgres, ServerName, 24, AsOf.ToString("o", CultureInfo.InvariantCulture), DatabaseFilter.One("NoXmlDb"), ct));
            var force = Assert.Single(doc.RootElement.GetProperty("forced_plans").EnumerateArray());
            Assert.Equal("UNKNOWN_PLAN_NOT_COLLECTED", force.GetProperty("state").GetString());
            Assert.Equal(12, Assert.Single(force.GetProperty("unknown_plans").EnumerateArray()).GetProperty("plan_id").GetInt64());
            Assert.Contains("not known", force.GetProperty("unknown_reason").GetString(), StringComparison.Ordinal);
            Assert.Equal(1, doc.RootElement.GetProperty("state_counts").GetProperty("UNKNOWN_PLAN_NOT_COLLECTED").GetInt32());
        });
    }

    [Fact]
    public async Task OnceThePlanXmlIsStored_TheUnknownResolves_ToTheStateItsXmlGives()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            await SeedAsync(rig, "LaterDb", 12, null, ct, storePlan: false);
            Assert.Equal(ForcedPlanState.UnknownPlanNotCollected, (await ForceAsync(rig, "LaterDb", 11, ct)).State);

            await StoredPlanSeed.AddAsync(rig.Connection, ct, ServerId, "LaterDb", 12, StoredPlanSeed.Variant);
            Assert.Equal(ForcedPlanState.Working, (await ForceAsync(rig, "LaterDb", 11, ct)).State);
        });
    }

    [Fact]
    public async Task ADefiniteOtherPlan_BeatsAnUnknownOne()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            const string db = "MixedDb";
            await CaptureAsync(rig.Connection, ct, db, (11, 1));
            await RanAsync(rig.Connection, ct, db, 1, 12, T0.AddMinutes(40), 8);
            await RanAsync(rig.Connection, ct, db, 1, 13, T0.AddMinutes(41), 2);
            await StoredPlanSeed.AddAsync(rig.Connection, ct, ServerId, db, 13, StoredPlanSeed.Plain);

            var fact = await ForceAsync(rig, db, 11, ct);
            Assert.Equal(ForcedPlanState.Ignored, fact.State);
            Assert.Equal(13, Assert.Single(fact.OtherPlans).PlanId);
            Assert.Equal(12, Assert.Single(fact.UnknownPlans).PlanId);
        });
    }

    [Fact]
    public async Task ALaterPlanThatDidNotRunInTheWindow_IsNotLookedUpAndChangesNothing()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            const string db = "QuietDb";
            await CaptureAsync(rig.Connection, ct, db, (11, 1));
            /* Plan 12 ran before the force was first seen: it is not a run after the force, so its missing XML is not an unknown. Plan 11 ran after. */
            await RanAsync(rig.Connection, ct, db, 1, 12, T0.AddMinutes(-30), 99);
            await RanAsync(rig.Connection, ct, db, 1, 11, T0.AddMinutes(40), 4);

            var fact = await ForceAsync(rig, db, 11, ct);
            Assert.Equal(ForcedPlanState.Working, fact.State);
            Assert.Empty(fact.UnknownPlans);
            Assert.Null(fact.UnknownReason);
        });
    }

    [Fact]
    public async Task MoreLaterPlansThanTheBound_AreNotAllLookedUp_AndTheRestAreUnknownWithThatReason()
    {
        await WithStoreAsync(async (rig, ct) =>
        {
            const string db = "ManyDb";
            var plans = Enumerable.Range(0, DarlingForcedPlanStateReader.MaxStoredPlanChecks + 5).Select(i => 100L + i).ToArray();
            await CaptureAsync(rig.Connection, ct, db, (11, 1));
            foreach (var plan in plans)
            {
                await RanAsync(rig.Connection, ct, db, 1, plan, T0.AddMinutes(40), 1);
                await StoredPlanSeed.AddAsync(rig.Connection, ct, ServerId, db, plan, StoredPlanSeed.Variant);
            }

            var fact = await ForceAsync(rig, db, 11, ct);
            Assert.Equal(DarlingForcedPlanStateReader.MaxStoredPlanChecks, fact.VariantPlans.Count);
            Assert.Equal(5, fact.UnknownPlans.Count);
            Assert.Equal(ForcedPlanState.UnknownPlanNotCollected, fact.State);
            Assert.Contains("were not looked up", fact.UnknownReason, StringComparison.Ordinal);
        });
    }
}
