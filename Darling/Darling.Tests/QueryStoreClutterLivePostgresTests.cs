/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one, so it is not serialized against the live-postgres collection. It plants rows in five collector
   tables under two server ids and reads them back through the real tool; a shared store would make every one
   of those plants a race with the sibling classes that plant the same tables. */

/// <summary>
/// The half of <c>get_query_store_clutter</c> (#3797) no text pin reaches: whether the four statements PARSE
/// and whether their arithmetic agrees with <see cref="QueryStoreClutter"/>'s on planted rows whose numbers
/// are known in advance — three databases with one dominating <c>slowest_item</c> on 8 of 10 runs, plan churn
/// where one database carries one plan per query and another six, a replica row (READ_ONLY, reason 8) that
/// must come back excluded, and a <c>QDS_*</c> sleep wait present in the store that must land in
/// <c>excluded_wait_types_present_in_store</c> and not in the proxy. The raw numbers are asserted off the
/// tool's JSON, not off the reader's rows, so the projection is under test too.
/// </summary>
public sealed class QueryStoreClutterLivePostgresTests
{
    private const int PrimaryId = 5001;
    private const string PrimaryName = "clutter-primary";
    private const int ReplicaId = 5002;
    private const string ReplicaName = "clutter-replica";
    private const int EmptyId = 5003;
    private const string EmptyName = "clutter-empty";

    private const string Alpha = "db_alpha";
    private const string Beta = "db_beta";
    private const string Gamma = "db_gamma";
    private const string Off = "db_off";

    [Fact]
    public async Task ThePlantedFleet_ReadsBackThroughTheTool_WithTheRawNumbersAsserted()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;

        /* The anchor: a whole second, so the stamps the tool renders round-trip exactly. Every plant is
           relative to it and the tool is called as_of it, so nothing here depends on the wall clock. */
        var anchor = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
            await PlantAsync(connection, anchor, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString);
        var asOf = Stamp(anchor);

        /* ── the primary, whole ── */
        var json = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: 24, include_fleet_median: true, as_of: asOf);
        Assert.False(McpHelpers.IsErrorEnvelope(json), json);
        using (var doc = JsonDocument.Parse(json))
        {
            var root = doc.RootElement;
            Assert.Equal(PrimaryName, root.GetProperty("server").GetString());
            Assert.Equal(24, root.GetProperty("hours_back").GetInt32());
            Assert.False(root.GetProperty("server_is_replica").GetBoolean());
            Assert.False(root.GetProperty("capture_mode_known").GetBoolean());

            /* the window floor: the raw tier's first row is six hours before the anchor, the request asked for 24 */
            var window = root.GetProperty("window");
            Assert.Equal(Stamp(anchor.AddHours(-24)), window.GetProperty("start").GetString());
            Assert.Equal(Stamp(anchor), window.GetProperty("end").GetString());
            Assert.True(window.GetProperty("window_truncated").GetBoolean());
            Assert.Equal(6.0, window.GetProperty("effective_hours_back").GetDouble());
            Assert.Equal(Stamp(anchor.AddHours(-6)), window.GetProperty("effective_start").GetString());
            Assert.Contains("plan-churn arm", window.GetProperty("truncation_note").GetString(), StringComparison.Ordinal);

            /* the page: four databases, none cut at the default limit */
            Assert.Equal(4, root.GetProperty("database_count").GetInt32());
            Assert.Equal(4, root.GetProperty("databases_returned").GetInt32());
            Assert.False(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(DarlingMcpQueryStoreClutterTools.DefaultLimit, root.GetProperty("limit").GetInt32());

            var databases = root.GetProperty("databases").EnumerateArray().ToArray();
            Assert.Equal(new[] { Alpha, Beta, Gamma, Off }, databases.Select(d => d.GetProperty("database_name").GetString()).ToArray());

            /* ── alpha: the motivating shape — slowest on 8 of 10 runs, 90% of the pass ── */
            var alpha = databases[0];
            Assert.Equal("Critical", alpha.GetProperty("verdict").GetString());
            Assert.Equal(new[] { QueryStoreClutter.ReasonReadCostDominant }, alpha.GetProperty("verdict_reasons").EnumerateArray().Select(r => r.GetString()).ToArray());
            Assert.False(alpha.GetProperty("excluded").GetBoolean());
            var alphaCost = alpha.GetProperty("read_cost");
            Assert.Equal(10, alphaCost.GetProperty("runs_observed").GetInt32());
            Assert.Equal(8, alphaCost.GetProperty("runs_slowest").GetInt32());
            Assert.Equal(80.0, alphaCost.GetProperty("runs_slowest_pct").GetDouble());
            Assert.Equal(9000, alphaCost.GetProperty("slowest_item_ms_p50").GetInt32());
            Assert.Equal(9000, alphaCost.GetProperty("slowest_item_ms_p95").GetInt32());
            Assert.Equal(10000, alphaCost.GetProperty("run_duration_ms_p50").GetInt32());
            Assert.Equal(90.0, alphaCost.GetProperty("slowest_share_pct").GetDouble());
            /* the others' pooled median: beta's 4000 and gamma's 3000 → percentile_disc picks 3000 → 9000 / 3000 */
            Assert.Equal(3000, alphaCost.GetProperty("others_slowest_item_ms_p50").GetInt32());
            Assert.Equal(3.0, alphaCost.GetProperty("dominance_ratio").GetDouble());
            Assert.Equal(3, alphaCost.GetProperty("fanout_items_max").GetInt32());
            Assert.Equal(Stamp(anchor.AddHours(-10).AddMinutes(7 * 30)), alphaCost.GetProperty("last_slowest_at").GetString());
            var alphaChurn = alpha.GetProperty("plan_churn");
            Assert.Equal(5, alphaChurn.GetProperty("distinct_queries").GetInt32());
            Assert.Equal(5, alphaChurn.GetProperty("distinct_plans").GetInt32());
            Assert.Equal(1, alphaChurn.GetProperty("plans_per_query_p95").GetInt32());
            Assert.Equal(1, alphaChurn.GetProperty("plans_per_query_max").GetInt32());
            Assert.Equal(0, alphaChurn.GetProperty("plans_seen_once").GetInt32());
            Assert.Equal(0.0, alphaChurn.GetProperty("never_seen_twice_fraction").GetDouble());
            Assert.Equal(0, alphaChurn.GetProperty("plans_first_seen_in_window").GetInt32());
            Assert.Equal(0.0, alphaChurn.GetProperty("new_plans_per_day").GetDouble());
            Assert.Equal(3, alphaChurn.GetProperty("collections_observed").GetInt32());
            var alphaConfig = alpha.GetProperty("config");
            Assert.Equal("READ_WRITE", alphaConfig.GetProperty("actual_state").GetString());
            Assert.Equal(0, alphaConfig.GetProperty("readonly_reason").GetInt32());
            Assert.Equal(JsonValueKind.Null, alphaConfig.GetProperty("readonly_reason_decoded").ValueKind);
            /* the NEWEST capture in the window (4096 MB), not the older one (2048 MB) */
            Assert.Equal(4096, alphaConfig.GetProperty("current_storage_size_mb").GetInt64());
            Assert.Equal(50.0, alphaConfig.GetProperty("pct_of_cap").GetDouble());
            Assert.Equal(Stamp(anchor.AddMinutes(-30)), alphaConfig.GetProperty("captured_at").GetString());
            Assert.Equal(200, alphaConfig.GetProperty("max_plans_per_query").GetInt64());
            Assert.Equal(JsonValueKind.Null, alphaConfig.GetProperty("query_capture_mode").ValueKind);
            Assert.False(alphaConfig.GetProperty("capture_mode_known").GetBoolean());
            Assert.Equal(DarlingMcpQueryStoreClutterTools.CaptureModeNote, alphaConfig.GetProperty("capture_mode_note").GetString());
            Assert.Contains("per-database schedule override", alpha.GetProperty("recommendations")[0].GetString(), StringComparison.Ordinal);
            Assert.Contains("get_query_store_top", alpha.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()));

            /* ── beta: one query with six plans, three of them one-shots ── */
            var beta = databases[1];
            Assert.Equal("Warning", beta.GetProperty("verdict").GetString());
            Assert.Equal(new[] { QueryStoreClutter.ReasonPlanChurnHigh }, beta.GetProperty("verdict_reasons").EnumerateArray().Select(r => r.GetString()).ToArray());
            var betaCost = beta.GetProperty("read_cost");
            Assert.Equal(1, betaCost.GetProperty("runs_slowest").GetInt32());
            Assert.Equal(10.0, betaCost.GetProperty("runs_slowest_pct").GetDouble());
            Assert.Equal(40.0, betaCost.GetProperty("slowest_share_pct").GetDouble());
            /* the others: alpha's eight 9000s and gamma's 3000 → percentile_disc over nine values is 9000 */
            Assert.Equal(9000, betaCost.GetProperty("others_slowest_item_ms_p50").GetInt32());
            Assert.Equal(0.44, betaCost.GetProperty("dominance_ratio").GetDouble());
            var betaChurn = beta.GetProperty("plan_churn");
            Assert.Equal(4, betaChurn.GetProperty("distinct_queries").GetInt32());
            Assert.Equal(9, betaChurn.GetProperty("distinct_plans").GetInt32());
            Assert.Equal(6, betaChurn.GetProperty("plans_per_query_p95").GetInt32());
            Assert.Equal(6, betaChurn.GetProperty("plans_per_query_max").GetInt32());
            Assert.Equal(3, betaChurn.GetProperty("plans_seen_once").GetInt32());
            Assert.Equal(0.333, betaChurn.GetProperty("never_seen_twice_fraction").GetDouble());
            Assert.Equal(3, betaChurn.GetProperty("plans_first_seen_in_window").GetInt32());
            /* three arrivals over the two hours beta's collections span → 36 a day */
            Assert.Equal(36.0, betaChurn.GetProperty("new_plans_per_day").GetDouble());
            Assert.Equal(Stamp(anchor.AddHours(-6)), betaChurn.GetProperty("first_collection").GetString());
            Assert.Equal(Stamp(anchor.AddHours(-4)), betaChurn.GetProperty("last_collection").GetString());

            /* ── gamma: measured (a fan-out item once, configured, no runtime rows) and quiet ── */
            var gamma = databases[2];
            Assert.Equal("Healthy", gamma.GetProperty("verdict").GetString());
            Assert.Empty(gamma.GetProperty("verdict_reasons").EnumerateArray());
            Assert.Equal(JsonValueKind.Null, gamma.GetProperty("plan_churn").ValueKind);
            Assert.Equal(30.0, gamma.GetProperty("read_cost").GetProperty("slowest_share_pct").GetDouble());

            /* ── off: configured OFF, nothing else → Unknown with the reason, not a band ── */
            var off = databases[3];
            Assert.Equal("Unknown", off.GetProperty("verdict").GetString());
            Assert.Equal(new[] { QueryStoreClutter.ReasonQueryStoreOff }, off.GetProperty("verdict_reasons").EnumerateArray().Select(r => r.GetString()).ToArray());
            Assert.False(off.GetProperty("excluded").GetBoolean());

            /* ── the per-server block ── */
            var overhead = root.GetProperty("qs_overhead");
            var waits = overhead.GetProperty("wait_stats");
            var included = waits.GetProperty("included").EnumerateArray().ToArray();
            Assert.Equal(new[] { "QDS_LOADDB", "QDS_BLOCKING_TASK" }, included.Select(w => w.GetProperty("wait_type").GetString()).ToArray());
            var loaddb = included[0];
            /* three rated rows of 600 ms over 60 s, one unknowable (0, 0), one pre-V127 row of 100 ms with no interval */
            Assert.Equal(1900, loaddb.GetProperty("wait_ms_total").GetInt64());
            Assert.Equal(180, loaddb.GetProperty("measured_seconds").GetInt64());
            Assert.Equal(36000.0, loaddb.GetProperty("wait_ms_per_hour").GetDouble());
            Assert.Equal(5, loaddb.GetProperty("rows_observed").GetInt32());
            Assert.Equal(1, loaddb.GetProperty("rows_unknowable").GetInt32());
            Assert.Equal(1, loaddb.GetProperty("rows_without_interval").GetInt32());
            Assert.Equal(7200.0, included[1].GetProperty("wait_ms_per_hour").GetDouble());
            Assert.Equal(2020, waits.GetProperty("total_wait_ms").GetInt64());
            Assert.Equal(43200.0, waits.GetProperty("total_wait_ms_per_hour").GetDouble());
            Assert.Equal(QueryStoreClutter.ExcludedQdsWaitTypes.ToArray(), waits.GetProperty("excluded_wait_types").EnumerateArray().Select(w => w.GetString()!).ToArray());
            /* the sleep wait planted in the store lands here, and nowhere in the proxy */
            Assert.Equal(new[] { "QDS_ASYNC_QUEUE" }, waits.GetProperty("excluded_wait_types_present_in_store").EnumerateArray().Select(w => w.GetString()).ToArray());

            var clerk = overhead.GetProperty("memory_clerk");
            Assert.Equal(DarlingQueryStoreClutterReader.QueryStoreMemoryClerk, clerk.GetProperty("clerk_type").GetString());
            Assert.Equal(640.25m, clerk.GetProperty("latest_memory_mb").GetDecimal());
            Assert.Equal(640.25m, clerk.GetProperty("max_memory_mb_in_window").GetDecimal());
            Assert.Equal(2, clerk.GetProperty("clerk_samples").GetInt32());
            Assert.Equal(2, clerk.GetProperty("captures_in_window").GetInt32());
            Assert.True(clerk.GetProperty("clerk_in_latest_capture").GetBoolean());
            Assert.Equal(Stamp(anchor.AddMinutes(-5)), clerk.GetProperty("latest_clerk_captured_at").GetString());

            Assert.Equal(JsonValueKind.Array, overhead.GetProperty("discontinuities").ValueKind);
            Assert.Empty(overhead.GetProperty("discontinuities").EnumerateArray());

            /* ── the fleet reference: the replica server is excluded, the primary is the population ── */
            var fleet = root.GetProperty("fleet_median");
            /* three registered targets; the replica is excluded; the empty server is a non-replica with nothing to contribute, so it is
               in the server population and in no arm's */
            Assert.Equal(3, fleet.GetProperty("enabled_sql_server_targets").GetInt32());
            Assert.Equal(1, fleet.GetProperty("replica_servers_excluded").GetInt32());
            Assert.Equal(2, fleet.GetProperty("servers_in_median").GetInt32());
            /* shares 90 / 40 / 30 → 40; plans per query 1 / 6 → 1; one server's QDS rate → 43200 */
            Assert.Equal(40.0, fleet.GetProperty("slowest_share_pct").GetDouble());
            Assert.Equal(3, fleet.GetProperty("databases_in_read_cost_median").GetInt32());
            Assert.Equal(1.0, fleet.GetProperty("plans_per_query_p95").GetDouble());
            Assert.Equal(2, fleet.GetProperty("databases_in_churn_median").GetInt32());
            Assert.Equal(43200.0, fleet.GetProperty("qds_wait_ms_per_hour").GetDouble());
            Assert.Equal(1, fleet.GetProperty("servers_in_wait_median").GetInt32());
        }

        /* ── the page cut: two of four, worst first, observed off the fetch ── */
        var paged = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: 24, limit: 2, as_of: asOf);
        using (var doc = JsonDocument.Parse(paged))
        {
            var root = doc.RootElement;
            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(2, root.GetProperty("databases_returned").GetInt32());
            Assert.Equal(4, root.GetProperty("database_count").GetInt32());
            Assert.Equal(new[] { Alpha, Beta }, root.GetProperty("databases").EnumerateArray().Select(d => d.GetProperty("database_name").GetString()).ToArray());
            Assert.Equal(JsonValueKind.Null, root.GetProperty("fleet_median").ValueKind);
            Assert.Contains("include_fleet_median=true", root.GetProperty("fleet_median_note").GetString(), StringComparison.Ordinal);
        }

        /* ── the replica: excluded by architecture, the overhead block still real ── */
        var replica = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, ReplicaName, hours_back: 24, as_of: asOf);
        Assert.False(McpHelpers.IsErrorEnvelope(replica), replica);
        using (var doc = JsonDocument.Parse(replica))
        {
            var root = doc.RootElement;
            Assert.True(root.GetProperty("server_is_replica").GetBoolean());
            Assert.Contains("readable secondary", root.GetProperty("server_note").GetString(), StringComparison.Ordinal);
            var row = Assert.Single(root.GetProperty("databases").EnumerateArray());
            Assert.Equal(Alpha, row.GetProperty("database_name").GetString());
            Assert.True(row.GetProperty("excluded").GetBoolean());
            Assert.Equal(QueryStoreClutter.ReasonReplica, row.GetProperty("excluded_reason").GetString());
            Assert.Equal("Unknown", row.GetProperty("verdict").GetString());
            Assert.Equal(new[] { QueryStoreClutter.ReasonReplica }, row.GetProperty("verdict_reasons").EnumerateArray().Select(r => r.GetString()).ToArray());
            Assert.Equal("READ_ONLY", row.GetProperty("config").GetProperty("actual_state").GetString());
            Assert.Equal(8, row.GetProperty("config").GetProperty("readonly_reason").GetInt32());
            Assert.Equal("database is a secondary replica", row.GetProperty("config").GetProperty("readonly_reason_decoded").GetString());
            Assert.Contains("nothing here is a defect on this server", string.Join(" ", row.GetProperty("recommendations").EnumerateArray().Select(r => r.GetString())), StringComparison.Ordinal);
            /* the excluded row's next_tools stop at the two composition sources: no query-level read for a catalog that is the primary's */
            Assert.Equal(new[] { "get_query_store_health", "get_collection_health", "get_wait_stats", "get_memory_clerks" }, row.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()).ToArray());
            var included = root.GetProperty("qs_overhead").GetProperty("wait_stats").GetProperty("included").EnumerateArray().ToArray();
            Assert.Equal("QDS_LOADDB", Assert.Single(included).GetProperty("wait_type").GetString());
            Assert.Equal(600, included[0].GetProperty("wait_ms_total").GetInt64());
        }

        /* ── nothing at all: the plain miss, through the ladder ── */
        var empty = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, EmptyName, hours_back: 24, as_of: asOf);
        Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(empty));
        Assert.Contains("24-hour window", empty, StringComparison.Ordinal);

        /* ── a bad limit and a bad window are refused through the shared shapes ── */
        Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, limit: 0)));
        Assert.True(McpHelpers.IsRefusalEnvelope(await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: 169)));
    }

    /// <summary>
    /// The reader agrees with the pure judgment's arithmetic on the same planted rows — the cross-check that
    /// the SQL's <c>percentile_disc</c> and the C# <see cref="QueryStoreClutter.DiscreteMedian"/> are one
    /// definition: the fleet share median the tool published (40) is the discrete median of the three
    /// per-database shares the reader returned.
    /// </summary>
    [Fact]
    public async Task TheReadersDiscreteMedian_AndThePureOne_AgreeOnTheSameRows()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        var anchor = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
            await PlantAsync(connection, anchor, ct);
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString);
        var start = anchor.AddHours(-24);

        var readCost = await DarlingQueryStoreClutterReader.GetReadCostAsync(postgres, [PrimaryId, ReplicaId, EmptyId], start, anchor, ct);
        Assert.Equal(3, readCost.Count);
        Assert.All(readCost, r => Assert.Equal(PrimaryId, r.ServerId));
        Assert.Equal(40.0, QueryStoreClutter.DiscreteMedian(readCost.Select(r => r.SlowestSharePctP50)));

        var churn = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(postgres, [PrimaryId, ReplicaId, EmptyId], start, anchor, ct);
        Assert.Equal(new[] { Alpha, Beta }, churn.Select(r => r.DatabaseName).ToArray());

        var config = await DarlingQueryStoreClutterReader.GetConfigAsync(postgres, [PrimaryId, ReplicaId, EmptyId], start, anchor, ct);
        Assert.Equal(5, config.Count);
        Assert.Single(config, c => c.IsSecondaryReplica);
        Assert.True(QueryStoreClutter.IsReplicaServer(config.Where(c => c.ServerId == ReplicaId)));
        Assert.False(QueryStoreClutter.IsReplicaServer(config.Where(c => c.ServerId == PrimaryId)));

        var waits = await DarlingQueryStoreClutterReader.GetQdsWaitsAsync(postgres, [PrimaryId, ReplicaId, EmptyId], start, anchor, ct);
        /* the sleep wait comes back from the store (the read does not know the ignore list) — the tool excludes it by name */
        Assert.Contains(waits, w => w.WaitType == "QDS_ASYNC_QUEUE" && w.ServerId == PrimaryId);
        Assert.DoesNotContain(waits, w => w.WaitType == "PAGEIOLATCH_SH");

        var fleet = await DarlingQueryStoreClutterReader.GetEnabledSqlServerTargetsAsync(postgres, MonitoredEngineKind.SqlServer, ct);
        Assert.Equal(new[] { PrimaryId, ReplicaId, EmptyId }, fleet);
    }

    /* ─────────────────────────── the plant ─────────────────────────── */

    private static async Task PlantAsync(NpgsqlConnection c, DateTime anchor, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(c, PrimaryId, PrimaryName, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ReplicaId, ReplicaName, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, EmptyId, EmptyName, ct);

        /* (a) ten query_store fan-out runs, 30 minutes apart from ten hours back: alpha slowest on the first
           eight (9000 of 10000 ms), beta on the ninth (4000), gamma on the tenth (3000). Plus a run with no
           fan-out, a zero-duration run, and a run of another collector — none of which may count. */
        for (var i = 0; i < 10; i++)
        {
            var (slowest, ms) = i < 8 ? (Alpha, 9000) : i == 8 ? (Beta, 4000) : (Gamma, 3000);
            await PlantRunAsync(c, PrimaryId, PrimaryName, "query_store", anchor.AddHours(-10).AddMinutes(30 * i), 10000, 3, slowest, ms, ct);
        }

        await PlantRunAsync(c, PrimaryId, PrimaryName, "query_store", anchor.AddHours(-11), 40, null, null, null, ct);
        await PlantRunAsync(c, PrimaryId, PrimaryName, "query_store", anchor.AddHours(-12), 0, 3, Alpha, 0, ct);
        await PlantRunAsync(c, PrimaryId, PrimaryName, "query_store_health", anchor.AddHours(-9), 5000, 3, Beta, 4900, ct);

        /* (b) three collections, six, five and four hours back. alpha: five queries, one plan each, every plan
           in every collection. beta: q1 with plans 101-106 (101-103 in every collection; 104 only at -5h, 105
           and 106 only at -4h), q2-q4 one plan each in every collection. */
        var collections = new[] { anchor.AddHours(-6), anchor.AddHours(-5), anchor.AddHours(-4) };
        foreach (var t in collections)
        {
            for (var q = 1; q <= 5; q++)
            {
                await PlantStatAsync(c, PrimaryId, PrimaryName, Alpha, q, q * 10, t, ct);
            }

            foreach (var plan in new long[] { 101, 102, 103 })
            {
                await PlantStatAsync(c, PrimaryId, PrimaryName, Beta, 1, plan, t, ct);
            }

            for (var q = 2; q <= 4; q++)
            {
                await PlantStatAsync(c, PrimaryId, PrimaryName, Beta, q, q * 100 + 1, t, ct);
            }
        }

        await PlantStatAsync(c, PrimaryId, PrimaryName, Beta, 1, 104, collections[1], ct);
        await PlantStatAsync(c, PrimaryId, PrimaryName, Beta, 1, 105, collections[2], ct);
        await PlantStatAsync(c, PrimaryId, PrimaryName, Beta, 1, 106, collections[2], ct);

        /* (c) two health captures on the primary: an older one 90 minutes back at 2048 MB and the newest 30
           minutes back at 4096 MB — the tool must publish the newest. off is OFF. The replica's alpha carries
           the engine's readable-secondary bit. */
        foreach (var db in new[] { Alpha, Beta, Gamma })
        {
            await PlantHealthAsync(c, PrimaryId, PrimaryName, db, "READ_WRITE", 0, 2048, anchor.AddMinutes(-90), ct);
            await PlantHealthAsync(c, PrimaryId, PrimaryName, db, "READ_WRITE", 0, 4096, anchor.AddMinutes(-30), ct);
        }

        await PlantHealthAsync(c, PrimaryId, PrimaryName, Off, "OFF", 0, 0, anchor.AddMinutes(-30), ct);
        await PlantHealthAsync(c, ReplicaId, ReplicaName, Alpha, "READ_ONLY", DarlingQueryStoreClutterReader.SecondaryReplicaReadonlyBit, 4096, anchor.AddMinutes(-30), ct);

        /* (d) QDS waits on the primary: QDS_LOADDB three rated rows (600 ms over 60 s), one unknowable (0, 0),
           one pre-V127 row (100 ms, no interval); QDS_BLOCKING_TASK one rated row; the sleep wait
           QDS_ASYNC_QUEUE present in the store; a non-QDS wait beside them. One rated QDS_LOADDB row on the
           replica. */
        for (var i = 0; i < 3; i++)
        {
            await PlantWaitAsync(c, PrimaryId, PrimaryName, "QDS_LOADDB", anchor.AddMinutes(-60 + i), 600, 10, 60, ct);
        }

        await PlantWaitAsync(c, PrimaryId, PrimaryName, "QDS_LOADDB", anchor.AddMinutes(-70), 0, 0, 0, ct);
        await PlantWaitAsync(c, PrimaryId, PrimaryName, "QDS_LOADDB", anchor.AddMinutes(-80), 100, 2, null, ct);
        await PlantWaitAsync(c, PrimaryId, PrimaryName, "QDS_BLOCKING_TASK", anchor.AddMinutes(-60), 120, 1, 60, ct);
        await PlantWaitAsync(c, PrimaryId, PrimaryName, "QDS_ASYNC_QUEUE", anchor.AddMinutes(-60), 59000, 1, 60, ct);
        await PlantWaitAsync(c, PrimaryId, PrimaryName, "PAGEIOLATCH_SH", anchor.AddMinutes(-60), 5000, 200, 60, ct);
        await PlantWaitAsync(c, ReplicaId, ReplicaName, "QDS_LOADDB", anchor.AddMinutes(-60), 600, 10, 60, ct);

        /* the clerk: two captures, the clerk in both, growing */
        await PlantClerkAsync(c, PrimaryId, PrimaryName, anchor.AddMinutes(-10), "MEMORYCLERK_SQLBUFFERPOOL", 4000m, ct);
        await PlantClerkAsync(c, PrimaryId, PrimaryName, anchor.AddMinutes(-10), DarlingQueryStoreClutterReader.QueryStoreMemoryClerk, 512.5m, ct);
        await PlantClerkAsync(c, PrimaryId, PrimaryName, anchor.AddMinutes(-5), "MEMORYCLERK_SQLBUFFERPOOL", 4100m, ct);
        await PlantClerkAsync(c, PrimaryId, PrimaryName, anchor.AddMinutes(-5), DarlingQueryStoreClutterReader.QueryStoreMemoryClerk, 640.25m, ct);
    }

    private static Task PlantRunAsync(NpgsqlConnection c, int serverId, string serverName, string collector, DateTime at, int durationMs, int? fanout, string? slowest, int? slowestMs, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms, fanout_item_count, slowest_item, slowest_item_ms)
VALUES ($1, $2, $3, $4, $5, $6, 'SUCCESS', NULL, 100, $7, 10, $8, $9, $10)",
            CollectionIdGenerator.Next(), serverId, serverName, collector, DarlingMcpTestData.Naive(at), durationMs, Math.Max(0, durationMs - 10), fanout, slowest, slowestMs);

    private static Task PlantStatAsync(NpgsqlConnection c, int serverId, string serverName, string db, long queryId, long planId, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, last_execution_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 10, 1000, 800, $2)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, db, queryId, planId,
            "0xQ" + queryId.ToString(CultureInfo.InvariantCulture), "0xP" + planId.ToString(CultureInfo.InvariantCulture), "SELECT 1");

    private static Task PlantHealthAsync(NpgsqlConnection c, int serverId, string serverName, string db, string actual, int readonlyReason, long currentMb, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes)
VALUES ($1, $2, $3, $4, $5, $6, 'READ_WRITE', $7, $8, 8192, 'AUTO', 21, 200, 60)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, db, actual, readonlyReason, currentMb);

    private static Task PlantWaitAsync(NpgsqlConnection c, int serverId, string serverName, string waitType, DateTime at, long deltaMs, long deltaTasks, int? intervalSeconds, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms, signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 1000, 100000, 100, $6, $7, 0, $8)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, waitType, deltaTasks, deltaMs, intervalSeconds);

    private static Task PlantClerkAsync(NpgsqlConnection c, int serverId, string serverName, DateTime at, string clerk, decimal memoryMb, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1, $2, $3, $4, $5, $6)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, clerk, memoryMb);

    private static string Stamp(DateTime naiveUtc) =>
        DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
}
