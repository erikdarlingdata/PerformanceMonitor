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
using PerformanceMonitor.Darling.Service;
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
            /* False because ONE database (`off`) is still on a pre-rung capture — the server-level flag is the
               AND over the rows, not an average, so one unanswered row makes it false. */
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
            /* The NEWEST capture's mode, not the older pre-rung NULL beside it (V137, #3796). */
            Assert.Equal("ALL", alphaConfig.GetProperty("query_capture_mode").GetString());
            Assert.True(alphaConfig.GetProperty("capture_mode_known").GetBoolean());
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
            /* beta's mode is AUTO, and the churn recommendation SAYS so rather than sending the reader to
               find it — the sentence is a function of this row's own column. */
            Assert.Equal("AUTO", beta.GetProperty("config").GetProperty("query_capture_mode").GetString());
            Assert.Contains(
                "it is already AUTO",
                beta.GetProperty("recommendations").EnumerateArray().Select(r => r.GetString()!).Single(r => r.Contains("MAX_PLANS_PER_QUERY", StringComparison.Ordinal)),
                StringComparison.Ordinal);

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
            /* The pre-rung row: a NULL mode with the flag false — never rendered as the engine's NONE. */
            Assert.Equal(JsonValueKind.Null, off.GetProperty("config").GetProperty("query_capture_mode").ValueKind);
            Assert.False(off.GetProperty("config").GetProperty("capture_mode_known").GetBoolean());

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
    /// #9478: a fleet reference that runs past its own budget fails soft. The seam fires the fleet bound (or raises the
    /// <c>57014</c> the bound's cancel request produces) with no wait, and the per-server rows must still come back with a null
    /// <c>fleet_median</c> and a note naming the overrun. With the caller's own token cancelled the cancellation still
    /// propagates, because that is not an overrun.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AFleetReferenceOverBudget_ReturnsThePerServerRows_WithANullMedianAndANote(bool queryCanceledError)
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
        var asOf = Stamp(anchor);

        DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = fleetBudget =>
        {
            fleetBudget.Cancel();
            if (queryCanceledError) throw new PostgresException("canceling statement due to user request", "ERROR", "ERROR", "57014");
            return Task.Delay(Timeout.Infinite, fleetBudget.Token);
        };
        try
        {
            var json = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: 24, include_fleet_median: true, as_of: asOf);
            Assert.False(McpHelpers.IsErrorEnvelope(json), json);
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;
                Assert.Equal(4, root.GetProperty("database_count").GetInt32());
                Assert.Equal(Alpha, root.GetProperty("databases").EnumerateArray().First().GetProperty("database_name").GetString());
                Assert.Equal(JsonValueKind.Null, root.GetProperty("fleet_median").ValueKind);
                Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, root.GetProperty("fleet_median_note").GetString());
            }

            using var callerCancelled = new CancellationTokenSource();
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = _ =>
            {
                callerCancelled.Cancel();
                return Task.Delay(Timeout.Infinite, callerCancelled.Token);
            };
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(
                postgres, PrimaryName, hours_back: 24, include_fleet_median: true, as_of: asOf, logger: null, cancellationToken: callerCancelled.Token));
        }
        finally
        {
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = null;
        }
    }

    /// <summary>
    /// #8570: the waits, clerk and baseline-marker reads are in flight at the same time. Each one's seam waits for the other two to
    /// have started, so a call that ran them one after another would never get past the first (the wait gives up after 20 seconds
    /// and the test fails). The answer is the one the planted fleet gives.
    /// </summary>
    [Fact]
    public async Task TheThreeIndependentReads_AreInFlightTogether_AndTheAnswerIsUnchanged()
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
        var asOf = Stamp(anchor);

        var started = 0;
        var allStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var names = new System.Collections.Concurrent.ConcurrentBag<string>();
        DarlingMcpQueryStoreClutterTools.IndependentReadSeam.Value = async (name, token) =>
        {
            names.Add(name);
            if (Interlocked.Increment(ref started) == 3) allStarted.TrySetResult();
            await allStarted.Task.WaitAsync(TimeSpan.FromSeconds(20), token);
        };
        try
        {
            var json = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: 24, as_of: asOf);
            Assert.False(McpHelpers.IsErrorEnvelope(json), json);
            Assert.Equal(new[] { "clerk", "discontinuities", "waits" }, names.OrderBy(n => n, StringComparer.Ordinal).ToArray());
            using var doc = JsonDocument.Parse(json);
            Assert.Equal(4, doc.RootElement.GetProperty("database_count").GetInt32());
            var included = doc.RootElement.GetProperty("qs_overhead").GetProperty("wait_stats").GetProperty("included").EnumerateArray().ToArray();
            Assert.Contains(included, w => w.GetProperty("wait_type").GetString() == "QDS_LOADDB" && w.GetProperty("wait_ms_total").GetInt64() == 1900);
        }
        finally
        {
            DarlingMcpQueryStoreClutterTools.IndependentReadSeam.Value = null;
        }
    }

    /// <summary>
    /// #8570: a failing wait read still gives the error envelope the tool gave when the reads ran in sequence, and the two reads that
    /// were started beside it do not outlive the call. On an empty answer the baseline-marker read, which is no part of that answer, is
    /// stopped and finished before the call returns, and the answer is the same one the plain miss gave.
    /// </summary>
    [Fact]
    public async Task AFailingWaitsRead_GivesTheErrorEnvelope_AndTheEmptyAnswer_LeavesNoReadRunning()
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
        var asOf = Stamp(anchor);

        var running = 0;
        var finished = 0;
        DarlingMcpQueryStoreClutterTools.IndependentReadSeam.Value = async (name, token) =>
        {
            Interlocked.Increment(ref running);
            try
            {
                if (name == "waits") throw new InvalidOperationException("the waits read failed");
                if (name == "discontinuities") await Task.Delay(Timeout.Infinite, token);
            }
            finally
            {
                Interlocked.Increment(ref finished);
            }
        };
        try
        {
            var failed = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: 24, as_of: asOf);
            Assert.True(McpHelpers.IsErrorEnvelope(failed), failed);
            Assert.Contains("the waits read failed", failed, StringComparison.Ordinal);
            Assert.Equal(3, Volatile.Read(ref running));
            Assert.Equal(3, Volatile.Read(ref finished));

            Interlocked.Exchange(ref running, 0);
            Interlocked.Exchange(ref finished, 0);
            DarlingMcpQueryStoreClutterTools.IndependentReadSeam.Value = async (name, token) =>
            {
                Interlocked.Increment(ref running);
                try
                {
                    if (name == "discontinuities") await Task.Delay(Timeout.Infinite, token);
                }
                finally
                {
                    Interlocked.Increment(ref finished);
                }
            };
            var empty = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, EmptyName, hours_back: 24, as_of: asOf);
            Assert.Equal("unavailable", DarlingMcpTestData.StatusOf(empty));
            Assert.Contains("24-hour window", empty, StringComparison.Ordinal);
            Assert.Equal(3, Volatile.Read(ref running));
            Assert.Equal(3, Volatile.Read(ref finished));
        }
        finally
        {
            DarlingMcpQueryStoreClutterTools.IndependentReadSeam.Value = null;
        }
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

        var readCost = await DarlingQueryStoreClutterReader.GetReadCostAsync(postgres, [PrimaryId, ReplicaId, EmptyId], start, anchor, cancellationToken: ct);
        Assert.Equal(3, readCost.Count);
        Assert.All(readCost, r => Assert.Equal(PrimaryId, r.ServerId));
        Assert.Equal(40.0, QueryStoreClutter.DiscreteMedian(readCost.Select(r => r.SlowestSharePctP50)));

        var churn = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(postgres, [PrimaryId, ReplicaId, EmptyId], start, anchor, cancellationToken: ct);
        Assert.Equal(new[] { Alpha, Beta }, churn.Select(r => r.DatabaseName).ToArray());

        var config = await DarlingQueryStoreClutterReader.GetConfigAsync(postgres, [PrimaryId, ReplicaId, EmptyId], start, anchor, cancellationToken: ct);
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

    /* ─────────────────────────── #9882: the kept fleet reference ─────────────────────────── */

    /// <summary>A clock the test moves: it names the clock hour and measures a computation, so "dear" and "the next hour" are set, never waited for.</summary>
    private sealed class ManualClock(DateTime utcNow) : TimeProvider
    {
        private long _ticks = utcNow.Ticks;
        public void Advance(TimeSpan by) => Interlocked.Add(ref _ticks, by.Ticks);
        public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);
        public override long GetTimestamp() => Interlocked.Read(ref _ticks);
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;
    }

    private static readonly TimeSpan HeldCallGivesUp = TimeSpan.FromSeconds(60);

    private static async Task<(ScratchPostgres Scratch, string ConnectionString, string AsOf)> PlantedStoreAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(Environment.GetEnvironmentVariable("DARLING_TEST_PG")!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        var anchor = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
            await PlantAsync(connection, anchor, ct);
        }

        return (scratch, connectionString, Stamp(anchor));
    }

    /// <summary>The call's <c>fleet_median</c> as JSON text (the literal <c>null</c> when it has none) and its note.</summary>
    private static async Task<(string Median, string? Note)> FleetOfAsync(NpgsqlDataSource postgres, string asOf, int hoursBack = 24)
    {
        var json = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: hoursBack, include_fleet_median: true, as_of: asOf);
        Assert.False(McpHelpers.IsErrorEnvelope(json), json);
        using var doc = JsonDocument.Parse(json);
        /* the per-server answer is whole whatever the fleet arm did */
        Assert.True(doc.RootElement.GetProperty("database_count").GetInt32() > 0, json);
        return (doc.RootElement.GetProperty("fleet_median").GetRawText(), doc.RootElement.GetProperty("fleet_median_note").GetString());
    }

    private static void ClearFleetSeams()
    {
        DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = null;
        QueryStoreClutterFleetReference.ComputationSeam.Value = null;
        QueryStoreClutterFleetReference.Clock.Value = null;
    }

    /// <summary>
    /// #9882: a fleet reference that took longer than the read bar is kept for the clock hour it was started in, and a cheaper one is
    /// not. The computation seam counts the computations and moves the clock, so nothing here waits. Two calls on a cheap reference
    /// compute it twice. Once it is dear, the second call gets the same reference with no second computation and says it was shared,
    /// a different <c>hours_back</c> computes its own, and the next clock hour computes it again.
    /// </summary>
    [Fact]
    public async Task AFleetReferenceOverTheReadBar_IsKeptForItsClockHour_AndACheapOneIsNot()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var computations = 0;
        var took = TimeSpan.Zero;
        QueryStoreClutterFleetReference.Clock.Value = clock;
        QueryStoreClutterFleetReference.ComputationSeam.Value = _ =>
        {
            Interlocked.Increment(ref computations);
            clock.Advance(took);
            return Task.CompletedTask;
        };
        try
        {
            var cheap = await FleetOfAsync(postgres, asOf);
            Assert.NotEqual("null", cheap.Median);
            Assert.Equal(cheap, await FleetOfAsync(postgres, asOf));
            Assert.Equal(2, Volatile.Read(ref computations));

            took = TimeSpan.FromSeconds(QueryStoreClutterFleetReference.KeepAboveSeconds + 1);
            var dear = await FleetOfAsync(postgres, asOf);
            Assert.Equal(cheap, dear);
            Assert.Equal(3, Volatile.Read(ref computations));

            took = TimeSpan.Zero;
            clock.Advance(TimeSpan.FromMinutes(20));
            var kept = await FleetOfAsync(postgres, asOf);
            Assert.Equal(dear.Median, kept.Median);
            Assert.NotEqual(dear.Note, kept.Note);
            Assert.Equal(3, Volatile.Read(ref computations));

            /* another window length is another reference */
            await FleetOfAsync(postgres, asOf, hoursBack: 23);
            Assert.Equal(4, Volatile.Read(ref computations));

            /* 15:00 has passed: the reference kept at 14:10 is not this hour's */
            clock.Advance(TimeSpan.FromMinutes(40));
            var nextHour = await FleetOfAsync(postgres, asOf);
            Assert.Equal(dear, nextHour);
            Assert.Equal(5, Volatile.Read(ref computations));
        }
        finally
        {
            ClearFleetSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /// <summary>
    /// #9882: a computation that outlives its call's budget goes on, and there is one of it. The computation seam holds it in flight.
    /// Two calls whose budgets have fired both answer with the per-server rows, a null median and the note, and one computation was
    /// started. Once the computation is older than the budget a call with its whole budget left answers while the computation is
    /// still held. After the release the next call has the reference a store read gives, with no second computation.
    /// </summary>
    [Fact]
    public async Task AnOverBudgetFleetReference_IsComputedOnce_AndLaterCallsAnswerAtOnce_ThenWithTheKeptMedian()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var computations = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        QueryStoreClutterFleetReference.Clock.Value = clock;
        QueryStoreClutterFleetReference.ComputationSeam.Value = async cap =>
        {
            Interlocked.Increment(ref computations);
            entered.TrySetResult();
            await release.Task.WaitAsync(cap.Token);
        };
        try
        {
            /* the budget has fired before the wait starts, so each call leaves at once and the computation stays */
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = fleetBudget =>
            {
                fleetBudget.Cancel();
                return Task.CompletedTask;
            };
            var first = FleetOfAsync(postgres, asOf);
            var second = FleetOfAsync(postgres, asOf);
            foreach (var overBudget in await Task.WhenAll(first, second).WaitAsync(HeldCallGivesUp, ct))
            {
                Assert.Equal("null", overBudget.Median);
                Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, overBudget.Note);
            }

            await entered.Task.WaitAsync(HeldCallGivesUp, ct);
            Assert.Equal(1, Volatile.Read(ref computations));

            /* no seam fires this call's budget: it answers because the computation is older than a budget, not because it waited one out */
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = null;
            clock.Advance(TimeSpan.FromSeconds(DarlingMcpQueryStoreClutterTools.ReadBarSeconds + 1));
            var atOnce = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.False(release.Task.IsCompleted);
            Assert.Equal("null", atOnce.Median);
            Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, atOnce.Note);

            release.SetResult();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
            var kept = await FleetOfAsync(postgres, asOf);
            Assert.NotEqual("null", kept.Median);
            Assert.Equal(1, Volatile.Read(ref computations));

            /* the same store through another pool has no kept reference, so this one is read from the store now */
            ClearFleetSeams();
            await using var other = NpgsqlDataSource.Create(connectionString);
            var fresh = await FleetOfAsync(other, asOf);
            Assert.Equal(fresh.Median, kept.Median);
            Assert.NotEqual(fresh.Note, kept.Note);
        }
        finally
        {
            release.TrySetResult();
            ClearFleetSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /// <summary>
    /// #9882: a computation that ends with no reference is not tried again that clock hour. The seam fires the cap (or raises the
    /// <c>57014</c> a store role's statement timeout raises) with no wait. The call that started it and the next one both answer with
    /// the per-server rows, a null median and the unfinished note, on one computation, and the next clock hour tries again.
    /// A computation that fails any other way fails its call as before and is not kept.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AFleetReferenceThatDoesNotFinish_IsNotTriedAgainThatClockHour_AndAFailedOneIsNotKept(bool storeCancelledAStatement)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var computations = 0;
        QueryStoreClutterFleetReference.Clock.Value = clock;
        QueryStoreClutterFleetReference.ComputationSeam.Value = cap =>
        {
            Interlocked.Increment(ref computations);
            if (storeCancelledAStatement) throw new PostgresException("canceling statement due to statement timeout", "ERROR", "ERROR", "57014");
            cap.Cancel();
            return Task.Delay(Timeout.Infinite, cap.Token);
        };
        try
        {
            foreach (var call in new[] { 1, 2 })
            {
                var unfinished = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
                Assert.Equal("null", unfinished.Median);
                Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetUnfinishedNote, unfinished.Note);
                Assert.Equal(1, Volatile.Read(ref computations));
            }

            clock.Advance(TimeSpan.FromHours(1));
            await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.Equal(2, Volatile.Read(ref computations));

            /* any other failure is the call's own, and the next call reads the store */
            clock.Advance(TimeSpan.FromHours(1));
            QueryStoreClutterFleetReference.ComputationSeam.Value = _ => throw new InvalidOperationException("planted");
            var failed = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: 24, include_fleet_median: true, as_of: asOf);
            Assert.True(McpHelpers.IsErrorEnvelope(failed), failed);
            QueryStoreClutterFleetReference.ComputationSeam.Value = null;
            Assert.NotEqual("null", (await FleetOfAsync(postgres, asOf)).Median);
        }
        finally
        {
            ClearFleetSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /// <summary>
    /// #9882: one fleet computation runs at a time per store. The computation seam holds the 24-hour computation in flight. A call for
    /// another <c>hours_back</c> has its whole budget left and answers while the computation is still held, with the per-server rows, a
    /// null median and the busy note, and starts nothing. The same holds once the clock hour has turned under the held computation.
    /// After the release that call computes its own reference.
    /// </summary>
    [Fact]
    public async Task WhileOneFleetReferenceIsComputed_ACallForAnotherWindow_AnswersAtOnce_AndStartsNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var computations = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        QueryStoreClutterFleetReference.Clock.Value = clock;
        QueryStoreClutterFleetReference.ComputationSeam.Value = async cap =>
        {
            Interlocked.Increment(ref computations);
            entered.TrySetResult();
            await release.Task.WaitAsync(cap.Token);
        };
        try
        {
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = fleetBudget =>
            {
                fleetBudget.Cancel();
                return Task.CompletedTask;
            };
            var held = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, held.Note);
            await entered.Task.WaitAsync(HeldCallGivesUp, ct);

            /* no seam fires this call's budget: it answers because it starts nothing, not because it waited a budget out */
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = null;
            var other = await FleetOfAsync(postgres, asOf, hoursBack: 23).WaitAsync(HeldCallGivesUp, ct);
            Assert.False(release.Task.IsCompleted);
            Assert.Equal("null", other.Median);
            Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetBusyNote, other.Note);

            /* 15:00 has passed under the held computation: it still is the store's one */
            clock.Advance(TimeSpan.FromHours(1));
            var nextHour = await FleetOfAsync(postgres, asOf, hoursBack: 23).WaitAsync(HeldCallGivesUp, ct);
            Assert.False(release.Task.IsCompleted);
            Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetBusyNote, nextHour.Note);
            Assert.Equal(1, Volatile.Read(ref computations));

            release.SetResult();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
            QueryStoreClutterFleetReference.ComputationSeam.Value = _ =>
            {
                Interlocked.Increment(ref computations);
                return Task.CompletedTask;
            };
            Assert.NotEqual("null", (await FleetOfAsync(postgres, asOf, hoursBack: 23)).Median);
            Assert.Equal(2, Volatile.Read(ref computations));
        }
        finally
        {
            release.TrySetResult();
            ClearFleetSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /// <summary>
    /// #9882: the computation runs outside the read scope of the call that starts it, because it can outlive that call and the
    /// call's record is written when the call answers. The call runs inside a scope, the computation seam notes the scope the
    /// computation runs in, and that is a scope of its own, opened outside every other. The call's own flow keeps its scope.
    /// </summary>
    [Fact]
    public async Task TheFleetComputation_RunsOutsideTheStartingCallsReadScope()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        using var opened = ReadScope.Open(null);
        /* starts as the call's scope, so a seam that never ran cannot pass for "none" */
        ReadScope? computationScope = opened.Scope;
        var computations = 0;
        QueryStoreClutterFleetReference.ComputationSeam.Value = _ =>
        {
            Volatile.Write(ref computationScope, ReadScope.Current);
            Interlocked.Increment(ref computations);
            return Task.CompletedTask;
        };
        try
        {
            Assert.NotEqual("null", (await FleetOfAsync(postgres, asOf)).Median);
            Assert.Equal(1, Volatile.Read(ref computations));
            var own = Volatile.Read(ref computationScope);
            Assert.NotNull(own);
            Assert.NotSame(opened.Scope, own);
            Assert.True(own.IsOutermost, "the computation's scope is nested in the starting call's");
            Assert.Same(opened.Scope, ReadScope.Current);
        }
        finally
        {
            ClearFleetSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /// <summary>
    /// #9882: the fleet computation is a read of its own in the slow-read record. Each call runs in a scope whose recorder names a
    /// slow-read log, as the MCP filter's does. The computation seam moves the clock, so "as long as the threshold" is set, never
    /// waited for. A cheap computation writes no row. One that took as long as the threshold writes one row under the computation's
    /// own route, as the starting call's caller, with the window it read and the statements it ran. The starting call's scope holds
    /// the same statements with the fleet arm as without it, so none of the fleet statements are in the call's row. A scope also
    /// holds an entry with no text for each physical connection its call opens, and whether a call opens one depends on where the
    /// pool stands, so the comparison is of the statements that have text.
    /// </summary>
    [Fact]
    public async Task AFleetComputationOverTheSlowReadBar_WritesOneRowUnderItsOwnRoute_AndACheapOneWritesNone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);
        ReadStatementCapture.Register();

        const string caller = "the starting caller";
        var slowReads = new SlowReadLog();
        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var took = TimeSpan.Zero;
        QueryStoreClutterFleetReference.Clock.Value = clock;
        QueryStoreClutterFleetReference.ComputationSeam.Value = _ =>
        {
            clock.Advance(took);
            return Task.CompletedTask;
        };

        /* One call in a recorder's scope: the statements with text that the call's own scope saw, as hash and label in a fixed order.
           The scope's other entries have no text: Npgsql's activity for a physical connection the call opened. */
        async Task<string[]> CallAsync(bool fleet)
        {
            using var opened = ReadScope.Open(null);
            opened.Scope.CaptureStatements = true;
            opened.Scope.RecordsTo(slowReads, ReadSurface.Mcp, caller);
            var json = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: 24, include_fleet_median: fleet, as_of: asOf);
            Assert.False(McpHelpers.IsErrorEnvelope(json), json);
            using var doc = JsonDocument.Parse(json);
            Assert.Equal(fleet, doc.RootElement.GetProperty("fleet_median").ValueKind == JsonValueKind.Object);
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
            var seen = opened.Scope.Snapshot();
            /* the scope keeps every entry it counted, so the list below hides none of them */
            Assert.Equal(opened.Scope.StatementTotal, seen.Count);
            var noText = StatementLabel.Hash(string.Empty);
            return seen.Where(t => t.Hash != noText).Select(t => $"{t.Hash} {t.Label}").Order(StringComparer.Ordinal).ToArray();
        }

        try
        {
            /* the first call of each kind opens the pool's connections and fills what a data source reads once, so neither is the one compared */
            await CallAsync(fleet: true);
            await CallAsync(fleet: false);
            var withoutFleet = await CallAsync(fleet: false);
            Assert.NotEmpty(withoutFleet);

            /* cheap: the clock did not move, so the computation is under the threshold and writes nothing */
            Assert.Equal(withoutFleet, await CallAsync(fleet: true));
            Assert.False(slowReads.TryRead(out _), "a cheap computation wrote a slow-read row");

            /* as long as the threshold: one row, the computation's own */
            took = TimeSpan.FromMilliseconds(slowReads.ThresholdMs);
            Assert.Equal(withoutFleet, await CallAsync(fleet: true));
            Assert.True(slowReads.TryRead(out var row), "a computation as long as the threshold wrote no slow-read row");
            Assert.Equal(QueryStoreClutterFleetReference.Route, row!.Route);
            Assert.Equal("mcp", row.Surface);
            Assert.Equal(slowReads.ThresholdMs, row.TotalMs);
            Assert.Null(row.ErrorClass);
            Assert.Equal(caller, row.Caller);
            /* the five fleet reads at the least: the targets, read cost, plan churn, config and waits */
            Assert.True(row.StatementCount >= 5, $"the row holds {row.StatementCount} statements");
            using (var statements = JsonDocument.Parse(row.StatementsJson))
            {
                Assert.Equal(Math.Min(row.StatementCount, SlowReadLog.MaxStatementsStored), statements.RootElement.GetArrayLength());
            }
            Assert.NotNull(row.WindowStart);
            Assert.NotNull(row.WindowEnd);
            Assert.Equal(TimeSpan.FromHours(24), row.WindowEnd!.Value - row.WindowStart!.Value);
            Assert.Contains("\"hours_back\":24", row.ArgumentsJson);
            Assert.False(slowReads.TryRead(out _), "one computation wrote more than one slow-read row");
        }
        finally
        {
            ClearFleetSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /* ─────────────────────────── #9958: the wait is what is left of the read bar ─────────────────────────── */

    /// <summary>
    /// #9958: the wait the call gives the fleet computation is what is left of the 5 s read bar after the time the call has used, and
    /// never less than the 1 s floor. Whole milliseconds in and out, so no figure here is rounded.
    /// </summary>
    [Theory]
    [InlineData(0, 5000)]
    [InlineData(1000, 4000)]
    [InlineData(2500, 2500)]
    [InlineData(3999, 1001)]
    [InlineData(4000, 1000)]
    [InlineData(4500, 1000)]
    [InlineData(5000, 1000)]
    [InlineData(30000, 1000)]
    public void TheFleetWait_IsWhatIsLeftOfTheReadBar_AndNeverUnderTheFloor(int elapsedMs, int expectedWaitMs)
    {
        Assert.Equal(TimeSpan.FromMilliseconds(expectedWaitMs), DarlingMcpQueryStoreClutterTools.FleetWait(TimeSpan.FromMilliseconds(elapsedMs)));
    }

    /// <summary>The call's answer without its two fleet fields, as JSON text: the per-server figures, which the fleet arm must not change.</summary>
    private static async Task<string> PerServerOfAsync(NpgsqlDataSource postgres, string asOf, bool includeFleetMedian)
    {
        var json = await DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(postgres, PrimaryName, hours_back: 24, include_fleet_median: includeFleetMedian, as_of: asOf);
        Assert.False(McpHelpers.IsErrorEnvelope(json), json);
        var root = System.Text.Json.Nodes.JsonNode.Parse(json)!.AsObject();
        Assert.True(root["database_count"]!.GetValue<int>() > 0, json);
        root.Remove("fleet_median");
        root.Remove("fleet_median_note");
        return root.ToJsonString();
    }

    /// <summary>Stands the per-server reads' time on the manual clock: the first of the call's three independent reads moves it by <paramref name="used"/>.</summary>
    private static void PerServerReadsTake(ManualClock clock, TimeSpan used)
    {
        DarlingMcpQueryStoreClutterTools.IndependentReadSeam.Value = (name, _) =>
        {
            if (name == "waits") clock.Advance(used);
            return Task.CompletedTask;
        };
    }

    private static void ClearWaitSeams()
    {
        ClearFleetSeams();
        DarlingMcpQueryStoreClutterTools.FleetWaitSeam.Value = null;
        DarlingMcpQueryStoreClutterTools.IndependentReadSeam.Value = null;
    }

    /// <summary>
    /// #9958: a held fleet computation. The per-server reads use 3 s of the bar on the manual clock, so the call gives the computation
    /// the 2 s that are left, and the seam that fires the bound stands for those 2 s being spent. The first call answers with the
    /// overrun note and the same per-server figures as a call with the fleet arm off. The computation goes on. Once it ends, having
    /// run over the read bar, a later call in the clock hour gets its reference with no second computation.
    /// </summary>
    [Fact]
    public async Task AHeldFleetReference_AnswersTheFirstCallInsideTheBar_AndALaterCallInTheHourGetsTheKeptReference()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var computations = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var waits = new System.Collections.Concurrent.ConcurrentQueue<TimeSpan>();
        QueryStoreClutterFleetReference.Clock.Value = clock;
        QueryStoreClutterFleetReference.ComputationSeam.Value = async cap =>
        {
            Interlocked.Increment(ref computations);
            entered.TrySetResult();
            await release.Task.WaitAsync(cap.Token);
        };
        PerServerReadsTake(clock, TimeSpan.FromSeconds(3));
        DarlingMcpQueryStoreClutterTools.FleetWaitSeam.Value = waits.Enqueue;
        try
        {
            var withoutFleet = await PerServerOfAsync(postgres, asOf, includeFleetMedian: false);
            Assert.Empty(waits);

            /* the 2 s that are left are spent: the bound fires with the computation still held */
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = fleetBudget =>
            {
                fleetBudget.Cancel();
                return Task.CompletedTask;
            };
            var first = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.Equal("null", first.Median);
            Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, first.Note);
            Assert.Equal(TimeSpan.FromSeconds(2), Assert.Single(waits));
            Assert.Equal(withoutFleet, await PerServerOfAsync(postgres, asOf, includeFleetMedian: true));
            await entered.Task.WaitAsync(HeldCallGivesUp, ct);
            Assert.False(release.Task.IsCompleted);

            /* the computation ran longer than the read bar when it ends, so it is kept for the clock hour */
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = null;
            clock.Advance(TimeSpan.FromSeconds(DarlingMcpQueryStoreClutterTools.ReadBarSeconds + 1));
            release.SetResult();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);

            var later = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.NotEqual("null", later.Median);
            Assert.NotEqual(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, later.Note);
            Assert.Contains("computed at", later.Note, StringComparison.Ordinal);
            Assert.Equal(1, Volatile.Read(ref computations));
            Assert.Equal(withoutFleet, await PerServerOfAsync(postgres, asOf, includeFleetMedian: true));
        }
        finally
        {
            release.TrySetResult();
            ClearWaitSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /// <summary>
    /// #9958: a fleet computation that ends inside what is left of the bar is in the first answer. The per-server reads use 1 s of the
    /// bar on the manual clock, the call gives the computation the 4 s that are left, and nothing fires the bound or holds the
    /// computation: the reference is there because the computation ended first. The per-server figures are the same with the fleet
    /// arm off.
    /// </summary>
    [Fact]
    public async Task AFleetReferenceThatEndsInsideWhatIsLeftOfTheBar_IsInTheFirstAnswer()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var waits = new System.Collections.Concurrent.ConcurrentQueue<TimeSpan>();
        QueryStoreClutterFleetReference.Clock.Value = clock;
        PerServerReadsTake(clock, TimeSpan.FromSeconds(1));
        DarlingMcpQueryStoreClutterTools.FleetWaitSeam.Value = waits.Enqueue;
        try
        {
            var withoutFleet = await PerServerOfAsync(postgres, asOf, includeFleetMedian: false);
            var first = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.NotEqual("null", first.Median);
            Assert.NotEqual(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, first.Note);
            Assert.StartsWith("Discrete medians", first.Note, StringComparison.Ordinal);
            Assert.Equal(TimeSpan.FromSeconds(4), Assert.Single(waits));
            Assert.Equal(withoutFleet, await PerServerOfAsync(postgres, asOf, includeFleetMedian: true));
        }
        finally
        {
            ClearWaitSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /// <summary>
    /// #9958: a reference that took under the read bar but that its call stopped waiting for is kept for the clock hour. The per-server
    /// reads use 3 s of the bar on the manual clock, so the call gives the computation the 2 s that are left, and the seam that fires the
    /// bound stands for those 2 s being spent with the computation held. The computation ends at an age of 3 s, under the bar. The first
    /// call answered with the overrun note, so a later call in the clock hour gets the kept reference ("computed at", took 3.0 s) with no
    /// second computation. A store whose per-server reads take that long would otherwise overrun on every call and never get a reference.
    /// </summary>
    [Fact]
    public async Task AFleetReferenceUnderTheBar_ThatItsCallStoppedWaitingFor_IsKeptForTheClockHour()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var computations = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var waits = new System.Collections.Concurrent.ConcurrentQueue<TimeSpan>();
        QueryStoreClutterFleetReference.Clock.Value = clock;
        QueryStoreClutterFleetReference.ComputationSeam.Value = async cap =>
        {
            Interlocked.Increment(ref computations);
            entered.TrySetResult();
            await release.Task.WaitAsync(cap.Token);
        };
        PerServerReadsTake(clock, TimeSpan.FromSeconds(3));
        DarlingMcpQueryStoreClutterTools.FleetWaitSeam.Value = waits.Enqueue;
        try
        {
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = fleetBudget =>
            {
                fleetBudget.Cancel();
                return Task.CompletedTask;
            };
            var first = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.Equal("null", first.Median);
            Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, first.Note);
            Assert.Equal(TimeSpan.FromSeconds(2), Assert.Single(waits));
            await entered.Task.WaitAsync(HeldCallGivesUp, ct);

            /* the computation ends at an age of 3 s, under the 5 s bar: it is not dear, and the call that stopped waiting keeps it */
            DarlingMcpQueryStoreClutterTools.FleetReadSeam.Value = null;
            clock.Advance(TimeSpan.FromSeconds(3));
            release.SetResult();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);

            var later = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.NotEqual("null", later.Median);
            Assert.NotEqual(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, later.Note);
            Assert.Contains("computed at", later.Note, StringComparison.Ordinal);
            Assert.Contains("took 3.0 s", later.Note, StringComparison.Ordinal);
            Assert.Equal(1, Volatile.Read(ref computations));
        }
        finally
        {
            release.TrySetResult();
            ClearWaitSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /// <summary>
    /// #9958: a reference that took under the read bar and that every call that waited for it received is dropped as it finishes. The
    /// per-server reads use 1 s of the bar on the manual clock, the call gives the computation the 4 s that are left, and the
    /// computation takes 1 s on the manual clock with nothing firing the bound. Both calls get a reference, and the second one is a
    /// computation of its own, not a kept one.
    /// </summary>
    [Fact]
    public async Task AFleetReferenceUnderTheBar_ThatEveryCallReceived_IsDropped_AndTheNextCallComputesAgain()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var computations = 0;
        QueryStoreClutterFleetReference.Clock.Value = clock;
        QueryStoreClutterFleetReference.ComputationSeam.Value = _ =>
        {
            Interlocked.Increment(ref computations);
            clock.Advance(TimeSpan.FromSeconds(1));
            return Task.CompletedTask;
        };
        PerServerReadsTake(clock, TimeSpan.FromSeconds(1));
        try
        {
            var first = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.NotEqual("null", first.Median);
            Assert.StartsWith("Discrete medians", first.Note, StringComparison.Ordinal);
            Assert.Equal(1, Volatile.Read(ref computations));

            var second = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.Equal(first.Median, second.Median);
            Assert.StartsWith("Discrete medians", second.Note, StringComparison.Ordinal);
            Assert.DoesNotContain("computed at", second.Note, StringComparison.Ordinal);
            Assert.Equal(2, Volatile.Read(ref computations));
        }
        finally
        {
            ClearWaitSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
        }
    }

    /// <summary>
    /// #9958: a call that arrives when the computation is already older than its own wait answers at once with the overrun note, and
    /// the reference is kept for the clock hour even when it ends under the read bar. Call A has no per-server time and waits for the
    /// whole bar; call B arrives at a computation 3 s old with 3 s of its own per-server reads, so its 2 s wait is over before it
    /// starts. Once the computation ends at an age of 3 s, a call C gets the kept reference with no second computation.
    /// </summary>
    [Fact]
    public async Task ACallThatArrivesAfterItsWaitWasOver_KeepsTheReference_ThatEndsUnderTheBar()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_PG")),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Query Store clutter test (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connectionString, asOf) = await PlantedStoreAsync(ct);
        await using var dropScratch = scratch;
        await using var postgres = NpgsqlDataSource.Create(connectionString);

        var clock = new ManualClock(new DateTime(2026, 3, 10, 14, 10, 0, DateTimeKind.Utc));
        var computations = 0;
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        QueryStoreClutterFleetReference.Clock.Value = clock;
        QueryStoreClutterFleetReference.ComputationSeam.Value = async cap =>
        {
            Interlocked.Increment(ref computations);
            entered.TrySetResult();
            await release.Task.WaitAsync(cap.Token);
        };
        Task? callA = null;
        try
        {
            callA = FleetOfAsync(postgres, asOf);
            await entered.Task.WaitAsync(HeldCallGivesUp, ct);

            /* B's per-server reads take 3 s, and the computation is then 3 s old: past B's 2 s wait */
            PerServerReadsTake(clock, TimeSpan.FromSeconds(3));
            var b = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.Equal("null", b.Median);
            Assert.Equal(DarlingMcpQueryStoreClutterTools.FleetOverrunNote, b.Note);
            Assert.False(release.Task.IsCompleted);

            release.SetResult();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
            await callA.WaitAsync(HeldCallGivesUp, ct);

            DarlingMcpQueryStoreClutterTools.IndependentReadSeam.Value = null;
            var c = await FleetOfAsync(postgres, asOf).WaitAsync(HeldCallGivesUp, ct);
            Assert.NotEqual("null", c.Median);
            Assert.Contains("computed at", c.Note, StringComparison.Ordinal);
            Assert.Contains("took 3.0 s", c.Note, StringComparison.Ordinal);
            Assert.Equal(1, Volatile.Read(ref computations));
        }
        finally
        {
            release.TrySetResult();
            ClearWaitSeams();
            await QueryStoreClutterFleetReference.WhenIdleAsync(postgres);
            if (callA is not null)
            {
                try { await callA; }
                catch (Exception) { /* the call's own failure is not this test's subject */ }
            }
        }
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
           the engine's readable-secondary bit.

           The V137 (#3796) capture mode is planted THREE ways on purpose, because the column's three states
           are three different facts: alpha's newest capture says ALL (the plan-churn factory) while its OLDER
           capture is a pre-rung NULL, which is also how the newest-wins rule is proved on this column and not
           only on the storage figure; beta and gamma say AUTO; and `off` is left pre-rung NULL, which is what
           makes the SERVER-level capture_mode_known false while every other row's is true. */
        foreach (var db in new[] { Alpha, Beta, Gamma })
        {
            await PlantHealthAsync(c, PrimaryId, PrimaryName, db, "READ_WRITE", 0, 2048, anchor.AddMinutes(-90), ct, captureMode: null);
            await PlantHealthAsync(c, PrimaryId, PrimaryName, db, "READ_WRITE", 0, 4096, anchor.AddMinutes(-30), ct, captureMode: db == Alpha ? "ALL" : "AUTO");
        }

        await PlantHealthAsync(c, PrimaryId, PrimaryName, Off, "OFF", 0, 0, anchor.AddMinutes(-30), ct, captureMode: null);
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

    /// <param name="captureMode">The V137 (#3796) column. NULL plants a PRE-RUNG row — the state a store
    /// upgraded yesterday is really in — so the null path is exercised by a row that has one rather than by
    /// a column nobody wrote to.</param>
    private static Task PlantHealthAsync(NpgsqlConnection c, int serverId, string serverName, string db, string actual, int readonlyReason, long currentMb, DateTime at, CancellationToken ct, string? captureMode = "AUTO") =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode)
VALUES ($1, $2, $3, $4, $5, $6, 'READ_WRITE', $7, $8, 8192, 'AUTO', 21, 200, 60, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(at), serverId, serverName, db, actual, readonlyReason, currentMb, captureMode);

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
