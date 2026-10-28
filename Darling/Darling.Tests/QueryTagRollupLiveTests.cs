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

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one. */

/// <summary>
/// <c>get_resource_use_by_tag</c> (#5983, wave 2 of #5971) on a real store, with <c>config.query_tag_map</c> seeded directly (the
/// map builder is #5985) and the fact rows seeded into the collect tables. One test per behaviour.
/// </summary>
public sealed class QueryTagRollupLiveTests
{
    private const int ServerOne = 7301;
    private const int ServerTwo = 7302;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed record Ctx(NpgsqlConnection Store, NpgsqlDataSource Source, CancellationToken Ct);

    private static async Task WithStoreAsync(Func<Ctx, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the query-tag roll-up pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        var connectionString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var store = new NpgsqlConnection(connectionString);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);
        await using var source = NpgsqlDataSource.Create(connectionString);
        foreach (var (id, name) in new[] { (ServerOne, "srv-one"), (ServerTwo, "srv-two") })
        {
            await ExecAsync(store, $"INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ({id}, '{name}', '{name}', TRUE, 15, now(), now())", ct);
        }

        var bodySucceeded = false;
        try
        {
            await body(new Ctx(store, source, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object?[] args)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = arg ?? DBNull.Value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> TagAsync(Ctx c, string name, int? parent = null)
    {
        await using var command = new NpgsqlCommand("INSERT INTO config.query_tags (name, parent_id) VALUES ($1, $2) RETURNING id", c.Store);
        command.Parameters.Add(new NpgsqlParameter { Value = name });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)parent ?? DBNull.Value });
        return Convert.ToInt32(await command.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture);
    }

    private static Task MapAsync(Ctx c, int server, string kind, string key, int tagId) =>
        ExecAsync(c.Store, "INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source) VALUES ($1, $2, $3, $4, 'hand_hash')", c.Ct, server, kind, key, tagId);

    private static Task PlanCacheAsync(Ctx c, int server, string hash, double minutesAgo, long cpuUs, long executions = 1) =>
        ExecAsync(c.Store,
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, delta_execution_count, delta_elapsed_time, delta_worker_time, delta_logical_reads, delta_logical_writes) VALUES ($1, $2, $3, 'x', 'db1', $4, $5, $6, $6, 10, 1)",
            c.Ct, CollectionIdGenerator.Next(), DateTime.UtcNow.AddMinutes(-minutesAgo), server, hash, executions, cpuUs);

    private static Task QueryStoreAsync(Ctx c, int server, string hash, long queryId, long avgCpuUs, long executions) =>
        ExecAsync(c.Store,
            "INSERT INTO query_store_interval_wide (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, max_query_max_used_memory, runtime_stats_interval_id, interval_start_time_utc) VALUES ($1, $2, 'db1', $3, $3, 'Regular', $4, $5, $6, $7, $7, 5, 1, 64, $3, $4)",
            c.Ct, DateTime.UtcNow.AddMinutes(-30), server, queryId, DateTime.UtcNow.AddMinutes(-40), hash, executions, avgCpuUs);

    /* The Query Store gate (QueryStoreIntervalWide.ResolveReadAsync) needs a coverage row that reaches the window's start. */
    private static async Task CoverQueryStoreAsync(Ctx c, int server)
    {
        /* A plain store has no raw chunk floor, so the gate also wants the table's own floor to sit a purge margin (26 h) below the window's
           start: one interval from three days back, outside the 24-hour window and never counted in it. */
        await ExecAsync(c.Store,
            "INSERT INTO query_store_interval_wide (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, max_query_max_used_memory, runtime_stats_interval_id, interval_start_time_utc) VALUES ($1, $2, 'db1', 99, 99, 'Regular', $1, '0xOLD', 1, 1, 1, 1, 1, 1, 99, $1)",
            c.Ct, DateTime.UtcNow.AddHours(-72), server);
        await ExecAsync(c.Store,
            "INSERT INTO query_store_interval_wide_coverage (server_id, filled_since, applied_through) VALUES ($1, $2, $3)",
            c.Ct, server, DateTime.UtcNow.AddHours(-30), DateTime.UtcNow.AddMinutes(-1));
    }

    private static Task DeadlockWithoutGraphAsync(Ctx c, int server) =>
        ExecAsync(c.Store,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1, $2, $3, 'x', $2, NULL, 'db1')",
            c.Ct, CollectionIdGenerator.Next(), DateTime.UtcNow.AddMinutes(-15), server);

    private static string Caller(string app, string login, string host, string db) => QueryTagRollupSql.CallerKey(app, login, host, db);

    private static Task BlockedAsync(Ctx c, int server, string blockedApp, string blockingApp, long waitMs) =>
        ExecAsync(c.Store,
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms, blocking_spid, blocked_spid, blocking_status, database_name, blocked_client_app, blocked_login_name, blocked_host_name, blocking_client_app, blocking_login_name, blocking_host_name) VALUES ($1, $2, $3, 'x', $2, $4, 60, 70, 'suspended', 'db1', $5, 'lg', 'h', $6, 'lg', 'h')",
            c.Ct, CollectionIdGenerator.Next(), DateTime.UtcNow.AddMinutes(-20), server, waitMs, blockedApp, blockingApp);

    private static Task DeadlockAsync(Ctx c, int server, params string[] apps) =>
        ExecAsync(c.Store,
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, deadlock_graph_xml, database_name) VALUES ($1, $2, $3, 'x', $2, $4, 'db1')",
            c.Ct, CollectionIdGenerator.Next(), DateTime.UtcNow.AddMinutes(-15), server,
            "<deadlock><process-list>" + string.Concat(apps.Select((a, i) => $"<process id=\"p{i}\" clientapp=\"{a}\" loginname=\"lg\" hostname=\"h\" currentdbname=\"db1\" />")) + "</process-list></deadlock>");

    private static async Task<JsonDocument> ReadAsync(Ctx c, string? server, int hours = 24, string? tag = null, int limit = 25) =>
        JsonDocument.Parse(await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(c.Source, server, hours, tag, limit, null, null, c.Ct));

    private static JsonElement Section(JsonDocument doc, string name) =>
        doc.RootElement.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == name);

    private static JsonElement Row(JsonElement section, string tag) =>
        section.GetProperty("tag_rows").EnumerateArray().Single(r => r.GetProperty("tag").GetString() == tag);

    private static double Figure(JsonElement figures, string measure) => figures.GetProperty(measure).GetDouble();

    [Fact]
    public async Task TwoKeysTaggedA_AndOneUntagged_ShowATheUntaggedRowAndATotalThatIsTheirSum()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            await MapAsync(c, ServerOne, "hash", "0xA1", a);
            await MapAsync(c, ServerOne, "hash", "0xA2", a);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 4_000_000, 2);
            await PlanCacheAsync(c, ServerOne, "0xA2", 30, 6_000_000, 3);
            await PlanCacheAsync(c, ServerOne, "0xFF", 30, 1_000_000, 1);

            using var doc = await ReadAsync(c, "srv-one");
            var plan = Section(doc, "plan_cache");
            Assert.Equal(10_000, Figure(Row(plan, "A").GetProperty("own"), "cpu_ms"));
            Assert.Equal(5, Figure(Row(plan, "A").GetProperty("own"), "executions"));
            Assert.Equal(1_000, Figure(plan.GetProperty("untagged"), "cpu_ms"));
            Assert.Equal(11_000, Figure(plan.GetProperty("total"), "cpu_ms"));
        });
    }

    /// <summary>#6860: a server whose display name differs from its connection key is named by the display name in the answer's
    /// <c>server</c> field and in the not_collected sentence (#6179), not by the key, whether the caller asked by key or by display name.</summary>
    [Fact]
    public async Task ADisplayNamedServer_IsAnsweredByItsDisplayName_NotItsConnectionKey()
    {
        await WithStoreAsync(async c =>
        {
            await ExecAsync(c.Store, "UPDATE servers SET display_name = 'Orders Primary' WHERE server_id = $1", c.Ct, ServerOne);
            await ExecAsync(c.Store, "UPDATE servers SET display_name = 'Orders Reporting Postgres', engine_kind = 'postgres' WHERE server_id = $1", c.Ct, ServerTwo);

            var a = await TagAsync(c, "A");
            await MapAsync(c, ServerOne, "hash", "0xA1", a);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 4_000_000, 7);
            await CoverQueryStoreAsync(c, ServerOne);
            foreach (var asked in new[] { "srv-one", "Orders Primary" })
            {
                using var named = await ReadAsync(c, asked);
                Assert.Equal("Orders Primary", named.RootElement.GetProperty("server").GetString());
                Assert.DoesNotContain("srv-one", named.RootElement.GetRawText(), StringComparison.Ordinal);
            }

            foreach (var asked in new[] { "srv-two", "Orders Reporting Postgres" })
            {
                using var unmet = await ReadAsync(c, asked);
                Assert.Equal("not_collected", unmet.RootElement.GetProperty("status").GetString());
                Assert.Contains("Orders Reporting Postgres", unmet.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
                Assert.DoesNotContain("srv-two", unmet.RootElement.GetRawText(), StringComparison.Ordinal);
            }
        });
    }

    [Fact]
    public async Task EachSourceHasItsOwnSection_AndTheSectionsAreNeverSummed()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            await MapAsync(c, ServerOne, "hash", "0xA1", a);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 4_000_000, 7);
            await QueryStoreAsync(c, ServerOne, "0xA1", 1, 4_000_000, 7);
            await CoverQueryStoreAsync(c, ServerOne);

            using var doc = await ReadAsync(c, "srv-one");
            Assert.False(doc.RootElement.GetProperty("sections_additive").GetBoolean());
            Assert.False(doc.RootElement.TryGetProperty("total", out _));
            var plan = Section(doc, "plan_cache");
            var qs = Section(doc, "query_store");
            Assert.Equal(7, Figure(plan.GetProperty("total"), "executions"));
            Assert.Equal(7, Figure(qs.GetProperty("total"), "executions"));
            Assert.Equal(4_000 * 7, Figure(qs.GetProperty("total"), "cpu_ms"));
            Assert.Equal(64, Figure(Row(qs, "A").GetProperty("own"), "peak_memory_pages"));
            Assert.Contains("query_stats", plan.GetProperty("source").GetString(), StringComparison.Ordinal);
            Assert.Contains("query_store_interval_wide", qs.GetProperty("source").GetString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task AParentsSubtreeFigureIncludesItsChild_ButItsOwnFigureDoesNot()
    {
        await WithStoreAsync(async c =>
        {
            var parent = await TagAsync(c, "Billing");
            var child = await TagAsync(c, "Invoices", parent);
            await MapAsync(c, ServerOne, "hash", "0xA1", parent);
            await MapAsync(c, ServerOne, "hash", "0xA2", child);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 3_000_000);
            await PlanCacheAsync(c, ServerOne, "0xA2", 30, 5_000_000);

            using var doc = await ReadAsync(c, "srv-one");
            var plan = Section(doc, "plan_cache");
            var billing = Row(plan, "Billing");
            Assert.Equal(3_000, Figure(billing.GetProperty("own"), "cpu_ms"));
            Assert.Equal(8_000, Figure(billing.GetProperty("subtree"), "cpu_ms"));
            Assert.Equal(5_000, Figure(Row(plan, "Invoices").GetProperty("subtree"), "cpu_ms"));
            Assert.Equal(8_000, Figure(plan.GetProperty("total"), "cpu_ms"));
        });
    }

    [Fact]
    public async Task AKeyTaggedOnServerOneOnly_IsUntaggedOnServerTwo()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            await MapAsync(c, ServerOne, "hash", "0xA1", a);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 2_000_000);
            await PlanCacheAsync(c, ServerTwo, "0xA1", 30, 2_000_000);

            using var one = await ReadAsync(c, "srv-one");
            using var two = await ReadAsync(c, "srv-two");
            Assert.Equal(2_000, Figure(Row(Section(one, "plan_cache"), "A").GetProperty("own"), "cpu_ms"));
            Assert.Equal(0, Section(two, "plan_cache").GetProperty("tag_rows").GetArrayLength());
            Assert.Equal(2_000, Figure(Section(two, "plan_cache").GetProperty("untagged"), "cpu_ms"));
        });
    }

    [Fact]
    public async Task BlockingTimeLandsOnTheBlockedSidesTag_AndTimeCausedOnTheBlockingSidesTag()
    {
        await WithStoreAsync(async c =>
        {
            var victim = await TagAsync(c, "Reports");
            var culprit = await TagAsync(c, "Etl");
            await MapAsync(c, ServerOne, "caller", Caller("reportApp", "lg", "h", "db1"), victim);
            await MapAsync(c, ServerOne, "caller", Caller("etlApp", "lg", "h", "db1"), culprit);
            await BlockedAsync(c, ServerOne, "reportApp", "etlApp", 12_000);

            using var doc = await ReadAsync(c, "srv-one");
            var blocking = Section(doc, "blocking_reports");
            Assert.Equal(12_000, Figure(Row(blocking, "Reports").GetProperty("own"), "blocked_ms"));
            Assert.Equal(0, Figure(Row(blocking, "Reports").GetProperty("own"), "caused_ms"));
            Assert.Equal(12_000, Figure(Row(blocking, "Etl").GetProperty("own"), "caused_ms"));
            Assert.Equal(0, Figure(Row(blocking, "Etl").GetProperty("own"), "blocked_ms"));
            Assert.Equal(12_000, Figure(blocking.GetProperty("total"), "blocked_ms"));
        });
    }

    /// <summary>The interval table is read only behind the gate every other reader applies; with no coverage row the section is unavailable
    /// (raw may hold the work), never empty, and nothing is read from the table.</summary>
    [Fact]
    public async Task AStoreWithNoQueryStoreCoverageRow_ReportsTheSectionUnavailable_NotEmpty()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            await MapAsync(c, ServerOne, "hash", "0xA1", a);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 4_000_000, 7);
            await QueryStoreAsync(c, ServerOne, "0xA1", 1, 4_000_000, 7);

            using var doc = await ReadAsync(c, "srv-one");
            var qs = Section(doc, "query_store");
            Assert.Equal("unavailable", qs.GetProperty("status").GetString());
            Assert.Contains("not readable here", qs.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal(0, Figure(qs.GetProperty("total"), "executions"));
            Assert.Equal("ok", Section(doc, "plan_cache").GetProperty("status").GetString());
        });
    }

    /// <summary>A deadlock whose graph is NULL still counts in the section's total, with unparsed_graphs beside it, in the fleet line too.</summary>
    [Fact]
    public async Task ADeadlockWithANullGraph_CountsInTheTotal_WithAnUnparsedGraphsCount()
    {
        await WithStoreAsync(async c =>
        {
            await DeadlockWithoutGraphAsync(c, ServerOne);

            using var doc = await ReadAsync(c, "srv-one");
            var deadlocks = Section(doc, "deadlocks");
            Assert.Equal("ok", deadlocks.GetProperty("status").GetString());
            Assert.Equal(1, Figure(deadlocks.GetProperty("total"), "deadlocks"));
            Assert.Equal(0, Figure(deadlocks.GetProperty("total"), "processes"));
            Assert.Equal(1, deadlocks.GetProperty("unparsed_graphs").GetInt32());

            using var fleet = await ReadAsync(c, null);
            var fleetDeadlocks = Section(fleet, "deadlocks");
            Assert.Equal(1, Figure(fleetDeadlocks.GetProperty("fleet_total"), "deadlocks"));
            Assert.Equal(1, fleetDeadlocks.GetProperty("unparsed_graphs").GetInt32());
        });
    }

    /// <summary>Under a tag filter an empty section carries the keys a section with rows carries: no untagged, and total_scope.</summary>
    [Fact]
    public async Task UnderATagFilter_AnEmptySection_CarriesTotalScope_AndNoUntagged()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            await MapAsync(c, ServerOne, "hash", "0xA1", a);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 4_000_000, 7);

            using var doc = await ReadAsync(c, "srv-one", tag: "A");
            var blocking = Section(doc, "blocking_reports");
            Assert.Equal("empty", blocking.GetProperty("status").GetString());
            Assert.False(blocking.TryGetProperty("untagged", out _));
            Assert.Equal("tag_subtree", blocking.GetProperty("total_scope").GetString());
        });
    }

    [Fact]
    public async Task ADeadlockBetweenTagsAAndB_CountsOnceForAAndOnceForB()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            var b = await TagAsync(c, "B");
            await MapAsync(c, ServerOne, "caller", Caller("appA", "lg", "h", "db1"), a);
            await MapAsync(c, ServerOne, "caller", Caller("appB", "lg", "h", "db1"), b);
            await DeadlockAsync(c, ServerOne, "appA", "appB");

            using var doc = await ReadAsync(c, "srv-one");
            var deadlocks = Section(doc, "deadlocks");
            Assert.Equal(1, Figure(Row(deadlocks, "A").GetProperty("own"), "deadlocks"));
            Assert.Equal(1, Figure(Row(deadlocks, "B").GetProperty("own"), "deadlocks"));
            Assert.Equal(1, Figure(deadlocks.GetProperty("total"), "deadlocks"));
            Assert.False(deadlocks.GetProperty("rows_add_up_to_total").GetBoolean());
        });
    }

    [Fact]
    public async Task TheFleetViewListsEachServer_AndTotalsAcrossThem()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            await MapAsync(c, ServerOne, "hash", "0xA1", a);
            await MapAsync(c, ServerTwo, "hash", "0xA1", a);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 2_000_000);
            await PlanCacheAsync(c, ServerTwo, "0xA1", 30, 3_000_000);

            using var doc = await ReadAsync(c, null);
            Assert.Equal("fleet", doc.RootElement.GetProperty("scope").GetString());
            var plan = Section(doc, "plan_cache");
            var servers = plan.GetProperty("servers").EnumerateArray().ToDictionary(s => s.GetProperty("server").GetString()!);
            Assert.Equal(new[] { "srv-one", "srv-two" }, servers.Keys.OrderBy(k => k, StringComparer.Ordinal));
            Assert.Equal(2_000, Figure(Row(servers["srv-one"], "A").GetProperty("own"), "cpu_ms"));
            Assert.Equal(3_000, Figure(Row(servers["srv-two"], "A").GetProperty("own"), "cpu_ms"));
            Assert.Equal(5_000, Figure(plan.GetProperty("fleet_total"), "cpu_ms"));
        });
    }

    /// <summary>With a tag set, the fleet line is the sum of the servers' tagged totals: the untagged work and the other tags on every
    /// server stay out of it, as they stay out of each server's own total (#5983).</summary>
    [Fact]
    public async Task UnderATagFilter_TheFleetTotal_IsTheSumOfTheServersTaggedTotals_NotEveryFact()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            var b = await TagAsync(c, "B");
            await MapAsync(c, ServerOne, "hash", "0xA1", a);
            await MapAsync(c, ServerOne, "hash", "0xB1", b);
            await MapAsync(c, ServerTwo, "hash", "0xA1", a);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 2_000_000);
            await PlanCacheAsync(c, ServerOne, "0xB1", 30, 1_000_000);
            await PlanCacheAsync(c, ServerOne, "0xU1", 30, 4_000_000);
            await PlanCacheAsync(c, ServerTwo, "0xA1", 30, 3_000_000);
            await PlanCacheAsync(c, ServerTwo, "0xU1", 30, 8_000_000);

            await MapAsync(c, ServerOne, "caller", Caller("appA", "lg", "h", "db1"), a);
            await MapAsync(c, ServerOne, "caller", Caller("appB", "lg", "h", "db1"), b);
            await MapAsync(c, ServerTwo, "caller", Caller("appA", "lg", "h", "db1"), a);
            await DeadlockAsync(c, ServerOne, "appA", "appB");
            await DeadlockAsync(c, ServerTwo, "appA", "appA");
            await DeadlockAsync(c, ServerTwo, "appU", "appU");

            using var doc = await ReadAsync(c, null, tag: "A");
            var plan = Section(doc, "plan_cache");
            var servers = plan.GetProperty("servers").EnumerateArray().ToDictionary(x => x.GetProperty("server").GetString()!);
            Assert.Equal(2_000, Figure(servers["srv-one"].GetProperty("total"), "cpu_ms"));
            Assert.Equal(3_000, Figure(servers["srv-two"].GetProperty("total"), "cpu_ms"));
            Assert.False(servers["srv-one"].TryGetProperty("untagged", out _));
            Assert.Equal(5_000, Figure(plan.GetProperty("fleet_total"), "cpu_ms"));
            Assert.Equal(2, Figure(plan.GetProperty("fleet_total"), "executions"));

            var deadlocks = Section(doc, "deadlocks");
            Assert.Equal(2, Figure(deadlocks.GetProperty("fleet_total"), "deadlocks"));
            Assert.Equal(3, Figure(deadlocks.GetProperty("fleet_total"), "processes"));
        });
    }

    [Fact]
    public async Task AWindowWithNoRows_AnswersEmpty()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            await MapAsync(c, ServerOne, "hash", "0xA1", a);
            await PlanCacheAsync(c, ServerOne, "0xA1", 60 * 30, 2_000_000);
            await CoverQueryStoreAsync(c, ServerOne);

            using var doc = await ReadAsync(c, "srv-one");
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            /* The empty answer carries the sections, as the Lite twin's does (#5983), not a bare status and message. */
            Assert.Equal(6, doc.RootElement.GetProperty("sections").GetArrayLength());
            Assert.Contains("No resource use was collected", doc.RootElement.GetProperty("message").GetString());
        });
    }

    /// <summary>A KNOWN tag that no collected work carries, in a window that holds work, answers empty with its own message (not as zero-figure
    /// data, and not the unknown-name refusal), and the empty carries the sections. Plan-cache work tagged B, asked for as tag A, for one
    /// server and for the fleet; asked for as tag B the same window holds data.</summary>
    [Fact]
    public async Task AKnownTagThatNoWorkCarries_InAWindowThatHoldsWork_AnswersEmptyWithItsOwnMessage()
    {
        await WithStoreAsync(async c =>
        {
            await TagAsync(c, "A");
            var b = await TagAsync(c, "B");
            await MapAsync(c, ServerOne, "hash", "0xB1", b);
            await PlanCacheAsync(c, ServerOne, "0xB1", 30, 3_000_000, 5);
            await CoverQueryStoreAsync(c, ServerOne);

            foreach (var server in new string?[] { "srv-one", null })
            {
                using var doc = await ReadAsync(c, server, tag: "A");
                Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
                Assert.Contains("No collected work carries query tag 'A'", doc.RootElement.GetProperty("message").GetString());
                Assert.Equal(6, doc.RootElement.GetProperty("sections").GetArrayLength());
                Assert.Equal("A", doc.RootElement.GetProperty("tag").GetString());
            }

            using var withData = await ReadAsync(c, "srv-one", tag: "B");
            Assert.False(withData.RootElement.TryGetProperty("status", out _));
        });
    }

    /// <summary>The deadlock section counts toward "the tag matched" the way the stats sections do: a window whose only work is a deadlock between
    /// tags A and B is data for tag A and empty for tag C.</summary>
    [Fact]
    public async Task ADeadlockOnlyWindow_IsDataForATagItsProcessesCarry_AndEmptyForAnotherKnownTag()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            var b = await TagAsync(c, "B");
            await TagAsync(c, "C");
            await MapAsync(c, ServerOne, "caller", Caller("appA", "lg", "h", "db1"), a);
            await MapAsync(c, ServerOne, "caller", Caller("appB", "lg", "h", "db1"), b);
            await DeadlockAsync(c, ServerOne, "appA", "appB");

            using var forA = await ReadAsync(c, "srv-one", tag: "A");
            Assert.False(forA.RootElement.TryGetProperty("status", out _));
            using var forC = await ReadAsync(c, "srv-one", tag: "C");
            Assert.Equal("empty", forC.RootElement.GetProperty("status").GetString());
            Assert.Contains("No collected work carries query tag 'C'", forC.RootElement.GetProperty("message").GetString());
        });
    }

    /// <summary>A 1-hour window is under the 24-hour minimum the Query Store interval table is read for. The other five sections were served and
    /// held nothing, so the top-level status is empty (not unavailable), and the Query Store section says why it was not served.</summary>
    [Fact]
    public async Task AOneHourAllQuietWindow_AnswersEmpty_WithTheQueryStoreSectionUnavailableAndTheFixNamed()
    {
        await WithStoreAsync(async c =>
        {
            using var doc = await ReadAsync(c, "srv-one", hours: 1);
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            Assert.Contains("query_store", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            var qs = Section(doc, "query_store");
            Assert.Equal("unavailable", qs.GetProperty("status").GetString());
            Assert.Contains("24-hour minimum", qs.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal("empty", Section(doc, "plan_cache").GetProperty("status").GetString());
            Assert.Equal("empty", Section(doc, "deadlocks").GetProperty("status").GetString());
        });
    }

    /// <summary>Fleet ask, server one has no Query Store coverage row, server two is covered, and every section is quiet. The answer is empty
    /// (not a bare status that drops the refusal): the sections ride along and the Query Store section names server one's refusal.</summary>
    [Fact]
    public async Task AFleetAskWhereOneServerIsRefusedByTheQueryStoreGate_AndEverySectionIsQuiet_AnswersEmptyWithTheRefusalNamed()
    {
        await WithStoreAsync(async c =>
        {
            await CoverQueryStoreAsync(c, ServerTwo);

            using var doc = await ReadAsync(c, null);
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            Assert.True(doc.RootElement.TryGetProperty("sections", out _));
            Assert.Contains("query_store", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
            var qs = Section(doc, "query_store");
            var message = qs.GetProperty("message").GetString();
            Assert.Contains("srv-one", message, StringComparison.Ordinal);
            Assert.DoesNotContain("srv-two", message, StringComparison.Ordinal);
            Assert.Contains("no coverage row", message, StringComparison.Ordinal);
            var servers = qs.GetProperty("servers").EnumerateArray().ToDictionary(x => x.GetProperty("server").GetString()!);
            Assert.Equal("unavailable", servers["srv-one"].GetProperty("status").GetString());
            Assert.Equal("empty", servers["srv-two"].GetProperty("status").GetString());
            Assert.Equal("empty", Section(doc, "plan_cache").GetProperty("status").GetString());
        });
    }

    /// <summary>The Query Store gate and the read it authorizes share ONE repeatable-read snapshot (QueryStoreIntervalWide.ResolveReadAsync's
    /// contract, as get_query_store_top). The read is held at its table lock after the gate has decided; a batch committed meanwhile must not
    /// show in the read. A read through a fresh command (the old shape) takes its snapshot after the lock is released and counts it.</summary>
    [Fact]
    public async Task TheQueryStoreGateAndItsRead_ShareOneSnapshot_ABatchCommittedBetweenThemIsNotSeen()
    {
        await WithStoreAsync(async c =>
        {
            var now = DateTime.UtcNow;
            await CoverQueryStoreAsync(c, ServerOne);
            await QueryStoreAsync(c, ServerOne, "0xA1", 1, 4_000_000, 7);

            await using var holder = new NpgsqlConnection(c.Store.ConnectionString);
            await holder.OpenAsync(c.Ct);
            await ExecAsync(holder, "BEGIN", c.Ct);
            /* The gate does not touch the tag map; the table read joins it, so the read waits here once the gate has decided. */
            await ExecAsync(holder, "LOCK TABLE config.query_tag_map IN ACCESS EXCLUSIVE MODE", c.Ct);

            var read = DarlingQueryTagRollupReader.ReadQueryStoreAsync(c.Source, [ServerOne], now.AddHours(-24), now, null, c.Ct);
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (true)
            {
                await using var probe = new NpgsqlCommand(
                    "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%query_store_interval_wide AS q%'", c.Store);
                if (Convert.ToInt32(await probe.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture) > 0)
                {
                    break;
                }

                Assert.False(read.IsCompleted, "the read finished before it waited on the table lock: the gate refused, or the read does not join the tag map");
                Assert.True(DateTime.UtcNow < deadline, "the Query Store read never waited on the tag map lock");
                await Task.Delay(50, c.Ct);
            }

            await QueryStoreAsync(c, ServerOne, "0xA1", 2, 4_000_000, 5);
            await ExecAsync(holder, "COMMIT", c.Ct);

            var result = await read;
            Assert.True(result.Allowed.ContainsKey(ServerOne));
            var executions = QueryTagRollupSection.QueryStore.Measures.ToList().FindIndex(m => m.Name == "executions");
            Assert.Equal(7, result.Facts.Sum(f => f.Values[executions]));
        });
    }

    /// <summary>#6794: the Query Store section is ONE statement for every server the gate allows, not one per server, and each server's
    /// figures are still its own. Two covered servers: one statement runs, and each server's executions are the ones planted for it.</summary>
    [Fact]
    public async Task AFleetQueryStoreRead_RunsOneStatementForEveryAllowedServer_AndKeepsEachServersFigures()
    {
        await WithStoreAsync(async c =>
        {
            var now = DateTime.UtcNow;
            await CoverQueryStoreAsync(c, ServerOne);
            await CoverQueryStoreAsync(c, ServerTwo);
            await QueryStoreAsync(c, ServerOne, "0xA1", 1, 4_000_000, 7);
            await QueryStoreAsync(c, ServerTwo, "0xB1", 2, 4_000_000, 5);

            var database = new NpgsqlConnectionStringBuilder(c.Store.ConnectionString).Database!;
            using var counter = new NpgsqlCommandCounter(database, "query_store_interval_wide AS q", "GROUP BY q.server_id, m.tag_id");
            var result = await DarlingQueryTagRollupReader.ReadQueryStoreAsync(c.Source, [ServerOne, ServerTwo], now.AddHours(-24), now, null, c.Ct);

            Assert.Equal(1L, counter.Count);
            Assert.Equal([ServerOne, ServerTwo], result.Allowed.Keys.Order());
            var executions = QueryTagRollupSection.QueryStore.Measures.ToList().FindIndex(m => m.Name == "executions");
            Assert.Equal(7, result.Facts.Where(f => f.ServerId == ServerOne).Sum(f => f.Values[executions]));
            Assert.Equal(5, result.Facts.Where(f => f.ServerId == ServerTwo).Sum(f => f.Values[executions]));
        });
    }

    /// <summary>#6807 review: the gate's fleet floor statement runs inside the read's REPEATABLE READ transaction. When it fails, or passes
    /// its short deadline, only it is undone (a savepoint): every server is still decided through its own floor read on the same snapshot,
    /// and the section reads each server's figures. Without the savepoint the failure aborts the transaction and every server is refused.</summary>
    [Fact]
    public async Task AFleetFloorStatementThatFailsOrTimesOut_StillAnswersEveryServer_OnTheSameSnapshot()
    {
        await WithStoreAsync(async c =>
        {
            var now = DateTime.UtcNow;
            await CoverQueryStoreAsync(c, ServerOne);
            await CoverQueryStoreAsync(c, ServerTwo);
            await QueryStoreAsync(c, ServerOne, "0xA1", 1, 4_000_000, 7);
            await QueryStoreAsync(c, ServerTwo, "0xB1", 2, 4_000_000, 5);
            var executions = QueryTagRollupSection.QueryStore.Measures.ToList().FindIndex(m => m.Name == "executions");
            const string Binds = "$1::integer[] IS NOT NULL AND $2::timestamp IS NOT NULL AND ($3::timestamp IS NULL OR TRUE)";
            foreach (var failing in new[]
            {
                "SELECT (1 / 0)::integer AS server_id, NULL::timestamp AS floor WHERE " + Binds,
                "SELECT 1 AS server_id, NULL::timestamp AS floor FROM pg_sleep(60) WHERE " + Binds,
            })
            {
                QueryStoreIntervalWide.StoreWideInputsCache.TableFloorsSqlForTests.Value = failing;
                try
                {
                    var result = await DarlingQueryTagRollupReader.ReadQueryStoreAsync(c.Source, [ServerOne, ServerTwo], now.AddHours(-24), now, null, c.Ct);
                    Assert.Empty(result.Refused);
                    Assert.Equal([ServerOne, ServerTwo], result.Allowed.Keys.Order());
                    Assert.Equal(7, result.Facts.Where(f => f.ServerId == ServerOne).Sum(f => f.Values[executions]));
                    Assert.Equal(5, result.Facts.Where(f => f.ServerId == ServerTwo).Sum(f => f.Values[executions]));
                }
                finally
                {
                    QueryStoreIntervalWide.StoreWideInputsCache.TableFloorsSqlForTests.Value = null;
                }
            }
        });
    }

    /// <summary>#6794: the batched Query Store statement binds each server's own read start. Server two's start is an hour ago, so its row
    /// from two hours ago is not read, while server one's row of the same age (its start is the window's) is.</summary>
    [Fact]
    public async Task TheBatchedQueryStoreStatement_BindsEachServersOwnReadStart()
    {
        await WithStoreAsync(async c =>
        {
            var now = DateTime.UtcNow;
            foreach (var (server, queryId) in new[] { (ServerOne, 1L), (ServerTwo, 2L) })
            {
                await ExecAsync(c.Store,
                    "INSERT INTO query_store_interval_wide (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes, max_query_max_used_memory, runtime_stats_interval_id, interval_start_time_utc) VALUES ($1, $2, 'db1', $3, $3, 'Regular', $4, '0xAA', 3, 10, 10, 5, 1, 64, $3, $4)",
                    c.Ct, now.AddHours(-2), server, queryId, now.AddHours(-2).AddMinutes(-10));
            }

            await using var command = new NpgsqlCommand(QueryTagRollupSql.QueryStoreSql, c.Store);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = new[] { ServerOne, ServerTwo } });
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp,
                Value = new[] { DateTime.SpecifyKind(now.AddHours(-24), DateTimeKind.Unspecified), DateTime.SpecifyKind(now.AddHours(-1), DateTimeKind.Unspecified) },
            });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(now, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(now.AddHours(-24), DateTimeKind.Unspecified) });
            var servers = new List<int>();
            await using (var reader = await command.ExecuteReaderAsync(c.Ct))
            {
                while (await reader.ReadAsync(c.Ct))
                {
                    servers.Add(reader.GetInt32(0));
                }
            }

            Assert.Equal([ServerOne], servers);
        });
    }

    /// <summary>The top-level status is unavailable only when every section is: here the five sources do not exist in this store and the
    /// window is under the Query Store minimum, so nothing could be served.</summary>
    [Fact]
    public async Task WhenEverySectionIsUnavailable_TheTopLevelStatusIsUnavailable()
    {
        await WithStoreAsync(async c =>
        {
            foreach (var table in new[] { "collect.query_stats", "collect.procedure_stats", "collect.blocked_process_reports", "collect.dmv_blocking_snapshots", "collect.deadlocks" })
            {
                await ExecAsync(c.Store, $"DROP TABLE {table} CASCADE", c.Ct);
            }

            using var doc = await ReadAsync(c, "srv-one", hours: 1);
            Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
            Assert.Contains("query_store", doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task WithATagSet_TheAnswerHoldsOnlyThatTagsSubtree_AndNoUntaggedRow()
    {
        await WithStoreAsync(async c =>
        {
            var parent = await TagAsync(c, "Billing");
            var child = await TagAsync(c, "Invoices", parent);
            var other = await TagAsync(c, "Other");
            await MapAsync(c, ServerOne, "hash", "0xA1", parent);
            await MapAsync(c, ServerOne, "hash", "0xA2", child);
            await MapAsync(c, ServerOne, "hash", "0xA3", other);
            await PlanCacheAsync(c, ServerOne, "0xA1", 30, 3_000_000);
            await PlanCacheAsync(c, ServerOne, "0xA2", 30, 5_000_000);
            await PlanCacheAsync(c, ServerOne, "0xA3", 30, 9_000_000);
            await PlanCacheAsync(c, ServerOne, "0xFF", 30, 1_000_000);

            using var doc = await ReadAsync(c, "srv-one", tag: "billing");
            var plan = Section(doc, "plan_cache");
            Assert.Equal(new[] { "Billing", "Invoices" }, plan.GetProperty("tag_rows").EnumerateArray().Select(r => r.GetProperty("tag").GetString()!).OrderBy(n => n, StringComparer.Ordinal));
            Assert.False(plan.TryGetProperty("untagged", out _));
            Assert.Equal(8_000, Figure(plan.GetProperty("total"), "cpu_ms"));
        });
    }

    [Fact]
    public async Task TheWindowEdge_KeepsARowJustInside_AndDropsARowJustOutside()
    {
        await WithStoreAsync(async c =>
        {
            var a = await TagAsync(c, "A");
            await MapAsync(c, ServerOne, "hash", "0xIN", a);
            await MapAsync(c, ServerOne, "hash", "0xOUT", a);
            await PlanCacheAsync(c, ServerOne, "0xIN", 58, 2_000_000);
            await PlanCacheAsync(c, ServerOne, "0xOUT", 62, 7_000_000);

            using var doc = await ReadAsync(c, "srv-one", hours: 1);
            Assert.Equal(2_000, Figure(Section(doc, "plan_cache").GetProperty("total"), "cpu_ms"));
        });
    }

    [Fact]
    public async Task TagRowsAreOrderedByTheSectionsResourceColumn_AndALimitSetsTruncated_WithTheUntaggedRowStillThere()
    {
        await WithStoreAsync(async c =>
        {
            foreach (var (name, cpu) in new[] { ("Small", 1L), ("Big", 9L), ("Mid", 5L) })
            {
                var id = await TagAsync(c, name);
                await MapAsync(c, ServerOne, "hash", "0x" + name, id);
                await PlanCacheAsync(c, ServerOne, "0x" + name, 30, cpu * 1_000_000);
            }

            await PlanCacheAsync(c, ServerOne, "0xFF", 30, 2_000_000);

            using var doc = await ReadAsync(c, "srv-one", limit: 2);
            var plan = Section(doc, "plan_cache");
            Assert.Equal(new[] { "Big", "Mid" }, plan.GetProperty("tag_rows").EnumerateArray().Select(r => r.GetProperty("tag").GetString()!));
            Assert.True(plan.GetProperty("truncated").GetBoolean());
            Assert.Equal(3, plan.GetProperty("tag_rows_total").GetInt32());
            Assert.Equal(2_000, Figure(plan.GetProperty("untagged"), "cpu_ms"));
            Assert.Equal(17_000, Figure(plan.GetProperty("total"), "cpu_ms"));
        });
    }

    /// <summary>
    /// #6800 part 2, step 3a: today's batched Query Store statement must not scan the wide table by its <c>collection_time</c> index for
    /// the window-end bound. Step 2b measured that with the window end 6 hours back (an <c>as_of</c> in the past) a plain
    /// <c>collection_time &lt;= $3</c> made the planner choose that index, the bitmap went lossy, and inside the per-server loop that index is
    /// the 13.0 s against 6.0 s case. The statement still serves every one-server read and every fleet read the route falls back from.
    /// Found by what the plan's nodes do: no scan of the wide table or of one of its partitions has an index condition on
    /// <c>collection_time</c> (sequential scans are off, the seed is small, so the plan shows the paths the statement allows). The seed is
    /// 30 servers, the heap is laid out in <c>collection_time</c> order as the store's is, the store's two indexes are present, and the seed
    /// and the read are anchored at <see cref="LiveClock.AnchoredEnd"/> (#5608).
    /// </summary>
    [Fact]
    public async Task TodaysQueryStoreStatement_WithTheWindowEndSixHoursBack_DoesNotUseTheCollectionTimeIndexForTheEndBound()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), "Set DARLING_TEST_PG to run this.");
        var ct = TestContext.Current.CancellationToken;
        const int Servers = 30;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct, servers: 3, build: false, at: LiveClock.AnchoredEnd());
        var bodySucceeded = false;
        try
        {
            /* 30 servers: the three seeded ones copied to servers 4 to 30. */
            await ExecAsync(connection, @"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
 module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, replica_role,
 runtime_stats_interval_id, interval_start_time_utc)
SELECT w.collection_time, s.server_id, w.database_name, w.query_id, w.plan_id, w.execution_type_desc, w.first_execution_time, w.last_execution_time,
       w.module_name, w.query_hash, w.execution_count, w.avg_duration_us, w.avg_cpu_time_us, w.max_duration_us, w.max_cpu_time_us, w.replica_role,
       w.runtime_stats_interval_id, w.interval_start_time_utc
FROM collect.query_store_interval_wide AS w
CROSS JOIN generate_series(4, 30) AS s (server_id)
WHERE w.server_id = 1", ct);

            /* The store appends rows in collection_time order, which is what the BRIN index on that column is for: lay the heap out that way. */
            await ExecAsync(connection, "CREATE INDEX ix_zz_cluster_collection_time ON collect.query_store_interval_wide (collection_time)", ct);
            await ExecAsync(connection, "CLUSTER collect.query_store_interval_wide USING ix_zz_cluster_collection_time", ct);
            await ExecAsync(connection, "DROP INDEX collect.ix_zz_cluster_collection_time", ct);
            await ExecAsync(connection, "CREATE INDEX IF NOT EXISTS ix_query_store_interval_wide_server_first_exec ON collect.query_store_interval_wide (server_id, first_execution_time)", ct);
            await ExecAsync(connection, "CREATE INDEX IF NOT EXISTS ix_query_store_interval_wide_collection_time_brin ON collect.query_store_interval_wide USING brin (collection_time) WITH (pages_per_range = 4)", ct);
            await ExecAsync(connection, "ANALYZE collect.query_store_interval_wide", ct);

            var end = DateTime.SpecifyKind(hourNow.AddHours(-6), DateTimeKind.Unspecified);
            var start = end.AddHours(-24);
            var ids = Enumerable.Range(1, Servers).ToArray();
            await using var transaction = await connection.BeginTransactionAsync(ct);
            await ExecAsync(connection, "SET LOCAL enable_seqscan = off", ct);
            await using var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + QueryTagRollupSql.QueryStoreSql, connection) { Transaction = transaction, CommandTimeout = 120 };
            explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = ids });
            explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = ids.Select(_ => start).ToArray() });
            explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = end });
            explain.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = start });
            var json = Convert.ToString(await explain.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
            using var document = JsonDocument.Parse(json);
            var nodes = new List<JsonElement>();
            void Walk(JsonElement node)
            {
                nodes.Add(node);
                if (node.TryGetProperty("Plans", out var children))
                {
                    foreach (var child in children.EnumerateArray())
                    {
                        Walk(child);
                    }
                }
            }

            Walk(document.RootElement[0].GetProperty("Plan"));
            string Text(JsonElement n, string name) => n.TryGetProperty(name, out var v) ? v.GetString() ?? string.Empty : string.Empty;

            var wideNodes = nodes.Where(n =>
                Text(n, "Relation Name").StartsWith("query_store_interval_wide", StringComparison.Ordinal)
                || Text(n, "Index Name").Contains("query_store_interval_wide", StringComparison.Ordinal)).ToList();
            Assert.NotEmpty(wideNodes);
            var byCollectionTime = wideNodes.Where(n => Text(n, "Index Cond").Contains("collection_time", StringComparison.Ordinal)).ToList();
            Assert.True(
                byCollectionTime.Count == 0,
                "the window-end bound must not become an index condition on collection_time; nodes: "
                + string.Join(" | ", byCollectionTime.Select(n => Text(n, "Node Type") + " " + Text(n, "Index Name") + " " + Text(n, "Index Cond"))));
            await transaction.RollbackAsync(ct);
            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public void TheToolsHeadNamesTheNeverSummedRule_AndPointsAtTheGuide()
    {
        var served = McpToolGuideTests.Served("get_resource_use_by_tag");
        Assert.NotNull(served.Tail);
        Assert.Contains("SECTIONS ARE NEVER SUMMED", served.Served, StringComparison.Ordinal);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head {served.Served.Length} is over the 620 target");
    }
}
