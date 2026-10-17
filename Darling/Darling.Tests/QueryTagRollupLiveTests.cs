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
        });
    }

    /// <summary>A window too short for the Query Store interval table is not an all-clear: the answer is unavailable, naming why.</summary>
    [Fact]
    public async Task AOneHourWindowWithNoRows_AnswersUnavailable_BecauseQueryStoreCannotBeRead()
    {
        await WithStoreAsync(async c =>
        {
            using var doc = await ReadAsync(c, "srv-one", hours: 1);
            Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
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
