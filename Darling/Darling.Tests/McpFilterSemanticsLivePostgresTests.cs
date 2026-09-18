/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

/// <summary>
/// #3541 A13 / A9 against live PostgreSQL: the properties only the SQL can prove.
///
/// <para><b>A filter is part of the query (A13).</b> <c>get_top_queries_by_cpu</c> applied
/// <c>parallel_only</c> / <c>min_dop</c> in C# over the top-N page the SQL had already cut, so a box whose
/// hottest N plans were all serial answered an EMPTY page under <c>parallel_only</c> while the window held a
/// parallel plan just past the cut. The fixture is exactly that shape: three serial groups hotter than one
/// parallel group, read at <c>top = 2</c>. The old code returned nothing; the fixed read must return the
/// parallel group, and the unfiltered read at the same cap must still return the two hottest serial ones —
/// the pair is what separates "filter in the query" from "filter the page".</para>
///
/// <para><c>get_active_queries</c> read the whole window, filtered in C#, published the pre-filter
/// <c>rows.Count</c> as <c>total_snapshots</c>, and its WAITFOR trim dropped the head blocker its victims
/// pointed at. One capture is seeded with the three blocker situations the tool now names: a victim whose
/// blocker is a WAITFOR shell in the same capture (kept, flagged), a victim whose blocker was never captured
/// (an idle open transaction), and a victim in one database whose blocker is in another (present unfiltered,
/// <c>filtered</c> under <c>database_name</c>). The count beside the page must be the FILTERED population's,
/// and truncation must be observed on it.</para>
///
/// <para><b>Retention ghosts (A9).</b> A collector run 45 days back sits inside the collection log's 60-day
/// horizon and outside the signals' 30-day one — the exact stretch that used to band Healthy on COALESCEd
/// zeros. It must come back <c>purged</c> / <c>NoData</c> with the horizon stated; today's run must stay
/// <c>collected</c> / <c>Healthy</c> beside it; the single-day read of the purged day must refuse a verdict;
/// and a fleet-wide retention override on ONE signal collector must move the horizon, because the horizon is
/// the store's effective retention rather than the shipped default.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class McpFilterSemanticsLivePostgresTests
{
    private const string ServerName = "darling-mcp-filter-semantics-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Db = "StackOverflow";
    private const string OtherDb = "AdventureWorks";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ParallelFilter_RanksTheFilteredPopulation_NotTheFilteredPage()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live filter-semantics test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);

            /* Three serial groups, hottest first, then ONE parallel group cooler than all three. */
            await PlantQueryAsync(connection, ct, now, "0xSERIAL1", "SELECT 1", cpuUs: 900_000L, maxDop: 1);
            await PlantQueryAsync(connection, ct, now, "0xSERIAL2", "SELECT 2", cpuUs: 800_000L, maxDop: 1);
            await PlantQueryAsync(connection, ct, now, "0xSERIAL3", "SELECT 3", cpuUs: 700_000L, maxDop: 1);
            await PlantQueryAsync(connection, ct, now, "0xPARALLEL", "SELECT 4", cpuUs: 100_000L, maxDop: 8);

            /* Unfiltered at top = 2: the two hottest, both serial, and no filter stated. */
            var unfiltered = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 1, 2)).RootElement;
            Assert.Equal(new[] { "0xSERIAL1", "0xSERIAL2" }, Hashes(unfiltered));
            Assert.Equal(JsonValueKind.Null, unfiltered.GetProperty("filter_applied").ValueKind);

            /* parallel_only at top = 2: the OLD code cut the two serial rows and then filtered them away —
               an empty page over a window that holds a parallel plan. The fixed read ranks the parallel
               population and returns it. */
            var parallel = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 1, 2, parallel_only: true)).RootElement;
            Assert.Equal(new[] { "0xPARALLEL" }, Hashes(parallel));
            Assert.Contains("max_dop >= 2", parallel.GetProperty("filter_applied").GetString(), StringComparison.Ordinal);
            Assert.True(parallel.GetProperty("queries")[0].GetProperty("is_parallel").GetBoolean());

            /* min_dop above the seeded DOP: an empty FILTERED page is the window's answer, not a collection miss. */
            var tooHigh = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 1, 2, min_dop: 16)).RootElement;
            Assert.Equal("empty", tooHigh.GetProperty("status").GetString());
            Assert.Contains("max_dop >= 16", tooHigh.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Contains("applied in SQL over the whole window", tooHigh.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* The rollup read carries the same floor. */
            var rolled = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 1, 2, parallel_only: true, group_by: "host_object")).RootElement;
            Assert.Equal(new[] { "0xPARALLEL" }, Hashes(rolled));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task ActiveQueries_FiltersInTheQuery_KeepsHeadBlockers_AndNamesAbsentOnes()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live filter-semantics test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var t = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-2);

            /* One capture:
                 55 (Db)      blocked by 60 — a WAITFOR shell in the same capture: the classic head blocker.
                 60 (Db)      WAITFOR DELAY, blocking 55. The old read stripped it.
                 56 (Db)      blocked by 61 — 61 never captured (idle open transaction).
                 57 (OtherDb) blocked by 62 — 62 captured, in Db.
                 62 (Db)      running, blocking 57.
                 70 (Db)      running, not involved in blocking.
               A SECOND capture two minutes earlier holds an unrelated session 60 so a cross-capture match
               would wrongly resurrect it as a head blocker: same id, different capture, must NOT be kept. */
            await PlantSnapshotAsync(connection, ct, t, 55, Db, "UPDATE Posts SET Score = 1", blockingSessionId: 60, cpuMs: 500);
            await PlantSnapshotAsync(connection, ct, t, 60, Db, "WAITFOR DELAY '00:05'", blockingSessionId: 0, cpuMs: 1);
            await PlantSnapshotAsync(connection, ct, t, 56, Db, "DELETE FROM Votes", blockingSessionId: 61, cpuMs: 400);
            await PlantSnapshotAsync(connection, ct, t, 57, OtherDb, "SELECT * FROM Sales", blockingSessionId: 62, cpuMs: 300);
            await PlantSnapshotAsync(connection, ct, t, 62, Db, "UPDATE Users SET Reputation = 0", blockingSessionId: 0, cpuMs: 900);
            await PlantSnapshotAsync(connection, ct, t, 70, Db, "SELECT COUNT(*) FROM Comments", blockingSessionId: 0, cpuMs: 200);
            await PlantSnapshotAsync(connection, ct, t.AddMinutes(-2), 60, Db, "WAITFOR DELAY '00:05'", blockingSessionId: 0, cpuMs: 1);

            /* Unfiltered: the WAITFOR head blocker is on the page, flagged; the stale WAITFOR in the other
               capture is not; the never-captured blocker is named as such; the cross-database blocker is present. */
            var all = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, limit: 50)).RootElement;
            var rows = all.GetProperty("queries").EnumerateArray().ToArray();
            Assert.Equal(6, rows.Length);
            Assert.Equal(6, all.GetProperty("total_snapshots").GetInt64());
            Assert.Equal(6, all.GetProperty("snapshots_returned").GetInt32());
            Assert.False(all.GetProperty("truncated").GetBoolean());
            Assert.Equal("collection_time_desc", all.GetProperty("order").GetString());

            var head = Assert.Single(rows, r => r.GetProperty("session_id").GetInt32() == 60);
            Assert.True(head.GetProperty("is_head_blocker").GetBoolean());
            Assert.StartsWith("WAITFOR", head.GetProperty("query_text").GetString(), StringComparison.Ordinal);

            Assert.Equal(JsonValueKind.Null, Row(rows, 55).GetProperty("blocker_not_shown").ValueKind);
            Assert.Equal("not_captured", Row(rows, 56).GetProperty("blocker_not_shown").GetString());
            Assert.Equal(JsonValueKind.Null, Row(rows, 57).GetProperty("blocker_not_shown").ValueKind);
            Assert.Equal(JsonValueKind.Null, Row(rows, 70).GetProperty("is_head_blocker").ValueKind);

            /* database_name filter, IN the query: the population is OtherDb's one victim, its blocker is
               in Db and therefore filtered — the caller asked for that database, and the row says so. */
            var other = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, OtherDb)).RootElement;
            Assert.Equal(1, other.GetProperty("total_snapshots").GetInt64());
            Assert.Equal("filtered", Assert.Single(other.GetProperty("queries").EnumerateArray()).GetProperty("blocker_not_shown").GetString());
            Assert.Equal(OtherDb, other.GetProperty("filters_applied").GetProperty("database_name").GetString());

            /* blocking_only, IN the query: victims 55/56/57 + heads 60/62 = 5; 70 is out. The count is the
               blocking population's, not the window's. */
            var blocking = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, blocking_only: true)).RootElement;
            Assert.Equal(5, blocking.GetProperty("total_snapshots").GetInt64());
            Assert.DoesNotContain(blocking.GetProperty("queries").EnumerateArray(), r => r.GetProperty("session_id").GetInt32() == 70);

            /* Truncation is observed on the FILTERED population: limit = 4 over 5 blocking rows is truncated,
               limit = 5 is not, and the total stays the population's under both. The page is CPU-descending
               within the capture, so the WAITFOR head (1 ms) falls past a 4-row page and its victim says so. */
            var cut = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, blocking_only: true, limit: 4)).RootElement;
            Assert.True(cut.GetProperty("truncated").GetBoolean());
            Assert.Equal(4, cut.GetProperty("snapshots_returned").GetInt32());
            Assert.Equal(5, cut.GetProperty("total_snapshots").GetInt64());
            Assert.Equal("past_page", Row(cut.GetProperty("queries").EnumerateArray().ToArray(), 55).GetProperty("blocker_not_shown").GetString());
            var whole = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, blocking_only: true, limit: 5)).RootElement;
            Assert.False(whole.GetProperty("truncated").GetBoolean());

            /* A filtered miss names the filter rather than calling the window empty. */
            var miss = JsonDocument.Parse(await DarlingMcpSessionTools.GetActiveQueries(postgres, ServerName, 1, "NoSuchDb")).RootElement;
            Assert.Equal("empty", miss.GetProperty("status").GetString());
            Assert.Contains("database_name 'NoSuchDb'", miss.GetProperty("message").GetString(), StringComparison.Ordinal);

            /* A13's third item, on the same fixture: the uncapped reads refuse a negative span. */
            Assert.StartsWith("Invalid hours_back value '-24'", await DarlingMcpDataTools.GetCollectionLog(postgres, ServerName, -24), StringComparison.Ordinal);
            Assert.StartsWith("Invalid hours_back value '0'", await DarlingMcpDataTools.GetCurrentWaitsTrend(postgres, ServerName, 0), StringComparison.Ordinal);
            Assert.StartsWith("Invalid hours_back value '-1'", await DarlingMcpDataTools.GetBlockingStats(postgres, ServerName, -1), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task DailySummary_StopsPaintingPurgedDaysGreen_AndPublishesTheHorizon()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live retention-ghost test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var today = DateTime.UtcNow.Date;
            /* Inside the collection log's 60-day horizon, outside the signals' 30 — the ghost stretch. */
            var ghostDay = today.AddDays(-45);
            /* Inside every default horizon; becomes a ghost once ONE signal's retention is overridden to 10. */
            var nearDay = today.AddDays(-20);
            var expectedHorizon = DailySummaryRetention.HorizonFor(DateTime.UtcNow, DarlingRetention.DataRetentionBaseDays);

            await SeedRunAsync(connection, ct, DateTime.UtcNow.AddMinutes(-2), "SUCCESS");
            await SeedRunAsync(connection, ct, ghostDay.AddHours(12), "SUCCESS");
            await SeedRunAsync(connection, ct, nearDay.AddHours(12), "SUCCESS");

            var range = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummaryRange(postgres, ServerName, 60)).RootElement;
            Assert.Equal(expectedHorizon.ToString("yyyy-MM-dd"), range.GetProperty("retention_horizon").GetString());
            Assert.Equal(3, range.GetProperty("day_count").GetInt32());
            Assert.Equal(1, range.GetProperty("days_before_horizon").GetInt32());
            Assert.Equal(1, range.GetProperty("purged_day_count").GetInt32());
            Assert.Equal(2, range.GetProperty("collected_day_count").GetInt32());

            var days = range.GetProperty("days").EnumerateArray().ToArray();
            var ghost = Assert.Single(days, d => d.GetProperty("summary_date").GetString() == ghostDay.ToString("yyyy-MM-dd"));
            /* The day the run record kept on the spine: runs = 1, every signal a COALESCEd zero — and it is
               NOT Healthy. */
            Assert.Equal(1, ghost.GetProperty("collection_runs").GetInt64());
            Assert.Equal("purged", ghost.GetProperty("data_state").GetString());
            Assert.Equal("NoData", ghost.GetProperty("health_band").GetString());
            Assert.Equal("No Data", ghost.GetProperty("overall_health").GetString());
            Assert.Contains("PURGED", ghost.GetProperty("data_note").GetString(), StringComparison.Ordinal);
            Assert.Contains(expectedHorizon.ToString("yyyy-MM-dd"), ghost.GetProperty("data_note").GetString(), StringComparison.Ordinal);

            var live = Assert.Single(days, d => d.GetProperty("summary_date").GetString() == today.ToString("yyyy-MM-dd"));
            Assert.Equal("collected", live.GetProperty("data_state").GetString());
            Assert.Equal("Healthy", live.GetProperty("overall_health").GetString());
            Assert.Equal(JsonValueKind.Null, live.GetProperty("data_note").ValueKind);

            /* The single-day read of the ghost refuses a verdict, in the miss vocabulary's own word for it. */
            var single = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummary(postgres, ServerName, ghostDay.ToString("yyyy-MM-dd"))).RootElement;
            Assert.Equal("unavailable", single.GetProperty("status").GetString());
            Assert.Contains("retention_horizon", single.GetProperty("message").GetString(), StringComparison.Ordinal);
            Assert.Equal("purged", single.GetProperty("hints").GetProperty("data_state").GetString());
            Assert.Equal(1, single.GetProperty("hints").GetProperty("collection_runs").GetInt64());

            /* And of a collected day, the verdict with its state. */
            var todayRow = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummary(postgres, ServerName)).RootElement;
            Assert.Equal("collected", todayRow.GetProperty("data_state").GetString());
            Assert.Equal(expectedHorizon.ToString("yyyy-MM-dd"), todayRow.GetProperty("retention_horizon").GetString());

            /* summary_date is exact ISO-8601: the ambiguous spelling is refused, not guessed at. */
            Assert.StartsWith("Invalid summary_date value '01/02/2026'", await DarlingMcpHealthTools.GetDailySummary(postgres, ServerName, "01/02/2026"), StringComparison.Ordinal);

            /* A signal row surviving before the horizon (the purge has not reached it — gate-held, paused,
               or simply not yet run) turns the shell into past_horizon: the row is real, the verdict is
               still withheld, and the day is NOT called purged because that would be false. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(ghostDay.AddHours(6)), ServerId, ServerName,
                DarlingMcpTestData.Naive(ghostDay.AddHours(6)), "process1", "DELETE FROM Posts", "<deadlock/>");
            var survived = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummaryRange(postgres, ServerName, 60)).RootElement;
            Assert.Equal(1, survived.GetProperty("days_before_horizon").GetInt32());
            Assert.Equal(0, survived.GetProperty("purged_day_count").GetInt32());
            var pastHorizon = Assert.Single(survived.GetProperty("days").EnumerateArray(), d => d.GetProperty("summary_date").GetString() == ghostDay.ToString("yyyy-MM-dd"));
            Assert.Equal("past_horizon", pastHorizon.GetProperty("data_state").GetString());
            Assert.Equal("NoData", pastHorizon.GetProperty("health_band").GetString());
            Assert.Equal(1, pastHorizon.GetProperty("deadlock_count").GetInt64());
            Assert.Contains("1 of 7 signal sources", pastHorizon.GetProperty("data_note").GetString(), StringComparison.Ordinal);
            /* The single-day read of a past-horizon day is a DATA payload (the rows are there), not unavailable. */
            var singlePast = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummary(postgres, ServerName, ghostDay.ToString("yyyy-MM-dd"))).RootElement;
            Assert.Equal("past_horizon", singlePast.GetProperty("data_state").GetString());
            Assert.Equal("No Data", singlePast.GetProperty("overall_health").GetString());
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DELETE FROM deadlocks WHERE server_id = {ServerId}");

            /* The horizon is the store's EFFECTIVE retention: a fleet-wide override shortening one signal
               collector to 10 days moves it, and the 20-day-old run becomes a ghost too. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO config_collector_schedules (server_id, collector_name, retention_days, enabled) VALUES (NULL, 'deadlocks', 10, TRUE)");
            var shortened = JsonDocument.Parse(await DarlingMcpHealthTools.GetDailySummaryRange(postgres, ServerName, 60)).RootElement;
            Assert.Equal(DailySummaryRetention.HorizonFor(DateTime.UtcNow, 10).ToString("yyyy-MM-dd"), shortened.GetProperty("retention_horizon").GetString());
            Assert.Equal(2, shortened.GetProperty("days_before_horizon").GetInt32());
            var near = Assert.Single(shortened.GetProperty("days").EnumerateArray(), d => d.GetProperty("summary_date").GetString() == nearDay.ToString("yyyy-MM-dd"));
            Assert.Equal("purged", near.GetProperty("data_state").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static string[] Hashes(JsonElement root) =>
        root.GetProperty("queries").EnumerateArray().Select(q => q.GetProperty("query_hash").GetString()!).ToArray();

    private static JsonElement Row(JsonElement[] rows, int sessionId) =>
        Assert.Single(rows, r => r.GetProperty("session_id").GetInt32() == sessionId);

    private static async Task PlantQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string queryHash, string queryText, long cpuUs, int maxDop)
    {
        var digest = System.Security.Cryptography.SHA256.HashData(System.Text.Encoding.UTF8.GetBytes(queryText));
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, query_plan_hash, sql_handle, plan_handle, query_text,
                                       query_text_digest, delta_execution_count, delta_worker_time,
                                       delta_elapsed_time, delta_logical_reads, min_dop, max_dop)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, Db,
            queryHash, "0xPLANHASH", "0xSQLH" + queryHash, "0xPLANH", queryText,
            digest, 10L, cpuUs, cpuUs, 100L, 1, maxDop);
    }

    private static Task PlantSnapshotAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, int sessionId, string database, string text, int blockingSessionId, long cpuMs) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, status, blocking_session_id, wait_type, cpu_time_ms, total_elapsed_time_ms, request_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, sessionId, database, text,
            blockingSessionId > 0 ? "suspended" : "running", blockingSessionId, blockingSessionId > 0 ? "LCK_M_X" : null, cpuMs, cpuMs * 2, 0);

    private static Task SeedRunAsync(NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, string status) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), ServerId, ServerName, "wait_stats",
            DarlingMcpTestData.Naive(collectionTimeUtc), 100, status, null, 10, 80, 20);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql = string.Join(" ", new[] { "query_stats", "query_snapshots", "collection_log", "deadlocks" }
            .Select(tbl => $"DELETE FROM {tbl} WHERE server_id = {ServerId};"));
        sql += " DELETE FROM config_collector_schedules WHERE server_id IS NULL AND collector_name = 'deadlocks' AND retention_days = 10;";
        sql += $" DELETE FROM servers WHERE server_id = {ServerId};";
        sql += $" DELETE FROM config_monitored_servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
