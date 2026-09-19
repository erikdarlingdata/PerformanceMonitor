/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3541 A10, Lite half — <b>latest is a time.</b> Every latest-snapshot MCP read publishes <c>captured_at</c>,
/// the snapshot's own collection instant; the anchored ones publish <c>age_seconds</c> against the anchor the
/// caller sent, never the wall clock; a tool whose <c>hours_back</c> is the span SEARCHED for the newest snapshot
/// refuses one older than that; and the two memory-grant tools READ their window beside the snapshot, so a
/// grant storm three hours ago is visible under a calm latest row. Darling's twin
/// (<c>Darling.Tests/McpLatestSnapshotStampTests</c>) holds the cross-SKU census — roster, shapes, descriptions
/// pinned byte-equal — and executes against live Postgres; this file executes the Lite tools against a real
/// DuckDB through the real tool methods, because the stamps live in the SQL and a helper-only test would pass
/// with the column missing from the SELECT.
///
/// <para><b>Every age assertion is an equality against a PAST anchor.</b> Rows are seeded two hours back and
/// the tools are called with <c>as_of</c> five minutes after the newest row, so <c>age_seconds</c> is exactly
/// 300 — an anchor of "now" would make every age a race, and a range assertion would pass a tool that read the
/// service clock instead of the anchor, which is the defect <c>AsOfWindowAnchorTests</c> exists to prevent.</para>
/// </summary>
public sealed class McpLatestSnapshotStampTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "TestServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public McpLatestSnapshotStampTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "McpLatestStamp_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);

        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);

        /* The derived id, not a literal: seeding under a hand-picked number makes every read return
           nothing and the "unavailable" half of every pair pass for the wrong reason. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /* ───────────────────────── the windowed pair: a storm the latest snapshot cannot see ───────────────────────── */

    [Fact]
    public async Task GetResourceSemaphore_ReadsTheWindowBesideTheStampedSnapshot()
    {
        var (@base, anchor) = Past();
        await SeedGrantAsync(@base.AddMinutes(-30), waiters: 12, timeoutsDelta: 3, grantedMb: 6000);
        await SeedGrantAsync(@base, waiters: 0, timeoutsDelta: 0, grantedMb: 500);

        var root = Parse(await McpMemoryTools.GetResourceSemaphore(_dataService, _serverManager, ServerName, 1, anchor));

        Assert.Equal(Stamp(@base), root.GetProperty("captured_at").GetString());
        Assert.Equal(300, root.GetProperty("age_seconds").GetInt64());
        Assert.Equal(1, root.GetProperty("hours_back").GetInt32());

        /* grants[] is the calm latest row; window[] is where the storm lives. */
        var latest = Assert.Single(root.GetProperty("grants").EnumerateArray());
        Assert.Equal(0, latest.GetProperty("waiter_count").GetInt32());

        var window = Assert.Single(root.GetProperty("window").EnumerateArray());
        Assert.Equal(0, window.GetProperty("resource_semaphore_id").GetInt32());
        Assert.Equal(2, window.GetProperty("pool_id").GetInt32());
        Assert.Equal(2, window.GetProperty("snapshots_in_window").GetInt64());
        Assert.Equal(12, window.GetProperty("peak_waiter_count").GetInt64());
        Assert.Equal(Stamp(@base.AddMinutes(-30)), window.GetProperty("peak_waiters_at").GetString());
        Assert.Equal(3, window.GetProperty("timeout_errors_in_window").GetInt64());
        Assert.Equal(0, window.GetProperty("forced_grants_in_window").GetInt64());
        Assert.Equal(6000d, window.GetProperty("peak_granted_memory_mb").GetDouble());
        Assert.Equal(2000d, window.GetProperty("min_available_memory_mb").GetDouble());
        Assert.Equal(Stamp(@base.AddMinutes(-30)), window.GetProperty("first_snapshot_at").GetString());
        /* The window's last snapshot IS the stamped one — one span, one set of rows, two halves. */
        Assert.Equal(root.GetProperty("captured_at").GetString(), window.GetProperty("last_snapshot_at").GetString());
    }

    [Fact]
    public async Task GetMemoryGrants_ReadsThePoolWindow_WithANullSemaphoreId()
    {
        var (@base, anchor) = Past();
        /* Two semaphores on one pool at the storm instant: the pool lens SUMs them first (7 + 5 = 12 waiters at
           one instant), so the peak is the pool's, not the larger semaphore's. */
        await SeedGrantAsync(@base.AddMinutes(-30), waiters: 7, timeoutsDelta: 2, grantedMb: 4000, semaphore: 0);
        await SeedGrantAsync(@base.AddMinutes(-30), waiters: 5, timeoutsDelta: 1, grantedMb: 2000, semaphore: 1);
        await SeedGrantAsync(@base, waiters: 0, timeoutsDelta: 0, grantedMb: 500, semaphore: 0);
        await SeedGrantAsync(@base, waiters: 0, timeoutsDelta: 0, grantedMb: 100, semaphore: 1);

        var root = Parse(await McpMemoryTools.GetMemoryGrants(_dataService, _serverManager, ServerName, 1, anchor));

        Assert.Equal(Stamp(@base), root.GetProperty("captured_at").GetString());
        Assert.Equal(300, root.GetProperty("age_seconds").GetInt64());

        var latest = Assert.Single(root.GetProperty("grants").EnumerateArray());
        Assert.Equal(600d, latest.GetProperty("granted_memory_mb").GetDouble());

        var window = Assert.Single(root.GetProperty("window").EnumerateArray());
        Assert.Equal(JsonValueKind.Null, window.GetProperty("resource_semaphore_id").ValueKind);
        Assert.Equal(12, window.GetProperty("peak_waiter_count").GetInt64());
        Assert.Equal(3, window.GetProperty("timeout_errors_in_window").GetInt64());
        Assert.Equal(6000d, window.GetProperty("peak_granted_memory_mb").GetDouble());
        Assert.Equal(2, window.GetProperty("snapshots_in_window").GetInt64());
    }

    /* ───────────────────────── the search-bound pair: a snapshot older than the span is refused ───────────────────────── */

    [Fact]
    public async Task GetCpuSchedulerPressure_IsStamped_Aged_Verdicted_AndBoundedByItsSearchSpan()
    {
        var (@base, anchor) = Past();
        var schedulerAt = @base.AddMinutes(5).AddHours(-3);
        await SeedSchedulerAsync(schedulerAt, runnable: 60);

        var root = Parse(await McpPlanCacheSchedulerTools.GetCpuSchedulerPressure(_dataService, _serverManager, ServerName, 4, anchor));
        Assert.Equal(Stamp(schedulerAt), root.GetProperty("captured_at").GetString());
        Assert.Equal(3 * 3600, root.GetProperty("age_seconds").GetInt64());
        /* The verdict Darling always published, from the SHARED banding: 60 runnable > 50. */
        Assert.Equal("CRITICAL - High runnable task queue", root.GetProperty("pressure_level").GetString());
        Assert.Contains("CPU pressure detected", root.GetProperty("recommendation").GetString(), StringComparison.Ordinal);

        /* One hour of search does not reach a three-hour-old row: unavailable, never a stale verdict. */
        var refused = JsonDocument.Parse(await McpPlanCacheSchedulerTools.GetCpuSchedulerPressure(_dataService, _serverManager, ServerName, 1, anchor)).RootElement;
        Assert.Equal("unavailable", refused.GetProperty("status").GetString());
    }

    [Fact]
    public async Task GetLatchAndSpinlockStats_AreStampedAndAged()
    {
        var (@base, anchor) = Past();
        await SeedLatchAsync(@base.AddMinutes(-20), 20000);
        await SeedLatchAsync(@base, 100);
        await SeedSpinlockAsync(@base);

        var latch = Parse(await McpLatchSpinlockTools.GetLatchStats(_dataService, _serverManager, ServerName, 1, as_of: anchor));
        Assert.Equal(Stamp(@base), latch.GetProperty("captured_at").GetString());
        Assert.Equal(300, latch.GetProperty("age_seconds").GetInt64());
        /* The snapshot is the newest one: its delta, not the hot earlier one's. */
        Assert.Equal(100, Assert.Single(latch.GetProperty("latches").EnumerateArray()).GetProperty("delta_wait_time_ms").GetInt64());

        var spin = Parse(await McpLatchSpinlockTools.GetSpinlockStats(_dataService, _serverManager, ServerName, 1, as_of: anchor));
        Assert.Equal(Stamp(@base), spin.GetProperty("captured_at").GetString());
        Assert.Equal(300, spin.GetProperty("age_seconds").GetInt64());
    }

    /* ───────────────────────── the stamped family: the newest snapshot's own instant ───────────────────────── */

    [Fact]
    public async Task GetMemoryClerks_FileIo_Perfmon_StampTheNewestSnapshot_NotTheOlderOne()
    {
        var (@base, _) = Past();
        foreach (var t in new[] { @base.AddMinutes(-10), @base })
        {
            await SeedClerkAsync(t);
            await SeedFileIoAsync(t);
            await SeedPerfmonAsync(t);
        }

        Assert.Equal(Stamp(@base), Parse(await McpMemoryTools.GetMemoryClerks(_dataService, _serverManager, ServerName)).GetProperty("captured_at").GetString());
        Assert.Equal(Stamp(@base), Parse(await McpIoTools.GetFileIoStats(_dataService, _serverManager, ServerName)).GetProperty("captured_at").GetString());

        /* Perfmon: the stamp comes from the unfiltered snapshot, so a filter that matches nothing still says when. */
        var perfmon = Parse(await McpPerfmonTools.GetPerfmonStats(_dataService, _serverManager, ServerName, "no such counter"));
        Assert.Equal(Stamp(@base), perfmon.GetProperty("captured_at").GetString());
        Assert.Empty(perfmon.GetProperty("counters").EnumerateArray());
    }

    [Fact]
    public async Task ConfigFamily_StampsTheConnectTimeCapture()
    {
        var (@base, _) = Past();
        var connectAt = @base.AddDays(-3);
        await ExecAsync(@"
INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, 'max degree of parallelism', 4, 4, true, true)", _nextId--, Naive(connectAt), _serverId, ServerName);
        await ExecAsync(@"
INSERT INTO trace_flags (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, $4, 3226, true, true, false)", _nextId--, Naive(connectAt), _serverId, ServerName);
        await ExecAsync(@"
INSERT INTO database_scoped_config (config_id, capture_time, server_id, server_name, database_name, configuration_name, value, value_for_secondary)
VALUES ($1, $2, $3, $4, 'AppDb', 'MAXDOP', '4', NULL)", _nextId--, Naive(connectAt), _serverId, ServerName);

        Assert.Equal(Stamp(connectAt), Parse(await McpConfigTools.GetServerConfig(_dataService, _serverManager, ServerName)).GetProperty("captured_at").GetString());
        Assert.Equal(Stamp(connectAt), Parse(await McpConfigTools.GetTraceFlags(_dataService, _serverManager, ServerName)).GetProperty("captured_at").GetString());

        /* A database_name filter that matches nothing still says when the (empty) answer is as of. */
        var scoped = Parse(await McpConfigTools.GetDatabaseScopedConfig(_dataService, _serverManager, ServerName, "NoSuchDb"));
        Assert.Equal(Stamp(connectAt), scoped.GetProperty("captured_at").GetString());
        Assert.Equal(0, scoped.GetProperty("database_count").GetInt32());
    }

    [Fact]
    public async Task GetServerSummary_NamesThreeClocks_SoAStaleCpuRowCannotHideUnderAFreshLog()
    {
        var (@base, _) = Past();
        var cpuAt = @base.AddDays(-1);
        await ExecAsync(@"
INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $2, 42, 3)", _nextId--, Naive(cpuAt), _serverId, ServerName);
        await ExecAsync(@"
INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, total_server_memory_mb)
VALUES ($1, $2, $3, $4, 65536, 8192, 40000)", _nextId--, Naive(@base), _serverId, ServerName);
        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, 'memory_stats', $4, 120, 'SUCCESS', NULL, 7, 90, 30)", _nextId--, _serverId, ServerName, Naive(@base.AddMinutes(1)));

        var root = Parse(await McpHealthTools.GetServerSummary(_dataService, _serverManager, ServerName));
        Assert.Equal(Stamp(cpuAt), root.GetProperty("cpu_captured_at").GetString());
        Assert.Equal(Stamp(@base), root.GetProperty("memory_captured_at").GetString());
        Assert.Equal(Stamp(@base.AddMinutes(1)), root.GetProperty("last_collection").GetString());
        Assert.Equal(McpHealthTools.ServerSummaryCountsWindowHours, root.GetProperty("counts_window_hours").GetInt32());
        Assert.Equal(42d, root.GetProperty("cpu_percent").GetDouble());
    }

    /* ───────────────────────── the Lite surface, from reflection and source ───────────────────────── */

    /// <summary>The Lite half of the roster Darling's census walks: every one publishes <c>captured_at</c> and
    /// describes itself as a time. Listed here so a Lite-only edit fails a Lite test, not only the cross-SKU one.</summary>
    public static readonly (Type Tools, string ToolName)[] LatestTools =
    [
        (typeof(McpMemoryTools), "get_memory_stats"),
        (typeof(McpMemoryTools), "get_memory_clerks"),
        (typeof(McpMemoryTools), "get_resource_semaphore"),
        (typeof(McpMemoryTools), "get_memory_grants"),
        (typeof(McpIoTools), "get_file_io_stats"),
        (typeof(McpPerfmonTools), "get_perfmon_stats"),
        (typeof(McpConfigTools), "get_server_config"),
        (typeof(McpConfigTools), "get_database_config"),
        (typeof(McpConfigTools), "get_database_scoped_config"),
        (typeof(McpConfigTools), "get_query_store_health"),
        (typeof(McpConfigTools), "get_trace_flags"),
        (typeof(McpPlanCacheSchedulerTools), "get_plan_cache_bloat"),
        (typeof(McpPlanCacheSchedulerTools), "get_cpu_scheduler_pressure"),
        (typeof(McpLatchSpinlockTools), "get_latch_stats"),
        (typeof(McpLatchSpinlockTools), "get_spinlock_stats"),
        /* #3653: the four reads that stamped themselves as collection_time before #3637's vocabulary and were
           carried by Darling's census as a named allowance; renamed on both SKUs, roster rows on both. */
        (typeof(McpServerInfoTools), "get_database_sizes"),
        (typeof(McpJobTools), "get_running_jobs"),
        (typeof(McpServerInfoTools), "get_server_properties"),
        (typeof(McpSessionTools), "get_session_stats"),
    ];

    [Fact]
    public void EveryLatestTool_SaysLatestIsATime_AndNamesCapturedAt()
    {
        foreach (var (type, name) in LatestTools)
        {
            var description = ToolMethod(type, name).GetCustomAttribute<DescriptionAttribute>()!.Description;
            Assert.Contains("captured_at", description, StringComparison.Ordinal);
            Assert.True(
                description.Contains("LATEST IS A TIME", StringComparison.Ordinal) || description.Contains("TWO READS UNDER ONE WINDOW", StringComparison.Ordinal),
                $"{name}: the description never says the read is a moment, not a window");
        }
    }

    /// <summary>A latest tool that takes <c>hours_back</c> says which of the two honest things it means, in the
    /// words Darling's census pins, and takes the anchor to measure <c>age_seconds</c> against.</summary>
    [Fact]
    public void EveryLatestToolWithAWindowParameter_SaysWhatTheWindowMeans_AndTakesTheAnchor()
    {
        foreach (var (type, name) in LatestTools)
        {
            var parameters = ToolMethod(type, name).GetParameters();
            var hours = parameters.SingleOrDefault(p => p.Name == "hours_back");
            if (hours is null)
            {
                Assert.DoesNotContain("as_of", parameters.Select(p => p.Name));
                continue;
            }

            var words = hours.GetCustomAttribute<DescriptionAttribute>()!.Description;
            Assert.True(
                words.Contains("search for the latest snapshot", StringComparison.Ordinal)
                || words.Contains("window[] aggregates every snapshot in these hours", StringComparison.Ordinal),
                $"{name}: hours_back is described as neither the search span nor the read window: \"{words}\"");
            Assert.Contains("as_of", parameters.Select(p => p.Name));
        }
    }

    /// <summary>The two window reads, pinned on the dialect: the peak's instant from a <c>DISTINCT ON</c> over
    /// the SAME windowed rows, the deltas SUMmed, and no interval arithmetic and no literal cap.</summary>
    [Theory]
    [InlineData(nameof(LocalDataService.ResourceSemaphoreWindowSql))]
    [InlineData(nameof(LocalDataService.MemoryGrantsWindowSql))]
    public void MemoryGrantWindowReads_AggregateEverySnapshot_AndNameThePeaksInstant(string sqlName)
    {
        var sql = sqlName == nameof(LocalDataService.ResourceSemaphoreWindowSql)
            ? LocalDataService.ResourceSemaphoreWindowSql
            : LocalDataService.MemoryGrantsWindowSql;
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS snapshots_in_window", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(waiter_count) AS peak_waiter_count", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(timeout_error_count_delta) AS timeout_errors_in_window", sql, StringComparison.Ordinal);
        Assert.Contains("SELECT DISTINCT ON", sql, StringComparison.Ordinal);
        Assert.Contains("waiter_count DESC, collection_time DESC", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\bLIMIT\b", sql);
        Assert.DoesNotContain("sample_interval_seconds", sql, StringComparison.Ordinal);
    }

    /// <summary>Whole seconds from stamp to anchor; never negative; Kind-blind — the twin of Darling's
    /// <c>LatestSnapshotStamp.AgeSeconds</c>, held to the same values.</summary>
    [Fact]
    public void AgeSeconds_IsTheWholeSecondDistanceToTheAnchor_AndNeverNegative()
    {
        var stamp = new DateTime(2026, 9, 18, 12, 0, 0, DateTimeKind.Unspecified);
        var anchor = new DateTime(2026, 9, 18, 12, 5, 0, DateTimeKind.Utc);
        Assert.Equal(300L, McpLatestSnapshotStamp.AgeSeconds(stamp, anchor));
        Assert.Equal(300L, McpLatestSnapshotStamp.AgeSeconds(stamp, anchor.AddTicks(4_000_000)));
        Assert.Equal(301L, McpLatestSnapshotStamp.AgeSeconds(stamp, anchor.AddTicks(6_000_000)));
        Assert.Equal(0L, McpLatestSnapshotStamp.AgeSeconds(anchor, stamp));
        Assert.Equal(0L, McpLatestSnapshotStamp.AgeSeconds(stamp, stamp));
    }

    /* ───────────────────────── plumbing ───────────────────────── */

    /// <summary>A base instant two hours back, truncated to the second, and an anchor five minutes after it —
    /// the pair every age assertion is an equality against.</summary>
    private static (DateTime Base, string Anchor) Past()
    {
        var now = DateTime.UtcNow;
        var @base = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc).AddHours(-2);
        return (@base, @base.AddMinutes(5).ToString("o"));
    }

    private static MethodInfo ToolMethod(Type type, string toolName) => type
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == toolName);

    private static JsonElement Parse(string json)
    {
        var root = JsonDocument.Parse(json).RootElement.Clone();
        Assert.False(root.TryGetProperty("status", out _), "expected a data-bearing payload, got a status envelope: " + json);
        return root;
    }

    /// <summary>What a seeded UTC instant looks like on the payload: the store holds a naive timestamp, and the
    /// tools emit it with <c>ToString("o")</c> on a <c>Kind = Unspecified</c> value, so no <c>Z</c>.</summary>
    private static string Stamp(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified).ToString("o");

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values)
            cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedGrantAsync(DateTime at, int waiters, long timeoutsDelta, double grantedMb, short semaphore = 0, int pool = 2) => ExecAsync(@"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, resource_semaphore_id, pool_id,
     target_memory_mb, max_target_memory_mb, total_memory_mb, available_memory_mb, granted_memory_mb, used_memory_mb,
     grantee_count, waiter_count, timeout_error_count, forced_grant_count, timeout_error_count_delta, forced_grant_count_delta)
VALUES ($1, $2, $3, $4, $5, $6, 8000, 12000, 8000, $7, $8, $8, 3, $9, 4, 2, $10, 0)",
        _nextId--, Naive(at), _serverId, ServerName, semaphore, pool, 8000 - grantedMb, grantedMb, waiters, timeoutsDelta);

    private Task SeedSchedulerAsync(DateTime at, int runnable) => ExecAsync(@"
INSERT INTO cpu_scheduler_stats
    (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count,
     total_runnable_tasks_count, total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count,
     total_active_request_count, total_queued_request_count, total_blocked_task_count, total_active_parallel_thread_count,
     runnable_percent, worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning, queued_requests_warning,
     total_physical_memory_kb, available_physical_memory_kb, physical_memory_pressure_warning,
     total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
VALUES ($1, $2, $3, $4, 512, 8, 8, $5, 5, 100, 7.5, 40, 12, 2, 20, 12.5, false, true, false, true, 65536000, 32768000, false, 1, 1, 0, false)",
        _nextId--, Naive(at), _serverId, ServerName, runnable);

    private Task SeedLatchAsync(DateTime at, long deltaWaitMs) => ExecAsync(@"
INSERT INTO latch_stats
    (collection_id, collection_time, server_id, server_name, latch_class, waiting_requests_count, wait_time_ms, max_wait_time_ms,
     delta_waiting_requests_count, delta_wait_time_ms, delta_max_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'ACCESS_METHODS_DATASET_PARENT', 1000, 20100, 50, 100, $5, 5, 60)",
        _nextId--, Naive(at), _serverId, ServerName, deltaWaitMs);

    private Task SeedSpinlockAsync(DateTime at) => ExecAsync(@"
INSERT INTO spinlock_stats
    (collection_id, collection_time, server_id, server_name, spinlock_name, collisions, spins, spins_per_collision, sleep_time, backoffs,
     delta_collisions, delta_spins, delta_sleep_time, delta_backoffs)
VALUES ($1, $2, $3, $4, 'LOCK_HASH', 900000, 5000000, 5.5, 100, 200, 400, 2000, 3, 7)",
        _nextId--, Naive(at), _serverId, ServerName);

    private Task SeedClerkAsync(DateTime at) => ExecAsync(@"
INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1, $2, $3, $4, 'MEMORYCLERK_SQLBUFFERPOOL', 40000)",
        _nextId--, Naive(at), _serverId, ServerName);

    private Task SeedFileIoAsync(DateTime at) => ExecAsync(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'AppDb', 'AppDb_data', 'ROWS', 'D:\AppDb.mdf', 100, 10, 5, 81920, 40960, 50, 10, 60)",
        _nextId--, Naive(at), _serverId, ServerName);

    private Task SeedPerfmonAsync(DateTime at) => ExecAsync(@"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'SQLServer:SQL Statistics', 'Batch Requests/sec', '', 1200000, 600000, 60)",
        _nextId--, Naive(at), _serverId, ServerName);
}
