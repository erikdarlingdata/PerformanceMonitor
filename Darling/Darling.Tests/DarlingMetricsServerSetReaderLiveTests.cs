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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Metrics;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9013: the metrics port's server-set statements read the NEWEST collection per enabled SQL Server from each
/// collect table, inside that table's freshness bound, and never let text from a monitored server reach a label.
/// Every test seeds its own servers, an older collection and the newest one, and asserts the newest one's values.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMetricsServerSetReaderLiveTests
{
    /* NEGATIVE, per the live-postgres family's convention. */
    private const int CpuId = -913_001;
    private const int SchedulerId = -913_002;
    private const int PerfmonId = -913_003;
    private const int SessionsId = -913_004;
    private const int MemoryId = -913_005;
    private const int GrantsId = -913_006;
    private const int FileIoId = -913_007;
    private const int VolumeId = -913_008;
    private const int WaitsId = -913_009;
    private const int TempdbId = -913_010;
    private const int StaleId = -913_011;
    private const int VolumeFreshId = -913_012;
    private const int VolumeOldId = -913_013;
    private const int EmptyId = -913_014;
    private const int PostgresId = -913_015;
    private const int EverythingId = -913_016;
    private const int SlowWaitsRecentId = -913_017;
    private const int SlowWaitsOldId = -913_018;
    private const int UnscheduledWaitsId = -913_019;
    private const int FleetScheduledId = -913_020;
    private const int ServerRowWinsId = -913_021;
    private const int DisabledOldId = -913_022;
    private const int DisabledRecentId = -913_023;
    private const int EnabledSlowId = -913_024;
    private const int FleetDisabledNullServerId = -913_025;
    private const int FleetDisabledServerEnabledId = -913_026;
    private const int FleetDisabledOnlyId = -913_027;

    private const string SecretDb = "SECRET_DB_9013";
    private const string TopApp = "TOP_APP_9013";
    private const string TopHost = "TOP_HOST_9013";

    private static readonly int[] SentinelIds =
    [
        CpuId, SchedulerId, PerfmonId, SessionsId, MemoryId, GrantsId, FileIoId, VolumeId, WaitsId, TempdbId,
        StaleId, VolumeFreshId, VolumeOldId, EmptyId, PostgresId, EverythingId, SlowWaitsRecentId, SlowWaitsOldId,
        UnscheduledWaitsId, DisabledOldId, DisabledRecentId, EnabledSlowId,
    ];

    private static readonly string[] Tables =
    [
        "cpu_utilization_stats", "cpu_scheduler_stats", "perfmon_stats", "session_summary_stats", "memory_stats",
        "memory_grant_stats", "file_io_stats", "database_size_stats", "wait_stats", "tempdb_stats",
        "config.config_collector_schedules", "servers",
    ];

    [Fact]
    public Task Cpu_NewestSample_AndOtherNullIsAbsent() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, CpuId, MonitoredEngineKind.SqlServer, ct);
        await InsertCpuAsync(c, CpuId, now.AddMinutes(-2), 30, 4, ct);
        await InsertCpuAsync(c, CpuId, now.AddMinutes(-1), 55, null, ct);

        var values = await ReadForAsync(c, now, CpuId, ct);

        Assert.Equal(55, Single(values, "darling_cpu_sql_percent"));
        Assert.DoesNotContain(values, v => v.Metric == "darling_cpu_other_percent");
    });

    [Fact]
    public Task Scheduler_SumsTheSplitRowsOfTheNewestCollection() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, SchedulerId, MonitoredEngineKind.SqlServer, ct);
        await InsertRowAsync(c, "cpu_scheduler_stats", SchedulerId, now.AddMinutes(-2), ct, ("total_runnable_tasks_count", 50));
        await InsertRowAsync(c, "cpu_scheduler_stats", SchedulerId, now.AddMinutes(-1), ct, ("total_runnable_tasks_count", 3));
        await InsertRowAsync(c, "cpu_scheduler_stats", SchedulerId, now.AddMinutes(-1), ct, ("total_runnable_tasks_count", 4));

        var values = await ReadForAsync(c, now, SchedulerId, ct);

        Assert.Equal(7, Single(values, "darling_scheduler_runnable_tasks"));
    });

    [Fact]
    public Task Perfmon_RatesUseTotalAndMaxInterval_GaugeUsesValue() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, PerfmonId, MonitoredEngineKind.SqlServer, ct);
        var old = now.AddMinutes(-2);
        var newest = now.AddMinutes(-1);
        await InsertPerfmonAsync(c, PerfmonId, old, "SQLServer:Databases", "Transactions/sec", "_Total", 0, 6000, 60, ct);
        await InsertPerfmonAsync(c, PerfmonId, old, "SQLServer:SQL Statistics", "Batch Requests/sec", "", 0, 6000, 60, ct);

        await InsertPerfmonAsync(c, PerfmonId, newest, "SQLServer:Databases", "Transactions/sec", "db1", 0, 600, 60, ct);
        await InsertPerfmonAsync(c, PerfmonId, newest, "SQLServer:Databases", "Transactions/sec", "_Total", 0, 900, 60, ct);
        /* An instance seen for the first time: the interval is unknown, so MAX keeps 60. */
        await InsertPerfmonAsync(c, PerfmonId, newest, "SQLServer:Databases", "Transactions/sec", "db2", 0, 0, 0, ct);
        await InsertPerfmonAsync(c, PerfmonId, newest, "SQLServer:SQL Statistics", "Batch Requests/sec", "", 0, 120, 60, ct);
        await InsertPerfmonAsync(c, PerfmonId, newest, "SQLServer:General Statistics", "Processes blocked", "", 3, null, null, ct);
        await InsertPerfmonAsync(c, PerfmonId, newest, "SQLServer:Buffer Manager", "Lazy writes/sec", "", 0, 0, 0, ct);

        var values = await ReadForAsync(c, now, PerfmonId, ct);

        Assert.Equal(15, Single(values, "darling_transactions_per_second"));
        Assert.Equal(2, Single(values, "darling_batch_requests_per_second"));
        Assert.Equal(3, Single(values, "darling_processes_blocked"));
        Assert.DoesNotContain(values, v => v.Metric == "darling_lazy_writes_per_second");
    });

    [Fact]
    public Task Sessions_FourStates_AndNoTopColumns() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, SessionsId, MonitoredEngineKind.SqlServer, ct);
        await InsertSessionsAsync(c, SessionsId, now.AddMinutes(-10), 100, ct);
        await InsertSessionsAsync(c, SessionsId, now.AddMinutes(-5), 1, ct);

        var values = await ReadForAsync(c, now, SessionsId, ct);

        var sessions = values.Where(v => v.Metric == "darling_sessions").ToList();
        Assert.Equal(4, sessions.Count);
        Assert.Equal(
            new Dictionary<string, double> { ["running"] = 1, ["sleeping"] = 2, ["background"] = 3, ["dormant"] = 4 },
            sessions.ToDictionary(v => Assert.Single(v.Labels, l => l.Key == "state").Value, v => v.Value));
        Assert.DoesNotContain(values.SelectMany(v => v.Labels), l => l.Value == TopApp || l.Value == TopHost);
    });

    [Fact]
    public Task Memory_FourValues() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, MemoryId, MonitoredEngineKind.SqlServer, ct);
        await InsertMemoryAsync(c, MemoryId, now.AddMinutes(-2), 1, ct);
        await InsertMemoryAsync(c, MemoryId, now.AddMinutes(-1), 10, ct);

        var values = await ReadForAsync(c, now, MemoryId, ct);

        Assert.Equal(1000, Single(values, "darling_memory_total_server_mb"));
        Assert.Equal(2000, Single(values, "darling_memory_target_server_mb"));
        Assert.Equal(300, Single(values, "darling_memory_plan_cache_mb"));
        Assert.Equal(400, Single(values, "darling_memory_os_available_mb"));
    });

    [Fact]
    public Task Grants_RegularSemaphoreOnly_SummedOverPools() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, GrantsId, MonitoredEngineKind.SqlServer, ct);
        await InsertGrantAsync(c, GrantsId, now.AddMinutes(-2), 0, 1, 100, 50, ct);
        var newest = now.AddMinutes(-1);
        await InsertGrantAsync(c, GrantsId, newest, 0, 1, 1, 10, ct);
        await InsertGrantAsync(c, GrantsId, newest, 0, 2, 2, 20, ct);
        await InsertGrantAsync(c, GrantsId, newest, 1, 1, 5, 40, ct);

        var values = await ReadForAsync(c, now, GrantsId, ct);

        Assert.Equal(3, Single(values, "darling_memory_grants_pending"));
        Assert.Equal(30, Single(values, "darling_memory_granted_mb"));
    });

    [Fact]
    public Task FileIo_GroupsByTypeAndScope_DatabaseNameNeverLeaves() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, FileIoId, MonitoredEngineKind.SqlServer, ct);
        const long MiB = 1_048_576;
        await InsertFileIoAsync(c, FileIoId, now.AddMinutes(-2), SecretDb, "ROWS", 1000, 1000, 1000, 1000, 9000, 9000, 60, ct);

        var newest = now.AddMinutes(-1);
        await InsertFileIoAsync(c, FileIoId, newest, SecretDb, "ROWS", 10, 5, 60 * MiB, 0, 50, 10, 60, ct);
        await InsertFileIoAsync(c, FileIoId, newest, SecretDb, "FILESTREAM", 10, 0, 0, 0, 150, 0, 60, ct);
        await InsertFileIoAsync(c, FileIoId, newest, SecretDb, "LOG", 2, 30, 0, 120 * MiB, 4, 90, 60, ct);
        await InsertFileIoAsync(c, FileIoId, newest, "tempdb", "ROWS", 6, 6, 0, 0, 12, 60, 60, ct);
        /* No delta is knowable for this file: it must add nothing to (data, user). */
        await InsertFileIoAsync(c, FileIoId, newest, SecretDb, "ROWS", 7777, 7777, 7777, 7777, 7777, 7777, 0, ct);

        var values = await ReadForAsync(c, now, FileIoId, ct);

        Assert.Equal(10, Disk(values, "darling_disk_read_latency_ms", "data", "user"));
        Assert.Equal(2, Disk(values, "darling_disk_write_latency_ms", "data", "user"));
        Assert.Equal(1, Disk(values, "darling_disk_read_mb_per_second", "data", "user"));
        Assert.Equal(20.0 / 60, Disk(values, "darling_disk_reads_per_second", "data", "user"), 6);
        Assert.Equal(2, Disk(values, "darling_disk_read_latency_ms", "log", "user"));
        Assert.Equal(3, Disk(values, "darling_disk_write_latency_ms", "log", "user"));
        Assert.Equal(2, Disk(values, "darling_disk_write_mb_per_second", "log", "user"));
        Assert.Equal(0.5, Disk(values, "darling_disk_writes_per_second", "log", "user"));
        Assert.Equal(2, Disk(values, "darling_disk_read_latency_ms", "data", "tempdb"));
        Assert.Equal(10, Disk(values, "darling_disk_write_latency_ms", "data", "tempdb"));

        var groups = values
            .Select(v => (Label(v, "file_type"), Label(v, "scope")))
            .Distinct()
            .OrderBy(g => g)
            .ToList();
        Assert.Equal([("data", "tempdb"), ("data", "user"), ("log", "user")], groups);
        Assert.DoesNotContain(values.SelectMany(v => v.Labels), l => l.Value == SecretDb);
    });

    [Fact]
    public Task Volume_MinFreePercent_OverDedupedVolumes() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, VolumeId, MonitoredEngineKind.SqlServer, ct);
        await InsertSizeAsync(c, VolumeId, now.AddMinutes(-90), "D:\\", @"D:\data\a.mdf", 1000, 10, ct);

        var newest = now.AddMinutes(-30);
        await InsertSizeAsync(c, VolumeId, newest, "D:\\", @"D:\data\a.mdf", 1000, 400, ct);
        await InsertSizeAsync(c, VolumeId, newest, "D:\\", @"D:\data\a_log.ldf", 1000, 400, ct);
        await InsertSizeAsync(c, VolumeId, newest, null, "/var/opt/mssql/data/b.mdf", 2000, 500, ct);

        var values = await ReadForAsync(c, now, VolumeId, ct);

        Assert.Equal(25, Single(values, "darling_disk_volume_min_free_percent"), 6);
    });

    [Fact]
    public Task Waits_PerCategory_PerSecond_AndSignalPercent() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, WaitsId, MonitoredEngineKind.SqlServer, ct);
        await InsertWaitAsync(c, WaitsId, now.AddMinutes(-2), "SOS_SCHEDULER_YIELD", 60000, 0, 60, ct);

        var newest = now.AddMinutes(-1);
        await InsertWaitAsync(c, WaitsId, newest, "SOS_SCHEDULER_YIELD ", 600, 300, 60, ct);
        await InsertWaitAsync(c, WaitsId, newest, "LCK_M_X", 1200, 0, 60, ct);
        await InsertWaitAsync(c, WaitsId, newest, "WRITELOG", 9999, 9999, 0, ct);

        var values = await ReadForAsync(c, now, WaitsId, ct);

        var waits = values.Where(v => v.Metric == "darling_wait_ms_per_second")
            .ToDictionary(v => Label(v, "category"), v => v.Value);
        Assert.Equal(new Dictionary<string, double> { ["cpu"] = 10, ["locks"] = 20 }, waits);
        Assert.Equal(16.67, Single(values, "darling_signal_wait_percent"), 0.01);
    });

    [Fact]
    public Task Tempdb_ThreeKinds_UnallocatedAndLog() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, TempdbId, MonitoredEngineKind.SqlServer, ct);
        await InsertTempdbAsync(c, TempdbId, now.AddMinutes(-2), 1, 50, ct);
        await InsertTempdbAsync(c, TempdbId, now.AddMinutes(-1), 10, 12.5, ct);

        var values = await ReadForAsync(c, now, TempdbId, ct);

        var used = values.Where(v => v.Metric == "darling_tempdb_used_mb").ToDictionary(v => Label(v, "kind"), v => v.Value);
        Assert.Equal(
            new Dictionary<string, double> { ["user_objects"] = 10, ["internal_objects"] = 20, ["version_store"] = 30 },
            used);
        Assert.Equal(40, Single(values, "darling_tempdb_unallocated_mb"));
        Assert.Equal(12.5, Single(values, "darling_tempdb_log_used_percent"));
    });

    [Fact]
    public Task StaleFamily_IsAbsent_FreshFamilyOnTheSameServerStays() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, StaleId, MonitoredEngineKind.SqlServer, ct);
        await InsertMemoryAsync(c, StaleId, now.AddMinutes(-20), 10, ct);
        await InsertCpuAsync(c, StaleId, now.AddMinutes(-1), 40, 2, ct);

        var values = await ReadForAsync(c, now, StaleId, ct);

        Assert.DoesNotContain(values, v => v.Metric.StartsWith("darling_memory_", StringComparison.Ordinal));
        Assert.Equal(40, Single(values, "darling_cpu_sql_percent"));
    });

    [Fact]
    public Task HourlyVolume_Within3h_IsPresent_At4h_IsAbsent() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, VolumeFreshId, MonitoredEngineKind.SqlServer, ct);
        await InsertServerAsync(c, VolumeOldId, MonitoredEngineKind.SqlServer, ct);
        await InsertSizeAsync(c, VolumeFreshId, now.AddMinutes(-170), "D:\\", @"D:\data\a.mdf", 1000, 100, ct);
        await InsertSizeAsync(c, VolumeOldId, now.AddHours(-4), "D:\\", @"D:\data\a.mdf", 1000, 100, ct);

        Assert.Equal(10, Single(await ReadForAsync(c, now, VolumeFreshId, ct), "darling_disk_volume_min_free_percent"), 6);
        Assert.Empty(await ReadForAsync(c, now, VolumeOldId, ct));
    });

    [Fact]
    public Task WaitsScheduledEvery30Minutes_StayForThreeIntervals_AndGoAfterThem() => RunAsync(async (c, now, ct) =>
    {
        foreach (var (serverId, age) in new[] { (SlowWaitsRecentId, 40), (SlowWaitsOldId, 100) })
        {
            await InsertServerAsync(c, serverId, MonitoredEngineKind.SqlServer, ct);
            await InsertScheduleAsync(c, serverId, "wait_stats", 30, ct);
            await InsertWaitAsync(c, serverId, now.AddMinutes(-age), "LCK_M_X", 60, 6, 60, ct);
        }

        Assert.Equal(1, Single(await ReadForAsync(c, now, SlowWaitsRecentId, ct), "darling_wait_ms_per_second"));
        Assert.Empty(await ReadForAsync(c, now, SlowWaitsOldId, ct));
    });

    [Fact]
    public Task WithNoScheduleRows_TheOneMinuteFloorHolds() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, UnscheduledWaitsId, MonitoredEngineKind.SqlServer, ct);
        await InsertWaitAsync(c, UnscheduledWaitsId, now.AddMinutes(-20), "LCK_M_X", 60, 6, 60, ct);

        Assert.Empty(await ReadForAsync(c, now, UnscheduledWaitsId, ct));
    });

    [Fact]
    public async Task AFleetWideSchedule_Applies_AndAServerRowWinsOverIt()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics server-set tests.");

        /* #9013: a fleet-wide schedule row would apply to every server on the shared store, so this fact mints its own
           scratch database through ScratchPostgres and never touches another test's rows. */
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);

        var now = LiveClock.Now();
        await InsertScheduleAsync(c, null, "file_io_stats", 30, ct);
        foreach (var serverId in new[] { FleetScheduledId, ServerRowWinsId })
        {
            await InsertServerAsync(c, serverId, MonitoredEngineKind.SqlServer, ct);
            await InsertFileIoAsync(c, serverId, now.AddMinutes(-40), "db1", "ROWS", 6, 0, 0, 0, 12, 0, 60, ct);
        }
        await InsertScheduleAsync(c, ServerRowWinsId, "file_io_stats", 1, ct);

        Assert.Equal(2, Disk(await ReadForAsync(c, now, FleetScheduledId, ct), "darling_disk_read_latency_ms", "data", "user"));
        Assert.Empty(await ReadForAsync(c, now, ServerRowWinsId, ct));
    }

    /// <summary>
    /// #9139: a collector that is switched off collects nothing more, so its last reading is bounded by the floor alone.
    /// <c>database_size_stats</c> on a 360 minute schedule has a 3 hour floor and an 18 hour bound while enabled.
    /// </summary>
    [Fact]
    public Task ADisabledServerSchedule_BoundsTheReadingByTheFloor_AnEnabledOneByThreeIntervals() => RunAsync(async (c, now, ct) =>
    {
        foreach (var id in new[] { DisabledOldId, DisabledRecentId, EnabledSlowId })
        {
            await InsertServerAsync(c, id, MonitoredEngineKind.SqlServer, ct);
        }

        await InsertScheduleRowAsync(c, DisabledOldId, "database_size_stats", 360, false, ct);
        await InsertScheduleRowAsync(c, DisabledRecentId, "database_size_stats", 360, false, ct);
        await InsertScheduleRowAsync(c, EnabledSlowId, "database_size_stats", 360, true, ct);
        await InsertSizeAsync(c, DisabledOldId, now.AddHours(-4), "D:\\", @"D:\data\a.mdf", 1000, 400, ct);
        await InsertSizeAsync(c, DisabledRecentId, now.AddHours(-2), "D:\\", @"D:\data\a.mdf", 1000, 400, ct);
        await InsertSizeAsync(c, EnabledSlowId, now.AddHours(-4), "D:\\", @"D:\data\a.mdf", 1000, 400, ct);

        Assert.Empty(await ReadForAsync(c, now, DisabledOldId, ct));
        Assert.Equal(40, Single(await ReadForAsync(c, now, DisabledRecentId, ct), "darling_disk_volume_min_free_percent"), 6);
        Assert.Equal(40, Single(await ReadForAsync(c, now, EnabledSlowId, ct), "darling_disk_volume_min_free_percent"), 6);
    });

    /// <summary>
    /// #9139: <c>enabled</c> resolves as the scheduler resolves it, column by column: the server row, else the fleet row,
    /// else the shipped default. A server row that sets only the interval (a NULL <c>enabled</c>) does not re-enable a
    /// collector the fleet row switched off, and a server row that says enabled wins over a disabled fleet row.
    /// </summary>
    [Fact]
    public async Task EnabledResolvesServerRowThenFleetRow_ColumnByColumn()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics server-set tests.");

        /* A fleet-wide row applies to every server on the shared store, so this fact mints its own scratch database. */
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);

        var now = LiveClock.Now();
        await InsertScheduleRowAsync(c, null, "database_size_stats", 360, false, ct);
        foreach (var serverId in new[] { FleetDisabledNullServerId, FleetDisabledServerEnabledId, FleetDisabledOnlyId })
        {
            await InsertServerAsync(c, serverId, MonitoredEngineKind.SqlServer, ct);
            await InsertSizeAsync(c, serverId, now.AddHours(-4), "D:\\", @"D:\data\a.mdf", 1000, 400, ct);
        }

        await InsertScheduleRowAsync(c, FleetDisabledNullServerId, "database_size_stats", 360, null, ct);
        await InsertScheduleRowAsync(c, FleetDisabledServerEnabledId, "database_size_stats", 360, true, ct);

        Assert.Empty(await ReadForAsync(c, now, FleetDisabledOnlyId, ct));
        Assert.Empty(await ReadForAsync(c, now, FleetDisabledNullServerId, ct));
        Assert.Equal(40, Single(await ReadForAsync(c, now, FleetDisabledServerEnabledId, ct), "darling_disk_volume_min_free_percent"), 6);
    }

    [Fact]
    public Task ServerWithNoRows_ReturnsNothing_ForIt() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, EmptyId, MonitoredEngineKind.SqlServer, ct);

        Assert.Empty(await ReadForAsync(c, now, EmptyId, ct));
    });

    [Fact]
    public Task PostgresTarget_IsNotRead() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, PostgresId, MonitoredEngineKind.Postgres, ct);
        await SeedEverythingAsync(c, PostgresId, now.AddMinutes(-1), ct);

        Assert.Empty(await ReadForAsync(c, now, PostgresId, ct));
    });

    [Fact]
    public Task EveryValue_UsesACatalogMetric_ItsLabelNames_AndFixedLabelValues() => RunAsync(async (c, now, ct) =>
    {
        await InsertServerAsync(c, EverythingId, MonitoredEngineKind.SqlServer, ct);
        await SeedEverythingAsync(c, EverythingId, now.AddMinutes(-1), ct);

        var all = await DarlingMetricsServerSetReader.ReadAsync(c, now, ct);
        var ours = all.Where(v => v.ServerId == EverythingId).ToList();

        /* Every one of the ten statements returned something for the fully seeded server. */
        Assert.Equal(39, ours.Count);
        foreach (var value in all)
        {
            Assert.True(DarlingMetricCatalog.Metrics.TryGetValue(value.Metric, out var entry), value.Metric);
            var expectedLabels = entry.Labels.Where(l => l != DarlingMetricCatalog.ServerLabel).OrderBy(l => l, StringComparer.Ordinal);
            Assert.Equal(expectedLabels, value.Labels.Select(l => l.Key).OrderBy(l => l, StringComparer.Ordinal));
            foreach (var label in value.Labels)
            {
                Assert.Contains(label.Value, DarlingMetricCatalog.FixedLabelValues[label.Key]);
            }
        }
    });

    private static async Task RunAsync(Func<NpgsqlConnection, DateTime, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics server-set tests.");

        var ct = TestContext.Current.CancellationToken;
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await body(connection, LiveClock.Now(), ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(builder.ConnectionString, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    private static async Task<List<ServerSetValue>> ReadForAsync(NpgsqlConnection connection, DateTime now, int serverId, CancellationToken ct)
    {
        var all = await DarlingMetricsServerSetReader.ReadAsync(connection, now, ct);
        return all.Where(v => v.ServerId == serverId).ToList();
    }

    private static double Single(IEnumerable<ServerSetValue> values, string metric) =>
        Assert.Single(values, v => v.Metric == metric).Value;

    private static string Label(ServerSetValue value, string key) => Assert.Single(value.Labels, l => l.Key == key).Value;

    private static double Disk(IEnumerable<ServerSetValue> values, string metric, string fileType, string scope) =>
        Assert.Single(values, v => v.Metric == metric && Label(v, "file_type") == fileType && Label(v, "scope") == scope).Value;

    private static async Task SeedEverythingAsync(NpgsqlConnection c, int serverId, DateTime at, CancellationToken ct)
    {
        await InsertCpuAsync(c, serverId, at, 20, 5, ct);
        await InsertRowAsync(c, "cpu_scheduler_stats", serverId, at, ct, ("total_runnable_tasks_count", 2));
        foreach (var (obj, counter, instance) in new[]
        {
            ("SQLServer:SQL Statistics", "Batch Requests/sec", ""), ("SQLServer:SQL Statistics", "SQL Compilations/sec", ""),
            ("SQLServer:SQL Statistics", "SQL Re-Compilations/sec", ""), ("SQLServer:Databases", "Transactions/sec", "_Total"),
            ("SQLServer:Locks", "Lock Waits/sec", "_Total"), ("SQLServer:Buffer Manager", "Lazy writes/sec", ""),
            ("SQLServer:Buffer Manager", "Page reads/sec", ""), ("SQLServer:Buffer Manager", "Page writes/sec", ""),
            ("SQLServer:Buffer Manager", "Free list stalls/sec", ""), ("SQLServer:Databases", "Log Flushes/sec", "_Total"),
            ("SQLServer:Databases", "Log Bytes Flushed/sec", "_Total"),
        })
        {
            await InsertPerfmonAsync(c, serverId, at, obj, counter, instance, 0, 60, 60, ct);
        }
        await InsertPerfmonAsync(c, serverId, at, "SQLServer:General Statistics", "Processes blocked", "", 1, null, null, ct);
        await InsertSessionsAsync(c, serverId, at, 1, ct);
        await InsertMemoryAsync(c, serverId, at, 1, ct);
        await InsertGrantAsync(c, serverId, at, 0, 1, 1, 1, ct);
        await InsertFileIoAsync(c, serverId, at, SecretDb, "ROWS", 1, 1, 1, 1, 1, 1, 60, ct);
        await InsertSizeAsync(c, serverId, at, "D:\\", @"D:\data\a.mdf", 100, 50, ct);
        await InsertWaitAsync(c, serverId, at, "LCK_M_X", 60, 6, 60, ct);
        await InsertTempdbAsync(c, serverId, at, 1, 1, ct);
    }

    private static async Task InsertServerAsync(NpgsqlConnection connection, int serverId, string engineKind, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind)
VALUES ($1, $2, $2, TRUE, 0, $3)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NameOf(serverId));
        command.Parameters.AddWithValue(engineKind);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static string NameOf(int serverId) => "metrics-server-set-" + serverId.ToString(CultureInfo.InvariantCulture);

    /// <summary>One collect row: the four key columns, then the payload columns given.</summary>
    private static async Task InsertRowAsync(
        NpgsqlConnection connection, string table, int serverId, DateTime collectionTime, CancellationToken ct,
        params (string Column, object? Value)[] payload)
    {
        var columns = string.Join(", ", payload.Select(p => p.Column));
        var placeholders = string.Join(", ", payload.Select((_, i) => "$" + (i + 5).ToString(CultureInfo.InvariantCulture)));
        using var command = new NpgsqlCommand(
            $"INSERT INTO {table} (collection_id, collection_time, server_id, server_name, {columns}) VALUES ($1, $2, $3, $4, {placeholders})",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NameOf(serverId));
        foreach (var (_, value) in payload)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value ?? DBNull.Value });
        }
        await command.ExecuteNonQueryAsync(ct);
    }

    private static Task InsertCpuAsync(NpgsqlConnection c, int serverId, DateTime at, int sqlCpu, int? otherCpu, CancellationToken ct) =>
        InsertRowAsync(c, "cpu_utilization_stats", serverId, at, ct,
            ("sample_time", DateTime.SpecifyKind(at, DateTimeKind.Unspecified)),
            ("sqlserver_cpu_utilization", sqlCpu),
            ("other_process_cpu_utilization", otherCpu));

    private static Task InsertPerfmonAsync(
        NpgsqlConnection c, int serverId, DateTime at, string objectName, string counter, string instance,
        long cntrValue, long? delta, int? interval, CancellationToken ct) =>
        InsertRowAsync(c, "perfmon_stats", serverId, at, ct,
            ("object_name", objectName), ("counter_name", counter), ("instance_name", instance),
            ("cntr_value", cntrValue), ("delta_cntr_value", delta), ("sample_interval_seconds", interval),
            ("cntr_type", delta is null ? 65792 : 272696576));

    private static Task InsertSessionsAsync(NpgsqlConnection c, int serverId, DateTime at, int scale, CancellationToken ct) =>
        InsertRowAsync(c, "session_summary_stats", serverId, at, ct,
            ("running_sessions", 1 * scale), ("sleeping_sessions", 2 * scale),
            ("background_sessions", 3 * scale), ("dormant_sessions", 4 * scale),
            ("top_application_name", TopApp), ("top_host_name", TopHost));

    private static Task InsertMemoryAsync(NpgsqlConnection c, int serverId, DateTime at, int scale, CancellationToken ct) =>
        InsertRowAsync(c, "memory_stats", serverId, at, ct,
            ("total_server_memory_mb", 100m * scale), ("target_server_memory_mb", 200m * scale),
            ("plan_cache_mb", 30m * scale), ("available_physical_memory_mb", 40m * scale));

    private static Task InsertGrantAsync(
        NpgsqlConnection c, int serverId, DateTime at, short semaphore, int pool, int waiters, decimal grantedMb, CancellationToken ct) =>
        InsertRowAsync(c, "memory_grant_stats", serverId, at, ct,
            ("resource_semaphore_id", semaphore), ("pool_id", pool), ("waiter_count", waiters), ("granted_memory_mb", grantedMb));

    private static Task InsertFileIoAsync(
        NpgsqlConnection c, int serverId, DateTime at, string database, string fileType,
        long reads, long writes, long readBytes, long writeBytes, long stallRead, long stallWrite, int interval, CancellationToken ct) =>
        InsertRowAsync(c, "file_io_stats", serverId, at, ct,
            ("database_name", database), ("file_name", database + "_file"), ("file_type", fileType),
            ("physical_name", "C:\\" + database + ".mdf"),
            ("delta_reads", reads), ("delta_writes", writes), ("delta_read_bytes", readBytes), ("delta_write_bytes", writeBytes),
            ("delta_stall_read_ms", stallRead), ("delta_stall_write_ms", stallWrite), ("sample_interval_seconds", interval));

    private static Task InsertSizeAsync(
        NpgsqlConnection c, int serverId, DateTime at, string? mountPoint, string physicalName,
        decimal totalMb, decimal freeMb, CancellationToken ct) =>
        InsertRowAsync(c, "database_size_stats", serverId, at, ct,
            ("database_name", SecretDb), ("physical_name", physicalName), ("volume_mount_point", mountPoint),
            ("volume_total_mb", totalMb), ("volume_free_mb", freeMb));

    private static Task InsertWaitAsync(
        NpgsqlConnection c, int serverId, DateTime at, string waitType, long waitMs, long signalMs, int interval, CancellationToken ct) =>
        InsertRowAsync(c, "wait_stats", serverId, at, ct,
            ("wait_type", waitType), ("wait_time_ms", waitMs + 1), ("delta_wait_time_ms", waitMs),
            ("delta_signal_wait_time_ms", signalMs), ("sample_interval_seconds", interval));

    private static Task InsertTempdbAsync(NpgsqlConnection c, int serverId, DateTime at, int scale, double logPercent, CancellationToken ct) =>
        InsertRowAsync(c, "tempdb_stats", serverId, at, ct,
            ("user_object_reserved_mb", 1m * scale), ("internal_object_reserved_mb", 2m * scale),
            ("version_store_reserved_mb", 3m * scale), ("unallocated_mb", 4m * scale),
            ("log_used_percent", (decimal)logPercent),
            ("holder_program_name", TopApp), ("top_session_id", 99));

    private static async Task InsertScheduleAsync(
        NpgsqlConnection connection, int? serverId, string collectorName, int frequencyMinutes, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes)
VALUES ($1, $2, $3)", connection);
        command.Parameters.Add(new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(frequencyMinutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertScheduleRowAsync(
        NpgsqlConnection connection, int? serverId, string collectorName, int frequencyMinutes, bool? enabled, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, enabled)
VALUES ($1, $2, $3, $4)", connection);
        command.Parameters.Add(new NpgsqlParameter<int?> { TypedValue = serverId, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(frequencyMinutes);
        command.Parameters.Add(new NpgsqlParameter<bool?> { TypedValue = enabled, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Boolean });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", SentinelIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));
        foreach (var table in Tables)
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
