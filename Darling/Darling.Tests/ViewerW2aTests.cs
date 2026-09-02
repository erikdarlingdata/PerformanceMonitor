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
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Overview server-cards reads (W2a viewer copy-parity, copied from Lite's
/// LocalDataService.Overview.cs) against the Darling store contract — no live Postgres. The five
/// per-server summary reads run over the same v_* passthrough views the other tabs read; these pins are
/// the load-bearing clauses (source view, per-server filter, one-hour window on the event/deadlock time,
/// XE→DMV blocking fallback, newest-first LIMIT 1) plus the Pg dialect.
/// </summary>
public sealed class ViewerOverviewSqlTests
{
    [Fact]
    public void SummaryCpuSql_ReadsLatestSample_FromCpuView()
    {
        var sql = ViewerDataService.ServerSummaryCpuSql;
        Assert.Contains("FROM v_cpu_utilization_stats", sql, StringComparison.Ordinal);
        Assert.Contains("sqlserver_cpu_utilization", sql, StringComparison.Ordinal);
        Assert.Contains("other_process_cpu_utilization", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY sample_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SummaryMemorySql_ReadsLatestTotalServerMemoryAndBufferPool_CastToDouble()
    {
        var sql = ViewerDataService.ServerSummaryMemorySql;
        Assert.Contains("FROM v_memory_stats", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(total_server_memory_mb AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(buffer_pool_mb AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SummaryMemoryPressureSql_SumsSemaphoreColumns_AtTheLatestCollection()
    {
        var sql = ViewerDataService.ServerSummaryMemoryPressureSql;
        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(waiter_count)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(timeout_error_count_delta)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(forced_grant_count_delta)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(granted_memory_mb)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        /* Latest collection instant = MAX(collection_time), summed across every pool at that instant. */
        Assert.Contains("collection_time = (SELECT MAX(collection_time) FROM v_memory_grant_stats WHERE server_id = $1)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SummaryThreadsSql_ReadsLatestSchedulerSnapshot_WorkerPressureColumns()
    {
        var sql = ViewerDataService.ServerSummaryThreadsSql;
        Assert.Contains("FROM v_cpu_scheduler_stats", sql, StringComparison.Ordinal);
        Assert.Contains("max_workers_count", sql, StringComparison.Ordinal);
        Assert.Contains("total_current_workers_count", sql, StringComparison.Ordinal);
        Assert.Contains("total_runnable_tasks_count", sql, StringComparison.Ordinal);
        Assert.Contains("total_work_queue_count", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SummaryBlockingSql_CountAndMaxWait_FromBothSources_OverTheWindow()
    {
        var sql = ViewerDataService.ServerSummaryBlockingSql;
        /* Count + worst wait from each source; the caller applies Lite's XE-preferred, DMV-fallback rule. */
        Assert.Contains("COUNT(*)", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(wait_time_ms)", sql, StringComparison.Ordinal);
        /* Newest event ever (unbounded) for the "Last: N ago" detail when the window is clear. */
        Assert.Contains("MAX(event_time)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_blocked_process_reports", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_dmv_blocking_snapshots", sql, StringComparison.Ordinal);
        Assert.Contains("event_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SummaryDeadlockSql_CountsOverTheWindow_AndNewestEver()
    {
        var sql = ViewerDataService.ServerSummaryDeadlockSql;
        Assert.Contains("FROM v_deadlocks", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*)", sql, StringComparison.Ordinal);
        Assert.Contains("deadlock_time >= $2", sql, StringComparison.Ordinal);
        /* Newest deadlock ever (unbounded) for the "Last: N ago" detail. */
        Assert.Contains("MAX(deadlock_time)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SummaryLastCollectionSql_TakesTheNewestCollectionTime()
    {
        var sql = ViewerDataService.ServerSummaryLastCollectionSql;
        Assert.Contains("MAX(collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_collection_log", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SummaryReads_ArePgDialect_PositionalParams_NoBareNow_NoNLiterals()
    {
        foreach (var sql in new[]
        {
            ViewerDataService.ServerSummaryCpuSql,
            ViewerDataService.ServerSummaryMemorySql,
            ViewerDataService.ServerSummaryMemoryPressureSql,
            ViewerDataService.ServerSummaryThreadsSql,
            ViewerDataService.ServerSummaryBlockingSql,
            ViewerDataService.ServerSummaryDeadlockSql,
            ViewerDataService.ServerSummaryLastCollectionSql,
        })
        {
            Assert.DoesNotContain("now(", sql.ToLowerInvariant());
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.Contains("$1", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void SummaryReads_ReferenceColumnsThatExistInTheGeneratedSourceTables()
    {
        var cpu = PgSchemaGenerator.CreateTable(CpuUtilizationCollector.Instance);
        Assert.Contains("sqlserver_cpu_utilization", cpu, StringComparison.Ordinal);
        Assert.Contains("other_process_cpu_utilization", cpu, StringComparison.Ordinal);
        Assert.Contains("sample_time", cpu, StringComparison.Ordinal);

        var memory = PgSchemaGenerator.CreateTable(MemoryStatsCollector.Instance);
        Assert.Contains("total_server_memory_mb", memory, StringComparison.Ordinal);
        Assert.Contains("buffer_pool_mb", memory, StringComparison.Ordinal);

        /* Enrichment: the Threads row's four scheduler columns + the Memory row's semaphore columns. */
        var scheduler = PgSchemaGenerator.CreateTable(CpuSchedulerStatsCollector.Instance);
        Assert.Contains("max_workers_count", scheduler, StringComparison.Ordinal);
        Assert.Contains("total_current_workers_count", scheduler, StringComparison.Ordinal);
        Assert.Contains("total_runnable_tasks_count", scheduler, StringComparison.Ordinal);
        Assert.Contains("total_work_queue_count", scheduler, StringComparison.Ordinal);

        var grants = PgSchemaGenerator.CreateTable(MemoryGrantsCollector.Instance);
        Assert.Contains("waiter_count", grants, StringComparison.Ordinal);
        Assert.Contains("timeout_error_count_delta", grants, StringComparison.Ordinal);
        Assert.Contains("forced_grant_count_delta", grants, StringComparison.Ordinal);
        Assert.Contains("granted_memory_mb", grants, StringComparison.Ordinal);

        /* Blocking duration comes from wait_time_ms on both blocking sources. */
        Assert.Contains("wait_time_ms", PgSchemaGenerator.CreateTable(BlockedProcessReportCollector.Instance), StringComparison.Ordinal);
        Assert.Contains("wait_time_ms", PgSchemaGenerator.CreateTable(DmvBlockingSnapshotCollector.Instance), StringComparison.Ordinal);

        Assert.Contains("event_time", PgSchemaGenerator.CreateTable(BlockedProcessReportCollector.Instance), StringComparison.Ordinal);
        Assert.Contains("event_time", PgSchemaGenerator.CreateTable(DmvBlockingSnapshotCollector.Instance), StringComparison.Ordinal);
        Assert.Contains("deadlock_time", PgSchemaGenerator.CreateTable(DeadlocksCollector.Instance), StringComparison.Ordinal);
    }

    [Fact]
    public void PassthroughViews_ForEverySummarySource_ExistInTheMigrations()
    {
        var allMigrationSql = string.Concat(PgMigrations.Scripts.Select(m => m.Sql));
        foreach (var view in new[]
        {
            "v_cpu_utilization_stats", "v_memory_stats", "v_memory_grant_stats",
            "v_cpu_scheduler_stats", "v_blocked_process_reports",
            "v_dmv_blocking_snapshots", "v_deadlocks", "v_collection_log",
        })
        {
            Assert.Contains(view, allMigrationSql, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// The Overview card view-model's pure display + the viewer's freshness-derived status (#1262). No
/// Postgres: <see cref="ServerSummaryItem.ClassifyFreshness"/> is pure over (last-collection, now), and
/// every display property is a pure format. Verifies the one semantic change — status from collection
/// freshness, not a live ping — maps onto Lite's Online / Warning / Offline card states.
/// </summary>
public sealed class ViewerServerSummaryDisplayTests
{
    private static readonly DateTime Now = new(2026, 7, 3, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void ClassifyFreshness_NoCollection_IsNeverCollected()
    {
        /* Never-collected is DISTINCT from Offline: during a slow fleet bootstrap a queued server
           must not render the red "data stopped" overlay (24-server field incident, 2026-07-17). */
        Assert.Equal(ServerFreshness.NeverCollected, ServerSummaryItem.ClassifyFreshness(null, Now));
    }

    [Theory]
    [InlineData(0, ServerFreshness.Fresh)]      // just collected
    [InlineData(30, ServerFreshness.Fresh)]     // within 2x cadence
    [InlineData(120, ServerFreshness.Fresh)]    // exactly 2x cadence — still fresh
    [InlineData(121, ServerFreshness.Stale)]    // just past 2x cadence
    [InlineData(300, ServerFreshness.Stale)]    // 5 min — stale
    [InlineData(900, ServerFreshness.Stale)]    // exactly 15 min — still stale
    [InlineData(901, ServerFreshness.Offline)]  // just past 15 min
    [InlineData(1200, ServerFreshness.Offline)] // 20 min — offline
    public void ClassifyFreshness_BandsByAge(int ageSeconds, ServerFreshness expected)
    {
        var lastCollection = Now.AddSeconds(-ageSeconds);
        Assert.Equal(expected, ServerSummaryItem.ClassifyFreshness(lastCollection, Now));
    }

    [Fact]
    public void ApplyFreshness_Fresh_IsOnline()
    {
        var item = new ServerSummaryItem { LastCollectionTime = Now.AddSeconds(-30) };
        item.ApplyFreshness(Now);
        Assert.True(item.IsOnline);
        Assert.False(item.HasCollectorErrors);
        Assert.False(item.IsOffline);
        Assert.Equal("Online", item.StatusDisplay);
    }

    [Fact]
    public void ApplyFreshness_Stale_IsWarning()
    {
        var item = new ServerSummaryItem { LastCollectionTime = Now.AddMinutes(-5) };
        item.ApplyFreshness(Now);
        Assert.True(item.IsOnline);
        Assert.True(item.HasCollectorErrors);
        Assert.False(item.IsOffline);
        Assert.Equal("Warning", item.StatusDisplay);
    }

    [Fact]
    public void ApplyFreshness_Offline_ShowsOverlay()
    {
        var offlineByAge = new ServerSummaryItem { LastCollectionTime = Now.AddMinutes(-20) };
        offlineByAge.ApplyFreshness(Now);
        Assert.False(offlineByAge.IsOnline);
        Assert.True(offlineByAge.IsOffline);
        Assert.Equal("Offline", offlineByAge.StatusDisplay);

        /* No collection EVER is not an outage — it renders as the amber awaiting state. */
        var neverCollected = new ServerSummaryItem { LastCollectionTime = null };
        neverCollected.ApplyFreshness(Now);
        Assert.Null(neverCollected.IsOnline);
        Assert.False(neverCollected.IsOffline);
        Assert.True(neverCollected.AwaitingFirstCollection);
        Assert.False(neverCollected.HasCollectorErrors);
        Assert.Equal("Awaiting first collection", neverCollected.StatusDisplay);

        /* And a later real collection clears the awaiting state through the same path. */
        neverCollected.LastCollectionTime = Now.AddSeconds(-30);
        neverCollected.ApplyFreshness(Now);
        Assert.False(neverCollected.AwaitingFirstCollection);
        Assert.True(neverCollected.IsOnline);
        Assert.Equal("Online", neverCollected.StatusDisplay);
    }

    [Theory]
    [InlineData(60.0, null, "60%")]                    // SQL-only when other-process is unavailable
    [InlineData(60.0, 15.0, "75% (SQL 60%)")]          // total prominently, SQL alongside
    public void CpuDisplay_ShowsTotalWithSqlAlongside(double sqlCpu, double? otherCpu, string expected)
    {
        var item = new ServerSummaryItem { CpuPercent = sqlCpu, OtherProcessCpuPercent = otherCpu };
        Assert.Equal(expected, item.CpuDisplay);
    }

    [Fact]
    public void CpuDisplay_NoData_IsDashes()
    {
        Assert.Equal("--", new ServerSummaryItem().CpuDisplay);
    }

    [Fact]
    public void CpuPercentForAlert_IsAlwaysTotal_TheViewerHasNoCpuAlertMode()
    {
        var item = new ServerSummaryItem { CpuPercent = 60, OtherProcessCpuPercent = 25 };
        Assert.Equal(85, item.CpuPercentForAlert);
    }

    [Theory]
    [InlineData(2048.0, "2.0 GB")]
    [InlineData(1536.0, "1.5 GB")]
    public void MemoryDisplay_FormatsGb(double memoryMb, string expected)
    {
        Assert.Equal(expected, new ServerSummaryItem { MemoryMb = memoryMb }.MemoryDisplay);
    }

    [Fact]
    public void MemoryDisplay_NoData_IsDashes()
    {
        Assert.Equal("--", new ServerSummaryItem().MemoryDisplay);
    }

    [Fact]
    public void LastCollectionDisplay_TreatsStoredValueAsUtc_ShownLocal()
    {
        var storedUtc = new DateTime(2026, 7, 3, 3, 30, 45, DateTimeKind.Unspecified);
        var expected = DateTime.SpecifyKind(storedUtc, DateTimeKind.Utc).ToLocalTime().ToString("HH:mm:ss");
        Assert.Equal(expected, new ServerSummaryItem { LastCollectionTime = storedUtc }.LastCollectionDisplay);
    }

    [Fact]
    public void LastCollectionDisplay_NoData_IsNever()
    {
        Assert.Equal("Never", new ServerSummaryItem().LastCollectionDisplay);
    }

    [Fact]
    public void CountDisplays_MirrorLite()
    {
        Assert.Equal("0", new ServerSummaryItem { BlockingCount = 0 }.BlockingDisplay);
        Assert.Equal("3", new ServerSummaryItem { BlockingCount = 3 }.BlockingDisplay);
        Assert.Equal("0", new ServerSummaryItem { DeadlockCount = 0 }.DeadlockDisplay);
        Assert.Equal("2", new ServerSummaryItem { DeadlockCount = 2 }.DeadlockDisplay);
        Assert.True(new ServerSummaryItem { DeadlockCount = 1 }.HasAlerts);
        Assert.False(new ServerSummaryItem().HasAlerts);
    }

    [Fact]
    public void CardBorderBrush_RedForDeadlock_DefaultOtherwise()
    {
        var deadlocked = new ServerSummaryItem { DeadlockCount = 1, LastCollectionTime = Now };
        deadlocked.ApplyFreshness(Now);
        Assert.Equal("#FFE57373", deadlocked.CardBorderBrush.Color.ToString());

        var calm = new ServerSummaryItem { LastCollectionTime = Now };
        calm.ApplyFreshness(Now);
        Assert.Equal("#FF2A2D35", calm.CardBorderBrush.Color.ToString());
    }

    // ── Enrichment: per-metric severity bands (verbatim mirrors of ServerHealthStatus) ──────────────

    [Theory]
    [InlineData(40.0, HealthSeverity.Healthy)]
    [InlineData(79.0, HealthSeverity.Healthy)]
    [InlineData(80.0, HealthSeverity.Warning)]   // ServerHealthStatus.CpuSeverity: >=80 Warning
    [InlineData(94.0, HealthSeverity.Warning)]
    [InlineData(95.0, HealthSeverity.Critical)]  // >=95 Critical
    [InlineData(100.0, HealthSeverity.Critical)]
    public void CpuSeverity_BandsOnTotalCpu(double sqlCpu, HealthSeverity expected)
    {
        Assert.Equal(expected, new ServerSummaryItem { CpuPercent = sqlCpu }.CpuSeverity);
    }

    [Fact]
    public void CpuSeverity_NoData_IsUnknown()
    {
        Assert.Equal(HealthSeverity.Unknown, new ServerSummaryItem().CpuSeverity);
    }

    [Fact]
    public void CpuSeverity_UsesTotalNotSqlOnly()
    {
        // SQL 60 + other 40 = 100 total → Critical, though SQL alone is only Warning.
        Assert.Equal(HealthSeverity.Critical,
            new ServerSummaryItem { CpuPercent = 60, OtherProcessCpuPercent = 40 }.CpuSeverity);
    }

    [Fact]
    public void ThreadsSeverity_NoSnapshot_IsUnknown_DisplayDashes()
    {
        var item = new ServerSummaryItem();  // TotalThreads null (e.g. Azure SQL DB — collector n/a)
        Assert.Equal(HealthSeverity.Unknown, item.ThreadsSeverity);
        Assert.Equal("--", item.ThreadsDisplay);
        Assert.Equal("", item.ThreadsDetail);
        Assert.Null(item.AvailableThreads);
    }

    [Fact]
    public void ThreadsSeverity_WorkQueueStarvation_IsCritical()
    {
        // ServerHealthStatus.ThreadsSeverity: requests_waiting_for_threads (work queue) > 0 → Critical
        var item = new ServerSummaryItem { TotalThreads = 512, CurrentWorkers = 100, RequestsWaitingForThreads = 3 };
        Assert.Equal(HealthSeverity.Critical, item.ThreadsSeverity);
        Assert.Equal("3 starved", item.ThreadsDisplay);
    }

    [Fact]
    public void ThreadsSeverity_ManyRunnableWaitingForCpu_IsWarning()
    {
        // >=20 runnable tasks waiting for CPU → Warning
        var item = new ServerSummaryItem { TotalThreads = 512, CurrentWorkers = 100, ThreadsWaitingForCpu = 20 };
        Assert.Equal(HealthSeverity.Warning, item.ThreadsSeverity);
        Assert.Equal("20 runnable", item.ThreadsDisplay);
    }

    [Fact]
    public void ThreadsSeverity_LessThanTenPercentAvailable_IsWarning()
    {
        // available = 512 - 470 = 42 < 51.2 (10% of 512) → Warning, "Low"
        var item = new ServerSummaryItem { TotalThreads = 512, CurrentWorkers = 470 };
        Assert.Equal(HealthSeverity.Warning, item.ThreadsSeverity);
        Assert.Equal("Low", item.ThreadsDisplay);
        Assert.Equal(42, item.AvailableThreads);
    }

    [Fact]
    public void ThreadsSeverity_HealthyShowsAvailableDetail()
    {
        var item = new ServerSummaryItem { TotalThreads = 512, CurrentWorkers = 100 };
        Assert.Equal(HealthSeverity.Healthy, item.ThreadsSeverity);
        Assert.Equal("OK", item.ThreadsDisplay);
        Assert.Equal(412, item.AvailableThreads);
        Assert.Equal("Available: 412/512", item.ThreadsDetail);
    }

    [Fact]
    public void MemorySeverity_NoPressure_IsHealthy_DetailShowsSizes()
    {
        var item = new ServerSummaryItem { MemoryMb = 8192, BufferPoolMb = 6144, GrantedMemoryMb = 1024 };
        Assert.Equal(HealthSeverity.Healthy, item.MemorySeverity);
        Assert.False(item.HasMemoryPressure);
        Assert.Equal("BP 6.0, QMG 1.0 GB", item.MemoryDetail);
    }

    [Theory]
    [InlineData(3, 0, 0, "3 grant waiters")]                          // ServerHealthStatus: waiter_count > 0 → Critical
    [InlineData(1, 0, 0, "1 grant waiter")]
    [InlineData(0, 2, 0, "2 timeouts")]                               // viewer extends to timeouts (collected delta)
    [InlineData(0, 0, 1, "1 forced")]                                 // and forced grants
    [InlineData(2, 1, 1, "2 grant waiters, 1 timeout, 1 forced")]
    public void MemorySeverity_AnyPressure_IsCritical_DetailNamesIt(long waiters, long timeouts, long forced, string expectedDetail)
    {
        var item = new ServerSummaryItem { MemoryWaiterCount = waiters, MemoryTimeoutCount = timeouts, MemoryForcedCount = forced };
        Assert.Equal(HealthSeverity.Critical, item.MemorySeverity);
        Assert.True(item.HasMemoryPressure);
        Assert.Equal(expectedDetail, item.MemoryDetail);
    }

    [Theory]
    [InlineData(0, 0, HealthSeverity.Healthy)]
    [InlineData(1, 0, HealthSeverity.Warning)]        // any blocking at all → Warning
    [InlineData(2, 0, HealthSeverity.Warning)]        // >=2 events → Warning
    [InlineData(5, 0, HealthSeverity.Critical)]       // >=5 events → Critical
    [InlineData(1, 10000, HealthSeverity.Warning)]    // 10s max wait → Warning
    [InlineData(1, 59000, HealthSeverity.Warning)]
    [InlineData(1, 60000, HealthSeverity.Critical)]   // 60s max wait → Critical
    public void BlockingSeverity_BandsOnCountAndDuration(int count, long maxWaitMs, HealthSeverity expected)
    {
        Assert.Equal(expected, new ServerSummaryItem { BlockingCount = count, MaxBlockingWaitMs = maxWaitMs }.BlockingSeverity);
    }

    [Fact]
    public void BlockingDetail_MaxWhenBlocked_LastAgoWhenClear_BlankWhenNever()
    {
        Assert.Equal("max: 42s", new ServerSummaryItem { BlockingCount = 3, MaxBlockingWaitMs = 42000 }.BlockingDetail);
        /* Window clear but blocking happened earlier → the Dashboard's "Last: N ago". */
        Assert.Equal("Last: 3h ago", new ServerSummaryItem { BlockingCount = 0, LastBlockingMinutesAgo = 180 }.BlockingDetail);
        Assert.Equal("Last: 2d ago", new ServerSummaryItem { BlockingCount = 0, LastBlockingMinutesAgo = 2880 }.BlockingDetail);
        /* Never any blocking → blank. */
        Assert.Equal("", new ServerSummaryItem { BlockingCount = 0 }.BlockingDetail);
    }

    [Fact]
    public void DeadlockDetail_ShowsLastAgo_WhenKnown_BlankWhenNever()
    {
        Assert.Equal("Last: 5m ago", new ServerSummaryItem { LastDeadlockMinutesAgo = 5 }.DeadlockDetail);
        Assert.Equal("Last: just now", new ServerSummaryItem { LastDeadlockMinutesAgo = 0 }.DeadlockDetail);
        Assert.Equal("", new ServerSummaryItem().DeadlockDetail);
    }

    [Fact]
    public void CollectorSeverity_FailingIsWarning_HealthyOtherwise()
    {
        Assert.Equal(HealthSeverity.Healthy, new ServerSummaryItem { HealthyCollectorCount = 30 }.CollectorSeverity);
        var failing = new ServerSummaryItem { HealthyCollectorCount = 28, FailedCollectorCount = 2 };
        Assert.Equal(HealthSeverity.Warning, failing.CollectorSeverity);
        Assert.Equal("2 failed", failing.CollectorDisplay);
        Assert.Equal("Healthy: 28, Failing: 2", failing.CollectorDetail);
        Assert.Equal("OK", new ServerSummaryItem { HealthyCollectorCount = 30 }.CollectorDisplay);
    }

    [Fact]
    public void CollectorSeverity_OfflineServer_ReadsStaleNeutral_NotGreenOk()
    {
        // #2784 (parity with the web #2779/#2783 fix): a server that has gone dark has STALE collector counts
        // — FailedCollectorCount stays 0 because a stale collector is neither healthy nor failing — so the
        // failing-count-only verdict rendered a green "OK / Healthy: 0, Failing: 0" on an offline server
        // (latent behind the offline overlay, but wrong). Keyed on IsOffline — the same reachability signal
        // that drives the overlay — the verdict now reads a neutral "Stale".
        var offline = new ServerSummaryItem { IsOnline = false, HealthyCollectorCount = 0, FailedCollectorCount = 0 };
        Assert.True(offline.IsOffline);
        Assert.Equal("Stale", offline.CollectorDisplay);
        Assert.Equal("No recent collection", offline.CollectorDetail);
        Assert.Equal(HealthSeverity.Unknown, offline.CollectorSeverity);   // neutral, NOT green Healthy

        // Offline WINS over a stale failing count too: once the server is dark every count is unmeasured, so a
        // leftover "2 failing" from the last collection must not keep reading as an active failure.
        var offlineWithStaleFailures = new ServerSummaryItem { IsOnline = false, FailedCollectorCount = 2 };
        Assert.Equal("Stale", offlineWithStaleFailures.CollectorDisplay);
        Assert.Equal(HealthSeverity.Unknown, offlineWithStaleFailures.CollectorSeverity);

        // A GENUINE collector failure on a reachable server still surfaces red / "N failed" — not swallowed
        // into Stale.
        var failing = new ServerSummaryItem { IsOnline = true, HealthyCollectorCount = 28, FailedCollectorCount = 2 };
        Assert.Equal("2 failed", failing.CollectorDisplay);
        Assert.Equal(HealthSeverity.Warning, failing.CollectorSeverity);

        // A healthy ONLINE server is unchanged — green "OK".
        var healthy = new ServerSummaryItem { IsOnline = true, HealthyCollectorCount = 30, FailedCollectorCount = 0 };
        Assert.Equal("OK", healthy.CollectorDisplay);
        Assert.Equal("Healthy: 30, Failing: 0", healthy.CollectorDetail);
        Assert.Equal(HealthSeverity.Healthy, healthy.CollectorSeverity);

        // Not-yet-connection-classified (IsOnline null — awaiting first collection) keeps the normal reading:
        // "Stale" is for a KNOWN-offline server only, matching the web's strict `is_online === false`.
        var notChecked = new ServerSummaryItem { HealthyCollectorCount = 30, FailedCollectorCount = 0 };
        Assert.False(notChecked.IsOffline);
        Assert.Equal("OK", notChecked.CollectorDisplay);
        Assert.Equal(HealthSeverity.Healthy, notChecked.CollectorSeverity);
    }

    [Fact]
    public void DeadlockSeverity_AnyInWindow_IsCritical()
    {
        Assert.Equal(HealthSeverity.Healthy, new ServerSummaryItem { DeadlockCount = 0 }.DeadlockSeverity);
        Assert.Equal(HealthSeverity.Critical, new ServerSummaryItem { DeadlockCount = 1 }.DeadlockSeverity);
    }

    [Fact]
    public void OverallMetricSeverity_TakesTheWorstBand_UnknownDoesNotEscalate()
    {
        // Threads Critical dominates even when every other metric is calm/unknown.
        var item = new ServerSummaryItem { TotalThreads = 512, CurrentWorkers = 100, RequestsWaitingForThreads = 1 };
        Assert.Equal(HealthSeverity.Critical, item.OverallMetricSeverity);

        // A pure Warning (collectors) with no Critical → Warning.
        Assert.Equal(HealthSeverity.Warning, new ServerSummaryItem { FailedCollectorCount = 1 }.OverallMetricSeverity);

        // No data anywhere (CPU Unknown, rest Healthy) → Healthy baseline (Unknown never escalates).
        Assert.Equal(HealthSeverity.Healthy, new ServerSummaryItem().OverallMetricSeverity);
    }

    [Fact]
    public void CardBorderBrush_EnrichedBands_ThreadsCriticalRed_CollectorsWarningOrange()
    {
        var threadsCritical = new ServerSummaryItem
        {
            TotalThreads = 512, CurrentWorkers = 100, RequestsWaitingForThreads = 1, LastCollectionTime = Now,
        };
        threadsCritical.ApplyFreshness(Now);
        Assert.Equal("#FFE57373", threadsCritical.CardBorderBrush.Color.ToString());

        var collectorsWarning = new ServerSummaryItem { FailedCollectorCount = 1, LastCollectionTime = Now };
        collectorsWarning.ApplyFreshness(Now);
        Assert.Equal("#FFFFD54F", collectorsWarning.CardBorderBrush.Color.ToString());
    }

    [Fact]
    public void SeverityBrushes_MapBandsToDarkPalette()
    {
        Assert.Equal("#FF81C784", new ServerSummaryItem { CpuPercent = 10 }.CpuSeverityBrush.Color.ToString());  // Healthy green
        Assert.Equal("#FFFFD54F", new ServerSummaryItem { CpuPercent = 85 }.CpuSeverityBrush.Color.ToString());  // Warning amber
        Assert.Equal("#FFE57373", new ServerSummaryItem { CpuPercent = 96 }.CpuSeverityBrush.Color.ToString());  // Critical red
        Assert.Equal("#FF888888", new ServerSummaryItem().ThreadsSeverityBrush.Color.ToString());                // Unknown gray
    }
}

/// <summary>
/// Pins the Alert History parity SQL (W2a): the all-servers read shape, the per-server read carrying the
/// Server column's columns, and the dismiss write path (set-based UPDATE keyed on
/// (alert_time, server_id, metric_name)). No live Postgres.
/// </summary>
public sealed class ViewerAlertHistoryW2aSqlTests
{
    [Fact]
    public void AllServersSql_ReadsConfigAlertLog_NoServerFilter_NewestFirst_ExcludesDismissed()
    {
        var sql = ViewerDataService.AlertHistoryAllServersSql;
        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE alert_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("dismissed = FALSE", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY alert_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $2", sql, StringComparison.Ordinal);
        /* No per-server predicate in the all-servers read (that's the per-server read's $2). */
        Assert.DoesNotContain("server_id = $", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BothReads_CarryServerColumns_ForTheServerColumnAndDismissKey()
    {
        foreach (var sql in new[] { ViewerDataService.AlertHistorySql, ViewerDataService.AlertHistoryAllServersSql })
        {
            Assert.Contains("server_id", sql, StringComparison.Ordinal);
            Assert.Contains("server_name", sql, StringComparison.Ordinal);
            Assert.Contains("metric_name", sql, StringComparison.Ordinal);
            Assert.Contains("context_json", sql, StringComparison.Ordinal);
        }

        /* Per-server read keeps its scoping predicate + limit. */
        Assert.Contains("server_id = $2", ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", ViewerDataService.AlertHistorySql, StringComparison.Ordinal);
    }

    [Fact]
    public void DismissAlertsSql_SetBasedUpdate_KeyedOnTheAlertTuple_GuardsAlreadyDismissed()
    {
        var sql = ViewerDataService.DismissAlertsSql;
        Assert.Contains("UPDATE config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("SET    dismissed = TRUE", sql, StringComparison.Ordinal);
        Assert.Contains("dismissed = FALSE", sql, StringComparison.Ordinal);
        Assert.Contains("(alert_time, server_id, metric_name) IN (", sql, StringComparison.Ordinal);
        /* Batch dismiss via unnested arrays — one round-trip for the whole selection. */
        Assert.Contains("unnest($1::timestamp[], $2::integer[], $3::text[])", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DismissAllSql_WindowsThenScopes_GuardsAlreadyDismissed()
    {
        Assert.Contains("UPDATE config_alert_log", ViewerDataService.DismissAllAlertsSql, StringComparison.Ordinal);
        Assert.Contains("alert_time >= $1", ViewerDataService.DismissAllAlertsSql, StringComparison.Ordinal);
        Assert.Contains("dismissed = FALSE", ViewerDataService.DismissAllAlertsSql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_id", ViewerDataService.DismissAllAlertsSql, StringComparison.Ordinal);

        Assert.Contains("alert_time >= $1", ViewerDataService.DismissAllAlertsForServerSql, StringComparison.Ordinal);
        Assert.Contains("server_id = $2", ViewerDataService.DismissAllAlertsForServerSql, StringComparison.Ordinal);
        Assert.Contains("dismissed = FALSE", ViewerDataService.DismissAllAlertsForServerSql, StringComparison.Ordinal);
    }

    [Fact]
    public void AllW2aAlertSql_ArePgDialect_NoBareNow_NoNLiterals()
    {
        foreach (var sql in new[]
        {
            ViewerDataService.AlertHistoryAllServersSql,
            ViewerDataService.DismissAlertsSql,
            ViewerDataService.DismissAllAlertsSql,
            ViewerDataService.DismissAllAlertsForServerSql,
        })
        {
            Assert.DoesNotContain("now(", sql.ToLowerInvariant());
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.Contains("$1", sql, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for W2a: the Overview server-summary read against planted
/// metric rows (including the XE→DMV blocking fallback and the freshness-derived status), and the alert
/// dismiss write path (dismissed rows drop out of the default read; the all-servers read spans servers).
/// Shares the serialized "live-postgres" collection; negative sentinel server_ids, cleaned up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerW2aLivePostgresTests
{
    private const int SummaryServerId = -929201;
    private const int FallbackServerId = -929202;
    private const int DismissServerA = -929203;
    private const int DismissServerB = -929204;
    private const int EnrichServerId = -929205;
    private const string ServerName = "viewer-w2a-e2e";

    [Fact]
    public async Task ServerSummary_ReadsPlantedMetrics_AndDerivesOnlineFromFreshCollection_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server-summary test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteSummaryRowsAsync(connection, SummaryServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = TruncateToSeconds(DateTime.UtcNow);
            var withinHour = now.AddMinutes(-20);

            /* Two CPU samples; the newest (sample_time later) wins. */
            await InsertCpuAsync(connection, SummaryServerId, now.AddMinutes(-2), sampleTime: now.AddMinutes(-2), sqlCpu: 10, otherCpu: 2);
            await InsertCpuAsync(connection, SummaryServerId, now.AddMinutes(-1), sampleTime: now.AddMinutes(-1), sqlCpu: 42, otherCpu: 8);

            /* Two memory rows; the newest collection_time wins. */
            await InsertMemoryAsync(connection, SummaryServerId, now.AddMinutes(-2), totalServerMemoryMb: 4096.00m);
            await InsertMemoryAsync(connection, SummaryServerId, now.AddMinutes(-1), totalServerMemoryMb: 8192.00m);

            /* One blocked-process report + one deadlock inside the one-hour window. */
            await InsertBlockedProcessAsync(connection, SummaryServerId, collectionTime: withinHour, eventTime: withinHour);
            await InsertDeadlockAsync(connection, SummaryServerId, collectionTime: withinHour, deadlockTime: withinHour);

            /* A fresh collection_log row → freshness = Online. */
            await InsertCollectionLogAsync(connection, SummaryServerId, "cpu_utilization", now, "SUCCESS");

            var summary = await viewer.GetServerSummaryAsync(SummaryServerId, "Summary E2E");
            summary.ServerName = ServerName;
            summary.ApplyFreshness(DateTime.UtcNow);

            Assert.Equal(42.0, summary.CpuPercent);
            Assert.Equal(8.0, summary.OtherProcessCpuPercent);
            Assert.Equal(50.0, summary.TotalCpuPercent);
            Assert.Equal(8192.0, summary.MemoryMb!.Value, precision: 1);
            Assert.Equal(1, summary.BlockingCount);          // XE report count
            Assert.Equal(1, summary.DeadlockCount);
            Assert.NotNull(summary.LastCollectionTime);
            Assert.True(summary.IsOnline);                   // fresh collection
            Assert.Equal("Online", summary.StatusDisplay);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSummaryRowsAsync(cleanup, SummaryServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task ServerSummary_BlockingCount_FallsBackToDmvSnapshot_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live blocking-fallback test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteSummaryRowsAsync(connection, FallbackServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = TruncateToSeconds(DateTime.UtcNow);
            var withinHour = now.AddMinutes(-15);

            /* No XE blocked-process reports; two DMV snapshots → COALESCE(NULLIF(0,0), dmv) = 2. */
            await InsertDmvBlockingAsync(connection, FallbackServerId, collectionTime: withinHour, eventTime: withinHour);
            await InsertDmvBlockingAsync(connection, FallbackServerId, collectionTime: withinHour, eventTime: withinHour);

            var summary = await viewer.GetServerSummaryAsync(FallbackServerId, "Fallback E2E");

            Assert.Equal(2, summary.BlockingCount);
            Assert.Equal(0, summary.DeadlockCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSummaryRowsAsync(cleanup, FallbackServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task ServerSummary_ReadsEnrichedThreadsMemoryBlockingCollectors_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live enrichment test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteEnrichmentRowsAsync(connection, EnrichServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = TruncateToSeconds(DateTime.UtcNow);
            var latest = now.AddMinutes(-1);
            var older = now.AddMinutes(-2);
            var withinHour = now.AddMinutes(-20);

            /* Memory: total + buffer pool. */
            await InsertMemoryWithBufferPoolAsync(connection, EnrichServerId, now, totalServerMemoryMb: 8192.00m, bufferPoolMb: 6144.00m);

            /* Resource semaphore: an older heavy sample (must be IGNORED) + the latest two-pool sample (summed). */
            await InsertGrantAsync(connection, EnrichServerId, older, poolId: 1, grantedMb: 4096m, waiter: 99, timeoutDelta: 0, forcedDelta: 0);
            await InsertGrantAsync(connection, EnrichServerId, latest, poolId: 1, grantedMb: 512m, waiter: 1, timeoutDelta: 0, forcedDelta: 0);
            await InsertGrantAsync(connection, EnrichServerId, latest, poolId: 2, grantedMb: 512m, waiter: 2, timeoutDelta: 0, forcedDelta: 0);

            /* Scheduler snapshot: 512 ceiling, 128 in use → 384 available; 5 runnable, no work-queue starvation. */
            await InsertSchedulerAsync(connection, EnrichServerId, now, runnable: 5);

            /* Two XE blocked-process reports in the window; the worst wait is 42s. */
            await InsertBlockedProcessWithWaitAsync(connection, EnrichServerId, withinHour, waitTimeMs: 5000);
            await InsertBlockedProcessWithWaitAsync(connection, EnrichServerId, withinHour, waitTimeMs: 42000);

            /* Collection health: one HEALTHY collector (recent success) + one FAILING (error, never succeeded). */
            await InsertCollectionLogAsync(connection, EnrichServerId, "cpu_utilization", now, "SUCCESS");
            await InsertCollectionLogAsync(connection, EnrichServerId, "memory_stats", now, "ERROR");

            var summary = await viewer.GetServerSummaryAsync(EnrichServerId, "Enrich E2E");

            /* Threads — the latest scheduler snapshot (available = ceiling − in-use). */
            Assert.Equal(512, summary.TotalThreads);
            Assert.Equal(384, summary.AvailableThreads);
            Assert.Equal(5, summary.ThreadsWaitingForCpu);
            Assert.Equal(0, summary.RequestsWaitingForThreads);
            Assert.Equal(HealthSeverity.Healthy, summary.ThreadsSeverity);
            Assert.Equal("Available: 384/512", summary.ThreadsDetail);

            /* Memory — total + buffer pool + resource-semaphore pressure summed at the LATEST collection only. */
            Assert.Equal(8192.0, summary.MemoryMb!.Value, precision: 1);
            Assert.Equal(6144.0, summary.BufferPoolMb!.Value, precision: 1);
            Assert.Equal(3, summary.MemoryWaiterCount);            // 1 + 2 at latest; the older 99 is ignored
            Assert.Equal(1024.0, summary.GrantedMemoryMb!.Value, precision: 1);
            Assert.True(summary.HasMemoryPressure);
            Assert.Equal(HealthSeverity.Critical, summary.MemorySeverity);

            /* Blocking — count + worst wait, both from the XE source; last-event read populated. */
            Assert.Equal(2, summary.BlockingCount);
            Assert.Equal(42000, summary.MaxBlockingWaitMs);
            Assert.Equal("max: 42s", summary.BlockingDetail);
            Assert.Equal(HealthSeverity.Warning, summary.BlockingSeverity);
            Assert.NotNull(summary.LastBlockingMinutesAgo);

            /* Collectors — REUSE of the 7-day banding (one HEALTHY, one FAILING). */
            Assert.Equal(1, summary.HealthyCollectorCount);
            Assert.Equal(1, summary.FailedCollectorCount);
            Assert.Equal(HealthSeverity.Warning, summary.CollectorSeverity);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteEnrichmentRowsAsync(cleanup, EnrichServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task Dismiss_HidesRowsFromTheDefaultRead_AllServersSpansServers_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live dismiss test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteAlertRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = TruncateToSeconds(DateTime.UtcNow);
            var since = now.AddHours(-1);

            /* Three visible rows on server A (distinct alert_times so a single dismiss targets one),
               one on server B. */
            await InsertAlertAsync(connection, now, DismissServerA, "server-a", "High CPU");
            await InsertAlertAsync(connection, now.AddMinutes(-1), DismissServerA, "server-a", "Deadlocks Detected");
            await InsertAlertAsync(connection, now.AddMinutes(-2), DismissServerA, "server-a", "Blocking Detected");
            await InsertAlertAsync(connection, now.AddMinutes(-1), DismissServerB, "server-b", "High CPU");

            /* All-servers read spans both servers. */
            var all = await viewer.GetAlertHistoryAsync(since);
            Assert.Equal(4, all.Count(r => r.ServerId is DismissServerA or DismissServerB));
            Assert.Contains(all, r => r.ServerId == DismissServerB);

            /* Per-server read scopes to A. */
            var aRows = await viewer.GetAlertHistoryAsync(since, DismissServerA);
            Assert.Equal(3, aRows.Count);

            /* Dismiss one A row → it drops out; the other two remain. */
            var target = aRows.Single(r => r.MetricName == "Deadlocks Detected");
            var dismissedOne = await viewer.DismissAlertsAsync(new[] { target });
            Assert.Equal(1, dismissedOne);

            var aAfterOne = await viewer.GetAlertHistoryAsync(since, DismissServerA);
            Assert.Equal(2, aAfterOne.Count);
            Assert.DoesNotContain(aAfterOne, r => r.MetricName == "Deadlocks Detected");

            /* Dismissing the same row again is a no-op (the dismissed = FALSE guard). */
            Assert.Equal(0, await viewer.DismissAlertsAsync(new[] { target }));

            /* Dismiss all remaining A rows → A empties, B untouched. */
            var dismissedAll = await viewer.DismissAllVisibleAlertsAsync(since, DismissServerA);
            Assert.Equal(2, dismissedAll);
            Assert.Empty(await viewer.GetAlertHistoryAsync(since, DismissServerA));
            Assert.Single(await viewer.GetAlertHistoryAsync(since, DismissServerB));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAlertRowsAsync(cleanup, cleanupCt));
        }
    }

    // ── planting helpers ─────────────────────────────────────────────────────────────

    private static async Task InsertCpuAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, DateTime sampleTime, int sqlCpu, int otherCpu)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sampleTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(sqlCpu);
        command.Parameters.AddWithValue(otherCpu);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertMemoryAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, decimal totalServerMemoryMb)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_server_memory_mb) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(totalServerMemoryMb);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertBlockedProcessAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, DateTime eventTime)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDmvBlockingAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, DateTime eventTime)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertDeadlockAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, DateTime deadlockTime)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time) VALUES ($1, $2, $3, $4, $5)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(deadlockTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertCollectionLogAsync(
        NpgsqlConnection connection, int serverId, string collectorName, DateTime collectionTime, string status)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, $4, $5, $6)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(status);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertMemoryWithBufferPoolAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, decimal totalServerMemoryMb, decimal bufferPoolMb)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_server_memory_mb, buffer_pool_mb) VALUES ($1, $2, $3, $4, $5, $6)",
            connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(totalServerMemoryMb);
        command.Parameters.AddWithValue(bufferPoolMb);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertGrantAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, int poolId, decimal grantedMb, int waiter, long timeoutDelta, long forcedDelta)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, pool_id,
     available_memory_mb, granted_memory_mb, used_memory_mb,
     grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(poolId);
        command.Parameters.AddWithValue(1000m);        // available_memory_mb
        command.Parameters.AddWithValue(grantedMb);
        command.Parameters.AddWithValue(500m);         // used_memory_mb
        command.Parameters.AddWithValue(1);            // grantee_count
        command.Parameters.AddWithValue(waiter);
        command.Parameters.AddWithValue(timeoutDelta);
        command.Parameters.AddWithValue(forcedDelta);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertSchedulerAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTime, int runnable)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_scheduler_stats
    (collection_id, collection_time, server_id, server_name,
     max_workers_count, scheduler_count, cpu_count,
     total_runnable_tasks_count, total_work_queue_count, total_current_workers_count,
     avg_runnable_tasks_count, total_active_request_count, total_queued_request_count,
     total_blocked_task_count, total_active_parallel_thread_count,
     runnable_request_count, total_request_count, runnable_percent,
     worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning,
     queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb,
     system_memory_state_desc, physical_memory_pressure_warning,
     total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
VALUES ($1, $2, $3, $4, 512, 8, 8, $5, 0, 128, 0.5, 2, 0, 0, 0, 3, 20, 12.5,
        false, false, false, false, 67108864, 8388608, 'ok', false, 2, 2, 0, false)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(runnable);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertBlockedProcessWithWaitAsync(
        NpgsqlConnection connection, int serverId, DateTime eventTime, long waitTimeMs)
    {
        using var command = new NpgsqlCommand(
            "INSERT INTO blocked_process_reports (blocked_report_id, collection_time, server_id, server_name, event_time, wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6)",
            connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(waitTimeMs);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertAlertAsync(
        NpgsqlConnection connection, DateTime alertTimeUtc, int serverId, string serverName, string metric)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, dismissed, muted, detail_text, context_json)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(alertTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(metric);
        command.Parameters.AddWithValue(1.0);
        command.Parameters.AddWithValue(1.0);
        command.Parameters.AddWithValue(false);
        command.Parameters.AddWithValue("tray");
        command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.AddWithValue(false);
        command.Parameters.AddWithValue(false);
        command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = DBNull.Value, NpgsqlDbType = NpgsqlDbType.Text });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteSummaryRowsAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[]
        {
            "cpu_utilization_stats", "memory_stats", "blocked_process_reports",
            "dmv_blocking_snapshots", "deadlocks", "collection_log",
        })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteEnrichmentRowsAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        foreach (var table in new[]
        {
            "memory_stats", "memory_grant_stats", "cpu_scheduler_stats",
            "blocked_process_reports", "collection_log",
        })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task DeleteAlertRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM config_alert_log WHERE server_id IN ({DismissServerA}, {DismissServerB});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
