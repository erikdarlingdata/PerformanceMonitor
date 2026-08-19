/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the FinOps-tab reads (copy-parity program) against the Darling store contract, no live Postgres.
/// The nine ported sub-tabs' SQL is Lite's DuckDB SQL rewritten to Postgres; these tests guard the two
/// things a silent DuckDB→PG rewrite can break: (1) the load-bearing clauses/params survive the port, and
/// (2) every column the reads reference actually exists in the generated collector DDL (column parity).
/// The one substantive rewrite — Utilization/Server-Inventory reading the collected <c>server_properties</c>
/// base table instead of Lite's non-existent <c>v_server_properties</c> view — is pinned explicitly.
/// </summary>
public sealed class ViewerFinOpsSqlTests
{
    // ── Utilization ──

    [Fact]
    public void UtilizationEfficiencySql_ReadsServerPropertiesBaseTable_NotTheMissingView()
    {
        var sql = ViewerDataService.UtilizationEfficiencySql;

        Assert.Contains("FROM v_cpu_utilization_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_memory_stats", sql, StringComparison.Ordinal);
        /* Darling has no v_server_properties view (V4/V5 never created one); the core count comes from the
           collected server_properties base table, bare-name resolved via the connection's Search Path. */
        Assert.Contains("FROM server_properties", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(vcore_count, cpu_count)", sql, StringComparison.Ordinal);
        Assert.Contains("PERCENTILE_CONT(0.95) WITHIN GROUP", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT JOIN server_info s ON true", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ProvisioningTrendSql_DailyCpuAndMemoryRatio()
    {
        var sql = ViewerDataService.ProvisioningTrendSql;
        Assert.Contains("CAST(collection_time AS DATE)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_cpu_utilization_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_memory_stats", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY CAST(collection_time AS DATE)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryGrantEfficiencySql_ReadsColumnsThatExistInTheGeneratedTable()
    {
        var sql = ViewerDataService.MemoryGrantEfficiencySql;
        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);

        var ddl = PgSchemaGenerator.CreateTable(MemoryGrantsCollector.Instance);
        Assert.Equal("memory_grant_stats", MemoryGrantsCollector.Instance.TargetTable);
        foreach (var col in new[]
        {
            "granted_memory_mb", "used_memory_mb", "grantee_count", "waiter_count",
            "timeout_error_count_delta", "forced_grant_count_delta",
        })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    // ── Database Resources / Workload ──

    [Fact]
    public void DatabaseResourceUsageSql_FullJoinsWorkloadAndIo_OverTheWindow()
    {
        var sql = ViewerDataService.DatabaseResourceUsageSql;
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_file_io_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FULL JOIN io i", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ApplicationConnectionsSql_ReadsColumnsThatExistInTheGeneratedSessionTable()
    {
        var sql = ViewerDataService.ApplicationConnectionsSql;
        Assert.Contains("FROM v_session_stats", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY program_name", sql, StringComparison.Ordinal);

        /* The read surfaces the collected per-app resource + session-status metrics (DC-F1), not just
           connection counts. Every source column it aggregates must exist in the generated session table. */
        var sourceColumns = new[]
        {
            "program_name", "connection_count", "running_count", "sleeping_count", "dormant_count",
            "total_cpu_time_ms", "total_reads", "total_writes", "total_logical_reads"
        };

        var ddl = PgSchemaGenerator.CreateTable(SessionStatsCollector.Instance);
        Assert.Equal("session_stats", SessionStatsCollector.Instance.TargetTable);
        foreach (var col in sourceColumns)
        {
            Assert.Contains(col, sql, StringComparison.Ordinal);
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }

        /* AVG + MAX aggregates for each metric family are all present. */
        foreach (var agg in new[]
                 {
                     "AVG(running_count)", "MAX(running_count)",
                     "AVG(sleeping_count)", "MAX(sleeping_count)",
                     "AVG(dormant_count)", "MAX(dormant_count)",
                     "AVG(total_cpu_time_ms)", "MAX(total_cpu_time_ms)",
                     "AVG(total_reads)", "MAX(total_reads)",
                     "AVG(total_writes)", "MAX(total_writes)",
                     "AVG(total_logical_reads)", "MAX(total_logical_reads)"
                 })
        {
            Assert.Contains(agg, sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TopResourceConsumersSql_ParameterizeTheLimit()
    {
        Assert.Contains("LIMIT $3", ViewerDataService.TopResourceConsumersByTotalSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", ViewerDataService.TopResourceConsumersByAvgSql, StringComparison.Ordinal);
        Assert.Contains("HAVING SUM(delta_execution_count) > 0", ViewerDataService.TopResourceConsumersByAvgSql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitCategorySummarySql_UsesIlikeCategoryCase()
    {
        var sql = ViewerDataService.WaitCategorySummarySql;
        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        Assert.Contains("wait_type ILIKE 'PAGEIOLATCH%'", sql, StringComparison.Ordinal);
        Assert.Contains("wait_type ILIKE 'LCK_M_%'", sql, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY category ORDER BY wait_time_ms DESC)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ExpensiveQueriesSql_GroupsByHandleAndText_LimitParam()
    {
        var sql = ViewerDataService.ExpensiveQueriesSql;
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("LEFT(query_text, 200)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY", sql, StringComparison.Ordinal);
        Assert.Contains("sql_handle", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", sql, StringComparison.Ordinal);
        /* The stored statement-level plan is aggregated per group so "View Plan" opens it (Darling stores
           plans) — restores the plan action the port had wrongly dropped as "no live SQL". */
        Assert.Contains("MAX(query_plan_xml)", sql, StringComparison.Ordinal);
        /* #2069: plans written since V54 are gzip bytes with the text NULL — the aggregate must carry
           both forms or the plan action goes dark as the GC retires the last text rows. */
        Assert.Contains("MAX(query_plan_gz)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void HighImpactQueriesSql_ReadsTheResolvingViewForItsCorrelatedSampleTextAndPlan()
    {
        var sql = ViewerDataService.HighImpactQueriesSql;
        /* Aggregates to query_hash level; the correlated subqueries pull sample text, full text and the
           stored plan, so every one of them must read v_query_stats — the #1767 resolving view. On the base
           table their `query_text IS NOT NULL AND query_text != ''` filters match nothing on rows written
           since the migration, and the panel just goes empty with no error anywhere. */
        Assert.Contains("FROM v_query_stats AS qs", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_query_stats qs2", sql, StringComparison.Ordinal);
        Assert.Equal(4, sql.Split("FROM v_query_stats qs2").Length - 1); /* sample text, full text, plan text, plan gz (#2069) */
        Assert.Contains("GROUP BY query_hash", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY qs2.delta_execution_count DESC NULLS LAST", sql, StringComparison.Ordinal);
        /* The stored statement-level plan is fetched by the correlated subquery so "View Plan" opens it —
           one subquery per content form (#2069): post-V54 plans are gzip bytes with the text NULL. */
        Assert.Contains("qs2.query_plan_xml", sql, StringComparison.Ordinal);
        Assert.Contains("qs2.query_plan_gz IS NOT NULL", sql, StringComparison.Ordinal);

        var ddl = PgSchemaGenerator.CreateTable(QueryStatsCollector.Instance);
        foreach (var col in new[]
        {
            "query_hash", "query_text", "sql_handle", "database_name", "max_grant_kb", "query_plan_xml",
            "delta_execution_count", "delta_worker_time", "delta_elapsed_time",
            "delta_logical_reads", "delta_logical_writes", "delta_physical_reads",
        })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    // ── Storage (Database Sizes / Storage Growth / object drill / tempdb / idle) ──

    [Fact]
    public void DatabaseSizeReads_ReadColumnsThatExistInTheGeneratedTable()
    {
        Assert.Contains("FROM v_database_size_stats", ViewerDataService.DatabaseSizeLatestSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $2", ViewerDataService.DatabaseSizeSummarySql, StringComparison.Ordinal);

        var ddl = PgSchemaGenerator.CreateTable(DatabaseSizeStatsCollector.Instance);
        Assert.Equal("database_size_stats", DatabaseSizeStatsCollector.Instance.TargetTable);
        foreach (var col in new[]
        {
            "file_type_desc", "file_name", "total_size_mb", "used_size_mb", "volume_mount_point",
            "volume_total_mb", "volume_free_mb", "recovery_model_desc", "auto_growth_mb",
            "is_percent_growth", "growth_pct", "vlf_count",
        })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void StorageGrowthSql_ComparesLatestTo7dAnd30dAgo()
    {
        var sql = ViewerDataService.StorageGrowthSql;
        Assert.Contains("collection_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY growth_30d_mb DESC", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ObjectGrowthReads_RankTopN_AndBuildDailySeries()
    {
        Assert.Contains("FROM v_index_object_stats", ViewerDataService.ObjectGrowthSummarySql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", ViewerDataService.ObjectGrowthSummarySql, StringComparison.Ordinal);
        Assert.Contains("date_trunc('day', ios.collection_time)", ViewerDataService.ObjectGrowthSeriesSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", ViewerDataService.ObjectGrowthSeriesSql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(last_user_seek, last_user_scan, last_user_lookup, last_user_update)",
            ViewerDataService.ObjectIndexDetailSql, StringComparison.Ordinal);
    }

    [Fact]
    public void IdleDatabasesSql_UsesExceptToFindZeroActivityDbs()
    {
        var sql = ViewerDataService.IdleDatabasesSql;
        Assert.Contains("FROM v_database_size_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("COALESCE(a.total_executions, 0) = 0", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TempdbSummarySql_LatestVsPeak_ReadsColumnsThatExist()
    {
        Assert.Contains("FROM v_tempdb_stats", ViewerDataService.TempdbSummarySql, StringComparison.Ordinal);

        var ddl = PgSchemaGenerator.CreateTable(TempDbStatsCollector.Instance);
        foreach (var col in new[]
        {
            "user_object_reserved_mb", "internal_object_reserved_mb", "version_store_reserved_mb", "total_reserved_mb",
        })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    // ── Locking ──

    [Fact]
    public void IndexLockingReads_AllAndByDbVariants_ReadColumnsThatExist()
    {
        Assert.Contains("LIMIT $2", ViewerDataService.IndexLockingAllSql, StringComparison.Ordinal);
        Assert.Contains("AND database_name = $2", ViewerDataService.IndexLockingByDbSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", ViewerDataService.IndexLockingByDbSql, StringComparison.Ordinal);
        Assert.Contains("FROM v_index_object_stats", ViewerDataService.IndexLockingDatabasesSql, StringComparison.Ordinal);

        var ddl = PgSchemaGenerator.CreateTable(IndexObjectStatsCollector.Instance);
        Assert.Equal("index_object_stats", IndexObjectStatsCollector.Instance.TargetTable);
        foreach (var col in new[]
        {
            "reserved_mb", "used_mb", "total_rows", "row_lock_wait_in_ms", "page_lock_wait_in_ms",
            "page_latch_wait_in_ms", "page_io_latch_wait_in_ms", "index_lock_promotion_count",
            "user_seeks", "user_updates", "last_user_seek",
        })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    // ── Server Inventory ──

    [Fact]
    public void ServerInventorySql_JoinsLatestServerPropertiesToRegistry_WithInventoryFieldsAndCost()
    {
        var sql = ViewerDataService.ServerInventorySql;
        Assert.Contains("DISTINCT ON (server_id)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("JOIN servers s ON s.server_id = sp.server_id", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_server_properties", sql, StringComparison.Ordinal);

        /* The restored inventory fields (start time / host OS / AG role) + the per-server budget from the
           registry are all selected (fix for the earlier drop — they are collected now, not live-only). */
        Assert.Contains("sqlserver_start_time", sql, StringComparison.Ordinal);
        Assert.Contains("host_os_version", sql, StringComparison.Ordinal);
        Assert.Contains("ag_replica_role", sql, StringComparison.Ordinal);
        Assert.Contains("monthly_cost_usd", sql, StringComparison.Ordinal);

        /* Every server_properties column the inventory read surfaces exists in the generated table — the
           three restored ones included, so the shared collector + Darling schema actually carry them. */
        var ddl = PgSchemaGenerator.CreateTable(ServerPropertiesCollector.Instance);
        Assert.Equal("server_properties", ServerPropertiesCollector.Instance.TargetTable);
        foreach (var col in new[]
        {
            "edition", "product_version", "product_level", "product_update_level", "engine_edition",
            "cpu_count", "vcore_count", "physical_memory_mb", "socket_count", "cores_per_socket",
            "is_hadr_enabled", "is_clustered",
            "sqlserver_start_time", "host_os_version", "ag_replica_role",
        })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The inventory grid read now projects the verdict INPUTS and classifies in C# through the shared
    /// <c>ProvisioningVerdict</c>, rather than deciding inline in SQL.
    ///
    /// <para>It used to carry its own <c>CASE ... memory_ratio &gt; 0.95 THEN UNDER_PROVISIONED</c> — copies
    /// 5 and 6 of the bug in #2246, and on the screen the field report was actually looking at, so the grid
    /// disagreed with the drill-down for the same server. The absence assertions are the drift guard: a SQL
    /// verdict here can never come back without failing this test.</para>
    /// </summary>
    [Fact]
    public void ServerMetricsSql_ProjectsTheVerdictInputs_AndDoesNotDecideInSql()
    {
        var sql = ViewerDataService.ServerMetricsSql;
        Assert.Contains("FROM v_cpu_utilization_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_database_size_stats", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_query_stats", sql, StringComparison.Ordinal);
        Assert.Contains("EXCEPT", sql, StringComparison.Ordinal);

        /* The pressure inputs the shared predicate needs, which this read did not fetch before. */
        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
        Assert.Contains("waiter_count", sql, StringComparison.Ordinal);
        Assert.Contains("timeout_error_count_delta", sql, StringComparison.Ordinal);
        Assert.Contains("forced_grant_count_delta", sql, StringComparison.Ordinal);
        Assert.Contains("max_workers_count", sql, StringComparison.Ordinal);

        /* And the verdict itself must NOT be decided here any more. */
        Assert.DoesNotContain("OVER_PROVISIONED", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UNDER_PROVISIONED", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("RIGHT_SIZED", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The drift guard, applied uniformly to all THREE reads that feed the provisioning verdict rather than
    /// only the one that used to decide in SQL (#2246).
    ///
    /// <para>Darling has no live-Postgres harness for these, so a source pin is the whole safety net: if a
    /// future edit drops the grants CTE, the reader keeps consuming ordinals the SELECT list no longer
    /// produces and the verdict silently falls back to "no pressure anywhere" — the same shape of silent
    /// wrongness this issue is about. The SQL was executed against the live store when it was written; this
    /// is what keeps it honest afterwards.</para>
    /// </summary>
    [Theory]
    [InlineData(nameof(ViewerDataService.UtilizationEfficiencySql))]
    [InlineData(nameof(ViewerDataService.ProvisioningTrendSql))]
    [InlineData(nameof(ViewerDataService.ServerMetricsSql))]
    public void EveryVerdictRead_FetchesThePressureInputs(string sqlName)
    {
        var sql = (string)typeof(ViewerDataService).GetField(sqlName)!.GetValue(null)!;

        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
        Assert.Contains("waiter_count", sql, StringComparison.Ordinal);
        Assert.Contains("timeout_error_count_delta", sql, StringComparison.Ordinal);
        Assert.Contains("forced_grant_count_delta", sql, StringComparison.Ordinal);
        Assert.Contains("max_workers_count", sql, StringComparison.Ordinal);

        /* And none of them may decide the verdict, which is now the shared predicate's job alone. */
        Assert.DoesNotContain("OVER_PROVISIONED", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("UNDER_PROVISIONED", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("RIGHT_SIZED", sql, StringComparison.Ordinal);
    }

    // ── PG dialect guard across every FinOps read ──

    [Theory]
    [InlineData(nameof(ViewerDataService.UtilizationEfficiencySql))]
    [InlineData(nameof(ViewerDataService.ProvisioningTrendSql))]
    [InlineData(nameof(ViewerDataService.MemoryGrantEfficiencySql))]
    [InlineData(nameof(ViewerDataService.DatabaseResourceUsageSql))]
    [InlineData(nameof(ViewerDataService.ApplicationConnectionsSql))]
    [InlineData(nameof(ViewerDataService.TopResourceConsumersByTotalSql))]
    [InlineData(nameof(ViewerDataService.TopResourceConsumersByAvgSql))]
    [InlineData(nameof(ViewerDataService.WaitCategorySummarySql))]
    [InlineData(nameof(ViewerDataService.ExpensiveQueriesSql))]
    [InlineData(nameof(ViewerDataService.HighImpactQueriesSql))]
    [InlineData(nameof(ViewerDataService.DatabaseSizeLatestSql))]
    [InlineData(nameof(ViewerDataService.DatabaseSizeSummarySql))]
    [InlineData(nameof(ViewerDataService.StorageGrowthSql))]
    [InlineData(nameof(ViewerDataService.ObjectGrowthSummarySql))]
    [InlineData(nameof(ViewerDataService.ObjectGrowthSeriesSql))]
    [InlineData(nameof(ViewerDataService.ObjectIndexDetailSql))]
    [InlineData(nameof(ViewerDataService.IdleDatabasesSql))]
    [InlineData(nameof(ViewerDataService.TempdbSummarySql))]
    [InlineData(nameof(ViewerDataService.IndexLockingAllSql))]
    [InlineData(nameof(ViewerDataService.IndexLockingByDbSql))]
    [InlineData(nameof(ViewerDataService.IndexLockingDatabasesSql))]
    [InlineData(nameof(ViewerDataService.ServerMetricsSql))]
    public void FinOpsReads_PgDialect_PositionalParams_NoNamedParams_NoTSqlNLiterals(string sqlName)
    {
        var sql = (string)typeof(ViewerDataService).GetField(sqlName)!.GetValue(null)!;

        /* Postgres positional params only; no T-SQL @named params leaking from the SQL-Server origin, and
           no bare now() (the reads window on a C#-supplied cutoff). (A DuckDB->PG port never carries N''
           unicode literals — Lite's source uses plain '' — and a naive "N'" scan false-matches wait-type
           names ending in N like ASYNC_IO_COMPLETION, so that check is deliberately omitted here.) */
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    /* ── #1591: the permission-denied badge query ── */

    /// <summary>
    /// The badge counts DISTINCT collectors, not log rows. One collector denied every cycle for a week is "1
    /// collector has no permission" — counting rows would render a meaningless four-figure number and train the
    /// operator to ignore the badge.
    /// </summary>
    [Fact]
    public void PermissionDeniedCountSql_CountsDistinctCollectors_NotRows()
    {
        var sql = ViewerDataService.PermissionDeniedCollectorCountSql;

        Assert.Contains("COUNT(DISTINCT collector_name)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("COUNT(*)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// It must filter to PERMISSIONS only. Counting ERROR too would badge ordinary collector failures as a
    /// permission problem and send the operator hunting for a grant that is not missing.
    /// </summary>
    [Fact]
    public void PermissionDeniedCountSql_FiltersToPermissionsOnly()
    {
        var sql = ViewerDataService.PermissionDeniedCollectorCountSql;

        Assert.Contains("status = 'PERMISSIONS'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("'ERROR'", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
    }

    /* ── #1591: the Hardware Note ── */

    /// <summary>
    /// A collector that could not read <c>sys.dm_os_sys_info</c> stores NULL hardware, and the grid's
    /// <c>IsDBNull ? 0</c> coalesce would otherwise render that as a literal 0 — indistinguishable from a real
    /// zero, and reading as "this server has no CPUs" rather than "we were not allowed to look".
    /// </summary>
    [Fact]
    public void HardwareNote_ExplainsAbsentHardware_WhenBothColumnsAreNull()
    {
        var note = ViewerDataService.HardwareNoteFor(cpuCountIsNull: true, physicalMemoryIsNull: true);

        Assert.NotNull(note);
        Assert.Contains("VIEW SERVER STATE", note, StringComparison.Ordinal);
        Assert.Contains("VIEW DATABASE STATE", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// Present hardware must produce no note — the column stays empty for the normal case, which is what makes it
    /// non-alarming rather than noise on every row.
    /// </summary>
    [Fact]
    public void HardwareNote_IsNull_WhenHardwareIsPresent() =>
        Assert.Null(ViewerDataService.HardwareNoteFor(cpuCountIsNull: false, physicalMemoryIsNull: false));

    /// <summary>
    /// Both columns come from the SAME guarded DMV read, so a single odd NULL is not a permission failure and must
    /// not be annotated as one.
    /// </summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void HardwareNote_IsNull_WhenOnlyOneColumnIsMissing(bool cpuNull, bool memNull) =>
        Assert.Null(ViewerDataService.HardwareNoteFor(cpuNull, memNull));

    /// <summary>
    /// The note is only reachable because #1663 made the hardware columns nullable. Before it, a login without the
    /// grant lost the ENTIRE server_properties row, so there was no row left to annotate — pin that the collector
    /// still types them nullable.
    /// </summary>
    [Fact]
    public void HardwareColumns_AreStillNullable_WhichIsWhatMakesTheNoteReachable()
    {
        foreach (var name in new[] { "CpuCount", "PhysicalMemoryMb" })
        {
            var property = typeof(PerformanceMonitor.Collectors.ServerPropertiesCollector.Row).GetProperty(name);
            Assert.NotNull(property);
            Assert.True(
                Nullable.GetUnderlyingType(property!.PropertyType) is not null,
                $"{name} must stay nullable — the Hardware Note depends on distinguishing unknown from zero (#1591/#1663).");
        }
    }
}
