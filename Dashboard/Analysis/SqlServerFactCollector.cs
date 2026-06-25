using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// Collects facts from SQL Server for the Dashboard analysis engine.
/// Each fact category has its own collection method, added incrementally.
/// Port of DuckDbFactCollector from Lite — queries collect.* tables instead of DuckDB views.
/// </summary>
public partial class SqlServerFactCollector : IFactCollector
{
    private readonly string _connectionString;

    public SqlServerFactCollector(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<List<Fact>> CollectFactsAsync(AnalysisContext context)
    {
        var facts = new List<Fact>();

        await CollectWaitStatsFactsAsync(context, facts);
        FactCollectorHelpers.GroupGeneralLockWaits(facts, context);
        FactCollectorHelpers.GroupParallelismWaits(facts, context);
        await CollectBlockingFactsAsync(context, facts);
        await CollectDeadlockFactsAsync(context, facts);
        await CollectServerConfigFactsAsync(context, facts);
        await CollectMemoryFactsAsync(context, facts);
        await CollectDatabaseSizeFactAsync(context, facts);
        await CollectServerMetadataFactsAsync(context, facts);
        await CollectCpuUtilizationFactsAsync(context, facts);
        await CollectIoLatencyFactsAsync(context, facts);
        await CollectTempDbFactsAsync(context, facts);
        await CollectMemoryGrantFactsAsync(context, facts);
        await CollectQueryStatsFactsAsync(context, facts);
        await CollectParameterSensitivityFactsAsync(context, facts);
        await CollectPlanRegressionFactsAsync(context, facts);
        await CollectBlockingChainFactsAsync(context, facts);
        await CollectBadActorFactsAsync(context, facts);
        await CollectPerfmonFactsAsync(context, facts);
        await CollectMemoryClerkFactsAsync(context, facts);
        await CollectDatabaseConfigFactsAsync(context, facts);
        await CollectFileAutogrowthFactsAsync(context, facts);
        await CollectProcedureStatsFactsAsync(context, facts);
        await CollectActiveQueryFactsAsync(context, facts);
        await CollectRunningJobFactsAsync(context, facts);
        await CollectSessionFactsAsync(context, facts);
        await CollectTraceFlagFactsAsync(context, facts);
        await CollectServerPropertiesFactsAsync(context, facts);
        await CollectDiskSpaceFactsAsync(context, facts);
        await CollectPlanAdvisoryFactsAsync(context, facts);

        return facts;
    }

    /// <summary>
    /// Collects server hardware properties: CPU count, cores, sockets, memory.
    /// Critical context for MAXDOP and memory recommendations.
    /// </summary>
    /// <summary>
    /// Builds the latest-row server_properties SELECT, including the WS5 server-health columns only
    /// when the DB has them. Extracted for testability and so a not-yet-upgraded server (no WS5
    /// columns) reads the core hardware columns without a bind error.
    /// </summary>
    // The WS5 server-health column names (compile-time literals — never built from data, so the
    // probe + SELECT-list construction below are injection-safe). Each is referenced only when it
    // actually exists on the target DB.
    private const string LpimColumn = "lock_pages_in_memory";
    private const string IfiColumn = "instant_file_initialization_enabled";
    private const string DumpsColumn = "memory_dump_count";

    /// <summary>
    /// Builds the latest-row server_properties SELECT, including ONLY the WS5 server-health columns
    /// that exist on the target DB. Each flag is probed independently, so any subset — none, all, or
    /// a partial / out-of-order-migration state — produces a SELECT that never references an absent
    /// column (so it never bind-errors on a not-yet-upgraded server). Extracted for testability.
    /// </summary>
    internal static string BuildServerPropertiesQuery(bool hasLpim, bool hasIfi, bool hasDumps)
    {
        var health = string.Empty;
        if (hasLpim) health += ",\n    " + LpimColumn;
        if (hasIfi) health += ",\n    " + IfiColumn;
        if (hasDumps) health += ",\n    " + DumpsColumn;

        return $@"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 1
    cpu_count,
    hyperthread_ratio,
    physical_memory_mb,
    socket_count,
    cores_per_socket,
    is_hadr_enabled,
    edition,
    product_version{health}
FROM collect.server_properties
ORDER BY collection_time DESC";
    }

}
