using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Collects facts from DuckDB for the Lite analysis engine.
/// Each fact category has its own collection method, added incrementally.
/// </summary>
public partial class DuckDbFactCollector : IFactCollector
{
    private readonly DuckDbInitializer _duckDb;

    public DuckDbFactCollector(DuckDbInitializer duckDb)
    {
        _duckDb = duckDb;
    }

    public async Task<List<Fact>> CollectFactsAsync(AnalysisContext context)
    {
        var facts = new List<Fact>();

        await CollectWaitStatsFactsAsync(context, facts);
        FactCollectorHelpers.GroupGeneralLockWaits(facts, context);
        FactCollectorHelpers.GroupParallelismWaits(facts, context);
        await CollectBlockingFactsAsync(context, facts);
        await CollectBlockingChainFactsAsync(context, facts);
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

    // Single BigInteger-tolerant impl lives in BlockingPairRowQuery (shared with the pair-row reader);
    // delegate so the check isn't duplicated.
    private static long ToInt64(object value) => BlockingPairRowQuery.ToInt64(value);
}
