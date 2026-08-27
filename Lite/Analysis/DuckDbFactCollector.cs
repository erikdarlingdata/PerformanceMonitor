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

        /* #2412: one cancellation checkpoint per collector, not one for the phase. This is the
           longest stage of an analysis pass — thirty-one collectors, each opening the store and
           running its own reads — so a check only at the phase boundary would let a pass that has
           already blown its budget run every remaining collector before noticing. Routing the
           calls through a local step is what buys the per-collector check without writing it out
           thirty-one times. The two grouping helpers below rewrite facts already in hand rather
           than reading the store, so they stay inline where their ordering matters. */
        async Task RunCollectorAsync(Func<AnalysisContext, List<Fact>, Task> collect)
        {
            context.CancellationToken.ThrowIfCancellationRequested();
            await collect(context, facts);
        }

        await RunCollectorAsync(CollectWaitStatsFactsAsync);
        FactCollectorHelpers.GroupGeneralLockWaits(facts, context);
        FactCollectorHelpers.GroupParallelismWaits(facts, context);
        await RunCollectorAsync(CollectBlockingFactsAsync);
        await RunCollectorAsync(CollectBlockingChainFactsAsync);
        await RunCollectorAsync(CollectDeadlockFactsAsync);
        await RunCollectorAsync(CollectServerConfigFactsAsync);
        await RunCollectorAsync(CollectMemoryFactsAsync);
        await RunCollectorAsync(CollectDatabaseSizeFactAsync);
        await RunCollectorAsync(CollectServerMetadataFactsAsync);
        await RunCollectorAsync(CollectCpuUtilizationFactsAsync);
        await RunCollectorAsync(CollectRunnableTaskFactsAsync);
        await RunCollectorAsync(CollectIoLatencyFactsAsync);
        await RunCollectorAsync(CollectTempDbFactsAsync);
        await RunCollectorAsync(CollectMemoryGrantFactsAsync);
        await RunCollectorAsync(CollectQueryStatsFactsAsync);
        await RunCollectorAsync(CollectParameterSensitivityFactsAsync);
        await RunCollectorAsync(CollectPlanRegressionFactsAsync);
        await RunCollectorAsync(CollectBadActorFactsAsync);
        await RunCollectorAsync(CollectPerfmonFactsAsync);
        await RunCollectorAsync(CollectMemoryClerkFactsAsync);
        await RunCollectorAsync(CollectPlanCacheFactsAsync);
        await RunCollectorAsync(CollectMemoryPressureEventFactsAsync);
        await RunCollectorAsync(CollectDatabaseConfigFactsAsync);
        await RunCollectorAsync(CollectFileAutogrowthFactsAsync);
        await RunCollectorAsync(CollectProcedureStatsFactsAsync);
        await RunCollectorAsync(CollectActiveQueryFactsAsync);
        await RunCollectorAsync(CollectRunningJobFactsAsync);
        await RunCollectorAsync(CollectSessionFactsAsync);
        await RunCollectorAsync(CollectTraceFlagFactsAsync);
        await RunCollectorAsync(CollectServerPropertiesFactsAsync);
        await RunCollectorAsync(CollectDiskSpaceFactsAsync);
        await RunCollectorAsync(CollectPlanAdvisoryFactsAsync);

        return facts;
    }

    // Single BigInteger-tolerant impl lives in BlockingPairRowQuery (shared with the pair-row reader);
    // delegate so the check isn't duplicated.
    private static long ToInt64(object value) => BlockingPairRowQuery.ToInt64(value);
}
