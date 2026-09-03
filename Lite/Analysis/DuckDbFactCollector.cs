using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

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

    /// <summary>
    /// Reports a fact-collection failure that is being swallowed, so a collector that CANNOT run is
    /// distinguishable from one that ran and found nothing (#2826). Darling's
    /// <c>PgFactCollector.ReportCollectionFailure</c> counterpart, and the reason this file's catches
    /// are no longer empty either — the two collectors are a method-for-method port and a blind spot
    /// fixed on one side only would silently re-open on the other.
    ///
    /// <para>Two arms here where Darling has three, and both differences are DuckDB facts rather than
    /// oversights:</para>
    /// <list type="bullet">
    /// <item><description>Darling's 42P01 arm has no counterpart. DuckDB surfaces a missing relation
    /// as a <c>DuckDBException</c> distinguishable only by MESSAGE TEXT, and this project detects
    /// exception meaning structurally, never by message (the
    /// <c>PgBaselineProvider.IsCommandTimeout</c> discipline). It would also be the wrong call: Lite
    /// creates every one of these tables at startup from
    /// <c>Schema.GetAllTableStatements()</c>, so a missing table here is a defect and not the
    /// expected condition the old comment claimed — which is precisely the assumption that let a real
    /// fault read as "no data" for as long as it did.</description></item>
    /// <item><description>The non-timeout arm logs at ERROR rather than Darling's DEBUG-for-42P01,
    /// for the same reason, and because <see cref="AppLogger.Debug"/> is compiled out of Release
    /// builds — a level nobody can turn on is indistinguishable from the empty catch this replaces.</description></item>
    /// </list>
    ///
    /// <para>The timeout arm is a <see cref="TimeoutException"/> only: Lite's deadline is
    /// <c>DuckDbInitializer</c>'s READ-LOCK acquisition, not a server-side statement timeout, since
    /// the store is an embedded file in this process rather than a separate postmaster. It still
    /// means this collector produced nothing because it could not get in, which is the distinction
    /// worth drawing.</para>
    /// </summary>
    private static void ReportCollectionFailure(
        Exception ex,
        AnalysisContext context,
        [CallerMemberName] string collectMethod = "")
    {
        if (ex is TimeoutException || ex.InnerException is TimeoutException)
        {
            AppLogger.Warn("DuckDbFactCollector",
                $"{collectMethod} timed out acquiring the store read lock for {context.ServerName} " +
                $"(server {context.ServerId}) — that analysis input is MISSING for this pass, which is " +
                $"not the same as the server having none: {ex.Message}");
        }
        else
        {
            AppLogger.Error("DuckDbFactCollector",
                $"{collectMethod} failed for {context.ServerName} (server {context.ServerId}) and " +
                $"contributes no facts this pass: {ex.Message}");
        }
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
