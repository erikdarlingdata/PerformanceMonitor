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
    /// <para><b>One arm here where Darling has three, and the asymmetry is the interesting part.</b>
    /// Darling separates a command timeout (WARNING) from an undefined table (DEBUG) from everything
    /// else (ERROR). Neither of the first two has a reachable counterpart in Lite:</para>
    /// <list type="bullet">
    /// <item><description><b>No timeout arm, because Lite has no timeout that can arrive here.</b>
    /// Darling's is a real 30 s Npgsql command deadline that fires INDEPENDENTLY of the pass token,
    /// which is what lets it survive the <c>when</c> filter. Lite's only deadline is the pass budget
    /// itself: all thirty-one reads take <c>AcquireReadLock(context.CancellationToken)</c>, which
    /// throws <see cref="OperationCanceledException"/> from
    /// <c>ThrowIfCancellationRequested()</c> — never a <see cref="TimeoutException"/>, which exists
    /// only on <c>AcquireWriteLock</c> and is never called from analysis. And that exception on that
    /// token is precisely <c>AnalysisAbandon.IsExpected</c>, so the filter excludes it and it unwinds
    /// to <c>AnalysisService</c>'s own abandon handling rather than reaching this method. An arm for
    /// it would be dead code carrying a false explanation, which is the defect this issue is about
    /// pointing the other way.</description></item>
    /// <item><description><b>No 42P01/42703 counterpart.</b> DuckDB surfaces a missing relation or
    /// column distinguishably only by MESSAGE TEXT, and this project detects exception meaning
    /// structurally, never by message (the <c>PgBaselineProvider.IsCommandTimeout</c> discipline).
    /// The condition Darling keeps quiet for also does not arise here: Darling's is a ROLLING-DEPLOY
    /// skew, an analysis service running against a store some other process migrates, so the two can
    /// legitimately disagree for a window. Lite's store is an embedded file migrated in-process by
    /// <c>DuckDbInitializer</c> before any analysis runs, so there is no window in which a column can
    /// be missing — every one of these tables comes from <c>Schema.GetAllTableStatements()</c> at
    /// startup. A missing table or column here is a defect, not the expected condition the old
    /// comments claimed, and that assumption is exactly what let a real fault read as "no data" for
    /// as long as it did.</description></item>
    /// </list>
    ///
    /// <para>So everything that actually reaches here is a fault, and ERROR is the honest level.
    /// <see cref="AppLogger.Debug"/> would have been the wrong home for a quieter arm regardless: it
    /// is compiled out of Release builds, and a level nobody can turn on is indistinguishable from
    /// the empty catch this replaces.</para>
    /// </summary>
    private static void ReportCollectionFailure(
        Exception ex,
        AnalysisContext context,
        [CallerMemberName] string collectMethod = "")
    {
        AppLogger.Error("DuckDbFactCollector",
            $"{collectMethod} failed for {context.ServerName} (server {context.ServerId}) and " +
            $"contributes no facts this pass: {ex.Message}");
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
