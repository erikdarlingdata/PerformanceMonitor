using System.Threading.Tasks;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Pins the Tier-0 baseline-model collapse (Lite half of the Lite&lt;-&gt;Darling lockstep): Lite's
/// baseline/anomaly brain consumes the SHARED model from the PerformanceMonitor.Analysis assembly
/// (namespace <c>PerformanceMonitor.Analysis.Baselines</c>) and keeps NO per-app copy of it, so Lite
/// and Darling can't silently re-fork the once-triplicated model + constants. The deprecated Dashboard
/// keeps its OWN copy on purpose (<c>PerformanceMonitorDashboard.Analysis</c>) — this pin is
/// Lite&lt;-&gt;Darling only. Darling.Tests carries the mirror-image pin for the Darling half.
/// </summary>
public class SharedBaselineModelPinTests
{
    private const string SharedAssembly = "PerformanceMonitor.Analysis";
    private const string SharedNamespace = "PerformanceMonitor.Analysis.Baselines";

    [Fact]
    public void BaselineModelTypes_LiveInSharedAssemblyAndNamespace()
    {
        Assert.Equal(SharedAssembly, typeof(BaselineBucket).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(BaselineTier).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(MetricNames).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(BaselineMath).Assembly.GetName().Name);
        Assert.Equal(SharedAssembly, typeof(AnomalyThresholds).Assembly.GetName().Name);

        Assert.Equal(SharedNamespace, typeof(BaselineBucket).Namespace);
        Assert.Equal(SharedNamespace, typeof(BaselineTier).Namespace);
        Assert.Equal(SharedNamespace, typeof(MetricNames).Namespace);
        Assert.Equal(SharedNamespace, typeof(BaselineMath).Namespace);
        Assert.Equal(SharedNamespace, typeof(AnomalyThresholds).Namespace);
    }

    [Fact]
    public void Lite_KeepsNoPrivateCopyOfTheBaselineModel()
    {
        // If someone re-declares any of these in Lite's own Analysis namespace, this fails —
        // the whole point of the collapse is that the model has exactly ONE home.
        var liteAssembly = typeof(BaselineProvider).Assembly;
        foreach (var forkedType in new[]
        {
            "PerformanceMonitorLite.Analysis.BaselineBucket",
            "PerformanceMonitorLite.Analysis.BaselineTier",
            "PerformanceMonitorLite.Analysis.MetricNames",
            "PerformanceMonitorLite.Analysis.BaselineMath",
            "PerformanceMonitorLite.Analysis.AnomalyThresholds",
        })
        {
            Assert.Null(liteAssembly.GetType(forkedType));
        }
    }

    [Fact]
    public void LiteBaselineProvider_BindsToTheSharedBaselineBucket()
    {
        // GetBaselineAsync returns Task<BaselineBucket>; pin the element type to the shared type.
        var method = typeof(BaselineProvider).GetMethod(nameof(BaselineProvider.GetBaselineAsync));
        Assert.NotNull(method);
        var bucketType = method!.ReturnType.GetGenericArguments()[0];
        Assert.Same(typeof(BaselineBucket), bucketType);
        Assert.Equal(SharedAssembly, bucketType.Assembly.GetName().Name);
    }

    [Fact]
    public void SharedThresholds_KeepBehaviorPreservingValues()
    {
        // Behavior-preserving value pin: the collapse must not have altered any threshold. These are
        // the tier-selection thresholds and a representative slice of the ~20 detector constants.
        Assert.Equal(30, BaselineMath.BaselineWindowDays);
        Assert.Equal(10, BaselineMath.CollapseThreshold);
        Assert.Equal(15, BaselineMath.RestoreThreshold);

        Assert.Equal(2.0, AnomalyThresholds.DefaultDeviationThreshold);
        Assert.Equal(4.0, AnomalyThresholds.DefaultRatioThreshold);
        Assert.Equal(3.0, AnomalyThresholds.DefaultEventRatioThreshold);
        Assert.Equal(25.0, AnomalyThresholds.SigmaDisplayCap);
        Assert.Equal(90.0, AnomalyThresholds.CpuFallbackPct);
        Assert.Equal(250.0, AnomalyThresholds.WaitProfileFallbackMsPerSec);
        Assert.Equal(100.0, AnomalyThresholds.NoBaselineRatio);

        Assert.Equal(100m, AnomalyThresholds.ObjectGrowthMbThreshold);
        Assert.Equal(20.0, AnomalyThresholds.ObjectGrowthPctThreshold);
        Assert.Equal(60000L, AnomalyThresholds.ObjectLockWaitMsDeltaThreshold);

        // Bounded-metric dispersion floors (server-relative metrics floor at 0).
        Assert.Equal(5.0, BaselineMath.AbsStdDevFloorFor(MetricNames.Cpu));
        Assert.Equal(4.0, BaselineMath.AbsStdDevFloorFor(MetricNames.Memory));
        Assert.Equal(2.5, BaselineMath.AbsStdDevFloorFor(MetricNames.IoLatency));
        Assert.Equal(0.0, BaselineMath.AbsStdDevFloorFor(MetricNames.BatchRequests));

        /* #3691: the PostgreSQL-target CPU metric is percent of the capacity ceiling — percentage points, the
           unit the SQL Server CPU floor is stated in — so it carries the SAME floor by unit parity, not a new
           number. Every other PostgreSQL name is server-relative (tps, sessions, deadlocks/h, wait ms/s) and
           floors at 0 exactly as its SQL Server sibling does. */
        Assert.Equal(BaselineMath.AbsStdDevFloorFor(MetricNames.Cpu), BaselineMath.AbsStdDevFloorFor(MetricNames.PgCpu));
        Assert.Equal(5.0, BaselineMath.AbsStdDevFloorFor(MetricNames.PgCpu));
        Assert.Equal(0.0, BaselineMath.AbsStdDevFloorFor(MetricNames.PgWaitMsPerSec));
        Assert.Equal(0.0, BaselineMath.AbsStdDevFloorFor(MetricNames.PgTps));
        Assert.Equal(0.0, BaselineMath.AbsStdDevFloorFor(MetricNames.PgSessionCount));
        Assert.Equal(0.0, BaselineMath.AbsStdDevFloorFor(MetricNames.PgDeadlockRate));
    }
}
