using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins PlanDisplayText.CpuElapsedRatio, the CPU:Elapsed ratio shown in the runtime summary
/// right after Elapsed, matching erikdarlingdata/PerformanceStudio@28d4c74: CPU time with
/// external-wait time (BenefitScorer.IsExternalWait) subtracted, divided by elapsed time.
/// </summary>
public class PlanSync4537Tests
{
    private static PlanStatement MakeStatement(long cpuMs, long elapsedMs, params (string WaitType, long WaitTimeMs)[] waits)
    {
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = cpuMs, ElapsedTimeMs = elapsedMs }
        };

        foreach (var (waitType, waitTimeMs) in waits)
            stmt.WaitStats.Add(new WaitStatInfo { WaitType = waitType, WaitTimeMs = waitTimeMs });

        return stmt;
    }

    [Fact]
    public void No_waits_gives_plain_cpu_over_elapsed()
    {
        var stmt = MakeStatement(cpuMs: 800, elapsedMs: 400);

        var ratio = PlanDisplayText.CpuElapsedRatio(stmt);

        Assert.Equal(2.00, ratio!.Value, precision: 2);
    }

    [Fact]
    public void External_wait_is_subtracted_from_cpu()
    {
        var stmt = MakeStatement(cpuMs: 800, elapsedMs: 400, ("PREEMPTIVE_OS_WRITEFILEGATHER", 300));

        var ratio = PlanDisplayText.CpuElapsedRatio(stmt);

        Assert.Equal(1.25, ratio!.Value, precision: 2);
    }

    [Fact]
    public void Zero_elapsed_gives_null()
    {
        var stmt = MakeStatement(cpuMs: 800, elapsedMs: 0);

        Assert.Null(PlanDisplayText.CpuElapsedRatio(stmt));
    }

    [Fact]
    public void External_wait_bigger_than_cpu_floors_at_zero()
    {
        var stmt = MakeStatement(cpuMs: 100, elapsedMs: 400, ("PREEMPTIVE_OS_WRITEFILEGATHER", 500));

        var ratio = PlanDisplayText.CpuElapsedRatio(stmt);

        Assert.Equal(0.0, ratio!.Value, precision: 2);
    }
}
