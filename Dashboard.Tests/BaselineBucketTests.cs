using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Analysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Tests for BaselineBucket.EffectiveStdDev: proportional floor and division-by-zero handling.
/// </summary>
public class BaselineBucketTests
{
    // ── Division by zero: proportional floor ──

    [Fact]
    public void EffectiveStdDev_ZeroStdDev_UsesProportionalFloor()
    {
        // All identical values → stddev = 0, mean = 50
        var bucket = new BaselineBucket
        {
            HourOfDay = 14, DayOfWeek = 3,
            Mean = 50.0, StdDev = 0.0, SampleCount = 20,
            Tier = BaselineTier.Full
        };

        // Should be max(0, 50 * 0.01) = 0.5
        Assert.Equal(0.5, bucket.EffectiveStdDev);
    }

    [Fact]
    public void EffectiveStdDev_ZeroMeanAndZeroStdDev_ReturnsZero()
    {
        // Zero activity → skip scoring
        var bucket = new BaselineBucket
        {
            HourOfDay = 14, DayOfWeek = 3,
            Mean = 0.0, StdDev = 0.0, SampleCount = 20,
            Tier = BaselineTier.Full
        };

        Assert.Equal(0.0, bucket.EffectiveStdDev);
    }

    [Fact]
    public void EffectiveStdDev_NormalStdDev_ReturnsActual()
    {
        var bucket = new BaselineBucket
        {
            HourOfDay = 14, DayOfWeek = 3,
            Mean = 50.0, StdDev = 5.0, SampleCount = 20,
            Tier = BaselineTier.Full
        };

        // StdDev (5.0) > Mean * 0.01 (0.5), so return actual
        Assert.Equal(5.0, bucket.EffectiveStdDev);
    }
}
