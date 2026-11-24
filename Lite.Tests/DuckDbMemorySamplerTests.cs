using System;
using System.Threading;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>The merge's DuckDB memory sampler (#9421): it never disposes its connection under a read that is still running.</summary>
public class DuckDbMemorySamplerTests
{
    /// <summary>
    /// A sampler that is still inside a read when Dispose gives up waiting must not have its connection disposed by Dispose, because the
    /// read is on the native handle. The sampler thread disposes it on its way out instead.
    /// </summary>
    [Fact]
    public void ASamplerStillBusyPastTheJoin_IsNotDisposedUntilItExits()
    {
        using var merge = new DuckDBConnection("DataSource=:memory:");
        merge.Open();
        using var entered = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);
        var sampler = new ParquetCompaction.DuckDbMemorySampler(
            merge,
            _ =>
            {
                entered.Set();
                release.Wait(TimeSpan.FromSeconds(30));
            },
            joinTimeout: TimeSpan.FromMilliseconds(50));
        Assert.True(entered.Wait(TimeSpan.FromSeconds(30)), "the sampler never took a sample");

        sampler.Dispose();

        Assert.False(sampler.ResourcesDisposedForTests, "Dispose disposed the connection while the sampler was still busy");

        release.Set();
        SpinWait.SpinUntil(() => sampler.ResourcesDisposedForTests, TimeSpan.FromSeconds(30));

        Assert.True(sampler.ResourcesDisposedForTests, "the sampler thread never disposed its connection on its way out");
    }

    /// <summary>A sampler that stops in time is disposed by the time Dispose returns, as before.</summary>
    [Fact]
    public void ASamplerThatStopsInTime_IsDisposedWhenDisposeReturns()
    {
        using var merge = new DuckDBConnection("DataSource=:memory:");
        merge.Open();
        using var sampled = new ManualResetEventSlim(false);
        var sampler = new ParquetCompaction.DuckDbMemorySampler(merge, _ => sampled.Set());
        Assert.True(sampled.Wait(TimeSpan.FromSeconds(30)), "the sampler never took a sample");

        sampler.Dispose();

        Assert.True(sampler.ResourcesDisposedForTests);
    }
}
