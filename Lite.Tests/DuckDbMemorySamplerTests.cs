using System;
using System.IO;
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

    /// <summary>
    /// A merge that ends within a few milliseconds still reports a DuckDB peak above 0 (#9421). The first read of a sampler is taken before
    /// the merge allocates anything and reads 0, and Dispose used to stop the thread before its next read. Here the first read is held
    /// until the merge has ended and is then released with Dispose right behind it, which is what a loaded machine does to the thread.
    /// </summary>
    [Fact]
    public void AMergeThatEndsBeforeTheSecondRead_StillReportsAPeak()
    {
        var dir = Path.Combine(Path.GetTempPath(), "sampler_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var source = Path.Combine(dir, "in.parquet").Replace('\\', '/');
            var output = Path.Combine(dir, "out.parquet").Replace('\\', '/');
            using (var writer = new DuckDBConnection("DataSource=:memory:"))
            {
                writer.Open();
                using var write = writer.CreateCommand();
                write.CommandText = $"COPY (SELECT range AS id, md5(range::VARCHAR) AS s FROM range(12000)) TO '{source}' (FORMAT parquet)";
                write.ExecuteNonQuery();
            }

            using var merge = new DuckDBConnection("DataSource=:memory:");
            merge.Open();
            using var entered = new ManualResetEventSlim(false);
            using var release = new ManualResetEventSlim(false);
            long peak = 0;
            var samples = 0;
            var sampler = new ParquetCompaction.DuckDbMemorySampler(
                merge,
                bytes =>
                {
                    peak = Math.Max(peak, bytes);
                    if (Interlocked.Increment(ref samples) == 1)
                    {
                        entered.Set();
                        release.Wait(TimeSpan.FromSeconds(30));
                    }
                });
            Assert.True(entered.Wait(TimeSpan.FromSeconds(30)), "the sampler never took a sample");
            Assert.Equal(0, peak);

            using (var copy = merge.CreateCommand())
            {
                copy.CommandText = $"COPY (SELECT * FROM read_parquet(['{source}']) ORDER BY id) TO '{output}' (FORMAT parquet)";
                copy.ExecuteNonQuery();
            }

            release.Set();
            sampler.Dispose();

            Assert.True(peak > 0, "a merge that ended before the sampler's second read reported no DuckDB peak");
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
