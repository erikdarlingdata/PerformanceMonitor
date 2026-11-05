using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class ProcedureStatsWindowCacheTests
{
    [Fact]
    public async Task GetOrBuildAsync_builds_the_raw_window_exactly_once_across_multiple_consumers()
    {
        var buildCount = 0;
        using var cache = new ProcedureStatsWindowCache(_ =>
        {
            buildCount++;
            return Task.FromResult("/tmp/fake-path.duckdb");
        });

        // Simulates a run that requests procedure_stats_hourly and procedure_plan: each
        // consumer calls GetOrBuildAsync, but the raw window must only be built once.
        var pathA = await cache.GetOrBuildAsync(CancellationToken.None);
        var pathB = await cache.GetOrBuildAsync(CancellationToken.None);

        Assert.Equal(1, buildCount);
        Assert.Equal(1, cache.BuildCount);
        Assert.Equal(pathA, pathB);
    }

    [Fact]
    public void Cache_is_never_built_when_no_consumer_calls_GetOrBuildAsync()
    {
        var buildCount = 0;
        using var cache = new ProcedureStatsWindowCache(_ =>
        {
            buildCount++;
            return Task.FromResult("/tmp/fake-path.duckdb");
        });

        Assert.Equal(0, buildCount);
        Assert.Equal(0, cache.BuildCount);
    }

    [Fact]
    public async Task Dispose_deletes_the_temp_file_even_after_an_exception_from_a_consumer()
    {
        var tempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-psw-cache-test-{Guid.NewGuid():N}.duckdb");
        File.WriteAllText(tempDbPath, "not a real duckdb file, just needs to exist");
        Assert.True(File.Exists(tempDbPath));

        var cache = new ProcedureStatsWindowCache(_ => Task.FromResult(tempDbPath));

        try
        {
            await cache.GetOrBuildAsync(CancellationToken.None);
            throw new InvalidOperationException("simulated failure partway through the run");
        }
        catch (InvalidOperationException)
        {
            // expected; the point of the test is what happens in finally below
        }
        finally
        {
            cache.Dispose();
        }

        Assert.False(File.Exists(tempDbPath));
    }

    [Fact]
    public void Dispose_is_safe_when_the_cache_was_never_built()
    {
        var cache = new ProcedureStatsWindowCache(_ => Task.FromResult("/tmp/never-built.duckdb"));

        // Must not throw, and must not try to delete a path that was never assigned.
        cache.Dispose();
    }
}
