using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class QueryStoreRawWindowCacheTests
{
    [Fact]
    public async Task GetOrBuildAsync_builds_the_raw_window_exactly_once_across_multiple_consumers()
    {
        var buildCount = 0;
        using var cache = new QueryStoreRawWindowCache(_ =>
        {
            buildCount++;
            return Task.FromResult("/tmp/fake-path.duckdb");
        });

        // Simulates a run that requests query_store_stats, plan_shape and query_text: each
        // consumer calls GetOrBuildAsync, but the raw window must only be built once.
        var pathA = await cache.GetOrBuildAsync(CancellationToken.None);
        var pathB = await cache.GetOrBuildAsync(CancellationToken.None);
        var pathC = await cache.GetOrBuildAsync(CancellationToken.None);

        Assert.Equal(1, buildCount);
        Assert.Equal(1, cache.BuildCount);
        Assert.Equal(pathA, pathB);
        Assert.Equal(pathA, pathC);
    }

    [Fact]
    public async Task GetOrBuildAsync_builds_once_for_plan_shape_and_query_text_alone_under_query_store_origin()
    {
        // A run with --dimension-origin query_store and only plan_shape/query_text requested
        // (no query_store_stats table itself) still needs exactly one raw read.
        var buildCount = 0;
        using var cache = new QueryStoreRawWindowCache(_ =>
        {
            buildCount++;
            return Task.FromResult("/tmp/fake-path.duckdb");
        });

        await cache.GetOrBuildAsync(CancellationToken.None); // plan_shape's Query Store keys
        await cache.GetOrBuildAsync(CancellationToken.None); // query_text's Query Store keys

        Assert.Equal(1, buildCount);
    }

    [Fact]
    public void Cache_is_never_built_when_no_consumer_calls_GetOrBuildAsync()
    {
        // A run whose --dimension-origin is query_stats never touches Query Store at all
        // (DimensionTableExporter never calls GetOrBuildAsync in that case): the cache object
        // exists but its build delegate must never run.
        var buildCount = 0;
        using var cache = new QueryStoreRawWindowCache(_ =>
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
        var tempDbPath = Path.Combine(Path.GetTempPath(), $"darling-export-cache-test-{Guid.NewGuid():N}.duckdb");
        File.WriteAllText(tempDbPath, "not a real duckdb file, just needs to exist");
        Assert.True(File.Exists(tempDbPath));

        var cache = new QueryStoreRawWindowCache(_ => Task.FromResult(tempDbPath));

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
        var cache = new QueryStoreRawWindowCache(_ => Task.FromResult("/tmp/never-built.duckdb"));

        // Must not throw, and must not try to delete a path that was never assigned.
        cache.Dispose();
    }
}
