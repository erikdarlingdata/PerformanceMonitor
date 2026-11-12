using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Run-scoped cache around one query_stats raw-window DuckDB temp file (<see
/// cref="QueryStatsWindow.BuildRawWindowFileAsync"/>). Built at most once per <see
/// cref="ExportRunner.RunAsync"/>, lazily, by whichever of `query_stats_hourly`, plan_shape or
/// query_text (under `--dimension-origin` `query_stats` or `both`) actually needs it, so a run
/// that needs none of the three never reads `collect.query_stats` at all, and a run that needs
/// several still triggers exactly one read. The temp file is deleted once, in the caller's
/// `finally`, when <see cref="DisposeAsync"/> (or <see cref="Dispose"/>) runs -- never
/// per-consumer. Mirrors <see cref="QueryStoreRawWindowCache"/>.
/// </summary>
public sealed class QueryStatsWindowCache : IDisposable
{
    private readonly Func<CancellationToken, Task<string>> _buildRawWindow;
    private string? _tempDbPath;

    /// <summary>How many times the underlying raw-window build actually ran. Test-visible; production code never reads it.</summary>
    public int BuildCount { get; private set; }

    public QueryStatsWindowCache(Func<CancellationToken, Task<string>> buildRawWindow)
    {
        _buildRawWindow = buildRawWindow;
    }

    /// <summary>Builds the standard production cache: one raw window read per (connectionStringBuilder, options).</summary>
    public static QueryStatsWindowCache Create(NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options) =>
        new(ct => QueryStatsWindow.BuildRawWindowFileAsync(connectionStringBuilder, options, ct));

    /// <summary>
    /// Returns the cached raw-window temp file path, building it on the first call. Every
    /// later call in the same run returns the same path without touching the store again.
    /// </summary>
    public Task<string> GetOrBuildAsync(CancellationToken ct) => GetOrBuildAsync(null, ct);

    /// <summary>
    /// Same as <see cref="GetOrBuildAsync(CancellationToken)"/>, and when THIS call runs the build, adds its
    /// wall time to <paramref name="timings"/> (<c>window_cache_build_ms</c>). A call that finds the window
    /// already built, by this or another exporter, adds nothing.
    /// </summary>
    public async Task<string> GetOrBuildAsync(ExportTimings? timings, CancellationToken ct)
    {
        if (_tempDbPath is not null)
        {
            return _tempDbPath;
        }

        var start = System.Diagnostics.Stopwatch.GetTimestamp();
        _tempDbPath = await _buildRawWindow(ct).ConfigureAwait(false);
        BuildCount++;
        timings?.AddWindowCacheBuild(System.Diagnostics.Stopwatch.GetTimestamp() - start);
        return _tempDbPath;
    }

    /// <summary>Deletes the temp file if it was ever built. Safe to call whether or not any consumer used the cache.</summary>
    public void Dispose()
    {
        if (_tempDbPath is not null)
        {
            QueryStatsWindow.TryDeleteTempFile(_tempDbPath);
            _tempDbPath = null;
        }
    }
}
