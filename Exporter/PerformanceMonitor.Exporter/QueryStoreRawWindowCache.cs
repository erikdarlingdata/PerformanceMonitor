using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Run-scoped cache around one Query Store raw-window DuckDB temp file (<see
/// cref="QueryStoreWindow.BuildRawWindowFileAsync"/>). Built at most once per <see
/// cref="ExportRunner.RunAsync"/>, on the first consumer that actually needs it --
/// `query_store_stats`, or plan_shape/query_text when `--dimension-origin` is `query_store` or
/// `both` -- so a run that needs only the Query Store keys (not the `query_store_stats` table
/// itself) still triggers exactly one raw read, and a run that needs none of the three never
/// reads Query Store at all. The temp file is deleted once, in the caller's `finally`, when
/// <see cref="DisposeAsync"/> (or <see cref="Dispose"/>) runs -- never per-consumer.
/// </summary>
public sealed class QueryStoreRawWindowCache : IDisposable
{
    private readonly Func<CancellationToken, Task<string>> _buildRawWindow;
    private string? _tempDbPath;
    private readonly QueryStoreSourceReport? _sourceReport;

    /// <summary>How many times the underlying raw-window build actually ran. Test-visible; production code never reads it.</summary>
    public int BuildCount { get; private set; }

    public QueryStoreRawWindowCache(Func<CancellationToken, Task<string>> buildRawWindow)
        : this(buildRawWindow, null)
    {
    }

    /// <param name="buildRawWindow">Builds the window file.</param>
    /// <param name="sourceReport">What the build did (`--query-store-source wide|compare`), filled in by the build. Null in raw mode.</param>
    public QueryStoreRawWindowCache(Func<CancellationToken, Task<string>> buildRawWindow, QueryStoreSourceReport? sourceReport)
    {
        _buildRawWindow = buildRawWindow;
        _sourceReport = sourceReport;
    }

    /// <summary>
    /// What the window build did when `--query-store-source` is `wide` or `compare`; null in raw mode and before the build ran.
    /// Read by <see cref="QueryStoreStatsExporter"/> for the manifest.
    /// </summary>
    public QueryStoreSourceReport? SourceReport => _sourceReport;

    /// <summary>
    /// Builds the standard production cache: one window read per (connectionStringBuilder, options). In raw mode (the
    /// default) that is exactly the raw read, with no extra store query; in wide/compare mode see
    /// <see cref="QueryStoreWindow.BuildWindowFileAsync"/>.
    /// </summary>
    public static QueryStoreRawWindowCache Create(NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options) =>
        Create(connectionStringBuilder, options, () => DateTime.UtcNow);

    /// <summary>Same as <see cref="Create(NpgsqlConnectionStringBuilder, ExportOptions)"/> with the clock the wide gate's 8-day rule reads (a test injects it).</summary>
    public static QueryStoreRawWindowCache Create(NpgsqlConnectionStringBuilder connectionStringBuilder, ExportOptions options, Func<DateTime> utcNow)
    {
        if (options.QueryStoreSource == QueryStoreSourceMode.Raw)
        {
            return new(ct => QueryStoreWindow.BuildRawWindowFileAsync(connectionStringBuilder, options, ct));
        }

        var report = new QueryStoreSourceReport { Mode = options.QueryStoreSource };
        return new(ct => QueryStoreWindow.BuildWindowFileAsync(connectionStringBuilder, options, report, utcNow, ct), report);
    }

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
            QueryStoreWindow.TryDeleteTempFile(_tempDbPath);
            _tempDbPath = null;
        }
    }
}
