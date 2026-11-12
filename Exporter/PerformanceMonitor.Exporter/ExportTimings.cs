using System.Diagnostics;
using System.Text;
using Npgsql;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// The split timing fields written next to <c>store_query_elapsed_ms</c> on a manifest table entry. A null field was
/// not measured for that table and is left out of the JSON.
/// </summary>
public sealed record ExportTimingSnapshot(
    double? StoreReadMs,
    double? StoreConnectMs,
    double? WindowCacheBuildMs,
    double? PaceSleepMs,
    double? ParseMs,
    long? ContentFetchCalls,
    long? ContentRowsFetched,
    long? ContentBytesFetched);

/// <summary>
/// Accumulates the pieces that <c>store_query_elapsed_ms</c> lumps together, for one exporter run:
/// time inside store calls, connection open, window-cache builds, local parsing, pacing sleeps, and
/// how much content was fetched. One instance per table export; an exporter wraps each store call
/// and each parse step in it instead of carrying its own stopwatches.
/// </summary>
public sealed class ExportTimings
{
    private ContentFetchPacer? _pacer;
    private long _storeReadTicks;
    private long _connectTicks;
    private long _parseTicks;
    private long _windowCacheBuildTicks;
    private long _contentFetchCalls;
    private long _contentRowsFetched;
    private long _contentBytesFetched;

    /// <param name="pacer">The pacer whose <see cref="ContentFetchPacer.TotalSleepMs"/> is reported as <c>pace_sleep_ms</c>. Null when the exporter does not pace.</param>
    public ExportTimings(ContentFetchPacer? pacer = null)
    {
        _pacer = pacer;
    }

    /// <summary>Sets the pacer whose total sleep is reported as <c>pace_sleep_ms</c>, for an exporter that builds its pacer after this object.</summary>
    public void UsePacer(ContentFetchPacer pacer) => _pacer = pacer;

    /// <summary>Opens a store connection (including the session guards) and adds the elapsed time to <c>store_connect_ms</c>.</summary>
    public async Task<NpgsqlConnection> OpenConnectionAsync(NpgsqlConnectionStringBuilder builder, CancellationToken ct)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            return await StoreConnectionFactory.OpenAsync(builder, ct).ConfigureAwait(false);
        }
        finally
        {
            _connectTicks += Stopwatch.GetTimestamp() - start;
        }
    }

    /// <summary>Runs one store call (command execution through the end of the read loop) and adds its time to <c>store_read_ms</c>.</summary>
    public async Task<T> StoreReadAsync<T>(Func<Task<T>> read)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            return await read().ConfigureAwait(false);
        }
        finally
        {
            _storeReadTicks += Stopwatch.GetTimestamp() - start;
        }
    }

    /// <summary>Runs one local parse step (plan shape hash, feature extraction, gunzip) and adds its time to <c>parse_ms</c>.</summary>
    public T Parse<T>(Func<T> parse)
    {
        var start = Stopwatch.GetTimestamp();
        try
        {
            return parse();
        }
        finally
        {
            _parseTicks += Stopwatch.GetTimestamp() - start;
        }
    }

    /// <summary>Adds the time a window cache's <c>GetOrBuildAsync</c> spent building (called by the cache only when this call built it).</summary>
    public void AddWindowCacheBuild(long elapsedTicks) => _windowCacheBuildTicks += elapsedTicks;

    /// <summary>Counts one content-fetch call that returned <paramref name="rows"/> rows holding <paramref name="bytes"/> bytes.</summary>
    public void RecordContentFetch(long rows, long bytes)
    {
        _contentFetchCalls++;
        _contentRowsFetched += rows;
        _contentBytesFetched += bytes;
    }

    /// <summary>
    /// The raw size of a plan/text content result as it came off the store: the UTF-8 byte count of
    /// each text value plus the length of each gzip value. Not the size after gunzip.
    /// </summary>
    public static (long Rows, long Bytes) MeasureContent(Dictionary<string, (string?, byte[]?)> fetched)
    {
        long bytes = 0;
        foreach (var value in fetched.Values)
        {
            bytes += MeasureValue(value);
        }

        return (fetched.Count, bytes);
    }

    /// <summary>The raw size of one fetched content value: the UTF-8 byte count of its text plus the length of its gzip. See <see cref="MeasureContent"/>.</summary>
    public static long MeasureValue((string?, byte[]?) value)
    {
        var (text, gz) = value;
        long bytes = 0;
        if (text is not null)
        {
            bytes += Encoding.UTF8.GetByteCount(text);
        }

        if (gz is not null)
        {
            bytes += gz.Length;
        }

        return bytes;
    }

    /// <summary>
    /// One paced content fetch: <paramref name="fetch"/> runs inside <see cref="StoreReadAsync{T}"/>
    /// (so its time is store read time, not pacing), the result is counted as one content-fetch call,
    /// and <paramref name="pacer"/> sleeps afterwards as for every content batch.
    /// </summary>
    public Task<Dictionary<string, (string?, byte[]?)>> PacedContentFetchAsync(
        ContentFetchPacer pacer, Func<Task<Dictionary<string, (string?, byte[]?)>>> fetch, CancellationToken ct) =>
        PacedContentFetchCoreAsync(pacer, fetch, ct);

    private async Task<Dictionary<string, (string?, byte[]?)>> PacedContentFetchCoreAsync(
        ContentFetchPacer pacer, Func<Task<Dictionary<string, (string?, byte[]?)>>> fetch, CancellationToken ct)
    {
        // Counting the bytes happens after the pacer has timed the call, so it cannot lengthen the pacing sleep.
        var result = await pacer.RunAsync(() => StoreReadAsync(fetch), ct).ConfigureAwait(false);
        var (rows, bytes) = MeasureContent(result);
        RecordContentFetch(rows, bytes);
        return result;
    }

    private static double ToMs(long ticks) => ticks * 1000.0 / Stopwatch.Frequency;

    /// <summary>
    /// The fields to put on a manifest entry. <paramref name="includeParse"/> and
    /// <paramref name="includeWindowCache"/> say whether this table has a parse step or reads a window
    /// cache at all; when it does not, the field is null rather than 0. Content counters are included
    /// only when <paramref name="includeContent"/> is set.
    /// </summary>
    public ExportTimingSnapshot Snapshot(bool includeParse, bool includeWindowCache, bool includeContent = true) => new(
        StoreReadMs: ToMs(_storeReadTicks),
        StoreConnectMs: ToMs(_connectTicks),
        WindowCacheBuildMs: includeWindowCache ? ToMs(_windowCacheBuildTicks) : null,
        PaceSleepMs: _pacer is null ? null : _pacer.TotalSleepMs,
        ParseMs: includeParse ? ToMs(_parseTicks) : null,
        ContentFetchCalls: includeContent ? _contentFetchCalls : null,
        ContentRowsFetched: includeContent ? _contentRowsFetched : null,
        ContentBytesFetched: includeContent ? _contentBytesFetched : null);
}
