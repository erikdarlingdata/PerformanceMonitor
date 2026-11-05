using System.Text.Json;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class ExportTimingsTests
{
    private static ContentFetchPacer NoSleepPacer() =>
        new(sleep: (_, _) => Task.CompletedTask);

    [Fact]
    public async Task Pacer_TotalSleepMs_adds_up_every_sleep_without_changing_the_math()
    {
        var ticks = 0L;
        var slept = new List<int>();
        var pacer = new ContentFetchPacer(
            timestampNow: () => ticks,
            sleep: (ms, _) => { slept.Add(ms); return Task.CompletedTask; });

        Assert.Equal(0, pacer.TotalSleepMs);

        await pacer.RunAsync(() => { ticks += System.Diagnostics.Stopwatch.Frequency / 10; return Task.FromResult(1); }, CancellationToken.None); // 100 ms
        await pacer.RunAsync(() => Task.FromResult(1), CancellationToken.None); // instant call: the 50 ms floor

        Assert.Equal([100, 50], slept);
        Assert.Equal(150, pacer.TotalSleepMs);
    }

    [Fact]
    public async Task PacedContentFetch_counts_calls_rows_and_raw_bytes_and_paces_once_per_call()
    {
        var pacer = NoSleepPacer();
        var timings = new ExportTimings(pacer);
        var gz = new byte[] { 1, 2, 3, 4 };

        var result = await timings.PacedContentFetchAsync(pacer, () => Task.FromResult(new Dictionary<string, (string?, byte[]?)>
        {
            ["aa"] = ("héllo", null), // 6 UTF-8 bytes
            ["bb"] = (null, gz),      // 4 raw column bytes
        }), CancellationToken.None);
        await timings.PacedContentFetchAsync(pacer, () => Task.FromResult(new Dictionary<string, (string?, byte[]?)>()), CancellationToken.None);

        Assert.Equal(2, result.Count);
        var snap = timings.Snapshot(includeParse: false, includeWindowCache: false);
        Assert.Equal(2, snap.ContentFetchCalls);
        Assert.Equal(2, snap.ContentRowsFetched);
        Assert.Equal(10, snap.ContentBytesFetched);
        Assert.Equal(100, snap.PaceSleepMs); // two calls, 50 ms floor each
        Assert.Null(snap.ParseMs);
        Assert.Null(snap.WindowCacheBuildMs);
    }

    [Fact]
    public async Task StoreReadAsync_and_Parse_accumulate_separately()
    {
        var timings = new ExportTimings();
        await timings.StoreReadAsync(async () => { await Task.Delay(30); return 0; });
        timings.Parse(() => { Thread.Sleep(20); return 0; });

        var snap = timings.Snapshot(includeParse: true, includeWindowCache: true);
        Assert.True(snap.StoreReadMs >= 25, $"store_read_ms {snap.StoreReadMs}");
        Assert.True(snap.ParseMs >= 15, $"parse_ms {snap.ParseMs}");
        Assert.True(snap.ParseMs < snap.StoreReadMs + 20);
        Assert.Null(snap.PaceSleepMs); // no pacer given
        Assert.Equal(0, snap.WindowCacheBuildMs);
    }

    [Fact]
    public async Task ProcedurePlan_example_fetch_goes_through_the_pacer_once_per_digest()
    {
        var pacer = NoSleepPacer();
        var timings = new ExportTimings(pacer);
        var fetchCalls = 0;
        var digestA = new byte[] { 0xAA };
        var digestB = new byte[] { 0xBB };

        Task<Dictionary<string, (string?, byte[]?)>> Fetch(List<byte[]> digests)
        {
            fetchCalls++;
            var hex = DimensionContent.ToHexDigest(digests.Single());
            return Task.FromResult(new Dictionary<string, (string?, byte[]?)> { [hex] = ("<ShowPlanXML/>", null) });
        }

        var a = await ProcedurePlanExporter.FetchExampleContentAsync(timings, pacer, Fetch, digestA, CancellationToken.None);
        var b = await ProcedurePlanExporter.FetchExampleContentAsync(timings, pacer, Fetch, digestB, CancellationToken.None);

        Assert.Equal("<ShowPlanXML/>", a);
        Assert.Equal("<ShowPlanXML/>", b);
        Assert.Equal(2, fetchCalls);
        // One pacer sleep (the 50 ms floor) per fetch: the example fetch is paced.
        Assert.Equal(2 * ContentFetchPacer.MinSleepMs, pacer.TotalSleepMs);
        Assert.Equal(2, timings.Snapshot(includeParse: true, includeWindowCache: false).ContentFetchCalls);
    }

    [Fact]
    public async Task ProcedurePlan_example_fetch_returns_null_when_the_store_has_no_content()
    {
        var pacer = NoSleepPacer();
        var timings = new ExportTimings(pacer);

        var content = await ProcedurePlanExporter.FetchExampleContentAsync(
            timings, pacer, _ => Task.FromResult(new Dictionary<string, (string?, byte[]?)>()), [0x01], CancellationToken.None);

        Assert.Null(content);
        Assert.Equal(ContentFetchPacer.MinSleepMs, pacer.TotalSleepMs);
    }

    [Fact]
    public async Task WindowCaches_record_build_time_only_on_the_call_that_built()
    {
        var qs = new QueryStatsWindowCache(async _ => { await Task.Delay(40); return "qs.duckdb"; });
        var first = new ExportTimings();
        var second = new ExportTimings();

        await qs.GetOrBuildAsync(first, CancellationToken.None);
        await qs.GetOrBuildAsync(second, CancellationToken.None);

        Assert.Equal(1, qs.BuildCount);
        Assert.True(first.Snapshot(false, true).WindowCacheBuildMs >= 30);
        Assert.Equal(0, second.Snapshot(false, true).WindowCacheBuildMs);

        var raw = new QueryStoreRawWindowCache(async _ => { await Task.Delay(40); return "qsr.duckdb"; });
        var rawTimings = new ExportTimings();
        await raw.GetOrBuildAsync(rawTimings, CancellationToken.None);
        Assert.True(rawTimings.Snapshot(false, true).WindowCacheBuildMs >= 30);

        var proc = new ProcedureStatsWindowCache(async _ => { await Task.Delay(40); return "ps.duckdb"; });
        var procTimings = new ExportTimings();
        await proc.GetOrBuildAsync(procTimings, CancellationToken.None);
        await proc.GetOrBuildAsync(new ExportTimings(), CancellationToken.None);
        Assert.True(procTimings.Snapshot(false, true).WindowCacheBuildMs >= 30);
    }

    private static ManifestTableEntry Entry(ExportTimingSnapshot? timings) => new()
    {
        Table = "t", OutputFile = "t/x.parquet", RowCount = 1, ByteSize = 1, Sha256 = "ab", Columns = [],
        StoreQueryElapsedMs = 10, WriteElapsedMs = 1, MissingContentCount = 0, Timings = timings,
    };

    private static async Task<JsonElement> WriteEntry(ManifestTableEntry entry)
    {
        var manifest = new Manifest
        {
            Store = "s", FromUtc = DateTime.UnixEpoch, ToUtc = DateTime.UnixEpoch.AddHours(1),
            ExporterVersion = "t", DarlingSchemaVersion = null, Tables = [entry],
        };
        var path = Path.Combine(Path.GetTempPath(), $"manifest-timings-{Guid.NewGuid()}.json");
        try
        {
            await ManifestWriter.WriteAsync(manifest, path, CancellationToken.None);
            using var doc = JsonDocument.Parse(await File.ReadAllTextAsync(path));
            return doc.RootElement.GetProperty("tables")[0].Clone();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task ManifestWriter_omits_timing_fields_when_none_were_measured()
    {
        var json = await WriteEntry(Entry(null));
        foreach (var key in new[] { "store_read_ms", "store_connect_ms", "window_cache_build_ms", "pace_sleep_ms", "parse_ms", "content_fetch_calls", "content_rows_fetched", "content_bytes_fetched" })
        {
            Assert.False(json.TryGetProperty(key, out _), key);
        }

        Assert.Equal(10, json.GetProperty("store_query_elapsed_ms").GetDouble()); // kept as is
    }

    [Fact]
    public async Task ManifestWriter_writes_the_measured_timing_fields_and_skips_null_ones()
    {
        var json = await WriteEntry(Entry(new ExportTimingSnapshot(
            StoreReadMs: 5.5, StoreConnectMs: 2.5, WindowCacheBuildMs: null, PaceSleepMs: 100, ParseMs: null,
            ContentFetchCalls: 3, ContentRowsFetched: 40, ContentBytesFetched: 999)));

        Assert.Equal(5.5, json.GetProperty("store_read_ms").GetDouble());
        Assert.Equal(2.5, json.GetProperty("store_connect_ms").GetDouble());
        Assert.Equal(100, json.GetProperty("pace_sleep_ms").GetDouble());
        Assert.Equal(3, json.GetProperty("content_fetch_calls").GetInt64());
        Assert.Equal(40, json.GetProperty("content_rows_fetched").GetInt64());
        Assert.Equal(999, json.GetProperty("content_bytes_fetched").GetInt64());
        Assert.False(json.TryGetProperty("window_cache_build_ms", out _));
        Assert.False(json.TryGetProperty("parse_ms", out _));
        Assert.Equal(10, json.GetProperty("store_query_elapsed_ms").GetDouble());
    }

    [Fact]
    public void CoreTableMetadata_WithMetadata_keeps_the_timing_fields()
    {
        var snap = new ExportTimingSnapshot(1, 2, 3, 4, 5, 6, 7, 8);
        var kept = CoreTableMetadata.WithMetadata(Entry(snap));
        Assert.Equal(snap, kept.Timings);
    }
}
