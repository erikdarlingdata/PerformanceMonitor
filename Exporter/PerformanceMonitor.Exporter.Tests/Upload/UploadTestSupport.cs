using PerformanceMonitor.Exporter.Upload;

namespace PerformanceMonitor.Exporter.Tests.Upload;

/// <summary>An uploader that records what it is asked to upload and can be told to fail.</summary>
internal sealed class FakeUploader : IUploader
{
    public List<string> Keys { get; } = [];
    public List<string> Attempts { get; } = [];
    public Func<string, int, Exception?> FailureFor { get; set; } = (_, _) => null;
    public Func<Exception, bool> Permanent { get; set; } = _ => false;
    public string Name { get; init; } = "fake";

    public string Describe(string key) => $"{Name}://{key}";

    public Task UploadFileAsync(string localPath, string key, CancellationToken ct)
    {
        Attempts.Add(key);
        var failure = FailureFor(key, Attempts.Count(a => a == key));
        if (failure is not null)
        {
            throw failure;
        }

        Keys.Add(key);
        return Task.CompletedTask;
    }

    public bool IsPermanent(Exception ex) => Permanent(ex);

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}

internal static class UploadTestSupport
{
    public static readonly DateTime From = new(2026, 9, 26, 8, 0, 0, DateTimeKind.Utc);
    public static readonly DateTime To = new(2026, 9, 26, 9, 0, 0, DateTimeKind.Utc);

    public static ExportOptions Options(string outDir, string store = "store_a") =>
        ExportOptions.Validate(
            store, "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", outDir, null,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, new DateTime(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc));

    /// <summary>Writes one small file per table under <c>outDir/store/&lt;table&gt;/</c> and a manifest for them.</summary>
    public static (Manifest Manifest, string ManifestPath) WriteRun(string outDir, string store, params string[] tables)
    {
        var entries = new List<ManifestTableEntry>();
        foreach (var table in tables)
        {
            var relative = OutputPaths.RelativeParquetPath(store, table, From, To);
            var path = Path.Combine(outDir, store, relative.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            File.WriteAllText(path, "parquet:" + table);
            entries.Add(new ManifestTableEntry
            {
                Table = table,
                OutputFile = relative,
                RowCount = 1,
                ByteSize = new FileInfo(path).Length,
                Sha256 = "x",
                Columns = [],
                StoreQueryElapsedMs = 0,
                WriteElapsedMs = 0,
                MissingContentCount = 0,
            });
        }

        var manifest = new Manifest
        {
            Store = store,
            FromUtc = From,
            ToUtc = To,
            ExporterVersion = "test",
            DarlingSchemaVersion = 1,
            Tables = entries,
        };
        var manifestPath = Path.Combine(outDir, store, "_manifests", $"{store}__run.json");
        Directory.CreateDirectory(Path.GetDirectoryName(manifestPath)!);
        File.WriteAllText(manifestPath, "{\"manifest\":true}");
        return (manifest, manifestPath);
    }

    public static string TempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "darling-export-upload-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }
}
