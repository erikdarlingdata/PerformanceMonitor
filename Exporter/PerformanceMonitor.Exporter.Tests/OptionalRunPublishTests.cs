using PerformanceMonitor.Exporter.Tests.Upload;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// The optional run's `pg_statement_text` keys are recorded only after the optional manifest is published. A failed
/// upload leaves them unknown, so the next run fetches that text again instead of skipping it as already exported.
/// </summary>
public sealed class OptionalRunPublishTests : IDisposable
{
    private static readonly (string? ServerName, long QueryId)[] Keys = [("srv1", 101L), ("srv1", 102L)];
    private readonly string _root = UploadTestSupport.TempDir();

    private string OutDir => Path.Combine(_root, "out");
    private string StateDir => Path.Combine(_root, "state");

    private ExportOptions Options(string? upload) => ExportOptions.Validate(
        "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", OutDir, null,
        "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, new DateTime(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc),
        upload: upload, stateDir: StateDir, alsoCsv: "pg_statement_stats,pg_statement_text");

    /// <summary>One pg_statement_text part on disk, its manifest entry, and two keys the run fetched for it.</summary>
    private OptionalRunResult Run()
    {
        var (manifest, _) = UploadTestSupport.WriteRun(OutDir, "store_a", "pg_statement_text");
        return new OptionalRunResult(manifest.Tables.ToList(), [], [], 0, Keys.ToList());
    }

    private HashSet<(string? ServerName, long QueryId)> Unknown()
    {
        using var store = StateStore.Open(StateDir);
        return store.FilterUnknownPgTextKeys("store_a", Keys);
    }

    [Fact]
    public async Task A_failed_optional_upload_records_no_pg_text_keys_so_the_next_run_still_sees_them_as_unknown()
    {
        var uploader = new FakeUploader { FailureFor = (_, _) => new IOException("destination refused"), Permanent = _ => true };

        await Assert.ThrowsAsync<IOException>(() => ExportRunner.PublishOptionalRunAsync(
            Options(Path.Combine(_root, "landing")), Run(), TextWriter.Null, CancellationToken.None, _ => uploader));

        Assert.Equal(Keys.ToHashSet(), Unknown());
    }

    [Fact]
    public async Task The_keys_are_recorded_after_the_last_upload_not_before_it()
    {
        var unknownDuringUploads = new List<int>();
        var uploader = new FakeUploader
        {
            FailureFor = (_, _) =>
            {
                unknownDuringUploads.Add(Unknown().Count);
                return null;
            },
        };

        await ExportRunner.PublishOptionalRunAsync(
            Options(Path.Combine(_root, "landing")), Run(), TextWriter.Null, CancellationToken.None, _ => uploader);

        // The part and then the manifest went up, and both times every key was still unknown.
        Assert.Equal(2, uploader.Keys.Count);
        Assert.Equal([Keys.Length, Keys.Length], unknownDuringUploads);
        Assert.Empty(Unknown());
    }

    [Fact]
    public async Task Without_an_upload_the_keys_are_recorded_once_the_manifest_is_written()
    {
        await ExportRunner.PublishOptionalRunAsync(Options(null), Run(), TextWriter.Null, CancellationToken.None);

        Assert.Empty(Unknown());
    }

    [Fact]
    public async Task A_run_with_no_new_keys_records_nothing()
    {
        var (manifest, _) = UploadTestSupport.WriteRun(OutDir, "store_a", "pg_statement_text");

        await ExportRunner.PublishOptionalRunAsync(Options(null), new OptionalRunResult(manifest.Tables.ToList(), [], []), TextWriter.Null, CancellationToken.None);

        Assert.Equal(Keys.ToHashSet(), Unknown());
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { }
    }
}
