using PerformanceMonitor.Exporter.Upload;
using Xunit;
using static PerformanceMonitor.Exporter.Tests.Upload.UploadTestSupport;

namespace PerformanceMonitor.Exporter.Tests.Upload;

public class RunUploadTests : IDisposable
{
    private static readonly Func<int, TimeSpan> NoDelay = _ => TimeSpan.Zero;
    private readonly string _dir = TempDir();

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private Task Run(FakeUploader fake, UploadLayout layout, Manifest manifest, string manifestPath, StringWriter? log = null, CancellationToken ct = default) =>
        RunUpload.UploadRunAsync(fake, layout, Options(_dir), manifest, manifestPath, log ?? new StringWriter(), ct, NoDelay);

    [Fact]
    public async Task Every_parquet_file_uploads_in_manifest_order_and_the_manifest_uploads_last()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a", "wait_stats", "plan_shape", "query_stats");
        var fake = new FakeUploader();

        await Run(fake, new UploadLayout("exports"), manifest, manifestPath);

        Assert.Equal(4, fake.Keys.Count);
        Assert.Equal(manifest.Tables.Select(t => "exports/store_a/" + t.OutputFile), fake.Keys.Take(3));
        Assert.Equal("exports/store_a/_manifests/store_a__run.json", fake.Keys[3]);
    }

    [Fact]
    public async Task The_first_failure_stops_the_run_and_no_manifest_is_uploaded()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a", "t1", "t2", "t3");
        var failingKey = new UploadLayout("p").ParquetKey("store_a", manifest.Tables[1].OutputFile);
        var fake = new FakeUploader { FailureFor = (key, _) => key == failingKey ? new IOException("boom") : null };

        await Assert.ThrowsAsync<IOException>(() => Run(fake, new UploadLayout("p"), manifest, manifestPath));

        Assert.Equal(["p/store_a/" + manifest.Tables[0].OutputFile], fake.Keys);
        Assert.DoesNotContain(fake.Attempts, k => k.Contains("_manifests"));
        Assert.DoesNotContain(fake.Attempts, k => k.Contains(manifest.Tables[2].OutputFile));
    }

    [Fact]
    public async Task A_transient_failure_is_retried_on_the_same_key_and_the_run_then_completes()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a", "t1", "t2");
        var flaky = "p/store_a/" + manifest.Tables[0].OutputFile;
        var fake = new FakeUploader { FailureFor = (key, attempt) => key == flaky && attempt < 3 ? new IOException("blip") : null };
        var log = new StringWriter();

        await Run(fake, new UploadLayout("p"), manifest, manifestPath, log);

        Assert.Equal(3, fake.Attempts.Count(k => k == flaky));
        Assert.Equal(3, fake.Keys.Count);
        Assert.EndsWith("_manifests/store_a__run.json", fake.Keys[^1]);
        Assert.Contains("retrying in 0s", log.ToString());
    }

    [Fact]
    public async Task A_permanent_failure_is_not_retried_and_no_manifest_is_uploaded()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a", "t1");
        var fake = new FakeUploader
        {
            FailureFor = (_, _) => new UnauthorizedAccessException("denied"),
            Permanent = ex => ex is UnauthorizedAccessException,
        };

        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => Run(fake, new UploadLayout("p"), manifest, manifestPath));

        Assert.Single(fake.Attempts);
    }

    [Fact]
    public async Task A_failed_manifest_upload_is_retried_and_then_thrown()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a", "t1");
        var fake = new FakeUploader { FailureFor = (key, _) => key.Contains("_manifests") ? new IOException("no") : null };

        await Assert.ThrowsAsync<IOException>(() => Run(fake, new UploadLayout("p"), manifest, manifestPath));

        Assert.Equal(3, fake.Attempts.Count(k => k.Contains("_manifests")));
    }

    [Fact]
    public async Task A_cancelled_run_stops_without_uploading_the_manifest()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a", "t1", "t2");
        using var cts = new CancellationTokenSource();
        var fake = new FakeUploader { FailureFor = (_, _) => { cts.Cancel(); return new OperationCanceledException(); } };

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Run(fake, new UploadLayout("p"), manifest, manifestPath, ct: cts.Token));

        Assert.Single(fake.Attempts);
    }

    [Fact]
    public async Task The_same_window_gives_the_same_keys_so_a_re_export_overwrites()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a", "t1");
        var first = new FakeUploader();
        var second = new FakeUploader();

        await Run(first, new UploadLayout("p"), manifest, manifestPath);
        await Run(second, new UploadLayout("p"), manifest, manifestPath);

        Assert.Equal(first.Keys, second.Keys);
    }

    [Fact]
    public async Task The_log_line_uses_the_uploaders_own_description_of_the_destination()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a", "t1");
        var log = new StringWriter();

        await Run(new FakeUploader { Name = "mem" }, new UploadLayout("p"), manifest, manifestPath, log);

        Assert.Contains("Uploading t1 -> mem://p/store_a/t1/", log.ToString());
        Assert.Contains("Uploading manifest -> mem://p/store_a/_manifests/store_a__run.json", log.ToString());
    }

    [Fact]
    public async Task Hive_store_folder_changes_every_key_the_run_uploads()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a", "t1");
        var fake = new FakeUploader();

        await Run(fake, new UploadLayout("p", HiveStoreFolder: true), manifest, manifestPath);

        Assert.All(fake.Keys, k => Assert.StartsWith("p/store=store_a/", k));
    }

    [Fact]
    public async Task An_empty_manifest_table_list_uploads_only_the_manifest()
    {
        var (manifest, manifestPath) = WriteRun(_dir, "store_a");
        var fake = new FakeUploader();

        await Run(fake, new UploadLayout(""), manifest, manifestPath);

        Assert.Equal(["store_a/_manifests/store_a__run.json"], fake.Keys);
    }
}
