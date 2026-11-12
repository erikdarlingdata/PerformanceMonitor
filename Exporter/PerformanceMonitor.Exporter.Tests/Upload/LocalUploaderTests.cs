using PerformanceMonitor.Exporter.Upload;
using Xunit;
using static PerformanceMonitor.Exporter.Tests.Upload.UploadTestSupport;

namespace PerformanceMonitor.Exporter.Tests.Upload;

public class LocalUploaderTests : IDisposable
{
    private readonly string _src = TempDir();
    private readonly string _root = TempDir();

    public void Dispose()
    {
        Directory.Delete(_src, recursive: true);
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    private string Source(string content)
    {
        var path = Path.Combine(_src, "file.parquet");
        File.WriteAllText(path, content);
        return path;
    }

    [Fact]
    public async Task Copies_the_file_creating_the_folders_the_key_names()
    {
        await using var uploader = new LocalUploader(_root);

        await uploader.UploadFileAsync(Source("abc"), "p/store_a/t1/f.parquet", CancellationToken.None);

        Assert.Equal("abc", File.ReadAllText(Path.Combine(_root, "p", "store_a", "t1", "f.parquet")));
    }

    [Fact]
    public async Task A_second_upload_to_the_same_key_replaces_the_file()
    {
        await using var uploader = new LocalUploader(_root);

        await uploader.UploadFileAsync(Source("old content that is longer"), "k/f.parquet", CancellationToken.None);
        await uploader.UploadFileAsync(Source("new"), "k/f.parquet", CancellationToken.None);

        Assert.Equal("new", File.ReadAllText(Path.Combine(_root, "k", "f.parquet")));
    }

    [Fact]
    public async Task No_partial_file_is_left_behind_after_success()
    {
        await using var uploader = new LocalUploader(_root);

        await uploader.UploadFileAsync(Source("abc"), "k/f.parquet", CancellationToken.None);

        Assert.Empty(Directory.GetFiles(_root, "*.partial", SearchOption.AllDirectories));
        Assert.Single(Directory.GetFiles(Path.Combine(_root, "k")));
    }

    [Fact]
    public async Task A_reader_never_sees_a_half_written_file_under_the_final_name()
    {
        // Hold the destination file open for reading while it is replaced: the replace must not truncate the file the
        // reader has, which a copy straight onto the final name would do (the reader would see the new, shorter bytes).
        await using var uploader = new LocalUploader(_root);
        await uploader.UploadFileAsync(Source("original-original-original"), "k/f.parquet", CancellationToken.None);
        var finalPath = Path.Combine(_root, "k", "f.parquet");

        string seen;
        using (var reader = new FileStream(finalPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
        {
            await uploader.UploadFileAsync(Source("x"), "k/f.parquet", CancellationToken.None);
            seen = await new StreamReader(reader).ReadToEndAsync();
        }

        Assert.Equal("original-original-original", seen);
        Assert.Equal("x", File.ReadAllText(finalPath));
    }

    [Fact]
    public async Task A_failed_copy_leaves_no_partial_and_keeps_the_previous_file()
    {
        await using var uploader = new LocalUploader(_root);
        await uploader.UploadFileAsync(Source("keep me"), "k/f.parquet", CancellationToken.None);

        await Assert.ThrowsAnyAsync<IOException>(() => uploader.UploadFileAsync(Path.Combine(_src, "missing.parquet"), "k/f.parquet", CancellationToken.None));

        Assert.Equal("keep me", File.ReadAllText(Path.Combine(_root, "k", "f.parquet")));
        Assert.Empty(Directory.GetFiles(_root, "*.partial", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task A_missing_root_folder_is_a_permanent_error_and_is_not_created()
    {
        var missing = Path.Combine(_root, "not-here");
        await using var uploader = new LocalUploader(missing);

        var ex = await Assert.ThrowsAnyAsync<Exception>(() => uploader.UploadFileAsync(Source("abc"), "k/f.parquet", CancellationToken.None));

        Assert.True(uploader.IsPermanent(ex));
        Assert.False(Directory.Exists(missing));
    }

    [Fact]
    public void Access_denied_is_permanent_and_an_ordinary_io_error_is_not()
    {
        var uploader = new LocalUploader(_root);

        Assert.True(uploader.IsPermanent(new UnauthorizedAccessException()));
        Assert.False(uploader.IsPermanent(new IOException("disk blip")));
    }

    [Fact]
    public async Task A_missing_root_stops_RunUpload_after_one_attempt_with_no_retry_delay()
    {
        var outDir = TempDir();
        try
        {
            var (manifest, manifestPath) = WriteRun(outDir, "store_a", "t1");
            await using var uploader = new LocalUploader(Path.Combine(_root, "gone"));

            // The default delays (10 s, 30 s) would make this test slow if the error were treated as transient.
            var started = DateTime.UtcNow;
            await Assert.ThrowsAnyAsync<DirectoryNotFoundException>(() =>
                RunUpload.UploadRunAsync(uploader, new UploadLayout("p"), Options(outDir), manifest, manifestPath, TextWriter.Null, CancellationToken.None));

            Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(5));
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }

    [Fact]
    public async Task A_full_run_lands_a_readable_tree_with_the_manifest_last()
    {
        var outDir = TempDir();
        try
        {
            var (manifest, manifestPath) = WriteRun(outDir, "store_a", "t1", "t2");
            await using var uploader = new LocalUploader(_root);

            await RunUpload.UploadRunAsync(uploader, new UploadLayout("landing"), Options(outDir), manifest, manifestPath, TextWriter.Null, CancellationToken.None);

            foreach (var table in manifest.Tables)
            {
                Assert.Equal("parquet:" + table.Table, File.ReadAllText(Path.Combine(_root, "landing", "store_a", table.OutputFile.Replace('/', Path.DirectorySeparatorChar))));
            }

            Assert.True(File.Exists(Path.Combine(_root, "landing", "store_a", "_manifests", "store_a__run.json")));
        }
        finally
        {
            Directory.Delete(outDir, recursive: true);
        }
    }
}
