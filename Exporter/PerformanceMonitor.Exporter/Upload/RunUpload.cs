namespace PerformanceMonitor.Exporter.Upload;

/// <summary>
/// Uploads a finished local export: every table's Parquet file first, the manifest last, so a manifest present at the
/// destination always means every file it lists is already there. The first failed file stops the run before the
/// manifest is uploaded. Never logs a credential.
/// </summary>
public static class RunUpload
{
    /// <summary>
    /// Uploads every table's Parquet file named in the manifest, then the manifest file itself. Throws on the first
    /// failure (after the retries in <see cref="UploadRetry"/>) and does not upload the manifest if any file failed.
    /// <paramref name="retryDelay"/> is for tests; the default is 10 s then 30 s.
    /// </summary>
    public static async Task UploadRunAsync(
        IUploader uploader, UploadLayout layout, ExportOptions options, Manifest manifest, string manifestPath, TextWriter output,
        CancellationToken ct, Func<int, TimeSpan>? retryDelay = null)
    {
        foreach (var table in manifest.Tables)
        {
            var localPath = Path.Combine(options.OutDir, options.Store, table.OutputFile.Replace('/', Path.DirectorySeparatorChar));
            var key = layout.ParquetKey(options.Store, table.OutputFile);

            output.WriteLine($"Uploading {table.Table} -> {uploader.Describe(key)}");
            await UploadRetry.WithRetryAsync(() => uploader.UploadFileAsync(localPath, key, ct), key, output, ct, uploader.IsPermanent, retryDelay).ConfigureAwait(false);
        }

        var manifestKey = layout.ManifestKey(options.Store, Path.GetFileName(manifestPath));

        output.WriteLine($"Uploading manifest -> {uploader.Describe(manifestKey)}");
        await UploadRetry.WithRetryAsync(() => uploader.UploadFileAsync(manifestPath, manifestKey, ct), manifestKey, output, ct, uploader.IsPermanent, retryDelay).ConfigureAwait(false);
    }
}
