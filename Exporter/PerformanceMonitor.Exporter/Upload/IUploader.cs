namespace PerformanceMonitor.Exporter.Upload;

/// <summary>
/// One upload destination (a local or network folder, an S3-compatible bucket, an Azure Blob container). The run
/// logic in <see cref="RunUpload"/> owns the order of files and the retry policy; an uploader only moves one file to
/// one key and says which failures a retry cannot fix.
/// </summary>
public interface IUploader : IAsyncDisposable
{
    /// <summary>Where <paramref name="key"/> lands, for the log line: <c>s3://bucket/key</c>, <c>azure://account/container/key</c> or <c>file://path</c>.</summary>
    string Describe(string key);

    /// <summary>
    /// Uploads one file to <paramref name="key"/>, replacing whatever is there. The key is always '/'-separated.
    /// A reader must never see a half-written file under the key.
    /// </summary>
    Task UploadFileAsync(string localPath, string key, CancellationToken ct);

    /// <summary>True when a retry cannot fix the failure (access denied, missing bucket or folder).</summary>
    bool IsPermanent(Exception ex);
}
