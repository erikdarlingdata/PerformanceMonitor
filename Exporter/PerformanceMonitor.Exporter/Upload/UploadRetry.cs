namespace PerformanceMonitor.Exporter.Upload;

/// <summary>The per-file retry policy.</summary>
public static class UploadRetry
{
    /// <summary>
    /// Attempts per file. A large multipart upload once failed on a single bad part with a checksum error and the same
    /// upload then succeeded from another machine; TransferUtility does not retry a failed part and aborts the whole
    /// upload, so one bad part cost the whole export. Re-uploading the file is safe: it overwrites the same key, and
    /// the manifest is uploaded only after every file.
    /// </summary>
    public const int MaxUploadAttempts = 3;

    /// <summary>The wait after a failed attempt: 10 s after the first, 30 s after the second.</summary>
    public static TimeSpan DefaultDelay(int failedAttempt) => TimeSpan.FromSeconds(failedAttempt == 1 ? 10 : 30);

    /// <summary>
    /// Runs <paramref name="upload"/> up to <see cref="MaxUploadAttempts"/> times, waiting 10 s and then 30 s between
    /// attempts. It does not retry on cancellation or on an exception <paramref name="isPermanent"/> accepts, which a
    /// retry cannot fix. The last failure is thrown.
    /// </summary>
    public static async Task WithRetryAsync(
        Func<Task> upload, string key, TextWriter output, CancellationToken ct, Func<Exception, bool> isPermanent, Func<int, TimeSpan>? delayForAttempt = null)
    {
        delayForAttempt ??= DefaultDelay;
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await upload().ConfigureAwait(false);
                return;
            }
            catch (Exception ex) when (attempt < MaxUploadAttempts && !ct.IsCancellationRequested && !isPermanent(ex))
            {
                var delay = delayForAttempt(attempt);
                output.WriteLine($"  upload attempt {attempt} of {MaxUploadAttempts} for {key} failed ({ex.GetType().Name}: {ex.Message.Trim()}); retrying in {delay.TotalSeconds:F0}s");
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
    }
}
