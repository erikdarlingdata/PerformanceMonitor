namespace PerformanceMonitor.Exporter.Upload;

/// <summary>
/// Uploads to a local or network (UNC) folder. Each file is copied to a hidden <c>.&lt;name&gt;.&lt;guid&gt;.partial</c>
/// file in the destination folder and then moved over the final name, so a reader never opens a half-written file.
/// Whether the move is atomic on every SMB server is not verified; trust only files a manifest lists.
/// </summary>
public sealed class LocalUploader : IUploader
{
    private readonly string _root;

    public LocalUploader(string rootPath)
    {
        _root = rootPath;
    }

    public string Describe(string key) => "file://" + ResolvePath(key).Replace('\\', '/');

    public async Task UploadFileAsync(string localPath, string key, CancellationToken ct)
    {
        if (!Directory.Exists(_root))
        {
            throw new LocalRootMissingException($"The destination folder '{_root}' does not exist.");
        }

        var finalPath = ResolvePath(key);
        var directory = Path.GetDirectoryName(finalPath)!;
        Directory.CreateDirectory(directory);

        var partialPath = Path.Combine(directory, $".{Path.GetFileName(finalPath)}.{Guid.NewGuid():N}.partial");
        try
        {
            await using (var source = new FileStream(localPath, FileMode.Open, FileAccess.Read, FileShare.Read, 81920, FileOptions.Asynchronous | FileOptions.SequentialScan))
            await using (var target = new FileStream(partialPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 81920, FileOptions.Asynchronous))
            {
                await source.CopyToAsync(target, 81920, ct).ConfigureAwait(false);
                await target.FlushAsync(ct).ConfigureAwait(false);
                target.Flush(flushToDisk: true);
            }

            File.Move(partialPath, finalPath, overwrite: true);
        }
        catch
        {
            TryDelete(partialPath);
            throw;
        }
    }

    /// <summary>Access denied, or a root folder that does not exist: a retry cannot fix either.</summary>
    public bool IsPermanent(Exception ex) => ex is UnauthorizedAccessException or LocalRootMissingException;

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private string ResolvePath(string key) => Path.Combine(_root, key.Replace('/', Path.DirectorySeparatorChar));

    private static void TryDelete(string path)
    {
        try
        {
            File.Delete(path);
        }
        catch (Exception)
        {
            // Best effort: the original failure is the one worth reporting.
        }
    }

    private sealed class LocalRootMissingException(string message) : DirectoryNotFoundException(message);
}
