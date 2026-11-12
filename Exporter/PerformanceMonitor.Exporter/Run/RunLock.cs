namespace PerformanceMonitor.Exporter.Run;

/// <summary>
/// Single instance (R18): `&lt;state&gt;/export-run.lock` opened with FileShare.None. A second run sees an IOException and exits 0.
/// The OS drops the lock when the process dies, so a killed run never leaves a stale lock behind.
/// </summary>
public sealed class RunLock : IDisposable
{
    private readonly FileStream stream;

    private RunLock(FileStream s) => stream = s;

    public static string PathFor(string stateDir) => Path.Combine(stateDir, "export-run.lock");

    /// <summary>The held lock, or null when another run holds it.</summary>
    public static RunLock? TryAcquire(string stateDir)
    {
        Directory.CreateDirectory(stateDir);
        try
        {
            return new RunLock(new FileStream(PathFor(stateDir), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None));
        }
        catch (IOException)
        {
            return null;
        }
    }

    public void Dispose() => stream.Dispose();
}
