using System.ComponentModel;
using System.Diagnostics;

namespace PerformanceMonitor.Exporter.Run;

public sealed record PostRunResult(int ExitCode, string LogPath);

/// <summary>
/// The post-run hook (R24; for example an alert check). It runs whether or not the export loop failed, and its failure never touches
/// export state: the caller only flips the run to failed. Output goes to one per-run file, folded into the daily log.
/// </summary>
public static class PostRunHook
{
    public const int TimeoutExitCode = 124;
    public const int StartFailedExitCode = 127;

    public static async Task<PostRunResult> RunAsync(PostRunSettings hook, string logDir, DateTime nowUtc, CancellationToken ct)
    {
        var logPath = Path.Combine(logDir, $"{hook.Name}-{nowUtc:yyyyMMdd-HHmmss}.log");
        var psi = new ProcessStartInfo(hook.Command[0])
        {
            RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true,
        };
        foreach (var a in hook.Command.Skip(1)) psi.ArgumentList.Add(a);
        try
        {
            using var proc = Process.Start(psi);
            if (proc is null) return new PostRunResult(StartFailedExitCode, logPath);
            var stdout = proc.StandardOutput.ReadToEndAsync();
            var stderr = proc.StandardError.ReadToEndAsync();
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
            timeout.CancelAfter(TimeSpan.FromSeconds(hook.TimeoutSeconds));
            try
            {
                await proc.WaitForExitAsync(timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                try { proc.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
                await File.WriteAllTextAsync(logPath, $"post-run {hook.Name} timed out after {hook.TimeoutSeconds} s and was stopped\n", CancellationToken.None).ConfigureAwait(false);
                return new PostRunResult(TimeoutExitCode, logPath);
            }
            await File.WriteAllTextAsync(logPath, await stdout.ConfigureAwait(false) + await stderr.ConfigureAwait(false), CancellationToken.None).ConfigureAwait(false);
            return new PostRunResult(proc.ExitCode, logPath);
        }
        catch (Win32Exception ex)
        {
            await File.WriteAllTextAsync(logPath, $"could not start {hook.Command[0]}: {ex.Message}\n", CancellationToken.None).ConfigureAwait(false);
            return new PostRunResult(StartFailedExitCode, logPath);
        }
    }
}
