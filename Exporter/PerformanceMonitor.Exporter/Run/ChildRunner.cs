using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>One child invocation: the exe, its full argument list, and the two files its output goes to.</summary>
public sealed record ChildCall(string Exe, IReadOnlyList<string> Args, string LogPath, string ErrPath);

public sealed record ChildResult
{
    public int ExitCode { get; init; }
    public bool Aborted { get; init; }
    public int? AbortFreeMb { get; init; }
    public int FloorMb { get; init; }
    public long PeakPrivateMb { get; init; }
    public long? MinFreeMb { get; init; }
    public double ElapsedSeconds { get; init; }
    public string LogPath { get; init; } = "";
    public string ErrPath { get; init; } = "";

    /// <summary>Set when the child never started (for example not enough free memory); the caller treats it as a family failure with this text.</summary>
    public string? NotStarted { get; init; }
}

public interface IChildRunner
{
    Task<ChildResult> RunAsync(ChildCall call, CancellationToken ct);
}

public sealed record ChildRunnerTiming(TimeSpan SampleInterval, int FreeEveryTicks)
{
    /// <summary>The real cadence: child memory every second, free memory every 10 s (R19).</summary>
    public static readonly ChildRunnerTiming Production = new(TimeSpan.FromSeconds(1), 10);
}

/// <summary>
/// Starts the child with the memory guard (R19): refuses to start below `StartMultiplier` x the floor, samples the child's
/// memory every tick and the host's free memory every `FreeEveryTicks`, kills the child below the floor, runs it at BelowNormal
/// priority. A short child returns the moment it exits. An abort is exit 9 (a failure), never 3.
/// </summary>
public sealed class ProcessChildRunner(MemoryGuardSettings guard, IHostMemory memory, TextWriter output, ChildRunnerTiming? timing = null) : IChildRunner
{
    public const int AbortExitCode = 9;
    private readonly ChildRunnerTiming timing = timing ?? ChildRunnerTiming.Production;

    public async Task<ChildResult> RunAsync(ChildCall call, CancellationToken ct)
    {
        var floor = guard.MinFreeMb;
        long? freeAtStart = guard.Enabled ? memory.FreeMb() : null;
        if (freeAtStart is { } f && f < (long)floor * guard.StartMultiplier)
        {
            return new ChildResult
            {
                NotStarted = $"only {f} MB free; need at least {(long)floor * guard.StartMultiplier} MB to start",
                LogPath = call.LogPath, ErrPath = call.ErrPath, FloorMb = floor,
            };
        }

        var psi = new ProcessStartInfo(call.Exe)
        {
            RedirectStandardOutput = true, RedirectStandardError = true, RedirectStandardInput = false,
            UseShellExecute = false, CreateNoWindow = true,
        };
        foreach (var a in call.Args) psi.ArgumentList.Add(a);

        var sw = Stopwatch.StartNew();
        using var proc = new Process { StartInfo = psi };
        await using var logStream = new FileStream(call.LogPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        await using var errStream = new FileStream(call.ErrPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        try { proc.Start(); }
        catch (Win32Exception ex)
        {
            return new ChildResult { NotStarted = $"could not start {call.Exe}: {ex.Message}", LogPath = call.LogPath, ErrPath = call.ErrPath, FloorMb = floor };
        }
        var copyOut = proc.StandardOutput.BaseStream.CopyToAsync(logStream, CancellationToken.None);
        var copyErr = proc.StandardError.BaseStream.CopyToAsync(errStream, CancellationToken.None);
        try { proc.PriorityClass = ProcessPriorityClass.BelowNormal; }
        catch (Exception ex) when (ex is Win32Exception or InvalidOperationException or PlatformNotSupportedException)
        {
            output.WriteLine($"could not lower priority: {ex.Message}");
        }

        long peakMb = 0;
        long? minFree = freeAtStart;
        var aborted = false;
        int? abortFree = null;
        var tick = 0;
        var waitTask = proc.WaitForExitAsync(CancellationToken.None);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(ct);
        while (true)
        {
            var finished = await Task.WhenAny(waitTask, Task.Delay(timing.SampleInterval, linked.Token)).ConfigureAwait(false);
            if (finished == waitTask) break;
            ct.ThrowIfCancellationRequested();
            tick++;
            try
            {
                proc.Refresh();
                if (proc.HasExited) break;
                peakMb = Math.Max(peakMb, proc.PrivateMemorySize64 / (1024 * 1024));
                if (OperatingSystem.IsWindows()) peakMb = Math.Max(peakMb, proc.PeakPagedMemorySize64 / (1024 * 1024));
                else peakMb = Math.Max(peakMb, proc.WorkingSet64 / (1024 * 1024));   // macOS reports 0 private bytes; the working set is the closest figure there
            }
            catch (InvalidOperationException) { break; }   // the child exited between the check and the read
            if (!guard.Enabled || tick % timing.FreeEveryTicks != 0) continue;
            var free = memory.FreeMb();
            if (free is not { } now) continue;
            minFree = minFree is { } m ? Math.Min(m, now) : now;
            if (now < floor)
            {
                output.WriteLine($"ABORTED: host free memory {now} MB is below {floor} MB; stopping the exporter");
                try { proc.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
                aborted = true;
                abortFree = now;
                break;
            }
        }
        linked.Cancel();
        await proc.WaitForExitAsync(CancellationToken.None).ConfigureAwait(false);
        // A killed child's grandchildren can keep the pipes open; do not wait on them for long.
        await Task.WhenAny(Task.WhenAll(copyOut, copyErr), Task.Delay(TimeSpan.FromSeconds(5))).ConfigureAwait(false);
        await logStream.FlushAsync().ConfigureAwait(false);
        await errStream.FlushAsync().ConfigureAwait(false);

        var elapsed = sw.Elapsed.TotalSeconds;
        try
        {
            var own = (proc.ExitTime - proc.StartTime).TotalSeconds;
            if (own >= 0) elapsed = own;
        }
        catch (Exception ex) when (ex is InvalidOperationException or Win32Exception) { }
        var code = aborted ? AbortExitCode : proc.ExitCode;
        return new ChildResult
        {
            ExitCode = code, Aborted = aborted, AbortFreeMb = abortFree, FloorMb = floor,
            PeakPrivateMb = peakMb, MinFreeMb = minFree, ElapsedSeconds = elapsed,
            LogPath = call.LogPath, ErrPath = call.ErrPath,
        };
    }

    public static string Summary(ChildResult r, long? freeAtStart) =>
        string.Format(CultureInfo.InvariantCulture,
            "exit code: {0}; elapsed: {1:N0} s; exporter peak private memory: {2:N0} MB; host free memory at start {3} MB, lowest {4} MB; log: {5}",
            r.ExitCode, r.ElapsedSeconds, r.PeakPrivateMb, freeAtStart?.ToString(CultureInfo.InvariantCulture) ?? "unknown", r.MinFreeMb?.ToString(CultureInfo.InvariantCulture) ?? "unknown", r.LogPath);
}
