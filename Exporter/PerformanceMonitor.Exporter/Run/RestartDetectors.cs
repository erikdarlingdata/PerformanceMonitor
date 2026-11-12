using System.Diagnostics;
using System.Globalization;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>When did the monitored store last restart? Null means unknown, which means no delay.</summary>
public interface IRestartDetector
{
    DateTime? RestartUtc();
}

public static class RestartDelay
{
    /// <summary>True only when the restart is known, not in the future (clock skew), and less than delayMinutes old. Delay 0 is never inside.</summary>
    public static bool IsWithin(DateTime nowUtc, DateTime? restartUtc, int delayMinutes)
    {
        if (restartUtc is not { } restart || delayMinutes <= 0) return false;
        if (restart > nowUtc) return false;
        return (nowUtc - restart).TotalMinutes < delayMinutes;
    }

    /// <summary>A test hook, like DARLING_EXPORT_TEST_FREE_MB: an ISO UTC instant here is the restart time, whatever the detector.</summary>
    public const string TestRestartUtcEnv = "DARLING_EXPORT_TEST_RESTART_UTC";

    public static IRestartDetector Create(RestartSettings s, TextWriter warn) =>
        Environment.GetEnvironmentVariable(TestRestartUtcEnv) is { Length: > 0 } fake
            && DateTime.TryParse(fake, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var t)
            ? new FixedRestartDetector(t)
            : s.Detector switch
    {
        "darling_process" => new DarlingProcessDetector(new WindowsDarlingProcessSource(s.DarlingServiceName, s.DarlingRuntimePath)),
        "file" => new FileRestartDetector(s.File!),
        "command" => new CommandRestartDetector(s.Command!, TimeSpan.FromSeconds(10)),
        _ => new NoRestartDetector(),
    };
}

public sealed class FixedRestartDetector(DateTime restartUtc) : IRestartDetector
{
    public DateTime? RestartUtc() => restartUtc;
}

public sealed class NoRestartDetector : IRestartDetector
{
    public DateTime? RestartUtc() => null;
}

/// <summary>The last-write time of a path (for example postmaster.pid). Reads metadata only.</summary>
public sealed class FileRestartDetector(string path) : IRestartDetector
{
    public DateTime? RestartUtc()
    {
        try
        {
            return File.Exists(path) ? File.GetLastWriteTimeUtc(path) : null;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return null;
        }
    }
}

/// <summary>Runs argv; the first stdout line must be an ISO UTC instant. A failure, a timeout or anything else means unknown.</summary>
public sealed class CommandRestartDetector(IReadOnlyList<string> command, TimeSpan timeout) : IRestartDetector
{
    public DateTime? RestartUtc()
    {
        try
        {
            var psi = new ProcessStartInfo(command[0]) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
            foreach (var a in command.Skip(1)) psi.ArgumentList.Add(a);
            using var proc = Process.Start(psi);
            if (proc is null) return null;
            var first = proc.StandardOutput.ReadLineAsync();
            if (!proc.WaitForExit(timeout))
            {
                try { proc.Kill(entireProcessTree: true); } catch (InvalidOperationException) { }
                return null;
            }
            if (proc.ExitCode != 0) return null;
            var line = first.GetAwaiter().GetResult();
            return DateTime.TryParse(line?.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var t) ? t : null;
        }
        catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException or IOException)
        {
            return null;
        }
    }
}

/// <summary>The process start times the darling_process detector combines (a seam: the real source is Windows-only).</summary>
public interface IDarlingProcessSource
{
    /// <summary>The start time of the Darling service's process, or null.</summary>
    DateTime? ServiceProcessStartUtc();

    /// <summary>The start times of postgres processes running from the Darling runtime folder.</summary>
    IReadOnlyList<DateTime> PostgresStartUtcs();
}

/// <summary>The LATER of the service process's start and the oldest runtime postgres start (the postmaster). Never throws.</summary>
public sealed class DarlingProcessDetector(IDarlingProcessSource source) : IRestartDetector
{
    public DateTime? RestartUtc()
    {
        try
        {
            var times = new List<DateTime>();
            try { if (source.ServiceProcessStartUtc() is { } svc) times.Add(svc); } catch (Exception) { }
            try
            {
                var pg = source.PostgresStartUtcs();
                if (pg.Count > 0) times.Add(pg.Min());
            }
            catch (Exception) { }
            return times.Count == 0 ? null : times.Max();
        }
        catch (Exception)
        {
            return null;
        }
    }
}

public sealed class WindowsDarlingProcessSource(string serviceName, string runtimePath) : IDarlingProcessSource
{
    public DateTime? ServiceProcessStartUtc()
    {
        if (!OperatingSystem.IsWindows()) return null;
        var psi = new ProcessStartInfo("sc.exe") { RedirectStandardOutput = true, UseShellExecute = false, CreateNoWindow = true };
        psi.ArgumentList.Add("queryex");
        psi.ArgumentList.Add(serviceName);
        using var proc = Process.Start(psi);
        if (proc is null) return null;
        var text = proc.StandardOutput.ReadToEnd();
        proc.WaitForExit(10_000);
        var m = System.Text.RegularExpressions.Regex.Match(text, @"PID\s*:\s*(\d+)");
        if (!m.Success || int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture) <= 0) return null;
        return Process.GetProcessById(int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture)).StartTime.ToUniversalTime();
    }

    public IReadOnlyList<DateTime> PostgresStartUtcs()
    {
        var result = new List<DateTime>();
        if (!OperatingSystem.IsWindows()) return result;
        var prefix = runtimePath.TrimEnd('\\', '/') + "\\";
        foreach (var p in Process.GetProcessesByName("postgres"))
        {
            try
            {
                var path = p.MainModule?.FileName;
                if (path is not null && path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) result.Add(p.StartTime.ToUniversalTime());
            }
            catch (Exception) { }
            finally { p.Dispose(); }
        }
        return result;
    }
}
