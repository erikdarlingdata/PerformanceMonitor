/*
 * SQL Server Performance Monitor Lite
 *
 * Method profiler for tracking slow application code
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Profiles method execution time and logs slow methods.
/// Usage: using var _ = MethodProfiler.StartTiming("context");
/// </summary>
public static class MethodProfiler
{
    private static string s_logDirectory = "";
    private static readonly object _lock = new();
    private static volatile bool _isEnabled = true;
    private static double _thresholdMs = 500;

    public static void Initialize(string logDirectory)
    {
        s_logDirectory = logDirectory;
        try
        {
            if (!Directory.Exists(s_logDirectory))
                Directory.CreateDirectory(s_logDirectory);
            CleanOldLogs();
        }
        catch { }
    }

    public static string GetCurrentLogFile()
        => Path.Combine(s_logDirectory, $"MethodProfile_{DateTime.Now:yyyyMMdd}.log");

    public static string GetLogDirectory() => s_logDirectory;

    public static void SetEnabled(bool enabled) => _isEnabled = enabled;
    public static bool IsEnabled => _isEnabled;

    public static void SetThresholdMs(double thresholdMs) => _thresholdMs = thresholdMs;
    public static double ThresholdMs => _thresholdMs;

    public static MethodTimingContext StartTiming(
        string? context = null,
        [CallerMemberName] string memberName = "",
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0)
    {
        return new MethodTimingContext(context, memberName, filePath, lineNumber);
    }

    /// <summary>
    /// Times an async operation under <paramref name="context"/>, logging it if it exceeds the
    /// slow-method threshold. Use for per-section profiling of parallel data loads, e.g.:
    /// <code>var task = MethodProfiler.TimeAsync("Overview.DefaultTrace", () => LoadDefaultTraceAsync());</code>
    /// The timing covers the full wall-clock of the operation (including any connection-throttle wait).
    ///
    /// <para>An operation that ends in <see cref="OperationCanceledException"/> (#5371), one a newer request stopped on purpose,
    /// is not a slow method: at or past the threshold it logs ONE line, <c>&lt;context&gt; cancelled after N ms (superseded)</c>,
    /// and no <c>SLOW METHOD</c> block; below it, nothing. A run that long before the cancel is the contention signal.
    /// <paramref name="readLogsOwnCancellation"/> is for a read that writes that same line itself, with its phases
    /// (<see cref="PerformanceMonitorLite.Services.ReadPhaseTimer"/>), so it is not logged twice.</para>
    /// </summary>
    public static async Task<T> TimeAsync<T>(
        string context,
        Func<Task<T>> operation,
        bool readLogsOwnCancellation = false,
        [CallerMemberName] string memberName = "",
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0)
    {
        using var timing = StartTiming(context, memberName, filePath, lineNumber);
        try
        {
            return await operation();
        }
        catch (OperationCanceledException)
        {
            /* #5371: an operation a newer request stopped on purpose is not a slow method: it logs one "cancelled after" line instead. */
            timing.Cancel(readLogsOwnCancellation);
            throw;
        }
    }

    /// <summary>
    /// Void-returning overload of <see cref="TimeAsync{T}"/>.
    /// </summary>
    public static async Task TimeAsync(
        string context,
        Func<Task> operation,
        bool readLogsOwnCancellation = false,
        [CallerMemberName] string memberName = "",
        [CallerFilePath] string filePath = "",
        [CallerLineNumber] int lineNumber = 0)
    {
        using var timing = StartTiming(context, memberName, filePath, lineNumber);
        try
        {
            await operation();
        }
        catch (OperationCanceledException)
        {
            timing.Cancel(readLogsOwnCancellation);
            throw;
        }
    }

    internal static void LogSlowMethod(
        DateTime startTime,
        DateTime endTime,
        double elapsedMs,
        string? context,
        string memberName,
        string filePath,
        int lineNumber)
    {
        if (!_isEnabled || string.IsNullOrEmpty(s_logDirectory))
            return;

        if (elapsedMs < _thresholdMs)
            return;

        try
        {
            lock (_lock)
            {
                var sb = new StringBuilder();
                sb.AppendLine("--------------------------------------------------------------------------------");
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "SLOW METHOD: {0:F0}ms - {1}", elapsedMs, memberName));
                sb.AppendLine("--------------------------------------------------------------------------------");
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Start Time:   {0:yyyy-MM-dd HH:mm:ss.fff}", startTime));
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "End Time:     {0:yyyy-MM-dd HH:mm:ss.fff}", endTime));
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Elapsed:      {0:F0}ms", elapsedMs));

                if (!string.IsNullOrEmpty(context))
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Context:      {0}", context));

                var fileName = Path.GetFileName(filePath);
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Location:     {0}:{1}", fileName, lineNumber));
                sb.AppendLine();

                /* #4281 review: AppendAllText's strict UTF-8 encoder throws on a lone surrogate, dropping the whole batch. */
                File.AppendAllText(GetCurrentLogFile(), sb.ToString(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            }
        }
        catch { }
    }

    /// <summary>
    /// The one line a cancelled timed operation leaves (#5371), in the format <c>ReadPhaseTimer</c> uses for a cancelled read.
    /// It has to pass the same threshold the SLOW METHOD block does, since an operation cut off in a few milliseconds is noise.
    /// </summary>
    internal static void LogCancelledMethod(double elapsedMs, string? context, string memberName)
    {
        if (!_isEnabled || elapsedMs < _thresholdMs)
            return;

        var name = string.IsNullOrEmpty(context) ? memberName : context;
        AppLogger.Info("MethodProfiler", string.Format(CultureInfo.InvariantCulture, "{0} cancelled after {1:F0} ms (superseded)", name, elapsedMs));
    }

    private static void CleanOldLogs()
    {
        try
        {
            var files = Directory.GetFiles(s_logDirectory, "MethodProfile_*.log");
            var cutoff = DateTime.Now.AddDays(-7);
            foreach (var file in files)
            {
                if (new FileInfo(file).CreationTime < cutoff)
                    File.Delete(file);
            }
        }
        catch { }
    }
}

/// <summary>
/// Disposable context for timing method execution.
/// </summary>
public sealed class MethodTimingContext : IDisposable
{
    private readonly DateTime _startTime;
    private readonly Stopwatch _stopwatch;
    private readonly string? _context;
    private readonly string _memberName;
    private readonly string _filePath;
    private readonly int _lineNumber;
    private bool _disposed;

    internal MethodTimingContext(string? context, string memberName, string filePath, int lineNumber)
    {
        _startTime = DateTime.Now;
        _stopwatch = Stopwatch.StartNew();
        _context = context;
        _memberName = memberName;
        _filePath = filePath;
        _lineNumber = lineNumber;
    }

    public double ElapsedMs => _stopwatch.Elapsed.TotalMilliseconds;

    private bool _cancelled;

    /// <summary>
    /// Marks the timed operation as one a newer request stopped on purpose (#5371): disposing it then writes no SLOW METHOD
    /// block, because a superseded read is not a slow method. It writes one "cancelled after N ms (superseded)" line when the
    /// operation passed the threshold, unless <paramref name="readLogsOwnCancellation"/> says the read already wrote that line.
    /// </summary>
    internal void Cancel(bool readLogsOwnCancellation = false)
    {
        _cancelled = true;
        _readLogsOwnCancellation = readLogsOwnCancellation;
    }

    private bool _readLogsOwnCancellation;

    /// <summary>Whether disposing this context will write a SLOW METHOD block when the operation passed the threshold.</summary>
    internal bool LogsOnDispose => !_cancelled;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _stopwatch.Stop();

        if (_cancelled)
        {
            if (!_readLogsOwnCancellation)
                MethodProfiler.LogCancelledMethod(_stopwatch.Elapsed.TotalMilliseconds, _context, _memberName);

            GC.SuppressFinalize(this);
            return;
        }

        MethodProfiler.LogSlowMethod(
            _startTime, DateTime.Now, _stopwatch.Elapsed.TotalMilliseconds,
            _context, _memberName, _filePath, _lineNumber);

        GC.SuppressFinalize(this);
    }
}
