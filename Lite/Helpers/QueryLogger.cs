/*
 * SQL Server Performance Monitor Lite
 *
 * Query logger for tracking slow DuckDB and SQL Server queries
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Logs slow queries to a dedicated log file for performance analysis.
/// Covers both DuckDB reads and SQL Server collector queries.
/// Usage: using var _ = QueryLogger.StartQuery("context", sql, source: "DuckDB");
/// </summary>
public static class QueryLogger
{
    private static string s_logDirectory = "";
    private static readonly object _lock = new();
    private static volatile bool _isEnabled = true;
    private static double _thresholdSeconds = 0.5;

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
        => Path.Combine(s_logDirectory, $"SlowQueries_{DateTime.Now:yyyyMMdd}.log");

    public static string GetLogDirectory() => s_logDirectory;

    public static void SetEnabled(bool enabled) => _isEnabled = enabled;
    public static bool IsEnabled => _isEnabled;

    public static void SetThreshold(double thresholdSeconds) => _thresholdSeconds = thresholdSeconds;
    public static double ThresholdSeconds => _thresholdSeconds;

    public static void LogSlowQuery(
        DateTime startTime,
        DateTime endTime,
        double elapsedMs,
        string context,
        string queryText,
        string? source = null,
        string? serverName = null)
    {
        if (!_isEnabled || string.IsNullOrEmpty(s_logDirectory))
            return;

        double elapsedSeconds = elapsedMs / 1000.0;
        if (elapsedSeconds < _thresholdSeconds)
            return;

        try
        {
            lock (_lock)
            {
                var sb = new StringBuilder();
                sb.AppendLine("================================================================================");
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "SLOW QUERY DETECTED - {0:F3} seconds", elapsedSeconds));
                sb.AppendLine("================================================================================");
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Start Time:   {0:yyyy-MM-dd HH:mm:ss.fff}", startTime));
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "End Time:     {0:yyyy-MM-dd HH:mm:ss.fff}", endTime));
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Elapsed:      {0:F3} seconds ({1:N0} ms)", elapsedSeconds, elapsedMs));
                sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Context:      {0}", context));

                if (!string.IsNullOrEmpty(source))
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Source:       {0}", source));

                if (!string.IsNullOrEmpty(serverName))
                    sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Server:       {0}", serverName));

                sb.AppendLine("--------------------------------------------------------------------------------");
                sb.AppendLine("Query:");
                sb.AppendLine("--------------------------------------------------------------------------------");
                sb.AppendLine(queryText);
                sb.AppendLine("================================================================================");
                sb.AppendLine();

                File.AppendAllText(GetCurrentLogFile(), sb.ToString());
            }
        }
        catch { }
    }

    /// <summary>
    /// Creates a query execution context for timing queries.
    /// </summary>
    public static QueryExecutionContext StartQuery(
        string context,
        string queryText,
        string? source = null,
        string? serverName = null)
    {
        return new QueryExecutionContext(context, queryText, source, serverName);
    }

    private static void CleanOldLogs()
    {
        try
        {
            var files = Directory.GetFiles(s_logDirectory, "SlowQueries_*.log");
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
/// Disposable context for timing query execution.
/// </summary>
public sealed class QueryExecutionContext : IDisposable
{
    private readonly DateTime _startTime;
    private readonly Stopwatch _stopwatch;
    private readonly string _context;
    private readonly string _queryText;
    private readonly string? _source;
    private readonly string? _serverName;
    private bool _disposed;

    internal QueryExecutionContext(string context, string queryText, string? source, string? serverName)
    {
        _startTime = DateTime.Now;
        _stopwatch = Stopwatch.StartNew();
        _context = context;
        _queryText = queryText;
        _source = source;
        _serverName = serverName;
    }

    public double ElapsedMs => _stopwatch.Elapsed.TotalMilliseconds;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _stopwatch.Stop();

        QueryLogger.LogSlowQuery(
            _startTime, DateTime.Now, _stopwatch.Elapsed.TotalMilliseconds,
            _context, _queryText, _source, _serverName);

        GC.SuppressFinalize(this);
    }
}
