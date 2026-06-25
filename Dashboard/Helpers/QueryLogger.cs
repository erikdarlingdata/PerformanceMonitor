/*
 * SQL Server Performance Monitor Dashboard
 *
 * Query logger for tracking slow database queries from the Dashboard
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;

namespace PerformanceMonitorDashboard.Helpers
{
    /// <summary>
    /// Logs slow database queries to a dedicated log file for performance analysis.
    /// </summary>
    public static class QueryLogger
    {
        private static readonly string LogDirectory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "PerformanceMonitor",
            "Logs"
        );

        private static readonly object _lock = new object();
        private static volatile bool _isEnabled = true;
        private static double _thresholdSeconds = 0.5; // Accessed within lock when set

        static QueryLogger()
        {
            try
            {
                if (!Directory.Exists(LogDirectory))
                {
                    Directory.CreateDirectory(LogDirectory);
                }

                // Clean up old query logs (keep last 7 days)
                CleanOldLogs();
            }
            catch
            {
                // If we can't create log directory, logging will fail silently
            }
        }

        /// <summary>
        /// Gets the current query log file path.
        /// </summary>
        public static string GetCurrentLogFile()
        {
            return Path.Combine(LogDirectory, $"SlowQueries_{DateTime.Now:yyyyMMdd}.log");
        }

        /// <summary>
        /// Gets the log directory path.
        /// </summary>
        public static string GetLogDirectory()
        {
            return LogDirectory;
        }

        /// <summary>
        /// Sets whether query logging is enabled.
        /// </summary>
        public static void SetEnabled(bool enabled)
        {
            _isEnabled = enabled;
        }

        /// <summary>
        /// Gets whether query logging is enabled.
        /// </summary>
        public static bool IsEnabled => _isEnabled;

        /// <summary>
        /// Sets the threshold in seconds for logging slow queries.
        /// </summary>
        public static void SetThreshold(double thresholdSeconds)
        {
            _thresholdSeconds = thresholdSeconds;
        }

        /// <summary>
        /// Logs a slow query if it exceeds the threshold.
        /// </summary>
        /// <param name="startTime">When the query started</param>
        /// <param name="endTime">When the query finished</param>
        /// <param name="elapsedMs">Elapsed time in milliseconds</param>
        /// <param name="context">The tab or context where the query was executed</param>
        /// <param name="queryText">The SQL query text</param>
        /// <param name="serverName">The server name (optional)</param>
        /// <param name="databaseName">The database name (optional)</param>
        public static void LogSlowQuery(
            DateTime startTime,
            DateTime endTime,
            double elapsedMs,
            string context,
            string queryText,
            string? serverName = null,
            string? databaseName = null)
        {
            if (!_isEnabled)
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

                    if (!string.IsNullOrEmpty(serverName))
                        sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Server:       {0}", serverName));

                    if (!string.IsNullOrEmpty(databaseName))
                        sb.AppendLine(string.Format(CultureInfo.InvariantCulture, "Database:     {0}", databaseName));

                    sb.AppendLine("--------------------------------------------------------------------------------");
                    sb.AppendLine("Query:");
                    sb.AppendLine("--------------------------------------------------------------------------------");
                    sb.AppendLine(queryText);
                    sb.AppendLine("================================================================================");
                    sb.AppendLine();

                    File.AppendAllText(GetCurrentLogFile(), sb.ToString());
                }
            }
            catch
            {
                // Logging failed, nothing we can do
            }
        }

        /// <summary>
        /// Creates a query execution context for timing queries.
        /// Use with 'using' statement for automatic logging.
        /// </summary>
        public static QueryExecutionContext StartQuery(string context, string queryText, string? serverName = null, string? databaseName = null)
        {
            return new QueryExecutionContext(context, queryText, serverName, databaseName);
        }

        private static void CleanOldLogs()
        {
            try
            {
                var files = Directory.GetFiles(LogDirectory, "SlowQueries_*.log");
                var cutoffDate = DateTime.Now.AddDays(-7);

                foreach (var file in files)
                {
                    var fileInfo = new FileInfo(file);
                    if (fileInfo.CreationTime < cutoffDate)
                    {
                        fileInfo.Delete();
                    }
                }
            }
            catch
            {
                // Cleanup failed, not critical
            }
        }
    }

    /// <summary>
    /// Disposable context for timing query execution.
    /// </summary>
    public class QueryExecutionContext : IDisposable
    {
        private readonly DateTime _startTime;
        private readonly Stopwatch _stopwatch;
        private readonly string _context;
        private readonly string _queryText;
        private readonly string? _serverName;
        private readonly string? _databaseName;
        private bool _disposed;

        public QueryExecutionContext(string context, string queryText, string? serverName, string? databaseName)
        {
            _startTime = DateTime.Now;
            _stopwatch = Stopwatch.StartNew();
            _context = context;
            _queryText = queryText;
            _serverName = serverName;
            _databaseName = databaseName;
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;
            _stopwatch.Stop();

            var endTime = DateTime.Now;
            QueryLogger.LogSlowQuery(
                _startTime,
                endTime,
                _stopwatch.Elapsed.TotalMilliseconds,
                _context,
                _queryText,
                _serverName,
                _databaseName);

            GC.SuppressFinalize(this);
        }
    }
}
