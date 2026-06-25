/*
 * SQL Server Performance Monitor Dashboard
 *
 * Simple file-based logger for application diagnostics
 */

using System;
using System.Globalization;
using System.IO;
using System.Text;

namespace PerformanceMonitorDashboard.Helpers
{
    public static class Logger
    {
        private static readonly string LogDirectory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "PerformanceMonitor",
            "Logs"
        );

        private static readonly object _lock = new object();

        /// <summary>
        /// Gets the current log file path. Computed dynamically so logs roll over at midnight.
        /// </summary>
        private static string GetLogFilePath()
        {
            return Path.Combine(LogDirectory, $"PerformanceMonitor_{DateTime.Now:yyyyMMdd}.log");
        }

        static Logger()
        {
            try
            {
                if (!Directory.Exists(LogDirectory))
                {
                    Directory.CreateDirectory(LogDirectory);
                }

                // Clean up old logs (keep last 7 days)
                CleanOldLogs();
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Logger: Failed to create directory: {ex.Message}");
            }
        }

        public static void Info(string message)
        {
            WriteLog("INFO", message);
        }

        public static void Warning(string message)
        {
            WriteLog("WARN", message);
        }

        public static void Error(string message, Exception? ex = null)
        {
            var fullMessage = ex != null
                ? $"{message}\nException: {ex.GetType().Name}\nMessage: {ex.Message}\nStackTrace: {ex.StackTrace}"
                : message;

            WriteLog("ERROR", fullMessage);
        }

        public static void Fatal(string message, Exception ex)
        {
            var fullMessage = $"{message}\nException: {ex.GetType().Name}\nMessage: {ex.Message}\nStackTrace: {ex.StackTrace}\nInnerException: {ex.InnerException?.Message}";
            WriteLog("FATAL", fullMessage);
        }

        private static void WriteLog(string level, string message)
        {
            try
            {
                lock (_lock)
                {
                    var logEntry = new StringBuilder();
                    logEntry.AppendLine(CultureInfo.InvariantCulture, $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] [{level}] {message}");

                    File.AppendAllText(GetLogFilePath(), logEntry.ToString());
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Logger: Failed to write log: {ex.Message}");
            }
        }

        private static void CleanOldLogs()
        {
            try
            {
                var files = Directory.GetFiles(LogDirectory, "PerformanceMonitor_*.log");
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
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"Logger: Failed to clean old logs: {ex.Message}");
            }
        }

        public static string GetLogDirectory()
        {
            return LogDirectory;
        }

        public static string GetCurrentLogFile()
        {
            return GetLogFilePath();
        }
    }
}
