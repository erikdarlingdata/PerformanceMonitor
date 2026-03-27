/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Simple file-based application logger. Writes to logs/ directory with daily rotation.
/// Thread-safe with buffered writes.
/// </summary>
public static class AppLogger
{
    private static readonly ConcurrentQueue<string> s_buffer = new();
    private static readonly Timer s_flushTimer;
    private static string s_logDirectory = "";
    private static bool s_initialized;

    static AppLogger()
    {
        s_flushTimer = new Timer(_ => Flush(), null, Timeout.Infinite, Timeout.Infinite);
    }

    public static void Initialize(string logDirectory)
    {
        s_logDirectory = logDirectory;
        if (!Directory.Exists(s_logDirectory))
        {
            Directory.CreateDirectory(s_logDirectory);
        }
        s_initialized = true;
        s_flushTimer.Change(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

        Info("AppLogger", "Logging initialized");
    }

    public static void Info(string source, string message)
    {
        Log("INFO", source, message);
    }

    public static void Warn(string source, string message)
    {
        Log("WARN", source, message);
    }

    public static void Error(string source, string message, Exception? ex = null)
    {
        if (ex != null)
        {
            Log("ERROR", source, $"{message} | {ex.GetType().Name}: {ex.Message}");
            Log("ERROR", source, $"Stack: {ex.StackTrace}");

            /* Log all inner exceptions recursively */
            var inner = ex.InnerException;
            var depth = 1;
            while (inner != null)
            {
                Log("ERROR", source, $"Inner[{depth}]: {inner.GetType().Name}: {inner.Message}");
                Log("ERROR", source, $"Inner[{depth}] Stack: {inner.StackTrace}");
                inner = inner.InnerException;
                depth++;
            }

            /* For AggregateException, log all inner exceptions */
            if (ex is AggregateException aggEx)
            {
                var idx = 0;
                foreach (var innerEx in aggEx.InnerExceptions)
                {
                    Log("ERROR", source, $"Aggregate[{idx}]: {innerEx.GetType().Name}: {innerEx.Message}");
                    Log("ERROR", source, $"Aggregate[{idx}] Stack: {innerEx.StackTrace}");
                    idx++;
                }
            }
        }
        else
        {
            Log("ERROR", source, message);
        }
    }

    public static void Debug(string source, string message)
    {
#if DEBUG
        Log("DEBUG", source, message);
#endif
    }

    public static void DataDiag(string source, string message)
    {
        Log("DATA", source, message);
    }

    private static void Log(string level, string source, string message)
    {
        var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level,-5}] [{source}] {message}";
        s_buffer.Enqueue(line);
    }

    public static void Flush()
    {
        if (!s_initialized || s_buffer.IsEmpty) return;

        try
        {
            var sb = new StringBuilder();
            while (s_buffer.TryDequeue(out var line))
            {
                sb.AppendLine(line);
            }

            if (sb.Length > 0)
            {
                var logFile = Path.Combine(s_logDirectory, $"lite_{DateTime.Now:yyyyMMdd}.log");
                File.AppendAllText(logFile, sb.ToString());
            }
        }
        catch
        {
            /* Don't let logging failures crash the app */
        }
    }

    public static void Shutdown()
    {
        s_flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
        Flush();
        s_flushTimer.Dispose();
    }

    public static string GetLogDirectory() => s_logDirectory;

    public static string GetCurrentLogFile() =>
        Path.Combine(s_logDirectory, $"lite_{DateTime.Now:yyyyMMdd}.log");
}
