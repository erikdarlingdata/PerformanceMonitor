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
/// Thread-safe with buffered writes. Files older than <see cref="RetentionDays"/> are swept at
/// <see cref="Initialize"/> so the log directory never grows without bound.
/// </summary>
public static class AppLogger
{
    /// <summary>
    /// How long a rotated <c>lite_yyyyMMdd.log</c> is kept. "Daily rotation" only ever meant a NEW
    /// file per day — nothing deleted the old ones, so the directory grew for the life of the install
    /// (#1652). Seven days matches both sibling loggers in this app (<see cref="Helpers.QueryLogger"/>
    /// and <see cref="Helpers.MethodProfiler"/>), which is the window a bug report needs: the user
    /// reproduces, then sends the logs.
    /// </summary>
    internal const int RetentionDays = 7;
    private static readonly ConcurrentQueue<string> s_buffer = new();
    private static readonly Timer s_flushTimer;
    private static string s_logDirectory = "";
    private static bool s_initialized;
    private static readonly object s_flushLock = new();

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
        CleanOldLogs(s_logDirectory);
        s_initialized = true;
        s_flushTimer.Change(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));

        Info("AppLogger", "Logging initialized");
    }

    /// <summary>
    /// Deletes <c>lite_*.log</c> files older than <see cref="RetentionDays"/>. Mirrors
    /// <see cref="Helpers.QueryLogger"/>'s sweep (same file-age predicate, same window) — the two
    /// sibling loggers already pruned and this one did not, so an install that ran for a year kept
    /// every daily log it ever wrote. Takes the directory rather than reading the static field so a
    /// test can exercise it without initializing the process-wide logger. Best-effort and
    /// never-throws: a locked or undeletable file is tomorrow's sweep's problem, and logging
    /// retention must never be the reason startup fails.
    /// </summary>
    internal static void CleanOldLogs(string logDirectory)
    {
        try
        {
            var cutoff = DateTime.Now.AddDays(-RetentionDays);
            foreach (var file in Directory.GetFiles(logDirectory, "lite_*.log"))
            {
                if (new FileInfo(file).CreationTime < cutoff)
                {
                    File.Delete(file);
                }
            }
        }
        catch
        {
            /* Retention is best-effort — never let it break logging or startup */
        }
    }

    public static void Info(string source, string message)
    {
        Log("INFO", source, message);
    }

    public static void Warn(string source, string message)
    {
        Log("WARN", source, message);
    }

    /// <summary>
    /// Warning WITH the exception's full detail (#2320 review catch) — a warning-severity event whose
    /// diagnosis still needs the stack and inner exceptions. Same emission as Error's, at WARN.
    /// </summary>
    public static void Warn(string source, string message, Exception ex)
    {
        LogWithException("WARN", source, message, ex);
    }

    public static void Error(string source, string message, Exception? ex = null)
    {
        if (ex != null)
        {
            LogWithException("ERROR", source, message, ex);
        }
        else
        {
            Log("ERROR", source, message);
        }
    }

    private static void LogWithException(string level, string source, string message, Exception ex)
    {
        Log(level, source, $"{message} | {ex.GetType().Name}: {ex.Message}");
        Log(level, source, $"Stack: {ex.StackTrace}");

        /* Log all inner exceptions recursively */
        var inner = ex.InnerException;
        var depth = 1;
        while (inner != null)
        {
            Log(level, source, $"Inner[{depth}]: {inner.GetType().Name}: {inner.Message}");
            Log(level, source, $"Inner[{depth}] Stack: {inner.StackTrace}");
            inner = inner.InnerException;
            depth++;
        }

        /* For AggregateException, log all inner exceptions */
        if (ex is AggregateException aggEx)
        {
            var idx = 0;
            foreach (var innerEx in aggEx.InnerExceptions)
            {
                Log(level, source, $"Aggregate[{idx}]: {innerEx.GetType().Name}: {innerEx.Message}");
                Log(level, source, $"Aggregate[{idx}] Stack: {innerEx.StackTrace}");
                idx++;
            }
        }
    }

    public static void Debug(string source, string message)
    {
#if DEBUG
        Log("DEBUG", source, message);
#endif
    }

    private static void Log(string level, string source, string message)
    {
        var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level,-5}] [{source}] {message}";
        s_buffer.Enqueue(line);
    }

    public static void Flush()
    {
        if (!s_initialized || s_buffer.IsEmpty) return;

        /* Serialize flushes: the 5s timer and Shutdown() can call Flush concurrently, and two
           File.AppendAllText calls to the same file throw — which the catch below would swallow,
           silently dropping the lines already dequeued from the buffer. */
        lock (s_flushLock)
        {
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
