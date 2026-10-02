/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's minimal file-based application logger — a faithful port of Lite's front-end
/// <c>AppLogger</c> (buffered, thread-safe, daily-rotated), added so the sidebar's "View Log" /
/// "Open Log Folder" buttons have a real target. Before this the viewer wrote no application log of
/// its own (diagnostics went to <see cref="System.Diagnostics.Debug"/> output), which is invisible to
/// an operator collecting logs for a bug report. Writes one file per day under the viewer's per-user
/// directory (%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_yyyyMMdd.log) — the same
/// Darling-branded Roaming folder <see cref="ViewerPreferencesStore"/> and
/// <see cref="ViewerAppSettingsStore"/> already use — so all viewer state sits in one place.
/// </summary>
public static class ViewerLogger
{
    private static readonly ConcurrentQueue<string> s_buffer = new();
    private static readonly Timer s_flushTimer;
    private static string s_logDirectory = "";
    private static bool s_initialized;
    private static readonly object s_flushLock = new();

    static ViewerLogger()
    {
        s_flushTimer = new Timer(_ => Flush(), null, Timeout.Infinite, Timeout.Infinite);
    }

    /// <summary>%APPDATA%\PerformanceMonitorDarling\logs — the viewer's per-user log directory.</summary>
    public static string DefaultLogDirectory() => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "PerformanceMonitorDarling",
        "logs");

    /// <summary>
    /// Points the logger at a directory (default <see cref="DefaultLogDirectory"/>) and starts the 5-second
    /// flush timer. Safe to call once on startup; a failure to create the directory leaves the logger a
    /// silent no-op rather than crashing the app.
    /// </summary>
    public static void Initialize(string? logDirectory = null)
    {
        try
        {
            s_logDirectory = logDirectory ?? DefaultLogDirectory();
            if (!Directory.Exists(s_logDirectory))
            {
                Directory.CreateDirectory(s_logDirectory);
            }
            s_initialized = true;
            s_flushTimer.Change(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
            Info("ViewerLogger", "Logging initialized");
        }
        catch
        {
            /* A logging directory we cannot create must never take down the viewer. */
            s_initialized = false;
        }
    }

    public static void Info(string source, string message) => Log("INFO", source, message);

    public static void Warn(string source, string message) => Log("WARN", source, message);

    public static void Error(string source, string message, Exception? ex = null)
    {
        if (ex is not null)
        {
            Log("ERROR", source, $"{message} | {ex.GetType().Name}: {ex.Message}");
            Log("ERROR", source, $"Stack: {ex.StackTrace}");

            var inner = ex.InnerException;
            var depth = 1;
            while (inner is not null)
            {
                Log("ERROR", source, $"Inner[{depth}]: {inner.GetType().Name}: {inner.Message}");
                inner = inner.InnerException;
                depth++;
            }
        }
        else
        {
            Log("ERROR", source, message);
        }
    }

    private static void Log(string level, string source, string message)
    {
        if (!s_initialized)
        {
            return;
        }

        s_buffer.Enqueue($"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level,-5}] [{source}] {message}");
    }

    /// <summary>Drains the buffer to today's log file. Serialized so the timer and shutdown never collide.</summary>
    public static void Flush()
    {
        if (!s_initialized || s_buffer.IsEmpty)
        {
            return;
        }

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
                    /* #4281 review: AppendAllText's strict UTF-8 encoder throws on a lone surrogate, dropping the whole batch. */
                    File.AppendAllText(GetCurrentLogFile(), sb.ToString(), new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
                }
            }
            catch
            {
                /* Never let a logging failure crash the app. */
            }
        }
    }

    public static void Shutdown()
    {
        s_flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
        Flush();
        s_flushTimer.Dispose();
    }

    /// <summary>The resolved log directory (empty until <see cref="Initialize"/> succeeds).</summary>
    public static string GetLogDirectory() => s_logDirectory;

    /// <summary>Today's log file path (darling-viewer_yyyyMMdd.log under the log directory).</summary>
    public static string GetCurrentLogFile() =>
        Path.Combine(s_logDirectory, $"darling-viewer_{DateTime.Now:yyyyMMdd}.log");
}
