/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Simple file-based application logger. Writes to logs/ directory with daily rotation.
/// Thread-safe with buffered writes. Files older than <see cref="RetentionDays"/> are swept at
/// <see cref="Initialize"/> so the log directory never grows without bound.
///
/// <para><b>This type owns the whole app's level gate.</b> <see cref="IsEnabled"/> is the one decision
/// point, and <see cref="AppLoggerAdapter{T}"/> defers to it so a component logging through
/// <c>ILogger&lt;T&gt;</c> and a component calling the static methods here get the same answer. Lite has
/// no logger factory — every <c>ILogger&lt;T&gt;</c> in the app is a directly-constructed
/// <see cref="AppLoggerAdapter{T}"/> — so there is no <c>LoggerFilterOptions</c> anywhere in the path and
/// nothing upstream of this to filter on (#3104).
/// </para>
/// </summary>
public static class AppLogger
{
    /// <summary>
    /// The level a line must reach to be written, absent a configured one.
    ///
    /// <para><see cref="LogLevel.Information"/> is the level a default .NET logging factory filters at, and
    /// the level Darling's service is gated at (#3102), so both SKUs answer "does this line appear on a
    /// default install" the same way and a level chosen on one reads the same on the other.</para>
    /// </summary>
    internal const LogLevel DefaultMinimumLevel = LogLevel.Information;

    /// <summary>
    /// The level in force, read on every call so it can be raised at startup from settings.json and
    /// lowered again without a restart.
    ///
    /// <para><b>Runtime rather than conditional compilation, which is the decision this field records.</b>
    /// A <c>#if DEBUG</c> around a write is a gate no operator can reach: the shipped artifact is Release,
    /// so the level that directive selects is the only level an install will ever have, and changing it
    /// needs a rebuild rather than a setting — which makes a level lowered onto such a write a deletion
    /// rather than a suppression. It also splits the app's own tests from the artifact: the same source
    /// writes the line under one configuration and not the other, so what a suite observed depends on how
    /// it was built. One runtime value has one answer in every configuration.</para>
    /// </summary>
    private static volatile LogLevel s_minimumLevel = DefaultMinimumLevel;

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

    /// <summary>The level currently in force. A line below it is not written.</summary>
    public static LogLevel MinimumLevel => s_minimumLevel;

    /// <summary>
    /// Raises or lowers the level. Called from startup once settings.json has been read, and safe to call
    /// at any time — the next call to <see cref="IsEnabled"/> sees it.
    /// </summary>
    public static void SetMinimumLevel(LogLevel level) => s_minimumLevel = level;

    /// <summary>
    /// Whether a line at <paramref name="level"/> would be written. The gate for both this type's static
    /// methods and <see cref="AppLoggerAdapter{T}"/>, so the level an operator sets is the level every
    /// component in the app is held to.
    ///
    /// <para><see cref="LogLevel.None"/> is never enabled. It is the "log nothing" sentinel rather than a
    /// severity, so ordering it against the minimum — where it compares highest of all — would make it the
    /// one level nothing can suppress.</para>
    /// </summary>
    public static bool IsEnabled(LogLevel level) =>
        level != LogLevel.None && level >= s_minimumLevel;

    /// <summary>
    /// A settings.json token as a level. False — with <paramref name="level"/> left at
    /// <see cref="DefaultMinimumLevel"/> — for anything that is not one of the seven NAMES, so a caller
    /// that ignores the result still gets a usable level rather than whatever the token decoded to.
    ///
    /// <para><b>Names only, and both checks below reject something the other accepts.</b>
    /// <c>Enum.TryParse</c> succeeds on a NUMBER, which goes wrong two ways: an undefined one
    /// (<c>"999"</c>) becomes a minimum no real level can reach, so every line including
    /// <see cref="LogLevel.Error"/> is dropped and nothing is reported — a verbosity setting silencing
    /// the log completely is the opposite of what it is for — and a defined one (<c>"3"</c>) is accepted
    /// vocabulary this setting has never documented. The letter check turns both into a reported token.
    /// <c>Enum.IsDefined</c> then holds the invariant independently of how the token was spelled, so a
    /// future spelling the letter check lets through cannot become an out-of-range minimum.</para>
    /// </summary>
    public static bool TryParseMinimumLevel(string? token, out LogLevel level)
    {
        level = DefaultMinimumLevel;

        if (string.IsNullOrWhiteSpace(token))
        {
            return false;
        }

        foreach (var c in token)
        {
            if (!char.IsAsciiLetter(c))
            {
                return false;
            }
        }

        if (!Enum.TryParse(token, ignoreCase: true, out LogLevel parsed) || !Enum.IsDefined(parsed))
        {
            return false;
        }

        level = parsed;
        return true;
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
        if (!IsEnabled(LogLevel.Information)) return;

        Log("INFO", source, message);
    }

    public static void Warn(string source, string message)
    {
        if (!IsEnabled(LogLevel.Warning)) return;

        Log("WARN", source, message);
    }

    public static void Error(string source, string message, Exception? ex = null) =>
        Error(LogLevel.Error, source, message, ex);

    /// <summary>
    /// The error sink, gated on the level the CALLER logged at rather than on
    /// <see cref="LogLevel.Error"/>.
    ///
    /// <para><b>Why the level is a parameter.</b> <see cref="AppLoggerAdapter{T}"/> routes both
    /// <see cref="LogLevel.Error"/> and <see cref="LogLevel.Critical"/> here, so a fixed
    /// <c>IsEnabled(Error)</c> would answer for the wrong level on the Critical half: at a minimum of
    /// <c>Critical</c> the adapter admits a Critical line and this gate then drops it, which makes
    /// <c>Critical</c> silence the log as completely as <c>None</c> — a documented level quietly meaning
    /// something else. The <c>Trace</c>/<c>Debug</c> pair does not have the same problem in the other
    /// direction, because there the sink is the HIGHER of the two: admitting <c>Trace</c> requires a
    /// minimum at or below it, which already admits <c>Debug</c>. Taking the level makes both pairs answer
    /// from one comparison instead of relying on that asymmetry holding.</para>
    /// </summary>
    internal static void Error(LogLevel level, string source, string message, Exception? ex)
    {
        /* Gated once here rather than inside Log, so an exception's stack and its whole inner chain are
           admitted or dropped together — a half-written error is harder to read than none. */
        if (!IsEnabled(level)) return;

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
        if (!IsEnabled(LogLevel.Debug)) return;

        Log("DEBUG", source, message);
    }

    private static void Log(string level, string source, string message)
    {
        var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{level,-5}] [{source}] {message}";
        s_buffer.Enqueue(line);
    }

    /// <summary>
    /// Removes and returns everything buffered but not yet written, so a test can observe whether a call
    /// was ADMITTED by the gate.
    ///
    /// <para>The seam exists because <see cref="Initialize"/> is not usable from a test: it repoints the
    /// whole process's logging at the caller's directory and starts a timer against it, which is the
    /// constraint <c>AppLoggerRetentionTests</c> documents for the same reason. Nothing in the app calls
    /// this — <see cref="Flush"/> owns the buffer in production and drains it to the file.</para>
    /// </summary>
    internal static List<string> DrainBufferedLines()
    {
        var lines = new List<string>();
        while (s_buffer.TryDequeue(out var line))
        {
            lines.Add(line);
        }

        return lines;
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
