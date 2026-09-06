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
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The service's rolling file log — an <see cref="ILoggerProvider"/> so every existing
/// <c>ILogger</c> call site (connect edges, reload notices, warnings, errors) lands in a greppable
/// file with zero changes at the call sites. This is the service's PRIMARY diagnostic
/// surface: the Windows Event Log provider is also wired, but its event source can only be created
/// by an elevated principal — the recommended <c>NT SERVICE</c> virtual account cannot, so on a
/// by-the-book install the Event Log silently receives nothing until the installer pre-creates the
/// source (see the README's install step). A headless service must not depend on that.
///
/// <para>Disciplines ported from the viewer's <c>ViewerLogger</c> (itself a port of Lite's
/// AppLogger): buffered writes on a 5-second flush timer (a log line never blocks a collection
/// sweep on disk I/O), one file per day, and never-throw — a full disk or an ACL surprise turns
/// logging into a no-op, never a service crash. Files older than <see cref="RetentionDays"/> are
/// swept at startup AND on the worker's daily maintenance tick (see the static
/// <see cref="SweepOldFiles(string)"/>) so an unattended service never grows an unbounded log
/// directory — startup alone left a months-uptime service sweeping exactly once (#1652).</para>
///
/// <para>Directory: <c>%ProgramData%\PerformanceMonitorDarling\logs</c> — the machine-level Darling
/// folder the managed store already lives under (<c>...\pg</c>), created by the service account on
/// first use exactly like the data directory. File: <c>darling-service_yyyyMMdd.log</c>.</para>
///
/// <para><b>This provider is not the level gate, and reading it as one inverts the conclusion.</b>
/// <c>FileLogger.IsEnabled</c> accepts every level except <see cref="LogLevel.None"/>, which reads like a
/// provider that writes <c>Debug</c> unconditionally and therefore like a call site's level being
/// decorative. The gate is upstream: <c>AddLogging</c> registers a default
/// <c>LoggerFilterOptions.MinLevel</c> of <see cref="LogLevel.Information"/>, so a <c>Debug</c> call is
/// dropped by the factory and never reaches any provider — the permissive test here is what makes
/// <c>Debug</c> land in the FILE once configuration raises that minimum, rather than only on the console.
/// Both halves are asserted in <c>PerCycleTimingLogLevelTests</c>; #3102 is the level decision that
/// depends on them.</para>
/// </summary>
public sealed class DarlingFileLoggerProvider : ILoggerProvider
{
    internal const int RetentionDays = 14;

    private readonly ConcurrentQueue<string> _buffer = new();
    private readonly Timer _flushTimer;
    private readonly object _flushLock = new();
    private readonly string _logDirectory;
    private readonly bool _enabled;

    /// <summary>
    /// The always-available fallback channel used to surface a file-logging FAILURE (#1581) — the log directory
    /// could not be created/enabled at construction, or a flush write threw. Defaults to a best-effort single
    /// Windows Event Log Warning (<see cref="DefaultReportFailure"/>) under the source Program.cs registers;
    /// injectable so a unit test can drive the once-only latch without a real Event Log.
    /// </summary>
    private readonly Action<string> _reportFailure;

    /// <summary>
    /// Interlocked once-only latch (0 = not yet surfaced, 1 = surfaced): a persistently-broken log — a flush that
    /// throws on every 5s tick — emits a SINGLE fallback event for the provider's lifetime, not one per flush.
    /// Shared by the constructor and flush failure paths: whichever fails first reports, and no later failure
    /// re-reports (the field box's unwritable log dir went silent precisely because nothing was ever surfaced).
    /// </summary>
    private int _failureReported;

    public DarlingFileLoggerProvider()
        : this(DefaultLogDirectory())
    {
    }

    /// <summary>Test seam: point the provider at any directory.</summary>
    internal DarlingFileLoggerProvider(string logDirectory)
        : this(logDirectory, DefaultReportFailure)
    {
    }

    /// <summary>
    /// Full test seam: also inject the failure-report sink (the caller latches it to at most one call) so a unit
    /// test can drive the once-only fallback without a real Event Log. Production uses the parameterless / single
    /// -directory constructor, whose sink is <see cref="DefaultReportFailure"/>.
    /// </summary>
    internal DarlingFileLoggerProvider(string logDirectory, Action<string> reportFailure)
    {
        _logDirectory = logDirectory;
        _reportFailure = reportFailure;
        try
        {
            Directory.CreateDirectory(_logDirectory);
            SweepOldFiles();
            _enabled = true;
        }
        catch (Exception ex)
        {
            /* A log directory we cannot create must never take down the service — but it must not go SILENT
               either (#1581): surface it ONCE so an operator learns file logging is disabled instead of
               discovering it only when they tail an empty log directory. */
            _enabled = false;
            ReportFailureOnce(
                $"File logging is disabled - cannot create log directory '{_logDirectory}': {ex.GetType().Name}: {ex.Message}");
        }

        _flushTimer = new Timer(_ => Flush(), null, TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
    }

    /// <summary>%ProgramData%\PerformanceMonitorDarling\logs — the service's machine-level log directory.</summary>
    public static string DefaultLogDirectory() => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
        "PerformanceMonitorDarling",
        "logs");

    /// <summary>Today's log file path (darling-service_yyyyMMdd.log under the log directory).</summary>
    public string CurrentLogFile() =>
        Path.Combine(_logDirectory, $"darling-service_{DateTime.Now:yyyyMMdd}.log");

    public ILogger CreateLogger(string categoryName) => new FileLogger(this, categoryName);

    public void Dispose()
    {
        _flushTimer.Change(Timeout.Infinite, Timeout.Infinite);
        Flush();
        _flushTimer.Dispose();
    }

    private void Enqueue(string line)
    {
        if (_enabled)
        {
            _buffer.Enqueue(line);
        }
    }

    /// <summary>Internal (not private) so a unit test can drive a flush deterministically instead of waiting on
    /// the 5-second timer — the once-only failure latch (#1581) is asserted by flushing a broken directory
    /// repeatedly and checking the fallback fired exactly once.</summary>
    internal void Flush()
    {
        if (!_enabled || _buffer.IsEmpty)
        {
            return;
        }

        lock (_flushLock)
        {
            try
            {
                var sb = new StringBuilder();
                while (_buffer.TryDequeue(out var line))
                {
                    sb.AppendLine(line);
                }

                if (sb.Length > 0)
                {
                    File.AppendAllText(CurrentLogFile(), sb.ToString());
                }
            }
            catch (Exception ex)
            {
                /* Never let a logging failure crash the service — but surface it ONCE (#1581) so a log
                   directory that turned unwritable mid-run (the field box's ACL artifact) does not silently
                   swallow every flush until someone notices the log stopped growing. The dequeued lines are
                   already gone; the fallback event is the only remaining signal. */
                ReportFailureOnce(
                    $"File logging is disabled - cannot write '{CurrentLogFile()}': {ex.GetType().Name}: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Surface a file-logging failure through <see cref="_reportFailure"/> exactly ONCE for the provider's
    /// lifetime (#1581). The Interlocked latch means a persistently-broken log — a flush that throws on every
    /// 5s tick — emits a SINGLE event, not one per flush. Best-effort and never-throw: the sink is wrapped so a
    /// failure to report (a missing Event Log source, say) can never re-enter and crash the very logging path it
    /// is reporting on.
    /// </summary>
    private void ReportFailureOnce(string message)
    {
        if (Interlocked.Exchange(ref _failureReported, 1) != 0)
        {
            return;
        }

        try
        {
            _reportFailure(message);
        }
        catch
        {
            /* Logging-about-logging must never crash the service — a sink that throws is swallowed. */
        }
    }

    /// <summary>
    /// The default failure sink: a best-effort single Windows Event Log Warning under the source Program.cs
    /// registers ("PerformanceMonitor Darling"). Windows-guarded and fully swallowed — a missing/unregistered
    /// source (a by-the-book <c>NT SERVICE</c> install cannot create it) or a non-Windows host must never let
    /// surfacing a file-logging failure itself throw. The <see cref="ReportFailureOnce"/> caller owns the latch.
    /// </summary>
    private static void DefaultReportFailure(string message)
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        try
        {
            System.Diagnostics.EventLog.WriteEntry(
                "PerformanceMonitor Darling", message, System.Diagnostics.EventLogEntryType.Warning);
        }
        catch
        {
            /* Best-effort: an unregistered source or an Event Log write failure must never crash the service
               just because file logging already failed. */
        }
    }

    private void SweepOldFiles() => SweepOldFiles(_logDirectory);

    /// <summary>
    /// Deletes <c>darling-service_*.log</c> files older than <see cref="RetentionDays"/> from
    /// <paramref name="logDirectory"/>. Static and directory-parameterized so the DAILY MAINTENANCE TICK can
    /// run it (DarlingWorker, alongside the retention purge) without holding a reference to the provider the
    /// host owns — the sweep used to run ONLY from the constructor (#1652), so a service that stayed up for
    /// months (which is the whole point of a service) swept exactly once, at the start, and then never again
    /// while it wrote a file a day forever. Production always uses <see cref="DefaultLogDirectory"/> — the
    /// directory-taking constructor is a test seam — so the tick sweeps precisely the directory the provider
    /// writes to. Today's file is never in range: the cutoff is 14 days of last-write time.
    /// Best-effort and never-throws, on both call paths.
    /// </summary>
    internal static void SweepOldFiles(string logDirectory)
    {
        try
        {
            var cutoff = DateTime.Now.AddDays(-RetentionDays);
            foreach (var file in Directory.EnumerateFiles(logDirectory, "darling-service_*.log")
                .Where(f => File.GetLastWriteTime(f) < cutoff))
            {
                File.Delete(file);
            }
        }
        catch
        {
            /* Retention is best-effort; a locked or undeletable file is tomorrow's problem. */
        }
    }

    private sealed class FileLogger : ILogger
    {
        private readonly DarlingFileLoggerProvider _provider;
        private readonly string _category;

        public FileLogger(DarlingFileLoggerProvider provider, string category)
        {
            _provider = provider;
            /* The short category name — "DarlingWorker", not the full namespace — keeps lines scannable. */
            var dot = category.LastIndexOf('.');
            _category = dot >= 0 && dot < category.Length - 1 ? category[(dot + 1)..] : category;
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            try
            {
                var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [{Abbreviate(logLevel)}] [{_category}] {formatter(state, exception)}";
                if (exception is not null)
                {
                    line += $" | {exception.GetType().Name}: {exception.Message}";
                }

                _provider.Enqueue(line);
            }
            catch
            {
                /* A formatter that throws must never take down the caller. */
            }
        }

        private static string Abbreviate(LogLevel level) => level switch
        {
            LogLevel.Trace => "TRACE",
            LogLevel.Debug => "DEBUG",
            LogLevel.Information => "INFO ",
            LogLevel.Warning => "WARN ",
            LogLevel.Error => "ERROR",
            LogLevel.Critical => "CRIT ",
            _ => "?????",
        };
    }
}
