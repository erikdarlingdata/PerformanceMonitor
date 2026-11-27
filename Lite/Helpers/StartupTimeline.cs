/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// #9853 - the time each start-up phase takes, written as one log line once the store is open. A first start after Setup took
/// about 25 s on a clean machine against under 2 s later, and the log only showed the two ends of the gap. The first phase is
/// the time from process start to <c>OnStartup</c>, so it covers the runtime and WPF loading before any of our code runs.
/// Each later phase is the time since the mark before it. A mark is one clock read and one list add, so the cost is the
/// same whether the start is fast or slow, and nothing marks after <see cref="Report"/>.
/// </summary>
public sealed class StartupTimeline
{
    private static StartupTimeline? s_current;

    private readonly Func<long> _elapsedMs;
    private readonly List<(string Name, long DurationMs)> _phases = [];
    private readonly object _lock = new();
    private long _lastMarkMs;

    /// <summary>
    /// <paramref name="beforeStartMs"/> is the time from process start to now, and becomes the first phase named
    /// <paramref name="beforeStartPhase"/>. <paramref name="elapsedMs"/> reads a monotonic clock in milliseconds.
    /// </summary>
    public StartupTimeline(long beforeStartMs, string beforeStartPhase, Func<long> elapsedMs)
    {
        _elapsedMs = elapsedMs;
        _phases.Add((beforeStartPhase, Math.Max(0, beforeStartMs)));
        _lastMarkMs = elapsedMs();
    }

    /// <summary>Records the time since the last mark under <paramref name="name"/>.</summary>
    public void Mark(string name)
    {
        lock (_lock)
        {
            var now = _elapsedMs();
            _phases.Add((name, Math.Max(0, now - _lastMarkMs)));
            _lastMarkMs = now;
        }
    }

    /// <summary>
    /// The log line: each phase in the order it was marked with its milliseconds, then the total. The total is the sum of the
    /// phases, so it reads the same as the sum a person would add up from the line.
    /// </summary>
    public string Format()
    {
        lock (_lock)
        {
            var text = new StringBuilder("Startup phases: ");
            long total = 0;
            foreach (var (name, durationMs) in _phases)
            {
                total += durationMs;
                text.Append(name).Append(' ').Append(durationMs.ToString(CultureInfo.InvariantCulture)).Append(" ms, ");
            }

            text.Append("total ").Append(total.ToString(CultureInfo.InvariantCulture)).Append(" ms");
            return text.ToString();
        }
    }

    /// <summary>Starts the timeline for this process. Call once, first thing in <c>OnStartup</c>.</summary>
    public static void Begin(string beforeStartPhase)
    {
        long beforeStartMs = 0;
        try
        {
            using var process = Process.GetCurrentProcess();
            beforeStartMs = (long)(DateTime.Now - process.StartTime).TotalMilliseconds;
        }
        catch (Exception ex) when (ex is InvalidOperationException or System.ComponentModel.Win32Exception or NotSupportedException)
        {
            /* A process whose start time cannot be read still gets every later phase. */
        }

        var watch = Stopwatch.StartNew();
        Volatile.Write(ref s_current, new StartupTimeline(beforeStartMs, beforeStartPhase, () => watch.ElapsedMilliseconds));
    }

    /// <summary>Marks a phase on the process's timeline. Does nothing before <see cref="Begin"/> and after <see cref="Report"/>.</summary>
    public static void MarkPhase(string name) => Volatile.Read(ref s_current)?.Mark(name);

    /// <summary>
    /// Takes the process's timeline, so a second call finds none, and returns its line. Null when none is running, which is
    /// every call after the first one, such as the store being opened again by a reset.
    /// </summary>
    public static string? Report() => Interlocked.Exchange(ref s_current, null)?.Format();
}
