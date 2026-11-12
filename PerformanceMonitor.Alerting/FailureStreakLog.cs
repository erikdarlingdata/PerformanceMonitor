/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// Tracks the failure streaks of a step that runs on every tick for every server, so the exception is written once per
/// streak rather than on every repeat.
///
/// <para>A logger given the exception writes its type and message and, in the sinks that render it in full (Lite's
/// AppLogger, the service's console), the stack and every inner exception: twenty to forty lines for a store fault. A
/// fault that persists (a damaged table, a schema mismatch) makes the alert engine's per-check reads, the alert history
/// store's reads and writes, Lite's alert sweep and its analysis pass fail for every server on every tick, and every
/// tick's stack is the same one. So the first failure of a streak carries the exception, each later failure writes the
/// message only, and every <see cref="FailureStreakLine.CountEvery"/>th says how long the streak is. A success ends the
/// streak, and the next failure carries the exception again.</para>
///
/// <para>The key names the step and what it ran for: a server id for a per-server step, a (server, check) pair for one
/// check inside a sweep. The same shape as the collector's throttle on its own logging failures (Lite's
/// <c>RemoteCollectorService.LogCollectionAsync</c>), with the exception on the first failure only. <see cref="Failed"/>
/// only decides what the line carries; the caller writes it through <see cref="FailureStreakLogging.LogFailure"/> (an
/// <see cref="ILogger"/>) or Lite's AppLogger extension, so one streak rule serves both sinks.</para>
/// </summary>
public sealed class FailureStreakLog<TKey> where TKey : notnull
{
    private readonly Dictionary<TKey, int> _streaks = new();
    private readonly object _lock = new();

    /// <summary>
    /// Records one failure for the key and says what its line carries: the exception on the first failure of a streak,
    /// the streak's length on every <see cref="FailureStreakLine.CountEvery"/>th, the message alone otherwise.
    /// </summary>
    public FailureStreakLine Failed(TKey key)
    {
        lock (_lock)
        {
            _streaks.TryGetValue(key, out var count);
            count++;
            _streaks[key] = count;
            return new FailureStreakLine(count);
        }
    }

    /// <summary>Records a success for the key, ending its streak, so its next failure carries the exception again.</summary>
    public void Succeeded(TKey key)
    {
        lock (_lock)
        {
            _streaks.Remove(key);
        }
    }
}

/// <summary>
/// What one failure's log line carries, decided by its place in the streak (<see cref="FailureStreakLog{TKey}.Failed"/>).
/// </summary>
public readonly record struct FailureStreakLine(int Count)
{
    /// <summary>
    /// How often a continuing streak says its length: every this-many consecutive failures. The collector's reminder for
    /// its own logging failures uses the same interval.
    /// </summary>
    public const int CountEvery = 100;

    /// <summary>True on the first failure of a streak: the line carries the exception, so the stack is written once per streak.</summary>
    public bool CarriesException => Count == 1;

    /// <summary>The streak's length, appended to the message on every <see cref="CountEvery"/>th failure, and empty otherwise.</summary>
    public string CountSuffix => Count % CountEvery == 0 ? $" ({Count} consecutive failures)" : string.Empty;
}

/// <summary>Writes a streak-tracked failure through an <see cref="ILogger"/>.</summary>
public static class FailureStreakLogging
{
    /// <summary>
    /// Writes the failure at Error as its <paramref name="line"/> says: with <paramref name="ex"/> on the first failure of
    /// a streak, the message alone after that, and the streak's length appended on every
    /// <see cref="FailureStreakLine.CountEvery"/>th. The message template and its arguments are the caller's, unchanged.
    /// </summary>
    public static void LogFailure(this ILogger? logger, FailureStreakLine line, Exception ex, string message, params object?[] args)
    {
        logger?.LogError(line.CarriesException ? ex : null, message + line.CountSuffix, args);
    }
}
