/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Writes the failure of a per-server step that runs on every tick, carrying the exception once per streak.
///
/// <para><see cref="AppLogger.Error(string, string, Exception?)"/> given an exception writes the stack and every
/// inner exception after the line, twenty to forty lines for a DuckDB fault, into a log kept for
/// <see cref="AppLogger.RetentionDays"/> days with no size cap. A fault that persists (a damaged table, a schema
/// mismatch) makes the alert sweep and the analysis pass throw for every server on every tick, and every tick's stack
/// is the same one. So the first failure of a streak carries the exception, each later failure writes the message
/// only, and every <see cref="CountEvery"/>th says how long the streak is. A success ends the streak, and the next
/// failure carries the exception again.</para>
///
/// <para>The same shape as the collector's throttle on its own logging failures
/// (<c>RemoteCollectorService.LogCollectionAsync</c>), kept per server, and with the exception on the first failure
/// only.</para>
/// </summary>
internal sealed class FailureStreakLog
{
    /// <summary>
    /// How often a continuing streak says its length: every this-many consecutive failures. The collector's reminder
    /// for its own logging failures uses the same interval.
    /// </summary>
    internal const int CountEvery = 100;

    private readonly Dictionary<int, int> _streaks = new();
    private readonly object _lock = new();

    /// <summary>
    /// Records one failure for the server and writes it: with <paramref name="ex"/> on the first failure of a streak,
    /// the message alone after that.
    /// </summary>
    public void Failed(string source, int serverId, string message, Exception ex)
    {
        int count;
        lock (_lock)
        {
            _streaks.TryGetValue(serverId, out count);
            count++;
            _streaks[serverId] = count;
        }

        if (count == 1)
        {
            AppLogger.Error(source, message, ex);
        }
        else if (count % CountEvery == 0)
        {
            AppLogger.Error(source, $"{message} ({count} consecutive failures)");
        }
        else
        {
            AppLogger.Error(source, message);
        }
    }

    /// <summary>Records a success for the server, ending its streak, so its next failure carries the exception again.</summary>
    public void Succeeded(int serverId)
    {
        lock (_lock)
        {
            _streaks.Remove(serverId);
        }
    }
}
