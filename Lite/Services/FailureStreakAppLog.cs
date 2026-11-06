/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Writes a streak-tracked failure through <see cref="AppLogger"/>: the Lite sink's side of
/// <see cref="FailureStreakLog{TKey}"/>, whose <see cref="FailureStreakLogging.LogFailure"/> is the ILogger side.
/// </summary>
internal static class FailureStreakAppLog
{
    /// <summary>
    /// Records one failure for the key and writes it at Error as its place in the streak says: with <paramref name="ex"/>
    /// on the first failure of a streak, the message alone after that, and the streak's length appended on every
    /// <see cref="FailureStreakLine.CountEvery"/>th. <see cref="AppLogger.Error(string, string, Exception?)"/> given the
    /// exception writes the stack and every inner exception after the line, so those lines are written once per streak.
    /// </summary>
    public static void Failed<TKey>(this FailureStreakLog<TKey> streaks, string source, TKey key, string message, Exception ex)
        where TKey : notnull
    {
        var line = streaks.Failed(key);
        AppLogger.Error(source, message + line.CountSuffix, line.CarriesException ? ex : null);
    }
}
