/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>Compile-only stand-in; the next commit gives it its body.</summary>
internal static class PurgeNowWatch
{
    internal static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(15);

    internal static readonly TimeSpan RawRecordWait = TimeSpan.FromMinutes(5);

    internal static readonly TimeSpan GiveUpAfter = TimeSpan.FromHours(2);

    internal const string StillRunningText = "";

    internal static bool TryReadStartedAtUtc(string? resultJson, out DateTime startedAtUtc)
    {
        startedAtUtc = default;
        return false;
    }

    internal static string RunningText(TimeSpan elapsed) => "";

    internal static Task WatchAsync(
        DateTime startedAtUtc,
        Func<DateTime, CancellationToken, Task<List<ManualPurgeRunRecord>>> readRecords,
        Func<TimeSpan, CancellationToken, Task> delay,
        Func<DateTime> utcNow,
        Action<string> show,
        Func<Task> reload,
        CancellationToken cancellationToken)
        => throw new NotImplementedException();
}
