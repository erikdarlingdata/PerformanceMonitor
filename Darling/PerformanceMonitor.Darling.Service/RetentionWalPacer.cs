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
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Paces the retention purge's WAL (#4823). Inert until the next commit: every member exists so the tests can
/// pin the behavior, and none of them does the work yet.
/// </summary>
internal sealed class RetentionWalPacer
{
    internal const long MinRateBytesPerSecond = 1_048_576;
    internal const long MaxRateBytesPerSecond = 67_108_864;
    internal const long FallbackRateBytesPerSecond = 4_194_304;
    internal const double BurstSeconds = 10;
    internal const double MaxWaitSeconds = 30;
    internal const double BatchTargetSeconds = 30;

    internal RetentionWalPacer(
        long rateBytesPerSecond,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<double>? secondsClock = null,
        ILogger? logger = null)
    {
        RateBytesPerSecond = rateBytesPerSecond;
    }

    internal long RateBytesPerSecond { get; }

    internal long BurstBytes => 0;

    internal long BatchWalTargetBytes => 0;

    internal long TotalWalBytes => 0;

    internal double TotalWaitSeconds => 0;

    internal bool IsUnpaced => false;

    internal static long RawRateFromSettings(long maxWalSizeMb, double checkpointCompletionTarget, long checkpointTimeoutSeconds) => 0;

    internal static long RateFromSettings(long maxWalSizeMb, double checkpointCompletionTarget, long checkpointTimeoutSeconds) => 0;

    internal static RetentionWalPacer FromSettingRows(
        IReadOnlyList<(string Name, string Setting, string? Unit)>? rows,
        ILogger? logger,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<double>? secondsClock = null) => new(0, delay, secondsClock, logger);

    internal static Task<RetentionWalPacer> CreateAsync(
        NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken) =>
        Task.FromResult(new RetentionWalPacer(0, null, null, logger));

    internal Task<long?> ReadWalPositionAsync(NpgsqlConnection connection, CancellationToken cancellationToken) =>
        Task.FromResult<long?>(null);

    internal Task AfterBatchAsync(long walBytes, CancellationToken cancellationToken) => Task.CompletedTask;
}
