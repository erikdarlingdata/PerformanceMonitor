/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// The large start-path indexes share one build time budget per start (#9795), so a store with several of them missing can
/// need more than one start to be fully tuned. A test that needs a fully tuned store runs the start pass several times.
/// </summary>
internal static class TuningStartPasses
{
    /// <summary>Runs the start-path pass four times, which builds every missing index on a test store.</summary>
    internal static async Task ConvergeAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        for (var pass = 0; pass < 4; pass++)
        {
            await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);
        }
    }

    /// <summary>
    /// A clock on which every build that shares the start's build time takes <paramref name="secondsPerBuild"/> seconds: its monotonic
    /// timestamp moves that far each time it is read, and the pass reads it once when a build starts and once when it ends. The
    /// time is kept in milliseconds, so a build can take a fraction of a second (#9827). The wall clock is the system's.
    /// </summary>
    internal sealed class BuildClock(double secondsPerBuild) : TimeProvider
    {
        private long _milliseconds;

        public override long TimestampFrequency => 1000;

        public override long GetTimestamp() => _milliseconds += (long)Math.Round(secondsPerBuild * 1000);
    }
}
