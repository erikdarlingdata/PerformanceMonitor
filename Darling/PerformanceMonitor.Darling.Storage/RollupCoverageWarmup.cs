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
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Compile-only placeholder for the tests-first commit (#4957): the warm entry point the live and source-pin tests
/// call, doing nothing yet, so those tests build and fail for the right reason. The next commit replaces it.
/// </summary>
public static class RollupCoverageWarmup
{
    /// <summary>How long the service waits after start before warming the rollup floors.</summary>
    public static readonly TimeSpan ServiceStartDelay = TimeSpan.FromSeconds(30);

    /// <summary>How long the Viewer waits after opening its store before warming the rollup floors.</summary>
    public static readonly TimeSpan ViewerStartDelay = TimeSpan.FromSeconds(3);

    /// <summary>Placeholder: warms nothing.</summary>
    public static Task RunDelayedAsync(
        NpgsqlDataSource postgres, ILogger? logger, TimeSpan delay, CancellationToken cancellationToken)
        => Task.CompletedTask;
}
