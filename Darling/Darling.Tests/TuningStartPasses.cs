/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// A start builds at most one of the large start-path indexes, so a store with several of them missing needs
/// several starts to be fully tuned. A test that needs a fully tuned store runs the start pass until it stops building.
/// </summary>
internal static class TuningStartPasses
{
    /// <summary>Runs the start-path pass once per large start-path index, which is enough to build every missing one.</summary>
    internal static async Task ConvergeAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        for (var pass = 0; pass < 4; pass++)
        {
            await PgTableTuning.ApplyAsync(connection, NullLogger.Instance, ct);
        }
    }
}
