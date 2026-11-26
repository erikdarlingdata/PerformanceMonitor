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
using Npgsql;
using PerformanceMonitor.Darling.Service;

namespace Darling.Tests;

/// <summary>
/// The drain <c>DarlingRetention.PurgeOneAsync</c> runs for a time-sliced purge, for a live test that executes one purge on its own
/// connection: one slice per execution, each slice's end handed to the next as its probe's lower bound (#9796), ended by a slice that
/// deletes nothing.
/// </summary>
internal static class TimeSlicedPurgeDrain
{
    /// <summary>Drains <paramref name="purge"/> and returns the rows deleted.</summary>
    /// <param name="secondParameter">The purge's <c>$2</c> when its extra predicate takes one (the procedure totals' raw floor).</param>
    internal static async Task<int> DrainAsync(
        NpgsqlConnection connection,
        DarlingRetention.PurgeSql purge,
        DateTime cutoff,
        CancellationToken cancellationToken,
        NpgsqlParameter? secondParameter = null)
    {
        object? lowerBound = null;

        return await DarlingRetention.DrainBatchesAsync(
            async token =>
            {
                var (rows, sliceEnd) = await DarlingRetention.ExecuteTimeSliceAsync(connection, purge, cutoff, secondParameter, lowerBound, token);
                lowerBound = sliceEnd ?? lowerBound;
                return (rows, 1);
            },
            cancellationToken);
    }
}
