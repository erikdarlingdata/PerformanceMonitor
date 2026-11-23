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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #9374: keeps <c>collect.store_relation_sizes_latest</c> (<see cref="StoreRelationSizesStore"/>) in step with the store, so the Viewer's
/// Store Health tab opens on the last measured sizes. The worker's own <see cref="DarlingStoreTableHealthCache"/> takes the pass (the cache stays the one place a
/// pass happens) and its <see cref="DarlingStoreTableHealthCache.OnSizesMeasured"/> writes it. This loop only asks the cache for a pass when it holds
/// none newer than the cadence (<see cref="DarlingConfig.StoreHealthSizesIntervalMinutes"/>): once soon after the store is ready, then once each cadence.
/// A pass any other read takes through the same cache is written too. Never throws.
/// </summary>
internal static class StoreRelationSizesWriter
{
    /// <summary>How long after the store is ready the first pass waits, so it does not start with the MCP and web hosts' own first passes.</summary>
    internal static readonly TimeSpan FirstPassDelay = TimeSpan.FromMinutes(2);

    /// <summary>How often the loop asks whether a pass is due. The check is a lock and a clock read, so the cadence is kept to within this.</summary>
    internal static readonly TimeSpan CheckInterval = TimeSpan.FromMinutes(1);

    /// <summary>Wires <paramref name="cache"/> to write each pass it takes to <paramref name="postgres"/>, with the log lines for it.</summary>
    internal static void Attach(DarlingStoreTableHealthCache cache, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(logger);
        cache.OnReadFailed = (part, ex) => logger.LogWarning(
            ex, "Store relation sizes: the read of {Part} failed ({Reason}); the next cadence tries again", part, ex.GetType().Name);
        cache.OnSizesMeasured = async (store, measured, token) =>
        {
            var write = await StoreRelationSizesStore.WriteAsync(store, measured, token).ConfigureAwait(false);
            logger.LogInformation(
                "Store relation sizes: wrote the pass of {Relations} relation(s) measured {MeasuredAtUtc:O} ({Deleted} removed, {Upserted} added or changed)",
                measured.Pass.Oids.Length, measured.MeasuredAtUtc, write.Deleted, write.Upserted);
        };
    }

    /// <summary>The cadence loop. Ends at shutdown.</summary>
    internal static async Task RunAsync(
        NpgsqlDataSource postgres, DarlingStoreTableHealthCache cache, Func<TimeSpan> cadence, ILogger logger, CancellationToken stoppingToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(cadence);
        ArgumentNullException.ThrowIfNull(logger);
        Attach(cache, logger);
        try
        {
            await Task.Delay(FirstPassDelay, stoppingToken).ConfigureAwait(false);
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await cache.RunDueSizePassAsync(postgres, cadence(), stoppingToken).ConfigureAwait(false);
                }
#pragma warning disable CA1031 // A failed pass is logged and tried again a check later: the loop never stops the worker.
                catch (Exception ex) when (ex is not OperationCanceledException)
#pragma warning restore CA1031
                {
                    logger.LogWarning(
                        ex, "Store relation sizes: the size pass failed ({Reason}); the next check tries again", ex.GetType().Name);
                }

                await Task.Delay(CheckInterval, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            /* Shutdown. */
        }
    }
}
