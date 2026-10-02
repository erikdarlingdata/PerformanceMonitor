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
/// #4957: runs one rollup-coverage probe in the background shortly after a process opens its store, so the first
/// reader finds every rollup's floor already measured.
///
/// <para><b>Why.</b> On a store with a long Query Store history, a cold
/// <see cref="TimescaleSupport.DetectRollupCoverageAsync(NpgsqlDataSource, RollupAvailability, CancellationToken)"/>
/// is about ten seconds: each rollup's <c>min(bucket)</c> decompresses and sorts the batches of that rollup's
/// oldest compressed chunk. Before this, the first <c>get_daily_summary</c> (or the Viewer's first routed read)
/// after a start paid that inline, ahead of its own query. The floors are cached per store (see
/// <see cref="TimescaleSupport"/>), so one probe here is enough for every data source the process opens on that
/// store.</para>
///
/// <para><b>What it probes.</b> The same two calls both production callers make —
/// <see cref="TimescaleSupport.DetectRollupsAsync"/>, then
/// <see cref="TimescaleSupport.DetectRollupCoverageAsync(NpgsqlDataSource, RollupAvailability, CancellationToken)"/>
/// with what it found — so the views measured are exactly the union the callers need: every rollup the store has.
/// It deliberately does not go through the service's cached availability answer or the Viewer's: a failed probe
/// there is cached as "no rollups" for five minutes, and a failed warm must change nothing else.</para>
///
/// <para><b>The delay</b> follows <see cref="QueryStoreIntervalWideBrinIndex.RunDelayedAsync"/>: wait, make one
/// attempt on the process's own data source, never throw. It is shorter than that index build's, because this
/// probe is read-only and bounded, and a first call that arrives before it still measures inline, exactly as it
/// did before.</para>
/// </summary>
public static class RollupCoverageWarmup
{
    /// <summary>How long the service waits after start before warming the rollup floors: past the start-up
    /// collection burst, early enough that the first call from an agent or the web UI usually finds them measured.</summary>
    public static readonly TimeSpan ServiceStartDelay = TimeSpan.FromSeconds(30);

    /// <summary>How long the Viewer waits after opening its store before warming the rollup floors: enough for its
    /// open-time reachability, schema and seat reads to clear the pool, short enough to beat the first tab load.</summary>
    public static readonly TimeSpan ViewerStartDelay = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Waits <paramref name="delay"/>, then probes the store's rollup coverage once, and never throws. A cancel
    /// (shutdown, or the data source going away) and any failure alike log once at Debug and change nothing else:
    /// the caller that needs the floors measures them itself, as it always has. A null
    /// <paramref name="logger"/> logs nothing (the Viewer has no <see cref="ILogger"/> and no Debug level).
    /// </summary>
    public static async Task RunDelayedAsync(
        NpgsqlDataSource postgres, ILogger? logger, TimeSpan delay, CancellationToken cancellationToken)
    {
        try
        {
            if (delay > TimeSpan.Zero)
            {
                await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            }

            var rollups = await TimescaleSupport.DetectRollupsAsync(postgres, cancellationToken).ConfigureAwait(false);
            await TimescaleSupport.DetectRollupCoverageAsync(postgres, rollups, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger?.LogDebug(
                "Rollup coverage warm-up did not complete, so the first caller measures the floors itself: {ExceptionType}: {Message}",
                ex.GetType().Name, ex.Message);
        }
    }
}
