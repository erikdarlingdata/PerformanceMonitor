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
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The lightweight store reads that drive the shell chrome ported from Lite's MainWindow: the sidebar
/// status dot (one <c>v_collection_log</c> freshness query for every server at once) and the status bar's
/// database-size field (<c>pg_database_size</c>). Both are single round-trips so they can run on the
/// refresh timers without weighing anything down; the SQL lives in public constants so tests can pin the
/// load-bearing clauses without a live Postgres.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// Newest collection time per server across all collectors, in one statement — the sidebar dots and the
    /// status bar's collection field derive freshness from this (the same newest <c>collection_time</c> the
    /// Overview cards use per server, so a dot and its card agree). Timestamps are the store's naive UTC.
    /// Excludes <c>server_id = 0</c>, the fleet-level retention run-record sentinel
    /// (<c>DarlingObservability.FleetServerId</c>) — it is not a real server, so it must not appear as a
    /// phantom key a future key-iterating consumer could render as "server 0".
    ///
    /// <para><b>One probe per registered server, not an aggregate over the log</b> (#3895). This was a
    /// <c>GROUP BY server_id</c> over every retained row of <c>collection_log</c> — the store's biggest
    /// table, re-read on every refresh tick for a handful of timestamps: 93.8 ms of planning and 129.5 ms on
    /// DARLING01, and linear in servers x retained days on a field store. Now each registry row gets its own
    /// <c>LIMIT 1</c>, an index-only descent in the newest chunk for a server that is collecting (2.5 ms and
    /// 6.4 ms there). Every registry row, enabled or not, because Manage Servers shows a disabled server's
    /// last collection too; unbounded, because that "last collected" may be weeks old and is still the
    /// answer. The two callers look up registry ids only, so the rows they read are identical.</para>
    /// </summary>
    public const string ServerFreshnessSql = @"
SELECT
    s.server_id,
    latest.collection_time
FROM servers AS s
CROSS JOIN LATERAL
(
    SELECT collection_time
    FROM v_collection_log
    WHERE server_id = s.server_id
    ORDER BY collection_time DESC
    LIMIT 1
) AS latest
WHERE s.server_id <> 0";

    /// <summary>The store's on-disk size in bytes, measured live. No parameters.
    ///
    /// <para><c>pg_database_size</c> walks every file in the database directory, so its cost scales with the
    /// store rather than with the one number it returns (#4477 measured 468 ms mean / 1.96 s worst-case on a
    /// production store; walk finding D15 measured a mean of 4.4 s, worst 39.9 s, on a large one). The status-bar
    /// Database field therefore reads the size the service records on its self-metrics sweep first
    /// (<see cref="StoreSelfMetrics.LatestStoreSizeSql"/>) and runs this live walk only when no size is recorded, or
    /// the recorded read fails (#5555). See <see cref="StoreSizeCacheLifetime"/>.</para>
    /// </summary>
    public const string StoreSizeSql = "SELECT pg_database_size(current_database())";

    /// <summary>How long <see cref="GetStoreSizeBytesAsync"/> serves its cached reading before it reads again (#4477).
    /// Five minutes, not the refresh timer's own 10-600 s <c>NocRefreshIntervalSeconds</c>: the status-bar field is a
    /// coarse operator signal ("about how big is the store"), never a threshold or a stored numeric value.
    ///
    /// <para>#5555: the figure the field shows is now the self-metrics row, so it is only as fresh as the service's last
    /// sweep: about an hour old on average (mean ~59 min between sweeps), and older still if the sweep stalls while
    /// ingest continues. That is acceptable for a display-only field, but the five minutes now caches a figure that
    /// changes about hourly; it still bounds the store round trips (one recorded read per window), it no longer keeps the
    /// field fresher than that row. Only a store with no recorded size yet falls back to the live walk, and gets the
    /// live figure.</para></summary>
    public static readonly TimeSpan StoreSizeCacheLifetime = TimeSpan.FromMinutes(5);

    /// <summary>#4477: single-flighted and TTL-memoized the same way as
    /// <see cref="GetFleetCollectionHealthByServerAsync"/> — two refreshes racing a cold cache share ONE
    /// <see cref="StoreSizeSql"/> round trip instead of each running its own. A null reading (a transient
    /// read failure) is never cached, so the very next call retries rather than serving null for the rest
    /// of the window — the same rule the previous single-caller cache followed.</summary>
    private readonly SingleFlightTtlCache<long?> _storeSizeCache = new(StoreSizeCacheLifetime);



    /// <summary>
    /// Reads MAX(collection_time) for every server in a single query, keyed by server_id. A server with no
    /// collection rows simply isn't in the dictionary (the caller treats a miss as "no collection" → Offline).
    /// </summary>
    public async Task<Dictionary<int, DateTime>> GetServerFreshnessAsync(CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<int, DateTime>();

        await using var command = _dataSource.CreateCommand(ServerFreshnessSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (!reader.IsDBNull(1))
            {
                result[reader.GetInt32(0)] = reader.GetDateTime(1);
            }
        }

        return result;
    }

    /// <summary>The store database's size in bytes, or null when it can't be read. Cached for
    /// <see cref="StoreSizeCacheLifetime"/> (#4477): a call inside the window returns the cached reading with
    /// no store round trip at all. A cold read takes the size the service recorded on its last self-metrics sweep
    /// (up to one sweep old, see <see cref="StoreSizeCacheLifetime"/>) and walks the live directory
    /// (<see cref="StoreSizeSql"/>) only when none is recorded or the recorded read fails.</summary>
    public Task<long?> GetStoreSizeBytesAsync(CancellationToken cancellationToken = default)
        => _storeSizeCache.GetOrStartAsync(FetchStoreSizeBytesAsync, shouldCache: static bytes => bytes is not null, cancellationToken);

    /// <summary>The actual read behind <see cref="GetStoreSizeBytesAsync"/>'s single-flight gate. Runs with
    /// <see cref="CancellationToken.None"/> (via <see cref="SingleFlightTtlCache{T}"/>): shared work, not any
    /// one caller's.</summary>
    private async Task<long?> FetchStoreSizeBytesAsync()
    {
        /* Walk finding D15: the status-bar field is display only, so it reads the size the service already records on its
           hourly self-metrics sweep (the #3209 shape the service's own disk check reads) and measures the live directory
           only for a store that has not swept yet. pg_database_size took a mean of 4.4 s (worst 39.9 s) on a large store,
           every five minutes, for a number the field rounds to a whole MB or one GB decimal. */
        try
        {
            await using var recorded = _dataSource.CreateCommand(StoreSelfMetrics.LatestStoreSizeSql);
            recorded.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            var recordedBytes = await recorded.ExecuteScalarAsync(CancellationToken.None);
            if (recordedBytes is not null && recordedBytes != DBNull.Value)
            {
                return Convert.ToInt64(recordedBytes);
            }
        }
        catch (Exception)
        {
            /* The recorded row is an optimisation. A viewer role without SELECT on collect.store_metrics, a store that does not
               have the relation yet, or a transient error must not take the live answer away: before this read existed the live
               directory walk answered, so it still does. */
        }

        await using var command = _dataSource.CreateCommand(StoreSizeSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        var result = await command.ExecuteScalarAsync(CancellationToken.None);
        return result is null || result == DBNull.Value ? (long?)null : Convert.ToInt64(result);
    }
}
