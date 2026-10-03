/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// #4938: what a collector's run time and a restart need from Lite's local database. The last run of every daily
/// collector is read from collection_log once per server, and the server's clock from its newest server_properties
/// row, then the scheduler works from memory.
/// </summary>
public partial class RemoteCollectorService
{
    /// <summary>One start-up read per server, shared by the scheduled sweep and the tab-open run, so a tab opened at
    /// launch waits for the same read instead of racing it. A read that failed is dropped, and the next call tries again.</summary>
    private readonly ConcurrentDictionary<string, Task<bool>> _runTimeReady = new();

    /// <summary>
    /// Reads, once per server, the last run of every daily collector (effective interval of a day or more, with or
    /// without a run time) and the server's clock, and hands both to the scheduler. After this a restart, or a tab
    /// open, no longer makes a daily collector that ran today due.
    ///
    /// <para><b>The statuses that count as a run: all of them.</b> Darling's connect-time last-run read
    /// (<c>DarlingWorker.ReadCollectorWatermarksAsync</c>) has no status filter, because a failed attempt still wrote
    /// a row and still reset the cadence clock, so this read has none either and the two apps agree. The read covers
    /// the longest daily interval plus a day, in the live collection_log only; a collector whose last run is older
    /// than that is due anyway.</para>
    ///
    /// <para>Failure-isolated: a store hiccup leaves the collectors as never run, which is how Lite started before
    /// this read, and never stops the sweep.</para>
    /// </summary>
    internal async Task EnsureRunTimeReadyAsync(ServerConnection server, CancellationToken cancellationToken = default)
    {
        var ready = _runTimeReady.GetOrAdd(server.Id, _ => PrepareRunTimeAsync(server));
        var ok = await ready.WaitAsync(cancellationToken);
        if (!ok)
        {
            _runTimeReady.TryRemove(new KeyValuePair<string, Task<bool>>(server.Id, ready));
        }
    }

    private async Task<bool> PrepareRunTimeAsync(ServerConnection server)
    {
        var storageId = GetServerId(server);
        try
        {
            var clock = await ReadServerClockAsync(storageId, CancellationToken.None);
            _scheduleManager.SetServerRunContext(server.Id, storageId, server.DisplayName, clock);

            var daily = _scheduleManager.GetDailyCollectorIntervalsForServer(server.Id);
            if (daily.Count > 0)
            {
                var lookback = TimeSpan.FromMinutes(daily.Values.Max()) + TimeSpan.FromDays(1);
                var lastRuns = await ReadCollectorLastRunsAsync(storageId, daily.Keys, lookback, CancellationToken.None);
                _scheduleManager.SeedLastRunsForServer(server.Id, lastRuns);
            }

            return true;
        }
        catch (Exception ex)
        {
            _logger?.LogDebug("Reading the last runs and the clock for server '{Server}' failed: {Message}", server.DisplayName, ex.Message);

            /* The stable id still goes in, so a run time's spread is the same whether or not the read worked. */
            _scheduleManager.SetServerRunContext(server.Id, storageId, server.DisplayName, clock: null);
            return false;
        }
    }

    /// <summary>
    /// Reads the server's clock again after a new server_properties row, and gives it to the scheduler. No row, or a
    /// failed read, keeps the clock already known.
    /// </summary>
    internal async Task RefreshRunTimeClockAsync(ServerConnection server, CancellationToken cancellationToken = default)
    {
        try
        {
            var storageId = GetServerId(server);
            var clock = await ReadServerClockAsync(storageId, cancellationToken);
            if (clock is not null)
            {
                _scheduleManager.SetServerRunContext(server.Id, storageId, server.DisplayName, clock);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger?.LogDebug("Refreshing the clock for server '{Server}' failed: {Message}", server.DisplayName, ex.Message);
        }
    }

    /// <summary>The newest server_properties row's clock (the statement the Lite tabs read with), or null when no row
    /// carries an offset yet.</summary>
    private async Task<ServerClock?> ReadServerClockAsync(int storageServerId, CancellationToken cancellationToken)
    {
        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = LocalDataService.ServerClockSql;
        command.Parameters.Add(new DuckDBParameter { Value = storageServerId });

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        var offset = reader.IsDBNull(0) ? (int?)null : Convert.ToInt32(reader.GetValue(0));
        var zoneId = reader.IsDBNull(1) ? null : reader.GetString(1);
        return offset.HasValue ? ServerClock.Resolve(zoneId, offset) : null;
    }

    /// <summary>The newest collection_time per collector for a server within <paramref name="lookback"/>, for the
    /// named collectors, any status. The log's naive UTC time is relabeled UTC (a relabel, not a shift).</summary>
    private async Task<Dictionary<string, DateTime>> ReadCollectorLastRunsAsync(
        int storageServerId, IEnumerable<string> collectors, TimeSpan lookback, CancellationToken cancellationToken)
    {
        var wanted = new HashSet<string>(collectors, StringComparer.OrdinalIgnoreCase);
        var lastRuns = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);

        using var readLock = _duckDb.AcquireReadLock(cancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT collector_name, MAX(collection_time)
FROM collection_log
WHERE server_id = $1
AND   collection_time >= $2
GROUP BY collector_name";
        command.Parameters.Add(new DuckDBParameter { Value = storageServerId });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow - lookback });

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.IsDBNull(1))
            {
                continue;
            }

            var name = reader.GetString(0);
            if (wanted.Contains(name))
            {
                lastRuns[name] = DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc);
            }
        }

        return lastRuns;
    }
}
