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
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitorLite.Services;

public sealed partial class LocalDataService
{
    /// <summary>The cache key of the All Servers scope in <see cref="_oldestJobHistory"/>: no server has this id.</summary>
    private const int AllServersScope = int.MinValue;

    private readonly ConcurrentDictionary<int, (long Generation, DateTime? Oldest, DateTime ReadUtc)> _oldestJobHistory = new();

    /// <summary>
    /// The oldest instant the store holds job history from, for the Job History tab's time range picker's "Data starts" note:
    /// the earlier of the oldest <c>job_history</c> row's collection and the oldest run it holds, for <paramref name="serverId"/>,
    /// or for every server with a null <paramref name="serverId"/> (the tab's All Servers view), archived or hot. A first
    /// collection copies the history msdb already holds, so a run can sit long before the collection that stored it, and the
    /// grid shows it: the note names that run. Null when the scope has no row yet. The note previews whatever range a reader
    /// types, so it cannot take the grid's window-bounded coverage probe (<see cref="GetJobHistoryDataStartAsync"/>): a probe
    /// answers the oldest row inside the window it was asked about, which for a covered window is the window's own start.
    /// <para>One read over <c>v_job_history</c>, so the archive counts: per server, <c>MIN(collection_time)</c> and
    /// <c>MIN(run_datetime)</c>, the run on the server's wall clock and read to UTC on the clock the grid reads that server's
    /// runs on (<see cref="ReadJobHistoryClockAsync"/>, with <paramref name="openTabClocks"/> as the grid's read takes them).
    /// Kept per scope per archive view generation and for at most <see cref="ServerDataStartMaxAge"/>, as
    /// <see cref="GetOldestRetainedAsync"/> keeps the server tab's: a store whose archive stands still costs one read per scope,
    /// and an archive pass is followed by one more. The generation is sampled before the read, so a rebuild that lands mid-read
    /// leaves an entry the next call reads again.</para>
    /// </summary>
    public Task<DateTime?> GetOldestJobHistoryAsync(int? serverId, CancellationToken cancellationToken = default) =>
        GetOldestJobHistoryAsync(serverId, null, cancellationToken);

    /// <inheritdoc cref="GetOldestJobHistoryAsync(int?, CancellationToken)"/>
    public async Task<DateTime?> GetOldestJobHistoryAsync(
        int? serverId, IReadOnlyDictionary<int, ServerClock>? openTabClocks, CancellationToken cancellationToken = default)
    {
        var generation = _duckDb.ArchiveViewGeneration;
        var scope = serverId ?? AllServersScope;
        if (_oldestJobHistory.TryGetValue(scope, out var cached)
            && cached.Generation == generation
            && DateTime.UtcNow - cached.ReadUtc < ServerDataStartMaxAge)
        {
            return cached.Oldest;
        }

        var servers = new List<(int ServerId, DateTime OldestCollection, DateTime? OldestRun)>();
        using (var slot = await WaitForArchiveSlotAsync(timer: null, cancellationToken))
        using (var connection = await OpenConnectionAsync(timer: null, cancellationToken))
        using (var command = connection.CreateCommand())
        {
            command.CommandText = serverId is null
                ? "SELECT server_id, MIN(collection_time), MIN(run_datetime) FROM v_job_history GROUP BY server_id"
                : "SELECT server_id, MIN(collection_time), MIN(run_datetime) FROM v_job_history WHERE server_id = $1 GROUP BY server_id";
            if (serverId is int id)
            {
                command.Parameters.Add(new DuckDBParameter { Value = id });
            }

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.GetValue(1) is not DateTime collected)
                {
                    continue;
                }

                servers.Add(((int)ToInt64(reader.GetValue(0)), DateTime.SpecifyKind(collected, DateTimeKind.Utc), reader.GetValue(2) as DateTime?));
            }
        }

        DateTime? oldest = null;
        foreach (var (id, oldestCollection, oldestRun) in servers)
        {
            var start = oldestCollection;
            if (oldestRun is DateTime run)
            {
                var clock = await ReadJobHistoryClockAsync(id, openTabClocks is not null && openTabClocks.TryGetValue(id, out var open) ? open : null);
                var runUtc = clock.ToUtc(run);
                if (runUtc < start)
                {
                    start = runUtc;
                }
            }

            if (oldest is null || start < oldest.Value)
            {
                oldest = start;
            }
        }

        _oldestJobHistory[scope] = (generation, oldest, DateTime.UtcNow);
        return oldest;
    }
}
