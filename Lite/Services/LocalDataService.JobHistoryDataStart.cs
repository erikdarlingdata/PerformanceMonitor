/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public sealed partial class LocalDataService
{
    /// <summary>The cache key of the All Servers scope in <see cref="_oldestJobHistory"/>: no server has this id.</summary>
    private const int AllServersScope = int.MinValue;

    private readonly ConcurrentDictionary<int, (long Generation, DateTime? Oldest, DateTime ReadUtc)> _oldestJobHistory = new();

    /// <summary>
    /// The oldest instant the store holds job history from, for the Job History tab's time range picker's "Data starts" note:
    /// the oldest <c>job_history</c> row of <paramref name="serverId"/>, or of every server with a null <paramref name="serverId"/>
    /// (the tab's All Servers view), archived or hot, by the time it was collected. Null when the scope has no row yet. The note
    /// previews whatever range a reader types, so it cannot take the grid's window-bounded coverage probe
    /// (<see cref="GetJobHistoryDataStartAsync"/>): a probe answers the oldest row inside the window it was asked about, which
    /// for a covered window is the window's own start, not where the data starts.
    /// <para>One <c>MIN(collection_time)</c> over <c>v_job_history</c>, so the archive counts, kept per scope per archive view
    /// generation and for at most <see cref="ServerDataStartMaxAge"/>, as <see cref="GetOldestRetainedAsync"/> keeps the server
    /// tab's: a store whose archive stands still costs one read per scope, and an archive pass is followed by one more. The
    /// generation is sampled before the read, so a rebuild that lands mid-read leaves an entry the next call reads again.</para>
    /// </summary>
    public async Task<DateTime?> GetOldestJobHistoryAsync(int? serverId, CancellationToken cancellationToken = default)
    {
        var generation = _duckDb.ArchiveViewGeneration;
        var scope = serverId ?? AllServersScope;
        if (_oldestJobHistory.TryGetValue(scope, out var cached)
            && cached.Generation == generation
            && DateTime.UtcNow - cached.ReadUtc < ServerDataStartMaxAge)
        {
            return cached.Oldest;
        }

        using var slot = await WaitForArchiveSlotAsync(timer: null, cancellationToken);
        using var connection = await OpenConnectionAsync(timer: null, cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = serverId is null
            ? "SELECT MIN(collection_time) FROM v_job_history"
            : "SELECT MIN(collection_time) FROM v_job_history WHERE server_id = $1";
        if (serverId is int id)
        {
            command.Parameters.Add(new DuckDBParameter { Value = id });
        }

        var value = await command.ExecuteScalarAsync(cancellationToken);
        DateTime? oldest = value is DateTime found ? DateTime.SpecifyKind(found, DateTimeKind.Utc) : null;
        _oldestJobHistory[scope] = (generation, oldest, DateTime.UtcNow);
        return oldest;
    }
}
