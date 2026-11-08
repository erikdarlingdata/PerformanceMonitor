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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Services;

public sealed partial class LocalDataService
{
    /// <summary>How long a server's first collection read stands (#7386). The first collection only moves later, when archiving
    /// takes the oldest rows out of the hot table, and the archive guard in <see cref="GetServerDataStartAsync"/> covers that.</summary>
    internal static readonly TimeSpan ServerDataStartMaxAge = TimeSpan.FromMinutes(10);

    private readonly ConcurrentDictionary<int, (DateTime? First, DateTime ReadUtc)> _serverDataStarts = new();

    /// <summary>
    /// When the server was first collected, for the charts' X axis (#7386), or null when that cannot be said without reading the
    /// Parquet archive (the axis then stays as picked). One cheap read of the hot <c>collection_log</c>, the table
    /// <see cref="GetCollectorLastRunAsync"/> takes the server's first collection from, kept per server for
    /// <see cref="ServerDataStartMaxAge"/>.
    ///
    /// <para><b>It never scans the archive.</b> Archiving moves the oldest rows out of the hot table, so the hot table's oldest row
    /// is the server's first collection only when the archive holds nothing at or after <paramref name="windowStartUtc"/>: every row
    /// from there on is hot, and a first collection later than the start means the server had nothing before it. That is the same
    /// archive span the window-floor probes use (<see cref="GetArchiveSpanAsync"/>), read once per archive generation for every
    /// reader (#7256). An archive that may hold rows in the window, or an unknown span, answers null.</para>
    /// </summary>
    public async Task<DateTime?> GetServerDataStartAsync(int serverId, DateTime windowStartUtc, CancellationToken cancellationToken = default)
    {
        var span = await GetArchiveSpanAsync("collection_log", "collection_time", cancellationToken);
        if (!ArchiveHoldsNothingFrom(span, windowStartUtc))
        {
            return null;
        }

        if (_serverDataStarts.TryGetValue(serverId, out var cached) && DateTime.UtcNow - cached.ReadUtc < ServerDataStartMaxAge)
        {
            return cached.First;
        }

        using var connection = await OpenConnectionAsync(timer: null, cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT MIN(collection_time)
FROM collection_log
WHERE server_id = $1
AND   collector_name <> '{CollectionCycleSkip.CollectorName}'";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var value = await command.ExecuteScalarAsync(cancellationToken);
        DateTime? first = value is DateTime found ? DateTime.SpecifyKind(found, DateTimeKind.Utc) : null;
        _serverDataStarts[serverId] = (first, DateTime.UtcNow);
        return first;
    }

    private readonly ConcurrentDictionary<int, (long Generation, DateTime? Oldest, DateTime ReadUtc)> _oldestRetained = new();

    /// <summary>
    /// The oldest instant the store holds data from for <paramref name="serverId"/>, for the time range picker's "Data starts"
    /// note, or null when the server has no row yet. The archive's span (<see cref="GetArchiveSpanAsync"/>) covers every server's
    /// rows, so when it holds none the answer is the server's first hot collection (<see cref="GetServerDataStartAsync"/>), the
    /// cached read the charts already make. When the archive holds rows, the hot table's oldest row cannot say where this
    /// server's data starts: an archive-and-reset leaves every server with a recent hot start, and a server added today has
    /// nothing in the archive at all. That case reads <c>MIN(collection_time)</c> over <c>v_collection_log</c> for the server,
    /// once per server per archive view generation and for at most <see cref="ServerDataStartMaxAge"/>, so a store whose archive
    /// stands still costs one read per server, and an archive pass is followed by one more. The generation is sampled before
    /// the read, so a rebuild that lands mid-read leaves an entry the next call reads again.
    /// </summary>
    public async Task<DateTime?> GetOldestRetainedAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var span = await GetArchiveSpanAsync("collection_log", "collection_time", cancellationToken);
        if (span is { Oldest: null })
        {
            return await GetServerDataStartAsync(serverId, DateTime.MinValue, cancellationToken);
        }

        var generation = _duckDb.ArchiveViewGeneration;
        if (_oldestRetained.TryGetValue(serverId, out var cached)
            && cached.Generation == generation
            && DateTime.UtcNow - cached.ReadUtc < ServerDataStartMaxAge)
        {
            return cached.Oldest;
        }

        using var slot = await WaitForArchiveSlotAsync(timer: null, cancellationToken);
        using var connection = await OpenConnectionAsync(timer: null, cancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT MIN(collection_time)
FROM v_collection_log
WHERE server_id = $1
AND   collector_name <> '{CollectionCycleSkip.CollectorName}'";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var value = await command.ExecuteScalarAsync(cancellationToken);
        DateTime? oldest = value is DateTime found ? DateTime.SpecifyKind(found, DateTimeKind.Utc) : null;
        _oldestRetained[serverId] = (generation, oldest, DateTime.UtcNow);
        return oldest;
    }
}
