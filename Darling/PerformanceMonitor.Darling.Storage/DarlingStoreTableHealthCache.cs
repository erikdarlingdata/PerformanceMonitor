/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableHealthReader;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One part of the store health read (tables, indexes, partition index coverage or autovacuum settings) with the time the store finished
/// answering it. Every surface shows <see cref="ReadAtUtc"/> or the age, so a held answer is never shown as a new one.</summary>
public sealed record StoreHealthRead<T>(T Rows, DateTime ReadAtUtc)
{
    /// <summary>Whole seconds between <see cref="ReadAtUtc"/> and <paramref name="utcNow"/>, never negative.</summary>
    public long AgeSeconds(DateTime utcNow) => Math.Max(0L, (long)(utcNow - ReadAtUtc).TotalSeconds);
}

/// <summary>
/// #8924: the last answer of each <see cref="DarlingStoreTableHealthReader"/> read, held in process for
/// <see cref="Lifetime"/>. The table statement and the index statement call a size function for every relation and every
/// index of the store, chunks included, so each takes 3 to 8 s on a store with a few thousand indexes however few rows
/// the caller asks for. The catalogs they read move slowly (sizes, statistics, autovacuum counters), so a repeat inside
/// the lifetime answers from the held rows, and a caller cuts them (top, offset, candidates only, section) afterwards.
///
/// <para><b>Key.</b> One cache per <see cref="NpgsqlDataSource"/> (<see cref="For"/>): a data source is one store read as
/// one role, and what a role may see in the catalogs is part of the answer. The MCP tool and the web route each use their
/// host's data source, and the Viewer holds a cache on its data service.</para>
///
/// <para><b>Single flight.</b> Each part is a <see cref="SingleFlightTtlCache{T}"/>: callers that arrive while a read is
/// running wait for that read instead of starting another, the read runs under no caller's token (each statement is still
/// bounded by <see cref="StorageCommandDeadlines.McpReadSeconds"/>), and a caller's own token ends only its wait. A read
/// that fails or runs past its limit is not held, so the next call reads the store again.</para>
///
/// <para><b>Lifetime.</b> Five minutes, the lifetime of the other held reads whose figures move slowly
/// (<c>ReadAdmission.FleetCachedRoutes</c>, <c>ReadAdmission.ServerHeldRoutes</c>, the Viewer's store size). It is counted
/// from the moment the read finished, not from a wall-clock bucket, so a read of several seconds is held for the full
/// five minutes after it.</para>
/// </summary>
public sealed class DarlingStoreTableHealthCache
{
    /// <summary>How long a finished read is held.</summary>
    public static readonly TimeSpan Lifetime = TimeSpan.FromMinutes(5);

    private static readonly ConditionalWeakTable<NpgsqlDataSource, DarlingStoreTableHealthCache> Stores = new();

    private readonly Func<DateTime> _clock;
    private readonly Func<NpgsqlDataSource, CancellationToken, Task<List<StoreTableHealthRow>>> _readTables;
    private readonly Func<NpgsqlDataSource, CancellationToken, Task<List<StoreIndexHealthRow>>> _readIndexes;
    private readonly Func<NpgsqlDataSource, CancellationToken, Task<List<StoreIndexCoverageRow>>> _readCoverage;
    private readonly Func<NpgsqlDataSource, CancellationToken, Task<StoreAutovacuumSettings>> _readAutovacuumSettings;
    private readonly object _gate = new();

    private SingleFlightTtlCache<StoreHealthRead<IReadOnlyList<StoreTableHealthRow>>> _tables;
    private SingleFlightTtlCache<StoreHealthRead<IReadOnlyList<StoreIndexHealthRow>>> _indexes;
    private SingleFlightTtlCache<StoreHealthRead<IReadOnlyList<StoreIndexCoverageRow>>> _coverage;
    private SingleFlightTtlCache<StoreHealthRead<StoreAutovacuumSettings>> _autovacuumSettings;

    /// <summary>A cache over the reader's own statements, on the wall clock.</summary>
    public DarlingStoreTableHealthCache()
        : this(
            static () => DateTime.UtcNow,
            DarlingStoreTableHealthReader.GetAsync,
            DarlingStoreTableHealthReader.GetIndexesAsync,
            DarlingStoreTableHealthReader.GetIndexCoverageAsync)
    {
    }

    /// <summary>The same cache over <paramref name="clock"/> (a UTC reading) and the reads given. A test hands in a
    /// clock it steps across <see cref="Lifetime"/> and reads that count their calls. With no
    /// <paramref name="readAutovacuumSettings"/>, the settings come from <see cref="DarlingStoreTableHealthReader.GetAutovacuumSettingsAsync"/>
    /// as of <paramref name="clock"/>.</summary>
    public DarlingStoreTableHealthCache(
        Func<DateTime> clock,
        Func<NpgsqlDataSource, CancellationToken, Task<List<StoreTableHealthRow>>> readTables,
        Func<NpgsqlDataSource, CancellationToken, Task<List<StoreIndexHealthRow>>> readIndexes,
        Func<NpgsqlDataSource, CancellationToken, Task<List<StoreIndexCoverageRow>>> readCoverage,
        Func<NpgsqlDataSource, CancellationToken, Task<StoreAutovacuumSettings>>? readAutovacuumSettings = null)
    {
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(readTables);
        ArgumentNullException.ThrowIfNull(readIndexes);
        ArgumentNullException.ThrowIfNull(readCoverage);
        _clock = clock;
        _readTables = readTables;
        _readIndexes = readIndexes;
        _readCoverage = readCoverage;
        _readAutovacuumSettings = readAutovacuumSettings
            ?? ((store, ct) => DarlingStoreTableHealthReader.GetAutovacuumSettingsAsync(store, clock(), ct));
        _tables = new(Lifetime, clock);
        _indexes = new(Lifetime, clock);
        _coverage = new(Lifetime, clock);
        _autovacuumSettings = new(Lifetime, clock);
    }

    /// <summary>The process's cache for <paramref name="postgres"/>. It lives as long as the data source does.</summary>
    public static DarlingStoreTableHealthCache For(NpgsqlDataSource postgres)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        return Stores.GetValue(postgres, static _ => new DarlingStoreTableHealthCache());
    }

    /// <summary>The cache's clock, so a caller works out an age against the clock the read was stamped on.</summary>
    public DateTime UtcNow() => _clock();

    /// <summary>Every store table's health in the reader's order (<see cref="DarlingStoreTableHealthReader.GetAsync"/>), from the held read when one is
    /// inside <see cref="Lifetime"/>. <paramref name="fresh"/> drops the held read first, for a caller that asked for a new one.</summary>
    public Task<StoreHealthRead<IReadOnlyList<StoreTableHealthRow>>> GetTablesAsync(
        NpgsqlDataSource postgres, bool fresh = false, CancellationToken cancellationToken = default) =>
        ReadAsync(ref _tables, _readTables, postgres, fresh, cancellationToken);

    /// <summary>Every store index (<see cref="DarlingStoreTableHealthReader.GetIndexesAsync"/>), held the same way.</summary>
    public Task<StoreHealthRead<IReadOnlyList<StoreIndexHealthRow>>> GetIndexesAsync(
        NpgsqlDataSource postgres, bool fresh = false, CancellationToken cancellationToken = default) =>
        ReadAsync(ref _indexes, _readIndexes, postgres, fresh, cancellationToken);

    /// <summary>The leaf index coverage of every partitioned parent (<see cref="DarlingStoreTableHealthReader.GetIndexCoverageAsync"/>), held the same way.</summary>
    public Task<StoreHealthRead<IReadOnlyList<StoreIndexCoverageRow>>> GetIndexCoverageAsync(
        NpgsqlDataSource postgres, bool fresh = false, CancellationToken cancellationToken = default) =>
        ReadAsync(ref _coverage, _readCoverage, postgres, fresh, cancellationToken);

    /// <summary>The store's autovacuum settings, workers running and cancels in the window
    /// (<see cref="DarlingStoreTableHealthReader.GetAutovacuumSettingsAsync"/>), held the same way.</summary>
    public Task<StoreHealthRead<StoreAutovacuumSettings>> GetAutovacuumSettingsAsync(
        NpgsqlDataSource postgres, bool fresh = false, CancellationToken cancellationToken = default) =>
        ReadOneAsync(ref _autovacuumSettings, _readAutovacuumSettings, postgres, fresh, cancellationToken);

    private Task<StoreHealthRead<IReadOnlyList<TRow>>> ReadAsync<TRow>(
        ref SingleFlightTtlCache<StoreHealthRead<IReadOnlyList<TRow>>> slot,
        Func<NpgsqlDataSource, CancellationToken, Task<List<TRow>>> read,
        NpgsqlDataSource postgres,
        bool fresh,
        CancellationToken cancellationToken) =>
        ReadOneAsync(
            ref slot,
            async (store, ct) => (IReadOnlyList<TRow>)await read(store, ct).ConfigureAwait(false),
            postgres,
            fresh,
            cancellationToken);

    private Task<StoreHealthRead<T>> ReadOneAsync<T>(
        ref SingleFlightTtlCache<StoreHealthRead<T>> slot,
        Func<NpgsqlDataSource, CancellationToken, Task<T>> read,
        NpgsqlDataSource postgres,
        bool fresh,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        /* A caller that has already left starts no read: the read runs under no caller's token, so it would run to its end for nobody.
           The caller gets the OperationCanceledException a connection open under its token gives. */
        if (cancellationToken.IsCancellationRequested)
        {
            return Task.FromException<StoreHealthRead<T>>(new OperationCanceledException(cancellationToken));
        }

        SingleFlightTtlCache<StoreHealthRead<T>> part;
        lock (_gate)
        {
            if (fresh)
            {
                /* A new part holds nothing, so the next read goes to the store. A read still running on the part it replaces
                   ends there and is not held. */
                slot = new(Lifetime, _clock);
            }

            part = slot;
        }

        return part.GetOrStartAsync(
            async () => new StoreHealthRead<T>(await read(postgres, CancellationToken.None).ConfigureAwait(false), _clock()),
            cancellationToken);
    }
}
