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
/// <para><b>Single flight (#9224).</b> A part is never read twice at once: a call that arrives while a read of that part is
/// running waits for that read instead of starting another, the read runs under no caller's token (each statement is still
/// bounded by <see cref="StorageCommandDeadlines.McpReadSeconds"/>), and a caller's own token ends only its wait. A part
/// that has never been read, or whose first read failed or ran past its limit, holds nothing, so the call reads the store
/// inline, and the next call after a failure reads it again.</para>
///
/// <para><b>Lifetime.</b> Five minutes, the lifetime of the other held reads whose figures move slowly
/// (<c>ReadAdmission.FleetCachedRoutes</c>, <c>ReadAdmission.ServerHeldRoutes</c>, the Viewer's store size). It is counted
/// from the moment the read finished, not from a wall-clock bucket, so a read of several seconds is held for the full
/// five minutes after it.</para>
///
/// <para><b>Held first, refreshed behind (#9224).</b> A cold pass takes 8 to 13 s on a store with thousands of chunks (the
/// size work is per relation), more than a caller should wait however it is trimmed. So a part already held is never waited
/// for: past <see cref="Lifetime"/> the call gets the held part at once, with its own <see cref="StoreHealthRead{T}.ReadAtUtc"/>,
/// and starts ONE read behind it. At most one refresh starts per part per <see cref="Lifetime"/>, counted from the last
/// attempt, so a refresh that failed (the held part is kept and <see cref="OnReadFailed"/> says so once) is tried again by the first
/// call a <see cref="Lifetime"/> after it. <see cref="WarmAsync"/> takes the first pass of every part when the service starts, so a
/// part is normally held before anyone asks. <c>fresh</c> still drops the held part and waits for a new read.</para>
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

    private HeldPart<IReadOnlyList<StoreTableHealthRow>> _tables;
    private HeldPart<IReadOnlyList<StoreIndexHealthRow>> _indexes;
    private HeldPart<IReadOnlyList<StoreIndexCoverageRow>> _coverage;
    private HeldPart<StoreAutovacuumSettings> _autovacuumSettings;
    private MeasuredSizePass? _lastNoted;
    private Task _writeTail = Task.CompletedTask;

    /// <summary>Told once for each read that failed outside a caller's wait: a refresh behind a held part (the held part is kept) and a
    /// part of <see cref="WarmAsync"/>. The service logs it and the Viewer writes it to its log; with none set the failure is silent.</summary>
    public Action<string, Exception>? OnReadFailed { get; set; }

    /// <summary>
    /// #9374: told once for each size pass that finished under a tables or indexes read of this cache (a warm, a refresh or a fresh read), with the
    /// data source it ran on and the pass. The service writes the pass to the store here (<see cref="StoreRelationSizesStore.WriteAsync"/>), so the
    /// store sees no size pass of its own. Writes run one at a time in the background and never delay or fail the read that took the pass: a failed
    /// write is reported to <see cref="OnReadFailed"/> as "relation sizes write". A pass shared by the tables read and the indexes read is told once.
    /// </summary>
    public Func<NpgsqlDataSource, MeasuredSizePass, CancellationToken, Task>? OnSizesMeasured { get; set; }

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
        _tables = new();
        _indexes = new();
        _coverage = new();
        _autovacuumSettings = new();
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
        ReadAsync(ref _tables, "tables", NoticingSizes(_readTables), postgres, fresh, cancellationToken);

    /// <summary>Every store index (<see cref="DarlingStoreTableHealthReader.GetIndexesAsync"/>), held the same way.</summary>
    public Task<StoreHealthRead<IReadOnlyList<StoreIndexHealthRow>>> GetIndexesAsync(
        NpgsqlDataSource postgres, bool fresh = false, CancellationToken cancellationToken = default) =>
        ReadAsync(ref _indexes, "indexes", NoticingSizes(_readIndexes), postgres, fresh, cancellationToken);

    /// <summary>The leaf index coverage of every partitioned parent (<see cref="DarlingStoreTableHealthReader.GetIndexCoverageAsync"/>), held the same way.</summary>
    public Task<StoreHealthRead<IReadOnlyList<StoreIndexCoverageRow>>> GetIndexCoverageAsync(
        NpgsqlDataSource postgres, bool fresh = false, CancellationToken cancellationToken = default) =>
        ReadAsync(ref _coverage, "partition index coverage", _readCoverage, postgres, fresh, cancellationToken);

    /// <summary>The store's autovacuum settings, workers running and cancels in the window
    /// (<see cref="DarlingStoreTableHealthReader.GetAutovacuumSettingsAsync"/>), held the same way.</summary>
    public Task<StoreHealthRead<StoreAutovacuumSettings>> GetAutovacuumSettingsAsync(
        NpgsqlDataSource postgres, bool fresh = false, CancellationToken cancellationToken = default) =>
        ReadOneAsync(ref _autovacuumSettings, "autovacuum settings", _readAutovacuumSettings, postgres, fresh, cancellationToken);

    /// <summary>The first pass of every part, one after another (the tables read's size pass is held for the indexes read), for a service
    /// that has just started: a call after it finds every part held. A part a caller is already reading is joined, never read twice. A part
    /// that fails is reported to <see cref="OnReadFailed"/> and does not stop the others. It never throws.</summary>
    public async Task WarmAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        await WarmPartAsync("tables", () => GetTablesAsync(postgres, cancellationToken: cancellationToken), cancellationToken).ConfigureAwait(false);
        await WarmPartAsync("indexes", () => GetIndexesAsync(postgres, cancellationToken: cancellationToken), cancellationToken).ConfigureAwait(false);
        await WarmPartAsync("partition index coverage", () => GetIndexCoverageAsync(postgres, cancellationToken: cancellationToken), cancellationToken).ConfigureAwait(false);
        await WarmPartAsync("autovacuum settings", () => GetAutovacuumSettingsAsync(postgres, cancellationToken: cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// #9374: takes one pass through this cache when it holds no tables read newer than <paramref name="cadence"/>, so the pass stays in the one
    /// place a pass happens and <see cref="OnSizesMeasured"/> writes it. A held read inside the cadence is reused and no pass runs. Returns whether it
    /// ran a pass. The pass is the tables read, dropped first (<c>fresh</c>) so it is new whatever <see cref="Lifetime"/> says.
    /// </summary>
    public async Task<bool> RunDueSizePassAsync(NpgsqlDataSource postgres, TimeSpan cadence, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var held = _tables.HeldRead;
        if (held is not null && _clock() - held.ReadAtUtc < cadence)
        {
            return false;
        }

        await GetTablesAsync(postgres, fresh: true, cancellationToken: cancellationToken).ConfigureAwait(false);
        return true;
    }

    /// <summary>#9374: whether this cache holds a tables read now (old or not), so a caller that would show the store's last measured sizes
    /// first knows the tab has a read to show already. Read only: it starts nothing.</summary>
    public bool HoldsTables => _tables.HeldRead is not null;

    private Func<NpgsqlDataSource, CancellationToken, Task<List<TRow>>> NoticingSizes<TRow>(
        Func<NpgsqlDataSource, CancellationToken, Task<List<TRow>>> read) =>
        async (store, ct) =>
        {
            var rows = await read(store, ct).ConfigureAwait(false);
            NoteSizePass(store);
            return rows;
        };

    private void NoteSizePass(NpgsqlDataSource store)
    {
        var write = OnSizesMeasured;
        var last = write is null ? null : DarlingStoreTableHealthReader.LastSizePassOn(store);
        if (write is null || last is null)
        {
            return;
        }

        lock (_gate)
        {
            if (ReferenceEquals(last, _lastNoted))
            {
                return;
            }

            _lastNoted = last;
            var before = _writeTail;
            _writeTail = Task.Run(async () =>
            {
                await before.ConfigureAwait(false);
                try
                {
                    await write(store, last, CancellationToken.None).ConfigureAwait(false);
                }
#pragma warning disable CA1031 // A failed write is reported and the next pass writes again; it never fails the read that took the pass.
                catch (Exception ex)
#pragma warning restore CA1031
                {
                    Report("relation sizes write", ex);
                }
            });
        }
    }

    private async Task WarmPartAsync(string name, Func<Task> read, CancellationToken cancellationToken)
    {
        try
        {
            await read().ConfigureAwait(false);
        }
#pragma warning disable CA1031 // The start pass is best effort: a failed part is reported and the first caller reads it.
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
#pragma warning restore CA1031
        {
            Report(name, ex);
        }
    }

    private void Report(string name, Exception ex)
    {
        try
        {
            OnReadFailed?.Invoke(name, ex);
        }
#pragma warning disable CA1031 // A broken log sink must not fail the read behind it.
        catch (Exception)
#pragma warning restore CA1031
        {
        }
    }

    /// <summary>Completes when no read is running on any part: a test waits on it after releasing a refresh, instead of a delay.</summary>
    internal Task WhenIdleAsync()
    {
        Task[] running;
        lock (_gate)
        {
            running = [_tables.Running, _indexes.Running, _coverage.Running, _autovacuumSettings.Running, _writeTail];
        }

        return Task.WhenAll(running);
    }

    private Task<StoreHealthRead<IReadOnlyList<TRow>>> ReadAsync<TRow>(
        ref HeldPart<IReadOnlyList<TRow>> slot,
        string name,
        Func<NpgsqlDataSource, CancellationToken, Task<List<TRow>>> read,
        NpgsqlDataSource postgres,
        bool fresh,
        CancellationToken cancellationToken) =>
        ReadOneAsync(
            ref slot,
            name,
            async (store, ct) => (IReadOnlyList<TRow>)await read(store, ct).ConfigureAwait(false),
            postgres,
            fresh,
            cancellationToken);

    private Task<StoreHealthRead<T>> ReadOneAsync<T>(
        ref HeldPart<T> slot,
        string name,
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

        HeldPart<T> part;
        lock (_gate)
        {
            if (fresh)
            {
                /* A new part holds nothing, so the next read goes to the store. A read still running on the part it replaces
                   ends there and is not held. */
                slot = new();
            }

            part = slot;
        }

        return part.GetAsync(
            () => read(postgres, CancellationToken.None),
            _clock,
            ex => Report(name, ex),
            cancellationToken);
    }

    /// <summary>One part: the read held, the read running (at most one), and when a refresh behind the held read last started.</summary>
    private sealed class HeldPart<T>
    {
        private readonly object _partGate = new();
        private StoreHealthRead<T>? _held;
        private Task<StoreHealthRead<T>>? _inFlight;
        private DateTime _lastRefreshStartUtc = DateTime.MinValue;

        /// <summary>The read held now, or null when the part holds none.</summary>
        public StoreHealthRead<T>? HeldRead
        {
            get
            {
                lock (_partGate)
                {
                    return _held;
                }
            }
        }

        /// <summary>The read running now, or a completed task when none is.</summary>
        public Task Running
        {
            get
            {
                lock (_partGate)
                {
                    return (Task?)_inFlight ?? Task.CompletedTask;
                }
            }
        }

        public Task<StoreHealthRead<T>> GetAsync(
            Func<Task<T>> read, Func<DateTime> clock, Action<Exception> reportRefreshFailure, CancellationToken cancellationToken)
        {
            Task<StoreHealthRead<T>> shared;
            lock (_partGate)
            {
                var now = clock();
                if (_held is { } held)
                {
                    /* A held part is returned at once, old or not. Past the lifetime, one refresh starts behind it, and not
                       another until a lifetime after that one started (a failed refresh included). */
                    if (now - held.ReadAtUtc >= Lifetime
                        && _inFlight is null
                        && now - _lastRefreshStartUtc >= Lifetime)
                    {
                        _lastRefreshStartUtc = now;
                        Start(read, clock, held, reportRefreshFailure);
                    }

                    return Task.FromResult(held);
                }

                shared = _inFlight ?? Start(read, clock, keep: null, reportRefreshFailure);
            }

            /* One caller's own token only ends its wait, never the shared read the others wait on. */
            return shared.WaitAsync(cancellationToken);
        }

        /* Called under _partGate. Task.Run, not an inline call: the read cannot finish (or throw before its first await) before the
           assignment below, and its finally takes the same lock, so it can only run after it. */
        private Task<StoreHealthRead<T>> Start(
            Func<Task<T>> read, Func<DateTime> clock, StoreHealthRead<T>? keep, Action<Exception> reportRefreshFailure)
        {
            Task<StoreHealthRead<T>>? started = null;
            started = Task.Run(async () =>
            {
                StoreHealthRead<T>? result = null;
                try
                {
                    result = new StoreHealthRead<T>(await read().ConfigureAwait(false), clock());
                    return result;
                }
#pragma warning disable CA1031 // A refresh behind a held part ends here: the held part is kept and the failure is reported once.
                catch (Exception ex) when (keep is not null)
#pragma warning restore CA1031
                {
                    reportRefreshFailure(ex);
                    return keep;
                }
                finally
                {
                    lock (_partGate)
                    {
                        if (ReferenceEquals(_inFlight, started))
                        {
                            _inFlight = null;
                        }

                        if (result is not null)
                        {
                            _held = result;
                        }
                    }
                }
            });
            _inFlight = started;
            return started;
        }
    }
}
