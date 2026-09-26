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

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The 5-minute shared cache behind <c>get_store_host</c> (#4214 round-1 review, Medium 2): the store/settings
/// facts <see cref="DarlingStoreHostProfile.GatherAsync"/> reads are a live <c>pg_settings</c> query plus a
/// <c>pg_database_size</c> read whose cost scales with the store (measured 3,177 ms on a 225 GiB store — see
/// <c>SerialLoopStoreSizeSourceTests</c>, #3199), so a burst of MCP calls should cost one live read, not one
/// per call.
///
/// <para>One immutable <see cref="CacheEntry"/> behind a single <c>volatile</c> reference field, not a bare
/// tuple field read without a lock: a tuple field is not read/written atomically and can tear under
/// concurrent access, where a single reference assignment is atomic and <c>volatile</c> gives the needed
/// cross-thread visibility for the lock-free fast path (a fresh hit never touches the gate).</para>
///
/// <para>A failed gather is never cached: <see cref="GetOrGatherAsync"/> only assigns <see cref="_entry"/>
/// after <paramref name="gather"/> in <see cref="GetOrGatherAsync"/>'s own call returns successfully, and its
/// <c>finally</c> always releases the gate, so a thrown exception leaves the next call free to try again
/// rather than stuck behind a faulted cache.</para>
/// </summary>
public sealed class StoreHostProfileCache : IDisposable
{
    /// <summary>The production singleton, registered once per host start (<c>DarlingMcpHostService.cs</c>,
    /// <c>DarlingWebEndpoints.cs</c>'s direct-call dispatch) so every caller shares the same 5-minute window.
    /// A test builds its own instance instead, with an injected clock.</summary>
    public static readonly StoreHostProfileCache Shared = new(TimeSpan.FromMinutes(5));

    private sealed record CacheEntry(HostProfile Profile, DateTime GatheredAtUtc);

    private readonly TimeSpan _ttl;
    private readonly Func<DateTime> _utcNow;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private volatile CacheEntry? _entry;

    /// <summary>Public, not internal (unlike the rest of this file's neighbors, which favor <c>internal</c>):
    /// <c>GetStoreHost</c> is a public <c>[McpServerTool]</c> method — the SDK's own convention, matched by
    /// every other DI-service-typed parameter it takes (<c>PostgresConfig</c>, <c>DarlingAnalysisService</c>)
    /// — and a parameter type may never be less accessible than the method it appears on (CS0051).</summary>
    public StoreHostProfileCache(TimeSpan ttl, Func<DateTime>? utcNow = null)
    {
        _ttl = ttl;
        _utcNow = utcNow ?? (() => DateTime.UtcNow);
    }

    /// <summary>Returns the cached profile when it is still fresh; otherwise calls <paramref name="gather"/>
    /// exactly once, even under concurrent callers, and caches the result. The connection <paramref
    /// name="gather"/> needs is the caller's to open, INSIDE the delegate — a cache hit never calls
    /// <paramref name="gather"/> at all, so a hit opens no connection.
    ///
    /// <para>Internal, not public (unlike the class itself and its constructor): <see cref="HostProfile"/> is
    /// internal, so this method's signature can be no more accessible than that without CS0051. Every real
    /// caller (<c>GetStoreHost</c>) and every test lives in this same assembly or
    /// <c>Darling.Tests</c> (<c>InternalsVisibleTo</c>), so internal is not a reach restriction in
    /// practice.</para></summary>
    internal async Task<(HostProfile Profile, DateTime GatheredAtUtc)> GetOrGatherAsync(
        Func<CancellationToken, Task<HostProfile>> gather, CancellationToken cancellationToken)
    {
        if (TryGetFresh() is { } fresh)
        {
            return (fresh.Profile, fresh.GatheredAtUtc);
        }

        /* #4203: an explicit check ahead of the gate, not left to SemaphoreSlim.WaitAsync's own cancellation
           path — the semaphore throws TaskCanceledException, a subclass callers checking the exact
           OperationCanceledException type (WebReadCancellationPinTests' ratchet) would not match, where
           ThrowIfCancellationRequested throws the base type directly. */
        cancellationToken.ThrowIfCancellationRequested();

        await _gate.WaitAsync(cancellationToken);
        try
        {
            /* Re-check after winning the gate: a caller that lost the race to whoever gathered while this
               call waited should reuse that result rather than gathering a second time. */
            if (TryGetFresh() is { } freshAfterWait)
            {
                return (freshAfterWait.Profile, freshAfterWait.GatheredAtUtc);
            }

            var profile = await gather(cancellationToken);
            var entry = new CacheEntry(profile, _utcNow());
            _entry = entry;
            return (entry.Profile, entry.GatheredAtUtc);
        }
        finally
        {
            _gate.Release();
        }
    }

    private CacheEntry? TryGetFresh()
    {
        var entry = _entry;
        return entry is not null && _utcNow() - entry.GatheredAtUtc < _ttl ? entry : null;
    }

    /// <summary>CA1001 (owns the disposable <see cref="_gate"/>) — mirrors <c>DarlingWebOidcClient</c>'s own
    /// gate disposal. Safe for <see cref="Shared"/> despite two hosts (<c>DarlingMcpHostService</c>,
    /// <c>DarlingWebEndpoints.cs</c>'s direct-call dispatch) both holding a reference to it: both register it
    /// with the DI INSTANCE overload (<c>AddSingleton&lt;StoreHostProfileCache&gt;(Shared)</c>), and the
    /// built-in container never disposes an instance it did not itself construct — so no container shutdown
    /// disposes <see cref="Shared"/> out from under the other host.</summary>
    public void Dispose() => _gate.Dispose();
}
