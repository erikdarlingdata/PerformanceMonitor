/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One cached watermark entry for a (server, collector) pair (#4197 part b).
///
/// Carries the plain timestamp watermark, the numeric (bigint) twin job_history uses, and the #3778
/// UTC-twin frame flag together, because <see cref="FromUtcColumn"/> is not a derived fact recomputable
/// from <see cref="Value"/> alone — it decides which column a definition's dedup compares against on a
/// given run, so caching the value without its frame would let a later run compare a UTC-stamped value
/// against a local-stamped one, exactly the cross-frame comparison the frame flag exists to prevent.
/// </summary>
internal readonly record struct ServerWatermarkEntry(
    DateTime? Value,
    bool FromUtcColumn,
    long? NumericValue);

/// <summary>
/// In-memory cache of each server's per-collector watermark, keyed the same way as
/// <see cref="DarlingCollectorRunner"/>'s other per-server-and-collector dictionaries (#4197 part b).
///
/// <para><b>Seed:</b> the first read for a (server, collector) pair, or the first after
/// <see cref="InvalidateServer"/>/<see cref="Invalidate"/>, still goes to the store via the runner's
/// existing <c>GetLastCollectedTimeAsync</c>/<c>GetLastCollectedTimeWithFrameAsync</c>/
/// <c>GetLastCollectedInstanceIdAsync</c> methods — this type holds the answer, it does not read it.</para>
///
/// <para><b>Advance:</b> after a batch COMMITS, the caller computes the max watermark value over the rows
/// that batch actually wrote and calls <see cref="Advance"/>, which keeps the GREATER of that and the
/// cached value. A batch that writes zero rows must not call <see cref="Advance"/> at all — the cache is
/// then unchanged, which is the same as calling it with the old value.</para>
///
/// <para><b>Invalidate:</b> any fault in a run (collection, dedup, write, or cancel) drops the entry so the
/// next run re-seeds from the store, because a watermark that advances past uncommitted rows skips events
/// (data loss), while one that merely lags only re-collects duplicates — the correct failure direction.</para>
/// </summary>
internal sealed class ServerWatermarkCache
{
    private readonly ConcurrentDictionary<(int ServerId, string Collector), ServerWatermarkEntry> _entries = new();

    /// <summary>Returns the cached entry, or <c>null</c> on a cache miss (never seeded, or invalidated).</summary>
    public ServerWatermarkEntry? TryGet(int serverId, string collector)
        => _entries.TryGetValue((serverId, collector), out var entry) ? entry : null;

    /// <summary>
    /// Seeds the cache with a store-read value. Overwrites any existing entry unconditionally — callers
    /// only seed after a miss, so there is nothing to compare against.
    /// </summary>
    public void Seed(int serverId, string collector, DateTime? value, bool fromUtcColumn, long? numericValue)
        => _entries[(serverId, collector)] = new ServerWatermarkEntry(value, fromUtcColumn, numericValue);

    /// <summary>
    /// Advances the cached watermark to the greater of the batch's own max value and whatever is already
    /// cached. Never moves the value backwards — that only happens through <see cref="Invalidate"/> plus a
    /// reseed from the store's true MAX. A miss (nothing cached yet) seeds with the batch's value directly,
    /// so a run that only ever advances (never explicitly seeds first) still ends up with the right value.
    /// </summary>
    public void Advance(int serverId, string collector, DateTime? batchMaxValue, bool fromUtcColumn, long? batchMaxNumericValue)
    {
        var key = (serverId, collector);

        _entries.AddOrUpdate(
            key,
            addValueFactory: static (_, seed) => seed,
            updateValueFactory: static (_, existing, seed) =>
            {
                var value = Greater(existing.Value, seed.Value);
                var numeric = Greater(existing.NumericValue, seed.NumericValue);

                /* The frame flag belongs to whichever value is now newer; on a tie (or when the batch
                   contributed nothing new) the existing frame stands, since re-adopting the incoming
                   frame on an unchanged value would be free to flip without cause. */
                var fromUtc = seed.Value.HasValue && seed.Value == value ? seed.FromUtcColumn : existing.FromUtcColumn;

                return new ServerWatermarkEntry(value, fromUtc, numeric);
            },
            factoryArgument: new ServerWatermarkEntry(batchMaxValue, fromUtcColumn, batchMaxNumericValue));
    }

    /// <summary>Drops one (server, collector) entry — a fault in that run, so the next call re-seeds.</summary>
    public void Invalidate(int serverId, string collector)
        => _entries.TryRemove((serverId, collector), out _);

    /// <summary>
    /// Drops every cached entry for a server — a reconnect (mirrors <see cref="DarlingCollectorRunner"/>'s
    /// existing <c>OnServerReconnected</c> for the Azure master-access verdict) or a re-add under the same
    /// <c>server_id</c>, where the store's real MAX may now sit anywhere relative to whatever was cached.
    /// </summary>
    public void InvalidateServer(int serverId)
    {
        foreach (var key in _entries.Keys)
        {
            if (key.ServerId == serverId)
            {
                _entries.TryRemove(key, out _);
            }
        }
    }

    private static DateTime? Greater(DateTime? left, DateTime? right)
    {
        if (left is null)
        {
            return right;
        }

        if (right is null)
        {
            return left;
        }

        return left.Value >= right.Value ? left : right;
    }

    private static long? Greater(long? left, long? right)
    {
        if (left is null)
        {
            return right;
        }

        if (right is null)
        {
            return left;
        }

        return left.Value >= right.Value ? left : right;
    }
}
