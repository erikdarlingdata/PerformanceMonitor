/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The AWS answers the four RDS readers of one runner share for a short time (#9522): a cluster's writer, an
/// instance's log file listing, one log file portion, and an instance's <c>DbiResourceId</c>.
///
/// <para><b>What it removes.</b> On a cluster endpoint, a run of <c>pg_deadlocks</c>, <c>pg_log_events</c> or
/// <c>pg_plan_capture</c> resolves the writer, lists the log files and downloads the new bytes, and a run of
/// <c>pg_cpu_utilization</c> resolves the writer and the resource id. The four collectors of one cycle ask the
/// same questions of the same instance seconds apart. With one cache between them the first asks and the others
/// read its answer.</para>
///
/// <para><b>A shared download is the same request, answered once.</b> A portion is kept under the file, the marker
/// and the line count the request carried. A reader whose own position is that marker gets the kept answer. A reader
/// at any other position asks AWS for its own portion. Each <see cref="RdsLogSource"/> still holds its own position
/// and moves it only after its own rows are stored, so no reader can consume a window for another, which is why the
/// positions are separate. A reader that fails to store its rows leaves its position where it was and reads that
/// window again later, on its own request.</para>
///
/// <para><b>Failover.</b> A writer is kept for <see cref="WriterLifetime"/>, and the callers drop it at once when a
/// call addressed to it fails (<see cref="ForgetWriter"/>), so the run after a failed one resolves the writer
/// again. That is one failed run at most, the same as a reader that asks every run. The lifetime is short because
/// a failover can leave the old writer up as a reader, where every call on it still succeeds and nothing fails to
/// say the writer moved. The readers then follow the new writer within the lifetime.</para>
///
/// <para><b>One call for the callers that arrive together.</b> An entry holds the call in flight, so two readers
/// that ask at the same moment wait on one AWS call. A call that fails or is cancelled is not kept: a reader that
/// was waiting on it makes its own call, so one reader's cancelled cycle does not fail another.</para>
///
/// <para>An answer is keyed by the server's AWS role as well as the region and the id, so a server that reads under
/// one role never gets an answer fetched under another.</para>
/// </summary>
public sealed class RdsLookupCache
{
    /// <summary>
    /// How long a resolved writer, a log file listing and a log file portion are kept. Long enough for the
    /// collectors of one cycle, which run within seconds of each other, to share them. It is half the shortest
    /// interval a collector can be scheduled at (one minute), so a reader's next run never gets the answer its own
    /// last run fetched, where the file has grown since. And a failover no call fails on is followed within it.
    /// </summary>
    public static readonly TimeSpan WriterLifetime = TimeSpan.FromSeconds(30);

    /// <summary>The same lifetime, for a listing: a rotation is seen by the next cycle.</summary>
    public static readonly TimeSpan ListingLifetime = WriterLifetime;

    /// <summary>
    /// The same lifetime, for a downloaded portion. One reader makes at most
    /// <see cref="RdsLogSource.MaxPassesPerCycle"/> reads of a file in a cycle, and AWS returns at most 1 MB a read,
    /// so that is the most text held for a file at once.
    /// </summary>
    public static readonly TimeSpan PortionLifetime = WriterLifetime;

    /// <summary>
    /// How long an instance's <c>DbiResourceId</c> is kept. AWS assigns it when the instance is created and never
    /// changes it. An hour bounds the one case that does change it, an instance deleted and created again under the
    /// same identifier, when no call fails first.
    /// </summary>
    public static readonly TimeSpan ResourceIdLifetime = TimeSpan.FromHours(1);

    private enum Slot
    {
        Writer,
        Listing,
        Portion,
        ResourceId,
    }

    private readonly record struct Key(Slot Slot, AwsRoleKey? Role, string Region, string Id);

    private sealed record Entry(Task Call, DateTime ExpiresUtc);

    private readonly object _lock = new();

    private readonly Dictionary<Key, Entry> _entries = new();

    private readonly Func<DateTime> _clock;

    private readonly Func<TimeSpan, Task> _wait;

    /// <param name="clock">The time an answer's lifetime is measured on. Null is the wall clock.</param>
    /// <param name="wait">How the cache waits out a lifetime before it drops what has ended. Null is
    /// <see cref="Task.Delay(TimeSpan)"/>. A test passes its own so the drop happens when the test says.</param>
    public RdsLookupCache(Func<DateTime>? clock = null, Func<TimeSpan, Task>? wait = null)
    {
        _clock = clock ?? (() => DateTime.UtcNow);
        _wait = wait ?? Task.Delay;
    }

    /// <summary>How many answers are held, ended or not.</summary>
    internal int HeldCount
    {
        get
        {
            lock (_lock)
            {
                return _entries.Count;
            }
        }
    }

    /// <summary>The writer of <paramref name="clusterId"/>, from <paramref name="resolve"/> when none is held.</summary>
    public Task<string> WriterAsync(
        AwsRoleKey? role, string region, string clusterId, Func<Task<string>> resolve, CancellationToken cancellationToken)
        => GetAsync(new Key(Slot.Writer, role, region, clusterId), WriterLifetime, resolve, cancellationToken);

    /// <summary>The <c>DbiResourceId</c> of <paramref name="instanceId"/>, from <paramref name="resolve"/> when none is held.</summary>
    public Task<string> ResourceIdAsync(
        AwsRoleKey? role, string region, string instanceId, Func<Task<string>> resolve, CancellationToken cancellationToken)
        => GetAsync(new Key(Slot.ResourceId, role, region, instanceId), ResourceIdLifetime, resolve, cancellationToken);

    /// <summary>The whole log file listing of <paramref name="instanceId"/>, from <paramref name="list"/> when none is held.</summary>
    public Task<T> ListingAsync<T>(
        AwsRoleKey? role, string region, string instanceId, Func<Task<T>> list, CancellationToken cancellationToken)
        => GetAsync(new Key(Slot.Listing, role, region, instanceId), ListingLifetime, list, cancellationToken);

    /// <summary>
    /// One portion of <paramref name="fileName"/> on <paramref name="instanceId"/>. <paramref name="request"/> is the
    /// marker and line count asked for: a held portion answers only the request that fetched it, and any other
    /// request for the file calls <paramref name="download"/> and is held beside it. So a second reader that starts
    /// where the first started follows every read the first made, each from the held answer.
    /// </summary>
    public Task<T> PortionAsync<T>(
        AwsRoleKey? role, string region, string instanceId, string fileName, string request, Func<Task<T>> download,
        CancellationToken cancellationToken)
        => GetAsync(
            new Key(Slot.Portion, role, region, instanceId + "|" + fileName + "|" + request), PortionLifetime, download,
            cancellationToken);

    /// <summary>Drops the writer held for <paramref name="clusterId"/>: a call addressed to it failed.</summary>
    public void ForgetWriter(AwsRoleKey? role, string region, string clusterId)
    {
        lock (_lock)
        {
            _entries.Remove(new Key(Slot.Writer, role, region, clusterId));
        }
    }

    /// <summary>
    /// Drops everything held for <paramref name="instanceId"/> (its listing, its portions and its resource id): a
    /// call addressed to it failed, so the next run asks AWS again.
    /// </summary>
    public void ForgetInstance(AwsRoleKey? role, string region, string instanceId)
    {
        var portionPrefix = instanceId + "|";

        lock (_lock)
        {
            var stale = _entries.Keys
                .Where(k => k.Role == role
                    && string.Equals(k.Region, region, StringComparison.Ordinal)
                    && k.Slot switch
                    {
                        Slot.Listing or Slot.ResourceId => string.Equals(k.Id, instanceId, StringComparison.Ordinal),
                        Slot.Portion => k.Id.StartsWith(portionPrefix, StringComparison.Ordinal),
                        _ => false,
                    })
                .ToList();

            foreach (var key in stale)
            {
                _entries.Remove(key);
            }
        }
    }

    /// <summary>
    /// Drops the answers whose lifetime has ended, once <paramref name="lifetime"/> has passed. A held portion is up
    /// to 1 MB of log text, and the next request that would replace it can be a whole collector interval away, so
    /// an answer is not left to wait for one. Whether an answer has ended is read from the cache's clock, as it is
    /// on every request, so this removes only what no request would be given.
    /// </summary>
    private async Task DropEndedAfterAsync(TimeSpan lifetime)
    {
        await _wait(lifetime).ConfigureAwait(false);

        var now = _clock();

        lock (_lock)
        {
            foreach (var ended in _entries.Where(e => e.Value.ExpiresUtc <= now).Select(e => e.Key).ToList())
            {
                _entries.Remove(ended);
            }
        }
    }

    private async Task<T> GetAsync<T>(
        Key key, TimeSpan lifetime, Func<Task<T>> fetch, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(fetch);

        var now = _clock();
        Task<T>? shared = null;
        TaskCompletionSource<T>? mine = null;

        /* Nothing awaits or calls out while the lock is held: a caller that finds no answer leaves a pending task
           in the table and runs its call after the lock is released. */
        lock (_lock)
        {
            if (_entries.TryGetValue(key, out var held)
                && held.ExpiresUtc > now
                && held.Call is Task<T> call
                && !call.IsCanceled)
            {
                shared = call;
            }
            else
            {
                foreach (var expired in _entries.Where(e => e.Value.ExpiresUtc <= now).Select(e => e.Key).ToList())
                {
                    _entries.Remove(expired);
                }

                mine = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);
                _entries[key] = new Entry(mine.Task, now + lifetime);
            }
        }

        if (mine is not null)
        {
            _ = DropEndedAfterAsync(lifetime);
        }

        if (shared is not null)
        {
            try
            {
                return await shared.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                /* The call this one was waiting on failed or was cancelled, and this caller was not. It makes its
                   own call, as a reader with no cache does. */
                return await fetch();
            }
        }

        try
        {
            var value = await fetch();
            mine!.SetResult(value);
            return value;
        }
        catch
        {
            /* A failed call is not an answer. The entry goes, and the waiters see a cancelled task rather than this
               caller's exception, so each makes its own call. */
            lock (_lock)
            {
                if (_entries.TryGetValue(key, out var current) && ReferenceEquals(current.Call, mine!.Task))
                {
                    _entries.Remove(key);
                }
            }

            mine!.TrySetCanceled(CancellationToken.None);
            throw;
        }
    }
}
