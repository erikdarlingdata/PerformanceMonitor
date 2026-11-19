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
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service.Metrics;

/// <summary>
/// #9013: the one snapshot every /metrics scrape reads. A scrape never reads the store: it returns what is cached and,
/// when that is <see cref="RefreshAge"/> old (or there is none yet), starts ONE background refresh and returns at once.
///
/// <para><b>Single flight.</b> The in-flight task is claimed with <see cref="Interlocked.CompareExchange{T}"/>, so
/// any number of concurrent scrapes start at most one refresh, and a refresh that hangs holds the slot until its
/// token (<c>stopping</c> or <see cref="RefreshTimeout"/>) ends it: a hung store never piles up refreshes.</para>
///
/// <para><b>State.</b> The snapshot and the last refresh's outcome live in one immutable record behind a volatile
/// field. Only the refresh that holds the slot writes it, so a scrape always sees a consistent pair.</para>
/// </summary>
internal sealed class DarlingMetricsSnapshotCache
{
    internal static readonly TimeSpan RefreshAge = TimeSpan.FromSeconds(55), DropAge = TimeSpan.FromMinutes(5), RefreshTimeout = TimeSpan.FromSeconds(30);

    private readonly Func<CancellationToken, Task<MetricsSnapshot>> _refresh;
    private readonly Func<bool> _canRefresh;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly CancellationToken _stopping;

    private volatile CacheState _state = new(null, LastRefreshSucceeded: false, LastRefreshSeconds: null, Failing: false);
    private Task? _inFlight;

    public DarlingMetricsSnapshotCache(
        Func<CancellationToken, Task<MetricsSnapshot>> refresh,
        Func<bool> canRefresh,
        TimeProvider time,
        ILogger logger,
        CancellationToken stopping)
    {
        _refresh = refresh ?? throw new ArgumentNullException(nameof(refresh));
        _canRefresh = canRefresh ?? throw new ArgumentNullException(nameof(canRefresh));
        _time = time ?? throw new ArgumentNullException(nameof(time));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        _stopping = stopping;
    }

    /// <summary>The running refresh, or null. Completes after the refresh's outcome is visible to <see cref="Scrape"/>.</summary>
    internal Task? InFlight => Volatile.Read(ref _inFlight);

    /// <summary>The cached snapshot and the last refresh's outcome. Never blocks and never awaits.</summary>
    internal MetricsScrape Scrape()
    {
        var state = _state;
        var now = _time.GetUtcNow().UtcDateTime;

        if (IsDue(state.Snapshot, now) && Volatile.Read(ref _inFlight) is null && _canRefresh())
        {
            TryStartRefresh();
        }

        var serveServers = state.Snapshot is not null && now - state.Snapshot.BuiltAtUtc < DropAge;
        return new MetricsScrape(state.Snapshot, serveServers, state.LastRefreshSucceeded, state.LastRefreshSeconds);
    }

    private static bool IsDue(MetricsSnapshot? snapshot, DateTime now) =>
        snapshot is null || now - snapshot.BuiltAtUtc >= RefreshAge;

    private void TryStartRefresh()
    {
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (Interlocked.CompareExchange(ref _inFlight, done.Task, null) is not null)
        {
            return;
        }

        /* #9013: re-read after claiming the slot. A scrape that read the old state can reach the claim just after a
           refresh finished (it writes the state BEFORE it frees the slot), and must not start a second one. */
        if (!IsDue(_state.Snapshot, _time.GetUtcNow().UtcDateTime))
        {
            Release(done);
            return;
        }

        _ = Task.Run(() => RefreshAsync(done));
    }

    private async Task RefreshAsync(TaskCompletionSource done)
    {
        var started = _time.GetTimestamp();
        try
        {
            /* #9013: the timeout runs on the injected clock (R2), so a test's clock drives it as the host's real one does. */
            using var timeout = new CancellationTokenSource(RefreshTimeout, _time);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(_stopping, timeout.Token);
            try
            {
                var snapshot = await _refresh(linked.Token).ConfigureAwait(false);
                var seconds = _time.GetElapsedTime(started).TotalSeconds;
                var wasFailing = _state.Failing;
                _state = new CacheState(snapshot, LastRefreshSucceeded: true, seconds, Failing: false);
                if (wasFailing)
                {
                    _logger.LogInformation("Metrics refresh recovered; the /metrics snapshot is current again.");
                }
            }
            catch (OperationCanceledException) when (_stopping.IsCancellationRequested)
            {
                /* #9013: the service is stopping. Not a failure, so nothing is logged and the state stays as it was. */
            }
            catch (Exception ex)
            {
                var seconds = _time.GetElapsedTime(started).TotalSeconds;
                var previous = _state;
                _state = previous with { LastRefreshSucceeded = false, LastRefreshSeconds = seconds, Failing = true };
                if (!previous.Failing)
                {
                    /* #9013: the type and message only, never the exception object: one Warning per failure streak. */
                    var reason = timeout.IsCancellationRequested
                        ? $"timed out after {RefreshTimeout.TotalSeconds:F0} s"
                        : $"{ex.GetType().Name}: {ex.Message}";
                    _logger.LogWarning(
                        "Metrics refresh failed ({Reason}); /metrics keeps serving the last snapshot and drops per-server series once it is {DropMinutes:F0} minutes old. Further failures are not logged until a refresh succeeds.",
                        reason,
                        DropAge.TotalMinutes);
                }
            }
        }
        finally
        {
            Release(done);
        }
    }

    /* The slot is freed before the task completes, so whoever awaits InFlight then sees no refresh running. */
    private void Release(TaskCompletionSource done)
    {
        Volatile.Write(ref _inFlight, null);
        done.TrySetResult();
    }

    private sealed record CacheState(MetricsSnapshot? Snapshot, bool LastRefreshSucceeded, double? LastRefreshSeconds, bool Failing);
}
