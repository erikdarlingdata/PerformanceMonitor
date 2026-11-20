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

    private volatile CacheState _state = new(null, LastRefreshSucceeded: false, LastRefreshSeconds: null, Failing: false, LastAttemptStartedUtc: null);
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

        if (IsDue(state, now) && Volatile.Read(ref _inFlight) is null && _canRefresh())
        {
            TryStartRefresh();
        }

        var serveServers = state.Snapshot is not null && !IsAtLeast(state.Snapshot.BuiltAtUtc, now, DropAge);
        return new MetricsScrape(state.Snapshot, serveServers, state.LastRefreshSucceeded, state.LastRefreshSeconds);
    }

    /* #9013: two gates. The snapshot must be stale, AND the previous attempt must have STARTED at least RefreshAge ago:
       after a failure the old snapshot stays stale, and without the second gate every scrape would start a refresh
       against a failing store. So it is at most one attempt a minute while scraped, failing or not. */
    private static bool IsDue(CacheState state, DateTime now) =>
        (state.Snapshot is null || IsAtLeast(state.Snapshot.BuiltAtUtc, now, RefreshAge))
        && (state.LastAttemptStartedUtc is null || IsAtLeast(state.LastAttemptStartedUtc.Value, now, RefreshAge));

    /* #9013: a stamp ahead of the clock (the wall clock stepped back since it was written) counts as old enough, not as
       fresh. Otherwise a 10-minute step back would freeze the snapshot, served as current, for 10 minutes plus a window.
       #4732's LastFiredStamp.Settle does not fit: it holds one more window from the step, and a scrape wants the
       refresh now. */
    private static bool IsAtLeast(DateTime stampUtc, DateTime now, TimeSpan age) => stampUtc > now || now - stampUtc >= age;

    private void TryStartRefresh()
    {
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (Interlocked.CompareExchange(ref _inFlight, done.Task, null) is not null)
        {
            return;
        }

        /* #9013: re-read after claiming the slot. A scrape that read the old state can reach the claim just after a
           refresh finished (it writes the state BEFORE it frees the slot), and must not start a second one. */
        var state = _state;
        var now = _time.GetUtcNow().UtcDateTime;
        if (!IsDue(state, now))
        {
            Release(done);
            return;
        }

        /* #9013: the attempt time is stamped only here, by the slot holder (the only writer) after the re-check passed,
           so a claim that loses the re-check never moves it. */
        _state = state with { LastAttemptStartedUtc = now };

        _ = Task.Run(() => RefreshAsync(done));
    }

    private async Task RefreshAsync(TaskCompletionSource done)
    {
        var started = _time.GetTimestamp();
        try
        {
            /* #9013: the timeout runs on the injected TimeProvider, as the snapshot ages do, so a test's clock drives it as the host's
               real one does. */
            using var timeout = new CancellationTokenSource(RefreshTimeout, _time);
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(_stopping, timeout.Token);
            try
            {
                var snapshot = await _refresh(linked.Token).ConfigureAwait(false);
                var seconds = _time.GetElapsedTime(started).TotalSeconds;
                var previous = _state;
                _state = previous with { Snapshot = snapshot, LastRefreshSucceeded = true, LastRefreshSeconds = seconds, Failing = false };
                if (previous.Failing)
                {
                    _logger.LogInformation("Metrics refresh recovered; the /metrics snapshot is current again.");
                }
            }
            catch (Exception ex) when (!_stopping.IsCancellationRequested)
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
            catch (Exception) when (_stopping.IsCancellationRequested)
            {
                /* #9013: the service is stopping, whatever the delegate threw on the way out. Not a failure, so nothing
                   is logged and the outcome stays as it was. */
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

    private sealed record CacheState(
        MetricsSnapshot? Snapshot, bool LastRefreshSucceeded, double? LastRefreshSeconds, bool Failing, DateTime? LastAttemptStartedUtc);
}
