/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The background loop that re-groups the perfmon_stats chunks compressed by <c>server_id</c> alone (#5574), one chunk at
/// a time, newest first, inside the 30-day reach (<see cref="TimescaleSupport.PerfmonRegroupReachDays"/>).
///
/// <para><b>Why a loop of its own, not a step of the hourly pass.</b> The first version re-grouped for at most two minutes
/// of each hourly pass. A large store's chunk (13.1 M rows measured, about 14 M per day on the biggest) takes minutes
/// to rewrite, so two minutes an hour reached one chunk an hour and the 30-day reach took more than a day of hourly
/// passes. The hourly pass now only calls <see cref="StartIfIdle"/>, which does no database work and returns at once; the
/// rewrite runs here, on its own connection, so the pass (and the collection sweep that awaits it) never waits for a chunk.</para>
///
/// <para><b>One chunk, then a pause as long as the chunk took</b> (never under <see cref="TimescaleSupport.PerfmonRegroupMinPause"/>),
/// so the store spends at most half its time on the rewrite. Thirty chunks of about 3 minutes finish in about three hours,
/// which is "hours, not days" without loading the store: the reads and the collectors share the disk with it.</para>
///
/// <para><b>It stops by itself.</b> When no chunk is left it ends after two catalog reads (Debug); the next hourly pass
/// starts it again for the price of those two reads on a new connection. It also ends on three failed chunks in a row
/// (<see cref="TimescaleSupport.PerfmonRegroupFailureLimit"/>), and when every chunk left was skipped for a busy lock this
/// run (a skipped chunk is not tried again until the next hourly start, so one busy chunk cannot pin the loop). It stops
/// within a statement of the service stopping: the chunk in progress is cancelled and rolls back, or, when only the compress
/// half was cancelled, is left uncompressed for the compression policy to compress with the new grouping.</para>
///
/// <para>Gated exactly as the first version was: <see cref="TimescaleSupport.PerfmonRegroupBlockedReason"/> is asked
/// before every chunk (the hypertable must already carry the new grouping, TimescaleDB must be able to change settings
/// with compressed chunks, and the per-chunk settings view must exist), and the caller starts it only on a store with
/// TimescaleDB and only on the hourly pass, never on the start path.</para>
/// </summary>
public sealed class PerfmonRegroupDrain
{
    private readonly Func<CancellationToken, ValueTask<NpgsqlConnection>> _openConnection;
    private readonly ILogger? _logger;
    private readonly int _reachDays;
    private readonly TimeSpan _minPause;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly object _gate = new();
    private Task? _running;

    /// <param name="openConnection">Opens a store connection; the drain owns and disposes what it gets, one per run.</param>
    /// <param name="logger">Where the per-chunk and per-run lines go.</param>
    public PerfmonRegroupDrain(Func<CancellationToken, ValueTask<NpgsqlConnection>> openConnection, ILogger? logger)
        : this(openConnection, logger, TimescaleSupport.PerfmonRegroupReachDays, TimescaleSupport.PerfmonRegroupMinPause, Task.Delay)
    {
    }

    /// <summary>The test seam: the reach, the pause floor and the wait itself (a test records the pauses it is asked for
    /// instead of sleeping them).</summary>
    internal PerfmonRegroupDrain(
        Func<CancellationToken, ValueTask<NpgsqlConnection>> openConnection,
        ILogger? logger,
        int reachDays,
        TimeSpan minPause,
        Func<TimeSpan, CancellationToken, Task> delay)
    {
        _openConnection = openConnection ?? throw new ArgumentNullException(nameof(openConnection));
        _logger = logger;
        _reachDays = reachDays;
        _minPause = minPause;
        _delay = delay ?? throw new ArgumentNullException(nameof(delay));
    }

    /// <summary>The run in flight (or the last one), so shutdown can wait for it. It never faults.</summary>
    public Task? Completion
    {
        get
        {
            lock (_gate)
            {
                return _running;
            }
        }
    }

    /// <summary>
    /// Starts a run unless one is in flight; returns whether it started one. No database work happens on the caller's
    /// thread: the first thing the run does is open its own connection and read the catalog, so the hourly pass pays
    /// nothing for a converged store.
    /// </summary>
    public bool StartIfIdle(CancellationToken stoppingToken)
    {
        lock (_gate)
        {
            if (_running is { IsCompleted: false })
            {
                return false;
            }

            _running = Task.Run(() => RunGuardedAsync(stoppingToken), CancellationToken.None);
            return true;
        }
    }

    private async Task RunGuardedAsync(CancellationToken stoppingToken)
    {
        try
        {
            await RunAsync(stoppingToken);
        }
        catch (OperationCanceledException)
        {
            /* The service is stopping; the chunk in flight rolled back or was left for the compression policy. */
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                "TimescaleDB: the perfmon_stats re-group stopped on an error and starts again at the next hourly check (#5574): {Message}",
                ex.Message);
        }
    }

    /// <summary>
    /// One run: re-groups chunks, newest first, until none is left, the failure limit is reached, every chunk left was
    /// skipped, or <paramref name="cancellationToken"/> is cancelled (which throws). Returns the chunks re-grouped.
    /// </summary>
    internal async Task<int> RunAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _openConnection(cancellationToken);
        var skipped = new HashSet<string>(StringComparer.Ordinal);
        var run = Stopwatch.StartNew();
        var done = 0;
        var failuresInARow = 0;
        var left = 0;
        var owedPause = TimeSpan.Zero;
        var started = false;

        while (true)
        {
            var candidates = await TimescaleSupport.ReadPerfmonRegroupCandidatesAsync(connection, _logger, _reachDays, cancellationToken);
            var next = Next(candidates, skipped);
            if (next is null)
            {
                left = candidates?.Count ?? -1;
                break;
            }

            if (owedPause > TimeSpan.Zero)
            {
                /* The wait is spent only when there is more to do (nothing waits after the last chunk), and the catalog
                   is read again after it: the policy or a restart may have re-grouped the chunk meanwhile. */
                await _delay(owedPause, cancellationToken);
                owedPause = TimeSpan.Zero;
                candidates = await TimescaleSupport.ReadPerfmonRegroupCandidatesAsync(connection, _logger, _reachDays, cancellationToken);
                next = Next(candidates, skipped);
                if (next is null)
                {
                    left = candidates?.Count ?? -1;
                    break;
                }
            }

            if (!started)
            {
                started = true;
                _logger?.LogInformation(
                    "TimescaleDB: perfmon_stats re-group started: {Chunks} chunk(s) inside the {Days}-day reach have the old grouping; they are re-grouped one at a time, newest first, each followed by a pause as long as it took (at least {MinPauseSeconds} s) (#5574)",
                    candidates!.Count, _reachDays, (int)_minPause.TotalSeconds);
            }

            var outcome = await TimescaleSupport.RegroupPerfmonChunkAsync(connection, _logger, next.Value, cancellationToken);
            switch (outcome.Result)
            {
                case TimescaleSupport.PerfmonRegroupChunkResult.Regrouped:
                    done++;
                    failuresInARow = 0;
                    owedPause = Max(outcome.Elapsed, _minPause);
                    break;

                case TimescaleSupport.PerfmonRegroupChunkResult.LockBusy:
                    /* Nothing changed. Skipped until the next start, so a chunk that stays busy is not hammered, and the
                       wait is the floor: the chunk cost a 3 s lock wait, not a rewrite. */
                    skipped.Add(next.Value.Chunk);
                    failuresInARow = 0;
                    owedPause = _minPause;
                    break;

                case TimescaleSupport.PerfmonRegroupChunkResult.LeftUncompressed:
                    /* The chunk is now the compression policy's (no longer a candidate: it is not compressed). The store
                       just did a whole rewrite, so the wait is as long as it took, and it counts toward the limit. */
                    owedPause = Max(outcome.Elapsed, _minPause);
                    failuresInARow++;
                    break;

                default:
                    skipped.Add(next.Value.Chunk);
                    failuresInARow++;
                    owedPause = _minPause;
                    break;
            }

            if (failuresInARow >= TimescaleSupport.PerfmonRegroupFailureLimit)
            {
                _logger?.LogWarning(
                    "TimescaleDB: {Failures} perfmon_stats chunks in a row could not be re-grouped, so the re-group stops; the next hourly check tries again (#5574)",
                    failuresInARow);
                left = -1;
                break;
            }
        }

        if (done > 0 || skipped.Count > 0 || started)
        {
            _logger?.LogInformation(
                "TimescaleDB: perfmon_stats re-group run over: {Done} chunk(s) re-grouped to '{New}' in {ElapsedMs} ms; {Skipped} skipped for a busy lock or an error (tried again at the next hourly check); {Left} chunk(s) inside the {Days}-day reach still have the old grouping (#5574)",
                done, TimescaleSupport.PerfmonStatsSegmentBy, run.ElapsedMilliseconds, skipped.Count, left < 0 ? "an unknown number of" : left.ToString(System.Globalization.CultureInfo.InvariantCulture), _reachDays);
        }
        else
        {
            _logger?.LogDebug("TimescaleDB: no perfmon_stats chunk inside the {Days}-day reach has the old grouping to re-group (#5574)", _reachDays);
        }

        return done;
    }

    private static TimescaleSupport.PerfmonRegroupCandidate? Next(
        IReadOnlyList<TimescaleSupport.PerfmonRegroupCandidate>? candidates, HashSet<string> skipped)
    {
        if (candidates is null)
        {
            return null;
        }

        foreach (var candidate in candidates)
        {
            if (!skipped.Contains(candidate.Chunk))
            {
                return candidate;
            }
        }

        return null;
    }

    private static TimeSpan Max(TimeSpan a, TimeSpan b) => a >= b ? a : b;
}
