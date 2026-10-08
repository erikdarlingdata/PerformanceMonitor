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

/// <summary>Why a drain run ended (#5574); tests assert it, so "nothing ran" can be told apart from "could not run".</summary>
internal enum PerfmonRegroupRunEnd
{
    /// <summary>The catalog was read and no chunk in the reach has the old grouping (converged).</summary>
    NothingLeft,

    /// <summary>The gate was closed (the hypertable still has the old grouping, or TimescaleDB cannot change settings).</summary>
    Blocked,

    /// <summary>The catalog could not be read.</summary>
    ReadFailed,

    /// <summary>Three chunks in a row failed.</summary>
    FailureLimit,

    /// <summary>Every chunk left was skipped (busy, failed, gone, timed out, or already re-grouped by this process).</summary>
    AllSkipped,
}

/// <summary>One run's chunks re-grouped and the reason it ended.</summary>
internal readonly record struct PerfmonRegroupRunResult(int Done, PerfmonRegroupRunEnd End);

/// <summary>
/// The background loop that re-groups the perfmon_stats chunks compressed by <c>server_id</c> alone (#5574), one chunk at
/// a time, newest first, inside the reach: 30 days (<see cref="TimescaleSupport.PerfmonRegroupReachDays"/>), or one day less
/// than the perfmon retention when that is shorter (a chunk retention is about to drop is not worth a rewrite).
///
/// <para><b>Why a loop of its own, not a step of the hourly pass.</b> The first version re-grouped for at most two minutes
/// of each hourly pass. A large store's chunk (13.1 M rows measured, about 14 M per day on the biggest) takes minutes
/// to rewrite, so two minutes an hour reached one chunk an hour and the 30-day reach took more than a day of hourly
/// passes. The hourly pass now only calls <see cref="StartIfIdle"/>, which does no database work and returns at once; the
/// rewrite runs here, on its own connection, so the pass (and the collection sweep that awaits it) never waits for a chunk.</para>
///
/// <para><b>One chunk, then a pause as long as the chunk took</b> (never under <see cref="TimescaleSupport.PerfmonRegroupMinPause"/>),
/// so the store spends at most half its time on the rewrite. Every outcome pauses, a busy lock included: a lock lost at the
/// END of a decompress (the final ACCESS EXCLUSIVE) throws away the whole rewrite, and the pause is the time that cost, not
/// the floor. Thirty chunks of about 3 minutes finish in about three hours, which is "hours, not days" without loading the
/// store: the reads and the collectors share the disk with it.</para>
///
/// <para><b>It stops by itself.</b> When no chunk is left it ends after two catalog reads (Debug); the next hourly pass
/// starts it again for the price of those two reads on a new connection. It also ends on three failed chunks in a row
/// (<see cref="TimescaleSupport.PerfmonRegroupFailureLimit"/>), and when every chunk left was skipped this run (a skipped chunk
/// is not tried again until the next hourly start, so one busy chunk cannot pin the loop). It stops within a statement of
/// the service stopping: the chunk in progress is cancelled and rolls back, or, when only the compress half was cancelled,
/// is left uncompressed for the compression policy to compress with the new grouping.</para>
///
/// <para><b>What it remembers for the life of the process</b> (the drain object lives as long as the service): the chunks it
/// re-grouped, so a chunk that still reads as a candidate afterwards (a TimescaleDB release that spells the setting
/// differently) is skipped with one Warning instead of rewritten forever; the chunks whose decompress hit the statement cap
/// (<see cref="TimescaleSupport.PerfmonRegroupStatementTimeoutSeconds"/>), which would hit it every hour; and that a failed
/// catalog read has been reported once at Warning.</para>
///
/// <para>Gated exactly as the first version was: <see cref="TimescaleSupport.PerfmonRegroupBlockedReason"/> is asked
/// before every chunk (the hypertable must already carry the new grouping, TimescaleDB must be able to change settings
/// with compressed chunks, and the per-chunk settings view must exist), and the caller starts it only on a store with
/// TimescaleDB and only on the hourly pass, never on the start path.</para>
/// </summary>
public sealed class PerfmonRegroupDrain
{
    /// <summary>A busy lock that lost at least this much work is worth an Information line of its own (a lock lost at the
    /// start of a decompress costs the 3 s lock wait; anything near this was lost after a rewrite).</summary>
    internal static readonly TimeSpan DefaultLostWorkThreshold = TimeSpan.FromSeconds(10);

    private readonly Func<CancellationToken, ValueTask<NpgsqlConnection>> _openConnection;
    private readonly ILogger? _logger;
    private readonly int _reachDays;
    private readonly TimeSpan _minPause;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly int _statementTimeoutSeconds;
    private readonly Func<NpgsqlConnection, ILogger?, int, CancellationToken, Task<TimescaleSupport.PerfmonRegroupRead>> _readCandidates;
    private readonly TimeSpan _lostWorkThreshold;
    private readonly object _gate = new();
    private readonly HashSet<string> _regrouped = new(StringComparer.Ordinal);
    private readonly HashSet<string> _gaveUp = new(StringComparer.Ordinal);
    private bool _warnedReadFailure;
    private bool _warnedRepeatCandidate;
    private Task? _running;

    /// <param name="openConnection">Opens a store connection; the drain owns and disposes what it gets, one per run.</param>
    /// <param name="logger">Where the per-chunk and per-run lines go.</param>
    public PerfmonRegroupDrain(Func<CancellationToken, ValueTask<NpgsqlConnection>> openConnection, ILogger? logger)
        : this(openConnection, logger, TimescaleSupport.PerfmonRegroupReachDays, TimescaleSupport.PerfmonRegroupMinPause, Task.Delay)
    {
    }

    /// <summary>The test seam: the reach, the pause floor, the wait itself (a test records the pauses it is asked for
    /// instead of sleeping them) and the statement cap in seconds.</summary>
    internal PerfmonRegroupDrain(
        Func<CancellationToken, ValueTask<NpgsqlConnection>> openConnection,
        ILogger? logger,
        int reachDays,
        TimeSpan minPause,
        Func<TimeSpan, CancellationToken, Task> delay,
        int statementTimeoutSeconds = TimescaleSupport.PerfmonRegroupStatementTimeoutSeconds,
        Func<NpgsqlConnection, ILogger?, int, CancellationToken, Task<TimescaleSupport.PerfmonRegroupRead>>? readCandidates = null,
        TimeSpan? lostWorkThreshold = null)
    {
        _readCandidates = readCandidates ?? TimescaleSupport.ReadPerfmonRegroupCandidatesAsync;
        _lostWorkThreshold = lostWorkThreshold ?? DefaultLostWorkThreshold;
        _openConnection = openConnection ?? throw new ArgumentNullException(nameof(openConnection));
        _logger = logger;
        _reachDays = reachDays;
        _minPause = minPause;
        _delay = delay ?? throw new ArgumentNullException(nameof(delay));
        _statementTimeoutSeconds = statementTimeoutSeconds;
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
    internal async Task<int> RunAsync(CancellationToken cancellationToken) => (await RunDetailedAsync(cancellationToken)).Done;

    /// <summary><see cref="RunAsync"/> with the reason the run ended, so a test can tell "nothing left" from "could not run".</summary>
    internal async Task<PerfmonRegroupRunResult> RunDetailedAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _openConnection(cancellationToken);
        var reach = await EffectiveReachDaysAsync(connection, cancellationToken);
        var skipped = new HashSet<string>(StringComparer.Ordinal);
        var run = Stopwatch.StartNew();
        var done = 0;
        var failuresInARow = 0;
        var left = 0;
        var end = PerfmonRegroupRunEnd.NothingLeft;
        var owedPause = TimeSpan.Zero;
        var started = false;

        while (true)
        {
            var read = await _readCandidates(connection, _logger, reach, cancellationToken);
            var next = Next(read, skipped, out end, out left);
            if (next is null)
            {
                break;
            }

            if (owedPause > TimeSpan.Zero)
            {
                /* The wait is spent only when there is more to do (nothing waits after the last chunk), and the catalog
                   is read again after it: the policy or a restart may have re-grouped the chunk meanwhile. */
                await _delay(owedPause, cancellationToken);
                owedPause = TimeSpan.Zero;
                read = await _readCandidates(connection, _logger, reach, cancellationToken);
                next = Next(read, skipped, out end, out left);
                if (next is null)
                {
                    break;
                }
            }

            if (!started)
            {
                started = true;
                _logger?.LogInformation(
                    "TimescaleDB: perfmon_stats re-group started: {Chunks} chunk(s) inside the {Days}-day reach have the old grouping; they are re-grouped one at a time, newest first, each followed by a pause as long as it took (at least {MinPauseSeconds} s) (#5574)",
                    read.Candidates.Count, reach, (int)_minPause.TotalSeconds);
            }

            var chunk = next.Value.Chunk;
            var outcome = await TimescaleSupport.RegroupPerfmonChunkAsync(connection, _logger, next.Value, cancellationToken, _statementTimeoutSeconds);

            /* Every outcome waits as long as the work took: a lock lost at the end of a decompress threw a whole rewrite away,
               and the store was just as busy as after a good one. */
            owedPause = Max(outcome.Elapsed, _minPause);
            switch (outcome.Result)
            {
                case TimescaleSupport.PerfmonRegroupChunkResult.Regrouped:
                    done++;
                    failuresInARow = 0;
                    _regrouped.Add(chunk);
                    break;

                case TimescaleSupport.PerfmonRegroupChunkResult.LockBusy:
                    /* Nothing changed. Skipped until the next start, so a chunk that stays busy is not hammered. */
                    skipped.Add(chunk);
                    failuresInARow = 0;
                    if (outcome.Elapsed >= _lostWorkThreshold)
                    {
                        _logger?.LogInformation(
                            "TimescaleDB: the lock on perfmon_stats chunk {Chunk} was lost after {ElapsedMs} ms of decompress work, which rolled back; the chunk is tried again at the next hourly check and the drain waits as long as the lost work took (#5574)",
                            chunk, (long)outcome.Elapsed.TotalMilliseconds);
                    }

                    break;

                case TimescaleSupport.PerfmonRegroupChunkResult.LeftUncompressed:
                    /* The chunk is now the compression policy's (no longer a candidate: it is not compressed). The store
                       just did a whole rewrite, and it counts toward the limit. */
                    failuresInARow++;
                    break;

                case TimescaleSupport.PerfmonRegroupChunkResult.Gone:
                    /* Retention dropped the chunk between the read and the decompress: nothing to do, nothing failed. */
                    skipped.Add(chunk);
                    failuresInARow = 0;
                    _logger?.LogDebug("TimescaleDB: perfmon_stats chunk {Chunk} no longer exists (retention dropped it), so it is not re-grouped (#5574)", chunk);
                    break;

                case TimescaleSupport.PerfmonRegroupChunkResult.TimedOut:
                    /* The decompress ran into its statement cap. The same chunk would run into it at every hourly start,
                       so it is left alone while this process lives. The statement's own Warning has said why. */
                    skipped.Add(chunk);
                    _gaveUp.Add(chunk);
                    failuresInARow++;
                    _logger?.LogInformation(
                        "TimescaleDB: perfmon_stats chunk {Chunk} could not be decompressed within {Seconds} s, so it is not tried again until the service restarts (#5574)",
                        chunk, _statementTimeoutSeconds);
                    break;

                default:
                    skipped.Add(chunk);
                    failuresInARow++;
                    break;
            }

            if (failuresInARow >= TimescaleSupport.PerfmonRegroupFailureLimit)
            {
                _logger?.LogWarning(
                    "TimescaleDB: {Failures} perfmon_stats chunks in a row could not be re-grouped, so the re-group stops; the next hourly check tries again (#5574)",
                    failuresInARow);
                left = -1;
                end = PerfmonRegroupRunEnd.FailureLimit;
                break;
            }
        }

        if (done > 0 || skipped.Count > 0 || started)
        {
            _logger?.LogInformation(
                "TimescaleDB: perfmon_stats re-group run over: {Done} chunk(s) re-grouped to '{New}' in {ElapsedMs} ms; {Skipped} skipped for a busy lock or an error (tried again at the next hourly check); {Left} chunk(s) inside the {Days}-day reach still have the old grouping (#5574)",
                done, TimescaleSupport.PerfmonStatsSegmentBy, run.ElapsedMilliseconds, skipped.Count, left < 0 ? "an unknown number of" : left.ToString(System.Globalization.CultureInfo.InvariantCulture), reach);
        }
        else if (end == PerfmonRegroupRunEnd.NothingLeft)
        {
            _logger?.LogDebug("TimescaleDB: no perfmon_stats chunk inside the {Days}-day reach has the old grouping to re-group (#5574)", reach);
        }
        else if (end == PerfmonRegroupRunEnd.Blocked)
        {
            _logger?.LogDebug("TimescaleDB: the perfmon_stats re-group did not run: the gate is closed (the reason is in the line above) (#5574)");
        }

        return new PerfmonRegroupRunResult(done, end);
    }

    /// <summary>The reach this run uses: the configured days, or one day less than the perfmon retention when that is
    /// shorter (#5574). A retention that could not be read leaves the configured reach.</summary>
    private async Task<int> EffectiveReachDaysAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var retention = await TimescaleSupport.ReadPerfmonRetentionDaysAsync(connection, cancellationToken);
        return retention is int days ? Math.Min(_reachDays, Math.Max(0, days - 1)) : _reachDays;
    }

    /// <summary>
    /// The newest candidate this run may still do, or <c>null</c> with <paramref name="end"/> saying why not: the gate
    /// blocked, the read failed (a Warning the first time in the process, Debug after), nothing is left, or every candidate
    /// was skipped. A candidate this process already re-grouped is skipped, with one Warning per process that names the
    /// setting text the view returned (a TimescaleDB that spells it another way than the literal the read compares to).
    /// </summary>
    private TimescaleSupport.PerfmonRegroupCandidate? Next(
        TimescaleSupport.PerfmonRegroupRead read, HashSet<string> skipped, out PerfmonRegroupRunEnd end, out int left)
    {
        left = -1;
        switch (read.Status)
        {
            case TimescaleSupport.PerfmonRegroupReadStatus.Blocked:
                end = PerfmonRegroupRunEnd.Blocked;
                return null;

            case TimescaleSupport.PerfmonRegroupReadStatus.ReadFailed:
                end = PerfmonRegroupRunEnd.ReadFailed;
                if (!_warnedReadFailure)
                {
                    _warnedReadFailure = true;
                    _logger?.LogWarning(
                        "TimescaleDB: could not read perfmon_stats's chunk compression settings, so no chunk is re-grouped; this is logged once per service run and repeated only at Debug (#5574): {Message}",
                        read.Detail);
                }
                else
                {
                    _logger?.LogDebug("TimescaleDB: could not read perfmon_stats's chunk compression settings again (#5574): {Message}", read.Detail);
                }

                return null;
        }

        left = read.Candidates.Count;
        foreach (var candidate in read.Candidates)
        {
            if (skipped.Contains(candidate.Chunk) || _gaveUp.Contains(candidate.Chunk))
            {
                continue;
            }

            if (_regrouped.Contains(candidate.Chunk))
            {
                skipped.Add(candidate.Chunk);
                if (!_warnedRepeatCandidate)
                {
                    _warnedRepeatCandidate = true;
                    _logger?.LogWarning(
                        "TimescaleDB: perfmon_stats chunk {Chunk} was re-grouped by this service run but the catalog still reports its grouping as '{Setting}' (the wanted one is '{New}'), so it is not re-grouped again; check the TimescaleDB version's spelling of the setting (#5574)",
                        candidate.Chunk, candidate.OldSegmentBy ?? "(none)", TimescaleSupport.PerfmonStatsChunkSegmentBy);
                }

                continue;
            }

            end = PerfmonRegroupRunEnd.NothingLeft;
            return candidate;
        }

        end = read.Candidates.Count == 0 ? PerfmonRegroupRunEnd.NothingLeft : PerfmonRegroupRunEnd.AllSkipped;
        return null;
    }

    private static TimeSpan Max(TimeSpan a, TimeSpan b) => a >= b ? a : b;
}
