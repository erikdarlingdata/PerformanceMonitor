/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #8528: what the service's own ensure of the capture sessions (deadlock, blocked process, client timeouts) last found on one
/// server, read by the Capture Down self-alert beside the store's newest collector rows. Keyed by collector name.
/// </summary>
/// <param name="RestartedAwaitingRead">The collectors whose session the ensure found stopped or absent and started, and that have not
/// written a run row since. Their newest SESSION_MISSING row is from before the start, so it is not a capture that is down: the
/// self-alert holds its verdict for them until the collector reads again.</param>
/// <param name="Failures">The last ensure failure per collector: a permission denial, or a session the ensure could not start or
/// create, with what the server said. The self-alert's detail quotes the server's text instead of guessing at a cause.</param>
public sealed record CaptureSessionEnsureReport(
    IReadOnlyCollection<string> RestartedAwaitingRead,
    IReadOnlyDictionary<string, DarlingXeSessions.XeEnsureFailure> Failures)
{
    /// <summary>A report with nothing in it: no session awaiting its read, no failure recorded.</summary>
    public static readonly CaptureSessionEnsureReport Empty = new(
        Array.Empty<string>(),
        new Dictionary<string, DarlingXeSessions.XeEnsureFailure>(StringComparer.OrdinalIgnoreCase));

    /// <summary>
    /// The collectors whose run found the session stopped while an ensure that failed with a transient error (a timeout, a dropped
    /// connection) had its retry pending. Nothing is known about the session until that retry, so the self-alert holds its verdict
    /// for them until the retry has run. A collector is held this way once per run of SESSION_MISSING rows, so a retry that fails
    /// again leaves the capture to fire.
    /// </summary>
    public IReadOnlyCollection<string> AwaitingRetry { get; init; } = Array.Empty<string>();

    /// <summary>The collectors whose session the last ensure started or created. One that is named by the read again stopped after that start.</summary>
    public IReadOnlyCollection<string> StartedAtLastEnsure { get; init; } = Array.Empty<string>();

    /// <summary>The collectors whose last ensure did not complete: it failed with a transient error and says nothing about the session.</summary>
    public IReadOnlyCollection<string> TransientAtLastEnsure { get; init; } = Array.Empty<string>();

    /// <summary>
    /// The collectors whose run found the session stopped although an ensure in that same run had completed and found nothing to
    /// start. Only for these does the detail say the service's check found nothing to start.
    /// </summary>
    public IReadOnlyCollection<string> CheckedAfterMissingRead { get; init; } = Array.Empty<string>();

    /// <summary>
    /// The capture collectors that are turned off for the server. A collector that does not run writes no new row, so its last
    /// SESSION_MISSING row stays the newest: the self-alert judges no capture by it, neither as down nor as awaiting a read.
    /// </summary>
    public IReadOnlyCollection<string> NotCollecting { get; init; } = Array.Empty<string>();

    /// <summary>
    /// The newest run status the service wrote for each capture collector in this process (SUCCESS, PERMISSIONS, ERROR and the
    /// rest). The store read names only the collectors whose newest row is SESSION_MISSING; this tells a row that read the running
    /// session from one that read nothing. A collector with no run in this process has no entry.
    /// </summary>
    public IReadOnlyDictionary<string, string> LastRowStatuses { get; init; } =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

    /// <summary>The newest run status the service wrote for this collector, or null when it has not run in this process.</summary>
    public string? LastRowStatusOf(string collectorName) =>
        LastRowStatuses.TryGetValue(collectorName, out var status) ? status : null;

    /// <summary>Whether the ensure started this collector's session and the collector has not read it since.</summary>
    public bool IsAwaitingRead(string collectorName) => Names(RestartedAwaitingRead, collectorName);

    /// <summary>Whether this collector's SESSION_MISSING row waits for the retry of an ensure that failed transiently.</summary>
    public bool IsAwaitingRetry(string collectorName) => Names(AwaitingRetry, collectorName);

    /// <summary>Whether the self-alert holds its verdict for this collector: its newest row does not yet say whether the capture is down.</summary>
    public bool IsHeld(string collectorName) => IsAwaitingRead(collectorName) || IsAwaitingRetry(collectorName);

    /// <summary>Whether the last ensure started or created this collector's session.</summary>
    public bool WasStartedAtLastEnsure(string collectorName) => Names(StartedAtLastEnsure, collectorName);

    /// <summary>Whether the last ensure of this collector's session did not complete.</summary>
    public bool WasTransientAtLastEnsure(string collectorName) => Names(TransientAtLastEnsure, collectorName);

    /// <summary>Whether an ensure in the collector's own run completed, found nothing to start, and the read still found the session stopped.</summary>
    public bool WasCheckedAfterMissingRead(string collectorName) => Names(CheckedAfterMissingRead, collectorName);

    /// <summary>Whether this capture collector is turned off for the server.</summary>
    public bool IsNotCollecting(string collectorName) => Names(NotCollecting, collectorName);

    /// <summary>The last ensure failure for this collector, or null when the ensure left its session in place or never ran.</summary>
    public DarlingXeSessions.XeEnsureFailure? FailureOf(string collectorName) =>
        Failures.TryGetValue(collectorName, out var failure) ? failure : null;

    private static bool Names(IReadOnlyCollection<string> collectors, string collectorName) =>
        collectors.Contains(collectorName, StringComparer.OrdinalIgnoreCase);
}

/// <summary>
/// #8528: what the Capture Down self-alert must not judge yet, per capture collector of one server
/// (<c>ServerLoopState.CaptureSessionRestarts</c>). A session the ensure started is awaiting its collector's read until that
/// collector writes a row; a collector whose run found the session stopped while a transient ensure failure awaits its retry is
/// held until the retry has run. Both holds also lapse by age, so nothing that stops a collector from writing rows, and no retry
/// that never comes, can hold a verdict for the life of the process. The ensure and the run write it on the sweep's pool thread;
/// the fleet self-alert pass reads it from another, so each set is a <see cref="ConcurrentDictionary{TKey, TValue}"/>.
/// In memory only: after a service restart a standing SESSION_MISSING row is judged as it stands.
/// </summary>
internal sealed class CaptureSessionRestarts
{
    /// <summary>How long a started session awaits its read when the collector's own interval is not given: twice the longest capture interval (deadlocks, five minutes).</summary>
    public static readonly TimeSpan DefaultReadBound = TimeSpan.FromMinutes(10);

    /// <summary>How long a collector is held for the retry of a transient ensure failure: twice the retry interval, so a retry that never runs does not hold it for good.</summary>
    public static readonly TimeSpan RetryBound = DarlingXeSessions.TransientRetryInterval + DarlingXeSessions.TransientRetryInterval;

    private readonly ConcurrentDictionary<string, DateTime> _awaitingRead = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DateTime> _awaitingRetry = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _retryHoldSpent = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _readEnsureSpent = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _checkedAfterMissingRead = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _rowFromReadStart = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _startedAtRead = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, bool> _stoppedAfterStart = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, DateTime[]> _readStarts = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, long> _lastRowSequence = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, string> _lastRowStatus = new(StringComparer.OrdinalIgnoreCase);
    private long _sequence;
    private long _lastEnsureSequence;

    /// <summary>How many times in <see cref="ReadStartWindow"/> a collector's read may start its stopped session before the next stop is left to fire.</summary>
    public const int MaxReadStartsPerWindow = 2;

    /// <summary>The window <see cref="MaxReadStartsPerWindow"/> counts over.</summary>
    public static readonly TimeSpan ReadStartWindow = TimeSpan.FromHours(1);

    /// <summary>
    /// Whether a run status says something about the session: SUCCESS read it, PERMISSIONS was refused it, SESSION_MISSING found
    /// it stopped. ERROR, YIELDED, SKIPPED and ABANDONED rows did not read the session and settle nothing.
    /// </summary>
    public static bool StatusSettles(string? status) => status is "SUCCESS" or "PERMISSIONS" or "SESSION_MISSING";

    /// <summary>The ensure started this collector's session; its newest row is from before the start.</summary>
    public void Mark(string collectorName) => _awaitingRead[collectorName] = DateTime.UtcNow;

    /// <summary>
    /// The collector wrote a run row. A row whose status settles nothing (<see cref="StatusSettles"/>) changes nothing here: the
    /// collector did not read the session, so a started session still awaits its read and the streak of SESSION_MISSING rows goes
    /// on. A row that read the session is the collector's own reading after any start, so the session stops awaiting its read.
    /// A SUCCESS or PERMISSIONS row also ends the streak, and the next missing-session row may ensure at its read and be held for
    /// a retry again; the SUCCESS of the run that started the session at its read is the exception, because that session has not
    /// yet been read running without a start.
    /// </summary>
    public void RowWritten(string collectorName, string status)
    {
        _lastRowStatus[collectorName] = status;
        _lastRowSequence[collectorName] = Interlocked.Increment(ref _sequence);
        if (!StatusSettles(status))
        {
            return;
        }

        _awaitingRead.TryRemove(collectorName, out _);
        if (string.Equals(status, "SESSION_MISSING", StringComparison.Ordinal))
        {
            return;
        }

        if (_rowFromReadStart.TryRemove(collectorName, out _) && string.Equals(status, "SUCCESS", StringComparison.Ordinal))
        {
            return;
        }

        _awaitingRetry.TryRemove(collectorName, out _);
        _retryHoldSpent.TryRemove(collectorName, out _);
        _readEnsureSpent.TryRemove(collectorName, out _);
        _checkedAfterMissingRead.TryRemove(collectorName, out _);
        _startedAtRead.TryRemove(collectorName, out _);
        _stoppedAfterStart.TryRemove(collectorName, out _);
    }

    /// <summary>
    /// Whether this collector's run may ensure the sessions at its read. Not when an ensure has run since the collector's previous
    /// row: that ensure looked at every session, so one sweep ensures once whichever collector asks first. Not when the session
    /// was started at a read <see cref="MaxReadStartsPerWindow"/> times within <see cref="ReadStartWindow"/>. And once per streak of
    /// SESSION_MISSING rows, which the SUCCESS of a read-time start does not end: a session that is stopped again by the next run
    /// is left stopped, its row is SESSION_MISSING and Capture Down fires. A session that stays unreadable is ensured by the
    /// hourly pass and not on every run.
    /// </summary>
    public bool TryBeginReadEnsure(string collectorName, DateTime utcNow)
    {
        var lastRow = _lastRowSequence.TryGetValue(collectorName, out var sequence) ? sequence : 0;
        if (Interlocked.Read(ref _lastEnsureSequence) > lastRow)
        {
            return false;
        }

        if (_readStarts.TryGetValue(collectorName, out var starts) && starts.Count(start => utcNow - start < ReadStartWindow) >= MaxReadStartsPerWindow)
        {
            _stoppedAfterStart[collectorName] = true;
            return false;
        }

        if (!_readEnsureSpent.TryAdd(collectorName, true))
        {
            if (_startedAtRead.ContainsKey(collectorName))
            {
                _stoppedAfterStart[collectorName] = true;
            }

            return false;
        }

        return true;
    }

    /// <summary>The run's ensure at its read started the stopped session and the second read succeeded: counted, and the row it writes keeps the streak.</summary>
    public void NoteStartedAtRead(string collectorName, DateTime utcNow)
    {
        _rowFromReadStart[collectorName] = true;
        _startedAtRead[collectorName] = true;
        var earlier = _readStarts.TryGetValue(collectorName, out var starts) ? starts : Array.Empty<DateTime>();
        _readStarts[collectorName] = earlier.Where(start => utcNow - start < ReadStartWindow).Append(utcNow).ToArray();
    }

    /// <summary>The collectors whose session a read started and that found it stopped again: the detail says the service started it and it stopped.</summary>
    public IReadOnlyCollection<string> StoppedAfterStart() => _stoppedAfterStart.Keys.ToArray();

    /// <summary>The newest run status written for each of the given collectors in this process.</summary>
    public IReadOnlyDictionary<string, string> LastRowStatuses(IEnumerable<string> collectorNames)
    {
        var statuses = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var collectorName in collectorNames)
        {
            if (_lastRowStatus.TryGetValue(collectorName, out var status))
            {
                statuses[collectorName] = status;
            }
        }

        return statuses;
    }

    /// <summary>An ensure in the collector's own run completed and found nothing to start, and the read still found the session stopped.</summary>
    public void NoteCheckedAfterMissingRead(string collectorName) => _checkedAfterMissingRead[collectorName] = true;

    /// <summary>
    /// Holds the collector for the retry of a transient ensure failure, once per run of SESSION_MISSING rows. Returns false when
    /// that one hold is spent, so an outage that outlasts the retry fires.
    /// </summary>
    public bool TryHoldForRetry(string collectorName)
    {
        if (!_retryHoldSpent.TryAdd(collectorName, true))
        {
            return false;
        }

        _awaitingRetry[collectorName] = DateTime.UtcNow;
        return true;
    }

    /// <summary>An ensure has run, so no collector waits for a retry any more: what it found is on the outcome and in the marks.</summary>
    public void EnsureRan()
    {
        _awaitingRetry.Clear();
        Interlocked.Exchange(ref _lastEnsureSequence, Interlocked.Increment(ref _sequence));
    }

    /// <summary>The collectors still awaiting their read at <paramref name="utcNow"/>: marked no longer ago than their bound.</summary>
    public IReadOnlyCollection<string> AwaitingRead(DateTime utcNow, Func<string, TimeSpan> boundOf) =>
        _awaitingRead.Where(mark => utcNow - mark.Value <= boundOf(mark.Key)).Select(mark => mark.Key).ToArray();

    /// <summary>The collectors still held for a retry at <paramref name="utcNow"/>.</summary>
    public IReadOnlyCollection<string> AwaitingRetry(DateTime utcNow) =>
        _awaitingRetry.Where(hold => utcNow - hold.Value <= RetryBound).Select(hold => hold.Key).ToArray();

    /// <summary>The collectors whose own run ensured, found nothing to start, and still read a stopped session.</summary>
    public IReadOnlyCollection<string> CheckedAfterMissingRead() => _checkedAfterMissingRead.Keys.ToArray();
}
