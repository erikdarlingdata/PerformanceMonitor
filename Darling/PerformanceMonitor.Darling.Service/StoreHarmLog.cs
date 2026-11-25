/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The kinds of store operation that count as harm to the Store Checkpointer Pressure self-alert (#9549).</summary>
internal enum StoreHarmKind
{
    /// <summary>An alert pass's store read failed and was swallowed (<see cref="AlertReadFailureCounter.RecordReadFailure"/>).</summary>
    AlertReadFailed,

    /// <summary>An alert pass's store read crossed its deadline and was re-asked (<see cref="AlertReadFailureCounter.RecordRetriedRead"/>).</summary>
    AlertReadRetried,

    /// <summary>A read the MCP host, the web API or a composed panel served ended at its deadline (<see cref="ReadOutcome.Timeout"/>).</summary>
    HostReadDeadline,

    /// <summary>A collector's store write stalled: it ended in a transport fault (the store did not answer it: a timeout
    /// or a torn connection), or the store answered it after longer than <see cref="StoreHarmLog.StalledWriteMs"/>.</summary>
    CollectorWriteStalled,
}

/// <summary>One store operation that suffered (#9549).</summary>
/// <param name="StartedUtc">When it began, UTC: its end minus how long it ran.</param>
/// <param name="EndedUtc">When it failed, was re-asked or was cut off, UTC.</param>
/// <param name="Kind">What kind of operation it was.</param>
/// <param name="Name">Its short constant name: the alert read's name, the served route, or the collector.</param>
internal readonly record struct StoreHarmEvent(DateTime StartedUtc, DateTime EndedUtc, StoreHarmKind Kind, string Name)
{
    /// <summary>The event as the alert text states it: what suffered and at what minute (UTC).</summary>
    public string Describe()
    {
        var at = DarlingSelfAlertEvaluator.FormatSyncMinute(EndedUtc);
        return Kind switch
        {
            StoreHarmKind.AlertReadFailed => $"the alert pass's {Name} read failed at {at}",
            StoreHarmKind.AlertReadRetried => $"the alert pass's {Name} read crossed its deadline and was retried at {at}",
            StoreHarmKind.HostReadDeadline => $"the {Name} read was cut off at its deadline at {at}",
            _ => $"the {Name} collector's store write stalled at {at}",
        };
    }
}

/// <summary>One store operation that suffered and the checkpoint sync it overlapped (#9549).</summary>
internal sealed record CheckpointHarmFinding(StoreHarmEvent Harm, CheckpointSync Sync);

/// <summary>
/// What the service saw of one hourly window (#9549): the checkpoint syncs the minute samples saw finish, and the store
/// operations that suffered while one of them ran. In process memory only, so it describes the window the running
/// service watched and nothing before its start.
/// </summary>
/// <param name="JudgedUtc">When the window was judged, UTC: right after the sweep stored the hour's checkpointer row.</param>
/// <param name="Syncs">The syncs of the window, oldest first.</param>
/// <param name="Findings">One per operation that overlapped a sync that can carry harm (one of at least
/// <see cref="DarlingSelfAlertEvaluator.CheckpointSyncBarMs"/>), oldest first. An operation that overlapped several is
/// stated against the longest of them.</param>
internal sealed record CheckpointHarmVerdict(
    DateTime JudgedUtc, IReadOnlyList<CheckpointSync> Syncs, IReadOnlyList<CheckpointHarmFinding> Findings)
{
    /// <summary>
    /// How far a verdict's <see cref="JudgedUtc"/> may sit from a checkpointer row's stamp and still describe that row's
    /// interval. The verdict is judged right after the sweep that writes the row, so the two are seconds apart on a
    /// healthy store. Fifteen minutes allows a slow sweep and a store clock that differs from the service's, and is far
    /// short of the hour to the neighbouring rows.
    /// </summary>
    internal static readonly TimeSpan RowMatchWindow = TimeSpan.FromMinutes(15);

    /// <summary>True when this verdict describes the interval that ends at <paramref name="observedAtUtc"/>.</summary>
    public bool Describes(DateTime observedAtUtc) => (JudgedUtc - observedAtUtc).Duration() <= RowMatchWindow;
}

/// <summary>
/// The store operations that suffered, with their times, for the Store Checkpointer Pressure self-alert (#9549). The
/// alert fires on harm: a store read or a collector store write that failed, was retried, was cut off or stalled while
/// a checkpoint sync ran. Two of those are recorded here because nothing else in the process keeps them with a time:
/// a read the MCP host, the web API or a composed panel served that ended at its deadline (its row in
/// <c>collect.slow_reads</c> is written to the store, by another task), and a collector store write that stalled: one
/// that ended in a transport fault, or that the store answered after longer than <see cref="StalledWriteMs"/> (its
/// <c>collection_log</c> row is written to the store too). The alert pass's failed and retried
/// reads come from <see cref="AlertReadFailureCounter.RecentReads"/>.
///
/// <para>In memory, for the counter's reason: what is recorded is a store that did not answer, so a record that had to
/// be written to that store would be missing exactly when it mattered. A restart empties it, with the sampler's
/// baseline, so the first window after a start covers only what the new process saw. Bounded at
/// <see cref="Capacity"/> events, the newest.</para>
/// </summary>
internal sealed class StoreHarmLog
{
    /// <summary>The process's log: the served reads and the collector runner record here, and the hourly tick reads it.</summary>
    public static StoreHarmLog Shared { get; } = new StoreHarmLog();

    /// <summary>How many events are kept, the newest.</summary>
    internal const int Capacity = 1024;

    private readonly Queue<StoreHarmEvent> _events = new();
    private readonly object _gate = new();
    private CheckpointHarmVerdict? _latestVerdict;

    /// <summary>Records one operation that ended at <paramref name="endedUtc"/> after running
    /// <paramref name="elapsedMilliseconds"/> (a negative value is zero). Never throws.</summary>
    public void Record(StoreHarmKind kind, string name, DateTime endedUtc, long elapsedMilliseconds)
    {
        var elapsed = elapsedMilliseconds < 0 ? 0 : elapsedMilliseconds;
        var harm = new StoreHarmEvent(
            endedUtc.AddMilliseconds(-elapsed), endedUtc, kind, string.IsNullOrWhiteSpace(name) ? "unnamed" : name);

        lock (_gate)
        {
            if (_events.Count == Capacity)
            {
                _events.Dequeue();
            }

            _events.Enqueue(harm);
        }
    }

    /// <summary>
    /// Records a collector store write that ended in <paramref name="fault"/> when the fault is a transport fault
    /// (<see cref="PostgresTransportFault.IsTransportFault"/>): the store did not answer the write. A fault the store
    /// answered with (a constraint, a permission) is not a stall and is not recorded. Always returns false, so it can
    /// sit in an exception filter and leave the fault to the arms that handle it.
    /// </summary>
    public bool NoteCollectorWriteFault(Exception fault, string collector, DateTime endedUtc, long elapsedMilliseconds)
    {
        if (fault is not OperationCanceledException && PostgresTransportFault.IsTransportFault(fault))
        {
            Record(StoreHarmKind.CollectorWriteStalled, collector, endedUtc, elapsedMilliseconds);
        }

        return false;
    }

    /// <summary>
    /// How long a collector store write may run before it counts as stalled, in milliseconds: the deadline an alert-pass
    /// store read is re-asked at and a served read is cut off at (<see cref="DarlingSelfAlertEvaluator.CheckpointSyncBarMs"/>).
    /// A write has a longer command deadline than a read, so one caught inside a long checkpoint sync usually waits and
    /// then lands. The rows are stored, and collection on that server stood still for longer than a read is allowed to.
    /// </summary>
    internal const long StalledWriteMs = DarlingSelfAlertEvaluator.CheckpointSyncBarMs;

    /// <summary>Records a collector store write the store answered, when it ran longer than <see cref="StalledWriteMs"/>.
    /// A write at or under it is not a stall and is not recorded.</summary>
    public void NoteCollectorWriteTime(string collector, DateTime endedUtc, long elapsedMilliseconds)
    {
        if (elapsedMilliseconds > StalledWriteMs)
        {
            Record(StoreHarmKind.CollectorWriteStalled, collector, endedUtc, elapsedMilliseconds);
        }
    }

    /// <summary>The recorded events, oldest first. A copy.</summary>
    public IReadOnlyList<StoreHarmEvent> Snapshot()
    {
        lock (_gate)
        {
            return _events.ToArray();
        }
    }

    /// <summary>The verdict of the newest judged window, or null before the first one. <c>get_store_metrics</c> reads it
    /// to publish the same judgment the self-alert was handed.</summary>
    public CheckpointHarmVerdict? LatestVerdict
    {
        get => Volatile.Read(ref _latestVerdict);
        set => Volatile.Write(ref _latestVerdict, value);
    }

    /// <summary>The alert pass's recent failed and retried reads as harm events.</summary>
    public static IEnumerable<StoreHarmEvent> FromAlertReads(AlertReadFailureCounter? counter) =>
        counter is null
            ? Array.Empty<StoreHarmEvent>()
            : counter.RecentReads().Select(read => new StoreHarmEvent(
                read.AtUtc.AddMilliseconds(-read.ElapsedMs),
                read.AtUtc,
                read.Retried ? StoreHarmKind.AlertReadRetried : StoreHarmKind.AlertReadFailed,
                read.Read));

    /// <summary>
    /// True when <paramref name="sync"/> can carry harm: it ran for at least the store read deadline
    /// (<see cref="DarlingSelfAlertEvaluator.CheckpointSyncBarMs"/>, the milliseconds compared as stored). A sync shorter
    /// than the deadline cannot cut a read off by itself, so a read that failed or ran long beside one has another cause.
    /// </summary>
    internal static bool CanCarryHarm(CheckpointSync sync) => sync.SyncMs >= DarlingSelfAlertEvaluator.CheckpointSyncBarMs;

    /// <summary>
    /// Judges one window: every event in <paramref name="harm"/> that overlaps one of the <paramref name="syncs"/> that
    /// <see cref="CanCarryHarm"/> (<see cref="CheckpointSync.Overlaps"/>) is a finding, stated against the longest sync it
    /// overlapped. A shorter sync stays in the verdict's <see cref="CheckpointHarmVerdict.Syncs"/> and is never blamed.
    /// Pure, so the rule the alert fires on is tested without a store or a clock.
    /// </summary>
    public static CheckpointHarmVerdict Judge(
        DateTime judgedUtc, IReadOnlyList<CheckpointSync> syncs, IEnumerable<StoreHarmEvent> harm)
    {
        var findings = new List<CheckpointHarmFinding>();
        foreach (var suffered in harm.OrderBy(h => h.EndedUtc))
        {
            CheckpointSync? inside = null;
            foreach (var sync in syncs)
            {
                if (CanCarryHarm(sync)
                    && sync.Overlaps(suffered.StartedUtc, suffered.EndedUtc)
                    && (inside is not CheckpointSync held || sync.SyncMs > held.SyncMs))
                {
                    inside = sync;
                }
            }

            if (inside is CheckpointSync found)
            {
                findings.Add(new CheckpointHarmFinding(suffered, found));
            }
        }

        return new CheckpointHarmVerdict(judgedUtc, syncs, findings);
    }

    /// <summary>Judges the window from this log and the alert pass's counter together, and keeps the verdict as
    /// <see cref="LatestVerdict"/>.</summary>
    public CheckpointHarmVerdict JudgeAndKeep(
        DateTime judgedUtc, IReadOnlyList<CheckpointSync> syncs, AlertReadFailureCounter? alertReads)
    {
        var verdict = Judge(judgedUtc, syncs, Snapshot().Concat(FromAlertReads(alertReads)));
        LatestVerdict = verdict;
        return verdict;
    }

    /// <summary>"1 store operation" or "3 store operations", invariant culture.</summary>
    internal static string CountText(int count) =>
        count.ToString(CultureInfo.InvariantCulture) + (count == 1 ? " store operation" : " store operations");
}
