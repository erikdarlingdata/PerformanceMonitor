/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One cheap in-process read of whether collection is keeping up (#5592), which the retention drain takes so that
/// a test can fake it. Read between every batch and again at each re-check of a wait, so an implementation must
/// not touch the store or a monitored server.
/// </summary>
public interface ICollectionPressure
{
    /// <summary>Null when collection is keeping up; otherwise a short reason, for the log line that says why the drain waits.</summary>
    string? BehindReason();
}

/// <summary>
/// The real signal (#5592): collection is "behind" when any of three things holds. Each one is read from state the
/// worker already keeps, so there is no second counter beside the one "Collection Falling Behind" (#4732) is judged on.
/// <list type="number">
/// <item><description><b>A slot was skipped</b> in the last <see cref="SkipWindowMinutes"/> minutes, read from the
/// same per-minute buckets as the self-alert (<see cref="FleetGateStats.SkippedInLastMinutes"/>). The self-alert
/// needs 5% of an hour and 20 slots to fire; a drain that waited for that would already have done the damage, so
/// the drain waits on the first skipped slot.</description></item>
/// <item><description><b>A collection body has been running</b> for <see cref="BodyRunLimit"/> or longer: half of
/// the shortest collector cadence (<see cref="BodyRunShare"/> of the 1-minute tier in
/// <see cref="CollectorScheduleDefaults"/>). A body that runs past one cadence skips the next slot, and the worker's
/// own hang rule (<see cref="DarlingWorker.SweepWatchdogSeconds"/>, 60 s of execution) warns at that point, so
/// half of it is the lead time before a skip. A body that is still queued for the fleet gate does not count: its
/// run has not started, and the running bodies in front of it already do.</description></item>
/// <item><description><b>The service is still settling</b>: it started less than <see cref="SettleWindow"/> ago.
/// The first collections after a start run heavy, and neither arm above can see them in their first minute (the
/// first bodies are spread over <see cref="DarlingWorker.ColdStartSpreadSeconds"/> seconds, then queue behind the
/// gate, and a body that is queued is not running). The field store's first skips came 5 minutes after the start and
/// its relaunch skips 5 to 8 minutes after it, while the drain was still on small tables.</description></item>
/// </list>
/// </summary>
internal sealed class CollectionPressure : ICollectionPressure
{
    /// <summary>The window a skipped slot counts for. The stats keep whole minutes, so this reads 4 to 5 minutes back.</summary>
    internal const int SkipWindowMinutes = 5;

    /// <summary>The share of the shortest collector cadence a body may run before collection counts as behind.</summary>
    internal const double BodyRunShare = 0.5;

    /// <summary>How long after the service starts collection counts as settling.</summary>
    internal static readonly TimeSpan SettleWindow = TimeSpan.FromMinutes(10);

    private readonly FleetGateStats? _stats;
    private readonly Func<DateTime> _utcNow;
    private readonly DateTime _startedUtc;
    private Func<long>? _oldestRunningBodyTicks;

    /// <param name="stats">The fleet gate's counts; null reads no skipped slots (a worker built without them).</param>
    /// <param name="utcNow">The clock, injected so a test does not wait.</param>
    /// <param name="startedUtc">When the service started.</param>
    internal CollectionPressure(FleetGateStats? stats, Func<DateTime> utcNow, DateTime startedUtc)
    {
        ArgumentNullException.ThrowIfNull(utcNow);
        _stats = stats;
        _utcNow = utcNow;
        _startedUtc = startedUtc;
    }

    /// <summary>The longest a collection body may run before the drain waits: <see cref="BodyRunShare"/> of the shortest cadence.</summary>
    internal static TimeSpan BodyRunLimit { get; } = ComputeBodyRunLimit();

    /// <summary>
    /// Supplies the UTC ticks at which the longest-running collection body started its run, or 0 when none is
    /// running. The worker sets it once; the read is made from the retention task, so the supplier must be safe on
    /// any thread.
    /// </summary>
    internal void SetBodySource(Func<long> oldestRunningBodyTicks) => Volatile.Write(ref _oldestRunningBodyTicks, oldestRunningBodyTicks);

    /// <inheritdoc />
    public string? BehindReason()
    {
        var now = _utcNow();
        var sinceStart = now - _startedUtc;
        if (sinceStart < SettleWindow)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"the service started {Math.Max(0, sinceStart.TotalSeconds):F0} s ago and collection is still settling");
        }

        var skipped = _stats?.SkippedInLastMinutes(SkipWindowMinutes) ?? 0;
        if (skipped > 0)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"{skipped} collector slot(s) were skipped in the last {SkipWindowMinutes} minutes");
        }

        var oldest = Volatile.Read(ref _oldestRunningBodyTicks)?.Invoke() ?? 0;
        if (oldest > 0)
        {
            var runningFor = now - new DateTime(oldest, DateTimeKind.Utc);
            if (runningFor >= BodyRunLimit)
            {
                return string.Create(
                    CultureInfo.InvariantCulture,
                    $"a collection body has been running for {runningFor.TotalSeconds:F0} s (limit {BodyRunLimit.TotalSeconds:F0} s)");
            }
        }

        return null;
    }

    private static TimeSpan ComputeBodyRunLimit()
    {
        var shortestMinutes = int.MaxValue;
        foreach (var entry in CollectorScheduleDefaults.All.Values)
        {
            if (entry.FrequencyMinutes > 0)
            {
                shortestMinutes = Math.Min(shortestMinutes, entry.FrequencyMinutes);
            }
        }

        if (shortestMinutes == int.MaxValue)
        {
            shortestMinutes = 1;
        }

        return TimeSpan.FromSeconds(shortestMinutes * 60 * BodyRunShare);
    }
}

/// <summary>
/// Makes the retention drain give way to collection (#5592). One instance per pass; it travels on the pass's
/// <see cref="RetentionWalPacer"/> (<see cref="RetentionWalPacer.Yield"/>), the one object every paced batch already
/// carries, so the paced drain loop in <see cref="DarlingRetention"/> (<c>DrainBatchesAsync</c>) is the single place
/// it is applied and no call site passes anything new.
///
/// <para>The cause it answers: the first drain after the 3.10 upgrade paced only its WAL. The field store's drain
/// deleted 18,007,201 rows in 40 minutes while collection skipped 97 of 1,252 due slots in the hour (7.7%, past the
/// self-alert's 5%), nine servers' bodies ran past 60 s, and the Query Store collector's 95th percentile went from 13.1 s
/// to 20.0 s. The WAL pacer does not bound the reads and index work each batch does, and those compete with the
/// collectors' writes.</para>
///
/// <para>Three rules, all of them cheap and all of them pure over the injected signal, clock and delay:</para>
/// <list type="number">
/// <item><description><b>Before each batch</b>, wait while collection is behind
/// (<see cref="ICollectionPressure"/>), re-checking every <see cref="RecheckSeconds"/>. One wait is bounded by
/// <see cref="MaxWaitSeconds"/>, so a fleet that is behind for its own reasons still sees one batch go through every
/// few minutes instead of a drain that never moves.</description></item>
/// <item><description><b>After each batch</b>, pause for the batch's own run time times <see cref="PauseFactor"/>
/// (1, so the drain deletes at most half the time), at most <see cref="MaxPauseSeconds"/>. It comes before the WAL
/// pacer's wait, and that wait refills by elapsed time, so the two overlap and do not add.</description></item>
/// <item><description><b>A wall budget</b> (<see cref="DefaultWallBudget"/>): a pass that has run that long, waits
/// included, stops at the next batch boundary. That is normal control flow, not an error: the run record and the
/// Query Store warm-up after the drain still run, and the next pass continues from the rows left.</description></item>
/// </list>
/// </summary>
internal sealed class RetentionCollectionYield
{
    /// <summary>How often a wait looks at the signal again.</summary>
    internal const double RecheckSeconds = 5;

    /// <summary>
    /// The longest one batch waits for collection before it goes anyway. It equals the skipped-slot window
    /// (<see cref="CollectionPressure.SkipWindowMinutes"/> minutes), so a single skipped slot ages out of the window
    /// within one wait, and a fleet that is behind for reasons of its own still gets a batch every few minutes.
    /// </summary>
    internal const double MaxWaitSeconds = CollectionPressure.SkipWindowMinutes * 60;

    /// <summary>The pause after a batch, as a multiple of the batch's own run time. 1 means the drain deletes at most half the time.</summary>
    internal const double PauseFactor = 1.0;

    /// <summary>The longest pause after one batch, so a batch that spent minutes in a timed-out attempt does not stall the pass for as long again.</summary>
    internal const double MaxPauseSeconds = 60;

    /// <summary>
    /// The default wall budget of one pass. The daily purge and <c>purge_now</c> both run the same chores behind the
    /// purge in the same task (findings cleanup, the log sweep, the module-map refresh, the chunk-interval
    /// reconcile), and the first drain in the field held them for 40 minutes. Thirty minutes is under that, and
    /// about 2% of the 24-hour cadence. At the pause factor of 1 the field store's backlog needs about 70 minutes of
    /// batches and pauses, so it takes 3 daily passes, 4 when collection pushes back.
    /// </summary>
    internal static readonly TimeSpan DefaultWallBudget = TimeSpan.FromMinutes(30);

    private readonly ICollectionPressure _pressure;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<double> _secondsClock;
    private readonly ILogger? _logger;
    private readonly double _pauseFactor;
    private readonly double _startSeconds;
    private bool _loggedFirstWait;
    private bool _stopPending;

    internal RetentionCollectionYield(
        ICollectionPressure pressure,
        TimeSpan wallBudget,
        ILogger? logger = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        Func<double>? secondsClock = null,
        double pauseFactor = PauseFactor)
    {
        ArgumentNullException.ThrowIfNull(pressure);
        _pressure = pressure;
        WallBudget = wallBudget;
        _logger = logger;
        _delay = delay ?? ((wait, cancellationToken) => Task.Delay(wait, cancellationToken));
        _secondsClock = secondsClock ?? (() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency);
        _pauseFactor = Math.Max(0, pauseFactor);
        _startSeconds = _secondsClock();
    }

    /// <summary>The wall time this pass may run before it stops at a batch boundary.</summary>
    internal TimeSpan WallBudget { get; }

    /// <summary>The seconds spent waiting for collection so far.</summary>
    internal double TotalWaitSeconds { get; private set; }

    /// <summary>The seconds spent in pauses between batches so far.</summary>
    internal double TotalPauseSeconds { get; private set; }

    /// <summary>True once any table's drain stopped, or was never started, because the budget was spent.</summary>
    internal bool StoppedOnBudget { get; private set; }

    /// <summary>The first table the budget stopped, or null.</summary>
    internal string? FirstStoppedTable { get; private set; }

    /// <summary>Tables whose drain was never started because the budget was already spent.</summary>
    internal int TablesNotReached { get; private set; }

    /// <summary>The clock a batch's run time is measured on.</summary>
    internal double NowSeconds => _secondsClock();

    /// <summary>True once the pass has run for its whole budget.</summary>
    internal bool BudgetSpent => _secondsClock() - _startSeconds >= WallBudget.TotalSeconds;

    /// <summary>
    /// Waits, in re-checks of <see cref="RecheckSeconds"/>, while collection is behind, then returns true so the batch
    /// runs. Returns false, without running anything, when the pass's budget is spent (checked first, and again at
    /// every re-check, because a wait counts against it). A wait that reaches <see cref="MaxWaitSeconds"/> returns true
    /// too. Honors <paramref name="cancellationToken"/> before the read and during every delay.
    /// </summary>
    internal async Task<bool> BeforeBatchAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (BudgetSpent)
        {
            return false;
        }

        var reason = _pressure.BehindReason();
        if (reason is null)
        {
            return true;
        }

        if (!_loggedFirstWait)
        {
            _loggedFirstWait = true;
            _logger?.LogInformation(
                "Retention drain is waiting for collection to catch up ({Reason}); it resumes on its own", reason);
        }
        else
        {
            _logger?.LogDebug("Retention drain is waiting for collection to catch up ({Reason})", reason);
        }

        var waited = 0.0;
        while (waited < MaxWaitSeconds)
        {
            var wait = Math.Min(RecheckSeconds, MaxWaitSeconds - waited);
            await _delay(TimeSpan.FromSeconds(wait), cancellationToken);
            waited += wait;
            TotalWaitSeconds += wait;

            cancellationToken.ThrowIfCancellationRequested();
            if (BudgetSpent)
            {
                return false;
            }

            if (_pressure.BehindReason() is null)
            {
                _logger?.LogDebug("Retention drain resumed after waiting {Seconds:F0} s for collection", waited);
                return true;
            }
        }

        _logger?.LogDebug(
            "Retention drain waited {Seconds:F0} s for collection and goes ahead with one batch so the pass keeps moving", waited);
        return true;
    }

    /// <summary>
    /// Pauses for the batch's own run time times <see cref="PauseFactor"/>, at most <see cref="MaxPauseSeconds"/>.
    /// <paramref name="batchStartedSeconds"/> is <see cref="NowSeconds"/> from just before the batch, so the pause
    /// never includes a wait. A spent budget skips the pause: the next <see cref="BeforeBatchAsync"/> ends the pass.
    /// </summary>
    internal async Task AfterBatchAsync(double batchStartedSeconds, CancellationToken cancellationToken)
    {
        if (BudgetSpent)
        {
            return;
        }

        var pause = Math.Min(Math.Max(0, _secondsClock() - batchStartedSeconds) * _pauseFactor, MaxPauseSeconds);
        if (pause <= 0)
        {
            return;
        }

        await _delay(TimeSpan.FromSeconds(pause), cancellationToken);
        TotalPauseSeconds += pause;
    }

    /// <summary>
    /// Called by the drain loop when <see cref="BeforeBatchAsync"/> said stop. The table is recorded by
    /// <see cref="FinishTable"/>, which knows its name.
    /// </summary>
    internal void NoteBudgetStop() => _stopPending = true;

    /// <summary>
    /// Entry check for a table's purge: true when the budget is already spent, so the table is not started (no
    /// connection, no statement). Records the table as not reached and the pass as stopped.
    /// </summary>
    internal bool SkipTableOnBudget(string tableName)
    {
        if (!BudgetSpent)
        {
            return false;
        }

        StoppedOnBudget = true;
        FirstStoppedTable ??= tableName;
        TablesNotReached++;
        return true;
    }

    /// <summary>
    /// Called after a table's drain returned. When the drain stopped on the budget, records the table as the place the
    /// pass stopped (the first one is kept); a stop that deleted nothing counts the table as not reached.
    /// </summary>
    internal void FinishTable(string tableName, int rowsDeleted)
    {
        if (!_stopPending)
        {
            return;
        }

        _stopPending = false;
        StoppedOnBudget = true;
        FirstStoppedTable ??= tableName;
        if (rowsDeleted <= 0)
        {
            TablesNotReached++;
        }
    }

    /// <summary>
    /// The sentence the run record and the log carry when the pass yielded to collection or stopped on its budget; null
    /// when it did neither, so a pass that had no pressure reads exactly as before.
    /// </summary>
    internal string? Describe()
    {
        var inv = CultureInfo.InvariantCulture;
        if (!StoppedOnBudget && TotalWaitSeconds <= 0 && TotalPauseSeconds <= 0)
        {
            return null;
        }

        var text = string.Create(
            inv,
            $"yielded to collection: waited {TotalWaitSeconds:F0} s while it was behind, paused {TotalPauseSeconds:F0} s between batches");
        if (StoppedOnBudget)
        {
            text += string.Create(
                inv,
                $"; stopped at its {WallBudget.TotalMinutes:F0}-minute time budget in {FirstStoppedTable}, with {TablesNotReached} table(s) not reached - the next pass continues from the rows left");
        }

        return text;
    }
}
