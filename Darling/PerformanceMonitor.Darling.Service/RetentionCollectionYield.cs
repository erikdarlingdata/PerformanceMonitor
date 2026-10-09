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
/// The real signal (#5592): collection is "behind" when either of two things holds. Each one is read from state the
/// worker already keeps, so there is no second counter beside the one "Collection Falling Behind" (#4732) is judged on.
/// <list type="number">
/// <item><description><b>Collection is skipping a real share of its slots</b>: over the last
/// <see cref="SkipWindowMinutes"/> minutes at least <see cref="SkipMinCount"/> slots were skipped AND they are at least
/// <see cref="SkipSharePerMille"/> thousandths (2.5%) of the slots that came due, read from the same per-minute
/// buckets as the self-alert (<see cref="FleetGateStats.SlotsInLastMinutes"/>) with the same two-part test as
/// <c>FleetGateReport.IsBehind</c>, on a shorter window. The first version waited on any single skipped slot and
/// on any body running 30 s. Measured on a large field store over a normal day, with the drain's own window left
/// out, that read "behind" in 513 of 843 five-minute windows (61%): the hang rule (a body past
/// <see cref="DarlingWorker.SweepWatchdogSeconds"/>, 60 s) warned 58 to 75 times a day, one per hang episode, each
/// skipping one slot of every one-minute collector the stalled body runs (about 19 on a default SQL Server target),
/// and a fleet's per-minute sum of collector run time passed 30 s in 22% of minutes (its p90 is 47.7 s). A drain that
/// backs off for a normal day never gets its work done. The incident's own line skipped 97 of 1,252 due slots (7.7%),
/// but that is a short first line after a service start, not an hour: this store's full hours hold about 54,000 due
/// slots (51,561 to 54,056), and its normal post-start lines with no drain running ran 6.8% to 24.1% (613 to 1,503 due
/// slots). So the 7.7% does not show by itself that the drain caused the skips; the drain hour has to be compared with
/// a normal post-start hour as well as a normal full hour.
/// <para>The normal day's own hourly lines (3 days, full hours of about 54,000 due slots, 90 hours): the share of
/// slots skipped was 0.00% at the median, 0.20% at the 90th percentile and 4.39% at the 99th, and one hour met the
/// alert's rule; six more short lines right after a service start did (6.8% to 24.1% of 613 to 1,503 due), so a burst
/// after a start is real, and the settle arm below covers it.</para>
/// <list type="bullet">
/// <item><description><b>Share, 2.5%</b>: half the alert's 5% (<c>DarlingSelfAlertEvaluator.FleetGateBehindPercent</c>),
/// so the drain backs off before an hour reaches the alert, not after it has fired. It sits about 12 times over a normal
/// hour's 0.20% (p90) and under the 5% line, so a normal day does not hold the drain and a bad hour does.</description></item>
/// <item><description><b>Window, 10 minutes.</b> The stats keep whole minutes, so the read covers 9 to 10 minutes. A full
/// hour on that fleet holds about 54,000 due slots, so the window holds about 9,000 and the share asks for about 225
/// skipped slots there: a single hang episode (about 19 slots) cannot reach it, a sustained skip rate can. A shorter
/// window would hold fewer slots and let the minimum below decide; a longer one would average a burst away. Ten minutes
/// is also the settle window below. On a short line right after a start (1,252 due slots, about 208 in ten minutes) the
/// same share is 6 slots, which is why the settle arm covers that period and the share takes over once it ends.</description></item>
/// <item><description><b>Minimum, 3 slots.</b> It only matters on a small fleet, where 2.5% of a thin window is a
/// fraction of one slot. The alert counts 20 skipped slots over 60 one-minute buckets, which is exactly six disjoint
/// 10-minute windows. A drain held just under the minimum lets (minimum - 1) slots skip per window, and the signal is
/// read only at batch boundaries, so about a slot more lands between reads: 6 x (3 - 1) = 12 an hour, 18 with one
/// slot of slop per window, under the alert's 20. A minimum of 5 allowed 24 to 30 an hour, over it, and 4 gives 24 with
/// the slop, so 3 is the largest that stays under at any fleet size. The minimum only binds when the hour holds fewer
/// than about 720 due slots (0.025 x D / 6 &lt; 3), and the alert could only fire from that rate at 360 or fewer (18 is
/// 5% of 360); a default SQL Server target alone is about 1,300 due slots an hour, so by the defaults no fleet is in
/// that zone. The minimum is not what keeps a real hang from holding the drain: a stalled body skips one slot of each
/// one-minute collector it runs (about 19), so on a small fleet one hang reads behind at any minimum from 3 up to 19.
/// It only keeps a one- or two-slot blip from doing so.</description></item>
/// </list>
/// A wait from this arm lasts until the skips age out of the window (up to ten minutes), and the drain's own cap
/// (<see cref="RetentionCollectionYield.MaxWaitSeconds"/>) lets one batch through every five. A pass's waits together
/// stop at half its budget (<see cref="RetentionCollectionYield.WaitBudgetFraction"/>).</description></item>
/// <item><description><b>The service is still settling</b>: collection started less than <see cref="SettleWindow"/> ago.
/// The clock starts when the sweep loop starts collecting (<see cref="MarkCollectionStarted"/>), after the store
/// retries and migrations, and it is a monotonic clock, so neither a slow start nor a wall-clock step moves it. Until the
/// loop has started the signal reads settling.
/// The first collections after a start run heavy, and the share above cannot see them in their first minutes (the
/// first bodies are spread over <see cref="DarlingWorker.ColdStartSpreadSeconds"/> seconds, then queue behind the
/// gate, and a slot is only counted skipped when its run ends). The field store's first skips came 5 minutes after
/// the start and its relaunch skips 5 to 8 minutes after it, while the drain was still on small tables.</description></item>
/// </list>
/// <para>There is no arm for a body that is running long. It was tried at half the 1-minute cadence (30 s) and
/// dropped: a fleet's per-minute run-time sum passes 30 s in 22% of normal minutes (p50 15.4 s, p90 47.7 s), so any limit
/// low enough to catch the incident early fires on a normal day, and a limit above the p99 (93.9 s; the p99.9 is 623 s)
/// would need a persistence rule to be safe and would still only catch the hangs that the skip share counts the moment
/// the body ends. Even the hang rule's own 60 s is met in 172 of 1,108 five-minute windows (15.5%) of a normal 3 days
/// (265 warnings; 125 windows with one, 32 with two, 15 with three or more). A body that runs past its cadence skips its
/// next slot, which is counted here.</para>
/// </summary>
internal sealed class CollectionPressure : ICollectionPressure
{
    /// <summary>The window skipped slots are counted over. The stats keep whole minutes, so this reads 9 to 10 minutes back.</summary>
    internal const int SkipWindowMinutes = 10;

    /// <summary>
    /// The fewest skipped slots in the window that can count as behind, however large a share of a thin window they are.
    /// The largest value whose worst case stays under the alert's 20 an hour: 6 windows x (minimum - 1 + 1 slot of slop
    /// between reads) = 18 at 3, 24 at 4 (see the type's remarks).
    /// </summary>
    internal const long SkipMinCount = 3;

    /// <summary>
    /// The share of due slots (ran plus skipped) that must be skipped, in thousandths: half the self-alert's percentage,
    /// so the drain backs off before the hour reaches the alert. Integer arithmetic, so 2.5% exactly counts and 2.49% does not.
    /// </summary>
    internal const long SkipSharePerMille = DarlingSelfAlertEvaluator.FleetGateBehindPercent * 10 / 2;

    /// <summary>How long after the service starts collection counts as settling.</summary>
    internal static readonly TimeSpan SettleWindow = TimeSpan.FromMinutes(10);

    private readonly FleetGateStats? _stats;
    private readonly Func<double> _secondsClock;
    private long _startedBits = NotStarted;

    private static readonly long NotStarted = BitConverter.DoubleToInt64Bits(double.NaN);

    /// <param name="stats">The fleet gate's counts; null reads no skipped slots (a worker built without them).</param>
    /// <param name="secondsClock">A monotonic clock in seconds, injected so a test does not wait; null takes the stopwatch.</param>
    internal CollectionPressure(FleetGateStats? stats, Func<double>? secondsClock = null)
    {
        _stats = stats;
        _secondsClock = secondsClock ?? (() => Stopwatch.GetTimestamp() / (double)Stopwatch.Frequency);
    }

    /// <summary>
    /// Stamps the start of the settle window: called when the sweep loop starts collecting (#5595), not when the worker
    /// is built, because the store retries and migrations before the loop can take minutes. The first call wins.
    /// </summary>
    internal void MarkCollectionStarted() =>
        Interlocked.CompareExchange(ref _startedBits, BitConverter.DoubleToInt64Bits(_secondsClock()), NotStarted);

    /// <summary>The test: at least <see cref="SkipMinCount"/> skipped AND at least <see cref="SkipSharePerMille"/> thousandths of the due slots.</summary>
    internal static bool IsBehind(long run, long skipped) =>
        skipped >= SkipMinCount && skipped * 1000 >= (run + skipped) * SkipSharePerMille;

    /// <inheritdoc />
    public string? BehindReason()
    {
        var started = BitConverter.Int64BitsToDouble(Interlocked.Read(ref _startedBits));
        if (double.IsNaN(started))
        {
            return "collection has not started yet";
        }

        var sinceStart = _secondsClock() - started;
        if (sinceStart < SettleWindow.TotalSeconds)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"collection started {Math.Max(0, sinceStart):F0} s ago and is still settling");
        }

        var (run, skipped) = _stats?.SlotsInLastMinutes(SkipWindowMinutes) ?? (0, 0);
        if (IsBehind(run, skipped))
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"{skipped} of {run + skipped} collector slots were skipped in the last {SkipWindowMinutes} minutes ({100.0 * skipped / (run + skipped):F1}%)");
        }

        return null;
    }
}

/// <summary>
/// Makes the retention drain give way to collection (#5592). One instance per pass; it travels on the pass's
/// <see cref="RetentionWalPacer"/> (<see cref="RetentionWalPacer.Yield"/>), the one object every paced batch already
/// carries, so the paced drain loop in <see cref="DarlingRetention"/> (<c>DrainBatchesAsync</c>) is the single place
/// it is applied and no call site passes anything new.
///
/// <para>The cause it answers: the first drain after the 3.10 upgrade paced only its WAL. The field store's drain
/// deleted 18,007,201 rows in 40 minutes while collection skipped 97 of 1,252 due slots on its first line after the
/// start (7.7%; a short line, not an hour, see <see cref="CollectionPressure"/>), nine servers' bodies ran past 60 s, and
/// the Query Store collector's 95th percentile went from 13.1 s to 20.0 s. The WAL pacer does not bound the reads and
/// index work each batch does, and those compete with the collectors' writes.</para>
///
/// <para>Three rules, all of them cheap and all of them pure over the injected signal, clock and delay:</para>
/// <list type="number">
/// <item><description><b>Before each batch</b>, wait while collection is behind
/// (<see cref="ICollectionPressure"/>), re-checking every <see cref="RecheckSeconds"/>. One wait is bounded by
/// <see cref="MaxWaitSeconds"/>, so a fleet that is behind for its own reasons still sees one batch go through every
/// few minutes instead of a drain that never moves. A pass's waits together stop at half its budget
/// (<see cref="WaitBudgetFraction"/>); after that the drain keeps only the pause below. Without that cap a pass that is
/// behind the whole time spends 300 s before the first batch of every table and starts only about 6 of the 80-odd, the
/// same ones every day. The first batch of a table is not exempt: the field's relaunch skips came while the drain was
/// on the small tables, where each table needs one batch.</description></item>
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
    /// The longest one batch waits for collection before it goes anyway: half of the skipped-slot window
    /// (<see cref="CollectionPressure.SkipWindowMinutes"/> minutes). A burst of skips can hold the signal for the whole
    /// window, so a wait that lasted for it would stall the pass for ten minutes at a time; at five, a fleet that is
    /// behind for reasons of its own still gets a batch every five minutes, and a normal burst is mostly waited out.
    /// </summary>
    internal const double MaxWaitSeconds = 300;

    /// <summary>
    /// The share of a pass's wall budget its waits may use in all (#5595). Once the waits reach it the pass stops waiting
    /// and keeps only the pause after each batch, so a fleet that stays behind still walks every table.
    /// </summary>
    internal const double WaitBudgetFraction = 0.5;

    /// <summary>The pause after a batch, as a multiple of the batch's own run time. 1 means the drain deletes at most half the time.</summary>
    internal const double PauseFactor = 1.0;

    /// <summary>The longest pause after one batch, so a batch that spent minutes in a timed-out attempt does not stall the pass for as long again.</summary>
    internal const double MaxPauseSeconds = 60;

    /// <summary>
    /// The default wall budget of one pass. The daily purge and <c>purge_now</c> both run the same chores behind the
    /// purge in the same task (findings cleanup, the log sweep, the module-map refresh, the chunk-interval
    /// reconcile), and the first drain in the field held them for 40 minutes. Thirty minutes is under that, and
    /// about 2% of the 24-hour cadence. At the pause factor of 1 the field store's backlog (17.78 M rows in the wide
    /// table, 356 batches, about 29 minutes of batches; 18.0 M rows in all, 40 minutes unpaced) needs 58 to 80 minutes of
    /// batches and pauses. A wait only happens while collection is behind. On a normal day that is a few percent of the
    /// time (1 of 90 full hours met the alert's rule, and the share rule is behind for at most the 10 minutes after
    /// the skips that trip it; the six post-start bursts are the settle window's), so take 5%: a pass gets
    /// 30 x (1 - 0.05) = 28.5 minutes of work. The daily purge launches on the sweep loop's first tick, inside the settle
    /// window, so the first pass after a start spends its first 10 minutes on two 300 s waits and gets about 20 minutes of
    /// work. The first drain then takes (58 - 20) / 28.5 + 1 = 2.3 to (80 - 20) / 28.5 + 1 = 3.1 passes: 3 daily passes,
    /// 4 when collection pushes back. The waits are capped at half the budget (<see cref="WaitBudgetFraction"/>), so even a
    /// pass that is behind from start to end keeps 15 minutes of work and the drain takes at most 80 / 15 = 5.3, so 6,
    /// passes, instead of starting the same few tables every day.
    /// </summary>
    internal static readonly TimeSpan DefaultWallBudget = TimeSpan.FromMinutes(30);

    private readonly ICollectionPressure _pressure;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private readonly Func<double> _secondsClock;
    private readonly ILogger? _logger;
    private readonly double _pauseFactor;
    private readonly double _startSeconds;
    private bool _loggedFirstWait;
    private bool _loggedWaitCap;
    private bool _stopPending;
    private bool _firstStopWasEntrySkip;

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

    /// <summary>The first table the budget stopped, or never let start, or null.</summary>
    internal string? FirstStoppedTable { get; private set; }

    /// <summary>
    /// Where the pass stopped, for the run record and the log: "in &lt;table&gt;" when the budget ran out inside that table's
    /// drain, "before &lt;table&gt;" when it was already spent at the table's entry and the table was never started (#5595).
    /// Null when the pass did not stop.
    /// </summary>
    internal string? StoppedPlace =>
        FirstStoppedTable is null ? null : (_firstStopWasEntrySkip ? "before " : "in ") + FirstStoppedTable;

    /// <summary>The most the waits may add up to in one pass: <see cref="WaitBudgetFraction"/> of the wall budget.</summary>
    internal double WaitCapSeconds => WallBudget.TotalSeconds * WaitBudgetFraction;

    /// <summary>True once this pass's waits have reached <see cref="WaitCapSeconds"/>: from then on it only pauses.</summary>
    internal bool WaitsCapped => TotalWaitSeconds >= WaitCapSeconds;

    /// <summary>Tables whose drain was never started because the budget was already spent.</summary>
    internal int TablesNotReached { get; private set; }

    /// <summary>
    /// Tables that still have rows to drain because of the budget: the ones never started plus the ones stopped part-way
    /// (#5592). Above zero exactly when <see cref="StoppedOnBudget"/> is, and it is what the worker reads to schedule the
    /// continuation pass.
    /// </summary>
    internal int TablesLeft { get; private set; }

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

        /* #5595: the pass has waited as long as it may. Not even a table's first batch waits any more, so a pass that is
           behind the whole time still reaches every table; the pause after each batch stays. */
        if (WaitsCapped)
        {
            return true;
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
            var wait = Math.Min(Math.Min(RecheckSeconds, MaxWaitSeconds - waited), WaitCapSeconds - TotalWaitSeconds);
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

            if (WaitsCapped)
            {
                if (!_loggedWaitCap)
                {
                    _loggedWaitCap = true;
                    _logger?.LogInformation(
                        "Retention drain has waited {Seconds:F0} s for collection in this pass, half of its {Budget:F0}-minute budget; it stops waiting and keeps only the pause after each batch",
                        TotalWaitSeconds, WallBudget.TotalMinutes);
                }

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
        if (FirstStoppedTable is null)
        {
            FirstStoppedTable = tableName;
            _firstStopWasEntrySkip = true;
        }

        TablesNotReached++;
        TablesLeft++;
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
        TablesLeft++;
        if (rowsDeleted <= 0)
        {
            TablesNotReached++;
        }
    }

    /// <summary>
    /// The sentence the run record and the log carry when the pass waited for collection or stopped on its budget; null
    /// when it did neither, so a pass that had no pressure reads exactly as before. The ordinary pause between batches
    /// happens on every paced pass and is not pressure, so on its own it adds nothing; it is named only when the sentence
    /// is written for another reason.
    /// </summary>
    internal string? Describe()
    {
        var inv = CultureInfo.InvariantCulture;
        if (!StoppedOnBudget && TotalWaitSeconds <= 0)
        {
            return null;
        }

        var text = TotalWaitSeconds > 0
            ? string.Create(
                inv,
                $"yielded to collection: waited {TotalWaitSeconds:F0} s while it was behind, paused {TotalPauseSeconds:F0} s between batches")
            : string.Create(inv, $"paused {TotalPauseSeconds:F0} s between batches");
        if (WaitsCapped)
        {
            text += string.Create(
                inv,
                $"; stopped waiting at {WaitCapSeconds / 60:F0} minutes, half the time budget, and kept only the pauses");
        }

        if (StoppedOnBudget)
        {
            text += string.Create(
                inv,
                $"; stopped at its {WallBudget.TotalMinutes:F0}-minute time budget {StoppedPlace}, with {TablesNotReached} table(s) not reached - the next pass continues from the rows left");
        }

        return text;
    }
}
