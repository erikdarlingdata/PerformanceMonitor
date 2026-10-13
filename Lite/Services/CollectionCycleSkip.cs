using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitorLite.Services;

/// <summary>One timed step of a collection cycle: the name the log line and the collection_log row use, and its length.</summary>
internal readonly record struct CycleStep(string Name, long ElapsedMs);

/// <summary>
/// #5643: what the collection loop says about a cycle that missed collection slots. The same text is the Warning log
/// line and the <c>error_message</c> of the per-server <c>collection_log</c> row, so the two never disagree.
/// </summary>
/// <param name="SlotsSkipped">Whole collection intervals that passed with no cycle: the row's <c>fanout_item_count</c>.</param>
/// <param name="CycleMs">The late cycle's total length: the row's <c>duration_ms</c>.</param>
/// <param name="SlowestStep">The slowest step, or <c>wait</c> when the wait was the longer: the row's <c>slowest_item</c>.</param>
/// <param name="SlowestStepMs">The slowest step's length: the row's <c>slowest_item_ms</c>.</param>
/// <param name="Message">The Warning line and the row's <c>error_message</c>.</param>
internal sealed record CycleSkipReport(int SlotsSkipped, int CycleMs, string SlowestStep, int SlowestStepMs, string Message);

/// <summary>
/// #5643: detects the collection slots a late cycle skipped, and words the report. The loop skips a missed grid slot
/// rather than replaying it (<see cref="CollectionBackgroundService.WaitForNextCycleAsync"/>, #4728), so a long
/// housekeeping step or a store-lock stall used to leave a gap in every chart with no log line and no
/// <c>collection_log</c> row: Collection Health stayed green. Static, with no clock of its own, so a test drives it
/// with the stamps a fake-clock wait returns.
/// </summary>
internal static class CollectionCycleSkip
{
    /// <summary>The <c>collection_log.collector_name</c> of a skipped-slots row. It is not a collector: every reader of
    /// the log leaves it out of any collector list, rate, streak and "last success".</summary>
    internal const string CollectorName = "collection_cycle";

    /// <summary>The <c>collection_log.status</c> of a skipped-slots row.</summary>
    internal const string Status = "SKIPPED";

    /// <summary>The step name the row carries when the cycle was short and the wait between cycles overran.</summary>
    internal const string WaitStepName = "wait";

    /// <summary>
    /// The whole collection intervals between the slot that should have come next (<paramref name="previousCycleStart"/>
    /// plus one interval) and the stamp the wait returned. 0 for a cycle on time, for a stamp a hair early or late
    /// (less than one interval off), and for a stamp before the expected slot: #4732's wall clock that stepped
    /// backwards re-anchors the grid at the clock's reading, which is not a skipped slot.
    /// </summary>
    internal static int CountSkippedSlots(DateTime previousCycleStart, DateTime nextCycleStart, TimeSpan interval)
    {
        if (interval <= TimeSpan.Zero)
        {
            return 0;
        }

        var expected = previousCycleStart + interval;
        var ahead = nextCycleStart - expected;
        if (ahead < interval)
        {
            return 0;
        }

        return (int)Math.Min(int.MaxValue, ahead.Ticks / interval.Ticks);
    }

    /// <summary>
    /// The report for a cycle that skipped <paramref name="slotsSkipped"/> slots, or null for none. Each part of the
    /// gap is attributed on its own. The cycle is a cause when it ran a whole interval or more, and the line lists its
    /// slowest steps. The wait is a cause when it ran past its slot by a whole interval or more (the wait's length minus
    /// the time left in the interval after the cycle): the computer slept or hibernated, or the clock stepped forward,
    /// and the line says so with the wait's length. When both hold the line prints both, and says the slots passed
    /// across the two. With neither (the stamps alone show a gap) the line is the wait's. The slot count is always the
    /// whole gap. When the two stamps are given the line ends by naming the gap from one to the other.
    /// </summary>
    internal static CycleSkipReport? Build(
        int slotsSkipped, TimeSpan cycleElapsed, TimeSpan waitElapsed, IReadOnlyList<CycleStep> steps, TimeSpan interval,
        DateTime? previousCycleStart = null, DateTime? nextCycleStart = null)
    {
        if (slotsSkipped <= 0)
        {
            return null;
        }

        var cycleMs = ClampMs(cycleElapsed.TotalMilliseconds);
        var waitMs = ClampMs(waitElapsed.TotalMilliseconds);
        var intervalText = Format((long)interval.TotalMilliseconds);

        var timeLeftAfterCycle = interval > cycleElapsed ? interval - cycleElapsed : TimeSpan.Zero;
        var cycleCaused = cycleElapsed >= interval;
        var waitCaused = waitElapsed - timeLeftAfterCycle >= interval;

        var slowest = steps.Where(s => s.ElapsedMs > 0).OrderByDescending(s => s.ElapsedMs).Take(3).ToList();
        var listed = slowest.Count == 0
            ? "none timed"
            : string.Join(", ", slowest.Select(s => s.Name.Replace('_', ' ') + " " + Format(s.ElapsedMs)));
        var top = slowest.Count == 0 ? new CycleStep("collectors", 0) : slowest[0];
        var gap = GapSentence(previousCycleStart, nextCycleStart);

        if (cycleCaused && waitCaused)
        {
            var both = string.Create(CultureInfo.InvariantCulture,
                $"Collection cycle took {Format(cycleMs)} and the wait overran, so {slotsSkipped} collection slot(s) were skipped across the two. Slowest steps: {listed}. The wait lasted {Format(waitMs)} against a {intervalText} interval; the computer may have slept or hibernated, or the clock stepped forward.{gap}");
            return waitMs > top.ElapsedMs
                ? new CycleSkipReport(slotsSkipped, cycleMs, WaitStepName, waitMs, both)
                : new CycleSkipReport(slotsSkipped, cycleMs, top.Name, ClampMs(top.ElapsedMs), both);
        }

        if (cycleCaused)
        {
            var message = string.Create(CultureInfo.InvariantCulture,
                $"Collection cycle took {Format(cycleMs)}, so {slotsSkipped} collection slot(s) were skipped. Slowest steps: {listed}.{gap}");
            return new CycleSkipReport(slotsSkipped, cycleMs, top.Name, ClampMs(top.ElapsedMs), message);
        }

        var overran = string.Create(CultureInfo.InvariantCulture,
            $"Collection wait overran: it lasted {Format(waitMs)} against a {intervalText} interval, so {slotsSkipped} collection slot(s) were skipped. The cycle itself took {Format(cycleMs)}; the computer may have slept or hibernated, or the clock stepped forward.{gap}");
        return new CycleSkipReport(slotsSkipped, cycleMs, WaitStepName, waitMs, overran);
    }

    /// <summary>" The gap ran from X UTC to Y UTC.", or nothing when either stamp is missing. No equals sign: the baseline
    /// marker read keys on one.</summary>
    private static string GapSentence(DateTime? from, DateTime? to)
        => from.HasValue && to.HasValue
            ? string.Create(CultureInfo.InvariantCulture,
                $" The gap ran from {from.Value:yyyy-MM-dd HH:mm:ss} UTC to {to.Value:yyyy-MM-dd HH:mm:ss} UTC.")
            : "";

    /// <summary>Milliseconds below one second, one decimal below ten seconds, whole seconds from ten seconds up.</summary>
    internal static string Format(long ms)
    {
        if (ms < 1000)
        {
            return ms.ToString(CultureInfo.InvariantCulture) + "ms";
        }

        return ms < 10_000
            ? (ms / 1000.0).ToString("0.#", CultureInfo.InvariantCulture) + "s"
            : Math.Round(ms / 1000.0).ToString("0", CultureInfo.InvariantCulture) + "s";
    }

    private static int ClampMs(double ms) => (int)Math.Clamp(ms, 0, int.MaxValue);
}
