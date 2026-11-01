using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Alerting;

/// <summary>
/// #7083: when a Blocking Detected alert pages, for SQL Server (<see cref="AlertEngine"/>) and the PostgreSQL
/// target path alike. A blocking event is one blocked session with its longest wait, so the figures here are
/// events and the longest wait among them, never raw reports.
///
/// <para>Two ways to page, either is enough:</para>
/// <list type="bullet">
/// <item><b>Count arm:</b> at least <c>countThreshold</c> events in the window (default 3) AND the worst wait
/// among them at least <see cref="CountArmWorstWaitFloorMs"/> (15 seconds, fixed). Three two-second blips do not page.</item>
/// <item><b>Long-event route:</b> any one event whose own wait is at or over the wait bar
/// (<c>blockingWaitSecondsThreshold</c>, 60 seconds by default), whatever the count. A bar of 0 turns the
/// route off, together with the snapshot wait arm it is the same setting for.</item>
/// </list>
/// The window is the rolling <see cref="AlertEngine.RollingCountWindowHours"/>; the edge trigger, watermark and
/// cooldown are <see cref="RollingCountAlertGate"/>'s, unchanged.
/// </summary>
public static class BlockingPageRule
{
    /// <summary>The shipped count bar since #7083 (it was 1). One place for Darling, Lite, the Viewer and the web form.</summary>
    public const int CountThresholdDefault = 3;

    /// <summary>The shipped wait bar since #7083, in seconds (it was 0, off).</summary>
    public const int WaitSecondsThresholdDefault = 60;

    /// <summary>The worst wait the count arm needs among its events (#7083). A fixed rule, not a setting.</summary>
    public const long CountArmWorstWaitFloorMs = 15_000;

    /// <summary>
    /// Runs the gate for one sweep. When the count reaches the threshold but neither arm is met (events too
    /// short), no page is raised and no incident is opened; an incident that is already open stays open, so the
    /// Cleared row still arrives exactly when the count falls under the threshold, as before.
    /// </summary>
    public static RollingCountAlertGate.Decision Evaluate(
        int count, int countThreshold, long worstWaitMs, int waitBarSeconds,
        int watermark, bool cooldownElapsed, bool suppressed, bool wasActive)
    {
        bool longEvent = waitBarSeconds > 0 && count > 0 && worstWaitMs >= waitBarSeconds * 1000L;
        bool countArm = count >= countThreshold && worstWaitMs >= CountArmWorstWaitFloorMs;

        if (longEvent || countArm)
        {
            return RollingCountAlertGate.Evaluate(
                count, longEvent ? 1 : countThreshold, watermark, cooldownElapsed, suppressed);
        }

        if (count >= countThreshold)
        {
            return new RollingCountAlertGate.Decision(
                Fire: false, Active: wasActive, Watermark: wasActive ? Math.Min(watermark, count) : 0);
        }

        return RollingCountAlertGate.Evaluate(count, countThreshold, watermark, cooldownElapsed, suppressed);
    }

    /// <summary>
    /// #7083 part 2: true when the count reached the count bar but no page is due, because every event's wait is under
    /// <see cref="CountArmWorstWaitFloorMs"/> and none is at the long-event bar. That window is held for the digest
    /// instead of vanishing. False when a count bar of 0 turns the arm off, when the server is silenced
    /// (<paramref name="suppressed"/>), and when a Blocking Detected incident is already open
    /// (<paramref name="wasActive"/>): an open page already says blocking is happening. A page that is only waiting out
    /// its cooldown or its watermark is not this case either, since its worst wait is over the floor.
    /// </summary>
    public static bool IsHeldBack(
        int count, int countThreshold, long worstWaitMs, int waitBarSeconds, bool wasActive, bool suppressed)
    {
        if (countThreshold <= 0 || count < countThreshold || wasActive || suppressed)
        {
            return false;
        }

        bool longEvent = waitBarSeconds > 0 && worstWaitMs >= waitBarSeconds * 1000L;
        return !longEvent && worstWaitMs < CountArmWorstWaitFloorMs;
    }

    /// <summary>
    /// The held-fire gate for one server: true, and the stamp written, when <see cref="IsHeldBack"/> holds and no held
    /// row was written for <paramref name="key"/> inside <paramref name="cooldown"/>. A sustained low-wait pattern
    /// therefore writes one row per cooldown. The stamp is the hold's own: it is never the page cooldown, so a real
    /// page right after a held row goes out at once.
    /// </summary>
    public static bool TryHold(
        ConcurrentDictionary<string, DateTime> lastHold, string key, DateTime now, TimeSpan cooldown,
        int count, int countThreshold, long worstWaitMs, int waitBarSeconds, bool wasActive, bool suppressed)
    {
        if (!IsHeldBack(count, countThreshold, worstWaitMs, waitBarSeconds, wasActive, suppressed))
        {
            return false;
        }

        if (LastFiredStamp.TryGet(lastHold, key, now, out var last) && now - last < cooldown)
        {
            return false;
        }

        lastHold[key] = now;
        return true;
    }

    /// <summary>
    /// The digest line for a held fire: "14 blocking events, worst wait 9 s, longest chain 2". On the PostgreSQL path
    /// the events are root blockers (<paramref name="rootBlockers"/>).
    /// </summary>
    public static string HoldReason(int count, long worstWaitMs, int longestChain, bool rootBlockers = false)
    {
        string noun = rootBlockers
            ? (count == 1 ? "root blocker" : "root blockers")
            : (count == 1 ? "blocking event" : "blocking events");
        string seconds = (Math.Max(0, worstWaitMs) / 1000.0).ToString("0.#", CultureInfo.InvariantCulture);
        return $"{count} {noun}, worst wait {seconds} s, longest chain {Math.Max(1, longestChain)}";
    }

    /// <summary>
    /// The longest blocking chain among the events of one window, counted in blocked sessions stacked behind a root
    /// (A blocks B is 1, A blocks B blocks C is 2). Each pair is (blocked session, its blocker); a pair naming the same
    /// session twice is a self-wait and is skipped, and a loop stops at the first session seen twice. Zero when there
    /// are no usable pairs.
    /// </summary>
    public static int LongestChain(IEnumerable<(int Blocked, int Blocking)> pairs)
    {
        var blockerOf = new Dictionary<int, int>();
        foreach (var (blocked, blocking) in pairs)
        {
            if (blocked != blocking)
            {
                blockerOf.TryAdd(blocked, blocking);
            }
        }

        int best = 0;
        foreach (var start in blockerOf.Keys)
        {
            var seen = new HashSet<int> { start };
            int length = 0;
            int current = start;
            while (blockerOf.TryGetValue(current, out var blocker) && seen.Add(blocker))
            {
                length++;
                current = blocker;
            }

            best = Math.Max(best, length);
        }

        return best;
    }

    /// <summary>
    /// The Cleared text: says what clears the alert. It is the rolling window and not the length of the
    /// blocking that sets the timing, so the text names the window.
    /// </summary>
    public static string ClearedMessage(string serverName, int countThreshold, int waitBarSeconds)
    {
        int windowMinutes = AlertEngine.RollingCountWindowHours * 60;
        string longClause = waitBarSeconds > 0 ? $" and none of them waited {waitBarSeconds}s" : "";
        return $"{serverName}: Blocking cleared. The alert clears when the last {windowMinutes} minutes hold fewer than "
            + $"{countThreshold} blocked events{longClause}, so it can arrive up to {windowMinutes} minutes after the blocking ended";
    }
}
