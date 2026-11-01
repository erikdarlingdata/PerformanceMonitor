using System;

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
