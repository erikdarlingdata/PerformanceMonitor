/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests
{
    /// <summary>
    /// Coverage for <see cref="RollingCountAlertGate"/> (#1091). The overview blocking/deadlock
    /// counts are rolling 1-hour totals, so the old level check re-fired the same event every
    /// cooldown for the whole hour it lingered in the window. These tests pin the edge-trigger
    /// semantics: fire only on a genuinely new event, never lose an event that arrives during a
    /// cooldown, and re-alert after the window shrinks and grows again.
    /// </summary>
    public class RollingCountAlertGateTests
    {
        private static RollingCountAlertGate.Decision Eval(
            int count, int threshold, int watermark, bool cooldownElapsed, bool suppressed) =>
            RollingCountAlertGate.Evaluate(count, threshold, watermark, cooldownElapsed, suppressed);

        /// <summary>
        /// The reported bug: two deadlocks sit in the rolling window and every check (spaced past
        /// the cooldown, as the user saw at 5-minute intervals) re-fired. With the gate the
        /// stable count must fire exactly once, not on every check.
        /// </summary>
        [Fact]
        public void StableCount_FiresOnceEvenWhenCooldownAlwaysElapsed()
        {
            int watermark = 0;
            int fires = 0;

            for (int i = 0; i < 12; i++)
            {
                var d = Eval(count: 2, threshold: 1, watermark, cooldownElapsed: true, suppressed: false);
                if (d.Fire) fires++;
                watermark = d.Watermark;
                Assert.True(d.Active);
            }

            Assert.Equal(1, fires);
        }

        /// <summary>A genuinely new event (count climbs above the watermark) re-fires.</summary>
        [Fact]
        public void RisingCount_FiresAgainOnTheNewEvent()
        {
            int watermark = 0;

            var first = Eval(2, 1, watermark, cooldownElapsed: true, suppressed: false);
            Assert.True(first.Fire);
            watermark = first.Watermark;

            var stable = Eval(2, 1, watermark, cooldownElapsed: true, suppressed: false);
            Assert.False(stable.Fire);
            watermark = stable.Watermark;

            var newEvent = Eval(3, 1, watermark, cooldownElapsed: true, suppressed: false);
            Assert.True(newEvent.Fire);
        }

        /// <summary>
        /// An event that arrives while the cooldown is suppressing notifications must still fire
        /// once the cooldown elapses — even if no new event lands on that exact check. This is the
        /// reason the watermark advances only on fire, never on a suppressed/cooled-down check.
        /// </summary>
        [Fact]
        public void EventArrivingDuringCooldown_FiresWhenCooldownElapses()
        {
            int watermark = 0;

            var firstDeadlock = Eval(1, 1, watermark, cooldownElapsed: true, suppressed: false);
            Assert.True(firstDeadlock.Fire);
            watermark = firstDeadlock.Watermark;

            // Second deadlock lands while the cooldown is still active.
            var duringCooldown = Eval(2, 1, watermark, cooldownElapsed: false, suppressed: false);
            Assert.False(duringCooldown.Fire);
            watermark = duringCooldown.Watermark;

            // A quiet check, cooldown still active.
            var quiet = Eval(2, 1, watermark, cooldownElapsed: false, suppressed: false);
            Assert.False(quiet.Fire);
            watermark = quiet.Watermark;

            // Cooldown elapses with no brand-new event on this check; the held deadlock must fire.
            var afterCooldown = Eval(2, 1, watermark, cooldownElapsed: true, suppressed: false);
            Assert.True(afterCooldown.Fire);
        }

        /// <summary>
        /// As old events age out the rolling count drops; the watermark decays with it so a later
        /// rise back to the same level is recognised as new and re-alerts.
        /// </summary>
        [Fact]
        public void WatermarkDecays_SoLaterRiseReAlerts()
        {
            int watermark = 0;

            var peak = Eval(2, 1, watermark, cooldownElapsed: true, suppressed: false);
            Assert.True(peak.Fire);
            Assert.Equal(2, peak.Watermark);
            watermark = peak.Watermark;

            // One event ages out: count drops to 1, still active, watermark decays to 1, no fire.
            var aged = Eval(1, 1, watermark, cooldownElapsed: true, suppressed: false);
            Assert.False(aged.Fire);
            Assert.Equal(1, aged.Watermark);
            watermark = aged.Watermark;

            // A new event brings the count back to 2 > watermark 1 -> fire.
            var risesAgain = Eval(2, 1, watermark, cooldownElapsed: true, suppressed: false);
            Assert.True(risesAgain.Fire);
        }

        /// <summary>
        /// Falling below the threshold clears the alert (Active=false) and resets the watermark so
        /// the next event re-alerts from a clean slate.
        /// </summary>
        [Fact]
        public void BelowThreshold_ClearsAndResetsWatermark()
        {
            var fired = Eval(1, 1, 0, cooldownElapsed: true, suppressed: false);
            Assert.True(fired.Fire);

            var emptied = Eval(0, 1, fired.Watermark, cooldownElapsed: true, suppressed: false);
            Assert.False(emptied.Fire);
            Assert.False(emptied.Active);
            Assert.Equal(0, emptied.Watermark);

            var nextEvent = Eval(1, 1, emptied.Watermark, cooldownElapsed: true, suppressed: false);
            Assert.True(nextEvent.Fire);
            Assert.True(nextEvent.Active);
        }

        /// <summary>
        /// Suppression (server acknowledged/silenced) never fires and never consumes the event, so
        /// the alert still fires once suppression lifts.
        /// </summary>
        [Fact]
        public void Suppressed_DoesNotFireNorConsumeEvent()
        {
            var suppressed = Eval(2, 1, 0, cooldownElapsed: true, suppressed: true);
            Assert.False(suppressed.Fire);
            Assert.True(suppressed.Active);
            Assert.Equal(0, suppressed.Watermark);

            var unsuppressed = Eval(2, 1, suppressed.Watermark, cooldownElapsed: true, suppressed: false);
            Assert.True(unsuppressed.Fire);
        }

        /// <summary>Active tracks the threshold: the alert is only "present" at or above it.</summary>
        [Theory]
        [InlineData(0, 1, false)]
        [InlineData(1, 1, true)]
        [InlineData(1, 2, false)]
        [InlineData(2, 2, true)]
        [InlineData(5, 2, true)]
        public void Active_TracksThreshold(int count, int threshold, bool expectedActive)
        {
            var d = Eval(count, threshold, watermark: 0, cooldownElapsed: true, suppressed: false);
            Assert.Equal(expectedActive, d.Active);
        }

        /// <summary>
        /// A higher threshold still edge-triggers: it goes active at the threshold, fires once for
        /// the new event, and does not re-fire while the count holds.
        /// </summary>
        [Fact]
        public void HigherThreshold_EdgeTriggersOnce()
        {
            int watermark = 0;

            var belowThreshold = Eval(1, 3, watermark, cooldownElapsed: true, suppressed: false);
            Assert.False(belowThreshold.Active);
            Assert.False(belowThreshold.Fire);
            watermark = belowThreshold.Watermark;

            var reachesThreshold = Eval(3, 3, watermark, cooldownElapsed: true, suppressed: false);
            Assert.True(reachesThreshold.Active);
            Assert.True(reachesThreshold.Fire);
            watermark = reachesThreshold.Watermark;

            var holds = Eval(3, 3, watermark, cooldownElapsed: true, suppressed: false);
            Assert.False(holds.Fire);
        }
    }
}
