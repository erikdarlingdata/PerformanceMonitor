/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorDashboard.Services;
using Xunit;

namespace PerformanceMonitorDashboard.Tests
{
    /// <summary>
    /// Truth-table coverage for <see cref="DeadlockAlertClearPolicy.ShouldClear"/> (#1091). Deadlock
    /// detection is edge-triggered, so the check right after a deadlock has a zero delta and would
    /// flap straight to "Deadlocks Cleared". The policy holds the alert active until a deadlock-quiet
    /// window has elapsed since the last new deadlock, matching Lite's rolling-window semantics.
    /// </summary>
    public class DeadlockAlertClearPolicyTests
    {
        private static readonly TimeSpan QuietWindow = TimeSpan.FromHours(1);
        private static readonly DateTime Now = new DateTime(2026, 6, 11, 12, 0, 0, DateTimeKind.Utc);

        [Fact]
        public void NotActive_NeverClears()
        {
            Assert.False(DeadlockAlertClearPolicy.ShouldClear(
                wasActive: false, lastDeadlockActivity: Now - TimeSpan.FromHours(2), Now, QuietWindow));
        }

        [Fact]
        public void Active_WithinQuietWindow_DoesNotFlap()
        {
            // A deadlock one minute ago: the next check must NOT clear (the #1091 flap).
            Assert.False(DeadlockAlertClearPolicy.ShouldClear(
                wasActive: true, lastDeadlockActivity: Now - TimeSpan.FromMinutes(1), Now, QuietWindow));
        }

        [Fact]
        public void Active_JustBeforeWindow_StillHeld()
        {
            Assert.False(DeadlockAlertClearPolicy.ShouldClear(
                wasActive: true, lastDeadlockActivity: Now - TimeSpan.FromMinutes(59), Now, QuietWindow));
        }

        [Fact]
        public void Active_AtQuietWindow_Clears()
        {
            Assert.True(DeadlockAlertClearPolicy.ShouldClear(
                wasActive: true, lastDeadlockActivity: Now - QuietWindow, Now, QuietWindow));
        }

        [Fact]
        public void Active_PastQuietWindow_Clears()
        {
            Assert.True(DeadlockAlertClearPolicy.ShouldClear(
                wasActive: true, lastDeadlockActivity: Now - TimeSpan.FromHours(2), Now, QuietWindow));
        }

        [Fact]
        public void Active_WithoutRecordedActivity_Clears()
        {
            // Active but no timestamp tracked: nothing holds it open, so clearing is safe.
            Assert.True(DeadlockAlertClearPolicy.ShouldClear(
                wasActive: true, lastDeadlockActivity: null, Now, QuietWindow));
        }
    }
}
