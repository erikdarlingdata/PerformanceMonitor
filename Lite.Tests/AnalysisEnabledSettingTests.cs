/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Recommendations rebuild PR-1 (D0): App.AnalysisEnabled gates analysis *production*
/// (run + persist on the independent AnalysisIntervalMinutes cadence) and is independent
/// of App.AnalysisNotificationsEnabled, which gates notification *delivery* only.
///
/// Note: App.* are process-wide mutable statics that other parallel test classes also
/// touch. This test asserts the AnalysisEnabled default once and otherwise only reads it,
/// to avoid flaking sibling classes — matching the discipline documented in
/// AppAlertSettingsTests.
/// </summary>
public class AnalysisEnabledSettingTests
{
    [Fact]
    public void AnalysisEnabled_And_NotificationsEnabled_AreSeparateProperties()
    {
        // Setting one must not move the other — they are independent gates (production vs delivery).
        App.AnalysisEnabled = true;
        App.AnalysisNotificationsEnabled = false;

        Assert.True(App.AnalysisEnabled);
        Assert.False(App.AnalysisNotificationsEnabled);

        App.AnalysisEnabled = false;
        App.AnalysisNotificationsEnabled = true;

        Assert.False(App.AnalysisEnabled);
        Assert.True(App.AnalysisNotificationsEnabled);

        // Restore the shipped default so sibling test classes observe the production default.
        App.AnalysisEnabled = true;
    }

    /// <summary>#3464: analysis notification DELIVERY is the master alerts switch AND the family toggle —
    /// the same AND'd idiom Lite's connection edge uses — where before this the family toggle decided
    /// alone, so an operator who flipped the master switch off still got analysis mail (the measured
    /// Darling incident delivered 38 minutes into a fleet-wide mute; Lite's gate had the identical shape).
    /// Production stays ungated by BOTH (the D0 split this file already pins).</summary>
    [Fact]
    public void ShouldNotifyAnalysisFindings_RequiresTheMasterSwitch_AndTheFamilyToggle()
    {
        try
        {
            /* The measured bypass: family toggle on, master OFF — must not deliver. */
            App.AlertsEnabled = false;
            App.AnalysisNotificationsEnabled = true;
            Assert.False(CollectionBackgroundService.ShouldNotifyAnalysisFindings());

            /* The family toggle keeps working under master on — it is the narrower knob, not a synonym. */
            App.AlertsEnabled = true;
            App.AnalysisNotificationsEnabled = false;
            Assert.False(CollectionBackgroundService.ShouldNotifyAnalysisFindings());

            App.AlertsEnabled = false;
            App.AnalysisNotificationsEnabled = false;
            Assert.False(CollectionBackgroundService.ShouldNotifyAnalysisFindings());

            /* Both on — the only combination that delivers. */
            App.AlertsEnabled = true;
            App.AnalysisNotificationsEnabled = true;
            Assert.True(CollectionBackgroundService.ShouldNotifyAnalysisFindings());
        }
        finally
        {
            /* Shipped defaults, restored for sibling classes (this file's own discipline). */
            App.AlertsEnabled = true;
            App.AnalysisNotificationsEnabled = false;
        }
    }
}
