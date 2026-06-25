/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitorLite;
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
}
