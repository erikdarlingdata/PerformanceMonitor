/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Alerting;
using PerformanceMonitorLite;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7083: the blocking bars move to 3 and 60. A settings.json saved by an older build holds 1 and 0, the old defaults and not choices, so
/// without the marker those load as the new defaults and any other value is kept. With the marker (written by every save of this build) the saved
/// values are choices. Shares the <c>app-alert-statics</c> collection with the other classes that drive <c>App.LoadAlertSettings</c>.
/// </summary>
[Collection("app-alert-statics")]
public class BlockingBarsDefaultsSettingsTests
{
    private static void FailOnWrite(string key, string value) =>
        Assert.Fail($"LoadAlertSettings wrote credential '{key}' when nothing should have been saved.");

    private static (int Count, int Wait) LoadFrom(string json)
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pmlite_blockbars_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "settings.json"), json);
        var beforeCount = App.AlertBlockingThreshold;
        var beforeWait = App.AlertBlockingWaitSecondsThreshold;
        try
        {
            App.AlertBlockingThreshold = BlockingPageRule.CountThresholdDefault;
            App.AlertBlockingWaitSecondsThreshold = BlockingPageRule.WaitSecondsThresholdDefault;
            App.LoadAlertSettings(dir, key => "stored:" + key, FailOnWrite);
            return (App.AlertBlockingThreshold, App.AlertBlockingWaitSecondsThreshold);
        }
        finally
        {
            App.AlertBlockingThreshold = beforeCount;
            App.AlertBlockingWaitSecondsThreshold = beforeWait;
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void AFileSavedByAnOlderBuild_At1And0_LoadsTheNewBars() =>
        Assert.Equal((3, 60), LoadFrom("""{ "alert_blocking_threshold": 1, "alert_blocking_wait_seconds_threshold": 0 }"""));

    [Fact]
    public void AFileSavedByAnOlderBuild_WithOtherValues_KeepsThem() =>
        Assert.Equal((5, 30), LoadFrom("""{ "alert_blocking_threshold": 5, "alert_blocking_wait_seconds_threshold": 30 }"""));

    [Fact]
    public void AFileWithNoBars_LoadsTheNewBars() =>
        Assert.Equal((3, 60), LoadFrom("""{ "alerts_enabled": true }"""));

    [Fact]
    public void AFileSavedByThisBuild_At1And0_KeepsThem() =>
        Assert.Equal((1, 0), LoadFrom("""{ "alert_blocking_threshold": 1, "alert_blocking_wait_seconds_threshold": 0, "alert_blocking_defaults_applied": true }"""));
}
