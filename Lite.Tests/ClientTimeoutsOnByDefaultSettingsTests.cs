/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json.Nodes;
using PerformanceMonitorLite;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5736: the Client Timeouts alert ships on. Lite's Settings save wrote the WHOLE alert block, so a settings.json saved by an older build
/// holds <c>alert_client_timeouts_enabled</c> = false, which is the old default and not a choice. Such a file must load with the alert on.
/// A file saved by this build carries the marker beside the switch, and an alert switched off from then on stays off.
/// Shares the <c>app-alert-statics</c> collection with the other classes that drive <c>App.LoadAlertSettings</c>.
/// </summary>
[Collection("app-alert-statics")]
public class ClientTimeoutsOnByDefaultSettingsTests
{
    private static void FailOnWrite(string key, string value) =>
        Assert.Fail($"LoadAlertSettings wrote credential '{key}' when nothing should have been saved.");

    private static bool LoadFrom(string json)
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pmlite_ctdefault_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "settings.json"), json);
        var before = App.AlertClientTimeoutsEnabled;
        try
        {
            /* A start: the static holds its shipped default before the file is read. */
            App.AlertClientTimeoutsEnabled = true;
            App.LoadAlertSettings(dir, key => "stored:" + key, FailOnWrite);
            return App.AlertClientTimeoutsEnabled;
        }
        finally
        {
            App.AlertClientTimeoutsEnabled = before;
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void AFileSavedByAnOlderBuild_WithTheSwitchFalse_LoadsWithTheAlertOn() =>
        Assert.True(LoadFrom("""{ "alerts_enabled": true, "alert_client_timeouts_enabled": false }"""));

    [Fact]
    public void AFileWithNoSwitchAtAll_LoadsWithTheAlertOn() =>
        Assert.True(LoadFrom("""{ "alerts_enabled": true }"""));

    [Fact]
    public void AFileSavedByThisBuild_WithTheSwitchOff_KeepsTheAlertOff() =>
        Assert.False(LoadFrom("""{ "alert_client_timeouts_enabled": false, "alert_client_timeouts_on_by_default_applied": true }"""));

    [Fact]
    public void TheSettingsSave_WritesTheSwitchBesideItsMarker_SoAnOffSurvivesTheNextStart()
    {
        var before = App.AlertClientTimeoutsEnabled;
        try
        {
            App.AlertClientTimeoutsEnabled = false;
            var root = new JsonObject();

            App.WriteClientTimeoutsSwitch(root);

            Assert.False(root["alert_client_timeouts_enabled"]!.GetValue<bool>());
            Assert.True(root["alert_client_timeouts_on_by_default_applied"]!.GetValue<bool>());
            Assert.False(LoadFrom(root.ToJsonString()));
        }
        finally
        {
            App.AlertClientTimeoutsEnabled = before;
        }
    }
}
