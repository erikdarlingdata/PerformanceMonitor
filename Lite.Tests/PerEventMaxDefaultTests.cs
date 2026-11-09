/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Common;
using PerformanceMonitorLite;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The per-event delivery cap, <see cref="App.AlertPerEventMaxPerCycle"/>, ships at
/// <see cref="AlertDefaults.PerEventMax"/>, the one figure both apps read, so Lite and the Darling service
/// split the same cycle of incidents at the same point. The settings loader only assigns the property when
/// settings.json carries <c>alert_per_event_max_per_cycle</c>, so a file that set its own value keeps it and a
/// file that never set one gets the shipped default.
/// Shares the <c>app-alert-statics</c> collection with the other classes that drive <c>App.LoadAlertSettings</c>.
/// </summary>
[Collection("app-alert-statics")]
public sealed class PerEventMaxDefaultTests
{
    private static void FailOnWrite(string key, string value) =>
        Assert.Fail($"LoadAlertSettings wrote credential '{key}' when nothing should have been saved.");

    private static int LoadFrom(string json, int before)
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pmlite_pereventmax_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "settings.json"), json);
        var saved = App.AlertPerEventMaxPerCycle;
        try
        {
            App.AlertPerEventMaxPerCycle = before;
            App.LoadAlertSettings(dir, key => "stored:" + key, FailOnWrite);
            return App.AlertPerEventMaxPerCycle;
        }
        finally
        {
            App.AlertPerEventMaxPerCycle = saved;
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// The initializer's value, as <see cref="ShippedAlertStatics"/> recorded it when the assembly loaded, before any
    /// test could write the static. The static itself is not read here: the other classes in this collection reset it
    /// to the constant, so its value after one of them ran would prove nothing about the initializer.
    /// </summary>
    [Fact]
    public void TheShippedDefault_IsTheSharedConstant() =>
        Assert.Equal(AlertDefaults.PerEventMax, ShippedAlertStatics.AlertPerEventMaxPerCycle);

    [Fact]
    public void AFileThatSetTheCap_KeepsItsValue_AcrossTheDefaultChange() =>
        Assert.Equal(10, LoadFrom("""{ "alerts_enabled": true, "alert_per_event_max_per_cycle": 10 }""", AlertDefaults.PerEventMax));

    [Fact]
    public void AFileWithNoCap_LeavesTheShippedDefaultInPlace() =>
        Assert.Equal(AlertDefaults.PerEventMax, LoadFrom("""{ "alerts_enabled": true }""", AlertDefaults.PerEventMax));

    [Fact]
    public void AFileWithNoCap_DoesNotWriteTheProperty() =>
        Assert.Equal(42, LoadFrom("""{ "alerts_enabled": true }""", 42));
}
