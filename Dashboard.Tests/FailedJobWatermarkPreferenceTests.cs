/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using PerformanceMonitorDashboard.Models;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Failed-job tray watermark persistence (the Dashboard half of the #1145 parity fix): a reopen
/// must not re-fire tray toasts for failures still inside the lookback window that the user already
/// saw and dismissed. The watermark is the newest already-alerted failure's server-local run time,
/// stored as <see cref="DateTime.Ticks"/> in <see cref="UserPreferences.FailedJobAlertWatermarkTicks"/>.
/// These pin the JSON persistence contract UserPreferencesService relies on (it serializes/
/// deserializes the whole UserPreferences object with the same options).
/// </summary>
public class FailedJobWatermarkPreferenceTests
{
    /// <summary>Serializer options matching UserPreferencesService (WriteIndented = true).</summary>
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    [Fact]
    public void FailedJobAlertWatermarkTicks_DefaultsToEmpty()
    {
        var prefs = new UserPreferences();
        Assert.NotNull(prefs.FailedJobAlertWatermarkTicks);
        Assert.Empty(prefs.FailedJobAlertWatermarkTicks);
    }

    [Fact]
    public void FailedJobAlertWatermarkTicks_RoundTripsExactValue_ThroughJson()
    {
        /* Ticks keep the server-local run time basis-exact across the JSON round-trip — the value is
           compared directly against FailedJobInfo.RunDateTime, so DateTimeKind drift would corrupt it. */
        var serverLocal = new DateTime(2026, 6, 19, 14, 30, 15);
        var prefs = new UserPreferences
        {
            FailedJobAlertWatermarkTicks = new Dictionary<string, long>
            {
                ["server-a"] = serverLocal.Ticks,
                ["server-b"] = new DateTime(2026, 6, 19, 9, 5, 0).Ticks
            }
        };

        var json = JsonSerializer.Serialize(prefs, s_jsonOptions);
        var loaded = JsonSerializer.Deserialize<UserPreferences>(json);

        Assert.NotNull(loaded);
        Assert.Equal(2, loaded!.FailedJobAlertWatermarkTicks.Count);
        Assert.Equal(serverLocal, new DateTime(loaded.FailedJobAlertWatermarkTicks["server-a"]));
        Assert.Equal(new DateTime(2026, 6, 19, 9, 5, 0), new DateTime(loaded.FailedJobAlertWatermarkTicks["server-b"]));
    }

    [Fact]
    public void FailedJobAlertWatermarkTicks_AbsentFromExistingJson_DeserializesToEmpty()
    {
        // A preferences.json written before this PR has no "FailedJobAlertWatermarkTicks" key;
        // the default initializer (= empty dict) must apply so upgraded installs don't NRE on seed.
        const string legacyJson = """
        {
            "AnalysisIntervalMinutes": 30
        }
        """;

        var loaded = JsonSerializer.Deserialize<UserPreferences>(legacyJson);

        Assert.NotNull(loaded);
        Assert.NotNull(loaded!.FailedJobAlertWatermarkTicks);
        Assert.Empty(loaded.FailedJobAlertWatermarkTicks);
    }
}
