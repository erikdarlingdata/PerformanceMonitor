/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System.Text.Json;
using PerformanceMonitorDashboard.Models;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Recommendations rebuild PR-1 (D0): the new <see cref="UserPreferences.AnalysisEnabled"/>
/// setting gates analysis *production* and is independent of the notification *delivery*
/// gate (<see cref="UserPreferences.AnalysisNotificationsEnabled"/>). These tests pin the
/// defaults and the JSON persistence contract that UserPreferencesService relies on
/// (it serializes/deserializes the whole UserPreferences object with the same options).
/// </summary>
public class AnalysisEnabledPreferenceTests
{
    /// <summary>Serializer options matching UserPreferencesService (WriteIndented = true).</summary>
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    [Fact]
    public void AnalysisEnabled_DefaultsToTrue()
    {
        var prefs = new UserPreferences();
        Assert.True(prefs.AnalysisEnabled);
    }

    [Fact]
    public void AnalysisNotificationsEnabled_StillDefaultsToFalse()
    {
        // D0 keeps delivery opt-in: production is on by default, notifications are not.
        var prefs = new UserPreferences();
        Assert.False(prefs.AnalysisNotificationsEnabled);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AnalysisEnabled_RoundTripsThroughJson(bool value)
    {
        // This is exactly what UserPreferencesService.SavePreferences/LoadPreferences do:
        // JsonSerializer.Serialize(prefs) -> file -> JsonSerializer.Deserialize<UserPreferences>.
        var prefs = new UserPreferences { AnalysisEnabled = value };

        var json = JsonSerializer.Serialize(prefs, s_jsonOptions);
        var loaded = JsonSerializer.Deserialize<UserPreferences>(json);

        Assert.NotNull(loaded);
        Assert.Equal(value, loaded!.AnalysisEnabled);
    }

    [Fact]
    public void AnalysisEnabled_AbsentFromExistingJson_DeserializesToTrueDefault()
    {
        // An existing preferences.json written before this PR has no "AnalysisEnabled" key.
        // The default initializer (= true) must apply so upgraded installs get analysis on.
        const string legacyJson = """
        {
            "AnalysisNotificationsEnabled": false,
            "AnalysisIntervalMinutes": 30
        }
        """;

        var loaded = JsonSerializer.Deserialize<UserPreferences>(legacyJson);

        Assert.NotNull(loaded);
        Assert.True(loaded!.AnalysisEnabled);
        Assert.False(loaded.AnalysisNotificationsEnabled);
    }

    [Fact]
    public void AnalysisEnabled_AndNotifications_AreIndependentInJson()
    {
        // Production on, delivery off — the canonical D0 default-on/notify-off state.
        var prefs = new UserPreferences
        {
            AnalysisEnabled = true,
            AnalysisNotificationsEnabled = false
        };

        var json = JsonSerializer.Serialize(prefs, s_jsonOptions);
        var loaded = JsonSerializer.Deserialize<UserPreferences>(json);

        Assert.NotNull(loaded);
        Assert.True(loaded!.AnalysisEnabled);
        Assert.False(loaded.AnalysisNotificationsEnabled);
    }
}
