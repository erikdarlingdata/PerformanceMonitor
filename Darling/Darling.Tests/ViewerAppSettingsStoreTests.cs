/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the viewer's application-settings store — the Darling equivalent of Lite's settings.json that the
/// full Settings window (the port of Lite's SettingsWindow) reads and writes: MCP, every alert threshold and
/// toggle, notification options, SMTP/webhook (non-secret) config, and the automated-analysis knobs. Covers
/// the four behaviors the store promises: Lite-matching defaults when the file is missing, a lossless
/// save/load round-trip across sections, clamping out-of-range values back into range (a corrupt / hand-edited
/// / forward-version file must never seed an out-of-range control), and defaults on a corrupt file. Each store
/// points at a unique temp file so nothing touches the real profile. Secrets (SMTP password, webhook URLs) are
/// deliberately NOT covered here — they live in Windows Credential Manager, not this file.
/// </summary>
public sealed class ViewerAppSettingsStoreTests : IDisposable
{
    private readonly string _tempFile =
        Path.Combine(Path.GetTempPath(), $"viewer-settings-test-{Guid.NewGuid():N}.json");

    public void Dispose()
    {
        if (File.Exists(_tempFile))
        {
            File.Delete(_tempFile);
        }
    }

    [Fact]
    public void Load_WhenFileMissing_ReturnsLiteMatchingDefaults()
    {
        var settings = new ViewerAppSettingsStore(_tempFile).Load();

        /* Spot-check one value per section — every default mirrors Lite's App.* exactly, except the MCP
           port, which is Darling's 5152 (avoids Lite 5151 / Dashboard 5150). */
        Assert.False(settings.McpEnabled);
        Assert.Equal(5152, settings.McpPort);
        Assert.Equal(5, settings.ConnectionTimeoutSeconds);
        Assert.Equal(30, settings.NocRefreshIntervalSeconds);
        Assert.Equal("ServerTime", settings.TimeDisplayMode);
        Assert.True(settings.AlertsEnabled);
        Assert.Equal(80, settings.AlertCpuThreshold);
        Assert.Equal("Total", settings.AlertCpuMode);
        Assert.Equal(500, settings.AlertPoisonWaitThresholdMs);
        Assert.Equal("Summary", settings.AlertDeliveryMode);
        Assert.Equal("24 hours", settings.MuteRuleDefaultExpiration);
        Assert.Equal(1.5, settings.AnalysisNotifySeverity);
        Assert.False(settings.SmtpEnabled);
        Assert.Equal(587, settings.SmtpPort);
        Assert.True(settings.SmtpUseSsl);
        Assert.Empty(settings.AlertExcludedDatabases);
    }

    [Fact]
    public void SaveThenLoad_RoundTripsValuesAcrossEverySection()
    {
        var store = new ViewerAppSettingsStore(_tempFile);
        store.Save(new ViewerAppSettings
        {
            McpEnabled = true,
            McpPort = 5200,
            ConnectionTimeoutSeconds = 30,
            NocRefreshIntervalSeconds = 120,
            CsvSeparator = ";",
            TimeDisplayMode = "UTC",
            OverviewSortMode = "Name",
            AlertsEnabled = false,
            AlertCpuThreshold = 65,
            AlertCpuMode = "SqlOnly",
            AlertBlockingThreshold = 3,
            AlertLongRunningQueryMaxResults = 12,
            AlertLongRunningQueryExcludeCdc = false,
            AlertExcludedDatabases = new List<string> { "msdb", "tempdb", "DBAtools" },
            AlertLowDiskThresholdGb = 20,
            AlertDeliveryMode = "PerEvent",
            AlertPerEventMaxPerCycle = 25,
            MuteRuleDefaultExpiration = "7 days",
            LogAlertDismissals = false,
            AnalysisEnabled = false,
            AnalysisIntervalMinutes = 120,
            AnalysisNotifySeverity = 0.5,
            SmtpEnabled = true,
            SmtpServer = "smtp.example.com",
            SmtpPort = 2525,
            SmtpUseSsl = false,
            SmtpUsername = "monitor",
            SmtpFromAddress = "alerts@example.com",
            SmtpRecipients = "dba@example.com",
            TeamsWebhookEnabled = true,
            TeamsProxyAddress = "http://proxy.corp:8080",
            SlackWebhookEnabled = true,
            SlackProxyAddress = "http://proxy.corp:3128",
        });

        /* A fresh store on the same path proves the values survived a real serialize -> file -> deserialize. */
        var reloaded = new ViewerAppSettingsStore(_tempFile).Load();

        Assert.True(reloaded.McpEnabled);
        Assert.Equal(5200, reloaded.McpPort);
        Assert.Equal(30, reloaded.ConnectionTimeoutSeconds);
        Assert.Equal(120, reloaded.NocRefreshIntervalSeconds);
        Assert.Equal(";", reloaded.CsvSeparator);
        Assert.Equal("UTC", reloaded.TimeDisplayMode);
        Assert.Equal("Name", reloaded.OverviewSortMode);
        Assert.False(reloaded.AlertsEnabled);
        Assert.Equal(65, reloaded.AlertCpuThreshold);
        Assert.Equal("SqlOnly", reloaded.AlertCpuMode);
        Assert.Equal(3, reloaded.AlertBlockingThreshold);
        Assert.Equal(12, reloaded.AlertLongRunningQueryMaxResults);
        Assert.False(reloaded.AlertLongRunningQueryExcludeCdc);
        Assert.Equal(new[] { "msdb", "tempdb", "DBAtools" }, reloaded.AlertExcludedDatabases);
        Assert.Equal(20, reloaded.AlertLowDiskThresholdGb);
        Assert.Equal("PerEvent", reloaded.AlertDeliveryMode);
        Assert.Equal(25, reloaded.AlertPerEventMaxPerCycle);
        Assert.Equal("7 days", reloaded.MuteRuleDefaultExpiration);
        Assert.False(reloaded.LogAlertDismissals);
        Assert.False(reloaded.AnalysisEnabled);
        Assert.Equal(120, reloaded.AnalysisIntervalMinutes);
        Assert.Equal(0.5, reloaded.AnalysisNotifySeverity);
        Assert.True(reloaded.SmtpEnabled);
        Assert.Equal("smtp.example.com", reloaded.SmtpServer);
        Assert.Equal(2525, reloaded.SmtpPort);
        Assert.False(reloaded.SmtpUseSsl);
        Assert.Equal("monitor", reloaded.SmtpUsername);
        Assert.Equal("alerts@example.com", reloaded.SmtpFromAddress);
        Assert.Equal("dba@example.com", reloaded.SmtpRecipients);
        Assert.True(reloaded.TeamsWebhookEnabled);
        Assert.Equal("http://proxy.corp:8080", reloaded.TeamsProxyAddress);
        Assert.True(reloaded.SlackWebhookEnabled);
        Assert.Equal("http://proxy.corp:3128", reloaded.SlackProxyAddress);
    }

    [Fact]
    public void Load_ClampsOutOfRangeValues_BackIntoRange()
    {
        /* Persist deliberately out-of-range numerics and unknown enum strings by writing the JSON directly,
           then confirm Load normalizes each one so no control is ever seeded with a bad value. */
        File.WriteAllText(
            _tempFile,
            """
            {
              "McpPort": 0,
              "ConnectionTimeoutSeconds": 1,
              "NocRefreshIntervalSeconds": 9999,
              "AlertCpuThreshold": 999,
              "AlertCpuMode": "Bogus",
              "AlertLongRunningQueryMaxResults": 99999,
              "AlertCooldownMinutes": 500,
              "AnalysisIntervalMinutes": 2,
              "AnalysisNotifySeverity": 5.0,
              "AlertDeliveryMode": "Whenever",
              "MuteRuleDefaultExpiration": "Whenever",
              "TimeDisplayMode": "Martian",
              "OverviewSortMode": "Bogus",
              "CsvSeparator": "|"
            }
            """);

        var settings = new ViewerAppSettingsStore(_tempFile).Load();

        Assert.Equal(5152, settings.McpPort);                 // 0 (unset) -> Darling default
        Assert.Equal(5, settings.ConnectionTimeoutSeconds);   // below min 5 -> 5
        Assert.Equal(600, settings.NocRefreshIntervalSeconds); // above max 600 -> 600
        Assert.Equal(100, settings.AlertCpuThreshold);        // above max 100 -> 100
        Assert.Equal("Total", settings.AlertCpuMode);         // unknown -> Total
        Assert.Equal(1000, settings.AlertLongRunningQueryMaxResults); // above max 1000 -> 1000
        Assert.Equal(120, settings.AlertCooldownMinutes);     // above max 120 -> 120
        Assert.Equal(5, settings.AnalysisIntervalMinutes);    // below min 5 -> 5
        Assert.Equal(2.0, settings.AnalysisNotifySeverity);   // above max 2.0 -> 2.0
        Assert.Equal("Summary", settings.AlertDeliveryMode);  // unknown -> Summary
        Assert.Equal("24 hours", settings.MuteRuleDefaultExpiration); // unknown -> 24 hours
        Assert.Equal("ServerTime", settings.TimeDisplayMode); // unknown -> ServerTime
        Assert.Equal("Cpu", settings.OverviewSortMode);       // unknown -> Cpu
        Assert.Contains(settings.CsvSeparator, new[] { ",", ";", "\t" }); // unknown -> a valid locale default
    }

    /// <summary>#5847: a hand-edited Alert Details size is bounded as Lite's loader bounds it (0..10000), so the window
    /// the size store restores cannot open 99999 pixels wide; a negative size reads as never resized (the default).</summary>
    [Theory]
    [InlineData(99999, 99999, 10000, 10000)]
    [InlineData(-5, 700, 0, 700)]
    [InlineData(900, 650, 900, 650)]
    public void Load_BoundsTheAlertDetailWindowSize(double storedWidth, double storedHeight, double width, double height)
    {
        File.WriteAllText(
            _tempFile,
            $$"""{ "AlertDetailWindowWidth": {{storedWidth}}, "AlertDetailWindowHeight": {{storedHeight}} }""");
        var store = new ViewerAppSettingsStore(_tempFile);

        var settings = store.Load();
        var restored = new AlertDetailWindow.SettingsSizeStore(store).Load();

        Assert.Equal(width, settings.AlertDetailWindowWidth);
        Assert.Equal(height, settings.AlertDetailWindowHeight);
        Assert.Equal(width > 0 && height > 0 ? new System.Windows.Size(width, height) : null, restored);
    }

    [Fact]
    public void Load_WhenFileCorrupt_ReturnsDefaults()
    {
        File.WriteAllText(_tempFile, "{ not valid json");

        var settings = new ViewerAppSettingsStore(_tempFile).Load();

        Assert.Equal(5152, settings.McpPort);
        Assert.True(settings.AlertsEnabled);
        Assert.Equal(80, settings.AlertCpuThreshold);
    }

    [Fact]
    public void ViewerExportSettings_Apply_SeedsTheRuntimeCsvSeparator()
    {
        /* The one persisted setting the viewer honors at runtime today: grid CSV export reads this static. */
        ViewerExportSettings.Apply(new ViewerAppSettings { CsvSeparator = ";" });
        Assert.Equal(";", ViewerExportSettings.CsvSeparator);

        ViewerExportSettings.Apply(new ViewerAppSettings { CsvSeparator = "\t" });
        Assert.Equal("\t", ViewerExportSettings.CsvSeparator);
    }

    [Fact]
    public void Save_CreatesTheAppDataDirectory_WhenItDoesNotExist()
    {
        var nestedDir = Path.Combine(Path.GetTempPath(), $"viewer-settings-dir-{Guid.NewGuid():N}");
        var nestedFile = Path.Combine(nestedDir, "viewer-settings.json");
        try
        {
            new ViewerAppSettingsStore(nestedFile).Save(new ViewerAppSettings());

            Assert.True(File.Exists(nestedFile));
        }
        finally
        {
            if (Directory.Exists(nestedDir))
            {
                Directory.Delete(nestedDir, recursive: true);
            }
        }
    }
}
