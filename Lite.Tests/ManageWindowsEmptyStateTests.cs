/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A management window with nothing to list says so inside its grid, instead of showing a header row over empty space: Manage
/// Mute Rules with no rules, and Credential Profiles with no profiles. Each window is the real one, rendered with the theme. The
/// Manage Mute Rules message keeps to one line at the window's own size, so no word ends up alone on a line of its own.
/// </summary>
[Collection("credential-manager")]
public sealed class ManageWindowsEmptyStateTests
{
    [Fact]
    public void ManageMuteRules_WithNoRules_SaysSoInsideTheGrid()
    {
        var found = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
        {
            var window = new ManageMuteRulesWindow(new MuteRuleService(new AlertWebhookCancelTests.NoMuteRules(), new AppLoggerAdapter<MuteRuleService>()));
            DialogWalkTestHost.Show(window);
            var texts = DialogWalkTestHost.VisibleTextStarting(window, "No mute rules").Select(t => t.Text).ToList();
            window.Close();
            return texts;
        }));

        Assert.Single(found);
    }

    [Fact]
    public void ManageMuteRules_WithNoRules_KeepsItsMessageOnOneLine_AtTheWindowsOwnSize()
    {
        var (lines, needed, room) = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
        {
            var window = new ManageMuteRulesWindow(new MuteRuleService(new AlertWebhookCancelTests.NoMuteRules(), new AppLoggerAdapter<MuteRuleService>()));
            DialogWalkTestHost.Show(window);
            var message = DialogWalkTestHost.VisibleTextStarting(window, "No mute rules").Single();
            var result = WrappedLines(message);
            window.Close();
            return result;
        }));

        Assert.True(lines == 1, $"the empty-grid message wraps onto {lines} lines: it needs {needed:F0} px on one line and its grid gives {room:F0}");
    }

    /// <summary>How many lines a wrapped TextBlock took, with the width its text needs on one line and the width its parent gives it.</summary>
    private static (int Lines, double Needed, double Room) WrappedLines(TextBlock text)
    {
        var oneLine = new TextBlock { Text = text.Text, FontFamily = text.FontFamily, FontSize = text.FontSize, FontStyle = text.FontStyle, FontWeight = text.FontWeight, FontStretch = text.FontStretch };
        oneLine.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
        var lines = (int)Math.Round(text.ActualHeight / oneLine.DesiredSize.Height);
        return (lines, oneLine.DesiredSize.Width, ((FrameworkElement)VisualTreeHelper.GetParent(text)).ActualWidth);
    }

    [Fact]
    public void CredentialProfiles_WithNoProfiles_SaysSoInsideTheGrid()
    {
        var dir = DialogWalkTestHost.ScratchDir("ProfilesEmpty");
        try
        {
            var found = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
            {
                var window = new ManageCredentialProfilesDialog(new ProfileManager(new ServerManager(dir)));
                DialogWalkTestHost.Show(window);
                var texts = DialogWalkTestHost.VisibleTextStarting(window, "No credential profiles").Select(t => t.Text).ToList();
                window.Close();
                return texts;
            }));

            Assert.Single(found);
        }
        finally
        {
            DialogWalkTestHost.Delete(dir);
        }
    }
}
