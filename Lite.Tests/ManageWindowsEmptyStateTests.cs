/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A management window with nothing to list says so inside its grid, instead of showing a header row over empty space: Manage
/// Mute Rules with no rules, and Credential Profiles with no profiles. Each window is the real one, rendered with the theme.
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
