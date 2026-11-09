/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8434: a test that points the app's static store paths (<see cref="App.DatabasePath"/>, <see cref="App.ConfigDirectory"/>,
/// <see cref="App.SharedConfigDirectory"/>) at a folder of its own leaves them as it found them, so a test that builds a
/// <see cref="MainWindow"/> after it in the same process does not read and write a folder the earlier test has deleted. Each fact
/// runs one test of a class that sets the paths, as a plain method call so the order is the test's own, and reads the three statics
/// back. The last fact runs the sidebar test and then the status-bar test, the pair that failed when the two shared a process.
/// </summary>
[Collection("credential-manager")]
public sealed class AppPathsRestoredAcrossClassesTests
{
    [Fact]
    public void TheSidebarAndCardButtonTests_LeaveTheAppPathsAsTheyFoundThem() =>
        AssertPathsKept(nameof(LiteSidebarAndCardButtonTests), () => new LiteSidebarAndCardButtonTests().TheSidebar_ShowsTheServerName_OnlyWhenItDiffersFromTheDisplayName("DarkTheme"));

    [Fact]
    public void TheMinWindowChromeTests_LeaveTheAppPathsAsTheyFoundThem() =>
        AssertPathsKept(nameof(MinWindowChromeTests), () => new MinWindowChromeTests().TheSidebar_KeepsAManualChoiceWhenTheWindowIsResized());

    [Fact]
    public void TheOfflineServerStatusTextTests_LeaveTheAppPathsAsTheyFoundThem() =>
        AssertPathsKept(nameof(OfflineServerStatusTextTests), () => new OfflineServerStatusTextTests().TheStatusBar_FollowsTheConnectionCheck_WhileATabStaysSelected());

    [Fact]
    public void TheServerTabStripFitTests_LeaveTheAppPathsAsTheyFoundThem() =>
        AssertPathsKept(nameof(Lite.Tests.ServerTabStripFitTests), () => new Lite.Tests.ServerTabStripFitTests().At900x600_TheStripKeepsOneRow_AndTheButtonCarriesTheHiddenCountAndNames("MainTabControl"));

    [Fact]
    public void TheSidebarRailRenderTests_LeaveTheAppPathsAsTheyFoundThem() =>
        AssertPathsKept(nameof(SidebarRailRenderTests), () => new SidebarRailRenderTests().TheOpenSidebarSubtitle_StaysOnOneLine());

    [Fact]
    public void AfterTheSidebarTest_TheStatusBarTest_StillBuildsItsWindow()
    {
        new LiteSidebarAndCardButtonTests().TheSidebar_ShowsTheServerName_OnlyWhenItDiffersFromTheDisplayName("DarkTheme");

        var failure = Record.Exception(() => new StatusBarTooltipTests().AnEmptyBar_OpensNoTooltip_AndAFilledOneCarriesItsText());

        Assert.True(failure is null, "the status-bar test, run after the sidebar test in the same process, failed: " + failure);
    }

    private static void AssertPathsKept(string className, Action test)
    {
        var (databasePath, configDirectory, sharedConfigDirectory) = (App.DatabasePath, App.ConfigDirectory, App.SharedConfigDirectory);

        test();

        Assert.True(App.DatabasePath == databasePath, $"{className} left App.DatabasePath changed: was \"{databasePath}\", now \"{App.DatabasePath}\"");
        Assert.True(App.ConfigDirectory == configDirectory, $"{className} left App.ConfigDirectory changed: was \"{configDirectory}\", now \"{App.ConfigDirectory}\"");
        Assert.True(App.SharedConfigDirectory == sharedConfigDirectory, $"{className} left App.SharedConfigDirectory changed: was \"{sharedConfigDirectory}\", now \"{App.SharedConfigDirectory}\"");
    }
}
