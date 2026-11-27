/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9892: the shell names the tray icon from <c>TaskbarIcon.ToolTipText</c>, and a screen reader or a UI Automation search by name
/// reads nothing else. The icon carries the same sentence the styled tooltip popup shows, and that sentence follows the paused state
/// whichever window changed it: the tray menu or the Settings window's Pause button.
/// </summary>
public sealed class SystemTrayIconNameTests
{
    private static string StyledTooltipText(TaskbarIcon icon) =>
        Assert.IsType<TextBlock>(Assert.IsType<Border>(icon.TrayToolTip).Child).Text;

    private static string[] MenuHeaders(TaskbarIcon icon) =>
        icon.ContextMenu.Items.OfType<MenuItem>().Select(item => item.Header?.ToString() ?? string.Empty).ToArray();

    private static void WithTray(bool pausedBeforeInitialize, Action<SystemTrayService, CollectionBackgroundService> body)
    {
        StaTestThread.Run(() =>
        {
            var collection = new CollectionBackgroundService(null!) { IsPaused = pausedBeforeInitialize };
            using var tray = new SystemTrayService(new Window(), () => { }, collection);
            tray.Initialize();
            body(tray, collection);
        });
    }

    [Fact]
    public void TheIcon_IsNamedWithTheSentenceItsStyledTooltipShows()
    {
        WithTray(pausedBeforeInitialize: false, (tray, _) =>
        {
            var icon = Assert.IsAssignableFrom<TaskbarIcon>(tray.TrayIcon);

            Assert.False(string.IsNullOrWhiteSpace(icon.ToolTipText));
            Assert.Equal(StyledTooltipText(icon), icon.ToolTipText);
        });
    }

    [Fact]
    public void AnIconBuiltWhileCollectionIsPaused_IsNamedWithThePausedSentence()
    {
        string runningSentence = "";
        WithTray(pausedBeforeInitialize: false, (tray, _) => runningSentence = tray.TrayIcon!.ToolTipText);

        WithTray(pausedBeforeInitialize: true, (tray, _) =>
        {
            var icon = Assert.IsAssignableFrom<TaskbarIcon>(tray.TrayIcon);

            Assert.NotEqual(runningSentence, icon.ToolTipText);
            Assert.Equal(StyledTooltipText(icon), icon.ToolTipText);
        });
    }

    [Fact]
    public void APauseAndAResumeFromTheTrayMenu_MoveTheIconsNameWithTheStyledTooltip()
    {
        WithTray(pausedBeforeInitialize: false, (tray, collection) =>
        {
            var icon = Assert.IsAssignableFrom<TaskbarIcon>(tray.TrayIcon);
            string runningSentence = icon.ToolTipText;

            tray.ToggleCollection();

            Assert.True(collection.IsPaused);
            Assert.NotEqual(runningSentence, icon.ToolTipText);
            Assert.Equal(StyledTooltipText(icon), icon.ToolTipText);

            tray.ToggleCollection();

            Assert.False(collection.IsPaused);
            Assert.Equal(runningSentence, icon.ToolTipText);
            Assert.Equal(StyledTooltipText(icon), icon.ToolTipText);
        });
    }

    [Fact]
    public void APauseFromTheSettingsWindow_MovesTheIconsNameTheStyledTooltipAndThePauseItem()
    {
        WithTray(pausedBeforeInitialize: false, (tray, collection) =>
        {
            var icon = Assert.IsAssignableFrom<TaskbarIcon>(tray.TrayIcon);
            string runningSentence = icon.ToolTipText;
            string[] runningHeaders = MenuHeaders(icon);

            // The Settings window's Pause button sets the property directly, never through the tray.
            collection.IsPaused = true;

            Assert.NotEqual(runningSentence, icon.ToolTipText);
            Assert.Equal(StyledTooltipText(icon), icon.ToolTipText);
            Assert.Equal(1, MenuHeaders(icon).Zip(runningHeaders, (now, before) => now != before).Count(changed => changed));

            collection.IsPaused = false;

            Assert.Equal(runningSentence, icon.ToolTipText);
            Assert.Equal(runningHeaders, MenuHeaders(icon));
        });
    }

    [Fact]
    public void ASecondInitialize_NamesTheNewIcon_AndAPauseReachesOnlyThatIcon()
    {
        WithTray(pausedBeforeInitialize: false, (tray, collection) =>
        {
            var first = Assert.IsAssignableFrom<TaskbarIcon>(tray.TrayIcon);
            string runningSentence = first.ToolTipText;

            tray.Initialize();
            var second = Assert.IsAssignableFrom<TaskbarIcon>(tray.TrayIcon);
            Assert.NotSame(first, second);
            Assert.Equal(runningSentence, second.ToolTipText);

            collection.IsPaused = true;

            Assert.NotEqual(runningSentence, second.ToolTipText);
            Assert.Equal(StyledTooltipText(second), second.ToolTipText);
        });
    }
}
