/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Controls;
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9892: the shell names the Viewer's tray icon from <c>TaskbarIcon.ToolTipText</c>, and a screen reader or a UI Automation search by
/// name reads nothing else. The icon carries the same sentence the styled tooltip popup shows.
/// </summary>
public sealed class ViewerTrayIconNameTests
{
    [Fact]
    public void TheIcon_IsNamedWithTheSentenceItsStyledTooltipShows()
    {
        StaTestThread.Run(() =>
        {
            using var tray = new SystemTrayService(new Window(), () => { }, () => true);
            tray.Initialize();

            var icon = Assert.IsAssignableFrom<TaskbarIcon>(tray.TrayIcon);
            string styled = Assert.IsType<TextBlock>(Assert.IsType<Border>(icon.TrayToolTip).Child).Text;

            Assert.False(string.IsNullOrWhiteSpace(icon.ToolTipText));
            Assert.Equal(styled, icon.ToolTipText);
        });
    }

    [Fact]
    public void ASecondInitialize_NamesTheNewIconToo()
    {
        StaTestThread.Run(() =>
        {
            using var tray = new SystemTrayService(new Window(), () => { }, () => true);
            tray.Initialize();
            string first = tray.TrayIcon!.ToolTipText;

            tray.Initialize();

            Assert.False(string.IsNullOrWhiteSpace(tray.TrayIcon!.ToolTipText));
            Assert.Equal(first, tray.TrayIcon.ToolTipText);
        });
    }
}
