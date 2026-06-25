/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Truth-table coverage for <see cref="WindowResumeGuard.ShouldRestore"/> (#1050).
/// The guard restores a window only when it was visible before a sleep/lock transition
/// but is now hidden or minimized — and must leave a window the user deliberately sent
/// to the tray (wasVisibleBeforeSuspend == false) alone, so resume never pops it back out.
/// </summary>
public class WindowResumeGuardTests
{
    [Theory]
    /* Was visible before the transition... */
    [InlineData(true, false, WindowState.Minimized, true)]   // now hidden (Hide()) -> restore (the bug)
    [InlineData(true, false, WindowState.Normal, true)]      // now hidden, state stale -> restore
    [InlineData(true, true, WindowState.Minimized, true)]    // now minimized-but-shown -> restore
    [InlineData(true, true, WindowState.Normal, false)]      // still up and normal -> nothing to do
    [InlineData(true, true, WindowState.Maximized, false)]   // still up and maximized -> nothing to do
    /* Was NOT visible before (user had it in the tray)... */
    [InlineData(false, false, WindowState.Minimized, false)] // leave tray-hidden window alone
    [InlineData(false, false, WindowState.Normal, false)]
    [InlineData(false, true, WindowState.Normal, false)]
    public void ShouldRestore_MatchesTruthTable(
        bool wasVisibleBeforeSuspend,
        bool isVisibleNow,
        WindowState state,
        bool expected)
    {
        Assert.Equal(expected, WindowResumeGuard.ShouldRestore(wasVisibleBeforeSuspend, isVisibleNow, state));
    }
}
