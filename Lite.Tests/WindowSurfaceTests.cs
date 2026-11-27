/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.InteropServices;
using System.Windows;
using System.Windows.Interop;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9886: Lite's tray restore returns a window that was Normal or Maximized before it was minimized to that state, visible
/// and not minimized, with Minimize to tray on (the tray hides the window as it minimizes) and off. The window is a rendered
/// WPF window with Lite's real tray service attached, minimized the way a person minimizes it, and the answer is read from
/// the window handle as well as from <see cref="Window.WindowState"/>. A test window stands in for the main window, which
/// cannot be shown here without starting its collectors.
/// </summary>
[Collection(AppAlertStaticsCollection.Name)]
public sealed class WindowSurfaceTests
{
    [DllImport("user32.dll")]
    private static extern bool IsIconic(IntPtr hWnd);

    [DllImport("user32.dll")]
    private static extern bool IsWindowVisible(IntPtr hWnd);

    [DllImport("user32.dll")]
    private static extern bool IsZoomed(IntPtr hWnd);

    public static TheoryData<WindowState, bool> Cases => new()
    {
        { WindowState.Normal, true },
        { WindowState.Maximized, true },
        { WindowState.Normal, false },
        { WindowState.Maximized, false },
    };

    [Theory]
    [MemberData(nameof(Cases))]
    public void Minimized_window_returns_in_the_state_it_had_before(WindowState before, bool minimizeToTray)
    {
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var savedMinimizeToTray = App.MinimizeToTray;
            var window = new Window { Width = 640, Height = 420 };
            var memory = new WindowStateMemory();
            memory.Attach(window);
            using var tray = new SystemTrayService(window, () => Restore(window, memory));
            try
            {
                App.MinimizeToTray = minimizeToTray;
                StaTestThread.ShowOffScreen(window);
                StaTestThread.WaitForIdle(window.Dispatcher, StaTestThread.IdleLimit);
                /* Both states are reached by a state change, so the memory has recorded the one the window ends in. */
                window.WindowState = WindowState.Maximized;
                window.WindowState = before;
                tray.Initialize();
                StaTestThread.WaitForIdle(window.Dispatcher, StaTestThread.IdleLimit);

                window.WindowState = WindowState.Minimized;
                StaTestThread.WaitForIdle(window.Dispatcher, StaTestThread.IdleLimit);
                var handle = new WindowInteropHelper(window).Handle;

                Assert.True(IsIconic(handle), "the window is minimized before the restore");
                Assert.Equal(!minimizeToTray, window.IsVisible);

                /* The app's window is activated when it shows, and WPF refuses to show an unactivated window that is Maximized. */
                window.ShowActivated = true;
                Restore(window, memory);
                StaTestThread.WaitForIdle(window.Dispatcher, StaTestThread.IdleLimit);

                Assert.True(window.IsVisible);
                Assert.True(IsWindowVisible(handle));
                Assert.False(IsIconic(handle));
                Assert.Equal(before, window.WindowState);
                Assert.Equal(before == WindowState.Maximized, IsZoomed(handle));
            }
            finally
            {
                App.MinimizeToTray = savedMinimizeToTray;
                window.Close();
            }
        });
    }

    private static void Restore(Window window, WindowStateMemory memory)
    {
        memory.ShowRestored(window);
        window.Activate();
    }
}
