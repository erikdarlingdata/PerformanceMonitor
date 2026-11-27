/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.InteropServices;
using System.Windows;
using System.Windows.Interop;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9886: a window that was Normal before it was minimized comes back Normal and visible when the Viewer's surface call
/// runs, with Minimize to tray on (the tray hides the window as it minimizes) and off. The window is a rendered WPF window
/// with the Viewer's real tray service attached, minimized the way a person minimizes it, and the answer is read from the
/// window handle as well as from <see cref="Window.WindowState"/>, because the gap was a handle that stayed minimized.
/// </summary>
public sealed class ViewerWindowSurfaceTests
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
    public void Minimized_window_surfaces_in_the_state_it_had_before(WindowState before, bool minimizeToTray)
    {
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var window = new Window { Width = 640, Height = 420 };
            var memory = new WindowStateMemory();
            memory.Attach(window);
            using var tray = new SystemTrayService(window, () => Surface(window, memory), () => minimizeToTray);
            try
            {
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

                Assert.True(IsIconic(handle), "the window is minimized before the surface call");
                Assert.Equal(!minimizeToTray, window.IsVisible);

                /* The app's window is activated when it shows, and WPF refuses to show an unactivated window that is Maximized. */
                window.ShowActivated = true;
                Surface(window, memory);
                StaTestThread.WaitForIdle(window.Dispatcher, StaTestThread.IdleLimit);

                Assert.True(window.IsVisible);
                Assert.True(IsWindowVisible(handle));
                Assert.False(IsIconic(handle));
                Assert.Equal(before, window.WindowState);
                Assert.Equal(before == WindowState.Maximized, IsZoomed(handle));
            }
            finally
            {
                window.Close();
            }
        });
    }

    private static void Surface(Window window, WindowStateMemory memory) => memory.ShowRestored(window);
}
