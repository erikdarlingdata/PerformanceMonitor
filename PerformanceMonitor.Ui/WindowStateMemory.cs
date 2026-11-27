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

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Remembers the last non-minimized <see cref="WindowState"/> of a window, so a window brought back from the
    /// tray or by a second launch returns to the state it had before it was minimized (#9735). Lite used to
    /// force Normal, which returned a maximized window at its declared 1200x700 size, past the edge of a small
    /// screen. Both apps start Maximized, so Maximized is the answer when no state was recorded.
    /// </summary>
    public sealed class WindowStateMemory
    {
        private WindowState? _lastNonMinimized;

        /// <summary>Starts recording the window's state on every <see cref="Window.StateChanged"/>.</summary>
        public void Attach(Window window)
        {
            ArgumentNullException.ThrowIfNull(window);
            window.StateChanged += (_, _) => Record(window.WindowState);
        }

        /// <summary>Records <paramref name="state"/> unless it is Minimized, which is never a state to restore to.</summary>
        internal void Record(WindowState state)
        {
            if (state != WindowState.Minimized)
            {
                _lastNonMinimized = state;
            }
        }

        /// <summary>The state to restore a window now in <paramref name="current"/> to.</summary>
        public WindowState RestoreTarget(WindowState current) => RestoreTarget(current, _lastNonMinimized);

        /// <summary>
        /// Shows a window the tray hid or the user minimized, in the state it had before it was minimized (#9886). Minimize to tray hides
        /// the window inside its <see cref="Window.StateChanged"/> handler, so the window is hidden and its handle is still minimized.
        /// <see cref="Window.Show"/> on it does not restore a Normal window (a Maximized one comes back), and WPF's own
        /// <see cref="Window.WindowState"/> set can read as Normal while the handle stays minimized and no window appears. The target is
        /// read before <see cref="Window.Show"/>, the window is shown, the handle is restored when it is still minimized, and the
        /// remembered state is applied last. A window that is already showing and not minimized keeps its state. Must run on the UI thread.
        /// </summary>
        public void ShowRestored(Window window)
        {
            ArgumentNullException.ThrowIfNull(window);

            var target = RestoreTarget(window.WindowState);
            window.Show();
            window.ShowInTaskbar = true;

            var handle = new WindowInteropHelper(window).Handle;
            if (handle != IntPtr.Zero && NativeMethods.IsIconic(handle))
            {
                NativeMethods.ShowWindow(handle, NativeMethods.SW_RESTORE);
            }

            window.WindowState = target;
        }

        private static class NativeMethods
        {
            internal const int SW_RESTORE = 9;

            [DllImport("user32.dll")]
            [return: MarshalAs(UnmanagedType.Bool)]
            internal static extern bool IsIconic(IntPtr hWnd);

            [DllImport("user32.dll")]
            [return: MarshalAs(UnmanagedType.Bool)]
            internal static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
        }

        /// <summary>
        /// A minimized window goes back to its last non-minimized state, or Maximized when none was recorded.
        /// A window that is not minimized keeps its state.
        /// </summary>
        internal static WindowState RestoreTarget(WindowState current, WindowState? lastNonMinimized)
        {
            return current == WindowState.Minimized ? lastNonMinimized ?? WindowState.Maximized : current;
        }
    }
}
