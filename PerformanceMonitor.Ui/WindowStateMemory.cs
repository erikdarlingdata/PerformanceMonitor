/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;

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
        /// A minimized window goes back to its last non-minimized state, or Maximized when none was recorded.
        /// A window that is not minimized keeps its state.
        /// </summary>
        internal static WindowState RestoreTarget(WindowState current, WindowState? lastNonMinimized)
        {
            return current == WindowState.Minimized ? lastNonMinimized ?? WindowState.Maximized : current;
        }
    }
}
