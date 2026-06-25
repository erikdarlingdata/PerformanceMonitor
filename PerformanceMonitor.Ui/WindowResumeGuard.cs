/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using Microsoft.Win32;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Restores a window that was auto-hidden to the system tray when Windows turned a
    /// sleep- or lock-driven minimize into a hidden window (#1050). On resume from sleep or
    /// on session unlock, if the window was visible before the transition but is now hidden
    /// or minimized, the supplied <paramref name="restoreToForeground"/> action is invoked
    /// on the window's UI thread.
    ///
    /// This guard does NO rendering work. The companion "blank window after resume" symptom
    /// is handled separately by disabling hardware acceleration
    /// (<c>RenderOptions.ProcessRenderMode = SoftwareOnly</c>) at application startup.
    ///
    /// It subscribes to the static <see cref="SystemEvents"/> events, which hold strong
    /// references to their handlers and raise on a dedicated (non-UI) thread. Callers MUST
    /// <see cref="Dispose"/> the guard when the window really closes, or it leaks the window
    /// and fires against a destroyed HWND. Construct it once per window (e.g. guard the
    /// construction site against a double <c>Loaded</c>).
    /// </summary>
    public sealed class WindowResumeGuard : IDisposable
    {
        private readonly Window _window;
        private readonly Action _restoreToForeground;

        /* Seeded true because the app starts visible (App.OnStartup → MainWindow.Show()). This
           covers a Resume/SessionUnlock that arrives with no preceding Suspend/Lock snapshot; the
           normal sleep/lock path recomputes it first. */
        private bool _wasVisibleBeforeSuspend = true;
        private bool _disposed;

        public WindowResumeGuard(Window window, Action restoreToForeground)
        {
            _window = window ?? throw new ArgumentNullException(nameof(window));
            _restoreToForeground = restoreToForeground ?? throw new ArgumentNullException(nameof(restoreToForeground));

            SystemEvents.PowerModeChanged += OnPowerModeChanged;
            SystemEvents.SessionSwitch += OnSessionSwitch;
        }

        /// <summary>
        /// Pure decision: restore only when the window was visible before the transition but is
        /// now hidden or minimized. A window the user deliberately sent to the tray beforehand
        /// (<paramref name="wasVisibleBeforeSuspend"/> == false) is left alone.
        /// </summary>
        internal static bool ShouldRestore(bool wasVisibleBeforeSuspend, bool isVisibleNow, WindowState state)
            => wasVisibleBeforeSuspend && (!isVisibleNow || state == WindowState.Minimized);

        private void OnPowerModeChanged(object? sender, PowerModeChangedEventArgs e)
        {
            if (_disposed) return;

            if (e.Mode == PowerModes.Suspend)
                Snapshot();
            else if (e.Mode == PowerModes.Resume)
                RestoreIfNeeded();
        }

        private void OnSessionSwitch(object? sender, SessionSwitchEventArgs e)
        {
            if (_disposed) return;

            if (e.Reason == SessionSwitchReason.SessionLock)
                Snapshot();
            else if (e.Reason == SessionSwitchReason.SessionUnlock)
                RestoreIfNeeded();
        }

        /* SystemEvents raise on a dedicated, non-UI thread — marshal to the window's dispatcher
           before touching it. BeginInvoke (not Invoke) avoids a deadlock if the UI thread is mid
           resume-pump; the try/catch covers a late event hitting a dispatcher that is shutting
           down between Closing and teardown. */
        private void Snapshot()
        {
            try
            {
                _window.Dispatcher.BeginInvoke(new Action(() =>
                {
                    if (_disposed) return;
                    _wasVisibleBeforeSuspend = _window.IsVisible && _window.WindowState != WindowState.Minimized;
                }));
            }
            catch { /* dispatcher shutting down */ }
        }

        private void RestoreIfNeeded()
        {
            try
            {
                _window.Dispatcher.BeginInvoke(new Action(() =>
                {
                    if (_disposed) return;
                    if (ShouldRestore(_wasVisibleBeforeSuspend, _window.IsVisible, _window.WindowState))
                        _restoreToForeground();
                }));
            }
            catch { /* dispatcher shutting down */ }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            SystemEvents.PowerModeChanged -= OnPowerModeChanged;
            SystemEvents.SessionSwitch -= OnSessionSwitch;
        }
    }
}
