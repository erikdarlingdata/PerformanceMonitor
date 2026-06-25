/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Cross-process "surface the existing window" channel for a single-instance app (#769, #1050).
    ///
    /// The first (owning) instance constructs one of these with a callback and keeps it for the life
    /// of the process. A later launch calls the static <see cref="TrySignal"/> and exits; the owning
    /// instance wakes and invokes the callback.
    ///
    /// The callback runs on this class's background listener thread — the owner is responsible for
    /// marshalling to its UI thread and restoring the window through WPF's own <c>Window.Show()</c>
    /// path. The bug this exists to prevent (#1050): the old approach poked the running window's HWND
    /// with raw Win32 <c>ShowWindow(SW_RESTORE)</c>, which un-minimizes the native window but does NOT
    /// reconcile WPF's <c>Visibility.Hidden</c> state left by a minimize-to-tray <c>Hide()</c> — so the
    /// window comes back visible but blank. Routing the restore back through the owner lets it use
    /// <c>Show()</c>, the only path that re-arranges and re-renders the WPF tree.
    ///
    /// The named event uses <see cref="EventResetMode.AutoReset"/>, so each launch delivers exactly one
    /// restore. <see cref="Dispose"/> stops the listener; callers MUST dispose on app exit.
    /// </summary>
    public sealed class SingleInstanceSignal : IDisposable
    {
        private readonly EventWaitHandle _handle;
        private readonly Thread _listener;
        private readonly Action _onSignal;
        private volatile bool _disposed;

        public SingleInstanceSignal(string eventName, Action onSignal)
        {
            if (string.IsNullOrEmpty(eventName))
                throw new ArgumentException("Event name is required.", nameof(eventName));

            _onSignal = onSignal ?? throw new ArgumentNullException(nameof(onSignal));
            _handle = new EventWaitHandle(false, EventResetMode.AutoReset, eventName);
            _listener = new Thread(ListenLoop)
            {
                IsBackground = true,
                Name = "SingleInstanceSignal"
            };
            _listener.Start();
        }

        /// <summary>
        /// Best-effort wake of an already-running owner. Returns true if an instance was listening on
        /// <paramref name="eventName"/> and was signaled; false when no instance owns the channel
        /// (nothing to surface) or the owner is mid start-up/tear-down.
        /// </summary>
        public static bool TrySignal(string eventName)
        {
            try
            {
                if (EventWaitHandle.TryOpenExisting(eventName, out var existing))
                {
                    using (existing)
                        existing.Set();
                    return true;
                }
            }
            catch
            {
                /* Owner is starting up or shutting down — treat as "no one to tell". */
            }

            return false;
        }

        /* Poll with a timeout so the thread can observe _disposed and exit promptly on shutdown,
           instead of blocking forever on a WaitOne that will never be signaled again. A single bad
           callback must not kill the channel, so non-disposal exceptions are swallowed and we keep
           listening. */
        private void ListenLoop()
        {
            while (!_disposed)
            {
                try
                {
                    if (!_handle.WaitOne(500))
                        continue;
                    if (_disposed)
                        return;

                    _onSignal();
                }
                catch (ObjectDisposedException)
                {
                    return;
                }
                catch
                {
                    /* Keep listening — one failed restore shouldn't disable the channel. */
                }
            }
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            /* Nudge the listener out of WaitOne so it sees _disposed now rather than after the poll
               timeout, then wait briefly for it to unwind before disposing the handle it waits on. */
            try { _handle.Set(); }
            catch { /* already disposed */ }

            _listener.Join(TimeSpan.FromSeconds(1));
            _handle.Dispose();
        }
    }
}
