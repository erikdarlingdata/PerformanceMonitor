/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Threading;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Keeps a tray card open while a reader is on it. The tray's balloon closes on its own timer, which runs on under the pointer,
    /// so a message long enough to scroll could close part-way through the text. <see cref="Attach"/> handles the card's pointer and
    /// keyboard-focus events: when the pointer enters the card, or focus moves inside it, <c>holdOpen</c> runs (the tray's
    /// <c>ResetBalloonCloseTimer</c>, which stops its timer for good) and any pending close is dropped; when the pointer leaves and
    /// focus is outside, a timer of <c>closeAfter</c> starts and runs <c>close</c> (the tray's <c>CloseBalloon</c>). Unloading the
    /// card drops a pending close, so a card Dismiss or the next toast closed never closes anything later.
    /// </summary>
    public static class BalloonHoverHold
    {
        /// <param name="card">The balloon control the tray shows.</param>
        /// <param name="closeAfter">How long the card stays after the reader leaves it, the tray's own timeout for that card.</param>
        /// <param name="holdOpen">Stops the tray's close timer.</param>
        /// <param name="close">Closes the balloon.</param>
        /// <param name="startTimer">Starts a timer that runs its callback once after the given time, and stops it when the result is
        /// disposed. Null means a <see cref="DispatcherTimer"/> on the card's dispatcher; a test passes its own and fires it itself.</param>
        public static void Attach(FrameworkElement card, TimeSpan closeAfter, Action holdOpen, Action close, Func<TimeSpan, Action, IDisposable>? startTimer = null)
        {
            ArgumentNullException.ThrowIfNull(card);
            ArgumentNullException.ThrowIfNull(holdOpen);
            ArgumentNullException.ThrowIfNull(close);
            startTimer ??= DispatcherOnce;
            IDisposable? pending = null;

            void Hold()
            {
                pending?.Dispose();
                pending = null;
                holdOpen();
            }

            void Release()
            {
                if (card.IsMouseOver || card.IsKeyboardFocusWithin) return;
                pending?.Dispose();
                pending = startTimer(closeAfter, () =>
                {
                    pending = null;
                    close();
                });
            }

            card.MouseEnter += (_, _) => Hold();
            card.MouseLeave += (_, _) => Release();
            card.IsKeyboardFocusWithinChanged += (_, e) =>
            {
                if ((bool)e.NewValue) Hold();
                else Release();
            };
            card.Unloaded += (_, _) =>
            {
                pending?.Dispose();
                pending = null;
            };
        }

        private static TimerStop DispatcherOnce(TimeSpan after, Action fire)
        {
            var timer = new DispatcherTimer { Interval = after };
            timer.Tick += (_, _) =>
            {
                timer.Stop();
                fire();
            };
            timer.Start();
            return new TimerStop(timer);
        }

        private sealed class TimerStop(DispatcherTimer timer) : IDisposable
        {
            public void Dispose() => timer.Stop();
        }
    }
}
