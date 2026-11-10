/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Input;
using System.Windows.Threading;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Keeps a tray card open while a reader is on it. The tray's balloon closes on its own timer, which runs on under the pointer,
    /// so a message long enough to scroll could close part-way through the text. <see cref="Attach"/> handles the card's pointer and
    /// keyboard-focus events: when the pointer enters the card, or focus moves inside it, <c>holdOpen</c> runs (the tray's
    /// <c>ResetBalloonCloseTimer</c>, which stops its timer for good) and any pending close is dropped; when the pointer leaves and
    /// focus is outside, a timer of <c>closeAfter</c> starts and runs <c>close</c> (the tray's <c>CloseBalloon</c>).
    /// <para>
    /// The hold ends with the card. Once the card has unloaded (its popup closed by Dismiss, by its timeout or by the next toast), the
    /// pending close is dropped, the handlers come off, and a pointer or focus event WPF delivers after that, such as the
    /// <c>MouseLeave</c> that follows the popup's close while the pointer is still over where the card was, holds and arms nothing.
    /// A card that is gone therefore never stops or closes the toast shown after it.
    /// </para>
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
            var ended = false;

            void Hold()
            {
                if (ended) return;
                pending?.Dispose();
                pending = null;
                holdOpen();
            }

            void Release()
            {
                if (ended || card.IsMouseOver || card.IsKeyboardFocusWithin) return;
                pending?.Dispose();
                pending = startTimer(closeAfter, () =>
                {
                    pending = null;
                    if (!ended) close();
                });
            }

            MouseEventHandler onEnter = (_, _) => Hold();
            MouseEventHandler onLeave = (_, _) => Release();
            DependencyPropertyChangedEventHandler onFocusWithin = (_, e) =>
            {
                if ((bool)e.NewValue) Hold();
                else Release();
            };
            RoutedEventHandler? onUnloaded = null;
            onUnloaded = (_, _) =>
            {
                ended = true;
                pending?.Dispose();
                pending = null;
                card.MouseEnter -= onEnter;
                card.MouseLeave -= onLeave;
                card.IsKeyboardFocusWithinChanged -= onFocusWithin;
                card.Unloaded -= onUnloaded;
            };

            card.MouseEnter += onEnter;
            card.MouseLeave += onLeave;
            card.IsKeyboardFocusWithinChanged += onFocusWithin;
            card.Unloaded += onUnloaded;
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
