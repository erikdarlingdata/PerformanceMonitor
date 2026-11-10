/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using Hardcodet.Wpf.TaskbarNotification;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8475: an alert toast carries the alert's whole message, which can run to twenty lines. The card stays the height of a toast and
/// scrolls the rest of the message, so the end of it can be read from the toast. The toast sits in a popup with no window above it, so
/// a theme change, which repaints every window, has to reach it too: a toast raised under Dark follows a switch to Cool Breeze. A card
/// the reader is on stays open, its timeout runs again from the moment the pointer leaves, and a card that has gone never closes the
/// toast after it. Both cards are built on the shared STA thread and laid out at their own width, as the tray shows them.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class LiteAlertToastTests
{
    private const string ThemeUriFormat = "pack://application:,,,/PerformanceMonitorLite;component/Themes/{0}Theme.xaml";

    /// <summary>The height a toast may reach: a title row, a capped message and, on the snoozable card, its buttons.</summary>
    private const double StyledCardCap = 260;
    private const double SnoozeCardCap = 300;

    private const string Title = "Blocking";

    /// <summary>About 270 words: the shape of a long alert message, which names counts, phases, a reason and columns.</summary>
    private static readonly string LongMessage = string.Join(" ", Enumerable.Repeat(
        "The checkpointer wrote 8,192 pages in 41 checkpoints over the hour, 39 of them REQUESTED, and the sync phase took 12.4 seconds in all; the longest single sync was 3.1 seconds.", 10));

    [Fact]
    public void StyledBalloon_WithALongMessage_StaysAToastHigh_AndScrollsToTheEndOfTheMessage()
    {
        StaTestThread.Run(() =>
        {
            var card = new StyledBalloon(Title, LongMessage, ToastSeverity.Warning);
            WithOwnTheme(card, "Dark");
            Layout(card);
            AssertMessageScrolls(card, StyledCardCap);
        });
    }

    [Fact]
    public void SnoozeBalloon_WithALongMessage_StaysAToastHigh_AndKeepsItsButtonsInside()
    {
        StaTestThread.Run(() =>
        {
            var card = new SnoozeBalloon(Title, LongMessage, BalloonIcon.Warning, "a server", "blocking", null!, () => { });
            WithOwnTheme(card, "Dark");
            Layout(card);
            AssertMessageScrolls(card, SnoozeCardCap);
            var dismiss = Assert.IsType<Button>(card.FindName("DismissButton"));
            var bottom = dismiss.TransformToAncestor(card).Transform(new Point(0, dismiss.ActualHeight)).Y;
            Assert.True(bottom <= card.ActualHeight + 0.5, $"the Dismiss button ends at {bottom:F1} px in a card {card.ActualHeight:F1} px tall");
        });
    }

    [Fact]
    public void AToastRaisedUnderDark_FollowsAThemeChangeToCoolBreeze()
    {
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var previousTheme = ThemeManager.CurrentTheme;
            var previousProvider = ThemeManager.ThemeUriProvider;
            var previousDictionaries = app.Resources.MergedDictionaries.ToList();
            ThemeManager.ThemeUriProvider = name => new Uri(string.Format(ThemeUriFormat, name));
            try
            {
                ThemeManager.Apply("Dark");
                var styled = new StyledBalloon(Title, LongMessage, ToastSeverity.Success);
                var snooze = new SnoozeBalloon(Title, LongMessage, BalloonIcon.Error, "a server", "blocking", null!, () => { });
                Layout(styled);
                Layout(snooze);
                Assert.Equal(ColorOf("Dark", "BackgroundBrush"), BackgroundOf(styled));
                Assert.Equal(ColorOf("Dark", "BackgroundBrush"), BackgroundOf(snooze));

                ThemeManager.Apply("CoolBreeze");

                Assert.Equal(ColorOf("CoolBreeze", "BackgroundBrush"), BackgroundOf(styled));
                Assert.Equal(ColorOf("CoolBreeze", "BorderBrush"), BorderOf(styled));
                Assert.Equal(ColorOf("CoolBreeze", "ForegroundBrush"), ForegroundOf(styled, "MessageText"));
                Assert.Equal(ColorOf("CoolBreeze", "ForegroundBrush"), ForegroundOf(styled, "TitleText"));
                Assert.Equal(ColorOf("CoolBreeze", "BackgroundBrush"), BackgroundOf(snooze));
                Assert.Equal(ColorOf("CoolBreeze", "BorderBrush"), BorderOf(snooze));
                Assert.Equal(ColorOf("CoolBreeze", "ForegroundBrush"), ForegroundOf(snooze, "MessageText"));
                Assert.Equal(ColorOf("CoolBreeze", "ForegroundBrush"), ForegroundOf(snooze, "TitleText"));
            }
            finally
            {
                ThemeManager.Apply(previousTheme);
                ThemeManager.ThemeUriProvider = previousProvider;
                app.Resources.MergedDictionaries.Clear();
                foreach (var dictionary in previousDictionaries) app.Resources.MergedDictionaries.Add(dictionary);
            }
        });
    }

    [Fact]
    public void ACardUnderThePointer_StaysOpen_AndClosesItsTimeoutAfterThePointerLeaves()
    {
        StaTestThread.Run(() =>
        {
            var card = new SnoozeBalloon(Title, LongMessage, BalloonIcon.Warning, "a server", "blocking", null!, () => { });
            WithOwnTheme(card, "Dark");
            Layout(card);
            var holds = 0;
            var closes = 0;
            var timers = new List<FakeTimer>();
            BalloonHoverHold.Attach(card, TimeSpan.FromSeconds(15), () => holds++, () => closes++, (after, fire) => { var timer = new FakeTimer(after, fire); timers.Add(timer); return timer; });

            Pointer(card, Mouse.MouseEnterEvent);
            Assert.Equal(1, holds);
            Assert.Empty(timers);

            Pointer(card, Mouse.MouseLeaveEvent);
            var first = Assert.Single(timers);
            Assert.Equal(TimeSpan.FromSeconds(15), first.After);
            Assert.Equal(0, closes);

            // The reader comes back before the time is up: the pending close is dropped, and firing it closes nothing.
            Pointer(card, Mouse.MouseEnterEvent);
            Assert.True(first.Disposed, "the pending close was not dropped when the pointer came back");
            first.Fire();
            Assert.Equal(0, closes);
            Assert.Equal(2, holds);

            Pointer(card, Mouse.MouseLeaveEvent);
            Assert.Equal(2, timers.Count);
            timers[1].Fire();
            Assert.Equal(1, closes);
        });
    }

    private static void Pointer(UIElement card, RoutedEvent routedEvent) =>
        card.RaiseEvent(new MouseEventArgs(Mouse.PrimaryDevice, 0) { RoutedEvent = routedEvent });

    /// <summary>The tray as the hold sees it: a current balloon, a hold that stops its timer, and a close that closes whichever is current.</summary>
    private sealed class FakeTray
    {
        public UserControl? Current;
        public int Holds;
        public readonly List<UserControl> Closed = new();
        public void HoldCurrent() => Holds++;
        public void CloseCurrent()
        {
            if (Current is null) return;
            Closed.Add(Current);
            Current = null;
        }
    }

    /// <summary>The close timer the hold starts after the pointer leaves: the test fires it, and a disposed one fires nothing.</summary>
    private sealed class FakeTimer(TimeSpan after, Action fire) : IDisposable
    {
        public TimeSpan After { get; } = after;
        public bool Disposed { get; private set; }
        public void Fire() { if (!Disposed) fire(); }
        public void Dispose() => Disposed = true;
    }

    [Fact]
    public void ACardThatHasUnloaded_NeverArmsACloseAgain_SoTheNextToastStaysOpen()
    {
        StaTestThread.Run(() =>
        {
            var first = new StyledBalloon(Title, LongMessage, ToastSeverity.Warning);
            var next = new StyledBalloon(Title, LongMessage, ToastSeverity.Warning);
            WithOwnTheme(first, "Dark");
            WithOwnTheme(next, "Dark");
            Layout(first);
            Layout(next);
            /* The tray as the hold sees it: one current balloon, a hold that stops the current balloon's timer, and a close that closes
               whichever balloon is current, as the tray's own CloseBalloon does. */
            var tray = new FakeTray { Current = first };
            var timers = new List<FakeTimer>();
            BalloonHoverHold.Attach(first, TimeSpan.FromSeconds(10), tray.HoldCurrent, tray.CloseCurrent, (after, fire) => { var timer = new FakeTimer(after, fire); timers.Add(timer); return timer; });

            Pointer(first, Mouse.MouseEnterEvent);
            Assert.Equal(1, tray.Holds);

            /* The card is dismissed or replaced while the pointer is still on it: WPF raises Unloaded first and the pointer's leave after. */
            first.RaiseEvent(new RoutedEventArgs(FrameworkElement.UnloadedEvent));
            Pointer(first, Mouse.MouseLeaveEvent);
            Assert.Empty(timers);

            tray.Current = next;
            Pointer(first, Mouse.MouseEnterEvent);
            Assert.Equal(1, tray.Holds);
            foreach (var timer in timers) timer.Fire();
            Assert.Same(next, tray.Current);
            Assert.Empty(tray.Closed);
        });
    }

    /// <summary>The message block is taller than the card allows, the card stays under its cap, and the viewer reaches the end of the text.</summary>
    private static void AssertMessageScrolls(UserControl card, double cap)
    {
        var message = Assert.IsType<TextBlock>(card.FindName("MessageText"));
        Assert.True(message.ActualHeight > cap, $"the message wraps to {message.ActualHeight:F0} px, which does not exceed the card's cap of {cap} px");
        Assert.True(card.ActualHeight <= cap, $"the card is {card.ActualHeight:F0} px tall for a message that wraps to {message.ActualHeight:F0} px; it may reach {cap} px");
        var viewer = Descendants(card).OfType<ScrollViewer>().FirstOrDefault();
        Assert.True(viewer is not null, "the message has no scroll viewer, so the part past the card's edge cannot be reached");
        Assert.Equal(Visibility.Visible, viewer!.ComputedVerticalScrollBarVisibility);
        Assert.True(viewer.ExtentHeight >= message.ActualHeight - 0.5, $"the viewer's extent is {viewer.ExtentHeight:F0} px for a message of {message.ActualHeight:F0} px");
        viewer.ScrollToEnd();
        card.UpdateLayout();
        Assert.True(viewer.VerticalOffset >= viewer.ScrollableHeight - 0.5, $"after ScrollToEnd the offset is {viewer.VerticalOffset:F0} of {viewer.ScrollableHeight:F0} px");
    }

    private static void Layout(UserControl card)
    {
        card.Measure(new Size(card.Width, double.PositiveInfinity));
        card.Arrange(new Rect(new Point(0, 0), new Size(card.Width, card.DesiredSize.Height)));
        card.UpdateLayout();
    }

    /// <summary>Lite's theme in the card's own resources, as the tray hosts it with the application theme above it.</summary>
    private static void WithOwnTheme(FrameworkElement card, string theme)
    {
        card.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri(string.Format(ThemeUriFormat, theme)) });
    }

    private static Color ColorOf(string theme, string key)
    {
        var dictionary = new ResourceDictionary { Source = new Uri(string.Format(ThemeUriFormat, theme)) };
        return Assert.IsType<SolidColorBrush>(dictionary[key]).Color;
    }

    private static Color BackgroundOf(UserControl card) => Assert.IsType<SolidColorBrush>(Assert.IsType<Border>(card.Content).Background).Color;

    private static Color BorderOf(UserControl card) => Assert.IsType<SolidColorBrush>(Assert.IsType<Border>(card.Content).BorderBrush).Color;

    private static Color ForegroundOf(UserControl card, string name) => Assert.IsType<SolidColorBrush>(Assert.IsType<TextBlock>(card.FindName(name)).Foreground).Color;

    private static IEnumerable<DependencyObject> Descendants(DependencyObject node)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(node).OfType<DependencyObject>())
        {
            yield return child;
            foreach (var grandchild in Descendants(child)) yield return grandchild;
        }
    }
}
