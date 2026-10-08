/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Windows;
using System.Windows.Media;
using System.Windows.Threading;
using PerformanceMonitor.Ui;
using Xunit;

/* Darling.Tests only: a Darling file Lite.Tests compiles must be named in build.yml's Lite path filter. */
namespace Darling.Tests;

/// <summary>#5562: the shared WPF time range picker constructs, renders and raises its event on an STA thread.</summary>
public sealed class TimeRangePickerControlTests
{
    private static readonly TimeZoneInfo NewYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
    private static readonly DateTime Now = new(2026, 10, 8, 11, 1, 0, DateTimeKind.Unspecified);

    private static TimeRangePicker Build()
        => new() { ZoneProvider = () => NewYork, NowProvider = () => Now };

    [Fact]
    public void Constructs_WithTheShortPresetsAndTheCalendarPeriods()
    {
        OnStaThread(() =>
        {
            var picker = Build();
            Assert.Equal(9, picker.RollingPanel.Children.Count);
            Assert.Equal(8, picker.PeriodPanel.Children.Count);
            Assert.Equal("4h", picker.Value.Id);
            Assert.Equal("Past 4 hours", picker.ButtonText.Text);
            Assert.Equal("Oct 8, 3:01 am - Oct 8, 7:01 am (UTC-04:00)", picker.ResolvedText.Text);
            Assert.False(picker.IsPopupOpen);
        });
    }

    [Fact]
    public void Value_RendersWithoutRaisingTheEvent_AndSelectRaisesIt()
    {
        OnStaThread(() =>
        {
            var picker = Build();
            var raised = 0;
            TimeRangeChangedEventArgs? last = null;
            picker.RangeChanged += (_, e) => { raised++; last = e; };

            picker.Value = TimeRangeSpec.ForPeriod(CalendarPeriod.WeekToDate);
            Assert.Equal(0, raised);
            Assert.Equal("Week to Date", picker.ButtonText.Text);
            Assert.Equal("Oct 5, 12:00 am - Oct 8, 7:01 am (UTC-04:00)", picker.ResolvedText.Text);

            Assert.True(picker.Select(TimeRangePresets.Find("1w")!));
            Assert.Equal(1, raised);
            Assert.Equal("1w", last!.Spec.Id);
            Assert.Equal(TimeSpan.FromDays(7), last.Range.Span);
            Assert.Equal("1w", picker.Value.Id);
        });
    }

    [Fact]
    public void Select_RefusesARangeUnderTheMinimum_AndHoldsTheOldOne()
    {
        OnStaThread(() =>
        {
            var picker = Build();
            picker.NowProvider = () => new DateTime(2026, 10, 8, 4, 2, 0, DateTimeKind.Unspecified);
            var raised = false;
            picker.RangeChanged += (_, _) => raised = true;

            Assert.False(picker.Select(TimeRangeSpec.Relative(TimeSpan.FromMinutes(4))));
            Assert.False(raised);
            Assert.Equal("4h", picker.Value.Id);

            /* A calendar period is exempt from the floor: Today at 00:02 is held, never replaced by another range. */
            Assert.True(picker.Select(TimeRangeSpec.ForPeriod(CalendarPeriod.Today)));
            Assert.True(raised);
            Assert.Equal("today", picker.Value.Id);
        });
    }

    [Fact]
    public void Resolve_FollowsTheZoneProvider()
    {
        OnStaThread(() =>
        {
            var picker = Build();
            picker.Value = TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday);
            var eastern = picker.Resolve()!;
            picker.ZoneProvider = () => TimeZoneInfo.Utc;
            picker.Refresh();
            var utc = picker.Resolve()!;
            Assert.NotEqual(eastern.StartUtc, utc.StartUtc);
            Assert.Equal("Oct 7, 12:00 am - Oct 8, 12:00 am (UTC+00:00)", picker.ResolvedText.Text);
        });
    }

    [Fact]
    public void Notes_ShowTheDataStartAndTheCollectorInterval()
    {
        OnStaThread(() =>
        {
            var picker = Build();
            picker.Value = TimeRangePresets.Find("1w")!;
            Assert.Equal(Visibility.Collapsed, picker.NoteText.Visibility);

            picker.DataStartUtc = new DateTime(2026, 10, 3, 18, 0, 0);
            Assert.Equal(Visibility.Visible, picker.NoteText.Visibility);
            Assert.Equal("Data starts Oct 3, 2:00 pm", picker.NoteText.Text);

            picker.Value = TimeRangePresets.Find("5m")!;
            picker.DataStartUtc = null;
            picker.SampleInterval = TimeSpan.FromMinutes(5);
            Assert.Equal("Data here is collected every 5 minutes.", picker.NoteText.Text);

            picker.SampleInterval = TimeSpan.FromMinutes(1);
            Assert.Equal(Visibility.Collapsed, picker.NoteText.Visibility);
        });
    }

    [Fact]
    public void Compact_HidesTheDetailBesideTheButton_AndKeepsItInTheTooltip()
    {
        OnStaThread(() =>
        {
            var picker = Build();
            picker.Compact = true;
            Assert.Equal(Visibility.Collapsed, picker.DetailPanel.Visibility);
            Assert.Contains("(UTC-04:00)", (string)picker.PickerButton.ToolTip, StringComparison.Ordinal);
            picker.Compact = false;
            Assert.Equal(Visibility.Visible, picker.DetailPanel.Visibility);
        });
    }

    [Fact]
    public void TypingARange_PreviewsItBeforeApply_AndAnErrorDisablesApply()
    {
        OnStaThread(() =>
        {
            var picker = Build();
            picker.InputBox.Text = "last month";
            Assert.True(picker.ApplyButton.IsEnabled);
            Assert.Equal("30d  Sep 1, 12:00 am - Oct 1, 12:00 am (UTC-04:00)", picker.EchoText.Text);

            picker.InputBox.Text = "3m";
            Assert.False(picker.ApplyButton.IsEnabled);
            Assert.Contains("5 minutes", picker.EchoText.Text, StringComparison.Ordinal);
        });
    }

    [Fact]
    public void OpeningThePopup_ThemesTheCalendarFromTheHostBrushes_AndAPickedRangeFillsTheTextBox()
    {
        OnStaThread(() =>
        {
            var dark = new SolidColorBrush(Color.FromRgb(0x11, 0x12, 0x17));
            var light = new SolidColorBrush(Color.FromRgb(0xE4, 0xE6, 0xEB));
            var picker = Build();
            picker.Resources["BackgroundBrush"] = dark;
            picker.Resources["ForegroundBrush"] = light;
            var window = new Window { Content = picker, Width = 300, Height = 200, ShowActivated = false, WindowStyle = WindowStyle.None, ShowInTaskbar = false, Left = -4000, Top = -4000 };
            try
            {
                window.Show();
                picker.PickerPopup.IsOpen = true;
                // Let the Opened handler and its Loaded-priority theming pass run.
                window.Dispatcher.Invoke(() => { }, DispatcherPriority.ApplicationIdle);

                Assert.Same(dark, picker.PickCalendar.Background);
                Assert.Same(light, picker.PickCalendar.Foreground);
                Assert.Equal("Examples:", picker.EchoText.Text.Substring(0, 9));
                Assert.NotEmpty(picker.PeriodPanel.Children);

                picker.PickCalendar.SelectedDates.AddRange(new DateTime(2026, 10, 1), new DateTime(2026, 10, 3));
                Assert.Equal("Oct 1 - Oct 3", picker.InputBox.Text);
                Assert.True(picker.ApplyButton.IsEnabled);

                TimeRangeChangedEventArgs? picked = null;
                picker.RangeChanged += (_, e) => picked = e;
                picker.ApplyButton.RaiseEvent(new RoutedEventArgs(System.Windows.Controls.Primitives.ButtonBase.ClickEvent));
                Assert.NotNull(picked);
                Assert.Equal(TimeRangeKind.Fixed, picked!.Spec.Kind);
                Assert.Equal(new DateTime(2026, 10, 1, 4, 0, 0), picked.Range.StartUtc);
                Assert.Equal(new DateTime(2026, 10, 4, 4, 0, 0), picked.Range.EndUtc);
                Assert.False(picker.IsPopupOpen);
            }
            finally
            {
                window.Close();
            }
        });
    }

    /// <summary>WPF objects require STA; same shape as AvailabilityGroupsTabRefreshTests.</summary>
    private static void OnStaThread(Action body)
    {
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }
    }
}
