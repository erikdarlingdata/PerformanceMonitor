/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// A navigable month heatmap of composite daily health bands, shared by Lite and the Darling viewer so
    /// the Performance Calendar can't drift between them. The host supplies <see cref="Days"/> (one banded
    /// <see cref="PerformanceCalendarDay"/> per collected day) and the displayed month; the control lays out
    /// a 6x7 grid, washes each in-month cell with its band color, and raises <see cref="MonthChanged"/> when
    /// the user navigates (so the host can fetch that month). Clicking a day opens the built-in day-detail
    /// panel; its drill buttons raise <see cref="DayDrillRequested"/> so the host can scope + navigate.
    /// </summary>
    public partial class PerformanceCalendar : UserControl
    {
        private const int DaysPerWeek = 7;

        public PerformanceCalendar()
        {
            InitializeComponent();
            BuildWeekdayHeaders();
            RebuildCells();
        }

        /// <summary>The banded days to render (one per collected day in the displayed month).</summary>
        public static readonly DependencyProperty DaysProperty = DependencyProperty.Register(
            nameof(Days), typeof(IEnumerable<PerformanceCalendarDay>), typeof(PerformanceCalendar),
            new PropertyMetadata(null, OnVisualInputChanged));
        public IEnumerable<PerformanceCalendarDay>? Days
        {
            get => (IEnumerable<PerformanceCalendarDay>?)GetValue(DaysProperty);
            set => SetValue(DaysProperty, value);
        }

        /// <summary>The month to display (any day within it; normalized to the first). Defaults to the current
        /// UTC month — the calendar operates in UTC day-space to match the collected data (naive-UTC collection times).</summary>
        public static readonly DependencyProperty DisplayMonthProperty = DependencyProperty.Register(
            nameof(DisplayMonth), typeof(DateTime), typeof(PerformanceCalendar),
            new PropertyMetadata(FirstOfMonth(DateTime.UtcNow), OnVisualInputChanged));
        public DateTime DisplayMonth
        {
            get => (DateTime)GetValue(DisplayMonthProperty);
            set => SetValue(DisplayMonthProperty, value);
        }

        /// <summary>The currently selected/drilled day, framed with an accent ring. Null when none.</summary>
        public static readonly DependencyProperty SelectedDateProperty = DependencyProperty.Register(
            nameof(SelectedDate), typeof(DateTime?), typeof(PerformanceCalendar),
            new PropertyMetadata(null, OnVisualInputChanged));
        public DateTime? SelectedDate
        {
            get => (DateTime?)GetValue(SelectedDateProperty);
            set => SetValue(SelectedDateProperty, value);
        }

        /// <summary>Raised when the user navigates to a different month (prev / next / Today), carrying the new month's first day.</summary>
        public event EventHandler<PerformanceCalendarMonthEventArgs>? MonthChanged;

        /// <summary>Raised when the user clicks a drill button in the day-detail panel (View Deadlocks /
        /// Blocking / Top Queries), carrying the day + which grid to jump to. The host scopes its
        /// toolbar to that day's window and switches to the target tab.</summary>
        public event EventHandler<PerformanceCalendarDrillEventArgs>? DayDrillRequested;

        /// <summary>The weekday column headers in the current culture's week order.</summary>
        public ObservableCollection<string> WeekdayHeaders { get; } = new();

        /// <summary>The rendered day cells: the weeks the month spans (four to six), seven cells each.</summary>
        public ObservableCollection<PerformanceCalendarCell> Cells { get; } = new();

        /// <summary>The supplied days indexed by date, for O(1) day-detail-panel lookup on click. Rebuilt
        /// alongside the cells whenever <see cref="Days"/> changes.</summary>
        private Dictionary<DateTime, PerformanceCalendarDay> _daysByDate = new();

        /// <summary>The date the day-detail panel is currently showing (drives the drill buttons). Null when hidden.</summary>
        private DateTime? _detailDate;

        private static DateTime FirstOfMonth(DateTime value) => new(value.Year, value.Month, 1);

        private static void OnVisualInputChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
        {
            ((PerformanceCalendar)d).RebuildCells();
        }

        private void BuildWeekdayHeaders()
        {
            WeekdayHeaders.Clear();
            var culture = CultureInfo.CurrentCulture;
            var firstDay = (int)culture.DateTimeFormat.FirstDayOfWeek;
            var names = culture.DateTimeFormat.AbbreviatedDayNames; // index 0 = Sunday
            for (var i = 0; i < DaysPerWeek; i++)
            {
                WeekdayHeaders.Add(names[(firstDay + i) % DaysPerWeek]);
            }
        }

        private void RebuildCells()
        {
            // MonthLabel/Cells may be referenced before the template is applied during construction.
            var month = FirstOfMonth(DisplayMonth);
            if (MonthLabel != null)
            {
                MonthLabel.Text = month.ToString("MMMM yyyy", CultureInfo.CurrentCulture);
            }

            // Index the supplied days by date for O(1) lookup (cells + day-detail panel).
            var byDate = new Dictionary<DateTime, PerformanceCalendarDay>();
            if (Days != null)
            {
                foreach (var day in Days)
                {
                    byDate[day.Date.Date] = day;
                }
            }
            _daysByDate = byDate;

            var today = DateTime.UtcNow.Date; // calendar days are UTC-bucketed; highlight the UTC "today"
            var selected = SelectedDate?.Date;

            // The grid starts on the first day-of-week on or before the 1st of the month.
            var firstDayOfWeek = (int)CultureInfo.CurrentCulture.DateTimeFormat.FirstDayOfWeek;
            var offset = ((int)month.DayOfWeek - firstDayOfWeek + DaysPerWeek) % DaysPerWeek;
            var gridStart = month.AddDays(-offset);

            // Only the weeks the month spans: a week row that holds no day of the month is not drawn.
            var weeks = (offset + DateTime.DaysInMonth(month.Year, month.Month) + DaysPerWeek - 1) / DaysPerWeek;

            Cells.Clear();
            for (var i = 0; i < weeks * DaysPerWeek; i++)
            {
                var date = gridStart.AddDays(i);
                var inMonth = date.Year == month.Year && date.Month == month.Month;

                var band = DailyHealthBand.NoData;
                var tooltip = string.Empty;
                if (inMonth && byDate.TryGetValue(date, out var day))
                {
                    band = day.Band;
                    tooltip = day.Tooltip;
                }
                else if (inMonth)
                {
                    tooltip = "No data collected.";
                }

                Cells.Add(new PerformanceCalendarCell(
                    date,
                    inMonth,
                    isToday: date == today,
                    isSelected: selected.HasValue && date == selected.Value,
                    band,
                    tooltip));
            }

            UpdateDayDetailPanel();
        }

        private void PrevMonthButton_Click(object sender, RoutedEventArgs e) =>
            NavigateToMonth(FirstOfMonth(DisplayMonth).AddMonths(-1));

        private void NextMonthButton_Click(object sender, RoutedEventArgs e) =>
            NavigateToMonth(FirstOfMonth(DisplayMonth).AddMonths(1));

        private void TodayButton_Click(object sender, RoutedEventArgs e) =>
            NavigateToMonth(FirstOfMonth(DateTime.UtcNow));

        private void NavigateToMonth(DateTime month)
        {
            DisplayMonth = month; // triggers RebuildCells via the DP callback
            MonthChanged?.Invoke(this, new PerformanceCalendarMonthEventArgs(month));
        }

        private void Cell_MouseLeftButtonUp(object sender, MouseButtonEventArgs e)
        {
            if (sender is FrameworkElement { DataContext: PerformanceCalendarCell cell } && cell.IsInDisplayMonth)
            {
                // Setting SelectedDate reframes the accent ring and (via the DP callback -> RebuildCells ->
                // UpdateDayDetailPanel) shows the day-detail panel for this day.
                SelectedDate = cell.Date;
            }
        }

        /// <summary>
        /// (Re)populates the day-detail panel for <see cref="SelectedDate"/>: a band-colored header, the
        /// "why this day is &lt;band&gt;" reasons, a key-metrics line, and the drill buttons that make sense for
        /// the day — all from the shared <see cref="DailyHealthBandCalculator"/>. Hidden when no day is selected
        /// or the selection isn't in the displayed month (e.g. after navigating to another month). A selected
        /// day that had no collection (absent from <see cref="Days"/>) still shows a No-Data panel.
        /// </summary>
        private void UpdateDayDetailPanel()
        {
            if (DayDetailPanel == null)
            {
                return; // template not applied yet (early construction)
            }

            var selected = SelectedDate?.Date;
            var month = FirstOfMonth(DisplayMonth);
            var inMonth = selected.HasValue && selected.Value.Year == month.Year && selected.Value.Month == month.Month;
            if (!inMonth)
            {
                DayDetailPanel.Visibility = Visibility.Collapsed;
                _detailDate = null;
                return;
            }

            _detailDate = selected!.Value;

            // A day absent from the supplied set had no collection -> render it as a No-Data day.
            _daysByDate.TryGetValue(selected.Value, out var day);
            var band = day?.Band ?? DailyHealthBand.NoData;
            var signals = day?.Signals ?? default; // default => HasData == false
            var hasData = signals.HasData;
            var bandLabel = DailyHealthBandCalculator.Label(band);

            DayDetailHeader.Text = $"{selected.Value:dddd, MMM d} — {bandLabel}";
            DayDetailHeader.SetResourceReference(TextBlock.ForegroundProperty, DailyHealthBandCalculator.BrushKey(band));

            DayDetailWhyLabel.Text = $"Why this day is {bandLabel}:";
            DayDetailReasons.ItemsSource = DailyHealthBandCalculator.BuildReasons(signals, day?.PeakBlockMs ?? 0);

            // Key metrics only make sense for a collected day.
            if (hasData)
            {
                DayDetailMetrics.Text = DailyHealthBandCalculator.BuildKeyMetricsLine(
                    day!.TopWaitType, signals.HighCpuEvents, day.TotalWaitSeconds, day.UniqueQueries);
                DayDetailMetrics.Visibility = Visibility.Visible;
            }
            else
            {
                DayDetailMetrics.Visibility = Visibility.Collapsed;
            }

            var drills = DailyHealthBandCalculator.AvailableDrills(signals);
            DeadlocksDrillButton.Visibility = drills.Contains(DayDrillTarget.Deadlocks) ? Visibility.Visible : Visibility.Collapsed;
            BlockingDrillButton.Visibility = drills.Contains(DayDrillTarget.Blocking) ? Visibility.Visible : Visibility.Collapsed;
            TopQueriesDrillButton.Visibility = drills.Contains(DayDrillTarget.TopQueries) ? Visibility.Visible : Visibility.Collapsed;

            DayDetailPanel.Visibility = Visibility.Visible;
        }

        private void DeadlocksDrillButton_Click(object sender, RoutedEventArgs e) => RaiseDrill(DayDrillTarget.Deadlocks);

        private void BlockingDrillButton_Click(object sender, RoutedEventArgs e) => RaiseDrill(DayDrillTarget.Blocking);

        private void TopQueriesDrillButton_Click(object sender, RoutedEventArgs e) => RaiseDrill(DayDrillTarget.TopQueries);

        private void RaiseDrill(DayDrillTarget target)
        {
            if (_detailDate is DateTime date)
            {
                DayDrillRequested?.Invoke(this, new PerformanceCalendarDrillEventArgs(date, target));
            }
        }
    }

    /// <summary>
    /// The render model for one calendar cell. Immutable; the control rebuilds the whole set on any change,
    /// so no change notification is needed. The presentation opacities/label are derived from the band here
    /// (rather than in XAML) so the cell template stays declarative.
    /// </summary>
    public sealed class PerformanceCalendarCell
    {
        public PerformanceCalendarCell(
            DateTime date, bool isInDisplayMonth, bool isToday, bool isSelected,
            DailyHealthBand band, string tooltip)
        {
            Date = date;
            IsInDisplayMonth = isInDisplayMonth;
            IsToday = isToday;
            IsSelected = isSelected;
            Band = band;
            Tooltip = tooltip;
            DayNumber = date.Day.ToString(CultureInfo.CurrentCulture);

            if (!isInDisplayMonth)
            {
                WashOpacity = 0.0;
                NumberOpacity = 0.45;
            }
            else if (band == DailyHealthBand.NoData)
            {
                WashOpacity = 0.14;
                NumberOpacity = 0.55;
            }
            else
            {
                WashOpacity = 0.55;
                NumberOpacity = 1.0;
            }

            // A textual band label on non-healthy in-month days aids at-a-glance scanning and color-blind
            // legibility; healthy / no-data cells stay uncluttered (color and emptiness carry the meaning).
            BandLabel = isInDisplayMonth && (band == DailyHealthBand.Warning || band == DailyHealthBand.Critical)
                ? DailyHealthBandCalculator.Label(band)
                : string.Empty;
        }

        public DateTime Date { get; }
        public bool IsInDisplayMonth { get; }
        public bool IsToday { get; }
        public bool IsSelected { get; }
        public DailyHealthBand Band { get; }
        public string Tooltip { get; }
        public string DayNumber { get; }
        public string BandLabel { get; }
        public double WashOpacity { get; }
        public double NumberOpacity { get; }
    }
}
