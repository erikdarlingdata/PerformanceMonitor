/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Threading;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The per-server toolbar's time-window + auto-refresh + time-display state (the header row added above
/// the inner tab strip), mirroring Lite's <c>ServerTab.TimeRange.cs</c>: a custom range is held as two
/// naive-UTC instants (<see cref="CustomRangeState"/>, #4766) and the From/To pickers are a drawing of them in
/// the CURRENT display mode's zone (Server/Local/UTC — <see cref="ViewerTimeHelper.DisplayZoneFor"/>, on this
/// tab's own server clock, ported from Lite's <c>TimeDisplayModeBox</c>). Only a typed edit reads text back,
/// through <see cref="CustomRangeState.ApplyEdit"/>, so a range that crosses a daylight saving change keeps the
/// instants it names and a display-mode switch changes only the text. This replaces the old hardcoded
/// 24-hour <c>s_dataWindow</c>: every inner-tab load reads <see cref="GetWindowUtc"/> (preset
/// 1h/4h/12h/24h/7d or a custom From/To), the auto-refresh cadence comes from the toolbar instead of
/// MainWindow's fleet-refresh timer, and the display-mode picker re-renders the visible tab so every
/// timestamp honors the chosen mode.
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>The "Custom Range" slot in <c>TimeRangeCombo</c> (after 1h/4h/12h/24h/7d).</summary>
    private const int CustomRangeIndex = 5;

    /// <summary>Guards the range handlers while pickers/combo are set programmatically (Apply-to-all,
    /// custom-range defaults) so one external change drives exactly one reload, not a cascade.</summary>
    private bool _suppressRangeEvents;

    private DispatcherTimer? _autoRefreshTimer;

    /// <summary>
    /// Raised when the user clicks "Apply to All": MainWindow broadcasts the selected range (index plus,
    /// for a custom range, the held From/To as naive-UTC instants) to every other open server
    /// tab. The source tab is carried so the broadcast can skip it (it already holds the range).
    /// </summary>
    public event Action<ViewerServerTab, int, DateTime?, DateTime?>? ApplyTimeRangeRequested;

    /// <summary>
    /// Raised when the user changes the toolbar's Server/Local/UTC display picker. MainWindow persists the
    /// new global mode to <see cref="ViewerAppSettings.TimeDisplayMode"/> and syncs every other open tab's
    /// picker (the mode is process-wide). The raising tab has already updated
    /// <see cref="ViewerTimeHelper.CurrentDisplayMode"/> and reloaded itself.
    /// </summary>
    public event Action<TimeDisplayMode>? DisplayModeChanged;

    /// <summary>This server's clock (from <c>server_properties</c>: its time zone id where SQL Server 2022 or
    /// later reports one, else <c>utc_offset_minutes</c>), applied to <see cref="ViewerTimeHelper.ActiveServerClock"/>
    /// before this tab renders so Server-time mode shows the monitored server's own wall clock, on both sides
    /// of a daylight-saving change. Seeds to the viewer machine's offset until the per-server value is loaded
    /// (so Server mode degrades gracefully to ~Local meanwhile), and is read again on every refresh (#4766).</summary>
    private ServerClock _serverClock =
        ServerClock.FixedOffset((int)TimeZoneInfo.Local.GetUtcOffset(DateTime.UtcNow).TotalMinutes);

    /// <summary>The custom range this tab holds (#4766): two naive-UTC instants, or nothing while a preset is
    /// selected. The From/To pickers are drawn from it (<see cref="RenderCustomRange"/>); a picker edit changes
    /// one side of it (<see cref="ApplyPickerEdit"/>) and nothing else reads the pickers' text back.</summary>
    private readonly CustomRangeState _customRange = new();

    /// <summary>The zone the pickers are drawn in and a typed value is read in: the current display mode's, on
    /// THIS tab's own server clock rather than the process-wide one another tab may have set last.</summary>
    private TimeZoneInfo TabDisplayZone => ViewerTimeHelper.DisplayZoneFor(ViewerTimeHelper.CurrentDisplayMode, _serverClock);

    /// <summary>Preset combo index → hours back. Index 5 (custom) and any stray value fall to the
    /// viewer's historical 24-hour default. Pure + static so the mapping is unit-testable.</summary>
    internal static int TimeRangeIndexToHours(int index) => index switch
    {
        0 => 1,
        1 => 4,
        2 => 12,
        3 => 24,
        4 => 168,
        _ => 24,
    };

    /// <summary>Populates the custom-range hour (00:00–23:00) and minute (:00/:15/:30/:45) combos,
    /// matching Lite's <c>InitializeTimeComboBoxes</c>. Called from the constructor after
    /// <c>InitializeComponent</c>.</summary>
    private void InitializeTimeComboBoxes()
    {
        var hours = new List<string>();
        for (int h = 0; h < 24; h++)
        {
            hours.Add(DateTime.Today.AddHours(h).ToString("HH:00"));
        }

        FromHourCombo.ItemsSource = hours;
        ToHourCombo.ItemsSource = hours;
        FromHourCombo.SelectedIndex = 0;   // 00:00
        ToHourCombo.SelectedIndex = 23;    // 23:00

        var minutes = new List<string> { ":00", ":15", ":30", ":45" };
        FromMinuteCombo.ItemsSource = minutes;
        ToMinuteCombo.ItemsSource = minutes;
        FromMinuteCombo.SelectedIndex = 0; // :00
        ToMinuteCombo.SelectedIndex = 3;   // :45
    }

    /// <summary>Wires the auto-refresh timer (default 1 minute, matching the old fixed 60-second cadence)
    /// and starts it when the toolbar's checkbox is checked. Called from the constructor.</summary>
    private void InitializeAutoRefreshTimer()
    {
        _autoRefreshTimer = new DispatcherTimer();
        UpdateAutoRefreshInterval();
        _autoRefreshTimer.Tick += OnAutoRefreshTick;
        if (AutoRefreshCheckBox.IsChecked == true)
        {
            _autoRefreshTimer.Start();
        }
    }

    private async void OnAutoRefreshTick(object? sender, EventArgs e)
    {
        /* Only the VISIBLE server tab auto-refreshes (the viewer's visible-only rule); a background tab's
           timer no-ops until the user switches to it, at which point MainWindow reloads it on activation. */
        if (!IsVisible)
        {
            return;
        }

        await RefreshActiveInnerTabAsync();
    }

    // ── Window computation ───────────────────────────────────────────────────────────

    /// <summary>True when the user picked "Custom Range", a range is held, AND both date pickers hold a value.</summary>
    private bool IsCustomRange => TimeRangeCombo.SelectedIndex == CustomRangeIndex
        && _customRange.IsCustom
        && FromDatePicker?.SelectedDate != null
        && ToDatePicker?.SelectedDate != null;

    /// <summary>
    /// The current window as store-native naive-UTC bounds, driving every inner-tab load (it replaces the
    /// old fixed 24-hour <c>s_dataWindow</c>). A preset ends "now"; a valid custom range uses its From/To.
    /// </summary>
    private (DateTime startUtc, DateTime endUtc) GetWindowUtc()
    {
        if (IsCustomRange)
        {
            var (fromUtc, toUtc) = GetCustomRangeUtc();
            if (fromUtc.HasValue && toUtc.HasValue)
            {
                return (fromUtc.Value, toUtc.Value);
            }
        }

        var endUtc = DateTime.UtcNow;
        return (endUtc.AddHours(-TimeRangeIndexToHours(TimeRangeCombo.SelectedIndex)), endUtc);
    }

    /// <summary>Hours-back for the reads that take an integer window (the Overview lanes + Collection
    /// Health log). A custom range collapses to its rounded-up span so those surfaces still track it.</summary>
    private int GetWindowHoursBack()
    {
        if (IsCustomRange)
        {
            var (fromUtc, toUtc) = GetCustomRangeUtc();
            if (fromUtc.HasValue && toUtc.HasValue)
            {
                var hours = (int)Math.Ceiling((toUtc.Value - fromUtc.Value).TotalHours);
                return hours < 1 ? 1 : hours;
            }
        }

        return TimeRangeIndexToHours(TimeRangeCombo.SelectedIndex);
    }

    /// <summary>Custom From/To as naive-UTC bounds for the Overview lanes (null,null when not a valid
    /// custom range — the lanes then fall back to their hours-back window).</summary>
    private (DateTime? fromUtc, DateTime? toUtc) GetOverviewCustomRange()
        => IsCustomRange ? GetCustomRangeUtc() : (null, null);

    /// <summary>The held custom range as naive-UTC bounds, or (null,null) while a preset is selected. The pickers
    /// are not parsed here: they are a drawing of these two instants, and a typed edit already changed the held
    /// side (<see cref="ApplyPickerEdit"/>), so a window that crosses a daylight saving change keeps the instants
    /// it names (#4766).</summary>
    private (DateTime? fromUtc, DateTime? toUtc) GetCustomRangeUtc()
        => _customRange.IsCustom ? (_customRange.FromUtc, _customRange.ToUtc) : (null, null);

    /// <summary>Draws the held range on the From/To pickers as the wall clock of <paramref name="zone"/> (the hour
    /// and 15-minute combos as always). Nothing is drawn while a preset is selected. Runs under
    /// <see cref="_suppressRangeEvents"/> so the drawing is not read back as an edit, and leaves the flag as the
    /// caller had it.</summary>
    private void RenderCustomRange(TimeZoneInfo zone)
    {
        if (FromDatePicker is null || _customRange.Render(zone) is not { } wall)
        {
            return;
        }

        var wasSuppressed = _suppressRangeEvents;
        _suppressRangeEvents = true;
        try
        {
            FromDatePicker.SelectedDate = wall.From.Date;
            FromHourCombo.SelectedIndex = wall.From.Hour;
            FromMinuteCombo.SelectedIndex = wall.From.Minute / 15;
            ToDatePicker.SelectedDate = wall.To.Date;
            ToHourCombo.SelectedIndex = wall.To.Hour;
            ToMinuteCombo.SelectedIndex = wall.To.Minute / 15;
        }
        finally
        {
            _suppressRangeEvents = wasSuppressed;
        }
    }

    /// <summary>Holds what the two pickers currently read as the custom range: each typed value is a wall clock in
    /// <paramref name="zone"/>, and FROM takes the earliest instant that reads at or after it and TO the latest at
    /// or before it (<see cref="DisplayZone.ToUtcBound"/>). Returns false, holding nothing, when a date is unset.</summary>
    private bool HoldPickersAsRange(TimeZoneInfo zone)
    {
        var from = GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo);
        var to = GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);
        if (!from.HasValue || !to.HasValue)
        {
            return false;
        }

        _customRange.Set(
            DisplayZone.ToUtcBound(from.Value, zone, BoundSide.From),
            DisplayZone.ToUtcBound(to.Value, zone, BoundSide.To));
        return true;
    }

    /// <summary>A user edit of one picker (its date, hour or minute): reads that side's typed wall clock back as an
    /// instant in this tab's zone (<see cref="CustomRangeState.ApplyEdit"/>) and draws both pickers from the held
    /// range again, so a time that never happened shows the change instant and the other side is untouched. With no
    /// range held yet, both pickers are held together.</summary>
    private void ApplyPickerEdit(object? sender)
    {
        var zone = TabDisplayZone;
        var fromSide = ReferenceEquals(sender, FromDatePicker)
            || ReferenceEquals(sender, FromHourCombo)
            || ReferenceEquals(sender, FromMinuteCombo);
        var side = fromSide ? BoundSide.From : BoundSide.To;
        var wall = fromSide
            ? GetDateTimeFromPickers(FromDatePicker!, FromHourCombo, FromMinuteCombo)
            : GetDateTimeFromPickers(ToDatePicker!, ToHourCombo, ToMinuteCombo);

        if (_customRange.IsCustom && wall.HasValue)
        {
            _customRange.ApplyEdit(wall.Value, side, zone);
        }
        else if (!HoldPickersAsRange(zone))
        {
            return;
        }

        RenderCustomRange(zone);
    }

    private static DateTime? GetDateTimeFromPickers(DatePicker datePicker, ComboBox hourCombo, ComboBox minuteCombo)
    {
        if (datePicker?.SelectedDate is not { } date)
        {
            return null;
        }

        int hour = hourCombo.SelectedIndex >= 0 ? hourCombo.SelectedIndex : 0;
        int minute = minuteCombo.SelectedIndex >= 0 ? minuteCombo.SelectedIndex * 15 : 0;
        return date.Date.AddHours(hour).AddMinutes(minute);
    }

    // ── Toolbar handlers ─────────────────────────────────────────────────────────────

    private async void TimeRangeCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _suppressRangeEvents)
        {
            return;
        }

        var isCustom = TimeRangeCombo.SelectedIndex == CustomRangeIndex;
        var visibility = isCustom ? Visibility.Visible : Visibility.Collapsed;

        if (FromDatePicker != null)
        {
            FromDatePicker.Visibility = visibility;
            FromHourCombo.Visibility = visibility;
            FromMinuteCombo.Visibility = visibility;
            ToLabel.Visibility = visibility;
            ToDatePicker.Visibility = visibility;
            ToHourCombo.Visibility = visibility;
            ToMinuteCombo.Visibility = visibility;

            if (isCustom)
            {
                /* Hold what the pickers read as the range, in this tab's zone (#4766). With no date yet, seed a
                   sensible default first so switching to Custom shows a real window; the picker changes below
                   drive the reload. Suppress so the picker writes coalesce into one load. */
                _suppressRangeEvents = true;
                try
                {
                    if (FromDatePicker.SelectedDate == null)
                    {
                        FromDatePicker.SelectedDate = DateTime.Today.AddDays(-1);
                        ToDatePicker.SelectedDate = DateTime.Today;
                    }

                    var zone = TabDisplayZone;
                    if (HoldPickersAsRange(zone))
                    {
                        RenderCustomRange(zone);
                    }
                }
                finally
                {
                    _suppressRangeEvents = false;
                }
            }

            if (!isCustom)
            {
                /* #2154: a DatePicker's calendar dropdown is a POPUP, which lives outside the visual
                   tree's visibility — collapsing the picker does not close an already-open dropdown,
                   so backing out of Custom Range without picking a date left an orphaned floating
                   calendar on screen. Close them explicitly alongside the collapse (Lite twin fix). */
                FromDatePicker.IsDropDownOpen = false;
                ToDatePicker.IsDropDownOpen = false;
            }
        }

        if (!isCustom)
        {
            /* A preset is in force: no range is held (#4766). Choosing Custom again holds whatever the pickers
               read, so nothing here keeps the old instants alive under the preset. */
            _customRange.Clear();
        }

        /* Presets reload here; a custom range reloads off the picker changes (below), except the seeded
           default above which is suppressed — so drive that one reload explicitly. */
        if (!isCustom || FromDatePicker?.SelectedDate != null)
        {
            await RefreshActiveInnerTabAsync();
        }
    }

    private async void CustomDateRange_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _suppressRangeEvents)
        {
            return;
        }

        if (FromDatePicker?.SelectedDate != null && ToDatePicker?.SelectedDate != null)
        {
            ApplyPickerEdit(sender);
            await RefreshActiveInnerTabAsync();
        }
    }

    private async void CustomTimeCombo_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _suppressRangeEvents)
        {
            return;
        }

        if (FromDatePicker?.SelectedDate != null && ToDatePicker?.SelectedDate != null)
        {
            ApplyPickerEdit(sender);
            await RefreshActiveInnerTabAsync();
        }
    }

    private async void RefreshDataButton_Click(object sender, RoutedEventArgs e)
    {
        RefreshDataButton.IsEnabled = false;
        try
        {
            await RefreshActiveInnerTabAsync();
        }
        finally
        {
            RefreshDataButton.IsEnabled = true;
        }
    }

    private void AutoRefreshCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        if (_autoRefreshTimer == null)
        {
            return;
        }

        if (AutoRefreshCheckBox.IsChecked == true)
        {
            UpdateAutoRefreshInterval();
            _autoRefreshTimer.Start();
        }
        else
        {
            _autoRefreshTimer.Stop();
        }
    }

    private void AutoRefreshInterval_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (_autoRefreshTimer == null)
        {
            return;
        }

        UpdateAutoRefreshInterval();
    }

    private void UpdateAutoRefreshInterval()
    {
        if (_autoRefreshTimer == null || AutoRefreshIntervalCombo == null)
        {
            return;
        }

        _autoRefreshTimer.Interval = AutoRefreshIntervalCombo.SelectedIndex switch
        {
            0 => TimeSpan.FromSeconds(30),
            1 => TimeSpan.FromMinutes(1),
            2 => TimeSpan.FromMinutes(5),
            _ => TimeSpan.FromMinutes(1),
        };
    }

    private void ApplyTimeRangeToAll_Click(object sender, RoutedEventArgs e)
    {
        /* The held instants go out, not the pickers' wall clock: every other tab draws them in its own server's
           zone, so all of them window on the same period (#4766). */
        DateTime? fromUtc = null;
        DateTime? toUtc = null;
        if (TimeRangeCombo.SelectedIndex == CustomRangeIndex && _customRange.IsCustom)
        {
            fromUtc = _customRange.FromUtc;
            toUtc = _customRange.ToUtc;
        }

        ApplyTimeRangeRequested?.Invoke(this, TimeRangeCombo.SelectedIndex, fromUtc, toUtc);
    }

    // ── Time-display mode (Server / Local / UTC) ─────────────────────────────────────

    /// <summary>Maps a picker item's Tag to the mode; unknown/absent falls to Server-time (Lite's default).</summary>
    private static TimeDisplayMode ParseDisplayMode(string? tag) => tag switch
    {
        "LocalTime" => TimeDisplayMode.LocalTime,
        "UTC" => TimeDisplayMode.UTC,
        _ => TimeDisplayMode.ServerTime,
    };

    /// <summary>
    /// Reads this server's clock (from <c>server_properties</c>) again. Awaited before each render (see
    /// <c>RefreshActiveInnerTabAsync</c>) so the visible tab's timestamps use ITS server's clock, and read
    /// every time rather than cached for the tab's life: a server that moves to a new zone, or upgrades to a
    /// version that reports a zone id, is picked up on the next refresh (#4766). A failure, or nothing
    /// collected yet, keeps the clock the tab already has (the machine-local seed until the first read).
    /// </summary>
    internal async Task RefreshServerClockAsync()
    {
        try
        {
            var clock = await _dataService.GetServerClockAsync(_server.ServerId);
            if (clock is not null)
            {
                _serverClock = clock;
            }
        }
        catch
        {
            /* No collected offset yet (or a read hiccup): keep the clock the tab already has so Server mode
               degrades gracefully to ~Local until server_properties.utc_offset_minutes is populated. */
        }

        /* The pickers are a drawing of the held instants in this tab's zone, and the clock is what may just have
           changed it (the first read after a range was applied from another tab, or a server that moved zone), so
           draw them again. Nothing is drawn while a preset is selected (#4766). */
        RenderCustomRange(TabDisplayZone);
    }

    /// <summary>Pushes this tab's server clock onto the process-wide helper. Called before every render so
    /// the visible tab (only it renders) drives the conversions with its own server's clock.</summary>
    internal void ApplyServerClockToHelper() => ViewerTimeHelper.ActiveServerClock = _serverClock;

    /// <summary>
    /// The Server/Local/UTC picker changed: apply this tab's clock, set the new global mode and draw the held
    /// custom range in the new mode's zone (same instants, only the text changes), then persist (via
    /// <see cref="DisplayModeChanged"/>) and reload the visible tab so every timestamp and chart re-renders
    /// in the new mode. No-op while loading/suppressed or when the mode is unchanged.
    /// </summary>
    private async void TimeDisplayMode_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded || _suppressRangeEvents)
        {
            return;
        }
        if (TimeDisplayModeBox.SelectedItem is not ComboBoxItem item)
        {
            return;
        }

        var mode = ParseDisplayMode(item.Tag?.ToString());
        if (mode == ViewerTimeHelper.CurrentDisplayMode)
        {
            return;
        }

        /* Server-mode conversions (and the picker drawing below) need this server's clock. */
        await RefreshServerClockAsync();
        ApplyServerClockToHelper();

        /* The same held instants stay selected across the switch: only the text the pickers show changes, so
           nothing is parsed back and a range that crosses a daylight saving change returns to the instants it
           had (#4766). Suppress range events while drawing them so this drives exactly one reload (below), not
           a cascade. */
        _suppressRangeEvents = true;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            RenderCustomRange(ViewerTimeHelper.DisplayZoneFor(mode, _serverClock));
        }
        finally
        {
            _suppressRangeEvents = false;
        }

        DisplayModeChanged?.Invoke(mode);
        await RefreshActiveInnerTabAsync();
    }

    /// <summary>
    /// Sets the toolbar's display-mode picker WITHOUT raising a change — used by MainWindow to keep every
    /// open tab's picker in sync when the global mode changes elsewhere (another tab's picker, or the
    /// Settings window). Suppressed so it drives no reload/persist. Also used to seed the picker on
    /// construction from the persisted mode.
    /// </summary>
    public void SetDisplayModeSelection(TimeDisplayMode mode)
    {
        if (TimeDisplayModeBox is null)
        {
            return;
        }

        var tag = mode.ToString();
        _suppressRangeEvents = true;
        try
        {
            foreach (var candidate in TimeDisplayModeBox.Items)
            {
                if (candidate is ComboBoxItem item && item.Tag?.ToString() == tag)
                {
                    TimeDisplayModeBox.SelectedItem = item;
                    break;
                }
            }

            /* The mode is process-wide, so a range this tab holds is drawn in the new mode's zone here too,
               not left showing the old mode's text until the tab reloads (#4766). */
            RenderCustomRange(ViewerTimeHelper.DisplayZoneFor(mode, _serverClock));
        }
        finally
        {
            _suppressRangeEvents = false;
        }
    }

    /// <summary>
    /// Applies a range chosen on another server tab (the "Apply to All" broadcast). The range arrives as
    /// naive-UTC instants: this tab holds them and draws them in ITS zone, so every tab windows on the same period
    /// whatever its server's clock (#4766). Sets the pickers and combo under the suppress guard so the copy
    /// doesn't cascade multiple reloads, then drives exactly one reload of this tab's active inner tab.
    /// </summary>
    public void ApplyExternalTimeRange(int index, DateTime? customFromUtc, DateTime? customToUtc)
    {
        _suppressRangeEvents = true;
        try
        {
            var isCustom = index == CustomRangeIndex && customFromUtc.HasValue && customToUtc.HasValue;
            var visibility = isCustom ? Visibility.Visible : Visibility.Collapsed;

            if (FromDatePicker != null)
            {
                FromDatePicker.Visibility = visibility;
                FromHourCombo.Visibility = visibility;
                FromMinuteCombo.Visibility = visibility;
                ToLabel.Visibility = visibility;
                ToDatePicker.Visibility = visibility;
                ToHourCombo.Visibility = visibility;
                ToMinuteCombo.Visibility = visibility;
            }

            if (isCustom)
            {
                _customRange.Set(customFromUtc!.Value, customToUtc!.Value);
                RenderCustomRange(TabDisplayZone);
            }
            else
            {
                _customRange.Clear();
            }

            TimeRangeCombo.SelectedIndex = index;
        }
        finally
        {
            _suppressRangeEvents = false;
        }

        _ = RefreshActiveInnerTabAsync();
    }

    /// <summary>
    /// Scopes the toolbar to an explicit naive-UTC <c>[from, to)</c> window — the Performance Calendar day
    /// drill's "set the time window to that day" step. Selects "Custom Range", reveals the pickers, holds the
    /// bounds and draws them in the active display mode's zone (<see cref="CustomRangeState.Render"/>), so
    /// <see cref="GetWindowUtc"/> returns exactly the window given. Runs under <see cref="_suppressRangeEvents"/>
    /// so it drives no reload of its own — the caller (the day drill) loads the target inner tab itself. Mirrors
    /// <see cref="ApplyExternalTimeRange"/>'s picker manipulation, without the reload.
    /// </summary>
    internal void SetToolbarWindowUtc(DateTime fromUtc, DateTime toUtc)
    {
        _customRange.Set(fromUtc, toUtc);

        _suppressRangeEvents = true;
        try
        {
            if (FromDatePicker != null)
            {
                FromDatePicker.Visibility = Visibility.Visible;
                FromHourCombo.Visibility = Visibility.Visible;
                FromMinuteCombo.Visibility = Visibility.Visible;
                ToLabel.Visibility = Visibility.Visible;
                ToDatePicker.Visibility = Visibility.Visible;
                ToHourCombo.Visibility = Visibility.Visible;
                ToMinuteCombo.Visibility = Visibility.Visible;
            }

            RenderCustomRange(TabDisplayZone);
            TimeRangeCombo.SelectedIndex = CustomRangeIndex;
        }
        finally
        {
            _suppressRangeEvents = false;
        }
    }

    // ── Custom-range DatePicker calendar theming (viewer is fixed Dark) ───────────────

    private void DatePicker_CalendarOpened(object sender, RoutedEventArgs e)
    {
        if (sender is not DatePicker datePicker)
        {
            return;
        }

        /* Defer so the popup's visual tree exists before we re-color it. */
        Dispatcher.BeginInvoke(new Action(() =>
        {
            if (datePicker.Template.FindName("PART_Popup", datePicker) is Popup { Child: Calendar calendar })
            {
                ApplyDarkThemeToCalendar(calendar);
            }
        }));
    }

    private static void ApplyDarkThemeToCalendar(Calendar calendar)
    {
        var primaryBg = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#111217")!);
        var fg = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#E4E6EB")!);
        var borderBrush = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#2a2d35")!);

        calendar.Background = primaryBg;
        calendar.Foreground = fg;
        calendar.BorderBrush = borderBrush;
        ApplyDarkThemeRecursively(calendar, primaryBg, fg);
    }

    private static void ApplyDarkThemeRecursively(DependencyObject parent, Brush primaryBg, Brush fg)
    {
        for (int i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);

            switch (child)
            {
                case CalendarItem calendarItem:
                    calendarItem.Background = primaryBg;
                    calendarItem.Foreground = fg;
                    break;
                case CalendarDayButton dayButton:
                    dayButton.Background = Brushes.Transparent;
                    dayButton.Foreground = fg;
                    break;
                case CalendarButton calButton:
                    calButton.Background = Brushes.Transparent;
                    calButton.Foreground = fg;
                    break;
                case Button button:
                    button.Background = Brushes.Transparent;
                    button.Foreground = fg;
                    break;
                case TextBlock textBlock:
                    textBlock.Foreground = fg;
                    break;
                case Border { Background: SolidColorBrush bg } border when bg.Color is { R: > 200, G: > 200, B: > 200 }:
                    border.Background = primaryBg;
                    break;
                case Grid { Background: SolidColorBrush gridBg } grid when gridBg.Color is { R: > 200, G: > 200, B: > 200 }:
                    grid.Background = primaryBg;
                    break;
            }

            ApplyDarkThemeRecursively(child, primaryBg, fg);
        }
    }
}
