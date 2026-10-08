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
using System.Windows.Threading;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The per-server toolbar's time-window + auto-refresh + time-display state (the header row added above
/// the inner tab strip), mirroring Lite's <c>ServerTab.TimeRange.cs</c>. The window is the shared
/// <see cref="TimeRangePicker"/> (#5562): rolling presets from 5 minutes, calendar periods, or a typed or
/// calendar-picked range, held as a <see cref="TimeRangeSpec"/> that the picker draws in this tab's display zone
/// (Server/Local/UTC, <see cref="ViewerTimeHelper.DisplayZoneFor"/> on this tab's own server clock). A fixed range
/// holds its two naive-UTC instants, so a display-mode switch only redraws the text (#4766) and a range that
/// crosses a daylight saving change keeps the instants it names. Every inner-tab load reads <see cref="GetWindowUtc"/>
/// (<see cref="ViewerTimeRangeWindow"/> does the arithmetic), the auto-refresh cadence comes from the toolbar instead
/// of MainWindow's fleet-refresh timer, and the display-mode picker re-renders the visible tab so every timestamp
/// honors the chosen mode. There is no reach limit on the desktop (#5562): a long period reads what the data holds,
/// through the retention routers the long-window reads already use.
/// </summary>
public partial class ViewerServerTab
{
    /// <summary>Guards the range handlers while the picker or the display-mode box is set programmatically (Apply-to-all,
    /// the day drill, the default seed) so one external change drives exactly one reload, not a cascade.</summary>
    private bool _suppressRangeEvents;

    private DispatcherTimer? _autoRefreshTimer;

    /// <summary>
    /// Raised when the user clicks "Apply to All": MainWindow broadcasts the held range to every other open server
    /// tab (<see cref="TimeRangePresets.ForBroadcast"/>: a calendar period goes as the instants it names). The
    /// source tab is carried so the broadcast can skip it (it already holds the range).
    /// </summary>
    public event Action<ViewerServerTab, TimeRangeSpec>? ApplyTimeRangeRequested;

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

    /// <summary>The zone the picker draws and reads typed times in: the current display mode's, on THIS tab's own
    /// server clock rather than the process-wide one another tab may have set last.</summary>
    private TimeZoneInfo TabDisplayZone => ViewerTimeHelper.DisplayZoneFor(ViewerTimeHelper.CurrentDisplayMode, _serverClock);

    /// <summary>The range the toolbar holds (rolling, calendar period, fixed or since).</summary>
    internal TimeRangeSpec HeldRange => RangePicker.Value;

    /// <summary>Preset combo index → hours back, the legacy mapping the preferences file still uses
    /// (<see cref="ViewerTimeRangeWindow.LegacyIndexToHours"/>). Index 5 (the old Custom slot) and any stray value fall to
    /// the viewer's historical 24-hour default. Pure + static so the mapping is unit-testable.</summary>
    internal static int TimeRangeIndexToHours(int index) => ViewerTimeRangeWindow.LegacyIndexToHours(index);

    /// <summary>Wires the picker to this tab: its display zone and the range it starts on (the persisted default).
    /// Called from the constructor after <c>InitializeComponent</c>.</summary>
    private void InitializeRangePicker(ViewerPreferences preferences)
    {
        RangePicker.ZoneProvider = () => TabDisplayZone;
        RangePicker.Value = ViewerTimeRangeWindow.DefaultFor(preferences);
    }

    /// <summary>The picker raised a pick (preset, period, typed text or calendar): reload the active inner tab, which
    /// reads the new window through <see cref="GetWindowUtc"/>.</summary>
    private async void RangePicker_RangeChanged(object? sender, TimeRangeChangedEventArgs e)
    {
        if (!IsLoaded || _suppressRangeEvents)
        {
            return;
        }

        await RefreshActiveInnerTabAsync();
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

    /// <summary>
    /// The current window as store-native naive-UTC bounds, driving every inner-tab load (it replaces the
    /// old fixed 24-hour <c>s_dataWindow</c>). A live range ends "now" and slides on every call; a fixed range or a
    /// finished calendar period keeps its instants. A sub-hour span (5, 15, 30 minutes) flows through unchanged: the
    /// reads take two instants and size their buckets in minutes.
    /// </summary>
    private (DateTime startUtc, DateTime endUtc) GetWindowUtc()
    {
        var (startUtc, endUtc, _) = ViewerTimeRangeWindow.Window(RangePicker.Value, DateTime.UtcNow, TabDisplayZone);
        return (startUtc, endUtc);
    }

    /// <summary>True when the window ends in the past (a fixed range, or a calendar period that has finished), so a read
    /// that is start-only server-side must bound its end itself (the CPU chart). A window that slides with now is not.</summary>
    private bool IsCustomRange => !ViewerTimeRangeWindow.Window(RangePicker.Value, DateTime.UtcNow, TabDisplayZone).IsLive;

    /// <summary>Hours-back for the reads that take an integer window: the window's span rounded up to whole hours, at
    /// least one (<see cref="ViewerTimeRangeWindow.HoursBack"/>).</summary>
    private int GetWindowHoursBack()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        return ViewerTimeRangeWindow.HoursBack(startUtc, endUtc);
    }

    /// <summary>The window's two bounds for the Overview lanes, ALWAYS as the explicit pair: the lanes then plot exactly the
    /// window (a 15-minute or a calendar-period window is not rounded to whole hours back from now).</summary>
    private (DateTime? fromUtc, DateTime? toUtc) GetOverviewCustomRange()
    {
        var (startUtc, endUtc) = GetWindowUtc();
        return (startUtc, endUtc);
    }

    private static readonly TimeSpan ScheduleOverridesMaxAge = TimeSpan.FromMinutes(5);

    private IReadOnlyList<CollectorScheduleRow>? _scheduleOverrides;

    private string? _noteCollector;

    private DateTime _scheduleOverridesReadUtc;

    /// <summary>Redraws the picker's resolved text from the clock, the zone and the notes. Run when the display zone moved
    /// and on every refresh, so a range that slides with now shows the window it is reading.</summary>
    private void RefreshRangePicker() => RangePicker.Refresh();

    /// <summary>The main collector of the page on screen: the inner tab's header and, when that tab holds a sub-tab control, the
    /// selected sub-tab's (<see cref="ViewerTimeRangeWindow.MainCollectorFor(string?, string?)"/>). Never widens the range.</summary>
    private string? CurrentMainCollector()
    {
        if (InnerTabs?.SelectedItem is not TabItem top)
        {
            return null;
        }

        string? sub = null;
        if (top.Content is DependencyObject content && FindFirstTabControl(content) is { SelectedItem: TabItem selected })
        {
            sub = selected.Header as string;
        }

        return ViewerTimeRangeWindow.MainCollectorFor(top.Header as string, sub);
    }

    private static TabControl? FindFirstTabControl(DependencyObject parent)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(parent))
        {
            if (child is TabControl tabs)
            {
                return tabs;
            }

            if (child is DependencyObject next && FindFirstTabControl(next) is { } found)
            {
                return found;
            }
        }

        return null;
    }

    /// <summary>
    /// Sets the picker's "collected every N minutes" note (#5562 R3) from the main collector of the inner tab on screen and
    /// that collector's ACTUAL interval on this server (the schedule overrides, else the shipped default). A read that
    /// fails leaves the shipped default; a tab with no main collector shows no note.
    /// </summary>
    private async Task UpdateSampleIntervalAsync()
    {
        try
        {
            var collector = CurrentMainCollector();
            if (!string.Equals(collector, _noteCollector, StringComparison.Ordinal))
            {
                /* A different top-level tab: the data-start note belonged to the last tab's surface, so it clears here and
                   the new tab's own probe sets it again. */
                _noteCollector = collector;
                RecordDataStart(null);
            }

            IReadOnlyList<CollectorScheduleRow>? overrides = null;
            if (collector is not null)
            {
                try
                {
                    /* The schedule table is small but the refresh runs every 30 seconds to 5 minutes per tab, so the rows are
                       read at most every five minutes. */
                    if (_scheduleOverrides is null || DateTime.UtcNow - _scheduleOverridesReadUtc > ScheduleOverridesMaxAge)
                    {
                        _scheduleOverrides = await _dataService.GetCollectorSchedulesAsync();
                        _scheduleOverridesReadUtc = DateTime.UtcNow;
                    }

                    overrides = _scheduleOverrides;
                }
                catch (Exception ex)
                {
                    ViewerLogger.Warn("ViewerServerTab", $"time range note: could not read the collector schedule, using the shipped cadence | {ex.GetType().Name}: {ex.Message}");
                }
            }

            RangePicker.SampleInterval = ViewerTimeRangeWindow.SampleIntervalFor(collector, _server.ServerId, overrides);
        }
        catch (Exception ex)
        {
            ViewerLogger.Warn("ViewerServerTab", $"time range note failed | {ex.GetType().Name}: {ex.Message}");
        }
    }

    /// <summary>
    /// Sets the picker's "Data starts ..." note from a data-start probe a surface already awaited (#5562, the desktop twin
    /// of the web's retention note): the same <c>floor</c> the surface's "Showing since" banner reads. No query of its own.
    /// Every banner site reaches it through <see cref="UpdateTruncationBanner"/> and <see cref="ViewerDataStartNote.Feed"/>
    /// (#5562 R7), for the tab on screen. A null floor (the probe found nothing, or failed) clears the note.
    /// </summary>
    internal void RecordDataStart(DateTime? floor)
    {
        if (RangePicker.DataStartUtc != floor)
        {
            RangePicker.DataStartUtc = floor;
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
        /* A calendar period goes as the instants it names in THIS tab's zone, a fixed range as its held instants: every
           other tab draws them in its own server's zone, so all of them window on the same period (#4766, #5562). */
        ApplyTimeRangeRequested?.Invoke(this, TimeRangePresets.ForBroadcast(RangePicker.Value, DateTime.UtcNow, TabDisplayZone));
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

        /* The picker draws the held range in this tab's zone, and the clock is what may just have changed it (the
           first read after a range was applied from another tab, or a server that moved zone), so draw it again. A
           range that slides with now is redrawn from the clock too (#4766, #5562). */
        RefreshRangePicker();
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
            RefreshRangePicker();
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
               not left showing the old mode's text until the tab reloads (#4766). The picker reads the zone from
               ViewerTimeHelper.CurrentDisplayMode, which the caller has already set to this mode. */
            RefreshRangePicker();
        }
        finally
        {
            _suppressRangeEvents = false;
        }
    }

    /// <summary>
    /// Applies a range chosen on another server tab (the "Apply to All" broadcast). The range arrives as a spec whose
    /// instants are already fixed where they must be (<see cref="TimeRangePresets.ForBroadcast"/>): this tab holds
    /// it and draws it in ITS zone, so every tab windows on the same period whatever its server's clock (#4766). Sets
    /// the picker under the suppress guard so the copy doesn't cascade multiple reloads, then, when this is the
    /// visible tab, drives exactly one reload of its active inner tab. A hidden tab holds the range and reloads when it
    /// is selected, so it never puts its own server's clock on the shared <see cref="ViewerTimeHelper"/> while another
    /// tab is on screen.
    /// </summary>
    public void ApplyExternalTimeRange(TimeRangeSpec range)
    {
        _suppressRangeEvents = true;
        try
        {
            RangePicker.Value = range;
        }
        finally
        {
            _suppressRangeEvents = false;
        }

        /* Apply to All calls this on every open tab, hidden ones too. A reload writes this server's clock onto the
           process-wide ViewerTimeHelper, so a hidden tab reloading here would leave ITS clock on the visible tab's
           hover and ticks (the last one to finish wins). Only the visible tab reloads; a hidden one reloads when it
           is selected (MainWindow's tab switch), and shows the range it now holds then. Same rule as
           OnAutoRefreshTick (#4766). */
        if (IsVisible)
        {
            _ = RefreshActiveInnerTabAsync();
        }
    }

    /// <summary>
    /// Scopes the toolbar to an explicit naive-UTC <c>[from, to)</c> window — the Performance Calendar day
    /// drill's "set the time window to that day" step. Holds the bounds as a fixed range and draws them in the active
    /// display mode's zone, so <see cref="GetWindowUtc"/> returns exactly the window given. Runs under
    /// <see cref="_suppressRangeEvents"/> so it drives no reload of its own — the caller (the day drill) loads the target
    /// inner tab itself. Mirrors <see cref="ApplyExternalTimeRange"/>, without the reload.
    /// </summary>
    internal void SetToolbarWindowUtc(DateTime fromUtc, DateTime toUtc)
    {
        _suppressRangeEvents = true;
        try
        {
            RangePicker.Value = TimeRangeSpec.FixedRange(fromUtc, toUtc);
        }
        finally
        {
            _suppressRangeEvents = false;
        }
    }
}
