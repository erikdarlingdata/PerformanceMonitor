/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Controls.Primitives;
using System.ComponentModel;
using System.Windows.Data;
using System.Windows.Threading;
using Microsoft.Data.SqlClient;
using Microsoft.Win32;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using ScottPlot;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Controls;

public partial class ServerTab : UserControl
{
    /// <summary>
    /// The range the toolbar picker holds, resolved against the clock now (#5562). A live range (the last 5 minutes,
    /// 'Today', 'since ...') slides with every call. When the held range cannot be used at this moment ('Today' in the
    /// first minutes after midnight is under the 5-minute floor) the last window that did resolve keeps being read, so
    /// a refresh never reads nothing; with none yet, the default of four hours.
    /// </summary>
    private ResolvedTimeRange CurrentRange()
    {
        if (RangePicker.Resolve() is { } resolved)
        {
            _lastResolvedRange = resolved;
            return resolved;
        }

        if (_lastResolvedRange != null)
        {
            return _lastResolvedRange;
        }

        LiteTimeRange.Default.TryResolve(DateTime.UtcNow, GetPickerZone(), out var fallback, out _);
        return fallback!;
    }

    private ResolvedTimeRange? _lastResolvedRange;

    /// <summary>
    /// How often the main collector of the page on screen samples, read from the schedule when asked (#5562, ruling R3): the
    /// picker shows 'Data here is collected every N minutes.' when a span holds fewer than 3 samples. The argument is the
    /// collector's name (<see cref="CurrentMainCollector"/>) and the answer its ACTUAL interval on this server. Set by
    /// MainWindow, which owns the schedule; a tab opened without one (a test) shows no note.
    /// </summary>
    private Func<string, TimeSpan?>? _sampleIntervalProvider;
    private bool _sampleNoteWired;

    /// <summary>Hands the tab the way to read a collector's actual interval on this server.</summary>
    public void SetSampleIntervalSource(Func<string, TimeSpan?> provider)
    {
        _sampleIntervalProvider = provider;
        if (!_sampleNoteWired)
        {
            _sampleNoteWired = true;

            /* A tab or sub-tab change moves the page on screen, so the note names that page's collector. */
            AddHandler(System.Windows.Controls.Primitives.Selector.SelectionChangedEvent, new SelectionChangedEventHandler((_, e) =>
            {
                if (e.OriginalSource is TabControl)
                {
                    RefreshRangeNotes();
                }
            }));
        }

        RefreshRangeNotes();
    }

    /// <summary>
    /// The main collector of the sub-tab on screen (<see cref="LiteTimeRange.MainCollectorFor"/>): the top tab's header and,
    /// when that tab holds a sub-tab control, the selected sub-tab's. Never widens the range; <c>null</c> shows no note.
    /// </summary>
    internal string? CurrentMainCollector()
    {
        if (MainTabControl.SelectedItem is not TabItem top)
        {
            return null;
        }

        string? sub = null;
        if (top.Content is DependencyObject content && FindFirstTabControl(content) is { SelectedItem: TabItem selected })
        {
            sub = selected.Header as string;
        }

        return LiteTimeRange.MainCollectorFor(top.Header as string, sub);
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
    /// The floors the tab's banner probes found (#5562 R7), by the page that asked: a collector name, or "overview" for the
    /// lanes on the Overview page. The picker names the floor of the page on screen; a page that has not probed yet, or a page
    /// with no probe, gets the archive's static retention edge. No query of its own: every value is one a banner site
    /// already awaited.
    /// </summary>
    private readonly Dictionary<string, DateTime?> _probedFloors = new();

    /// <summary>
    /// Hands the picker the data start a banner site found (every <c>ApplyWindowFloorToBanner</c> site calls this with its
    /// surface's main collector, so a source-scan test can pin the sites). It shows only while that page is on screen.
    /// </summary>
    internal void FeedDataStart(string? collector, DateTime? floor)
    {
        if (collector is not null)
        {
            _probedFloors[collector] = floor;
        }

        ApplyDataStart();
    }

    /// <summary>Points the picker's data start at the page on screen: its probed floor, else the static retention edge.</summary>
    private void ApplyDataStart()
    {
        var key = MainTabControl.SelectedItem is TabItem { Header: "Overview" } ? "overview" : CurrentMainCollector();
        RangePicker.DataStartUtc = LiteTimeRange.DataStartFor(
            key is not null && _probedFloors.TryGetValue(key, out var floor) ? floor : null, DateTime.UtcNow);
    }

    /// <summary>Redraws the picker's resolved text and its sample-interval note (a live range slides; the schedule can be edited).</summary>
    private void RefreshRangeNotes()
    {
        ApplyDataStart();
        RangePicker.SampleInterval = CurrentMainCollector() is { } collector ? _sampleIntervalProvider?.Invoke(collector) : null;
        RangePicker.Refresh();
    }

    /// <summary>
    /// Hours back from now that cover the range (#5562): the preset's own hours for a whole-hour rolling range, else the
    /// whole hours from the range's start to now rounded up (at least 1), so a reader that only takes hours back (a
    /// history window opened from a row) still covers a 15-minute span or a calendar period. Readers that carry
    /// instants take them from <see cref="GetCurrentWindowUtc"/>.
    /// </summary>
    private int GetHoursBack() => LiteTimeRange.WindowFor(CurrentRange()).hoursBack;

    /// <summary>
    /// True during a synchronous, programmatic write to the range controls (a display-mode switch re-rendering the pickers,
    /// a drill setting Custom), so those writes are not read as the user choosing a range and do not start a refresh (#5371).
    /// It used to share <c>_isRefreshing</c> with "a refresh is in flight", which made every range handler bail on an
    /// in-flight refresh and drop the user's change; the in-flight half is now the refresh coordinator's, which remembers it.
    /// </summary>
    private bool _suppressRangeRefresh;

    /// <summary>
    /// The zone the pickers show and are read in for <paramref name="mode"/>: UTC, this machine's zone, or the tab's OWN
    /// server clock (<paramref name="tabClock"/>, not the selected tab's, so a tab that is not the selected one keeps
    /// its own server's zone; a server with no collected clock keeps the fixed offset the connect probe read).
    /// </summary>
    internal static TimeZoneInfo PickerZone(TimeDisplayMode mode, ServerClock tabClock) =>
        ServerTimeHelper.DisplayZoneFor(mode, tabClock);

    private TimeZoneInfo GetPickerZone() => PickerZone(ServerTimeHelper.CurrentDisplayMode, _serverClock);

    /// <summary>
    /// The chart axis window as UTC instants (#4766), the frame every chart on this tab plots in: the custom range
    /// when one is set, else the last <paramref name="hoursBack"/> real hours ending now
    /// (<see cref="TimeWindows.ChartAxis"/>). The axis spans the same real hours the reads fetch, so a 24 hour
    /// preset is 24 hours across a daylight saving change, and its ends are the first and last rows' own instants.
    /// The display mode and the server's clock decide only how the ticks and the hover are worded
    /// (<see cref="GetPickerZone"/>), never where a point sits. <paramref name="utcNow"/> is a parameter so a test
    /// can drive it on either side of a clock change.
    /// </summary>
    internal static (DateTime Start, DateTime End) GetChartWindow(
        int hoursBack, DateTime? fromUtc, DateTime? toUtc, DateTime utcNow) =>
        TimeWindows.ChartAxis(hoursBack, fromUtc, toUtc, utcNow);

    /// <summary>The chart axis window, as of now.</summary>
    private (DateTime Start, DateTime End) GetChartWindow(int hoursBack, DateTime? fromDate, DateTime? toDate) =>
        GetChartWindow(hoursBack, fromDate, toDate, DateTime.UtcNow);

    /// <summary>
    /// Holds <paramref name="spec"/> on this tab as if the user picked it (used by Apply to All): the picker redraws, and its
    /// RangeChanged handler persists the choice and refreshes. A range this tab cannot use right now is left as it was.
    /// </summary>
    public void SetTimeRange(TimeRangeSpec spec) => RangePicker.Select(spec);

    private void ApplyTimeRangeToAll_Click(object sender, RoutedEventArgs e)
    {
        ApplyTimeRangeRequested?.Invoke(RangePicker.Value);
    }

    /* The _refreshTimer null guard in both handlers below is doing two jobs. It always absorbed the
       events InitializeComponent fires while applying the XAML defaults; since #3479 it is also what
       lets the constructor restore the saved values through these same handlers without persisting
       them back or touching a timer that is not built yet — the timer is constructed AFTER the
       restore, so "timer exists" is exactly "a change is the user's". */

    private void AutoRefreshCheckBox_Changed(object sender, RoutedEventArgs e)
    {
        if (_refreshTimer == null) return;

        if (AutoRefreshCheckBox.IsChecked == true)
        {
            UpdateAutoRefreshInterval();
            _refreshTimer.Start();
        }
        else
        {
            _refreshTimer.Stop();
        }

        PersistAutoRefresh();
    }

    private void AutoRefreshInterval_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (_refreshTimer == null) return;
        UpdateAutoRefreshInterval();
        PersistAutoRefresh();
    }

    private void UpdateAutoRefreshInterval()
    {
        if (AutoRefreshIntervalCombo == null) return;

        _refreshTimer.Interval = TimeSpan.FromSeconds(
            AutoRefreshSecondsForIndex(AutoRefreshIntervalCombo.SelectedIndex));
    }

    /// <summary>
    /// Seconds for each <c>AutoRefreshIntervalCombo</c> index — the ONE place the combo's items are
    /// given meaning (#3479). The timer interval, the persisted value and the restore all route through
    /// this pair, because the interval used to be mapped in one private switch with a silent default
    /// arm, and persistence would have added a second and third copy for drift to live between.
    /// <c>AutoRefreshMappingTests</c> pins the arm count to the XAML item count, so adding a combo item
    /// without teaching both directions fails a test instead of falling silently into the default arm.
    /// The default arm is the XAML default (one minute), preserving the old switch's behavior for an
    /// impossible index.
    /// </summary>
    internal static int AutoRefreshSecondsForIndex(int index) => index switch
    {
        0 => 30,
        1 => 60,
        2 => 300,
        _ => 60
    };

    /// <summary>
    /// The restore direction: a stored seconds value back to a combo index. A value the combo does not
    /// offer — a hand-edited settings.json, or a future build that once wrote intervals this one no
    /// longer has — lands on the XAML default index rather than on nothing, so the combo can never come
    /// up blank with the timer running an interval no item shows.
    /// </summary>
    internal static int AutoRefreshIndexForSeconds(int seconds) => seconds switch
    {
        30 => 0,
        60 => 1,
        300 => 2,
        _ => 1
    };

    /// <summary>
    /// #3479: remember both auto-refresh controls. Before this the pair was write-only in the other
    /// direction — the XAML hardcoded checked/one-minute, the handlers moved only the in-memory timer,
    /// and an operator who set five minutes was back at one on every launch. Same shape as
    /// <see cref="PersistSelectedTimeRange"/> and for the same reasons: the App-level statics first so a
    /// second tab opened THIS session constructs on the values just chosen, then one
    /// <see cref="App.WriteSetting"/> for both keys — they change from the same toolbar gesture, and two
    /// writes would be two chances for the file to hold half a preference. Failure is logged by
    /// WriteSetting and never interrupts the refresh.
    /// </summary>
    private void PersistAutoRefresh()
    {
        App.AutoRefreshEnabled = AutoRefreshCheckBox.IsChecked == true;
        App.AutoRefreshIntervalSeconds = AutoRefreshSecondsForIndex(AutoRefreshIntervalCombo.SelectedIndex);

        App.WriteSetting("auto-refresh", root =>
        {
            root["auto_refresh_enabled"] = App.AutoRefreshEnabled;
            root["auto_refresh_interval_seconds"] = App.AutoRefreshIntervalSeconds;
        });
    }

    private async void RefreshDataButton_Click(object sender, RoutedEventArgs e)
    {
        RefreshDataButton.IsEnabled = false;
        try
        {
            if (ManualRefreshRequested != null)
            {
                await ManualRefreshRequested.Invoke();
            }
            /* Manual refresh loads all sub-tabs of the visible tab, not all 13 tabs */
            await RefreshAllDataAsync();
        }
        finally
        {
            RefreshDataButton.IsEnabled = true;
        }
    }

    private async void TimeDisplayMode_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (!IsLoaded) return;
        if (TimeDisplayModeBox.SelectedItem is not ComboBoxItem item) return;
        var tag = item.Tag?.ToString();
        var mode = tag switch
        {
            "LocalTime" => TimeDisplayMode.LocalTime,
            "UTC" => TimeDisplayMode.UTC,
            _ => TimeDisplayMode.ServerTime
        };
        if (mode == ServerTimeHelper.CurrentDisplayMode) return;

        /* The held range is two instants (#4766), so a switch of display zone re-renders the same pair in the new
           zone and parses nothing back: a range in the repeated hour, or one the pickers cannot spell exactly,
           returns to exactly the instants it held. Suppress refreshes while updating pickers to avoid cascading queries. */
        _suppressRangeRefresh = true;
        try
        {
            ServerTimeHelper.CurrentDisplayMode = mode;
            RangePicker.Refresh();
        }
        finally
        {
            _suppressRangeRefresh = false;
        }

        // Refresh every grid so each row's time text (ServerTimeHelper.FormatServerTime / FormatServerClock) is read again in the new mode
        QuerySnapshotsGrid.Items.Refresh();
        QueryStatsGrid.Items.Refresh();
        ProcedureStatsGrid.Items.Refresh();
        QueryStoreGrid.Items.Refresh();
        BlockedProcessReportGrid.Items.Refresh();
        DeadlockGrid.Items.Refresh();
        RunningJobsGrid.Items.Refresh();
        CollectionHealthGrid.Items.Refresh();
        CollectionLogGrid.Items.Refresh();

        // Refresh slicer labels
        ActiveQueriesSlicer.Redraw();
        QueryStatsSlicer.Redraw();
        ProcStatsSlicer.Redraw();
        QueryStoreSlicer.Redraw();
        BlockingSlicer.Redraw();
        DeadlockSlicer.Redraw();

        /* #1831: the chart axes are labelled at render time in the display zone, but nothing
           re-rendered them on a toggle flip — the new mode only showed after the next data cycle,
           which read as "refresh doesn't help" in the field. Re-plot everything, the same full
           refresh a range change does — the Viewer's toggle ends with RefreshActiveInnerTabAsync
           for the same reason. */
        await RefreshAllDataAsync();
    }

    /// <summary>
    /// The picker's choice (a preset, a calendar period, a typed range or a picked day). Remembers it
    /// (<see cref="PersistSelectedTimeRange"/>) and re-reads every grid and chart. A programmatic write that must not
    /// refresh sets <c>_suppressRangeRefresh</c>; an in-flight refresh is the coordinator's to replay (#5371), so this
    /// does not look at <c>_isRefreshing</c>.
    ///
    /// <para>#2640: before the choice was remembered the control was write-only: the setting existed and was read at
    /// startup, but only the Settings window wrote it, so choosing a longer range here and restarting came back at four
    /// hours. A typed range is deliberately NOT persisted: restoring a window that ended two days ago would show an
    /// empty chart of a range the operator has moved on from.</para>
    /// </summary>
    private async void RangePicker_RangeChanged(object? sender, TimeRangeChangedEventArgs e)
    {
        if (!IsLoaded || _suppressRangeRefresh) return;

        PersistSelectedTimeRange(e.Spec);

        await RefreshAllDataAsync();
    }

    /// <summary>
    /// Writes the picked range to settings.json, the same keys the Settings window writes and startup reads
    /// (<see cref="LiteTimeRange.SettingsFor"/>): a whole-hour rolling range to the legacy <c>default_time_range_hours</c>
    /// (older builds read it), any other preset or calendar period to <c>default_time_range</c>. A typed range writes
    /// nothing. Failure is logged by <see cref="App.WriteSetting"/> and never interrupts the refresh.
    /// </summary>
    private void PersistSelectedTimeRange(TimeRangeSpec spec)
    {
        var (rangeId, hours) = LiteTimeRange.SettingsFor(spec);
        if (rangeId == null && hours == null)
        {
            return;
        }

        /* The in-memory values too, not only the file: a second server tab opened in this same session reads them in its
           constructor, and a tab that opens on a different range from the one just chosen is the same complaint. */
        App.ApplyDefaultTimeRange(rangeId, hours);

        App.WriteSetting("time range", root => App.WriteDefaultTimeRange(root, rangeId, hours));
    }
}
