/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's per-USER preferences — the persisted DEFAULTS that seed a newly-opened server tab's
/// toolbar (see ViewerServerTab.TimeRange.cs). This is deliberately distinct from <see cref="ViewerSettings"/>,
/// which reads the SERVICE's darling.json (the Postgres connection): those are the operator's collection
/// config; these are the viewer's own tiny UI state, written when the user changes them in the Settings
/// window and read when each server tab opens. Two settings, three values: the default time-range preset,
/// and the default auto-refresh state (on/off + interval).
/// The values are stored as the toolbar combos' own indices — the toolbar is the only consumer and is
/// index-native (TimeRangeCombo.SelectedIndex, AutoRefreshIntervalCombo.SelectedIndex), so seeding a tab
/// is a direct assignment with no second mapping layer. The Settings window mirrors the same item order,
/// so an index means the same thing in both. Out-of-range values are clamped to the historical defaults
/// on load (see <see cref="Normalize"/>), so a hand-edited or forward-version file never leaves a combo
/// unselected. The mutable properties are what <see cref="System.Text.Json.JsonSerializer"/> round-trips.
/// </summary>
public sealed class ViewerPreferences
{
    /* The historical toolbar defaults (the XAML SelectedIndex / IsChecked values these settings replace):
       time-range index 3 = "Last 24 hours", auto-refresh ON, interval index 1 = "1m". A default-constructed
       ViewerPreferences therefore reproduces the pre-settings behavior exactly. */
    internal const int DefaultTimeRangeIndexValue = 3;
    internal const int DefaultAutoRefreshIntervalIndexValue = 1;

    /* The toolbar's time-range combo lists 1h / 4h / 12h / 24h / 7d / Custom; a persisted DEFAULT only
       covers the five presets (0..4) — "Custom Range" needs concrete From/To dates that make no sense as a
       standing default, so it is intentionally not offered here. The interval combo lists 30s / 1m / 5m (0..2). */
    internal const int MaxTimeRangeIndex = 4;
    internal const int MaxAutoRefreshIntervalIndex = 2;

    /// <summary>Default time window for a newly-opened server tab: 0=1h, 1=4h, 2=12h, 3=24h, 4=7d.</summary>
    public int DefaultTimeRangeIndex { get; set; } = DefaultTimeRangeIndexValue;

    /// <summary>Whether a newly-opened server tab starts with auto-refresh running.</summary>
    public bool AutoRefreshEnabled { get; set; } = true;

    /// <summary>Default auto-refresh cadence for a newly-opened server tab: 0=30s, 1=1m, 2=5m.</summary>
    public int AutoRefreshIntervalIndex { get; set; } = DefaultAutoRefreshIntervalIndexValue;

    /// <summary>
    /// Collapsed sidebar tag groups, so expand/collapse survives a restart. Each entry is a
    /// <c>FleetGroupKey</c> storage string ("Favorites", "Untagged", or "Tag:{id}"); unknown or stale
    /// entries (a deleted tag) are ignored on load. Empty by default, so a fresh fleet shows everything.
    /// </summary>
    public List<string> CollapsedFleetGroups { get; set; } = new();

    /// <summary>
    /// Clamps the two index values back to their defaults when they fall outside the combos' valid ranges
    /// (a corrupt, hand-edited, or forward-version file), then returns this same instance for chaining.
    /// Pure and side-effect-free beyond the clamp, so it is unit-testable without any file I/O.
    /// </summary>
    internal ViewerPreferences Normalize()
    {
        if (DefaultTimeRangeIndex < 0 || DefaultTimeRangeIndex > MaxTimeRangeIndex)
        {
            DefaultTimeRangeIndex = DefaultTimeRangeIndexValue;
        }

        if (AutoRefreshIntervalIndex < 0 || AutoRefreshIntervalIndex > MaxAutoRefreshIntervalIndex)
        {
            AutoRefreshIntervalIndex = DefaultAutoRefreshIntervalIndexValue;
        }

        return this;
    }
}

/// <summary>
/// Loads and saves <see cref="ViewerPreferences"/> as a small JSON file in the viewer's per-user app-data
/// directory (mirroring the Dashboard's <c>UserPreferencesService</c> idiom — %APPDATA%\{app}\*.json,
/// indented JSON, defaults on a missing or corrupt file). The default path is
/// %APPDATA%\PerformanceMonitorDarling\viewer-preferences.json: the Darling-branded per-user folder (the
/// Roaming root keeps it clear of the service's machine-wide %ProgramData%\PerformanceMonitorDarling data),
/// with the file namespaced to the viewer. The file path is injectable so tests round-trip against a temp
/// file instead of the real profile.
/// </summary>
public sealed class ViewerPreferencesStore
{
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    /// <summary>The <see cref="ViewerLogger"/> source every diagnostic from this store is filed under.</summary>
    private const string LogSource = "ViewerPreferencesStore";

    private readonly string _filePath;

    /// <param name="filePath">Override the on-disk location (tests pass a temp file); null uses <see cref="DefaultFilePath"/>.</param>
    public ViewerPreferencesStore(string? filePath = null)
    {
        _filePath = filePath ?? DefaultFilePath();
    }

    /// <summary>The resolved settings file path (surfaced mainly so tests and diagnostics can name it).</summary>
    public string FilePath => _filePath;

    /// <summary>
    /// What the last <see cref="Load"/> on this instance found. Same three-way split as
    /// <see cref="ViewerAppSettingsStore.LastLoadState"/>, and for the same reason: a missing
    /// viewer-preferences.json is a first run and must stay silent, while a present one that could not be
    /// read is a configuration the viewer is currently ignoring.
    /// </summary>
    public SettingsFileState LastLoadState { get; private set; } = SettingsFileState.Absent;

    /// <summary>Why the last <see cref="Load"/> could not read the file, or null when it could.</summary>
    public string? LastLoadProblem { get; private set; }

    /// <summary>
    /// The settings the last <see cref="Load"/> could not read, by NAME, and empty when there were none
    /// (#2456). Non-empty means the rest of the file loaded normally and only these reverted to their
    /// defaults — the distinction <see cref="LastLoadProblem"/> alone cannot make, and the reason the
    /// startup dialog can now say which settings were lost instead of only where the parse stopped.
    /// </summary>
    public IReadOnlyList<SettingsMemberProblem> LastLoadUnreadableMembers { get; private set; } =
        Array.Empty<SettingsMemberProblem>();

    /// <summary>%APPDATA%\PerformanceMonitorDarling\viewer-preferences.json.</summary>
    public static string DefaultFilePath()
    {
        var directory = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
            "PerformanceMonitorDarling");
        return Path.Combine(directory, "viewer-preferences.json");
    }

    /// <summary>
    /// Reads the preferences, returning defaults when the file does not exist yet or cannot be
    /// read/parsed. Loaded values are clamped to valid ranges. A present-but-unreadable file is reported
    /// rather than silently treated as absent (#2434).
    /// </summary>
    public ViewerPreferences Load()
    {
        var read = ViewerSettingsFile.Load<ViewerPreferences>(_filePath, LogSource, s_jsonOptions);
        LastLoadState = read.State;
        LastLoadProblem = read.Problem;
        LastLoadUnreadableMembers = read.UnreadableMembers ?? Array.Empty<SettingsMemberProblem>();
        return read.Value!.Normalize();
    }

    /// <summary>
    /// Writes the preferences as indented JSON, creating the app-data directory on first save, and returns
    /// whether the write happened. Collapsing a sidebar group is a whole-file rewrite of this document and
    /// nobody thinks of it as a save, which is precisely why the unreadable case is copied aside first and
    /// a refusal is reported rather than swallowed (#2434).
    /// </summary>
    public bool Save(ViewerPreferences preferences)
    {
        ArgumentNullException.ThrowIfNull(preferences);

        return ViewerSettingsFile.Save(_filePath, preferences, LogSource, s_jsonOptions);
    }
}
