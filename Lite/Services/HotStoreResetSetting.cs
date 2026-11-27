/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The amount of data in the hot store at which Lite archives every hot table to Parquet and recreates <c>monitor.duckdb</c>
/// (#9238), and where that number came from. <see cref="Source"/> is <see cref="HotStoreResetSetting.SourceDefault"/> or
/// <see cref="HotStoreResetSetting.SourceSetting"/>.
/// </summary>
internal sealed record HotStoreResetLimit(double Mb, string Source)
{
    /// <summary>The limit in force for this process. <see cref="HotStoreResetSetting.LoadAtStartup"/> sets it once, before the
    /// background service and the store-health readers are built. Every reader goes through this one property.</summary>
    internal static HotStoreResetLimit Current { get; set; } = HotStoreResetSetting.DefaultLimit;

    /// <summary>The file length at which the store resets whatever the data in it, so the disk the hot file uses stays bounded.</summary>
    internal double FileCeilingMb => Mb * FileCeilingFactor;

    /// <summary>The file may grow to this many times the reset point before it forces a reset.</summary>
    internal const double FileCeilingFactor = 2;

    /// <summary>
    /// True when the store is due a reset (#9238). The reset point counts the DATA in the store (<paramref name="usedMb"/>, the used
    /// blocks), not the file: DuckDB keeps freed blocks in the file, so the file runs well past its data, and a file-length check
    /// reset the store long before the data warranted it. A null <paramref name="usedMb"/> (the read timed out behind a checkpoint or
    /// failed) does not decide: the file length is not the data, so falling back to it would bring back the early reset. Only the file
    /// reaching <see cref="FileCeilingMb"/>, twice the reset point, resets without the data read, and the next housekeeping tick
    /// reads again.
    /// </summary>
    internal bool IsReached(double? usedMb, double fileMb) => fileMb >= FileCeilingMb || (usedMb is { } used && used >= Mb);
}

/// <summary>
/// The <c>hot_store_reset_mb</c> setting (#9238). The reset point was a fixed 512 MB, and a headless install with six servers and
/// the default collectors already holds 470 to 520 MB, so it reset routinely and held collection for a few seconds each time.
/// Automatic scaling with the server count was not chosen: a larger hot file has a cost (see the guide), and an operator
/// should weigh it knowingly.
///
/// <para>A number of MB. The default is <see cref="DefaultMb"/>, the floor <see cref="MinMb"/> and the ceiling
/// <see cref="MaxMb"/>. A value outside the range is clamped to the nearest bound, and the log says so once. A value that is not a
/// number is the default, also logged once. It is read once at startup, so a changed value takes effect at the next start.</para>
/// </summary>
internal static class HotStoreResetSetting
{
    internal const string SettingsKey = "hot_store_reset_mb";

    internal const double DefaultMb = 512;

    internal const double MinMb = 128;

    internal const double MaxMb = 2048;

    internal const string SourceDefault = "default";

    internal const string SourceSetting = "setting";

    internal static HotStoreResetLimit DefaultLimit { get; } = new(DefaultMb, SourceDefault);

    /// <summary>
    /// Resolves the limit from the text of settings.json. <paramref name="note"/> is the one log line to write when the stored
    /// value was clamped or unusable, otherwise null. A missing key or file is the default with no note.
    /// </summary>
    internal static HotStoreResetLimit Resolve(string? settingsText, out string? note)
    {
        note = null;
        if (string.IsNullOrWhiteSpace(settingsText))
        {
            return DefaultLimit;
        }

        try
        {
            using var doc = JsonDocument.Parse(settingsText);
            if (doc.RootElement.ValueKind != JsonValueKind.Object
                || !doc.RootElement.TryGetProperty(SettingsKey, out var val))
            {
                return DefaultLimit;
            }

            if (val.ValueKind != JsonValueKind.Number || !val.TryGetDouble(out var given) || double.IsNaN(given))
            {
                note = $"settings.json '{SettingsKey}' is not a number ({val.GetRawText()}), so the hot store reset point is the default {Format(DefaultMb)} MB.";
                return DefaultLimit;
            }

            var mb = Math.Clamp(given, MinMb, MaxMb);
            if (mb != given)
            {
                note = $"settings.json '{SettingsKey}' is {Format(given)}, outside {Format(MinMb)} to {Format(MaxMb)}, so the hot store reset point is {Format(mb)} MB.";
            }

            return new HotStoreResetLimit(mb, SourceSetting);
        }
        catch (JsonException)
        {
            return DefaultLimit;
        }
    }

    /// <summary>Reads the setting and installs it as <see cref="HotStoreResetLimit.Current"/>. Runs once at startup.</summary>
    internal static void LoadAtStartup(string? settingsText) => HotStoreResetLimit.Current = ResolveAndLog(settingsText);

    /// <summary>Resolves the limit and writes the one log line for it: a warning when the value was clamped or unusable, otherwise
    /// the value in force. Does not install it.</summary>
    internal static HotStoreResetLimit ResolveAndLog(string? settingsText)
    {
        var limit = Resolve(settingsText, out var note);
        if (note != null)
        {
            AppLogger.Warn("Settings", note);
        }
        else
        {
            AppLogger.Info("Settings", $"The hot store reset point is {Format(limit.Mb)} MB ({limit.Source}).");
        }

        return limit;
    }

    private static string Format(double mb) => mb.ToString("0.##", CultureInfo.InvariantCulture);
}
