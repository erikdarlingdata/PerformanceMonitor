/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The user-set <c>memory_limit</c> of Lite's main DuckDB connection (#5457, owner ruling 2026-10-08). It was a
/// hard-coded constant, so a user whose archive made a read run out of memory could not do anything about it.
///
/// <para>Stored in settings.json as <see cref="SettingsKey"/>, a whole number of GB. The default is
/// <see cref="DefaultGb"/> (2 GB, #5381), the minimum 1 GB, the maximum 80% of physical memory rounded down
/// (DuckDB's own default ceiling), never below the minimum. A missing, unparsable or out-of-range stored value
/// falls back to the default and says so in the log. The value is applied once at startup
/// (<see cref="LoadAtStartup"/>): <c>memory_limit</c> belongs to a DuckDB instance, so a changed value takes
/// effect at the next start.</para>
///
/// <para>Compaction's own in-memory 4 GB instance and the data importer's plain connection are separate DuckDB
/// instances and do not follow this setting.</para>
/// </summary>
internal static class DuckDbMemoryLimitSetting
{
    internal const string SettingsKey = "duckdb_memory_limit_gb";

    internal const int DefaultGb = 2;

    internal const int MinGb = 1;

    private const double MaxFractionOfPhysical = 0.8;

    private const long BytesPerGb = 1024L * 1024 * 1024;

    /// <summary>
    /// The largest accepted value for a machine with <paramref name="physicalBytes"/> of memory: 80%, rounded
    /// down to a whole GB, never below <see cref="MinGb"/>.
    /// </summary>
    internal static int MaxGb(long physicalBytes)
    {
        if (physicalBytes <= 0)
        {
            return DefaultGb;
        }

        var gb = (long)Math.Floor(physicalBytes * MaxFractionOfPhysical / BytesPerGb);
        return (int)Math.Clamp(gb, MinGb, int.MaxValue);
    }

    internal static long PhysicalMemoryBytes() => GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;

    internal static bool IsInRange(long gb, long physicalBytes) => gb >= MinGb && gb <= MaxGb(physicalBytes);

    /// <summary>The sentence the Settings window and the log use for the accepted range.</summary>
    internal static string RangeText(long physicalBytes) =>
        $"{MinGb} to {MaxGb(physicalBytes)} GB (a whole number, at most 80% of this machine's memory)";

    /// <summary>
    /// Resolves the setting from the text of settings.json. <paramref name="fallbackNote"/> is the one log line
    /// to write when the stored value is missing or unusable, otherwise null.
    /// </summary>
    internal static int Resolve(string? settingsText, long physicalBytes, out string? fallbackNote)
    {
        fallbackNote = null;

        if (string.IsNullOrWhiteSpace(settingsText))
        {
            fallbackNote = $"settings.json has no '{SettingsKey}', so the DuckDB memory limit is the default {DefaultGb} GB.";
            return DefaultGb;
        }

        try
        {
            using var doc = JsonDocument.Parse(settingsText);

            if (doc.RootElement.ValueKind != JsonValueKind.Object
                || !doc.RootElement.TryGetProperty(SettingsKey, out var val))
            {
                fallbackNote = $"settings.json has no '{SettingsKey}', so the DuckDB memory limit is the default {DefaultGb} GB.";
                return DefaultGb;
            }

            if (val.ValueKind != JsonValueKind.Number || !val.TryGetInt32(out var gb))
            {
                fallbackNote = $"settings.json '{SettingsKey}' is not a whole number ({val.GetRawText()}), "
                    + $"so the DuckDB memory limit is the default {DefaultGb} GB.";
                return DefaultGb;
            }

            if (!IsInRange(gb, physicalBytes))
            {
                fallbackNote = $"settings.json '{SettingsKey}' is {gb}, outside {RangeText(physicalBytes)}, "
                    + $"so the DuckDB memory limit is the default {DefaultGb} GB.";
                return DefaultGb;
            }

            return gb;
        }
        catch (JsonException)
        {
            fallbackNote = $"settings.json could not be parsed, so the DuckDB memory limit is the default {DefaultGb} GB.";
            return DefaultGb;
        }
    }

    /// <summary>
    /// Reads the setting and installs it as <see cref="DuckDbInitializer.ConfiguredMemoryLimitGb"/>. Runs at
    /// startup before the first DuckDB connection is created.
    /// </summary>
    internal static void LoadAtStartup(string? settingsText)
    {
        var gb = Resolve(settingsText, PhysicalMemoryBytes(), out var note);
        DuckDbInitializer.ConfiguredMemoryLimitGb = gb;
        if (note != null)
        {
            AppLogger.Info("Settings", note);
        }
        else
        {
            AppLogger.Info("Settings", $"The DuckDB memory limit is {gb} GB (settings.json '{SettingsKey}').");
        }
    }

    /// <summary>
    /// True when <paramref name="ex"/> (or an inner exception) is DuckDB reporting that it ran out of memory.
    /// </summary>
    internal static bool IsOutOfMemory(Exception? ex)
    {
        for (var e = ex; e != null; e = e.InnerException)
        {
            var m = e.Message;
            if (m.Contains("Out of Memory", StringComparison.OrdinalIgnoreCase)
                || m.Contains("could not free up enough memory", StringComparison.OrdinalIgnoreCase)
                || m.Contains("memory_limit", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// The sentence appended to a DuckDB out-of-memory error: names the setting, where it is, and its current
    /// value.
    /// </summary>
    internal static string OutOfMemoryHint(int currentGb) =>
        $"DuckDB ran out of memory (its memory limit is {currentGb} GB). Raise \"DuckDB memory limit\" under "
        + "Dashboard Defaults in Settings; the new value takes effect after Lite restarts.";

    /// <summary>
    /// The text a surface shows for a failed read: the exception's message, and, for a DuckDB out-of-memory
    /// error, the hint naming the setting and its current value. Logs one line for that case too.
    /// </summary>
    internal static string Describe(Exception ex, string logSource = "DuckDB")
    {
        if (!IsOutOfMemory(ex))
        {
            return ex.Message;
        }

        var hint = OutOfMemoryHint(DuckDbInitializer.ConfiguredMemoryLimitGb);
        AppLogger.Warn(logSource, hint);
        return $"{ex.Message} {hint}";
    }
}
