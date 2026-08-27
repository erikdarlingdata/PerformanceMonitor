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
/// The one read and the one write behind both of the viewer's per-user JSON settings files —
/// <see cref="ViewerAppSettingsStore"/> (viewer-settings.json) and <see cref="ViewerPreferencesStore"/>
/// (viewer-preferences.json) — sitting on the shared <see cref="SettingsFileGuard"/> (#2434).
///
/// <para>Both stores previously ended their load in a bare <c>catch</c> whose only output was a
/// <see cref="System.Diagnostics.Debug"/> trace. <c>Debug.WriteLine</c> carries
/// <see cref="System.Diagnostics.ConditionalAttribute"/>("DEBUG") and is removed by the compiler from a
/// Release build, so in the viewer anyone actually runs, a settings file that could not be read produced
/// no record at all — not a log line, not a dialog, nothing. And neither Save merged: each serialized its
/// whole in-memory object over the file, so a load that had silently fallen back to defaults, followed by
/// any save at all, replaced every setting in the file with a default. Changing the time-display dropdown
/// once was enough.</para>
///
/// <para>So there are two rules here, and the second is the one that turns an annoyance into data loss.
/// A file that is present and unreadable is always reported, to the log the viewer now has
/// (<see cref="ViewerLogger"/> — the "the viewer writes no application log of its own" comment these
/// stores carried predates it). And nothing replaces a file it could not read until a copy of it exists:
/// when even the copy cannot be made, the save is refused and says so, because leaving the file alone
/// beats replacing it when the alternative is permanent.</para>
///
/// <para>An ABSENT file goes through both paths in total silence. A first run has nothing to preserve and
/// nothing to explain, and a warning there would be pure noise — keeping absent apart from unreadable is
/// half the reason the guard exists.</para>
///
/// <para>Generic in the value rather than written per store, because there are three of these files and
/// the third — <see cref="ViewerServerStore"/>'s registry of monitored servers — is the one where losing
/// the file costs the operator the most and is a JSON ARRAY rather than an object. Everything the guard
/// decides is decided by the same code for all three.</para>
/// </summary>
internal static class ViewerSettingsFile
{
    /// <summary>
    /// The (file, problem) pairs already reported this session, so one broken file is one log line rather
    /// than one per read.
    ///
    /// <para>Nothing reads these files once. The theme is applied from viewer-settings.json before the
    /// window exists, MainWindow loads it again to seed itself, the Settings window loads it on open and
    /// MainWindow re-loads it on close, and the control-plane migration loads it too — so an unreported
    /// duplicate would put five identical ERROR lines in the log for one defect, which is a poor reward for
    /// whoever went looking. The first line is the one that carries the fact; the rest carry nothing.</para>
    ///
    /// <para>Keyed on the problem as well as the path deliberately: if the file changes mid-session and
    /// breaks in a NEW way, that is a new fact and it is said again.</para>
    /// </summary>
    private static readonly HashSet<string> s_reported = new(StringComparer.Ordinal);

    private static bool FirstReportOf(string filePath, string? problem)
    {
        lock (s_reported)
        {
            return s_reported.Add($"{filePath}|{problem}");
        }
    }

    /// <summary>
    /// Reads <paramref name="filePath"/> into <typeparamref name="T"/>, substituting a default instance
    /// when there is nothing usable to read. The returned <c>State</c> is what a caller needs to decide
    /// whether the defaults it just got are a legitimate first run or a configuration that is still on
    /// disk and could not be understood; the log line for the second case is written here, so a call site
    /// that never looks at the state is still not silent.
    /// </summary>
    internal static SettingsObjectRead<T> Load<T>(string filePath, string logSource, JsonSerializerOptions options)
        where T : class, new()
    {
        var read = SettingsFileGuard.ReadObject<T>(filePath, options);

        if (read.State == SettingsFileState.Unreadable && FirstReportOf(filePath, read.Problem))
        {
            /* Two different facts, so two different sentences. A file the reader could not use AT ALL costs
               every setting in it; a file it read after dropping named members costs only those. Saying the
               first when the second is true is the overstatement #2456 was filed to end — and saying the
               second when the first is true would be worse, because it implies the rest survived. */
            ViewerLogger.Error(logSource,
                read.UnreadableMembers is { Count: > 0 }
                    ? $"'{filePath}': {read.Problem}. Every other setting in the file loaded normally. " +
                      "The file has not been changed; the next save copies it aside before replacing it."
                    : $"'{filePath}' could not be read ({read.Problem}), so every setting it holds is at " +
                      "its default for this session. The file has not been changed; the next save copies " +
                      "it aside before replacing it.");
        }

        return read.Value is null ? read with { Value = new T() } : read;
    }

    /// <summary>
    /// Serializes <paramref name="value"/> over <paramref name="filePath"/>, and returns whether it
    /// actually reached disk.
    ///
    /// <para>The bool is the point. This is a whole-object replacement, so a save that fails silently and
    /// a save that worked are indistinguishable to a caller that cannot ask — which is how a UI ends up
    /// saying it saved something it did not. Every failure here is logged AND reported, and the two call
    /// sites that persist a setting from an ordinary click surface it to the user rather than dropping
    /// it into a log nobody reads after a dialog said it worked.</para>
    ///
    /// <para>It returns false rather than throwing because the handlers that call it — a dropdown
    /// selection change, a sidebar group collapsing — do not wrap it, and a full disk is not a reason to
    /// take the viewer down.</para>
    /// </summary>
    internal static bool Save<T>(string filePath, T value, string logSource, JsonSerializerOptions options)
        where T : class
    {
        var permit = SettingsFileGuard.PermitReplace<T>(filePath, DateTime.Now, options);

        if (!permit.Allowed)
        {
            ViewerLogger.Error(logSource,
                $"'{filePath}' could not be read ({permit.Problem}) and no copy of it could be made, so " +
                "it has been left untouched rather than overwritten with defaults, and nothing was saved. " +
                "Fix the file, or move it aside by hand, and try again.");
            return false;
        }

        if (permit.QuarantinedTo is not null)
        {
            /* Deliberately does not claim WHAT the replacement is written from. Since #2456 that depends on
               how much of the file the load recovered — everything but the named members, or nothing at all
               — and this permit is asked at save time by a caller holding an object it did not necessarily
               load. The one fact that matters here is true either way: the original bytes are in the copy. */
            ViewerLogger.Warn(logSource,
                $"'{Path.GetFileName(filePath)}' could not be read ({permit.Problem}), so this save " +
                "replaces it. The unreadable original was copied to " +
                $"'{Path.GetFileName(permit.QuarantinedTo)}' first — the settings it held are recoverable " +
                "from there.");
        }

        try
        {
            var directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            File.WriteAllText(filePath, JsonSerializer.Serialize(value, options));
            return true;
        }
        catch (Exception ex)
        {
            ViewerLogger.Error(logSource, $"'{filePath}' could not be written, so nothing was saved", ex);
            return false;
        }
    }
}
