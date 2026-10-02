/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Single source for the configured "ignored" (benign/idle) wait types — the per-user editable
/// %LOCALAPPDATA%\PerformanceMonitorLite-Data\config\ignored_wait_types.json, falling back to the copy
/// bundled next to the exe. Used by BOTH the collector (skip at collection) and the wait-stats tab
/// queries (skip at display) so the two can't drift. Display-side filtering is what hides waits already
/// collected before the filter was active — copying the JSON only stops NEW collection, it can't remove
/// existing DuckDB rows (#1240).
/// </summary>
public static class IgnoredWaitTypes
{
    /// <summary>
    /// Loads the ignored wait-type set (case-insensitive): the per-user copy first, then the bundled
    /// copy next to the exe. Best-effort — returns an empty set if neither file is present or parsing
    /// fails (callers warn when the result is empty).
    /// </summary>
    public static HashSet<string> Load()
    {
        var waits = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var configPath = Path.Combine(App.ConfigDirectory, "ignored_wait_types.json");
        if (!File.Exists(configPath))
        {
            configPath = Path.Combine(AppContext.BaseDirectory, "config", "ignored_wait_types.json");
        }

        if (!File.Exists(configPath))
        {
            return waits;
        }

        try
        {
            using var doc = JsonDocument.Parse(File.ReadAllText(configPath));
            if (doc.RootElement.TryGetProperty("ignored_waits", out var waitsArray))
            {
                foreach (var wait in waitsArray.EnumerateArray())
                {
                    var waitType = wait.GetString();
                    if (!string.IsNullOrEmpty(waitType))
                    {
                        waits.Add(waitType);
                    }
                }
            }
        }
        catch
        {
            /* Best-effort; callers warn when the resulting set is empty. */
        }

        return waits;
    }

    /// <summary>
    /// The defaults that shipped after v3.8.0: the only names a per-user file written by v3.8.0 or earlier
    /// can lack. A file with no <c>seen_defaults</c> property is treated as having seen the bundled list
    /// minus these, which is v3.8.0's bundled list exactly. A later release that adds defaults needs no
    /// change here, because by then every merged file carries <c>seen_defaults</c> and the new names
    /// differ from it naturally.
    /// </summary>
    internal static readonly string[] DefaultsAddedAfter380 = ["RBIO_COMM_RETRY", "SQP_STATS_REPORTING"];

    private const string WaitsProperty = "ignored_waits";
    private const string SeenProperty = "seen_defaults";

    /// <summary>
    /// Merges the bundled defaults the per-user file has not seen yet into its <c>ignored_waits</c>, once.
    /// The user's file wins everywhere else: a default they removed stays removed, their own additions and
    /// ordering stay, and every other property survives. Writes the file only when something changed,
    /// through a temp file and a move so a crash can't leave it half-written. Best-effort: a missing,
    /// unreadable, locked or malformed file is logged and left exactly as it was. Returns true when the
    /// file was rewritten.
    /// </summary>
    public static bool MergeNewDefaults(string bundledPath, string userPath)
    {
        try
        {
            if (!File.Exists(userPath))
            {
                return false;
            }

            if (!File.Exists(bundledPath))
            {
                AppLogger.Warn("Config", $"Bundled ignored_wait_types.json not found at {bundledPath}; new default ignored waits were not merged");
                return false;
            }

            var bundled = ReadWaits(JsonNode.Parse(File.ReadAllText(bundledPath)) as JsonObject);
            if (bundled is null)
            {
                AppLogger.Warn("Config", "Bundled ignored_wait_types.json has no ignored_waits list; new default ignored waits were not merged");
                return false;
            }

            var original = File.ReadAllText(userPath);
            if (JsonNode.Parse(original) is not JsonObject user)
            {
                AppLogger.Warn("Config", "ignored_wait_types.json is not a JSON object; new default ignored waits were not merged");
                return false;
            }

            if (!TryMergeNewDefaults(user, bundled, out var merged))
            {
                return false;
            }

            var newline = original.Contains("\r\n", StringComparison.Ordinal) ? "\r\n" : "\n";
            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                NewLine = newline,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };
            var text = merged.ToJsonString(options);
            if (original.EndsWith('\n'))
            {
                text += newline;
            }

            var tempPath = userPath + ".tmp";
            try
            {
                File.WriteAllText(tempPath, text, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
                File.Move(tempPath, userPath, overwrite: true);
            }
            catch
            {
                try { File.Delete(tempPath); } catch { /* best-effort cleanup */ }
                throw;
            }

            AppLogger.Info("Config", "Merged new default ignored waits into ignored_wait_types.json");
            return true;
        }
        catch (Exception ex)
        {
            AppLogger.Warn("Config", $"Could not merge new default ignored waits into ignored_wait_types.json: {ex.Message}");
            return false;
        }
    }

    /// <summary>
    /// The pure merge over the parsed per-user document. <paramref name="merged"/> is a copy with the new
    /// defaults appended to <c>ignored_waits</c> and <c>seen_defaults</c> set to what the file has now seen;
    /// it is the input itself when nothing changed. Returns false, with the input untouched, when nothing
    /// needs writing or when the document's shape is not the expected one.
    /// </summary>
    internal static bool TryMergeNewDefaults(JsonObject user, IReadOnlyCollection<string> bundled, out JsonObject merged)
    {
        merged = user;

        var current = ReadWaits(user);
        if (current is null)
        {
            return false;
        }

        List<string> seen;
        var seenPresent = user.TryGetPropertyValue(SeenProperty, out var seenNode);
        if (seenPresent)
        {
            var seenList = seenNode is JsonArray ? ReadWaits(user, SeenProperty) : null;
            if (seenList is null)
            {
                return false;
            }
            seen = seenList;
        }
        else
        {
            seen = bundled.Where(w => !DefaultsAddedAfter380.Contains(w, StringComparer.OrdinalIgnoreCase)).ToList();
        }

        var seenSet = new HashSet<string>(seen, StringComparer.OrdinalIgnoreCase);
        var currentSet = new HashSet<string>(current, StringComparer.OrdinalIgnoreCase);

        var unseen = bundled.Where(w => !string.IsNullOrEmpty(w) && seenSet.Add(w)).ToList();
        var toAdd = unseen.Where(currentSet.Add).ToList();

        if (seenPresent && unseen.Count == 0)
        {
            return false;
        }

        var copy = (JsonObject)user.DeepClone();
        var waits = (JsonArray)copy[WaitsProperty]!;
        foreach (var w in toAdd)
        {
            waits.Add(JsonValue.Create(w));
        }

        var seenArray = new JsonArray();
        foreach (var w in seen.Concat(unseen))
        {
            seenArray.Add(JsonValue.Create(w));
        }
        copy[SeenProperty] = seenArray;

        merged = copy;
        return true;
    }

    /// <summary>Reads a string-array property of the document, or null when it is absent or not all strings.</summary>
    private static List<string>? ReadWaits(JsonObject? root, string property = WaitsProperty)
    {
        if (root is null || root[property] is not JsonArray array)
        {
            return null;
        }

        var list = new List<string>(array.Count);
        foreach (var item in array)
        {
            if (item is JsonValue value && value.TryGetValue<string>(out var s))
            {
                list.Add(s);
            }
            else
            {
                return null;
            }
        }
        return list;
    }

    /// <summary>
    /// Builds a SQL predicate <c>AND wait_type NOT IN ('A','B',...)</c> excluding the ignored types, or an
    /// empty string when there is nothing to exclude. Names are sanitized to [A-Za-z0-9_] before being
    /// inlined, so the literals are injection-safe (the source is controlled config and SQL Server
    /// wait_type names are always identifiers). Applied at display time so benign waits already in the
    /// DuckDB don't surface in the wait-stats tab/picker.
    /// </summary>
    public static string BuildExclusionClause(IReadOnlyCollection<string>? ignored)
    {
        if (ignored == null || ignored.Count == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder();
        foreach (var wait in ignored)
        {
            if (string.IsNullOrEmpty(wait) || !IsSafeIdentifier(wait))
            {
                continue;
            }
            if (sb.Length > 0)
            {
                sb.Append(',');
            }
            sb.Append('\'').Append(wait).Append('\'');
        }

        return sb.Length == 0 ? string.Empty : $"AND wait_type NOT IN ({sb})";
    }

    private static bool IsSafeIdentifier(string value)
    {
        foreach (var c in value)
        {
            if (!char.IsLetterOrDigit(c) && c != '_')
            {
                return false;
            }
        }
        return true;
    }
}
