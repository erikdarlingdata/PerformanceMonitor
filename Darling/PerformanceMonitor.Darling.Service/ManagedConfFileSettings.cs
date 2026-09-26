/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One row of PostgreSQL's <c>pg_file_settings</c> as Step A's verification needs it (#4336): the
/// file and line the setting was found on (null when the row itself represents a parse error not tied to a
/// resolved assignment), the setting's name, the raw value text as the file carries it, whether this row is
/// the one actually in force (<c>applied</c>), and an error message when PostgreSQL rejected the line outright.
/// </summary>
internal readonly record struct FileSettingRow(
    string? SourceFile,
    int? SourceLine,
    string? Name,
    string? Setting,
    bool Applied,
    string? Error);

internal static class ManagedConfFileSettings
{
    /// <summary>
    /// The query Step A's snapshot delegate runs, both before and after the two-step write: every row
    /// <c>pg_file_settings</c> reports, re-parsed from the files on disk at query time with no reload or
    /// restart required.
    /// </summary>
    internal const string SnapshotSql =
        "SELECT sourcefile, sourceline, name, setting, applied, error FROM pg_file_settings";

    /// <summary>
    /// Compares a <paramref name="before"/> snapshot (taken while the OLD conf is still in force) against an
    /// <paramref name="after"/> snapshot (taken once both new files are written) for Step A's verification:
    /// the migration must never change a value (design goal, #4215/#4336). Names are compared
    /// case-insensitively (PostgreSQL's own GUC names are); <c>sourcefile</c> and <c>sourceline</c> are
    /// ignored entirely — a setting's line moving (v1-v15's blocks collapsing into one included file, an
    /// operator line moving below the include) is exactly what this migration is FOR, not a mismatch.
    ///
    /// <para>Only <c>applied</c> rows are compared for value equality: for every name that has an applied row
    /// in EITHER snapshot, the applied setting text (raw, unescaped) must be identical in both — a name
    /// applied in one snapshot and absent from the other's applied rows is itself a mismatch (rule: "no name
    /// silently starts or stops being the one in force").</para>
    ///
    /// <para>An error row present in <paramref name="before"/> is not a mismatch by itself when the SAME name
    /// still carries an error row in <paramref name="after"/> — a pre-existing bad line the migration did not
    /// touch stays exactly as broken as it always was. A NEW error row in <paramref name="after"/> — a name
    /// with no error row in <paramref name="before"/> that gained one — is always a mismatch: Step A must
    /// never make a previously-clean setting start failing to parse.</para>
    /// </summary>
    internal static (bool Match, IReadOnlyList<string> Mismatches) Compare(
        IReadOnlyList<FileSettingRow> before,
        IReadOnlyList<FileSettingRow> after)
    {
        var beforeApplied = AppliedByName(before);
        var afterApplied = AppliedByName(after);
        var beforeErrorNames = ErrorNames(before);
        var afterErrorNames = ErrorNames(after);

        var mismatches = new List<string>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var name in beforeApplied.Keys)
        {
            if (!seen.Add(name))
            {
                continue;
            }

            if (!afterApplied.TryGetValue(name, out var afterValue) ||
                !string.Equals(beforeApplied[name], afterValue, StringComparison.Ordinal))
            {
                mismatches.Add(name);
            }
        }

        foreach (var name in afterApplied.Keys)
        {
            if (!seen.Add(name))
            {
                continue;
            }

            // In afterApplied but not beforeApplied (the loop above only adds names it saw in beforeApplied).
            mismatches.Add(name);
        }

        foreach (var name in afterErrorNames)
        {
            if (!beforeErrorNames.Contains(name) && seen.Add(name))
            {
                mismatches.Add(name);
            }
        }

        return (mismatches.Count == 0, mismatches);
    }

    private static Dictionary<string, string?> AppliedByName(IReadOnlyList<FileSettingRow> rows)
    {
        var result = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var row in rows)
        {
            if (row.Applied && row.Name is not null)
            {
                result[row.Name] = row.Setting;
            }
        }

        return result;
    }

    private static HashSet<string> ErrorNames(IReadOnlyList<FileSettingRow> rows)
    {
        var result = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var row in rows)
        {
            if (row.Error is not null && row.Name is not null)
            {
                result.Add(row.Name);
            }
        }

        return result;
    }
}
