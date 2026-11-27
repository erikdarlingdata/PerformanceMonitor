/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One <c>pg_file_settings</c> row that is NOT the value in force (#9418): a line a later file or an
/// <c>ALTER SYSTEM</c> hides, or one PostgreSQL could not apply. <see cref="Value"/> is the text the file carries.</summary>
internal readonly record struct UnappliedFileSetting(string? File, int? Line, string? Value, string? Error);

/// <summary>Where one store setting's running value comes from (#9418): the <c>pg_settings</c> source, file and
/// line of the winning value, whether it waits for a restart, and every <c>pg_file_settings</c> line for the same
/// name that is not applied. <see cref="SourceFile"/> and <see cref="SourceLine"/> are null when the role cannot
/// read them (see <see cref="SettingProvenanceRead.SourceColumnsReadable"/>), and when the value does not come
/// from a file at all. <see cref="AppliedFileValue"/> is the text the winning file line carries (for example
/// <c>77MB</c>), where <c>pg_settings.setting</c> only has the normalized figure (<c>78848</c> kB).
/// <see cref="Shown"/> is the running value the way <c>SHOW</c> prints it (<c>77MB</c>, <c>2GB</c>, <c>5s</c>): in
/// <c>pg_settings</c> the figure and its unit are separate columns and the unit can be a multiple (<c>8kB</c> for
/// <c>shared_buffers</c>), so joining them reads wrong. <see cref="Context"/> is <c>pg_settings.context</c>.</summary>
internal sealed record SettingProvenance(
    string Name,
    string Setting,
    string? Unit,
    string? Source,
    string? SourceFile,
    int? SourceLine,
    bool PendingRestart,
    IReadOnlyList<UnappliedFileSetting> UnappliedFileRows,
    string? AppliedFileValue = null,
    string? Shown = null,
    string? Context = null)
{
    /// <summary>The running value the way <c>SHOW</c> prints it. Falls back to the bare <c>pg_settings.setting</c>
    /// figure when no <c>SHOW</c> text was read, never a figure joined to a unit that may be a multiple.</summary>
    public string ValueDisplay => Shown ?? Setting;
}

/// <summary>The provenance of a set of settings plus what the reading role could and could not see.</summary>
internal sealed record SettingProvenanceRead(
    IReadOnlyDictionary<string, SettingProvenance> ByName,
    bool SourceColumnsReadable,
    bool FileSettingsReadable,
    string RoleName);

/// <summary>
/// Reads and words the provenance of store settings (#9418): where the running value comes from, which file lines
/// are hidden behind it, and whether a restart is waiting. <c>pg_settings.sourcefile</c> and <c>sourceline</c> need superuser or
/// <c>pg_read_all_settings</c>, and <c>pg_file_settings</c> needs superuser or a grant, so a role without them
/// gets a read that says so with the grant that fixes it, never a silent blank.
/// </summary>
internal static class StoreSettingProvenance
{
    /// <summary>The role's name and whether it can read the source columns and <c>pg_file_settings</c>.</summary>
    internal const string ReadabilitySql = @"
SELECT current_user::text,
       pg_has_role(current_user, 'pg_read_all_settings', 'USAGE'),
       has_table_privilege(current_user, 'pg_file_settings', 'SELECT')";

    internal const string SettingsSql = @"
SELECT name, setting, unit, source, sourcefile, sourceline, context, current_setting(name)
FROM pg_settings
WHERE name = ANY($1)";

    internal const string FileRowsSql = @"
SELECT name, sourcefile, sourceline, setting, error, coalesce(applied, false)
FROM pg_file_settings
WHERE name = ANY($1)
ORDER BY name, seqno";

    /// <summary>The <c>pg_file_settings.error</c> text PostgreSQL gives a line it read but could not apply. The same
    /// text <see cref="DarlingStoreHostProfile.ManagedConfPgFileSettingsErrorSql"/> matches (#4251).</summary>
    internal const string FileSettingNotAppliedError = "setting could not be applied";

    /// <summary>The <c>pg_settings.context</c> of a setting that only a restart can change.</summary>
    internal const string PostmasterContext = "postmaster";

    /// <summary>The restart rule the managed verdicts use (#4251): a setting is pending a restart when its context is
    /// <c>postmaster</c> and <c>pg_file_settings</c> holds a line for it that PostgreSQL could not apply. Never
    /// <c>pg_settings.pending_restart</c>, which reads <c>f</c> on Windows after a reload. Unknown (false) when the
    /// role cannot read <c>pg_file_settings</c>.</summary>
    internal static bool IsPendingRestart(string? context, IReadOnlyList<UnappliedFileSetting> unappliedRows)
        => string.Equals(context, PostmasterContext, StringComparison.Ordinal)
            && unappliedRows.Any(r => string.Equals(r.Error, FileSettingNotAppliedError, StringComparison.Ordinal));

    /// <summary>How many unapplied lines one setting's sentence names before it says how many more there are. A
    /// store that has carried several versioned managed blocks has one hidden line per block.</summary>
    internal const int MaxNamedFileRows = 5;

    /// <summary>Reads the provenance of <paramref name="names"/> on <paramref name="connection"/>.</summary>
    internal static async Task<SettingProvenanceRead> ReadAsync(
        NpgsqlConnection connection, IReadOnlyCollection<string> names, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var nameArray = names.ToArray();
        var role = "the connected role";
        var sourceReadable = false;
        var fileReadable = false;
        await using (var cmd = new NpgsqlCommand(ReadabilitySql, connection) { CommandTimeout = commandTimeoutSeconds })
        await using (var reader = await cmd.ExecuteReaderAsync(cancellationToken))
        {
            if (await reader.ReadAsync(cancellationToken))
            {
                role = reader.GetString(0);
                sourceReadable = !reader.IsDBNull(1) && reader.GetBoolean(1);
                fileReadable = !reader.IsDBNull(2) && reader.GetBoolean(2);
            }
        }

        var unapplied = new Dictionary<string, List<UnappliedFileSetting>>(StringComparer.Ordinal);
        var appliedValue = new Dictionary<string, string?>(StringComparer.Ordinal);
        if (fileReadable)
        {
            await using var cmd = new NpgsqlCommand(FileRowsSql, connection) { CommandTimeout = commandTimeoutSeconds };
            cmd.Parameters.AddWithValue(nameArray);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var name = reader.GetString(0);
                if (reader.GetBoolean(5))
                {
                    appliedValue[name] = reader.IsDBNull(3) ? null : reader.GetString(3);
                    continue;
                }

                if (!unapplied.TryGetValue(name, out var list))
                {
                    unapplied[name] = list = [];
                }

                list.Add(new UnappliedFileSetting(
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetInt32(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4)));
            }
        }

        var byName = new Dictionary<string, SettingProvenance>(StringComparer.Ordinal);
        await using (var cmd = new NpgsqlCommand(SettingsSql, connection) { CommandTimeout = commandTimeoutSeconds })
        {
            cmd.Parameters.AddWithValue(nameArray);
            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var name = reader.GetString(0);
                IReadOnlyList<UnappliedFileSetting> rows = unapplied.TryGetValue(name, out var found) ? found : [];
                var context = reader.IsDBNull(6) ? null : reader.GetString(6);
                byName[name] = new SettingProvenance(
                    name,
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    IsPendingRestart(context, rows),
                    rows,
                    appliedValue.GetValueOrDefault(name),
                    reader.IsDBNull(7) ? null : reader.GetString(7),
                    context);
            }
        }

        return new SettingProvenanceRead(byName, sourceReadable, fileReadable, role);
    }

    /// <summary>The sentence that says what the role cannot see and the grants that fix it, or null when it can see
    /// everything this read needs. The role name is quoted so the grant can be pasted.</summary>
    internal static string? DescribeUnreadable(SettingProvenanceRead read)
    {
        if (read.SourceColumnsReadable && read.FileSettingsReadable)
        {
            return null;
        }

        var quoted = "\"" + read.RoleName.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
        var missing = new List<string>();
        var grants = new List<string>();
        if (!read.SourceColumnsReadable)
        {
            missing.Add("the source file and line of a setting");
            grants.Add($"GRANT pg_read_all_settings TO {quoted}");
        }

        if (!read.FileSettingsReadable)
        {
            missing.Add("the lines in pg_file_settings that are not applied");
            grants.Add($"GRANT SELECT ON pg_file_settings TO {quoted}");
        }

        return $"Role {read.RoleName} cannot read {string.Join(" or ", missing)}. Run as a superuser: {string.Join("; ", grants)}. "
            + "Both show every line of every configuration file, so grant them only if that is acceptable for this role.";
    }

    /// <summary>
    /// One plain sentence for a setting whose running value is not the managed one: the value and where it comes
    /// from, each line that is not applied (file, line, value), and a restart note. <paramref name="formatFile"/>
    /// turns a file path into what the reader may see: the full path in the local log, a data-directory-relative
    /// name for a remote MCP caller. Returns null when there is nothing to name.
    /// </summary>
    internal static string? Describe(SettingProvenance? provenance, Func<string, string> formatFile)
    {
        if (provenance is null)
        {
            return null;
        }

        var parts = new List<string>();
        /* The file's own text first (77MB), then the running figure when it reads differently (78848kB). */
        var value = provenance.AppliedFileValue is { } fileValue && fileValue != provenance.ValueDisplay
            ? FormattableString.Invariant($"{fileValue} (running {provenance.ValueDisplay})")
            : provenance.ValueDisplay;
        var winner = provenance.SourceFile is null
            ? FormattableString.Invariant($"{provenance.Name} is {value} (source: {provenance.Source ?? "unknown"})")
            : FormattableString.Invariant(
                $"{provenance.Name} is {value} (source: {provenance.Source ?? "unknown"}, {formatFile(provenance.SourceFile)} line {provenance.SourceLine?.ToString(CultureInfo.InvariantCulture) ?? "?"})");
        parts.Add(winner);

        var named = provenance.UnappliedFileRows.Take(MaxNamedFileRows)
            .Select(r => FormattableString.Invariant(
                $"{(r.File is null ? "(no file)" : formatFile(r.File))} line {r.Line?.ToString(CultureInfo.InvariantCulture) ?? "?"} = {r.Value ?? "(none)"}"))
            .ToList();
        if (named.Count > 0)
        {
            var more = provenance.UnappliedFileRows.Count - named.Count;
            parts.Add("not applied: " + string.Join(", ", named) + (more > 0 ? FormattableString.Invariant($", and {more} more") : string.Empty));
        }

        if (provenance.PendingRestart)
        {
            parts.Add("pending a restart");
        }

        return string.Join("; ", parts);
    }
}
