/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Version choices for PostgreSQL collector SQL (#6206): the oldest major a spelling works on, kept in small
/// functions so which query a target gets is a testable decision rather than text scattered through each
/// collector. A major of 0 means the probe has not run (the value every PostgreSQL version gate here treats as
/// "assume newest"), so it never selects the old spelling.
/// </summary>
public static class PgVersionSql
{
    /// <summary>
    /// <c>AS MATERIALIZED</c> / <c>AS NOT MATERIALIZED</c> on a CTE is a PostgreSQL 12 feature; earlier majors raise
    /// a syntax error (42601) at the keyword. Before 12 every CTE referenced more than once, and every one that
    /// is not inlinable, was materialized anyway, so dropping the keyword gives the same plan shape.
    /// </summary>
    public const int MaterializedCteMajor = 12;

    private static readonly Regex s_materialized = new(
        @"\bAS\s+(?:NOT\s+)?MATERIALIZED\b", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>The SQL as written on PostgreSQL 12 and later; below 12 with the CTE materialization keyword removed.</summary>
    public static string ForMajor(string sql, int postgresMajorVersion)
    {
        ArgumentNullException.ThrowIfNull(sql);
        return postgresMajorVersion is > 0 and < MaterializedCteMajor
            ? s_materialized.Replace(sql, "AS")
            : sql;
    }

    /// <summary>
    /// Parses a <c>pg_extension.extversion</c> such as <c>1.8</c> into a comparable value; null when it is missing
    /// or not a dotted pair of integers (a development build suffix such as <c>1.8devel</c> counts for its number).
    /// </summary>
    public static Version? ParseExtensionVersion(string? extversion)
    {
        if (string.IsNullOrWhiteSpace(extversion))
        {
            return null;
        }

        var match = Regex.Match(extversion, @"^\s*(\d+)(?:\.(\d+))?", RegexOptions.CultureInvariant);
        if (!match.Success)
        {
            return null;
        }

        var major = int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture);
        var minor = match.Groups[2].Success ? int.Parse(match.Groups[2].Value, CultureInfo.InvariantCulture) : 0;
        return new Version(major, minor);
    }

    /// <summary>
    /// The <c>pg_stat_statements</c> catalog version a server major ships with, for a host that has not read the
    /// extension's own <c>extversion</c>: 1.7 (before the 1.8 rename) up to 12, then 1.8, 1.9, 1.10 and 1.11 at 13,
    /// 14, 15 and 17. It is only a stand-in, because an extension created before an in-place upgrade keeps its old
    /// view until <c>ALTER EXTENSION ... UPDATE</c> (#3818); the real <c>extversion</c> wins whenever it is known.
    /// </summary>
    public static Version StatementsVersionForMajor(int postgresMajorVersion) => postgresMajorVersion switch
    {
        <= 0 => new Version(1, 11),
        < 13 => new Version(1, 7),
        13 => new Version(1, 8),
        14 => new Version(1, 9),
        < 17 => new Version(1, 10),
        _ => new Version(1, 11),
    };

    /// <summary>
    /// Which columns the stock <c>pg_stat_statements</c> view has, from the extension's catalog version.
    /// </summary>
    public sealed record StatementsLayout(
        bool HasExecTimeNames,
        bool HasWal,
        bool HasToplevel,
        bool HasSharedBlkTimeNames,
        bool HasStatsSince)
    {
        /// <summary>
        /// The layout for <paramref name="extensionVersion"/> (<c>extversion</c>) when the host read it, otherwise
        /// the one the server major ships with.
        /// </summary>
        public static StatementsLayout For(int postgresMajorVersion, string? extensionVersion)
        {
            var version = ParseExtensionVersion(extensionVersion) ?? StatementsVersionForMajor(postgresMajorVersion);
            return new StatementsLayout(
                HasExecTimeNames: version >= new Version(1, 8),
                HasWal: version >= new Version(1, 8),
                HasToplevel: version >= new Version(1, 9),
                HasSharedBlkTimeNames: version >= new Version(1, 11),
                HasStatsSince: version >= new Version(1, 11));
        }
    }
}
