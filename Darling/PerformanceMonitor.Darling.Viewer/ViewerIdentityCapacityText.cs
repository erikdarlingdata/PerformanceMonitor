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
using System.Numerics;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One identity column of the Identity Capacity tab, in the web page's columns and words.</summary>
public sealed record ViewerIdentityColumnRow(
    string Database, string Table, string Column, string Type, string PercentUsed, string ValuesRemaining,
    string RatePerDay, string DaysToExhaustion, string Band);

/// <summary>One database of the Identity Capacity tab: whether it was scanned, and when.</summary>
public sealed record ViewerIdentityScanRow(string Database, string State, string LastScanned);

/// <summary>
/// The words of the Identity Capacity tab (#6875), as functions of <see cref="ViewerIdentityCapacityRead"/>. They match the Identity
/// Capacity panels on the web Configuration tab: the same column words, the same scan states, a dash for an unknown. The figures come
/// from <see cref="IdentityColumnFact"/> (the reader and <see cref="IdentityCapacityMath"/>), so no number is judged here. Values are
/// drawn from the exact <see cref="BigInteger"/>, never through a double, so a decimal(38,0) column keeps every digit.
/// </summary>
public static class ViewerIdentityCapacityText
{
    /// <summary>What the scan list says when no database has a marker in the window and the read gave no sentence of its own.</summary>
    public const string NoScan = "No database has been scanned yet.";

    /// <summary>What the column list says when databases were scanned and none has a column above 10 percent.</summary>
    public const string NoColumns = "No identity column is above 10 percent used.";

    /// <summary>The scan state of a database the rotation has not reached yet.</summary>
    public const string NotYetScanned = "Not yet scanned";

    /// <summary>The scan state of a database scanned with no column above 10 percent.</summary>
    public const string ScannedClean = "Scanned, clean";

    private const string Dash = "-";

    /// <summary>The status line: how many columns are critical and warning, and how many databases were scanned.</summary>
    public static string StatusLine(ViewerIdentityCapacityRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        if (read.Read is not { HasScan: true } scan)
        {
            return read.Message ?? NoScan;
        }

        var critical = scan.Columns.Count(c => c.Band == IdentityCapacityMath.BandCritical);
        var warning = scan.Columns.Count(c => c.Band == IdentityCapacityMath.BandWarning);
        return string.Create(CultureInfo.InvariantCulture,
            $"Critical: {critical}   Warning: {warning}   Columns above 10 percent: {scan.Columns.Count}   Databases scanned: {scan.Databases.Count}");
    }

    /// <summary>The columns, critical and warning first (the reader's order is fullest first), one row each.</summary>
    public static IReadOnlyList<ViewerIdentityColumnRow> ColumnRows(ViewerIdentityCapacityRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return read.Read is null ? [] : read.Read.Columns.Select(Row).ToList();
    }

    /// <summary>The sentence under the column list when it has no row.</summary>
    public static string ColumnsEmptyText(ViewerIdentityCapacityRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return read.Read is { HasScan: true } ? NoColumns : read.Message ?? NoScan;
    }

    /// <summary>The databases, one row each: scanned clean, scanned with columns, or not yet scanned.</summary>
    public static IReadOnlyList<ViewerIdentityScanRow> ScanRows(ViewerIdentityCapacityRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        if (read.Read is null)
        {
            return [];
        }

        return read.Read.Databases
            .Select(d => new ViewerIdentityScanRow(
                d.DatabaseName,
                ScanState(read.Read.Columns.Count(c => string.Equals(c.DatabaseName, d.DatabaseName, StringComparison.Ordinal))),
                DateTime.SpecifyKind(d.ScannedAt, DateTimeKind.Utc).ToLocalTime().ToString("g", CultureInfo.CurrentCulture)))
            .ToList();
    }

    /// <summary>The sentence under the scan list when it has no row.</summary>
    public static string ScanEmptyText(ViewerIdentityCapacityRead read)
    {
        ArgumentNullException.ThrowIfNull(read);
        return read.Message ?? NoScan;
    }

    /// <summary>The scan state of a scanned database that has <paramref name="columnsAbove10"/> columns above 10 percent.</summary>
    public static string ScanState(int columnsAbove10) =>
        columnsAbove10 <= 0
            ? ScannedClean
            : string.Create(CultureInfo.InvariantCulture, $"Scanned, {columnsAbove10} {(columnsAbove10 == 1 ? "column" : "columns")} above 10 percent");

    private static ViewerIdentityColumnRow Row(IdentityColumnFact c) => new(
        c.DatabaseName,
        string.Join(".", new[] { c.SchemaName, c.TableName }.Where(x => !string.IsNullOrEmpty(x))),
        c.ColumnName ?? "",
        TypeText(c),
        c.PercentUsed is double p ? p.ToString("N1", CultureInfo.InvariantCulture) : Dash,
        Digits(c.ValuesRemaining),
        c.Exhaustion is { } e ? e.StepsPerDay.ToString("N1", CultureInfo.InvariantCulture) : Dash,
        c.Exhaustion is { } x ? x.DaysLeft.ToString("N1", CultureInfo.InvariantCulture) : Dash,
        c.Band ?? Dash);

    private static string TypeText(IdentityColumnFact c) =>
        c.DataType is "decimal" or "numeric" && c.TypePrecision is > 0
            ? string.Create(CultureInfo.InvariantCulture, $"{c.DataType}({c.TypePrecision})")
            : c.DataType ?? Dash;

    /// <summary>A whole value with thousands separators, from the exact integer.</summary>
    private static string Digits(BigInteger? value) =>
        value is { } v ? v.ToString("N0", CultureInfo.InvariantCulture) : Dash;
}
