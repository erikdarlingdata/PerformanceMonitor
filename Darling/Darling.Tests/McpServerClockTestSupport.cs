/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using PerformanceMonitor.Analysis.Baselines;

namespace Darling.Tests;

/// <summary>
/// Shared pieces for the MCP readers' server-clock tests (#4793). US Eastern springs forward on 2026-03-08
/// (07:00Z) and falls back on 2026-11-01 (06:00Z), so a stored server-local time is -5 h in winter and -4 h in
/// summer. The readers are fed a <see cref="DataTable"/> reader in the SELECT's column order, the way
/// <c>ViewerDefaultTraceServerClockTests</c> feeds the viewer's.
/// </summary>
internal static class McpServerClockTestSupport
{
    public const string EasternWindowsId = "Eastern Standard Time";

    public static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    /// <summary>The clock the readers build for an Eastern server whose newest snapshot said -300 (or -240 in
    /// summer): the zone wins over the snapshot's offset.</summary>
    public static ServerClock Eastern(int snapshotOffsetMinutes = -300) =>
        ServerClock.Resolve(EasternWindowsId, snapshotOffsetMinutes);

    /// <summary>
    /// A table of <paramref name="width"/> columns. The columns in <paramref name="dateTimeOrdinals"/> are
    /// timestamps and every other column is text; a test leaves the text columns NULL, which every reader
    /// maps to its empty value, so only the timestamps under test need to be typed.
    /// </summary>
    public static DataTable Table(int width, params int[] dateTimeOrdinals)
    {
        var table = new DataTable();
        for (var i = 0; i < width; i++)
        {
            table.Columns.Add("c" + i, Array.IndexOf(dateTimeOrdinals, i) >= 0 ? typeof(DateTime) : typeof(string));
        }

        return table;
    }

    /// <summary>Adds a row of NULLs with the given ordinals set.</summary>
    public static void AddRow(DataTable table, params (int Ordinal, object Value)[] values)
    {
        var row = table.NewRow();
        foreach (var (ordinal, value) in values)
        {
            row[ordinal] = value;
        }

        table.Rows.Add(row);
    }
}
