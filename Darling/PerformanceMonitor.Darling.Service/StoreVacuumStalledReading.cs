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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What the Store Vacuum Stalled check (#8950) found in one read of the store's autovacuum progress
/// (<see cref="DarlingStoreTableHealthReader.GetVacuumAsync"/>): the store tables behind on autovacuum and the ones whose
/// autovacuums were cancelled repeatedly, with the worst of each. The fields after the three counts are only the evidence the
/// alert text names.
/// </summary>
/// <param name="TablesStalled">Store tables behind, repeatedly cancelled, or both. Each table counts once.</param>
/// <param name="TablesBehind">Store tables with a part past its autovacuum start point and no completed vacuum in
/// <see cref="DarlingStoreTableHealthReader.BehindWindow"/> (<see cref="DarlingStoreTableHealthReader.BehindAsOf"/>).</param>
/// <param name="TablesRepeatedlyCancelled">Store tables whose autovacuums were cancelled
/// <see cref="DarlingStoreTableHealthReader.RepeatedCancelMinimum"/> or more times in
/// <see cref="DarlingStoreTableHealthReader.CancelWindow"/>.</param>
/// <param name="WorstBehindTable"><c>schema.name</c> of the table behind the longest, or null when none is behind.</param>
/// <param name="WorstBehindHours">The hours since that table's oldest vacuum among its parts past the start point, or since the
/// statistics began when one of them has had none.</param>
/// <param name="WorstBehindNeverVacuumed">True when <paramref name="WorstBehindHours"/> runs from when the statistics began.</param>
/// <param name="WorstCancelledTable"><c>schema.name</c> of the table cancelled the most times, or null when none was cancelled
/// repeatedly.</param>
/// <param name="WorstCancels">That table's cancels in the window.</param>
/// <param name="WorstLastCancel">That table's latest cancel, UTC.</param>
internal sealed record StoreVacuumStalledReading(
    int TablesStalled,
    int TablesBehind,
    int TablesRepeatedlyCancelled,
    string? WorstBehindTable = null,
    double? WorstBehindHours = null,
    bool WorstBehindNeverVacuumed = false,
    string? WorstCancelledTable = null,
    long WorstCancels = 0,
    DateTime? WorstLastCancel = null)
{
    /// <summary>True when at least one store table is behind or repeatedly cancelled.</summary>
    public bool IsStalled => TablesStalled > 0;

    /// <summary>
    /// The reading from the autovacuum progress rows as of <paramref name="asOfUtc"/>. Ties go to the first name in ordinal
    /// order, so the same rows always name the same table.
    /// </summary>
    public static StoreVacuumStalledReading From(
        IEnumerable<DarlingStoreTableHealthReader.StoreVacuumObjectRow> rows, DateTime asOfUtc)
    {
        ArgumentNullException.ThrowIfNull(rows);
        int stalled = 0, behind = 0, cancelled = 0;
        string? worstBehind = null;
        double worstHours = -1;
        var worstNever = false;
        string? worstCancelled = null;
        long worstCancels = 0;
        DateTime? worstLastCancel = null;
        foreach (var row in rows)
        {
            var name = $"{row.SchemaName}.{row.TableName}";
            var state = DarlingStoreTableHealthReader.BehindAsOf(row, asOfUtc);
            if (state.Behind || row.RepeatedlyCancelled)
            {
                stalled++;
            }

            if (state.Behind)
            {
                behind++;
                var hours = state.Hours ?? 0;
                if (hours > worstHours || (hours == worstHours && string.CompareOrdinal(name, worstBehind) < 0))
                {
                    worstBehind = name;
                    worstHours = hours;
                    worstNever = state.Source == DarlingStoreTableHealthReader.BehindSourceStatsSince;
                }
            }

            if (row.RepeatedlyCancelled)
            {
                cancelled++;
                if (row.Cancels > worstCancels || (row.Cancels == worstCancels && string.CompareOrdinal(name, worstCancelled) < 0))
                {
                    worstCancelled = name;
                    worstCancels = row.Cancels;
                    worstLastCancel = row.LastCancel;
                }
            }
        }

        return new StoreVacuumStalledReading(
            stalled, behind, cancelled,
            worstBehind, worstBehind is null ? null : worstHours, worstNever,
            worstCancelled, worstCancels, worstLastCancel);
    }

    /// <summary>Hours as "31 h" below three days and "4 days" from there, rounded down, in the invariant culture.</summary>
    internal static string FormatHours(double hours) =>
        hours < 72
            ? string.Create(CultureInfo.InvariantCulture, $"{Math.Floor(Math.Max(0, hours)):0} h")
            : string.Create(CultureInfo.InvariantCulture, $"{Math.Floor(hours / 24):0} days");
}
