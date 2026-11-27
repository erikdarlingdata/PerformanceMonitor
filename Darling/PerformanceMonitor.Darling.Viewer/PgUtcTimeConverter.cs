/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Windows.Data;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8919: shows a stored naive-UTC time in the tab's time display zone, to the second, like the other times on a PostgreSQL tab
/// (<see cref="PgDisplay.SnapshotTime"/>), while the column stays bound to the instant so a header click still orders by time.
/// The log event and deadlock grids bound the raw UTC value and showed UTC whatever the time display setting said.
/// </summary>
public sealed class PgUtcTimeConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is DateTime utc ? PgDisplay.SnapshotTime(utc) : string.Empty;

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}
