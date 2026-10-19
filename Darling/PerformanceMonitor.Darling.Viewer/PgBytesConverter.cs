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
/// #6135: shows a byte count the way every other size on a PostgreSQL tab reads ("21.4 MB") while the column stays bound to
/// the number, so a header click still orders by bytes and not by characters. A null (no estimate) shows as blank.
/// </summary>
public sealed class PgBytesConverter : IValueConverter
{
    public object Convert(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        value is long bytes ? PgDisplay.Bytes(bytes) : string.Empty;

    public object ConvertBack(object? value, Type targetType, object? parameter, CultureInfo culture) =>
        throw new NotSupportedException();
}
