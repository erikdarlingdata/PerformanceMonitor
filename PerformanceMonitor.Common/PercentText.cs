/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>A fraction as a percent in the one form operator-facing text uses on every host: no space before the sign
/// ("110%"). The standard <c>P0</c> / <c>P1</c> formats follow the host culture's percent pattern, and the invariant
/// culture a Linux host without a locale runs under puts a space there ("110 %"), so the same advice read differently
/// per host (#9942). The digits and group separators are the invariant ones, as the same text spells its other
/// figures.</summary>
public static class PercentText
{
    /// <summary><paramref name="fraction"/> times 100, rounded to <paramref name="decimals"/> places (0 or 1), with a
    /// thousands separator and a trailing <c>%</c>: 1.1 gives "110%" and 0.0334 with one decimal gives "3.3%".</summary>
    public static string Of(double fraction, int decimals = 0)
    {
        var format = decimals <= 0 ? "#,##0" : "#,##0." + new string('0', decimals);
        return (fraction * 100).ToString(format, CultureInfo.InvariantCulture) + "%";
    }
}
