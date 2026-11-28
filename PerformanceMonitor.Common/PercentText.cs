/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>A fraction as a percent in the one form operator-facing text uses on every host. It is the standard
/// <c>P0</c> / <c>P1</c> format over a fixed number format, so the digits are exactly the en-US ones ("110%", "0.5%")
/// and the percent sign never takes a space. The host culture's own percent pattern put a space there on a Linux host
/// without a locale ("110 %"), so the same advice read differently per host (#9942). A custom pattern over
/// <c>fraction * 100</c> was tried first and moved decimal midpoints (0.015 printed "2%" where <c>P0</c> printed "1%"),
/// because the multiplication is a second rounding step.</summary>
public static class PercentText
{
    // The invariant culture's number format with en-US's percent patterns: "n%" and "-n%".
    private static readonly NumberFormatInfo Format = BuildFormat();

    /// <summary><paramref name="fraction"/> as a percent with <paramref name="decimals"/> places (0 or 1), the digits
    /// en-US <c>P0</c> / <c>P1</c> prints: 1.1 gives "110%" and 0.0334 with one decimal gives "3.3%".</summary>
    public static string Of(double fraction, int decimals = 0) =>
        fraction.ToString(FormatName(decimals), Format);

    /// <summary>The same for a <see cref="decimal"/> fraction.</summary>
    public static string Of(decimal fraction, int decimals = 0) =>
        fraction.ToString(FormatName(decimals), Format);

    private static string FormatName(int decimals) => "P" + Math.Max(decimals, 0).ToString(CultureInfo.InvariantCulture);

    private static NumberFormatInfo BuildFormat()
    {
        var format = (NumberFormatInfo)CultureInfo.InvariantCulture.NumberFormat.Clone();
        format.PercentPositivePattern = 1;
        format.PercentNegativePattern = 1;
        return NumberFormatInfo.ReadOnly(format);
    }
}
