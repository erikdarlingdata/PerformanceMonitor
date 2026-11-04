/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The one place the get_finops reply rounds a number (#7467). The store returns <c>numeric</c> values with up to twenty
/// decimal places and a zero as <c>0.00000000000000000000</c>; the reply rounds percentages to 0.1, ratios to 3 places and
/// megabytes to 2, and prints no trailing zeros, so a zero is <c>0</c>.
/// </summary>
internal static class FinOpsWireFigures
{
    /// <summary>A decimal with scale 28; dividing by it drops trailing zeros without changing the value.</summary>
    private const decimal Unscale = 1.0000000000000000000000000000m;

    /// <summary>Below this a megabyte figure keeps two significant digits instead of two decimals, so a real 0.0008 MB does not read as nothing.</summary>
    private const decimal MbFloor = 0.01m;

    /// <summary>Percentages: 0.1, away from zero.</summary>
    internal static decimal Pct(decimal value) => Clean(Math.Round(value, 1, MidpointRounding.AwayFromZero));

    internal static decimal Pct(double value) => Pct((decimal)value);

    internal static decimal? Pct(decimal? value) => value is decimal v ? Pct(v) : null;

    /// <summary>Ratios: 3 places, away from zero.</summary>
    internal static decimal Ratio(decimal value) => Clean(Math.Round(value, 3, MidpointRounding.AwayFromZero));

    /// <summary>Megabytes: 2 places; a nonzero value under 0.01 keeps its first two significant digits.</summary>
    internal static decimal Mb(decimal value)
    {
        var magnitude = Math.Abs(value);
        if (magnitude >= MbFloor || magnitude == 0m)
            return Clean(Math.Round(value, 2, MidpointRounding.AwayFromZero));

        /* 0.00082 has its first nonzero digit in the fourth place, so two significant digits reach the fifth. */
        var places = 2;
        for (var probe = magnitude; probe < MbFloor * 10m && places < 28; probe *= 10m) places++;
        return Clean(Math.Round(value, Math.Min(places, 28), MidpointRounding.AwayFromZero));
    }

    /// <summary>Milliseconds follow the megabyte rule: 2 places, two significant digits under 0.01.</summary>
    internal static decimal Ms(decimal value) => Mb(value);

    internal static decimal? Mb(decimal? value) => value is decimal v ? Mb(v) : null;

    /// <summary>The same value with no trailing zeros, so 10.0 prints 10 and a zero prints 0.</summary>
    internal static decimal Clean(decimal value) => value / Unscale;
}
