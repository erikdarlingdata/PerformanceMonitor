/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Numerics;

namespace PerformanceMonitor.Collectors;

/// <summary>How full one identity column is: values issued, values the column can issue, values left, and the percent between.</summary>
/// <param name="Used">Values issued so far, counting the seed itself. 0 for a column with no row inserted yet.</param>
/// <param name="Total">Values the column can issue from its seed to the end its increment moves toward.</param>
/// <param name="Remaining"><c>Total - Used</c>: inserts left before the engine raises an arithmetic overflow.</param>
/// <param name="PercentUsed"><c>Used / Total * 100</c>, 0 to 100, to four decimals.</param>
public readonly record struct IdentityCapacity(BigInteger Used, BigInteger Total, BigInteger Remaining, double PercentUsed);

/// <summary>
/// How fast a column is filling, from two stored samples of its last value: <see cref="StepsPerDay"/> identity values issued per day
/// (one step is one increment, so it is in the same unit as <see cref="IdentityCapacity.Remaining"/>) and <see cref="DaysLeft"/>, the days
/// until the values left are issued at that rate. 0 when the column is already full.
/// </summary>
public readonly record struct IdentityExhaustion(double StepsPerDay, double DaysLeft);

/// <summary>
/// The identity capacity arithmetic for #6875 (<c>identity_capacity</c>), pure so the collector's top-20 filter and the later readers
/// share one definition.
///
/// <para><b>What counts as the end.</b> A column issues values from its SEED toward the end its INCREMENT moves to: the type's maximum for
/// a positive increment, its minimum for a negative one. So an <c>int</c> seeded at 1 has 2,147,483,647 values, one seeded at
/// -2,147,483,648 has 4,294,967,296, and a <c>smallint</c> counting down from -1 has 32,768. The percent is against that, not against the
/// whole type range, because a seed already past the type's midpoint leaves less room than the type name suggests.</para>
///
/// <para><b>No row yet.</b> <c>last_value</c> is NULL until the first insert: nothing is used, whatever the seed.</para>
///
/// <para><b>Types.</b> <c>tinyint</c> 0 to 255, <c>smallint</c>, <c>int</c> and <c>bigint</c> at their signed ranges, <c>decimal(p,0)</c> and
/// <c>numeric(p,0)</c> from -(10^p - 1) to 10^p - 1. Any other type has no capacity here and returns null.</para>
/// </summary>
public static class IdentityCapacityMath
{
    /// <summary>A column is kept only when more than this percent of it is used.</summary>
    public const double MinimumPercentKept = 10.0;

    /// <summary>The percent used at which a column reads as warning (the default the alert uses, #6875).</summary>
    public const double WarningPercent = 70.0;

    /// <summary>The percent used at which a column reads as critical (the default the alert uses, #6875).</summary>
    public const double CriticalPercent = 90.0;

    /// <summary>Band word under <see cref="WarningPercent"/>.</summary>
    public const string BandOk = "ok";

    /// <summary>Band word from <see cref="WarningPercent"/> up to <see cref="CriticalPercent"/>.</summary>
    public const string BandWarning = "warning";

    /// <summary>Band word from <see cref="CriticalPercent"/> up.</summary>
    public const string BandCritical = "critical";

    /// <summary>The band of a percent used: ok under 70, warning from 70, critical from 90.</summary>
    public static string Band(double percentUsed) => Band(percentUsed, WarningPercent, CriticalPercent);

    /// <summary>The band against bars other than the defaults: the Identity Capacity alert uses the bars that apply to a column's server and database (a threshold override). Critical from <paramref name="criticalPercent"/> up, warning from <paramref name="warningPercent"/>.</summary>
    public static string Band(double percentUsed, double warningPercent, double criticalPercent) =>
        percentUsed >= criticalPercent ? BandCritical : percentUsed >= warningPercent ? BandWarning : BandOk;

    /// <summary>The most columns kept per database per pass.</summary>
    public const int MaxColumnsPerDatabase = 20;

    /// <summary>The smallest and largest value a column of this type can hold; false for a type with no capacity here.</summary>
    public static bool TryGetRange(string? dataType, int precision, out BigInteger minimum, out BigInteger maximum)
    {
        minimum = BigInteger.Zero;
        maximum = BigInteger.Zero;

        switch (dataType?.Trim().ToLowerInvariant())
        {
            case "tinyint":
                maximum = 255;
                return true;
            case "smallint":
                minimum = short.MinValue;
                maximum = short.MaxValue;
                return true;
            case "int":
                minimum = int.MinValue;
                maximum = int.MaxValue;
                return true;
            case "bigint":
                minimum = long.MinValue;
                maximum = long.MaxValue;
                return true;
            case "decimal":
            case "numeric":
                if (precision is < 1 or > 38)
                {
                    return false;
                }

                maximum = BigInteger.Pow(10, precision) - 1;
                minimum = -maximum;
                return true;
            default:
                return false;
        }
    }

    /// <summary>
    /// The capacity of one column, or null when the type has no capacity here or the increment is 0 (the engine does not allow it).
    /// <paramref name="lastValue"/> NULL means no row has been inserted: nothing used. A <paramref name="lastValue"/> on the wrong side of
    /// the seed (a reseed backwards) counts as nothing used, and one past the end counts as full.
    /// </summary>
    public static IdentityCapacity? Calculate(string? dataType, int precision, BigInteger seed, BigInteger increment, BigInteger? lastValue)
    {
        if (increment.IsZero || !TryGetRange(dataType, precision, out var minimum, out var maximum))
        {
            return null;
        }

        var step = BigInteger.Abs(increment);
        var distanceToEnd = increment.Sign > 0 ? maximum - seed : seed - minimum;
        if (distanceToEnd.Sign < 0)
        {
            /* A seed outside the type's range: the engine does not allow it, so there is no capacity to report. */
            return null;
        }

        var total = (distanceToEnd / step) + 1;

        BigInteger used;
        if (lastValue is null)
        {
            used = BigInteger.Zero;
        }
        else
        {
            var distanceIssued = increment.Sign > 0 ? lastValue.Value - seed : seed - lastValue.Value;
            used = distanceIssued.Sign < 0 ? BigInteger.Zero : (distanceIssued / step) + 1;
            if (used > total)
            {
                used = total;
            }
        }

        /* Four decimals through integer division: exact for any magnitude, where a double division of two 10^38 values is not. */
        var percent = (double)(used * 1_000_000 / total) / 10_000d;
        return new IdentityCapacity(used, total, total - used, percent);
    }

    /// <summary>
    /// Days until the column is full, from two samples of <c>last_value</c> (#6875): <paramref name="oldValue"/> at <paramref name="oldTime"/>
    /// and <paramref name="newValue"/> at <paramref name="newTime"/>. The rate is the values issued per day toward the end the increment moves
    /// to, <c>(new - old) / |increment| / days</c>, and the days left are <see cref="IdentityCapacity.Remaining"/> at the newer sample over
    /// that rate. The arithmetic is integer (<see cref="BigInteger"/>, to the tick) until the final division, so a
    /// <c>decimal(38,0)</c> value is exact.
    ///
    /// <para>Null when there is nothing to divide: a missing sample, no time between the samples, a value that did not move (rate 0), one that
    /// moved away from the end (the column was reseeded between the samples, or its increment is the other sign), or a type with no
    /// capacity here.</para>
    /// </summary>
    public static IdentityExhaustion? EstimateExhaustion(
        string? dataType, int precision, BigInteger seed, BigInteger increment,
        BigInteger? oldValue, DateTime oldTime, BigInteger? newValue, DateTime newTime)
    {
        if (oldValue is null || newValue is null || newTime <= oldTime)
        {
            return null;
        }

        var steps = ((newValue.Value - oldValue.Value) * increment.Sign);
        if (steps.Sign <= 0 || increment.IsZero)
        {
            return null;
        }

        steps /= BigInteger.Abs(increment);
        if (steps.IsZero)
        {
            return null;
        }

        if (Calculate(dataType, precision, seed, increment, newValue) is not { } capacity)
        {
            return null;
        }

        var spanTicks = new BigInteger((newTime - oldTime).Ticks);
        var ticksPerDay = new BigInteger(TimeSpan.TicksPerDay);
        var stepsPerDay = (double)(steps * ticksPerDay) / (double)spanTicks;
        var ticksLeft = capacity.Remaining * spanTicks / steps;
        return new IdentityExhaustion(stepsPerDay, (double)ticksLeft / (double)ticksPerDay);
    }
}
