using System;
using System.Globalization;

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// The shared shape for the two metric families that reach a human: optimizer costs
/// and durations. Costs come out of showplan at full float precision, which reads as
/// noise in a panel ("16.765900", "0.000000"), and durations come out as raw
/// milliseconds, which stop being readable somewhere past a few seconds.
///
/// Human-display strings only. Machine-readable output — MCP tool results, plan XML
/// round-tripping — must NOT come through here: those keep the raw values so whatever
/// reads them can do its own arithmetic.
/// </summary>
public static class MetricFormatter
{
    /// <summary>
    /// Magnitude below which a cost rounds away to nothing at four decimals. Anything
    /// under this is reported as "smaller than the smallest thing we show" rather than
    /// rounded to "0".
    /// </summary>
    private const double SmallestShownCost = 0.00005;

    /// <summary>
    /// An optimizer cost (operator, subtree, I/O, CPU) with at most four decimal places,
    /// trailing zeros stripped, and thousands separators like the rest of the UI:
    /// 3526.210000 becomes "3,526.21", 16.765900 becomes "16.7659", 0.028900 becomes
    /// "0.0289". Exactly zero is "0", never "0.0000".
    ///
    /// A non-zero cost too small to survive four decimals reads "&lt;0.0001" rather than
    /// "0", so a cost that exists never displays as free. Never scientific notation.
    /// </summary>
    public static string FormatCost(double cost, IFormatProvider? provider = null)
    {
        provider ??= CultureInfo.CurrentCulture;

        if (cost == 0)
            return "0";

        if (Math.Abs(cost) < SmallestShownCost)
            return cost > 0 ? "<0.0001" : ">-0.0001";

        // "#,##0.####" both caps the decimals at four and drops the trailing zeros, and
        // unlike "G"/"R" it can never fall back to scientific notation on a huge cost.
        return cost.ToString("#,##0.####", provider);
    }

    /// <summary>
    /// The same ladder the statements grid already uses (under a second stays in
    /// milliseconds like "847ms", under a minute scales to seconds with one decimal like
    /// "3.5s", and anything longer splits into minutes and seconds like "20m 35s"), for a
    /// duration that can be finer than a millisecond.
    /// </summary>
    /// <remarks>
    /// The ladder below starts at whole milliseconds, so an average CPU of 0.4ms — an ordinary
    /// number on a healthy OLTP database — rounds to "0ms" and reports a query that ran as one that
    /// did not. Query Store keeps microseconds, so its numbers arrive here as fractions and get two
    /// extra rungs below 1ms; anything already counted in whole milliseconds climbs exactly the
    /// rungs it always did, because that is what the overload below hands it.
    /// </remarks>
    public static string FormatDuration(double ms, IFormatProvider? provider = null)
    {
        provider ??= CultureInfo.CurrentCulture;

        if (ms > 0 && ms < 1)
            return ms < 0.01 ? "<0.01ms" : ms.ToString("0.##", provider) + "ms";

        return FormatDuration((long)Math.Round(ms), provider);
    }

    public static string FormatDuration(long ms, IFormatProvider? provider = null)
    {
        provider ??= CultureInfo.CurrentCulture;

        if (ms < 1000)
            return ms.ToString("N0", provider) + "ms";

        if (ms < 60_000)
            return (ms / 1000.0).ToString("F1", provider) + "s";

        var minutes = (ms / 60_000).ToString("N0", provider);
        var seconds = (ms % 60_000 / 1000).ToString("N0", provider);
        return $"{minutes}m {seconds}s";
    }
}
