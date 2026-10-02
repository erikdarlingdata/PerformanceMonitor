using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Locks in #1134: Lite's Alert History Value/Threshold columns bind to
/// <see cref="AlertHistoryRow.CurrentValueDisplay"/> / <see cref="AlertHistoryRow.ThresholdValueDisplay"/>,
/// which run the stored DuckDB <c>current_value</c>/<c>threshold_value</c> doubles through
/// <c>FormatValue</c>. That method formerly special-cased only CPU/TempDB and fell through to ":G"
/// for everything else, leaking the raw stored double — the reported Volume Free Space
/// "0.9746057751382348". It is now keyed on the exact metric_name strings the Lite alert engine
/// emits (MainWindow.AlertEngine.cs), with a ":F2" fallback that can never render a raw
/// full-precision float. Dashboard is structurally immune (its Value is a pre-formatted string built
/// at the alert site), so this is Lite-only and these tests live in Lite.Tests.
/// </summary>
public class AlertHistoryValueFormatTests
{
    private static AlertHistoryRow Row(string metricName, double current) =>
        new() { MetricName = metricName, CurrentValue = current };

    [Fact]
    public void VolumeFreeSpace_RawFloat_RendersAsOneDecimalPercent()
    {
        // The exact value from the issue report must not leak as a raw float.
        Assert.Equal("1.0%", Row("Volume Free Space", 0.9746057751382348).CurrentValueDisplay);
    }

    [Theory]
    [InlineData("High CPU", 45.0, "45.0%")]
    [InlineData("tempdb Space", 87.3, "87.3%")]
    [InlineData("Volume Free Space", 9.74, "9.7%")]
    [InlineData("Long-Running Job", 247.8, "247.8%")] // job's "% of average"
    public void PercentMetrics_RenderOneDecimalPercent(string metric, double value, string expected)
    {
        Assert.Equal(expected, Row(metric, value).CurrentValueDisplay);
    }

    [Fact]
    public void LegacyTempDbSpaceName_StillRendersAsPercent()
    {
        /* The tempdb token was lowercased across both apps' UI (c0109f34), which changed this metric's
           metric_name KEY from "TempDB Space" to "tempdb Space"; that commit accepted that stored
           alert-history rows keep the old name. Matching here is ordinal, so those rows fell through to
           the :F2 default and showed a percentage with no unit. Both spellings must format identically. */
        Assert.Equal("87.3%", Row("TempDB Space", 87.3).CurrentValueDisplay);
        Assert.Equal(Row("tempdb Space", 87.3).CurrentValueDisplay, Row("TempDB Space", 87.3).CurrentValueDisplay);
    }

    [Fact]
    public void PoisonWait_RendersWholeMilliseconds()
    {
        Assert.Equal("1235 ms", Row("Poison Wait", 1234.56).CurrentValueDisplay);
    }

    [Fact]
    public void LongRunningQuery_RendersWholeMinutes()
    {
        Assert.Equal("12 m", Row("Long-Running Query", 12.0).CurrentValueDisplay);
    }

    [Theory]
    [InlineData("Blocking Detected", 3.0, "3")]
    [InlineData("Deadlocks Detected", 2.0, "2")]
    [InlineData("Failed Agent Job", 1.0, "1")]
    public void CountMetrics_RenderWholeNumbers(string metric, double value, string expected)
    {
        Assert.Equal(expected, Row(metric, value).CurrentValueDisplay);
    }

    [Fact]
    public void ThresholdColumn_UsesSameUnitAwareFormat()
    {
        // The Threshold column binds to ThresholdValueDisplay through the same formatter; the issue
        // showed a bare "10" for the Volume Free Space threshold, which now carries its unit.
        var row = new AlertHistoryRow { MetricName = "Volume Free Space", ThresholdValue = 10.0 };
        Assert.Equal("10.0%", row.ThresholdValueDisplay);
    }

    [Fact]
    public void UnmappedMetric_FallsBackToTwoDecimals_NeverRawFloat()
    {
        // Analysis findings log metric names like "Analysis: CPU [a1b2c3d4]" carrying a raw severity
        // double; the old ":G" fallback would leak its full precision. ":F2" must bound it.
        var display = Row("Analysis: CPU [a1b2c3d4]", 0.9746057751382348).CurrentValueDisplay;
        Assert.Equal("0.97", display);
        Assert.DoesNotContain("0.9746", display);
    }

    /* ── #1846 state-only metrics ──────────────────────────────────────────────────────────────────
       The alert-history value columns are NOT NULL doubles, but a whole family of alerts has no
       number to put there: their "current value" is a role, a connection state, a suspend reason or
       the literal "resolved". The write side hands the store a display STRING and
       AlertValueParser.ParseOrDefault yields 0 for any text with no digits, so those rows stored 0 and
       the grid rendered "0.00" — a measurement the alert never made. The read side now renders an em
       dash instead. This is the pinned list, taken from the fire sites, not from the issue text. */

    /// <summary>
    /// The exact metric-name strings whose stored 0 is a sentinel. Named here so a rename at the fire
    /// site breaks a test rather than silently restoring "0.00" to the grid.
    /// </summary>
    public static TheoryData<string> StateOnlyMetrics() => new()
    {
        /* The five actionable state metrics — AgAlertPolicy's consts plus the connection-loss alert.
           Values are respectively a role_desc, a connected_state_desc, JudgeSync's prose reason, the
           suspend_reason_desc, and the connection failure reason. */
        "AG Failover",
        "AG Replica Disconnected",
        "AG Sync Fell Behind",
        "AG Database Suspended",
        "Server Unreachable",

        /* The AG/connection resolutions — matched by AlertMetricClassifier.IsResolution rather than by
           name, but pinned explicitly because these four are the ones #1846 enumerated. */
        "AG Replica Reconnected",
        "AG Sync Recovered",
        "AG Data Movement Resumed",
        "Server Restored",

        /* Every other resolution notice. Darling's BuildResolutionRecord hardcodes
           CurrentValueText: "resolved" with a null numeric for its own self-alert recoveries AND for
           the shared engine's resolution callback, so all of these stored 0 too. */
        "Collection Resumed",
        "Capture Restored",
        "Agent Restarted",
        "Store Disk Pressure Resolved",
        "Compression Job Recovered",
        /* #3816: the other two policy families' recoveries, written by the same BuildResolutionRecord path. */
        "Refresh Job Recovered",
        "Retention Job Recovered",
        "CPU Resolved",
        "Blocking Cleared",
        "Blocking Wait Cleared",
        "Deadlocks Cleared",
        "Poison Waits Cleared",
        "Long-Running Queries Cleared",
        "tempdb Space Resolved",
        "Volume Free Space Resolved",
        "Long-Running Jobs Cleared",
    };

    [Theory]
    [MemberData(nameof(StateOnlyMetrics))]
    public void StateOnlyMetric_StoredZero_RendersDashNotZeroPointZeroZero(string metric)
    {
        Assert.Equal("—", Row(metric, 0).CurrentValueDisplay);
        Assert.Equal("—", new AlertHistoryRow { MetricName = metric, ThresholdValue = 0 }.ThresholdValueDisplay);
    }

    [Theory]
    [MemberData(nameof(StateOnlyMetrics))]
    public void StateOnlyMetric_NonZero_StillRendersTheNumber(string metric)
    {
        /* Defensive: the dash is gated on the 0 sentinel, not on the metric name alone. A nonzero value
           on one of these means some future producer started passing a real numeric, and hiding a
           number somebody deliberately supplied would be a worse bug than the one being fixed. */
        Assert.Equal("42.00", Row(metric, 42).CurrentValueDisplay);
    }

    [Theory]
    [InlineData("High CPU", "0.0%")]
    [InlineData("tempdb Space", "0.0%")]
    [InlineData("Volume Free Space", "0.0%")]
    [InlineData("Long-Running Job", "0.0%")]
    [InlineData("Poison Wait", "0 ms")]
    [InlineData("Long-Running Query", "0 m")]
    [InlineData("Blocking Wait Time", "0 s")]
    [InlineData("Blocking Detected", "0")]
    [InlineData("Deadlocks Detected", "0")]
    [InlineData("Failed Agent Job", "0")]
    public void MeasuredMetric_AtZero_KeepsItsNumber(string metric, string expected)
    {
        /* The dash must never swallow a real measurement that happens to be zero — "no blocking right
           now" and "no value was ever measured" are different statements. */
        Assert.Equal(expected, Row(metric, 0).CurrentValueDisplay);
    }

    [Fact]
    public void StoreDiskPressure_AtZero_RendersZero_BecauseZeroPercentFreeIsReal()
    {
        /* Deliberately NOT a state-only metric even though its producer passes no numeric: its text
           ("… has only 3% free (…)") carries a digit, so ParseOrDefault stores percent-free, and a
           genuine 0 there means a FULL VOLUME. Adding it to the list would hide the worst disk alert
           the product has behind a dash. */
        Assert.Equal("0.00", Row("Store Disk Pressure", 0).CurrentValueDisplay);
    }

    [Fact]
    public void UnmappedMetric_AtZero_IsUnaffected()
    {
        // An analysis finding with severity 0 is a measurement, not a missing value.
        Assert.Equal("0.00", Row("Analysis: CPU [a1b2c3d4]", 0).CurrentValueDisplay);
    }
}
