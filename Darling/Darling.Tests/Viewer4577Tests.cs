using System;
using System.Globalization;
using System.IO;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

// Locks in MetricFormatter — the one place optimizer costs and durations are shaped for a
// human. Contract for costs: at most four decimals, trailing zeros stripped, thousands
// separators, exactly zero is "0", a non-zero cost too small for four decimals is "<0.0001"
// and never "0", and never scientific notation. Contract for durations: the statements-grid
// ladder, ms under a second, seconds with one decimal under a minute, m+s beyond that.
//
// Every case passes InvariantCulture explicitly. Production formats in the caller's culture
// (the panels next to these values already use "N0"/"N1"), so pinning the provider here is
// what keeps the expected strings honest on a machine that separates numbers differently.
//
// Ported from erikdarlingdata/PerformanceStudio@caa57ce's MetricFormatterTests.
public class Viewer4577Tests
{
    private static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;

    [Theory]
    // Exactly zero is the whole point of the change: "0.000000" was the worst offender.
    [InlineData(0.0, "0")]
    // Trailing zeros come off, and four decimals is the cap.
    [InlineData(3526.21, "3,526.21")]
    [InlineData(16.7659, "16.7659")]
    [InlineData(0.0289, "0.0289")]
    [InlineData(0.5, "0.5")]
    // Whole numbers lose the decimal point entirely rather than showing ".0000".
    [InlineData(1.0, "1")]
    [InlineData(42.0, "42")]
    // Past four decimals it rounds, it does not truncate.
    [InlineData(0.00123456, "0.0012")]
    [InlineData(1.99999, "2")]
    // Thousands separators, including a cost big enough that "G" would go exponential.
    [InlineData(44940.5, "44,940.5")]
    [InlineData(1234567.891, "1,234,567.891")]
    [InlineData(1e12, "1,000,000,000,000")]
    // Negative is not a cost showplan produces, but it must not turn into garbage if one arrives.
    [InlineData(-16.7659, "-16.7659")]
    public void FormatCost_ShapesTheNumber(double cost, string expected)
    {
        Assert.Equal(expected, MetricFormatter.FormatCost(cost, Invariant));
    }

    [Theory]
    // The case that must NOT read "0": a cost this small is tiny, not absent. Everything
    // below half of the last shown digit rounds away, so that is where the label starts.
    [InlineData(0.000001)]
    [InlineData(0.00001)]
    [InlineData(0.000049)]
    public void FormatCost_TinyButRealCostNeverReadsAsZero(double cost)
    {
        Assert.Equal("<0.0001", MetricFormatter.FormatCost(cost, Invariant));

        // And the distinction is the point — a real zero still reads "0".
        Assert.NotEqual(MetricFormatter.FormatCost(0, Invariant), MetricFormatter.FormatCost(cost, Invariant));
    }

    [Fact]
    public void FormatCost_TinyNegativeKeepsItsSign()
    {
        Assert.Equal(">-0.0001", MetricFormatter.FormatCost(-0.000001, Invariant));
    }

    [Fact]
    public void FormatCost_JustAboveTheThresholdShowsTheDigitInstead()
    {
        // 0.00006 survives rounding to four decimals, so it gets the digit rather than the label.
        Assert.Equal("0.0001", MetricFormatter.FormatCost(0.00006, Invariant));
    }

    [Theory]
    // No showplan cost is ever this small or this large, but a format string that falls back to
    // scientific notation would corrupt the panel silently, so assert it cannot happen.
    [InlineData(1e-30)]
    [InlineData(1e20)]
    [InlineData(double.MaxValue)]
    public void FormatCost_NeverUsesScientificNotation(double cost)
    {
        var text = MetricFormatter.FormatCost(cost, Invariant);

        Assert.DoesNotContain("E", text, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    // Under a second: milliseconds, no scaling.
    [InlineData(0L, "0ms")]
    [InlineData(1L, "1ms")]
    [InlineData(847L, "847ms")]
    [InlineData(999L, "999ms")]
    // A second and up: seconds with one decimal.
    [InlineData(1000L, "1.0s")]
    [InlineData(3475L, "3.5s")]
    [InlineData(59_999L, "60.0s")]
    // A minute and up: minutes plus whole seconds.
    [InlineData(60_000L, "1m 0s")]
    [InlineData(1_235_000L, "20m 35s")]
    [InlineData(3_600_000L, "60m 0s")]
    public void FormatDuration_ClimbsTheLadder(long ms, string expected)
    {
        Assert.Equal(expected, MetricFormatter.FormatDuration(ms, Invariant));
    }

    [Fact]
    public void FormatDuration_MatchesTheLadderTheStatementsGridAlreadyUsed()
    {
        /* The helper exists to replace a private copy of exactly this ladder in StatementRow.
           If someone "improves" the boundaries here, the statements grid and every panel that
           now shares this helper move together — which is the point — so the boundaries are
           worth pinning rather than leaving to whoever edits next. */
        Assert.Equal("999ms", MetricFormatter.FormatDuration(999, Invariant));
        Assert.Equal("1.0s", MetricFormatter.FormatDuration(1000, Invariant));
        Assert.Equal("59.9s", MetricFormatter.FormatDuration(59_900, Invariant));
        Assert.Equal("1m 0s", MetricFormatter.FormatDuration(60_000, Invariant));
    }

    [Theory]
    // Below a millisecond, where the whole-ms ladder would report a query that ran as one
    // that did not. Two rungs: the number itself, then a floor for the vanishingly small.
    [InlineData(0.4, "0.4ms")]
    [InlineData(0.42, "0.42ms")]
    [InlineData(0.425, "0.43ms")]
    [InlineData(0.01, "0.01ms")]
    [InlineData(0.009, "<0.01ms")]
    [InlineData(0.0000001, "<0.01ms")]
    // Zero is zero, and a duration is never negative - but if one ever arrives it reads as 0ms
    // rather than as a fraction with a minus sign.
    [InlineData(0.0, "0ms")]
    [InlineData(-0.4, "0ms")]
    // At a millisecond and above it is the same ladder, reached by rounding rather than truncating.
    [InlineData(1.0, "1ms")]
    [InlineData(1.6, "2ms")]
    [InlineData(999.4, "999ms")]
    [InlineData(1000.0, "1.0s")]
    public void FormatDuration_HasTwoMoreRungsBelowAMillisecond(double ms, string expected)
    {
        Assert.Equal(expected, MetricFormatter.FormatDuration(ms, Invariant));
    }

    [Fact]
    public void FormatDuration_AgreesWithItselfWhicheverOverloadIsCalled()
    {
        /* The Query Store Overview kept its own copy of the sub-millisecond rungs because this
           helper had none. It delegates now, so the two must not drift: a whole number of
           milliseconds reads the same whether it arrives as a long or as a double. */
        foreach (var ms in new long[] { 0, 1, 7, 999, 1000, 59_900, 60_000, 1_235_000 })
            Assert.Equal(
                MetricFormatter.FormatDuration(ms, Invariant),
                MetricFormatter.FormatDuration((double)ms, Invariant));
    }

    /// <summary>
    /// Census pin: no cost display site in PerformanceMonitor.Ui still hand-formats with a raw
    /// ":F6" or ":F4" numeric format string for a cost. MemoryFractionInput/Output keep their own
    /// ":F4" (a different metric family PerformanceStudio leaves alone), so this only forbids
    /// ":F6", which was exclusively the unrouted cost sites this issue replaces.
    /// </summary>
    [Fact]
    public void NoRawCostFormatStringSurvivesInTheUi()
    {
        var uiDir = Path.Combine(RepoRoot(), "PerformanceMonitor.Ui");
        Assert.True(Directory.Exists(uiDir), $"PerformanceMonitor.Ui not found at {uiDir}");

        var offenders = new System.Collections.Generic.List<string>();
        var filesScanned = 0;

        foreach (var file in Directory.EnumerateFiles(uiDir, "*.cs", SearchOption.AllDirectories))
        {
            filesScanned++;
            var text = File.ReadAllText(file);
            if (text.Contains(":F6"))
                offenders.Add(file);
        }

        /* Floor over the walk itself, same idiom as ViewerButtonAccessKeyTests.RepoRoot: a walk that
           silently found nothing would satisfy an empty-offenders assertion for the wrong reason. */
        Assert.True(filesScanned >= 5, $"only {filesScanned} .cs files found under {uiDir}; the walk is not finding the source");

        Assert.True(offenders.Count == 0, "Raw \":F6\" cost formatting left in: " + string.Join(", ", offenders));
    }

    /// <summary>Same walk-up idiom as <c>ViewerButtonAccessKeyTests.RepoRoot</c>.</summary>
    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException(
            "Could not locate the repository root (walked up from the test binary looking for "
          + "PerformanceMonitor.sln). This test scans the source tree, so it cannot run without it.");
    }
}
