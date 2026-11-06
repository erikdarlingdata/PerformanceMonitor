/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The web charts' axes and the system_health CPU chart's window (#6158). The axis checks run the shipped <c>util.js</c>,
/// <c>compose.js</c> and <c>charts.js</c> under Node (<c>web-chart-axis-harness.mjs</c>); they are skipped when Node is not
/// installed. The bucketing checks run the C# the CPU chart's read uses.
/// </summary>
public sealed class WebChartAxisBehaviourTests
{
    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-chart-axis-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped chart script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(30000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the chart axis harness did not finish in 30 s");
            }

            Assert.True(proc.ExitCode == 0, "the chart axis harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    private static string[] Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToArray();

    /// <summary>The issue's three axes: a 0-1 range on a millisecond, a plain number and a percent axis printed "1, 1, 1, 0,
    /// 0, 0". Each now reads 0.0, 0.2, ... 1.0, every tick different.</summary>
    [Fact]
    public void ASmallRangeAxis_PrintsDecimals_SoNoTwoTicksReadAlike()
    {
        var r = Run();
        Assert.Equal(new[] { "0.0 ms", "0.2 ms", "0.4 ms", "0.6 ms", "0.8 ms", "1.0 ms" }, Strings(r.GetProperty("msAxis")));
        Assert.Equal(new[] { "0.0%", "0.2%", "0.4%", "0.6%", "0.8%", "1.0%" }, Strings(r.GetProperty("pctAxis")));
        Assert.Equal(new[] { "0.0", "0.2", "0.4", "0.6", "0.8", "1.0" }, Strings(r.GetProperty("plainAxis")));
    }

    /// <summary>#7586: the FinOps Version Store (PVS) size trend, drawn by the shipped tab (version-store.js) from a stand-in read. A range
    /// under 0.01 MB reads one decimal finer than the step needs, so every tick is its own value; a large range keeps its two decimals.</summary>
    [Fact]
    public void ThePvsSizeAxis_GrowsDecimalsWithItsStep_AndKeepsTwoOnALargeRange()
    {
        var r = Run();
        var small = Strings(r.GetProperty("pvsSmall"));
        Assert.Equal(new[] { "0.000", "0.002", "0.004", "0.006", "0.008", "0.010" }, small);
        Assert.Equal(small.Length, small.Distinct().Count());
        Assert.Equal(new[] { "0.00", "2,000.00", "4,000.00", "6,000.00", "8,000.00", "10,000.00" }, Strings(r.GetProperty("pvsLarge")));
    }

    /// <summary>A range that was already whole reads exactly as before, and a formatter that ignores the step still draws no
    /// repeated label: the repeat is left blank.</summary>
    [Fact]
    public void AWholeRangeAxis_IsUnchanged_AndAStepBlindFormatterNeverRepeats()
    {
        var r = Run();
        Assert.Equal(new[] { "0%", "20%", "40%", "60%", "80%", "100%" }, Strings(r.GetProperty("wholePct")));
        // #7586: one unit per axis, from the largest tick: the 1,000 ms top reads in seconds, so no "800 ms, 1.0 s" on one axis.
        Assert.Equal(new[] { "0 s", "0.2 s", "0.4 s", "0.6 s", "0.8 s", "1.0 s" }, Strings(r.GetProperty("wholeMs")));
        var labels = Strings(r.GetProperty("roundingAxis")).Where(l => l.Length > 0).ToArray();
        Assert.Equal(labels.Length, labels.Distinct().Count());
        Assert.Contains("0 ms", labels);
        Assert.Contains("1 ms", labels);
    }

    /// <summary>#7587: the issue's rate axis. Gridlines 0, 0.002 ... 0.01 read 0, 0, 0, 0.01, 0.01, 0.01, so the 0.006 gridline carried the "0.01"
    /// label. Each now reads its own value. A formatter a decimal short grows decimals on a second pass with a finer step instead of blanking,
    /// and one that ignores the step altogether still never repeats a label.</summary>
    [Fact]
    public void ASmallRateAxis_LabelsEachGridlineWithItsOwnValue()
    {
        var r = Run().GetProperty("smallRate");
        var expected = new[] { "0 MB/s", "0.002 MB/s", "0.004 MB/s", "0.006 MB/s", "0.008 MB/s", "0.01 MB/s" };
        Assert.Equal(expected, Strings(r.GetProperty("labels")));
        Assert.Equal(new[] { "0", "0.002", "0.004", "0.006", "0.008", "0.01" },
            r.GetProperty("scale").GetProperty("ticks").EnumerateArray().Select(t => t.GetDouble().ToString("0.###", System.Globalization.CultureInfo.InvariantCulture)));
        // the drawn chart: 0.0042 MB/s peaks are not rounded into 0.01 steps
        var drawn = Strings(r.GetProperty("drawn")).Where(l => l.Length > 0).ToArray();
        Assert.Equal(drawn.Length, drawn.Distinct().Count());
        Assert.DoesNotContain("0.01 MB/s", drawn.Take(drawn.Length - 1));
        // a tooltip (no step) keeps two places, but a small rate keeps two significant digits and never reads 0
        Assert.Equal(new[] { "0.0042", "1,234.57", "0" }, Strings(r.GetProperty("tooltip")));
        var oneShort = Strings(r.GetProperty("oneShort"));
        Assert.Equal(new[] { "0.000", "0.002", "0.004", "0.006", "0.008", "0.010" }, oneShort);
        var blind = Strings(r.GetProperty("blind")).Where(l => l.Length > 0).ToArray();
        Assert.Equal(blind.Length, blind.Distinct().Count());
    }

    /// <summary>#7586: the per-query history chart for a 30 s query read "0 ms ... 80,000 ms". A duration axis prints one unit from its largest
    /// tick, and its ticks sit on round numbers of that unit.</summary>
    [Fact]
    public void ADurationAxis_PrintsOneUnit_FromTheLargestTick()
    {
        var r = Run().GetProperty("durationAxis");
        Assert.Equal(new[] { "s" }, Strings(r.GetProperty("s30Units")));
        Assert.Equal(new[] { "0 s", "10 s", "20 s", "30 s", "40 s" }, Strings(r.GetProperty("s30")));
        Assert.Equal(new[] { "min" }, Strings(r.GetProperty("m5Units")));
        Assert.All(Strings(r.GetProperty("m5")), l => Assert.EndsWith("min", l));
        Assert.All(Strings(r.GetProperty("small")), l => Assert.EndsWith(" ms", l));
        // a grid cell or tooltip (no axis) still picks its unit from its own value
        Assert.Equal(new[] { "999 ms", "1.5 s", "1.5 min", "1.5 h" }, Strings(r.GetProperty("cell")));
    }

    /// <summary>Across 495 maxima (0.0001 to 9.9 million) and the five formatters the panels use, no two ticks of an axis
    /// read the same.</summary>
    [Fact]
    public void NoAxisRepeatsALabel_AtAnyMagnitude()
    {
        var sweep = Run().GetProperty("sweep");
        Assert.True(sweep.GetProperty("checked").GetInt32() >= 400);
        Assert.True(sweep.GetProperty("repeatCount").GetInt32() == 0, sweep.GetProperty("repeats").ToString());
    }

    /// <summary>A chart of counts keeps every gridline on a whole number, so a small-count axis reads 0, 1, 2 and never "1 1 1 0 0 0".
    /// (This replaces a source-text pin of niceScale's signature in ChartIntegerTicksPinTests, #5833.)</summary>
    [Fact]
    public void ACountAxis_KeepsWholeNumberTicks_WhereTheSameRangeOtherwiseStepsByFractions()
    {
        var a = Run().GetProperty("integerAxis");
        foreach (var c in a.GetProperty("withOption").EnumerateArray())
        {
            var max = c.GetProperty("max").GetDouble();
            var ticks = c.GetProperty("ticks").EnumerateArray().Select(t => t.GetDouble()).ToArray();
            Assert.All(ticks, t => Assert.Equal(Math.Round(t), t));
            Assert.Equal(ticks.Length, ticks.Distinct().Count());
            Assert.True(ticks[^1] >= max);
        }

        Assert.Contains(a.GetProperty("without").EnumerateArray().Select(t => t.GetDouble()), t => t != Math.Round(t));
        var drawn = Strings(a.GetProperty("drawn")).Where(l => l.Length > 0).ToArray();
        Assert.Equal(drawn.Length, drawn.Distinct().Count());
    }

    /// <summary>#7488: a size axis whose labels step up to GB puts its ticks on whole GB (20, 40, 60), not on whole MB that read 19.5 / 39.1 / 58.6.
    /// A chart whose largest value stays under 1,024 MB keeps the ticks it had. The expected words are built in Node with the page's own number
    /// formatter, so the test holds in any locale.</summary>
    [Fact]
    public void ASizeAxis_ThatStepsUpToGb_PicksItsTicksInGb_AndASmallOneKeepsItsMbTicks()
    {
        var a = Run().GetProperty("sizeAxis");
        Assert.Equal(Strings(a.GetProperty("bigExpected")), Strings(a.GetProperty("big")));
        Assert.NotEqual(Strings(a.GetProperty("bigWithout")), Strings(a.GetProperty("big")));

        // Composed panels: both the left axis and the overlay axis on the right.
        Assert.Equal(Strings(a.GetProperty("composedLeftExpected")), Strings(a.GetProperty("composedLeft")));
        Assert.Equal(Strings(a.GetProperty("composedRightExpected")), Strings(a.GetProperty("composedRight")));

        // Under 1,024 MB the axis is exactly the one it drew without the size unit.
        Assert.Equal(Strings(a.GetProperty("smallWithout")), Strings(a.GetProperty("small")));
        Assert.All(Strings(a.GetProperty("small")), l => Assert.EndsWith("MB", l, StringComparison.Ordinal));
    }

    /// <summary>#7555: a gridline's label states the value the line is drawn at. A size axis whose data max sits just under a unit step (1,023 MB
    /// rounds up to a 1,500 MB top tick, which labels in GB) used to pick its ticks in MB and print them in GB, so the 1,000 MB line read "1.0 GB"
    /// (1,024 MB) while the 1,023 MB peak was drawn above it. Each gridline's value is read from where the line is drawn, scaled by a dot drawn at a
    /// known value, and must equal its label's number times its unit. The ticks are round numbers of the unit they print in (0.2, 0.4 ... 1.0 GB), so a label is exact, and the
    /// bound is only float noise: a 500 MB line labelled "0.5 GB" (512 MB) is off by 2% of the axis and fails. Left axis and right axis, in MB and in bytes.</summary>
    [Fact]
    public void ASizeAxis_JustUnderAUnitStep_LabelsEveryGridlineWithTheValueItIsDrawnAt()
    {
        var a = Run().GetProperty("gridAxis");
        foreach (var name in new[] { "mb1023", "mb1010", "mb386", "bytes1030000", "right386", "right1023" })
        {
            var lines = a.GetProperty(name).EnumerateArray().ToArray();
            Assert.True(lines.Length >= 3, name + " drew fewer than three labelled gridlines");
            var top = lines.Max(x => Math.Abs(x.GetProperty("tick").GetDouble()));
            foreach (var l in lines)
            {
                var tick = l.GetProperty("tick").GetDouble();
                var said = l.GetProperty("said").GetDouble();
                Assert.True(Math.Abs(tick - said) <= top * 1e-4,
                    $"{name}: the gridline drawn at {tick} is labelled \"{l.GetProperty("label").GetString()}\", which says {said}");
            }
        }
    }

    /// <summary>#7515: the panels pass the size unit themselves. A composed panel (renderComposedResult, left axis and overlay axis) and a
    /// "format: mb" panel (VIZ.line, the public way into vizLine) are drawn with a series near 60,000 MB and nothing sets sizeUnit by hand,
    /// so a panel that stops passing it reads 19.5, 39.1, 58.6 GB again. A small "mb" panel keeps its MB ticks.</summary>
    [Fact]
    public void ThePanels_PassTheSizeUnit_SoABigSizeAxisReadsInGb()
    {
        var a = Run().GetProperty("panelAxis");
        var left = Strings(a.GetProperty("expectedLeft"));
        var right = Strings(a.GetProperty("expectedRight"));
        Assert.Equal(left, Strings(a.GetProperty("composedLeft")));
        Assert.Equal(left, Strings(a.GetProperty("composedOverlayLeft")));
        Assert.Equal(right, Strings(a.GetProperty("composedOverlayRight")));
        Assert.Equal(left, Strings(a.GetProperty("mbPanel")));
        Assert.Equal(Strings(a.GetProperty("mbPanelSmallWithout")), Strings(a.GetProperty("mbPanelSmall")));
    }

    /// <summary>#7555: a "format: bytes" panel (I/O Read Bytes per Second, Database Temp Bytes) picks its ticks in the unit its labels print, so a
    /// series near 60,000 B reads 20, 40, 60 KB and not 19.5, 39.1, 58.6 KB; a series near 60 MB reads 20, 40, 60 MB. A small one stays in bytes.
    /// A cell or tooltip (no axis) still picks a unit by its own value.</summary>
    [Fact]
    public void ABytesAxis_PicksItsTicksInThePrintedUnit()
    {
        var a = Run().GetProperty("byteAxis");
        Assert.Equal(Strings(a.GetProperty("kbExpected")), Strings(a.GetProperty("kb")));
        Assert.Equal(Strings(a.GetProperty("mbExpected")), Strings(a.GetProperty("mb")));
        Assert.All(Strings(a.GetProperty("small")), l => Assert.EndsWith(" B", l, StringComparison.Ordinal));
        Assert.Equal(new[] { "0 B", "900 B", "1.5 KB", "2.0 MB", "3.0 GB" }, Strings(a.GetProperty("cell")));
        Assert.Equal(new[] { "900 B", "2.0 KB" }, Strings(a.GetProperty("mapIndex")));
    }

    /// <summary>#7555: every tick of a size axis prints in the unit of its largest tick. A Memory Trend near 2 GB read 0 MB, 512 MB, 1.0 GB, 1.5 GB;
    /// it now reads 0 GB, 0.5 GB, 1.0 GB, 1.5 GB, 2.0 GB. A tooltip or grid cell keeps the unit that fits its own value.</summary>
    [Fact]
    public void ASizeAxis_PrintsEveryTickInTheUnitOfItsLargest()
    {
        var a = Run().GetProperty("memoryAxis");
        Assert.Equal(new[] { "GB" }, Strings(a.GetProperty("units")));
        Assert.Equal(Strings(a.GetProperty("expected")), Strings(a.GetProperty("labels")).Take(4).ToArray());
        Assert.Equal(new[] { "512 MB", "1.5 GB" }, Strings(a.GetProperty("tooltipCell")));
    }

    /// <summary>The Custom View chart: 80,000 MB reads "78.1 GB" and the axis margins are as wide as the widest label, left and
    /// right, so no digit is cut. A chart with small numbers keeps its 58 px left margin and 56 px right gutter.</summary>
    [Fact]
    public void ABigSizeAxis_ReadsGb_AndReservesRoomForItsWidestLabel()
    {
        var r = Run();
        var big = r.GetProperty("bigAxis");
        Assert.Equal(new[] { "0 GB", "19.5 GB", "39.1 GB", "58.6 GB", "78.1 GB" }, Strings(big.GetProperty("left")));
        Assert.Equal("80,000", Strings(big.GetProperty("right")).Last()); // #7552: a 20,000 step needs no decimal
        Assert.True(big.GetProperty("leftRoom").GetDouble() >= big.GetProperty("leftNeed").GetDouble());
        Assert.True(big.GetProperty("rightRoom").GetDouble() >= big.GetProperty("rightNeed").GetDouble());

        var tiny = r.GetProperty("tinyAxis");
        Assert.Equal(58, tiny.GetProperty("left").GetInt32());
        Assert.Equal(56, tiny.GetProperty("rightGutter").GetInt32());
    }

    /// <summary>A size is written "MB" and steps up to "GB" past 1,024, as the rest of the web writes sizes; a tick step gives it
    /// the decimals it needs.</summary>
    [Fact]
    public void ASizeIsWrittenInCapitals_AndStepsUpToGb()
    {
        var composed = Run().GetProperty("composed");
        Assert.Equal(new[] { "0.5 MB", "512.0 MB", "1,023.0 MB", "1.0 GB", "1.5 GB", "78.1 GB", "1.9 TB" }, Strings(composed.GetProperty("mb")));
        Assert.Equal("2.0 MB", composed.GetProperty("kb").GetString());
        Assert.Equal("80.0 GB", composed.GetProperty("gb").GetString());
        Assert.Equal(new[] { "0 MB", "100 MB", "200 MB", "300 MB" }, Strings(composed.GetProperty("mbStep")));
        Assert.Equal(new[] { "0.0 MB", "0.2 MB", "0.4 MB" }, Strings(composed.GetProperty("mbStepSmall")));
        // Values that are not sizes read as they did.
        Assert.Equal("12.3%", composed.GetProperty("percent").GetString());
        Assert.Equal("1,234", composed.GetProperty("count").GetString());
        Assert.Equal("0.3 ms/s", composed.GetProperty("bare").GetString());
    }

    /// <summary>Called without a step (a tooltip, a table cell, <c>array.map(fmtMs)</c>) a formatter reads as it always did.</summary>
    [Fact]
    public void AFormatterWithoutAStep_ReadsAsBefore()
    {
        var f = Run().GetProperty("formatters");
        Assert.Equal(new[] { "0 ms", "2 ms", "999 ms", "1.5 s" }, Strings(f.GetProperty("msNoStep")));
        Assert.Equal("1%", f.GetProperty("pctNoStep").GetString());
        Assert.Equal(new[] { "5 ms", "7 ms" }, Strings(f.GetProperty("mapIndex")));
        Assert.Equal(new[] { "200 MB", "1,023 MB", "1.0 GB", "2.0 GB" }, Strings(f.GetProperty("mb")));
        Assert.Equal(new[] { "0.0 ms", "0.2 ms", "0.4 ms" }, Strings(f.GetProperty("msStep")));
        Assert.Equal(new[] { "1.00 s", "1.05 s", "1.10 s" }, Strings(f.GetProperty("msSeconds")));
    }

    private static SystemHealthRecord Snapshot(DateTime at, long system, long sql) =>
        new() { EventTime = at, SystemCpuUtilization = system, SqlCpuUtilization = sql };

    /// <summary>A day of one-a-minute snapshots, which the row-limited read cut to its newest rows, becomes buckets that cover the
    /// whole day, oldest first, within the chart's point budget, each the mean of its snapshots.</summary>
    [Fact]
    public void ADayOfMinuteSnapshots_BucketsAcrossTheWholeWindow_WithinTheChartBudget()
    {
        var end = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);
        var start = end.AddHours(-24);
        var rows = Enumerable.Range(0, 24 * 60).Select(i => Snapshot(start.AddMinutes(i), i % 60, 10)).Reverse().ToList();

        Assert.Null(TrendBuckets.Resolve(24, null, 2, TrendBudget.Chart, out var width));
        var buckets = DarlingMcpHealthParserTools.BucketCpu(rows, width);

        Assert.True(buckets.Count * 2 <= TrendBudget.Chart.AutoPoints, $"{buckets.Count} buckets x 2 series is over the chart budget");
        Assert.True(buckets.Count > 100, "a day should not collapse to a few buckets");
        Assert.Equal(start, buckets[0].Start);
        Assert.True(buckets[^1].Start > end.AddMinutes(-2 * width), "the newest bucket is at the end of the window");
        Assert.Equal(buckets.OrderBy(b => b.Start).Select(b => b.Start), buckets.Select(b => b.Start));
        Assert.Equal(rows.Count, buckets.Sum(b => b.Snapshots));
        Assert.All(buckets, b => Assert.Equal(10, b.SqlCpu));
    }

    /// <summary>A bucket is the mean of the readings in it; a reading that is absent is left out of the mean, and a bucket whose
    /// snapshots all lack one reads null for it rather than 0.</summary>
    [Fact]
    public void ABucket_IsTheMeanOfItsReadings_AndAbsenceStaysNull()
    {
        var t = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);
        var rows = new[]
        {
            Snapshot(t, 10, 2),
            Snapshot(t.AddMinutes(1), 20, 3),
            new SystemHealthRecord { EventTime = t.AddMinutes(2), SystemCpuUtilization = null, SqlCpuUtilization = 7 },
            Snapshot(t.AddMinutes(30), 50, 1),
        };

        var buckets = DarlingMcpHealthParserTools.BucketCpu(rows, 10);

        Assert.Equal(2, buckets.Count);
        Assert.Equal(t, buckets[0].Start);
        Assert.Equal(15, buckets[0].SystemCpu);
        Assert.Equal(4, buckets[0].SqlCpu);
        Assert.Equal(3, buckets[0].Snapshots);
        Assert.Equal(t.AddMinutes(30), buckets[1].Start);
        Assert.Null(DarlingMcpHealthParserTools.BucketCpu(new[] { new SystemHealthRecord { EventTime = t, SqlCpuUtilization = 1 } }, 10)[0].SystemCpu);
    }

    /// <summary>#7578: a chart says its unit once. Ticks that carry the unit (200 ms, 0.3 MB/s) draw no caption over the axis, bare ticks keep it.</summary>
    [Fact]
    public void AUnit_IsPrintedOnTheTicksOrInTheCaption_NeverBoth()
    {
        var c = Run().GetProperty("unitCaptions");
        Assert.Empty(Strings(c.GetProperty("ticksWithUnit")));
        Assert.Empty(Strings(c.GetProperty("ticksWithRateUnit")));
        Assert.Empty(Strings(c.GetProperty("stackedSecondsTicks")));
        Assert.Equal(new[] { "MB" }, Strings(c.GetProperty("bareTicks")));
        Assert.Equal(new[] { "n" }, Strings(c.GetProperty("secondAxisWithUnit")));
    }
}
