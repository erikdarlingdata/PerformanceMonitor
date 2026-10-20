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

    /// <summary>A range that was already whole reads exactly as before, and a formatter that ignores the step still draws no
    /// repeated label: the repeat is left blank.</summary>
    [Fact]
    public void AWholeRangeAxis_IsUnchanged_AndAStepBlindFormatterNeverRepeats()
    {
        var r = Run();
        Assert.Equal(new[] { "0%", "20%", "40%", "60%", "80%", "100%" }, Strings(r.GetProperty("wholePct")));
        Assert.Equal(new[] { "0 ms", "200 ms", "400 ms", "600 ms", "800 ms", "1.0 s" }, Strings(r.GetProperty("wholeMs")));
        var labels = Strings(r.GetProperty("roundingAxis")).Where(l => l.Length > 0).ToArray();
        Assert.Equal(labels.Length, labels.Distinct().Count());
        Assert.Contains("0 ms", labels);
        Assert.Contains("1 ms", labels);
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

    /// <summary>The Custom View chart: 80,000 MB reads "78.1 GB" and the axis margins are as wide as the widest label, left and
    /// right, so no digit is cut. A chart with small numbers keeps its 58 px left margin and 56 px right gutter.</summary>
    [Fact]
    public void ABigSizeAxis_ReadsGb_AndReservesRoomForItsWidestLabel()
    {
        var r = Run();
        var big = r.GetProperty("bigAxis");
        Assert.Equal(new[] { "0 MB", "19.5 GB", "39.1 GB", "58.6 GB", "78.1 GB" }, Strings(big.GetProperty("left")));
        Assert.Equal("80,000.0", Strings(big.GetProperty("right")).Last());
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
}
