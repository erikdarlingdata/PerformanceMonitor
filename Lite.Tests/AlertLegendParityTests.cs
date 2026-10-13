// Linked into Darling.Tests as well (see Darling.Tests.csproj): the legend text lives in one XAML per SKU and
// the colour authority is the shared AlertSeverity.ForMetric, so one file reads both and nothing else.
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5641 review: the Viewer's "Alert Severity Colors" legend listed Runnable Queue on the yellow line while
/// Lite's listed it on orange and red, and <see cref="AlertSeverity.ForMetric"/> resolves the metric to orange
/// for Warning and red for Critical. A legend is documentation of that function, so it is pinned to it: the
/// colour a legend line names for Runnable Queue must be one ForMetric returns for the metric, both tiers must
/// be listed, and both SKUs must agree. #5640 adds Client Timeouts, which is graded the same way (orange at
/// Warning, red at Critical) and sits on the same two legend lines.
/// </summary>
[Trait("Reads", "Darling")]
[Trait("Reads", "Lite")]
[Trait("Stage", "Guard")]
public sealed class AlertLegendParityTests
{
    private const string RunnableQueueMetric = "Runnable Queue";

    /* The legend text is the operator-facing name; the wire name is AttentionEvaluator.MetricName, and the fact below
       pins the two together. */
    private const string ClientTimeoutsMetric = "Client Timeouts";

    private static readonly Regex LegendLine = new(
        @"<Run Foreground=""(?<hex>#[0-9A-Fa-f]{6})""[^>]*Text=""[^""]*""\s*/>\s*—\s*(?<text>[^\r\n<]*)",
        RegexOptions.Compiled);

    public static IEnumerable<object[]> SkuData() => new[]
    {
        new object[] { "Lite", "Lite/Windows/SettingsWindow.xaml", RunnableQueueMetric },
        new object[] { "Darling Viewer", "Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml", RunnableQueueMetric },
        new object[] { "Lite", "Lite/Windows/SettingsWindow.xaml", ClientTimeoutsMetric },
        new object[] { "Darling Viewer", "Darling/PerformanceMonitor.Darling.Viewer/SettingsWindow.xaml", ClientTimeoutsMetric },
    };

    [Fact]
    public void TheClientTimeoutsLegendName_IsTheEnginesMetricName()
    {
        Assert.Equal(AttentionEvaluator.MetricName, ClientTimeoutsMetric);
    }

    [Theory]
    [MemberData(nameof(SkuData))]
    public void TheLegend_PutsTheMetricOnTheColoursForMetricReturns_AtBothTiers(string sku, string relativePath, string metric)
    {
        var warning = AlertSeverity.ForMetric(metric, AlertSeverityLevel.Warning).HexColor.ToUpperInvariant();
        var critical = AlertSeverity.ForMetric(metric, AlertSeverityLevel.Critical).HexColor.ToUpperInvariant();
        Assert.Equal(warning, AlertSeverity.ForMetric(metric).HexColor.ToUpperInvariant());

        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), relativePath.Replace('/', Path.DirectorySeparatorChar)));
        var hexesNamingTheMetric = LegendLine.Matches(xaml)
            .Where(m => m.Groups["text"].Value.Contains(metric, StringComparison.Ordinal))
            .Select(m => m.Groups["hex"].Value.ToUpperInvariant())
            .ToList();

        Assert.True(hexesNamingTheMetric.Count > 0, $"{sku}: no legend line names {metric}");
        Assert.Contains(warning, hexesNamingTheMetric);
        Assert.Contains(critical, hexesNamingTheMetric);
        Assert.All(
            hexesNamingTheMetric,
            hex => Assert.True(
                hex == warning || hex == critical,
                $"{sku}: the legend lists {metric} on {hex}, but ForMetric resolves it to {warning} or {critical}"));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
