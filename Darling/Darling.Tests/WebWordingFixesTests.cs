using System;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;
namespace Darling.Tests;

/// <summary>
/// #5644 W8, W10 and W15: wording fixes in the shipped page script, pinned against the source (the page itself needs a browser).
/// </summary>
public sealed class WebWordingFixesTests
{
    private static string ServerTabs() =>
        ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js").ReplaceLineEndings("\n");

    [Fact]
    public void ThePickerPlurals_AreWrittenOut_NotBuiltWithAnS()
    {
        var js = ServerTabs();

        /* W15: "latch class" + "s" read "latch classs" in the panel subtitle and the picker's defaults label. */
        Assert.Contains("plural: \"latch classes\"", js, StringComparison.Ordinal);
        Assert.Contains("plural: \"spinlocks\"", js, StringComparison.Ordinal);
        Assert.DoesNotContain("spec.noun + \"s\"", js, StringComparison.Ordinal);
        Assert.Contains("\"Top \" + spec.plural", js, StringComparison.Ordinal);
        Assert.Contains("spec.plural + \" you check\"", js, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLatchAndSpinlockRateColumns_SayTheyAreTheLatestInterval()
    {
        var js = ServerTabs();

        /* W10: the panels read "last 24 hours" and so do their totals; the per-second rate is the latest interval's. */
        Assert.Matches(new Regex(@"key: ""wait_ms_per_second"", label: ""ms/s, latest interval"""), js);
        Assert.Matches(new Regex(@"key: ""collisions_per_second"", label: ""Coll/s, latest interval"""), js);
    }

    [Fact]
    public void TheNoticeStrips_AndTheTrendNotes_RunThroughPlainText()
    {
        var util = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "util.js").ReplaceLineEndings("\n");
        var tabs = ServerTabs();

        /* W8: a payload's own note (aggregate_note, a truncation note) is written for an MCP caller. */
        Assert.Contains("[localizeIsoText(plainText(message))]", util, StringComparison.Ordinal);
        Assert.Contains("text: plainText(text)", util, StringComparison.Ordinal);
        Assert.DoesNotContain("class: \"mp-metric-note\", text: res.data.aggregate_note", tabs, StringComparison.Ordinal);
        Assert.DoesNotContain("class: \"mp-metric-note\", text: trend.data.aggregate_note", tabs, StringComparison.Ordinal);
    }
}
