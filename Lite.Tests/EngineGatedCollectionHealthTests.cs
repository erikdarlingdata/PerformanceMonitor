using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6912: on an Amazon RDS Web or Express server the Extended Events collectors stopped with #6893, and the SESSION_MISSING runs they logged
/// before that stayed inside the seven-day window and banded them FAILING in <c>get_collection_health</c>. A collector the engine does not run
/// reads <c>not_collected</c> whatever it logged.
/// </summary>
public sealed class EngineGatedCollectionHealthTests
{
    private static CollectorHealthRow Row(string collector, bool failing)
    {
        var last = DateTime.UtcNow.AddMinutes(-1);
        return new CollectorHealthRow
        {
            CollectorName = collector,
            TotalRuns = 315,
            SuccessCount = failing ? 0 : 315,
            ErrorCount = failing ? 315 : 0,
            LastSuccessTime = failing ? null : last,
            LastRunTime = last,
        };
    }

    [Fact]
    public void OnRdsWebOrExpress_ACollectorWithOldFailedRuns_ReadsNotCollected_AndTheOthersStay()
    {
        var attention = Row("attention", failing: true);
        var waits = Row("wait_stats", failing: false);
        Assert.Equal("FAILING", attention.HealthStatus);

        var (running, notCollected) = McpEngineCapability.SplitCollectionHealthByEngine(
            "RDS-WEB-1", 2, AwsRdsKind.RdsWebOrExpress, new() { attention, waits });

        Assert.Equal(new[] { "wait_stats" }, running.Select(r => r.CollectorName));
        var json = JsonSerializer.Serialize(notCollected);
        Assert.Contains("\"collector\":\"attention\"", json, StringComparison.Ordinal);
        Assert.Contains("\"status\":\"not_collected\"", json, StringComparison.Ordinal);
        Assert.Contains("Amazon RDS", json, StringComparison.Ordinal);
    }

    [Fact]
    public void OnAPlainServer_EveryLoggedRowStays()
    {
        var (running, notCollected) = McpEngineCapability.SplitCollectionHealthByEngine(
            "SQL-1", 2, AwsRdsKind.None, new() { Row("attention", failing: true), Row("wait_stats", failing: false) });

        Assert.Equal(2, running.Count);
        Assert.Empty(notCollected);
    }

    [Fact]
    public void TheCollectionHealthTab_ShowsAGatedCollectorAsNotCollected_WithTheEnginesSentence()
    {
        var attention = Row("attention", failing: true);
        var waits = Row("wait_stats", failing: false);
        var broken = Row("query_stats", failing: true);
        Assert.Equal("FAILING", attention.DisplayStatus);

        ServerTab.ApplyEngineGate(new() { attention, waits, broken }, "RDS-WEB-1", 2, AwsRdsKind.RdsWebOrExpress);

        Assert.Equal("not_collected", attention.DisplayStatus);
        Assert.Contains("Amazon RDS", attention.NoteFormatted, StringComparison.Ordinal);
        Assert.Equal("HEALTHY", waits.DisplayStatus);
        /* A collector the engine runs keeps its band, failing or not. */
        Assert.Equal("FAILING", broken.DisplayStatus);
    }

    [Fact]
    public void TheCollectionHealthTab_MakesNoClaimForAPlainServer_AndClearsAStaleMark()
    {
        var attention = Row("attention", failing: true);
        ServerTab.ApplyEngineGate(new() { attention }, "RDS-WEB-1", 2, AwsRdsKind.RdsWebOrExpress);
        Assert.Equal("not_collected", attention.DisplayStatus);

        ServerTab.ApplyEngineGate(new() { attention }, "SQL-1", 2, AwsRdsKind.None);
        Assert.Equal("FAILING", attention.DisplayStatus);
    }
}
