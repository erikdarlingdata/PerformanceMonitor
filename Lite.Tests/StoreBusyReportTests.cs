using System;
using System.IO;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5650: the Dismiss dialog blamed "archival or maintenance" for every lock timeout, and the timeout reached no log.
/// The text now says what the code knows (the database was busy, nothing was dismissed, try again) and names the
/// holder only when the lock registry named one.
/// </summary>
public sealed class StoreBusyReportTests
{
    private static StoreLockTimeoutException Timeout(params StoreLockHolder[] holders)
        => new(TimeSpan.FromSeconds(5.2), holders, "LocalDataService.OpenWriteConnectionAsync");

    [Fact]
    public void Dialog_WithNoKnownHolder_BlamesNothing()
    {
        var text = StoreBusyReport.DialogText(Timeout());

        Assert.DoesNotContain("archival", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("maintenance", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("startup", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("busy", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("nothing was dismissed", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("try again in a few minutes", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Dialog_ForAnUntypedTimeout_BlamesNothing()
    {
        var text = StoreBusyReport.DialogText(new TimeoutException("boom"));

        Assert.DoesNotContain("archival", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("startup", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("nothing was dismissed", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Dialog_WhenTheStartupRepairHoldsTheLock_NamesTheStartupRepair()
    {
        var text = StoreBusyReport.DialogText(Timeout(
            new StoreLockHolder("QueryStoreSliceRepairService.SurveyAsync", Write: false, TimeSpan.FromSeconds(41))));

        Assert.Contains("startup Query Store repair", text, StringComparison.Ordinal);
        Assert.Contains("nothing was dismissed", text, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Dialog_WhenSomethingElseHoldsTheLock_DoesNotNameStartupMaintenance()
    {
        var text = StoreBusyReport.DialogText(Timeout(
            new StoreLockHolder("AnomalyDetector.DetectAsync", Write: false, TimeSpan.FromSeconds(9))));

        Assert.DoesNotContain("startup", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("archival", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("AnomalyDetector.DetectAsync", text, StringComparison.Ordinal);
    }

    [Fact]
    public void LogLine_CarriesTheActionTheWaitAndTheHolder()
    {
        var line = StoreBusyReport.LogLine("Dismiss all", Timeout(
            new StoreLockHolder("ArchiveService.ExportAsync", Write: false, TimeSpan.FromSeconds(41))));

        Assert.Contains("Dismiss all", line, StringComparison.Ordinal);
        Assert.Contains("5.2 s", line, StringComparison.Ordinal);
        Assert.Contains("ArchiveService.ExportAsync", line, StringComparison.Ordinal);
        Assert.Contains("41 s", line, StringComparison.Ordinal);
    }

    [Fact]
    public void LogLine_WithNoKnownHolder_SaysTheHolderIsUnknown()
    {
        var line = StoreBusyReport.LogLine("Dismiss all", Timeout());

        Assert.Contains("5.2 s", line, StringComparison.Ordinal);
        Assert.Contains("holder unknown", line, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BothDismissHandlers_UseTheReport_AndDoNotBlameArchival()
    {
        var root = AppContext.BaseDirectory;
        var dir = new DirectoryInfo(root);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Lite", "Controls", "AlertsHistoryTab.xaml.cs")))
        {
            dir = dir.Parent;
        }

        Assert.NotNull(dir);
        var source = File.ReadAllText(Path.Combine(dir!.FullName, "Lite", "Controls", "AlertsHistoryTab.xaml.cs"));

        Assert.DoesNotContain("archival or maintenance", source, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, CountOf(source, "StoreBusyReport.DialogText("));
        Assert.Equal(2, CountOf(source, "StoreBusyReport.LogLine("));
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }
}
