using System;
using System.IO;
using System.Linq;
using Darling.Tests;
using Lite.Tests;
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

    /* ── the other ten user writes (#5650): same wording as Dismiss, naming the action ── */

    [Fact]
    public void Text_NamesTheAction_SaysNothingChanged_AndLeaksNoInternalSymbol()
    {
        var text = StoreBusyReport.Text(Timeout(
            new StoreLockHolder("LocalDataService.OpenConnectionAsync", Write: false, TimeSpan.FromSeconds(31))), "renaming the tag");

        Assert.Contains("busy while renaming the tag", text, StringComparison.Ordinal);
        Assert.Contains("nothing was changed", text, StringComparison.Ordinal);
        Assert.Contains("try again in a few minutes", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("dismiss", text, StringComparison.OrdinalIgnoreCase);
        /* The requester ("OpenWriteConnectionAsync") and the raw "Could not acquire ... write lock" text are what the
           old status bar line printed; only the holder is named. */
        Assert.DoesNotContain("OpenWriteConnectionAsync", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Could not acquire", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LocalDataService.OpenConnectionAsync", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Text_WithNoKnownHolder_BlamesNothing_AndAnOutcomeCanReplaceNothingWasChanged()
    {
        var plain = StoreBusyReport.Text(Timeout(), "saving the overrides");
        Assert.DoesNotContain("in use by", plain, StringComparison.Ordinal);
        Assert.Contains("nothing was changed", plain, StringComparison.Ordinal);

        var partial = StoreBusyReport.Text(Timeout(), "saving the overrides", "1 of 3 change(s) were saved and the rest were not");
        Assert.DoesNotContain("nothing was changed", partial, StringComparison.Ordinal);
        Assert.Contains("1 of 3 change(s) were saved", partial, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitingText_SaysThePatienceTheUserWriteBudgetBuys()
    {
        var text = StoreBusyReport.WaitingText("renaming the tag");

        Assert.StartsWith("Renaming the tag", text, StringComparison.Ordinal);
        Assert.Contains($"{(int)LocalDataService.UserWriteLockBudget.TotalSeconds} seconds", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every window or main-window file that starts one of the ten non-Dismiss user writes catches a store-lock timeout
    /// and reports it through <see cref="StoreBusyReport"/>, which also writes the Warning line. Without this, a new
    /// surface (or a rewritten catch) would go back to printing <c>ex.Message</c>, the raw "Could not acquire ... write
    /// lock for LocalDataService.OpenUserWriteConnectionAsync" text.
    /// </summary>
    [Fact]
    public void EveryUserWriteSurface_ReportsALockTimeoutThroughTheSharedReport()
    {
        var writes = new[]
        {
            "CreateServerTagAsync", "RenameServerTagAsync", "SetServerTagColorAsync", "ReparentServerTagAsync",
            "DeleteServerTagAsync", "AssignServerTagAsync", "UnassignServerTagAsync", "ClearServerTagsForServerAsync",
            "SetDatabaseStateExpectedAsync", "ResetDatabaseStateExpectedToCurrentAsync",
        };

        /* file -> how many timeout catches it must carry (one per distinct handler shape, not per call). */
        var expected = new (string File, int Catches)[]
        {
            ("Lite/MainWindow.SidebarTree.cs", 1),
            ("Lite/MainWindow.xaml.cs", 1),
            ("Lite/Windows/ManageTagsWindow.xaml.cs", 1),
            ("Lite/Windows/DatabaseStateOverridesWindow.xaml.cs", 2),
        };

        var root = ParitySource.RepoRoot();
        var lite = Path.Combine(root, "Lite");
        var callers = Directory.EnumerateFiles(Path.Combine(lite, "Controls"), "*.cs", SearchOption.AllDirectories)
            .Concat(Directory.EnumerateFiles(Path.Combine(lite, "Windows"), "*.cs", SearchOption.AllDirectories))
            .Concat(Directory.EnumerateFiles(lite, "MainWindow*.cs", SearchOption.TopDirectoryOnly))
            .Where(p => !p.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                     && !p.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            .Where(p => writes.Any(w => CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(p)).Contains(w + "(", StringComparison.Ordinal)))
            .Select(p => Path.GetRelativePath(root, p).Replace('\\', '/'))
            .OrderBy(p => p, StringComparer.Ordinal)
            .ToArray();

        /* The set of surfaces is itself pinned, so a new one has to be added here and given a catch. MainWindow.SidebarTree.cs
           and MainWindow.xaml.cs both qualify; the rest are windows. */
        Assert.Equal(expected.Select(e => e.File).OrderBy(f => f, StringComparer.Ordinal).ToArray(), callers);

        foreach (var (file, catches) in expected)
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(Path.Combine(root, file)));
            Assert.True(CountOf(code, "catch (StoreLockTimeoutException") == catches, $"{file}: expected {catches} timeout catch(es)");
            Assert.True(CountOf(code, "StoreBusyReport.Report(") == catches, $"{file}: expected {catches} StoreBusyReport.Report call(s)");
            Assert.True(CountOf(code, "StoreBusyReport.WaitingText(") >= 1, $"{file}: no waiting text for a write that can wait 30 s");
        }
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
