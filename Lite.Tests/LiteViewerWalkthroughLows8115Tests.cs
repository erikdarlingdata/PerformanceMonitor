using System;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8115: Lite's twins of the Viewer walkthrough details that share code: the Server column that sizes to its names, the
/// database-size label that says when the used size was not collected, and the Query Tag Usage window's one-sentence
/// answer on a store with no tags. New members are reached by name so this class builds against a tree without them.
/// </summary>
public sealed class LiteViewerWalkthroughLows8115Tests
{
    [Theory]
    [InlineData("Lite/Controls/AlertsHistoryTab.xaml")]
    [InlineData("Lite/Controls/JobHistoryTab.xaml")]
    public void ServerColumn_SizesToItsNames(string file)
    {
        var xaml = ReadRepoFile(file);
        var column = Regex.Match(xaml, "<DataGridTextColumn Binding=\"\\{Binding ServerName\\}\"[^>]*>");
        Assert.True(column.Success, $"{file} has no ServerName column.");
        Assert.Contains("Width=\"Auto\"", column.Value, StringComparison.Ordinal);
        Assert.Matches("MinWidth=\"1[0-9]{2}\"", column.Value);
    }

    [Fact]
    public void DatabaseSizeLabel_SaysWhenTheUsedSizeWasNotCollected()
    {
        var label = typeof(DatabaseSizeSummaryRow).GetProperty("SizeLabel");
        Assert.True(label is not null, "DatabaseSizeSummaryRow has no SizeLabel property.");

        var unknown = new DatabaseSizeSummaryRow { DatabaseName = "model", TotalMb = 16m, UsedMb = null };
        var known = new DatabaseSizeSummaryRow { DatabaseName = "sales", TotalMb = 2048m, UsedMb = 1536m };

        var unknownLabel = (string)label!.GetValue(unknown)!;
        Assert.DoesNotMatch("^\\s*/", unknownLabel);
        Assert.Contains("not collected", unknownLabel, StringComparison.Ordinal);
        Assert.EndsWith("/ 16 MB", unknownLabel, StringComparison.Ordinal);
        Assert.Equal("1,536 / 2,048 MB", (string)label.GetValue(known)!);
    }

    [Fact]
    public void DatabaseSizeBar_BindsTheLabel()
    {
        var xaml = ReadRepoFile("Lite/Controls/FinOpsTab.xaml");
        Assert.Contains("Text=\"{Binding SizeLabel}\"", xaml, StringComparison.Ordinal);
        Assert.DoesNotContain("StringFormat=\"{}{0:N0} / {1:N0} MB\"", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryTagUsage_WithNoTags_HasItsOneSentence()
    {
        var field = typeof(PerformanceMonitorLite.Windows.QueryTagUsageWindow)
            .GetField("NoTagsMessage", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(field is not null, "QueryTagUsageWindow has no NoTagsMessage constant.");
        var message = (string)field!.GetRawConstantValue()!;
        Assert.StartsWith("No query tags are defined", message, StringComparison.Ordinal);
        Assert.Contains("untagged", message, StringComparison.Ordinal);
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
