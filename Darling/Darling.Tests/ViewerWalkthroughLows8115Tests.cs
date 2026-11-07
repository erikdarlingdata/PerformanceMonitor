using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8115: the low-severity details from the Viewer walkthrough. Each text or decision is pulled into a plain member and
/// asserted here; the members the fix adds are reached by name, so this class builds against a tree without them and fails
/// at the lookup with the member's name.
/// </summary>
public sealed class ViewerWalkthroughLows8115Tests
{
    private static readonly Assembly ViewerAssembly = typeof(FleetRollup).Assembly;

    private static Type ViewerType(string name)
    {
        var type = ViewerAssembly.GetType("PerformanceMonitor.Darling.Viewer." + name);
        Assert.True(type is not null, $"The Viewer has no {name} type.");
        return type!;
    }

    private static MethodInfo Method(Type type, string name, params Type[] args)
    {
        var method = type.GetMethod(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance, args);
        Assert.True(method is not null, $"{type.Name} has no {name}({string.Join(", ", args.Select(a => a.Name))}).");
        return method!;
    }

    private static string Constant(Type type, string name)
    {
        var field = type.GetField(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(field is not null, $"{type.Name} has no {name} constant.");
        return (string)field!.GetRawConstantValue()!;
    }

    // ── F2: the attention counts say how many of their servers are Healthy with an advisory ──

    [Theory]
    [InlineData(12, 12, 3, "showing 12 of 12 (3 healthy with advisories)")]
    [InlineData(5, 9, 1, "showing 5 of 9 (1 healthy with an advisory)")]
    [InlineData(4, 4, 0, "showing 4 of 4")]
    public void AttentionFilterCount_SaysHowManyShownAreHealthyWithAdvisories(int shown, int total, int advisories, string expected)
    {
        var method = Method(typeof(FleetRollup), nameof(FleetRollup.AttentionFilterCountText), typeof(int), typeof(int), typeof(int));
        Assert.Equal(expected, (string)method.Invoke(null, new object[] { shown, total, advisories })!);
    }

    [Fact]
    public void MoreNeedAttention_SaysHowManyPastTheCapAreHealthyWithAdvisories()
    {
        var summaries = new List<ServerSummaryItem>();
        for (var i = 1; i <= 6; i++)
        {
            summaries.Add(new() { DisplayName = $"crit-{i}", ServerId = i, IsOnline = true, DeadlockCount = 30, DeadlockWindow = TimeSpan.FromHours(1) });
        }

        for (var i = 7; i <= 9; i++)
        {
            summaries.Add(new() { DisplayName = $"advisory-{i}", ServerId = i, IsOnline = true, DatabaseProtectionChipValue = "1 overdue", DatabaseProtectionOverdueCount = 1 });
        }

        var rollup = FleetRollup.Build(summaries, new FleetTotals(), worstCount: 5);

        Assert.Equal(6, rollup.CriticalCount);
        Assert.Equal(3, rollup.HealthyCount);
        Assert.Equal(4, rollup.AdditionalProblemCount);
        Assert.Equal("+4 more need attention (3 healthy with advisories)", rollup.AdditionalProblemText);
    }

    [Fact]
    public void MoreNeedAttention_WithNoAdvisoryServersPastTheCap_IsTheCountAlone()
    {
        var summaries = new List<ServerSummaryItem>();
        for (var i = 1; i <= 7; i++)
        {
            summaries.Add(new() { DisplayName = $"crit-{i}", ServerId = i, IsOnline = true, DeadlockCount = 30, DeadlockWindow = TimeSpan.FromHours(1) });
        }

        var rollup = FleetRollup.Build(summaries, new FleetTotals(), worstCount: 5);
        Assert.Equal("+2 more need attention", rollup.AdditionalProblemText);
    }

    [Fact]
    public void OverviewCountText_CountsTheHealthyAdvisoryCardsTheFilterKeeps()
    {
        var cards = new List<ServerSummaryItem>
        {
            new() { DisplayName = "crit", ServerId = 1, IsOnline = true, DeadlockCount = 30, DeadlockWindow = TimeSpan.FromHours(1) },
            new() { DisplayName = "advisory", ServerId = 2, IsOnline = true, DatabaseProtectionChipValue = "1 overdue", DatabaseProtectionOverdueCount = 1 },
            new() { DisplayName = "healthy", ServerId = 3, IsOnline = true },
        };

        Assert.Equal("showing 2 of 3 (1 healthy with an advisory)", OverviewCardView.CountText(cards, shown: 2, attentionOnly: true, search: null));
    }

    // ── F3: the Server column sizes to its names in Alert History and Job History ──

    [Theory]
    [InlineData("AlertsHistoryTab.xaml")]
    [InlineData("JobHistoryTab.xaml")]
    public void ServerColumn_SizesToItsNames(string file)
    {
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);
        var column = Regex.Match(xaml, "<DataGridTextColumn Binding=\"\\{Binding ServerName\\}\"[^>]*>");
        Assert.True(column.Success, $"{file} has no ServerName column.");
        Assert.Contains("Width=\"Auto\"", column.Value, StringComparison.Ordinal);
        Assert.Matches("MinWidth=\"1[0-9]{2}\"", column.Value);
    }

    // ── F7: a database whose used size was not collected says so ──

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
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml");
        Assert.Contains("Text=\"{Binding SizeLabel}\"", xaml, StringComparison.Ordinal);
        Assert.DoesNotContain("StringFormat=\"{}{0:N0} / {1:N0} MB\"", xaml, StringComparison.Ordinal);
    }

    // ── F8(b): the FinOps Recommendations count says it is loading until the read returns ──

    [Fact]
    public void RecommendationsCount_HasALoadingStateAndTheSameCountsAsBefore()
    {
        var type = ViewerType("FinOpsRecommendationsCountText");
        Assert.Equal("Loading recommendations…", Constant(type, "Loading"));
        var count = Method(type, "For", typeof(int));
        Assert.Equal("no recommendations", (string)count.Invoke(null, new object[] { 0 })!);
        Assert.Equal("3 recommendation(s)", (string)count.Invoke(null, new object[] { 3 })!);
    }

    // ── F9: a toast names the server once ──

    [Theory]
    [InlineData("SERVER-A", "", "SERVER-A: Client timeouts over the last 4 minutes are back below threshold", "SERVER-A: Client timeouts over the last 4 minutes are back below threshold")]
    [InlineData("Payroll", "SERVER-A", "SERVER-A: CPU is back below threshold", "SERVER-A: CPU is back below threshold")]
    [InlineData("SERVER-A", "", "CPU 97% for 5 minutes", "SERVER-A: CPU 97% for 5 minutes")]
    [InlineData("SERVER-A", "", "SERVER-AB: CPU 97%", "SERVER-A: SERVER-AB: CPU 97%")]
    public void ToastBody_PrefixesTheServerOnlyWhenTheBodyDoesNotOpenWithIt(string server, string stored, string body, string expected)
    {
        var method = Method(ViewerType("ViewerToastText"), "WithServer", typeof(string), typeof(string), typeof(string));
        Assert.Equal(expected, (string)method.Invoke(null, new object[] { server, stored, body })!);
    }

    // ── F13: the Settings dialog opens wide enough for its eight tab headers on one row ──

    [Fact]
    public void SettingsWindow_OpensWideEnoughForOneRowOfTabHeaders()
    {
        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml");
        var width = int.Parse(Regex.Match(xaml, "\\sWidth=\"(\\d+)\"").Groups[1].Value);
        var minWidth = int.Parse(Regex.Match(xaml, "MinWidth=\"(\\d+)\"").Groups[1].Value);
        Assert.True(width >= 860, $"Settings opens at {width} px, where the eighth tab header wraps under the other seven.");
        Assert.Equal(width, minWidth);
    }

    // ── F14: a read-only seat's fields refuse edits ──

    [Fact]
    public void ReadOnlyGate_MakesTextReadOnlyAndDisablesChoices_LeavingButtonsAlone()
    {
        var gate = Method(ViewerType("ReadOnlySettingsGate"), "Apply", typeof(System.Windows.DependencyObject), typeof(string));

        /* WPF controls are built on the suite's one shared STA thread (#5602). */
        StaTestThread.Run(() =>
        {
            var text = new System.Windows.Controls.TextBox { Text = "80" };
            var check = new System.Windows.Controls.CheckBox();
            var combo = new System.Windows.Controls.ComboBox();
            var button = new System.Windows.Controls.Button { Content = "Manage People..." };
            var inner = new System.Windows.Controls.StackPanel();
            inner.Children.Add(combo);
            inner.Children.Add(button);
            var root = new System.Windows.Controls.Grid();
            root.Children.Add(text);
            root.Children.Add(check);
            root.Children.Add(inner);

            var gated = (int)gate.Invoke(null, new object[] { root, "shown for reference" })!;

            Assert.Equal(3, gated);
            Assert.True(text.IsReadOnly);
            Assert.False(check.IsEnabled);
            Assert.False(combo.IsEnabled);
            Assert.True(button.IsEnabled);
            Assert.Equal("shown for reference", text.ToolTip);
        });
    }

    // ── F15: the People window's empty state on a read-only seat gives no step the seat cannot take ──

    [Fact]
    public void PeopleEmptyNote_OnAReadOnlySeat_DoesNotSayAddAPerson()
    {
        var note = Method(typeof(PeopleText), "EmptyNote", typeof(bool));
        var readOnly = (string)note.Invoke(null, new object[] { true })!;
        var writable = (string)note.Invoke(null, new object[] { false })!;

        Assert.StartsWith("No people yet.", readOnly, StringComparison.Ordinal);
        Assert.DoesNotContain("Add a person", readOnly, StringComparison.Ordinal);
        Assert.Contains("read-write seat", readOnly, StringComparison.Ordinal);
        Assert.Equal(PeopleText.NoPeopleNote, writable);
    }

    // ── F16: Query Tag Usage on a store with no tags is one sentence ──

    [Fact]
    public void QueryTagUsage_WithNoTags_IsOneSentenceAndNoTables()
    {
        var answer = Method(ViewerType("QueryTagUsageText"), "NoTagsAnswer", typeof(int));
        var none = (string?)answer.Invoke(null, new object[] { 0 });
        Assert.NotNull(none);
        Assert.StartsWith("No query tags are defined", none, StringComparison.Ordinal);
        Assert.Contains("untagged", none, StringComparison.Ordinal);
        Assert.Null(answer.Invoke(null, new object[] { 2 }));
    }
}
