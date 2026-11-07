using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8115: the low-severity details from the Viewer walkthrough. Each text or decision is a plain member asserted here, and the
/// two layout details are asserted on the real controls, built and shown on the shared STA thread with the Viewer's theme.
/// The members the fix adds are reached by name, so this class builds against a tree without them and fails at the lookup
/// with the member's name.
/// </summary>
public sealed class ViewerWalkthroughLows8115Tests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";
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

    // ── F3: the Server column shows a long name whole in Alert History and Job History ──

    private sealed class ServerRow
    {
        public string ServerName { get; set; } = "";
    }

    [Theory]
    [InlineData("alerts")]
    [InlineData("jobs")]
    public void ServerColumn_ShowsALongNameWhole(string tab)
    {
        const string name = "SERVER-GROUP-REGION-PRODUCTION-CLUSTER-PRIMARY-REPLICA";
        var (columnWidth, natural) = StaTestThread.Run(() => WithTheme(() =>
        {
            FrameworkElement control = tab == "alerts" ? new AlertsHistoryTab() : new JobHistoryTab();
            var grid = (DataGrid)control.FindName(tab == "alerts" ? "AlertsDataGrid" : "JobHistoryDataGrid")!;
            grid.ItemsSource = new List<ServerRow> { new() { ServerName = name }, new() { ServerName = "SHORT" } };
            var window = new Window { Content = control, Width = 1640, Height = 800, Left = 0, Top = 0 };
            window.Show();
            try
            {
                Settle(window);
                var column = grid.Columns.OfType<DataGridTextColumn>()
                    .Single(c => (c.Binding as System.Windows.Data.Binding)?.Path.Path == "ServerName");
                var probe = new TextBlock { Text = name };
                probe.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
                return (column.ActualWidth, probe.DesiredSize.Width);
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(columnWidth + 0.5 >= natural, $"the Server column is {columnWidth:F0} px wide and the name needs {natural:F0} px, so it is cut");
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

    // ── F13: the Settings dialog's eight tab headers sit on one row at its default size ──

    [Fact]
    public void SettingsWindow_TabHeadersSitOnOneRow_AtTheDefaultSize()
    {
        var rows = StaTestThread.Run(() => WithTheme(() =>
        {
            var store = new ViewerAppSettingsStore(Path.Combine(Path.GetTempPath(), "settings-tabs-8115-" + Guid.NewGuid().ToString("N") + ".json"));
            var window = new SettingsWindow(new ViewerPreferences(), store, null) { Left = 0, Top = 0 };
            window.Show();
            try
            {
                Settle(window);
                var tabs = window.SettingsTabs;
                return tabs.Items.OfType<TabItem>()
                    .Select(t => (Header: t.Header?.ToString() ?? "", Top: Math.Round(t.TransformToAncestor(tabs).Transform(new Point(0, 0)).Y)))
                    .ToList();
            }
            finally
            {
                /* Closing an unsaved Settings window re-applies the app's theme from its pack resources, which a test process does not carry; this window changed none. */
                typeof(SettingsWindow).GetField("_themeSaved", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(window, true);
                window.Close();
            }
        }));

        Assert.True(rows.Count >= 8, $"the dialog shows {rows.Count} tabs");
        var distinctRows = rows.Select(r => r.Top).Distinct().ToList();
        Assert.True(distinctRows.Count == 1,
            "the tab headers wrap onto more than one row at the dialog's default size: " + string.Join(", ", rows.Select(r => $"{r.Header} at {r.Top}")));
    }

    // ── F14: a read-only seat's fields refuse edits ──

    [Fact]
    public void ReadOnlyGate_MakesTextReadOnlyAndDisablesChoices_LeavingButtonsAlone()
    {
        var gate = Method(ViewerType("ReadOnlySettingsGate"), "Apply", typeof(DependencyObject), typeof(string));

        /* WPF controls are built on the suite's one shared STA thread (#5602). */
        StaTestThread.Run(() =>
        {
            var text = new TextBox { Text = "80" };
            var check = new CheckBox();
            var combo = new ComboBox();
            var button = new Button { Content = "Manage People..." };
            var inner = new StackPanel();
            inner.Children.Add(combo);
            inner.Children.Add(button);
            var root = new Grid();
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

    /// <summary>Lets the shown window measure, arrange and generate its rows before anything is read off it.</summary>
    private static void Settle(Window window)
    {
        window.UpdateLayout();
        window.Dispatcher.Invoke(() => { }, DispatcherPriority.Loaded);
        window.Dispatcher.Invoke(() => { }, DispatcherPriority.Render);
        window.UpdateLayout();
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("ForegroundBrush"))
        {
            added = new ResourceDictionary { Source = new Uri(ThemeUri) };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}
