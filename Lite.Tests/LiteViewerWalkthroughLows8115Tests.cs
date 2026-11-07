using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8115: Lite's twins of the Viewer walkthrough details that share code: the Server column that shows a long name whole, the
/// database-size label that says when the used size was not collected, and the Query Tag Usage window's one-sentence answer
/// on a store with no tags. New members are reached by name so this class builds against a tree without them; the grids are
/// the real controls, built and shown on the shared STA thread with Lite's theme.
/// </summary>
public sealed class LiteViewerWalkthroughLows8115Tests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml";

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
                window.UpdateLayout();
                window.Dispatcher.Invoke(() => { }, DispatcherPriority.Loaded);
                window.Dispatcher.Invoke(() => { }, DispatcherPriority.Render);
                window.UpdateLayout();
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
    public void QueryTagUsage_WithNoTags_HasItsOneSentence()
    {
        var field = typeof(PerformanceMonitorLite.Windows.QueryTagUsageWindow)
            .GetField("NoTagsMessage", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(field is not null, "QueryTagUsageWindow has no NoTagsMessage constant.");
        var message = (string)field!.GetRawConstantValue()!;
        Assert.StartsWith("No query tags are defined", message, StringComparison.Ordinal);
        Assert.Contains("untagged", message, StringComparison.Ordinal);
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
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
