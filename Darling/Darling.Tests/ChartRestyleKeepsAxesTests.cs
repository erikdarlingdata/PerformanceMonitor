using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9109: a theme change restyles the charts of an open tab (#8898) and changes colours only. The theme sets the axis margins, and ScottPlot
/// refits every axis when a margin is set, so the restyle used to put each plot back on a fit of its data and drop the limits the load had
/// set (a y axis fixed at 0 to 105, a time axis edge to edge over the selected range) until the next refresh.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ChartRestyleKeepsAxesTests
{
    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{name}Theme.xaml");

    private static void Themed(Action body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            var previous = ThemeManager.CurrentTheme;
            ThemeManager.ThemeUriProvider = ThemeUri;
            try
            {
                ThemeManager.Apply("Dark");
                body();
            }
            finally
            {
                ThemeManager.Apply(previous);
                app.Resources.MergedDictionaries.Clear();
                foreach (var dictionary in before)
                {
                    app.Resources.MergedDictionaries.Add(dictionary);
                }

                ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
            }
        });

    /// <summary>A plot as a load leaves it: data well inside the limits it then fixes, a time axis and a count axis.</summary>
    private static WpfPlot LoadedChart()
    {
        var chart = new WpfPlot();
        ChartStyle.ApplyThemeToChart(chart);
        chart.Plot.Add.Scatter(new double[] { 1, 2, 3 }, new double[] { 10, 20, 67 });
        chart.Plot.Axes.DateTimeTicksBottom();
        ChartStyle.UseCountAxis(chart);
        chart.Plot.Axes.SetLimits(0, 100, 0, 105);
        return chart;
    }

    private static (double, double, double, double, object, object) Axes(WpfPlot chart) =>
        (chart.Plot.Axes.Bottom.Min, chart.Plot.Axes.Bottom.Max, chart.Plot.Axes.Left.Min, chart.Plot.Axes.Left.Max,
            chart.Plot.Axes.Left.TickGenerator, chart.Plot.Axes.Bottom.TickGenerator);

    [Fact]
    public void ARestyle_ChangesColoursOnly_AndKeepsTheLimitsAndTicksTheLoadSet() =>
        Themed(() =>
        {
            var chart = LoadedChart();
            var before = Axes(chart);
            var darkFigure = chart.Plot.FigureBackground.Color.ToHex();

            ThemeManager.Apply("Light");
            ChartStyle.Restyle(chart);

            Assert.NotEqual(darkFigure, chart.Plot.FigureBackground.Color.ToHex());
            Assert.Equal(before, Axes(chart));
        });

    [Fact]
    public void TheMinimalThemeTheHistoryWindowsRestyleWith_KeepsTheLimitsAndTicks() =>
        Themed(() =>
        {
            var chart = LoadedChart();
            var before = Axes(chart);

            ThemeManager.Apply("Light");
            ChartStyle.ApplyMinimalChartTheme(chart);

            Assert.Equal(before, Axes(chart));
        });

    [Fact]
    public void ARestyle_KeepsAZoomTheOperatorSetAfterTheLoad() =>
        Themed(() =>
        {
            var chart = LoadedChart();
            chart.Plot.Axes.SetLimits(10, 40, 5, 50);
            var before = Axes(chart);

            ChartStyle.Restyle(chart);

            Assert.Equal(before, Axes(chart));
        });

    [Fact]
    public void EveryChartOnAnOpenServerTab_KeepsItsAxes_WhenTheThemeChanges() =>
        Themed(() =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "restyle-axes-test", "restyle-axes-test", true, null));
            var window = new Window { Content = tab, Width = 1280, Height = 800, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var charts = ChartLayoutProbe.Logical<WpfPlot>(tab).ToList();
                Assert.True(charts.Count > 30, $"expected the tab's whole chart set, found {charts.Count}");
                foreach (var chart in charts)
                {
                    chart.Plot.Add.Scatter(new double[] { 1, 2, 3 }, new double[] { 10, 20, 67 });
                    chart.Plot.Axes.SetLimits(0, 100, 0, 105);
                }

                var before = charts.Select(Axes).ToList();
                var darkFigure = charts[0].Plot.FigureBackground.Color.ToHex();

                ThemeManager.Apply("Light");

                Assert.NotEqual(darkFigure, charts[0].Plot.FigureBackground.Color.ToHex());
                var moved = charts.Where((c, i) => !Equals(before[i], Axes(c))).Select(c => string.IsNullOrEmpty(c.Name) ? "(unnamed chart)" : c.Name).ToList();
                Assert.True(moved.Count == 0, $"{moved.Count} charts changed their axes on a theme change: {string.Join(", ", moved)}");
            }
            finally
            {
                window.Close();
                tab.Dispose();
            }
        });
}
