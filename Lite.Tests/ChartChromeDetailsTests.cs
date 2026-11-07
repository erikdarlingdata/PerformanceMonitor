/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8154: four details of the shared chart chrome. A per-user color override (#3577) reaches the chart's figure, data area,
/// legend and hover popup and the in-grid bar's accent, because they read the theme dictionary rather than fixed hex. The hover
/// prints a small value at a precision its magnitude needs, so 0.04 ms is not "0.0 ms". Lite's axis labels and its hover share
/// one 24-hour clock, the Viewer's. And a grid cell bound to a <see cref="DateTime"/> with a <c>StringFormat</c> exports what it
/// shows, while Copy Cell keeps the raw value.
/// </summary>
public sealed class ChartChromeDetailsTests
{
    private const string Page = "#0A0B0C";
    private const string Raised = "#102030";
    private const string Well = "#0C1418";
    private const string Ink = "#FAFBFC";
    private const string Edge = "#405060";
    private const string Accent = "#ABCDEF";

    private static Color C(string hex) => (Color)ColorConverter.ConvertFromString(hex);

    private static string Hex(Color c) => $"#{c.R:X2}{c.G:X2}{c.B:X2}";

    private static string Hex(ScottPlot.Color c) => $"#{c.R:X2}{c.G:X2}{c.B:X2}";

    private static string Hex(Brush brush) => Hex(Assert.IsType<SolidColorBrush>(brush).Color);

    /// <summary>A regenerated theme dictionary with the operator's overrides, as <c>ThemeManager.Apply</c> installs one.</summary>
    private static ResourceDictionary Overrides()
    {
        var d = new ResourceDictionary();
        foreach (var (key, hex) in new[]
        {
            ("BackgroundColor", Page), ("BackgroundLightColor", Raised), ("BackgroundDarkColor", Well),
            ("ForegroundColor", Ink), ("BorderColor", Edge), ("AccentColor", Accent),
        })
        {
            d[key] = C(hex);
            var brush = new SolidColorBrush(C(hex));
            brush.Freeze();
            d[key.Replace("Color", "Brush", StringComparison.Ordinal)] = brush;
        }

        return d;
    }

    private static T WithTheme<T>(Func<T> body, bool overrides)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? theme = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            theme = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(theme);
        }

        var over = overrides ? Overrides() : null;
        if (over is not null) app.Resources.MergedDictionaries.Add(over);
        try
        {
            return body();
        }
        finally
        {
            if (over is not null) app.Resources.MergedDictionaries.Remove(over);
            if (theme is not null) app.Resources.MergedDictionaries.Remove(theme);
        }
    }

    [Fact]
    public void TheChartChrome_TakesTheThemeDictionarysColors_SoAnOverrideReachesTheChart()
    {
        var colors = StaTestThread.Run(() => WithTheme(ChartStyle.GetThemeColors, overrides: true));

        Assert.Equal(Raised, Hex(colors.FigureBackground));
        Assert.Equal(Well, Hex(colors.DataBackground));
        Assert.Equal(Ink, Hex(colors.Text));
        Assert.Equal(Raised, Hex(colors.LegendBackground));
        Assert.Equal(Ink, Hex(colors.LegendForeground));
        Assert.Equal(Edge, Hex(colors.LegendOutline));
    }

    [Fact]
    public void TheChartChrome_WithTheStockTheme_IsTheStockDarkPalette()
    {
        var colors = StaTestThread.Run(() => WithTheme(ChartStyle.GetThemeColors, overrides: false));

        Assert.Equal("#22252B", Hex(colors.FigureBackground));
        Assert.Equal("#111217", Hex(colors.DataBackground));
        Assert.Equal("#E4E6EB", Hex(colors.Text));
        Assert.Equal("#2A2D35", Hex(colors.LegendOutline));
    }

    [Fact]
    public void TheHoverPopup_IsPaintedFromTheThemeDictionary()
    {
        var (background, border, ink) = StaTestThread.Run(() => WithTheme(() =>
        {
            var helper = new ChartHoverHelper(new WpfPlot(), "ms");
            var popup = (Popup)typeof(ChartHoverHelper).GetField("_popup", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(helper)!;
            var frame = Assert.IsType<Border>(popup.Child);
            var text = Assert.IsType<TextBlock>(frame.Child);
            return (Hex(frame.Background), Hex(frame.BorderBrush), Hex(text.Foreground));
        }, overrides: true));

        Assert.Equal(Raised, background);
        Assert.Equal(Edge, border);
        Assert.Equal(Ink, ink);
    }

    [Fact]
    public void TheActiveSortBar_TakesTheThemesAccent()
    {
        var (overridden, stock) = StaTestThread.Run(() =>
        {
            var overridden = WithTheme(() => Hex(new BarChartCell { IsActiveSort = true }.EffectiveBarBrush), overrides: true);
            var stock = WithTheme(() => Hex(new BarChartCell { IsActiveSort = true }.EffectiveBarBrush), overrides: false);
            return (overridden, stock);
        });

        Assert.Equal(Accent, overridden);
        Assert.Equal("#2EAEF1", stock);
    }

    [Theory]
    [InlineData(0.04, "0.04")]
    [InlineData(0.5, "0.5")]
    [InlineData(0.0456, "0.046")]
    [InlineData(1.234, "1.23")]
    [InlineData(12.34, "12.3")]
    [InlineData(1234, "1,234")]
    [InlineData(0, "0")]
    public void TheHover_PrintsAValueAtThePrecisionItsMagnitudeNeeds(double y, string expected)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("en-US");
        try
        {
            var text = StaTestThread.Run(() =>
            {
                var chart = new WpfPlot();
                var scatter = chart.Plot.Add.Scatter(new[] { 1.0, 2.0, 3.0 }, new[] { y, y, y });
                var helper = new ChartHoverHelper(chart, "ms");
                helper.Add(scatter, "reads");
                chart.Plot.GetImage(600, 400);
                return helper.HoverTextAt(chart.Plot.GetPixel(new ScottPlot.Coordinates(2.0, y)));
            });

            Assert.NotNull(text);
            Assert.Equal(expected + " ms", text!.Split('\n')[1]);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

    [Fact]
    public void LitesAxisLabels_UseTheHoversTwentyFourHourClock()
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = new CultureInfo("en-US");
        var original = UiTimeContext.ConvertForDisplay;
        try
        {
            UiTimeContext.ConvertForDisplay = static t => t;
            var plot = new ScottPlot.Plot();
            plot.Axes.DateTimeTicksBottomDateChange();
            var formatter = Assert.IsType<ScottPlot.TickGenerators.DateTimeAutomatic>(plot.Axes.Bottom.TickGenerator).LabelFormatter!;

            var first = new DateTime(2026, 1, 15, 15, 0, 0);
            var second = first.AddMinutes(30);
            var dated = formatter(first);
            var label = formatter(second);
            var hover = ChartHoverHelper.FormatHoverTime(second, null);

            Assert.Equal("15:30:00", hover);
            Assert.Equal("15:30", label);
            Assert.EndsWith("\n15:00", dated, StringComparison.Ordinal);
        }
        finally
        {
            UiTimeContext.ConvertForDisplay = original;
            CultureInfo.CurrentCulture = saved;
        }
    }

    private sealed class Stamped
    {
        public DateTime SeenAt { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class ShoutConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture) => ((DateTime)value).Year.ToString(CultureInfo.InvariantCulture) + "!";
        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) => throw new NotSupportedException();
    }

    [Fact]
    public void ADateTimeCellWithAStringFormat_ExportsWhatItShows_AndCopyCellStaysRaw()
    {
        var (csv, raw, shouted) = StaTestThread.Run(() =>
        {
            var row = new Stamped { SeenAt = new DateTime(2026, 1, 15, 14, 30, 0), Name = "a" };
            var grid = new DataGrid();
            grid.Columns.Add(new DataGridTextColumn { Header = "Name", Binding = new Binding(nameof(Stamped.Name)) });
            grid.Columns.Add(new DataGridTextColumn { Header = "Seen", Binding = new Binding(nameof(Stamped.SeenAt)) { StringFormat = "yyyy-MM-dd HH:mm" } });
            grid.Columns.Add(new DataGridTextColumn { Header = "Braced", Binding = new Binding(nameof(Stamped.SeenAt)) { StringFormat = "{0:yyyy-MM-dd HH:mm:ss}" } });
            grid.Columns.Add(new DataGridTextColumn { Header = "Year", Binding = new Binding(nameof(Stamped.SeenAt)) { Converter = new ShoutConverter() } });
            grid.ItemsSource = new[] { row };
            return (DataGridExport.BuildCsv(grid, ",").Replace("\r\n", "\n"),
                    DataGridExport.GetRawCellValue(grid.Columns[1], row),
                    DataGridExport.GetCellValue(grid.Columns[3], row));
        });

        Assert.Equal("a,2026-01-15 14:30,2026-01-15 14:30:00,2026!", csv.TrimEnd('\n').Split('\n')[1]);
        Assert.Equal("01/15/2026 14:30:00", raw);
        Assert.Equal("2026!", shouted);
    }

    [Fact]
    public void ANumberCellWithAStringFormat_StillExportsTheNumber()
    {
        var csv = StaTestThread.Run(() =>
        {
            var grid = new DataGrid();
            grid.Columns.Add(new DataGridTextColumn { Header = "Reads", Binding = new Binding("Value") { StringFormat = "{0:N0}" } });
            grid.ItemsSource = new[] { new { Value = 1234567.5 } };
            return DataGridExport.BuildCsv(grid, ",").Replace("\r\n", "\n");
        });

        Assert.Equal("1234567.5", csv.TrimEnd('\n').Split('\n')[1]);
    }
}
