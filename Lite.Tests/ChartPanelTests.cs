/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8144: the card every chart sits in. The control is built on the shared STA thread, themed as Lite themes it, shown in a window,
/// and read back from what it drew: the title is on screen, the plot gets the card's remaining room, an empty card keeps its title
/// and shows one sentence in place of the plot and its axes, and an empty subtitle takes no room.
/// </summary>
[Collection("credential-manager")]
public sealed class ChartPanelTests
{
    private static readonly Uri LiteTheme = new("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml");
    private static readonly Uri Shared = new("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml");

    private static T Shown<T>(Func<ChartPanel> make, Func<ChartPanel, T> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var added = new List<ResourceDictionary>
            {
                new() { Source = LiteTheme },
                new() { Source = Shared },
            };
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }

            try
            {
                var panel = make();
                var window = new Window { Content = panel, Width = 600, Height = 340, Left = 0, Top = 0 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    ClippedTextProbe.Settle(window);
                    return body(panel);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                foreach (var dictionary in added)
                {
                    app.Resources.MergedDictionaries.Remove(dictionary);
                }
            }
        });

    private static TextBlock Part(ChartPanel panel, string name) => (TextBlock)panel.Template.FindName(name, panel);

    [Fact]
    public void ATitledPanel_ShowsItsTitle_AndGivesThePlotTheRestOfTheCard()
    {
        var (titleShown, titleSize, plotHeight, plotWidth, panelHeight, panelWidth) = Shown(
            () => new ChartPanel { Title = "tempdb", Content = new WpfPlot() },
            panel =>
            {
                var title = Part(panel, "PART_Title");
                var plot = (WpfPlot)panel.Content;
                return (title.IsVisible && title.Text == "tempdb" && title.ActualWidth > 0, title.FontSize, plot.ActualHeight, plot.ActualWidth, panel.ActualHeight, panel.ActualWidth);
            });

        Assert.True(titleShown, "the title is not on screen");
        Assert.Equal(14, titleSize);
        /* 12 px padding and a 1 px border on each side. */
        Assert.Equal(panelWidth - 26, plotWidth, 1);
        /* The same on top and bottom, and one title line above the plot: the plot keeps the rest. */
        Assert.True(plotHeight >= panelHeight * 0.75, $"the plot is {plotHeight:0.#} px tall in a {panelHeight:0.#} px card");
    }

    [Fact]
    public void AnEmptyPanel_KeepsItsTitle_AndShowsOneSentenceInPlaceOfThePlot()
    {
        var (titleShown, sentence, sentenceShown, plotShown, sentenceCentered) = Shown(
            () => new ChartPanel { Title = "tempdb", Content = new WpfPlot(), IsEmpty = true, EmptyText = "Nothing collected in this window." },
            panel =>
            {
                var title = Part(panel, "PART_Title");
                var empty = Part(panel, "PART_Empty");
                var plot = (WpfPlot)panel.Content;
                var at = empty.TransformToAncestor(panel).TransformBounds(new Rect(empty.RenderSize));
                return (title.IsVisible, empty.Text, empty.IsVisible, plot.IsVisible, Math.Abs(at.Left + at.Width / 2 - panel.ActualWidth / 2) < 2);
            });

        Assert.True(titleShown, "an empty card lost its title");
        Assert.Equal("Nothing collected in this window.", sentence);
        Assert.True(sentenceShown, "the sentence is not shown");
        Assert.False(plotShown, "the plot, and with it the axes, is still drawn on an empty card");
        Assert.True(sentenceCentered, "the sentence is not centered");
    }

    [Fact]
    public void ACardThatIsNotEmpty_HidesTheSentence()
    {
        var (sentenceShown, plotShown) = Shown(
            () => new ChartPanel { Title = "tempdb", Content = new WpfPlot(), EmptyText = "Nothing collected in this window." },
            panel => (Part(panel, "PART_Empty").IsVisible, ((WpfPlot)panel.Content).IsVisible));

        Assert.False(sentenceShown);
        Assert.True(plotShown);
    }

    [Fact]
    public void TheSubtitle_TakesNoRoomWhenEmpty_AndShowsWhenSet()
    {
        var (emptyShown, emptyHeight) = Shown(
            () => new ChartPanel { Title = "tempdb", Content = new Border { MinHeight = 40 } },
            panel =>
            {
                var subtitle = Part(panel, "PART_Subtitle");
                return (subtitle.IsVisible, subtitle.ActualHeight);
            });
        var (setShown, setText, setSize) = Shown(
            () => new ChartPanel { Title = "tempdb", Subtitle = "last 6 hours", Content = new Border { MinHeight = 40 } },
            panel =>
            {
                var subtitle = Part(panel, "PART_Subtitle");
                return (subtitle.IsVisible, subtitle.Text, subtitle.FontSize);
            });

        Assert.False(emptyShown);
        Assert.Equal(0, emptyHeight);
        Assert.True(setShown);
        Assert.Equal("last 6 hours", setText);
        Assert.Equal(11, setSize);
    }

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void EveryTextStyle_ResolvesAfterThemeManagerAppliesTheTheme_AndKeepsItsBrushInTheTheme(string theme)
    {
        var failures = StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var before = app.Resources.MergedDictionaries.ToList();
            var previous = ThemeManager.CurrentTheme;
            ThemeManager.ThemeUriProvider = name => new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{name}Theme.xaml");
            try
            {
                /* A switch from another theme first: the shared styles must survive it, not just the first apply. */
                ThemeManager.Apply(theme == "Dark" ? "Light" : "Dark");
                ThemeManager.Apply(theme);
                return Unresolved(app);
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

        Assert.True(failures.Count == 0, $"{theme}:{Environment.NewLine}" + string.Join(Environment.NewLine, failures));
    }

    private static List<string> Unresolved(Application app)
    {
        var failures = new List<string>();
        foreach (var (key, size, brush) in new[]
        {
            ("PageTitleText", 18.0, "ForegroundBrush"),
            ("PanelTitleText", 14.0, "ForegroundBrush"),
            ("PanelSubtitleText", 11.0, "ForegroundMutedBrush"),
            ("KpiValueText", 22.0, "ForegroundBrush"),
            ("EmptyHint", 0.0, "ForegroundMutedBrush"),
        })
        {
            if (app.TryFindResource(key) is not Style style)
            {
                failures.Add($"{key} does not resolve");
                continue;
            }

            var text = new TextBlock { Style = style };
            if (size > 0 && text.FontSize != size)
            {
                failures.Add($"{key} is {text.FontSize} px, expected {size}");
            }

            if (text.Foreground is not SolidColorBrush actual || app.TryFindResource(brush) is not SolidColorBrush expected || actual.Color != expected.Color)
            {
                failures.Add($"{key} does not carry {brush} from the theme");
            }
        }

        return failures;
    }
}
