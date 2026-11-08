/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8225: Lite's empty, count and note text reads the way the refreshed views do. A count label on a tab's toolbar is the muted
/// subtitle under a card title, not blue bold italic; an advice sentence or a footnote is muted and upright; an empty grid says one
/// muted, upright sentence, centred in the grid, in the words the other empty grids use, wrapping to the grid's width and never
/// narrower; and the sentence sits inside its grid, not under it. Every fact builds the real tab on the shared STA thread with the
/// theme and the shared styles loaded, shows it at the width a tab has beside the sidebar of a 1280x800 and a 900x600 window, and
/// reads the realized font style, weight, brush and bounds of each element. With REFRESH_SHOTS_DIR set, each screen is saved as a PNG.
/// </summary>
[Collection("credential-manager")]
public sealed class LiteEmptyAndCountTextTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    private static readonly (int Width, int Height)[] Sizes = { (1280, 800), (900, 600) };

    /* The sidebar beside a tab in MainWindow.xaml: 280 px, or its 40 px rail below about 1100 DIPs (#8219). */
    private static int PaneWidth(int windowWidth) => windowWidth - (windowWidth < 1100 ? 40 : 280);

    /* The longest sentence an empty System Events grid says: wider than the 900x600 pane and narrower than the 1280x800 one. */
    private const string LongSentence = "No significant scheduler-monitor samples (SQL CPU, other-process CPU, or memory utilization outside the warning thresholds) in the selected time window.";

    private static readonly string[] ServerTabCounts =
    {
        "SchedulerIssuesCountIndicator", "SevereErrorsCountIndicator", "MemoryConditionsCountIndicator", "MemoryBrokerCountIndicator",
        "MemoryNodeOomCountIndicator", "SignificantWaitsCountIndicator", "CpuTasksCountIndicator", "IoIssuesCountIndicator",
        "DefaultTraceCountIndicator", "ServerConfigChangesCountIndicator", "DatabaseConfigChangesCountIndicator", "TraceFlagChangesCountIndicator",
    };

    private static readonly string[] FinOpsCounts =
    {
        "RecommendationsCountIndicator", "DbResourcesCountIndicator", "StorageGrowthCountIndicator", "IndexLockingCountIndicator", "DbSizeCountIndicator",
        "PvsCountIndicator", "IndexAnalysisStatusText", "IdleDatabasesCountIndicator", "ExpensiveQueriesCountIndicator", "HighImpactCountIndicator",
        "AppConnectionsCountIndicator", "ServerInventoryCountIndicator",
    };

    /* The Optimization sub-tab's grids and the sentence each shows when it is empty. */
    private static readonly (string Grid, string Message)[] OptimizationGrids =
    {
        ("IdleDatabasesDataGrid", "IdleDatabasesNoDataMessage"), ("TempdbPressureDataGrid", "TempdbPressureNoDataMessage"),
        ("WaitCategorySummaryDataGrid", "WaitCategorySummaryNoDataMessage"), ("ExpensiveQueriesDataGrid", "ExpensiveQueriesNoDataMessage"),
        ("MemoryGrantEfficiencyDataGrid", "MemoryGrantEfficiencyNoDataMessage"),
    };

    public static IEnumerable<object[]> Themes()
    {
        yield return new object[] { "DarkTheme" };
        yield return new object[] { "LightTheme" };
        yield return new object[] { "CoolBreezeTheme" };
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheServerTabsCountLabelsAdviceAndFootnote_AreMutedAndUpright_InsideTheTab(string theme)
    {
        var problems = WithServerTab(theme, (tab, window, muted) =>
        {
            var found = new List<string>();
            foreach (var (width, height) in Sizes)
            {
                Resize(window, PaneWidth(width), height);
                var at = $"{theme} {width}x{height}";

                foreach (var name in ServerTabCounts)
                {
                    var label = Find<TextBlock>(tab, name);
                    label.Text = name.Contains("Changes", StringComparison.Ordinal) ? "2 changes" : "3 events";
                    Reveal(label);
                    Settle(window);
                    found.AddRange(Muted(label, tab, muted, $"{at}: {name}"));
                }

                Reveal(Find<TextBlock>(tab, "MemoryBrokerCountIndicator"));
                Settle(window);
                Shoot(window, $"MemoryBroker-{theme}-{width}x{height}.png");

                var advice = Find<TextBlock>(tab, "PlanCacheRecommendationText");
                advice.Text = "Forced parameterization would let these single-use plans be reused.";
                Reveal(advice);
                Settle(window);
                found.AddRange(Muted(advice, tab, muted, $"{at}: PlanCacheRecommendationText"));
                Shoot(window, $"PlanCache-{theme}-{width}x{height}.png");

                var note = Find<TextBlock>(tab, "CurrentWaitsSparseNote");
                Reveal(note);
                Settle(window);
                found.AddRange(Muted(note, tab, muted, $"{at}: CurrentWaitsSparseNote"));
                Shoot(window, $"CurrentWaits-{theme}-{width}x{height}.png");

                var hint = tab.FindName("DailySummaryHintText") as TextBlock;
                if (hint is null)
                {
                    found.Add($"{at}: the Daily Summary hint (DailySummaryHintText) is not on the tab");
                }
                else
                {
                    Reveal(hint);
                    Settle(window);
                    found.AddRange(Muted(hint, tab, muted, $"{at}: DailySummaryHintText"));
                    Shoot(window, $"DailySummary-{theme}-{width}x{height}.png");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void AnEmptyGrid_SaysOneMutedUprightSentence_CentredInTheGrid_WrappingToItsWidth(string theme)
    {
        var problems = WithServerTab(theme, (tab, window, muted) =>
        {
            var found = new List<string>();
            var grid = Find<DataGrid>(tab, "PlanCorrectionGrid");
            Reveal(grid);
            foreach (var (width, height) in Sizes)
            {
                Resize(window, PaneWidth(width), height);
                var at = $"{theme} {width}x{height}: Plan Corrections";

                /* What the filter manager does when a read comes back with no rows. */
                EmptyState.Show(grid, true);
                Settle(window);
                var sentence = EmptyText(tab, grid);
                found.AddRange(Muted(sentence, tab, muted, at, weight: false));
                if (!sentence.Text.StartsWith("No ", StringComparison.Ordinal) || !sentence.Text.EndsWith(" in the selected time window.", StringComparison.Ordinal))
                {
                    found.Add($"{at}: the sentence is \"{sentence.Text}\", not the \"No … in the selected time window.\" the other empty grids say");
                }

                if (Lines(sentence) != 1)
                {
                    found.Add($"{at}: the sentence wraps to {Lines(sentence)} lines");
                }

                var gridBounds = Bounds(grid, tab);
                var bounds = Bounds(sentence, tab);
                if (Math.Abs((bounds.Left + bounds.Right) / 2 - (gridBounds.Left + gridBounds.Right) / 2) > 1)
                {
                    found.Add($"{at}: the sentence is centred at {(bounds.Left + bounds.Right) / 2:0}, the grid at {(gridBounds.Left + gridBounds.Right) / 2:0}");
                }

                if (bounds.Top < gridBounds.Top - 0.5 || bounds.Bottom > gridBounds.Bottom + 0.5)
                {
                    found.Add($"{at}: the sentence spans {bounds.Top:0}..{bounds.Bottom:0}, outside the grid's {gridBounds.Top:0}..{gridBounds.Bottom:0}");
                }

                Shoot(window, $"PlanCorrections-{theme}-{width}x{height}.png");

                /* A long sentence wraps at the grid's width, never at a narrower one. */
                EmptyState.Show(grid, true, LongSentence);
                Settle(window);
                var natural = NaturalWidth(sentence);
                var room = grid.ActualWidth - 16;
                if (natural <= room && Lines(sentence) != 1)
                {
                    found.Add($"{at}: a {natural:0} px sentence wraps to {Lines(sentence)} lines in a {grid.ActualWidth:0} px grid");
                }

                if (natural > room && sentence.ActualWidth < room - 1)
                {
                    found.Add($"{at}: a {natural:0} px sentence wraps at {sentence.ActualWidth:0} px in a {grid.ActualWidth:0} px grid");
                }

                EmptyState.Show(grid, true);
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheFinOpsCountLabelsHintAndEmptySentences_ReadTheRefreshedWay(string theme)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, muted =>
        {
            var tab = new FinOpsTab();
            var window = Open(tab, PaneWidth(1280), 800);
            try
            {
                var found = new List<string>();
                foreach (var (width, height) in Sizes)
                {
                    Resize(window, PaneWidth(width), height);
                    var at = $"{theme} {width}x{height}";

                    foreach (var name in FinOpsCounts)
                    {
                        var label = Find<TextBlock>(tab, name);
                        label.Text = name == "IndexAnalysisStatusText" ? "4 indexes found" : "3 databases";
                        Reveal(label);
                        /* Optimization scrolls as one page, so a count in a lower card is brought into view before it is measured. */
                        label.BringIntoView();
                        Settle(window);
                        found.AddRange(Muted(label, tab, muted, $"{at}: {name}"));
                    }

                    Reveal(Find<TextBlock>(tab, "RecommendationsCountIndicator"));
                    Settle(window);
                    Shoot(window, $"FinOpsRecommendations-{theme}-{width}x{height}.png");

                    /* Locking & Contention: the hint beside the database picker. */
                    var locking = Find<TextBlock>(tab, "IndexLockingCountIndicator");
                    Reveal(locking);
                    Settle(window);
                    var hint = ClippedTextProbe.Descendants(tab).OfType<TextBlock>().FirstOrDefault(t => t.Text.StartsWith("Cumulative since restart", StringComparison.Ordinal));
                    if (hint is null)
                    {
                        found.Add($"{at}: the Locking hint is not on the tab");
                    }
                    else
                    {
                        found.AddRange(Muted(hint, tab, muted, $"{at}: the Locking hint", weight: false));
                    }

                    Shoot(window, $"FinOpsLocking-{theme}-{width}x{height}.png");

                    /* Index Analysis: the input has a label, and the lower grid shows the one empty sentence. */
                    var input = Find<TextBox>(tab, "IndexAnalysisDatabaseInput");
                    Reveal(input);
                    Settle(window);
                    var inputBounds = Bounds(input, tab);
                    var inputLabel = ClippedTextProbe.Descendants(tab).OfType<TextBlock>().FirstOrDefault(t =>
                        t.IsVisible && t.Text.TrimEnd(':').Equals("Database", StringComparison.OrdinalIgnoreCase)
                        && Bounds(t, tab).Right <= inputBounds.Left + 0.5 && Bounds(t, tab).Bottom > inputBounds.Top && Bounds(t, tab).Top < inputBounds.Bottom);
                    if (inputLabel is null)
                    {
                        found.Add($"{at}: the Index Analysis database box has no label beside it");
                    }

                    var lower = Find<DataGrid>(tab, "IndexAnalysisDetailGrid");
                    var sentence = Find<TextBlock>(tab, "IndexAnalysisNoDataMessage");
                    found.AddRange(Muted(sentence, tab, muted, $"{at}: IndexAnalysisNoDataMessage", weight: false));
                    found.AddRange(Inside(sentence, lower, tab, $"{at}: IndexAnalysisNoDataMessage"));
                    Shoot(window, $"FinOpsIndexAnalysis-{theme}-{width}x{height}.png");

                    /* Optimization: each empty sentence sits inside its grid. */
                    foreach (var (gridName, messageName) in OptimizationGrids)
                    {
                        var grid = Find<DataGrid>(tab, gridName);
                        var message = Find<TextBlock>(tab, messageName);
                        Reveal(grid);
                        message.Visibility = Visibility.Visible;
                        Settle(window);
                        found.AddRange(Muted(message, tab, muted, $"{at}: {messageName}", weight: false, insideTab: false));
                        found.AddRange(Inside(message, grid, tab, $"{at}: {messageName}"));
                    }

                    Find<TextBlock>(tab, "IdleDatabasesCountIndicator").Text = "";
                    Find<TextBlock>(tab, "ExpensiveQueriesCountIndicator").Text = "";
                    Settle(window);
                    Shoot(window, $"FinOpsOptimization-{theme}-{width}x{height}.png");
                }

                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheHistoryCountLabelsAndTheRecommendationsNotes_AreMutedAndUpright(string theme)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, muted =>
        {
            var found = new List<string>();
            foreach (var (width, height) in Sizes)
            {
                var at = $"{theme} {width}x{height}";

                var jobs = new JobHistoryTab();
                jobs.JobCountIndicator.Text = "12 runs";
                found.AddRange(Shown(jobs, width, height, window => Muted(jobs.JobCountIndicator, jobs, muted, $"{at}: JobCountIndicator"), $"JobHistory-{theme}-{width}x{height}.png"));

                var alerts = new AlertsHistoryTab();
                alerts.AlertCountIndicator.Text = "7 alerts";
                found.AddRange(Shown(alerts, width, height, window => Muted(alerts.AlertCountIndicator, alerts, muted, $"{at}: AlertCountIndicator"), $"AlertHistory-{theme}-{width}x{height}.png"));

                var recommendations = new RecommendationsTab();
                var status = Find<TextBlock>(recommendations, "StatusText");
                status.Text = "3 findings";
                var replica = Find<TextBlock>(recommendations, "ReplicaNoteText");
                replica.Text = "This server holds a secondary copy of two databases; their findings are left to the primary.";
                replica.Visibility = Visibility.Visible;
                found.AddRange(Shown(recommendations, width, height, window =>
                    Muted(status, recommendations, muted, $"{at}: Recommendations StatusText", weight: false)
                        .Concat(Muted(replica, recommendations, muted, $"{at}: Recommendations ReplicaNoteText", weight: false)).ToList(),
                    $"Recommendations-{theme}-{width}x{height}.png"));
            }

            return found;
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [MemberData(nameof(Themes))]
    public void TheSlicersEmptyText_IsMutedAndUpright_CentredInTheSlicer(string theme)
    {
        var problems = StaTestThread.Run(() => WithTheme(theme, muted =>
        {
            var found = new List<string>();
            foreach (var (width, height) in Sizes)
            {
                var at = $"{theme} {width}x{height}: the slicer's empty text";
                /* A slicer with no message host draws the sentence itself, on its own canvas. */
                var slicer = new TimeRangeSlicerControl { VerticalAlignment = VerticalAlignment.Top };
                var host = new Grid();
                host.Children.Add(slicer);
                var window = Open(host, PaneWidth(width), height);
                try
                {
                    slicer.ShowEmpty("No active queries in the selected time window.");
                    Settle(window);
                    var text = Find<Canvas>(slicer, "SlicerCanvas").Children.OfType<TextBlock>().FirstOrDefault(t => t.Text == slicer.EmptyText);
                    if (text is null)
                    {
                        found.Add($"{at} is not drawn");
                    }
                    else
                    {
                        found.AddRange(Muted(text, slicer, muted, at, weight: false));
                        var band = Bounds(Find<FrameworkElement>(slicer, "SlicerBorder"), slicer);
                        var bounds = Bounds(text, slicer);
                        if (Math.Abs((bounds.Left + bounds.Right) / 2 - (band.Left + band.Right) / 2) > 1)
                        {
                            found.Add($"{at} is centred at {(bounds.Left + bounds.Right) / 2:0}, the slicer at {(band.Left + band.Right) / 2:0}");
                        }
                    }

                    Shoot(window, $"Slicer-{theme}-{width}x{height}.png");
                }
                finally
                {
                    window.Close();
                }
            }

            return found;
        }));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Theory]
    [InlineData(1, "1 index")]
    [InlineData(3, "3 indexes")]
    public void ACountWithAnIrregularPlural_ReadsAsAPersonSaysIt(int count, string expected) =>
        Assert.Equal(expected, PerformanceMonitor.Common.CountText.Plural(count, "index", "indexes"));

    // ── helpers ──────────────────────────────────────────────────────────────────────────────────

    /// <summary>Builds the real ServerTab under <paramref name="theme"/>, shows it, and runs <paramref name="body"/> with the theme's muted colour.</summary>
    private static List<string> WithServerTab(string theme, Func<ServerTab, Window, Color, List<string>> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "EmptyAndCountText_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithTheme(theme, muted =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "empty-text-test", DisplayName = "empty-text-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                /* Switching to a tab would start that tab's load against a server that is not there. */
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                var window = Open(tab, PaneWidth(1280), 800);
                try
                {
                    return body(tab, window, muted);
                }
                finally
                {
                    window.Close();
                }
            }));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static List<string> Shown(FrameworkElement tab, int width, int height, Func<Window, List<string>> check, string shot)
    {
        var window = Open(tab, PaneWidth(width), height);
        try
        {
            var found = check(window);
            Shoot(window, shot);
            return found;
        }
        finally
        {
            window.Close();
        }
    }

    private static Window Open(FrameworkElement content, int width, int height)
    {
        var window = new Window { Content = content, Width = width, Height = height, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
        window.Show();
        Settle(window);
        return window;
    }

    private static void Resize(Window window, int width, int height)
    {
        window.Width = width;
        window.Height = height;
        Settle(window);
    }

    private static void Settle(Window window)
    {
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
    }

    private static T Find<T>(FrameworkElement tab, string name) where T : FrameworkElement =>
        tab.FindName(name) as T ?? throw new Xunit.Sdk.XunitException($"{name} is not on the tab");

    /// <summary>Selects every tab on the way up to <paramref name="element"/>, so it is laid out and drawn.</summary>
    private static void Reveal(FrameworkElement element)
    {
        for (DependencyObject? at = element; at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
            {
                owner.SelectedItem = item;
            }
        }
    }

    /// <summary>The text <see cref="EmptyState"/> drew for <paramref name="grid"/>.</summary>
    private static TextBlock EmptyText(FrameworkElement tab, DataGrid grid) =>
        ClippedTextProbe.Descendants(tab).OfType<TextBlock>().FirstOrDefault(t => t.Name == EmptyState.ElementName + "_" + grid.Name)
            ?? throw new Xunit.Sdk.XunitException($"{grid.Name} shows no empty-state text");

    private static Rect Bounds(FrameworkElement element, FrameworkElement ancestor) =>
        element.TransformToAncestor(ancestor).TransformBounds(new Rect(element.RenderSize));

    /// <summary>How many lines <paramref name="text"/> wrapped to, from its height against one line in the same font.</summary>
    private static int Lines(TextBlock text)
    {
        var one = new TextBlock { Text = "x", FontFamily = text.FontFamily, FontSize = text.FontSize, FontStyle = text.FontStyle, FontWeight = text.FontWeight };
        one.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
        return (int)Math.Round((text.ActualHeight - text.Padding.Top - text.Padding.Bottom) / one.DesiredSize.Height);
    }

    /// <summary>The width <paramref name="text"/> takes on one line.</summary>
    private static double NaturalWidth(TextBlock text)
    {
        var one = new TextBlock { Text = text.Text, FontFamily = text.FontFamily, FontSize = text.FontSize, FontStyle = text.FontStyle, FontWeight = text.FontWeight, TextWrapping = TextWrapping.NoWrap };
        one.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
        return one.DesiredSize.Width;
    }

    /// <summary>
    /// The realized look of <paramref name="label"/>: visible, upright, in the theme's muted foreground, normal weight when
    /// <paramref name="weight"/> is asked, and wholly inside <paramref name="tab"/> unless <paramref name="insideTab"/> is off (a
    /// sub-tab that scrolls keeps its lower content below the fold).
    /// </summary>
    private static List<string> Muted(TextBlock label, FrameworkElement tab, Color muted, string what, bool weight = true, bool insideTab = true)
    {
        var problems = new List<string>();
        if (!label.IsVisible)
        {
            problems.Add($"{what} is not visible");
            return problems;
        }

        if (label.FontStyle != FontStyles.Normal)
        {
            problems.Add($"{what} is {label.FontStyle}");
        }

        if (weight && label.FontWeight != FontWeights.Normal)
        {
            problems.Add($"{what} is {label.FontWeight}");
        }

        if (label.Foreground is not SolidColorBrush brush || brush.Color != muted)
        {
            problems.Add($"{what} is {Describe(label.Foreground)}, not the muted foreground {muted}");
        }

        var bounds = Bounds(label, tab);
        if (insideTab && (bounds.Left < -0.5 || bounds.Top < -0.5 || bounds.Right > tab.ActualWidth + 0.5 || bounds.Bottom > tab.ActualHeight + 0.5))
        {
            problems.Add($"{what} spans {bounds.Left:0},{bounds.Top:0}..{bounds.Right:0},{bounds.Bottom:0} in a {tab.ActualWidth:0}x{tab.ActualHeight:0} tab");
        }

        return problems;
    }

    /// <summary><paramref name="sentence"/> lies wholly inside <paramref name="grid"/>'s bounds.</summary>
    private static List<string> Inside(TextBlock sentence, DataGrid grid, FrameworkElement tab, string what)
    {
        var problems = new List<string>();
        if (!sentence.IsVisible)
        {
            return problems;
        }

        var g = Bounds(grid, tab);
        var s = Bounds(sentence, tab);
        if (s.Left < g.Left - 0.5 || s.Top < g.Top - 0.5 || s.Right > g.Right + 0.5 || s.Bottom > g.Bottom + 0.5)
        {
            problems.Add($"{what} spans {s.Top:0}..{s.Bottom:0}, outside {grid.Name}'s {g.Top:0}..{g.Bottom:0}");
        }

        return problems;
    }

    private static string Describe(Brush? brush) => brush is SolidColorBrush solid ? solid.Color.ToString() : brush?.ToString() ?? "null";

    /// <summary>Saves the window's content as a PNG under REFRESH_SHOTS_DIR when that directory is given; otherwise nothing.</summary>
    private static void Shoot(Window window, string fileName)
    {
        var dir = Environment.GetEnvironmentVariable("REFRESH_SHOTS_DIR");
        if (string.IsNullOrEmpty(dir) || window.Content is not FrameworkElement content || content.ActualWidth <= 0)
        {
            return;
        }

        var bitmap = new RenderTargetBitmap((int)content.ActualWidth, (int)content.ActualHeight, 96, 96, PixelFormats.Pbgra32);
        bitmap.Render(content);
        var encoder = new PngBitmapEncoder();
        encoder.Frames.Add(BitmapFrame.Create(bitmap));
        Directory.CreateDirectory(dir);
        using var stream = File.Create(Path.Combine(dir, fileName));
        encoder.Save(stream);
    }

    /// <summary>Runs <paramref name="body"/> with <paramref name="theme"/> and the shared styles as the app's resources, and gives it the theme's muted colour.</summary>
    private static T WithTheme<T>(string theme, Func<Color, T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var saved = app.Resources.MergedDictionaries.ToList();
        app.Resources.MergedDictionaries.Clear();
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{theme}.xaml") });
        app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
        try
        {
            return body(((SolidColorBrush)app.FindResource("ForegroundMutedBrush")).Color);
        }
        finally
        {
            app.Resources.MergedDictionaries.Clear();
            foreach (var dictionary in saved)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }
    }
}
