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
using System.Windows.Automation;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Shapes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8332: six small layout and status-bar defects in the Viewer, each read from a rendered control on the shared STA thread with the
/// app's theme. A grid cell shows its text whole (Fleet Sweeps' Position and timestamps, Job History's Category); the status bar keeps
/// to one line at the 900x600 minimum and shows the line of the tab on screen rather than the last read that finished; a Needs
/// Attention row keeps a gap between the name and its reason; an Overview card trims a long title and keeps its status dot.
/// Every host is sized to fit a 1024x768 screen; a wider layout is laid out in an explicitly sized element inside it.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerSmallLayoutDefectsTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    /// <summary>The card's own width, fixed in its template.</summary>
    private const double CardWidth = 300;

    /// <summary>A watch item's position text once a run has been open a while, and the fleet-wide stamp with its zone.</summary>
    private const string LongPosition = "49 consecutive hits in the current run";
    private const string FleetStamp = "2026-11-08 18:33:05 UTC";

    [Fact]
    public void FleetSweeps_ShowsThePositionAndTimestampCellsWhole()
    {
        var clipped = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new FleetSweepsTab();
            var window = new Window { Content = tab, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var runs = (DataGrid)tab.FindName("RunsDataGrid");
                var watch = (DataGrid)tab.FindName("WatchItemsDataGrid");
                runs.ItemsSource = new List<FleetSweepRunRow>
                {
                    new("sweep-1", FleetStamp, "", "57 servers", "50 healthy  7 warning", "3 open", "none") { SweptAtUtc = new DateTime(2026, 11, 8, 18, 33, 5, DateTimeKind.Utc) },
                };
                watch.ItemsSource = new List<FleetSweepWatchRow>
                {
                    new("server-01", "cpu", "open", LongPosition, FleetStamp, FleetStamp, "CPU over 90% on 3 of the last 3 sweeps", "evidence")
                    {
                        FirstSeenUtc = new DateTime(2026, 11, 8, 18, 33, 5, DateTimeKind.Utc),
                        LastSeenUtc = new DateTime(2026, 11, 8, 18, 33, 5, DateTimeKind.Utc),
                    },
                };
                ClippedTextProbe.Settle(window);
                var found = ClippedCells(runs, "Swept at").Concat(ClippedCells(watch, "watch items")).ToList();
                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
    }

    [Fact]
    public void TheStatusBar_KeepsToOneLine_At900x600_WithALongRefreshStamp()
    {
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var w = Shell();
            var host = Host(w, 900, 600);
            try
            {
                var status = (TextBlock)w.FindName("StatusText");
                status.Text = "sql-prod-eastus2-reporting-07x — refreshed 18:33:05 UTC";
                foreach (var (name, text) in new[]
                {
                    ("SeatText", "Access: read-only"),
                    ("CollectorHealthText", "Collectors: 44 healthy, 3 warning, 2 not collected"),
                    ("DatabaseSizeText", "Database: 123.4 GB"),
                    ("CollectionStatusText", "Collection: Running"),
                    ("ServerCountText", "Servers: 57"),
                })
                {
                    ((TextBlock)w.FindName(name)).Text = text;
                }

                ClippedTextProbe.Settle(host);
                var found = new List<string>();
                if (ClippedTextProbe.NaturalWidth(status) <= status.ActualWidth)
                {
                    found.Add($"the status text fits its {status.ActualWidth:F0} px column, so nothing is under test");
                }

                if (status.ActualHeight >= status.FontSize * 1.6)
                {
                    found.Add($"the status text is {status.ActualHeight:F0} px tall at {status.FontSize} px, more than one line");
                }

                if (!Equals(status.ToolTip, status.Text))
                {
                    found.Add("the status text's tooltip is " + (status.ToolTip is null ? "unset" : "\"" + status.ToolTip + "\"") + ", so the whole text cannot be read on hover");
                }

                return found;
            }
            finally
            {
                host.Close();
            }
        }));

        Assert.True(findings.Count == 0, string.Join(Environment.NewLine, findings));
    }

    [Fact]
    public void TheStatusBar_ShowsTheLineOfTheTabOnScreen_NotTheLastReadThatFinished()
    {
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var w = Shell();
            var host = Host(w, 1000, 700);
            try
            {
                var tabs = (TabControl)w.FindName("MainTabs");
                var overview = (TabItem)w.FindName("OverviewTab");
                var agTab = (TabItem)w.FindName("AvailabilityGroupsTabItem");
                var ag = (AvailabilityGroupsTab)w.FindName("AvailabilityGroupsContent");
                var status = (TextBlock)w.FindName("StatusText");

                tabs.SelectedItem = overview;
                ClippedTextProbe.Settle(host);
                status.Text = "overview — refreshed 18:33:05 UTC";

                /* The probe that decides whether to reveal the tab finishes after the Overview's own stamp. */
                ag.Render(new List<AgTopologyCard>());
                ClippedTextProbe.Settle(host);

                var found = new List<string>();
                if (!status.Text.StartsWith("overview", StringComparison.Ordinal))
                {
                    found.Add($"with the Overview on screen the status bar reads \"{status.Text}\"");
                }

                tabs.SelectedItem = agTab;
                ClippedTextProbe.Settle(host);
                if (status.Text != "No Availability Groups observed.")
                {
                    found.Add($"with Availability Groups on screen the status bar reads \"{status.Text}\"");
                }

                return found;
            }
            finally
            {
                host.Close();
            }
        }));

        Assert.True(findings.Count == 0, string.Join(Environment.NewLine, findings));
    }

    [Fact]
    public void JobHistory_ShowsTheDefaultCategoryWhole()
    {
        var clipped = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new JobHistoryTab();
            /* The tab is laid out at 1600 wide inside a window that fits the screen. */
            var wide = new Border { Child = tab, Width = 1600, Height = 700, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
            var window = new Window { Content = wide, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var grid = (DataGrid)tab.FindName("JobHistoryDataGrid");
                grid.ItemsSource = new List<ViewerJobHistoryRow>
                {
                    new() { ServerId = 1, ServerName = "server-01", JobName = "nightly maintenance", CategoryName = "[Uncategorized (Local)]", RunStatusDesc = "Succeeded", Message = "The job succeeded." },
                };
                ClippedTextProbe.Settle(window);
                return ClippedCells(grid, "job history");
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
    }

    [Fact]
    public void NeedsAttention_KeepsAGapBetweenTheLongestNameAndItsReason()
    {
        var item = new FleetRankedServer
        {
            ServerId = 1,
            DisplayName = "sql-prod-eastus2-reporting-07",
            Band = FleetHealthBand.Critical,
            Reason = "Identity capacity critical on 2 tables",
        };
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var host = new ContentControl
            {
                Content = item,
                ContentTemplate = TemplateOf("FleetWorstServersList"),
                Width = 600,
                HorizontalAlignment = HorizontalAlignment.Left,
                VerticalAlignment = VerticalAlignment.Top,
            };
            var window = new Window { Content = host, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var blocks = ClippedTextProbe.Descendants(host).OfType<TextBlock>().ToList();
                var name = blocks.First(b => b.Text == item.DisplayName);
                var reason = blocks.First(b => b.Text == item.Reason);
                var nameBounds = name.TransformToAncestor(host).TransformBounds(new Rect(name.RenderSize));
                var reasonBounds = reason.TransformToAncestor(host).TransformBounds(new Rect(reason.RenderSize));
                var textRight = nameBounds.Left + Math.Min(ClippedTextProbe.NaturalWidth(name), name.ActualWidth);
                var found = new List<string>();
                if (reasonBounds.Left - textRight < 8)
                {
                    found.Add($"the name's text ends at {textRight:F0} and the reason starts at {reasonBounds.Left:F0}, a gap of {reasonBounds.Left - textRight:F0} px");
                }

                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(findings.Count == 0, string.Join(Environment.NewLine, findings));
    }

    [Fact]
    public void AnOverviewCard_TrimsALongTitleInsideTheCard_AndKeepsItsStatusDot()
    {
        var item = new ServerSummaryItem
        {
            ServerId = 1,
            ServerName = "sqlprod-reporting-eastus2.example.internal",
            DisplayName = "sqlprod-reporting-eastus2.example.internal",
            IsOnline = true,
            CpuPercent = 42,
            LastCollectionTime = DateTime.UtcNow.AddMinutes(-2),
        };
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var host = new ContentControl
            {
                Content = item,
                ContentTemplate = TemplateOf("OverviewItemsControl"),
                HorizontalAlignment = HorizontalAlignment.Left,
                VerticalAlignment = VerticalAlignment.Top,
            };
            var window = new Window { Content = host, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            window.Resources["BoolToVisibility"] = new BooleanToVisibilityConverter();
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                var card = ClippedTextProbe.Descendants(host).OfType<Border>().First(b => b.Width == CardWidth);
                var title = ClippedTextProbe.Descendants(card).OfType<TextBlock>().First(b => AutomationProperties.GetAutomationId(b) == "OverviewServerCardName");
                var dot = ClippedTextProbe.Descendants(VisualTreeHelper.GetParent(title)).OfType<Ellipse>().First(e => e.Height == 10);
                var found = new List<string>();

                var titleBounds = title.TransformToAncestor(card).TransformBounds(new Rect(title.RenderSize));
                if (titleBounds.Right > card.ActualWidth + 0.5)
                {
                    found.Add($"the title ends at {titleBounds.Right:F0} px, past the {card.ActualWidth:F0} px card");
                }

                if (title.TextTrimming != TextTrimming.CharacterEllipsis)
                {
                    found.Add($"the title's TextTrimming is {title.TextTrimming}, so a long name is cut without a mark");
                }

                if (!Equals(title.ToolTip, item.DisplayName))
                {
                    found.Add("the title's tooltip is " + (title.ToolTip is null ? "unset" : "\"" + title.ToolTip + "\"") + ", so a trimmed name cannot be read on hover");
                }

                var slot = LayoutInformation.GetLayoutSlot(dot);
                if (slot.Width < 10)
                {
                    found.Add($"the status dot is given {slot.Width:F0} px of the title row");
                }

                if (LayoutInformation.GetLayoutClip(dot) is not null)
                {
                    found.Add("the status dot is clipped");
                }

                return found;
            }
            finally
            {
                window.Close();
            }
        }));

        Assert.True(findings.Count == 0, "the Overview card at " + CardWidth + " px:" + Environment.NewLine + string.Join(Environment.NewLine, findings));
    }

    /// <summary>The text in a grid's realized cells that runs past its cell. A grid that realized no cell is reported too, so an empty grid
    /// is not a pass.</summary>
    private static List<string> ClippedCells(DataGrid grid, string what)
    {
        var found = new List<string>();
        var cells = ClippedTextProbe.Descendants(grid).OfType<DataGridCell>().ToList();
        if (cells.Count == 0)
        {
            found.Add($"the {what} grid realized no cell");
            return found;
        }

        foreach (var cell in cells)
        {
            foreach (var block in ClippedTextProbe.Descendants(cell).OfType<TextBlock>().Where(b => b.IsVisible && !string.IsNullOrWhiteSpace(b.Text)))
            {
                var right = block.TransformToAncestor(cell).TransformBounds(new Rect(block.RenderSize)).Right;
                if (right > cell.ActualWidth + 1)
                {
                    found.Add($"[{what}] '{block.Text}' ends at {right:F0} in a {cell.ActualWidth:F0} px cell");
                }
            }
        }

        return found;
    }

    /// <summary>The main window, built and never shown; its constructor seeds the process-wide display mode from the app settings, which is put back.</summary>
    private static MainWindow Shell()
    {
        var mode = ViewerTimeHelper.CurrentDisplayMode;
        try
        {
            return new MainWindow();
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
        }
    }

    /// <summary>The item template of a named items control in the main window.</summary>
    private static DataTemplate TemplateOf(string itemsControlName) => ((ItemsControl)Shell().FindName(itemsControlName)).ItemTemplate;

    /// <summary>A window that was never shown has no layout, so the main window's content is moved into a shown host of the given size.</summary>
    private static Window Host(MainWindow w, double width, double height)
    {
        var root = (UIElement)w.Content;
        w.Content = null;
        var host = new Window { Content = root, Width = width, Height = height, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
        StaTestThread.ShowOffScreen(host);
        ClippedTextProbe.Settle(host);
        return host;
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("BackgroundLightBrush"))
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
