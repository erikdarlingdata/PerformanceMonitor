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
using System.Windows.Controls.Primitives;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8474: the FinOps Server Inventory grid shows a server's Edition, Version and Host OS whole, and a Hardware Note column that is blank
/// on every row is collapsed, while a noted row shows it open to a drag. The real tab is built on the shared STA thread with the app's theme, its inventory
/// sub-tab is shown at the width a maximized 1920 px window gives the grid, two rows are bound, and the realized cells and column
/// widths are read. Lite's twin is FinOpsInventoryColumnWidthTests.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerFinOpsInventoryColumnWidthTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    /// <summary>The grid's width in a maximized 1920 px window, the fixed widths the three text columns had, and the note column's width.</summary>
    private const double GridWidth = 1580;
    private const double EditionWidth = 140;
    private const double VersionWidth = 100;
    private const double HostOsWidth = 140;
    private const double HardwareNoteWidth = 220;

    /// <summary>An edition, a version with its update level, and a host build as the collector records them.</summary>
    private const string LongEdition = "Enterprise Edition: Core-based Licensing (64-bit)";
    private const string LongVersion = "16.0.4215.2 - CU21 GDR";
    private const string LongHostOs = "Windows Server 2019 Datacenter (10.0.17763)";

    [Fact]
    public void ServerInventory_ShowsEditionVersionAndHostOsWhole_AndCollapsesABlankHardwareNoteColumn()
    {
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new FinOpsTab();
            /* The tab is laid out at the grid's maximized width inside a window that fits a 1024x768 screen. */
            var wide = new Border { Child = tab, Width = GridWidth, Height = 700, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
            var window = new Window { Content = wide, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var grid = (DataGrid)tab.FindName("FinOpsServerInventoryDataGrid");
                ShowTabOf(grid);
                grid.ItemsSource = new List<ServerPropertyRow>
                {
                    new() { ServerId = 1, ServerName = "srv-a", Edition = LongEdition, ProductVersion = LongVersion, HostOsVersion = LongHostOs, EngineEdition = 3, CpuCount = 16, PhysicalMemoryMb = 131072, SocketCount = 2, CoresPerSocket = 8, ProvisioningStatus = "RIGHT_SIZED" },
                    new() { ServerId = 2, ServerName = "srv-b", Edition = "Standard Edition (64-bit)", ProductVersion = "15.0.4345.5 - CU25", HostOsVersion = "Windows Server 2022 Standard (10.0.20348)", EngineEdition = 2, CpuCount = 8, PhysicalMemoryMb = 65536, SocketCount = 1, CoresPerSocket = 8, ProvisioningStatus = "OVER_PROVISIONED" },
                };
                ClippedTextProbe.Settle(window);

                var found = ClippedCells(grid, "server inventory");
                /* The sample texts must be wider than the fixed widths were, or a cell that fits says nothing. */
                foreach (var (text, width, name) in new[] { (LongEdition, EditionWidth, "Edition"), (LongVersion, VersionWidth, "Version"), (LongHostOs, HostOsWidth, "Host OS") })
                {
                    var block = ClippedTextProbe.Descendants(grid).OfType<TextBlock>().FirstOrDefault(b => b.Text == text);
                    if (block is null)
                    {
                        found.Add($"the {name} cell '{text}' was not realized");
                    }
                    else if (ClippedTextProbe.NaturalWidth(block) <= width)
                    {
                        found.Add($"the {name} text fits {width} px, so nothing is under test");
                    }
                }

                var note = ColumnHeaded(grid, "Hardware Note");
                if (note.Visibility != Visibility.Collapsed)
                {
                    found.Add($"the Hardware Note column is on screen, {note.ActualWidth:F0} px wide, with every row blank");
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

    /// <summary>A server whose hardware read was denied carries a note: the column is on screen at its 220 px, the note trims with an
    /// ellipsis and keeps the whole sentence in its tooltip, and a reader can widen the column past 220 px to read it in place.</summary>
    [Fact]
    public void ServerInventory_ShowsTheHardwareNoteColumn_WhenARowCarriesANote_TrimmedAtItsWidth_AndOpenToADrag()
    {
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new FinOpsTab();
            var wide = new Border { Child = tab, Width = GridWidth, Height = 700, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
            var window = new Window { Content = wide, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var grid = (DataGrid)tab.FindName("FinOpsServerInventoryDataGrid");
                ShowTabOf(grid);
                grid.ItemsSource = new List<ServerPropertyRow>
                {
                    new() { ServerId = 1, ServerName = "srv-a", Edition = LongEdition, ProductVersion = LongVersion, HostOsVersion = LongHostOs, EngineEdition = 3, CpuCount = 16, PhysicalMemoryMb = 131072, SocketCount = 2, CoresPerSocket = 8, ProvisioningStatus = "RIGHT_SIZED" },
                    new() { ServerId = 2, ServerName = "srv-b", Edition = "Standard Edition (64-bit)", ProductVersion = "15.0.4345.5 - CU25", HostOsVersion = "Windows Server 2022 Standard (10.0.20348)", EngineEdition = 3, HardwareUnavailableReason = ServerHardwareScope.InventoryHardwareNote },
                };
                ClippedTextProbe.Settle(window);

                var found = new List<string>();
                var note = ColumnHeaded(grid, "Hardware Note");
                if (note.Visibility != Visibility.Visible)
                {
                    found.Add("the Hardware Note column is not on screen with a row that carries a note");
                    return found;
                }

                if (Math.Abs(note.ActualWidth - HardwareNoteWidth) > 1)
                {
                    found.Add($"the Hardware Note column is {note.ActualWidth:F0} px wide, not its {HardwareNoteWidth} px");
                }

                var block = ClippedTextProbe.Descendants(grid).OfType<TextBlock>().FirstOrDefault(b => b.Text == ServerHardwareScope.InventoryHardwareNote);
                if (block is null)
                {
                    found.Add("the note cell was not realized");
                }
                else
                {
                    if (ClippedTextProbe.NaturalWidth(block) <= HardwareNoteWidth)
                    {
                        found.Add($"the note fits {HardwareNoteWidth} px, so nothing is under test");
                    }

                    if (block.TextTrimming != TextTrimming.CharacterEllipsis)
                    {
                        found.Add($"the note trims with {block.TextTrimming}, not an ellipsis");
                    }

                    if (!Equals(block.ToolTip, ServerHardwareScope.InventoryHardwareNote))
                    {
                        found.Add("the note's tooltip does not carry the whole note");
                    }
                }

                /* A reader drags the column open to read the sentence: the width is not capped. */
                note.Width = new DataGridLength(600);
                ClippedTextProbe.Settle(window);
                if (Math.Abs(note.ActualWidth - 600) > 1)
                {
                    found.Add($"the Hardware Note column is {note.ActualWidth:F0} px wide after it was set to 600 px");
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

    /// <summary>A filter on the Hardware Note column itself (IsEmpty hides every noted row) must not collapse the column, or its own funnel
    /// goes with it and the filter can never be cleared: the column and its filter button stay on screen, and clearing shows both rows.</summary>
    [Fact]
    public void AFilterOnTheHardwareNoteColumn_KeepsTheColumnAndItsFilterButtonOnScreen_AndClearingShowsEveryRow()
    {
        var findings = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new FinOpsTab();
            var wide = new Border { Child = tab, Width = GridWidth, Height = 700, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
            var window = new Window { Content = wide, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var grid = (DataGrid)tab.FindName("FinOpsServerInventoryDataGrid");
                ShowTabOf(grid);
                var filters = tab.ServerInventoryFilters ?? throw new InvalidOperationException("the inventory grid has no filter manager");
                filters.UpdateData(new List<ServerPropertyRow>
                {
                    new() { ServerId = 1, ServerName = "srv-a", Edition = LongEdition, ProductVersion = LongVersion, HostOsVersion = LongHostOs, EngineEdition = 3, CpuCount = 16, PhysicalMemoryMb = 131072, SocketCount = 2, CoresPerSocket = 8 },
                    new() { ServerId = 2, ServerName = "srv-b", Edition = "Standard Edition (64-bit)", ProductVersion = "15.0.4345.5 - CU25", HostOsVersion = "Windows Server 2022 Standard (10.0.20348)", EngineEdition = 3, HardwareUnavailableReason = ServerHardwareScope.InventoryHardwareNote },
                });
                ClippedTextProbe.Settle(window);

                var found = new List<string>();
                var note = ColumnHeaded(grid, "Hardware Note");
                if (note.Visibility != Visibility.Visible)
                {
                    found.Add("the Hardware Note column is not on screen before the filter, with a noted row bound");
                    return found;
                }

                filters.SetFilter(new ColumnFilterState { ColumnName = nameof(ServerPropertyRow.HardwareUnavailableReason), Operator = FilterOperator.IsEmpty });
                ClippedTextProbe.Settle(window);
                if (!filters.AnyFilterActive)
                {
                    found.Add("the IsEmpty filter did not take, so nothing is under test");
                    return found;
                }

                if (grid.Items.Count != 1)
                {
                    found.Add($"the filter left {grid.Items.Count} rows on screen, not the one blank row");
                }

                if (note.Visibility != Visibility.Visible)
                {
                    found.Add("the Hardware Note column left the screen under its own filter");
                }

                var funnel = ClippedTextProbe.Descendants(grid).OfType<DataGridColumnHeader>().Where(h => h.Column == note)
                    .SelectMany(h => ClippedTextProbe.Descendants(h).OfType<Button>()).FirstOrDefault();
                if (funnel is null || !funnel.IsVisible)
                {
                    found.Add("the Hardware Note column's filter button is not on screen under its own filter");
                }

                filters.ClearFilters();
                ClippedTextProbe.Settle(window);
                if (grid.Items.Count != 2)
                {
                    found.Add($"clearing the filter left {grid.Items.Count} rows on screen, not both");
                }

                if (note.Visibility != Visibility.Visible)
                {
                    found.Add("the Hardware Note column is not on screen after the filter was cleared");
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

    /// <summary>Selects the sub-tab the grid sits in, so the grid is in the visual tree and realizes its rows.</summary>
    private static void ShowTabOf(DependencyObject element)
    {
        for (var node = element; node is not null; node = LogicalTreeHelper.GetParent(node))
        {
            if (node is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl tabs)
            {
                tabs.SelectedItem = item;
            }
        }
    }

    /// <summary>The column whose header text block reads <paramref name="header"/>.</summary>
    private static DataGridColumn ColumnHeaded(DataGrid grid, string header) =>
        grid.Columns.First(c => c.Header is StackPanel panel && panel.Children.OfType<TextBlock>().Any(t => t.Text == header));

    /// <summary>The text in a grid's realized cells that runs past its cell or is cut inside it. A grid that realized no cell is reported too,
    /// so an empty grid is not a pass.</summary>
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

                /* A trimming cell style keeps the text block inside the cell and cuts the text with an ellipsis, so the text's own width is
                   what says whether it is shown whole. */
                var natural = ClippedTextProbe.NaturalWidth(block);
                if (natural > block.ActualWidth + 1)
                {
                    found.Add($"[{what}] '{block.Text}' needs {natural:F0} px and is cut in a {block.ActualWidth:F0} px cell");
                }
            }
        }

        return found;
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
