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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8531: the Viewer's FinOps Server Inventory grid, drawn by the real tab. The Status and Health cells of a server with a
/// verdict and a score read them, and those of a server with neither (no CPU sample in the last 24 hours) read
/// "Not collected", never an empty cell or a bare dash.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerFinOpsServerInventoryCellsTests
{
    [Fact]
    public void StatusAndHealth_ReadTheCollectedValues_OrNotCollected_NeverBlank()
    {
        var problems = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var grid = (DataGrid)tab.FindName("FinOpsServerInventoryDataGrid");
                for (DependencyObject? at = grid; at is not null; at = LogicalTreeHelper.GetParent(at))
                {
                    if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
                    {
                        owner.SelectedItem = item;
                    }
                }

                var sampled = new ServerPropertyRow { ServerId = 1, ServerName = "inv-cells-sampled", ProvisioningStatus = ProvisioningVerdict.RightSized, HealthScore = 82 };
                var unsampled = new ServerPropertyRow { ServerId = 2, ServerName = "inv-cells-unsampled" };
                var cpuOnly = new ServerPropertyRow { ServerId = 3, ServerName = "inv-cells-cpu-only", ProvisioningStatus = ProvisioningVerdict.RightSized };
                grid.ItemsSource = new List<ServerPropertyRow> { sampled, unsampled, cpuOnly };
                window.UpdateLayout();
                ChartLayoutProbe.WaitForIdle();

                var found = new List<string>();
                var status = CellText(grid, sampled, "Status");
                if (status != ProvisioningVerdict.StatusLabel(ProvisioningVerdict.RightSized))
                {
                    found.Add($"the sampled server's Status cell reads \"{status}\"");
                }

                var health = CellText(grid, sampled, "Health");
                if (health != 82.ToString(System.Globalization.CultureInfo.CurrentCulture))
                {
                    found.Add($"the sampled server's Health cell reads \"{health}\", not its score of 82");
                }

                foreach (var header in new[] { "Status", "Health" })
                {
                    var text = CellText(grid, unsampled, header);
                    if (text != "Not collected")
                    {
                        found.Add($"the unsampled server's {header} cell reads \"{text}\", not \"Not collected\"");
                    }
                }

                /* Pointing at the Status words says what was not collected, and over what window. */
                CellText(grid, unsampled, "Status");
                var statusNote = grid.Columns.First(c => HeaderText(c) == "Status").GetCellContent(unsampled) is TextBlock statusCell ? statusCell.ToolTip as string : null;
                if (statusNote != "No CPU sample in the last 24 hours")
                {
                    found.Add($"the unsampled server's Status cell has the tooltip \"{statusNote}\", not what was not collected");
                }

                /* A server with a verdict and no score has its CPU sample and no memory sample: Health says which input is missing,
                   and each Health note names the one cause that applies to its server. */
                var cpuOnlyHealth = CellText(grid, cpuOnly, "Health");
                if (cpuOnlyHealth != "No memory sample")
                {
                    found.Add($"the Health cell of a server with a verdict and no memory sample reads \"{cpuOnlyHealth}\", not \"No memory sample\"");
                }

                var healthColumn = grid.Columns.First(c => HeaderText(c) == "Health");
                var cpuOnlyNote = healthColumn.GetCellContent(cpuOnly) is TextBlock cpuOnlyCell ? cpuOnlyCell.ToolTip as string : null;
                if (cpuOnlyNote != "No health score: no memory sample has been collected")
                {
                    found.Add($"that Health cell has the tooltip \"{cpuOnlyNote}\"");
                }

                CellText(grid, unsampled, "Health");
                var unsampledNote = healthColumn.GetCellContent(unsampled) is TextBlock unsampledCell ? unsampledCell.ToolTip as string : null;
                if (unsampledNote != "No health score: the last 24 hours hold no CPU sample")
                {
                    found.Add($"the unsampled server's Health cell has the tooltip \"{unsampledNote}\"");
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

    /// <summary>The text the grid draws in <paramref name="item"/>'s cell under the column headed <paramref name="header"/>.</summary>
    private static string CellText(DataGrid grid, object item, string header)
    {
        var column = grid.Columns.FirstOrDefault(c => HeaderText(c) == header)
            ?? throw new Xunit.Sdk.XunitException($"the grid has no {header} column");
        grid.ScrollIntoView(item, column);
        grid.UpdateLayout();
        return column.GetCellContent(item) is TextBlock text ? text.Text : "(the cell is not realized)";
    }

    private static string HeaderText(DataGridColumn column) => column.Header switch
    {
        string text => text,
        Panel panel => panel.Children.OfType<TextBlock>().FirstOrDefault()?.Text ?? "",
        _ => "",
    };

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
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
