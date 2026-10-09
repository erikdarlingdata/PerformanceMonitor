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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows;
using System.Windows.Automation.Peers;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Release walk D1, D2, D3 and D11 in the Darling Viewer: what UI Automation reads for a grid column header, its filter
/// button, an empty grid cell, a server box item, an Overview card and a Needs Attention row. The names came out as
/// "System.Windows.Controls.StackPanel", a bare glyph, "Item: ...ViewerJobHistoryRow, Column Display Index: 7",
/// "...DarlingServer", "...ServerSummaryItem" and "...FleetRankedServer". Lite's twin is ScreenReaderNamesTests in Lite.Tests;
/// the column-header naming itself is the shared <see cref="AccessibleNames"/>.
/// </summary>
public sealed class ViewerScreenReaderNamesTests
{
    private static string ViewerDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer"));

    private static void Settle(Window window)
    {
        window.Show();
        var frame = new DispatcherFrame();
        window.Dispatcher.BeginInvoke(DispatcherPriority.ApplicationIdle, new Action(() => frame.Continue = false));
        Dispatcher.PushFrame(frame);
    }

    private static void Walk(AutomationPeer peer, List<AutomationPeer> into)
    {
        into.Add(peer);
        foreach (var child in peer.GetChildren() ?? new List<AutomationPeer>())
        {
            Walk(child, into);
        }
    }

    [Fact]
    public void ServerItemTypes_ReadAsTheServersName_NotTheirTypeName()
    {
        var server = new DarlingServer(1, "example-sql-01", "Example SQL 01", true, 16);
        var card = new ServerSummaryItem { DisplayName = "Example SQL 02" };
        var ranked = new FleetRankedServer { DisplayName = "Example SQL 03" };

        Assert.Equal("Example SQL 01", server.ToString());
        Assert.Equal("Example SQL 02", card.ToString());
        Assert.Equal("Example SQL 03", ranked.ToString());
    }

    [Fact]
    public void AServerBoxAndAnItemsList_ExposeTheServerName_ForEachItem()
    {
        var names = StaTestThread.Run(() =>
        {
            /* A closed ComboBox builds no item peers: its drop-down list is a ListBox, which names its items the same way. */
            var combo = new ListBox { ItemsSource = new[] { new DarlingServer(1, "example-sql-01", "Example SQL 01", true, 16) } };
            var list = new ItemsControl { ItemsSource = new[] { new FleetRankedServer { DisplayName = "Example SQL 03" } } };
            var cards = new ItemsControl { ItemsSource = new[] { new ServerSummaryItem { DisplayName = "Example SQL 02" } } };
            var panel = new StackPanel();
            panel.Children.Add(combo);
            panel.Children.Add(list);
            panel.Children.Add(cards);
            var window = new Window { Content = panel, Width = 400, Height = 300 };
            Settle(window);
            var peers = new List<AutomationPeer>();
            Walk(UIElementAutomationPeer.CreatePeerForElement(window), peers);
            window.Close();
            return peers.Select(p => p.GetName()).ToList();
        });

        Assert.DoesNotContain(names, n => n.Contains("DarlingServer", StringComparison.Ordinal) || n.Contains("FleetRankedServer", StringComparison.Ordinal) || n.Contains("ServerSummaryItem", StringComparison.Ordinal));
        Assert.Contains("Example SQL 01", names);
        Assert.Contains("Example SQL 03", names);
        Assert.Contains("Example SQL 02", names);
    }

    public sealed class Row
    {
        public string Time { get; set; } = "";
        public string Retries { get; set; } = "";
        public override string ToString() => "Row";
    }

    [Fact]
    public void AGridsPanelHeaders_FilterButtons_AndBlankCells_AreNamedInTheAutomationTree()
    {
        var names = StaTestThread.Run(() =>
        {
            AccessibleNames.Register();
            var grid = new DataGrid { AutoGenerateColumns = false, HeadersVisibility = DataGridHeadersVisibility.Column };
            foreach (var title in new[] { "Time", "Retries" })
            {
                var panel = new StackPanel { Orientation = Orientation.Horizontal };
                panel.Children.Add(new Button { Tag = title, Content = new TextBlock { Text = "" } });
                panel.Children.Add(new TextBlock { Text = title });
                grid.Columns.Add(new DataGridTextColumn { Header = panel, Binding = new System.Windows.Data.Binding(title) });
            }

            grid.ItemsSource = new[] { new Row { Time = "10:00", Retries = "" } };
            var window = new Window { Content = grid, Width = 500, Height = 300 };
            Settle(window);
            var peers = new List<AutomationPeer>();
            Walk(UIElementAutomationPeer.CreatePeerForElement(grid), peers);
            window.Close();
            return peers.Select(p => (Type: p.GetType().Name, Name: p.GetName())).ToList();
        });

        Assert.Equal(new[] { "Time", "Retries" }, names.Where(n => n.Type == "DataGridColumnHeaderItemAutomationPeer").Select(n => n.Name).ToList());
        Assert.Contains(names, n => n.Type == "ButtonAutomationPeer" && n.Name == "Filter Time");
        Assert.Contains(names, n => n.Type == "ButtonAutomationPeer" && n.Name == "Filter Retries");
        Assert.Contains(names, n => n.Type == "DataGridCellItemAutomationPeer" && n.Name == "Retries: blank");
        Assert.DoesNotContain(names, n => n.Name.StartsWith("Item:", StringComparison.Ordinal) || n.Name.Contains("StackPanel", StringComparison.Ordinal));
    }

    private static readonly Regex s_columnHeader = new(@"<(DataGrid\w*Column)\.Header>(?<body>.*?)</\1\.Header>", RegexOptions.Singleline | RegexOptions.Compiled);

    private static string StripComments(string xaml) => Regex.Replace(xaml, "<!--.*?-->", "", RegexOptions.Singleline);

    private static IEnumerable<string> ViewerXamlFiles() =>
        Directory.EnumerateFiles(ViewerDir(), "*.xaml", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                     && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal));

    [Fact]
    public void EveryColumnHeaderMadeOfAPanel_CarriesAVisibleTitle_SoItHasAScreenReaderName()
    {
        var offenders = new List<string>();
        var checkedHeaders = 0;

        foreach (var file in ViewerXamlFiles())
        {
            foreach (Match header in s_columnHeader.Matches(StripComments(File.ReadAllText(file))))
            {
                var body = header.Groups["body"].Value;

                if (!body.Contains("<StackPanel", StringComparison.Ordinal) && !body.Contains("<Button", StringComparison.Ordinal))
                {
                    continue;
                }

                checkedHeaders++;

                if (!Regex.IsMatch(body, "<TextBlock\\b[^>]*\\bText=\"[^\"]+\""))
                {
                    var flat = Regex.Replace(body.Trim(), @"\s+", " ");
                    offenders.Add(Path.GetFileName(file) + ": " + flat.Substring(0, Math.Min(120, flat.Length)));
                }
            }
        }

        Assert.True(checkedHeaders > 50, "the census found only " + checkedHeaders + " panel headers; the pattern no longer matches the XAML");
        Assert.True(offenders.Count == 0, "column headers with no visible title to name them from:\n" + string.Join("\n", offenders));
    }

    [Fact]
    public void EveryFilterButtonSitsInAColumnHeader_WhereTheNameIsGivenToIt()
    {
        var offenders = new List<string>();

        foreach (var file in ViewerXamlFiles().Where(f => !f.Contains(Path.DirectorySeparatorChar + "Themes" + Path.DirectorySeparatorChar, StringComparison.Ordinal)))
        {
            var text = StripComments(File.ReadAllText(file));
            var inHeaders = s_columnHeader.Matches(text).Sum(m => Regex.Matches(m.Groups["body"].Value, "ColumnFilterButtonStyle").Count);
            var all = Regex.Matches(text, "ColumnFilterButtonStyle").Count;

            if (all != inHeaders)
            {
                offenders.Add(Path.GetFileName(file) + ": " + (all - inHeaders) + " filter button(s) outside a column header");
            }
        }

        Assert.True(offenders.Count == 0, string.Join("\n", offenders));
    }

    [Theory]
    [InlineData("FinOpsTab.xaml", "ServerSelector", "PickerLabel")]
    [InlineData("MainWindow.xaml", "RecommendationsServerSelector", "DisplayName")]
    public void TheServerBox_HasAName_AndItsItemsAreNamedByTheServer(string file, string boxName, string member)
    {
        var text = File.ReadAllText(Path.Combine(ViewerDir(), file));
        var start = text.IndexOf("<ComboBox x:Name=\"" + boxName + "\"", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var box = text.Substring(start, text.IndexOf("</ComboBox>", start, StringComparison.Ordinal) - start);

        Assert.Contains("AutomationProperties.Name=\"Server\"", box, StringComparison.Ordinal);
        Assert.Contains("<Setter Property=\"AutomationProperties.Name\" Value=\"{Binding " + member + ", Mode=OneTime}\"/>", box, StringComparison.Ordinal);
    }
}
