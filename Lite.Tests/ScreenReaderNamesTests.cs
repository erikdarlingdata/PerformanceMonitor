/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Release walk V2, V3 and V4: what UI Automation reads for a grid column header, the filter button inside it, an empty grid
/// cell and a server box item. The names came out as "System.Windows.Controls.StackPanel", a bare glyph, "Item: ...Row,
/// Column Display Index: 7" and "ServerConnection". The Viewer twin is in Darling.Tests (ViewerScreenReaderNamesTests).
/// </summary>
[Trait("Reads", "Darling")]
public class ScreenReaderNamesTests
{
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        using var staGate = WpfStaGate.Enter();
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();
        if (error is not null)
        {
            throw error;
        }

        return result;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    /// <summary>Shows the window and lets its layout and input-priority work run, so the first SizeChanged of every element has fired.</summary>
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

    public sealed class Row
    {
        public string Time { get; set; } = "";
        public string Retries { get; set; } = "";
        public override string ToString() => "Row";
    }

    [Fact]
    public void AGridsPanelHeaders_FilterButtons_AndBlankCells_AreNamedInTheAutomationTree()
    {
        var names = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var grid = new DataGrid { AutoGenerateColumns = false, HeadersVisibility = DataGridHeadersVisibility.Column };
            foreach (var (title, path) in new[] { ("Time", "Time"), ("Retries", "Retries") })
            {
                var panel = new StackPanel { Orientation = Orientation.Horizontal };
                panel.Children.Add(new Button { Tag = path, Content = new TextBlock { Text = "" } });
                panel.Children.Add(new TextBlock { Text = title });
                grid.Columns.Add(new DataGridTextColumn { Header = panel, Binding = new System.Windows.Data.Binding(path) });
            }

            grid.ItemsSource = new[] { new Row { Time = "10:00", Retries = "" } };
            var window = new Window { Content = grid, Width = 500, Height = 300 };
            Settle(window);
            var peers = new List<AutomationPeer>();
            Walk(UIElementAutomationPeer.CreatePeerForElement(grid), peers);
            window.Close();
            return peers.Select(p => (Type: p.GetType().Name, Name: p.GetName())).ToList();
        });

        var headerNames = names.Where(n => n.Type == "DataGridColumnHeaderItemAutomationPeer").Select(n => n.Name).ToList();
        Assert.Equal(new[] { "Time", "Retries" }, headerNames);
        Assert.Contains(names, n => n.Type == "ButtonAutomationPeer" && n.Name == "Filter Time");
        Assert.Contains(names, n => n.Type == "ButtonAutomationPeer" && n.Name == "Filter Retries");
        Assert.Contains(names, n => n.Type == "DataGridCellItemAutomationPeer" && n.Name == "Retries: blank");
        Assert.DoesNotContain(names, n => n.Name.Contains("System.Windows.Controls.StackPanel", StringComparison.Ordinal));
        Assert.DoesNotContain(names, n => n.Name.StartsWith("Item:", StringComparison.Ordinal));
    }

    [Fact]
    public void ABlankCell_LosesItsBlankName_WhenItsTextFillsIn()
    {
        var (blank, filled) = OnStaThread(() =>
        {
            AccessibleNames.Register();
            var grid = new DataGrid { AutoGenerateColumns = false, HeadersVisibility = DataGridHeadersVisibility.Column };
            grid.Columns.Add(new DataGridTextColumn { Header = "Retries", Binding = new System.Windows.Data.Binding("Retries") });
            var row = new Row();
            grid.ItemsSource = new[] { row };
            var window = new Window { Content = grid, Width = 400, Height = 300 };
            Settle(window);
            var cell = (DataGridCell)new List<DependencyObject>(Descendants(grid)).First(d => d is DataGridCell);
            var first = System.Windows.Automation.AutomationProperties.GetName(cell);
            ((TextBlock)cell.Content).Text = "3";
            var second = System.Windows.Automation.AutomationProperties.GetName(cell);
            window.Close();
            return (first, second);
        });

        Assert.Equal("Retries: blank", blank);
        Assert.Equal("", filled);
    }

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        for (var i = 0; i < System.Windows.Media.VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = System.Windows.Media.VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var deeper in Descendants(child))
            {
                yield return deeper;
            }
        }
    }

    [Fact]
    public void ServerItems_ReadAsTheServersName_NotTheTypeName()
    {
        var server = new ServerConnection { DisplayName = "example-sql-01" };
        var readOnly = new ServerConnection { DisplayName = "example-sql-02", ReadOnlyIntent = true };
        var card = new ServerSummaryItem { DisplayName = "example-sql-03" };

        Assert.Equal("example-sql-01", server.ToString());
        Assert.Equal("example-sql-02 (Read-Only)", readOnly.ToString());
        Assert.Equal("example-sql-03", card.ToString());

        var itemNames = OnStaThread(() =>
        {
            /* A closed ComboBox builds no item peers: its drop-down list is a ListBox, which names its items the same way. */
            var combo = new ListBox { ItemsSource = new[] { server, readOnly } };
            var window = new Window { Content = combo, Width = 300, Height = 200 };
            Settle(window);
            var peers = new List<AutomationPeer>();
            Walk(UIElementAutomationPeer.CreatePeerForElement(combo), peers);
            window.Close();
            return peers.Select(p => p.GetName()).ToList();
        });

        Assert.Contains("example-sql-01", itemNames);
        Assert.Contains("example-sql-02 (Read-Only)", itemNames);
        Assert.DoesNotContain(itemNames, n => n.Contains("ServerConnection", StringComparison.Ordinal));
    }

    private static readonly Regex s_columnHeader = new(@"<(DataGrid\w*Column)\.Header>(?<body>.*?)</\1\.Header>", RegexOptions.Singleline | RegexOptions.Compiled);

    private static string StripComments(string xaml) => Regex.Replace(xaml, "<!--.*?-->", "", RegexOptions.Singleline);

    private static IEnumerable<string> LiteXamlFiles() =>
        Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "*.xaml", SearchOption.AllDirectories)
            .Where(f => !f.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                     && !f.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal));

    [Fact]
    public void EveryColumnHeaderMadeOfAPanel_CarriesAVisibleTitle_SoItHasAScreenReaderName()
    {
        var offenders = new List<string>();
        var checkedHeaders = 0;

        foreach (var file in LiteXamlFiles())
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
                    offenders.Add(Path.GetRelativePath(RepoRoot(), file) + ": " + Regex.Replace(body.Trim(), @"\s+", " ").Substring(0, Math.Min(120, body.Trim().Length)));
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

        foreach (var file in LiteXamlFiles().Where(f => !f.Contains(Path.DirectorySeparatorChar + "Themes" + Path.DirectorySeparatorChar, StringComparison.Ordinal)))
        {
            var text = StripComments(File.ReadAllText(file));
            var inHeaders = s_columnHeader.Matches(text).Sum(m => Regex.Matches(m.Groups["body"].Value, "ColumnFilterButtonStyle").Count);
            var all = Regex.Matches(text, "ColumnFilterButtonStyle").Count;

            if (all != inHeaders)
            {
                offenders.Add(Path.GetRelativePath(RepoRoot(), file) + ": " + (all - inHeaders) + " filter button(s) outside a column header");
            }
        }

        Assert.True(offenders.Count == 0, string.Join("\n", offenders));
    }

    [Theory]
    [InlineData("Lite/Controls/RecommendationsTab.xaml")]
    [InlineData("Lite/Controls/FinOpsTab.xaml")]
    public void TheServerBox_HasAName_AndItsItemsAreNamedByTheServer(string relative)
    {
        var text = File.ReadAllText(Path.Combine(RepoRoot(), relative));
        var start = text.IndexOf("<ComboBox x:Name=\"ServerSelector\"", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var end = text.IndexOf("</ComboBox>", start, StringComparison.Ordinal);
        var box = text.Substring(start, end - start);

        Assert.Contains("AutomationProperties.Name=\"Server\"", box, StringComparison.Ordinal);
        Assert.Contains("<Setter Property=\"AutomationProperties.Name\" Value=\"{Binding DisplayNameWithIntent, Mode=OneTime}\"/>", box, StringComparison.Ordinal);
    }
}
