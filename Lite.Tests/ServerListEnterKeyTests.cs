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
using System.Windows.Input;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5763: Enter on Lite's server list opens the selected server's tab, through the body the double-click runs, and the
/// list keeps its selection when its items are replaced. The window is WPF code-behind that a test cannot instantiate
/// without a display, so the wiring is pinned from source (the way the other MainWindow wiring tests here do) and the
/// pure rules are tested directly in <see cref="ServerListKeyboard"/>.
/// </summary>
public sealed class ServerListEnterKeyTests
{
    [Fact]
    public void TheListXaml_WiresKeyDown_AndTheHintMentionsEnter()
    {
        var xaml = Read("Lite", "MainWindow.xaml");

        Assert.Matches(@"<ui:InvokableListBox[^>]*x:Name=""ServerListView""[^>]*KeyDown=""ServerListView_KeyDown""", xaml);
        Assert.Contains("MouseDoubleClick=\"ServerListView_MouseDoubleClick\"", xaml, StringComparison.Ordinal);
        Assert.Contains("Text=\"Double-click or press Enter to open a server tab\"", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void DoubleClickAndEnter_RunTheSameOpenMethod()
    {
        var code = Read("Lite", "MainWindow.xaml.cs");

        Assert.Matches(@"ServerListView_MouseDoubleClick\(object sender, MouseButtonEventArgs e\)\s*=>\s*OpenSelectedServerTab\(\);", code);
        Assert.Matches(@"ServerListView_KeyDown\(object sender, KeyEventArgs e\)\s*=>\s*ServerListKeyboard\.HandleKeyDown\(e, OpenSelectedServerTab\);", code);

        /* The open body exists once: only OpenSelectedServerTab turns the list's selection into ConnectToServer. */
        var body = Regex.Match(code, @"private void OpenSelectedServerTab\(\)\s*\{(?<b>.*?)\n    \}", RegexOptions.Singleline);
        Assert.True(body.Success, "OpenSelectedServerTab exists");
        Assert.Contains("ServerListView.SelectedItem is FleetServerRow row", body.Groups["b"].Value, StringComparison.Ordinal);
        Assert.Contains("ConnectToServer(server)", body.Groups["b"].Value, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(code, @"ServerListView\.SelectedItem is FleetServerRow row && row\.Server\.Connection"));
    }

    [Fact]
    public void EveryRebindOfTheList_KeepsItsKeyboard_AndRestoresTheSelectionById()
    {
        var sidebar = Read("Lite", "MainWindow.SidebarTree.cs");

        foreach (var file in Directory.EnumerateFiles(Path.Combine(RepoRoot(), "Lite"), "MainWindow*.cs"))
        {
            Assert.DoesNotContain("ServerListView.ItemsSource =", File.ReadAllText(file), StringComparison.Ordinal);
        }

        /* Each rebind is followed by a reselect of the server that was selected, resolved by its id. */
        var rebinds = Regex.Matches(sidebar, @"ServerListKeyboard\.Rebind\(ServerListView, _fleet\.Visible\);\s*ServerListView\.SelectedItem = _fleet\.ResolveSelection\(previousServerId\);").Count;
        Assert.Equal(2, rebinds);
        Assert.Equal(2, Regex.Matches(sidebar, @"ServerListKeyboard\.Rebind\(").Count);
    }

    [Fact]
    public void OnlyAPlainEnter_OpensTheTab()
    {
        Assert.True(ServerListKeyboard.IsOpenGesture(Key.Return, ModifierKeys.None));
        Assert.True(ServerListKeyboard.IsOpenGesture(Key.Enter, ModifierKeys.None));
        Assert.False(ServerListKeyboard.IsOpenGesture(Key.Return, ModifierKeys.Control));
        Assert.False(ServerListKeyboard.IsOpenGesture(Key.Return, ModifierKeys.Alt));
        Assert.False(ServerListKeyboard.IsOpenGesture(Key.Return, ModifierKeys.Shift));
        Assert.False(ServerListKeyboard.IsOpenGesture(Key.Space, ModifierKeys.None));
        Assert.False(ServerListKeyboard.IsOpenGesture(Key.Down, ModifierKeys.None));
    }

    [Fact]
    public void FindByIdentity_FindsTheSameServerAtANewPosition_ByIdNotIndex()
    {
        var before = new List<Row> { new(1), new(2), new(3) };
        var selectedId = (int?)before[1].Id;

        /* A refresh builds new row objects, reorders them, and puts a group header's worth of rows ahead. */
        var after = new List<Row> { new(9), new(3), new(1), new(2) };

        var found = ServerListKeyboard.FindByIdentity(after, selectedId, r => r.Id);

        Assert.NotNull(found);
        Assert.Equal(2, found!.Id);
        Assert.Same(after[3], found);
    }

    [Fact]
    public void FindByIdentity_IsNull_WhenNothingWasSelected_OrTheServerIsGone()
    {
        var rows = new List<Row> { new(1), new(2) };

        Assert.Null(ServerListKeyboard.FindByIdentity(rows, (int?)null, r => r.Id));
        Assert.Null(ServerListKeyboard.FindByIdentity(rows, (int?)7, r => r.Id));
        Assert.Null(ServerListKeyboard.FindByIdentity(Enumerable.Empty<Row>(), (int?)1, r => r.Id));
    }

    private sealed record Row(int Id);

    private static string Read(params string[] parts) =>
        File.ReadAllText(Path.Combine(new[] { RepoRoot() }.Concat(parts).ToArray())).Replace("\r\n", "\n");

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
