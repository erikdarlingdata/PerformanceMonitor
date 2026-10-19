/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5763: Enter on the viewer's server list opens the selected server's tab, through the body the double-click runs,
/// and the list keeps its selection when its items are replaced. The window is WPF code-behind that a test cannot
/// instantiate without a display, so the wiring is pinned from source, the way the other viewer window pins here do.
/// The pure rules (<c>ServerListKeyboard</c>) are tested in Lite.Tests/ServerListEnterKeyTests.cs, where the Ui project
/// is already in reach.
/// </summary>
public sealed class ViewerServerListEnterKeyTests
{
    [Fact]
    public void TheListXaml_WiresKeyDown_AndTheHintMentionsEnter()
    {
        var xaml = Read("MainWindow.xaml");

        Assert.Matches(@"<ui:InvokableListBox[^>]*x:Name=""ServerList""[^>]*KeyDown=""ServerList_KeyDown""", xaml);
        Assert.Contains("MouseDoubleClick=\"ServerList_MouseDoubleClick\"", xaml, StringComparison.Ordinal);
        Assert.Contains("Text=\"Double-click or press Enter to open a server tab\"", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void DoubleClickAndEnter_RunTheSameOpenMethod()
    {
        var code = Read("MainWindow.xaml.cs");

        Assert.Matches(@"ServerList_MouseDoubleClick\(object sender, MouseButtonEventArgs e\)\s*=>\s*OpenSelectedServerTab\(\);", code);
        Assert.Matches(@"ServerList_KeyDown\(object sender, KeyEventArgs e\)\s*=>\s*ServerListKeyboard\.HandleKeyDown\(e, OpenSelectedServerTab\);", code);

        /* The open body exists once: only OpenSelectedServerTab turns the list's selection into OpenServerTab. */
        var body = Regex.Match(code, @"private void OpenSelectedServerTab\(\)\s*\{(?<b>.*?)\n    \}", RegexOptions.Singleline);
        Assert.True(body.Success, "OpenSelectedServerTab exists");
        Assert.Contains("ServerList.SelectedItem is FleetServerRow row", body.Groups["b"].Value, StringComparison.Ordinal);
        Assert.Contains("OpenServerTab(row.Server)", body.Groups["b"].Value, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(code, @"OpenServerTab\(row\.Server\)"));
    }

    [Fact]
    public void EveryRebindOfTheList_KeepsItsKeyboard()
    {
        /* Replacing ItemsSource discards the focused row container; Rebind hands the focus to the list. */
        foreach (var file in Directory.EnumerateFiles(ViewerDir(), "MainWindow*.cs"))
        {
            Assert.DoesNotContain("ServerList.ItemsSource =", File.ReadAllText(file), StringComparison.Ordinal);
        }

        var rebinds = Directory.EnumerateFiles(ViewerDir(), "MainWindow*.cs")
            .Sum(f => Regex.Matches(File.ReadAllText(f), @"ServerListKeyboard\.Rebind\(ServerList, _fleet\.Visible\)").Count);
        Assert.Equal(5, rebinds);
    }

    [Fact]
    public void TheSearchBox_ReselectsTheSelectedServer_ById_UnderTheSuppressionGuard()
    {
        var code = Read("MainWindow.xaml.cs");
        var handler = Regex.Match(code, @"private void ServerSearchBox_TextChanged\(.*?\n    \}", RegexOptions.Singleline);
        Assert.True(handler.Success, "ServerSearchBox_TextChanged exists");
        var body = handler.Value;

        /* Read the selected server's id before the filter changes, find it among the rows still showing, and select it
           with the guard up so the tabs are not reloaded for a selection that did not move. */
        var read = body.IndexOf("(ServerList.SelectedItem as FleetServerRow)?.Server.ServerId", StringComparison.Ordinal);
        var filter = body.IndexOf("_fleet.SetSearch(", StringComparison.Ordinal);
        var guardUp = body.IndexOf("_suppressSidebarSelection = true;", StringComparison.Ordinal);
        var rebind = body.IndexOf("ServerListKeyboard.Rebind(ServerList, _fleet.Visible)", StringComparison.Ordinal);
        var reselect = body.IndexOf("ServerList.SelectedItem = ServerListKeyboard.FindByIdentity(", StringComparison.Ordinal);
        var guardDown = body.IndexOf("_suppressSidebarSelection = false;", StringComparison.Ordinal);

        Assert.True(read >= 0 && read < filter, "the selected id is read before the filter changes");
        Assert.True(filter < guardUp && guardUp < rebind && rebind < reselect && reselect < guardDown,
            "SetSearch, then guard up, rebind, reselect by id, guard down");
        Assert.Contains("r => r.Server.ServerId", body, StringComparison.Ordinal);
    }

    private static string Read(string file) => File.ReadAllText(Path.Combine(ViewerDir(), file)).Replace("\r\n", "\n");

    private static string ViewerDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer"));
}
