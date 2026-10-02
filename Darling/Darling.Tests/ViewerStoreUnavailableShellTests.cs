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
using System.Xml.Linq;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins #4648: the store/config failure message covers only the content column, so the sidebar entries
/// that need no store stay reachable and the ones that do are disabled. No WPF types, so it runs anywhere.
/// </summary>
public sealed class ViewerStoreUnavailableShellTests
{
    private static readonly XNamespace Wpf = "http://schemas.microsoft.com/winfx/2006/xaml/presentation";
    private static readonly XNamespace X = "http://schemas.microsoft.com/winfx/2006/xaml";

    private static string ViewerDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer"));

    private static XDocument Xaml() => XDocument.Load(Path.Combine(ViewerDir(), "MainWindow.xaml"));

    private static string MainCs() => File.ReadAllText(Path.Combine(ViewerDir(), "MainWindow.xaml.cs"));

    private static string PlanViewerCs() => File.ReadAllText(Path.Combine(ViewerDir(), "MainWindow.PlanViewer.cs"));

    private static XElement Named(XDocument doc, string name) =>
        doc.Descendants().Single(e => (string?)e.Attribute(X + "Name") == name);

    /// <summary>Text from the member's signature line to the next member-closing brace at four spaces.</summary>
    private static string Body(string source, string signature)
    {
        int start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' not found");
        int end = source.IndexOf("\n    }", start, StringComparison.Ordinal);
        Assert.True(end > start, $"end of '{signature}' not found");
        return source[start..end];
    }

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    public void OverlayVisible_TruthTable(bool unavailable, bool planViewerShowing, bool expected) =>
        Assert.Equal(expected, StoreUnavailableShell.OverlayVisible(unavailable, planViewerShowing));

    [Fact]
    public void EverySidebarFooterButton_IsClassifiedExactlyOnce()
    {
        var footer = Named(Xaml(), "SidebarFooter");
        var handlers = footer.Descendants(Wpf + "Button")
            .Select(b => (string?)b.Attribute("Click")).Where(h => h != null).Select(h => h!).ToList();
        Assert.NotEmpty(handlers);

        foreach (string h in handlers)
        {
            int hits = (StoreUnavailableShell.StoreDependentSidebarHandlers.Contains(h) ? 1 : 0)
                + (StoreUnavailableShell.StoreIndependentSidebarHandlers.Contains(h) ? 1 : 0);
            Assert.True(hits == 1, $"footer handler {h} is in {hits} of the two lists");
        }

        Assert.Equal(
            StoreUnavailableShell.StoreDependentSidebarHandlers
                .Concat(StoreUnavailableShell.StoreIndependentSidebarHandlers).OrderBy(x => x, StringComparer.Ordinal),
            handlers.Distinct().OrderBy(x => x, StringComparer.Ordinal));
    }

    [Fact]
    public void EveryStoreDependentFooterButton_IsNamedAndDisabledByTheShell()
    {
        var footer = Named(Xaml(), "SidebarFooter");
        string body = Body(MainCs(), "private void ApplyStoreUnavailableShell");
        var dependent = footer.Descendants(Wpf + "Button")
            .Where(b => StoreUnavailableShell.StoreDependentSidebarHandlers.Contains((string?)b.Attribute("Click") ?? ""))
            .ToList();
        Assert.Equal(StoreUnavailableShell.StoreDependentSidebarHandlers.Count, dependent.Count);

        foreach (var b in dependent)
        {
            string? name = (string?)b.Attribute(X + "Name");
            Assert.False(string.IsNullOrEmpty(name), $"{(string?)b.Attribute("Click")} has no x:Name");
            Assert.Contains(name!, body);
        }

        Assert.Contains("ServerSearchRow", body);
        Assert.Contains("ServerList", body);
    }

    [Fact]
    public void MessageOverlay_CoversOnlyTheContentColumn()
    {
        var doc = Xaml();
        var overlay = Named(doc, "MessageOverlay");
        var parent = overlay.Parent!;
        Assert.Equal(Wpf + "Grid", parent.Name);
        Assert.Contains(parent.Element(Wpf + "Grid.ColumnDefinitions")!.Elements(),
            c => (string?)c.Attribute(X + "Name") == "SidebarColumn");
        Assert.Equal("1", (string?)overlay.Attribute("Grid.Column"));
        Assert.Null(overlay.Attribute("Grid.Row"));
        Assert.Same(overlay, parent.Elements().Last());
    }

    [Fact]
    public void ShowMessage_GoesThroughTheShell()
    {
        string body = Body(MainCs(), "private void ShowMessage(");
        Assert.Contains("_storeUnavailable = true", body);
        Assert.Contains("ApplyStoreUnavailableShell()", body);
        Assert.DoesNotContain("MessageOverlay.Visibility = Visibility.Visible", body);
    }

    [Fact]
    public void PlanViewerOpenAndClose_ReapplyTheShell()
    {
        string cs = PlanViewerCs();
        Assert.Contains("ApplyStoreUnavailableShell()", Body(cs, "private void OpenPlanViewerButton_Click"));
        Assert.Contains("ApplyStoreUnavailableShell()", Body(cs, "private void MainWindowPlanViewerClose_Click"));
    }

    [Fact]
    public void NoStoreRead_StartsWhileTheStoreIsUnavailable()
    {
        string cs = MainCs();
        string tab = Body(cs, "private async void MainTabs_SelectionChanged");
        int guard = tab.IndexOf("if (_storeUnavailable)", StringComparison.Ordinal);
        Assert.True(guard >= 0);
        Assert.True(guard < tab.IndexOf("RefreshVisibleAsync", StringComparison.Ordinal));

        string tick = Body(cs, "private async void OnRefreshTimerTick");
        int tickGuard = tick.IndexOf("if (_storeUnavailable)", StringComparison.Ordinal);
        Assert.True(tickGuard >= 0);
        Assert.True(tickGuard < tick.IndexOf("RefreshServerStatusAsync", StringComparison.Ordinal));
    }
}
