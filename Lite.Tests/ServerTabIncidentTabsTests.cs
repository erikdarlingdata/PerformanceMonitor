/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Resources;
using System.Windows.Baml2006;
using System.Xaml;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5847: the tabs the Alert Details link and the chart and day drills select, read against the server tab Lite
/// actually ships. A <see cref="ServerTab"/> cannot be constructed in a test (its markup resolves theme keys that only
/// the running application holds), so the test reads the tab headers from the compiled markup in the Lite assembly and
/// checks that each named position lands on the tab the user is meant to see.
/// </summary>
public sealed class ServerTabIncidentTabsTests
{
    private static readonly Dictionary<string, List<string>> Tabs = ReadTabHeaders();

    [Theory]
    [InlineData(AlertChainView.Blocking, "Blocked Process Reports")]
    [InlineData(AlertChainView.Deadlocks, "Deadlocks")]
    public void IncidentTabs_LandOnTheGridTheAlertNames(AlertChainView view, string subTab)
    {
        var (mainTab, blockingSubTab) = ServerTab.IncidentTabs(view);

        Assert.Equal("Blocking", Tabs["MainTabControl"][mainTab]);
        Assert.Equal(subTab, Tabs["BlockingSubTabControl"][blockingSubTab]);
    }

    [Theory]
    [InlineData("MainTabControl", ServerTab.QueriesTabIndex, "Queries")]
    [InlineData("MainTabControl", ServerTab.BlockingTabIndex, "Blocking")]
    [InlineData("QueriesSubTabControl", ServerTab.ActiveQueriesSubTabIndex, "Active Queries")]
    [InlineData("QueriesSubTabControl", ServerTab.TopQueriesByDurationSubTabIndex, "Top Queries by Duration")]
    [InlineData("BlockingSubTabControl", ServerTab.BlockedProcessReportsSubTabIndex, "Blocked Process Reports")]
    [InlineData("BlockingSubTabControl", ServerTab.DeadlocksSubTabIndex, "Deadlocks")]
    public void NamedTabPositions_SelectTheTabTheirNameSays(string tabControl, int index, string header)
    {
        Assert.Equal(header, Tabs[tabControl][index]);
    }

    /// <summary>The string headers of each named TabControl's own TabItems, in order, from the compiled server tab.</summary>
    private static Dictionary<string, List<string>> ReadTabHeaders()
    {
        var assembly = typeof(ServerTab).Assembly;
        using var resources = assembly.GetManifestResourceStream(assembly.GetName().Name + ".g.resources")!;
        using var reader = new ResourceReader(resources);
        Stream? baml = null;
        foreach (DictionaryEntry entry in reader)
        {
            if ((string)entry.Key == "controls/servertab.baml")
            {
                baml = (Stream)entry.Value!;
            }
        }

        Assert.NotNull(baml);
        using var nodes = new Baml2006Reader(baml, new XamlReaderSettings { LocalAssembly = assembly });

        var result = new Dictionary<string, List<string>>();
        /* One frame per object: a TabControl frame collects its own items; a TabItem frame waits for its Header. */
        var frames = new Stack<Frame>();
        var members = new Stack<string?>();
        while (nodes.Read())
        {
            switch (nodes.NodeType)
            {
                case XamlNodeType.StartObject:
                    var frame = new Frame(nodes.Type?.Name);
                    if (frame.Type == "TabItem" && Nearest(frames, "TabControl") is { } owner)
                    {
                        owner.Items.Add(frame);
                    }

                    frames.Push(frame);
                    break;
                case XamlNodeType.GetObject:
                    frames.Push(new Frame(null));
                    break;
                case XamlNodeType.EndObject:
                    var done = frames.Pop();
                    if (done.Type == "TabControl" && done.Name is not null)
                    {
                        result[done.Name] = done.Items.ConvertAll(i => i.Header ?? "");
                    }

                    break;
                case XamlNodeType.StartMember:
                    members.Push(nodes.Member?.Name);
                    break;
                case XamlNodeType.EndMember:
                    members.Pop();
                    break;
                case XamlNodeType.Value when frames.Count > 0 && nodes.Value is string text:
                    var current = frames.Peek();
                    if (members.Peek() == "Name")
                    {
                        current.Name = text;
                    }
                    else if (members.Peek() == "Header" && current.Type == "TabItem")
                    {
                        current.Header = text;
                    }

                    break;
            }
        }

        return result;
    }

    private static Frame? Nearest(Stack<Frame> frames, string type)
    {
        foreach (var frame in frames)
        {
            if (frame.Type == type)
            {
                return frame;
            }
        }

        return null;
    }

    private sealed class Frame(string? type)
    {
        public string? Type { get; } = type;
        public string? Name { get; set; }
        public string? Header { get; set; }
        public List<Frame> Items { get; } = new();
    }
}
