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
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Census of how the kept column filters (#5565) are wired in both desktop apps. The server scope is inherited from the
/// server tab's root (and follows the FinOps tab's own server picker), the grid's Name is the grid half of the key, and
/// the store file is installed at startup in each app. A filtered grid with no Name, or two filtered grids in one scope
/// with the same Name, would silently go unstored or overwrite each other, so this fails first.
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class ColumnFilterWiringCensusTests
{
    private static readonly Regex s_manager = new(@"new\s+DataGridFilterManager<[^>]+>\(\s*(?<grid>[^)\s]+)\s*\)", RegexOptions.Compiled);

    private sealed record Host(string App, string Directory, string Stem);

    /// <summary>The controls whose grids sit under a scope: each server tab, and the FinOps tab (its own picker sets it).</summary>
    private static readonly Host[] s_scopedHosts =
    {
        new("Lite", "Lite/Controls", "ServerTab"),
        new("Lite", "Lite/Controls", "FinOpsTab"),
        new("Viewer", "Darling/PerformanceMonitor.Darling.Viewer", "ViewerServerTab"),
        new("Viewer", "Darling/PerformanceMonitor.Darling.Viewer", "FinOpsTab"),
    };

    private static IEnumerable<string> CodeFiles(Host host) =>
        Directory.GetFiles(PathTo(host.Directory.Split('/')), host.Stem + "*.cs")
            .Where(f => Path.GetFileName(f).StartsWith(host.Stem + ".", StringComparison.Ordinal));

    private static List<string> GridsOf(Host host) =>
        CodeFiles(host)
            .SelectMany(f => s_manager.Matches(File.ReadAllText(f)).Select(m => m.Groups["grid"].Value))
            .ToList();

    [Fact]
    public void Every_filtered_grid_under_a_scope_is_named_in_its_controls_xaml()
    {
        var problems = new List<string>();
        foreach (var host in s_scopedHosts)
        {
            var xaml = File.ReadAllText(PathTo(host.Directory.Split('/').Append(host.Stem + ".xaml").ToArray()));
            var grids = GridsOf(host);
            Assert.True(grids.Count > 0, $"{host.App} {host.Stem}: no filter managers found, so the census reads nothing");
            foreach (var grid in grids)
            {
                if (!Regex.IsMatch(xaml, $"x:Name=\"{Regex.Escape(grid)}\""))
                    problems.Add($"{host.App} {host.Stem}: grid '{grid}' has no x:Name in {host.Stem}.xaml, so its filters would not be kept");
            }
        }
        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void Filtered_grids_sharing_a_server_scope_have_distinct_names()
    {
        foreach (var app in s_scopedHosts.Select(h => h.App).Distinct())
        {
            var names = s_scopedHosts.Where(h => h.App == app).SelectMany(GridsOf).ToList();
            var duplicates = names.GroupBy(n => n).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
            Assert.True(duplicates.Count == 0, $"{app}: grids with the same Name under one server scope share one stored entry: {string.Join(", ", duplicates)}");
        }
    }

    [Fact]
    public void Each_server_tab_sets_its_scope_and_each_FinOps_picker_follows_it()
    {
        Assert.Contains("ColumnFilterScope.SetServer(this, server.Id);", ReadRepoFile("Lite", "Controls", "ServerTab.xaml.cs"), StringComparison.Ordinal);
        Assert.Contains("ColumnFilterScope.SetServer(this, _server.ServerName);", ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml.cs"), StringComparison.Ordinal);

        var liteFinOps = ReadRepoFile("Lite", "Controls", "FinOpsTab.xaml.cs");
        var viewerFinOps = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml.cs");
        Assert.Contains("ColumnFilterScope.SetServer(this, (ServerSelector.SelectedItem as ServerConnection)?.Id);", liteFinOps, StringComparison.Ordinal);
        Assert.Contains("ColumnFilterScope.SetServer(this, (ServerSelector.SelectedItem as DarlingServer)?.ServerName);", viewerFinOps, StringComparison.Ordinal);

        /* the scope follows the picker before the early return for a same-server repopulation */
        Assert.True(liteFinOps.IndexOf("ColumnFilterScope.SetServer", StringComparison.Ordinal) < liteFinOps.IndexOf("if (_populatingServers) return;", StringComparison.Ordinal));
        Assert.True(viewerFinOps.IndexOf("ColumnFilterScope.SetServer", StringComparison.Ordinal) < viewerFinOps.IndexOf("if (_populatingServers)", StringComparison.Ordinal));
    }

    [Fact]
    public void Each_app_installs_the_store_beside_its_own_settings_file()
    {
        var lite = ReadRepoFile("Lite", "App.xaml.cs");
        Assert.Contains("ColumnFilterStore.Install(Path.Combine(ConfigDirectory, \"column-filters.json\")", lite, StringComparison.Ordinal);

        var viewer = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "App.xaml.cs");
        Assert.Contains("ColumnFilterStore.Install(", viewer, StringComparison.Ordinal);
        /* The Viewer keeps the file under LOCAL application data like Lite; the roaming place is only the one an earlier
           build used, named so the file there is moved over once. */
        Assert.Contains("Environment.SpecialFolder.LocalApplicationData), \"PerformanceMonitorDarling\", \"column-filters.json\"", viewer, StringComparison.Ordinal);
        Assert.Contains("Path.GetDirectoryName(ViewerPreferencesStore.DefaultFilePath())!, \"column-filters.json\")", viewer, StringComparison.Ordinal);
    }

    [Fact]
    public void The_popup_controller_hands_the_manager_s_values_and_clear_all_to_the_popup()
    {
        var controller = ReadRepoFile("PerformanceMonitor.Ui", "ColumnFilterPopupController.cs");
        Assert.Contains("manager.GetValueCatalog(columnName)", controller, StringComparison.Ordinal);
        Assert.Contains("manager.ClearAllFilters()", controller, StringComparison.Ordinal);
    }

    [Fact]
    public void Lite_and_the_Viewer_both_reach_the_shared_filter_code()
    {
        /* Both apps filter through PerformanceMonitor.Ui's manager and popup; neither carries its own copy. */
        foreach (var dir in new[] { "Lite", "Darling/PerformanceMonitor.Darling.Viewer" })
        {
            var own = Directory.GetFiles(PathTo(dir.Split('/')), "*.cs", SearchOption.AllDirectories)
                .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                .Where(f => Regex.IsMatch(File.ReadAllText(f), @"class\s+(DataGridFilterManager|ColumnFilterPopup|ColumnFilterMatcher)\b"))
                .ToList();
            Assert.True(own.Count == 0, $"{dir} carries its own filter class: {string.Join(", ", own)}");
        }
    }

    [Fact]
    public void Alert_History_and_Job_History_set_the_one_all_servers_scope_in_both_apps()
    {
        /* One fixed scope for a cross-server list, set in the constructor before any filter manager is built, so a
           restart finds the filters again (#5565). Without it the grid has no scope and nothing is kept. */
        var files = new[]
        {
            ReadRepoFile("Lite", "Controls", "AlertsHistoryTab.xaml.cs"),
            ReadRepoFile("Lite", "Controls", "JobHistoryTab.xaml.cs"),
            ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AlertsHistoryTab.xaml.cs"),
            ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "JobHistoryTab.xaml.cs"),
        };
        foreach (var code in files)
        {
            var scope = code.IndexOf("ColumnFilterScope.SetServer(this, ColumnFilterScope.AllServers);", StringComparison.Ordinal);
            var manager = code.IndexOf("new DataGridFilterManager<", StringComparison.Ordinal);
            Assert.True(scope > 0 && manager > scope, "a cross-server history tab must set ColumnFilterScope.AllServers before it builds its filter manager");
        }
    }

    [Fact]
    public void The_three_HistoryDataGrid_windows_in_each_app_have_no_scope()
    {
        /* Three drill-down windows per app share the grid name HistoryDataGrid, so a scope would make them overwrite
           each other's filters. They stay unsaved: no ColumnFilterScope anywhere in them. */
        foreach (var dir in new[] { "Lite/Windows", "Darling/PerformanceMonitor.Darling.Viewer" })
        {
            var windows = Directory.GetFiles(PathTo(dir.Split('/')), "*.xaml")
                .Where(f => File.ReadAllText(f).Contains("x:Name=\"HistoryDataGrid\"", StringComparison.Ordinal))
                .ToList();
            Assert.Equal(3, windows.Count);
            foreach (var xaml in windows)
            {
                var code = File.ReadAllText(xaml + ".cs");
                Assert.Contains("new DataGridFilterManager<", code, StringComparison.Ordinal);
                Assert.DoesNotContain("ColumnFilterScope", code, StringComparison.Ordinal);
                Assert.DoesNotContain("ColumnFilterScope", File.ReadAllText(xaml), StringComparison.Ordinal);
            }
        }
    }
}
