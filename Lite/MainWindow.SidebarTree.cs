/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common.Tags;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;

namespace PerformanceMonitorLite;

/// <summary>
/// The sidebar tag-tree half of #2020 (stage 2b-i-b): binds the WPF-free <see cref="FleetView"/> projection
/// to <c>ServerListView</c>, so the flat server list becomes a Favorites → tags → Untagged tree once tags
/// exist, with inline assign (server rows) and tag CRUD (group headers). Ported from the Darling viewer's
/// <c>MainWindow.Tags.cs</c> sidebar wiring; the projection engine, row types, and template selector are the
/// Lite twins of the viewer's. Lite is single-connection with the user's own credentials, so there is no
/// read-only seat to gate (unlike the viewer's #2011 gate) — tag editing is simply enabled.
/// </summary>
public partial class MainWindow
{
    /// <summary>The single projection behind the sidebar. Consumers asking "what servers exist?" read
    /// <see cref="ServerManager.GetAllServers"/> (Lite's authoritative fleet), never the bound list.</summary>
    private readonly FleetView _fleet = new();

    /// <summary>Guards every programmatic selection change so the <see cref="ServerListView_SelectionChanged"/>
    /// handler does not re-enter while we rebind or restore selection.</summary>
    private bool _suppressSidebarSelection;

    /// <summary>Collapse state is restored from settings once, before the first tag projection.</summary>
    private bool _collapseRestored;

    /// <summary>Adapts a <see cref="ServerConnection"/> to what the projection keys on: the deterministic
    /// storage id the tag map uses (NOT the local GUID), the display label, the favourite flag, and the
    /// backing connection the row and its context-menu actions reach through.</summary>
    private static FleetServer ToFleetServer(ServerConnection s) => new(
        RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(s)),
        s.DisplayNameWithIntent,
        s.IsFavorite,
        s);

    /// <summary>
    /// Folds the last-loaded tags (<see cref="_tags"/> / <see cref="_serverTagIds"/>) into the projection and
    /// rebinds the sidebar, restoring persisted collapse state on the first call and re-selecting the given
    /// server. Runs under the suppression guard so the rebind cannot re-enter the selection handler.
    /// </summary>
    private void ApplyFleetTagsAndRebind(int? previousServerId)
    {
        _suppressSidebarSelection = true;
        try
        {
            if (!_collapseRestored)
            {
                var restored = new List<FleetGroupKey>();
                foreach (var stored in App.CollapsedFleetGroups)
                {
                    if (FleetGroupKey.TryParse(stored, out var key))
                    {
                        restored.Add(key);
                    }
                }

                _fleet.SetCollapsedKeys(restored);
                _collapseRestored = true;
            }

            var assignments = _serverTagIds
                .SelectMany(kv => kv.Value.Select(tid => new ServerTagAssignment(kv.Key, tid)))
                .ToList();
            _fleet.SetTags(_tags, assignments);

            ServerListKeyboard.Rebind(ServerListView, _fleet.Visible);
            ServerListView.SelectedItem = _fleet.ResolveSelection(previousServerId);
        }
        finally
        {
            _suppressSidebarSelection = false;
        }
    }

    /// <summary>Reloads tags from the store, reprojects the sidebar, and re-stamps the Overview pills after a
    /// tag mutation (assign, rename, delete, create) — the lighter path than a full Overview refresh, since
    /// the server summaries themselves are unchanged.</summary>
    private async System.Threading.Tasks.Task ReloadTagsAndReprojectAsync()
    {
        var previousServerId = (ServerListView.SelectedItem as FleetServerRow)?.Server.ServerId;
        await LoadServerTagsAsync();
        ApplyFleetTagsAndRebind(previousServerId);

        if (_overviewSummaries.Count > 0)
        {
            StampTagPills(_overviewSummaries);
            ApplyOverviewView();
        }
    }

    /// <summary>A plain <see cref="ListBox"/> selects a header row when it is clicked; we intercept that,
    /// recover the server that was selected before, toggle the group's expand/collapse, and restore the
    /// selection to a server (never a header). A server-row selection needs no extra work in Lite — the
    /// per-server tab opens on double-click.</summary>
    private void ServerListView_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_suppressSidebarSelection)
        {
            return;
        }

        if (ServerListView.SelectedItem is FleetHeaderRow header)
        {
            var previousServerId = e.RemovedItems.OfType<FleetServerRow>().FirstOrDefault()?.Server.ServerId;
            ToggleGroup(header, previousServerId);
        }
    }

    private void ToggleGroup(FleetHeaderRow header, int? previousServerId)
    {
        _suppressSidebarSelection = true;
        try
        {
            _fleet.ToggleExpanded(header);
            ServerListKeyboard.Rebind(ServerListView, _fleet.Visible);
            ServerListView.SelectedItem = _fleet.ResolveSelection(previousServerId);
            PersistCollapseState();
        }
        finally
        {
            _suppressSidebarSelection = false;
        }
    }

    /// <summary>Persists the collapsed groups to <c>settings.json</c> (keyed by <see cref="FleetGroupKey"/>
    /// storage strings), so expand/collapse survives a restart — the Lite twin of the viewer's
    /// <c>ViewerPreferences.CollapsedFleetGroups</c>.</summary>
    private void PersistCollapseState()
    {
        var keys = _fleet.CollapsedKeys.Select(k => k.ToStorageString()).ToList();
        App.CollapsedFleetGroups = keys;
        App.WriteSetting("sidebar collapse state", root =>
            root["collapsed_fleet_groups"] = new JsonArray(keys.Select(k => (JsonNode?)JsonValue.Create(k)).ToArray()));
    }

    // ── Tag-header context menu (CRUD) ───────────────────────────────────────────────────────────────

    /// <summary>The <see cref="FleetHeaderRow"/> behind a header context-menu click (mirrors the server resolver).</summary>
    private static FleetHeaderRow? GetHeaderFromContextMenu(object sender)
    {
        if (sender is not MenuItem menuItem)
        {
            return null;
        }

        var contextMenu = menuItem.Parent as ContextMenu;
        var target = contextMenu?.PlacementTarget as FrameworkElement;
        return target?.DataContext as FleetHeaderRow;
    }

    /// <summary>Disables the tag-only actions (New Child / Rename / Delete) on the Favorites / Untagged
    /// pseudo-groups, which have no backing tag. New Tag and Manage Tags stay enabled on every header.</summary>
    private void TagHeader_ContextMenuOpening(object sender, ContextMenuEventArgs e)
    {
        if (sender is not FrameworkElement fe || fe.DataContext is not FleetHeaderRow header || fe.ContextMenu is null)
        {
            return;
        }

        var isRealTag = header.Kind == FleetGroupKind.Tag;
        foreach (var item in fe.ContextMenu.Items.OfType<MenuItem>())
        {
            /* Matched on Tag, not header text — the items carry Alt mnemonics, so the display string is the
               wrong key for behaviour to hang on. */
            var tagOnly = (item.Tag as string) == "TagOnly";
            item.IsEnabled = !tagOnly || isRealTag;
        }
    }

    private async void TagHeaderContextMenu_NewTag_Click(object sender, RoutedEventArgs e)
    {
        var name = PromptTagName("New Tag", "Tag name:");
        if (name is not null)
        {
            await CreateTagAndReloadAsync(name, parentId: null);
        }
    }

    private async void TagHeaderContextMenu_NewChildTag_Click(object sender, RoutedEventArgs e)
    {
        var header = GetHeaderFromContextMenu(sender);
        if (header?.Tag is null)
        {
            return;
        }

        if (!CanAddChildTag(_tags, header.Tag.Id))
        {
            StatusText.Text = "Tags can nest at most four levels deep.";
            return;
        }

        var name = PromptTagName("New Child Tag", $"New tag under '{header.Tag.Name}':");
        if (name is not null)
        {
            await CreateTagAndReloadAsync(name, header.Tag.Id);
        }
    }

    private async void TagHeaderContextMenu_Rename_Click(object sender, RoutedEventArgs e)
    {
        var header = GetHeaderFromContextMenu(sender);
        if (header?.Tag is null || _dataService is null)
        {
            return;
        }

        var name = PromptTagName("Rename Tag", "New name:", header.Tag.Name);
        if (name is null || name == header.Tag.Name)
        {
            return;
        }

        var tagId = header.Tag.Id;
        await RunTagWriteAsync(
            doing: "renaming the tag",
            logAction: "Rename tag",
            couldNot: "Could not rename the tag",
            done: $"Renamed tag to '{name}'.",
            write: () => Task.Run(() => _dataService.RenameServerTagAsync(tagId, name)));
    }

    private async void TagHeaderContextMenu_Delete_Click(object sender, RoutedEventArgs e)
    {
        var header = GetHeaderFromContextMenu(sender);
        if (header?.Tag is null || _dataService is null)
        {
            return;
        }

        var hasChildren = _tags.Any(t => t.ParentId == header.Tag.Id);
        var childNote = hasChildren ? " and all of its child tags" : string.Empty;

        var confirm = MessageBox.Show(this,
            $"Delete '{header.Tag.Name}'{childNote}? Server assignments are removed; collected data is not affected.",
            "Delete Tag", MessageBoxButton.OKCancel, MessageBoxImage.Warning);
        if (confirm != MessageBoxResult.OK)
        {
            return;
        }

        var tagId = header.Tag.Id;
        await RunTagWriteAsync(
            doing: "deleting the tag",
            logAction: "Delete tag",
            couldNot: "Could not delete the tag",
            done: $"Deleted tag '{header.Tag.Name}'.",
            write: () => Task.Run(() => _dataService.DeleteServerTagAsync(tagId)));
    }

    private async System.Threading.Tasks.Task CreateTagAndReloadAsync(string name, int? parentId)
    {
        if (_dataService is null)
        {
            return;
        }

        await RunTagWriteAsync(
            doing: "creating the tag",
            logAction: "Create tag",
            couldNot: "Could not create the tag",
            done: $"Created tag '{name}'.",
            write: () => Task.Run(() => _dataService.CreateServerTagAsync(name, parentId)));
    }

    /* #5650: a tag write started from the sidebar can wait up to 30 s for the local database
       (LocalDataService.OpenUserWriteConnectionAsync) while archival or a collector write holds it. The sidebar's
       entries are context-menu items, which close on click, so there is no control to grey out: the status bar says
       the write is waiting, and a second tag write started meanwhile is refused with a line saying so rather than
       queued behind the first. The flag is read and set on the dispatcher, so it needs no lock; each caller passes its
       write already wrapped in Task.Run (TabLoadsOffUiThreadTests reads that at the call), so the window stays responsive. A busy store shows StoreBusyReport's wording, which says
       nothing was changed, and writes one Warning line; any other failure keeps the old Error line and message. */
    private bool _tagWriteInFlight;

    private async Task RunTagWriteAsync(string doing, string logAction, string couldNot, string? done, Func<Task> write)
    {
        if (_tagWriteInFlight)
        {
            StatusText.Text = "Another tag change is still being saved. Please wait for it to finish.";
            return;
        }

        _tagWriteInFlight = true;
        StatusText.Text = StoreBusyReport.WaitingText(doing);
        try
        {
            await write();
            await ReloadTagsAndReprojectAsync();
            if (done is not null)
            {
                StatusText.Text = done;
            }
            else
            {
                StatusText.Text = string.Empty;
            }
        }
        catch (StoreLockTimeoutException ex)
        {
            StatusText.Text = StoreBusyReport.Report("Tags", logAction, ex, doing);
        }
        catch (Exception ex)
        {
            AppLogger.Error("Tags", $"{logAction} failed: {ex.Message}", ex);
            StatusText.Text = $"{couldNot}: {ex.Message}";
        }
        finally
        {
            _tagWriteInFlight = false;
        }
    }

    /// <summary>Whether the tag may take a child: the shared tag tree rules decide from its depth (#5978), the
    /// same call the Manage Tags editor makes.</summary>
    internal static bool CanAddChildTag(IReadOnlyList<ServerTag> tags, int tagId)
        => TagTreeRules.CanHaveChildren(TagDepth(tags, tagId));

    /// <summary>0-based nesting depth of a tag, walking the ParentId chain (guarded against a bad cycle).</summary>
    internal static int TagDepth(IReadOnlyList<ServerTag> tags, int tagId)
    {
        var byId = tags.ToDictionary(t => t.Id);
        var depth = 0;
        var current = byId.TryGetValue(tagId, out var start) ? start : null;
        var guard = 0;
        while (current?.ParentId is int parentId
               && byId.TryGetValue(parentId, out var parent)
               && guard++ < TagTreeRules.MaxDepth + 2)
        {
            depth++;
            current = parent;
        }

        return depth;
    }

    private string? PromptTagName(string title, string prompt, string initial = "")
    {
        var dialog = new TagNameDialog(title, prompt, initial) { Owner = this };
        return dialog.ShowDialog() == true ? dialog.TagName : null;
    }

    // ── Server-row quick "Assign Tags" submenu ───────────────────────────────────────────────────────

    /// <summary>
    /// Rebuilds the "Assign Tags" submenu for the server whose row was right-clicked: one checkable item per
    /// tag (indented by depth, checked when assigned). Populated on open so it always reflects the live tag
    /// set; empty state offers a jump to Manage Tags.
    /// </summary>
    private void ServerRow_ContextMenuOpening(object sender, ContextMenuEventArgs e)
    {
        if (sender is not FrameworkElement fe || fe.DataContext is not FleetServerRow row || fe.ContextMenu is null)
        {
            return;
        }

        /* Located by Tag, not by header text — the header carries an Alt mnemonic. */
        var assignItem = fe.ContextMenu.Items.OfType<MenuItem>().FirstOrDefault(m => (m.Tag as string) == "AssignTags");
        if (assignItem is null)
        {
            return;
        }

        assignItem.Items.Clear();

        if (_tags.Count == 0)
        {
            assignItem.Items.Add(new MenuItem { Header = "No tags yet", IsEnabled = false });
            assignItem.Items.Add(new Separator());
            var manage = new MenuItem { Header = "_Manage Tags..." };
            manage.Click += ManageTagsButton_Click;
            assignItem.Items.Add(manage);
            return;
        }

        var assigned = _serverTagIds.TryGetValue(row.Server.ServerId, out var set) ? set : new HashSet<int>();

        foreach (var (tag, depth) in EnumerateTagForest())
        {
            var item = new MenuItem
            {
                // A MenuItem header parses a single "_" as an access-key marker, so a tag named "prod_east"
                // would render as "prodeast" and claim Alt+E. "__" is WPF's escape for a literal underscore.
                Header = new string(' ', depth * 2) + tag.Name.Replace("_", "__"),
                IsCheckable = true,
                IsChecked = assigned.Contains(tag.Id),
                Tag = new AssignTarget(row.Server.ServerId, tag.Id)
            };
            item.Click += AssignTag_Click;
            assignItem.Items.Add(item);
        }
    }

    /// <summary>The tag forest in display order (roots by sort/name, each followed by its subtree), with
    /// depth — the order and indentation the quick submenu shows. Cycle-safe, like the sidebar projection.</summary>
    private List<(ServerTag Tag, int Depth)> EnumerateTagForest()
    {
        var byParent = _tags
            .Where(t => t.ParentId is not null)
            .GroupBy(t => t.ParentId!.Value)
            .ToDictionary(
                g => g.Key,
                g => g.OrderBy(t => t.SortOrder).ThenBy(t => t.Name, StringComparer.OrdinalIgnoreCase).ToList());

        var known = _tags.Select(t => t.Id).ToHashSet();
        var roots = _tags
            .Where(t => t.ParentId is null || !known.Contains(t.ParentId.Value))
            .OrderBy(t => t.SortOrder)
            .ThenBy(t => t.Name, StringComparer.OrdinalIgnoreCase);

        var visited = new HashSet<int>();
        var result = new List<(ServerTag Tag, int Depth)>();

        void Walk(ServerTag tag, int depth)
        {
            if (!visited.Add(tag.Id))
            {
                return;
            }

            result.Add((tag, depth));
            if (byParent.TryGetValue(tag.Id, out var kids))
            {
                foreach (var kid in kids)
                {
                    Walk(kid, depth + 1);
                }
            }
        }

        foreach (var root in roots)
        {
            Walk(root, 0);
        }

        foreach (var tag in _tags)
        {
            if (!visited.Contains(tag.Id))
            {
                Walk(tag, 0);
            }
        }

        return result;
    }

    private sealed record AssignTarget(int ServerId, int TagId);

    private async void AssignTag_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null || sender is not MenuItem item || item.Tag is not AssignTarget target)
        {
            return;
        }

        /* A checkable MenuItem toggles IsChecked before Click fires, so it now holds the desired state. */
        var assign = item.IsChecked;
        await RunTagWriteAsync(
            doing: assign ? "assigning the tag" : "removing the tag from the server",
            logAction: assign ? "Assign tag" : "Unassign tag",
            couldNot: "Could not change tags",
            done: null,
            write: () => assign
                ? Task.Run(() => _dataService.AssignServerTagAsync(new[] { target.ServerId }, target.TagId))
                : Task.Run(() => _dataService.UnassignServerTagAsync(new[] { target.ServerId }, target.TagId)));
    }
}
