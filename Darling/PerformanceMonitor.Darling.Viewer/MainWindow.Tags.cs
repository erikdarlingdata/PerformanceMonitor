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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Fleet tags on the sidebar: loading the tag forest into <see cref="FleetView"/>, the tag-header context
/// menu (create / rename / delete / nest), and the server row's quick "Assign Tags" submenu. Bulk work
/// lives in <see cref="ManageTagsWindow"/>; this file is the inline, one-click surface.
/// </summary>
public partial class MainWindow
{
    /// <summary>Four levels: a tag at depth 0..2 may take a child; depth 3 (the fourth) may not.</summary>
    private const int MaxTagDepth = 4;

    private List<DarlingTag> _tags = new();

    /// <summary>serverId → its assigned tag ids, for the quick submenu's check marks.</summary>
    private Dictionary<int, HashSet<int>> _serverTagIds = new();

    /// <summary>Guards the one-time restore of persisted collapse state on first tag load.</summary>
    private bool _collapseRestored;

    /// <summary>
    /// Loads the tag forest and assignments, feeds the sidebar projection, and rebinds — preserving the
    /// current server selection. Safe to call on the initial load and after any mutation. Failures are
    /// logged and left non-fatal: the sidebar simply stays flat (tags are opt-in) rather than breaking.
    /// </summary>
    private async Task LoadTagsAsync()
    {
        if (_dataService is null)
        {
            return;
        }

        try
        {
            var tags = await _dataService.GetServerTagsAsync();
            var assignments = await _dataService.GetServerTagAssignmentsAsync();

            _tags = tags;
            _serverTagIds = new Dictionary<int, HashSet<int>>();
            foreach (var a in assignments)
            {
                if (!_serverTagIds.TryGetValue(a.ServerId, out var set))
                {
                    set = new HashSet<int>();
                    _serverTagIds[a.ServerId] = set;
                }

                set.Add(a.TagId);
            }

            /* Restore persisted expand/collapse state once, before the first projection with tags, so a
               reopened viewer shows the groups the way the user left them. Stale keys (a deleted tag) are
               ignored by TryParse. */
            if (!_collapseRestored)
            {
                var restored = new List<FleetGroupKey>();
                foreach (var stored in _preferences.CollapsedFleetGroups)
                {
                    if (FleetGroupKey.TryParse(stored, out var key))
                    {
                        restored.Add(key);
                    }
                }

                _fleet.SetCollapsedKeys(restored);
                _collapseRestored = true;
            }

            /* Re-projecting keeps the same server selected, so suppress the selection round-trip — the
               aggregate tabs are already showing that server and need no reload. */
            var previous = (ServerList.SelectedItem as FleetServerRow)?.Server.ServerId;
            _suppressSidebarSelection = true;
            try
            {
                _fleet.SetTags(tags, assignments);
                ServerList.ItemsSource = _fleet.Visible;
                ServerList.SelectedItem = _fleet.ResolveSelection(previous);
            }
            finally
            {
                _suppressSidebarSelection = false;
            }

            /* Tag names/colours/assignments may have changed — refresh the Overview pills if it's showing
               cards, so a colour edit or (un)assignment reflects there without waiting for a full reload. */
            RestampOverviewTagPills();
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("Tags", "Failed to load fleet tags", ex);
        }
    }

    /// <summary>Saves the current collapse state to the viewer preferences (called after each toggle).</summary>
    private void PersistCollapseState()
    {
        _preferences.CollapsedFleetGroups = _fleet.CollapsedKeys.Select(k => k.ToStorageString()).ToList();

        /* #2434: Save answers with a bool now instead of throwing, and this is the write that most needed
           the guard behind it — collapsing a sidebar group is a whole-file rewrite of viewer-preferences.json
           and nobody thinks of it as a save. Deliberately log-only rather than a dialog: a modal every time
           a group collapses would be worse than the thing it reports, and the collapse state is the one
           setting the next launch visibly re-states for you. The catch stays for anything Save's own
           best-effort failure path cannot reach — a sidebar click must not take the viewer down. */
        try
        {
            if (!_preferencesStore.Save(_preferences))
            {
                ViewerLogger.Warn("Tags",
                    "The sidebar collapse state was not saved; the groups will be back as they were on the next launch.");
            }
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("Tags", "Failed to persist tag collapse state", ex);
        }
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

    /// <summary>
    /// True when this seat may EDIT tags. Tags are shared fleet configuration, so editing follows
    /// the same rule as every other write surface: gate on <see cref="ViewerDataService.IsReadOnly"/>
    /// (#2008 — the tag menus shipped without the gate, so a least-privilege viewer-role seat showed
    /// working-looking editors whose writes then died as 42501 behind a status-bar line; the role
    /// model is deliberate — the viewer role's ONLY write is config.custom_views — so the fix is the
    /// missing gate, not a broader grant).
    /// </summary>
    private bool CanEditTags => _dataService?.IsReadOnly == false;

    /// <summary>Tooltip shown on disabled tag editors, so the read-only seat is TOLD why (#2008's
    /// complaint was the silence, and greying-out without a reason is only half an answer).</summary>
    private const string ReadOnlyTagToolTip =
        "This seat is connected with the read-only viewer role — tag editing needs the admin connection.";

    /// <summary>Disables the tag-only actions on the Favorites / Untagged pseudo-groups, which have no
    /// tag — and EVERY action here when the seat is read-only (all five entries mutate the tag tree or
    /// open the bulk editor, which is write-only).</summary>
    private void TagHeader_ContextMenuOpening(object sender, ContextMenuEventArgs e)
    {
        if (sender is not FrameworkElement fe || fe.DataContext is not FleetHeaderRow header || fe.ContextMenu is null)
        {
            return;
        }

        var isRealTag = header.Kind == FleetGroupKind.Tag;
        var canEdit = CanEditTags;
        foreach (var item in fe.ContextMenu.Items.OfType<MenuItem>())
        {
            /* Matched on Tag, not on the header text: the headers carry Alt mnemonics now, and a
               display string is the wrong key for behavior to hang on. */
            var tagOnly = (item.Tag as string) == "TagOnly";
            item.IsEnabled = canEdit && (!tagOnly || isRealTag);
            item.ToolTip = canEdit ? null : ReadOnlyTagToolTip;
            ToolTipService.SetShowOnDisabled(item, true);
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

        if (TagDepth(header.Tag.Id) >= MaxTagDepth - 1)
        {
            StatusText.Text = "Tags can nest at most four levels deep.";
            return;
        }

        var name = PromptTagName("New Child Tag", $"New tag under ‘{header.Tag.Name}’:");
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

        try
        {
            await _dataService.RenameServerTagAsync(header.Tag.Id, name);
            await LoadTagsAsync();
            StatusText.Text = $"Renamed tag to ‘{name}’.";
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("Tags", "Rename tag failed", ex);
            StatusText.Text = $"Could not rename the tag: {ex.Message}";
        }
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
            $"Delete ‘{header.Tag.Name}’{childNote}? Server assignments are removed; collected data is not affected.",
            "Delete Tag", MessageBoxButton.OKCancel, MessageBoxImage.Warning);
        if (confirm != MessageBoxResult.OK)
        {
            return;
        }

        try
        {
            await _dataService.DeleteServerTagAsync(header.Tag.Id);
            await LoadTagsAsync();
            StatusText.Text = $"Deleted tag ‘{header.Tag.Name}’.";
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("Tags", "Delete tag failed", ex);
            StatusText.Text = $"Could not delete the tag: {ex.Message}";
        }
    }

    private void ManageTags_Click(object sender, RoutedEventArgs e)
    {
        if (_dataService is null)
        {
            return;
        }

        /* Backstop for any entry point the menu gates miss: the bulk editor is write-only, so a
           read-only seat gets the reason instead of a window full of dead buttons (#2008). */
        if (!CanEditTags)
        {
            StatusText.Text = ReadOnlyTagToolTip;
            return;
        }

        var dialog = new ManageTagsWindow(_dataService, _fleet.All) { Owner = this };
        dialog.ShowDialog();
        if (dialog.ChangedAny)
        {
            _ = LoadTagsAsync();
        }
    }

    private async Task CreateTagAndReloadAsync(string name, int? parentId)
    {
        if (_dataService is null)
        {
            return;
        }

        try
        {
            await _dataService.CreateServerTagAsync(name, parentId);
            await LoadTagsAsync();
            StatusText.Text = $"Created tag ‘{name}’.";
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("Tags", "Create tag failed", ex);
            StatusText.Text = $"Could not create the tag: {ex.Message}";
        }
    }

    /// <summary>0-based nesting depth of a tag, walking the ParentId chain (guarded against a bad cycle).</summary>
    private int TagDepth(int tagId)
    {
        var byId = _tags.ToDictionary(t => t.Id);
        var depth = 0;
        var current = byId.TryGetValue(tagId, out var start) ? start : null;
        var guard = 0;
        while (current?.ParentId is int parentId
               && byId.TryGetValue(parentId, out var parent)
               && guard++ < MaxTagDepth + 2)
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
    /// Rebuilds the "Assign Tags" submenu for the server whose row was right-clicked: one checkable item
    /// per tag (indented by depth, checked when assigned). Populated on open so it always reflects the
    /// live tag set; empty state offers a jump to Manage Tags.
    /// </summary>
    private void ServerRow_ContextMenuOpening(object sender, ContextMenuEventArgs e)
    {
        if (sender is not FrameworkElement fe || fe.DataContext is not FleetServerRow row || fe.ContextMenu is null)
        {
            return;
        }

        /* #2031: Silence / Unsilence are mutually exclusive — DISABLE the inapplicable one (never hide it;
           Lite's semantics, and the same disabled-with-reason idiom as the read-only tag gate below). Driven
           from the polled IsSilenced flag so opening the menu costs no store read; the click handlers re-check
           the live rules anyway, so a stale flag degrades to a status-bar note, never a wrong write. Runs
           BEFORE the tag gating's early returns so the pair is gated on every seat. */
        var silenceItem = fe.ContextMenu.Items.OfType<MenuItem>().FirstOrDefault(m => (m.Tag as string) == "SilenceServer");
        var unsilenceItem = fe.ContextMenu.Items.OfType<MenuItem>().FirstOrDefault(m => (m.Tag as string) == "UnsilenceServer");
        if (silenceItem is not null && unsilenceItem is not null)
        {
            var silenced = row.Server.IsSilenced;

            silenceItem.IsEnabled = !silenced;
            silenceItem.ToolTip = silenced
                ? "Already silenced — use Unsilence to restore alert delivery"
                : "Mute every alert for this server until you Unsilence it";
            ToolTipService.SetShowOnDisabled(silenceItem, true);

            unsilenceItem.IsEnabled = silenced;
            unsilenceItem.ToolTip = silenced
                ? "Remove this server's whole-server alert silence"
                : "Not silenced — there is nothing to remove";
            ToolTipService.SetShowOnDisabled(unsilenceItem, true);
        }

        /* Located by Tag, not by header text — the header carries an Alt mnemonic now. */
        var assignItem = fe.ContextMenu.Items.OfType<MenuItem>().FirstOrDefault(m => (m.Tag as string) == "AssignTags");
        if (assignItem is null)
        {
            return;
        }

        /* Read-only seat: assignment is a config.server_tag_map write, gated like every other write
           surface (#2008). Disabled-with-reason instead of built-then-failing. */
        if (!CanEditTags)
        {
            assignItem.Items.Clear();
            assignItem.IsEnabled = false;
            assignItem.ToolTip = ReadOnlyTagToolTip;
            ToolTipService.SetShowOnDisabled(assignItem, true);
            return;
        }

        assignItem.IsEnabled = true;
        assignItem.ToolTip = null;
        assignItem.Items.Clear();

        if (_tags.Count == 0)
        {
            assignItem.Items.Add(new MenuItem { Header = "No tags yet", IsEnabled = false });
            assignItem.Items.Add(new Separator());
            var manage = new MenuItem { Header = "_Manage Tags..." };
            manage.Click += ManageTags_Click;
            assignItem.Items.Add(manage);
            return;
        }

        var assigned = _serverTagIds.TryGetValue(row.Server.ServerId, out var set) ? set : new HashSet<int>();

        foreach (var (tag, depth) in EnumerateTagForest())
        {
            var item = new MenuItem
            {
                // A MenuItem header parses a single "_" as an access-key marker, so a tag named
                // "prod_east" would render as "prodeast" and claim Alt+E. "__" is WPF's escape for a
                // literal underscore (same guard as the wait-type headers in ViewerServerTab.DrillDown).
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
    private List<(DarlingTag Tag, int Depth)> EnumerateTagForest()
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
        var result = new List<(DarlingTag Tag, int Depth)>();

        void Walk(DarlingTag tag, int depth)
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

        try
        {
            /* A checkable MenuItem toggles IsChecked before Click fires, so it now holds the desired state. */
            if (item.IsChecked)
            {
                await _dataService.AssignServerTagAsync(new[] { target.ServerId }, target.TagId);
            }
            else
            {
                await _dataService.UnassignServerTagAsync(new[] { target.ServerId }, target.TagId);
            }

            await LoadTagsAsync();
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("Tags", "Assign/unassign failed", ex);
            StatusText.Text = $"Could not change tags: {ex.Message}";
        }
    }
}
