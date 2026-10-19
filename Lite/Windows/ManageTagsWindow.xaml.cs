/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common.Tags;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>A node in the Manage Tags tree — a tag plus its children and its 0-based nesting depth.</summary>
public sealed class TagNode
{
    public TagNode(ServerTag tag, int depth)
    {
        Tag = tag;
        Depth = depth;
    }

    public ServerTag Tag { get; }

    public int Depth { get; }

    /// <summary>Whether a child tag may be added under this node: the nesting limit is the shared
    /// <see cref="TagTreeRules"/> one (#5978), the same as Darling's server tags.</summary>
    public bool CanAddChild => TagTreeRules.CanHaveChildren(Depth);

    public string Name => Tag.Name;

    /// <summary>The tag's colour as a brush for the tree swatch (neutral when the tag has no colour).</summary>
    public System.Windows.Media.Brush SwatchBrush => TagColorBrushes.Fill(Tag.Colour);

    public ObservableCollection<TagNode> Children { get; } = new();

    /// <summary>Bound two-way to the tree item, so a rebuilt tree can re-select the tag the operator was on
    /// (a Move Up or Move Down rebuilds every node).</summary>
    public bool IsSelected { get; set; }
}

/// <summary>A server row in the assignment checklist — its membership in the selected tag is
/// <see cref="IsChecked"/>, two-way bound so a click flips it before the handler persists it.</summary>
public sealed class ServerCheckItem : INotifyPropertyChanged
{
    private bool _isChecked;

    public ServerCheckItem(int serverId, string displayName, bool isChecked)
    {
        ServerId = serverId;
        DisplayName = displayName;
        _isChecked = isChecked;
    }

    public int ServerId { get; }

    public string DisplayName { get; }

    public bool IsChecked
    {
        get => _isChecked;
        set
        {
            if (_isChecked != value)
            {
                _isChecked = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(IsChecked)));
            }
        }
    }

    public event PropertyChangedEventHandler? PropertyChanged;
}

/// <summary>
/// The bulk tag editor: a tag tree with create/rename/delete/nest on the left, and a searchable server
/// checklist for the selected tag on the right. Every action persists immediately through
/// <see cref="LocalDataService"/> (no dirty-tracking), and <see cref="ChangedAny"/> tells the caller to
/// reload the sidebar tree on close. The 4-level nesting cap and cycle-safety are enforced here, mirroring
/// the store's intent — DuckDB cannot express either. Lite has no read-only seat, so unlike the Darling
/// viewer there is no privilege gate: the window always opens fully enabled.
/// </summary>
public partial class ManageTagsWindow : Window
{
    private readonly LocalDataService _dataService;
    private readonly IReadOnlyList<ServerConnection> _servers;

    private List<ServerTag> _tags = new();

    /// <summary>tagId → the servers currently assigned to it, kept in sync as toggles happen so switching
    /// tags reflects live membership without a round-trip.</summary>
    private Dictionary<int, HashSet<int>> _assignedServersByTag = new();

    private readonly ObservableCollection<ServerCheckItem> _serverItems = new();

    private TagNode? _selectedNode;

    /// <summary>True once anything changed, so the caller reloads the sidebar tree on close.</summary>
    public bool ChangedAny { get; private set; }

    public ManageTagsWindow(LocalDataService dataService, IReadOnlyList<ServerConnection> servers)
    {
        InitializeComponent();

        _dataService = dataService;
        _servers = servers;
        ServerCheckList.ItemsSource = _serverItems;

        Loaded += async (_, _) => await ReloadTagsAsync();
    }

    private async Task ReloadTagsAsync()
    {
        try
        {
            /* #5457: both reads run on the pool, not the dispatcher: each takes the store read lock, which an archive or
               compaction pass can hold, and the window would stop pumping input for as long. */
            _tags = await Task.Run(() => _dataService.GetServerTagsAsync());
            var assignments = await Task.Run(() => _dataService.GetServerTagAssignmentsAsync());

            _assignedServersByTag = new Dictionary<int, HashSet<int>>();
            foreach (var a in assignments)
            {
                if (!_assignedServersByTag.TryGetValue(a.TagId, out var set))
                {
                    set = new HashSet<int>();
                    _assignedServersByTag[a.TagId] = set;
                }

                set.Add(a.ServerId);
            }

            BuildTree();
            RefreshMembers();
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageTags", "Failed to load tags", ex);
            MessageBox.Show(this, $"Could not load tags: {ex.Message}", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void BuildTree()
    {
        var selectedId = _selectedNode?.Tag.Id;

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
            .ThenBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var visited = new HashSet<int>();

        TagNode Make(ServerTag tag, int depth)
        {
            visited.Add(tag.Id);
            var node = new TagNode(tag, depth);
            if (byParent.TryGetValue(tag.Id, out var kids))
            {
                foreach (var kid in kids)
                {
                    if (!visited.Contains(kid.Id))
                    {
                        node.Children.Add(Make(kid, depth + 1));
                    }
                }
            }

            return node;
        }

        var nodes = new List<TagNode>();
        foreach (var root in roots)
        {
            nodes.Add(Make(root, 0));
        }

        /* Cycle / disconnected safety, same as the sidebar projection: surface any unreached tag. */
        foreach (var tag in _tags)
        {
            if (!visited.Contains(tag.Id))
            {
                nodes.Add(Make(tag, 0));
            }
        }

        var reselect = selectedId is int id ? FindNode(nodes, id) : null;
        if (reselect is not null)
        {
            reselect.IsSelected = true;
        }

        TagTree.ItemsSource = nodes;

        _selectedNode = reselect;
        UpdateButtons();
    }

    private static TagNode? FindNode(IEnumerable<TagNode> nodes, int tagId)
    {
        foreach (var node in nodes)
        {
            if (node.Tag.Id == tagId)
            {
                return node;
            }

            var found = FindNode(node.Children, tagId);
            if (found is not null)
            {
                return found;
            }
        }

        return null;
    }

    private void TagTree_SelectedItemChanged(object sender, RoutedPropertyChangedEventArgs<object> e)
    {
        _selectedNode = e.NewValue as TagNode;
        UpdateButtons();
        RefreshMembers();
    }

    private void UpdateButtons()
    {
        var hasSelection = _selectedNode is not null;
        NewChildButton.IsEnabled = hasSelection && _selectedNode!.CanAddChild;
        RenameButton.IsEnabled = hasSelection;
        ColourButton.IsEnabled = hasSelection;
        DeleteButton.IsEnabled = hasSelection;
        MoveUpButton.IsEnabled = CanMoveSelected(TagMoveDirection.Up);
        MoveDownButton.IsEnabled = CanMoveSelected(TagMoveDirection.Down);
        CheckAllButton.IsEnabled = hasSelection;
        UncheckAllButton.IsEnabled = hasSelection;
    }

    /// <summary>Whether the selected tag has a sibling to swap with in <paramref name="direction"/>, from the
    /// siblings (same parent) in the order the tree shows them.</summary>
    private bool CanMoveSelected(TagMoveDirection direction)
    {
        if (_selectedNode is null)
        {
            return false;
        }

        var parentId = _selectedNode.Tag.ParentId;
        var siblings = _tags
            .Where(t => t.ParentId == parentId)
            .Select(t => new TagSibling(t.Id, t.Name, t.SortOrder))
            .ToList();
        return TagSiblingOrder.CanMove(siblings, _selectedNode.Tag.Id, direction);
    }

    private async void MoveUp_Click(object sender, RoutedEventArgs e) => await MoveSelectedAsync(TagMoveDirection.Up);

    private async void MoveDown_Click(object sender, RoutedEventArgs e) => await MoveSelectedAsync(TagMoveDirection.Down);

    /// <summary>Moves the selected tag one place among its siblings (#6094). The data service renumbers the
    /// siblings in one statement from a fresh read; the tree is rebuilt and the same tag stays selected.</summary>
    private async Task MoveSelectedAsync(TagMoveDirection direction)
    {
        if (_selectedNode is null)
        {
            return;
        }

        try
        {
            var movedId = _selectedNode.Tag.Id;
            var wrote = false;
            if (!await WriteWhileWaitingAsync(
                    "moving the tag", "Move tag",
                    async () => wrote = await Task.Run(() => _dataService.MoveServerTagAsync(movedId, direction))))
            {
                return;
            }

            if (wrote)
            {
                ChangedAny = true;
            }

            // Reload either way: a move that wrote nothing means the tree this window shows was already stale.
            await ReloadTagsAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageTags", "Move tag failed", ex);
            MessageBox.Show(this, $"Could not move the tag: {ex.Message}", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private void RefreshMembers()
    {
        _serverItems.Clear();

        if (_selectedNode is null)
        {
            MembersHeader.Text = "Select a tag to assign servers";
            return;
        }

        MembersHeader.Text = $"Servers in ‘{_selectedNode.Tag.Name}’";

        var assigned = _assignedServersByTag.TryGetValue(_selectedNode.Tag.Id, out var set)
            ? set
            : new HashSet<int>();

        var filter = ServerFilterBox.Text?.Trim() ?? string.Empty;

        foreach (var server in _servers.OrderBy(s => s.DisplayName, StringComparer.OrdinalIgnoreCase))
        {
            if (filter.Length > 0 && server.DisplayName.IndexOf(filter, StringComparison.OrdinalIgnoreCase) < 0)
            {
                continue;
            }

            var serverId = RemoteCollectorService.GetServerId(server);
            _serverItems.Add(new ServerCheckItem(serverId, server.DisplayName, assigned.Contains(serverId)));
        }
    }

    private async void ServerCheck_Click(object sender, RoutedEventArgs e)
    {
        if (_selectedNode is null || sender is not FrameworkElement fe || fe.DataContext is not ServerCheckItem item)
        {
            return;
        }

        await ApplyAssignmentAsync(item.ServerId, item.IsChecked);
    }

    private async Task ApplyAssignmentAsync(int serverId, bool assign)
    {
        if (_selectedNode is null)
        {
            return;
        }

        var tagId = _selectedNode.Tag.Id;
        try
        {
            if (!await WriteWhileWaitingAsync(
                    assign ? "assigning the tag" : "removing the tag from the server",
                    assign ? "Assign tag" : "Unassign tag",
                    () => assign
                        ? Task.Run(() => _dataService.AssignServerTagAsync(new[] { serverId }, tagId))
                        : Task.Run(() => _dataService.UnassignServerTagAsync(new[] { serverId }, tagId)),
                    reloadAfterBusy: true))
            {
                return;
            }

            if (!_assignedServersByTag.TryGetValue(tagId, out var set))
            {
                set = new HashSet<int>();
                _assignedServersByTag[tagId] = set;
            }

            if (assign)
            {
                set.Add(serverId);
            }
            else
            {
                set.Remove(serverId);
            }

            ChangedAny = true;
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageTags", "Failed to change a tag assignment", ex);
            MessageBox.Show(this, $"Could not change the assignment: {ex.Message}", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Error);
            await ReloadTagsAsync();
        }
    }

    private async void CheckAll_Click(object sender, RoutedEventArgs e) => await BulkSetAsync(true);

    private async void UncheckAll_Click(object sender, RoutedEventArgs e) => await BulkSetAsync(false);

    private async Task BulkSetAsync(bool assign)
    {
        if (_selectedNode is null)
        {
            return;
        }

        var tagId = _selectedNode.Tag.Id;
        var ids = _serverItems.Where(i => i.IsChecked != assign).Select(i => i.ServerId).ToList();
        if (ids.Count == 0)
        {
            return;
        }

        try
        {
            if (!await WriteWhileWaitingAsync(
                    assign ? "adding the servers to the tag" : "removing the servers from the tag",
                    assign ? "Assign tag to servers" : "Unassign tag from servers",
                    () => assign
                        ? Task.Run(() => _dataService.AssignServerTagAsync(ids, tagId))
                        : Task.Run(() => _dataService.UnassignServerTagAsync(ids, tagId)),
                    reloadAfterBusy: true))
            {
                return;
            }

            if (!_assignedServersByTag.TryGetValue(tagId, out var set))
            {
                set = new HashSet<int>();
                _assignedServersByTag[tagId] = set;
            }

            foreach (var id in ids)
            {
                if (assign)
                {
                    set.Add(id);
                }
                else
                {
                    set.Remove(id);
                }
            }

            foreach (var item in _serverItems)
            {
                item.IsChecked = assign;
            }

            ChangedAny = true;
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageTags", "Bulk tag assignment failed", ex);
            MessageBox.Show(this, $"Could not update assignments: {ex.Message}", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Error);
            await ReloadTagsAsync();
        }
    }

    /* #5650: a tag write can wait up to 30 s for the local database (LocalDataService.OpenUserWriteConnectionAsync) while
       archival or a collector write holds it. While it waits, the whole work area (tree, buttons and server list) is
       greyed out, so a second click cannot start another write, and the line at the bottom says what is happening.
       Close stays enabled. A busy store is reported with StoreBusyReport's wording ("nothing was changed", the holder
       when known) and one Warning line; the caller gets false and stops. Any other failure is rethrown to the caller's
       existing catch, which keeps the old Error line and message. The write is passed already wrapped in Task.Run
       (TabLoadsOffUiThreadTests reads that at the call), so the window stays responsive for the whole wait. */
    private async Task<bool> WriteWhileWaitingAsync(string doing, string logAction, Func<Task> write, bool reloadAfterBusy = false)
    {
        TagWorkArea.IsEnabled = false;
        WriteWaitText.Text = StoreBusyReport.WaitingText(doing);
        WriteWaitText.Visibility = Visibility.Visible;
        try
        {
            await write();
            return true;
        }
        catch (StoreLockTimeoutException ex)
        {
            var text = StoreBusyReport.Report("ManageTags", logAction, ex, doing);
            if (!IsLoaded)
            {
                return false;   // closed while it waited: nothing left to show or reload
            }

            MessageBox.Show(this, text, "Manage Tags", MessageBoxButton.OK, MessageBoxImage.Warning);
            if (reloadAfterBusy)
            {
                /* The check box was ticked before the write started; put it back to what the store holds. */
                await ReloadTagsAsync();
            }

            return false;
        }
        finally
        {
            WriteWaitText.Visibility = Visibility.Collapsed;
            TagWorkArea.IsEnabled = true;
        }
    }

    private void ServerFilterBox_TextChanged(object sender, TextChangedEventArgs e) => RefreshMembers();

    private async void NewRoot_Click(object sender, RoutedEventArgs e)
    {
        var name = PromptName("New Tag", "Tag name:");
        if (name is not null)
        {
            await CreateTagAsync(name, parentId: null);
        }
    }

    private async void NewChild_Click(object sender, RoutedEventArgs e)
    {
        if (_selectedNode is null)
        {
            return;
        }

        if (!_selectedNode.CanAddChild)
        {
            MessageBox.Show(this, "Tags can nest at most four levels deep.", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var name = PromptName("New Child Tag", $"New tag under ‘{_selectedNode.Tag.Name}’:");
        if (name is not null)
        {
            await CreateTagAsync(name, _selectedNode.Tag.Id);
        }
    }

    private async Task CreateTagAsync(string name, int? parentId)
    {
        try
        {
            if (!await WriteWhileWaitingAsync(
                    "creating the tag", "Create tag",
                    () => Task.Run(() => _dataService.CreateServerTagAsync(name, parentId))))
            {
                return;
            }

            ChangedAny = true;
            await ReloadTagsAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageTags", "Create tag failed", ex);
            MessageBox.Show(this, $"Could not create the tag: {ex.Message}", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private async void Rename_Click(object sender, RoutedEventArgs e)
    {
        if (_selectedNode is null)
        {
            return;
        }

        var name = PromptName("Rename Tag", "New name:", _selectedNode.Tag.Name);
        if (name is null || name == _selectedNode.Tag.Name)
        {
            return;
        }

        try
        {
            var renamedId = _selectedNode.Tag.Id;
            if (!await WriteWhileWaitingAsync(
                    "renaming the tag", "Rename tag",
                    () => Task.Run(() => _dataService.RenameServerTagAsync(renamedId, name))))
            {
                return;
            }

            ChangedAny = true;
            await ReloadTagsAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageTags", "Rename tag failed", ex);
            MessageBox.Show(this, $"Could not rename the tag: {ex.Message}", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private async void Colour_Click(object sender, RoutedEventArgs e)
    {
        if (_selectedNode is null)
        {
            return;
        }

        var dialog = new TagColorDialog(_selectedNode.Tag.Colour) { Owner = this };
        if (dialog.ShowDialog() != true)
        {
            return;   // cancelled — leave the colour as it was
        }

        try
        {
            var recolouredId = _selectedNode.Tag.Id;
            var colour = dialog.SelectedColour;
            if (!await WriteWhileWaitingAsync(
                    "setting the tag colour", "Set tag colour",
                    () => Task.Run(() => _dataService.SetServerTagColorAsync(recolouredId, colour))))
            {
                return;
            }

            ChangedAny = true;
            await ReloadTagsAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageTags", "Set tag colour failed", ex);
            MessageBox.Show(this, $"Could not set the tag colour: {ex.Message}", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private async void Delete_Click(object sender, RoutedEventArgs e)
    {
        if (_selectedNode is null)
        {
            return;
        }

        var descendants = CountDescendants(_selectedNode);
        var assignments = CountAssignedServers(_selectedNode);
        var childNote = descendants > 0 ? $" and its {descendants} child tag(s)" : string.Empty;
        var assignNote = assignments > 0 ? $" This removes {assignments} server assignment(s)." : string.Empty;

        var confirm = MessageBox.Show(this,
            $"Delete ‘{_selectedNode.Tag.Name}’{childNote}?{assignNote} Collected data is not affected.",
            "Delete Tag", MessageBoxButton.OKCancel, MessageBoxImage.Warning);
        if (confirm != MessageBoxResult.OK)
        {
            return;
        }

        try
        {
            var deletedId = _selectedNode.Tag.Id;
            if (!await WriteWhileWaitingAsync(
                    "deleting the tag", "Delete tag",
                    () => Task.Run(() => _dataService.DeleteServerTagAsync(deletedId))))
            {
                return;
            }

            _selectedNode = null;
            ChangedAny = true;
            await ReloadTagsAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageTags", "Delete tag failed", ex);
            MessageBox.Show(this, $"Could not delete the tag: {ex.Message}", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private static int CountDescendants(TagNode node)
    {
        var count = 0;
        foreach (var child in node.Children)
        {
            count += 1 + CountDescendants(child);
        }

        return count;
    }

    private int CountAssignedServers(TagNode node)
    {
        var ids = new HashSet<int>();

        void Collect(TagNode current)
        {
            if (_assignedServersByTag.TryGetValue(current.Tag.Id, out var set))
            {
                foreach (var id in set)
                {
                    ids.Add(id);
                }
            }

            foreach (var child in current.Children)
            {
                Collect(child);
            }
        }

        Collect(node);
        return ids.Count;
    }

    private string? PromptName(string title, string prompt, string initial = "")
    {
        var dialog = new TagNameDialog(title, prompt, initial) { Owner = this };
        return dialog.ShowDialog() == true ? dialog.TagName : null;
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
