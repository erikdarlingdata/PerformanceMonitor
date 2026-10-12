/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>A node in the Manage Tags tree — a tag plus its children and its 0-based nesting depth.</summary>
public sealed class TagNode
{
    public TagNode(DarlingTag tag, int depth)
    {
        Tag = tag;
        Depth = depth;
    }

    public DarlingTag Tag { get; }

    public int Depth { get; }

    public string Name => Tag.Name;

    /// <summary>The tag's colour as a brush for the tree swatch (neutral when the tag has no colour).</summary>
    public System.Windows.Media.Brush SwatchBrush => TagColorBrushes.Fill(Tag.Colour);

    public ObservableCollection<TagNode> Children { get; } = new();
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
/// <see cref="ViewerDataService"/> (no dirty-tracking), and <see cref="ChangedAny"/> tells the caller to
/// reload the sidebar tree on close. The 4-level nesting cap and cycle-safety are enforced here, mirroring
/// the store's intent — Postgres cannot express either.
/// </summary>
public partial class ManageTagsWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly IReadOnlyList<DarlingServer> _servers;

    private List<DarlingTag> _tags = new();

    /// <summary>tagId → the servers currently assigned to it, kept in sync as toggles happen so switching
    /// tags reflects live membership without a round-trip.</summary>
    private Dictionary<int, HashSet<int>> _assignedServersByTag = new();

    private readonly ObservableCollection<ServerCheckItem> _serverItems = new();

    private TagNode? _selectedNode;

    /// <summary>True once anything changed, so the caller reloads the sidebar tree on close.</summary>
    public bool ChangedAny { get; private set; }

    public ManageTagsWindow(ViewerDataService dataService, IReadOnlyList<DarlingServer> servers)
    {
        InitializeComponent();

        _dataService = dataService;
        _servers = servers;
        ServerCheckList.ItemsSource = _serverItems;

        /* Defense-in-depth for the #2008 read-only gate: every CURRENT entry point checks
           CanEditTags before opening this window, but this window is all writes — a future entry
           point that forgets the gate must land on inert controls with the reason in the title,
           not on working-looking buttons whose writes die as 42501. Generic on purpose (whole
           content tree), so it cannot drift as buttons are added. */
        if (dataService.IsReadOnly)
        {
            Title += " — read-only seat (tag editing needs the admin connection)";
            if (Content is FrameworkElement root)
            {
                root.IsEnabled = false;
            }

            return;
        }

        Loaded += async (_, _) => await ReloadTagsAsync();
    }

    private async Task ReloadTagsAsync()
    {
        try
        {
            _tags = await _dataService.GetServerTagsAsync();
            var assignments = await _dataService.GetServerTagAssignmentsAsync();

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
            ViewerLogger.Error("ManageTags", "Failed to load tags", ex);
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

        TagNode Make(DarlingTag tag, int depth)
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

        TagTree.ItemsSource = nodes;

        _selectedNode = selectedId is int id ? FindNode(nodes, id) : null;
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
        NewChildButton.IsEnabled = hasSelection && _selectedNode!.Depth < ServerTagRules.MaxDepth - 1;
        RenameButton.IsEnabled = hasSelection;
        ColourButton.IsEnabled = hasSelection;
        DeleteButton.IsEnabled = hasSelection;
        CheckAllButton.IsEnabled = hasSelection;
        UncheckAllButton.IsEnabled = hasSelection;
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

            _serverItems.Add(new ServerCheckItem(server.ServerId, server.DisplayName, assigned.Contains(server.ServerId)));
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
            if (assign)
            {
                await _dataService.AssignServerTagAsync(new[] { serverId }, tagId);
            }
            else
            {
                await _dataService.UnassignServerTagAsync(new[] { serverId }, tagId);
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
            ViewerLogger.Error("ManageTags", "Failed to change a tag assignment", ex);
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
            if (assign)
            {
                await _dataService.AssignServerTagAsync(ids, tagId);
            }
            else
            {
                await _dataService.UnassignServerTagAsync(ids, tagId);
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
            ViewerLogger.Error("ManageTags", "Bulk tag assignment failed", ex);
            MessageBox.Show(this, $"Could not update assignments: {ex.Message}", "Manage Tags",
                MessageBoxButton.OK, MessageBoxImage.Error);
            await ReloadTagsAsync();
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

        if (_selectedNode.Depth >= ServerTagRules.MaxDepth - 1)
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
            await _dataService.CreateServerTagAsync(name, parentId);
            ChangedAny = true;
            await ReloadTagsAsync();
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ManageTags", "Create tag failed", ex);
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
            await _dataService.RenameServerTagAsync(_selectedNode.Tag.Id, name);
            ChangedAny = true;
            await ReloadTagsAsync();
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ManageTags", "Rename tag failed", ex);
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
            await _dataService.SetServerTagColorAsync(_selectedNode.Tag.Id, dialog.SelectedColour);
            ChangedAny = true;
            await ReloadTagsAsync();
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ManageTags", "Set tag colour failed", ex);
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

        string ruleNote;
        try
        {
            ruleNote = ServerTagCoverage.DeleteWarning(await _dataService.GetServerTagDeleteImpactAsync(_selectedNode.Tag.Id));
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ManageTags", "Delete impact read failed", ex);
            ruleNote = string.Empty;
        }

        var confirm = MessageBox.Show(this,
            $"Delete ‘{_selectedNode.Tag.Name}’{childNote}?{assignNote} Collected data is not affected.{ruleNote}",
            "Delete Tag", MessageBoxButton.OKCancel, MessageBoxImage.Warning);
        if (confirm != MessageBoxResult.OK)
        {
            return;
        }

        try
        {
            await _dataService.DeleteServerTagAsync(_selectedNode.Tag.Id);
            _selectedNode = null;
            ChangedAny = true;
            await ReloadTagsAsync();
        }
        catch (Exception ex)
        {
            ViewerLogger.Error("ManageTags", "Delete tag failed", ex);
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
