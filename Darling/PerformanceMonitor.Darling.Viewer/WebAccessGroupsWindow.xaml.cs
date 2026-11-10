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
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

public partial class WebAccessGroupsWindow : Window
{
    public sealed record TagChoice(int Id, string Path);
    private readonly ViewerDataService _data;
    private readonly CancellationTokenSource _lifetime = new();
    private WebAccessGroup? _selected;
    private bool _busy;
    private bool _loading;
    private bool _allowClose;
    private bool _confirming;

    public WebAccessGroupsWindow(ViewerDataService data)
    {
        InitializeComponent();
        _data = data;
        Loaded += async (_, _) => await RunAsync(RefreshAsync);
        Closing += Window_Closing;
        Closed += (_, _) => { _lifetime.Cancel(); ClearToken(); };
    }

    private int[] TagIds => TagList.SelectedItems.Cast<TagChoice>().Select(t => t.Id).ToArray();
    private WebAccessGroupScope Scope => AllRadio.IsChecked == true ? WebAccessGroupScope.AllServers : WebAccessGroupScope.Tags;

    private async Task RefreshAsync()
    {
        if (!await _data.CanManageWebAccessGroupsAsync(_lifetime.Token))
            throw new InvalidOperationException("Web access groups can only be managed by a Viewer administrator.");
        var id = _selected?.GroupId;
        var tags = await _data.GetServerTagsAsync(_lifetime.Token);
        var byId = tags.ToDictionary(t => t.Id);
        string Path(DarlingTag tag)
        {
            var names = new List<string> { tag.Name };
            var seen = new HashSet<int> { tag.Id };
            while (tag.ParentId is int parent && byId.TryGetValue(parent, out var ancestor) && seen.Add(parent))
            { names.Insert(0, ancestor.Name); tag = ancestor; }
            return string.Join(" → ", names);
        }
        _loading = true;
        try
        {
            TagList.ItemsSource = tags.Select(t => new TagChoice(t.Id, Path(t))).OrderBy(t => t.Path, StringComparer.OrdinalIgnoreCase).ToArray();
            GroupList.ItemsSource = await _data.GetWebAccessGroupsAsync(_lifetime.Token);
            GroupList.SelectedItem = GroupList.Items.Cast<WebAccessGroup>().FirstOrDefault(g => g.GroupId == id);
        }
        finally { _loading = false; }
        LoadSelection(GroupList.SelectedItem as WebAccessGroup);
    }

    private void LoadSelection(WebAccessGroup? group)
    {
        _loading = true;
        try
        {
            ClearToken();
            _selected = group;
            NameBox.Text = group?.Name ?? "";
            NameBox.IsReadOnly = group is not null;
            EnabledBox.IsChecked = group?.Enabled ?? true;
            EnabledBox.IsEnabled = group is not null;
            AllRadio.IsChecked = group?.Scope == WebAccessGroupScope.AllServers;
            TagsRadio.IsChecked = AllRadio.IsChecked != true;
            TagList.SelectedItems.Clear();
            foreach (var tag in TagList.Items.Cast<TagChoice>().Where(t => group?.TagIds.Contains(t.Id) == true))
                TagList.SelectedItems.Add(tag);
            SaveButton.Content = group is null ? "_Create group and token" : "_Save changes";
            RotateButton.IsEnabled = group is { Enabled: true };
            DeleteButton.IsEnabled = group is not null;
            ActivationText.Text = group is null ? "A unique token is generated and sealed for the service." : StateText(group);
            StatusText.Text = group is null ? "New group: choose a name and a scope." : "Scope edits keep the token. Rotation or deletion ends existing sessions.";
        }
        finally { _loading = false; }
        UpdateScopeNote();
    }

    private static string StateText(WebAccessGroup group) => !group.Enabled ? "Disabled. Web access is refused."
        : group.ActivationState switch
        {
            "active" => "Active: read-only team access.",
            "pending" => "Pending: waiting for the service to accept the token.",
            _ => "Refused: " + (group.RefusalCode switch
            {
                "duplicate_name" => "The name conflicts with a configured web token. Delete this group and create one with a different name.",
                "duplicate_token" => "The token conflicts with another credential. Rotate it.",
                "invalid_token" => "The token is invalid. Rotate it.",
                _ => "The service could not open the token. Rotate it using the current service key."
            })
        };

    private void UpdateScopeNote()
    {
        if (TagList is null || ScopeNote is null) return;
        TagList.IsEnabled = Scope != WebAccessGroupScope.AllServers;
        ScopeNote.Text = Scope == WebAccessGroupScope.AllServers
            ? "All registered servers, including untagged and newly added servers. Administrative pages remain blocked."
            : TagIds.Length == 0
                ? "No tags selected: no server access. This never means All servers."
                : "Selected tag subtrees are combined. Parent-only and sibling servers are excluded; future tag-assignment changes apply automatically.";
    }

    private async Task RunAsync(Func<Task> action)
    {
        if (_busy) return;
        _busy = true;
        EditorPanel.IsEnabled = false;
        try { await action(); }
        catch (OperationCanceledException) when (_lifetime.IsCancellationRequested) { }
        catch (InvalidOperationException ex)
        {
            if (!_lifetime.IsCancellationRequested)
            {
                StatusText.Text = ex.Message;
            }
        }
        catch (Exception) { if (!_lifetime.IsCancellationRequested) StatusText.Text = "The operation could not be completed. Refresh the group list before retrying."; }
        finally { _busy = false; if (!_lifetime.IsCancellationRequested) EditorPanel.IsEnabled = true; }
    }

    private async Task<ViewerPasswordSealer?> GetSealerAsync()
    {
        var result = await ViewerPasswordKey.GetSealKeyAsync(_data, this, _lifetime.Token);
        _lifetime.Token.ThrowIfCancellationRequested();
        if (result.Notice is { } notice)
            StatusText.Text = notice;
        if (result.Sealer is null)
            StatusText.Text = result.Refusal ?? ViewerPasswordKey.NoKeyText;
        return result.Sealer;
    }

    private async Task RevealWhenActiveAsync(ViewerWebToken token)
    {
        // No plaintext is shown for a pending/refused/stale generation. A timeout leaves the stored
        // group intact; rotate it to obtain a new one-time credential if activation happens later.
        StatusText.Text = "Waiting for the service to accept the new token...";
        if (await _data.GetServiceConfigAsync(_lifetime.Token) is { WebEnabled: false })
        {
            // A service with its dashboard off never polls the group, so the wait would only end in the timeout text below.
            _selected = await _data.GetWebAccessGroupAsync(token.GroupId, _lifetime.Token);
            await RefreshAsync();
            StatusText.Text = "The web dashboard is off, so the service cannot accept the token. Enable the dashboard in Settings, then rotate the token to receive a new one.";
            return;
        }
        for (var attempt = 0; attempt < 30; attempt++)
        {
            var group = await _data.GetWebAccessGroupAsync(token.GroupId, _lifetime.Token);
            if (token.CanReveal(group))
            {
                _selected = group;
                await RefreshAsync();
                if (!TryShowToken(token, _selected))
                    StatusText.Text = "The group changed before handoff. Refresh and rotate to obtain a current token.";
                return;
            }
            if (group is null || !group.Enabled || group.TokenGeneration != token.Generation || group.ActivationState == "refused")
            {
                _selected = group;
                await RefreshAsync();
                StatusText.Text = group is null ? "The group was deleted before activation." : StateText(group);
                return;
            }
            await Task.Delay(1000, _lifetime.Token);
        }
        _selected = await _data.GetWebAccessGroupAsync(token.GroupId, _lifetime.Token);
        await RefreshAsync();
        StatusText.Text = "The token is still pending. Check that the web service is enabled and has its sealing key. Refresh, then rotate to obtain a new token.";
    }

    private async void Save_Click(object sender, RoutedEventArgs e) => await RunAsync(SaveAsync);

    private async Task SaveAsync()
    {
        ClearToken();
        var scope = Scope;
        var tags = scope == WebAccessGroupScope.AllServers ? Array.Empty<int>() : TagIds;
        if (_selected is null)
        {
            if (string.IsNullOrWhiteSpace(NameBox.Text) || (scope == WebAccessGroupScope.Tags && tags.Length == 0))
                throw new InvalidOperationException("Enter a group name and choose at least one tag or explicitly select All servers.");
            var sealer = await GetSealerAsync();
            if (sealer is null) return;
            await RevealWhenActiveAsync(await _data.CreateWebAccessGroupAsync(NameBox.Text, scope, tags, sealer, _lifetime.Token));
        }
        else
        {
            await _data.UpdateWebAccessGroupAsync(_selected, scope, tags, EnabledBox.IsChecked == true, _lifetime.Token);
            await RefreshAsync();
            StatusText.Text = "Saved. Changes apply to the next web request; the token is unchanged.";
        }
    }

    private async void Rotate_Click(object sender, RoutedEventArgs e)
    {
        var group = _selected;
        if (_busy || group is null) return;
        if (MessageBox.Show(this, "Replace this group's token? Its old token and existing sessions stop working immediately.",
            "Rotate web token", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes) return;
        await RunAsync(async () =>
        {
            ClearToken();
            var sealer = await GetSealerAsync();
            if (sealer is not null)
                await RevealWhenActiveAsync(await _data.RotateWebAccessGroupTokenAsync(group, sealer, _lifetime.Token));
        });
    }

    private async void Delete_Click(object sender, RoutedEventArgs e)
    {
        var group = _selected;
        if (_busy || group is null) return;
        if (MessageBox.Show(this, "Delete this group? Its token and existing sessions stop working immediately.",
            "Delete web access group", MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes) return;
        await RunAsync(async () =>
        {
            ClearToken();
            await _data.DeleteWebAccessGroupAsync(group, _lifetime.Token);
            _selected = null;
            await RefreshAsync();
            StatusText.Text = "Group deleted. Its token and sessions are revoked.";
        });
    }

    // Recheck the metadata obtained by the final refresh, not the earlier activation poll.
    internal bool TryShowToken(ViewerWebToken token, WebAccessGroup? refreshed)
    {
        ClearToken();
        if (!token.CanReveal(refreshed)) return false;
        TokenBox.Text = token.Plain;
        TokenPanel.Visibility = Visibility.Visible;
        TokenBox.SelectAll();
        StatusText.Text = "Token accepted. Copy it now: it is shown once, and a lost token is replaced by rotating it.";
        return true;
    }

    private void ClearToken() { TokenBox.Clear(); TokenPanel.Visibility = Visibility.Collapsed; }
    private void ClearToken_Click(object sender, RoutedEventArgs e) => ClearToken();
    private bool HasUnsavedChanges()
    {
        if (_selected is null)
            return !string.IsNullOrWhiteSpace(NameBox.Text) || Scope == WebAccessGroupScope.AllServers || TagIds.Length > 0;
        return EnabledBox.IsChecked != _selected.Enabled
            || Scope != _selected.Scope
            || (Scope == WebAccessGroupScope.Tags && !TagIds.OrderBy(id => id).SequenceEqual(_selected.TagIds.OrderBy(id => id)));
    }

    private async Task<bool> ConfirmLeaveAsync()
    {
        if (_busy || _confirming) return false;
        if (!HasUnsavedChanges()) return true;
        _confirming = true;
        try
        {
            var answer = MessageBox.Show(this,
                "There are unsaved changes. Yes saves them, No discards them, and Cancel keeps editing.",
                "Save changes?", MessageBoxButton.YesNoCancel, MessageBoxImage.Warning);
            if (answer == MessageBoxResult.Cancel) return false;
            if (answer == MessageBoxResult.No) return true;
            await RunAsync(SaveAsync);
            return !HasUnsavedChanges();
        }
        finally { _confirming = false; }
    }

    private async void New_Click(object sender, RoutedEventArgs e)
    {
        if (!await ConfirmLeaveAsync()) return;
        _loading = true;
        try { GroupList.SelectedItem = null; }
        finally { _loading = false; }
        LoadSelection(null);
    }

    private async void Refresh_Click(object sender, RoutedEventArgs e)
    {
        if (await ConfirmLeaveAsync()) await RunAsync(RefreshAsync);
    }

    private async void GroupList_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_loading) return;
        var next = GroupList.SelectedItem as WebAccessGroup;
        _loading = true;
        try { GroupList.SelectedItem = _selected; }
        finally { _loading = false; }
        if (!await ConfirmLeaveAsync()) return;
        _loading = true;
        try
        {
            GroupList.SelectedItem = GroupList.Items.Cast<WebAccessGroup>().FirstOrDefault(g => g.GroupId == next?.GroupId);
        }
        finally { _loading = false; }
        LoadSelection(GroupList.SelectedItem as WebAccessGroup);
    }

    private async void Window_Closing(object? sender, System.ComponentModel.CancelEventArgs e)
    {
        if (_allowClose) return;
        e.Cancel = true;
        if (!await ConfirmLeaveAsync()) return;
        _allowClose = true;
        Close();
    }
    private void Scope_Changed(object sender, RoutedEventArgs e) { if (!_loading) UpdateScopeNote(); }
    private void TagList_SelectionChanged(object sender, SelectionChangedEventArgs e) { if (!_loading) UpdateScopeNote(); }
    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
