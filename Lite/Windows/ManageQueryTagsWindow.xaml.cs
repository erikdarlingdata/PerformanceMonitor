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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>A tag-tree row the window binds to.</summary>
public sealed class QueryTagNodeView(QueryTagTreeItem item)
{
    public QueryTagTreeItem Item { get; } = item;
    public string Name => Item.Tag.Name;
    public System.Windows.Media.Brush Swatch => TagColorBrushes.Fill(Item.Tag.Colour);
    public List<QueryTagNodeView> Children { get; } = item.Children.Select(c => new QueryTagNodeView(c)).ToList();
}

/// <summary>A server choice: Id null is every server.</summary>
public sealed record QueryTagServerChoice(int? Id, string Name);

public sealed record QueryTagAssignmentView(QueryTagAssignmentRow Row, string Server)
{
    public string Kind => Row.Kind;
    public string CreatedBy => Row.CreatedBy;
    public string Key => Row.Kind == "hash" ? Row.QueryHash ?? string.Empty : $"{Row.DatabaseName}.{Row.SchemaName}.{Row.ObjectName}";
}

public sealed record QueryTagRuleView(QueryTagRuleRow Row, string TagName, string Server)
{
    public int Priority => Row.Priority;
    public string Field => Row.MatchField;
    public string Pattern => Row.Pattern;
    public string EnabledText => Row.Enabled ? "yes" : "no";
}

/// <summary>
/// The Manage Query Tags window (#5993): the query-tag tree, the selected tag's hand assignments and the rule list with its
/// form and Test button. All logic is in <see cref="ManageQueryTagsModel"/> and the shared rules; this file is the controls.
/// </summary>
public partial class ManageQueryTagsWindow : Window
{
    private readonly LocalDataService _dataService;
    private readonly ManageQueryTagsModel _model;
    private readonly IReadOnlyList<ServerConnection> _servers;
    private List<QueryTagRow> _tags = new();
    private List<QueryTagRuleRow> _rules = new();
    private List<QueryTagServerChoice> _serverChoices = new();
    private QueryTagTreeItem? _selected;

    /// <summary>True once anything changed.</summary>
    public bool ChangedAny { get; private set; }

    public ManageQueryTagsWindow(LocalDataService dataService, IReadOnlyList<ServerConnection> servers)
    {
        InitializeComponent();
        _dataService = dataService;
        _model = new ManageQueryTagsModel(dataService);
        _servers = servers;

        _serverChoices = new List<QueryTagServerChoice> { new(null, "All servers") };
        _serverChoices.AddRange(servers.Select(s => new QueryTagServerChoice(RemoteCollectorService.GetServerId(s), s.DisplayName)));
        AssignServerBox.ItemsSource = _serverChoices;
        AssignServerBox.SelectedIndex = 0;
        RuleServerBox.ItemsSource = _serverChoices;
        RuleServerBox.SelectedIndex = 0;
        FieldBox.ItemsSource = new[] { "application", "login", "host", "database", "object", "text", "text_tag" };
        FieldBox.SelectedIndex = 0;

        Loaded += async (_, _) => await ReloadAsync();
    }

    private string ServerName(int? id) => id is null ? "All servers" : _serverChoices.FirstOrDefault(c => c.Id == id)?.Name ?? $"Server {id}";

    private async Task ReloadAsync()
    {
        try
        {
            _tags = await Task.Run(() => _dataService.GetQueryTagsAsync());
            _rules = await Task.Run(() => _dataService.GetQueryTagRulesAsync());
            var selectedId = _selected?.Tag.Id;
            var roots = ManageQueryTagsModel.BuildTree(_tags).Select(r => new QueryTagNodeView(r)).ToList();
            TagTree.ItemsSource = roots;
            _selected = selectedId is int id ? Flatten(ManageQueryTagsModel.BuildTree(_tags)).FirstOrDefault(i => i.Tag.Id == id) : null;
            RuleTagBox.ItemsSource = _tags.OrderBy(t => t.Name, StringComparer.OrdinalIgnoreCase).ToList();
            RuleTagBox.SelectedIndex = RuleTagBox.Items.Count > 0 ? 0 : -1;
            await ReloadAssignmentsAsync();
            ReloadRules();
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageQueryTags", "Failed to load query tags", ex);
            MessageBox.Show(this, $"Could not load query tags: {ex.Message}", "Manage Query Tags", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    private static IEnumerable<QueryTagTreeItem> Flatten(IEnumerable<QueryTagTreeItem> items) =>
        items.SelectMany(i => new[] { i }.Concat(Flatten(i.Children)));

    private async Task ReloadAssignmentsAsync()
    {
        if (_selected is null)
        {
            AssignHeader.Text = "Select a tag";
            AssignGrid.ItemsSource = null;
            return;
        }

        var tagId = _selected.Tag.Id;
        AssignHeader.Text = $"Hand assignments for ‘{_selected.Tag.Name}’";
        var rows = await Task.Run(() => _dataService.GetQueryTagAssignmentsAsync(tagId));
        AssignGrid.ItemsSource = rows.Select(r => new QueryTagAssignmentView(r, ServerName(r.ServerId))).ToList();
    }

    private void ReloadRules() =>
        RuleGrid.ItemsSource = _rules.Select(r => new QueryTagRuleView(
            r, r.TagId is int t ? _tags.FirstOrDefault(x => x.Id == t)?.Name ?? $"#{t}" : "(from comment)", ServerName(r.ServerId))).ToList();

    private async void TagTree_SelectedItemChanged(object sender, RoutedPropertyChangedEventArgs<object> e)
    {
        _selected = (e.NewValue as QueryTagNodeView)?.Item;
        try
        {
            await ReloadAssignmentsAsync();
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageQueryTags", "Failed to load assignments", ex);
        }
    }

    /* A write can wait up to 30 s for the local database. The work area is greyed out meanwhile, a busy store is
       reported with StoreBusyReport's wording, and a refusal (the shared code and message, or a pending restore) is shown as is. */
    private async Task<bool> WriteAsync(string doing, string logAction, Func<Task<QueryTagWriteOutcome>> write)
    {
        WorkArea.IsEnabled = false;
        StatusText.Text = StoreBusyReport.WaitingText(doing);
        try
        {
            var outcome = await write();
            StatusText.Text = outcome.Message;
            if (!outcome.Ok)
            {
                MessageBox.Show(this, outcome.Message, "Manage Query Tags", MessageBoxButton.OK, MessageBoxImage.Information);
                return false;
            }

            ChangedAny = true;
            await ReloadAsync();
            return true;
        }
        catch (StoreLockTimeoutException ex)
        {
            var text = StoreBusyReport.Report("ManageQueryTags", logAction, ex, doing);
            StatusText.Text = text;
            if (IsLoaded)
            {
                MessageBox.Show(this, text, "Manage Query Tags", MessageBoxButton.OK, MessageBoxImage.Warning);
            }

            return false;
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageQueryTags", $"{logAction} failed", ex);
            MessageBox.Show(this, $"Could not complete the change: {ex.Message}", "Manage Query Tags", MessageBoxButton.OK, MessageBoxImage.Error);
            return false;
        }
        finally
        {
            WorkArea.IsEnabled = true;
        }
    }

    private string? PromptName(string title, string prompt, string initial = "")
    {
        var dialog = new TagNameDialog(title, prompt, initial) { Owner = this };
        return dialog.ShowDialog() == true ? dialog.TagName : null;
    }

    private async void NewRoot_Click(object sender, RoutedEventArgs e)
    {
        var name = PromptName("New Query Tag", "Tag name:");
        if (name is not null)
        {
            await WriteAsync("creating the tag", "Create tag", () => Task.Run(() => _model.CreateTagAsync(name, null)));
        }
    }

    private async void NewChild_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is null)
        {
            return;
        }

        /* The depth rule is the shared one; the store refuses a fifth level with the shared message, which the write shows. */
        var parent = _selected.Tag.Id;
        var name = PromptName("New Child Tag", $"New tag under ‘{_selected.Tag.Name}’:");
        if (name is not null)
        {
            await WriteAsync("creating the tag", "Create tag", () => Task.Run(() => _model.CreateTagAsync(name, parent)));
        }
    }

    private async void Rename_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is null)
        {
            return;
        }

        var id = _selected.Tag.Id;
        var name = PromptName("Rename Tag", "New name:", _selected.Tag.Name);
        if (name is not null && name != _selected.Tag.Name)
        {
            await WriteAsync("renaming the tag", "Rename tag", () => Task.Run(() => _model.RenameTagAsync(id, name)));
        }
    }

    private async void Colour_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is null)
        {
            return;
        }

        var id = _selected.Tag.Id;
        var dialog = new TagColorDialog(_selected.Tag.Colour) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            var colour = dialog.SelectedColour;
            await WriteAsync("recolouring the tag", "Recolour tag", () => Task.Run(() => _model.RecolourTagAsync(id, colour)));
        }
    }

    private async void Move_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is null)
        {
            return;
        }

        var id = _selected.Tag.Id;
        var choices = new List<QueryTagRow?> { null };
        choices.AddRange(_tags.Where(t => t.Id != id).OrderBy(t => t.Name, StringComparer.OrdinalIgnoreCase));
        var list = new ListBox { ItemsSource = choices.Select(c => c?.Name ?? "(top level)").ToList(), SelectedIndex = 0, Margin = new Thickness(0, 0, 0, 8) };
        var ok = new Button { Content = "Move", IsDefault = true, Width = 80, HorizontalAlignment = HorizontalAlignment.Right };
        var panel = new StackPanel { Margin = new Thickness(12) };
        panel.Children.Add(new TextBlock { Text = $"Move ‘{_selected.Tag.Name}’ under:", Margin = new Thickness(0, 0, 0, 6) });
        panel.Children.Add(list);
        panel.Children.Add(ok);
        var dialog = new Window
        {
            Title = "Move Tag", Content = panel, Width = 320, Height = 380, Owner = this,
            WindowStartupLocation = WindowStartupLocation.CenterOwner, ShowInTaskbar = false,
        };
        ok.Click += (_, _) => dialog.DialogResult = true;
        if (dialog.ShowDialog() == true && list.SelectedIndex >= 0)
        {
            var parent = choices[list.SelectedIndex]?.Id;
            await WriteAsync("moving the tag", "Move tag", () => Task.Run(() => _model.MoveTagAsync(id, parent)));
        }
    }

    private async void Delete_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is null)
        {
            return;
        }

        var id = _selected.Tag.Id;
        var casualties = await Task.Run(() => _model.CountDeleteCasualtiesAsync(id));
        var children = Flatten(new[] { _selected }).Count() - 1;
        var childNote = children > 0 ? $" and its {children} child tag(s)" : string.Empty;
        var note = casualties > 0 ? $" This also removes {casualties} hand assignment(s) and rule(s) that use it." : string.Empty;
        var confirm = MessageBox.Show(this, $"Delete ‘{_selected.Tag.Name}’{childNote}?{note} Collected data is not affected.",
            "Delete Query Tag", MessageBoxButton.OKCancel, MessageBoxImage.Warning);
        if (confirm == MessageBoxResult.OK)
        {
            _selected = null;
            await WriteAsync("deleting the tag", "Delete tag", () => Task.Run(() => _model.DeleteTagAsync(id)));
        }
    }

    private async void AddHash_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is null)
        {
            return;
        }

        var tag = _selected.Tag.Id;
        var hash = HashBox.Text.Trim();
        var server = (AssignServerBox.SelectedItem as QueryTagServerChoice)?.Id;
        if (await WriteAsync("assigning the tag", "Assign tag", () => Task.Run(() => _model.AddHashAssignmentAsync(tag, hash, server))))
        {
            HashBox.Clear();
        }
    }

    private async void AddObject_Click(object sender, RoutedEventArgs e)
    {
        if (_selected is null)
        {
            return;
        }

        var tag = _selected.Tag.Id;
        string db = ObjDbBox.Text.Trim(), schema = ObjSchemaBox.Text.Trim(), name = ObjNameBox.Text.Trim();
        var server = (AssignServerBox.SelectedItem as QueryTagServerChoice)?.Id;
        await WriteAsync("assigning the tag", "Assign tag", () => Task.Run(() => _model.AddObjectAssignmentAsync(tag, db, schema, name, server)));
    }

    private async void RemoveAssignment_Click(object sender, RoutedEventArgs e)
    {
        if (AssignGrid.SelectedItem is QueryTagAssignmentView view)
        {
            var id = view.Row.Id;
            await WriteAsync("removing the assignment", "Remove assignment", () => Task.Run(() => _model.RemoveAssignmentAsync(id)));
        }
    }

    private void FieldBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        var textTag = FieldBox.SelectedItem as string == "text_tag";
        PatternBox.IsEnabled = !textTag;
        RuleTagBox.IsEnabled = !textTag;
    }

    private QueryTagRuleForm? ReadForm()
    {
        if (!Enum.TryParse<QueryTagRuleField>((FieldBox.SelectedItem as string ?? string.Empty).Replace("_", string.Empty, StringComparison.Ordinal), true, out var field)
            || !int.TryParse(PriorityBox.Text.Trim(), out var priority))
        {
            MessageBox.Show(this, "Pick a field and give the priority as a whole number.", "Manage Query Tags", MessageBoxButton.OK, MessageBoxImage.Information);
            return null;
        }

        return new QueryTagRuleForm(field, PatternBox.Text, (RuleTagBox.SelectedItem as QueryTagRow)?.Id, priority,
            EnabledBox.IsChecked == true, (RuleServerBox.SelectedItem as QueryTagServerChoice)?.Id);
    }

    private void RuleGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (RuleGrid.SelectedItem is not QueryTagRuleView view)
        {
            return;
        }

        var r = view.Row;
        FieldBox.SelectedItem = r.MatchField;
        PatternBox.Text = r.MatchField == "text_tag" ? string.Empty : r.Pattern;
        RuleTagBox.SelectedItem = _tags.FirstOrDefault(t => t.Id == r.TagId);
        PriorityBox.Text = r.Priority.ToString(System.Globalization.CultureInfo.InvariantCulture);
        EnabledBox.IsChecked = r.Enabled;
        RuleServerBox.SelectedItem = _serverChoices.FirstOrDefault(c => c.Id == r.ServerId);
    }

    private async void AddRule_Click(object sender, RoutedEventArgs e)
    {
        if (ReadForm() is { } form)
        {
            await WriteAsync("adding the rule", "Add rule", () => Task.Run(() => _model.AddRuleAsync(form)));
        }
    }

    private async void SaveRule_Click(object sender, RoutedEventArgs e)
    {
        if (RuleGrid.SelectedItem is QueryTagRuleView view && ReadForm() is { } form)
        {
            var id = view.Row.Id;
            await WriteAsync("saving the rule", "Save rule", () => Task.Run(() => _model.UpdateRuleAsync(id, form)));
        }
    }

    private async void ToggleRule_Click(object sender, RoutedEventArgs e)
    {
        if (RuleGrid.SelectedItem is QueryTagRuleView view)
        {
            var id = view.Row.Id;
            var enable = !view.Row.Enabled;
            await WriteAsync(enable ? "enabling the rule" : "disabling the rule", "Toggle rule", () => Task.Run(() => _model.SetRuleEnabledAsync(id, enable)));
        }
    }

    private async void DeleteRule_Click(object sender, RoutedEventArgs e)
    {
        if (RuleGrid.SelectedItem is QueryTagRuleView view
            && MessageBox.Show(this, "Delete this rule?", "Delete Rule", MessageBoxButton.OKCancel, MessageBoxImage.Warning) == MessageBoxResult.OK)
        {
            var id = view.Row.Id;
            await WriteAsync("deleting the rule", "Delete rule", () => Task.Run(() => _model.DeleteRuleAsync(id)));
        }
    }

    private async void TestRule_Click(object sender, RoutedEventArgs e)
    {
        if (ReadForm() is not { } form)
        {
            return;
        }

        int? editing = (RuleGrid.SelectedItem as QueryTagRuleView)?.Row.Id;
        TestOutput.Text = "Testing…";
        try
        {
            var result = await Task.Run(() => _model.TestRuleAsync(form, editing));
            var lines = new List<string> { ManageQueryTagsModel.DescribeTest(result) };
            lines.AddRange(result.Samples.Take(10).Select(s =>
                $"  {ServerName(s.ServerId)}  {s.Kind}  {s.Key}  ({s.MatchedField}: {s.MatchedValue}){(s.Held ? "  held" : string.Empty)}"));
            TestOutput.Text = string.Join(Environment.NewLine, lines);
        }
        catch (Exception ex)
        {
            AppLogger.Error("ManageQueryTags", "Test rule failed", ex);
            TestOutput.Text = $"The test could not run: {ex.Message}";
        }
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
