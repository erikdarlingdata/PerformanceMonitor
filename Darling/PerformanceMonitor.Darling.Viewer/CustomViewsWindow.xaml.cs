/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's Custom Views manager (#6390 S5), <see cref="KnownPatternsWindow"/>'s shape: the stored views grouped by folder, New /
/// Rename / Move to folder / Delete / Import JSON / Export JSON, and a JSON editor with Validate and Save. Every read and write goes
/// through <see cref="ViewerDataService"/> (<see cref="CustomViewStore"/> and <see cref="CustomViewValidation"/> in Storage), so a view saved
/// here is the view the web shows and a definition the web would refuse gets the same sentence. A save carries the version it loaded,
/// and a stale one is refused with the store's own text. Each write button is on only when the store login holds that privilege on the
/// views table (not <see cref="ViewerDataService.IsReadOnly"/>, which the narrow <c>viewer</c> grant does not follow), and the window says
/// which privilege is missing.
/// </summary>
public partial class CustomViewsWindow : Window
{
    private readonly ViewerDataService _dataService;
    private ViewerDataService.CustomViewWriteAccess _access;
    private bool _supported = true;

    /// <summary>The view the editor holds, in full as it was loaded; null when the editor holds a new, unsaved view or nothing.</summary>
    private CustomView? _loaded;

    /// <summary>A new view (a New or an Import) waiting to be saved for the first time; null when the editor holds a stored view or nothing.</summary>
    private (string Name, string? Description, string? Folder)? _pending;

    private bool _dirty;
    private bool _quiet;

    /* A slow read of one row can finish after the operator has moved to another; the newest claim wins, as on Fleet Sweeps. */
    private readonly PerformanceMonitor.Ui.ScopedLoadGenerations _loads = new();

    public CustomViewsWindow(ViewerDataService dataService)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        UpdateButtons();
        Loaded += async (_, _) => await LoadAsync(null);
        Closing += (_, e) =>
        {
            if (!ConfirmDiscard())
            {
                e.Cancel = true;
            }
        };
    }

    /// <summary>#6390 S6: opens the saved version of the selected view in the run window.</summary>
    private void Run_Click(object sender, RoutedEventArgs e)
    {
        if (_loaded is { } view)
        {
            new CustomViewRunWindow(_dataService, view) { Owner = this }.Show();
        }
    }

    private bool EditorActive => _loaded is not null || _pending is not null;

    private bool CanSaveHere => _supported && ((_pending is not null && _access.CanInsert) || (_loaded is not null && _access.CanUpdate));

    private void UpdateButtons()
    {
        var stored = _loaded is not null && _pending is null;
        NewButton.IsEnabled = _supported && _access.CanInsert;
        ImportButton.IsEnabled = _supported && _access.CanInsert;
        RenameButton.IsEnabled = _supported && stored && _access.CanUpdate;
        MoveButton.IsEnabled = _supported && stored && _access.CanUpdate;
        /* Run reads only (the saved version), so it needs no write privilege. */
        RunButton.IsEnabled = _supported && stored;
        DeleteButton.IsEnabled = _supported && stored && _access.CanDelete;
        ExportButton.IsEnabled = stored;
        DefinitionBox.IsEnabled = EditorActive;
        DefinitionBox.IsReadOnly = !CanSaveHere;
        ValidateButton.IsEnabled = EditorActive;
        SaveButton.IsEnabled = CanSaveHere;
    }

    private void ShowBanner(string text)
    {
        StatusBanner.Text = text;
        StatusBanner.Visibility = Visibility.Visible;
    }

    private async Task LoadAsync(long? selectId)
    {
        try
        {
            if (!await _dataService.CustomViewsSupportedAsync())
            {
                _supported = false;
                ShowBanner(ViewerDataService.CustomViewsNeedNewerStoreMessage);
                NoViewsMessage.Visibility = Visibility.Visible;
                UpdateButtons();
                return;
            }

            _access = await _dataService.GetCustomViewWriteAccessAsync();
            if (_access.Banner is string banner)
            {
                ShowBanner(banner);
            }
            else
            {
                StatusBanner.Visibility = Visibility.Collapsed;
            }

            var views = await _dataService.GetCustomViewsAsync();
            var rows = CustomViewsEditorModel.Rows(views);
            var collection = new ListCollectionView(rows.ToList());
            collection.GroupDescriptions.Add(new PropertyGroupDescription(nameof(CustomViewRow.FolderDisplay)));
            _quiet = true;
            ViewsGrid.ItemsSource = collection;
            if (selectId is long id && rows.FirstOrDefault(r => r.Id == id) is { } selected)
            {
                ViewsGrid.SelectedItem = collection.Cast<CustomViewRow>().First(r => r.Id == selected.Id);
            }

            _quiet = false;
            NoViewsMessage.Visibility = rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            UpdateButtons();
        }
        catch (Exception ex)
        {
            _quiet = false;
            ShowError("Could not read custom views", ex);
        }
    }

    private void SetEditor(string header, string text, bool dirty)
    {
        _quiet = true;
        EditorHeader.Text = header;
        DefinitionBox.Text = text;
        _dirty = dirty;
        _quiet = false;
        SetStatus(null, false);
        UpdateButtons();
    }

    private void SetStatus(string? text, bool good)
    {
        EditorStatus.Text = text ?? string.Empty;
        EditorStatus.Foreground = good
            ? (Brush)FindResource("ForegroundBrush")
            : (Brush)FindResource("WarningTextBrush");
    }

    private void ClearEditor()
    {
        _loaded = null;
        _pending = null;
        SetEditor("Select a view to edit its definition.", string.Empty, false);
    }

    /// <summary>True when the editor has nothing unsaved, or the operator agrees to drop it.</summary>
    private bool ConfirmDiscard() =>
        !_dirty || MessageBox.Show("The definition in the editor has unsaved changes. Discard them?", "Custom Views",
            MessageBoxButton.YesNo, MessageBoxImage.Question) == MessageBoxResult.Yes;

    private async void ViewsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_quiet || ViewsGrid.SelectedItem is not CustomViewRow row)
        {
            return;
        }

        if (!ConfirmDiscard())
        {
            RestoreSelection();
            return;
        }

        var gen = _loads.Claim(nameof(ViewsGrid_SelectionChanged));
        try
        {
            var result = await _dataService.GetCustomViewAsync(row.Id);

            /* Two quick selection changes whose reads finish out of order would leave the grid on the second row and the editor
               (and Save, Move/Rename and Delete, which act on _loaded) on the first. Drop a result the operator has moved past. */
            if (_loads.Superseded(nameof(ViewsGrid_SelectionChanged), gen) || !ReferenceEquals(ViewsGrid.SelectedItem, row))
            {
                return;
            }

            if (result is CustomViewResult.Ok { View: { } view })
            {
                _loaded = view;
                _pending = null;
                SetEditor(HeaderFor(view), CustomViewsEditorModel.IndentDefinition(view.DefinitionJson), false);
            }
            else
            {
                MessageBox.Show("That view no longer exists. It was probably deleted by someone else.", "Custom Views",
                    MessageBoxButton.OK, MessageBoxImage.Information);
                ClearEditor();
                await LoadAsync(null);
            }
        }
        catch (Exception ex)
        {
            /* The success path's test, for the same reason: a read that failed after the operator moved on must not pop an error
               over the view the newer selection loaded. */
            if (_loads.Superseded(nameof(ViewsGrid_SelectionChanged), gen) || !ReferenceEquals(ViewsGrid.SelectedItem, row))
            {
                return;
            }

            ShowError("Could not read the custom view", ex);
        }
    }

    private void RestoreSelection()
    {
        _quiet = true;
        ViewsGrid.SelectedItem = _loaded is null || ViewsGrid.ItemsSource is not ListCollectionView collection
            ? null
            : collection.Cast<CustomViewRow>().FirstOrDefault(r => r.Id == _loaded.Id);
        _quiet = false;
    }

    private static string HeaderFor(CustomView view) =>
        $"{(string.IsNullOrEmpty(view.Folder) ? string.Empty : view.Folder + " / ")}{view.Name}  (version {view.Version})";

    private void DefinitionBox_TextChanged(object sender, TextChangedEventArgs e)
    {
        if (!_quiet && EditorActive)
        {
            _dirty = true;
            SetStatus(null, false);
        }
    }

    private string? Prompt(string title, string prompt, string initial, int maxLength, bool allowEmpty)
    {
        var dialog = new TagNameDialog(title, prompt, initial, maxLength, allowEmpty) { Owner = this };
        return dialog.ShowDialog() == true ? dialog.TagName : null;
    }

    private void StartNew(string name, string? description, string? folder, string text)
    {
        _loaded = null;
        _pending = (name, description, folder);
        _quiet = true;
        ViewsGrid.SelectedItem = null;
        _quiet = false;
        SetEditor($"New view: {(string.IsNullOrEmpty(folder) ? string.Empty : folder + " / ")}{name}  (not saved yet)", text, true);
    }

    private void New_Click(object sender, RoutedEventArgs e)
    {
        if (!ConfirmDiscard())
        {
            return;
        }

        var name = Prompt("New Custom View", "View name:", string.Empty, CustomViewStore.MaxNameLength, false);
        if (name is null)
        {
            return;
        }

        var folder = Prompt("New Custom View", "Folder (leave empty for the top level):", string.Empty, CustomViewStore.MaxFolderLength, true);
        if (folder is null)
        {
            return;
        }

        StartNew(name, null, folder.Length == 0 ? null : folder, CustomViewsEditorModel.NewViewTemplate);
    }

    private void Import_Click(object sender, RoutedEventArgs e)
    {
        if (!ConfirmDiscard())
        {
            return;
        }

        var dialog = new OpenFileDialog { Filter = "JSON files (*.json)|*.json|All files (*.*)|*.*", Title = "Import a custom view" };
        if (dialog.ShowDialog(this) != true)
        {
            return;
        }

        try
        {
            var (imported, error) = CustomViewsEditorModel.ParseImport(System.IO.File.ReadAllText(dialog.FileName));
            if (imported is null)
            {
                MessageBox.Show(error, "Custom Views", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            StartNew(imported.Name, imported.Description, null, CustomViewsEditorModel.IndentDefinition(imported.DefinitionJson));
            SetStatus("Imported from the file. Check it with Validate, then Save to create the view.", true);
        }
        catch (Exception ex) when (ex is System.IO.IOException or UnauthorizedAccessException)
        {
            ShowError("Could not read the file", ex);
        }
    }

    private void Export_Click(object sender, RoutedEventArgs e)
    {
        if (_loaded is not { } view || _pending is not null)
        {
            return;
        }

        var dialog = new SaveFileDialog
        {
            Filter = "JSON files (*.json)|*.json",
            FileName = CustomViewsEditorModel.SafeFileName(view.Name),
            Title = "Export a custom view",
        };
        if (dialog.ShowDialog(this) != true)
        {
            return;
        }

        try
        {
            System.IO.File.WriteAllText(dialog.FileName, CustomViewsEditorModel.ExportText(view));
            SetStatus("Exported the saved version of " + view.Name + ".", true);
        }
        catch (Exception ex) when (ex is System.IO.IOException or UnauthorizedAccessException)
        {
            ShowError("Could not write the file", ex);
        }
    }

    private void Validate_Click(object sender, RoutedEventArgs e)
    {
        var refusal = ViewerDataService.ValidateCustomViewDefinition(DefinitionBox.Text);
        SetStatus(refusal ?? "The definition is valid.", refusal is null);
    }

    private async void Save_Click(object sender, RoutedEventArgs e)
    {
        if (!CanSaveHere)
        {
            return;
        }

        try
        {
            CustomViewResult result;
            if (_pending is { } pending)
            {
                result = await _dataService.CreateCustomViewAsync(pending.Name, pending.Description, DefinitionBox.Text, pending.Folder);
            }
            else
            {
                var current = _loaded!;
                result = await _dataService.UpdateCustomViewAsync(
                    current.Id, current.Name, current.Description, DefinitionBox.Text, current.Version, current.Folder);
            }

            if (!await ShowResultAsync(result, "saved"))
            {
                return;
            }
        }
        catch (Exception ex)
        {
            ShowError("Could not save the custom view", ex);
        }
    }

    /// <summary>Shows the outcome of a write; true when it succeeded (the editor then holds the stored view).</summary>
    private async Task<bool> ShowResultAsync(CustomViewResult result, string verb)
    {
        switch (result)
        {
            case CustomViewResult.Ok { View: { } view }:
                _loaded = view;
                _pending = null;
                SetEditor(HeaderFor(view), CustomViewsEditorModel.IndentDefinition(view.DefinitionJson), false);
                SetStatus($"The view was {verb} (version {view.Version}).", true);
                await LoadAsync(view.Id);
                return true;
            case CustomViewResult.Conflict conflict:
                SetStatus(conflict.Message, false);
                return false;
            case CustomViewResult.Invalid invalid:
                SetStatus(invalid.Message, false);
                return false;
            case CustomViewResult.Forbidden forbidden:
                SetStatus(forbidden.Message, false);
                return false;
            default:
                SetStatus("That view no longer exists. It was probably deleted by someone else.", false);
                return false;
        }
    }

    private async void Rename_Click(object sender, RoutedEventArgs e) => await ChangeStoredViewAsync(
        "Rename Custom View", "New name:", view => view.Name, CustomViewStore.MaxNameLength, false,
        (view, name) => (name, view.Folder), "renamed");

    private async void Move_Click(object sender, RoutedEventArgs e) => await ChangeStoredViewAsync(
        "Move Custom View", "Folder (leave empty for the top level):", view => view.Folder ?? string.Empty, CustomViewStore.MaxFolderLength, true,
        (view, folder) => (view.Name, folder.Length == 0 ? null : folder), "moved");

    /// <summary>Renames or moves the stored view: its saved definition is written back unchanged, under the version loaded.</summary>
    private async Task ChangeStoredViewAsync(
        string title, string prompt, Func<CustomView, string> initial, int maxLength, bool allowEmpty,
        Func<CustomView, string, (string Name, string? Folder)> apply, string verb)
    {
        if (_loaded is not { } view || _pending is not null || !_access.CanUpdate)
        {
            return;
        }

        if (_dirty)
        {
            MessageBox.Show("Save or discard the changes in the editor first.", "Custom Views", MessageBoxButton.OK, MessageBoxImage.Information);
            return;
        }

        var answer = Prompt(title, prompt, initial(view), maxLength, allowEmpty);
        if (answer is null)
        {
            return;
        }

        try
        {
            var (name, folder) = apply(view, answer);
            var result = await _dataService.UpdateCustomViewAsync(
                view.Id, name, view.Description, view.DefinitionJson, view.Version, folder);
            await ShowResultAsync(result, verb);
        }
        catch (Exception ex)
        {
            ShowError("Could not change the custom view", ex);
        }
    }

    private async void Delete_Click(object sender, RoutedEventArgs e)
    {
        if (_loaded is not { } view || _pending is not null || !_access.CanDelete)
        {
            return;
        }

        if (MessageBox.Show($"Delete the custom view \"{view.Name}\"?\n\nThis cannot be undone.", "Confirm Delete",
                MessageBoxButton.YesNo, MessageBoxImage.Question) != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            var result = await _dataService.DeleteCustomViewAsync(view.Id);
            if (result is CustomViewResult.Forbidden forbidden)
            {
                SetStatus(forbidden.Message, false);
                return;
            }

            ClearEditor();
            await LoadAsync(null);
        }
        catch (Exception ex)
        {
            ShowError("Could not delete the custom view", ex);
        }
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();

    private void ShowError(string what, Exception ex) =>
        MessageBox.Show($"{what}: {ex.Message}", "Custom Views", MessageBoxButton.OK, MessageBoxImage.Warning);
}
