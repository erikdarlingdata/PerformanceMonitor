/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Input;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Manages the list of <see cref="ViewerCredentialProfile"/> records (Add/Edit/Delete) — the viewer port
/// of Lite's <c>ManageCredentialProfilesDialog</c>, launched from <see cref="ManageServersWindow"/>.
/// Delete is a constraint, not a cascade: a profile referenced by a server can't be deleted until the
/// server is reassigned.
/// </summary>
public partial class ManageCredentialProfilesDialog : Window
{
    private readonly ViewerProfileStore _profileStore;

    /// <summary>True if any profile was added/edited/deleted, so the caller can refresh.</summary>
    public bool ProfilesChanged { get; private set; }

    public ManageCredentialProfilesDialog(ViewerProfileStore profileStore)
    {
        InitializeComponent();
        _profileStore = profileStore;
        RefreshGrid();
    }

    private void RefreshGrid()
    {
        ProfilesGrid.ItemsSource = null;
        var profiles = _profileStore.GetAll();
        ProfilesGrid.ItemsSource = profiles;
        StatusText.Text = $"{profiles.Count} profile(s).";
    }

    private void AddButton_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new EditCredentialProfileDialog(_profileStore) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            ProfilesChanged = true;
            RefreshGrid();
        }
    }

    private void EditButton_Click(object sender, RoutedEventArgs e) => EditSelected();

    /* A double-click on a column header, the scrollbar or the empty area is not a row (#7210). */
    private void ProfilesGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e)
    {
        if (DataGridRowHit.IsOnRow(e.OriginalSource))
        {
            EditSelected();
        }
    }

    private void EditSelected()
    {
        if (ProfilesGrid.SelectedItem is not ViewerCredentialProfile selected)
        {
            return;
        }

        var dialog = new EditCredentialProfileDialog(_profileStore, selected) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            ProfilesChanged = true;
            RefreshGrid();
        }
    }

    private void DeleteButton_Click(object sender, RoutedEventArgs e)
    {
        if (ProfilesGrid.SelectedItem is not ViewerCredentialProfile selected)
        {
            return;
        }

        var confirm = MessageBox.Show(
            $"Delete credential profile '{selected.Name}'?\n\nThis removes the profile and its stored secret.",
            "Delete Credential Profile",
            MessageBoxButton.YesNo,
            MessageBoxImage.Warning);

        if (confirm != MessageBoxResult.Yes)
        {
            return;
        }

        try
        {
            _profileStore.DeleteProfile(selected.Id);
            ProfilesChanged = true;
            RefreshGrid();
        }
        catch (Exception ex)
        {
            /* Referential-integrity block (or any failure) surfaces here. */
            MessageBox.Show(ex.Message, "Cannot Delete Profile", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    private void CloseButton_Click(object sender, RoutedEventArgs e) => Close();
}
