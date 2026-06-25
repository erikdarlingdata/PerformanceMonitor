/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using System.Windows.Input;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// Manages the list of <see cref="CredentialProfile"/> records (Add/Edit/Delete). Launched from
/// ManageServersWindow (O-3). Delete is a constraint, not a cascade: a profile referenced by a server
/// can't be deleted until the server is reassigned (the §2 fail-closed resolution is the safety net
/// behind this best-effort check).
/// </summary>
public partial class ManageCredentialProfilesDialog : Window
{
    private readonly ProfileManager _profileManager;

    /// <summary>
    /// True if any profile was added/edited/deleted, so the caller can refresh.
    /// </summary>
    public bool ProfilesChanged { get; private set; }

    public ManageCredentialProfilesDialog(ProfileManager profileManager)
    {
        InitializeComponent();
        _profileManager = profileManager;
        RefreshGrid();
    }

    private void RefreshGrid()
    {
        ProfilesGrid.ItemsSource = null;
        var profiles = _profileManager.GetAll();
        ProfilesGrid.ItemsSource = profiles;
        StatusText.Text = $"{profiles.Count} profile(s).";
    }

    private void AddButton_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new EditCredentialProfileDialog(_profileManager) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            ProfilesChanged = true;
            RefreshGrid();
        }
    }

    private void EditButton_Click(object sender, RoutedEventArgs e) => EditSelected();

    private void ProfilesGrid_MouseDoubleClick(object sender, MouseButtonEventArgs e) => EditSelected();

    private void EditSelected()
    {
        if (ProfilesGrid.SelectedItem is not CredentialProfile selected)
            return;

        var dialog = new EditCredentialProfileDialog(_profileManager, selected) { Owner = this };
        if (dialog.ShowDialog() == true)
        {
            ProfilesChanged = true;
            RefreshGrid();
        }
    }

    private void DeleteButton_Click(object sender, RoutedEventArgs e)
    {
        if (ProfilesGrid.SelectedItem is not CredentialProfile selected)
            return;

        var confirm = MessageBox.Show(
            $"Delete credential profile '{selected.Name}'?\n\nThis removes the profile and its stored secret.",
            "Delete Credential Profile",
            MessageBoxButton.YesNo,
            MessageBoxImage.Warning);

        if (confirm != MessageBoxResult.Yes)
            return;

        try
        {
            _profileManager.DeleteProfile(selected.Id);
            ProfilesChanged = true;
            RefreshGrid();
        }
        catch (System.Exception ex)
        {
            // Referential-integrity block (or any failure) surfaces here.
            MessageBox.Show(ex.Message, "Cannot Delete Profile", MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }

    private void CloseButton_Click(object sender, RoutedEventArgs e) => Close();
}
