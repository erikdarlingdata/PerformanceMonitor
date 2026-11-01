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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The People screen (#7107): the people, their identities and their grants, read from the store and changed through the service.
/// A store below V237 shows that it needs an upgrade and offers no buttons. A read-only seat sees the lists and none of the write buttons.
/// The sentence of every command goes to the status line, a refusal included, in the service's own words.
/// </summary>
public partial class PeopleWindow : Window
{
    private readonly ViewerDataService _dataService;
    private readonly string _login;
    private readonly bool _readOnly;
    private bool _storeReady;

    public PeopleWindow(ViewerDataService dataService)
        : this(dataService, dataService?.IsReadOnly ?? false, Environment.UserName)
    {
        Loaded += async (_, _) => await LoadAsync();
    }

    /// <summary>The seam the tests use: the seat's role and the login are given, and nothing is read on load.</summary>
    internal PeopleWindow(ViewerDataService dataService, bool readOnly, string login)
    {
        InitializeComponent();
        _dataService = dataService ?? throw new ArgumentNullException(nameof(dataService));
        _readOnly = readOnly;
        _login = login;
        SetButtons();
    }

    private PersonViewRow? SelectedPerson => PeopleGrid.SelectedItem as PersonViewRow;

    private void SetButtons()
    {
        var writable = _storeReady && !_readOnly;
        var visibility = writable ? Visibility.Visible : Visibility.Collapsed;
        PersonButtons.Visibility = visibility;
        IdentityButtons.Visibility = visibility;
        GrantButtons.Visibility = visibility;
    }

    internal async Task LoadAsync()
    {
        try
        {
            var result = await _dataService.ReadPeopleAsync();
            _storeReady = !result.StoreNeedsUpgrade;
            SetButtons();
            if (result.StoreNeedsUpgrade)
            {
                PeopleGrid.ItemsSource = null;
                DetailPanel.Visibility = Visibility.Collapsed;
                EmptyMessage.Text = PeopleText.NeedsUpgradeNote;
                EmptyMessage.Visibility = Visibility.Visible;
                return;
            }

            DetailPanel.Visibility = Visibility.Visible;
            var selectedId = SelectedPerson?.PersonId;
            var rows = PersonViewRow.From(result.People);
            PeopleGrid.ItemsSource = rows;
            PeopleGrid.SelectedItem = rows.FirstOrDefault(r => r.PersonId == selectedId) ?? (rows.Count > 0 ? rows[0] : null);
            EmptyMessage.Text = PeopleText.NoPeopleNote;
            EmptyMessage.Visibility = rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            ShowDetail();
            if (_readOnly)
                StatusText.Text = PeopleText.ReadOnlyNote + ViewerReadOnlyException.HowToConnectAsAdmin;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            StatusText.Text = "Could not read the people: " + ex.Message;
        }
    }

    private void ShowDetail()
    {
        var person = SelectedPerson;
        IdentitiesGrid.ItemsSource = person?.Identities;
        GrantsGrid.ItemsSource = person?.Grants;
        IdentitiesHeader.Text = person is null ? "Identities" : "Identities of " + person.DisplayName;
        GrantsHeader.Text = person is null ? "Grants" : "Grants of " + person.DisplayName;
        ToggleButton.Content = person is { Enabled: false } ? "_Enable" : "_Disable";
    }

    private void PeopleGrid_SelectionChanged(object sender, SelectionChangedEventArgs e) => ShowDetail();

    /// <summary>Sends one command, shows the service's sentence and reads the lists again. Returns whether the change took.</summary>
    private async Task<bool> RunAsync(Func<Task<PersonCommandAnswer>> send)
    {
        try
        {
            StatusText.Text = "Asking the service...";
            var answer = await send();
            StatusText.Text = answer.Message;
            await LoadAsync();
            StatusText.Text = answer.Message;
            return answer.Succeeded;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            StatusText.Text = "Could not make the change: " + ex.Message;
            return false;
        }
    }

    private async void AddPerson_Click(object sender, RoutedEventArgs e)
    {
        var dialog = new TagNameDialog("Add person", "Display name:", "", PersonDirectory.MaxDisplayNameLength) { Owner = this };
        if (dialog.ShowDialog() == true)
            await RunAsync(() => _dataService.AddPersonAsync(dialog.TagName, _login));
    }

    private async void RenamePerson_Click(object sender, RoutedEventArgs e)
    {
        if (SelectedPerson is not { } person)
        {
            StatusText.Text = "Select a person first.";
            return;
        }

        var dialog = new TagNameDialog("Rename person", "Display name:", person.DisplayName, PersonDirectory.MaxDisplayNameLength) { Owner = this };
        if (dialog.ShowDialog() == true)
            await RunAsync(() => _dataService.RenamePersonAsync(person.PersonId, dialog.TagName, _login));
    }

    private async void TogglePerson_Click(object sender, RoutedEventArgs e)
    {
        if (SelectedPerson is not { } person)
        {
            StatusText.Text = "Select a person first.";
            return;
        }

        await RunAsync(() => _dataService.SetPersonEnabledAsync(person.PersonId, !person.Enabled, _login));
    }

    private async void AddIdentity_Click(object sender, RoutedEventArgs e)
    {
        if (SelectedPerson is not { } person)
        {
            StatusText.Text = "Select a person first.";
            return;
        }

        var dialog = new PersonIdentityDialog(person.DisplayName) { Owner = this };
        if (dialog.ShowDialog() == true)
            await RunAsync(() => _dataService.AddPersonIdentityAsync(person.PersonId, dialog.Kind, dialog.ExternalId, _login));
    }

    private async void RemoveIdentity_Click(object sender, RoutedEventArgs e)
    {
        if (IdentitiesGrid.SelectedItem is not PersonIdentityViewRow identity)
        {
            StatusText.Text = "Select an identity first.";
            return;
        }

        var ask = MessageBox.Show(this, "Remove the " + identity.KindDisplay + " identity " + identity.ExternalId + "?" + Environment.NewLine + Environment.NewLine
            + "That sign-in will no longer map to " + SelectedPerson?.DisplayName + ".", "Remove identity", MessageBoxButton.OKCancel, MessageBoxImage.Warning);
        if (ask == MessageBoxResult.OK)
            await RunAsync(() => _dataService.RemovePersonIdentityAsync(identity.IdentityId, _login));
    }

    private async void AddGrant_Click(object sender, RoutedEventArgs e)
    {
        if (SelectedPerson is not { } person)
        {
            StatusText.Text = "Select a person first.";
            return;
        }

        ViewerRouteScopeInputs? inputs = null;
        try
        {
            inputs = await _dataService.GetNotificationRouteScopeInputsAsync();
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            StatusText.Text = "Could not read the servers and tags: " + ex.Message;
        }

        var dialog = new PersonGrantDialog(person.DisplayName, inputs) { Owner = this };
        if (dialog.ShowDialog() == true)
            await RunAsync(() => _dataService.AddPersonGrantAsync(person.PersonId, dialog.Capability, dialog.Kinds, dialog.ScopeServerId, dialog.ScopeTagId, _login));
    }

    private async void RemoveGrant_Click(object sender, RoutedEventArgs e)
    {
        if (GrantsGrid.SelectedItem is not PersonGrantViewRow grant)
        {
            StatusText.Text = "Select a grant first.";
            return;
        }

        var ask = MessageBox.Show(this, "Remove the grant \"" + grant.CapabilityDisplay + "\" for " + grant.ScopeDisplay + "?", "Remove grant",
            MessageBoxButton.OKCancel, MessageBoxImage.Warning);
        if (ask == MessageBoxResult.OK)
            await RunAsync(() => _dataService.RemovePersonGrantAsync(grant.GrantId, _login));
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();
}
