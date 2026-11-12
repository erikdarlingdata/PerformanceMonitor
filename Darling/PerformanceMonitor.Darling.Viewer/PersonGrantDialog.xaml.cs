/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>Asks for one grant of a person (#7107). The scope picker is the route editor's (<see cref="ViewerNotificationRouteScope"/>) with the fleet worded as the fleet.</summary>
public partial class PersonGrantDialog : Window
{
    private sealed record CapabilityOption(string Capability, string Label);

    public PersonGrantDialog(string personName, ViewerRouteScopeInputs? scopeInputs)
    {
        InitializeComponent();
        HeaderText.Text = "Add a grant for " + personName;
        CapabilityCombo.ItemsSource = PersonCapabilities.All.Select(c => new CapabilityOption(c, PeopleText.CapabilityLabel(c))).ToList();
        CapabilityCombo.SelectedIndex = 0;
        foreach (var kind in ServerActionNames.All)
            KindsList.Children.Add(new CheckBox { Content = ServerActionNames.Label(kind), Tag = kind, Margin = new Thickness(0, 0, 0, 3) });

        var options = ViewerNotificationRouteScope.ScopeOptions(scopeInputs);
        options[0] = options[0] with { Label = "The whole fleet" };
        ScopeCombo.ItemsSource = options;
        ScopeCombo.SelectedIndex = 0;
        if (scopeInputs is null)
            ScopeNote.Text = "The servers and tags could not be read, so only the whole fleet is offered.";
    }

    public string Capability => (CapabilityCombo.SelectedItem as CapabilityOption)?.Capability ?? "";

    public IReadOnlyList<string> Kinds => PersonGrantEditor.ShowsKinds(Capability)
        ? KindsList.Children.OfType<CheckBox>().Where(c => c.IsChecked == true).Select(c => (string)c.Tag).ToList()
        : new List<string>();

    public int? ScopeServerId => ViewerNotificationRouteScope.ScopeIds(ScopeCombo.SelectedItem as RouteScopeOption).ServerId;

    public int? ScopeTagId => ViewerNotificationRouteScope.ScopeIds(ScopeCombo.SelectedItem as RouteScopeOption).TagId;

    private void Capability_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (KindsPanel is null)
            return;
        KindsPanel.Visibility = PersonGrantEditor.ShowsKinds(Capability) ? Visibility.Visible : Visibility.Collapsed;
        ErrorText.Visibility = Visibility.Collapsed;
    }

    private void Ok_Click(object sender, RoutedEventArgs e)
    {
        if (PersonGrantEditor.Error(Capability, Kinds) is { } error)
        {
            ErrorText.Text = error;
            ErrorText.Visibility = Visibility.Visible;
            return;
        }

        DialogResult = true;
    }
}
