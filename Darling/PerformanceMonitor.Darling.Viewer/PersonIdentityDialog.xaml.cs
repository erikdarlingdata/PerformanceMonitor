/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>Asks for one identity of a person (#7107): a Slack team and user id, or a web sign-in key. The checks are <see cref="PersonIdentityEditor"/>'s.</summary>
public partial class PersonIdentityDialog : Window
{
    public PersonIdentityDialog(string personName)
    {
        InitializeComponent();
        HeaderText.Text = "Map an identity to " + personName;
        WebHelp.Text = PeopleText.WebKeyHelp;
    }

    public string Kind => SlackRadio.IsChecked == true ? PersonIdentityKinds.Slack : PersonIdentityKinds.Web;

    /// <summary>The valid external id. Only meaningful when the dialog returned true.</summary>
    public string ExternalId { get; private set; } = "";

    private void Kind_Changed(object sender, RoutedEventArgs e)
    {
        if (WebPanel is null || SlackPanel is null)
            return;
        WebPanel.Visibility = Kind == PersonIdentityKinds.Web ? Visibility.Visible : Visibility.Collapsed;
        SlackPanel.Visibility = Kind == PersonIdentityKinds.Slack ? Visibility.Visible : Visibility.Collapsed;
        ErrorText.Visibility = Visibility.Collapsed;
    }

    private void Ok_Click(object sender, RoutedEventArgs e)
    {
        var id = PersonIdentityEditor.TryBuild(Kind, SlackTeamBox.Text, SlackUserBox.Text, WebKeyBox.Text, out var error);
        if (id is null)
        {
            ErrorText.Text = error;
            ErrorText.Visibility = Visibility.Visible;
            return;
        }

        ExternalId = id;
        DialogResult = true;
    }
}
