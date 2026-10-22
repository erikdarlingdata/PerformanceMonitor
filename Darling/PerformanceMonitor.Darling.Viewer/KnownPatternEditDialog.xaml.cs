/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Windows;
using PerformanceMonitor.Common.KnownPatterns;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The add and edit form for one known-pattern entry (#6334). Save checks the form through
/// <see cref="KnownPatternsEditorModel.TryBuild"/> and shows its refusal text in place, leaving the dialog open.
/// </summary>
public partial class KnownPatternEditDialog : Window
{
    /// <summary>One entry in the server box: a registered server, or null for every server.</summary>
    public sealed record ServerChoice(int? ServerId, string Display);

    private readonly List<ServerChoice> _servers;

    /// <summary>The accepted input; set when the dialog closes with true.</summary>
    public KnownPatternInput? Input { get; private set; }

    public KnownPatternEditDialog(IReadOnlyList<(int ServerId, string Name)> servers, KnownPattern? existing = null)
    {
        InitializeComponent();

        _servers = new List<ServerChoice> { new(null, "(any server)") };
        foreach (var (serverId, name) in servers)
        {
            _servers.Add(new ServerChoice(serverId, name));
        }

        /* An entry for a server that is no longer registered keeps its id visible, so editing it does not silently widen it to every server. */
        if (existing?.ServerId is { } existingId && !_servers.Exists(s => s.ServerId == existingId))
        {
            _servers.Add(new ServerChoice(existingId, $"#{existingId.ToString(CultureInfo.InvariantCulture)} (not registered)"));
        }

        ServerCombo.ItemsSource = _servers;
        MetricCombo.ItemsSource = KnownPatternsEditorModel.MetricChoices;

        if (existing is null)
        {
            ServerCombo.SelectedIndex = 0;
            TimeZoneBox.Text = ViewerDataService.DefaultKnownPatternTimeZone();
            WindowBox.Text = KnownPatternMatcher.DefaultWindowMinutes.ToString(CultureInfo.InvariantCulture);
        }
        else
        {
            Title = "Edit Known Pattern";
            HeaderText.Text = "Edit Known Pattern";
            ServerCombo.SelectedItem = _servers.Find(s => s.ServerId == existing.ServerId);
            MetricCombo.Text = existing.MetricName ?? "";
            TimeOfDayBox.Text = existing.TimeOfDay is { } time ? time.ToString("HH:mm", CultureInfo.InvariantCulture) : "";
            TimeZoneBox.Text = existing.TimeZone ?? "";
            WindowBox.Text = existing.WindowMinutes.ToString(CultureInfo.InvariantCulture);
            NoteBox.Text = existing.Note;
            ExpiresBox.Text = existing.ExpiresAtUtc is { } at ? at.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) : "";
        }
    }

    private void Save_Click(object sender, RoutedEventArgs e)
    {
        var serverId = (ServerCombo.SelectedItem as ServerChoice)?.ServerId;
        var refusal = KnownPatternsEditorModel.TryBuild(
            serverId, MetricCombo.Text, TimeOfDayBox.Text, TimeZoneBox.Text, WindowBox.Text, NoteBox.Text, ExpiresBox.Text, out var input);
        if (refusal is not null)
        {
            MessageBox.Show(refusal, "Known Pattern", MessageBoxButton.OK, MessageBoxImage.Warning);
            return;
        }

        Input = input;
        DialogResult = true;
    }

    private void Cancel_Click(object sender, RoutedEventArgs e) => DialogResult = false;
}
