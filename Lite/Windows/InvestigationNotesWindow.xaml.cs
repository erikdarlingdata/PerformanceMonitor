/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Helpers;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// The agent-note approve screen (#5903): the pending notes of Level 1 investigations over <see cref="IInvestigationNoteStore"/>,
/// with Approve and Reject. A decision is stamped <c>person:&lt;Windows user&gt;</c> (Lite has no login) and the time. Lite stays
/// advise-only for agents: this window is the only way a note is decided. Every write first asks
/// <see cref="PendingRestoreNotice.Refuse"/>, because a pending archive restore of <c>investigation_notes</c> would otherwise
/// be overwritten by the restore.
/// </summary>
public partial class InvestigationNotesWindow : Window
{
    /// <summary>The table name a pending restore is checked against (the store's own).</summary>
    private const string RestoreTable = "investigation_notes";

    private readonly IInvestigationNoteStore _store;
    private readonly ObservableCollection<PendingNoteRow> _rows = new();

    internal InvestigationNotesWindow(IInvestigationNoteStore store)
    {
        InitializeComponent();
        _store = store ?? throw new ArgumentNullException(nameof(store));
        NotesGrid.ItemsSource = _rows;
        Loaded += async (_, _) => await LoadNotesAsync();
    }

    /// <summary>Who a decision is stamped as: <c>person:</c> plus the Windows user, the prefix Darling's viewer stamps too.</summary>
    internal static string Decider() => "person:" + (string.IsNullOrWhiteSpace(Environment.UserName) ? "viewer" : Environment.UserName);

    private async Task LoadNotesAsync()
    {
        try
        {
            var notes = await _store.ListPendingNotesAsync();
            _rows.Clear();
            foreach (var note in notes)
            {
                _rows.Add(new PendingNoteRow(note));
            }

            NoNotesMessage.Visibility = _rows.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        }
        catch (Exception ex)
        {
            ShowError("Could not read the pending notes", ex);
        }
    }

    private async void Approve_Click(object sender, RoutedEventArgs e) => await DecideAsync(approve: true);

    private async void Reject_Click(object sender, RoutedEventArgs e) => await DecideAsync(approve: false);

    private async void Refresh_Click(object sender, RoutedEventArgs e) => await LoadNotesAsync();

    private async Task DecideAsync(bool approve)
    {
        if (PendingRestoreNotice.Refuse(RestoreTable, this)) return;
        if (NotesGrid.SelectedItem is not PendingNoteRow selected) return;
        try
        {
            var decided = approve
                ? await _store.ApproveNoteAsync(selected.NoteId, Decider())
                : await _store.RejectNoteAsync(selected.NoteId, Decider());
            if (!decided)
            {
                ShowMessage("This note was already decided or no longer exists.");
            }
        }
        catch (Exception ex)
        {
            ShowError(approve ? "Could not approve the note" : "Could not reject the note", ex);
        }

        await LoadNotesAsync();
    }

    private void Close_Click(object sender, RoutedEventArgs e) => Close();

    private void ShowMessage(string text) =>
        MessageBox.Show(this, text, "Agent Notes", MessageBoxButton.OK, MessageBoxImage.Warning);

    private void ShowError(string what, Exception ex) => ShowMessage($"{what}: {ex.Message}");
}

/// <summary>One line of the Agent Notes list: the pending note and the display text built from it.</summary>
internal sealed class PendingNoteRow
{
    public PendingNoteRow(PendingInvestigationNote note)
    {
        NoteId = note.NoteId;
        NoteText = note.NoteText;
        Author = note.Author;
        ServerDisplay = note.ServerName
            ?? (note.ServerId is int id ? "server " + id.ToString(CultureInfo.InvariantCulture) : "Fleet");
        MetricDisplay = note.MetricName ?? "";
        FamilyDisplay = note.Family ?? "";
        RequestDisplay = note.SourceRequestId?.ToString("D") ?? "";
        ProposedDisplay = note.ProposedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture);
    }

    public long NoteId { get; }
    public string NoteText { get; }
    public string Author { get; }
    public string ServerDisplay { get; }
    public string MetricDisplay { get; }
    public string FamilyDisplay { get; }
    public string RequestDisplay { get; }
    public string ProposedDisplay { get; }
}
