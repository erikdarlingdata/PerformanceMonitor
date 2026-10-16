/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Threading;
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
            var page = await PendingNotesPage.ReadAsync(_store);
            _rows.Clear();
            foreach (var note in page.Notes)
            {
                _rows.Add(new PendingNoteRow(note));
            }

            CutNotice.Text = page.CutNotice ?? "";
            CutNotice.Visibility = page.CutNotice is null ? Visibility.Collapsed : Visibility.Visible;

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

/// <summary>
/// One page of the pending list (#5903): the first <see cref="Limit"/> notes and, when more were waiting, the sentence that
/// says so. The store is asked for <see cref="Limit"/> + 1, so a queue of exactly <see cref="Limit"/> is not called cut. The
/// wording is the web Admin page's (<c>pendingNotesTruncatedNotice</c>): deciding the oldest notes shows the rest.
/// </summary>
internal sealed class PendingNotesPage
{
    /// <summary>How many notes the window lists.</summary>
    internal const int Limit = 200;

    private PendingNotesPage(IReadOnlyList<PendingInvestigationNote> notes, string? cutNotice)
    {
        Notes = notes;
        CutNotice = cutNotice;
    }

    public IReadOnlyList<PendingInvestigationNote> Notes { get; }

    /// <summary>The sentence over a list that was cut, else null.</summary>
    public string? CutNotice { get; }

    internal static async Task<PendingNotesPage> ReadAsync(IInvestigationNoteStore store, CancellationToken cancellationToken = default)
    {
        var read = await store.ListPendingNotesAsync(Limit + 1, cancellationToken);
        var shown = read.Take(Limit).ToList();
        return new PendingNotesPage(shown, read.Count > Limit
            ? "This list shows the oldest " + shown.Count.ToString(CultureInfo.InvariantCulture)
                + " pending notes and more are waiting. Approve or reject some and the next ones appear."
            : null);
    }
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
        ProposedLocal = note.ProposedAtUtc.ToLocalTime();
        ExpiresLocal = note.ExpiresAtUtc.ToLocalTime();
        ProposedDisplay = ProposedLocal.ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture);
        ExpiresDisplay = ExpiresLocal.ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture);
    }

    public long NoteId { get; }
    public string NoteText { get; }
    public string Author { get; }
    public string ServerDisplay { get; }
    public string MetricDisplay { get; }
    public string FamilyDisplay { get; }
    public string RequestDisplay { get; }
    public string ProposedDisplay { get; }

    /// <summary>The time the Proposed column sorts by (#5903): the column shows text, and sorting that text is not sorting the time.</summary>
    public DateTime ProposedLocal { get; }

    /// <summary>The time the Expires column sorts by (#5903), for the same reason as <see cref="ProposedLocal"/>.</summary>
    public DateTime ExpiresLocal { get; }

    /// <summary>When the note stops reaching a brief: approving it counts only before then (an expired note is not listed).</summary>
    public string ExpiresDisplay { get; }
}
