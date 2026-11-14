/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// The Store Health window (#8788): the size of Lite's own store against its reset point, the archive folder, the housekeeping
/// history and the rows per table. Read only. Each read runs off the UI thread through <see cref="StoreHealthReader"/>, which takes
/// only the store's read lock, so collection goes on while the window is open.
/// </summary>
public partial class StoreHealthWindow : Window
{
    /// <summary>One housekeeping history line, formatted for the grid.</summary>
    internal sealed record HistoryRow(string Time, DateTime TimeUtc, string Kind, string Outcome, string Work, string UsedMb, int Count, string Message, bool IsFailed)
    {
        /// <summary>The message on one line for the cell. The tooltip carries the full text.</summary>
        public string MessageLine => string.Join(" ", Message.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));
    }

    /// <summary>One table line, formatted for the grid.</summary>
    internal sealed record TableRow(string Name, string Rows, long EstimatedRows);

    private readonly StoreHealthReader _reader;
    private int _readSeq;

    internal StoreHealthWindow(StoreHealthReader reader)
    {
        InitializeComponent();
        _reader = reader;
        Loaded += async (_, _) => await LoadAsync();
    }

    private async void Refresh_Click(object sender, RoutedEventArgs e) => await LoadAsync();

    private async Task LoadAsync()
    {
        var seq = ++_readSeq;
        RefreshButton.IsEnabled = false;
        StatusText.Text = "Loading...";
        try
        {
            var snapshot = await Task.Run(_reader.Read);
            if (seq != _readSeq)
            {
                return;
            }

            Apply(snapshot);
        }
        catch (Exception ex)
        {
            AppLogger.Warn("StoreHealth", $"The Store Health window could not read the store: {ex.Message}");
            if (seq == _readSeq)
            {
                StatusText.Text = "The read failed: " + ex.Message;
            }
        }
        finally
        {
            if (seq == _readSeq)
            {
                RefreshButton.IsEnabled = true;
            }
        }
    }

    private void Apply(StoreHealthSnapshot snapshot)
    {
        var ci = CultureInfo.InvariantCulture;
        StatusText.Text = $"Read at {snapshot.ReadAtUtc.ToLocalTime():HH:mm:ss}";

        var store = snapshot.Store;
        StoreSentenceText.Text = StoreHealthText.StoreSentence(store);
        StoreFactsText.Text = string.Create(ci,
            $"Store file on disk: {StoreHealthText.Megabytes(store.FileMb)} of {store.ResetAtMb:0} MB ({store.FilePercentOfReset:0}%).   Data inside the file: {(store.UsedMb is { } used ? StoreHealthText.Megabytes(used) : "not read")}.   Write-ahead log: {StoreHealthText.Megabytes(store.WalMb)}.");
        StoreNoteText.Text = snapshot.StoreNote ?? "";
        StoreNoteText.Visibility = snapshot.StoreNote is null ? Visibility.Collapsed : Visibility.Visible;

        var a = snapshot.Archives;
        var undated = a.UndatedFiles > 0 ? "   " + StoreHealthText.UndatedSentence(a.UndatedFiles) : "";
        ArchiveFactsText.Text = string.Create(ci,
            $"{a.FileCount:N0} parquet files, {StoreHealthText.Bytes(a.TotalBytes)} on disk.   Oldest month: {StoreHealthText.Month(a.OldestMonth)}.   Newest month: {StoreHealthText.Month(a.NewestMonth)}.") + undated;
        ArchiveSentenceText.Text = StoreHealthText.ArchiveSentence(a);

        HistoryGrid.ItemsSource = snapshot.History.Select(e => new HistoryRow(
            e.TimeUtc.ToString("yyyy-MM-dd HH:mm", ci),
            e.TimeUtc,
            StoreHealthText.KindLabel(e.Kind),
            StoreHealthText.OutcomeLabel(e.Outcome),
            StoreHealthText.WorkSummary(e),
            e.UsedMb is { } mb ? mb.ToString("0", ci) : "",
            e.Count,
            e.Message ?? "",
            e.Outcome == StoreHousekeepingHistory.OutcomeFailed)).ToList();
        NoHistoryMessage.Visibility = snapshot.History.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

        TablesGrid.ItemsSource = snapshot.Tables?.Select(t => new TableRow(t.Name, t.EstimatedRows.ToString("N0", ci), t.EstimatedRows)).ToList();
        NoTablesMessage.Visibility = snapshot.Tables is null ? Visibility.Visible : Visibility.Collapsed;
    }
}
