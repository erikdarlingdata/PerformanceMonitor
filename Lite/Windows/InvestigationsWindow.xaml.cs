/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite.Investigations;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// The investigations panel (#5904): the requests agents were asked to look into, newest first, with the reports and the
/// journal of the picked one. Read only; every read goes through <see cref="InvestigationPanelReader"/>, which masks
/// reports, briefs and journal text at the level the MCP investigation tools serve. The same columns and grouping as the
/// Investigations page of the Darling web UI.
/// </summary>
public partial class InvestigationsWindow : Window
{
    private sealed record Choice(string Value, string Label);

    private static readonly Choice[] States =
    {
        new("", "All states"), new("open", "Open"), new("claimed", "Claimed"),
        new("reported", "Reported"), new("closed", "Closed"), new("expired", "Expired"),
    };

    private static readonly Choice[] Families =
    {
        new("", "All families"), new("blocking", "Blocking"), new("query_performance", "Query performance"),
        new("resources", "Resources"), new("availability", "Availability"), new("jobs", "Jobs"),
        new("self_monitor", "Self monitor"), new("other", "Other"),
    };

    private readonly InvestigationPanelReader _reader;
    private int _listSeq;
    private int _detailSeq;
    private bool _ready;

    internal InvestigationsWindow(InvestigationPanelReader reader)
    {
        InitializeComponent();
        _reader = reader;
        StateCombo.ItemsSource = States;
        StateCombo.SelectedIndex = 0;
        FamilyCombo.ItemsSource = Families;
        FamilyCombo.SelectedIndex = 0;
        _ready = true;
        Loaded += async (_, _) => await LoadListAsync();
    }

    private async Task LoadListAsync()
    {
        var seq = ++_listSeq;
        var state = StateCombo.SelectedValue as string;
        var family = FamilyCombo.SelectedValue as string;
        StatusText.Text = "Loading...";
        try
        {
            var (requests, truncated) = await Task.Run(() => _reader.ListAsync(state, family));
            if (seq != _listSeq)
            {
                return;
            }

            RequestsGrid.ItemsSource = requests;
            StatusText.Text = requests.Count == 0
                ? "No investigation requests."
                : requests.Count + " requests shown" + (truncated ? "; older ones are not listed. Narrow the state or family." : "");
            ClearDetail("Pick a request to see its reports and journal.");
        }
        catch (Exception ex)
        {
            AppLogger.Error("Investigations", $"Could not read the investigation requests: {ex.Message}");
            if (seq == _listSeq)
            {
                StatusText.Text = "Could not read the investigation requests: " + ex.Message;
            }
        }
    }

    private async Task LoadDetailAsync(Guid requestId)
    {
        var seq = ++_detailSeq;
        DetailHead.Text = "Loading this request...";
        try
        {
            var detail = await Task.Run(() => _reader.ReadDetailAsync(requestId));
            if (seq != _detailSeq)
            {
                return;
            }

            if (detail is null)
            {
                ClearDetail("This request is gone.");
                return;
            }

            ShowDetail(detail);
        }
        catch (Exception ex)
        {
            AppLogger.Error("Investigations", $"Could not read investigation request {requestId}: {ex.Message}");
            if (seq == _detailSeq)
            {
                ClearDetail("Could not read this request: " + ex.Message);
            }
        }
    }

    private void ShowDetail(InvestigationPanelDetail detail)
    {
        var r = detail.Request;
        var facts = string.Join(" | ", new[]
        {
            "State " + r.State,
            r.ClaimedBy is null ? "no agent" : "agent " + r.ClaimedBy,
            detail.Claimed.Length > 0 ? "claimed " + detail.Claimed : "never claimed",
            r.ReportedAtUtc is null ? "not reported" : "reported " + InvestigationPanelReader.LocalText(r.ReportedAtUtc),
        });
        DetailHead.Text = "Request on " + r.ServerName + " (" + r.Family + "): " + facts;
        ReportsList.ItemsSource = detail.Reports;
        NoReportsText.Visibility = detail.Reports.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        var hasBrief = detail.Brief is not null;
        BriefHead.Visibility = BriefBox.Visibility = hasBrief ? Visibility.Visible : Visibility.Collapsed;
        BriefBox.Text = detail.Brief ?? "";
        JournalGrid.ItemsSource = detail.Journal;
        JournalNote.Visibility = detail.JournalTruncated ? Visibility.Visible : Visibility.Collapsed;
    }

    private void ClearDetail(string head)
    {
        DetailHead.Text = head;
        ReportsList.ItemsSource = Array.Empty<InvestigationPanelReport>();
        NoReportsText.Visibility = Visibility.Collapsed;
        BriefHead.Visibility = BriefBox.Visibility = Visibility.Collapsed;
        JournalGrid.ItemsSource = Array.Empty<InvestigationPanelJournalRow>();
        JournalNote.Visibility = Visibility.Collapsed;
    }

    private async void Filter_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_ready)
        {
            await LoadListAsync();
        }
    }

    private async void Refresh_Click(object sender, RoutedEventArgs e) => await LoadListAsync();

    private async void RequestsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (RequestsGrid.SelectedItem is InvestigationPanelRequest picked)
        {
            await LoadDetailAsync(picked.RequestId);
        }
    }
}
