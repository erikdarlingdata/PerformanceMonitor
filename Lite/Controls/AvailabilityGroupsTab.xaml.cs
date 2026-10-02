/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

/// <summary>
/// Lite's Availability Groups tab (#991) — the same surface as the Darling viewer's, over local DuckDB instead of
/// the central store, and drawn from the same shared <see cref="AgTopology"/> projection so the two apps cannot
/// disagree about an AG.
///
/// <para>Hidden until the store actually has AG rows: Always On is opt-in, and a permanently empty tab is noise.
/// The shell owns the reveal (it owns the TabItem); this control reports what it found via
/// <see cref="HasAvailabilityGroups"/>. Shown with no rows, it renders an honest empty state rather than a blank
/// panel.</para>
/// </summary>
public partial class AvailabilityGroupsTab : UserControl
{
    private LocalDataService? _dataService;
    private bool _loading;

    /// <summary>The one long-lived card collection AgCards is ever bound to (#4238) — a refresh reconciles this
    /// in place through <see cref="AgTopology.Reconcile{TItem,TKey}"/> rather than replacing it, so an unchanged
    /// AG's realized container survives the refresh.</summary>
    private readonly ObservableCollection<AgTopologyCard> _cards = new();

    /// <summary>The last render's fingerprint (<see cref="AgTopology.ComputeDigest"/>). A refresh whose rows hash
    /// the same skips reconciling and re-binding entirely.</summary>
    private int? _lastDigest;

    /// <summary>True once a load has seen at least one AG — the shell's cue to reveal the tab.</summary>
    public bool HasAvailabilityGroups { get; private set; }

    public AvailabilityGroupsTab()
    {
        InitializeComponent();
        AgCards.ItemsSource = _cards;
    }

    /// <summary>Wires the data service. Nothing is read until the first refresh.</summary>
    public void Initialize(LocalDataService dataService)
    {
        _dataService = dataService;
    }

    /// <summary>Reloads the AG topology (the shell calls this on tab activation and on its refresh cycle).</summary>
    public Task RefreshAgAsync() => LoadAsync();

    private async void RefreshButton_Click(object sender, RoutedEventArgs e) => await LoadAsync();

    private async Task LoadAsync()
    {
        if (_dataService is null || _loading)
        {
            return;
        }

        _loading = true;
        try
        {
            var cards = await _dataService.GetAgTopologyAsync();

            /* Same rows as the last render — skip reconciling and re-binding entirely (#4238). */
            var digest = AgTopology.ComputeDigest(cards);
            if (_lastDigest == digest)
            {
                return;
            }

            _lastDigest = digest;
            Render(cards);
        }
        catch (Exception ex)
        {
            /* A failed read must not blank a tab that was showing good data a moment ago. */
            AppLogger.Debug("AvailabilityGroups", $"Failed to load AG topology: {ex.Message}");
        }
        finally
        {
            _loading = false;
        }
    }

    /// <summary>Applies a freshly read topology to the tab. <c>internal</c> so tests can drive it directly with
    /// constructed card lists (#4238), the same way <see cref="BuildSummary"/> already is.</summary>
    internal void Render(List<AgTopologyCard> cards)
    {
        HasAvailabilityGroups = cards.Count > 0;

        /* Reconciled in place, keyed by (reporting server, AG): an unchanged AG keeps its card instance (and so
           its realized WPF container); only a topology change adds or removes one (#4238). */
        AgTopology.Reconcile(_cards, cards, AgTopology.CardKey, static (existing, updated) => existing.UpdateFrom(updated));

        EmptyState.Visibility = cards.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        AgCards.Visibility = cards.Count == 0 ? Visibility.Collapsed : Visibility.Visible;
        SummaryText.Text = BuildSummary(cards);
    }

    /// <summary>
    /// The counts. Groups and views differ exactly when an AG is reported by more than one monitored server, so a
    /// reader seeing one AG name twice gets it said out loud instead of inferring a bug. Counted through the
    /// shared projection, so this header and Darling's cannot drift.
    /// </summary>
    internal static string BuildSummary(IReadOnlyList<AgTopologyCard> cards)
    {
        if (cards.Count == 0)
        {
            return "none observed";
        }

        var (distinctAgs, servers, views) = AgTopology.Counts(cards);

        return $"{distinctAgs} group{(distinctAgs == 1 ? "" : "s")} · {servers} reporting server{(servers == 1 ? "" : "s")} · {views} view{(views == 1 ? "" : "s")}";
    }
}
