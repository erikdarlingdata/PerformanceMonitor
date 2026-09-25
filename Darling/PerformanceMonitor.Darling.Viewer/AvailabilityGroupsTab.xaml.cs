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
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The fleet-level Availability Groups tab (#991) — the desktop twin of the web dashboard's AG page, following
/// the same self-loading control convention as <see cref="JobHistoryTab"/>: construct, <c>Initialize</c> with the
/// data service, then <c>RefreshAgAsync</c> from the shell on tab activation and on the refresh timer.
///
/// <para>The tab is HIDDEN until the store actually has AG rows — Always On is opt-in and most fleets have none,
/// so a permanently visible tab that only ever says "nothing here" is noise. The shell owns that reveal (it owns
/// the TabItem); this control reports whether it found anything via <see cref="HasAvailabilityGroups"/>. Deep
/// state is never lost by hiding: the tab renders an honest empty state if it is shown with no rows.</para>
/// </summary>
public partial class AvailabilityGroupsTab : UserControl
{
    private ViewerDataService? _dataService;
    private bool _loading;

    /// <summary>The one long-lived card collection AgCards is ever bound to (#4238) — a refresh reconciles this
    /// in place through <see cref="AgTopology.Reconcile{TItem,TKey}"/> rather than replacing it, so an unchanged
    /// AG's realized container survives the refresh.</summary>
    private readonly ObservableCollection<AgTopologyCard> _cards = new();

    /// <summary>The last render's fingerprint (<see cref="AgTopology.ComputeDigest"/>). A refresh whose rows hash
    /// the same skips reconciling and re-binding entirely — the common case, since the AG collectors run far
    /// less often than the shell's 30 s refresh timer.</summary>
    private int? _lastDigest;

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    /// <summary>True once a load has seen at least one AG — the shell's cue to reveal the tab.</summary>
    public bool HasAvailabilityGroups { get; private set; }

    public AvailabilityGroupsTab()
    {
        InitializeComponent();
        AgCards.ItemsSource = _cards;
    }

    /// <summary>Wires the data service. Nothing is read until the first refresh.</summary>
    public void Initialize(ViewerDataService dataService)
    {
        _dataService = dataService;
    }

    /// <summary>Reloads the fleet's AG topology (the shell calls this when the tab becomes visible / on the timer).</summary>
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
            var groups = await _dataService.GetAvailabilityGroupsAsync();

            /* Same rows as the last render — skip reconciling and re-binding 40+ cards entirely, rather than
               walking them to discover that nothing would change on screen (#4238). */
            var digest = AgTopology.ComputeDigest(groups);
            if (_lastDigest == digest)
            {
                return;
            }

            _lastDigest = digest;
            Render(groups);
        }
        catch (Exception ex)
        {
            /* A failed read must not blank a tab that was showing good data a moment ago; report and leave it. */
            StatusChanged?.Invoke($"Availability Groups failed to load: {ex.Message}");
        }
        finally
        {
            _loading = false;
        }
    }

    /// <summary>Applies a freshly read topology to the tab. <c>internal</c> so tests can drive it directly with
    /// constructed card lists, the same way <see cref="BuildSummary"/> already is (#4238) — bypassing the store
    /// read lets the refresh-in-place pins run without a live server.</summary>
    internal void Render(List<AgTopologyCard> groups)
    {
        HasAvailabilityGroups = groups.Count > 0;

        /* Reconciled in place, keyed by (reporting server, AG): an unchanged AG keeps its card instance (and so
           its realized WPF container); only a topology change adds or removes one (#4238). */
        AgTopology.Reconcile(_cards, groups, AgTopology.CardKey, static (existing, updated) => existing.UpdateFrom(updated));

        EmptyState.Visibility = groups.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        AgCards.Visibility = groups.Count == 0 ? Visibility.Collapsed : Visibility.Visible;
        SummaryText.Text = BuildSummary(groups);

        StatusChanged?.Invoke(groups.Count == 0
            ? "No Availability Groups observed."
            : $"Availability Groups: {SummaryText.Text}");
    }

    /// <summary>
    /// The counts, and why there are two of them: an AG whose replicas are all monitored produces one CARD per
    /// monitored replica, so "views" and "groups" legitimately differ and a reader seeing the same AG name twice
    /// needs that said out loud rather than inferred.
    /// </summary>
    internal static string BuildSummary(IReadOnlyList<AgTopologyCard> groups)
    {
        if (groups.Count == 0)
        {
            return "none observed";
        }

        /* Counted by the shared projection so Lite's header and this one cannot drift apart. */
        var (distinctAgs, servers, views) = AgTopology.Counts(groups);

        return $"{distinctAgs} group{(distinctAgs == 1 ? "" : "s")} · {servers} reporting server{(servers == 1 ? "" : "s")} · {views} view{(views == 1 ? "" : "s")}";
    }
}
