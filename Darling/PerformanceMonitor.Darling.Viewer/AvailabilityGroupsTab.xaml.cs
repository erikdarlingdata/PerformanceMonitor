/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;

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

    /// <summary>Raised with a short status message on load outcomes so the shell can show it.</summary>
    public event Action<string>? StatusChanged;

    /// <summary>True once a load has seen at least one AG — the shell's cue to reveal the tab.</summary>
    public bool HasAvailabilityGroups { get; private set; }

    public AvailabilityGroupsTab()
    {
        InitializeComponent();
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

    private void Render(List<ViewerAvailabilityGroup> groups)
    {
        HasAvailabilityGroups = groups.Count > 0;

        AgCards.ItemsSource = groups;
        EmptyState.Visibility = groups.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
        CardScroller.Visibility = groups.Count == 0 ? Visibility.Collapsed : Visibility.Visible;
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
    internal static string BuildSummary(IReadOnlyList<ViewerAvailabilityGroup> groups)
    {
        if (groups.Count == 0)
        {
            return "none observed";
        }

        var distinctAgs = groups
            .Select(g => (g.AgName ?? "").ToUpperInvariant())
            .Distinct(StringComparer.Ordinal)
            .Count();

        var servers = groups.Select(g => g.ServerId).Distinct().Count();

        return $"{distinctAgs} group{(distinctAgs == 1 ? "" : "s")} · {servers} reporting server{(servers == 1 ? "" : "s")} · {groups.Count} view{(groups.Count == 1 ? "" : "s")}";
    }
}
