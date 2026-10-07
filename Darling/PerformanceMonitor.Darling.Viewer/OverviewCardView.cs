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
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The pure projection behind the Overview card grid (#5352): the full card set, narrowed by the
/// needs-attention toggle (#2424) AND the search box, plus the count line that explains what that did.
/// Kept free of WPF so the composition can be unit-tested, and so <c>ApplyOverviewCardFilter</c> stays the one
/// place that decides what the grid holds.
/// </summary>
public static class OverviewCardView
{
    /// <summary>
    /// The fields a search term matches for a card: display name, instance name, and each tag pill's name, so
    /// "prod" finds <c>sql-prod-01</c> and every server tagged Production. The same fields as Lite's Overview
    /// search and the sidebar's.
    /// </summary>
    public static string?[] SearchFields(ServerSummaryItem card)
    {
        ArgumentNullException.ThrowIfNull(card);

        var fields = new List<string?> { card.DisplayName, card.ServerName };
        fields.AddRange(card.TagPills.Select(p => p.Name));
        return fields.ToArray();
    }

    /// <summary>
    /// The cards the grid shows: every card, narrowed by the needs-attention predicate when
    /// <paramref name="attentionOnly"/> is on, and by the search when it is non-blank. Both on means the
    /// intersection. The caller's order is kept, so the chosen sort survives.
    /// </summary>
    public static List<ServerSummaryItem> Project(
        IReadOnlyList<ServerSummaryItem> cards, bool attentionOnly, string? search)
    {
        ArgumentNullException.ThrowIfNull(cards);

        IEnumerable<ServerSummaryItem> shown = attentionOnly ? FleetRollup.NeedsAttention(cards) : cards;

        if (ServerOverviewFilter.Normalize(search) is { } term)
        {
            shown = shown.Where(c => ServerOverviewFilter.Matches(term, SearchFields(c)));
        }

        return shown.ToList();
    }

    /// <summary>
    /// The line beside the toggle, or null when nothing narrows the grid (no toggle, no search): a grid
    /// showing every server needs no arithmetic. A search that emptied the grid says so itself; it must never
    /// fall through to the all-clear, which claims the fleet is healthy.
    /// </summary>
    public static string? CountText(
        IReadOnlyList<ServerSummaryItem> cards, int shown, bool attentionOnly, string? search)
    {
        ArgumentNullException.ThrowIfNull(cards);

        var searching = ServerOverviewFilter.Normalize(search) is not null;

        if (!searching)
        {
            return attentionOnly ? FleetRollup.AttentionFilterCountText(shown, cards.Count) : null;
        }

        if (cards.Count == 0)
        {
            return attentionOnly ? FleetRollup.AttentionFilterCountText(shown, cards.Count) : null;
        }

        if (shown > 0)
        {
            return $"showing {shown} of {cards.Count}";
        }

        /* Empty grid with a search on. When the toggle is also on, the search may have matched cards that are
           simply healthy; saying "no match" would then be false, and the all-clear would be a lie about the
           whole fleet, so that case gets its own sentence. */
        if (attentionOnly && Project(cards, attentionOnly: false, search).Count > 0)
        {
            return "No server matching the search needs attention.";
        }

        return ServerOverviewFilter.NoMatchText;
    }

    /// <summary>True when the count line's sentence is the search's, not the attention filter's, so the viewer
    /// colours it neutrally instead of amber (a count) or green (an all-clear).</summary>
    public static bool IsSearchLine(int shown, bool attentionOnly, string? search, int cardCount) =>
        cardCount > 0 && ServerOverviewFilter.Normalize(search) is not null && (shown == 0 || !attentionOnly);
}
