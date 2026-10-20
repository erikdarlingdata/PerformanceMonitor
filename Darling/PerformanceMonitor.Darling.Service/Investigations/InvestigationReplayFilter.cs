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
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The one filter for a stored brief (#6236, #5918): while a practice request replays case X, X is left out of every other
/// case's <c>earlier_reports</c> (and of its <c>prior_links</c>). A brief is composed when its case is claimed and stored as it
/// was, so a brief stored before the practice started still lists X's report. The investigation export and the investigation
/// report read both call this, so the two cannot disagree. When the practice ends the case is no longer in the replayed set and
/// the stored brief is served unchanged.
/// </summary>
internal static class InvestigationReplayFilter
{
    /// <summary><paramref name="briefJson"/> without the reports and links of the cases in <paramref name="replayed"/>. The text is
    /// returned as it was when nothing is removed; a brief that cannot be read is withheld (null) while a practice is live.</summary>
    internal static string? WithoutReplayed(string? briefJson, IReadOnlySet<Guid> replayed)
    {
        ArgumentNullException.ThrowIfNull(replayed);
        if (briefJson is null || replayed.Count == 0)
        {
            return briefJson;
        }

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(briefJson);
        }
        catch (System.Text.Json.JsonException)
        {
            return null;
        }

        if (root is not JsonObject brief)
        {
            return null;
        }

        var changed = RemoveReplayed(brief, "earlier_reports", replayed) | RemoveReplayed(brief, "prior_links", replayed);
        return changed ? brief.ToJsonString() : briefJson;
    }

    private static bool RemoveReplayed(JsonObject brief, string section, IReadOnlySet<Guid> replayed)
    {
        if (brief[section] is not JsonArray entries)
        {
            return false;
        }

        var drop = entries
            .Where(entry => entry is JsonObject o
                && o["request_id"] is JsonNode id
                && Guid.TryParse((string?)id, out var request)
                && replayed.Contains(request))
            .ToList();
        foreach (var entry in drop)
        {
            entries.Remove(entry);
        }

        return drop.Count > 0;
    }
}
