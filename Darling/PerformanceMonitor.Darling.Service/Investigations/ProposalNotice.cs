/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Investigations;

/// <summary>
/// The text of the notice a new action proposal sends to its request's route (#7109). It carries the kind of action, the server and a link to
/// the proposal in the web, and nothing an agent wrote: no statement, no reason and no target. When nobody can approve the proposal it also
/// carries the sentence the route page and the agent's answer carry.
/// </summary>
public static class ProposalNotice
{
    /// <summary>The web route of one proposal; the id rides in the hash query, as the investigation link does.</summary>
    public const string Route = "/#/proposals?proposal=";

    /// <summary>The notice's title.</summary>
    public const string Title = "Agent action proposed";

    /// <summary>The line that stands in for the link when no web base URL is set.</summary>
    public const string NoLinkLine = "Open the proposal in the web or the Viewer.";

    /// <summary>The kind of action a proposal's tool runs, in plain words.</summary>
    public static string KindOf(string tool) => tool switch
    {
        "force_query_store_plan" => "force a Query Store plan",
        "unforce_query_store_plan" => "unforce a Query Store plan",
        "evict_cached_plan" => "evict a cached plan",
        "kill_session" => "kill a session",
        "recompile_module" => "recompile a module",
        "create_plan_guide" => "create a plan guide",
        "drop_plan_guide" => "drop a plan guide",
        _ => "run an action",
    };

    /// <summary>The proposal's link in the web, or null when the base is empty or not an absolute http or https URL.</summary>
    public static string? LinkFor(string? baseUrl, Guid proposalId)
    {
        var trimmed = baseUrl?.Trim().TrimEnd('/');
        if (string.IsNullOrEmpty(trimmed)
            || !Uri.TryCreate(trimmed, UriKind.Absolute, out var uri)
            || (!string.Equals(uri.Scheme, "http", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(uri.Scheme, "https", StringComparison.OrdinalIgnoreCase)))
        {
            return null;
        }

        return trimmed + Route + Uri.EscapeDataString(proposalId.ToString("D"));
    }

    /// <summary>The notice's current value (the kind) and detail (the sentence, the link or the line that stands in for it).</summary>
    public static (string CurrentValue, string Detail) Build(ProposalNoticeCandidate candidate, string? baseUrl, bool nobodyCanApprove)
    {
        ArgumentNullException.ThrowIfNull(candidate);
        var kind = KindOf(candidate.Tool);
        var detail = $"An agent proposed to {kind} on {candidate.ServerName}. A person has to approve it before it runs.";
        detail += LinkFor(baseUrl, candidate.ProposalId) is { } link ? " " + link : " " + NoLinkLine;
        if (nobodyCanApprove)
        {
            detail += " " + ActionApprovalRules.NobodyCanApproveMessage;
        }

        return (kind, detail);
    }
}
