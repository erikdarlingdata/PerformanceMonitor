/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// The rules every Level 1 investigation store and tool shares (#5632 Phase A; #5820 moved them here from Darling's
/// <c>InvestigationRequestStore</c> and <c>DarlingMcpInvestigationTools</c> so Lite reads the same values): the grouping
/// window, the claim limit, the agent id rule and the agent journal cap.
/// </summary>
public static class InvestigationRules
{
    /// <summary>The grouping window: an alert this long after a request's first alert still joins it.</summary>
    public static readonly TimeSpan GroupingWindow = TimeSpan.FromMinutes(30);

    /// <summary>The claims a request may take before it expires.</summary>
    public const int MaxClaims = 3;

    /// <summary>The longest agent id a claim or a report may carry (#5632 review round 1, L3). It is written into
    /// <c>claimed_by</c>, the journal's <c>actor</c> and the report, so the store refuses a longer one for every caller.</summary>
    public const int MaxAgentIdLength = 64;

    /// <summary>The agent a refusal is journaled under when the caller's agent id is not a valid one, so an id built to
    /// read as another journal line (the web panel joins a row's fields with a middle dot) is never written.</summary>
    public const string InvalidAgentId = "invalid";

    /// <summary>
    /// True when <paramref name="agentId"/> is 1 to <see cref="MaxAgentIdLength"/> characters from <c>[A-Za-z0-9._:-]</c>
    /// (#5632 review round 2, L4; V184's check constraint), so it cannot carry a separator, a person's prefix with spaces,
    /// or markup into the journal.
    /// </summary>
    public static bool IsValidAgentId(string? agentId) =>
        !string.IsNullOrEmpty(agentId) && agentId.Length <= MaxAgentIdLength
        && agentId.All(c => c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '.' or '_' or ':' or '-');

    /// <summary>The journal rows agents may write on one request (#5632 review round 1, L3). A token holder could otherwise
    /// add a row per guessed claim id, each kept 365 days. The next agent call on that request is refused, and one
    /// <c>journal_capped</c> row records it.</summary>
    public const int MaxAgentJournalRows = 200;

    /// <summary>What a valid agent id is (#5632 review round 2, L4).</summary>
    public const string AgentIdRule =
        "agent_id is required: 1 to 64 characters, each a letter, a digit, '.', '_', ':' or '-'.";
}
