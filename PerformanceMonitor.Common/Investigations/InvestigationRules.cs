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

namespace PerformanceMonitor.Common;

/// <summary>
/// The rules every Level 1 investigation store and tool shares (#5632 Phase A; #5820 moved them here from Darling's
/// <c>InvestigationRequestStore</c> and <c>DarlingMcpInvestigationTools</c> so Lite reads the same values): the grouping
/// window, the claim limit, the agent id rule and the agent journal cap.
/// </summary>
public static class InvestigationRules
{
    /// <summary>The grouping window (#5632): an alert joins a request when it fires within this long of the request's LATEST
    /// alert, up to <see cref="GroupingWindowCap"/> after its first. Incidents arrive in waves, so the window slides.</summary>
    public static readonly TimeSpan GroupingWindow = TimeSpan.FromMinutes(30);

    /// <summary>The longest a request keeps taking alerts: no alert joins one more than this long after its first alert,
    /// however steadily the alerts keep firing (#5632).</summary>
    public static readonly TimeSpan GroupingWindowCap = TimeSpan.FromHours(4);

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

    /// <summary>
    /// The Level 2 action tools (#5915): write tools an agent may call under the investigations arm, each through the one
    /// gate with an <c>agent_action</c>. The ONE definition (#3684): the service's write policy builds its served set from
    /// it and the brief composer names these in <c>allowed_actions</c>, so a tool added here (#5916) is both served and
    /// named with no second edit. Mutes, custom alert rules and per-server threshold overrides (#5916), on the request's server only.
    /// </summary>
    public static readonly IReadOnlyList<string> AgentActionToolNames = new[]
    {
        "create_mute_rule",
        "update_mute_rule",
        "delete_mute_rule",
        "set_mute_rule_enabled",
        "create_custom_alert_rule",
        "update_custom_alert_rule",
        "delete_custom_alert_rule",
        "set_alert_threshold_override",
        "merge_deadlock_signatures",
        "force_query_store_plan",
        "unforce_query_store_plan",
        "evict_cached_plan",
        "kill_session",
        "recompile_module",
        "create_plan_guide",
        "drop_plan_guide",
    };

    /// <summary>What a valid agent id is (#5632 review round 2, L4).</summary>
    public const string AgentIdRule =
        "agent_id is required: 1 to 64 characters, each a letter, a digit, '.', '_', ':' or '-'.";

    /// <summary>The labels every earlier report carries: no person reviewed it, and the agent attributed it to itself.</summary>
    public static readonly IReadOnlyList<string> EarlierReportLabels = new[] { "unreviewed", "self_attributed" };
}
