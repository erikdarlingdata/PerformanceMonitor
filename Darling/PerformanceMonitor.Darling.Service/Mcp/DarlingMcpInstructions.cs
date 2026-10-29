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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Server instructions sent to MCP clients during initialization. #3898 Phase 2 (D5): the per-tool "Tool
/// Reference" tables and their prose are gone — a tool's own description in <c>tools/list</c> is the
/// authoritative surface for what it does and what it takes, and the two were duplicated here only because
/// nothing enforced that the copies stayed in sync (several didn't: #3696, #3821). What stays is what a
/// single tool's own description cannot say by itself: cross-tool contracts (the five/six-word status
/// vocabulary, the <c>as_of</c>/<c>hours_back</c> anchor rule), and one trap that spans a family (the
/// PostgreSQL wait-instrument tiers). Per-tool notes, such as the Query Store capture modes, the partial-update
/// rule and the Custom Views authoring order, live only in those tools' own descriptions (#5779). The one-line
/// census (D5) still names every family so an agent knows the surface exists; it does
/// not re-describe each tool. Issue references, anecdotes and dropped-content notes belong in the CHANGELOG,
/// not here (D4) — see #3898 for the ones this rewrite carried off the wire.
/// </summary>
internal static class DarlingMcpInstructions
{
    /// <summary>
    /// The instructions with NO peer disclosure and the census of the whole catalog, as a server with every
    /// switch on serves them. The host no longer sends this text as-is (#5789): it renders the census from the
    /// tools it actually serves, through the three-argument <c>Build</c>. This stays as the reference rendering
    /// the content and budget tests read. Composed from <see cref="Preamble"/> + <see cref="BodyHead"/> +
    /// <see cref="CatalogCensus"/> + <see cref="BodyTail"/>; the preamble/body split exists only so the
    /// fleet-coverage section can sit between them, high enough that an agent reads which store it is talking
    /// to before it reads the tool census.
    /// </summary>
    public static readonly string Text = Preamble + "\n\n" + BodyHead + "\n\n" + CatalogCensus + "\n\n" + BodyTail;

    /// <summary>
    /// Renders the instructions with the whole-catalog census, inserting the declared fleet-coverage section
    /// (#2339) when the operator declared any. Returns <see cref="Text"/> unchanged for an empty declaration.
    /// </summary>
    public static string Build(DarlingPeerDirectory.Snapshot peers) => Compose(peers, CatalogCensus);

    /// <summary>
    /// #5789: the instructions THIS server sends. The census counts <paramref name="served"/> (the tools left
    /// after the write switches, the same collection <c>tools/list</c> lists), and when that is fewer than
    /// <paramref name="catalog"/> (every registered tool) it says which switch is off. The host calls this once,
    /// right after <see cref="DarlingMcpWritePolicy.Apply"/> narrows the collection, so the two cannot disagree.
    /// </summary>
    internal static string Build(
        DarlingPeerDirectory.Snapshot peers, IReadOnlyCollection<string> catalog, IReadOnlyCollection<string> served)
        => Compose(peers, ToolCensus(catalog, served), served.Contains(MuteTool));

    /// <summary>
    /// #6542: the instructions for a request that presented a NAMED token, rendered over the tools that token's role is served
    /// (<paramref name="served"/>) and its policy. The census and the write-policy paragraph name the role, not the
    /// <c>mcp.allowWrites</c> and <c>mcp.allowInvestigations</c> switches, which do not govern a named token, and the Workflow
    /// line drops the mute step for a role that is not served it. The legacy token keeps the three-argument overload.
    /// </summary>
    internal static string BuildForNamedToken(
        DarlingPeerDirectory.Snapshot peers, IReadOnlyCollection<string> catalog, IReadOnlyCollection<string> served,
        DarlingMcpWritePolicy.Effective policy)
        => Compose(peers, ToolCensus(catalog, served, policy), served.Contains(MuteTool), policy);

    private static string Compose(
        DarlingPeerDirectory.Snapshot peers, string census, bool muteServed = true, DarlingMcpWritePolicy.Effective? named = null)
    {
        var section = DarlingPeerDirectory.InstructionsSection(peers);
        // The fast path returns the fixed text when the served census equals the catalog census, the mute tool is
        // served, and there are no peers. The census is compared by value: ToolCensus builds a new string on each call.
        if (named is null && section.Length == 0 && census == CatalogCensus && muteServed)
            return Text;

        /* #5789 correction: the Workflow line names the mute write as its last step. With the writes off that tool
           is not served, so the step is dropped rather than pointing a client at a tool tools/list lacks. */
        var tail = muteServed ? BodyTail : BodyTail.Replace(WorkflowMuteStep, "", StringComparison.Ordinal);
        /* A named token's preamble swaps the settings paragraph for its role's, and says nothing of purge_now unless the role is served it. */
        var basePreamble = Preamble;
        if (named is { } role)
        {
            basePreamble = basePreamble.Replace(
                DarlingMcpWritePolicy.InstructionsLine, DarlingMcpWritePolicy.NamedInstructionsLine(role), StringComparison.Ordinal);
            if (!role.Writes)
                basePreamble = basePreamble.Replace(PurgeSentence, "", StringComparison.Ordinal);
        }

        var preamble = section.Length == 0 ? basePreamble : basePreamble + "\n\n" + section;
        return preamble + "\n\n" + BodyHead + "\n\n" + census + "\n\n" + tail;
    }

    private const string MuteTool = "mute_analysis_finding";

    /// <summary>The preamble sentence that names <c>purge_now</c>, a write an agent token is never served (#6542).</summary>
    private const string PurgeSentence = " With writes on, purge_now deletes history older than the retention given.";

    /// <summary>The Workflow line's last step, which names a write tool; <see cref="Compose"/> drops it when that
    /// tool is not served. <c>RenderedInstructions_NameOnlyServedTools</c> fails if this stops matching the line.</summary>
    private const string WorkflowMuteStep = " -> `mute_analysis_finding` to silence an accepted pattern";

    /// <summary>
    /// The census paragraph for a server that serves <paramref name="served"/> out of <paramref name="catalog"/>.
    /// Every figure comes from the two name sets: the total, the Lite overlap (<see cref="LiteToolNames"/>), and
    /// what each write switch adds. The family list names only families that still have a tool served: with the
    /// writes off, the server-tag and onboarding tools (all writes) are gone, while the query-tag read stays.
    /// </summary>
    internal static string ToolCensus(IReadOnlyCollection<string> catalog, IReadOnlyCollection<string> served)
        => ToolCensus(catalog, served, null);

    /// <summary><see cref="ToolCensus(IReadOnlyCollection{string}, IReadOnlyCollection{string})"/> for a named token's role (#6542) when
    /// <paramref name="named"/> is set: the sentence that says what is not served names the role, not the settings that govern
    /// the legacy token. Null leaves the legacy census byte-identical.</summary>
    internal static string ToolCensus(
        IReadOnlyCollection<string> catalog, IReadOnlyCollection<string> served, DarlingMcpWritePolicy.Effective? named)
    {
        var count = served.Count;
        var total = catalog.Count;
        var shared = served.Count(LiteToolNames.Contains);
        /* #5915: the Level 2 action tools are writes an agent may call under the investigations arm; they do not make the
           writes arm "on". */
        var writesServed = served.Any(n => DarlingMcpWritePolicy.IsWrite(n) && !DarlingMcpWritePolicy.IsServerAdmin(n)
            && !DarlingMcpWritePolicy.AgentActionTools.Contains(n));
        var agentActionServed = writesServed ? 0 : served.Count(n => DarlingMcpWritePolicy.IsWrite(n)
            && DarlingMcpWritePolicy.AgentActionTools.Contains(n) && !DarlingMcpWritePolicy.NeverAgentTools.Contains(n));
        var adminServed = served.Any(DarlingMcpWritePolicy.IsServerAdmin);
        var writeCount = catalog.Count(n => DarlingMcpWritePolicy.IsWrite(n) && !DarlingMcpWritePolicy.IsServerAdmin(n));
        var adminCount = catalog.Count(DarlingMcpWritePolicy.IsServerAdmin);

        /* #5632 Phase A: the investigation tools are a third set (ruling R2), never served with the writes, so each set
           names its own count and switch, and the census names the conflict instead of an unexplained remainder. */
        var investigationsServed = served.Any(DarlingMcpWritePolicy.IsInvestigation);
        var investigationCount = catalog.Count(DarlingMcpWritePolicy.IsInvestigation);
        var off = new List<string>();
        if (named is { } role)
        {
            if (role.Investigations)
                off.Add("this token has the agent role: the write tools beyond the actions an agent may take with an `agent_action` are " +
                    "served only to an operator token");
            else
            {
                off.Add("this token has the operator role: the investigation tools are served only to an agent token");
                if (!adminServed)
                    off.Add($"the {adminCount} tools that add, edit and remove monitored servers, or switch agent actions, need " +
                        $"both `{DarlingMcpWritePolicy.WritesSetting}` and `{DarlingMcpWritePolicy.ServerAdminSetting}` on");
            }
        }
        else if (!writesServed)
            off.Add($"the write tools are off, so the {writeCount + adminCount - agentActionServed} that change monitoring configuration are not served " +
                    $"(`{DarlingMcpWritePolicy.WritesSetting}` with a token adds {writeCount - agentActionServed}, `{DarlingMcpWritePolicy.ServerAdminSetting}` " +
                    $"{adminCount} more)" + (agentActionServed > 0
                        ? $"; the {agentActionServed} mute, custom-rule, threshold override, signature merge, plan force, plan evict, session kill, recompile and plan guide tools are served only to an agent with an `agent_action` (Level 2; the last five listed need Level 3 and the server's own switch)"
                        : string.Empty));
        else if (!adminServed)
            off.Add($"`{DarlingMcpWritePolicy.ServerAdminSetting}` is off, so the {adminCount} tools that add, edit and remove " +
                    "monitored servers, or switch agent actions, are not served");
        if (named is null && !investigationsServed && investigationCount > 0)
            off.Add(writesServed
                ? $"the {investigationCount} investigation tools stay off while `{DarlingMcpWritePolicy.WritesSetting}` is on (one token " +
                  "cannot tell an agent from a person)"
                : $"`{DarlingMcpWritePolicy.InvestigationsSetting}` with a token adds the {investigationCount} investigation tools");

        string opening;
        if (count == total)
            opening = $"This server exposes {total} tools.";
        else if (off.Count > 0)
            opening = $"This server exposes {count} of its {total} tools: " + string.Join("; ", off) + ".";
        else
            opening = $"This server exposes {count} of its {total} tools; the other {total - count} are not served in this configuration.";

        var families = adminServed
            ? "Custom Views, custom-alert-rule, alert-tuning, server/query-tag, onboarding and investigation-wait tools"
            : writesServed
                ? "Custom Views, custom-alert-rule, alert-tuning, server/query-tag and investigation-wait tools"
                : "the Custom Views, custom-alert-rule and query-tag reads and the investigation-wait tool";

        return opening + $" {shared} are the same names Lite exposes. The remaining {count - shared} are unique to Darling: " +
               $"thirty-five are the PostgreSQL reads (`get_pg_*`), plus {families}, and central-store-only reads " +
               "(`get_fleet_overview`, `get_ag_health` — Availability Groups — `get_sweep_reports`, `get_store_metrics`, " +
               "`get_store_log`). `get_blocking` = Lite's `get_blocked_process_reports`.";
    }

    /// <summary>
    /// The census of the whole catalog: <see cref="ToolCensus"/> with every tool served, written out as a constant
    /// so <see cref="Text"/> stays a plain string. <c>DarlingMcpInstructionsBudgetTests</c> fails unless the two
    /// are identical, so a new tool moves this or fails there.
    /// </summary>
    private const string CatalogCensus =
        "This server exposes 275 tools. 112 are the same names Lite exposes. The remaining 163 are unique to Darling: " +
        "thirty-five are the PostgreSQL reads (`get_pg_*`), plus Custom Views, custom-alert-rule, alert-tuning, server/query-tag, " +
        "onboarding and investigation-wait tools, and central-store-only reads (`get_fleet_overview`, `get_ag_health` — Availability Groups — " +
        "`get_sweep_reports`, `get_store_metrics`, `get_store_log`). `get_blocking` = Lite's `get_blocked_process_reports`.";

    /// <summary>
    /// Every tool name Performance Monitor Lite registers, so the census counts the overlap of what THIS server
    /// serves (#5789) instead of a fixed figure. <c>DarlingMcpInstructionsBudgetTests</c> reads Lite's
    /// tool-name attributes and fails unless this set is exactly theirs.
    /// </summary>
    internal static readonly IReadOnlySet<string> LiteToolNames = new HashSet<string>(StringComparer.Ordinal)
    {
        "assign_query_tag", "create_query_tag", "create_query_tag_rule", "delete_query_tag", "delete_query_tag_rule", "list_query_tag_rules", "list_query_tags", "test_query_tag_rule", "unassign_query_tag", "update_query_tag", "update_query_tag_rule",
        "analyze_plan_xml", "analyze_procedure_plan", "analyze_query_plan", "analyze_query_store_plan",
        "analyze_server", "audit_config", "claim_investigation_request", "compare_analysis", "delete_notification_route", "find_database", "get_active_queries",
        "get_alert_history", "get_alert_settings", "get_analysis_facts", "get_analysis_findings",
        "get_blocked_process_reports", "get_blocked_process_xml", "get_blocking_stats", "get_blocking_trend",
        "get_client_timeouts", "get_collection_health", "get_collection_log", "get_cpu_scheduler_pressure",
        "get_cpu_utilization", "get_current_waits_trend", "get_daily_summary", "get_daily_summary_range",
        "get_database_config", "get_database_config_changes", "get_database_scoped_config", "get_database_sizes",
        "get_deadlock_detail", "get_deadlock_trend", "get_deadlocks", "get_default_trace_events", "get_file_io_stats",
        "get_file_io_trend", "get_health_parser_cpu_tasks", "get_health_parser_io_issues",
        "get_health_parser_memory_broker", "get_health_parser_memory_conditions", "get_health_parser_memory_node_oom",
        "get_health_parser_scheduler_issues", "get_health_parser_severe_errors",
        "get_health_parser_significant_waits", "get_health_parser_system_health", "get_index_usage",
        "get_investigation_reports", "get_latch_stats", "get_lock_wait_trend", "get_long_query_completions", "get_memory_clerks",
        "get_memory_grants", "get_memory_pressure_events", "get_memory_stats", "get_memory_trend", "get_mute_rules",
        "get_notification_routes", "get_object_locking", "get_perfmon_stats", "get_perfmon_trend",
        "get_plan_cache_bloat", "get_plan_corrections", "get_plan_xml", "get_procedure_duration_trend",
        "get_pvs_stats", "get_query_duration_trend", "get_query_heatmap", "get_query_store_duration_trend",
        "get_query_store_health", "get_query_store_regressions", "get_query_store_top", "get_query_trend",
        "get_resource_semaphore", "get_resource_use_by_tag", "get_running_jobs", "get_server_config", "get_server_config_changes",
        "get_server_properties", "get_server_summary", "get_session_stats", "get_spinlock_stats",
        "get_table_index_sizes", "get_tempdb_trend", "get_tool_guide", "get_top_procedures_by_cpu",
        "get_top_queries_by_cpu", "get_trace_flag_changes", "get_trace_flags", "get_wait_stats", "get_wait_trend",
        "get_wait_types", "get_waiting_tasks", "list_investigation_requests", "list_servers",
        "mute_analysis_finding", "set_notification_route_enabled", "submit_investigation_report", "unmute_analysis_finding",
        /* #6114: Lite's twins of the #6072 reads, the same names as Darling's. */
        "get_cpu_by_database", "get_load_by_caller", "get_query_pileups",
        /* #5991: Lite's query-tag tools, the same names as Darling's. */
        "assign_query_tag", "create_query_tag", "create_query_tag_rule", "delete_query_tag", "delete_query_tag_rule",
        "list_query_tag_rules", "list_query_tags", "test_query_tag_rule", "unassign_query_tag", "update_query_tag", "update_query_tag_rule",
    };

    private const string Preamble = """
        You are connected to a SQL Server performance monitoring tool via PerformanceMonitor Darling, the headless collector service.

        ## What this server can and cannot do

        This server reads previously collected performance data. It cannot run SQL on a monitored server or run ad-hoc diagnostics; only the server action tools act on one. With writes on, purge_now deletes history older than the retention given.

        """ + DarlingMcpWritePolicy.InstructionsLine;

    private const string BodyHead = """
        ## How Data Is Collected

        The Darling service collects 24/7, every 1-15 minutes by collector. `server_name` resolves against the registry (exact, then partial). `analyze_server` needs 24+ hours of history (`wait_stats` for a SQL Server target, `pg_database_stats` for a PostgreSQL target) or returns `insufficient_data`.

        ## Tools
        """;

    private const string BodyTail = """
        ### Reading an empty result

        When a read comes back with no data, the `status` word says which kind of nothing it is, and the four are not interchangeable. `empty` is a true negative: we looked and there was nothing to find. `unavailable` means this server could have that data and does not have it right now, so collection health is worth a look. `not_collected` means this server does not collect that at all — and when the reason is the engine, the gap is permanent: the collector serving that read does not run on this server's engine (an Azure SQL Database has no system_health session, no default trace and no SQL Agent; a PostgreSQL target collects none of the SQL Server signals at all, and the `get_pg_*` reads are the ones that answer there), so there is no session to start, no collector to enable, and nothing to check. The message names the engine and the collector. Do not send anyone to go and fix it. `precondition` is the one that is worth acting on: this server could have that data, the collector is running, and a setup step on the monitored server is in the way — a Query Store that is off or has gone READ_ONLY, an Extended Events capture session that is not running, an extension that was never created, a grant the monitoring login was refused. The message names the precondition, quotes what the monitored server itself said, and gives the statement or grant that satisfies it. It is re-derived on every read rather than decided when the connection was made, so once somebody does the thing it asked for the next call answers with data — usually with nothing to restart on the monitoring side. A few preconditions are the exception and say so in their own message: the fact that gates them is read once when the service connects to that server and cached for the connection's life, so satisfying them also needs the service to reconnect before collection resumes. Read the message rather than assuming the general case — it tells you which kind you have, and telling somebody to retry a connect-scoped one without reconnecting sends them round a loop that never terminates.

        Every tool failure — an exception the tool caught while reading — is the same envelope with `status` = `error`: `{"status":"error","message":"Error during <tool_name>: <what went wrong>","hints":{"operation":"<tool_name>"}}`. It is the fifth `status` word: the four above say what kind of nothing the store holds, `error` says the read did not complete, so retry it or report it rather than reading it as an all-clear. Branch on `status`, not on the message text. Data results keep their own shape and never carry a top-level `message`, which is how the envelope is told apart from data without a schema. A refusal — a `server_name` that does not resolve, an `hours_back` outside its range, a `limit` past its ceiling, a required parameter that was not sent — is neither a failure nor a miss and has its own word: the same envelope with `status` = `invalid`: `{"status":"invalid","message":"<what was refused, and what is accepted>","hints":{"parameter":"<parameter_name>"}}`. The read did not run because the request as given cannot be served; `hints.parameter` names the knob to change and the message says what it accepts, so fix that and call again — retrying it unchanged answers the same way. It is the word the write tools already use for a body that will not parse, and it means the same thing there. The service runs only a few long reads at once: one that would wait too long for a slot gets `busy`, `{"status":"busy","message":"<why>","hints":{"retry_after_seconds":<n>,"running":<n>,"queued":<n>}}`. The call was correct but did not run (`running`, `queued`: long reads running and waiting then); call again after `retry_after_seconds`.

        `window_truncated` true: the window starts before the store's data, not a cut page (`effective_start`, `truncation_note`).

        ### PostgreSQL wait instruments

        Waits come from one instrument: Aurora native > the `pg_wait_sampling` extension > the service-side sampler. Aurora gets `engine_cumulative`; others read `get_pg_wait_sampling`'s `instrument`: `extension_sampled` or `service_sampled` — a floor, not parity.

        ### Asking about a past window

        Every tool below that takes `hours_back` also takes `as_of`: an ISO-8601 UTC instant that moves the window's end off "now"; `hours_back` stays the length. Use it for past-time questions, not a wider `hours_back` (a different aggregate).

        - Accepted: full ISO-8601 with an offset, or a bare `2026-08-19` (midnight UTC). An unparseable or future `as_of` is refused, not read as "now"; an older one returns the normal `empty` / `unavailable`.
        - Latest-snapshot reads take no `as_of` — read `captured_at` / `age_seconds`. `get_pvs_stats`, `get_fleet_overview`, `get_store_metrics` never take it.
        - The analysis family does take it: `get_analysis_facts`/`analyze_server` re-run fact collection, anomaly detection and scoring over the anchored window (`analyze_server` with `as_of` is exploratory, does not persist).

        Each finding's `confidence` is an evidence score (0.20 for the symptom alone, more as the root fact's amplifier checks match and the chain deepens — a lone uncorroborated symptom is 0.20, never 1.0) and `confidence_basis` says in words what it rests on (an ANOMALY_* fact's `baseline_confidence` is the baseline's trustworthiness, not a finding's `confidence`).

        ## Workflow

        `list_servers` -> `get_collection_health` -> `analyze_server` (follow each finding's `next_tools`) -> `compare_analysis`/`audit_config` -> `mute_analysis_finding` to silence an accepted pattern. `skipped_cycles`: late cycles (log: `collection_cycle`).
        """;
}
