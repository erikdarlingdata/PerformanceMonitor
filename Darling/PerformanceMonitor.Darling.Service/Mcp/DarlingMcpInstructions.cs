/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Server instructions sent to MCP clients during initialization. #3898 Phase 2 (D5): the per-tool "Tool
/// Reference" tables and their prose are gone — a tool's own description in <c>tools/list</c> is the
/// authoritative surface for what it does and what it takes, and the two were duplicated here only because
/// nothing enforced that the copies stayed in sync (several didn't: #3696, #3821). What stays is what a
/// single tool's own description cannot say by itself: cross-tool contracts (the five/six-word status
/// vocabulary, the <c>as_of</c>/<c>hours_back</c> anchor rule), and specific traps proven to cause a
/// misreading (AG's per-replica columns, the PostgreSQL wait-instrument tiers, the Query Store capture-mode
/// footgun). The one-line census (D5) still names every family so an agent knows the surface exists; it does
/// not re-describe each tool. Issue references, anecdotes and dropped-content notes belong in the CHANGELOG,
/// not here (D4) — see #3898 for the ones this rewrite carried off the wire.
/// </summary>
internal static class DarlingMcpInstructions
{
    /// <summary>
    /// The instructions with NO peer disclosure — a single-store deployment's text, byte-for-byte what this
    /// server sent before #2339. Composed from <see cref="Preamble"/> + <see cref="Body"/>; the split exists
    /// only so <see cref="Build"/> can put the fleet-coverage section between them, high enough that an
    /// agent reads which store it is talking to before it reads the tool census.
    /// </summary>
    public static readonly string Text = Preamble + "\n\n" + Body;

    /// <summary>
    /// Renders the instructions for THIS store, inserting the declared fleet-coverage section (#2339) when
    /// the operator declared any. Returns <see cref="Text"/> unchanged for an empty declaration, so nothing
    /// about a single-store deployment moves.
    /// </summary>
    public static string Build(DarlingPeerDirectory.Snapshot peers)
    {
        var section = DarlingPeerDirectory.InstructionsSection(peers);
        return section.Length == 0 ? Text : Preamble + "\n\n" + section + "\n\n" + Body;
    }

    private const string Preamble = """
        You are connected to a SQL Server performance monitoring tool via PerformanceMonitor Darling, the headless collector service.

        ## CRITICAL: Read-Only Access

        This MCP server provides STRICTLY READ-ONLY access to previously collected performance data. You CANNOT:
        - Execute arbitrary SQL queries against any monitored server
        - Kill sessions, processes, or connections
        - Change any server configuration or settings
        - Modify, insert, or delete any collected data
        - Run any ad-hoc diagnostics beyond what the collectors have already captured

        The writes this server performs (analysis mutes, Custom Views authoring, alert tuning, server onboarding) all land in the MONITORING store — the same store the web viewer and Settings window write. None of them touches a monitored SQL Server or the collected performance data itself.
        """;

    private const string Body = """
        ## How Data Is Collected

        The Darling service collects 24/7, every 1-15 minutes depending on the collector. Data is only as fresh as the last collection. `server_name` resolves against the registry (exact, then partial). `analyze_server` needs 24+ hours of history (`wait_stats` for a SQL Server target, `pg_database_stats` for a PostgreSQL target) or returns `insufficient_data`.

        ## Tools

        This server exposes 158 tools. 87 are the same names Performance Monitor Lite exposes. The remaining 71 are unique to Darling: thirty-five are the PostgreSQL reads (`get_pg_*`), plus Custom Views, custom-alert-rule, alert-tuning and server-onboarding tools, and central-store-only reads (`get_fleet_overview`, `get_ag_health` — Availability Groups — `get_sweep_reports`, `get_store_metrics`, `get_store_log`). `get_blocking` = Lite's `get_blocked_process_reports`.

        ### Reading an empty result

        When a read comes back with no data, the `status` word says WHICH kind of nothing it is, and the four are not interchangeable. `empty` is a true negative: we looked and there was nothing to find. `unavailable` means this server could have that data and does not have it right now, so collection health is worth a look. `not_collected` means this server does not collect that at all — and when the reason is the ENGINE, the gap is PERMANENT: the collector serving that read does not run on this server's engine (an Azure SQL Database has no system_health session, no default trace and no SQL Agent; a PostgreSQL target collects none of the SQL Server signals at all, and the `get_pg_*` reads are the ones that answer there), so there is no session to start, no collector to enable, and nothing to check. The message names the engine and the collector. Do not send anyone to go and fix it. `precondition` is the one that IS worth acting on: this server could have that data, the collector is running, and a setup step on the monitored server is in the way — a Query Store that is off or has gone READ_ONLY, an Extended Events capture session that is not running, an extension that was never created, a grant the monitoring login was refused. The message names the precondition, quotes what the monitored server itself said, and gives the statement or grant that satisfies it. It is re-derived on EVERY read rather than decided when the connection was made, so once somebody does the thing it asked for the next call answers with data — usually with nothing to restart on the monitoring side. A few preconditions are the exception and SAY SO IN THEIR OWN MESSAGE: the fact that gates them is read once when the service connects to that server and cached for the connection's life, so satisfying them also needs the service to reconnect before collection resumes. Read the message rather than assuming the general case — it tells you which kind you have, and telling somebody to retry a connect-scoped one without reconnecting sends them round a loop that never terminates.

        Every tool FAILURE — an exception the tool caught while reading — is the same envelope with `status` = `error`: `{"status":"error","message":"Error during <tool_name>: <what went wrong>","hints":{"operation":"<tool_name>"}}`. It is the fifth `status` word and the only one that is not an answer about the data: the four above say what kind of nothing the store holds, `error` says the read did not complete, so retry it or report it rather than reading it as an all-clear. Branch on `status`, not on the message text. Data results keep their own shape and never carry a top-level `message`, which is how the envelope is told apart from data without a schema. A REFUSAL — a `server_name` that does not resolve, an `hours_back` outside its range, a `limit` past its ceiling, a required parameter that was not sent — is neither a failure nor a miss and has its own word: the same envelope with `status` = `invalid`: `{"status":"invalid","message":"<what was refused, and what is accepted>","hints":{"parameter":"<parameter_name>"}}`. The read did not run because the request as given cannot be served; `hints.parameter` names the knob to change and the message says what it accepts, so fix that and call again — retrying it unchanged answers the same way. It is the word the write tools already use for a body that will not parse, and it means the same thing there. Six `status` words, then: four kinds of nothing, one failure (`error`: retry or report), one refusal (`invalid`: correct the call).

        ### PostgreSQL waits come from one of three instruments

        A PostgreSQL target's wait history comes from exactly ONE instrument: Aurora native > the `pg_wait_sampling` extension > the service-side sampler. Aurora gets `engine_cumulative`; others read `get_pg_wait_sampling`'s `instrument`: `extension_sampled` (10 ms profiler) or `service_sampled` (no extension; once-a-second poll) — a FLOOR, not parity, but better than none.

        ### Asking about a PAST window

        Every tool below that takes `hours_back` also takes `as_of`: an ISO-8601 UTC instant that moves the window's END off "now"; `hours_back` stays the LENGTH. Reach for it whenever the question is about a time rather than about the present, and do NOT substitute a wider `hours_back`: for an aggregate read a wider window is a DIFFERENT answer, not the same answer with more rows.

        - Accepted: full ISO-8601 with an offset, or a bare `2026-08-19` (midnight UTC). An unparseable or future `as_of` is REFUSED, not read as "now"; an older one returns the normal `empty` / `unavailable`.
        - Latest-snapshot reads take no `as_of` — read `captured_at` / `age_seconds` instead. `get_pvs_stats`, `get_fleet_overview`, `get_store_metrics` never take it.
        - The analysis family DOES take it: `get_analysis_facts`/`analyze_server` re-run fact collection, anomaly detection and scoring over the anchored window (`analyze_server` with `as_of` is EXPLORATORY, does not persist).

        Each finding's `confidence` is an EVIDENCE score (0.20 for the symptom alone, more as the root fact's amplifier checks match and the chain deepens — a lone uncorroborated symptom is 0.20, never 1.0) and `confidence_basis` says in words what it rests on (an ANOMALY_* fact's `baseline_confidence` is the baseline's trustworthiness, not a finding's `confidence`).

        | Tool | Note |
        |------|------|
        | `get_query_store_health` | Per-database Query Store health (latest hourly snapshot) — actual vs desired state, readonly_reason decoded, storage vs cap, cleanup thresholds, and the two capture modes (`query_capture_mode` ALL / AUTO / CUSTOM / NONE — ALL is the plan-churn factory; `wait_stats_capture_mode` ON / OFF; null = pre-rung row or pre-2017 engine) |

        `update_alert_settings`/`update_mute_rule` are PARTIAL updates: read `get_alert_settings`/`get_mute_rules` FIRST, send back only changed fields. For Custom Views, call `describe_custom_view_catalog` FIRST, then `validate_custom_view` before `create_custom_view`/`update_custom_view`.

        ## Workflow

        `list_servers` -> `get_collection_health` -> `analyze_server` (follow each finding's `next_tools`) -> `compare_analysis`/`audit_config` -> `mute_analysis_finding` to silence an accepted pattern.
        """;
}
