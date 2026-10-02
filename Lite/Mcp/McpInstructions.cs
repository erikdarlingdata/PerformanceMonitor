namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// Server instructions sent to MCP clients during initialization.
/// #3898 Phase 2 (D5, D6): moves in lockstep with Darling's rewrite. The per-tool "Tool Reference" tables
/// and their prose are gone — a tool's own description in <c>tools/list</c> is the authoritative surface for
/// what it does and what it takes. What stays is what no single tool's own description can say by itself:
/// cross-tool contracts (the status-word vocabulary, the <c>as_of</c>/<c>hours_back</c> anchor rule) and
/// specific traps proven to cause a misreading (memory-pressure indicator interpretation, the Query Store
/// capture-mode footgun).
/// </summary>
internal static class McpInstructions
{
    public const string Text = """
        You are connected to a SQL Server performance monitoring tool via Performance Monitor Lite.

        ## CRITICAL: Read-Only Access

        This MCP server provides STRICTLY READ-ONLY access to previously collected performance data. You CANNOT execute arbitrary SQL, kill sessions/connections, change server configuration, modify or delete any data, or run ad-hoc diagnostics beyond what the collectors have already captured. If asked to run a live query or check what's locking a table right now, answer only from what has already been collected and say so.

        ## How Data Is Collected

        Performance Monitor Lite collects into local DuckDB/Parquet, every 1-15 minutes depending on the collector. Data is only as fresh as the last collection cycle. Delta-based collectors (procedure stats, perfmon counters) need two cycles before producing non-zero values — a newly added server shows empty procedure stats for ~30 minutes, and `execution_count` 0 beside non-zero CPU/elapsed is that same delta artifact, not an error. `server_name` is used automatically when only one server is configured.

        ## Tools

        89 tools: discovery/health, waits, CPU, contention, plan cache, query performance, blocking/deadlocks, memory, I/O, tempdb, storage/index, perfmon, alerts, jobs, configuration, system-health parse-on-read + Default Trace, server info, sessions/active queries, and execution-plan + diagnostic analysis. Tool names and parameters are self-describing in each tool's own description.

        ### Reading an empty result

        When a read comes back with no data, the `status` word says WHICH kind of nothing it is, and the four are not interchangeable. `empty` is a true negative: we looked and there was nothing to find. `unavailable` means this server could have that data and does not have it right now, so collection health is worth a look. `not_collected` means this server does not collect that at all — and when the reason is the ENGINE, the gap is PERMANENT: the collector serving that read does not run on this server's engine (an Azure SQL Database has no system_health session, no default trace and no SQL Agent; a PostgreSQL target collects none of the SQL Server signals at all, and the `get_pg_*` reads are the ones that answer there), so there is no session to start, no collector to enable, and nothing to check. The message names the engine and the collector. Do not send anyone to go and fix it. `precondition` is the one that IS worth acting on: this server could have that data, the collector is running, and a setup step on the monitored server is in the way — a Query Store that is off or has gone READ_ONLY, an Extended Events capture session that is not running, an extension that was never created, a grant the monitoring login was refused. The message names the precondition, quotes what the monitored server itself said, and gives the statement or grant that satisfies it. It is re-derived on EVERY read rather than decided when the connection was made, so once somebody does the thing it asked for the next call answers with data — usually with nothing to restart on the monitoring side. A few preconditions are the exception and SAY SO IN THEIR OWN MESSAGE: the fact that gates them is read once when the service connects to that server and cached for the connection's life, so satisfying them also needs the service to reconnect before collection resumes. Read the message rather than assuming the general case — it tells you which kind you have, and telling somebody to retry a connect-scoped one without reconnecting sends them round a loop that never terminates.

        Every tool FAILURE — an exception the tool caught while reading — is the same envelope with `status` = `error`: `{"status":"error","message":"Error during <tool_name>: <what went wrong>","hints":{"operation":"<tool_name>"}}`. It is the fifth `status` word and the only one that is not an answer about the data: the four above say what kind of nothing the store holds, `error` says the read did not complete, so retry it or report it rather than reading it as an all-clear. Branch on `status`, not on the message text. Data results keep their own shape and never carry a top-level `message`, which is how the envelope is told apart from data without a schema. A REFUSAL — a `server_name` that does not resolve, an `hours_back` outside its range, a `limit` past its ceiling, a required parameter that was not sent — is neither a failure nor a miss and has its own word: the same envelope with `status` = `invalid`: `{"status":"invalid","message":"<what was refused, and what is accepted>","hints":{"parameter":"<parameter_name>"}}`. The read did not run because the request as given cannot be served; `hints.parameter` names the knob to change and the message says what it accepts, so fix that and call again — retrying it unchanged answers the same way. It is the word the write tools already use for a body that will not parse, and it means the same thing there. Six `status` words, then: four kinds of nothing, one failure (`error`: retry or report), one refusal (`invalid`: correct the call).

        ### Asking about a PAST window

        Every tool below that takes `hours_back` also takes `as_of`: an optional ISO-8601 UTC instant that moves the END of the window off "now"; `hours_back` stays the LENGTH. Reach for it whenever the question is about a time rather than about the present, and do NOT substitute a wider `hours_back`: for an aggregate read a wider window is a DIFFERENT answer, not the same answer with more rows.

        - Accepted: full ISO-8601 with an offset, or a bare `2026-08-19` (midnight UTC). An unparseable or future `as_of` is REFUSED, not read as "now"; an older one returns the normal `empty` / `unavailable`.
        - Latest-snapshot reads take no `as_of` — read `captured_at` / `age_seconds` instead. `get_pvs_stats` never takes it.
        - The analysis family DOES take it: `get_analysis_facts` and `analyze_server` re-run fact collection, anomaly detection and scoring over the anchored window (`analyze_server` with `as_of` is EXPLORATORY, does not persist).

        Each finding's `confidence` is an EVIDENCE score (0.20 for the symptom alone, more as the root fact's amplifier checks match and the chain deepens — a lone uncorroborated symptom is 0.20, never 1.0) and `confidence_basis` says in words what it rests on (an ANOMALY_* fact's `baseline_confidence` is the baseline's trustworthiness, not a finding's `confidence`).

        | Tool | Note |
        |------|------|
        | `get_query_store_health` | Per-database Query Store health (latest hourly snapshot) — actual vs desired state, readonly_reason decoded, storage vs cap, cleanup thresholds, and the two capture modes (`query_capture_mode` ALL / AUTO / CUSTOM / NONE — ALL is the plan-churn factory; `wait_stats_capture_mode` ON / OFF; null = pre-rung row or pre-2017 engine) |

        ## Memory pressure signals

        `get_memory_pressure_events`' indicator scale: 0-1 normal; 2 (medium) Resource Monitor trimming caches/grants; 3+ (severe) evicting buffer pool/plan cache, always worth investigating. `memory_indicators_process` is SQL Server itself under pressure (check `get_memory_grants`, `get_memory_clerks`); `memory_indicators_system` is Windows signalling low memory box-wide, which SQL Server may not be causing (check `get_server_properties`).

        ## Workflow

        `list_servers` -> `get_collection_health` -> `analyze_server` (follow each finding's `next_tools`) -> `compare_analysis`/`audit_config` -> `mute_analysis_finding` to silence an accepted pattern.

        ## Important Limitations

        ALL access is read-only. Query text is truncated to 2000 characters. CPU is downsampled to 1-minute averages. Omitting `server_name` with multiple servers configured lists the available ones.
        """;
}
