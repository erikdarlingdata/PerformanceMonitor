# MCP Server (LLM Integration)

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

Every edition includes an embedded [Model Context Protocol](https://modelcontextprotocol.io) server that exposes monitoring data to LLM clients like Claude Code and Cursor.

The tools answer from the monitor's own collected store, and that is the boundary worth stating up front: **no MCP tool runs SQL an AI client wrote against your monitored servers.** The write-capable tools (Darling's view authoring, alert tuning, collector control and server onboarding) change the monitor's own configuration. Five tools reach a monitored server live, each with the product's own fixed queries. They run under the same least-privilege monitoring login the collectors use: the analysis plan fetch, the onboarding connection probe (`add_servers`), the connection test `edit_server` runs before it saves a changed connection, `test_connection` and `run_collectors_now`, which runs that server's collectors once, now. All but the plan fetch are write tools, served only with `mcp.allowWrites` and a token (`add_servers` and `edit_server` also need `mcp.allowServerAdmin`).

Only six Darling tools act on a monitored server: `force_query_store_plan` (with `unforce_query_store_plan`), `evict_cached_plan`, `recompile_module`, `create_plan_guide`, `drop_plan_guide` and `kill_session`. They run fixed statements under a separate remediation login. Each runs only after a person turns on that server's switch for it. All switches start off, and [Agent dispatch](darling-agent-dispatch.md) explains them.

### Setup

Lite and Darling set up MCP in different ways. Follow the part for your edition, then ask a question.

#### Lite

1. Enable the MCP server in Settings (checkbox + port, default `5151`)
   - The port must be between **1024** and **65535**. Ports 0–1023 are well-known privileged ports reserved by the operating system.
   - On save, the app checks whether the chosen port is already in use and warns you if there is a conflict.
   - On startup, the app verifies the port is available before starting the MCP server.
2. Register with Claude Code. Lite's MCP server uses a token once it is turned on, like Darling's. Lite makes the token the first time the server starts. It keeps the token in Windows Credential Manager for your Windows user, under the name `PerformanceMonitorLite.Mcp_McpToken`, and never in a Lite file. Your MCP client keeps its own copy of it in its config (Claude Code's `claude mcp add --scope user` writes it to `.claude.json` in your user profile). Open Settings, Access, and press **Copy MCP config**. That copies the command below with your token in it. **Copy Token** copies the token alone. Both keep the text out of Windows Clipboard History and the cloud clipboard. **New token** makes a new token and the old one stops working at once; run **Copy MCP config** again and register it with your client.

```
claude mcp add --transport http --scope user sql-monitor http://localhost:5151/ --header "Authorization: Bearer <token>"
```

   If you type the command yourself, replace `<token>` with the token. A request with no token, or a wrong one, is answered `401`.

   If MCP was already on when you updated, Lite makes the token at its first start and logs a line saying so. Copy the new config then, because the old registration has no token.

   To serve local clients with no token, as earlier builds did, set `mcp_allow_tokenless_loopback` to `true` in `settings.json` and restart Lite. Lite then serves the read tools only: the write tools and the investigation tools need a token (at least 16 characters, with at least 6 different ones), so they stay off, and Lite logs one Critical line for each one you asked for. Import Settings never brings `mcp_allow_tokenless_loopback`, `mcp_allow_writes` or `mcp_allow_investigations` in from another folder.

#### Darling

Install the service first. See [Install Darling](darling/install.md).

The MCP token gates the entire read surface. It also gates `analyze_server`, which opens live outbound connections to your monitored SQL Servers. It gates the write tools too. Those change the configuration of the monitor itself (custom views, alert tuning, adding and removing monitored servers). That is a bigger blast radius than the web dashboard has. For this reason the two are separate surfaces with separate flags, ports and tokens.

1. Turn MCP on. It is off by default, and its port is `5152`. Use one of these:
   - In the Viewer, open Settings and turn on MCP.
   - On Windows with the bundled (managed) store, run `PerformanceMonitor.Darling.Service.exe --enable-mcp`. The running service applies it within one collection sweep, with no restart. With your own PostgreSQL, or on Linux and in the compose container, run `UPDATE config.config_service SET mcp_enabled = true;` against the store's PostgreSQL instead. The service picks it up within one sweep.
   - Set `mcp.enabled` to `true` in `darling.json`. The service reads this value only to seed the store at its first start. After that, the Viewer or `--enable-mcp` and `--disable-mcp` decide.
2. Set a token. A Darling MCP server that is on but has no token does not start. The service log names the settings to add. Make a token of at least 16 characters with at least 6 different ones. A shorter or more repetitive token still gates every request, but the write and investigation tools stay off. Put the token in `darling.json` and restart the service:

```json
"mcp": {
  "enabled": true,
  "network": { "encryptedToken": "<output of --encrypt-password>" }
}
```

   On Windows, use `encryptedToken`, which holds the output of `--encrypt-password`. `mcp.network.token` takes the token itself, or an `env:NAME` or `file:PATH` reference, and suits Linux and the compose container, where there is no DPAPI. Leave `mcp.network.listen` out to keep the server on loopback. `--configure-network` makes a 32-character token, and the [Darling network endpoints page](darling/network-endpoints.md#mcp-endpoint-assistant-over-the-lan) has a PowerShell command that makes one. If you lose the token, run `--print-mcp-token` elevated on the service host.

   To serve local clients with no token, set `mcp.allowTokenlessLoopback` to `true`. Darling then serves the read tools only, and logs a line at every start saying so.
3. Register with Claude Code. Send the token as a header on every request:

```
claude mcp add --transport http --scope user sql-monitor-darling http://localhost:5152/ --header "Authorization: Bearer <token>"
```

   Replace `<token>` with your token. A project's `.mcp.json` must read the token from an environment variable, because project files get committed. The [Darling MCP configuration page](darling/configuration-mcp.md#mcp) shows how.

**Proof**, in the service log:

```
Starting MCP server on http://localhost:5152 (loopback only) — enabled/port from the control plane (config.config_service)
```

And this command shows a listener on port 5152:

```powershell
Get-NetTCPConnection -State Listen | Where-Object LocalPort -eq 5152
```

The Viewer's Settings has a **Copy Setup Command** button. It puts the command on your clipboard with the port that you configured. Replace `<token>` with the output of `--print-mcp-token`.

**Proof:** `claude mcp list` shows `sql-monitor-darling` as connected. `/mcp` in a new Claude Code session lists the Darling tools, such as `list_servers`, `get_fleet_overview`, `get_wait_stats` and `analyze_server`. The server reports its own tool count in its instructions and in `tools/list`. The write tools add more tools when you turn them on. Ask the client "what servers are being monitored?" and it must answer from your store.

Two details save a round of guessing:

- The endpoint is the root path (`http://localhost:5152/`), not `/mcp` and not `/sse`.
- The transport is stateless Streamable HTTP. Each request is self-contained, so a client that does not echo an `Mcp-Session-Id` still works.

#### Ask a question

Open a new Claude Code session and ask questions like:
- "What servers are being monitored?"
- "What are the top wait types on my server?"
- "Are there any blocking or deadlock issues?"
- "Show me CPU utilization for the last 4 hours"
- "What are the most expensive queries by CPU?"

### Available Tools

**Lite** exposes more than 100 tools. **Darling** exposes more than 200. The deprecated **Dashboard** exposes more than 60 (see [deprecated/Dashboard/README.md](../deprecated/Dashboard/README.md)). Most of Lite's tool names are also in Darling. Darling has many more. They are the PostgreSQL reads, Custom Views, custom alert rules, query tags, collector control, agent actions, investigation reads and fleet-wide reads.

Of Darling's tools, `mcp.allowWrites` with a token serves the write tools. `mcp.allowServerAdmin` adds the server administration tools (`add_servers`, `edit_server`, `remove_server` and `set_server_action`). `mcp.allowInvestigations` with a token adds the investigation tools, and only while `mcp.allowWrites` is off. A service started before an upgrade reports the older, smaller tool list until you restart it.

The table below names the main tools, not all of them. The `tools/list` call on a running server gives the full list. For Darling, [MCP tools and their store tables](darling/mcp-tool-tables.md) lists every tool and the store tables that it reads.

By default Lite serves only read tools. Its twelve write tools are served when `mcp_allow_writes` is `true` in `settings.json`. They are `mute_analysis_finding`, `unmute_analysis_finding`, `set_notification_route_enabled`, `delete_notification_route`, and the eight query tag tools (`create_query_tag`, `update_query_tag`, `delete_query_tag`, `assign_query_tag`, `unassign_query_tag`, `create_query_tag_rule`, `update_query_tag_rule` and `delete_query_tag_rule`). Restart Lite after you change it. While the setting is off, a call to a write tool fails as an unknown tool. Mutes that already exist stay in force. The write tools also need the MCP token: with `mcp_allow_tokenless_loopback` on, they stay off whatever this setting says, and Lite logs one Critical line. Any client that has the token can use them when the setting is on.

Two more file settings, both off or at `full` by default and read at start (restart Lite after a change). `mcp_allow_investigations` serves the four investigation tools (`list_investigation_requests`, `claim_investigation_request`, `submit_investigation_report` and `get_investigation_reports`) only when it is `true` AND `mcp_allow_writes` is `false`; with both on, the investigation tools stay off and Lite logs one Critical line. They also need the MCP token, so with `mcp_allow_tokenless_loopback` on they stay off, and Lite logs one Critical line. `mcp_data_detail` sets how much collected data the tool answers hand out: `full` (the default), `maskLiterals` (statement text keeps its shape with every literal replaced) or `metadataOnly` (statement text, error text, plans and input buffers are withheld, and logins and host names are masked, each as `login#` or `host#` plus 8 hex characters of a keyed hash that is the same in every answer from one store; Lite keeps its key in Windows Credential Manager). Any other word keeps the MCP endpoint off and the log names the accepted words.

Darling has its own switches, under `mcp` in `darling.json`. All of them default to off, except `mcp.dataDetail`, which defaults to `full`. A change needs a restart of the service:

- `mcp.allowWrites` serves the tools that change the monitoring store's configuration, such as alert settings and notification routes, mute rules, custom alert rules and Custom Views.
- `mcp.allowServerAdmin` also serves `add_servers`, `edit_server`, `remove_server` and `set_server_action`. It has no effect while `mcp.allowWrites` is off.
- `mcp.allowInvestigations` serves `claim_investigation_request`, `submit_investigation_report` and the agent action tools. It works only while `mcp.allowWrites` is off.
- `mcp.dataDetail` sets how much collected data the answers hand out: `full` (the default), `maskLiterals` or `metadataOnly`. These mean the same as Lite's `mcp_data_detail`.

The write tools and the investigation tools need the token, and every request must present it, loopback included. Without a token that is at least 16 characters with 6 different ones, they stay off, and the service log says why. While a switch is off, a call to its tools fails as an unknown tool. The [Darling MCP configuration page](darling/configuration-mcp.md#mcp) lists each key.

#### Investigation tools (Lite)

The four investigation tools let your own agent pick up an alert, look into it with the read tools and report back. Lite runs no agent; you bring one (a starter set of instructions for it is in [Lite/AGENTS.example.md](../Lite/AGENTS.example.md)).

- **Turn them on.** Set `mcp_allow_investigations` to `true` and leave `mcp_allow_writes` at `false` in `settings.json`, then restart Lite. They are served only in that combination. With both settings on, the investigation tools stay off and Lite logs one Critical line; turn writes off to get them. With `mcp_allow_investigations` off, a call to one fails as an unknown tool.
- **Pick the alerts.** In a notification route's dialog, set **Agent investigation** to `Investigate` on the routes whose alerts your agent should look at. `Off` stops it for that route; `Inherit` takes the setting from a less specific route. The route also sets a limit per hour and per day, and how long a claim lasts.
- **The tools.** `list_investigation_requests` lists the open requests. `claim_investigation_request` takes one and returns its brief (what fired, what changed, earlier reports and the next calls to make), a `claim_id` and a lease. `submit_investigation_report` returns the agent's verdict (`real`, `false_positive` or `needs_person`), root cause, the calls it made as evidence and a summary. `get_investigation_reports` reads the reports and the journal back.
- **The flow.** Alerts of one server and one alert family join one request while they keep firing: each within 30 minutes of the request's latest alert, up to 4 hours after its first. The agent lists, claims, makes the calls the brief names and submits one report under its `claim_id`. To keep a request past its lease it claims again with the same `claim_id`. A request takes at most three claims and then expires.
- **Anyone with the token can claim.** The investigation tools need the MCP token, so any client that has it can list, claim and report on requests. A claim only stops two agents working one request; it does not say who the agent is. Reports are what an agent said, not findings.
- **What Lite has and does not have.** Lite has these four investigation tools: `list_investigation_requests`, `claim_investigation_request`, `submit_investigation_report` and `get_investigation_reports`. Lite does not have `wait_for_investigation_requests` or `get_investigation_evidence`, which Darling has. A Lite agent lists requests on a timer and reads evidence with the read tools that its brief names. Lite has no tool that changes alert settings or a monitored server for an agent, and no `agent_action` argument. Its route dialog offers only Off, Inherit and Investigate. The Darling page [Agent dispatch](darling-agent-dispatch.md) describes the action levels that Darling adds.
- **What it cannot do.** The agent investigates; it changes nothing on a monitored server and proposes no action (`action_proposals` is refused). An optional `proposed_note` stays pending until a person approves it in Lite (Settings, Review Agent Notes) or in the Darling Admin page's Pending Notes tab; a pending or rejected note never reaches a later brief, and a note that expires (90 days after it was proposed) stops reaching briefs and is no longer listed.
- **Data detail.** `mcp_data_detail` masks briefs and reports the same way it masks every other answer. At `metadataOnly` a login or host is the same `login#` or `host#` pseudonym in every answer, brief and webhook from one store (#6066); a brief claimed before that release keeps its per-brief numbers.

| Category | Tools |
|---|---|
| Discovery | `list_servers`, `find_database` |
| Tool guides | `get_tool_guide` (the long-form reading guide of a tool whose description ends "Reading guide: get_tool_guide.", and cross-tool topics; both editions) |
| Health | `get_server_summary`, `get_collection_health`, `get_daily_summary` |
| Alerts | `get_alert_history`, `get_alert_settings`, `get_mute_rules`, and in Darling `get_known_patterns` (the entries you wrote about alerts that recur) and, with writes on, `create_known_pattern`, `update_known_pattern`, `delete_known_pattern` |
| Waits | `get_wait_stats`, `get_wait_types`, `get_wait_trend`, `get_waiting_tasks` |
| Queries | `get_top_queries_by_cpu`, `get_top_procedures_by_cpu`, `get_cpu_by_database`, `get_load_by_caller`, `get_query_pileups`, `get_query_store_top`, `get_query_store_aborts`, `get_query_store_query_history`, `get_forced_plans`, `get_query_store_hints`, `get_query_duration_trend`, `get_query_trend`, `get_resource_use_by_tag` |
| Active Queries | `get_active_queries` |
| CPU | `get_cpu_utilization` |
| Memory | `get_memory_stats`, `get_memory_trend`, `get_memory_clerks`, `get_memory_grants`, `get_resource_semaphore` |
| Blocking | `get_deadlocks`, `get_deadlock_detail`, `get_blocked_process_reports`, `get_blocked_process_xml`, `get_blocking_trend`, `get_deadlock_trend` |
| I/O | `get_file_io_stats`, `get_file_io_trend` |
| TempDB | `get_tempdb_trend` |
| Perfmon | `get_perfmon_stats`, `get_perfmon_trend` |
| Jobs | `get_running_jobs` |
| Configuration | `get_server_config`, `get_database_config`, `get_database_scoped_config`, `get_query_store_health`, `get_trace_flags` |
| Server Info | `get_server_properties`, `get_database_sizes` |
| Object/Index Stats | `get_table_index_sizes`, `get_index_usage`, `get_object_locking` |
| Sessions | `get_session_stats` |
| System Events | `get_memory_pressure_events` |
| Latches & Spinlocks | `get_latch_stats`, `get_spinlock_stats` |
| Plan Cache & Scheduler | `get_plan_cache_bloat`, `get_cpu_scheduler_pressure` |
| Long Queries | `get_long_query_completions` |
| Default Trace | `get_default_trace_events` |
| Config Changes | `get_server_config_changes`, `get_database_config_changes`, `get_trace_flag_changes` |
| Health Parser | `get_health_parser_system_health`, `get_health_parser_severe_errors`, `get_health_parser_io_issues`, `get_health_parser_scheduler_issues`, `get_health_parser_memory_conditions`, `get_health_parser_cpu_tasks`, `get_health_parser_memory_broker`, `get_health_parser_memory_node_oom` |
| Plan Analysis | `analyze_query_plan`, `analyze_procedure_plan`, `analyze_query_store_plan`, `analyze_plan_xml`, `get_plan_xml`, `get_query_store_plan_xml`, `get_procedure_plan_xml`, `get_active_query_plan_xml`, `get_blocking_plan_xml`, `get_deadlock_plan_xml` |
| Diagnostic Analysis | `analyze_server`, `get_analysis_facts`, `compare_analysis`, `audit_config`, `get_analysis_findings`, `mute_analysis_finding` |
| FinOps (Darling) | `get_finops` (picks a view for one server), `get_finops_inventory` (the Server Inventory for the whole fleet), `get_finops_recommendations` |
| Jobs history (Darling) | `get_job_history` (retained SQL Agent job runs for a server or the fleet) |
| Agent alerts (Darling) | `get_agent_alerts` (the SQL Server Agent alerts on an instance, with an `instance_status` that names what they lack: no alert for severities 19 to 25 or errors 823 to 825, an alert with no operator, a disabled alert, and an alert that fired in the window and told no operator) |
| Trends and store history (Darling) | `get_server_trend` (one trend per `metric`, such as total waits, latch, spinlock, or tempdb size), `get_store_query_history` (the Darling store's own statements, hour by hour), `get_query_repro_script` |
| Server management (Darling) | `add_servers`, `edit_server`, `remove_server`, and the server tag tools such as `create_server_tag` and `assign_server_tag` (`update_server_tag` also reorders a tag among its siblings with `{"move":"up"}` or `{"move":"down"}`), and the query tag tools such as `create_query_tag` and `assign_query_tag`, with the reads `list_query_tags`, `list_query_tag_rules` and `test_query_tag_rule` |
| Investigations (Darling) | `wait_for_investigation_requests` (waits up to 25 seconds for a request), `get_investigation_evidence` (the evidence frozen at the open), and the four tools Lite also has. Served with `mcp.allowInvestigations` and a token, while `mcp.allowWrites` is off |
| Agent actions (Darling) | `force_query_store_plan`, `unforce_query_store_plan`, `evict_cached_plan`, `recompile_module`, `create_plan_guide`, `drop_plan_guide`, `kill_session`, `merge_deadlock_signatures` (counts two deadlock signatures as one) and `undo_plan_change` (puts a plan forced or unforced by an agent back), and the reads `get_plan_force_precheck`, `get_compiles_after_evict`, `get_agent_changes` and `get_server_actions`. A person can undo a plan force, an unforce or a plan guide create (the undo drops the guide). An eviction or a session kill cannot be undone. `set_server_action` (with `mcp.allowServerAdmin`) turns a server's switch on or off. `set_agent_actions_enabled` turns the service-wide agent switch off only: `enabled: true` is refused, because an MCP client must not give agents back the actions a person took away, and a person turns it on in the Viewer (Agent changes) or the web dashboard. `undo_agent_change` undoes one agent change that is not a plan force or unforce (a mute rule, a custom alert rule, a threshold override or a deadlock signature merge) by its `called_id` from `get_agent_changes` |
| Collector control (Darling) | `get_collector_schedules`, `get_collector_cost`, `enable_collector`, `disable_collector`, `set_collector_frequency`, `set_collector_retention`, `set_collector_run_at`, `set_collector_databases`, `reset_collector_schedules`, `run_collectors_now`, `update_service_flags` |
| Collection state (Darling) | `get_collection_state`, `get_server_settings`, `pause_collection`, `resume_collection`, `disable_server`, `enable_server`, `set_server_excluded_databases`, `test_connection`, `analyze_now`, and `purge_now` (needs `confirm`) |
| Quiet windows (Darling) | `set_quiet_window` and `clear_quiet_window` set and end a time-boxed mute for one server or the whole fleet |
| Database state expectations (Darling) | `get_database_state_expectations` lists each database with its current and expected state; `set_database_state_expected` sets, ignores or clears one (the Viewer's database state override editor) |
| Investigation admin (Darling) | `write_investigation_note` (a note a person stands behind, approved as written), `approve_investigation_note` and `reject_investigation_note` (decide a pending agent note), `close_investigation_request` (close an open, claimed or reported request with a reason), `start_practice_request` (replay a judged case), `save_environment_profile` (the profile text of the fleet, one server or one tag; a blank text removes it) and `record_investigation_feedback` (right, partly or wrong on a report). Served only with `mcp.allowWrites` and a token, never to an investigation agent. The answer shows the row before and after. The first four have no undo, as the web has none. Close a practice request with `close_investigation_request`. Restore a replaced feedback record by recording its 'before' again. Restore a profile edit by saving the answer's 'before' profile text again |
| Mute rule purge (Darling) | `purge_expired_mute_rules` deletes every expired mute rule (the Viewer's Purge Expired) and returns each rule it deleted |
| Custom alert rules (Darling) | `create_custom_alert_rule` and `update_custom_alert_rule` take no server argument. A rule applies to every monitored server unless its definition carries a `scope`: `{"scope":{"mode":"servers","servers":["<server name>"]}}` limits it to the named servers, and `{"scope":{"mode":"tag","tagId":<fleet tag id>}}` to the servers assigned that fleet tag (#6455) |
| Alert tuning (Darling) | `backtest_custom_alert_rule` (where a rule fired over stored history), `set_alert_threshold_override`, `dismiss_alert_history` and `restore_alert_history`, and route tools such as `create_notification_route`, `update_notification_route`, `set_notification_route_fallback`, `update_notification_channels` (the default SMTP, Teams, Slack, generic webhook and PagerDuty channels the Viewer sets in Settings > Notifications; secrets are sealed and never shown), and the webhook signing key tools `generate_webhook_signing_key` and `remove_webhook_signing_key` |

Most tools accept optional `server_name` and `hours_back` parameters. If only one server is configured, `server_name` is auto-resolved. Every tool that takes `hours_back` also takes an optional `as_of` — an ISO-8601 UTC instant that moves the END of the window off "now", so a past incident is one call (`as_of` its end, `hours_back` its length) rather than a very wide window filtered by hand. The MCP server binds to `localhost` only and does not accept remote connections. (Darling adds windowed-trend and fleet-overview tools plus agent-driven write tools — Custom Views authoring, alert-settings and mute-rule tuning, and bulk add/remove servers — and supports an opt-in LAN endpoint — see [Darling/README.md](../Darling/README.md).)

### MCP network access (Darling)

Darling's MCP server listens on loopback, port 5152, by default. A `network` block under `mcp` in `darling.json` exposes it to other machines. That needs a `listen` address that is not loopback, managed mode or the compose container, a bearer token, and `allowFrom`, a list of allowed address ranges in CIDR form. If any of these is missing or invalid, the listener stays loopback-only and the service logs a Critical line.

Without a `tls` block, the network listener is plain HTTP. A `tls` block under `mcp.network` (a PKCS#12 file, or a PEM certificate and key) makes it serve HTTPS, with TLS 1.2 or 1.3 only. If the certificate cannot be loaded, the listener stays loopback-only. The service raises a self-alert when that happens, and an "MCP TLS Certificate Expiring" alert 30 days before the certificate expires.

For the `tls` block shape, `mcp.network.hostName` and a TLS-terminating reverse proxy, see [the MCP endpoint on the network endpoints page](darling/network-endpoints.md#mcp-endpoint-assistant-over-the-lan).

```json
"mcp": {
  "enabled": true,
  "port": 5152,
  "network": {
    "listen": "192.168.1.205",
    "allowFrom": "192.168.1.0/24",
    "encryptedToken": "<output of --encrypt-password>"
  }
}
```

The rules are the same as for the web block (see [Reach it from a browser on another machine](darling/web-dashboard.md#reach-it-from-a-browser-on-another-machine)):

- The file is the authority, and the service reads it once at start.
- The exposure is fail-closed.
- The token is DPAPI at LocalMachine scope, so it must be produced on that box.

`--configure-network` generates and encrypts a token and prints the plaintext once. `--print-mcp-token` prints it again later, elevated and on the same box. A lost token does not cost you a new onboarding. Then, from an **elevated** PowerShell, run:

```powershell
Restart-Service 'PerformanceMonitor Darling'
& "C:\Program Files\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe" --configure-firewall
```

**Proof**, in the service log:

```
Starting MCP server on http://192.168.1.205:5152 (LAN-exposed to 192.168.1.0/24 behind a bearer token + in-app CIDR; loopback also bound) — enabled/port from the control plane (config.config_service); listen/allowFrom/token from darling.json mcp.network (file-only, restart-only)
```

Then set up the client. Point it at the LAN IP of the box and never at `localhost`. An off-box client that points at localhost is the most common cause of "MCP won't connect". It fails without an obvious error.

```powershell
claude mcp add --transport http --scope user sql-monitor-darling http://192.168.1.205:5152/ `
  --header "Authorization: Bearer <token>"
```

Two gates apply, in this order:

1. The bearer token. The host checks it in constant time, with no loopback exemption.
2. The CIDR check in the app, on the remote address. Loopback is always allowed.

A remote client with a bad or missing token gets a 401. A client from outside `allowFrom` is refused before any response.

Never expose this endpoint to the internet.

### When it says enabled and still will not connect

Both common failures leave the store flag at `true`, so "it says enabled" is not proof. Check in this order:

1. Is it bound to the LAN address or still to loopback? `Get-NetTCPConnection -State Listen | Where-Object LocalPort -eq 5152` must show the LAN IP of the box, not only `127.0.0.1` and `::1`. "Enabled but still loopback-bound" means that the service loaded `darling.json` before the `network` block existed. The enable toggle stops and starts the endpoint with the config that the service already loaded. It does not read the file again. Run `Restart-Service 'PerformanceMonitor Darling'`, then check again.
2. Does the firewall rule exist, on the right port? Run `Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling MCP (port 5152)'`. See the firewall item in [Things that will bite you](darling/install.md#things-that-will-bite-you).
3. Does the client point at the IP of the box, with the header? Do a fresh `initialize` and `tools/list`. Do not trust a cached tool list from a previous version.

The service logs rejected requests, so you can answer this from the service log. A 401 (bad or missing bearer), a 403 (outside `allowFrom`) or a 400 (Host-header guard) on either endpoint writes a line. The line names the gate that rejected the request and why:

```
MCP refused a request from 10.1.2.3 (HTTP 403): the source address allowlist (network.allowFrom) gate
rejected it — ... .
```

This answers "is my token wrong, or my CIDR wrong?" directly. You do not have to infer it from a status code on the client. The service writes at most one line per gate per source per 10 minutes. A folded line says how many further refusals it stands for. So a scanner that hits the port cannot flood the log, and you still see that it happened. If the budget for distinct sources fills, the line says it speaks for several sources. That is the signal that something scans the port.

If the block itself is bad, the service is loud, and it fails closed. A missing token or a bad CIDR list falls back to loopback-only. A token that is set but cannot be used stops that listener from starting:

```
MCP network exposure requested (mcp.network.listen is non-loopback) but no bearer token is set — refusing to expose; binding loopback-only. Set mcp.network.encryptedToken (via --encrypt-password) or mcp.network.token.
MCP network token could not be decrypted (...); MCP server not started.
MCP network token (mcp.network.token / mcp.network.encryptedToken) is shorter than 16 characters, and at least 16 are required for an exposed surface (...) — refusing to expose; binding loopback-only.
MCP bearer token (mcp.network.token / mcp.network.encryptedToken) is shorter than 16 characters, and at least 16 are required for an exposed surface (...) — the MCP write tools stay off.
MCP network exposure requested but mcp.network.allowFrom '...' is not a valid CIDR list or an entry's address family does not match mcp.network.listen — refusing to expose; binding loopback-only. Use one CIDR (e.g. 192.168.1.0/24) or several, separated by commas (e.g. 10.8.0.0/16,192.168.1.5/32): every entry in CIDR form (/32 for one address), with each IPv4 address written as four plain decimal numbers (no leading zeros) and no IPv6 zone index, and of the same family as listen (a :: listen takes IPv6 entries only). Host bits are masked (192.168.1.5/24 means 192.168.1.0/24).
mcp.network.* is set but postgres.managed = false — MCP network exposure is managed-mode (or container) only and is ignored ...
```

The MCP token must have at least 16 characters. In network mode, a shorter token keeps MCP loopback-only, with the "shorter than 16 characters" line above that ends in "refusing to expose". With `mcp.allowWrites` on, a shorter token leaves the write tools off, with the "shorter than 16 characters" line that ends in "the MCP write tools stay off".

One address can send three wrong bearer tokens without delay. The fourth starts a wait of 2 seconds, and each further wrong token doubles the wait, up to 5 minutes. During the wait the host answers every request from that address with `429` and a `Retry-After` header. It compares no token during the wait, so a correct token is refused too.

A "could not be decrypted" line on a box that you copied `darling.json` onto is not a bug. See item 4 in [Things that will bite you](darling/install.md#things-that-will-bite-you).
