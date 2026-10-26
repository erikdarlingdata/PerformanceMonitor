# Agent dispatch (Darling)

Audience: operators.

Agent dispatch hands an alert to an agent that you run yourself. Darling ships no agent. It opens a request when an alert fires, gives the agent a brief, and records the agent's report. At the higher levels the agent can also change settings, and Darling records every change so that a person can undo it.

Lite can hand an alert to an agent too, but only to read and report. See "Lite" at the end of this page.

## How it fits together

1. An alert fires on a notification route that has an agent level.
2. Darling opens an investigation request. Alerts of one server and one alert family join one request while they keep firing.
3. Your agent finds the request, claims it, and receives a brief.
4. The agent reads data with the read tools and submits one report. If the level allows it, the agent also changes settings.
5. A person reads the report, says whether it was right, and can undo any change the agent made.

## Turn it on

Agent dispatch needs three settings in `darling.json`, and the MCP server must run.

| Key | Value | What it does |
|---|---|---|
| `mcp.enabled` | `true` | Starts the MCP server. This key seeds the store on the first start. After that, the store decides. To change it later, run `--enable-mcp` or `--disable-mcp`. |
| `mcp.allowInvestigations` | `true` | Serves `claim_investigation_request` and `submit_investigation_report` to your agent. |
| `mcp.network.encryptedToken` or `mcp.network.token` | a token | The bearer token. Every MCP request must present it, loopback included. It must have at least 16 characters, and at least 6 of them must differ. Make an encrypted token with `--encrypt-password`. |
| `mcp.dataDetail` | `full`, `maskLiterals` or `metadataOnly` | Optional. It decides how much collected text the brief and every tool answer carry. The default is `full`. |

The reads `list_investigation_requests` and `get_investigation_reports` need no setting.

### How `mcp.allowWrites` relates to it

`mcp.allowWrites` serves the write tools to a person's MCP client. Today the investigation tools are served only when `mcp.allowWrites` is `false`. One token cannot tell an agent from a person, so Darling does not serve both to one token.

If both settings are `true`, `mcp.allowWrites` wins. The write tools are served. `claim_investigation_request` and `submit_investigation_report` are not served. The start logs one Critical line, and `--validate-config` prints a warning. Neither one stops the service or changes the exit code.

For a setup, this means you choose one use for the MCP service at a time:

- To run an agent, set `mcp.allowWrites` to `false` and restart the service. Darling reads `mcp.allowWrites` and `mcp.allowInvestigations` once, at start. A running service does not reload `darling.json`, and turning MCP off and on with `--disable-mcp` and `--enable-mcp` does not read it again. The investigation settings also serve the agent's action tools: mutes, custom rules, threshold overrides, signature merges and the server actions. Each one needs an `agent_action` argument.
- To use the write tools yourself, set `mcp.allowInvestigations` to `false` and `mcp.allowWrites` to `true`. Under `mcp.allowWrites`, the agent action tools are ordinary write tools and ignore `agent_action`.

### Check the setup

Run `PerformanceMonitor.Darling.Service.exe --validate-config`. With `mcp.allowInvestigations` on, it prints a `WARNING:` line for each of these setups:

- The MCP server is off. Turn MCP on, or turn `mcp.allowInvestigations` off.
- No usable token is set. Set `mcp.network.encryptedToken` or `mcp.network.token`.
- `mcp.allowWrites` is also on. Turn `mcp.allowWrites` off to serve the investigation tools.
- No enabled route has an agent level above Off. This one prints as a `NOTE:`. No request opens until you add a route.

With `mcp.tokens` set, it also prints its findings about named tokens, whether or not `mcp.allowInvestigations` is on. See "Named tokens" below.

The service log says the same at start-up. None of these changes the exit code of `--validate-config`, and none stops the service from starting. The first three keep the investigation tools from working. The route note does not stop anything.

## Named tokens: an operator and agents on one service

One bearer token cannot tell a person from an agent. So with `mcp.allowWrites` and `mcp.allowInvestigations` you serve the write tools or the investigation tools, never both. Named tokens remove that limit. Each token has a name and a role, and the token on a request decides which tools that request sees.

Set them in `darling.json`:

```json
"mcp": {
  "enabled": true,
  "tokens": [
    { "name": "ops-lead",   "encryptedToken": "...", "role": "operator" },
    { "name": "triage-bot", "encryptedToken": "...", "role": "agent" }
  ]
}
```

| Field | Rule |
|---|---|
| `name` | 1 to 64 characters from `A-Za-z0-9._:-`. Each name is used once. |
| `encryptedToken` or `token` | The same rules as the `mcp.network` token. At least 16 characters, and at least 6 of them different. Each value is used once. |
| `role` | `operator` or `agent`. |

What each role sees:

| Role | Sees | Never sees |
|---|---|---|
| `operator` | The reads and the write tools. The four server admin tools (`add_servers`, `edit_server`, `remove_server` and `set_server_action`) only when `mcp.allowWrites` and `mcp.allowServerAdmin` are both on. | `claim_investigation_request` and `submit_investigation_report`. |
| `agent` | The reads, the two investigation tools, and the Level 2 action tools through the `agent_action` gate. | Every tool in the never-agent list: the route tools, alert settings, Custom Views, tags, collector and server controls. |

`tools/list` shows only that set. A call to a tool outside it fails as an unknown tool. An unknown token gets 401, and so does a request with no token, loopback included.

An agent's name is its identity. `agent_id` must equal the name of the agent token. A different `agent_id` is refused as `agent_id_not_token`, and the refusal is journaled under the token's name. The log names a named token by its name. It never logs a token value. The legacy token still shows as a short fingerprint.

The old single token (`mcp.network.token` or `mcp.network.encryptedToken`) works as before. Its role still comes from `mcp.allowWrites` and `mcp.allowInvestigations`. With no `mcp.tokens`, nothing changes. You can set both, but give the legacy token a name and a role in `mcp.tokens` and remove it from `mcp.network` instead.

A named token that cannot be served is left out, and the start logs one Critical line that names the token and never its value. This covers a bad name or role, an unreadable value, a value that is too short, and a name or value shared with another token. If you set `mcp.tokens` and none can be served, and no legacy token is set, the MCP server does not start. `--validate-config` prints the same problems as `WARNING:` lines whenever `mcp.tokens` is set. It also warns when an enabled route has an agent level and no agent token can be served.

Darling reads `mcp.tokens` once, at start.

Route agent fields over MCP (the agent level and its limits on `create_notification_route` and `update_notification_route`, for the operator role only) come in a later change. Until then you set agent routes in the Viewer.

## Create an agent route

You create and edit agent routes in the Viewer's route editor. The web dashboard shows routes but does not edit them. No MCP tool changes an agent route.

Set **Agent investigation** on the route to one of these:

| Choice | Level | What an agent can do |
|---|---|---|
| Inherit | none | The route asks the next broader route. |
| Off | 0 | No request opens for this route's alerts. |
| Investigate | 1 | Read and report. |
| Act | 2 | Also change alert settings on the request's server. |
| Act on servers | 3 | Also run server actions on the request's server. |

A request that alerts from several routes opened takes the lowest level among them. A request joined by an alert of a Level 1 route stays at Level 1.

An alert never opens a request when it is muted, a recovery, a scheduled report or a digest.

### Request limits

Each route has limits. The dialog shows them next to the level.

| Limit | Default | Range |
|---|---|---|
| Requests per hour | 20 | 1 to 60 |
| Requests per day | 100 | 1 to 500 |
| Lease (how long one claim holds a request) | 15 minutes | 5 to 120 minutes |
| Expiry (how long a request stays open) | 240 minutes | 15 to 1440 minutes |

Past the hourly or daily limit, no request opens, and the journal says `limit_reached` once. A request can be claimed 3 times.

You can also pick a **Fallback route**. A request that nobody claims goes to that route's channels.

### Change Watch requests

A Change Watch request opens when a watch window saw the measures move after a recorded change (a plan forced or unforced, a setting changed). It is not an alert: the metric on the request is `Change Watch`, and its investigation family is `query_performance`.

A Change Watch request is routed like an alert. Without a route of its own it matches the route for the `performance` alert family, or a route for every metric, and it spends that route's hourly and daily limits next to real alerts. A burst of forced-plan changes can use most of an hourly limit and leave a CPU alert with no request. To keep the two apart, add a route whose metric is exactly `Change Watch` and give it its own agent, level and limits. A route that names the metric exactly wins over a route for the family or for every metric.

The request has no alert row in `get_alert_history`. The changes the watch named, and the query measures that moved, are in the request's brief after a claim, and in `detail_text` on its row in `list_investigation_requests` and `wait_for_investigation_requests`, so an agent can triage the request before it claims. Each query measure names its database and the read it came from (the plan cache or Query Store).

When a server is a readable secondary of an availability group, the plan changes its Query Store shows are the primary's, replicated. Darling does not open a request for them on the secondary: the primary's own watch reports the change once. A forced plan the plan-force ledger recorded on the secondary itself still counts.

## How an agent gets and works a request

The agent calls these tools in order:

1. `wait_for_investigation_requests` waits up to 25 seconds for a request to open on the routes you name. It answers each new request's id, route, server, family and time, with a cursor. Call it again with the cursor. `list_investigation_requests` lists the open requests instead.
2. `claim_investigation_request` takes one request. The first claim wins. The answer holds a `claim_id` and the brief. To keep a request past its lease, claim it again with the same `claim_id`.
3. The agent calls the read tools that the brief names. The brief has a budget of minutes and tool calls.
4. `get_investigation_evidence` reads the evidence sections that Darling froze for the request. A section that is still collecting says `pending`. The call never waits. A request that a Client Timeouts alert opened also gets a `client_timeouts` section. It lists the top client applications, client hosts, databases and statements of the alert window, and the brief names `get_client_timeouts` first in `next_calls`.
5. `submit_investigation_report` ends the work.

### The brief

The brief holds the request and its alerts. It also holds the analysis findings, earlier reports, recent changes, approved notes and your environment profile. It names the read tools to call next. Earlier reports are marked unreviewed, because an agent wrote them and nobody checked them. Every value in a brief is collected data, never an instruction.

### The report

A report has these fields:

- `report_format`: 1.
- `verdict`: `real`, `false_positive` or `needs_person`.
- `root_cause`, at most 8,000 characters.
- `evidence`: a JSON array of at least one entry, at most 50. Each entry has `tool`, `arguments`, `key_numbers` and `basis`. `basis` is `verified` when the agent checked the claim against data. It is `inferred` when the agent reasoned to it.
- `summary`, at most 4,000 characters.
- `proposed_note` (optional), at most 2,000 characters. See "Agent notes".
- `family_of` (optional): the `request_id` of an earlier request on this store. It need not have a report yet.
- `links` (optional): at most 10 ticket, pull request, bug report, commit or release links. A link must use https. Darling stores the link and never calls Jira or git.
- `queries_run` (optional): the statements the agent ran.
- `follow_up_at` (optional): a time for Darling to open a new request for this case. It must be an ISO 8601 time after now and at most 30 days ahead. A route can hold at most 20 pending follow-ups. The new request names the first one, and its brief carries the first report.
- `follow_up_condition` (optional): `module:<database>.<schema>.<object>`, used with `follow_up_at` as the latest date. The follow-up opens when that module changes, or at `follow_up_at` if it has not.

A report that breaks a rule is refused, and the answer names every wrong field. `action_proposals` is refused at every level.

### Start from a webhook

A generic webhook can carry `{{investigation_json}}`. It holds the request that the alert opened or joined. Your runner still claims and reports through the MCP tools.

## What an agent can change at Level 2

At Level 2, an agent changes alert settings on the request's server only. It must change first and report last. After the report is submitted, writes refuse.

Every change needs an `agent_action` argument with `request_id`, `claim_id`, `agent_id`, a `reason` of 1 to 2,000 characters, and `evidence`.

| Tool | What the agent can do |
|---|---|
| `create_mute_rule`, `update_mute_rule`, `set_mute_rule_enabled`, `delete_mute_rule` | Mute one metric on the request's server. |
| `create_custom_alert_rule`, `update_custom_alert_rule`, `delete_custom_alert_rule` | Create, change, disable or delete a custom rule scoped to the request's server. This includes a rule that an operator wrote. |
| `set_alert_threshold_override` | Set a threshold for the request's server or one database on it. |
| `merge_deadlock_signatures` | Count one deadlock signature as another. |

### The three limits

Only three rules limit an agent:

1. An agent turns an alert off only with a mute, never by disabling it. An agent also cannot add to an exclusion list. It can disable or delete a custom rule.
2. An agent cannot mute the service's own alerts or the alerts of the agent route itself.
3. Every change has a reason, is tied to its request, and gets a journal row.

Darling also stops an agent after 200 journal rows on one request. This protects the store and is not an action limit.

When the gate refuses a call, it answers `{status: refused, reason, message}`, changes nothing, and journals the refusal. The reasons include `actions_off`, `claim_not_live`, `practice`, `not_level_2`, `other_server` and `journal_capped`.

## Level 3 server actions

At Level 3, an agent can also change the monitored server. These tools exist:

- `force_query_store_plan` and `unforce_query_store_plan` (SQL Server).
- `evict_cached_plan` (SQL Server, not Azure SQL Database).
- `kill_session` (SQL Server, Azure SQL Database and PostgreSQL).
- `recompile_module` (SQL Server and Azure SQL Database): runs `sys.sp_recompile` on one procedure, function or trigger, so its next run compiles a new plan. The agent names the database and the two-part `schema.name`; Darling resolves the name in that database first and refuses a name that does not resolve or that is a table, a view or another kind of object. The name goes to the server as a parameter. There is no undo.
- `create_plan_guide` (SQL Server only; an Azure SQL Database or Managed Instance target is refused as `engine_not_supported`, because Microsoft documents `sp_control_plan_guide`, which the drop of an invalid guide and the undo use, for SQL Server alone): runs `sys.sp_create_plan_guide` to attach an `OPTION (...)` clause (`OPTIMIZE FOR`, `RECOMPILE`, `MAXDOP`, `USE PLAN`) to one statement without a change to the application. The agent names the database, the guide name, the statement, the type (`OBJECT`, `SQL` or `TEMPLATE`) and the hints, in any form `sys.sp_create_plan_guide` takes: an `OPTION (...)` clause, an XML showplan (text that starts with `<`), or none (empty: the guide applies no hints and removes the statement's own `OPTION` hints). A `TEMPLATE` guide takes no module or batch and needs `params`; `sys.sp_get_query_template` returns the statement and params for a query, and SQL Server checks which hints a type takes. Darling matches before it creates (an `OBJECT` guide's module must resolve and hold the statement exactly; a `SQL` guide reports how many cached batches compiled in its database equal its text, and zero is allowed; a `TEMPLATE` guide reports no count), refuses a name already in use, validates the new guide with `sys.fn_validate_plan_guide` in the same batch, and drops a guide that fails validation again. Every value goes to the server as a parameter. The change is journaled with the whole definition and a person can undo it from Agent changes: the undo drops the guide by name, and only while it still holds the statement and hints it was created with. 
- `drop_plan_guide` (SQL Server only, refused on an Azure SQL target as `engine_not_supported`): drops one plan guide by name with `sys.sp_control_plan_guide N'DROP'`. One batch in the request's database reads the guide from `sys.plan_guides` (query text, scope, the module's two-part name for an `OBJECT` guide, scope batch, parameters, hints, `is_disabled`), refuses a guide that is not there (`guide_not_found`), checks the grant for its type, and drops it. Every guide form is dropped (`OBJECT`, `SQL` and `TEMPLATE`, with `OPTION`, XML showplan or NULL hints), and the journal keeps NULL hints as NULL. It is a dry run by default and the dry run shows the statement and the definition that would be journaled. The change is journaled with that whole definition, and a person can undo it from Agent changes: the undo re-creates the guide from the journal through the create path (the name must be free, an `OBJECT` guide's module must still hold the statement, the new guide is validated and dropped again if it is invalid), whatever form the guide had and disables it again if it was disabled. If a guide with that name exists by then, the undo is refused as `guide_exists` and changes nothing. The undo is gated by that server's `drop_plan_guide` switch and remediation login.

Each tool runs as a dry run unless the agent passes `dry_run` as `false`. A dry run returns the exact statement and writes nothing. A dry run still signs in with the remediation login.

A server action runs only when all of these hold:

1. The service switch is on. It is `config_service.agent_actions_enabled`, and it is on by default.
2. The request is Level 3.
3. The server's action switch for that action is on. See "Turn on a server action".
4. The server has a remediation login, given as a login name and an `env:` or `file:` reference to its password.

When a condition fails, the gate refuses with `actions_off`, `not_level_3`, `server_action_off`, `no_remediation_credential` or `credential_not_reference`. `mcp.allowWrites` never skips these checks.

### Undo and its limits

A person can undo a plan force or unforce from the Agent changes list. A plan eviction has no undo. A session kill has no undo.

### The remediation login

The remediation login is a second login for one server. The monitoring login stays read-only and is never used for a write.

Set it in the server entry of `darling.json`:

```json
"remediationUsername": "pm_remediate",
"remediationEncryptedPassword": "env:PM_REMEDIATE_PASSWORD"
```

The value must be an `env:NAME` or `file:/path` reference. The gate refuses a stored password and an encrypted blob. Both keys are required. One alone counts as unarmed, and the service says so at connect.

On every service start, Darling sets or replaces the remediation login on each matching registered server from `darling.json`. Both keys set or replace the login. Anything else changes nothing: an entry with neither key, or with only one key, leaves the stored login as it is. Darling never clears a login. It skips a server whose stored connection no longer matches the `darling.json` entry, and it logs that server by name.

Because a missing key never clears a login, taking the keys out of `darling.json` does not remove it. To stop agent actions on a server, turn off its action switches, or turn off agent changes service-wide. The switches cover agent actions only.

To remove a server's remediation login entirely, take both keys out of `darling.json` first. An entry with both keys sets the login again on the next start. Then connect as the store owner and run this statement. It sets both remediation columns to NULL and bumps `modified_at`:

```sql
UPDATE config.config_monitored_servers
SET remediation_username = NULL,
    remediation_encrypted_password = NULL,
    modified_at = (now() AT TIME ZONE 'UTC')
WHERE name = 'alpha-sql-01';
```

The service reads the change when it next reloads its server list.

Grants for the remediation login:

| Action | Grant |
|---|---|
| Force or unforce a plan (SQL Server) | `ALTER` on each database where it forces a plan. `ALTER` on a database also lets the login create users, change roles and schemas, and add database DDL triggers, so grant it only on databases where that is acceptable. |
| Evict a plan (SQL Server) | `ALTER SERVER STATE`. Without it, an eviction is refused as `evict_permission_denied`. |
| Drop a plan guide (SQL Server) | As Microsoft documents `sp_control_plan_guide`: an `OBJECT` guide on a procedure or function needs `ALTER` on that object only, which also lets the login change its code; an `OBJECT` guide on a DML trigger needs `ALTER` on the trigger's table (`ALTER` cannot be granted on a trigger), which also lets the login alter or truncate that table; a `SQL` guide needs `ALTER` on the database, which also lets the login create users, change roles and schemas, and add database DDL triggers, so grant it only on databases where that is acceptable. Without the one that applies, the drop is refused as `guide_permission_denied` (a login without `ALTER` on the database does not see a `SQL` guide in `sys.plan_guides`, so for it a guide it cannot find is refused the same way, with the permission sentence, not as `guide_not_found`). A person's undo re-creates the guide and needs the grant `sp_create_plan_guide` documents, which is the same split (`ALTER` on the procedure or function, or on the trigger's table, for an `OBJECT` guide; `ALTER` on the database for a `SQL` guide). A refusal for a trigger names its table. |
| Create a plan guide (SQL Server) | As Microsoft documents `sp_create_plan_guide`: an `OBJECT` guide on a procedure or function needs `ALTER` on it, which also lets the login change its code; an `OBJECT` guide on a DML trigger needs `ALTER` on the trigger's table (`ALTER` cannot be granted on a trigger), which also lets the login alter or truncate that table; a `SQL` guide needs `ALTER` on the database, which also lets the login create users, change roles and schemas, and add database DDL triggers, so grant it only on databases where that is acceptable. Without the one that applies, the create is refused as `guide_permission_denied`. A refusal for a trigger names its table. A person's undo follows the same split (`sp_control_plan_guide`). |
| Recompile a module (SQL Server, Azure SQL Database) | A procedure or function needs `ALTER` on the module. A DML trigger needs `ALTER` on its table, and that grant also allows changing and emptying that table (`ALTER TABLE`, `TRUNCATE`), so grant it only where that is acceptable. Without the grant, a recompile is refused as `recompile_permission_denied`. |
| Kill a session (SQL Server) | `ALTER ANY CONNECTION`. This grant lets the login kill any session on that server. Grant it only where you want an agent to kill. |
| Kill a session (Azure SQL Database) | A user in each database, with `VIEW DATABASE STATE` and `KILL DATABASE CONNECTION`. |
| Kill a session (PostgreSQL) | `pg_signal_backend` and `pg_read_all_stats`. |

The Darling README, section "Ending a PostgreSQL session", lists the sign-in rules for PostgreSQL.

### Turn on a server action

Every server action is off until a person turns it on for that server. A server with no row in `config.config_server_actions` has every action off.

A person turns a switch on or off from any of three places. Each change is journaled as `server_action_switched` with who made it, the screen it came from, and the state before and after.

- **The Viewer.** Open the Investigations window and use the **Server actions** tab.
- **The web dashboard.** Open the Admin page and use the **Server actions** tab. You must be signed in to the dashboard as an admin. A dashboard with no sign-in configured, or a viewer account, lists the switches and answers 403 to a change.
- **MCP.** The `set_server_action` tool, served only with `mcp.allowServerAdmin`. Name one server by its exact name. A disabled server works too. A part of a name is refused and the near matches are listed.

A switch cannot be turned on for an action the server's engine cannot run, but it can always be turned off. An Azure SQL Database target cannot take `evict_plan`, `create_plan_guide` or `drop_plan_guide`, and an Azure SQL Managed Instance target cannot take `create_plan_guide` or `drop_plan_guide`. The edition is the one Darling collected for the server. Until Darling collects it, the switch can be turned on and the tool still refuses the call. No `darling.json` key sets a switch.

As a fallback, the store owner can write the table directly.

The actions are `force_plan` (it covers force and unforce), `evict_plan`, `kill_session`, `recompile_module`, `create_plan_guide` and `drop_plan_guide`. To write the table directly, connect as the store owner and run this statement for each action and server:

```sql
INSERT INTO config.config_server_actions (server_id, action, enabled, set_by)
SELECT server_id, 'kill_session', true, current_user
FROM config.config_monitored_servers
WHERE name = 'alpha-sql-01'
ON CONFLICT (server_id, action)
DO UPDATE SET enabled = EXCLUDED.enabled, set_by = EXCLUDED.set_by, updated_at = now();
```

To turn the action off again, run the same statement with `false` in place of `true`.

## The journal, Agent changes and undo

Darling journals every request. Each change shows as `action_called` (with the object as it was), then `action_done` or `action_failed`. A refusal is journaled once per request, tool, code and hour.

The Viewer's Investigations window and the web Admin page each have an **Agent changes** tab. Each row is one agent call, with the time, server, kind, tool, agent, reason and result. A call that has neither a "done" nor a "failed" result shows **result unknown**. It has no Undo button.

To undo a change, select **Undo** on its row. Undo restores what the setting was before the agent's change. It works only when nobody changed the setting after the agent. If someone did, Undo says **changed since** and leaves the setting alone. Undo works for 365 days after the change. Over MCP (`mcp.allowWrites` and a token), `undo_agent_change` does the same for a change that is not a plan force or unforce. It takes the change's `called_id` from `get_agent_changes`.

To stop all agent changes at once, use **Turn off agent changes** in the Viewer or the web. The gate then refuses every change as `actions_off`. Nothing already made is undone. Over MCP, `set_agent_actions_enabled` can turn the switch off but never back on. A person turns it on in the Viewer or the web.

The web can undo and switch only under the store's admin login. On a managed or compose store, the web signs in as the viewer role, so use the Viewer app there.

## Agent notes

A report can carry a `proposed_note`. The note waits until a person approves it. A person approves or rejects it in the Viewer's Investigations window or on the web Admin page, in **Pending Notes**. Once approved, the note reaches later briefs. A pending or rejected note never reaches a brief. A note stops reaching briefs 90 days after it was proposed.

This is the only approval step in agent dispatch. A change an agent makes at Level 2 or 3 needs no approval.

## Practice requests

Before you give an agent Level 2 or 3, watch it work cases you already judged. A person gives feedback on a report: right, partly right or wrong, with the correct verdict. A practice request replays that case and scores the agent's verdict against the feedback.

Start one in the Viewer: Manage Investigations, **Practice** tab. Every Level 2 and Level 3 action on a practice request is refused as `practice`. The score is advisory and turns nothing on or off. The Darling README describes the rules in "Practice requests".

## Lite

Lite is advise-only. Lite has four investigation tools: `list_investigation_requests`, `claim_investigation_request`, `submit_investigation_report` and `get_investigation_reports`. They need `mcp_allow_investigations` on and `mcp_allow_writes` off in `settings.json`. Lite has no `wait_for_investigation_requests`, no `get_investigation_evidence`, no `agent_action`, and no tool that changes a monitored server. Its route dialog offers Off, Inherit and Investigate. See [MCP server](mcp-server.md).
