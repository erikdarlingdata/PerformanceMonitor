# Agent dispatch (Darling)

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

The service log says the same at start-up. None of these changes the exit code of `--validate-config`, and none stops the service from starting. The first three keep the investigation tools from working. The route note does not stop anything.

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

## How an agent gets and works a request

The agent calls these tools in order:

1. `wait_for_investigation_requests` waits up to 25 seconds for a request to open on the routes you name. It answers each new request's id, route, server, family and time, with a cursor. Call it again with the cursor. `list_investigation_requests` lists the open requests instead.
2. `claim_investigation_request` takes one request. The first claim wins. The answer holds a `claim_id` and the brief. To keep a request past its lease, claim it again with the same `claim_id`.
3. The agent calls the read tools that the brief names. The brief has a budget of minutes and tool calls.
4. `get_investigation_evidence` reads the evidence sections that Darling froze for the request. A section that is still collecting says `pending`. The call never waits.
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
- `family_of` (optional): the `request_id` of the first report in this alert family.
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

Darling reads these keys only when it first adds a server to the store. For a server that is already registered, a person sets the login on the store row. Connect as the store owner:

```sql
UPDATE config.config_monitored_servers
SET remediation_username = 'pm_remediate',
    remediation_encrypted_password = 'env:PM_REMEDIATE_PASSWORD',
    modified_at = (now() AT TIME ZONE 'UTC')
WHERE name = 'alpha-sql-01';
```

The service reads the change when it next reloads its server list.

Grants for the remediation login:

| Action | Grant |
|---|---|
| Force or unforce a plan (SQL Server) | `ALTER` on each database where it forces a plan. |
| Evict a plan (SQL Server) | `ALTER SERVER STATE`. Without it, an eviction is refused as `evict_permission_denied`. |
| Kill a session (SQL Server) | `ALTER ANY CONNECTION`. This grant lets the login kill any session on that server. Grant it only where you want an agent to kill. |
| Kill a session (Azure SQL Database) | A user in each database, with `VIEW DATABASE STATE` and `KILL DATABASE CONNECTION`. |
| Kill a session (PostgreSQL) | `pg_signal_backend` and `pg_read_all_stats`. |

The Darling README, section "Ending a PostgreSQL session", lists the sign-in rules for PostgreSQL.

### Turn on a server action

Every server action is off until a person turns it on for that server. A server with no row in `config.config_server_actions` has every action off.

Today only the store owner can write that table. No screen, MCP tool or `darling.json` key turns a switch on. A later change adds a control.

The actions are `force_plan` (it covers force and unforce), `evict_plan` and `kill_session`. Connect as the store owner and run this statement for each action and server:

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

To undo a change, select **Undo** on its row. Undo restores what the setting was before the agent's change. It works only when nobody changed the setting after the agent. If someone did, Undo says **changed since** and leaves the setting alone. Undo works for 365 days after the change.

To stop all agent changes at once, use **Turn off agent changes** in the Viewer or the web. The gate then refuses every change as `actions_off`. Nothing already made is undone. No MCP tool turns the switch back on.

The web can undo and switch only under the store's admin login. On a managed or compose store, the web signs in as the viewer role, so use the Viewer app there.

## Agent notes

A report can carry a `proposed_note`. The note waits until a person approves it. A person approves or rejects it in the Viewer's Investigations window or on the web Admin page, in **Pending Notes**. Once approved, the note reaches later briefs. A pending or rejected note never reaches a brief. A note stops reaching briefs 90 days after it was proposed.

This is the only approval step in agent dispatch. A change an agent makes at Level 2 or 3 needs no approval.

## Practice requests

Before you give an agent Level 2 or 3, watch it work cases you already judged. A person gives feedback on a report: right, partly right or wrong, with the correct verdict. A practice request replays that case and scores the agent's verdict against the feedback.

Start one in the Viewer: Manage Investigations, **Practice** tab. Every Level 2 and Level 3 action on a practice request is refused as `practice`. The score is advisory and turns nothing on or off. The Darling README describes the rules in "Practice requests".

## Lite

Lite is advise-only. Lite has four investigation tools: `list_investigation_requests`, `claim_investigation_request`, `submit_investigation_report` and `get_investigation_reports`. They need `mcp_allow_investigations` on and `mcp_allow_writes` off in `settings.json`. Lite has no `wait_for_investigation_requests`, no `get_investigation_evidence`, no `agent_action`, and no tool that changes a monitored server. Its route dialog offers Off, Inherit and Investigate. See [MCP server](mcp-server.md).
