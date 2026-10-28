# The front door

Audience: operators.

A front door is one Darling service that sits in front of several Darling stores. A client or a browser talks to one MCP address and one web address. The front door sends each call to the store it belongs to. This page uses "store" for a Darling service that has its own monitoring database and collects data. The front door has no database and collects nothing.

Use a front door when your servers are split across several stores, for example one store per region. Then you do not need one address per store.

The front door does not merge data. It keeps each store's answer apart.

## The `frontDoor` block

The block sits at the top level of `darling.json`, next to `postgres` and `mcp`. It is file-only. Nothing in the service reads it back to a client, because it holds the access token of every store. An edit takes effect when you restart the service.

```json
"frontDoor": {
  "enabled": true,
  "refreshSeconds": 300,
  "peerTimeoutSeconds": 150,
  "stores": [
    {
      "name": "use1",
      "mcpUrl": "https://use1.example.com:5152/",
      "mcpEncryptedToken": "...",
      "mcpEncryptedAgentToken": "...",
      "webUrl": "https://use1.example.com:5153/",
      "webEncryptedToken": "...",
      "certificateSha256": "..."
    }
  ]
}
```

### Block fields

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Turns front-door mode on. An absent block, a `null` block or `false` leaves the service a normal single-store service. |
| `refreshSeconds` | `300` | Seconds between reads of each store's server list. From 30 to 3600. |
| `peerTimeoutSeconds` | `150` | Seconds that one forwarded call to a store can take before the front door gives up on that store. From 30 to 600. It bounds MCP calls and web calls. |
| `stores` | none | The stores to route to. At least one is required when `enabled` is `true`. The order in the file is the order the pages and fan-out answers use. |

### Store fields

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | The store's name. It is 1 to 64 characters of `A-Z`, `a-z`, `0-9`, `.`, `_` and `-`. Names must differ when you ignore case. The name goes into a request header and into the page address, so it must not look like a credential. |
| `mcpUrl` | yes | The address of the store's MCP endpoint. An absolute `http` or `https` address with no query string and no user name or password. |
| `mcpEncryptedToken` or `mcpToken` | yes, one of them | The store's operator-role MCP token. `mcpEncryptedToken` is the Windows DPAPI form and is read first. `mcpToken` is the token as text, or an `env:NAME` or `file:PATH` reference. |
| `mcpEncryptedAgentToken` or `mcpAgentToken` | no | The store's agent-role MCP token, in the same two forms. See [the agent role](#the-agent-role). |
| `webUrl` | no | The address of the store's web dashboard, with the same rules as `mcpUrl`. Without it the store has no web pages through the front door. |
| `webEncryptedToken` or `webToken` | when `webUrl` is set | The shared token that the front door signs in to the store's web dashboard with, in the same two forms. A store with a `webUrl` and no web token is refused when the service starts, and the message names the store's place in the list. |
| `certificateSha256` | no | The SHA-256 thumbprint of the store's certificate, 64 hex characters (colons and spaces are allowed). When set, the front door accepts only that exact certificate and does not check the certificate chain. Use it for a store with a self-signed certificate. When absent, the system's trust decides. |

To make an encrypted value, use the same `--encrypt-password` verb that the `mcp` and `web` tokens use. The `env:` and `file:` forms are for Linux and the compose setup, where there is no DPAPI. On a platform other than Windows, an encrypted value cannot be read, so use the plain form there.

### Checks at start

The service checks the block when it starts, together with the rest of `darling.json`. When the block is on and a rule below fails, the service reports each problem and does not start. A problem names the store by its position in the list (`frontDoor.stores[0]`) and never prints an address or a token.

- `refreshSeconds` must be from 30 to 3600.
- `peerTimeoutSeconds` must be from 30 to 600.

The MCP host and the web host do not run these checks when they load the file, so each also holds both numbers to the same limits where it uses them. A `refreshSeconds` of 0 or less reads as 30, and a `peerTimeoutSeconds` of 0 or less reads as 30. A value above the top limit reads as the top limit.
- `stores` must hold at least one store.
- Each store must have a valid, unique `name`.
- Each store must have a valid `mcpUrl` and an MCP token.
- A `webUrl`, when given, must be valid.
- A `certificateSha256`, when given, must be 64 hex characters.

A front door does not need a `postgres` section, so that rule is skipped when `frontDoor.enabled` is on. Nothing is checked when the block is off.

### Warnings

One kind of note is a warning and does not stop the start. If `mcpUrl` or `webUrl` uses plain `http` and the host is not this machine, the front door logs one warning per address. The warning names the store and the setting, and says that the store's token crosses the network in clear text. Use `https` when the store serves it. The front door still starts and still uses the address.

## What each store needs

The front door reaches each store as an ordinary client. Each store needs the following.

1. **MCP on.** The front door reads each store's server list with the MCP tool `list_servers`. Every store must serve MCP. Set `mcp.enabled`, and give the store an operator-role MCP token. The front door's `mcpToken` is that token.
2. **Web on, when you want pages.** Turn the store's web dashboard on and give it a shared token. The front door's `webToken` is that token.
3. **An address the store accepts.** The front door sends the host of `mcpUrl` and `webUrl` as the `Host` header. The store refuses a `Host` it does not know, so use an address the store names. For MCP, that is the store's `listen` address, `localhost`, or the name in `mcp.network.hostName`. For the web dashboard, that is the `listen` address, `localhost`, or the host of `web.publicBaseUrl`.
4. **The front door in `allowFrom`.** If a store listens on the network, add the front door's address to `mcp.network.allowFrom` and `web.network.allowFrom`. Use the narrowest range that covers it. See [Opt-in Network Endpoints](network-endpoints.md).
5. **An agent-role token.** Agents that use the front door need one. See [the agent role](#the-agent-role).
6. **The same build as the front door, for viewers.** A store must run a build that reads the front door's role header (see [Sign-in and roles](#sign-in-and-roles)). The front door refuses a viewer's successful answer from a store on an older build with 502, naming the store. People with the edit role are not affected.

If a store is behind a reverse proxy, put the proxy's address in `mcpUrl` and `webUrl`. Follow the proxy rules in [Opt-in Network Endpoints](network-endpoints.md).

## What stands down on the front door

When `frontDoor.enabled` is on, this service holds no store. It runs no collectors, no alerting, no retention job and no heartbeat. The service logs one line at start that says so and says how many stores it routes to. Its MCP server, its web dashboard and the directory refresh keep running.

The front door still needs its own `mcp` and `web` settings, because they decide who can call the front door. Its `mcp.enabled`, its tokens and its `network` block work as on any store. Its `mcp.allowWrites` and `mcp.allowServerAdmin` decide which write tools the front door lists. A store must also serve a write tool before the call succeeds, so the store's own settings apply on top.

## How the front door knows each store's servers

Every `refreshSeconds` the front door reads `list_servers` from each store in parallel. Each store gets 30 seconds. The front door keeps the server name, display name, engine and read-only flag.

Each store has one of four health words:

| Health | Meaning |
|---|---|
| `ok` | The last read worked. |
| `stale` | The last read failed, and the front door keeps the last good list. |
| `unreachable` | The store has never answered. |
| `auth_failed` | The store answered 401, 403 or 429. |

A store in `auth_failed` is held and gets no calls for at least 300 seconds, or for the store's `Retry-After` when that is longer. The hold is there because a wrong token, sent on every call, locks out the other clients of that store too. After the hold, the next refresh asks again.

When a server name matches nothing, the front door reads the lists again once, at most once every 30 seconds, and looks again. That covers a server added a moment ago.

## How MCP calls route

Every tool on the front door has one more optional argument, `store`. Its value is one of the configured store names. The front door removes `store` before it sends the call, so the store never sees it.

The front door decides where a call goes in this order.

1. **`store` is given.** The call goes to that store. If the name is not a configured store, the front door refuses the call and lists the stores. If `server_name` is also given, the front door looks it up. If the server is on a different store, it refuses the call.
2. **`server_name` is given and is not `*`.** The front door looks the name up in the directory. It uses the rule that a single store uses: an exact match beats a partial match. If one store matches, the call goes there. If several stores match at the same level, the front door refuses the call. The refusal names each store with its candidates. Pass `store` to choose.
3. **Otherwise the tool's class decides.**

### Tool classes

| Class | Which tools | What the front door does |
|---|---|---|
| Store-required | Every write tool, every investigation tool and every server-admin tool. Also `run_custom_view_panel`, `test_custom_alert_rule`, `backtest_custom_alert_rule` and `wait_for_investigation_requests`. Also any call that carries an id of one store's own: `rule_id`, `view_id`, `route_id`, `fallback_route_id`, `tag_id`, `parent_id`, `note_id`, `request_id`, `claim_id`, `command_id`, `journal_id`, `sweep_id`, `called_id`, `query_id`, `route_ids` or `server_ids`. | The call needs `store`, or a `server_name` that is on exactly one store. If it has neither, the front door refuses it. A write never goes to more than one store. |
| Any one store | `get_tool_guide`, `analyze_plan_xml`, `validate_custom_view`, `validate_custom_alert_rule`, `describe_custom_view_catalog` and `list_custom_alert_templates`. These compute from their arguments alone. | The call goes to the first store that is not held, healthiest first. If that store cannot be asked, the front door tries the next one. |
| Fan-out | The 27 tools below, and any other read that takes `server_name` when it is called with no `server_name` or with `*`. | The call goes to every store that is not held, at the same time. |

The 27 fan-out tools that take no server are:

`find_database`, `get_agent_changes`, `get_collection_state`, `get_collector_cost`, `get_command_result`, `get_compiles_after_evict`, `get_custom_alert_rule`, `get_custom_view`, `get_finops_inventory`, `get_fleet_overview`, `get_investigation_evidence`, `get_mute_rules`, `get_notification_routes`, `get_read_latency`, `get_server_actions`, `get_store_host`, `get_store_log`, `get_store_metrics`, `get_store_query_history`, `get_store_query_stats`, `get_sweep_reports`, `list_custom_alert_rules`, `list_custom_views`, `list_query_tag_rules`, `list_query_tags`, `list_servers` and `test_query_tag_rule`.

A test in the build fails if any tool is in no class, so a new tool cannot go missing from this routing.

### A single-store answer

When a call goes to one store, the answer is that store's own answer, unchanged. A `busy` answer from the store comes back as `busy`. The front door does not retry.

### A fan-out answer

A fan-out answer is one JSON text block:

```json
{
  "front_door": true,
  "tool": "get_fleet_overview",
  "stores": [
    { "store": "use1", "is_error": false, "result": { } },
    { "store": "use2", "status": "unavailable", "message": "..." }
  ]
}
```

- Each store's own answer sits under `result`, so a store's `status` words (`empty`, `not_collected`, `busy`) stay what that store said.
- A store that could not be asked has `status` `unavailable` and a `message`. The other stores still answer. The reasons are: the store is down, it did not answer in time, it is held, it has no token for the role, or it answered with an error of its own (an older build that does not have the tool, for example). The `message` then carries the store's error text.
- `is_error` on the call is set only when every store failed.

### A store that is down

A store that is down never fails the whole call. In a fan-out, that store gets an `unavailable` entry and the rest answer. An any-one-store tool goes to the next store when one cannot answer it. In a call to one store, the answer is a single `unavailable` message that names the store, and the call is marked as an error. A call that is not answered within `peerTimeoutSeconds` is reported the same way.

A call that names one store passes that store's JSON-RPC error through unchanged (an unknown tool or bad arguments), because the caller asked that store and nobody else. In a fan-out or a call to any store, the same error only makes that one store `unavailable`.

### Busy stores and 429

A store that is busy answers with a `busy` status inside the tool answer. That answer passes through as it is.

An HTTP 429 from a store is different. The front door treats 401, 403 and 429 the same way. It marks the store `auth_failed`, holds it, and sends it nothing until the hold ends. The call that met the refusal gets an `unavailable` message.

If a caller disconnects, the front door cancels the calls it has in flight to the stores.

### The agent role

A named MCP token can have the `agent` role (see [MCP configuration](configuration-mcp.md)). When the caller holds that role, the front door sends the call with the store's agent-role token (`mcpEncryptedAgentToken` or `mcpAgentToken`). It never falls back to the operator token.

If the store has no agent-role token, the store is not called. The caller gets an `unavailable` message that says no agent-role token is configured. So an agent never reaches a store with the operator token. The operator token skips the limits that the store sets for agents.

## The web dashboard

### Sign-in and roles

A person signs in to the front door, with the front door's own shared token or its OpenID Connect sign-in, as on any dashboard. The front door's Host guard, session cookie, `allowFrom` check and role gate run first and are unchanged. A read-only role cannot make a change through the front door. The front door also refuses a change that is not JSON before it forwards anything.

A person with a read-only role gets, through the front door, the answers a store gives a read-only role. The store's session at the front door can edit, so for a read-only person the front door sends every call with the header `X-Darling-Front-Door-Role: viewer`. The store then answers that call as a read-only caller: it refuses the reads it refuses a viewer (the Admin server form, the investigation export), and its edit flags say `false`. The header can only lower what the session may do, for that one call. The store confirms it with `X-Darling-Front-Door-Role-Applied`, and the front door passes on a read-only person's successful (2xx) answer only when the store confirmed it. A store on an older build does not confirm, so the front door answers 502 naming the store. An answer that is not successful carries the store's own refusal and no data, and a store sends some before its sign-in gate runs (its Host check, a rate limit), so it goes through as the store sent it.

If the front door uses OpenID Connect with no role mapping (`web.network.oidc.roleClaim`), every person who signs in has the edit seat. The front door logs a warning at start. Set `roleClaim` with `adminRoles` and `viewerRoles` to limit who can edit.

The browser never holds a store's session. For each store, the front door signs in with that store's `webToken` and keeps the store's session cookie in memory. It reuses the cookie for a little less than the store's 12-hour session. If a store answers 401, the front door signs in again once and retries once. If the sign-in fails, the front door holds the store for 60 seconds (or for the store's `Retry-After` when that is longer) and answers 502.

### The store picker and addresses

The top bar has a store picker. The page works inside one store at a time. The picker remembers the last store in the browser.

- Every page address carries the store: `#/s/use1/server/web01`. A bookmark or a pasted link opens in the same store.
- A link without a store, such as `#/server/web01`, asks the front door which store holds the server. If two stores hold it, the page says so and lists them.
- The fleet overview shows one section per store with the store's health. The sidebar groups servers by store.
- Pages for alerts, mute rules, tags, known patterns, views, admin and investigations show the picked store only.
- The front door sends the picked store with each call in the `X-Darling-Store` request header.

### Local routes

The front door answers four routes itself. They need a signed-in browser like every other route.

| Route | Answer |
|---|---|
| `GET /api/ping` | `{"status":"ok","front_door":true}`. |
| `GET /api/session` | The front door's own role, with `front_door` `true` and `can_own` `false`. |
| `GET /api/front-door` | Each store's `name`, `health`, `server_count`, `last_ok_utc` and, when the store said so, `admin_writes_off` and `managed_store`. |
| `GET /api/front-door/locate?server=NAME` | 200 with the `store` and the matched `server_name`. 409 with `ambiguous` and the `stores` that match. 404 with `not_found`. |

These routes accept `GET` only.

`admin_writes_off` and `managed_store` are the flags that a store's own `/api/session` reports. A store runs with Admin writes off when `web.allowAdminWrites` is off on a managed store. The front door reads both from each store through the sign-in it already holds, keeps the answer for 30 seconds, and leaves them out for a store that is down, has no `webUrl`, or runs a build that does not report them. The front door's own `/api/session` always says `false` for both, because one flag cannot speak for several stores. The page takes the flags of the store in use, so a store with Admin writes off shows no Admin write controls, the same as when you open that store directly.

### Which store gets a forwarded call

The front door forwards every other `/api/*` call to one store. It picks the store in this order.

1. The `X-Darling-Store` header. A name that is not configured gets 400 with `unknown_store` and the list of stores. A second copy of the header gets 400.
2. A `?server=` query value, through the directory. A name found on more than one store gets 409 and nothing is sent.
3. The only configured store, for a `GET` only.

If none applies, the answer is 400 with `store_required` and the list of stores. A change (any method but `GET`) never falls back to the only store. It needs the header, or a `server` value that matches one server exactly. A change body can be 8 MB at most (413 above that).

The front door never forwards `/auth/*` or the static pages.

### Headers

The front door sends a store only these request headers: `Accept`, `Accept-Language` and `Content-Type`, with the body, plus the store's own session cookie, and `X-Darling-Front-Door-Role` for a read-only person. It drops everything else, including a browser's own `X-Darling-Front-Door-Role`. In particular the browser's `Cookie`, `Authorization`, `Origin`, `Referer`, `X-Forwarded-*` and `X-Darling-Store` never reach a store. The `Host` header is the store's own address from `webUrl`.

From the store's answer the front door returns only the status, `Content-Type`, `Retry-After` and the body. It drops the store's `Set-Cookie` and its security headers, so the front door's own headers are the ones on every answer. The body is streamed. A JSON answer passes as it is. Any other type is sent with `Content-Disposition: attachment` and `X-Content-Type-Options: nosniff`, so a browser saves it as a file and never opens it as a page on the front door's address.

Redirects are never followed. A store that answers with a redirect gets a 502 that tells you to check its `webUrl`. A store that cannot be reached gets 502 with `store_unavailable`. A store that does not answer within `peerTimeoutSeconds` gets 504 with `store_timeout`. Both name the store and never an address.

## Limits

- **The store sees the shared token, not the person.** Every web call reaches a store through the front door's shared-token sign-in. The store cannot tell people apart. It attributes a change that a person makes at the front door to that shared sign-in. The front door's own log does record it: for every change that is not a GET, it writes one Information line with the signed-in name and role, the store, the method, the path without its query string, and the store's status code. It never logs the body, a token, a cookie or the query string. Personal views are off through the front door (`can_own` is `false`). Roles count at the front door. To keep a person read-only, give that person a read-only role at the front door: the store then answers that person as it answers a read-only role (see [Sign-in and roles](#sign-in-and-roles)).
- **Plain `http` to a store that is not on this machine only warns.** The front door starts and uses the address. The store's token crosses the network in clear text, so use `https`.
- **No merging.** A fan-out answer lists the stores one after another. The front door does not add up counts or merge servers across stores.
- **Selection by server is by name only.** The front door does not route by `server_id`, because ids belong to one store.
