# Darling MCP setup

Audience: operators.

MCP is the endpoint that an LLM client talks to. It is off by default. This page shows how to turn it on for Darling and how to connect from the same box or another machine. It also lists what to check when a client will not connect.

Install the service first. See [Install Darling](install.md). For the tools, the Lite setup and the shared client notes, see [MCP server](../mcp-server.md). That page covers both Lite and Darling, and this page does not copy it.

## Why it is off

The MCP token gates the entire read surface. It also gates `analyze_server`, which opens live outbound connections to your monitored SQL Servers. It gates the write tools too. Those change the configuration of the monitor itself (custom views, alert tuning, adding and removing monitored servers). That is a bigger blast radius than the web dashboard has. For this reason the two are separate surfaces with separate flags, ports and tokens.

No MCP tool runs SQL that an AI client wrote against your monitored servers. No such tool exists. Every read answers from the monitoring store. These tools reach a monitored server live, always with the fixed statements of the product:

- Under the monitoring login: the plan fetch of `analyze_server`, the connection probe of `add_servers`, the connection test that `edit_server` runs before it saves a changed connection, `test_connection` and `run_collectors_now`.
- Under a separate remediation login: `force_query_store_plan`, `unforce_query_store_plan`, `evict_cached_plan` and `kill_session`. Each one runs only after the switch for that server is on.

## Turn it on and connect from the same box

From an **elevated** PowerShell on the service host, run:

```powershell
cd "C:\Program Files\PerformanceMonitorDarling"
.\PerformanceMonitor.Darling.Service.exe --enable-mcp
```

Or tick **Enable MCP server** in the Viewer's **Settings**. The change is live within one sweep, with no restart.

**Proof**, in the service log:

```
Starting MCP server on http://localhost:5152 (loopback only) — enabled/port from the control plane (config.config_service)
```

And this command shows a listener on port 5152:

```powershell
Get-NetTCPConnection -State Listen | Where-Object LocalPort -eq 5152
```

Then register the server with your client. MCP needs a bearer token on loopback too. Before you turn MCP on, add a `network` block with only a token to `mcp`, and restart the service. Use `"encryptedToken": "<output of --encrypt-password>"`, or `"token"` with the token itself, an `env:NAME` or a `file:PATH`. On Windows, use `encryptedToken`. The references are for the compose and Linux setups. To make a token, use [the PowerShell one-liner in the README](../../Darling/README.md#mcp-endpoint-assistant-over-the-lan).

With no token, an enabled MCP server does not start, and the service log names these settings. To serve local clients with no token instead, set `"allowTokenlessLoopback": true` in the `mcp` block. The write and investigation tools then stay off, and every start logs a line that says so.

Every client sends the token as `Authorization: Bearer <token>`:

```powershell
claude mcp add --transport http --scope user sql-monitor-darling http://localhost:5152/ --header "Authorization: Bearer <token>"
```

In the `.mcp.json` of a project, read the token from an environment variable, because project files get committed:

```json
{ "mcpServers": { "sql-monitor-darling": { "type": "http", "url": "http://localhost:5152/", "headers": { "Authorization": "Bearer ${DARLING_MCP_TOKEN}" } } } }
```

Set `DARLING_MCP_TOKEN` in the environment that starts Claude Code, or keep the token in the user-scope entry above.

The Viewer's Settings has a **Copy Setup Command** button. It puts the command on your clipboard with the port that you configured. Replace `<token>` with the output of `--print-mcp-token`.

**Proof:** `claude mcp list` shows `sql-monitor-darling` as connected. `/mcp` in a new Claude Code session lists the Darling tools, such as `list_servers`, `get_fleet_overview`, `get_wait_stats` and `analyze_server`. The server reports its own tool count in its instructions and in `tools/list`. The write tools add more tools when you turn them on. Ask the client "what servers are being monitored?" and it must answer from your store.

Two details save a round of guessing:

- The endpoint is the root path (`http://localhost:5152/`), not `/mcp` and not `/sse`.
- The transport is stateless Streamable HTTP. Each request is self-contained, so a client that does not echo an `Mcp-Session-Id` still works.

## Connect from another machine

Add a `network` block to `mcp`. This works in managed mode only, and `mcp.enabled` must be on.

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

The rules are the same as for the web block (see [Reach it from a browser on another machine](web-dashboard.md#reach-it-from-a-browser-on-another-machine)):

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

TLS on MCP is opt-in. Without it, the bearer token travels in clear on the segment. Point `mcp.network.tls` at a PKCS#12 bundle or a PEM pair (`Darling/README.md` has the shape), and the LAN listener serves HTTPS only. Or put a TLS-terminating reverse proxy in front of the port. Set `mcp.network.hostName` when clients connect by a DNS name. A missing, unreadable or expired certificate keeps MCP loopback-only and does not fall back to cleartext.

Never expose this endpoint to the internet.

## When it says enabled and still will not connect

Both common failures leave the store flag at `true`, so "it says enabled" is not proof. Check in this order:

1. Is it bound to the LAN address or still to loopback? `Get-NetTCPConnection -State Listen | Where-Object LocalPort -eq 5152` must show the LAN IP of the box, not only `127.0.0.1` and `::1`. "Enabled but still loopback-bound" means that the service loaded `darling.json` before the `network` block existed. The enable toggle stops and starts the endpoint with the config that the service already loaded. It does not read the file again. Run `Restart-Service 'PerformanceMonitor Darling'`, then check again.
2. Does the firewall rule exist, on the right port? Run `Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling MCP (port 5152)'`. See the firewall item in [Things that will bite you](install.md#things-that-will-bite-you).
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

A "could not be decrypted" line on a box that you copied `darling.json` onto is not a bug. See item 4 in [Things that will bite you](install.md#things-that-will-bite-you).
