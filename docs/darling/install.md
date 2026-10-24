# Install Darling

This page takes you from a downloaded zip to a service that collects data. It is for anyone who installs Darling on Windows. You need a Windows box, a SQL Server that you are allowed to monitor, and a Darling build. You do not need to have run Darling before.

The service fills one store. Three surfaces read that store, and each has its own page:

| Surface | What it is | Page |
|---|---|---|
| WPF viewer | The desktop app. It shows plans, blocking chains, deadlock graphs and live active queries | [The WPF viewer](viewer.md) |
| Web dashboard | A browser view of the fleet, served by the service itself. It is off by default | [The web dashboard](web-dashboard.md) |
| MCP | The endpoint that an LLM client talks to. It is off by default | [Darling MCP setup](mcp.md) |

Everyone does the steps on this page. The three surface pages are independent of each other, so read only the one that you need.

This page is the procedure. [`Darling/README.md`](../../Darling/README.md) is the reference. It lists every config field, every permission and every operational setting, in more detail than belongs here. When you need to know what a setting means, this page links there. When you need to know what to type, it is here.

Every step ends with a proof point. A proof point is a log line, an HTTP response or something on the screen that tells you the step worked. If a proof point does not appear, stop at that step. The next step will not fix it.

## Two facts that explain most of the surprises

### `darling.json` is only a seed

On first start, the service copies most of `darling.json` into control-plane tables in its own store. From then on, the service reads its live configuration from the store. Edits that you make to the file for those sections are ignored.

This catches people most on the MCP and web blocks. The two halves of one JSON object have different owners:

| Setting | Authority | Changed with |
|---|---|---|
| `mcp.enabled`, `mcp.port` | the store (`config.config_service.mcp_enabled` and `mcp_port`) | `--enable-mcp` or `--disable-mcp`, or the Viewer's Settings. The change is live, with no restart |
| `web.enabled`, `web.port` | the store (`config.config_service.web_enabled` and `web_port`) | `--enable-web` or `--disable-web`, or the Viewer's Settings. The change is live, with no restart |
| `mcp.network` and `web.network` (`listen`, `allowFrom`, `encryptedToken`) | the file, always. No store equivalent exists | editing `darling.json`, then restarting the service |
| `postgres.*` (including `postgres.network`) | the file, always | editing `darling.json`, then restarting the service |
| SQL-auth secrets (`encryptedPassword`) | the file, always. The service never writes them to the store | editing `darling.json` |
| `servers` | seeded once, then the store | the Viewer's Add Server, the `add_servers` MCP tool, or `--add-server` |

For example, if you change `mcp.port` in the Viewer, that changes where MCP binds. A different `mcp.port` in the file does not override it. When the two disagree, the service says so once per start, and the message names both sides:

> `MCP configuration disagrees across the two planes and the CONTROL PLANE WINS: port is 5152 in darling.json
> (mcp.port) but 5199 in config.config_service.mcp_port. After the first run darling.json's
> mcp.enabled/mcp.port are only the SEED — change them with --enable-mcp/--disable-mcp or the Viewer's
> Settings, or the file values will keep being ignored. The mcp.network block is the OPPOSITE: file-only,
> restart-only, no store equivalent — the control plane cannot change where this endpoint binds or what token
> it requires.`

### `darling.json` is JSONC, not JSON

The file contains `//` comments. The shipped sample has more than 200 of them, and they are the documentation of the fields. Edit the file as text, in Notepad or VS Code.

PowerShell's `ConvertFrom-Json` fails on this file. Some parsers accept comments. Even then, a `ConvertFrom-Json | ConvertTo-Json` round trip silently strips every comment and leaves the next person with an undocumented file. There is no supported way to edit the file from a program. One tool rewrites the file: `--configure-network`. It keeps the comments on purpose, and it takes a timestamped `.bak-` copy first.

## Step 1: Prerequisites

- Windows on the service host. The bundled store, the DPAPI credential protection and the Windows-service lifetime are all Windows-only. A Linux path exists (see [Run on Linux](../../Darling/README.md#run-on-linux-docker-compose-or-systemd-1804)), but it is a different procedure. The WPF viewer is Windows either way.
- No .NET runtime to install. The service and the viewer in the zip carry their own .NET runtime, so they are self-contained. A stock Windows Server needs nothing installed first. Runtime updates arrive with each Darling release.
- A monitored SQL Server and a login on it. The server can be SQL Server 2016 SP2 to 2025 (2017 needs CU3 or later), Azure SQL MI, AWS RDS or Azure SQL DB. The login needs `VIEW SERVER STATE` and the rest of the [monitoring grants](../../Darling/README.md#permissions-on-monitored-servers).
- Nothing else. In the shipped default (`postgres.managed = true`), the service runs its own bundled PostgreSQL 18 and TimescaleDB. You do not provision a database.

## Step 2: Download and verify

Take `PerformanceMonitorDarling-<version>.zip` and `SHA256SUMS.txt` from the release (or the nightly) that you were pointed at. Check the hash before you extract anything. Work from an **elevated** PowerShell in the folder that you downloaded to. The next step extracts into Program Files, and only administrators can write there.

```powershell
$zip = 'PerformanceMonitorDarling-<version>.zip'    # the file you downloaded
(Get-FileHash -Path $zip -Algorithm SHA256).Hash.ToLower()
Select-String -Path .\SHA256SUMS.txt -SimpleMatch -Pattern $zip
```

**Proof:** the two hex strings match. `SHA256SUMS.txt` ends its lines with LF on purpose, so `shasum -c` works on macOS and Linux too.

## Step 3: Extract to a machine-scoped path

```powershell
Expand-Archive -Path $zip -DestinationPath "C:\Program Files\PerformanceMonitorDarling"
```

`C:\Program Files\PerformanceMonitorDarling` is the documented location, and we recommend that you use it. Do not use any of these locations:

- A folder made directly under `C:\`. Ordinary users can write to such a folder, so `install-darling.ps1` refuses it ([#4043](https://github.com/erikdarlingdata/PerformanceMonitor/issues/4043)).
- Your Desktop or Downloads.
- Anywhere under `C:\Users\`.
- A UNC path or a mapped drive.

The service runs as the unprivileged virtual account `NT SERVICE\PerformanceMonitor Darling`. That account is not you and not an administrator. A user profile grants access to nobody else. If you install there, the service installs cleanly and then cannot read its own program files. For this reason `install-darling.ps1` refuses a fresh install in those locations. The [full explanation is in the reference](../../Darling/README.md#the-install-location-has-to-be-machine-scoped).

**Proof:** the folder contains `PerformanceMonitor.Darling.Service.exe`, `darling.sample.json`, `install-darling.ps1`, `pg-runtime.zip`, `wwwroot\`, and a `viewer\` subfolder that holds `PerformanceMonitor.Darling.Viewer.exe`. That is the whole product. The viewer is not a separate download.

## Step 4: Write `darling.json`

Stay elevated, so that Notepad can save into Program Files:

```powershell
Copy-Item "C:\Program Files\PerformanceMonitorDarling\darling.sample.json" "C:\Program Files\PerformanceMonitorDarling\darling.json"
notepad "C:\Program Files\PerformanceMonitorDarling\darling.json"
```

The minimum is one entry in `servers`. Everything else in the sample has a working default:

```json
{
  "postgres": {
    "managed": true,
    "port": 5641
  },
  "servers": [
    {
      "name": "SQL2022",
      "host": "SQL2022",
      "auth": "integrated",
      "excludedDatabases": []
    }
  ]
}
```

`"auth": "integrated"` connects as the account that the service runs under. The default virtual account reaches remote servers as the machine account `DOMAIN\<host>$`. For integrated auth against a remote server, you usually want to [run the service as a domain account or gMSA](../../Darling/README.md#run-the-service-as-a-domain-account-or-gmsa) instead.

For SQL auth, set `"auth": "sql"`, a `"username"` and an `"encryptedPassword"`. To make the encrypted password, run:

```powershell
cd "C:\Program Files\PerformanceMonitorDarling"
.\PerformanceMonitor.Darling.Service.exe --encrypt-password
```

The command prints `Password: ` and waits. Type the password and press Enter. It prints one line of base64 that starts with `AQAAA`. Paste that line into `"encryptedPassword"`.

`--encrypt-password` is quiet when a script drives it. The `Password: ` prompt goes to stderr only when stdin is a console. The "Paste the line above" hint goes to stderr only when stdout is a console. If you pipe the password in and capture the output, a successful run writes nothing to stderr and one line of base64 to stdout. It then works under `$ErrorActionPreference = 'Stop'` with no workaround:

```powershell
$ErrorActionPreference = 'Stop'
$blob = Read-Host -Prompt 'password' |
    & "C:\Program Files\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe" --encrypt-password
```

A failure still writes its error line to stderr and exits with code 1. The guidance for a console that gave it no input goes to stdout.

Older builds wrote the prompt and the hint to stderr unconditionally. That stopped a `'Stop'` script and echoed the piped-in password in the error text. On one of those builds, wrap the call with `$ErrorActionPreference = 'Continue'` and add `2>$null`.

The blob is DPAPI at LocalMachine scope with a fixed entropy string. It decrypts only on the machine that produced it. Run `--encrypt-password` on the service host. If you ever move `darling.json` to a different box, run it again there. The same rule applies to the MCP and web tokens (see [the web dashboard](web-dashboard.md) and [MCP](mcp.md)). Do not copy a `darling.json` between machines and expect its secrets to survive.

## Step 5: Pre-flight

```powershell
cd "C:\Program Files\PerformanceMonitorDarling"
.\PerformanceMonitor.Darling.Service.exe --test-connection
```

This command validates the file and then connects to every configured server. It exits with code `0` only when the config is valid and every server is reachable, so you can use it as a gate.

**Proof:**

```
Validating connectivity to <count> server(s)...
  [PASS] SQL2022: SQL major version <major>, <edition> Edition (64-bit), msdb access: yes
All servers reachable.
```

`msdb access: NO (failed-job alerts unavailable)` is also a `[PASS]`. It means that the login cannot read `msdb.dbo.sysjobs`, so the service skips failed-job alerts. That is expected for a minimal monitoring login. If you want those alerts, add the direct msdb `SELECT` grants from the permissions section.

A `Configuration is invalid:` block lists literal problems for each field. Fix them and run the command again.

This command connects as you, the console user, and not as the service account. For `"auth": "integrated"` servers, a `[PASS]` proves reachability and a well-formed config. The grants that matter at runtime belong to the service account. The per-server connect lines in step 7 are the real proof.

## Step 6: Install the service

The easy way is to double-click `Install-Darling.cmd` in the install folder. It asks Windows for administrator rights and runs the script with the execution policy bypassed for that one run only. The policy of your machine does not change, and there is nothing to unblock. Any arguments pass through (`Install-Darling.cmd -Network`).

Or, from a PowerShell in the install folder, run the script. If the shell is not elevated, the script offers to restart itself elevated.

```powershell
cd "C:\Program Files\PerformanceMonitorDarling"
.\install-darling.ps1
```

The script does these things:

- It checks the install location and runs the pre-flight again.
- It registers the Event Log source.
- It creates the service `PerformanceMonitor Darling` under the virtual account.
- It reconciles the firewall rules and starts the service.
- It creates **Darling Viewer** shortcuts on the Desktop and in the Start Menu.

`.\uninstall-darling.ps1` reverses the install. It leaves your store and config alone unless you pass `-PurgeData`.

To set up the LAN endpoints in the same pass, use `.\install-darling.ps1 -Network`. It runs the guided wizard at the end. You can also do this later (see [the web dashboard](web-dashboard.md) and [MCP](mcp.md)).

## Step 7: Wait about two minutes

The first start does real work. The service unpacks `pg-runtime.zip`, runs `initdb`, migrates the store, provisions the least-privilege roles, and completes a first collection cycle. The installer waits for this, for up to 7 minutes. It shows each step as the service logs it. It ends with `Service is ready` and the next steps for this install: open the Viewer, add the first server, and the web address if the dashboard is on. If the service logs a startup failure, stops, or is not ready in time, the installer prints the log lines that explain it. It then exits with an error.

The install has not hung. Do not kill it. The internal `initdb` timeout alone is 180 seconds. To watch the log yourself, run:

```powershell
Get-Content "$env:ProgramData\PerformanceMonitorDarling\logs\darling-service_$(Get-Date -Format yyyyMMdd).log" -Wait -Tail 40
```

**Proof:** these lines, in this order:

```
Loaded configuration from C:\Program Files\PerformanceMonitorDarling\darling.json: <count> server(s)
Extracting the bundled Postgres runtime from ...\pg-runtime.zip (first run)
Initializing managed Postgres cluster in C:\ProgramData\PerformanceMonitorDarling\pg (first run)
Managed Postgres cluster initialized (scram-sha-256, data checksums, UTF8/C locale)
Managed Postgres started
Creating the 'darling' database
Postgres store ready (schema v<version>, <count> migration(s) applied)
Least-privilege roles ready (admin: ...; viewer: ...; mcp: ...)
TimescaleDB detected — hypertables, chunk-based retention, and compression enabled
TimescaleDB: <n>/<n> collector table(s) are hypertables
PerformanceMonitor Darling collection loop started
[SQL2022] Connected (major <major>, edition <edition>, server_id <id>)
  [SQL2022] wait_stats => <count> rows (sql:<ms>ms, pg:<ms>ms)
```

`PerformanceMonitor Darling collection loop started` means ready. The per-collector `=> N rows` lines that repeat on every sweep mean working. On the hypertable line, both numbers must be equal. A converted count below the total means that a table stayed plain, and the line above it names which table.

Two things look wrong on a first start and are not:

- One-time `discarding and regenerating` warnings about the role `.dpapi` credential files. A role password can be asserted again, so the service does that. The ownership check is working.
- A collector that reports `YIELDED`. The one-second lock-timeout guard fired on a busy target. The guard stops Darling from becoming the problem that it monitors.

If the service idles instead, the message is literal and named: `Cannot load configuration`, `Configuration problem: <field>`, or `Cannot reach or migrate the Postgres store`. See [Troubleshooting](../../Darling/README.md#troubleshooting).

When the service collects, go to the page for the surface that you need: [the WPF viewer](viewer.md), [the web dashboard](web-dashboard.md) or [MCP](mcp.md).

## Things that will bite you

Each of these has cost somebody real time.

1. `darling.json` is JSONC. `ConvertFrom-Json` fails on it, and a round trip strips the comments that document it. Edit it as text. (See [JSONC](#darlingjson-is-jsonc-not-json).)
2. The first start takes about two minutes and looks like a hang. The service unpacks PostgreSQL and runs `initdb`. Do not kill it. (See [Step 7](#step-7-wait-about-two-minutes).)
3. Older builds of `--encrypt-password` prompt on stderr. That is a terminating error under `$ErrorActionPreference = 'Stop'`, and PowerShell then echoes the source line, which leaks a piped-in password. Current builds stay quiet when stdin or stdout is redirected. (See [Step 4](#step-4-write-darlingjson).)
4. Every DPAPI blob is LocalMachine-scoped. This covers SQL passwords, the MCP token and the web token. They decrypt only on the machine that encrypted them. If you copy `darling.json` to another box, you get a file that cannot be decrypted, not a portable config.
5. A remote viewer seat is read-only by default. `postgres.network.role` defaults to `"viewer"`, and the local `connectAs` defaults to `"admin"`. A read-only seat cannot `INSERT` into `config_command` in the monitoring store. That is not a write to your SQL Server. The probe fails safe, so a store that answers oddly also reads as read-only. (See [Read-only versus read-write seats](viewer.md#read-only-versus-read-write-seats).)
6. `enabled` and `port` for `mcp` and `web` belong to the store. The `network` block belongs to the file and needs a restart. A port that you change in the Viewer changes where the endpoint binds, and the file cannot override it. A change to the `network` block in the file does nothing until you restart the service. (See [`darling.json` is only a seed](#darlingjson-is-only-a-seed).)
7. The name of the firewall rule includes its port. If you change a port, the rule does not move. The change creates a different rule and leaves the old one as an inbound allow with nothing behind it. Run `--configure-firewall` elevated after any port change. It sweeps `PerformanceMonitor Darling <surface> (port *)`, so the stale rule goes away.

   One narrow window remains on an upgrade of a box whose port you moved in the Viewer. The first `--configure-firewall` of the installer runs before the service starts, while the managed store (a child of the service) is down. The command then falls back to the seed port in `darling.json`. It says so and does not fall back silently. Since #2436, the installer runs the command again after the service is up, which normally closes the window.

   If the store still cannot answer within the ten-second budget of the command, the start-up check of the running service logs the exact command to fix it. Fresh installs are not affected, because there the file is the future answer of the control plane.
8. `--configure-firewall`, `--enable-*` and `--disable-*` need an elevated shell. The service account cannot create firewall rules, by design. If you run unelevated, the store toggle still succeeds and the rule does not change. The command prints the exact elevated command for you to run by hand.
9. An off-box MCP client that points at `localhost` fails quietly. Use the LAN IP of the box. (See [When it says enabled and still will not connect](mcp.md#when-it-says-enabled-and-still-will-not-connect).)
10. The web dashboard asks for its token on loopback too. A loopback-only dashboard needs the token like an exposed one. The browser on the service host signs in with it. Only `web.allowTokenlessLoopback` serves it with no token, and then for reads only. (See [Reach it from a browser on another machine](web-dashboard.md#reach-it-from-a-browser-on-another-machine).)
11. `Warning` means three different things, and you must look at the card to tell which. `ClassifyBand` returns `Warning` for any of these:
    - A metric at warning severity (CPU, blocking, deadlocks, memory, threads). This is a real condition on the server.
    - A server that still waits for its first collection.
    - A stale collection. The viewer re-uses its collector-errors flag to carry staleness.

    Only the first is an incident. On a fresh install the second is normal for every server at once. At fleet scale the third is routine. So an opening screen of amber is usually not an incident, but do not turn that into a habit of ignoring it. Hover over the status word. Since #2429 the tooltip names the reason. The service builds it from the same metric rows that the card shows below, so it cannot disagree with them.
12. The Recommendations tab is empty for the first day. Analysis runs every 30 minutes for each server. It starts only when the store holds 24 hours of history for that server. A fresh install has no findings yet.

## Known gaps

These gaps are known and current. They are not bugs. Read this list so that you do not file them twice.

- TLS is opt-in on the MCP and the web endpoint (`mcp.network.tls`, `web.network.tls`). Without it, the token travels in clear on the segment. The other control is a TLS-terminating reverse proxy. Both are opt-ins for a trusted LAN.
- The viewer has no client-side secret store. The `darling.json` of a remote seat holds the store role password in cleartext. Set an ACL on it. This is fine for the read-only `viewer` role. Think about it for `admin`.
- The web dashboard has no plan analysis and no live-server actions. That is the job of the WPF viewer. The page set of the dashboard is a deliberate subset of the page set of the viewer.
- `network` blocks need a service restart. There is no hot reload for exposure. The service says so at startup and names the block that it loaded. So a config edit that appears to do nothing is at least explained.
- PostgreSQL monitored targets collect, and you can read them on both UIs. Apart from the three Tier 0 outage predictors, they do not raise alerts or produce analysis findings yet. There is no execution plan capture at all for them. That is the largest remaining gap, because plans are the reason people open a database monitoring tool. See [PostgreSQL Targets](../../Darling/README.md#postgresql-targets).
- The PostgreSQL tabs have no charts and no drill-downs. Every panel on both surfaces is a table over the window. There is no PostgreSQL equivalent of the correlated timeline lanes on the SQL Server Overview. You cannot click through from a blocking root to the sessions behind it. There is no history window for a single query.

If you hit a problem that this list and [Troubleshooting](../../Darling/README.md#troubleshooting) do not cover, please report it. Include the service log lines around the failure.

## Where to go next

| For | Read |
|---|---|
| The desktop app | [The WPF viewer](viewer.md) |
| The browser view | [The web dashboard](web-dashboard.md) |
| An LLM client | [Darling MCP setup](mcp.md) |
| Every config field, in detail | [Darling operator guide](../../Darling/README.md) |
| Permissions on monitored servers | [Permissions](../../Darling/README.md#permissions-on-monitored-servers) |
| The LAN endpoints in full | [Opt-in Network Endpoints](../../Darling/README.md#opt-in-network-endpoints-lan) |
| The store, retention, TimescaleDB | [Operations](../../Darling/README.md#operations) |
| What each collector gathers and when | [How collection works](../how-collection-works.md) |
| Pointing Darling at a PostgreSQL target | [PostgreSQL first-target runbook](../postgres-first-target-runbook.md) |
