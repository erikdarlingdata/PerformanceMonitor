# Darling UAT onboarding: from nothing to all three surfaces

You have a Windows box, a SQL Server or two you are allowed to monitor, and a Darling build. You have never
run it. This gets you to the three things you can actually look at:

| Surface | What it is | Part |
|---|---|---|
| **WPF viewer** | The desktop app. Everything: plans, blocking chains, deadlock graphs, live active queries | [Part 2](#part-2--the-wpf-viewer) |
| **Web dashboard** | A browser view of the fleet, served by the service itself. Off by default | [Part 3](#part-3--the-web-dashboard) |
| **MCP** | The endpoint an LLM client talks to. Off by default | [Part 4](#part-4--mcp) |

**Part 1 is shared** — all three read the same store, filled by the same service, so everyone does it.
Parts 2, 3 and 4 are independent of each other and of each other's order. Do the one you need.

**This is the procedure. [`Darling/README.md`](../Darling/README.md) is the reference** — every config field,
every permission, every operational knob, in far more detail than belongs here. Where you need to know what a
setting *means*, this links there rather than restating it. Where you need to know what to *type*, it is here.

Every step ends with a **proof point**: the log line, the HTTP response, the thing on screen that tells you it
worked. If a proof point does not appear, stop there — the next step will not fix it.

---

## Part 0 — two facts that explain most of the surprises

### `darling.json` is JSONC, not JSON

It contains `//` comments — 200-plus of them in the shipped sample, and they are the field documentation.
**Edit it as text**, in Notepad or VS Code.

PowerShell's `ConvertFrom-Json` fails on it outright, and even where a parser tolerates comments, a
`ConvertFrom-Json | ConvertTo-Json` round-trip silently strips every one of them and leaves the next person
with an undocumented file. There is no supported programmatic edit path; the one tool that rewrites the file
(`--configure-network`) preserves comments on purpose and takes a timestamped `.bak-` copy first.

### The file is a first-run *seed*. After that, the store is authoritative

On first start the service copies most of `darling.json` into control-plane tables in its own store. From then
on it reads its live configuration from there, and edits you make to the file for those sections are ignored.

This trips people up most on the MCP and web blocks, where **the two halves of one JSON object have different
owners**:

| Setting | Authority | Changed with |
|---|---|---|
| `mcp.enabled`, `mcp.port` | **the store** (`config.config_service.mcp_enabled` / `mcp_port`) | `--enable-mcp` / `--disable-mcp`, or the Viewer's Settings. Live, no restart |
| `web.enabled`, `web.port` | **the store** (`config.config_service.web_enabled` / `web_port`) | `--enable-web` / `--disable-web`, or the Viewer's Settings. Live, no restart |
| `mcp.network` / `web.network` (`listen`, `allowFrom`, `encryptedToken`) | **the file**, always. No store equivalent exists | editing `darling.json`, then **restarting the service** |
| `postgres.*` (including `postgres.network`) | **the file**, always | editing `darling.json`, then restarting the service |
| SQL-auth secrets (`encryptedPassword`) | **the file**, always — never written to the store | editing `darling.json` |
| `servers` | seeded once, then **the store** | the Viewer's Add Server, the `add_servers` MCP tool, or `--add-server` |

So: changing `mcp.port` in the Viewer changes where MCP binds, and putting a different `mcp.port` in the file
will not override it. When the two disagree the service says so, once per start, naming both sides:

> `MCP configuration disagrees across the two planes and the CONTROL PLANE WINS: port is 5152 in darling.json
> (mcp.port) but 5199 in config.config_service.mcp_port. After the first run darling.json's
> mcp.enabled/mcp.port are only the SEED — change them with --enable-mcp/--disable-mcp or the Viewer's
> Settings, or the file values will keep being ignored. The mcp.network block is the OPPOSITE: file-only,
> restart-only, no store equivalent — the control plane cannot change where this endpoint binds or what token
> it requires.`

---

## Part 1 — get the service collecting

### 1.1 Prerequisites

- **Windows** on the service host. The bundled store, DPAPI credential protection and the Windows-service
  lifetime are all Windows-only. (There is a Linux path — see
  [Run on Linux](../Darling/README.md#run-on-linux-docker-compose-or-systemd-1804) — but it is not this
  procedure, and the WPF viewer is Windows either way.)
- **Two .NET 10 runtimes**, from <https://dotnet.microsoft.com/download/dotnet/10.0>:
  - **ASP.NET Core Runtime 10.0** — the service needs it whether or not you ever enable MCP or the web
    dashboard, because the host framework is referenced unconditionally.
  - **.NET Desktop Runtime 10.0** — the WPF viewer.

  A stock Windows Server image has neither. `install-darling.ps1` now checks: it **refuses** the install if
  the ASP.NET Core Runtime is missing, and **warns** if the .NET Desktop Runtime is. The asymmetry is
  deliberate — without ASP.NET Core the service cannot start at all, whereas without the Desktop Runtime the
  service runs fine and only the viewer will not open. Install both first anyway and skip the round trip.
- **A monitored SQL Server** (2016–2025, Azure SQL MI, AWS RDS, or Azure SQL DB) and a login on it with
  `VIEW SERVER STATE` and the rest of the [monitoring grants](../Darling/README.md#permissions-on-monitored-servers).
- **Nothing else.** In the shipped default (`postgres.managed = true`) the service runs its own bundled
  PostgreSQL 18 + TimescaleDB. There is no database to provision.

### 1.2 Download and verify

Take `PerformanceMonitorDarling-<version>.zip` and `SHA256SUMS.txt` from the release (or the nightly) you were
pointed at, then check the hash before you extract anything:

```powershell
$zip = 'PerformanceMonitorDarling-3.5.0.zip'    # whatever you actually downloaded
(Get-FileHash -Path $zip -Algorithm SHA256).Hash.ToLower()
Select-String -Path .\SHA256SUMS.txt -SimpleMatch -Pattern $zip
```

**Proof:** the two hex strings match. (`SHA256SUMS.txt` is LF-terminated on purpose so `shasum -c` works on
macOS and Linux too.)

### 1.3 Extract to a machine-scoped path

```powershell
Expand-Archive -Path $zip -DestinationPath C:\PerformanceMonitorDarling
```

**`C:\PerformanceMonitorDarling` is the documented location and you should use it.** Not your Desktop, not
Downloads, not anywhere under `C:\Users\`, not a UNC path or a mapped drive. The service runs as the
unprivileged virtual account `NT SERVICE\PerformanceMonitor Darling`, which is not you and not an
administrator, and a user profile grants access to nobody else — the service installs cleanly and then cannot
read its own program files. `install-darling.ps1` refuses a fresh install in those locations for exactly this
reason; the [full explanation is in the reference](../Darling/README.md#the-install-location-has-to-be-machine-scoped).

**Proof:** the folder contains `PerformanceMonitor.Darling.Service.exe`, `darling.sample.json`,
`install-darling.ps1`, `pg-runtime.zip`, `wwwroot\`, and a `viewer\` subfolder holding
`PerformanceMonitor.Darling.Viewer.exe`. That is the whole product — the viewer is not a separate download.

### 1.4 Write `darling.json`

```powershell
Copy-Item C:\PerformanceMonitorDarling\darling.sample.json C:\PerformanceMonitorDarling\darling.json
notepad C:\PerformanceMonitorDarling\darling.json
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

`"auth": "integrated"` connects as the account the service runs under. The default virtual account reaches
*remote* servers as the machine account `DOMAIN\<host>$`, so for integrated auth against a remote server you
usually want to [run the service as a domain account or gMSA](../Darling/README.md#run-the-service-as-a-domain-account-or-gmsa)
instead. For SQL auth, set `"auth": "sql"`, a `"username"`, and an `"encryptedPassword"`:

```powershell
cd C:\PerformanceMonitorDarling
.\PerformanceMonitor.Darling.Service.exe --encrypt-password
```

It prints `Password: ` and waits. Type the password, press Enter, and it prints one line of base64 (it starts
`AQAAA`) — paste that into `"encryptedPassword"`.

> **`--encrypt-password` writes its prompt and its confirmation to stderr; only the blob goes to stdout.**
> That is deliberate (`… --encrypt-password > blob.txt` captures exactly the blob and nothing else) but it is
> fatal under `$ErrorActionPreference = 'Stop'`: PowerShell treats the stderr write as a terminating error,
> and then echoes the offending source line — which, if you piped the password in, leaks it into your
> scrollback. In a script, do this instead:
>
> ```powershell
> $ErrorActionPreference = 'Continue'
> $blob = Read-Host -Prompt 'password' |
>     & C:\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe --encrypt-password 2>$null |
>     Select-String -Pattern '^AQAAA' | ForEach-Object { $_.Line }
> ```

The blob is DPAPI at **LocalMachine** scope with a fixed entropy string, so it decrypts **only on the machine
that produced it**. Run `--encrypt-password` on the service host, and re-run it if you ever move `darling.json`
to a different box. The same rule applies to the MCP and web tokens in Part 3 and Part 4 — do not copy a
`darling.json` between machines and expect its secrets to survive.

### 1.5 Pre-flight

```powershell
cd C:\PerformanceMonitorDarling
.\PerformanceMonitor.Darling.Service.exe --test-connection
```

This validates the file and then actually connects to every configured server. It exits `0` only when the
config is valid **and** every server is reachable, so it is usable as a gate.

**Proof:**

```
Validating connectivity to 1 server(s)...
  [PASS] SQL2022: SQL major version 16, Enterprise Edition (64-bit), msdb access: yes
All servers reachable.
```

`msdb access: NO (failed-job alerts unavailable)` is a `[PASS]` too — it means the login cannot read
`msdb.dbo.sysjobs`, so failed-job alerts are skipped. That is expected for a minimal monitoring login; add the
direct msdb `SELECT`s from the permissions section if you want them.

A `Configuration is invalid:` block lists literal per-field problems — fix them and re-run. One caveat: this
verb connects as **you**, the console user, not as the service account. For `"auth": "integrated"` servers a
`[PASS]` proves reachability and a well-formed config; the grants that matter at runtime are the service
account's, and the per-server connect lines in step 1.7 are the real proof.

### 1.6 Install the service

From an **elevated** PowerShell, in the install folder:

```powershell
cd C:\PerformanceMonitorDarling
.\install-darling.ps1
```

It checks the install location, runs the pre-flight again, registers the Event Log source, creates the service
`PerformanceMonitor Darling` under the virtual account, reconciles firewall rules, starts it, and creates
Desktop and Start Menu **Darling Viewer** shortcuts. `.\uninstall-darling.ps1` reverses it and deliberately
leaves your store and config alone unless you pass `-PurgeData`.

If you want the LAN endpoints set up in the same pass, use `.\install-darling.ps1 -Network` — it runs the
guided wizard at the end. Parts 3 and 4 cover doing it later.

### 1.7 Wait about two minutes

**The first start does real work: it unpacks `pg-runtime.zip`, runs `initdb`, migrates the store, provisions
the least-privilege roles, and completes a first collection cycle.** The installer says so on its last line:

> `Service is Running. First start does real work (unpack pg-runtime, initdb, store migration, first
> collection cycle) - give it ~2 minutes.`

**It has not hung. Do not kill it.** The internal `initdb` timeout alone is 180 seconds. Watch the log:

```powershell
Get-Content "$env:ProgramData\PerformanceMonitorDarling\logs\darling-service_$(Get-Date -Format yyyyMMdd).log" -Wait -Tail 40
```

**Proof** — these lines, in this order:

```
Loaded configuration from C:\PerformanceMonitorDarling\darling.json: 1 server(s)
Extracting the bundled Postgres runtime from ...\pg-runtime.zip (first run)
Initializing managed Postgres cluster in C:\ProgramData\PerformanceMonitorDarling\pg (first run)
Managed Postgres cluster initialized (scram-sha-256, data checksums, UTF8/C locale)
Managed Postgres started
Creating the 'darling' database
Postgres store ready (schema v79, 79 migration(s) applied)
Least-privilege roles ready (admin: ...; viewer: ...; mcp: ...)
TimescaleDB detected — hypertables, chunk-based retention, and compression enabled
TimescaleDB: 50/50 collector table(s) are hypertables
PerformanceMonitor Darling collection loop started
[SQL2022] Connected (major 16, edition Enterprise Edition, server_id 1)
  [SQL2022] wait_stats => 412 rows (sql:31ms, pg:12ms)
```

`PerformanceMonitor Darling collection loop started` is "ready". The per-collector `=> N rows` lines repeating
every sweep is "working". On the hypertable line **both numbers should be equal**; a converted count below the
total means a table stayed plain, and the line above it names which.

Two things that look wrong on a first start and are not:

- **One-time `discarding and regenerating` warnings** about the role `.dpapi` credential files. A role password
  can be re-asserted, so the service does; this is the ownership check working.
- **A collector reporting `YIELDED`.** That is the one-second lock-timeout guard firing on a busy target — the
  design refusing to become the problem it is monitoring.

If the service idles instead, the message is literal and named: `Cannot load configuration`,
`Configuration problem: <field>`, or `Cannot reach or migrate the Postgres store`. See
[Troubleshooting](../Darling/README.md#troubleshooting).

---

## Part 2 — the WPF viewer

The viewer talks **only to the PostgreSQL store**. It never connects to your monitored SQL Servers; every
number it shows was collected by the service. That is why it needs no SQL Server credentials and why a
read-only seat is a real thing (see 2.3).

### 2.1 On the service host

Double-click the **Darling Viewer** shortcut the installer made, or run
`C:\PerformanceMonitorDarling\viewer\PerformanceMonitor.Darling.Viewer.exe`.

**There is nothing to configure.** It looks for `darling.json` in four places, in order: an explicit
command-line path, `%DARLING_CONFIG%`, beside its own exe, and then **one directory up** — which is exactly
the zip's `viewer\`-under-the-service-root layout. It reads only the `postgres` section, derives
`Host=127.0.0.1;Port=5641;Database=darling`, and decrypts the store password from
`pg-admin-credential.dpapi` beside the data directory.

**Proof:** the left sidebar lists your servers, and the Overview tab shows a health card per server. Double-click
a card (or the sidebar entry) to open that server's tab.

**If it will not connect,** the failure window carries a **Configuration this viewer used** block naming the
`darling.json` it actually read, which of the four rules picked it, and the host/port/user/database/SSL mode it
parsed — with a **Copy details** button. Read that before changing anything: it separates *the viewer read a
different file than you edited* from *it read your file and a value in it is wrong*. It never contains a
password. The same lines are in `%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_yyyyMMdd.log`.

Two specific failures worth naming:

- `The Darling store is at schema v78, but this viewer needs v79. Update or restart the Darling service so it
  migrates the store, then reopen the viewer.` — the viewer is newer than the service. Upgrade or restart the
  service; it migrates on start. (The gate is `<`, not `≠`: a store *newer* than the viewer is not blocked.)
- `pg-admin-credential.dpapi ... does not exist yet.` — the service's first run never got far enough to
  provision the roles. The reason is in the **service** log, not in `darling.json`.

### 2.2 On another machine (a remote seat)

Two things have to be true, and the service-side one is the one people miss.

**Service side — the store has to be reachable off the box.** By default it listens on `127.0.0.1` and accepts
no remote viewer at all; no amount of viewer-side configuration changes that. On the service host, from an
**elevated** PowerShell:

```powershell
cd C:\PerformanceMonitorDarling
.\PerformanceMonitor.Darling.Service.exe --configure-network
```

Choose the **store** option. The wizard validates every answer against the same resolvers the running service
fail-closes on, edits `darling.json` in place preserving its comments (behind a timestamped `.bak-` backup),
prints the scoped firewall command, and offers to restart the service. On that restart the service adds the
bind IP to `listen_addresses`, generates a self-signed TLS certificate with an IP SAN, and writes a
`hostssl darling <role> <allowFrom> scram-sha-256` line into `pg_hba.conf`.

Then, still elevated:

```powershell
.\PerformanceMonitor.Darling.Service.exe --configure-firewall
```

**Proof:** `Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling store (port 5641)'` returns a rule
with `Enabled=True, Action=Allow`. Reading firewall rules needs no elevation, so this check works from any
shell.

**Then export the seat:**

```powershell
.\PerformanceMonitor.Darling.Service.exe --export-viewer-config
```

It writes a `viewer-config\` folder holding three files — `darling.json` (connection string resolved,
`"managed": false` already set, every field explained in comments inside the file), `server.crt` (the
certificate the connection pins), and `README.txt`. **That `darling.json` contains a live database password**;
the verb ACLs it, says so, and exits non-zero if the ACL did not take. Copy the folder over a channel you
trust.

**Viewer side — two ways to get the app onto the remote machine:**

| | Where the viewer lives | Where to put the three exported files |
|---|---|---|
| Copy `viewer\` out of `PerformanceMonitorDarling-<ver>.zip` | wherever you unpacked it | next to `PerformanceMonitor.Darling.Viewer.exe` — works unedited |
| Run the standalone `PerformanceMonitorDarlingViewer-<ver>-Setup.exe` (Velopack; auto-updating) | a versioned folder the installer manages | **anywhere**, then point `%DARLING_CONFIG%` at the exported `darling.json` |

Prefer `DARLING_CONFIG` with the Setup.exe: its install directory changes on every update, so files dropped
beside the exe do not survive. Either way keep the three files **together** — a bare `Root Certificate=server.crt`
resolves against the folder holding `darling.json`, not the working directory, so the folder can live anywhere
as long as it stays intact.

Re-run `--export-viewer-config` and re-copy after a credential rotation or a `listen` change — the store's
certificate regenerates when its bind IP changes, and an old copy stops matching.

### 2.3 Read-only versus read-write seats

**This is the single most-asked question about the viewer, and the answer is not what the wording suggests.**

Some viewer actions are not reads. "Current Active Queries" → Refresh, "Get Actual Plan", "Purge Now",
Recommendations → "Generate now", alert Dismiss, mute-rule editing, Add Server, schedule editing: all of these
work by **enqueueing a command for the service** — literally an `INSERT` into `config_command` **in the
monitoring store**. The service picks the row up, does the work over its own connection, and writes the result
back. A read-only seat cannot write that row.

**Nothing is written to your monitored SQL Server. Ever.** The monitored server is only read, by the service,
under the same least-privilege login the collectors use. The refusal says so out loud:

> `Read-only viewer — the live fetch queues a request for the service in the MONITORING STORE, which a
> read-only seat can't write to. Nothing is sent to or written on the monitored server. Reconnect with a
> read-write store profile to fetch live active queries.`

**Which seat you get, by default:**

| Seat | Setting | Default | Result |
|---|---|---|---|
| Local, on the service host | `postgres.connectAs` | `"admin"` | **read-write** |
| Remote, over the LAN | `postgres.network.role` | `"viewer"` | **read-only** |

Those defaults are deliberately opposite: the local seat is the operator's, the remote seat is a laptop. **So
a UAT tester on a remote seat should expect read-only** and should not read it as a fault.

**How the viewer decides.** On connect it runs one probe:

```sql
SELECT has_table_privilege('config_mute_rules', 'INSERT')
```

True means read-write. False means read-only. **Anything else also means read-only** — the probe fails safe, so
a permission quirk or a mis-provisioned store hides the write affordances rather than letting you dead-click
into a permission error. (One nuance: a failure to *reach* the store is not a read-only verdict; that surfaces
as "Can't reach the Darling store — is the Darling service running?" instead.)

So **if you see an unexpected read-only seat, run that probe yourself** against the store as the role the
viewer is using. If it returns `true` and the viewer still says read-only, that is a bug worth reporting.

**How to tell which seat you have.** There is no global banner — it shows up per surface. The quickest check
is to open **Settings**: a read-only seat shows

> `This viewer is connected with a read-only role. Monitoring settings below are shown for reference but can't
> be changed from here.`

Elsewhere you will see disabled buttons with tooltips, or a status line saying which write is blocked. Some
dialogs say it in their own title bar — Database State Overrides appends `(read-only seat — changes cannot
be saved)`.

**There is now one place to look.** The main window carries a seat indicator reading **`Seat: read-only`** or
**`Seat: read-write`**, set from the same probe, so you no longer have to trip a refusal to find out. Hover it
and the tooltip gives the reason for *this* connection — a host seat connects as `postgres.connectAs`
(default `admin`, read-write), a LAN seat as `postgres.network.role` (default `viewer`, read-only) — along
with the reassurance that nothing is ever written to a monitored SQL Server.

Before the probe returns, the indicator says so rather than guessing. A viewer that cannot reach the store
reports *that*, and does not claim read-only — a status line blaming your role when the service is simply
down would send you off fixing the wrong thing.

**How to change it.** On a managed store this is a config change, not a `GRANT` — the `admin` role and its
credential already exist, provisioned on every service start:

- Local seat: set `"connectAs": "admin"` in the `postgres` block and restart the viewer.
- Remote seat: set `postgres.network.role` to `"admin"` on the **service host**, restart the service, and
  re-run `--export-viewer-config`. The service logs a warning when you do, because `admin` holds the
  `config_command` / `config_monitored_servers` / `config_notification` pivot — that is the trade, made
  knowingly.
- Bring-your-own PostgreSQL: run [`Darling/tools/provision-roles.sql`](../Darling/tools/provision-roles.sql)
  as the store owner; it is the authoritative grant list.

---

## Part 3 — the web dashboard

### 3.1 Why it is off

On loopback the browser surface has no token, and TLS is opt-in rather than on. Shipping it on by default
would mean every install serves its monitoring data to anything that can open a socket on the box. So it is
off, and turning it on is one command.

### 3.2 Turn it on

**Headless**, from an **elevated** PowerShell on the service host:

```powershell
cd C:\PerformanceMonitorDarling
.\PerformanceMonitor.Darling.Service.exe --enable-web
```

Or tick **Enable web dashboard** in the Viewer's **Settings**. Both write the same control-plane flag, and the
running service applies it **within one collection sweep — no restart**.

Both routes write the *store*, so **the service must have completed Part 1 at least once**. On an unseeded box
the verb refuses and tells you to start the service first — it will not fall back to editing the file.

**Proof:**

```
web dashboard endpoint ENABLED in the control-plane store (config.config_service.web_enabled = true).
The running service applies this LIVE within one collection sweep (the write self-bumps the reload beacon) — no restart needed.
```

followed by, in the service log:

```
Starting web dashboard on http://localhost:5153 (loopback only) — enabled/port from the control plane (config.config_service)
```

### 3.3 Look at it

On the service host, open `http://localhost:5153/`. **A loopback-only dashboard — one with no `web.network`
block — is tokenless**; the auth middleware is not installed at all in that mode. (That changes once you
expose it on the LAN. See 3.5, and do not assume loopback keeps a free pass.)

**Proof:** a sidebar titled **Darling Web** with **Fleet Overview**, **Alert History** and **Custom Views**, a
list of your servers under them, and fleet cards in the main pane. (**Availability Groups** appears only once
the store holds AG data; the `#/ag` route works regardless.) Or, scriptably:

```powershell
Invoke-RestMethod http://localhost:5153/api/ping
```

**Proof:** `status : ok`, and `collecting : True`.

`/api/ping` is the only health surface that does not read the store, so it is the only one that can tell you
the store is the problem — it reports the collector's own startup verdict, in four states:

| HTTP | `status` | What it means |
|---|---|---|
| 200 | `ok` | The collection loop started. |
| 200 | `starting` | The service is still bootstrapping and has not reached a verdict. Normal for the first seconds of a start. |
| 503 | `degraded` | A startup step failed transiently and is being retried — `step`, `attempt`, `attempts` and `detail` say which and why. Clears itself, or becomes `stopped`. |
| 503 | `stopped` | A startup step failed terminally. **Collection will not start until the service is restarted**, and `detail` carries the reason the log's critical line gives. |

Point an uptime check at this route and alert on the status code: the two 503 states are the ones where the
service is up, the dashboard still answers, the Windows service still reports **Running**, and nothing is
being collected. Note that `Invoke-RestMethod` THROWS on a 503, which is the behaviour you want from a
scripted check — catch it and read `$_.ErrorDetails.Message` for the body.

### 3.4 What it will not do

Be clear about this before you file it as a bug. The web dashboard reads the store as the least-privilege
`viewer` role. Its `/api/read/*` surface is machine-derived from the MCP tool catalog minus an explicit
exclusion list — 93 read endpoints out of 112 tools — and these are the exclusions that matter to you:

- **No plan analysis.** The whole `analyze_query_plan` / `analyze_procedure_plan` / `analyze_query_store_plan` /
  `analyze_plan_xml` family is excluded. **Use the WPF viewer for plans** — it has the graphical plan viewer,
  the block-chain reconstruction and the deadlock-graph renderer.
- **No `analyze_server`.** That makes a live outbound connection to a monitored server; the web host does not.
- **No live "Current Active Queries" fetch, no actual-plan capture, no purge** — all of those are the
  `config_command` enqueue from 2.3, and the web host has no write path to it.
- **No alert tuning, no mute-rule writes, no adding or removing servers.**

The pages are Fleet Overview, per-server, Alert History, Availability Groups and Custom Views. The per-server
page carries one of **two** tab sets, chosen from the engine the store recorded for that server
([#2530](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2530)):

- **SQL Server — twelve sub-tabs.** Overview, Wait Stats, CPU, Memory, Blocking, File I/O, Queries,
  Configuration, Config Changes, Activity, System Events, Collection Health.
- **PostgreSQL — seven.** Overview, Activity, Vacuum, Waits, I/O, Replication, Storage. Seven is the design
  and not a shortfall: it is where every `get_pg_*` read lives, and the SQL Server tabs it does not have
  (tempdb, Query Store, trace flags, plan cache, the `system_health` ring buffer) have no PostgreSQL analogue
  to fill them. Vacuum is deliberately one tab rather than three — an old xmin horizon starves vacuum, starved
  vacuum falls behind on freezing, and freezing falling behind is what ends in wraparound; read separately each
  looks survivable, and together they are one escalating story. Since #2540 that story is FOUR panels: the
  first names the session actually holding the horizon, which is the link the chain used to start one step
  past. **Storage** is separate from Vacuum for the
  same reason in reverse: table bloat is what vacuum lag COSTS, and dropping the damage into the middle of a
  three-panel causal sequence would break the sequence — so bloat sits beside index usage instead, where both
  panels answer one question (where the space went and whether it is earning its keep).
- **A server whose engine the store has not recorded gets the SQL Server set**, which is what it always got.
  A NULL `engine_kind` means "no connect has stamped it", which is not a claim of either engine, and only a
  positive PostgreSQL claim moves a server off the default. A server that has never connected will therefore
  show SQL Server tabs until it does.

Between them the two sets reach **81 of the 93** read endpoints, 82 across all five pages, against the viewer's
nineteen top-level per-server tabs (65 counting their inner tabs). So most of what the viewer shows is now
here, and what is not divides into three groups.

**Reads that exist but no web page shows.** `get_database_scoped_config`, whose `databases[].settings[]` shape
the table renderer cannot draw, and `get_store_metrics`, which is store-wide and has no per-server home.

`get_query_trend` was on that list until [#2520](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2520),
and it was the only entry anywhere in this section whose absence was missing UI rather than a stated boundary:
the read keys on a **required** `query_hash` plus a **required** `database_name`, and nothing on the web could
supply either. The Queries tab's Top Queries table now carries a picker, and the query you pick gets a chart of
its avg CPU and avg duration over the window plus a grid of its per-collection snapshots. **The queries offered
are exactly the queries the table shows** — the picker indexes into the rows rendered directly above it, rather
than reading a second, wider list that could offer a query the table does not.

**Every PostgreSQL capture path is now on BOTH surfaces.**
[#2530](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2530) landed the web tabs first and the
WPF viewer second, and the desktop set is deliberately the same seven with the same names and the same
grouping — Overview, Activity, Vacuum, Waits, I/O, Replication, Storage — so the two front ends do not teach
one engine two shapes. The desktop panels are grids rather than charts, and the desktop Overview is the
per-collector collection report (which of them ran, when, with what result, and for one that cannot run here,
why), which is the answer a PostgreSQL operator wants first and the one the web splits across tiles.

Where the desktop set genuinely differs: the per-server **database filter is hidden** at a PostgreSQL target,
because it drives the SQL Server database-scoped reads and nothing else — offering to filter views that never
consult it is worse than not offering. And the Blocking panel prints its **sampling denominator** above the
grid whether or not the grid has rows: `pg_blocking` is a periodic sample, not an event log, so "two chains"
means something different in a window of 60 captures than in a window of 4, and an absent capture and a
capture that found nothing are the same absence of rows in the stored edge list.

**Two of the PostgreSQL capture paths are Amazon Aurora-only**, so on a stock PostgreSQL target two
panels are permanently empty — and neither is a fault. `get_pg_wait_stats` reads `aurora_stat_system_waits()`
and `get_pg_top_queries` reads `aurora_stat_statements()`; core PostgreSQL has an equivalent of neither, in any
version. Both answer `not_collected` there, naming the server, the engine and the collector and saying the gap
is permanent, rather than going blank — and both tabs carry a note saying so before you click.

**Three of them are writer-only**, which is a different kind of gap and needs a different response: on a read
replica `pg_autovacuum_stats`, `pg_index_usage_stats` and `pg_table_bloat_stats` do not run at all. The reason
is not availability but truth — a replica keeps its OWN statistics, so on it an index the primary's workload
scans a million times an hour reads as never scanned. Those collectors gate off rather than collect a
confidently wrong answer, and the Overview grid says so per collector. Ask the writer.

**The bloat figure on the Storage tab is an ESTIMATE and the surface says so in three places** — the tab note,
the panel note and the column heading. It is arithmetic over PostgreSQL's column-width statistics and never
reads the table, which is what makes hourly collection affordable; measured against `pgstattuple` it was
within about 2 percentage points where the statistics were current, and 81 percentage points out where they
were not. Rows whose statistics cannot be trusted show a **dash rather than a number**. If most rows show a
dash and the reason column says "no column statistics", that is a permissions gap rather than a broken
collector: `pg_stats` is filtered by SELECT privilege and `pg_monitor` does not confer it, so the monitoring
role needs `pg_read_all_data` (PostgreSQL 14+) before this panel can say anything. The measured sizes and the
dead-tuple percentage beside it are unaffected — those come from the server's own counters.

**The Sessions panel on the Vacuum tab needs `pg_monitor`, and without it it fails QUIETLY rather than
loudly.** PostgreSQL does not refuse the read: for every backend the login does not own it returns the
row with all but six columns NULL. Measured column by column on PostgreSQL 16.15, what survives is
`pid`, `application_name`, the database and the user — plus `backend_xid` and `backend_xmin`. `state`,
`state_change`, `xact_start`, `query_start`, `backend_start`, `wait_event`, `backend_type`,
`client_addr` and `query_id` all come back NULL and the statement text is replaced by an
insufficient-privilege literal — while `backend_xmin` and `backend_xid` stay VISIBLE. So the horizon
still reads as pinned and nothing on the screen can say by what. Measured against a least-privileged role
on the same nine backends, the privileged role saw four sessions idle in transaction and the unprivileged
one saw zero. Rows in that state are marked redacted and carry no severity at all rather than being
painted healthy. Unlike the bloat panel this needs no `pg_read_all_data` — plain `pg_monitor` is enough.

**A long `idle in transaction` on that panel is not automatically a problem**, and the panel will tell you
which kind you have. Two of the four idle-in-transaction shapes measured on a live instance pin nothing:
a READ COMMITTED transaction that only read has already released its snapshot, and one whose `UPDATE`
matched zero rows never got a transaction id. Those rows show a horizon age of "pins nothing" rather than
a number, and terminating them reclaims not one dead row. Read the horizon column, not the clock.

**Waits** is the only tab that is *wholly* Aurora-only. Activity's other half, the blocking panels, is
collected at every PostgreSQL target including standbys, where a recovery conflict is blocking that happens
nowhere else. Waits is **shown** at stock PostgreSQL deliberately, so the tab set does not change shape between
two PostgreSQL servers in one fleet, and so the Aurora capability is discoverable from a stock instance rather
than invisible.

The web used to carry an additional obstacle on top of that, and **it is now gone**: `/api/fleet`'s card
carried the SQL Server `engine_edition` and no target-engine discriminator, so a browser could not tell a
PostgreSQL target from a SQL Server one — a PostgreSQL target lands at `engine_edition` 0, which is also what
a SQL Server that has never connected lands at. The store now records the target engine explicitly
(`collect.servers.engine_kind`, schema v82) and the card carries `engine_kind`, the derived
`is_postgres` / `is_aurora` booleans, and `engine_description` — the engine's name in words, composed on the
server so no viewer owns a second copy of the vocabulary. **Both tab registries now branch on it** — the web's
`serverTabsFor(card)` and the viewer's `ViewerPostgresTabs` — and the viewer's per-server header carries the
same engine label. A server the store makes no claim about gets **no badge at all** rather than one reading
"SQL Server": the tabs it gets are a default, not a finding.

One consequence you may notice before the screens arrive: a SQL Server read aimed at a PostgreSQL target now
answers `not_collected` naming the engine ("...runs Aurora PostgreSQL. The system_health_events collector is
written against SQL Server...") instead of `unavailable` telling you to check that collection is running.
That applies to the reads wired to the capability helper, not yet to every read.

**Data with no read endpoint at all** — **this list is now empty.** It used to hold ten viewer surfaces
(Query Store regressions, the query heatmap, three of the four Performance Trends charts, the
blocking-duration and deadlock-severity statistics, the lock-wait / waiting-task / blocked-session trends, the
raw collection log, and the daily-summary month range), all tracked as
[#2484](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2484). Every one of them is now a read on
both surfaces — the data was always collected, so each was a missing endpoint rather than missing collection.
One correction came out of the work: the viewer's execution-count trend was a DUPLICATE of a number
`get_query_duration_trend` already returned rather than a missing sibling, so it did not become an eleventh
read; what it got instead was `executions_per_second` beside the truncated integer count, because a server
running 0.4 executions a second used to report zero and read as idle.

**Desktop things a web imitation would be worse than.** No graphical plan viewer, no interactive query
heatmap **plot** (the underlying read ships as a bucketed table on the Queries tab — same answer, no canvas),
no block-chain reconstruction and no interactive deadlock graph — the Blocking tab hands you the captured
blocked-process-report and deadlock-graph XML verbatim instead of pretending. No period-compare grids. The
per-query drill-down above charts the same history the viewer's window does, but it is not that **window**:
no stored-plan download or cached-plan fetch from it, and none of the desktop grids' own affordances —
per-column filter popups, CSV export, Copy Repro Script, and right-click drill-down into a ±30-minute
window.

What it *can* write is one thing: **Custom Views** — the saved dashboards and notebooks in
`config.custom_views`, created and edited through the built-in composer, plus `POST /api/compose/run` to
preview one. That is the only write the web host exposes at all, and it is available to **any authenticated
seat**, not only a local one. It is also the reason the loopback token exemption went away (see 3.5).

(The `viewer` role itself carries one more narrow grant — `config.database_state_expected`, behind the WPF
viewer's per-database state-override editor — but no web route reaches it, and the WPF editor gates itself on
the same read-only probe from 2.3, which a `viewer`-role seat fails. So it is a grant nothing on either
surface currently spends.)

### 3.5 Reach it from a browser on another machine

Add a `network` block to `web` in `darling.json` (managed mode only), or let `--configure-network` write it:

```json
"web": {
  "enabled": true,
  "port": 5153,
  "network": {
    "listen": "192.168.1.205",
    "allowFrom": "192.168.1.0/24",
    "encryptedToken": "<output of --encrypt-password>"
  }
}
```

`--configure-network` generates a 32-character token for you and prints the plaintext once. Bring your own
instead if you prefer, and encrypt it with `--encrypt-password`. A plaintext `"token"` works and is warned
about.

**You can reprint it.** Losing the plaintext no longer means regenerating and re-onboarding every client:

```powershell
.\PerformanceMonitor.Darling.Service.exe --print-web-token
```

Run it **elevated, on the box that encrypted it** — DPAPI is `LocalMachine`-scoped, so the blob only opens
there. It writes a live token to stdout, so treat that console the way you would the token itself.

**The block is file-authoritative and read once at service start.** After editing it, from an **elevated**
PowerShell:

```powershell
Restart-Service 'PerformanceMonitor Darling'
C:\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe --configure-firewall
```

**How the auth actually works in a browser.** Open `http://192.168.1.205:5153/` and paste the token into the
minimal login form, or go straight to `http://192.168.1.205:5153/?token=<token>`. The host validates it
constant-time, sets an **HMAC-signed, HttpOnly, SameSite=Strict cookie** named `darling_web_session` with a
12-hour lifetime, and 302-redirects to strip the token out of the URL so it never lands in history or a
`Referer`. Every later request rides the cookie. The signing key is per-process, so **a service restart logs
everyone out** — present the token again.

Two gates, and the order matters:

1. **CIDR**, checked in-app on the remote address. Outside `allowFrom` is **403** with an empty body, cookie or
   no cookie. A remote address the host cannot determine fails closed to 403 as well. **Loopback skips this
   check** — otherwise `127.0.0.1` would 403 the operator's own browser.
2. **Credential.** A valid cookie passes; a valid `?token=` is exchanged for one; anything else gets the login
   form back as **HTTP 200**, not a 401.

> **Loopback does *not* skip step 2.** Once a `web.network` block exposes the dashboard, **every** request
> authenticates, including one from the box itself. It used to pass tokenless on the grounds that the surface
> was read-only; that stopped being true when the Custom Views composer gained create/update/delete and
> `/api/compose/run`, so a local process — a scheduled task, SSRF'd code, another user's session — could read
> the whole store and mutate views with no credential. In exposed mode the token *is* the loopback guard.

**Proof:** the dashboard renders in the remote browser, and the address bar shows no `?token=`.

**It is fail-closed.** An unparseable `listen`, an invalid `allowFrom`, a missing token, or bring-your-own
PostgreSQL all degrade the endpoint back to loopback and log a critical line saying which one — the endpoint
never half-exposes.

**TLS is opt-in, and without it the token and cookie travel in clear on the segment.** The CIDR bounds *who
can route to* the port; it does not protect the wire, and an exposed dashboard with no certificate says so at
every start. Point `web.network.tls` at a PKCS#12 bundle or a PEM pair (`Darling/README.md` has the shape), or
put a TLS-terminating reverse proxy in front of the port. The certificate applies to the LAN listener only —
loopback stays plain HTTP — and a missing or expired one keeps the dashboard loopback-only rather than
falling back to cleartext. **Never expose this to the internet.**

---

## Part 4 — MCP

### 4.1 Why it is off

The MCP token gates the entire read surface plus `analyze_server`, which opens live outbound connections to
your monitored SQL Servers, plus the write tools that change the monitor's own configuration (custom views,
alert tuning, adding and removing monitored servers). That is a bigger blast radius than the web dashboard's,
which is why the two are separate surfaces with separate flags, ports and tokens.

Worth stating plainly, because it is the first question anyone asks: **no MCP tool runs SQL that an AI client
wrote against your monitored servers.** No such tool exists. Every tool answers from the monitoring store, and
the only live contact with a monitored server — `analyze_server`'s plan fetch and `add_servers`' connection
probe — runs the product's own fixed, read-only queries under the collectors' least-privilege login.

### 4.2 Turn it on and connect from the same box

From an **elevated** PowerShell on the service host:

```powershell
cd C:\PerformanceMonitorDarling
.\PerformanceMonitor.Darling.Service.exe --enable-mcp
```

Or tick **Enable MCP server** in the Viewer's **Settings**. Live within one sweep, no restart.

**Proof**, in the service log:

```
Starting MCP server on http://localhost:5152 (loopback only) — enabled/port from the control plane (config.config_service)
```

and:

```powershell
Get-NetTCPConnection -State Listen | Where-Object LocalPort -eq 5152
```

Then register it. **On loopback there is no token** — the bearer requirement only applies once you expose the
endpoint on the LAN:

```powershell
claude mcp add --transport http --scope user sql-monitor-darling http://localhost:5152/
```

(The Viewer's Settings has a **Copy Setup Command** button that puts exactly this on your clipboard with the
port you configured.)

**Proof:** `claude mcp list` shows `sql-monitor-darling` as connected, and `/mcp` in a new Claude Code session
lists **101 tools** — `list_servers`, `get_fleet_overview`, `get_wait_stats`, `analyze_server` and the rest.
Ask it *"what servers are being monitored?"* and it should answer from your store.

Two details that save a round of guessing: the endpoint is the **root path** (`http://localhost:5152/`, not
`/mcp` and not `/sse`), and the transport is **stateless Streamable HTTP** — each request is self-contained, so
a client that does not echo an `Mcp-Session-Id` still works.

### 4.3 Connect from another machine

Add a `network` block to `mcp` (managed mode only, and `mcp.enabled` must be on):

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

Same rules as the web block: file-authoritative, **read once at service start**, fail-closed, and the token is
DPAPI at LocalMachine scope so it must be produced on that box. `--configure-network` will generate and encrypt
one for you and print the plaintext once, and `--print-mcp-token` reprints it later — elevated, on that same
box — so a lost token does not cost you a re-onboard. Then, from an **elevated** PowerShell:

```powershell
Restart-Service 'PerformanceMonitor Darling'
C:\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe --configure-firewall
```

**Proof**, in the service log:

```
Starting MCP server on http://192.168.1.205:5152 (LAN-exposed to 192.168.1.0/24 behind a bearer token + in-app CIDR; loopback also bound) — enabled/port from the control plane (config.config_service); listen/allowFrom/token from darling.json mcp.network (file-only, restart-only)
```

Then the client. **Point it at the box's LAN IP, never `localhost`** — an off-box client aimed at localhost is
the number-one "MCP won't connect" cause, and it fails without an obvious error:

```powershell
claude mcp add --transport http --scope user sql-monitor-darling http://192.168.1.205:5152/ `
  --header "Authorization: Bearer <token>"
```

Two gates apply, in this order: the **bearer token**, checked constant-time with **no loopback exemption**, and
then the **in-app CIDR check** on the remote address (loopback always allowed). A remote client with a bad or
missing token gets **401**; one from outside `allowFrom` is refused before any response.

**There is no TLS on MCP** — a self-signed certificate breaks real MCP clients, so the bearer token travels in
clear on the segment. The MITM control is a TLS-terminating reverse proxy in front of the port. **Never expose
this to the internet.**

### 4.4 When it says enabled and still will not connect

Both common failures leave the store flag reading `true`, so "it says enabled" is not proof. In order:

1. **Is it bound to the LAN address, or still loopback?**
   `Get-NetTCPConnection -State Listen | Where-Object LocalPort -eq 5152` must show the box's LAN IP, not only
   `127.0.0.1` / `::1`. *Enabled but still loopback-bound* means the service loaded `darling.json` before the
   `network` block existed — the enable toggle stops and starts the endpoint with the already-loaded config and
   does **not** re-read the file. `Restart-Service 'PerformanceMonitor Darling'`, then re-check.
2. **Does the firewall rule exist, on the right port?**
   `Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling MCP (port 5152)'`. See the port gotcha in
   Part 5.
3. **Is the client pointed at the box's IP, with the header?** And do a fresh `initialize` + `tools/list`
   rather than trusting a cached tool list from a previous version.

**Rejected requests are logged now, so this is answerable from the service log.** A 401 (bad or missing
bearer), 403 (outside `allowFrom`) or 400 (Host-header guard) on either endpoint writes a line naming the
gate that rejected it and why:

```
MCP refused a request from 10.1.2.3 (HTTP 403): the source address allowlist (network.allowFrom) gate
rejected it — ... .
```

That is the whole "is my token wrong, or my CIDR wrong?" question answered directly, rather than inferred
from a status code on the client. It is rate-limited to one line per gate per source per 10 minutes, and a
folded line then says how many further refusals it stands for — so a scanner hitting the port cannot flood
the log, and you still see that it happened. If the budget for distinct sources fills, the line says it is
speaking for several, which is itself the signal that the port is being scanned.

If the block itself is bad the service *is* loud, and it fails closed to loopback rather than half-exposing:

```
MCP network exposure requested (mcp.network.listen is non-loopback) but no bearer token is set — refusing to expose; binding loopback-only. Set mcp.network.encryptedToken (via --encrypt-password) or mcp.network.token.
MCP network token could not be decrypted (...) — refusing to expose; binding loopback-only.
MCP network exposure requested but mcp.network.allowFrom '...' is not a valid CIDR or its address family does not match mcp.network.listen — refusing to expose; binding loopback-only.
mcp.network.* is set but postgres.managed = false — MCP network exposure is managed-mode (or container) only and is ignored ...
```

A "could not be decrypted" line on a box you copied `darling.json` onto is gotcha 4 in Part 5, not a bug.

---

## Part 5 — the things that will actually bite you

Every one of these has cost somebody real time.

1. **`darling.json` is JSONC.** `ConvertFrom-Json` fails on it; a round-trip strips the comments that are its
   documentation. Edit it as text. ([Part 0](#darlingjson-is-jsonc-not-json))
2. **First start takes about two minutes** and looks like a hang. It is unpacking PostgreSQL and running
   `initdb`. Do not kill it. ([1.7](#17-wait-about-two-minutes))
3. **`--encrypt-password` prompts on stderr**, which is a terminating error under
   `$ErrorActionPreference = 'Stop'` — and PowerShell then echoes the source line, leaking a piped-in password.
   ([1.4](#14-write-darlingjson))
4. **Every DPAPI blob is LocalMachine-scoped** — SQL passwords, the MCP token, the web token. They decrypt only
   on the machine that encrypted them. Copying `darling.json` to another box gives you an undecryptable file,
   not a portable config.
5. **A remote viewer seat is read-only by default**, because `postgres.network.role` defaults to `"viewer"`
   while the local `connectAs` defaults to `"admin"`. What a read-only seat cannot do is `INSERT` into
   `config_command` **in the monitoring store** — it is not a write to your SQL Server. The probe fails safe, so a store that answers oddly also
   reads as read-only. ([2.3](#23-read-only-versus-read-write-seats))
6. **`mcp`/`web` `enabled` and `port` are store-authoritative; the `network` block is file-authoritative and
   restart-only.** Changing the port in the Viewer changes where the endpoint binds and the file cannot
   override it; changing the `network` block in the file does nothing until you restart the service.
   ([Part 0](#the-file-is-a-first-run-seed-after-that-the-store-is-authoritative))
7. **The firewall rule is named for its port**, so changing a port does not move a rule — it creates a
   different one and leaves the old one standing as an inbound allow with nothing behind it. **Re-run
   `--configure-firewall` elevated after any port change.** It sweeps `PerformanceMonitor Darling <surface>
   (port *)` so the stale rule goes with it.

   One narrow window remains on an **upgrade of a box whose port was moved in the Viewer**: the installer's
   first `--configure-firewall` runs before the service starts, when the managed store (a child of the service)
   is down, so the verb falls back to `darling.json`'s seed port. It says so rather than falling back silently,
   and since #2436 the installer re-runs the verb *after* the service is up, which normally closes it. If the
   store still cannot answer inside the verb's ten-second budget, the running service's own start-up check
   logs the exact command to fix it. Fresh installs are unaffected — there, the file *is* the control plane's
   future answer.
8. **`--configure-firewall`, `--enable-*` and `--disable-*` want an elevated shell.** The service account
   cannot create firewall rules by design. Run unelevated and the store toggle still succeeds, the rule does
   not change, and the exact elevated command is printed for you to run by hand.
9. **An off-box MCP client pointed at `localhost` fails quietly.** Use the box's LAN IP.
   ([4.4](#44-when-it-says-enabled-and-still-will-not-connect))
10. **Exposing the web dashboard on the LAN takes loopback's free pass away.** A loopback-only dashboard is
    tokenless; the moment a `web.network` block exposes it, the browser on the service host has to present the
    token too, because the Custom Views composer made the surface writable.
    ([3.5](#35-reach-it-from-a-browser-on-another-machine))
11. **`Warning` means three different things, and you have to look at the card to tell which.**
    `ClassifyBand` returns `Warning` for any of: a metric at warning severity (CPU, blocking, deadlocks,
    memory, threads — a real condition on the server), a server still awaiting its first collection, or a
    **stale collection** — the viewer re-uses its collector-errors flag to carry staleness. Only the first is
    an incident. On a fresh install the second is normal for every server at once, and at fleet scale the
    third is routine, so an opening screen of amber usually is not one — but do not generalise that into
    ignoring it. Hover the status word: since #2429 the tooltip names the reason, and it is built from the
    same metric rows shown underneath so it cannot disagree with them.
12. **The Recommendations tab is empty for the first day.** Analysis runs every 30 minutes per server but only
    once the store holds 24 hours of history for that server. A fresh install has not earned findings yet.

---

## Part 6 — honest gaps, so you do not file them twice

These are known, current, and not bugs:

- **No TLS on the MCP endpoint**, and none on the web endpoint unless you configure `web.network.tls`. The
  bearer token travels in clear on the segment in both cases otherwise; the named control is a
  TLS-terminating reverse proxy. Both are trusted-LAN opt-ins.
- **No client-side secret store for the viewer.** A remote seat's `darling.json` holds the store role password
  in cleartext; ACL it. Fine for the read-only `viewer` role, worth thinking about for `admin`.
- **The web dashboard has no plan analysis and no live-server actions** — that is the WPF viewer's job — and
  its page set is a deliberate subset of the viewer's.
- **`network` blocks need a service restart.** There is no hot reload for exposure. The service now says so
  at startup, naming the block it loaded, so a config edit that appears to do nothing is at least explained.
- **PostgreSQL monitored targets** collect and are readable on both UIs, but apart from the three Tier 0
  outage predictors they do not raise alerts or produce analysis findings yet, and there is **no execution
  plan capture at all** for them — which is the largest remaining gap, since plans are why people open a
  database monitoring tool. See [PostgreSQL Targets](../Darling/README.md#postgresql-targets).
- **The PostgreSQL tabs have no charts and no drill-downs.** Every panel on both surfaces is a table over the
  window; there is no PostgreSQL equivalent of the SQL Server Overview's correlated timeline lanes, no
  click-through from a blocking root to the sessions behind it, and no per-query history window.

If you hit something that is *not* on this list and *not* in
[Troubleshooting](../Darling/README.md#troubleshooting), that is the interesting kind — please report it with
the service log lines around the failure.

---

## Where to go next

| For | Read |
|---|---|
| Every config field, in detail | [Darling operator guide](../Darling/README.md) |
| Permissions on monitored servers | [Permissions](../Darling/README.md#permissions-on-monitored-servers) |
| The LAN endpoints in full | [Opt-in Network Endpoints](../Darling/README.md#opt-in-network-endpoints-lan) |
| The store, retention, TimescaleDB | [Operations](../Darling/README.md#operations) |
| What each collector gathers and when | [How collection works](how-collection-works.md) |
| Pointing Darling at a PostgreSQL target | [PostgreSQL first-target runbook](postgres-first-target-runbook.md) |
