# Install Darling

Audience: operators.

This page takes you from a downloaded zip to a service that collects data. It is for anyone who installs Darling on Windows. You need a Windows box, a SQL Server that you are allowed to monitor, and a Darling build. You do not need to have run Darling before.

The service fills one store. Three surfaces read that store, and each has its own page:

| Surface | What it is | Page |
|---|---|---|
| WPF viewer | The desktop app. It shows plans, blocking chains, deadlock graphs and live active queries | [The WPF viewer](viewer.md) |
| Web dashboard | A browser view of the fleet, served by the service itself. It is off by default | [The web dashboard](web-dashboard.md) |
| MCP | The endpoint that an LLM client talks to. It is off by default | [MCP server](../mcp-server.md#darling) |

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

- Windows on the service host. The bundled store, the DPAPI credential protection and the Windows-service lifetime are all Windows-only. A Linux path exists (see [Run on Linux](../darling-quick-start.md#run-on-linux-docker-compose-or-systemd-1804)), but it is a different procedure. The WPF viewer is Windows either way.
- No .NET runtime to install. The service and the viewer in the zip carry their own .NET runtime, so they are self-contained. A stock Windows Server needs nothing installed first. Runtime updates arrive with each Darling release.
- A monitored SQL Server and a login on it. The server can be SQL Server 2016 SP2 to 2025 (2017 needs CU3 or later), Azure SQL MI, AWS RDS or Azure SQL DB. The login needs `VIEW SERVER STATE` and the rest of the [monitoring grants](monitored-servers.md#permissions-on-monitored-servers).
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

The service runs as the unprivileged virtual account `NT SERVICE\PerformanceMonitor Darling`. That account is not you and not an administrator. A user profile grants access to nobody else. If you install there, the service installs cleanly and then cannot read its own program files. For this reason `install-darling.ps1` refuses a fresh install in those locations. The [full explanation is in the reference](#the-install-location-has-to-be-machine-scoped).

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

`"auth": "integrated"` connects as the account that the service runs under. The default virtual account reaches remote servers as the machine account `DOMAIN\<host>$`. For integrated auth against a remote server, you usually want to [run the service as a domain account or gMSA](#run-the-service-as-a-domain-account-or-gmsa) instead.

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

The blob is DPAPI at LocalMachine scope with a fixed entropy string. It decrypts only on the machine that produced it. Run `--encrypt-password` on the service host. If you ever move `darling.json` to a different box, run it again there. The same rule applies to the MCP and web tokens (see [the web dashboard](web-dashboard.md) and [MCP](../mcp-server.md#darling)). Do not copy a `darling.json` between machines and expect its secrets to survive.

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

To set up the LAN endpoints in the same pass, use `.\install-darling.ps1 -Network`. It runs the guided wizard at the end. You can also do this later (see [the web dashboard](web-dashboard.md) and [MCP](../mcp-server.md#darling)).

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

If the service idles instead, the message is literal and named: `Cannot load configuration`, `Configuration problem: <field>`, or `Cannot reach or migrate the Postgres store`. See [Troubleshooting](troubleshooting.md).

When the service collects, go to the page for the surface that you need: [the WPF viewer](viewer.md), [the web dashboard](web-dashboard.md) or [MCP](../mcp-server.md#darling).

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
9. An off-box MCP client that points at `localhost` fails quietly. Use the LAN IP of the box. (See [When it says enabled and still will not connect](../mcp-server.md#when-it-says-enabled-and-still-will-not-connect).)
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
- PostgreSQL monitored targets collect, and you can read them on both UIs. Apart from the three Tier 0 outage predictors, they do not raise alerts or produce analysis findings yet. There is no execution plan capture at all for them. That is the largest remaining gap, because plans are the reason people open a database monitoring tool. See [PostgreSQL Targets](postgresql-targets.md#postgresql-targets).
- The PostgreSQL tabs have no charts and no drill-downs. Every panel on both surfaces is a table over the window. There is no PostgreSQL equivalent of the correlated timeline lanes on the SQL Server Overview. You cannot click through from a blocking root to the sessions behind it. There is no history window for a single query.

If you hit a problem that this list and [Troubleshooting](troubleshooting.md) do not cover, please report it. Include the service log lines around the failure.

## Install reference

This part of the page is the detail behind the steps above. It moved here from the Darling operator guide. The Linux install is in [the Darling quick start](../darling-quick-start.md#run-on-linux-docker-compose-or-systemd).

### Prerequisites

- **A store host sized for your estate.** The [store sizing guide](../darling-store-sizing.md) gives the disk, RAM and vCPUs by server count, from the stores we measured.
- **Windows** for the viewer (WPF) and for a Windows-service host (Windows-service lifetime, DPAPI password protection). A Linux host also runs the service: see [Run on Linux](../darling-quick-start.md#run-on-linux-docker-compose-or-systemd-1804). Monitored servers can be SQL Server 2016 SP2–2025 (2017 needs CU3 or later), Azure SQL Managed Instance, AWS RDS for SQL Server, or Azure SQL Database.
- **A PostgreSQL store — bundled or your own.** In managed mode (the shipped default, see [Managed Bundled PostgreSQL](managed-postgresql.md#managed-bundled-postgresql)) the service runs its own bundled PostgreSQL 18 + TimescaleDB and no database provisioning is needed. To bring your own instead, the minimum is PostgreSQL 16, and the service is developed and validated against PostgreSQL 18. The store needs a database and a login the service can create tables in. If that store has TimescaleDB, size its background workers before you rely on compression. The stock PostgreSQL defaults cannot run the policies (see [Background workers](operations.md#background-workers-sizing-an-unmanaged-store-and-what-happens-if-you-dont)). On SSD-backed storage, also set `random_page_cost = 1.1` (the managed store does): with the default of 4 the planner prices a BRIN index's heap reads as random I/O and can pick a sequential scan over the index for windows of 12 hours or more. The BRIN index on the per-interval Query Store table is skipped below PostgreSQL 16, because HOT updates past a BRIN index need PostgreSQL 16 or newer.
- **TimescaleDB is optional and auto-adopted.** If the extension is installed (or pre-created by an administrator) in the store database, the service detects it at startup and automatically converts the collector tables to hypertables with compression; without it, the service runs in plain-PostgreSQL mode, which is fully supported. No configuration flag either way. The detection is also **re-run hourly for as long as it says no** (#3815), so a service that fell back because the store was restarting, failing over or momentarily unreachable at its own start returns to hypertable mode within the hour instead of at the next restart.
- **A Windows that is current on its updates.** .NET 10 apps need them to start on a CPU with CET (a hardware protection). Without them the service and the viewer both stop at start with exit code `0x80131506`. Install all available Windows updates and restart. If you cannot do that yet, [a short-term workaround](#windows-stops-at-start-the-cet-error) exists.
- **No .NET runtime to install on Windows.** The service and the bundled `viewer\` folder in the zip carry their own .NET runtime. They are self-contained. A stock Windows Server needs nothing installed first. Runtime updates arrive with each Darling release. The Linux tarball is different: see [Run on Linux](../darling-quick-start.md#run-on-linux-docker-compose-or-systemd-1804). You need the .NET 10 SDK only to build from source.

### Configure darling.json

The service reads one JSON file. It resolves the path in this order:

1. An explicit path (when a component is handed one)
2. The `DARLING_CONFIG` environment variable
3. `darling.json` next to the service binary

The Windows installer writes a minimal `darling.json` for you, and the service starts with no monitored servers (it logs one line saying so); add the first one in the Viewer (Add Server) or with `--add-server`. The server's login needs grants first: `--grant-script --host <host>` prints the T-SQL for them (the Add Server dialog has a Copy grant script button). To write the file yourself, copy the shipped `darling.sample.json` (it lands next to the built binary) to `darling.json` and edit; an empty `servers` list is valid. Comments and trailing commas are allowed; property names are case-insensitive.

Minimal working example with the bundled store, which is the shipped default: one server, integrated auth. In managed mode the service runs its own bundled PostgreSQL, so you provision no database. See [Managed Bundled PostgreSQL](managed-postgresql.md#managed-bundled-postgresql).

```json
{
  "postgres": { "managed": true },
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

To use a PostgreSQL that you already run, replace the `postgres` block with a connection string and leave `servers` as it is:

```json
{
  "postgres": {
    "connectionString": "Host=localhost;Port=5432;Username=darling;Database=darling"
  }
}
```

**Integrated auth (recommended).** The service connects to monitored servers as the Windows account the service runs under — there is no separate Windows credential to configure. Grant that account the [permissions on monitored servers](monitored-servers.md#permissions-on-monitored-servers). The default install's virtual service account reaches *remote* servers as the collector machine's computer account (`DOMAIN\<machine>$`), so for integrated auth you will usually [run the service as a domain account or gMSA](#run-the-service-as-a-domain-account-or-gmsa) instead.

**SQL auth.** Set `"auth": "sql"`, a `username`, and an `encryptedPassword` produced by the `--encrypt-password` verb:

```
PerformanceMonitor.Darling.Service.exe --encrypt-password
```

It prompts for the password on stdin (so the plaintext never lands in your shell history) and prints a base64 DPAPI blob. Paste that blob into the server's `"encryptedPassword"`. The blob is protected with **DPAPI LocalMachine scope**, so an administrator can encrypt it interactively and the service account can decrypt it later on the same machine — but it is machine-bound: run `--encrypt-password` **on the machine that will run the service**, and re-encrypt if you move `darling.json` to another machine. A plaintext `"password"` also works as a dev convenience, but the service logs a warning every time it is used. The same slot also takes an **`env:NAME` or `file:/path` reference** (#1804): the service reads the named environment variable or the file's (trimmed) contents at connect time, nothing secret lands in `darling.json`, and no warning is logged — the supported shape on non-Windows hosts, and compose-`secrets:`-friendly everywhere. A missing or empty reference target is a configuration error naming both the setting and the target, never a silent empty password.

**Service principal.** Set `"auth": "serviceprincipal"`, the Entra application (client) id in `username`, and the client secret in `encryptedPassword`. `username` is required. Make the blob with the same `--encrypt-password` verb as for SQL auth.

```json
{
  "name": "AZSQL1",
  "host": "myserver.database.windows.net",
  "auth": "serviceprincipal",
  "username": "<application (client) id>",
  "encryptedPassword": "<output of --encrypt-password>"
}
```

**Managed identity.** Set `"auth": "managedidentity"`. There is no secret to store. A system-assigned identity needs no other field. A user-assigned identity puts its client id in `username`. The machine that runs the service must itself be an Azure resource with that identity. See [authentication](../authentication.md).

```json
{
  "name": "AZSQL2",
  "host": "myserver.database.windows.net",
  "auth": "managedidentity"
}
```

**excludedDatabases** (per server) removes databases from collection: per-database collectors skip them and the exclusion is spliced into the collector queries — the same filter Lite applies. There is a second, separate `alerts.excludedDatabases` list that excludes databases from blocking/deadlock/long-running-query **alert evaluation** without affecting collection.

### Validate the Config (Pre-flight)

Before installing the service, check that `darling.json` is well-formed and that every monitored server is reachable with the configured credentials:

```
PerformanceMonitor.Darling.Service.exe --test-connection
```

(`--validate-config` is an alias.) It validates the file, then connects to and probes each server, printing a `[PASS]`/`[FAIL]` line per server (SQL major version, engine edition, and whether the account has msdb access for failed-job alerts). It exits `0` only when the file is valid **and** every server is reachable, so it doubles as a deployment gate.

**Which server list it probes.** Once the store is reachable, its own registry (`config_monitored_servers`) is authoritative — not `darling.json`'s `servers` array, which is only a first-bootstrap seed. A server that is in the file but was never registered, or was since removed from the registry, is not a probe failure: it prints as a `WARNING` and is skipped, because the store — not the file — governs which servers actually run. Only when the store itself cannot be reached does this fall back to validating `darling.json`'s own list, with a `NOTE` saying so.

A PostgreSQL target reports what matters there instead — version, writer or reader, Aurora or not, and **how many of the PostgreSQL collectors will actually run against it**, naming the ones that will not:

```
  [PASS] aurora-writer: PostgreSQL 17 (server_version_num 170007), writer, Aurora — all 29 PostgreSQL collectors apply
  [PASS] aurora-reader: PostgreSQL 17 (server_version_num 170007), reader (in recovery), Aurora — 25 of 29 PostgreSQL collectors apply (skipped: pg_autovacuum_stats, pg_index_bloat, pg_index_usage_stats, pg_table_bloat_stats)
  [PASS] selfhosted:    PostgreSQL 15 (server_version_num 150012), reader (in recovery), not Aurora — 22 of 29 PostgreSQL collectors apply (skipped: pg_autovacuum_stats, pg_cpu_utilization, pg_index_bloat, pg_index_usage_stats, pg_io_stats, pg_table_bloat_stats, pg_wait_stats)
```

That count comes from the same [engine and version gate](postgresql-targets.md#postgresql-targets) the collector runner uses, not a separate list, so it is the real answer rather than an estimate — and it is the answer at *pre-flight*, before an empty table has to be explained weeks later. Add an explicit config path as a second argument if `darling.json` is not next to the exe and `DARLING_CONFIG` is not set. This is the same probe the Viewer's **Test Connection** button runs through the service.

One identity caveat: the verb connects as **you**, the console user — not as the service account. For `"auth": "integrated"` servers a `[PASS]` proves the server is reachable and the config is well-formed, but the grants that matter at runtime are the *service account's*: the per-server connect lines in the service log are the real proof (see [Run the service as a domain account or gMSA](#run-the-service-as-a-domain-account-or-gmsa)).

### Check Host/Store Sizing Settings

To choose a host size before you install, see the [store sizing guide](../darling-store-sizing.md).

For a managed store, check that the sizing-relevant settings (`shared_buffers`, `effective_cache_size`, `maintenance_work_mem`, `work_mem`, `timescaledb.max_background_workers`, `max_worker_processes`, `max_connections`, `max_wal_size`) still match what this host's current RAM, hypertable count and free disk would derive today:

```
PerformanceMonitor.Darling.Service.exe --check-settings [--json]
```

It prints the host facts (OS, containerization, CPUs, RAM) and the store facts (versions, size, lifetime buffer hit ratio, temp bytes, and how many bytes of uncompressed TimescaleDB chunks sit against RAM). Then it prints one row per setting: its live value, where that value came from, what this host would derive for it right now, and a verdict. The four verdicts are `matches`; `stale-after-hardware-change` (a managed block set it once, but re-deriving from the CURRENT host gives a different number — the case a RAM upgrade or an added hypertable creates, and the reason this verb exists); `operator-override` (an `ALTER SYSTEM`, or a hand-edited line after the managed blocks, is what is actually in force); and `not-managed` (a bring-your-own store, where this class never wrote a block to compare against). `--json` prints the same facts as JSON for scripting. This is diagnostic only: it never writes a setting, and the service always starts regardless of what it finds.

Exit codes: see [`--check-settings` in the command line reference](cli.md#exit-codes).

Exit codes for `--enable-collector` and `--disable-collector`: see [the command line reference](cli.md#exit-codes).

Every other verb, with its exit codes, platform rules and whether it needs the service running, is in the [command line reference](cli.md).

### Diagnostics Bundle

This section moved to [The command line reference](cli.md#diagnostics-bundle).

### Run It — Console Mode

The same executable serves interactive debugging and service installation; the Windows-service lifetime is a no-op when run from a console.

```
Darling\PerformanceMonitor.Darling.Service\bin\Release\net10.0\PerformanceMonitor.Darling.Service.exe
```

From a release zip, run `PerformanceMonitor.Darling.Service.exe` from the extracted folder. After a [build from source](#build-from-source), run the path above. Watch the log output: you should see the config load (`Loaded configuration from ...`), the store migrate (`Postgres store ready (schema v<N>, <M> migration(s) applied)`, which gives the store's schema version and how many migrations this start applied), the TimescaleDB detection result, and per-server connects. Per-collector run lines with row counts are at `Debug` and so are not in that output by default — add `--Logging:LogLevel:PerformanceMonitor.Darling.Service=Debug` to the command above to watch a first sweep collector by collector, or read the outcomes back from `collection_log` and the Viewer's Collection Health tab, which record every run at any level. See [Logs](operations.md#logs).

### Install as a Windows Service

**Scripted (recommended):** the packaged zips ship `install-darling.ps1` beside the service exe. Extract the zip to its final location (e.g. `C:\Program Files\PerformanceMonitorDarling` — see [below](#the-install-location-has-to-be-machine-scoped) for why that is preferred over a folder made directly under `C:\`), then double-click `Install-Darling.cmd` in that folder (it asks for administrator rights and runs the script with the execution policy bypassed for that one run only, leaving the machine's policy alone), or run `.\install-darling.ps1` from a PowerShell there (if it is not elevated, the script offers to restart itself elevated). It refuses a folder ordinary local users can already write to before anything else runs ([#4043](https://github.com/erikdarlingdata/PerformanceMonitor/issues/4043)), checks the install location and refuses anywhere the service could not read itself (see [below](#the-install-location-has-to-be-machine-scoped)), checks for `darling.json` (on a first install it writes a minimal one, the bundled store and no servers, and carries on: the service starts with no monitored servers and you add the first one afterwards in the Viewer's Add Server, so nothing needs editing first; `darling.sample.json` stays beside it as the full reference; a first install is refused if `darling.json` is an unedited copy of that sample, because the copy still names its example servers), runs the `--test-connection` pre-flight, registers the Event Log source, creates the service under the virtual account (or upgrades an existing install's binPath in place, preserving config/store/credentials), locks the install folder so only SYSTEM, Administrators and the service account can change what runs from it ([#4034](https://github.com/erikdarlingdata/PerformanceMonitor/issues/4034): a folder made directly under `C:\` otherwise lets any local user replace the service's binaries; the lock goes on before anything runs from the folder, and `upgrade-darling.ps1` applies it before laying a new build down, so an older install is closed at its next upgrade. No lock can undo a file swapped before it, which is what the #4043 check above catches instead — extract into a fresh folder and run the script straight away, and re-extract fresh if the folder has sat open on a box with other local users), starts it, and creates **Darling Viewer** shortcuts on the public Desktop and in the all-users Start Menu, so they show for every user and not only for the account that ran the installer (pin to taskbar from the Start Menu entry — Windows does not allow programmatic pinning). `uninstall-darling.ps1` reverses it, deliberately leaving the store/config in place unless you pass `-PurgeData`.

**Add the people who run the Viewer or the CLI to the operator group.** The installer creates the local group `PerformanceMonitor Darling Operators`, empty, and restricts `darling.json` to SYSTEM, Administrators, the service account and that group. `upgrade-darling.ps1` creates the group too when an older install does not have it yet. The service applies the same restriction at its next start. At the end, `install-darling.ps1` asks whether to add the account you are signed in to (default No). `upgrade-darling.ps1` adds no one. Until a person is added, they cannot run the Viewer or the CLI without elevation. See [Security & Least-Privilege Roles](security.md#security--least-privilege-roles) for who to add, why, and the scripted options.

#### Installing with the MSI

A release that includes `PerformanceMonitorDarling-<version>.msi` lets you install without extracting the zip. The MSI carries the same service, viewer and scripts as the zip. It installs the `PerformanceMonitor Darling` Windows service and adds a Start menu and desktop shortcut for the viewer.

To install, double-click the file and follow the wizard, or run this from an elevated prompt:

```
msiexec /i PerformanceMonitorDarling-<version>.msi /qn OPERATORACCOUNT=CONTOSO\jdoe WEBDASHBOARD=1
```

These properties are available:

| Property | Effect |
|---|---|
| `INSTALLFOLDER` | The install folder. The default is `C:\Program Files\PerformanceMonitorDarling`. Setup stops and rolls back if any account other than SYSTEM, Administrators and TrustedInstaller can change files in the folder. A folder made directly under `C:\` is one of these, because it gets Modify for Authenticated Users from the drive. The MSI log (`msiexec /l*v`) names the accounts. Choose a folder under Program Files, or remove those permissions and run setup again. |
| `OPERATORACCOUNT` | A `DOMAIN\user` account to add to the `PerformanceMonitor Darling Operators` group. |
| `WEBDASHBOARD` | If you set `1`, the `darling.json` that setup writes has `web.enabled` on and a web token that setup makes for you. The token is stored encrypted in `web.network.encryptedToken`. The dashboard listens on this machine only and asks for that token. To show it, run `& "C:\Program Files\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe" --print-web-token` in an elevated PowerShell (use your install folder if you chose another). If `darling.json` already exists, setup ignores this property and leaves its web settings as they are. |
| `PURGEDATA` | If you set `1` on uninstall, the uninstall also deletes the store. |

The MSI and the scripts must not manage the same install. If `install-darling.ps1` already installed the service on this machine, the MSI stops and changes nothing. To switch, run `uninstall-darling.ps1` from the script install's folder, which keeps `darling.json` and the store. Then run the MSI again with the same `INSTALLFOLDER`. To stay on the scripts, keep using `upgrade-darling.ps1`. In the other direction, `install-darling.ps1`, `upgrade-darling.ps1` and `uninstall-darling.ps1` refuse to run over an MSI install. Uninstall an MSI install from Settings, Apps.

A newer MSI upgrades an MSI install in place. It reinstalls into the folder of the first install, and it keeps `darling.json` and the store. Uninstalling from Settings, Apps keeps `darling.json` and the store too, unless you set `PURGEDATA=1`.

The upgrade and rollback steps for an existing install are in [Running Darling day to day](admin.md#upgrading-an-existing-install).

#### The install location has to be machine-scoped

Extract to a local, machine-scoped path. **Use `C:\Program Files\PerformanceMonitorDarling`**, not a folder made directly under `C:\` (the older documented example, `C:\PerformanceMonitorDarling`, which `install-darling.ps1` now refuses unless you pass `-AcceptWritableExtraction`): `C:\Program Files` denies write to ordinary local users by default, while a fresh folder directly under `C:\` inherits `Authenticated Users: Modify` from the volume root the instant it is extracted, before `install-darling.ps1` has run a single line ([#4043](https://github.com/erikdarlingdata/PerformanceMonitor/issues/4043)). Either way, **not** anywhere under a user profile (`C:\Users\...`, including your Desktop or Downloads), and not a UNC path or a mapped drive.

The service runs as the unprivileged virtual account `NT SERVICE\PerformanceMonitor Darling`, never LocalSystem, because the bundled PostgreSQL refuses to run with administrative privileges. That account is not you, not SYSTEM, and not Administrators — and a user profile grants access to about those three and nobody else, so the service cannot read its own program files there. It installs cleanly and then fails: `initdb.exe` dies at `0xC0000135` (STATUS_DLL_NOT_FOUND) before it can report anything (#2185). A folder created under `C:\` inherits read + execute for `BUILTIN\Users` instead, which the virtual account is a member of, which is why that documented location still works for the *service* — the residual is the *write* grant the same inheritance carries for every other local user, which is what `C:\Program Files` avoids and what the next paragraph checks for. Network paths fail for a related reason: a virtual account [reaches the network as the computer account](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-windows-service-accounts-and-permissions#virtual-accounts) rather than as you, and a mapped drive letter belongs to your logon session, which a service does not share.

**`install-darling.ps1` also refuses to run at all from a folder ordinary local users can already write to** ([#4043](https://github.com/erikdarlingdata/PerformanceMonitor/issues/4043), checked before anything else, including the install-tree lock below (#4034)): the lock only stops writes from the moment it runs, so a folder that was writable by someone else *before* that — the usual case is a fresh extract directly under `C:\` — may already hold a swapped binary the lock can never undo. The refusal names the principals and points you at `C:\Program Files\...`. For a folder that already holds an install (the service is registered, or `darling.json` or `pg-runtime` is there), the refusal gives the numbered steps instead: stop the service, extract the new zip into a new, empty `C:\Program Files\PerformanceMonitorDarling`, copy `darling.json` into it first (plus any certificate or key it points at, and `darling-keys` when `postgres.managed` is `false`), run `install-darling.ps1` from the new folder (it re-points the existing service; the logon account, credentials and store are not touched), start the service and check it collects, then delete the old folder. `upgrade-darling.ps1` gives the same steps. The service's own account (`NT SERVICE\PerformanceMonitor Darling`) is never listed as a finding in that folder, whether or not the service is registered today. `-AcceptWritableExtraction` skips it for a dev loop that extracts somewhere deliberately writable; it is not for a real install.

`install-darling.ps1` refuses a fresh install in any of these locations rather than leaving you a service that cannot start. A service registered by hand instead — the manual `sc create` path below, which the installer never sees — gets the same diagnosis from the service itself: on start, ahead of reading `darling.json` and long before the store bootstrap, it logs one critical line naming the path, why its own account cannot read it, and where to move it, so the cause is above the failure rather than three messages downstream of it. To move an existing install, stop the service, move the folder, and re-run `install-darling.ps1` from the new location — it updates the service's binPath in place and leaves your `darling.json`, store data, and credentials alone.

**Manual:** publish (or copy the build output) to a stable path, put `darling.json` next to the exe (or set `DARLING_CONFIG` as a machine environment variable), then register it:

```
dotnet publish Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj -c Release -o "C:\Program Files\PerformanceMonitorDarling"
```

```
sc create "PerformanceMonitor Darling" binPath= "\"C:\Program Files\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe\"" start= auto obj= "NT SERVICE\PerformanceMonitor Darling"
```

```
sc start "PerformanceMonitor Darling"
```

Also register the service's Windows event source once, from the same elevated shell — event-source registration requires elevation, and the virtual service account cannot do it itself (without this, Event Log diagnostics are silently dropped; the file log under `%ProgramData%\PerformanceMonitorDarling\logs` works regardless):

```
powershell -NoProfile -Command "New-EventLog -LogName Application -Source 'PerformanceMonitor Darling' -ErrorAction SilentlyContinue"
```

The `obj=` clause runs the service under a **virtual service account** (`NT SERVICE\<service name>` — password-less, per-service SID, unprivileged; the same convention SQL Server itself uses). That is the right account for SQL-auth monitoring, and with `postgres.managed = true` it is more than a preference: PostgreSQL refuses to execute with administrative privileges, so don't run the service as LocalSystem — a least-privilege account keeps the bundled store's initdb/start path on ground PostgreSQL supports. For integrated auth to monitored servers, [run the service as a domain account or gMSA](#run-the-service-as-a-domain-account-or-gmsa) instead. Note the space after `binPath=`, `start=`, and `obj=` — `sc` requires it.

One managed-mode handoff gotcha: if you test-drove the service from a console first, the bundled store's data directory belongs to *your* account, and the service account may not be able to write it. Point the service at a fresh `postgres.dataDirectory` (or delete the test directory) rather than fighting ACLs.

#### Run the service as a domain account or gMSA

With `"auth": "integrated"`, the monitoring identity **is** the service's Log On account — nothing in `darling.json` names a Windows account, and there is no separate credential to set. The default virtual account carries only the *machine* identity onto the network (remote servers see `DOMAIN\<collector-machine>$`), so for integrated auth against remote servers you almost always want a real AD service account or, better, a gMSA. Switching is a Windows-side change plus a SQL-side grant, with one file-permission step in the middle that bites everyone who skips it:

1. **Change the Log On account.** Stop the service, then **Services.msc → PerformanceMonitor Darling → Log On → This account** — that route also grants the account the *Log on as a service* right automatically. Or from an elevated prompt (with `sc config` you grant *Log on as a service* yourself, via secpol.msc or GPO):

   ```
   sc config "PerformanceMonitor Darling" obj= "DOMAIN\svc-account" password= "ThePassword"
   ```

   A gMSA works the same way with an empty password: `obj= "DOMAIN\gmsa-name$" password= ""`. Keep the account **out of the local Administrators group**: with `postgres.managed = true` the bundled PostgreSQL refuses to run with administrative privileges, exactly as it refuses LocalSystem.

2. **Grant the account on every monitored server** — a Windows login holding the same [permissions on monitored servers](monitored-servers.md#permissions-on-monitored-servers) (the `GRANT`s there apply to a Windows login unchanged):

   ```sql
   USE [master];
   CREATE LOGIN [DOMAIN\svc-account] FROM WINDOWS;
   ```

3. **Re-grant the service's own files — the step people miss.** The service deliberately locks its files down to SYSTEM, Administrators, and the account it was *running as*; the new account is on none of those ACLs, and the service will fail to read its config or write its store. One-time, from an elevated prompt, before starting the service:

   ```
   icacls "C:\ProgramData\PerformanceMonitorDarling" /grant "DOMAIN\svc-account:(OI)(CI)F"
   icacls "C:\ProgramData\PerformanceMonitorDarling\logs" /grant "DOMAIN\svc-account:(OI)(CI)F"
   icacls "C:\Program Files\PerformanceMonitorDarling\darling.json" /grant "DOMAIN\svc-account:F"
   ```

   Adjust the last path to wherever `darling.json` sits beside the service exe. The first grant covers the store folder and, in managed mode, the data directory. The `logs` folder has its own protected ACL that does **not** inherit the folder grant, so it gets its own line. On its next start the service re-asserts the tight ACL itself — now including the new account — so this does not need repeating.

   In managed-store mode there is one more grant: the store's superuser credential `pg-credential.dpapi` (beside the data directory, under `C:\ProgramData\PerformanceMonitorDarling` by default) also has a protected ACL that does **not** inherit the folder grant, so grant the new account on the file directly:

   ```
   icacls "C:\ProgramData\PerformanceMonitorDarling\pg-credential.dpapi" /grant "DOMAIN\svc-account:F"
   ```

   **No ownership step is needed when the service was running as the default virtual account** (the `NT SERVICE\PerformanceMonitor Darling` account `install-darling.ps1` creates). The service and `install-darling.ps1` trust a store item owned by SYSTEM, Administrators, TrustedInstaller, the account the service runs as, or *this service's own service SID* (`NT SERVICE\PerformanceMonitor Darling`, `S-1-5-80-…`), and everything the virtual account created is owned by that SID. Ownership is only a step when the service has already run as *another* account (a second switch, from one domain account or gMSA to the next): that account then owns the items, and the service refuses them as a possible pre-plant. Hand ownership to Administrators, which is trusted across any later account change, from an elevated prompt with the service stopped:

   ```
   icacls "C:\ProgramData\PerformanceMonitorDarling" /setowner "*S-1-5-32-544" /T /C /L
   ```

   The sibling role credentials (the admin/viewer/mcp `.dpapi` files) self-heal — a role password can be re-asserted, a superuser's cannot — so expect one-time `discarding and regenerating` warnings on the first start, not faults.

4. **Start the service and verify from its log** (`%ProgramData%\PerformanceMonitorDarling\logs`): the per-server connect lines are the proof that the *service account's* grants work. `--test-connection` from your console runs as you, not the service account — see the [pre-flight note above](#validate-the-config-pre-flight).

Nothing else moves: anything encrypted with `--encrypt-password` (SQL-auth server passwords, SMTP) survives the account change, because those blobs are DPAPI **machine**-scope, not account-scope — and collected data is untouched. Later `install-darling.ps1` upgrades preserve a custom Log On account and harden `darling.json` for the account the service actually runs as.

### Build from source

You need the .NET 10 SDK to build from source. A release zip needs no SDK.

Build from the repository root:

```
dotnet build Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj -c Release
```

```
dotnet build Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj -c Release
```

### Windows stops at start: the CET error

.NET 10 apps need a Windows that is current on its updates to start on a CPU with CET (a hardware protection). We saw this on Windows Server 2022 build 20348.1366. Without the updates the service and the viewer both stop at start. The exit code is `0x80131506` and stderr shows this message:

```text
Fatal error. Your Windows doesn't fully support CET. Please install all available Windows updates.
```

`install-darling.ps1` recognises that message in its pre-flight and stops with a plain message instead of asking whether to install anyway. The fix is to install all available Windows updates and restart. If you cannot do that yet, there is a short-term workaround. Run these from an elevated PowerShell before you install or start. Each turns off one protection (`UserShadowStack`, the hardware-enforced stack protection) for one Darling exe:

```powershell
Set-ProcessMitigation -Name PerformanceMonitor.Darling.Service.exe -Disable UserShadowStack
Set-ProcessMitigation -Name PerformanceMonitor.Darling.Viewer.exe -Disable UserShadowStack
```

This weakens that program's protection, so treat it as a stopgap. To undo it once Windows is patched, remove the values the first command wrote:

```powershell
$ifeo = 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Image File Execution Options'
foreach ($exe in 'PerformanceMonitor.Darling.Service.exe', 'PerformanceMonitor.Darling.Viewer.exe') {
    Remove-ItemProperty -Path "$ifeo\$exe" -Name MitigationOptions, MitigationAuditOptions, EAFModules -ErrorAction SilentlyContinue
}
```

On Windows Server 2022 build 20348.5622, `Set-ProcessMitigation -Remove -Disable UserShadowStack` did not remove the setting (it returned `C000000D`). `Set-ProcessMitigation -Reset` failed too ("Invalid input parameters"). The registry way above worked. The `Set-ProcessMitigation -Disable` command writes three values (`MitigationOptions`, `MitigationAuditOptions`, `EAFModules`) under the exe's key in `Image File Execution Options`. The undo removes only those values and leaves any other value on that key alone. Restart the program afterwards. The first command is described in Microsoft's [Set-ProcessMitigation](https://learn.microsoft.com/en-us/powershell/module/processmitigations/set-processmitigation) reference.

## Where to go next

| For | Read |
|---|---|
| The desktop app | [The WPF viewer](viewer.md) |
| The browser view | [The web dashboard](web-dashboard.md) |
| An LLM client | [MCP server](../mcp-server.md#darling) |
| Every config field, in detail | [Darling operator guide](../../Darling/README.md) |
| Permissions on monitored servers | [Permissions](monitored-servers.md#permissions-on-monitored-servers) |
| The LAN endpoints in full | [Opt-in Network Endpoints](network-endpoints.md) |
| The store, retention, TimescaleDB | [Operations](operations.md) |
| What each collector gathers and when | [How collection works](../how-collection-works.md) |
| Pointing Darling at a PostgreSQL target | [PostgreSQL first-target runbook](../postgres-first-target-runbook.md) |
