# Performance Monitor Darling — Headless Edition

Darling is the headless, centralized edition of Performance Monitor: a 24/7 Windows service that collects from your SQL Servers into a central PostgreSQL (optionally TimescaleDB) store, plus a detached desktop viewer that reads that store. No desktop app has to stay open for collection to happen, and every viewer seat reads the same central data.

It runs the **same monitoring brain as the Lite edition** — one shared codebase, two storage engines:

- `PerformanceMonitor.Collectors` owns all 48 collector definitions — 41 for SQL Server and 7 for PostgreSQL: the exact query sent to monitored servers, the result-row mappings, the delta rules, the default cadences and retention horizons, and the ignored-wait-types list. Lite writes those rows to DuckDB; Darling writes the same rows to PostgreSQL via binary COPY. Each definition declares which engine it targets, and a collector never runs against the other one — see [PostgreSQL targets](#postgresql-targets).
- `PerformanceMonitor.Alerting` owns the shared alert engine — the same thresholds, edge-trigger gates, cooldowns, and dedup fingerprints Lite uses.
- The analysis/recommendations pipeline (the same inference engine behind both apps' Recommendations tabs and the `analyze_server` MCP tool) runs on a schedule inside the service.

A collector, alert, or analysis change lands once in the shared libraries and both editions get it. A Darling install monitoring a server even derives the **same `server_id`** Lite would for that server, because the identity rule (`host[:database][:RO]`, hashed) is shared too.

> **Status: in development.** Darling builds and runs from source (it is wired into the solution and CI), but is not yet packaged into the signed release artifacts. Expect the surface documented here to grow.

---

## When to Choose Darling vs. Lite

| | **Lite** | **Darling** |
|---|---|---|
| Collection runs | While the desktop app is open (or in the tray) | 24/7 as a Windows service |
| Data lives | Locally per seat (DuckDB + Parquet) | Centrally (PostgreSQL / TimescaleDB) |
| Execution plans | Not stored (fetched live when you view a query) | Captured and stored, TOAST-compressed (`capturePlans`, default on) |
| Viewers | The app is the viewer | Any number of viewer seats read the central store |
| Setup | Download and run | Provision PostgreSQL, edit `darling.json`, install the service |
| Best for | Quick triage, consultants, a handful of servers | Always-on team monitoring, larger estates, one shared store |
| Configuration | Settings UI | One JSON file (no UI) |

Nothing is installed on the monitored SQL Servers by either edition beyond two lightweight Extended Events ring-buffer sessions and, when it is unset, a one-time `blocked process threshold` bootstrap (see [What the Service Does on Monitored Servers](#what-the-service-does-on-monitored-servers)).

---

## Quick Start

> **First time, on a box with nothing on it?** [**`docs/uat-onboarding.md`**](../docs/uat-onboarding.md) is the ordered procedure from a downloaded zip to a running service and whichever of the three surfaces you need — the WPF viewer, the web dashboard, or MCP — with a proof point at every step. This section, and the rest of this document, is the reference it links back into.

### Prerequisites

- **Windows** for the service host (Windows-service lifetime, DPAPI password protection) and for the viewer (WPF). Monitored servers can be SQL Server 2016–2025, Azure SQL Managed Instance, AWS RDS for SQL Server, or Azure SQL Database.
- **A PostgreSQL store — bundled or your own.** In managed mode (the shipped default, see [Managed Bundled PostgreSQL](#managed-bundled-postgresql)) the service runs its own bundled PostgreSQL 18 + TimescaleDB and no database provisioning is needed. To bring your own instead, PostgreSQL 16 or newer is recommended (developed and validated against PostgreSQL 18) with a database and a login the service can create tables in — and if that store has TimescaleDB, size its background workers before you rely on compression, because the stock PostgreSQL defaults cannot run the policies (see [Background workers](#background-workers-sizing-an-unmanaged-store-and-what-happens-if-you-dont)).
- **TimescaleDB is optional and auto-adopted.** If the extension is installed (or pre-created by an administrator) in the store database, the service detects it at startup and automatically converts the collector tables to hypertables with compression; without it, the service runs in plain-PostgreSQL mode, which is fully supported. No configuration flag either way.
- **Two .NET 10 runtimes on the host**, from <https://dotnet.microsoft.com/download/dotnet/10.0>. Both
  shipped binaries are framework-dependent, and a stock Windows Server image has neither:
  - **ASP.NET Core Runtime 10** for the service. Required unconditionally — the MCP package brings the
    ASP.NET Core framework reference in transitively, so it is needed whether or not you ever enable MCP
    or the web dashboard.
  - **.NET Desktop Runtime 10** for the viewer (WPF). Not needed on a headless collector host, and not
    needed for a remote seat installed from the viewer's own `Setup.exe`, which is self-contained.

  `install-darling.ps1` checks both before it installs anything: it refuses when ASP.NET Core is missing
  and warns when the Desktop runtime is. The .NET 10 SDK covers both if you are building from source.

Build from the repository root:

```
dotnet build Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj -c Release
```

```
dotnet build Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj -c Release
```

### Configure darling.json

The service reads one JSON file. It resolves the path in this order:

1. An explicit path (when a component is handed one)
2. The `DARLING_CONFIG` environment variable
3. `darling.json` next to the service binary

Copy the shipped `darling.sample.json` (it lands next to the built binary) to `darling.json` and edit. Comments and trailing commas are allowed; property names are case-insensitive.

Minimal working example — one server, integrated auth, bring-your-own PostgreSQL. (With the bundled store instead, replace the `postgres` block with `"postgres": { "managed": true }` and skip provisioning entirely — see [Managed Bundled PostgreSQL](#managed-bundled-postgresql).)

```json
{
  "postgres": {
    "connectionString": "Host=localhost;Port=5432;Username=darling;Database=darling"
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

**Integrated auth (recommended).** The service connects to monitored servers as the Windows account the service runs under — there is no separate Windows credential to configure. Grant that account the [permissions below](#permissions-on-monitored-servers). The default install's virtual service account reaches *remote* servers as the collector machine's computer account (`DOMAIN\<machine>$`), so for integrated auth you will usually [run the service as a domain account or gMSA](#run-the-service-as-a-domain-account-or-gmsa) instead.

**SQL auth.** Set `"auth": "sql"`, a `username`, and an `encryptedPassword` produced by the `--encrypt-password` verb:

```
PerformanceMonitor.Darling.Service.exe --encrypt-password
```

It prompts for the password on stdin (so the plaintext never lands in your shell history) and prints a base64 DPAPI blob. Paste that blob into the server's `"encryptedPassword"`. The blob is protected with **DPAPI LocalMachine scope**, so an administrator can encrypt it interactively and the service account can decrypt it later on the same machine — but it is machine-bound: run `--encrypt-password` **on the machine that will run the service**, and re-encrypt if you move `darling.json` to another machine. A plaintext `"password"` also works as a dev convenience, but the service logs a warning every time it is used. The same slot also takes an **`env:NAME` or `file:/path` reference** (#1804): the service reads the named environment variable or the file's (trimmed) contents at connect time, nothing secret lands in `darling.json`, and no warning is logged — the supported shape on non-Windows hosts, and compose-`secrets:`-friendly everywhere. A missing or empty reference target is a configuration error naming both the setting and the target, never a silent empty password.

**excludedDatabases** (per server) removes databases from collection: per-database collectors skip them and the exclusion is spliced into the collector queries — the same filter Lite applies. There is a second, separate `alerts.excludedDatabases` list that excludes databases from blocking/deadlock/long-running-query **alert evaluation** without affecting collection.

### Validate the Config (Pre-flight)

Before installing the service, check that `darling.json` is well-formed and that every monitored server is reachable with the configured credentials:

```
PerformanceMonitor.Darling.Service.exe --test-connection
```

(`--validate-config` is an alias.) It validates the file, then connects to and probes each server, printing a `[PASS]`/`[FAIL]` line per server (SQL major version, engine edition, and whether the account has msdb access for failed-job alerts). It exits `0` only when the file is valid **and** every server is reachable, so it doubles as a deployment gate.

A PostgreSQL target reports what matters there instead — version, writer or reader, Aurora or not, and **how many of the PostgreSQL collectors will actually run against it**, naming the ones that will not:

```
  [PASS] aurora-writer: PostgreSQL 17 (server_version_num 170007), writer, Aurora — all 12 PostgreSQL collectors apply
  [PASS] aurora-reader: PostgreSQL 17 (server_version_num 170007), reader (in recovery), Aurora — 9 of 12 PostgreSQL collectors apply (skipped: pg_autovacuum_stats, pg_index_usage_stats, pg_table_bloat_stats)
  [PASS] selfhosted:    PostgreSQL 15 (server_version_num 150012), reader (in recovery), not Aurora — 6 of 12 PostgreSQL collectors apply (skipped: pg_autovacuum_stats, pg_index_usage_stats, pg_io_stats, pg_statement_stats, pg_table_bloat_stats, pg_wait_stats)
```

That count comes from the same [engine and version gate](#postgresql-targets) the collector runner uses, not a separate list, so it is the real answer rather than an estimate — and it is the answer at *pre-flight*, before an empty table has to be explained weeks later. Add an explicit config path as a second argument if `darling.json` is not next to the exe and `DARLING_CONFIG` is not set. This is the same probe the Viewer's **Test Connection** button runs through the service.

One identity caveat: the verb connects as **you**, the console user — not as the service account. For `"auth": "integrated"` servers a `[PASS]` proves the server is reachable and the config is well-formed, but the grants that matter at runtime are the *service account's*: the per-server connect lines in the service log are the real proof (see [Run the service as a domain account or gMSA](#run-the-service-as-a-domain-account-or-gmsa)).

### Run It — Console Mode

The same executable serves interactive debugging and service installation; the Windows-service lifetime is a no-op when run from a console.

```
Darling\PerformanceMonitor.Darling.Service\bin\Release\net10.0\PerformanceMonitor.Darling.Service.exe
```

Watch the log output: you should see the config load (`Loaded configuration from ...`), the store migrate (`Postgres store ready (schema v44, ...)` — the number is whatever the current migration count is), the TimescaleDB detection result, per-server connects, and then per-collector run lines with row counts.

### Run on Linux (Docker Compose or systemd) {#1804}

The service is cross-platform .NET; only the **bundled zero-admin store** and DPAPI are Windows-specific. On Linux you pair the service with the official TimescaleDB image (compose, the recommended shape) or point it at PostgreSQL you already run (systemd), keeping `postgres.managed = false` either way. The Viewer stays a Windows desktop app — Linux hosts read the **web dashboard**, which the container exposes.

**Compose (the whole stack as one deployment)** — everything lives in [`Darling/compose/`](compose/):

```bash
cd Darling/compose
cp darling.sample.json darling.json        # edit: servers, alerting, tokens
#   one secret per file — see secrets/README.md for the exact list
docker compose up -d
```

Web dashboard on `http://<host>:5153` behind its token, MCP (if enabled) on `:5152` behind its bearer token. The port mappings are the exposure boundary: the container-aware bind gate honors `web.network`/`mcp.network` under `managed = false` **inside a container only**, and the tokens are still mandatory. Three rules worth knowing before they bite:

- **Nothing secret goes in darling.json.** Every secret slot — the whole `postgres.connectionString`, server `password`s, `smtp.password`, the tokens — takes an `env:NAME` or `file:/run/secrets/<name>` reference. The compose file mounts each secret from `secrets/`.
- **Start with a fresh store volume per deployment.** The control plane is store-authoritative after the first seed, so a reused volume's enable toggles override darling.json — by design.
- **File permissions are yours on Linux.** The Windows build locks config/credentials down with ACLs; here the container boundary is the isolation, and the `secrets/` directory should be `chmod 700` with `600` files (the systemd shape should do the same for `darling.json` itself).

**systemd + bring-your-own PostgreSQL** — download `PerformanceMonitorDarling-linux-x64-*.tar.gz` from the release, extract to `/opt/darling`, point `DARLING_CONFIG` at your config (connection string to your own PostgreSQL 15+ with TimescaleDB; the service degrades gracefully without TimescaleDB), and run `dotnet PerformanceMonitor.Darling.Service.dll` under a unit like:

```ini
[Unit]
Description=PerformanceMonitor Darling
After=network-online.target

[Service]
ExecStart=/usr/bin/dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll
Environment=DARLING_CONFIG=/etc/darling/darling.json
User=darling
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Use the same `env:`/`file:` secret references (systemd `LoadCredential=` pairs naturally with `file:`), and note `Microsoft.Data.SqlClient` needs `libgssapi-krb5-2` installed (`apt-get install libgssapi-krb5-2`) — the container image carries it already.

### Install as a Windows Service

**Scripted (recommended):** the packaged zips ship `install-darling.ps1` beside the service exe. Extract the zip to its final location (e.g. `C:\PerformanceMonitorDarling`), then from an elevated PowerShell in that folder run `.\install-darling.ps1`. It checks the install location and refuses anywhere the service could not read itself (see [below](#the-install-location-has-to-be-machine-scoped)), checks for `darling.json` (copying the sample and stopping for you to edit it on first run), runs the `--test-connection` pre-flight, registers the Event Log source, creates the service under the virtual account (or upgrades an existing install's binPath in place, preserving config/store/credentials), starts it, and creates Desktop + Start Menu **Darling Viewer** shortcuts (pin to taskbar from the Start Menu entry — Windows does not allow programmatic pinning). `uninstall-darling.ps1` reverses it, deliberately leaving the store/config in place unless you pass `-PurgeData`.

#### Upgrading an existing install

`install-darling.ps1` registers a service; it does not lay a new build over an old one. That step — stop, back up, copy, start, verify — is `upgrade-darling.ps1`, shipped in the same zip.

> **`upgrade-darling.ps1` does not exist in 3.5.0 or earlier.** It was added after 3.5.0 was tagged, so a build up to and including 3.5.0 does not contain it and neither does its zip. If you are upgrading FROM one of those, see [upgrading from a build that predates the script](#upgrading-from-a-build-that-predates-the-script) below and use the manual procedure — the steps in this section describe a script you will not have.

Extract the new zip to a **staging** folder and run *its* copy:

```powershell
Expand-Archive PerformanceMonitorDarling-3.5.1.zip -DestinationPath C:\staging\3.5.1
C:\staging\3.5.1\upgrade-darling.ps1 -Source C:\staging\3.5.1
```

It resolves the install directory from the registered service, verifies the zip's SHA256 when you point it at one (`-Source ...\PerformanceMonitorDarling-3.5.1.zip`, checked against `-Sha256` or a `SHA256SUMS.txt` beside it), backs the install root's files up to `_rollback_manual_<stamp>`, prunes the backups past the newest `-KeepRollbacks` (3), lays the new build down, confirms `darling.json` is byte-identical, and starts the service. Re-running after a failure is safe and is the intended recovery: a backup taken in the last `-BackupWindowMinutes` (60) is reused rather than replaced, so a re-run cannot overwrite the good pre-upgrade copy with a copy of a half-upgraded tree.

**It never kills a process**, and it checks for them twice. Before stopping anything it names processes running out of the install tree that a service stop will *not* close — your own `psql.exe`, a shell sitting in the folder, a Darling Viewer you left open — and refuses, costing nothing but a re-run. After the service is down it checks again with no exclusions; anything still there is usually a postmaster that outlived the stop, and that is exactly what must not be killed (the bundled PostgreSQL lives under `pg-runtime` and killing it takes the store down). Give it a few seconds and re-run.

**A rollback backup holds the install root's *files*, not `viewer\`, `wwwroot\`, `runtimes\` or `pg-runtime\`** — that is what keeps one to ~120 MB instead of ~1 GB. It matters in one case: if a copy dies partway, those subdirectories can be left mixed old-and-new, and a full revert is the *previous version's zip re-extracted* followed by the backup's files over the top. The script says so at the point of failure.

It **refuses to run from the install directory itself** — the copy would overwrite the script PowerShell is reading — which is why the staging folder above is not optional.

**Files the new build no longer ships.** The copy is an *overlay*: it writes what the new build ships and deletes nothing else, so a dependency that went away, an assembly that changed name, or a whole `runtimes\<rid>\lib\<tfm>\` subtree stranded by a target-framework move stays in the tree forever — and those are directories .NET probes for assemblies ([#2529](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2529)). It has happened: the Lite package dropped 44 shipped files across twelve consecutive releases, 43 of them in one step. After every successful copy the script writes `darling-install-manifest.txt` into the install root recording the files that copy laid down, and the next upgrade diffs its own payload against it and **names** whatever an earlier build shipped and this one does not. Add `-RemoveStaleFiles` to delete them rather than only list them — off by default for now, so you can see the answer on your own boxes before anything acts on it.

Everything it can name provably came out of one of our own zips, because the manifest is DERIVED from the payload rather than maintained by hand: `darling.json`, its `.bak-*` copies, the DPAPI credential blobs, the `_rollback_manual_*` backups and `pg-runtime\` were never in a payload, so they are never in the manifest and can never be nominated. When it cannot tell — no manifest yet, one it cannot parse, a source it cannot read — it removes nothing and says so on its own line. Deleting `darling-install-manifest.txt` is safe: the next upgrade reports that it cannot tell, removes nothing, and writes a fresh one.

**Backups pile up, and nothing used to remove them.** A dogfood box was found carrying 46 of them, 5.48 GB, the oldest three weeks old, with the service naming every one on every start ([#2525](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2525)). Retention above fixes new deploys; boxes that already have a backlog clear it with the installed copy, which needs no staging folder and does not stop the service:

```powershell
C:\PerformanceMonitorDarling\upgrade-darling.ps1 -ListRollbacks   # show what would go
C:\PerformanceMonitorDarling\upgrade-darling.ps1 -PruneOnly       # remove all but the newest 3
```

The service reports the set once per start with a count, a total and that command — informational while you are within retention, a warning past it. It never deletes one itself: it did not create them.

**The install is not verified when the service reaches Running.** That means the process started, not that it collects. The script prints the post-start checklist; work it about 10–15 minutes later against the store, and hold the upgrade unverified until every line passes.

#### Upgrading from a build that predates the script

`upgrade-darling.ps1` was added after 3.5.0, so upgrading **from 3.5.0 or earlier** is a manual
procedure. It is the same sequence the script automates ([#2593](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2593)).

**Nothing you need to preserve lives in the install directory.** The managed store, the DPAPI
credential blobs and the logs are all under `C:\ProgramData\PerformanceMonitorDarling`, and anything
encrypted with `--encrypt-password` survives because those blobs are DPAPI **machine** scope rather than
account scope. The only file that has to travel is `darling.json`. That also means renaming the install
folder does **not** back up your data — if you want a data rollback point, snapshot the volume or stop
the service and copy that ProgramData folder.

1. **Verify the download first.** `Get-FileHash <zip> -Algorithm SHA256` against `SHA256SUMS.txt` from the
   same release page. This is the one step with no recovery if it is skipped.
2. `Stop-Service 'PerformanceMonitor Darling' -Force`, then **poll until `Status` is actually `Stopped`**.
   Requesting a stop is not the same as it having stopped, and laying a build over a live tree is where
   this goes wrong.
3. Rename the current install folder aside (e.g. `...\PerformanceMonitorDarling_3.3`). This is your
   rollback.
4. Extract the new zip to the **original** folder name.
5. Copy `darling.json` from the renamed folder into the new one, then **hash it on both sides and confirm
   they match**. A config silently altered mid-upgrade is very hard to notice afterwards.
6. Run `.\install-darling.ps1` from the new folder. For an existing install it re-points the service's
   binPath in place and preserves config, store and credentials.
7. Start the service, then **verify** — see the post-start checklist below. Reaching `Running` means the
   process started, not that it collects, and an upgrade that crosses several schema migrations has more
   than usual to get wrong on first start.

A clean extract like this has one advantage over the scripted overlay: files a newer build stopped
shipping cannot accumulate, which is the problem `-RemoveStaleFiles` exists to clean up on an
overlay-upgraded tree.

#### The install location has to be machine-scoped

Extract to a local, machine-scoped path — `C:\PerformanceMonitorDarling` is the documented one. **Not** anywhere under a user profile (`C:\Users\...`, including your Desktop or Downloads), and not a UNC path or a mapped drive.

The service runs as the unprivileged virtual account `NT SERVICE\PerformanceMonitor Darling`, never LocalSystem, because the bundled PostgreSQL refuses to run with administrative privileges. That account is not you, not SYSTEM, and not Administrators — and a user profile grants access to about those three and nobody else, so the service cannot read its own program files there. It installs cleanly and then fails: `initdb.exe` dies at `0xC0000135` (STATUS_DLL_NOT_FOUND) before it can report anything (#2185). A folder created under `C:\` inherits read + execute for `BUILTIN\Users` instead, which the virtual account is a member of, which is why the documented location works. Network paths fail for a related reason: a virtual account [reaches the network as the computer account](https://learn.microsoft.com/en-us/sql/database-engine/configure-windows/configure-windows-service-accounts-and-permissions#virtual-accounts) rather than as you, and a mapped drive letter belongs to your logon session, which a service does not share.

`install-darling.ps1` refuses a fresh install in any of these locations rather than leaving you a service that cannot start. A service registered by hand instead — the manual `sc create` path below, which the installer never sees — gets the same diagnosis from the service itself: on start, ahead of reading `darling.json` and long before the store bootstrap, it logs one critical line naming the path, why its own account cannot read it, and where to move it, so the cause is above the failure rather than three messages downstream of it. To move an existing install, stop the service, move the folder, and re-run `install-darling.ps1` from the new location — it updates the service's binPath in place and leaves your `darling.json`, store data, and credentials alone.

**Manual:** publish (or copy the build output) to a stable path, put `darling.json` next to the exe (or set `DARLING_CONFIG` as a machine environment variable), then register it:

```
dotnet publish Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj -c Release -o C:\PerformanceMonitorDarling
```

```
sc create "PerformanceMonitor Darling" binPath= "C:\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe" start= auto obj= "NT SERVICE\PerformanceMonitor Darling"
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

2. **Grant the account on every monitored server** — a Windows login holding the same [permissions below](#permissions-on-monitored-servers) (the `GRANT`s there apply to a Windows login unchanged):

   ```sql
   USE [master];
   CREATE LOGIN [DOMAIN\svc-account] FROM WINDOWS;
   ```

3. **Re-grant the service's own files — the step people miss.** The service deliberately locks its files down to SYSTEM, Administrators, and the account it was *running as*; the new account is on none of those ACLs, and the service will fail to read its config or write its store. One-time, from an elevated prompt, before starting the service:

   ```
   icacls "C:\ProgramData\PerformanceMonitorDarling" /grant "DOMAIN\svc-account:(OI)(CI)F"
   icacls "C:\PerformanceMonitorDarling\darling.json" /grant "DOMAIN\svc-account:F"
   ```

   Adjust the second path to wherever `darling.json` sits beside the service exe; the first covers the logs and, in managed mode, the store's data directory. On its next start the service re-asserts the tight ACL itself — now including the new account — so this does not need repeating.

   In managed-store mode there is one more, and it needs **ownership**, not a grant: the store's superuser credential `pg-credential.dpapi` (beside the data directory, under `C:\ProgramData\PerformanceMonitorDarling` by default) is trusted only when *owned* by SYSTEM, Administrators, or the service account — an anti-pre-plant check — and `icacls /grant` changes permissions, never ownership, so after the switch the file is still owned by the *previous* service account and the service refuses it. Hand ownership to Administrators (trusted across any future account change, which is why not the new account itself) and grant the new account on the file directly — its ACL is protected and does **not** inherit the folder grant above:

   ```
   takeown /f "C:\ProgramData\PerformanceMonitorDarling\pg-credential.dpapi" /a
   icacls "C:\ProgramData\PerformanceMonitorDarling\pg-credential.dpapi" /grant "DOMAIN\svc-account:F"
   ```

   The sibling role credentials (the admin/viewer/mcp `.dpapi` files) hit the same ownership check but self-heal — a role password can be re-asserted, a superuser's cannot — so expect one-time `discarding and regenerating` warnings on the first start, not faults.

4. **Start the service and verify from its log** (`%ProgramData%\PerformanceMonitorDarling\logs`): the per-server connect lines are the proof that the *service account's* grants work. `--test-connection` from your console runs as you, not the service account — see the [pre-flight note above](#validate-the-config-pre-flight).

Nothing else moves: anything encrypted with `--encrypt-password` (SQL-auth server passwords, SMTP) survives the account change, because those blobs are DPAPI **machine**-scope, not account-scope — and collected data is untouched. Later `install-darling.ps1` upgrades preserve a custom Log On account and harden `darling.json` for the account the service actually runs as.

### What the Service Does on Monitored Servers

On each successful connect, the service:

1. **Probes the server** — one query against `sys.dm_os_sys_info` / `SERVERPROPERTY()` for version, engine edition (box / Managed Instance / Azure SQL DB), AWS RDS detection, and msdb access. It is the same detection query Lite runs, so both editions classify a server identically.
2. **Ensures two Extended Events ring-buffer sessions** (created if missing, started if stopped; ~4 MB ring buffer each, no files written on the server):
   - `PerformanceMonitor_Deadlock` — `xml_deadlock_report`, server-scoped on on-prem/Managed Instance/RDS; `database_xml_deadlock_report`, database-scoped on Azure SQL Database.
   - `PerformanceMonitor_BlockedProcess` — `blocked_process_report`, server-scoped (database-scoped on Azure SQL Database).
3. **Bootstraps the blocked-process threshold** — if `blocked process threshold (s)` is `0`, the service sets it to `5` via `sp_configure`. On AWS RDS `sp_configure` is unavailable; the attempt is tolerated and logged, and you set the threshold through an RDS Parameter Group instead (Azure SQL Database has a fixed 20-second threshold).
4. **Runs the on-connect config snapshots once** (`server_config`, `database_config`, `database_scoped_config`, `trace_flags`, `server_properties`), then runs all scheduled collectors on the shared default cadences.

Every failure in steps 2–3 is tolerated and logged: the deadlock/blocked-process collectors simply read zero rows until the sessions exist (and blocked-process reports only start arriving once the threshold is set). Monitoring queries connect with a 15-second connect budget and an application name of `PerformanceMonitorDarling`; connection encryption fails closed to `Mandatory` when the configured mode is unrecognized.

### Permissions on Monitored Servers

Darling needs the **same target-server grants as Lite**, so the copy-paste block lives in one place for both: **[Permissions in the root README](../README.md#lite--darling-on-premises)** — `VIEW SERVER STATE`, `CONNECT ANY DATABASE`, `VIEW ANY DEFINITION`, `ALTER ANY EVENT SESSION`, and the optional `ALTER TRACE`, `ALTER SETTINGS`, and msdb job-table grants, verified live against SQL Server 2025 with a scratch login carrying exactly them ([#1823](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1823)). That block is authoritative; this section is the Darling-specific reading of it. Keeping one list instead of two is deliberate — a second copy is how the old one went stale.

**The one Darling-specific line:** for `"auth": "integrated"` the grants go to the Windows account **the service runs as**, so use `CREATE LOGIN [DOMAIN\svc-account] FROM WINDOWS;` in place of the block's `CREATE LOGIN ... WITH PASSWORD`. Everything after it is unchanged. See [Run the service as a domain account or gMSA](#run-the-service-as-a-domain-account-or-gmsa) for which account that actually is — it is not the one you ran `--test-connection` as.

What each grant buys you, and what breaks without it:

| Grant | Why | If missing |
|---|---|---|
| `VIEW SERVER STATE` | All DMV collectors (wait stats, query stats, memory, CPU, file I/O, sessions, etc.) and the connect probe | Collection fails — this one is required |
| `ALTER ANY EVENT SESSION` | Create/start the two XE sessions | Logged; deadlock and blocked-process collectors read zero rows (an admin can pre-create the sessions instead) |
| `CONNECT ANY DATABASE` | The per-database collectors (`database_scoped_config`, `query_store_health`, `index_object_stats`, `database_size_stats`, `query_store_stats`) enter each database via `EXECUTE [db].sys.sp_executesql` | Databases the login cannot enter are skipped; without the grant that is every user database |
| `VIEW ANY DEFINITION` | Catalog-view row visibility everywhere: `sys.tables` / `sys.indexes` / `sys.objects` for the index and object collectors, `sys.dm_db_partition_stats`, and the AG catalog views (`sys.availability_groups`, `sys.availability_replicas`) | **Silently zero rows** — catalog views hide rows rather than erroring, so missing objects look exactly like empty databases, and a real AG cluster looks identical to a server with no AGs |
| `ALTER SETTINGS` | The `sp_configure` blocked-process-threshold bootstrap | Logged; set the threshold yourself (or via RDS Parameter Group) |
| `ALTER TRACE` | The `default_trace_events` collector — `sys.traces` / `fn_trace_gettable` accept nothing less | `PERMISSIONS` skip in collection health; the default-trace tab stays empty |
| msdb job-table `SELECT`s + `agent_datetime` `EXECUTE` | `running_jobs` / `job_history` / `agent_status` collectors and the failed/long-running-job alerts — all direct table reads; `SQLAgentReaderRole` alone leaves every one failing with error 229 | Skipped gracefully — logged as a permissions skip, alerts return no jobs |
| `DBCC TRACESTATUS` permission | `trace_flags` snapshot | Degrades to zero rows with a warning |

The msdb grants live inside a system database SQL Server setup can rewrite — re-check them after a CU or version upgrade.

**Azure SQL Database:** connect to the one database you monitor (set the server entry's `"database"`), using a contained user with `VIEW DATABASE STATE` and `VIEW DEFINITION`, matching the product's existing Azure guidance. The XE sessions are created database-scoped there (`ALTER ANY DATABASE EVENT SESSION`); SQL Agent collectors are skipped automatically.

Collectors that hit a permission error (SQL errors 229/297/300, plus 8189 from `sys.traces`) log a `PERMISSIONS` row in `collection_log` and retry on their next scheduled run — one denied collector never stops the rest.

#### Which collectors run on which platform

Every collector declares its own applicability in code (`AppliesTo(CollectorTargetInfo)`), so this is not a hand-maintained list of 36 rows — the collectors fall into five groups, and a collector outside its supported platform is **skipped before it runs**, not failed and logged every cycle.

| Runs on | Collectors | Gate |
|---|---|---|
| Everything | wait stats, CPU utilization, memory (stats/clerks/grants), file I/O, tempdb, latches, spinlocks, plan cache, session summary, plus blocking, deadlocks, blocked-process reports, DMV blocking snapshots, perfmon, query snapshots, procedure stats, index/object stats, long-query completions, database config/scoped-config/size, server properties, session stats, waiting tasks | no gate |
| On-prem, Managed Instance, RDS — **not** Azure SQL DB | CPU scheduler stats, default trace events, memory pressure events, server config, system health events, trace flags | `!IsAzureSqlDb` |
| On-prem and Managed Instance, needs msdb | job history | `!IsAzureSqlDb && HasMsdbAccess` |
| On-prem and Managed Instance, needs msdb — **not** RDS | agent status, running jobs | `!IsAzureSqlDb && !IsAwsRds && HasMsdbAccess` |
| SQL Server 2016+ (or any Azure flavour) | query stats, Query Store stats | `SqlMajorVersion >= 13 \|\| IsAzureSqlDb \|\| IsAzureManagedInstance` |

Notes:

- **Azure SQL DB** is the most restricted target: the six `!IsAzureSqlDb` collectors read server-scoped DMVs or on-disk artifacts that do not exist there, and the SQL Agent collectors have no Agent to read. Nothing about that is a permission problem, so it is not reported as one.
- **AWS RDS** blocks direct `msdb` job reads specifically; the rest of the SQL Agent surface is unaffected.
- **`HasMsdbAccess`** is probed per server at connect and is exactly `HAS_DBACCESS('msdb')` — *any* access to msdb, not a specific role or table grant. Losing msdb access later moves those collectors from running to skipped without an error storm. A login that can enter msdb but lacks `SELECT` on the job tables passes this probe and is caught one layer down as a `PERMISSIONS` skip instead.
- An unknown version (`SqlMajorVersion == 0`, i.e. detection has not completed yet) is treated as capable rather than skipped, so a collector is never silently dropped because a probe was slow.

If a tab or column is empty and you expect data, check **Collection Health**: a collector skipped for platform reasons shows no runs at all, whereas one denied by permissions logs `PERMISSIONS` and is classified `NO_PERMISSIONS`. Those are different problems with different fixes — the first is expected on that platform, the second is a grant to add from the table above.

---

## Configuration Reference

All sections except `postgres` and `servers` are optional — omit a section (or any key) to get the defaults listed here. Defaults deliberately mirror a fresh Lite install.

### postgres

Two mutually exclusive modes — setting both `managed: true` and `connectionString` is a validation error:

| Key | Default | Notes |
|---|---|---|
| `managed` | `false` | `true` runs the bundled PostgreSQL + TimescaleDB (Windows only; see [Managed Bundled PostgreSQL](#managed-bundled-postgresql)). The connection string is derived, never configured. |
| `port` | `5641` | Managed mode only: the loopback port the bundled server listens on. Deliberately uncommon so it coexists with any PostgreSQL (5432) already on the machine. |
| `dataDirectory` | *(null)* | Managed mode only: the cluster's data directory. `null` means `%ProgramData%\PerformanceMonitorDarling\pg`. |
| `connectAs` | `"admin"` | Managed mode only: which least-privilege role the Viewer connects as — `"admin"` (reads everything + manages mute rules and dismisses alerts) or `"viewer"` (read-only; those write actions are hidden/disabled). See [Security & Least-Privilege Roles](#security--least-privilege-roles). Ignored in bring-your-own mode (the connection string picks the role). |
| `connectionString` | *(required unless managed)* | Npgsql connection string for a store you provision yourself, e.g. `Host=localhost;Port=5432;Username=darling;Password=...;Database=darling`. You own that cluster's settings: if it has TimescaleDB, size its [background workers](#background-workers-sizing-an-unmanaged-store-and-what-happens-if-you-dont) — managed mode does this for you, this mode does not. |

### servers (array, at least one entry)

| Key | Default | Notes |
|---|---|---|
| `name` | `""` | Display name; falls back to `host` |
| `host` | *(required)* | Server/instance to monitor |
| `engine` | `"sqlserver"` | `"sqlserver"` or `"postgres"` (`postgresql` / `pg` / `aurora-postgresql` also accepted). Configuration rather than something probed, because it decides which driver builds the connection string before there is a connection to ask. An omitted or unrecognized value means SQL Server, so every existing `darling.json` keeps its exact present behaviour — see [PostgreSQL targets](#postgresql-targets) |
| `port` | *(driver default)* | PostgreSQL targets on a non-default port. SQL Server carries its port in the host as `host,1433` instead, and that convention is left alone |
| `database` | *(none)* | Azure SQL Database only: the one database this entry monitors (also part of the server's storage identity). PostgreSQL targets connect to the maintenance database and read cluster-wide catalogs |
| `auth` | `"integrated"` | `"integrated"` or `"sql"` |
| `username` | *(none)* | Required for `"sql"` |
| `encryptedPassword` | *(none)* | DPAPI blob from `--encrypt-password` (preferred) |
| `password` | *(none)* | A literal (dev only, warned on every use) or an `env:NAME` / `file:/path` reference (#1804) — references are the supported non-Windows shape and are not warned |
| `readOnlyIntent` | `false` | Route to a readable AG secondary (`ApplicationIntent=ReadOnly`) |
| `trustServerCertificate` | `false` | |
| `encryptMode` | `"Mandatory"` | `Mandatory` / `Strict` / `Optional`; unknown values fail closed to `Mandatory` |
| `multiSubnetFailover` | `false` | |
| `excludedDatabases` | `[]` | Databases excluded from collection |

### capturePlans (boolean, optional)

| Key | Default | Notes |
|---|---|---|
| `capturePlans` | `true` | Capture execution plans into `query_stats.query_plan_xml` and `query_store_stats.query_plan_text`. PostgreSQL TOAST compresses the plan text transparently (LZ4 on the managed store) and TimescaleDB chunk compression squeezes it further, so plans are cheap to keep — unlike Lite, which stores to DuckDB/Parquet and deliberately never captures them. Set `false` to skip plan capture (e.g. to shave storage across a very large fleet). |

### collectSchemaChangeEvents (boolean, optional)

| Key | Default | Notes |
|---|---|---|
| `collectSchemaChangeEvents` | `true` | Record `Object:Created` / `Object:Altered` / `Object:Deleted` schema-change (DDL) events in the built-in default-trace collector. Set `false` on a noisy or benchmark box where a create/drop-happy workload floods the viewer's **System Events > Default Trace** tab — e.g. HammerDB's TPC-H Query 15 creates and drops a `revenue` view thousands of times, and the collector faithfully records every create/delete. Only the Object DDL slice is suppressed; file auto-grow/shrink, ErrorLog, and security-audit events are still collected. The shared collector's equivalent of the full Dashboard's `@include_object_events`. A file-only knob (not stored in the control plane): edit and restart. |

### alerts

The shared alert engine's switches and thresholds. Every default mirrors Lite's alert defaults exactly, so an empty section alerts like a fresh Lite install. `enabled: false` turns off all alert evaluation **and** scheduled-analysis finding notifications (the analysis itself still runs and persists findings).

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Master switch for alert evaluation + finding notifications |
| `cpuEnabled` | `true` | |
| `cpuThresholdPercent` | `80` | |
| `cpuMode` | `"total"` | `"total"` = SQL + other processes; `"sql"` = SQL process only |
| `blockingEnabled` | `true` | |
| `blockingCountThreshold` | `1` | Blocked-process count (rolling window) that trips the alert |
| `blockingWaitSecondsThreshold` | `0` | Total blocked wait, in seconds, summed across the latest blocking snapshot; `0` = off. A second gate beside the count one, because a count cannot tell one session blocked for an hour from one blocked for a second. Reports as its own "Blocking Wait Time" alert, and unlike the count gate it is level-triggered: it re-fires every cooldown while the wait stays above the threshold and clears when it drops below |
| `deadlockEnabled` | `true` | |
| `deadlockCountThreshold` | `1` | Deadlock count (rolling window) that trips the alert |
| `poisonWaitEnabled` | `true` | THREADPOOL / RESOURCE_SEMAPHORE / RESOURCE_SEMAPHORE_QUERY_COMPILE |
| `poisonWaitThresholdMs` | `500` | Average ms per wait |
| `longRunningQueryEnabled` | `true` | |
| `longRunningQueryThresholdMinutes` | `30` | |
| `tempDbSpaceEnabled` | `true` | |
| `tempDbSpaceThresholdPercent` | `80` | |
| `lowDiskEnabled` | `true` | Volume free space; graded CRITICAL when critically low |
| `lowDiskThresholdPercent` | `10` | Fire below X% free; `0` disables this dimension (clamped 0–100) |
| `lowDiskThresholdGb` | `5` | Fire below X GB free; `0` disables this dimension |
| `longRunningJobEnabled` | `true` | SQL Agent job running long vs. its history |
| `longRunningJobMultiplier` | `3` | Fires at 3x the job's historical average |
| `failedJobEnabled` | `true` | Live msdb check for recently failed jobs |
| `failedJobLookbackMinutes` | `60` | Clamped 1–1440 |
| `cooldownMinutes` | `5` | Minimum minutes between repeats of the same alert condition (clamped 1–120) |
| `excludedDatabases` | `[]` | Excluded from blocking/deadlock/long-running-query **alert evaluation** (collection unaffected) |

Not configurable (hardcoded to Lite's defaults until someone needs a knob): the long-running-query read shape (top 5 results; the five noise filters — sp_server_diagnostics, WAITFOR, backups, misc waits, CDC — all on) and the analysis-finding notification policy (notify at severity >= 1.5, 6-hour per-finding cooldown).

### smtp

Email delivery is enabled when `host`, `from`, and `to` are all set — there is no separate enable flag.

| Key | Default | Notes |
|---|---|---|
| `host` | `""` | |
| `port` | `587` | |
| `useSsl` | `true` | |
| `username` | *(none)* | For authenticated relays |
| `encryptedPassword` | *(none)* | Same `--encrypt-password` DPAPI pattern as SQL auth |
| `password` | *(none)* | A literal or an `env:NAME` / `file:/path` reference (#1804) — the non-Windows email path |
| `from` | `""` | |
| `to` | `""` | Comma-separated recipients |
| `emailCooldownMinutes` | `15` | Email/webhook channel cooldown (clamped 1–120) |

### webhooks

A channel is enabled by a non-empty URL.

| Key | Default | Notes |
|---|---|---|
| `teamsUrl` | `""` | Teams incoming webhook |
| `teamsProxy` | `""` | Optional proxy address |
| `slackUrl` | `""` | Slack incoming webhook |
| `slackProxy` | `""` | Optional proxy address |

### mcp

The embedded MCP server, over Streamable HTTP bound to `localhost` by default (see [Opt-in Network Endpoints (LAN)](#opt-in-network-endpoints-lan) to reach it — and the store — from the LAN). It exposes the same tool names Lite and the Dashboard expose, plus small Darling-only WRITE surfaces — Custom Views management, alert tuning, and server onboarding (see the last three bullets):

- **Six diagnostic-analysis tools** — `analyze_server`, `get_analysis_facts`, `compare_analysis`, `audit_config`, `get_analysis_findings`, `mute_analysis_finding`.
- **Five plan-analysis tools** — `analyze_query_plan` (by `query_hash`), `analyze_procedure_plan` (by `sql_handle`), `analyze_query_store_plan` (by `database_name` + `query_id`), `analyze_plan_xml` (raw showplan XML, no fetch), and `get_plan_xml` (raw stored plan XML by `query_hash`). These run the shared execution-plan analyzer over the plan XML the collectors already captured into the store — a stored-plan read, never a live query against the monitored server. `analyze_query_plan`/`get_plan_xml` accept an optional `database_name`, and `analyze_query_store_plan` an optional `plan_id`, to pin the exact stored plan when the caller knows it.
- **Fifteen core data-read tools** — the diagnostic reads an assistant needs to investigate a server, each a stored read of the collected data (never a live query against the monitored server):
  - *Resource metrics* — `get_cpu_utilization`, `get_wait_stats`, `get_wait_trend`, `get_wait_types` (the distinct observed wait types, to pick one for `get_wait_trend`), `get_memory_stats`, `get_memory_clerks`, `get_file_io_stats`, `get_tempdb_trend`, `get_perfmon_stats`.
  - *Query performance* — `get_top_queries_by_cpu`, `get_top_procedures_by_cpu`, `get_query_store_top` (these hand back the `query_hash` / `sql_handle` / `query_id` + `plan_id` keys the plan-analysis tools consume).
  - *Discovery / health* — `list_servers` (with collection-freshness status, and the [declared peer stores](#peers) when the fleet is split across several Darling boxes), `get_collection_health`, `get_server_properties`.

  These are the tools the analysis findings' `next_tools` recommendations point at, so a client following a finding's advice resolves them on this same server. Result shapes match Lite's (the store is Lite's collector schema); where Lite and the Dashboard's shapes diverge, Darling follows Lite — the shape its collector-mirror store can serve faithfully.
- **Twenty diagnostic-depth data-read tools** — deeper reads for a blocking / deadlock / session / configuration / storage investigation, each a stored read:
  - *Blocking / deadlocks* — `get_blocking` (blocked/blocking pairs from the blocked-process-report XE + the always-on DMV fallback), `get_deadlocks`, `get_deadlock_detail` (raw graph XML), `get_blocked_process_xml` (raw report XML), and the per-minute count series `get_blocking_trend` / `get_deadlock_trend`.
  - *Sessions* — `get_session_stats` (latest per-application connection counts), `get_active_queries` (captured running-query snapshots), `get_waiting_tasks`.
  - *Config* — the change history `get_server_config_changes`, `get_database_config_changes`, `get_trace_flag_changes`, plus the latest-snapshot pair `get_database_scoped_config` / `get_query_store_health` (per-database Query Store health: actual vs desired state, readonly_reason decoded, storage vs cap) and the current-config snapshots `get_server_config` / `get_database_config` / `get_trace_flags` (what sp_configure / sys.databases / the active trace flags are set to **right now** — the companion to the `*_changes` diffs, which are empty on a stable server).
  - *Index / object* — `get_table_index_sizes` (size + growth), `get_index_usage` (Unused / Write-only / Active), `get_object_locking` (lock/latch contention), `get_database_sizes`.

  The three config-change tools diff the store's config snapshots. This edition captures configuration **when the service connects** to a server (not on a fixed schedule), so a change is detected between two connect snapshots and at least two are needed — a stable, always-connected deployment may show no changes until the next connect. They emit only the values the collectors capture; the Dashboard's `requires_restart` / setting `description` / `setting_type` / generated change-narrative enrichment is not collected here and is omitted. The Dashboard's `get_blocking_deadlock_stats` aggregate is **not** hosted (Darling has no blocking/deadlock rollup table — use `get_blocking` / `get_deadlocks` for the raw events).

- **Eight resource-contention + jobs data-read tools** — deeper reads for an internal-contention / worker-thread / plan-cache / SQL Agent investigation, each a stored read of the latest collected snapshot:
  - *Latch / spinlock* — `get_latch_stats` (top latch classes by wait time, per-second rates), `get_spinlock_stats` (top spinlocks by collisions).
  - *Memory grants* — `get_resource_semaphore` (workspace-memory target / max-target ceiling vs granted / used), `get_memory_grants` (per-pool grant detail), `get_memory_pressure_events` (RING_BUFFER_RESOURCE_MONITOR notifications — the process/system pressure indicators, not on Azure SQL DB).
  - *Plan cache / scheduler* — `get_plan_cache_bloat` (single-use vs multi-use + bloat level), `get_cpu_scheduler_pressure` (runnable queue, worker utilization, pressure level).
  - *Jobs* — `get_running_jobs` (running SQL Agent jobs vs historical average / p95).

  The Dashboard's per-class latch `severity` / `description` / `recommendation`, spinlock `description`, plan-cache `bloat_level`, and CPU-scheduler `pressure_level` / `recommendation` are the Dashboard / reporting-view CASE derivations (not collected columns), reproduced service-side so the full result shape is served. Darling's delta collectors store no `sample_interval_seconds`, so per-second latch/spinlock rates are derived from the collection interval, and the Dashboard's `get_resource_semaphore` `sample_interval_seconds` is not emitted for the same reason (`max_target_memory_mb`, the workspace-memory ceiling, is added since the store carries it).

- **Twelve PostgreSQL data-read tools** — the read surface for a PostgreSQL target's collectors, each a stored read (see [PostgreSQL targets](#postgresql-targets)):
  - *Waits and queries* — `get_pg_wait_stats` (top wait events in the window, decoded to type + event name), `get_pg_top_queries` (query shapes by total execution time, carrying Aurora's storage-vs-cache I/O split and per-statement peak memory).
  - *Outage predictors* — `get_pg_wraparound_risk` (XID and MultiXact freeze headroom per database), `get_pg_xmin_horizon` (why vacuum is reclaiming nothing, attributed to the specific holder), `get_pg_replication_slots` (slot health, including whether retained WAL is still growing).
  - *Maintenance* — `get_pg_autovacuum_health` (tables behind on vacuum or analyze, ranked by how far past each table's OWN trigger threshold it is — the ratio, not the dead-tuple count, because the same count is routine on a large table and urgent on a small one).
  - *I/O attribution* — `get_pg_io_stats` (reads, hits, extends and evictions by backend type, object and context). The context dimension is the one with no SQL Server counterpart and the one that changes the remedy: it separates ordinary buffer-pool misses, where more `shared_buffers` or a better index helps, from sequential scans that deliberately bypass the pool through a small ring buffer, where neither will.
  - *Per-database counters* — `get_pg_database_stats` (temp-file spills, cache hit ratio, deadlocks, and the commit/rollback split, from `pg_stat_database`). **Temp files are the reason it exists**: they are work `work_mem` could not hold, which is the most common cause of a PostgreSQL query being slow for a reason its plan shape does not show, and on stock PostgreSQL it is the only temp-file evidence available anywhere. A statistics RESET is reported as a reset — an explicit count and a lower-bound caveat — rather than surfacing as a negative rate or a spike.
  - *Storage* — `get_pg_table_bloat` (the per-table bloat ESTIMATE, suppressed rather than captioned when its statistics cannot be trusted) and `get_pg_index_usage` (per-index scan counts with the constraint, replica-identity and validity facts that decide whether an unscanned index can be dropped at all).
  - *Sessions and contention* — `get_pg_blocking` (blocking chains that were SAMPLED, with the root blocker attributed and both sides' state captured — PostgreSQL has no engine-side blocked-process recorder, so blocking shorter than the interval leaves no trace anywhere) and `get_pg_session_states` (who is holding a transaction open, and whether they actually pin the xmin horizon). **That second half is a different question from the first**, and the tool exists to keep them apart: an `idle in transaction` session under READ COMMITTED that has only read, or whose write matched no rows, holds neither a snapshot nor a transaction id and starves vacuum of nothing — both measured on a live instance. So the read gates its causal claim on `peak_horizon_age`, where `-1` means the session pinned NOTHING, and tells you so rather than letting a long duration imply otherwise. Pairs with `get_pg_xmin_horizon`, which names the CLASS of holder where this names the session.

  These are separate tools rather than widened SQL Server ones. PostgreSQL's waits are a two-level type/event taxonomy with no signal-wait concept reported in microseconds, and the wraparound / horizon / slot signals have no SQL Server counterpart at all — sharing a result shape would mean lying about a unit or emitting mostly-null columns. The three outage predictors are the ones worth wiring to a pager: each names a condition that stops the server outright, and each is silent until it is nearly too late.

- **Five trend data-read tools** — windowed time-series siblings of the core reads, each a stored read of the collected series over the window (BOTH-sides, naive-UTC):
  - `get_memory_trend` (total / target server memory, buffer pool, plan cache over time), `get_perfmon_trend` (a single counter's value + delta, `counter_name` required), `get_file_io_trend` (per-database read/write latency, top-10 busiest files), `get_query_trend` (one query's per-collection history by `query_hash` + `database_name`), `get_query_duration_trend` (overall elapsed-ms/sec + executions/sec).

  Each mirrors the viewer's proven chart read (byte-identical Postgres SQL); the shape follows Lite where the SKUs diverge. `get_perfmon_trend` reproduces Lite's miss vocabulary (Page Life Expectancy is intentionally not collected; an unknown counter hands back the collected names). `get_memory_trend` carries a `total_granted_mb` field for field-for-field parity with Lite, where its memory_stats-only read leaves it 0 (the grant overlay is a separate chart series).

- **Eight system-health parse-on-read tools** — the Dashboard's `get_health_parser_*` family, over Darling's raw `system_health_events`:
  - `get_health_parser_system_health` (corruption + contention counters), `get_health_parser_severe_errors` (severity ≥ 19, with `database_id` resolved to a name), `get_health_parser_scheduler_issues`, `get_health_parser_memory_conditions`, `get_health_parser_memory_broker`, `get_health_parser_memory_node_oom`, `get_health_parser_cpu_tasks`, `get_health_parser_io_issues`.

  Where the Dashboard reads its server-side-parsed `collect.HealthParser_*` tables, these shred the raw extended-event XML **on read** with the shared `SystemHealthParser` (the same parser the viewer's System Events tab uses) and gate with the service-side twin of the viewer's `SystemEventSignificance` — returning the same SIGNIFICANT warning set the Dashboard surfaces (sp_HealthParser at `@warnings_only = 1`). `get_health_parser_system_health` is the one UNGATED category (its counter series plots every snapshot). Each row carries the full sp_HealthParser column set keyed on the event's `event_time`; the tools window on `event_time` (the event's real time), so "last 24 hours" means events that happened in the last 24 hours.

- **Five alert + health-overview tools** — the fleet-triage reads the fleet edition previously lacked, each a stored read over the monitoring store (no live hit):
  - *Alerts* — `get_alert_history` (what fired, value vs threshold, delivery success/failure, muted — fleet-wide by default, or scoped to a server), `get_alert_settings` (the current alert config the service is using — per-alert enable/thresholds, cooldown, excluded databases, delivery mode, analysis cadence), `get_mute_rules` (the alert mute rules in force, so a suppressed server is distinguishable from a healthy-quiet one).
  - *Health overview* — `get_server_summary` (one-shot per-server CPU / memory / recent blocking / recent deadlocks), `get_daily_summary` (a day's composite health band — Healthy / Warning / Critical — folded through the shared `DailyHealthBandCalculator`, plus the signals behind it).

- **Eight Custom Views tools (Darling-only)** — discover, create, and manage the saved dashboards/notebooks a user composes from the curated measure catalog (the same views the web viewer's editor builds), stored in `config.custom_views`. None touches a monitored SQL Server or the collected performance data — the write tools write only view definitions to the monitoring store.
  - *Discover* — `describe_custom_view_catalog` (the compose vocabulary — measures with their source/kind/valid-aggregates/allowed-dimensions/units/per-server-type availability, dimensions, unit families, aggregates, time buckets, filter ops, and viz types). An MCP client calls this FIRST so a composed panel uses only legal identifiers instead of guessing at names; it returns the SAME `/api/catalog` vocabulary the web composer's picker binds to. Read-only static reference — no store, no server.
  - *Read* — `list_custom_views` (summaries: id, name, description, kind, version), `get_custom_view` (one view's full definition + version).
  - *Author* — `validate_custom_view` (dry-run a definition against the catalog + composer rules, no save), `create_custom_view` (validate then save), `update_custom_view` (validate then replace in place, optimistic-concurrency on `version`), `delete_custom_view`.
  - *Self-test* — `run_custom_view_panel` (compile + run a single composed panel and return `{sql, rows, annotations}` — the composer's live preview, for checking a generated panel's data before saving).

  The create/update/delete tools are the one view-authoring **write** surface; create/update run the SAME `ValidateDefinition` authority as `validate_custom_view`, so an invalid definition is rejected before it stores; every tool routes through the SAME store + validator + compile-and-run + catalog the web viewer's editor uses (no divergent second implementation). This write surface is part of what the MCP token gates — see [What a token can reach](#opt-in-network-endpoints-lan) below.

- **Three alert-tuning write tools (Darling-only)** — `update_alert_settings`, `create_mute_rule`, and `delete_mute_rule` let an MCP client TUNE the alert engine the fleet shares — the SAME config `get_alert_settings` / `get_mute_rules` read and the Viewer's Settings window writes. `update_alert_settings` is a PARTIAL update of the single global settings row: read via `get_alert_settings`, change fields, and send only those back in the same nested shape; every field is validated against the SAME ranges/enums the Settings window enforces BEFORE any write, an out-of-range or unknown field returns `{status:"invalid"}` and writes nothing, and the write self-bumps `config_version` so the running service hot-reloads within one collection sweep. `create_mute_rule` / `delete_mute_rule` reuse the SAME `PgMuteRuleStore` `get_mute_rules` reads through (and the same GUID id-generation the Viewer's mute-create path uses). None touches a monitored SQL Server or the collected data — only the shared alert configuration; SMTP/webhook delivery credentials are out of scope (the `mcp` role cannot read or write the secret columns). It is part of what the MCP token gates — see [What a token can reach](#opt-in-network-endpoints-lan) below.

- **Two server-onboarding write tools (Darling-only)** — `add_servers` (BULK) and `remove_server` let an MCP client stand up or tear down FLEET monitoring conversationally ("monitor these twenty servers with this login"), the service-side twin of the Viewer's Add / Manage Servers dialogs. `add_servers` takes a JSON **array** of server objects (`host` required; optional `display_name` / `database` / `read_only_intent` / `multi_subnet_failover`; `auth` `Windows`/`SQL` with `username`+`password` for SQL; and the exposed TLS options `encrypt_mode` `Optional`/`Mandatory`/`Strict` + `trust_server_certificate`) and processes them **in order**: it validates each entry, PROBES the connection in-process (reusing the same `DarlingServerConnector.ProbeAsync` the `--test-connection` verb runs — the service holds the network path + credentials, so no `test_connect` command plane is needed), skips a case-folded duplicate (`duplicate`) of an already-monitored server or an earlier entry, DPAPI-encrypts the SQL password (the service identity, so it round-trips at collection time), and INSERTs the row mirroring the service's own seed shape. A server that fails to connect is `connection_failed` and the batch continues; Entra/MFA/Service-Principal/Managed-Identity auth is `invalid` (the service connects with Windows or SQL only). `remove_server` DELETEs a monitored server by name (resolved the same way every `server_name` is) — already-collected history is kept. Both write only the monitoring store's `config.config_monitored_servers` registry; neither runs anything on a monitored server beyond the one-time probe. **The SQL password travels to the endpoint inside `add_servers`' request** and is DPAPI-encrypted at rest (never returned) — it is part of what the MCP token gates, and it puts a credential on the wire; see [What a token can reach](#opt-in-network-endpoints-lan) below.

| Key | Default | Notes |
|---|---|---|
| `enabled` | `false` | **Off by default** — a headless service does not open a local port unless you ask |
| `port` | `5152` | Chosen so all three editions coexist on one machine (Dashboard 5150, Lite 5151) |

Register with Claude Code:

```
claude mcp add --transport http --scope user sql-monitor-darling http://localhost:5152/
```

If the port is already in use at startup, the MCP server logs an error and does not start; collection is unaffected.

### web

The embedded read-only **web dashboard** — a browser view of the monitoring store, served over HTTP on its OWN port (default **5153**), separate from the MCP server. It is a distinct surface from [`### mcp`](#mcp): its own enable flag, port, token, and exposure block, because the two gate different blast radii (the MCP token guards `analyze_server`'s **live outbound** connections to your monitored SQL Servers; the web dashboard is **read-only over the collected store**). It connects to the store as the least-privilege `viewer` role. Loopback-only by default; see [Opt-in Network Endpoints (LAN)](#opt-in-network-endpoints-lan) to reach it from the LAN.

| Key | Default | Notes |
|---|---|---|
| `enabled` | `false` | **Off by default** — a headless service does not open a local port unless you ask |
| `port` | `5153` | Chosen so all four local surfaces coexist on one machine (Dashboard 5150, Lite 5151, Darling MCP 5152) |

Once enabled, open `http://localhost:5153/` in a browser on the service host. Like the MCP server, `enabled`/`port` here are the file SEED; after first start they live in the control plane and the Viewer's Settings toggles them LIVE (the service starts/stops/rebinds the dashboard within seconds — no restart). If the port is already in use at startup, the web host logs an error and retries on a calm cadence; collection is unaffected.

**What you see.** The dashboard opens on a **Fleet Overview**: a card per enabled server with a status dot, six per-metric health bands (CPU, threads, memory, blocking, deadlocks, collectors), and its last collection time — all banded server-side, so the browser only renders (a server that has never reported shows an amber "Awaiting first collection", never a red offline). Above the cards a worst-first "Needs attention" list surfaces the servers to look at, or an all-healthy line when there is nothing to chase. Click a card to **drill into one server**: an overview, wait stats with a trend for the heaviest wait, active queries, a CPU chart, memory and file-I/O trends, and collection health — the same collected data the viewer shows, over inline charts. A fleet-wide **Alert History** page (with a server filter box) rounds out phase 1, and a **Custom Views** page hosts the composer for the saved dashboards/notebooks in `config.custom_views` — the dashboard's one write path. Otherwise it is a read-only view — no settings, no live-server queries — and it refreshes every 60 seconds (pausing while the tab is hidden). The frontend ships fully self-contained (no CDN, no fonts, no remote anything), so it works on an air-gapped host with no internet access.

### peers

**Declared peer stores** — optional, and only relevant when the fleet is split across **several Darling boxes**, one store each (SQL Server primaries on one box, their readable replicas on another, PostgreSQL on a third). Each box's MCP server answers over **its own** store only, so a server monitored by a sibling resolves as not-found — which an agent cannot tell apart from *"nobody monitors this server."* Declaring the siblings fixes that at the three places an agent forms its picture of the fleet.

**Disclosure only.** There is no address and no credential in this block, and nothing behind it: the service never contacts a peer, cannot read a peer's data, and cannot tell whether a peer is even running. A peer is a **name** plus a **sentence**, so an agent (or its human) can pick the right endpoint. Everything here is sent verbatim to every connected MCP client, so the service **refuses to start** if any peer text looks like a connection string or credential.

| Key | Default | Notes |
|---|---|---|
| `thisStoreCovers` | `""` | One sentence naming what THIS store monitors — the anchor the peer list is relative to |
| `stores[].name` | — | **Required.** Whatever an operator would recognize (the box name, "the use1 store") |
| `stores[].covers` | `""` | A short sentence naming what that store monitors. Human prose — never parsed, only shown |
| `stores[].matches` | `[]` | Optional server-name **substrings** that store monitors, case-insensitive. The only machine-checked field |

```jsonc
"peers": {
  "thisStoreCovers": "the 42 us-east-1 SQL Server primaries",
  "stores": [
    {
      "name": "prod-sql-use2-monitor-01",
      "covers": "the readable replicas of those same 42 primaries, in-region from us-east-2",
      "matches": ["use2"]
    },
    { "name": "prod-sql-pg-monitor-01", "covers": "the Aurora PostgreSQL clusters", "matches": ["-aurora-"] }
  ]
}
```

What it changes, with peers declared:

- **The MCP instructions** gain a Fleet Coverage section, high enough that an agent reads which store it is talking to before it reads the tool census.
- **`list_servers`** gains `this_store_covers`, a `peer_fleets` array, and a `peer_note`. Both are always present: an *empty* `peer_fleets` has two very different meanings (this really is the only store, or nobody declared the siblings) and the service cannot tell them apart, so `peer_note` says exactly that rather than letting an empty array read as "this is the whole fleet." An **empty registry** answers in prose rather than JSON, and carries the peer list too — a store with nothing registered is a fresh or just-restarted box, which is the worst place to drop the disclosure.
- **The server-resolution miss** appends the disclosure to the existing "Could not resolve server. Available servers:" listing, naming the peer whose declared coverage matches — so *not monitored here* stops looking like *not monitored anywhere*.

`matches` is deliberately plain substrings, no globbing and no regex: it exists to answer "which region/role prefix is this name?", and a pattern language would be a config surface with its own failure modes. Blank entries are dropped — an empty substring matches every name, which would make one peer claim the whole fleet. A peer with no `matches` is still disclosed everywhere; it just cannot be singled out on a miss, and the miss message says so instead of implying the server is unmonitored.

A **file-only** block (not seeded into the control plane): it describes the deployment topology of *this* box, which must not be editable from a peer's Viewer. An edit takes effect on the next service restart. There is deliberately **no cross-store connectivity** here — actual federated reads (auth between stores, latency, partial failures) are a much larger surface, and may never be worth building if disclosure alone makes the split legible.

**Declaring nothing changes nothing, with one exception worth knowing about on upgrade.** The instructions, the resolution-miss message, and `list_servers`' empty-registry sentence are byte-for-byte what they were. But `list_servers`' JSON envelope carries `this_store_covers`, `peer_fleets` and `peer_note` on *every* response, declared or not — so a script comparing that tool's exact shape sees three new keys even if you never write a `peers` block. That is deliberate: an empty `peer_fleets` means *either* "this is the only store" *or* "nobody declared the siblings", and a note that only appeared when peers were declared would say nothing in precisely the case that produces the wrong conclusion.

**A `peers` block that fails validation is refused whole, and nothing is disclosed** — not the valid subset. An unfinished block that asserts coverage which may be wrong is worse than no block, and the service logs each problem at Critical. The check runs inside the publish rather than only in config validation, because the MCP host loads its own config and deliberately never validates it (its fail-closed checks are host-local), so validation alone would leave the one path that actually broadcasts uncovered.

### No Schedule Knobs, by Design

There are deliberately **no collection-schedule or retention settings** in `darling.json`. The service consumes the shared per-collector defaults (`CollectorScheduleDefaults`) — the same cadences and retention horizons a fresh Lite install uses, identity-pinned by tests so the two editions cannot drift. If a schedule knob is ever genuinely needed, it will be added then, not speculatively.

---

## PostgreSQL Targets

Darling monitors PostgreSQL alongside SQL Server. For the ordered procedure with a proof point at each step, see [**the first-target runbook**](../docs/postgres-first-target-runbook.md); this section is the reference for what each piece does.

Add `"engine": "postgres"` to a `servers` entry and that target is collected by the PostgreSQL collectors instead of the T-SQL ones:

```json
{
  "name": "orders-prod",
  "engine": "postgres",
  "host": "orders-prod.cluster-abc123.us-east-1.rds.amazonaws.com",
  "auth": "sql",
  "username": "darling_monitor",
  "encryptedPassword": "<DPAPI blob from --encrypt-password>"
}
```

**Which path registers a target depends on the store, not the file.** `darling.json` seeds `config.config_monitored_servers` once, when it is empty; after that the registry is authoritative and a darling.json edit adds nothing. So a fresh install declares its PostgreSQL targets in the file, and an existing one adds them with the [`add_servers`](#mcp) tool (or the Viewer's Add Server dialog), which takes effect within one collection sweep without a restart. The registry carries `engine` and `port` per row, so a target keeps its engine across restarts and reloads.

`auth` must be `"sql"` — PostgreSQL has no integrated-authentication path here, and an entry asking for it fails [`--test-connection`](#validate-the-config-pre-flight) rather than waiting to fail at first connect. `add_servers` enforces the same rule, and unlike the file parser it REFUSES an unrecognized `engine` rather than resolving it to SQL Server: the file's leniency keeps one bad line from stopping the whole fleet at startup, while onboarding is a single deliberate act where a silent fallback would surface as a connection failure against the wrong port. Password handling is identical to a SQL Server entry: `--encrypt-password` produces the DPAPI blob, and the `env:NAME` / `file:/path` references work the same way. TLS defaults to full certificate verification (`SslMode=VerifyFull`); `trustServerCertificate` relaxes it to `Require`, which is the setting Aurora usually needs since it presents an RDS CA a stock trust store does not know, and `"encryptMode": "Optional"` relaxes it further to `Prefer`.

**One store, both engines.** The PostgreSQL collectors write to the same store as the SQL Server ones, into their own tables, on the same naive-UTC contract and the same `server_id` identity. Nothing is partitioned by engine — a mixed fleet is one store, one viewer, one MCP endpoint.

**A collector table must never be named after a `pg_catalog` object.** `pg_catalog` is searched implicitly and *first*, ahead of every entry in `search_path`, so an unqualified reference to such a name resolves to the system object no matter what the store holds. It fails loudly in one place — `CREATE INDEX` on a view is 42809, which aborts the migration and leaves the store unusable — and silently everywhere else: a reader's `FROM <name>` would return the monitoring store's own system view instead of collected history, so the tool reports nothing and any alert behind it never fires. That is why the slot collector stores into `pg_replication_slot_stats` while still being *named* `pg_replication_slots` after the view it reads (the same split as `query_store` → `query_store_stats`). A live-store test asserts no collector table shadows a catalog object, against the real catalog rather than a hardcoded reserved list.

**A collector never runs against the wrong engine.** Every definition declares its `TargetEngine`, and both SKUs check it before dispatch, so a PostgreSQL target is never sent T-SQL and a SQL Server target never sees `pg_stat_statements`. A store monitoring only SQL Server still carries the five PostgreSQL tables, empty; nothing else about it changes.

### Permissions on a PostgreSQL target

One role covers every collector:

```sql
CREATE ROLE darling_monitor WITH LOGIN PASSWORD '<password>';
GRANT pg_monitor TO darling_monitor;
```

`pg_monitor` is the standard PostgreSQL monitoring role — it bundles `pg_read_all_stats`, `pg_read_all_settings`, and `pg_stat_scan_tables`. Without it the statistics views still return rows, but only for the connecting user's own backends, which silently turns fleet monitoring into self-monitoring. On Amazon Aurora and RDS the same grant works: `GRANT pg_monitor TO darling_monitor;` as an `rds_superuser`. No superuser is needed, and nothing is created on the monitored server — unlike a SQL Server target, there are no Extended Events sessions to provision and no server setting to bootstrap.

`pg_stat_statements` must be present for `pg_statement_stats`, which means the extension in `shared_preload_libraries` (a restart, or a parameter-group change plus reboot on Aurora/RDS) and `CREATE EXTENSION pg_stat_statements;` in the database Darling connects to. The extension tracks **all** databases in the cluster keyed by `dbid`, so one installation in the connect database covers the whole instance. The other six collectors need nothing installed — they read core catalogs and Aurora's built-in functions.

### What gets collected

| Collector | Source | Cadence / retention | Why it exists |
|---|---|---|---|
| `pg_wait_stats` | `aurora_stat_system_waits()` | 1 min / 30 d | **Aurora only.** Core PostgreSQL has no cumulative wait counters at all — `pg_stat_activity.wait_event` is an instantaneous sample — so there is no equivalent to `sys.dm_os_wait_stats` to read on a non-Aurora target |
| `pg_statement_stats` | `aurora_stat_statements()` | 1 min / 30 d | **Aurora only.** Per-query-shape totals, matching `query_stats`' cadence. Aurora's function adds the storage-vs-cache I/O split (`storage_blks_read` / `orcache_blks_hit`) and per-statement peak memory, neither of which core PostgreSQL exposes |
| `pg_wraparound_stats` | `pg_database`, `pg_class` | 5 min / 90 d | XID and MultiXact freeze headroom per database. The highest-consequence signal PostgreSQL has and one with no SQL Server counterpart: run out of transaction IDs and the server stops accepting writes. Freeze headroom moves in autovacuum-sized steps rather than continuously, so 5 minutes is ample and 90 days shows the age trend against the actual freeze threshold |
| `pg_xmin_horizon` | `pg_stat_activity`, `pg_replication_slots`, `pg_stat_replication`, `pg_prepared_xacts` | 1 min / 30 d | Why vacuum is reclaiming nothing. Four unrelated causes produce an identical symptom and need completely different fixes, so this attributes the specific holder instead of reporting the number. Per-minute because a holder is the fast-moving leading indicator — the useful answer is which session or slot appeared minutes ago |
| `pg_replication_slots` → `collect.pg_replication_slot_stats` | `pg_replication_slots` | 1 min / 90 d | Slot health and retained WAL. An abandoned slot retains WAL without bound by default, filling the volume and stopping the server, and it grows at whatever rate the server writes WAL — hours on a busy writer, not days |
| `pg_autovacuum_stats` | `pg_stat_user_tables`, `pg_class` | 60 min / 90 d | **Writers only**, and per database. Per-table autovacuum state. Stores each table's own computed trigger threshold beside its dead-tuple count, honouring per-table `reloptions` overrides rather than only the GUCs — without the threshold a dead-tuple count is not actionable, since the same count is routine on a large table and urgent on a small one |
| `pg_io_stats` | `pg_stat_io` | 1 min / 30 d | I/O attributed to a `(backend_type, object, context)` triple rather than to a file — who did it, to what, and why. PostgreSQL 16+; valid on a standby. Every counter column is nullable on purpose: PostgreSQL uses NULL for "does not apply to this combination", and on Aurora the whole write side is NULL because backends there do not write data files. The read reports whether write counters are TRACKED, so absent writes cannot be misread as zero writes |
| `pg_blocking` → `collect.pg_blocking_edges` | `pg_stat_activity`, `pg_blocking_pids()` | 1 min / 30 d | Who is blocked, by whom, and what state each side was in — stored as an edge list, one row per (blocked, blocking) pair, so the read layer can assemble chains and name the root. **This is a SAMPLE, not an event log**, and that is the one thing to carry away: SQL Server's blocked-process report is written by the engine when blocking crosses a threshold, whereas PostgreSQL records nothing unless something asks, so blocking shorter than the interval is never seen. Valid on a standby, where recovery conflicts are blocking that happens nowhere else. One minute is the floor worth paying for: `pg_blocking_pids()` takes ShareLock on the lock manager partitions per call, so it is evaluated only for backends already waiting on a lock |
| `pg_database_stats` | `pg_stat_database` | 1 min / 30 d | Four questions off one cluster-wide view: temp-file spills (`temp_files` / `temp_bytes`), cache hit ratio (`blks_hit` / `blks_read`), a server-recorded `deadlocks` count, and the commit/rollback split. **Every major and every target shape** — the newest column selected arrived in PostgreSQL 9.2, it is core rather than Aurora, and a standby is included deliberately, because a sort on a read replica spills the way it does on a writer. Stored PER DATABASE rather than aggregated, because `stats_reset` is per database: an aggregate would have to pick one reset timestamp for rows that legitimately disagree, and one database's reset would corrupt the cluster's delta with nothing left in the data to say so. `stats_reset` is captured so the read can report a reset AS a reset |
| `pg_index_usage_stats` | `pg_stat_user_indexes`, `pg_statio_user_indexes`, `pg_index` | 24 h / 90 d | Per-index scan counts, sizes, and the constraint/replica-identity/validity facts that decide droppability. **Writers only, and per database** — a replica reports its OWN scan counts rather than the writer's, so an index that is load-bearing on the primary reads as unused there, which is the single worst answer this surface can give. `last_idx_scan` is PostgreSQL 16+ and is substituted with a typed NULL below it so the row shape does not change across a mixed-version fleet. Daily rather than hourly, matching `index_object_stats`: "has anything scanned this index" is a structural question, and an hourly sample would record the same catalog facts 24 times a day at 24x the fan-out connections. 90 days of retention because the RETENTION WINDOW IS THE EVIDENCE — an index can only be called unused for as long as we have been watching it, and 30 days cannot clear a monthly report. Indexes below 64 KB are not collected (nothing to model), except invalid ones, which are a finding at any size |
| `pg_table_bloat_stats` | `pg_class`, `pg_stats`, `pg_stat_user_tables` | 60 min / 90 d | The statistics-based per-table bloat ESTIMATE, plus measured heap/TOAST/index sizes and the dead-tuple counts. **Never reads the relation**, which is what makes it affordable on a cadence: measured at 44 ms for a 2,001-table database against 860 ms for `pgstattuple` over the same tables, and that 19x is a floor on the ratio because those tables were tiny and fully cached. Hourly to match `pg_autovacuum_stats` deliberately — this measures the DAMAGE whose CAUSE that one measures, and correlating them needs a common grain. **Writers only, and per database.** Tables below 1 MB are not collected. The estimate is suppressed rather than captioned when its inputs cannot be trusted; see V85 |
| `pg_session_states` | `pg_stat_activity` | 1 min / 30 d | Which sessions are holding a transaction open, for how long, and — the part that decides whether it matters — whether that transaction is what pins the xmin horizon. **`idle in transaction` is NOT automatically a horizon holder**, which is the whole reason this stores an age rather than a state: measured on live PostgreSQL 16.15, a READ COMMITTED transaction that only read and one whose `UPDATE` matched zero rows both sit idle in transaction indefinitely with `backend_xmin` AND `backend_xid` NULL, pinning nothing at all. `horizon_age` is the GREATER of the two ages and is `-1` when the session holds neither — not `0`, which would read as "holds the newest possible xid". **No raw statement text is stored**: `pg_stat_activity.query` carries literal parameter values, so the normalised `query_id` and a whitelisted command keyword are stored instead. **A SAMPLE, like `pg_blocking`** — a transaction that opened and closed between two cycles is invisible, and the read carries its own capture counts so that is visible rather than assumed. Valid on a standby, deliberately: a standby holds its own transactions, and with `hot_standby_feedback` on its xmin propagates to the primary |

The two Aurora-only collectors are gated on Aurora detection, not on configuration: the connect probe looks for `aurora_version()` and the gate follows what it finds. Point Darling at self-managed PostgreSQL and the four core-catalog collectors run while those two sit out.

`pg_autovacuum_stats` additionally gates OFF on a standby (`pg_is_in_recovery()`), and the reason is worth knowing because it is not a permissions or availability problem. `pg_stat_user_tables` reads fine on a replica and reports **all zeros**: measured on Aurora 17.7, the same cluster, database and 15 tables, the writer reported 13,654,458 dead tuples and 150,790,506 live tuples while the reader reported 0 for every tuple counter. Those are the writer's stats-collector numbers and they are not replicated. Ungated, a replica target would return no rows, the activity filter would read that as "nothing has pending work", and you would get a confident report of perfect autovacuum health for a cluster 13 million dead tuples behind. For the same reason, treat an empty `get_pg_replication_slots` result from a replica as per-instance rather than cluster-wide — slots live on the writer.

Cadences and retention are the shared defaults, with no knobs, exactly as for SQL Server.

Three of the twelve are outage predictors rather than performance metrics, which is deliberate: PostgreSQL's most damaging failures are quiet, slow, and fully predictable days ahead, and nothing in the engine raises its hand about them. Every one of the twelve has an MCP tool (see [the tool list](#mcp)), a web tab and — since #2530 — a WPF viewer tab: a PostgreSQL target gets **seven** inner tabs (Overview, Activity, Vacuum, Waits, I/O, Replication, Storage) in place of the nineteen SQL Server ones, chosen from `collect.servers.engine_kind`. The placement is derived from the collector catalog in both directions, so a thirteenth PostgreSQL collector cannot ship without a screen.

### What it does not do yet

Plan capture has no PostgreSQL equivalent in the store yet, and that is the largest remaining gap — plans are why people open a database monitoring tool. Alerting and scheduled analysis are still SQL-Server-shaped, so a PostgreSQL target collects and is readable through MCP, the web dashboard and the viewer, but does not yet raise alerts or produce analysis findings. The PostgreSQL tabs on both UIs are tables over the window: no charts, no correlated timeline, and no drill-down from a blocking root to the sessions behind it. **The three Tier 0 outage predictors DO alert.** Wraparound risk, a blocked vacuum horizon and replication-slot retention are evaluated on the alert cadence and delivered through the same deliverer, history and mute rules as every SQL Server alert, so they land in the same places and obey the same suppression. They ride alongside the shared engine rather than inside it, via a separate `IPostgresAlertReadAdapter` consulted only for PostgreSQL targets — Lite has no PostgreSQL target, and extending the shared adapter would have left it implementing three methods that can only return empty. Thresholds are derived from the server's own settings (wraparound grades against that cluster's `autovacuum_freeze_max_age`, not a constant) and are not yet configurable; see [`docs/postgres-alerting-design-note.md`](../docs/postgres-alerting-design-note.md).

Scheduled ANALYSIS is still SQL-Server-shaped, so a PostgreSQL target does not yet produce analysis findings.

Collector FAILURES are classified, though. A PostgreSQL fault is routed through the same `ITargetProvider.Classify` the engine seam exposes, so a persistent, operator-actionable condition records as a non-fatal skip with an explanation instead of logging `ERROR` every cycle — a `pg_stat_statements` view that was never created (SQLSTATE 42P01), a source Aurora does not implement (0A000), a feature switched off in the parameter group (55006). The message says which kind it is, because the store's non-fatal bucket is named PERMISSIONS and none of those is a missing grant. Connection-level failures (the 08 class, 57P0x) still force a reconnect and reprobe; a `statement_timeout` (57014) deliberately does not, since dropping the connection over a slow query would turn a tuning problem into a reconnect storm.

The per-database fan-out itself is done — `pg_autovacuum_stats` is the collector that exercises it. Worth knowing what it costs: a SQL Server collector can reach another database without reconnecting (`EXECUTE [db].sys.sp_executesql`), while a PostgreSQL connection is bound to one database for its lifetime, so a per-database PostgreSQL collector is necessarily one connection per database per cycle. That is why its cadence is hourly and why new per-database collectors should be added deliberately rather than by default.

---

## Operations

### The Store

The service migrates the store itself at startup — plain versioned SQL scripts, each applied once inside its own transaction, tracked in `darling_schema_version`, safe under concurrent starters (advisory-locked). Current schema is **v73** — `StorageVersion.SchemaVersion` is the source of truth and a test pins it to the highest rung in the ladder.

The notable rungs are below. For the **complete** current schema, read `Darling/Darling.Tests/Fixtures/migration-ladder-*.sql` — the whole ladder as resolved SQL, regenerated per release; it is generated, so don't hand-edit it.

| Version | Contents |
|---|---|
| **V1** — collector tables | One table per collector, all 54, generated from the shared collector definitions (column-for-column identical to Lite's DuckDB schema): `wait_stats`, `latch_stats`, `spinlock_stats`, `query_stats`, `procedure_stats`, `query_store_stats`, `query_snapshots`, `plan_cache_stats`, `cpu_utilization_stats`, `cpu_scheduler_stats`, `file_io_stats`, `memory_stats`, `memory_clerks`, `memory_pressure_events`, `tempdb_stats`, `perfmon_stats`, `deadlocks`, `blocked_process_reports`, `dmv_blocking_snapshots`, `memory_grant_stats`, `waiting_tasks`, `session_stats`, `session_summary_stats`, `running_jobs`, `database_size_stats`, `index_object_stats`, `server_properties`, `system_health_events`, the four config snapshots (`server_config`, `database_config`, `database_scoped_config`, `trace_flags`), and the twenty PostgreSQL tables listed under V63–V69, V71, V83–V94 below |
| **V2** — observability | `servers` (registry, upserted on every successful connect: identity, display name, engine edition, major version) and `collection_log` (one row per collector run: SUCCESS / PERMISSIONS / ERROR, row count, SQL-phase and storage-phase timings) |
| **V3** — alerting | `config_alert_log` (one history row per fired alert), `config_edge_trigger_watermarks` (restart-surviving edge-trigger and failed-job watermarks), `config_mute_rules` (alert mute rules; starts empty) |
| **V4** — analysis | `analysis_findings` (persisted findings incl. the stored remediation action), `analysis_muted` (muted finding patterns), and 17 `v_<table>` passthrough views so the shared analysis SQL runs verbatim against this store |
| **V5** — viewer passthrough views | The five remaining `v_*` passthrough views (`v_running_jobs`, `v_server_config`, `v_database_scoped_config`, `v_trace_flags`, `v_collection_log`) that complete the viewer's read layer |
| **V6** — memory passthrough views | `v_memory_clerks` and `v_memory_pressure_events`, the two views the Memory tab reads |
| **V7** — plan-capture columns | Nullable plan-XML columns for the viewer's View Plan surfaces: `procedure_stats.query_plan_xml`, `blocked_process_reports.blocked_query_plan_xml` / `blocking_query_plan_xml`, `deadlocks.victim_query_plan_xml` |
| **V8** — schema split (collect/config) | Moves the tables into the `collect` and `config` schemas (least-privilege security split); the shared SQL keeps using bare names, resolved via `search_path = collect, config, public` |
| **V9** — inventory + cost fields | `server_properties` inventory columns (`sqlserver_start_time`, `host_os_version`, `ag_replica_role`) and `servers.monthly_cost_usd` (the FinOps per-server budget) |
| **V10** — latch + spinlock collectors | `latch_stats` and `spinlock_stats` tables plus their `v_*` views |
| **V11** — CPU scheduler + plan cache collectors | `cpu_scheduler_stats` and `plan_cache_stats` tables plus their `v_*` views |
| **V12** — session summary collector | `session_summary_stats` (server-wide connection-leak / idle signal) table plus its `v_*` view |
| **V13** — system health events collector | `system_health_events` (raw `system_health` Extended Events capture) table plus its `v_*` view |
| **V14** — refresh passthrough views | `CREATE OR REPLACE` on every `v_*` view so a store upgraded across a column-adding migration picks up the new columns (Postgres freezes a view's `SELECT *` expansion at create time) |
| **V15** — index metadata columns | Per-index definition columns on `index_object_stats` (ordered key/included column lists, filter, uniqueness/constraint/FK flags, `is_disabled`, and the reconstruct-a-CREATE options — compression, fill factor, page/row locks, etc.) for monitor-side UNUSED/DUPLICATE index analysis, and refreshes `v_index_object_stats` |
| **V16** — server UTC offset | Nullable UTC-offset column on `server_properties` so the viewer can render timestamps in the monitored server's own local time (the Server-time display mode ported from Lite; Server-time = stored naive-UTC + this offset) |
| **V17** — config control plane | The viewer-writable DESIRED-state tables (`config_service`, `config_monitored_servers`, `config_alert_settings`, `config_collector_schedules`) plus a `config_version` reload beacon — statement-level bump triggers increment it on any write, and the service polls that one integer each sweep and reloads only when it changes. Server secrets are DPAPI blobs, never plaintext |
| **V18** — alert delivery mode | Global `delivery_mode` (Summary / PerEvent) + `per_event_max` on `config_alert_settings`, plus a nullable per-server `alert_delivery_mode_override` on `config_monitored_servers` (null = inherit the global), resolved through the shared `AlertDeliveryModeResolver` (#1236 / #1141) |
| **V19** — analysis state marker | `collect.analysis_state` — the service-produced per-server "insufficient data" marker (with message + time) the viewer reads, so a not-enough-history analysis pass surfaces a reason instead of a blank |
| **V20** — alert tuning knobs | The previously-hardcoded alert tuning the viewer now customizes on `config_alert_settings`: the long-running-query read shape (`long_running_query_max_results` + five noise-filter opt-outs the shared `AlertEngine` forwards) and `notify_connection_changes` (the Server-Unreachable / Restored connect-edge gate) |
| **V21** — default trace events collector | `default_trace_events` table + its `v_*` view — the significant Default Trace events (file growth, ErrorLog, security audit, optional Object DDL) the viewer's System Events tab reads |
| **V22** — index-object latest index | The engine-agnostic `idx_index_object_stats_latest` partial index backing the latest-capture-per-index reads |
| **V23** — collection-log hypertable | Converts `collection_log` to a TimescaleDB hypertable (an object-invisible no-op on plain PostgreSQL) |
| **V24** — job history collector | `job_history` table + its `v_*` view — the SQL Agent Job History surface (#1433) |
| **V25** — agent status collector | `agent_status` table + its `v_*` view — SQL Agent up/down status (#1433) |
| **V26** — generic webhook channel | The generic-webhook columns on `config_notification` (`generic_url`, `generic_headers`, `generic_body_template`, `generic_proxy`) for POSTing alerts to any endpoint (#1506) |
| **V27** — deadlocks database name | `deadlocks.database_name` (the Azure SQL DB per-database deadlock-capture watermark key, #1535) and a refreshed `v_deadlocks` |
| **V28** — Query Store replica role | `query_store_stats.replica_role` (SQL Server 2022+ AG secondary-replica attribution, #1546) and a refreshed `v_query_store_stats` |
| **V29** — long-query completions collector | `collect.long_query_completions` + its index — the opt-in long-running-query completion trace's store table (#1496) |
| **V30** — web dashboard config | `config_service.web_enabled` + `web_port` — the read-only web dashboard's live enable/port toggle, the twin of `mcp_enabled`/`mcp_port` (#1562) |
| **V46** — automatic plan correction | `collect.plan_correction` + its index — the #1952 collector's store table (FORCE_LAST_GOOD_PLAN enablement plus the engine's live recommendation set). Additive and view-less, so a fresh store gets it from V1's generated schema and V46 is what an already-existing store gets |
| **V47** — ADR persistent version store | `collect.pvs_stats` + its index + the `v_pvs_stats` passthrough view — the #1951 ADR version-store collector's store table. A fresh store gets the table from V1's generated schema; V47 is what an already-existing store gets, and the view is what keeps the Darling viewer's FinOps read byte-identical to Lite's |
| **V61** — per-fingerprint occurrence counters | `config.config_incident_occurrences` — the accumulator’s memory for the monotonic count behind an alert incident (#2216). The count that rides on an incident is a GAUGE (it falls as events age out of the read window), so a consumer seeing only throttled deliveries cannot recover how many events happened between two of them. A NEW table, not columns on `config_edge_trigger_watermarks`: the key is wrong (per (server, metric) vs per (server, metric, fingerprint)) and Lite writes that row with a PARTIAL `INSERT OR REPLACE` column list, so an added column would zero itself every time an alert fired |
| **V62** — plan-XML codec knob | `config.config_service.plan_xml_compression` (#2171). `gzip` (default) keeps today’s write path; `none` stores plain text in `query_plan_xml` so direct-SQL readers get plans back — PostgreSQL exposes no inflate, so gzip bytes are unreadable without an untrusted-language UDF. Rides `config_service` like V58/V59 so the `config_version` trigger makes a flip visible to the next reload poll |
| **V63–V69** — PostgreSQL collector tables | `collect.pg_wait_stats`, `collect.pg_statement_stats`, `collect.pg_wraparound_stats`, `collect.pg_xmin_horizon`, `collect.pg_replication_slot_stats`, `collect.pg_autovacuum_stats`, and `collect.pg_io_stats`, each with its time index — one rung per PostgreSQL collector. Additive and view-less, exactly like V46/V47: a fresh store gets all seven from V1's generated schema, and these rungs are what an already-existing store gets. They add tables only, so a store that monitors no PostgreSQL target carries seven empty tables and nothing else changes |
| **V70** — monitored-server engine + port | `config.config_monitored_servers.engine` (`NOT NULL DEFAULT 'sqlserver'`) and `.port` (`NOT NULL DEFAULT 0` = the driver's default). The registry is authoritative for the server list once seeded, and these were the two `MonitoredServer` fields with no column — so a PostgreSQL target round-tripped as a SQL Server one and was connected to with `SqlConnection`. Every existing row means exactly what it meant before, and the SQL-Server-only writers keep inserting without naming either column |
| **V71** — PostgreSQL blocking edges | `collect.pg_blocking_edges` + its time index — the eighth PostgreSQL collector's store table. One row per (blocked, blocking) pair rather than a rendered tree, which is what lets the read layer compute root blocker, chain depth and fan-out in SQL instead of parsing a string. Additive and view-less exactly like V63–V69. **Sparse by design**: empty on a healthy instance, and because PostgreSQL has no engine-side blocked-process recorder, a gap means "not sampled" rather than "not blocked" — a count over this table measures how often blocking was *caught* |
| **V83** — PostgreSQL per-database counters | `collect.pg_database_stats` + its time index — the ninth PostgreSQL collector's store table (#2539): `pg_stat_database`'s temp-file, cache, deadlock and transaction counters. Additive and view-less exactly like V63–V69 and V71. Every counter column is nullable on purpose — they are cumulative counters differenced at read time, and a NOT NULL 0 default would turn "not reported" into a measurement; `database_name` is nullable because PostgreSQL genuinely emits a NULL-named row for shared relations |
| **V84** — PostgreSQL index usage | `collect.pg_index_usage_stats` + its time index — the tenth PostgreSQL collector's store table (#2541): per-index scan counts and sizes, plus the catalog facts that decide whether an unscanned index can be dropped at all. Most of the width is that second half, and it is stored rather than looked up on demand because the MCP has no ad-hoc path back to a monitored server — whatever is not captured here cannot be recovered later. `last_scan` is nullable with two distinct meanings the read separates (PostgreSQL 15 and below do not record it; on 16+ it is NULL for an index never scanned since the counters were reset), and `stats_reset` is nullable because it is genuinely NULL until a database's statistics are first reset |
| **V85** — PostgreSQL table bloat | `collect.pg_table_bloat_stats` + its time index — the eleventh PostgreSQL collector's store table (#2542): the statistics-based bloat ESTIMATE with the measured sizes and counter-based dead-tuple pair beside it. **Three tiers of certainty share the table and the column names carry the difference**: `heap_bytes`/`toast_bytes`/`index_bytes` are measured (`pg_relation_size` asks the filesystem), `live_tuples`/`dead_tuples` are the server's own counters, and `bloat_bytes_estimate`/`bloat_pct_estimate` are arithmetic over column-width statistics — suffixed `_estimate` in the store so the qualifier cannot be lost between here and a screen. `estimate_unavailable` is load-bearing rather than advisory: it is TRUE when the monitoring login cannot SELECT the table and `pg_stats` filtered every row out, a state in which the estimator does not fail but silently returns large numbers |
| **V86** — PostgreSQL session states | `collect.pg_session_states` + its time index — the twelfth PostgreSQL collector's store table (#2540): who is holding a transaction open and whether it pins the xmin horizon. **`horizon_age` is the column the table exists for**, and its `-1` is load-bearing rather than tidy: `pg_xmin_horizon` can say a session holds the horizon, and only this can say a session does NOT — which is the answer for two of the four idle-in-transaction shapes measured on a live instance. There is **no query-text column, deliberately**: `pg_stat_activity.query` carries literal parameter values and this table fills on a duration floor an ordinary application crosses, so `query_id` (joinable to `pg_statement_stats`, whose text is already `$1`-normalised) and a whitelisted `command_tag` carry the statement identity instead. `state_is_redacted` exists because without `pg_monitor` PostgreSQL does not refuse the read — it returns every row with the state columns NULL while leaving `backend_xmin` and `backend_xid` visible, so the horizon still reads as pinned and nothing can say by what |
| **V87** — PostgreSQL plan-capture readiness | `collect.pg_plan_capture_readiness` + its time index (#2564): whether a target could capture execution plans at all, and if not, WHICH step is missing. One row per facet rather than a single verdict, because “no plans” has several unrelated causes — the module not preloaded, a threshold that captures nothing, a log line prefix that cannot attribute a plan to a query — and each has a different remedy, carried on the row |
| **V88** — PostgreSQL write side | `collect.pg_write_stats` + its time index (#2544): checkpoints, background writing and WAL from `pg_stat_checkpointer`, `pg_stat_bgwriter` and `pg_stat_wal`, as ONE collector because they are one story. A union schema with version-conditional column expressions: PostgreSQL 17 moved checkpoint counters out of `pg_stat_bgwriter`, so a major that lacks a column reports NULL rather than the collector splitting into per-version twins. `wal_bytes` is `numeric(38,0)` because that is how upstream types it |
| **V89** — PostgreSQL extension availability | `collect.pg_extension_availability` + its time index (#2545): four states, not a boolean — `installed`, `outdated`, `available`, `absent`. The distinction is what makes it actionable: `available` is a `CREATE EXTENSION` away and `absent` is not. Absence is not a row in any catalog, so it is derived against an enumerated roster; `auto_explain` and `pg_wait_sampling` are deliberately off that roster, being preload-only modules that never appear in `pg_available_extensions` even where they are loaded and working. Runs per database as of V95 |
| **V90** — PostgreSQL lock states | `collect.pg_lock_stats` + its time index (#2544): lock state by mode, type and relation. Does NOT duplicate `pg_blocking_edges`, which stores blocked/blocker PAIRS — that answers who is stuck behind whom and cannot answer what lock, on what object, is being waited on |
| **V91** — PostgreSQL column statistics | `collect.pg_column_stats` + its time index (#2543): the planner inputs behind a misestimate — `n_distinct`, `null_frac`, `avg_width`, `correlation`. **The SHAPE of the skew, never the values**: `most_common_freqs[1]` and `cardinality(most_common_vals)` are stored and `most_common_vals` / `histogram_bounds` deliberately are not, because those hold raw customer data and every finding this table exists for survives without them. `n_distinct` is a floating type rather than a count because negatives are a RATIO of row count. **Needs `pg_read_all_data`**: `pg_stats` filters on `has_column_privilege` and `pg_monitor` confers no SELECT on user tables, so without that grant this collector succeeds and returns zero rows |
| **V92** — PostgreSQL replication | `collect.pg_replication_stats` + its time index (#2544): connected standbys and how far behind each one is — four byte distances measured from `pg_current_wal_lsn()` and three time lags. The lag columns keep growing during a stall rather than freezing, which is what makes them usable as an alerting signal |
| **V93** — PostgreSQL buffer residency | `collect.pg_buffer_usage` + its time index (#2544): what is resident in shared buffers, by relation — a hit ratio says how often the pool worked, this says what is IN it. Joins `pg_buffercache.relfilenode` to `pg_relation_filenode(c.oid)`, **not** to `c.oid`, which is the join every published example gets wrong, and scopes to the connected database because the pool is cluster-wide while `pg_class` is not |
| **V94** — PostgreSQL index bloat | `collect.pg_index_bloat` + its time index (#2561): b-tree index bloat MEASURED via `pgstatindex`, not estimated. The estimator route is blind under this product's permissions — it needs `pg_stats`, which returns nothing to a `pg_monitor` role — while `pgstatindex` runs, because pgstattuple grants EXECUTE to `pg_stat_scan_tables`. `avg_leaf_density` is stored RAW and never converted to a percentage, and `skipped_reason` exists so a size ceiling can never masquerade as an absence of bloat |
| **V95** — PostgreSQL per-database attribution | `database_name` on `collect.pg_column_stats`, `collect.pg_index_bloat` and `collect.pg_extension_availability` (#2599), and `pg_extension_availability` now runs per database. Two of these collectors ran per database and could not say WHICH database a row described, so on a cluster carrying one schema in two databases their rows collided; the third read the per-database `pg_extension` catalog through a single connection and reported one database's answer for the whole server. Nullable with no backfill — a catalog-only change in PostgreSQL, and rows collected before this rung genuinely do not know where they came from |
| **V72** — Query Store plan map | `collect.query_store_plan_map` — `(server_id, database_name, plan_id)` → digest, so Query Store facts can reference plan XML they no longer carry once that content moves into the shared `query_plan_dim`. Plan XML was stored INLINE on `query_store_stats` at roughly 5x redundancy. Not a hypertable: one row per distinct plan per database, so it is dimension-shaped and pruned on `last_seen` rather than by `drop_chunks`. Its `last_seen` is load-bearing — the dimension GC sweeps on timestamps rather than counting references, so ending the re-shipping also ends the liveness signal that used to keep those dim rows alive |
| **V73** — PostgreSQL statement text | `collect.pg_statement_text` — `(server_id, queryid)` → statement text, refreshed hourly, so `get_pg_top_queries` returns something readable (#2219). `pg_statement_stats` stores no text because `showtext` is a real per-collection cost and normalized text is highly repetitive; but `queryid` is NOT stable across a major version upgrade, so without this the stored history joins to nothing after one — a list of integers that used to be your slowest queries, unrecoverable because the live view no longer holds the old ids. Text is INLINE rather than a `query_text_dim` digest: the dimension route needs the GC liveness interlock whose failure mode is silently missing text, and inline cannot dangle. Not a hypertable and not a collector table, exactly like V72 — a bespoke upsert path, pruned on `last_seen` with a margin that makes text OUTLIVE the statistics referencing it |
| **V76** — Query Store health | `collect.query_store_health` + its index + the `v_query_store_health` passthrough view — the #2319 per-database `sys.database_query_store_options` collector's store table: actual vs desired state (the cap-hit READ_ONLY transition and its readonly_reason), current vs max storage, cleanup thresholds, and the runtime-stats interval length. A fresh store gets the table from V1's generated schema; V76 is what an already-existing store gets |
| **V77** — Activity-driven plan fetch | Three strokes behind #2312's reshape of the Query Store plan/text fetch: `query_store_plan_map.digest` goes **nullable** (a plan whose XML the engine cannot persist gets a NULL-digest map row — the content-less marker that stops the probe re-selecting it forever), `query_store_text` gains `query_hash` (the Query Store reset detector: an id whose stored hash differs from the live one names a DIFFERENT statement now and its text refetches within one cycle), and the retired `planwm:`/`textwm:` watermark state rows are deleted wholesale. The fetch itself no longer walks the plan catalog by watermark — the cycle's collected rows name their plans, the store answers which are missing, and only those are fetched |

All timestamps in the store are **naive-UTC** `timestamp` columns — the product-wide cross-store contract (Lite's DuckDB does the same).

### Reading the store directly (plan XML is compressed)

The store is deliberately queryable — it is documented PostgreSQL with named tables, and people build
panels and reports straight off it. One thing will surprise you if you do that: **execution-plan XML is
stored gzip-compressed**, and has been since v3.4.0.

`collect.query_plan_dim` holds plan content once, keyed by a content digest, in one of two columns:

| Column | Meaning |
|---|---|
| `query_plan_gz` (`bytea`) | The plan XML, **gzip-compressed** (magic bytes `1f 8b`). This is where new plans go. |
| `query_plan_xml` (`text`) | Uncompressed plan XML. Nullable since v3.4.0; only rows written by older builds still carry it. |

So a consumer that reads only `query_plan_xml` silently returns nothing for anything collected by a
current build. **`query_plan_xml IS NULL` does not mean "no plan" — it means look at `query_plan_gz`.**

Both apps and every MCP tool decompress client-side, so nothing in the product is affected; this note
exists because the change altered the contract for direct SQL consumers and the v3.4.0 release notes did
not say so. That omission is on us.

**Getting the XML back.** PostgreSQL has no built-in gunzip for arbitrary `bytea`, so a plain-SQL
consumer cannot decompress in the database without an extension. Practical options, in the order most
people should try them:

1. **Ask the product for the plan** rather than the store — `get_plan_xml` over MCP, or the Viewer's
   plan surfaces. Both hand back decompressed XML and neither cares how it is stored.
2. **Decompress in your client.** Any language's gzip library reads the bytes directly. Python:
   `gzip.decompress(row['query_plan_gz']).decode('utf-8')`. PowerShell: a `GZipStream` over a
   `MemoryStream` of the bytes. C#: the same, which is exactly what the apps do.
3. **Ship a UDF into your own store** if your tooling is SQL-only (Grafana, a reporting view). A
   `plpython3u` function works and has been used in the field, at the cost of an untrusted-language
   extension in a monitoring database — weigh that against how much you need it.

Why compressed at all: plan XML dominates store size, and gzip took a production dim table from 885 GB
of raw text to 64 GB — a 14x reduction. That is the tradeoff being made on your behalf.

### TimescaleDB (Optional, Auto-Adopted)

At startup, right after migration, the service attempts `CREATE EXTENSION IF NOT EXISTS timescaledb` and checks `pg_extension`:

- **Present** — every collector table is converted to a hypertable (partitioned on its own time column into **1-day chunks**, existing rows migrated) and gets a compression policy: chunks older than **1 day** compress automatically (segmented by `server_id`), checked **hourly**. The hourly tick is passed explicitly because TimescaleDB's own default is **12 hours** for 1-day chunks — that is a second, separate wait *after* a chunk is already eligible, and on a field store it left the newest closed chunk (always the least-compressed data on disk) uncompressed for most of a day. Stores created before this shipped are retuned automatically on the next service start. The short intervals matter at the 1-minute collection cadence — a chunk cannot compress until it closes and then ages, so TimescaleDB's 7-day default left the store fully uncompressed for ~2 weeks (a near-idle 5-server fleet still reached ~1 GB in a couple of days); 1-day chunks + 1-day compress keep it compact (measured ~16.7x on perfmon, ~6.4x on the plan-XML-heavy query_stats). Compressed chunks stay fully queryable — this is Darling's archival tier, the centralized-store answer to Lite's Parquet archive. Everything is idempotent and re-converges on every service start; a table that fails conversion stays a plain table and keeps working.
- **Absent** — the service logs one Information line and runs in plain-PostgreSQL mode, which is a fully supported configuration, not a degraded one.

`IF NOT EXISTS` short-circuits before privilege checks, so a store whose administrator pre-created the extension works for a service login that could never create it.

### Background workers: sizing an unmanaged store, and what happens if you don't

**This section is for bring-your-own PostgreSQL only.** In managed mode the service sizes these itself on every start and there is nothing to do.

Every TimescaleDB policy — compression, retention, continuous-aggregate refresh — runs in a **background worker**, and a policy that cannot get a worker does not run. PostgreSQL's stock `max_worker_processes = 8` is far below what this store needs, so an unmanaged store left at the defaults silently does very little compressing.

Managed mode derives the two settings from the live hypertable count, and an unmanaged store wants the same numbers:

```
timescaledb.max_background_workers = <hypertables> + 2
max_worker_processes               = 3 + timescaledb.max_background_workers + 8
```

Today that is **57** and **68** for 55 hypertables (the 54 collector tables plus `collection_log`). The `+ 2` is not slack — it is exactly TimescaleDB's own two built-in jobs, `policy_telemetry` and `policy_job_stat_history_retention`, so a fully migrated store holds precisely one job per worker:

```sql
SELECT proc_name, count(*) FROM timescaledb_information.jobs GROUP BY proc_name;
```

Both settings need a **server restart** (`max_worker_processes` is restart-only — a reload leaves the old value serving), and the hypertable count grows as collectors are added, so re-check it after a major upgrade rather than pinning 57/68 forever.

**One store per cluster is the assumption.** `timescaledb.max_background_workers` is a **cluster-wide** pool shared by every database, while the derivation above is **per-store**. Managed mode puts one store on one cluster so the two coincide, but if you run **N Darling stores on one PostgreSQL cluster** — or share the cluster with any other TimescaleDB database — multiply both numbers by N. Each database with the extension loaded also permanently holds a scheduler slot out of that same pool, so the sharing starts before any policy fires.

**What under-provisioning looks like.** The postmaster log (`pg.log`, or wherever your cluster logs) is where it shows up, in one of two shapes:

```
WARNING:  failed to launch job 1042 "Columnstore Policy [1042]": out of background workers
WARNING:  ... failed to start a background worker
```

The first means TimescaleDB's own pool is full; the second means PostgreSQL's is. Neither is fatal and neither corrupts anything — the job is skipped and retried on its next schedule, so **light contention is benign** and you may see a couple of these without any consequence. It matters at scale: when the shortfall is persistent rather than momentary, compression falls behind the 1-day policy and the store grows at its uncompressed rate (measured compression is ~16.7x on perfmon and ~6.4x on the plan-XML-heavy `query_stats`, so the gap is large), retention stops reclaiming chunks, and the jobs that keep losing the race are the ones whose backlog is worst. `timescaledb_information.job_stats` is the check that settles it — a healthy store shows successes with no failures:

```sql
SELECT sum(total_runs), sum(total_successes), sum(total_failures) FROM timescaledb_information.job_stats;
```

### Retention

A purge runs on the first sweep after startup and then daily, driven by the same shared per-collector horizons Lite uses:

| Horizon | Tables |
|---|---|
| 7 days | `query_snapshots`, `waiting_tasks`, `running_jobs` |
| 30 days | Most collector tables (wait/query/procedure/Query Store stats, CPU, memory, file I/O, tempdb, perfmon, deadlocks, blocking, sessions, config snapshots), plus `collection_log` and `analysis_findings` |
| 90 days | `database_size_stats`, `index_object_stats`, `pvs_stats` |
| 365 days | `server_properties` |

On plain PostgreSQL the purge is DELETE-based. With TimescaleDB it switches to `drop_chunks` — a metadata-only detach of whole expired chunks (rows inside a partially-expired chunk survive until the whole chunk ages out; up to ~1 day of grace at the 1-day chunk width), with a per-table DELETE fallback for any table that is not a hypertable. Failure-isolated per table: one stuck purge is logged and retried the next day without stopping the sweep.

#### The rollup tiers, on a TimescaleDB store

The table above is the **collector** horizon, and for three tables it is not the binding one. `query_stats`, `procedure_stats` and `query_store_stats` are rolled up into hourly and daily continuous aggregates, and a separate tiered policy drops their raw chunks at **4 days** — the aggregates hold the history past that point, and a read is routed to whichever tier covers the window it asks for. On a store without TimescaleDB none of this exists and the collector horizons above are the whole story.

| Tier | Horizon |
|---|---|
| Raw `query_stats`, `procedure_stats`, `query_store_stats` | 4 days |
| Hourly **history** rollups | 90 days |
| Daily **history** rollups | kept indefinitely (no policy) |
| Baseline aggregates | 35 days |
| `query_store_stats_interval_hourly`, `query_store_stats_interval_daily` | 7 days, 10 days |

Every one of these is visible in `timescaledb_information.jobs`, and the last row is the one worth knowing before you look: those two are **internal dedup plumbing, not history**. The corrected Query Store rollups are built from them, nothing reads them directly, and each horizon is sized only to outlive whatever gates on it — 7 days has to exceed raw's 4, and the 10-day layer has to outlive the 7-day one it consumes. So a horizon SHORTER than the tier above it is correct there and costs no history, which is the opposite of how it reads at a glance. The service's startup summary line names all of these for the same reason.

No raw tier is ever dropped before the aggregate that preserves it has caught up: each policy is created paused, and arms itself only once its rollup demonstrably covers what the tier below holds.

### Logs

The service's PRIMARY log is a **rolling file** under `%ProgramData%\PerformanceMonitorDarling\logs\darling-service_yyyyMMdd.log` — every collector run line, connect edge, reload notice, warning, and error lands there (buffered writes, one file per day, 14-day retention, and a logging failure can never crash the service). Console runs write the same file plus console output.

Warnings and errors also go to the **Windows Application event log** (source `PerformanceMonitor Darling`) — but only if that event source exists. Registering an event source requires elevation, and the recommended `NT SERVICE` virtual account cannot do it, so run the `New-EventLog` line in the install steps above (or any elevated run of the exe) once; without it, Windows silently drops the events and the file log is your only surface. Collection outcomes are also queryable in the store itself — `collection_log` records every collector run per server with status and timings, and the viewer's Collection Health tab renders exactly that.

### The Viewer

`PerformanceMonitor.Darling.Viewer.exe` is a WPF app that talks **only to the PostgreSQL store** — it never connects to your monitored SQL Servers. It reads the same `darling.json` the service uses, but only the `postgres` section, resolved in the same order (explicit path, then `DARLING_CONFIG`, then `darling.json` next to the binary) plus one viewer-only fallback: the parent directory, so the release zip's layout — viewer in a `viewer\` subfolder, `darling.json` beside the service exe — works with no setup. A viewer seat on **another machine** is set up by exporting that config folder from the service host — see [Connect a Remote Viewer](#connect-a-remote-viewer). If the file is missing it shows a hint instead of crashing.

At startup the viewer writes **which of those rules won**, the absolute path it produced, and whether that file exists to `%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_yyyyMMdd.log` — before it tries to read the file, so a missing or malformed one still says where it looked. Once the file loads it adds a non-secret summary of what it parsed (host, port, username, database, SSL mode, search path, whether the connection string was read verbatim or derived from `postgres.managed`, and the certificate — the value as written, the absolute path it resolves to, the folder a relative one was anchored to, and whether that file exists). Credentials are never written. The same block appears in the connection-failure window with a **Copy details** button — see [Troubleshooting](#troubleshooting).

The layout mirrors the Lite desktop app: a left sidebar lists the servers from the `servers` registry the service maintains, and the top tab strip holds three fixed **aggregate tabs** — Overview, Recommendations, and Alerts — alongside a closable **per-server tab** for each server you open. Overview (the all-servers server-cards grid) and Alerts (the all-servers alert history) span every server; Recommendations has its own server selector, independent of the sidebar. **Double-click a server** in the sidebar — or **double-click its Overview card** — to open (or focus) its tab, and close it with the × on the tab header; an empty-state panel is shown until the store has at least one server.

Each per-server tab has fourteen inner tabs:

| Inner tab | Contents |
|---|---|
| **Overview** | Five correlated, X-axis-synced timeline lanes over the last 24 hours — CPU % (SQL Server vs SQL+other Total), total wait ms/sec, blocking + deadlocking, buffer pool MB, and file-I/O latency — each with a ±2σ baseline band and anomaly markers, all sharing one crosshair so a spike in one lane lines up against the others |
| **Wait Stats** | A searchable wait-type picker (poison + usual-suspect + `PAGELATCH_` defaults, checked-to-top, a 30-type selection guide) beside a per-**type** trend chart for the checked types over the last 24 hours, with a Wait Time (ms/sec) ↔ Avg Wait Time (ms/wait) metric toggle — the per-type companion to the Overview's single total-wait lane |
| **Queries** | Six sub-tabs over the last 24 hours — **Performance Trends** (a 2×2 of per-second trend charts: query duration, procedure duration, Query Store duration, execution count), **Active Queries** (the ~26-column filterable snapshot grid of captured running queries with a time-range slicer, a **Latest Snapshot** button that re-reads the newest stored capture, and per-row Estimated / Actual plan buttons that open the stored plan in the Plan Viewer), **Top Queries by Duration** (the full query-stats grid with in-grid bar cells for executions/CPU/duration/reads and a CPU-by-database breakdown), **Top Procedures by Duration**, **Query Store by Duration**, and **Query Heatmap** (query counts per 5-minute bin × per-execution magnitude bucket, by a chosen metric; right-click a cell to drill into Active Queries for that window) — the three grids each carry a time-range slicer (drag to narrow the window) and a shared **Compare** control that overlays the current window against a baseline period (yesterday, last week, or same day last week), flagging new and vanished queries |
| **Plan Viewer** | Hosts execution plans as closable sub-tabs (the shared plan-viewer control, the same one Lite and the Dashboard use). Right-click a **Top Queries** or **Query Store** row and choose **View Plan** to open the plan the service captured for it (`query_stats.query_plan_xml` / `query_store_stats.query_plan_text`); Top Queries rows also carry a **Query Plan** column whose Download button saves the stored plan as a `.sqlplan` file (enabled only when a plan was captured). Top Procedures and the blocking / deadlock reports deliberately do **not** surface a plan here — procedure plans aren't stored, and blocked-process / deadlock rows carry only a `sql_handle` (not plan XML); resolving either to a plan needs a live SQL connection the viewer never makes. "Get Actual Plan" (a live re-execution) is likewise out |
| **CPU** | Raw per-sample CPU utilization (SQL Server vs other processes) over the last 24 hours — every ring-buffer sample, full-bleed as two series; the Overview's CPU lane plots the same raw samples compactly (SQL vs SQL+other Total) with a baseline |
| **Memory** | Four sub-tabs over the last 24 hours — **Overview** (a summary strip of physical / SQL Server / target / buffer pool / plan cache / page-file memory plus the system memory state and model, over a Total-vs-Target-vs-Buffer-Pool memory trend with a memory-grants overlay), **Memory Clerks** (a searchable clerk-type picker — top-5 default, checked-to-top, clear-only-the-filtered — beside a per-clerk memory trend for the checked clerks with a non-buffer-pool total and top-clerk summary), **Memory Grants** (per-resource-pool grant sizing — available / granted / used MB — and activity — grantees / waiters / timeouts / forced grants), and **Memory Pressure Events** (hour-bucketed stacked bars of `RING_BUFFER_RESOURCE_MONITOR` pressure, SQL Server vs OS, medium vs severe) |
| **File I/O** | Two sub-tabs over the last 24 hours — **Latency** (per-file read and write latency, with a dashed queued-I/O overlay) and **Throughput** (per-file read and write MB/s) — the top 10 files by activity |
| **tempdb** | Three stacked charts over the last 24 hours — space usage (user / internal objects / version store), total allocated size, and per-file I/O latency |
| **Blocking** | Four sub-tabs over the last 24 hours — **Trends** (lock-wait rate, blocking incidents, deadlocks), **Current Waits** (waiting-task duration by wait type, blocked sessions by database), **Blocked Process Reports** (the full ~25-column filterable grid — XE reports preferred with the always-on DMV blocking snapshot merged in as fallback, each row badged with its source, a time-range slicer, per-row report-XML save, and long-block highlighting; double-click or right-click **View Block Chain** to reconstruct and draw the blocking chain the row belongs to), and **Deadlocks** (one filterable row per process parsed from each deadlock graph, a slicer, per-row graph-XML save; double-click or right-click **View Deadlock Graph** to draw the deadlock graph) |
| **Perfmon** | A searchable counter picker with the shared counter packs (General Throughput, Memory Pressure, CPU / Compilation, I/O Pressure, TempDB Pressure, Lock / Blocking) beside a per-counter delta trend for the checked counters (up to 12) over the last 24 hours |
| **Running Jobs** | Latest snapshot of currently-running SQL Agent jobs — start time, current vs average vs p95 duration, % of average, and a highlighted row when a job is running past its p95 (a store-derived banner appears when the service's login lacks msdb access) |
| **Configuration** | Four column-filterable snapshot grids of the server's latest capture — server configuration (`sys.configurations`), database configuration (28 columns of `sys.databases`), database-scoped configuration, and trace flags |
| **Daily Summary** | A one-row roll-up of the selected day (default today, UTC, with a date picker) — total wait time, the top wait type, distinct query count, deadlock / blocking-event / high-CPU-sample counts, collector errors, and an overall health band |
| **Collection Health** | Three sub-tabs — **Health Summary** (a 7-day per-collector roll-up: run / success / error counts, failure rate, average duration, last success / run / error, and a health band of HEALTHY / WARNING / STALE / FAILING / NEVER_RUN / NO_PERMISSIONS — double-click a collector to open its full run history), **Collection Log** (the recent run log with per-run SQL and store-write timings and row counts), and **Duration Trends** (a per-collector success-duration scatter) |

The three aggregate tabs — **Overview** and **Alerts** span every server; **Recommendations** has its own server selector, independent of the sidebar:

| Tab | Contents |
|---|---|
| **Overview** | A card per registered server (all servers, not the sidebar selection): server name + status dot, CPU (total non-idle with the SQL-only number alongside), memory, blocking and deadlock counts over the last hour, and last-collection time, each colour-banded (CPU ≥ 80% red / ≥ 50% amber / green; blocking and deadlocks red-or-amber when present) with a red **Offline** overlay. Status is derived from **collection freshness** — the newest `collection_log` age — rather than a live ping (the viewer never connects to the monitored servers): fresh is Online, older than twice the fastest collector's one-minute cadence is a Warning, and no recent collection is Offline. **Double-click a card** to open that server's tab. Refreshes every 30 seconds |
| **Recommendations** | The latest analysis run's findings for the tab's **own selected server** — a server selector independent of the sidebar, a Refresh button, and a status line showing the last analysis time — re-skinned to Lite's advise-only **card** design: a scrollable list of collapsible **incident** sections, each holding severity-banded cards (a severity badge, the affected `[database]`, the title, and the advice). Every card offers **Ask AI** (copies an MCP investigation prompt referencing `analyze_server` / `get_analysis_findings`); a card whose stored remediation carries a copy-paste statement also offers **Copy fix** (copies the suggested T-SQL). Advise-only — the viewer never applies anything, and there is no mute affordance here (alert muting lives on the Alerts surface). There is no in-app "Generate now": the service runs analysis on its own 30-minute cadence, so the status line surfaces the last analysis time instead |
| **Alerts** | The full alert history from `config_alert_log` across **all servers** (newest first, selectable time range), with a Server column and a Server filter. Double-click a row (or **View Details**) for a modal detail window showing the alert's stored detail and structured advice / remediation / drill-down from its dedup-fingerprint context. **Dismiss Selected / Dismiss All** hide alerts from the view (a durable `dismissed` flag on `config_alert_log`); column filters, Copy Cell/Row/All, and Export to CSV match Lite's grid. Right-click to **Mute This Alert** or **Mute Similar** (metric-only), and a **Manage Mute Rules** button opens the mute-rule editor |

Only the visible tab loads (Lite's visible-only rule). The Alerts tab and the visible server tab's active inner tab refresh every 60 seconds; the Overview refreshes on its own faster 30-second timer (Lite's Overview cadence); and **Recommendations** refreshes on tab activation, its Refresh button, and its own server-selector change only, never on the timer — its findings change on the service's 30-minute analysis cadence, so a 60-second auto-refresh would be pointless churn (and would reset the incident expanders under the reader), matching Lite.

The viewer is read-only over collected data, but it does perform a small set of **user-initiated writes** — and those go straight to the PostgreSQL store, which is the coordination point (the service honors them on its next read; there is no viewer-to-service channel). From the Alerts tab, creating a mute rule from an alert (**Mute This Alert** / **Mute Similar**) or adding, editing, toggling, deleting, or purging one via **Manage Mute Rules** writes `config_mute_rules` (a rule scopes to a server by name, exactly as Lite's mute rules do); and **dismissing alerts** sets the `dismissed` flag on `config_alert_log` so they drop out of the Alert History view (a single atomic UPDATE — Darling has no parquet archive tier, so there is no dismissed-archive sidecar). The viewer never writes collector data.

### Restart Semantics

The service is built to restart cleanly, any time:

- **Delta continuity** — delta-based collectors (wait stats, file I/O, perfmon, memory grants) re-seed their baselines from the store at startup, so the first cycle after a restart produces real deltas instead of zeroes.
- **Alert no-re-fire** — edge-trigger watermarks and the failed-job watermark persist in `config_edge_trigger_watermarks`, and per-alert cooldowns re-seed from `config_alert_log`, so a restart does not replay alerts you already received.
- **Idempotent store setup** — migrations are versioned and skip what is already applied; TimescaleDB conversion and compression policies re-converge as no-ops.
- **Per-connect snapshots** — the on-connect config snapshot collectors run once per (re)connect, mirroring Lite's server-open behavior.
- Mute rules (`config_mute_rules`) load once at service startup — restart the service after adding rows.

A monitored server that is down is retried every 60 seconds forever; a collector that errors is logged and retried at its next scheduled time; a mid-cycle connection-level failure forces a clean reconnect and re-probe. The loop never dies for one bad cycle.

---

## Connect a Remote Viewer

For the person sitting at a machine with **nothing installed on it**, whose only goal is looking at a Darling service that already runs somewhere else. Three steps, nothing to hand-edit.

**The one service-side prerequisite.** The store has to be reachable from your LAN — a `postgres.network` block on the service host, which the `--configure-network` wizard writes for you. A store still on its loopback default accepts no remote viewer at all, and no amount of viewer-side configuration changes that. See [Store endpoint (viewer over the LAN)](#store-endpoint-viewer-over-the-lan) for that side; everything below assumes it is done.

### 1. Export the handoff folder (on the service host)

```
PerformanceMonitor.Darling.Service.exe --export-viewer-config
```

It writes the viewer machine's **whole configuration folder** — connection string resolved, certificate copied, every field documented in place:

```
viewer-config\darling.json    the complete viewer config: the resolved connection string and
                              "managed": false already set, every field explained in comments
                              IN the file
viewer-config\server.crt      the store's TLS certificate, the file the connection pins
viewer-config\README.txt      the same field reference in plain text, including the valid
                              "Root Certificate=" values and the one-line install instruction
```

The folder lands beside the service's own `darling.json` by default. Pass a directory to put it elsewhere (`--export-viewer-config D:\handoff`), and `--config <path>` if `darling.json` is not where the service would resolve it.

**The exported `darling.json` contains a live database password** — that is what the viewer authenticates with. The verb says so before it writes, ACLs the file to SYSTEM + Administrators + the account running it + INTERACTIVE (the Viewer reads it interactively, the same posture as the admin/viewer credentials), and confirms the ACL took: if the secret is still readable by ordinary users it says so and exits non-zero. Copy the folder over a channel you trust and keep it ACL'd on the viewer machine.

The verb refuses rather than clobbers: it will not export into the **service's own config directory** (that would overwrite the service's `darling.json` with the viewer's, destroying its servers, encrypted passwords and tokens), will not overwrite a file it did not write, and will not follow a junction or symlink. A destination it cannot use is named in the refusal.

### 2. Copy the folder to the viewer machine

Put the three files **next to `PerformanceMonitor.Darling.Viewer.exe`** — that works with nothing edited. (The Viewer ships in the same release zip as the service, in its `viewer\` subfolder; from source it is `dotnet build Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj -c Release`.)

To keep the folder somewhere else instead, point the `DARLING_CONFIG` environment variable at the exported `darling.json`. That works unedited too: a bare or relative `Root Certificate` resolves against **the folder holding `darling.json`**, so the `server.crt` beside it is found wherever you keep the folder ([#1970](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1970)). Keep the three files together and the folder can live anywhere.

### 3. Start the Viewer

That is the whole setup. Re-run the export after a credential or certificate rotation — the store's certificate regenerates when its bind IP changes — and copy the folder over again; it replaces its own previous output without ceremony.

### If it does not connect

The failure window carries a **Configuration this viewer used** block naming the `darling.json` it actually read, which rule picked it, and the host, port, username, database, SSL mode, search path and certificate path it parsed — with a **Copy details** button, and the same lines in `%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_yyyyMMdd.log`. Read it before changing anything: it separates *the viewer read a different file than you edited* from *it read your file and a value in it is wrong*. It never contains a password. See [Troubleshooting](#troubleshooting) for the individual failures.

### Manual configuration (fallback)

Only for the case where you want the connection string itself — to paste into a config that already exists, or to check what the viewer will dial. The export above is the supported path; this one is the same values, assembled by hand.

```
PerformanceMonitor.Darling.Service.exe --print-viewer-connection
```

It decrypts the `network.role` credential and prints a paste-ready connection string plus the server certificate PEM. Every warning is printed **before** the payload, but the payload is still a **live database password on STDOUT** — redirect it to an ACL'd file or pipe it to the clipboard (`... --print-viewer-connection | clip`); do not leave it in shell scrollback, CI logs, or a screenshare. The minimal viewer `darling.json` it targets is bring-your-own mode with the string pasted in verbatim (the string is consumed as-is), and the emitted PEM saved where `Root Certificate` points:

```json
{
  "postgres": {
    "managed": false,
    "connectionString": "Host=192.168.1.205;Port=5641;Username=viewer;Password=...;Database=darling;Search Path=collect,config,public;SSL Mode=VerifyFull;Root Certificate=server.crt"
  }
}
```

`"managed": false` is not a typo next to the service's `"managed": true`: the flag says who **owns** the PostgreSQL, not who is connecting. A viewer left on `true` goes looking for a bundled local PostgreSQL that is not there. (The export sets it for you, which is the point.)

**`Root Certificate=` — what the field accepts.** It is a path to the PEM the connection validates the store's certificate against, and under `SSL Mode=VerifyFull` it is what makes the check meaningful. A relative value anchors to **the folder holding the `darling.json` the viewer read**, never the process working directory, so how the Viewer was launched cannot change the answer:

| Value | Resolves to |
|---|---|
| `server.crt` | that name in the folder holding `darling.json` — the exported layout, correct wherever the folder lives |
| `certs\server.crt` | same anchor, one level down |
| `C:\Darling\server.crt` | an absolute path, used exactly as written, for a certificate kept somewhere else |
| omitted | nothing viewer-side to pin against: the store's certificate must already chain to a root the machine trusts. A managed store's certificate is **self-signed**, so it never does — omitting the field there fails `VerifyFull` |

**Where the certificate comes from.** In managed mode the service generates `server.crt` / `server.key` **beside the data directory** (`%ProgramData%\PerformanceMonitorDarling\pg\` unless you set `postgres.dataDirectory`), with an IP SAN for the `network.listen` address and a DNS SAN for the machine hostname. It **auto-regenerates if the bind IP changes**, so verify-full keeps working after a `listen` change — and every viewer must then re-copy the new certificate, because an old copy stops matching. To rotate on demand, delete the pair beside the data directory; the service regenerates it on its next start.

**Bring-your-own PostgreSQL.** Darling generates no certificate — your PostgreSQL's TLS is yours to configure — so `Root Certificate` points at the PEM that signed **your** server's certificate (the CA certificate, or the server's own certificate if it is self-signed), exactly the file you would hand `psql` as `sslrootcert`. The same relative-path anchoring applies, so keeping it beside `darling.json` is still the simplest layout.

**Plaintext at rest on the viewer machine.** However you get there, the connection string holds the role password in cleartext in that machine's `darling.json` (there is no client-side secret store yet). That is acceptable for the read-only `viewer` credential on a single-operator, ACL'd profile; if you use `role: "admin"`, treat that file as a secret and NTFS-ACL it to your account. DPAPI-encrypting the viewer's BYO connection string is future hardening, out of scope today.

---

## Troubleshooting

**"Cannot load configuration"** (critical, service idles) — no `darling.json` was found at the resolved path. The message names the path it tried; copy `darling.sample.json` there or point `DARLING_CONFIG` at your file.

**"Configuration problem: ..."** (critical, service idles) — validation failed. The messages are literal and per-field, e.g. `postgres.connectionString is required.`, `servers must contain at least one entry.`, `server 'X': host is required.`, `server 'X': sql auth requires username.`, `server 'X': sql auth requires encryptedPassword (preferred; see --encrypt-password) or password.`, `server 'X': auth must be 'integrated' or 'sql'`. Fix the file and restart the service.

**"Cannot reach or migrate the Postgres store"** (critical, service idles) — the store connection string is wrong, PostgreSQL is down/unreachable, or the login cannot create tables. Collection does not start until this succeeds; fix and restart.

**"uses a plaintext password in darling.json"** (warning, every connect) — you set `"password"` instead of `"encryptedPassword"`. It works, but run `--encrypt-password` on the service machine and switch.

**DPAPI decrypt fails after moving darling.json** — `encryptedPassword` blobs are machine-bound (DPAPI LocalMachine). Re-run `--encrypt-password` on the new machine.

**"Failed to ensure XE sessions"** — the login lacks `ALTER ANY EVENT SESSION` (or the database-scoped equivalent on Azure SQL Database). Deadlock and blocked-process collection read zero rows until the sessions exist; grant the permission or have an administrator create/start `PerformanceMonitor_Deadlock` and `PerformanceMonitor_BlockedProcess`. "Already exists / already started" XE errors are logged as benign and mean the sessions are up.

**Blocked-process reports empty** — the blocked-process threshold may still be 0. On AWS RDS set `blocked process threshold (s)` via a Parameter Group (the `sp_configure` bootstrap cannot run there); on Azure SQL Database the threshold is fixed at 20 seconds. Blocking stays visible either way through the always-on DMV blocking snapshot.

**`PERMISSIONS` rows in `collection_log`** — that collector's reads were denied (SQL errors 229/297/300). Check the [permissions](#permissions-on-monitored-servers); the collector retries every cycle and recovers as soon as the grant lands.

**"Skipping recently-failed-job check"** (info) — the login cannot read `msdb.dbo.sysjobs` / `sysjobhistory`, so failed-job alerts are skipped. Expected for minimal-privilege monitoring logins. If you want job alerts, add the direct msdb table `SELECT`s from the [permissions](#permissions-on-monitored-servers) section — **not** `SQLAgentReaderRole`, which gates the `sp_help_job*` procedures this product never calls and leaves the reads failing with error 229.

**"TimescaleDB setup failed — continuing in plain-PostgreSQL mode"** (warning) — the extension exists but conversion hit a problem. Everything still works (DELETE-based retention, plain tables); conversion is retried on the next service start.

**"out of background workers" / "failed to start a background worker" in the postmaster log, or the store keeps growing despite compression** — bring-your-own stores only: the cluster has fewer worker slots than the store has policies, so compression and retention jobs are being skipped. An occasional one is benign (the job retries on its next schedule); persistent ones mean the store is effectively uncompressed. Size the two settings and restart the server — see [Background workers](#background-workers-sizing-an-unmanaged-store-and-what-happens-if-you-dont), and multiply them if the cluster hosts more than one store. `timescaledb_information.job_stats` tells you whether jobs are actually succeeding.

**"Why are there 40+ postgres.exe processes?"** — the count is three populations, and only one is client connections: (1) PostgreSQL's own system processes (postmaster, checkpointer, WAL/background writers, autovacuum, stats); (2) **TimescaleDB background workers** — the managed conf sizes `timescaledb.max_background_workers` to the hypertable count + 2 (≈57), and every RUNNING compression/retention policy job is its own process, so the count legitimately surges during checkpoint/compression waves and falls back when they finish; (3) client backends — the service's pools are capped at 24, the co-located viewer's at 10. Decompose it live with: `SELECT backend_type, count(*) FROM pg_stat_activity GROUP BY backend_type ORDER BY 2 DESC;` — and remember Windows charges the shared buffer segment to every attached process's working set, so per-process memory numbers cannot be summed.

**query_store bursts every ~15 minutes** — two or three near-empty cycles, then one large one, is Query Store's own behavior, not a collector bug: the engine buffers in memory and flushes to its persisted tables on `DATA_FLUSH_INTERVAL_SECONDS` (default 900s), so the collector genuinely sees nothing new between flushes. Narrowing the collection interval will not smooth it. The per-database log lines show which database drove a burst.

**MCP client cannot connect** — MCP defaults to off. Enable it live from the Viewer's Settings (the checkbox writes the control plane; the service starts the endpoint within seconds, no restart), or set `mcp.enabled: true` in `darling.json` for a file-seeded install. If the log says `Port 5152 is already in use — MCP server not started`, change `mcp.port`. The MCP server binds to `localhost` only unless you opt into a LAN endpoint (see [Opt-in Network Endpoints (LAN)](#opt-in-network-endpoints-lan)); a remote client that gets 401 is missing or mismatching the required bearer token, and one that is refused before any response is outside the configured `allowFrom` CIDR.

**Recommendations tab says no findings** — analysis runs every 30 minutes per server but only once the store holds at least 24 hours of collected data for that server; a fresh install simply has not earned findings yet.

**The Viewer will not connect** — the failure window carries a **Configuration this viewer used** block naming the `darling.json` it read (and which rule picked it: an explicit command-line path, `DARLING_CONFIG`, beside the viewer, or the service root), plus the host, port, username, database, SSL mode, search path and certificate path it parsed. Read it before changing anything: the two faults it separates are *the viewer read a different file than you edited* and *it read your file and a value in it is wrong*. **Copy details** puts the whole block on the clipboard for a bug report, and the same lines are in `%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_yyyyMMdd.log`. It never contains a password.

**"Root Certificate ... exists: NO"** — with `SSL Mode=VerifyFull`, a **relative** `Root Certificate` path resolves against **the folder holding the `darling.json` the viewer read** — not the working directory, so how the viewer was launched no longer changes the answer. The diagnostics block prints that folder and the absolute path it actually opened; either put `server.crt` beside the config or make the `Root Certificate` value an absolute path. If you no longer have the certificate, re-run `--export-viewer-config` on the store host and copy the folder again (see [Connect a Remote Viewer](#connect-a-remote-viewer)) — it regenerates if the bind IP changes, so an old copy stops matching.

---

## How It Runs (Reference)

Fixed cadences, hardcoded on purpose:

| What | Cadence |
|---|---|
| Collector sweep loop | Every 15 seconds (each collector runs when its own shared schedule is due — most every 1 minute, some every 5, sizes hourly, index stats daily) |
| Alert evaluation | Every 30 seconds per connected server (Lite's overview cadence) |
| Scheduled analysis | Every 30 minutes per server, 120-second budget, analyzing the last 4 hours; findings persist to `analysis_findings` and high-severity ones notify through the configured channels |
| Retention purge | First sweep after startup, then daily |
| Reconnect attempts | Every 60 seconds while a server is unreachable |

---

## Managed Bundled PostgreSQL

With `postgres.managed = true` (the sample's default), the service runs its own bundled PostgreSQL 18 + TimescaleDB and a from-zero install needs no database provisioning at all. Windows only, like every DPAPI surface here.

```json
{
  "postgres": {
    "managed": true,
    "port": 5641,
    "dataDirectory": null
  }
}
```

**What first run does.** The service looks for `pg-runtime\pgsql\` beside its binary, extracting it from `pg-runtime.zip` when only the zip is present (deleting the extracted directory is therefore always safe — it self-heals). If the data directory has no cluster, it generates a 32-character random password, protects it with DPAPI LocalMachine into `pg-credential.dpapi` beside the data directory (credential first, so a crash mid-initdb never strands a cluster nobody can log into), then runs `initdb` with `scram-sha-256` auth, data checksums, and UTF8/C locale. A marker-guarded block appended to `postgresql.conf` preloads TimescaleDB, sets the port, and restricts listening to `127.0.0.1`; a second versioned block sizes background workers up for the per-hypertable compression jobs, DERIVED from the live hypertable count so it cannot go stale as collectors are added (`timescaledb.max_background_workers = hypertables + 2`, `max_worker_processes = 3 + that + 8` — today 57 and 68 for 55 hypertables; PostgreSQL's default of 8 workers cannot launch them); a third versioned block sizes memory from the host's physical RAM for the up-to-500-servers case (`shared_buffers = min(25% RAM, 1GB)`, `effective_cache_size = 75% RAM`, `maintenance_work_mem = min(max(5% RAM, 1536MB), 25% RAM, 2048MB)`, and a deliberately-modest per-connection `work_mem = clamp(RAM/512, 16MB, 64MB)` — on an 8 GB box that is `shared_buffers 1024MB` / `work_mem 16MB`; the stock 128 MB / 4 MB defaults are fine at small scale but bottleneck at fleet scale). Later blocks re-state single settings that field measurement moved: a fifth caps `shared_buffers` for the co-located store, a sixth turns on the log-rotation ring, and a seventh carries the `maintenance_work_mem` floor that TimescaleDB's compression sort runs on (measured at ~+70% compression throughput on a 16 GB-class host, plateauing by 1536 MB). `postgresql.conf` takes the LAST assignment of a setting, so these override without rewriting anything. Every append is re-checked on every start, so a crash between initdb and the append heals itself instead of silently degrading — and clusters initialized before a given block existed gain it on their next start (effective at the next PostgreSQL restart). Then `pg_ctl start`, `CREATE DATABASE darling`, and the normal startup path (migrations, TimescaleDB adoption — you should see `N/N collector table(s) are hypertables`, both numbers equal and equal to the collector count; a converted count BELOW the total means some table stayed plain and the line above it says which) continues exactly as in bring-your-own mode. The connection string is derived from the stored credential; the Viewer and the MCP host on the same machine derive it the same way, so nothing needs configuring there either.

**Why scram and not trust, even loopback-only.** Trust auth would hand superuser to any local code that can open a loopback socket — every other local user, and network-capable-but-not-filesystem-capable attack primitives like SSRF from a co-hosted app. With scram the credential travels on the wire, failed attempts are auditable, and access is confined to what can read the DPAPI-protected credential file. `listen_addresses = '127.0.0.1'` keeps the server unreachable off the machine on top — unless you deliberately opt into a LAN endpoint (see [Opt-in Network Endpoints (LAN)](#opt-in-network-endpoints-lan)), which reconciles `listen_addresses`, a `hostssl` pg_hba rule, and TLS on every start and is otherwise off.

**Lifecycle.** On shutdown the service stops the server (`pg_ctl stop -m fast`) **only when it started it**. A server that was already running — an operator's own `pg_ctl`, or a postmaster that survived a service crash — is adopted for connections but never stopped: you'll see `already running … will not stop it` in the log, and the service keeps collecting into it.

**The runtime zip.** `pg-runtime.zip` ships beside the service binary in packaged releases. Building from source, produce it once with `Darling\tools\fetch-pg-runtime.ps1` — it downloads the pinned EDB PostgreSQL 18 binaries and TimescaleDB, verifies their SHA256, prunes what the service doesn't need, and writes the zip to `Darling\artifacts\`; copy it next to the built service exe.

**Server log.** The bundled server's own log is `pg.log` beside the data directory — that's where PostgreSQL explains a refused start; bootstrap errors in the service log quote its tail.

## Security & Least-Privilege Roles

The store is split into two schemas so that no consumer connects with more privilege than it needs:

- **`collect`** — the collector hypertables (one per collector) plus the service-written, user-read metadata (`servers`, `collection_log`, `analysis_findings`, the `v_*` views). Read-only to everyone but the service.
- **`config`** — exactly the tables a human operator changes through the Viewer or MCP: `config_mute_rules`, `config_alert_log` (alert dismissals), `config_edge_trigger_watermarks`, and `analysis_muted`.

Table names are unchanged — only their schema moved — and the shared SQL keeps using the bare, unqualified names, resolved through `search_path = collect, config, public` (set as the database default and carried on the managed connection strings). This is deliberate: Darling's SQL is byte-identical to Lite's DuckDB SQL, and re-qualifying it would fork that twin.

**The roles.** The service still owns the store as the `darling` superuser (it does the DDL — migrations, hypertable conversion, retention). On top of that, **managed mode provisions three least-privilege login roles** (BYO provisions two — see below):

| Role | Privileges | Used by |
|---|---|---|
| `darling` | superuser / owner | the service (collection, migration, provisioning) |
| `admin` | SELECT on both schemas — **including** the secret columns, which the Settings window reads — plus INSERT/UPDATE/DELETE on `config` only. No statement timeout | the Viewer, by default (`connectAs: "admin"`) |
| `viewer` | SELECT on all of `collect`, and on `config` **minus the secret columns** of `config_monitored_servers` / `config_command` / `config_notification` (carved fail-closed, below) + INSERT/UPDATE/DELETE on `config.custom_views` only (the web composer's saved views). Runs under `statement_timeout = 15s` | a locked-down Viewer (`connectAs: "viewer"`), and the web dashboard |
| `mcp` | `viewer`'s exact read surface + INSERT on `collect.analysis_findings` / `config.analysis_muted` + INSERT/UPDATE/DELETE on `config.custom_views` (the custom-view tools) + the alert-tuning writes (INSERT/UPDATE/DELETE on `config.config_mute_rules`, UPDATE on `config.config_alert_settings`, and the `config_service` reload-beacon columns) + the server-onboarding writes (INSERT/UPDATE/DELETE on `config.config_monitored_servers` — the credential column stays SELECT-carved, so it can WRITE a password blob but never READ one back) | the store identity the opt-in MCP **network** endpoint connects as (managed only); dormant until MCP is exposed on the LAN |

`admin` cannot `DROP`, alter schema, touch `collect` data, or create objects — it can only do what the Viewer's mute-rule / alert-dismiss surfaces need. The `mcp` role is narrower still: it reads exactly what `viewer` reads (the secret config columns are carved out identically) and its writes are a small, enumerated set — the two analysis-table INSERTs (`analyze_server` + `mute_analysis_finding`), the single-table `config.custom_views` CRUD (the custom-view tools), the alert-tuning writes (`config.config_mute_rules` CRUD + a single-row `config.config_alert_settings` UPDATE, plus the two `config_service` beacon columns so a settings write's self-bump trigger can fire), and the server-onboarding writes (`config.config_monitored_servers` CRUD for `add_servers` / `remove_server` — its `config_monitored_servers` write fires the SAME `config_service` beacon trigger, already covered by that column grant) — so a token-holder on the network MCP endpoint can never reach the `config`-table service-credential pivot, the secret columns, or a service flag like `paused`. Even on `config_monitored_servers`, which it may write, the `encrypted_password` column stays in the fail-closed secret carve, so `mcp` can WRITE a credential blob (onboarding) but can never READ one back. `ALTER DEFAULT PRIVILEGES` means new collector tables auto-inherit SELECT for `admin`/`viewer`, so the model never drifts as collectors are added (every `mcp` write is an explicit single-table/single-column grant, deliberately not schema-wide).

**Managed mode** provisions all of this automatically on every start (idempotent and self-healing), generating a per-role DPAPI-LocalMachine credential — `pg-admin-credential.dpapi`, `pg-viewer-credential.dpapi`, and `pg-mcp-credential.dpapi` beside the data directory, same posture as the owner's `pg-credential.dpapi`. Nothing to configure beyond `connectAs`.

**Credential file protection.** DPAPI LocalMachine scope is deliberate (the service writes the credential, a *different* interactive user's Viewer reads it), which means the machine-bound blob is decryptable by anything that can *read* the file. So the credential files are locked down with an NTFS ACL that strips the inherited world-read `%ProgramData%` would give them:

| File(s) | Readable by |
|---|---|
| `pg-credential.dpapi` (superuser), `pg-mcp-credential.dpapi` (the network MCP role) + the transient init pwfile | SYSTEM, Administrators, the service account — **not** interactive users |
| `pg-admin-credential.dpapi`, `pg-viewer-credential.dpapi` | the above **+ `NT AUTHORITY\INTERACTIVE`** (the operator's Viewer) |

`pg-mcp-credential.dpapi` sits with the superuser (non-interactive) rather than with the Viewer's credentials because only the in-service MCP host reads it — never an interactive Viewer.

The principal model assumes the **single-operator VM** this edition targets: `INTERACTIVE` == the operator, so the admin/viewer credentials are readable by the Viewer with zero configuration, while non-interactive local code (other services, sandboxed/SSRF socket primitives, scheduled tasks) and the superuser credential are excluded outright. On a shared machine where untrusted users log on interactively, tighten those two files to the specific operator account by hand. The service also refuses to trust a credential file that isn't owned by SYSTEM/Administrators/itself (closing a pre-plant attack), and regenerates an untrusted role credential.

**A read-only (`viewer`) Viewer degrades gracefully.** It probes its own privileges on connect (`has_table_privilege`), so the mute-rule Add/Edit/Toggle/Delete/Purge buttons and the alert Dismiss / Dismiss All buttons are hidden or disabled, and any write that still slips through returns a clear "read-only connection" message instead of an error.

**Bring-your-own PostgreSQL.** The schema split runs everywhere (it's a migration — the service applies it on startup and best-effort sets the database `search_path`; if your collection login can't `ALTER DATABASE`, run that one statement yourself as the owner). Role provisioning is managed-only, so for BYO you create the roles yourself, once, with the shipped script:

```
psql -h <host> -U <owner> -d darling -f Darling/tools/provision-roles.sql
```

Edit the two password placeholders (and the database/owner names if yours differ) first. Then point a read-only Viewer's `connectionString` at the `viewer` role. **That script is the authoritative grant list for a BYO store** — it is what actually runs, the table above is its summary, and an `ALTER DEFAULT PRIVILEGES` in it means a store gaining collectors later needs no re-grant. Re-run it after a schema upgrade to cover new tables. **It creates two login roles — `admin` and `viewer`** — the two the Viewer connects as. Managed mode creates a third, `mcp`, but BYO deliberately does not: the MCP **network** endpoint (the only consumer of the `mcp` role) is managed-mode-only, and a BYO operator governs their own PostgreSQL's network exposure. If you expose MCP through your own reverse proxy against a BYO store, point it at whichever least-privilege role you choose (the `viewer` role covers the read tools; `analyze_server`'s finding persistence and `mute_analysis_finding` need INSERT on `collect.analysis_findings` / `config.analysis_muted`).

## Opt-in Network Endpoints (LAN)

By default all three network surfaces bind **loopback only** — the store to `127.0.0.1`, the MCP server and the web dashboard to `localhost` — exactly as they always have. Three optional, independent opt-ins let a remote viewer, MCP client, or browser on your **trusted LAN** reach them. This is a home-lab / trusted-subnet feature: **never expose any of these endpoints to the internet.** All three are **managed-mode only** (in bring-your-own mode your own PostgreSQL / reverse proxy governs exposure, and the config is ignored with a warning), and all three are **fail-closed** — any invalid or incomplete field degrades that endpoint back to loopback and logs a critical line rather than exposing it. Removing the config on the next restart closes the box again.

### Guided setup (`--configure-network`)

The fastest path is the interactive wizard — run it on the **service host**:

```
PerformanceMonitor.Darling.Service.exe --configure-network
```

It shows the current exposure (read from the service's own resolvers), then walks you through the **store**, **MCP**, the **web dashboard**, any comma combination (e.g. `1,3`), or all three at once (or a **disable** that removes all exposure). Every answer is validated **by delegation to the exact checks the running service fail-closes on**, so the wizard can never write a config the service would refuse — it re-prompts with the resolver's own reason. It generates the MCP bearer / web access tokens for you (DPAPI-protected; each plaintext is printed once, so save it then), edits `darling.json` **in place preserving every comment** behind a timestamped `darling.json.bak-<timestamp>` backup, prints the scoped firewall command(s), the `--export-viewer-config` handoff, and the web dashboard's browser login URL (`http://<listen>:<port>/?token=...`), and offers to restart the service to apply. `install-darling.ps1 -Network` runs it automatically right after the install reaches Running. The manual field reference below documents exactly what it writes.

### Firewall rules (`--configure-firewall`)

The service runs as `NT SERVICE\PerformanceMonitor Darling`, an unprivileged virtual account that **cannot create Windows Firewall rules** — and should not be able to. So the rules are managed from the elevated install instead: `install-darling.ps1` runs `--configure-firewall` for you (before the first start, and again after `-Network`), and `uninstall-darling.ps1` removes them. Run it by hand after any edit to a `network` block:

```
PerformanceMonitor.Darling.Service.exe --configure-firewall
```

Run **elevated**. It reconciles all three scoped rules — store, MCP, web dashboard — against `darling.json` in one pass: it opens the port for every surface that really is exposed and removes the rule for every surface that is not, so it also cleans up after an exposure you turned back off. It is idempotent (safe on every upgrade).

The **port** each rule is named for comes from the control plane, not from the file. `mcp.port` / `web.port` in `darling.json` are only a first-run seed; `config.config_service.mcp_port` / `web_port` is what the endpoint actually binds, so a rule named from the file after someone moved the port in the Viewer's Settings would open a port nothing is listening on and leave the served port shut. The verb therefore reads those two columns before it plans. That read is **best effort** — the verb still has to run at install time, before the store exists, where `darling.json`'s port is the value the store will be seeded *with* — and it is never silent: when the store cannot be reached it names the port it used, why it could not do better, and the one case in which that port is wrong (a port that has since been changed in the Viewer), so re-running it once the service is up moves the rule. `--enable-mcp` / `--enable-web` have no such window: they take the port back from the store write they already perform, so they cannot open a port on a guessed number at all.

Because the port is part of the rule's **name**, changing a port does not update a rule — it makes a different one and leaves the old one standing as an inbound allow on a port nothing serves. Every removal here therefore sweeps `PerformanceMonitor Darling <surface> (port *)` rather than one exact name, so the stale rule goes with it. The wildcard is derived from a rule name this product built; a name that does not parse degrades to the exact-name removal, so a sweep can never reach a rule that is not ours.

"Really exposed" is decided by the same resolvers the running service fail-closes on, not by reading `listen` at face value. A `network` block the service would degrade to loopback — an unparseable `listen`, a missing or invalid `allowFrom`, an address family that disagrees, a missing token, BYO mode — gets **no open port**, and the verb tells you why.

The running service never touches these rules. It **checks** them on start and logs what it finds: nothing at all for the normal loopback-only install, one INFO line when an exposed endpoint's rule is present, and one WARN naming the exact command when an exposed endpoint's rule is missing or when a loopback-only endpoint still has a stale rule open. It states each verdict once, not once per retry.

### Headless enable/disable + firewall (`--enable-mcp` / `--enable-web`)

On a box with no Viewer, two things are otherwise awkward: the `enabled` flags in the `mcp` / `web` blocks below are only a **first-run seed** — after the first run the store (`config.config_service.mcp_enabled` / `web_enabled`) is authoritative and is normally flipped only from the Viewer's Settings — and the service account (`NT SERVICE\PerformanceMonitor Darling`) **cannot open the firewall itself**. Four verbs, run on the **service host**, close both in one elevated action:

```
PerformanceMonitor.Darling.Service.exe --enable-mcp
PerformanceMonitor.Darling.Service.exe --disable-mcp
PerformanceMonitor.Darling.Service.exe --enable-web
PerformanceMonitor.Darling.Service.exe --disable-web
```

Each flips only its endpoint's **live store flag** with a targeted `config_service` write; the service **hot-reloads within one collection sweep — no restart.** If that endpoint's `network` block opts into LAN exposure (a non-loopback `listen`), the verb also reconciles that endpoint's **scoped, idempotent-by-name firewall rule**: **run elevated**, it opens (or, on `--disable-*`, removes) the rule; **run non-elevated**, the store toggle still succeeds and it prints the exact elevated firewall command to run by hand (a loopback-only endpoint needs no rule and says so). Managed-mode only, Windows only. So the headless bring-up is: write the `network` block (the wizard above or the manual reference below), then `--enable-mcp` / `--enable-web` from an **elevated** shell.

### Verify it's actually reachable (and the two failures that look like bugs)

Enabling an endpoint is **not** the same as reaching it, and both common failures leave the store flag reading `true`, so "it says enabled" is not proof. After `--enable-mcp` / `--enable-web`, verify on the **service host**:

1. **The listener is on the LAN address, not loopback.** `Get-NetTCPConnection -State Listen | Where-Object LocalPort -eq 5152` (or `5153` for web) must show the box's LAN IP, e.g. `10.0.0.5:5152` — **not** only `::1` / `127.0.0.1`. *Enabled but still loopback-bound* is the single most common failure: the store flag is on, but the service loaded `darling.json` **before** the `network` block existed. The block is read **once at service start** — the enable toggle stops/starts the endpoint with the already-loaded config and does **not** reload the file. **Restart the service** (`Restart-Service 'PerformanceMonitor Darling'`) so it re-reads the block, then re-check the listener; after the restart run `--configure-firewall` **elevated** if the firewall rule is missing (the service account cannot create it, so the service only tells you it is missing).
2. **The scoped firewall rule exists and covers the client.** `Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling MCP (port 5152)'` (or `... Web (port 5153)`) should be `Enabled=True, Action=Allow`, scoped to the `network.allowFrom` CIDR. If it is absent, the service's own start-up log already says so and names the command; `--configure-firewall` elevated is the one-step fix. Reading rules needs no elevation, so this check works from any shell.

Then from the **client** host:

3. **Connect to the box's LAN IP, never `localhost`.** Use `http://<box-LAN-IP>:5152/`. `localhost` / `127.0.0.1` only resolves *on the box itself*, so an off-box MCP client pointed at localhost fails silently — this is the number-one "MCP won't connect" cause. Send `Authorization: Bearer <token>` (the `network.token`), and do a **fresh** `initialize` + `tools/list` rather than trusting a cached tool list from a previous version.

**After a reinstall:** the installer replaces binaries but does **not** touch `darling.json` (the zip ships only `darling.sample.json`) or the store, so the `network` block and both live flags survive the upgrade — and the reinstall restarts the service, which re-reads the block. If MCP stops connecting afterward it is almost always failure 3 (the client pointed at `localhost`) or a missing firewall rule, **not** lost config: run `--configure-firewall` **elevated** to re-open the rule if check 2 comes up empty (the installer already does this, so an in-place upgrade normally leaves the rules correct). A stale loopback bind is unlikely after a restart unless the block itself is invalid, in which case the endpoint fail-closes to loopback and logs a critical line saying why — fix the block and restart again. A full `--configure-network` re-run is only needed if the `network` block itself is gone.

### Store endpoint (viewer over the LAN)

Add a `network` block to `postgres` (managed mode):

```json
"postgres": {
  "managed": true,
  "port": 5641,
  "network": {
    "listen": "192.168.1.205",
    "allowFrom": "192.168.1.0/24",
    "role": "viewer"
  }
}
```

On every start the service reconciles this against the live cluster: it adds the bind IP to `listen_addresses`, generates a self-signed TLS certificate (`server.crt` / `server.key` beside the data directory, with both an IP SAN for `listen` and a DNS SAN for the machine hostname), writes a marked `hostssl darling <role> <allowFrom> scram-sha-256` rule into `pg_hba.conf` and reloads, and **checks** (never creates — see [Firewall rules](#firewall-rules---configure-firewall)) that the store's scoped firewall rule matches.

- **`role`** — the pg_hba login role the rule names: `"viewer"` (default, **read-only** — the secure default, covering a laptop reading every dashboard, chart, and finding) or `"admin"` (full remote **writes**; the service logs a warning because `admin` holds the `config_command` / `config_monitored_servers` / `config_notification` service-credential pivot). Never the superuser. This is **distinct from `postgres.connectAs`** (the *local* VM viewer's loopback role, default `admin`): `network.role` is the *remote* role and defaults to `viewer`, so the two have opposite defaults — the local seat is writable, the remote seat is read-only, unless you say otherwise.
- **TLS is verify-full, not `require`.** Because Darling generates the cert, the client can pin it, so the connection string below uses `SSL Mode=VerifyFull` — which actually defends against an on-path MITM (`require` verifies nothing). The store's network pg_hba line is `hostssl`, so a non-TLS network client is refused.
- **The firewall is defense-in-depth, not the boundary** — pg_hba + TLS are. `--configure-firewall` (elevated) creates the store's scoped rule for you along with the other two; the equivalent by hand is:

  ```
  New-NetFirewallRule -DisplayName "PerformanceMonitor Darling store (port 5641)" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5641 -RemoteAddress 192.168.1.0/24
  ```

**That is the service side. The viewer side is [Connect a Remote Viewer](#connect-a-remote-viewer)** — `--export-viewer-config` on this host writes the viewer machine's whole configuration folder (config, certificate, and a plain-text field reference), and that section covers copying it over, the certificate's placement and rotation, and the manual `--print-viewer-connection` fallback.

### MCP endpoint (assistant over the LAN)

Add a `network` block to `mcp` (managed mode; `mcp.enabled` must be `true`):

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

When `listen` is a network address **and** a token is present **and** `allowFrom` is a valid CIDR, the MCP host binds that interface behind two gates: a **required bearer token** (checked first, constant-time, no loopback exemption) and an **in-app CIDR check** on the remote address (loopback is always allowed, so local clients keep working). Any missing precondition keeps MCP loopback-only. Prefer `encryptedToken` (a DPAPI blob from `--encrypt-password`); a plaintext `token` works for dev but is warned. Set the same scoped firewall rule for the MCP port:

**Lost the token?** `--print-mcp-token` (elevated) reprints it from `darling.json` — `--print-web-token` does the same for the dashboard. Both write the live token to **STDOUT** with every warning on STDERR, so `... --print-mcp-token | clip` captures the value and still shows the warning. This discloses nothing new: the blob is DPAPI at `LocalMachine` scope with an entropy constant published in this repository, and `darling.json` grants `INTERACTIVE` read by design (see [Security & Least-Privilege Roles](#security--least-privilege-roles)), so anyone who can log on to the host interactively could already decrypt it. The elevation requirement makes reprinting a deliberate act; the token's actual protection is the file's ACL. If the token has **leaked** rather than been mislaid, reprinting is not the fix — `--configure-network` generates a new one, and every client configured against the old one must be updated.

```
New-NetFirewallRule -DisplayName "Darling MCP" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5152 -RemoteAddress 192.168.1.0/24
```

**What a token-holder can — and cannot — do.** Start with the boundary: **no MCP tool runs SQL an AI client wrote against your monitored servers.** No such tool exists, and a stored custom view cannot become one either — a composed query names only `collect.*` collector tables in the monitoring store. The only live contact with a monitored SQL Server is `analyze_server`'s plan fetch and `add_servers`' one-time connection probe, and both run the product's own fixed, read-only queries under the same least-privilege monitoring login the collectors use — the ceiling on what they can see is the ceiling you granted that login, and it has no write grants to hit. Everything else answers from the monitoring store.

What the token does gate is the monitor's own configuration and collected data: the entire read surface, `analyze_server`, the Custom Views tools (create / modify / delete the saved dashboards and notebooks in `config.custom_views`), the alert-tuning tools (`update_alert_settings` / `create_mute_rule` / `delete_mute_rule`), and the server-onboarding tools (`add_servers` / `remove_server`), which edit the monitored-server registry in `config.config_monitored_servers` — including storing a SQL-auth credential for a server they add. The store-side identity is still the least-privilege `mcp` role: read, the two analysis-table INSERTs, INSERT/UPDATE/DELETE on the single `config.custom_views` table (the same narrow write the web composer's `viewer` role has), the narrow alert-config writes (`config.config_mute_rules` CRUD + a single-row `config.config_alert_settings` UPDATE, plus the `config_service` reload-beacon columns), and the single-table `config.config_monitored_servers` CRUD. So a token-holder can read everything collected, trigger analysis, author custom views, tune alerting, and onboard/offboard servers — and can never reach the `config_command` service-credential pivot, the carved secret columns (SMTP/webhook credentials, and the monitored-server `encrypted_password` blob it can WRITE during onboarding but never READ back, all included), or a service flag like `paused`. Custom-view JSON and alert config carry no secrets. Guard the token like the keys to your monitoring configuration — that is what it opens; your SQL Servers are not behind it.

**`add_servers` carries a credential in its request.** A SQL-auth `password` rides the request JSON; the service DPAPI-encrypts it at rest and never returns it, but on the wire it is only as protected as the endpoint — the same plaintext HTTP the token rides. On a segment you do not fully trust, front the MCP port with the TLS reverse proxy below, and prefer Windows/integrated auth for onboarded servers where you can — then no per-server secret crosses the wire at all.

**MCP has no TLS — the MITM control is a TLS reverse proxy.** A self-signed cert breaks real MCP clients, so the MCP endpoint is plain HTTP and the bearer token travels **cleartext on the segment**; an active on-path attacker (ARP spoof, rogue DHCP, compromised switch) could capture and replay it. The in-app CIDR bounds *who can route to* the port; it does **not** protect the wire. If your segment is not fully trusted, put a **TLS-terminating reverse proxy** in front of the MCP port and point clients at that — the named MITM control for this endpoint. (The store endpoint needs no such proxy: it has verify-full TLS built in.)

**Output format (GCF).** By default the MCP tools return JSON. Setting `DARLING_OUTPUT_FORMAT=gcf` makes the server return [Graph Compact Format](https://gcformat.com) instead: the repeated field names of the record arrays these tools return (blocking pairs, wait stats, index bloat, config rows, …) are factored into a single header and the indentation dropped. The saving is payload-shaped: about a quarter across a mixed real workload, more than half on the most uniform tools, and less on text-dominated ones (multi-line query text, plan or deadlock XML) where the field names are a smaller share of the bytes. In the wire a null field is written as `-` and an absent field (a key some rows omit) as `~`. It is opt-in and conservative: applied per result, and only when the GCF wire is both smaller than the JSON and decodes back to it exactly; a result carrying a number GCF cannot represent exactly (a non-integer beyond double precision) stays JSON, as does any result where the wire would be larger, so no result is ever grown, dropped, or garbled. `BlackwellSystems.Gcf` is a zero-dependency package.

### Web endpoint (browser over the LAN)

Add a `network` block to `web` (managed mode; `web.enabled` must be `true`):

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

When `listen` is a network address **and** a token is present **and** `allowFrom` is a valid CIDR, the web host binds that interface behind two gates: an **in-app CIDR check** on the remote address and an **access token**. A browser presents the token ONCE via `?token=` (open `http://192.168.1.205:5153/`, then paste it into the minimal login form, or append `?token=...` directly); the host validates it constant-time, sets an **HMAC-signed, HttpOnly, SameSite=Strict session cookie**, and 302-redirects to strip the token from the URL so it never lingers in history or a Referer header. Subsequent requests ride the cookie. **Loopback is exempt from the CIDR check only** — while the dashboard is exposed, a request from the box itself still has to present the token or a valid cookie. It used to pass tokenless on the grounds that the surface was read-only; the Custom Views composer's create / update / delete and `/api/compose/run` ended that, so in exposed mode the token *is* the loopback guard against SSRF and sandboxed local sockets ([#1649](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1649)). A dashboard with **no** `network` block registers no auth middleware at all and stays genuinely tokenless. An out-of-CIDR request is refused with **403**, cookie or no cookie; an in-CIDR request with no credential gets the login form back as a **200**, not a 401. The cookie signing key is per-process, so a service restart invalidates open sessions (just re-present the token). Prefer `encryptedToken` (a DPAPI blob from `--encrypt-password`); a plaintext `token` works for dev but is warned. Set the same scoped firewall rule for the web port:

```
New-NetFirewallRule -DisplayName "Darling Web" -Direction Inbound -Action Allow -Protocol TCP -LocalPort 5153 -RemoteAddress 192.168.1.0/24
```

**What a web token can reach.** The web dashboard is **read-only over the collected store** — it connects as the least-privilege `viewer` role and hosts no live-server queries (no `analyze_server`, no plan re-execution). Its one write path is the Custom Views composer: INSERT/UPDATE/DELETE on `config.custom_views`, and the web host maps no other write route. (The `viewer` role holds one further narrow grant — INSERT/UPDATE/DELETE on `config.database_state_expected`, for the Viewer's per-database state-override editor ([#1986](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1986)) — which no web route reaches, and which the WPF editor itself gates on the `has_table_privilege` read-only probe a `viewer`-role seat fails.) So a token-holder can read everything collected and author saved views, and reach no monitored server — but that write is why an exposed dashboard authenticates loopback too, exactly as MCP does.

**The web dashboard can serve HTTPS itself ([#2562](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2562)).** Without it the token and the session cookie it mints travel cleartext on the segment, and the in-app CIDR bounds *who can route to* the port without protecting the wire — so an exposed dashboard with no `tls` block warns about exactly that at every start. Add a certificate to `web.network`:

```jsonc
"network": {
  "listen": "192.168.1.205",
  "allowFrom": "192.168.1.0/24",
  "encryptedToken": "<output of --encrypt-password>",
  "tls": {
    "pfxPath": "C:\\ProgramData\\PerformanceMonitorDarling\\certs\\dashboard.pfx",
    "encryptedPfxPassword": "<output of --encrypt-password>"
    // ...or a PEM pair instead: "certPath" + "keyPath" (what the compose distribution mounts)
  }
}
```

Give **one** form, never both — a PKCS#12 bundle or a PEM pair — and the password may sit in `encryptedPfxPassword` (DPAPI), in `pfxPassword` as a `file:`/`env:` reference, or nowhere at all if the bundle is unprotected. TLS applies to the **network listener only**: the loopback listeners stay plain HTTP, because your certificate names the LAN address and not `localhost`, and loopback traffic never reaches the segment this protects. There is no redirect-from-HTTP, because one port cannot speak both schemes and adding a second HTTP port would reopen the cleartext surface; a plain-HTTP client simply fails the handshake.

The product **consumes** a certificate and does not manage a PKI — no ACME, and deliberately no self-signed fallback, which would buy encryption without authentication and train you to click through the warning. An internal CA is the normal answer on a LAN. Loading **fails closed**: a certificate that is missing, unreadable, ambiguous, or **expired** keeps the dashboard loopback-only with a Critical log line rather than quietly serving the LAN over HTTP — and because an expired certificate takes the dashboard down, the service warns for the last 30 days before that happens. A TLS-terminating reverse proxy in front of the port remains a perfectly good alternative, and is still the only control for MCP, which has no TLS of its own.
