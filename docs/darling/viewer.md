# The Darling WPF viewer

Audience: operators.

The viewer is the desktop app. It shows plans, blocking chains, deadlock graphs and live active queries. This page shows how to open the viewer on the service host and how to set it up on another machine. It also explains why a seat can be read-only.

Install the service first. See [Install Darling](install.md).

The viewer talks only to the PostgreSQL store. It never connects to your monitored SQL Servers. The service collected every number that the viewer shows. For this reason the viewer needs no SQL Server credentials, and a read-only seat is a real thing (see [Read-only versus read-write seats](#read-only-versus-read-write-seats)).

## On the service host

Double-click the **Darling Viewer** shortcut that the installer made. Or run
`C:\Program Files\PerformanceMonitorDarling\viewer\PerformanceMonitor.Darling.Viewer.exe`.

There is nothing to configure. The viewer looks for `darling.json` in four places, in this order:

1. An explicit command-line path.
2. `%DARLING_CONFIG%`.
3. Beside its own exe.
4. One directory up. This is the layout of the zip, where `viewer\` sits under the service root.

It reads only the `postgres` section and derives `Host=127.0.0.1;Port=5641;Database=darling`. It decrypts the store password from `pg-admin-credential.dpapi` beside the data directory.

**Proof:** the left sidebar lists your servers, and the Overview tab shows a health card per server. Double-click a card (or the sidebar entry) to open that server's tab.

If the viewer will not connect, the failure window shows a **Configuration this viewer used** block. The block names:

- the `darling.json` that the viewer read,
- which of the four rules picked that file,
- the host, port, user, database and SSL mode that the viewer parsed.

The window has a **Copy details** button. Read the block before you change anything. It shows whether the viewer read a different file than the one you edited, or read your file and found a wrong value. The block never contains a password. The same lines are in `%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_yyyyMMdd.log`.

Two failures have a known cause:

- `The Darling store is at schema v<store version>, but this viewer needs v<viewer version>. Update or restart the Darling service so it migrates the store, then reopen the viewer.` The viewer is newer than the service. Upgrade or restart the service, because it migrates the store on start. The gate is `<`, not `≠`, so a store that is newer than the viewer is not blocked.
- `pg-admin-credential.dpapi ... does not exist yet.` The first run of the service did not get far enough to provision the roles. The reason is in the service log, not in `darling.json`.

## On another machine (a remote seat)

Two things must be true. People often miss the service-side one.

### Service side: make the store reachable off the box

By default the store listens on `127.0.0.1` and accepts no remote viewer. A change on the viewer side cannot alter that. On the service host, from an **elevated** PowerShell, run:

```powershell
cd "C:\Program Files\PerformanceMonitorDarling"
.\PerformanceMonitor.Darling.Service.exe --configure-network
```

Choose the **store** option. The wizard:

- validates every answer against the same resolvers that the running service fail-closes on,
- edits `darling.json` in place and keeps its comments, behind a timestamped `.bak-` backup,
- prints the scoped firewall command,
- offers to restart the service.

On that restart the service adds the bind IP to `listen_addresses` and generates a self-signed TLS certificate with an IP SAN. It also writes a `hostssl darling <role> <allowFrom> scram-sha-256` line into `pg_hba.conf`.

Then, still elevated, run:

```powershell
& "C:\Program Files\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe" --configure-firewall
```

**Proof:** `Get-NetFirewallRule -DisplayName 'PerformanceMonitor Darling store (port 5641)'` returns a rule with `Enabled=True, Action=Allow`. Reading firewall rules needs no elevation, so this check works from any shell.

### Export the seat

Stay elevated, because the command writes into the install folder:

```powershell
& "C:\Program Files\PerformanceMonitorDarling\PerformanceMonitor.Darling.Service.exe" --export-viewer-config
```

It writes a `viewer-config\` folder that holds three files:

- `darling.json`: the connection string is resolved and `"managed": false` is already set. Comments inside the file explain every field.
- `server.crt`: the certificate that the connection pins.
- `README.txt`.

That `darling.json` contains a live database password. The command sets an ACL on the file and says so. It exits with a non-zero code if the ACL did not take. Copy the folder over a channel that you trust.

### Viewer side: two ways to get the app onto the remote machine

| | Where the viewer lives | Where to put the three exported files |
|---|---|---|
| Copy `viewer\` out of `PerformanceMonitorDarling-<version>.zip` | wherever you unpacked it | next to `PerformanceMonitor.Darling.Viewer.exe`. This works unedited |
| Run the standalone `PerformanceMonitorDarlingViewer-darlingviewer-Setup.exe` (Velopack, updates itself) | a versioned folder that the installer manages | anywhere. Then point `%DARLING_CONFIG%` at the exported `darling.json` |

With the Setup.exe, we recommend `DARLING_CONFIG`. Its install directory changes on every update, so files that you put beside the exe do not survive.

In both cases keep the three files together. A bare `Root Certificate=server.crt` resolves against the folder that holds `darling.json`, not the working directory. The folder can be anywhere if it stays intact.

Re-run `--export-viewer-config` and copy the folder again after a credential rotation or a `listen` change. The store's certificate regenerates when its bind IP changes, and an old copy stops matching.

## Connect a Remote Viewer

For the person sitting at a machine with **nothing installed on it**, whose only goal is looking at a Darling service that already runs somewhere else. Three steps, nothing to hand-edit.

**The one service-side prerequisite.** The store has to be reachable from your LAN — a `postgres.network` block on the service host, which the `--configure-network` wizard writes for you. A store still on its loopback default accepts no remote viewer at all, and no amount of viewer-side configuration changes that. See [Store endpoint (viewer over the LAN)](network-endpoints.md#store-endpoint-viewer-over-the-lan) for that side; everything below assumes it is done.

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

**The exported `darling.json` contains a live database password** — that is what the viewer authenticates with. The verb says so before it writes, ACLs the file to SYSTEM + Administrators + the account running it + members of the local group `PerformanceMonitor Darling Operators`, if that group exists on the machine (the Viewer reads it without elevation, the same posture as the admin/viewer credentials), and confirms the ACL took: if the secret is still readable by ordinary users it says so and exits non-zero. Copy the folder over a channel you trust and keep it ACL'd on the viewer machine.

The verb refuses rather than clobbers: it will not export into the **service's own config directory** (that would overwrite the service's `darling.json` with the viewer's, destroying its servers, encrypted passwords and tokens), will not overwrite a file it did not write, and will not follow a junction or symlink. A destination it cannot use is named in the refusal.

### 2. Copy the folder to the viewer machine

Put the three files **next to `PerformanceMonitor.Darling.Viewer.exe`** — that works with nothing edited. (The Viewer ships in the same release zip as the service, in its `viewer\` subfolder; from source it is `dotnet build Darling/PerformanceMonitor.Darling.Viewer/PerformanceMonitor.Darling.Viewer.csproj -c Release`.)

To keep the folder somewhere else instead, point the `DARLING_CONFIG` environment variable at the exported `darling.json`. That works unedited too: a bare or relative `Root Certificate` resolves against **the folder holding `darling.json`**, so the `server.crt` beside it is found wherever you keep the folder ([#1970](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1970)). Keep the three files together and the folder can live anywhere.

### 3. Start the Viewer

If you copied the folder next to `PerformanceMonitor.Darling.Viewer.exe`, run that exe. If you installed the Viewer with `PerformanceMonitorDarlingViewer-darlingviewer-Setup.exe` from the [latest release](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest), set `DARLING_CONFIG` to the exported `darling.json`, then start the Viewer.

That is the whole setup. Re-run the export after a credential or certificate rotation — the store's certificate regenerates when its bind IP changes — and copy the folder over again; it replaces its own previous output without ceremony.

### If it does not connect

The failure window carries a **Configuration this viewer used** block naming the `darling.json` it actually read, which rule picked it, and the host, port, username, database, SSL mode, search path and certificate path it parsed — with a **Copy details** button, and the same lines in `%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_yyyyMMdd.log`. Read it before changing anything: it separates *the viewer read a different file than you edited* from *it read your file and a value in it is wrong*. It never contains a password. See [Troubleshooting](troubleshooting.md) for the individual failures.

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

## Read-only versus read-write seats

This is the question that people ask most about the viewer. The answer is not what the wording suggests.

Some viewer actions are not reads:

- "Current Active Queries" Refresh
- "Get Actual Plan"
- "Purge Now"
- Recommendations "Generate now"
- alert Dismiss
- mute-rule editing
- Add Server
- schedule editing

All of these enqueue a command for the service. Each one is an `INSERT` into `config_command` in the monitoring store. The service picks up the row, does the work over its own connection, and writes the result back. A read-only seat cannot write that row.

Nothing is written to your monitored SQL Server. The service only reads that server, under the same least-privilege login that the collectors use. The refusal says so:

> `Read-only viewer — the live fetch queues a request for the service in the MONITORING STORE, which a
> read-only seat can't write to. Nothing is sent to or written on the monitored server. Reconnect with a
> read-write store profile to fetch live active queries.`

Which seat you get by default:

| Seat | Setting | Default | Result |
|---|---|---|---|
| Local, on the service host | `postgres.connectAs` | `"admin"` | read-write |
| Remote, over the LAN | `postgres.network.role` | `"viewer"` | read-only |

The defaults are opposite on purpose. The local seat belongs to the operator, and the remote seat is a laptop. A tester on a remote seat must expect read-only and must not read it as a fault.

### How the viewer decides

On connect the viewer runs one probe:

```sql
SELECT has_table_privilege('config_alert_log', 'UPDATE')
```

True means read-write. False means read-only. Any other result also means read-only. The probe fails safe, so a permission quirk or a mis-provisioned store hides the write controls. You do not click one and hit a permission error. A failure to reach the store is not a read-only verdict. The viewer then shows "Can't reach the Darling store — is the Darling service running?" instead.

If you see a read-only seat that you did not expect, run that probe yourself against the store, as the role that the viewer uses. If it returns `true` and the viewer still says read-only, that is a bug. Please report it.

### How to tell which seat you have

There is no global banner. The main window carries a seat indicator that reads `Seat: read-only` or `Seat: read-write`. The indicator comes from the same probe, so you do not have to trigger a refusal to find out. Hover over it. The tooltip gives the reason for this connection:

- A host seat connects as `postgres.connectAs` (default `admin`, read-write).
- A LAN seat connects as `postgres.network.role` (default `viewer`, read-only).

The tooltip also says that nothing is ever written to a monitored SQL Server.

Before the probe returns, the indicator says so and does not guess. A viewer that cannot reach the store reports that fact and does not claim read-only. A status line that blamed your role while the service is down would make you fix the wrong thing.

Other places also show the seat. To check quickly, open **Settings**. A read-only seat shows:

> `This viewer is connected with a read-only role. Monitoring settings below are shown for reference but can't
> be changed from here.`

Elsewhere you see disabled buttons with tooltips, or a status line that names the blocked write. Some dialogs say it in their own title bar. Database State Overrides appends `(read-only seat — changes cannot be saved)`.

### How to change it

On a managed store this is a config change, not a `GRANT`. The `admin` role and its credential already exist, because the service provisions them on every start.

- Local seat: set `"connectAs": "admin"` in the `postgres` block and restart the viewer.
- Remote seat: set `postgres.network.role` to `"admin"` on the service host, restart the service, and re-run `--export-viewer-config`. The service logs a warning when you do this. The `admin` role holds the `config_command`, `config_monitored_servers` and `config_notification` pivot, and the warning says so.
- Bring-your-own PostgreSQL: run [`Darling/tools/provision-roles.sql`](../../Darling/tools/provision-roles.sql) as the store owner. It is the authoritative grant list.
