# The Darling WPF viewer

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
SELECT has_table_privilege('config_mute_rules', 'INSERT')
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
