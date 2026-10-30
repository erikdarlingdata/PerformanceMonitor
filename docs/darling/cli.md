# Command line reference

Audience: operators.

This page lists every command line verb that the Darling service exe accepts. A verb is the first argument, for example `--check-settings`. The exe is `PerformanceMonitor.Darling.Service.exe` on Windows. On Linux, and anywhere the service runs as a .NET app, type `dotnet PerformanceMonitor.Darling.Service.dll` in its place.

Run the exe with `--help` to print the same list in short form. With no arguments, the exe runs the service. With an argument that is not on this page, the exe prints `Unknown option` and the help text, exits `1`, and does not start the service.

## How to read the table

Needs elevation:
- `yes` means the verb refuses to run, or cannot finish, from a prompt that is not elevated.
- `no` means the code does not check. The account must still be able to read `darling.json` and the store credential. Those files are open to SYSTEM, Administrators, the service account, and the local group `PerformanceMonitor Darling Operators`. A person outside those accounts needs an elevated prompt for every verb that reads them.
- A few verbs say what part of the work needs elevation.

Service running:
- `no` means the verb never needs the service.
- `either` means the verb does not need the service itself. A verb marked `either (1)` opens the store. The exception is `--grant-script --host`, which does not. A managed store runs only while the service runs. With a managed store, these verbs fail when the service is stopped. With an own store (`postgres.managed` off), they need only that the store is reachable.

Exit codes are numbers. The [exit code table](#exit-codes) below says what each number means for each family of verbs. A script can gate on the number.

## The table

| Verb | What it does | Needs elevation | Service running | Exit codes | Details |
| --- | --- | --- | --- | --- | --- |
| `--help`, `-h`, `-?`, `/?` | Prints the help text. | no | either | `0` | This page |
| `--version`, `-v` | Prints the product version. | no | either | `0` | This page |
| `--test-connection`, `--validate-config` | Checks `darling.json` and probes every configured server. | no | either | `0`, `1` | [Validate the config](install.md#validate-the-config-pre-flight) |
| `--check-settings` | Prints the store host profile and a verdict for each sizing setting. | no | either (1) | `0`, `1`, `2`, `3` | [Check settings](install.md#check-hoststore-sizing-settings) |
| `--diagnostics-bundle`, `--diag-bundle` | Writes one aliased file to attach to a bug report. | yes on the store host | either | `0` to `5` | [Diagnostics bundle](#diagnostics-bundle) |
| `--encrypt-password` | Encrypts a SQL login password for `darling.json`. Windows only. | no | no | `0`, `1` | [Configure darling.json](install.md#configure-darlingjson) |
| `--add-server`, `--add-servers` | Registers one or more monitored servers. | no | either (1) | `0`, `1` | [Add-server](#--add-server) |
| `--grant-script` | Prints the T-SQL that grants Darling its access on a monitored server. | no | either (1) | `0`, `1` | [Permissions](../permissions.md) |
| `--print-viewer-connection` | Prints a connection string for a remote Viewer. Windows only. | no | either | `0`, `1` | [Connect a remote Viewer](viewer.md#manual-configuration-fallback) |
| `--export-viewer-config` | Writes a ready-to-copy folder for a remote Viewer. Windows only. | no | either | `0`, `1` | [Export the handoff folder](viewer.md#1-export-the-handoff-folder-on-the-service-host) |
| `--print-mcp-token` | Prints the MCP access token again. Windows only. | yes | either | `0`, `1` | [MCP endpoint](network-endpoints.md#mcp-endpoint-assistant-over-the-lan) |
| `--print-web-token` | Prints the web dashboard access token again. Windows only. | yes | either | `0`, `1` | [Web endpoint](network-endpoints.md#web-endpoint-browser-over-the-lan) |
| `--configure-network` | Asks questions, then edits `darling.json` to expose the store, MCP or web endpoint on the network. Windows only. | no, but the service restart and firewall steps need it | either | `0`, `1` | [Guided setup](network-endpoints.md#guided-setup---configure-network) |
| `--configure-firewall` | Creates or removes the scoped firewall rules so they match `darling.json`. Windows only. | yes | either | `0`, `1` | [Firewall rules](network-endpoints.md#firewall-rules---configure-firewall) |
| `--harden-files` | Applies the locked-down permissions to `darling.json`, the store credentials, the log-hash key and the TLS key files. Windows only. | yes | either | `0`, `1` | [harden-files](#--harden-files) |
| `--enable-mcp`, `--disable-mcp` | Turns the MCP endpoint on or off in the store. Windows only. | no, but the firewall step needs it | either (1) | `0`, `1` | [Headless enable and disable](network-endpoints.md#headless-enabledisable--firewall---enable-mcp----enable-web) |
| `--enable-web`, `--disable-web` | Turns the web dashboard on or off in the store. Windows only. | no, but the firewall step needs it | either (1) | `0`, `1` | [Headless enable and disable](network-endpoints.md#headless-enabledisable--firewall---enable-mcp----enable-web) |
| `--enable-collector`, `--disable-collector` | Turns one collector on or off, for every server or one server. | no | either (1) | `0`, `1`, `2` | [Enable or disable a collector](collector-settings.md#enable-or-disable-a-collector-headlessly---enable-collector----disable-collector) |
| `--set-collector-run-at` | Sets the time of day a daily collector starts. | no | either (1) | `0`, `1`, `2` | [Set the run time](collector-settings.md#set-the-time-of-day-a-daily-collector-runs---set-collector-run-at) |
| `--set-collector-frequency` | Sets how often a collector runs, in minutes. | no | either (1) | `0`, `1`, `2` | [Collector settings](#collector-settings) |
| `--set-collector-retention` | Sets how many days a collector keeps its data. | no | either (1) | `0`, `1`, `2` | [Collector settings](#collector-settings) |
| `--reset-password-key` | Marks the current password key as replaced, so the next start makes a new one. | no | either (1) | `0`, `1`, `2` | [Make a new password key](collector-settings.md#make-a-new-password-key---reset-password-key) |
| `--drop-xe-sessions` | Drops the Extended Events sessions that Darling created on a server. | no | either (1) | `0`, `1`, `2` | [Drop the sessions](collector-settings.md#drop-the-extended-events-sessions-a-removed-server-left-behind---drop-xe-sessions) |
| `--add-alert-source` | Creates a login for a system that sends alerts, and prints its connection string once. | no | either (1) | `0`, `1`, `2` | [Alert sources](agents-and-alert-sources.md#make-a-login-for-a-system-that-sends-alerts---add-alert-source-and-friends) |
| `--rotate-alert-source` | Gives an alert source a new password and prints its connection string once. | no | either (1) | `0`, `1`, `2` | [Alert sources](agents-and-alert-sources.md#make-a-login-for-a-system-that-sends-alerts---add-alert-source-and-friends) |
| `--remove-alert-source` | Drops an alert source's login and its queued alerts. | no | either (1) | `0`, `1`, `2` | [Alert sources](agents-and-alert-sources.md#make-a-login-for-a-system-that-sends-alerts---add-alert-source-and-friends) |
| `--list-alert-sources` | Lists the alert sources. It never prints a password. | no | either (1) | `0`, `1`, `2` | [Alert sources](agents-and-alert-sources.md#make-a-login-for-a-system-that-sends-alerts---add-alert-source-and-friends) |
| `--export-investigations` | Writes the investigation cases that have feedback, one JSON object per line. | no | either (1) | `0`, `1`, `2`, `130` | [Export the judged cases](agents-and-alert-sources.md#export-the-judged-cases-to-test-your-own-agent-6236) |
| `--backfill-rollups` | Builds the retention rollups over existing history. | no | either (1) | `0`, `1` | [Retention hold runbook](../retention-hold-runbook.md) |
| `--collapse-legacy-slices` | Repairs Query Store rows collected before the split-slice fix, then rebuilds the rollups they fed. | no | either (1) | `0`, `1` | [Retention hold runbook](../retention-hold-runbook.md) |
| `--recompress-plan-dim` | Converts old plan text rows to gzip in batches, then compacts the table. | no | either (1) | `0`, `1` | [Retention hold runbook](../retention-hold-runbook.md) |

Two more names exist and are not in `--help`. The install scripts use `--tls-key-files-only`, a flag of `--harden-files`. The Linux build job uses `--self-check-password-key`. An operator has no use for either.

## Exit codes

Every verb exits `0` when it did its work. Any other number means it did not. The tables name the number, and the verbs that use it.

Most verbs use two numbers:

| Number | Meaning |
| --- | --- |
| `0` | The verb finished. |
| `1` | The verb did not finish. Causes include a bad argument, a configuration that is missing or invalid, a connection string that cannot be used, a managed store credential that cannot be read, and a platform the verb does not support. |

These verbs use only `0` and `1`:

`--test-connection`, `--validate-config`, `--encrypt-password`, `--add-server`, `--grant-script`, `--print-viewer-connection`, `--export-viewer-config`, `--print-mcp-token`, `--print-web-token`, `--configure-network`, `--configure-firewall`, `--harden-files`, `--enable-mcp`, `--disable-mcp`, `--enable-web`, `--disable-web`, `--backfill-rollups`, `--collapse-legacy-slices` and `--recompress-plan-dim`.

`--help` and `--version` always exit `0`.

The collector verbs, `--reset-password-key`, the alert source verbs, `--export-investigations` and `--drop-xe-sessions` add a third number:

| Number | Meaning |
| --- | --- |
| `0` | The verb finished. |
| `1` | A usage or configuration problem, or a name that matches nothing (or more than one thing). Examples are a bad argument, an unknown collector, a missing config, and a `--server` that names no server. It also covers a connection string that cannot be used and a managed store credential that cannot be read. |
| `2` | The store cannot be reached, refuses the login, or refuses the change. A wrong password is the store's answer, so it is `2`, not `1`. A failure to read the row back after the change counts here too. For `--drop-xe-sessions`, the store is not part of this number: `2` means the monitored server cannot be reached, cannot be searched, or refused a `DROP`. |

These verbs also use `1` for a few cases of their own:

- A managed store credential that was never stored. The setup is not finished. This holds for the verbs that open the store, but not for `--drop-xe-sessions`.
- A store that has not been upgraded yet. This is `--set-collector-run-at` when the run-time table is missing, and the alert source verbs when the alert tables are missing. Start the service once, then run the verb again.
- A source name that is invalid, already taken, or unknown. This is the alert source verbs.
- An output file that cannot be written, or that exists when `--force` was not given. This is `--export-investigations`. When it cannot read the store, it exits `2`.

`--export-investigations` also exits `130` when it is cancelled. It writes no file in that case.

`--check-settings` uses four numbers:

| Number | Meaning |
| --- | --- |
| `0` | No setting is stale. |
| `1` | The config failed to load or validate, or its store setting cannot be used. |
| `2` | The store could not be reached. A managed credential that was never stored counts here, because there is no store to check. |
| `3` | At least one setting has the verdict `stale-after-hardware-change`. |

The collector verbs differ in one case. A managed credential that was never stored exits `1` from them, because the setup is not finished. It exits `2` from `--check-settings`.

`--diagnostics-bundle` uses six numbers:

| Number | Meaning |
| --- | --- |
| `0` | The bundle was written and every section was read. |
| `1` | A bad argument, a config that does not parse or validate, an unusable connection string, or an unknown `--server`. No file is written. |
| `2` | The store could not be opened. A reduced bundle is still written. |
| `3` | The bundle was written, but one or more sections failed. The manifest in the file lists which. |
| `4` | The output path cannot be used, the file exists and `--force` was not given, or the size cap could not be met. No file is written. |
| `5` | The check after aliasing still found a known name or secret. No file is written. |

`--harden-files` exits `0` when every file is secured, and `1` when a file is still readable by an account that should not read it. With the flag `--tls-key-files-only`, the exit code is `0` whenever the step ran, whatever it found for each file. It is `2` only when the step could not run. The causes are a prompt that is not elevated, a `darling.json` that does not load, and no registered service account.

## Verbs that need more than a row

### `--add-server`

`--add-server` and `--add-servers` are the same verb. There are two ways to give it servers.

1. Pipe a JSON array to standard input. The shape is the one the `add_servers` MCP tool takes.
2. Give one server in flags: `--host`, `--name`, `--auth`, `--username`, `--engine`, `--database`. A SQL login is asked for its password, and the password is hidden.

`--auth` takes `windows` (or `integrated`, the same thing), `sql`, `serviceprincipal` or `managedidentity`. The password is never a flag, so it does not land in the process list or the shell history. Add `--config <path>` to name a `darling.json`. The running service picks up the new servers without a restart.

Re-running the same file is safe. Servers that are already registered do not count as a failure. A server that collides with another one, a batch where nothing was added, and an entry that was rejected all exit `1`.

```
PerformanceMonitor.Darling.Service.exe --add-server --host sql01 --name sql01 --auth integrated
```

### `--harden-files`

```
PerformanceMonitor.Darling.Service.exe --harden-files [--config <path to darling.json>]
```

The service knows the correct permissions for its secret files but cannot apply them. This verb applies them, then checks every file and reports it as secured or still readable. The config path can also be the one bare argument after the verb. Do not give both forms.

### Collector settings

```
PerformanceMonitor.Darling.Service.exe --set-collector-frequency <collector> <minutes|default> [--server <server>] [--config <path>]
PerformanceMonitor.Darling.Service.exe --set-collector-retention <collector> <days|default> [--server <server>] [--config <path>]
```

`--set-collector-frequency` takes a number of minutes. `0` means once, when the server loads. `--set-collector-retention` takes a number of days, at least `1`. Both have an upper limit, and the verb refuses a value past it. `--help` prints the limits.

For both, `default` clears the stored value, so the next level applies. Without `--server`, the change is fleet-wide. Retention is always fleet-wide: the purge reads one value for each collector, so `--set-collector-retention` with a number and `--server` is refused. `--server` with `default` still works, to remove a retention an older build stored for one server. Each verb writes only its own value. The running service applies the change within one sweep.

### Verbs that take a bare config path

`--test-connection`, `--validate-config`, `--check-settings`, `--print-viewer-connection`, `--print-mcp-token`, `--print-web-token`, `--configure-network`, `--configure-firewall`, `--backfill-rollups`, `--collapse-legacy-slices` and `--recompress-plan-dim` take the path of a `darling.json` as one bare argument. The other verbs use `--config <path>`. Without a path, the verb uses `DARLING_CONFIG`, or the `darling.json` next to the exe.

### `--backfill-rollups`, `--collapse-legacy-slices` and `--recompress-plan-dim`

All three accept `--dry-run`. It prints the plan and changes nothing. `--recompress-plan-dim` also accepts `--vacuum-full` and `--no-vacuum-full`. By default it compacts the table after the conversion when that returns space. `--no-vacuum-full` skips the compaction. `--vacuum-full` compacts a table that was converted earlier.

`--backfill-rollups` and `--recompress-plan-dim` also accept `--free-space <size>`. Before it materializes anything or compacts the table, the verb checks that the store's data volume has room, and `--free-space` gives it the free space when it cannot measure it, for example when the store is in another container or on another host. The size is a whole number of bytes, or a number with K, M, G or T, optionally followed by B or iB, in 1024-based units. The `--dry-run` of both verbs shows the check and its result. `--recompress-plan-dim` refuses `--free-space` together with `--no-vacuum-full`, because that flag skips the check. See [the retention hold runbook](../retention-hold-runbook.md#on-linux-or-any-host-that-runs-the-service-as-a-net-app-6071).

They run on Linux against an own store. Against a managed store they need Windows, because only Windows can read the managed credential.

### `--drop-xe-sessions`

```
PerformanceMonitor.Darling.Service.exe --drop-xe-sessions <server-name> [--dry-run] [--config <path>]
PerformanceMonitor.Darling.Service.exe --drop-xe-sessions --print-sql
```

The first form finds the server the way `--validate-config` does and drops the Extended Events sessions on it. `--dry-run` lists them and drops nothing. The second form prints guarded `DROP` statements and connects to nothing. Use it for a server that is no longer configured. `--print-sql` takes no server name, no `--dry-run` and no `--config`.

### Diagnostics Bundle

To report a problem, write one file with the store's state, the recent service log and the collection health, and attach it to the issue:

```
PerformanceMonitor.Darling.Service.exe --diagnostics-bundle darling-bundle.json [--hours N] [--server <name>] [--log-dir <dir>] [--config <path>] [--alias-map <path>] [--include-log-text] [--force]
```

Run it elevated on the store host (a managed store's credential is protected by Windows). On Linux run `./PerformanceMonitor.Darling.Service --diagnostics-bundle darling-bundle.json`; with compose, run it with `docker compose exec` and copy the file out with `docker compose cp`. `--diag-bundle` is the short form. The file is one JSON document of at most 4 MiB. `--hours` (1 to 168, default 24) sets the window; `--server` limits the server-scoped sections to one server.

Server, host, database, login and domain names and IP addresses are replaced with ordinal aliases (`server-1`, `db-2`, `ip-1`), and credentials are removed. Before anything is written the verb scans the finished file for every name and secret it knows and writes nothing if one is still there. That proves only that the names it knows are gone, so open the file and check it before you attach it. `--alias-map <path>` writes a separate file that maps the aliases back to names; it starts with a `DO NOT ATTACH` line, and the bundle never contains it.

When the store cannot be reached, the registry cannot be read, so a server known only to it cannot be aliased: the service log is then written as counts only. `--include-log-text` adds the message text anyway (open the file before you attach it). `--alias-map` refuses to overwrite the bundle or an existing file without `--force`. The `store_unreachable` section holds no sentence, only an error class and a fixed reason code. For a managed store the bundle reads the store's own `postmaster.pid` when the first connect is refused: a store listening on another port than `postgres.port` is used there (a warning says so; the service reads the port only at start-up), and a store still starting is waited for (about 20 seconds). If it still cannot connect, the reason is `store_not_running` (no live store on the data directory, so start the service and read its log), `store_starting`, or `data_directory_missing` (the bundle is reading a `darling.json` whose data directory is not the service's: pass `--config` with the install folder's file); a bare `connect_failed` means the store is listening on the configured port and still refused.

Exit codes: see [Exit codes](#exit-codes). If the service log is not where the verb looks, the `service_log` section says so. Pass `--log-dir`.

### `--grant-script`

```
PerformanceMonitor.Darling.Service.exe --grant-script <server-name> [--finops] [--azure] [--rds] [--config <path>]
PerformanceMonitor.Darling.Service.exe --grant-script --host <host> [--auth sql|integrated|windows|serviceprincipal|managedidentity] [--username <login>] [--finops] [--azure] [--rds]
```

The first form reads the server from the configuration. The second form is for a host that is not added yet. Both print T-SQL, and neither connects to a monitored server. The script is safe to run twice. `--azure` prints the Azure SQL Database form and a `*.database.windows.net` host implies it. `--rds` prints the Amazon RDS for SQL Server form, which a `*.rds.amazonaws.com` host implies. Run the RDS form as the RDS master user, see [AWS RDS for SQL Server](../permissions.md#aws-rds-for-sql-server).

### `--export-investigations`

```
PerformanceMonitor.Darling.Service.exe --export-investigations <file> [--since <date>] [--until <date>] [--server <name>] [--verdict right|partly|wrong] [--force] [--config <path>]
```

The file gets one JSON object per line. Without `--force`, the verb does not overwrite a file that exists.

### The alert source verbs

```
PerformanceMonitor.Darling.Service.exe --add-alert-source <name> [--config <path>]
PerformanceMonitor.Darling.Service.exe --rotate-alert-source <name> [--config <path>]
PerformanceMonitor.Darling.Service.exe --remove-alert-source <name> [--config <path>]
PerformanceMonitor.Darling.Service.exe --list-alert-sources [--config <path>]
```

A name has 2 to 31 characters. It starts with a lower-case letter, then has lower-case letters, digits or underscores. `--add-alert-source` and `--rotate-alert-source` print the connection string once, and Darling keeps no copy.
