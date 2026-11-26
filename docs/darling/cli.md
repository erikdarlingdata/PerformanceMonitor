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

## The Linux form

The examples on this page use the Windows form, `.\PerformanceMonitor.Darling.Service.exe --verb`. On Linux under systemd the same verb is `sudo runuser -u darling -- env DARLING_CONFIG=/etc/darling/darling.json dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll --verb`. [Service verbs on Linux](linux.md#service-verbs-on-linux) has the table, with what to do in place of each verb that is Windows only.

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
| `--edit-server` | Changes the settings of a server that is already registered. | no | either (1) | `0`, `1` | [Edit-server](#--edit-server) |
| `--grant-script` | Prints the T-SQL that grants Darling its access on a monitored server. | no | either (1) | `0`, `1` | [Permissions](../permissions.md) |
| `--print-viewer-connection` | Prints a connection string for a remote Viewer. Windows only. | no | either | `0`, `1` | [Connect a remote Viewer](viewer.md#manual-configuration-fallback) |
| `--export-viewer-config` | Writes a ready-to-copy folder for a remote Viewer. Windows only. | no | either | `0`, `1` | [Export the handoff folder](viewer.md#1-export-the-handoff-folder-on-the-service-host) |
| `--print-mcp-token` | Prints the MCP access token again. Windows only. | yes | either | `0`, `1` | [MCP endpoint](network-endpoints.md#mcp-endpoint-assistant-over-the-lan) |
| `--print-web-token` | Prints the web dashboard access token again. Windows only. | yes | either | `0`, `1` | [Web endpoint](network-endpoints.md#web-endpoint-browser-over-the-lan) |
| `--configure-network` | Asks questions, then edits `darling.json` to expose the store, MCP or web endpoint on the network. Windows only. | no, but the service restart and firewall steps need it | either | `0`, `1` | [Guided setup](network-endpoints.md#guided-setup---configure-network) |
| `--configure-firewall` | Creates or removes the scoped firewall rules so they match `darling.json`. Windows only. | yes | either | `0`, `1` | [Firewall rules](network-endpoints.md#firewall-rules---configure-firewall) |
| `--harden-files` | Applies the locked-down permissions to `darling.json`, the store credentials, the log-hash key and the TLS key files. Windows only. | yes | either | `0`, `1` | [harden-files](#--harden-files) |
| `--grant-cert-key-access` | Gives the service account Read on the private key of a certificate in the Windows certificate store, for the `store` form of `web.network.tls` and `mcp.network.tls`. Windows only. | yes, for `LocalMachine` | either | `0`, `1` | [Certificate from the Windows certificate store](network-endpoints.md#certificate-from-the-windows-certificate-store) |
| `--enable-mcp`, `--disable-mcp` | Turns the MCP endpoint on or off in the store. Windows only. | no, but the firewall step needs it | either (1) | `0`, `1` | [Headless enable and disable](network-endpoints.md#headless-enabledisable--firewall---enable-mcp----enable-web) |
| `--enable-web`, `--disable-web` | Turns the web dashboard on or off in the store. Windows only. | no, but the firewall step needs it | either (1) | `0`, `1` | [Headless enable and disable](network-endpoints.md#headless-enabledisable--firewall---enable-mcp----enable-web) |
| `--enable-collector`, `--disable-collector` | Turns one collector on or off, for every server or one server. | no | either (1) | `0`, `1`, `2` | [Enable or disable a collector](collector-settings.md#enable-or-disable-a-collector-headlessly---enable-collector----disable-collector) |
| `--set-collector-run-at` | Sets the time of day a daily collector starts. | no | either (1) | `0`, `1`, `2` | [Set the run time](collector-settings.md#set-the-time-of-day-a-daily-collector-runs---set-collector-run-at) |
| `--set-collector-frequency` | Sets how often a collector runs, in minutes. | no | either (1) | `0`, `1`, `2` | [Collector settings](#collector-settings) |
| `--set-collector-retention` | Sets how many days a collector keeps its data. | no | either (1) | `0`, `1`, `2` | [Collector settings](#collector-settings) |
| `--reset-password-key` | Marks the current password key as replaced, so the next start makes a new one. With `passwordKey` set, the service makes no key: the verb says to put a new key in the `passwordKey` source, then restart the service. | no | either (1) | `0`, `1`, `2` | [Make a new password key](collector-settings.md#make-a-new-password-key---reset-password-key) |
| `--set-login-password` | Saves one password, read from stdin, for every server that uses a login, disabled servers included. Use it after a store move or a password key reset. `--dry-run` lists what it would do. | no | either (1) | `0`, `1`, `2` | [Set-login-password](#--set-login-password) |
| `--new-password-key` | Writes a new password key to a path where nothing exists, for a `passwordKey` secret. Prints the path and the key's fingerprint, never the key. The file is readable only by the account that runs it (on Windows also SYSTEM and Administrators), so run it as the service's account. On Windows, where nobody can run a command as the default service account, move the key into your secret store and delete the file, or keep it in a folder locked down as the security page shows and run `icacls <file> /reset`, so that it takes the folder's permissions. | no | no | `0`, `1` | [Keeping the key outside the service](security.md#security--least-privilege-roles) |
| `--drop-xe-sessions` | Drops the Extended Events sessions that Darling created on a server. | no | either (1) | `0`, `1`, `2` | [Drop the sessions](collector-settings.md#drop-the-extended-events-sessions-a-removed-server-left-behind---drop-xe-sessions) |
| `--add-alert-source` | Creates a login for a system that sends alerts, and prints its connection string once. | no | either (1) | `0`, `1`, `2` | [Alert sources](agents-and-alert-sources.md#make-a-login-for-a-system-that-sends-alerts---add-alert-source-and-friends) |
| `--rotate-alert-source` | Gives an alert source a new password and prints its connection string once. | no | either (1) | `0`, `1`, `2` | [Alert sources](agents-and-alert-sources.md#make-a-login-for-a-system-that-sends-alerts---add-alert-source-and-friends) |
| `--remove-alert-source` | Drops an alert source's login and its queued alerts. | no | either (1) | `0`, `1`, `2` | [Alert sources](agents-and-alert-sources.md#make-a-login-for-a-system-that-sends-alerts---add-alert-source-and-friends) |
| `--list-alert-sources` | Lists the alert sources. It never prints a password. | no | either (1) | `0`, `1`, `2` | [Alert sources](agents-and-alert-sources.md#make-a-login-for-a-system-that-sends-alerts---add-alert-source-and-friends) |
| `--export-investigations` | Writes the investigation cases that have feedback, one JSON object per line. | no | either (1) | `0`, `1`, `2`, `130` | [Export the judged cases](agents-and-alert-sources.md#export-the-judged-cases-to-test-your-own-agent-6236) |
| `--object-usage-compare` | Reads one database's object usage two ways, from the stored chain and from every snapshot, and says whether the answers match. It changes nothing. | no | either (1) | `0`, `1`, `2`, `3`, `4` | [`--object-usage-compare`](#--object-usage-compare) |
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

`--test-connection`, `--validate-config`, `--encrypt-password`, `--add-server`, `--edit-server`, `--grant-script`, `--print-viewer-connection`, `--export-viewer-config`, `--print-mcp-token`, `--print-web-token`, `--configure-network`, `--configure-firewall`, `--harden-files`, `--grant-cert-key-access`, `--new-password-key`, `--enable-mcp`, `--disable-mcp`, `--enable-web`, `--disable-web`, `--backfill-rollups`, `--collapse-legacy-slices` and `--recompress-plan-dim`.

`--help` and `--version` always exit `0`.

The collector verbs, `--reset-password-key`, `--set-login-password`, the alert source verbs, `--export-investigations` and `--drop-xe-sessions` add a third number:

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

`--object-usage-compare` uses `0`, `1` and `2` as the collector verbs do, and adds `3` (the two answers differ) and `4` (the chain cannot answer the window). See [`--object-usage-compare`](#--object-usage-compare).

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
2. Give one server in flags: `--host`, `--name`, `--auth`, `--username`, `--engine`, `--database`, `--port`. A SQL login is asked for its password, and the password is hidden.

In the flags form the password is read the way `--set-login-password` reads it. At a terminal it is typed at the prompt, where it is not shown. From a pipe or a file it is one line of UTF-8 text, read as bytes, so the characters saved are the ones sent. A trailing newline is fine. Stdin that holds more than one non-empty line, starts with a byte order mark, holds a NUL byte (UTF-16 does) or a lone carriage return, is not UTF-8, or is longer than a password can be is refused with exit code `1`, and no server is added. A password that starts with `env:` or `file:` is refused the same way, whether it is piped or typed, because the flags form saves a password and does not take a reference. The JSON form still accepts an `env:` or `file:` reference as a server's password.

`--auth` takes `windows` (or `integrated`, the same thing), `sql`, `serviceprincipal` or `managedidentity`. The password is never a flag, so it does not land in the process list or the shell history. Add `--config <path>` to name a `darling.json`. The running service picks up the new servers without a restart.

`--port` takes a whole number from 1 to 65535 and is for a PostgreSQL server that listens on a port other than 5432. It is the `port` of the JSON form, so the same rules apply: a SQL Server target takes its port in the host as `host,port` and refuses `--port`. A value that is not a whole number from 1 to 65535 is refused before anything is added.

Re-running the same file is safe. Servers that are already registered do not count as a failure. A server that collides with another one, a batch where nothing was added, and an entry that was rejected all exit `1`.

```
PerformanceMonitor.Darling.Service.exe --add-server --host sql01 --name sql01 --auth integrated
```

### `--set-login-password`

After a store moves to another machine, or after `--reset-password-key`, no saved SQL login password opens. This verb saves one password for every server that uses one login, so a fleet that shares a login needs one entry. A server that is switched off is set like any other, and its line says the server is disabled. A server's remediation login, when its name is the same login, is set on a line of its own.

```
PerformanceMonitor.Darling.Service.exe --set-login-password --username monitor --dry-run
PerformanceMonitor.Darling.Service.exe --set-login-password --username monitor
```

The password is read from stdin, as one line of UTF-8 text. Pipe it in from a password manager, or type it at the prompt, where it is not shown. It is never an argument or an environment variable, and it is never printed or logged. The verb reads stdin as bytes, so the characters saved are the ones sent. A trailing newline is fine. Stdin that holds more than one non-empty line, starts with a byte order mark, holds a NUL byte (UTF-16 does) or a lone carriage return, is not UTF-8, or is longer than a password can be is refused, and nothing is changed. SQL Server takes a password of at most 128 characters. When the password is longer and a server that would get it is SQL Server, the verb stops before it saves anything and names the limit, and `--dry-run` says a run would stop. PostgreSQL servers have no such limit. An argument the verb does not know is refused and is never repeated in the message: the message gives its position only, because it may be a password typed by mistake. Option names match without regard to capitals. The `--username` value is compared exactly, capitals included.

Before it reads the password, the verb prints the store the configuration reaches (host, port and database) and the password key the password will be sealed to. Check them, and run with `--dry-run` first, when the configuration is not one you wrote.

The verb prints one line for each server that uses the login: the server, the login, and what it does.

| Line | Meaning |
| --- | --- |
| `set` (`would set` in a dry run) | The password is saved for this server. This covers a server with no saved password, a saved password sealed with another password key, one in the old Windows format, and one that is a different password. |
| `unchanged` | The saved password already opens and is the one you gave. |
| `skip` | The password does not come from a saved SQL login value: it is an `env:` or `file:` reference, darling.json supplies it (the service uses it, or writes it into the store again at each start), or the server's authentication is not a SQL login. A service principal's client secret is never replaced, even when its client id has the same text as the login. A remediation login that has a name and no saved password is not armed. The verb never writes a saved value over any of these. |

`--dry-run` reads the password the same way, prints the same list, and changes nothing. It also works before the password key is ready, and then says that a run would stop. A run saves every `set` server in one transaction, so it saves all of them or none, and it ends with the number of servers it changed. The service uses the new passwords at its next configuration reload, with no restart.

The remediation login of a server is sealed for its own connection settings, as the service reads it, and its line says `remediation login`. When the account that runs the verb cannot read the key file, the verb seals to the key the store publishes and prints its fingerprint, as `--add-server` does. The store's current key is held for the length of the run, and a run that finds it is no longer the key it chose stops with nothing saved.

Exit code `1` also means no password on stdin, stdin that is more than one line, no server with the login, or a password key that is not ready (the message says why). On Linux, run it as the service's user: see [Service verbs on Linux](linux.md#service-verbs-on-linux).

### `--harden-files`

```
PerformanceMonitor.Darling.Service.exe --harden-files [--config <path to darling.json>]
```

The service knows the correct permissions for its secret files but cannot apply them. This verb applies them, then checks every file and reports it as secured or still readable. The config path can also be the one bare argument after the verb. Do not give both forms.

### `--grant-cert-key-access`

```
PerformanceMonitor.Darling.Service.exe --grant-cert-key-access <thumbprint> [--account <name>] [--store-location LocalMachine|CurrentUser] [--store-name <name>]
```

Adds one Read entry for one account to the private key of the certificate the thumbprint names, and keeps every existing entry. It handles a legacy CryptoAPI key too, such as one from the `Microsoft RSA SChannel Cryptographic Provider`: it changes the access list of the key file under `MachineKeys`. If that file is missing, it prints the `icacls` command with that path. A smart card or a hardware module may stop Windows from opening the key's provider without a prompt. Then the command opens no card or PIN prompt. It stops, prints the provider's own message, and shows how to find the key file with `certutil`. The account is the one the service is registered under unless `--account` names another. The location and the store name default as they do in `darling.json`: `LocalMachine` and `My`. It grants to one account and refuses every group except the local Administrators group, whatever `--account` names: it asks Windows what the account is, and refuses a group, a computer or an account it cannot identify. It accepts a user (a domain user, a local user, a group managed service account) and these service identities: LocalSystem, LocalService, NetworkService, a per-service virtual account (`NT SERVICE\<name>`, only when a service with that name is registered on this machine), an IIS application pool identity and the local Administrators group. An empty `--account` or `--store-name`, and a `--store-name` with a backslash or a slash in it, are refused. It needs an elevated prompt for `LocalMachine`, and says so and changes nothing without one. An account that already has Read gets `Already granted`. Restart the service after it. The rules and the failures are in [Certificate from the Windows certificate store](network-endpoints.md#certificate-from-the-windows-certificate-store).

### `--edit-server`

```
PerformanceMonitor.Darling.Service.exe --edit-server <server name> --set <field>=<value> [--set <field>=<value> ...] [--config <path to darling.json>]
```

`--edit-server` changes the settings of a server that is already registered. After the first start the registry lives in the store, so editing `servers` in `darling.json` does nothing for a registered server. The verb is the way to change one on a headless host, where the Viewer (Windows only), the web dashboard and MCP are not available. It runs the same code as the MCP `edit_server` tool, so it makes the same checks. It runs as the store owner, as `--add-server` does. It needs Windows only for a managed store. A store you bring yourself (`postgres.connectionString`) works on any platform. With no arguments, the verb prints the usage and the field list. The server name must match a display name or a storage name exactly, capitals aside. A part of a name finds nothing, and the verb lists the servers whose names contain it. A string that more than one server answers to is refused, whichever kind of name it is for each: three servers that show it as their display name and a fourth that has it as its storage name are four matches. One server that has it as both is one match. The refusal lists every match with its id, its display name and its storage name. To name exactly one, use `id:` and its id (`--edit-server id:42 --set ...`), or a display name or storage name that no other listed server has. The MCP `edit_server` tool decides the same way and takes the same `id:` form. The verb finds the server once, then prints the store it will write to, the configuration file it read, and the server with its id, host and login, before it sends anything or asks for a password.

The fields are `name`, `host`, `port` (PostgreSQL servers only), `database`, `auth`, `username`, `encryptMode`, `trustServerCertificate`, `readOnlyIntent`, `multiSubnetFailover`, `queryStoreReplica`, `monthlyCostUsd` and `enabled`. They are the `darling.json` key spellings, matched without regard to case, and the snake_case keys of the `edit_server` MCP tool are accepted too. `auth` takes `windows` (or `integrated`, the same thing), `sql`, `serviceprincipal` or `managedidentity`. `encryptMode` takes `Optional`, `Mandatory` or `Strict`. `trustServerCertificate`, `readOnlyIntent`, `multiSubnetFailover` and `enabled` take `true` or `false`. `enabled` goes through the same path as the MCP `enable_server` and `disable_server` tools, by the id of the server the verb found at the start and never by a second name lookup, and the verb applies it after the other fields succeed.

The verb does not set a password, `engine`, `excludedDatabases`, `xeSessionDatabases`, the AWS role and external ID, plan capture or the alert delivery override. `--set password=...` is refused, because a password is never an argument. So are `encryptedPassword`, `pwd`, `passwd`, `secret`, `clientSecret` and `client_secret`, and the refusal says the value you typed is now in your shell history and the password should be changed. A server name or a `--set` value that holds a character that does not print is refused: a control character (a newline, a tab, any character below U+0020, DEL), a line or paragraph separator, a format character such as a right-to-left override or a zero-width space, a private-use or unassigned code point, or a lone surrogate. Letters outside ASCII and emoji are taken. Change the others in the Viewer, on the web dashboard's Admin page or through MCP.

A saved password is never sent with changed connection settings, such as a new host or a TLS downgrade that lets a middlebox read it, unless you type it again. So a change to how a server with a saved password (SQL or service-principal authentication) is reached needs that password again. That covers `host`, `port`, `database`, `auth`, `username`, `encryptMode`, `trustServerCertificate`, `readOnlyIntent`, `multiSubnetFailover` and `queryStoreReplica`. The prompt names the server, its host and its login, and says why it asks. Before the prompt the verb prints where the password will be sent for the connection test: the host, port, authentication and login the change sets. The verb asks for the password at a hidden prompt on a console, or reads stdin as bytes when input is redirected: one line of UTF-8 text, the same rules as [`--set-login-password`](#--set-login-password), so a byte order mark, a NUL byte, a second non-empty line, a lone carriage return, text that is not UTF-8 or a password that is too long is refused and nothing is changed. Each refusal is one line that names its cause. SQL Server takes a password of at most 128 characters, so for a SQL Server target a longer one is refused before anything is sent, with a line that names the limit. A PostgreSQL target has no such limit. Both requests carry the server's `modified_at` as the expected value, so a server that changed while the prompt waited is refused before the password is used, and the verb says so. A server that changed during the connection test is refused by the write. The password was sent for that test by then, so the verb says it was sent and was not saved. If the request that carries the password fails with an error, the verb prints one fixed sentence and the error's type and never the error's text, which could repeat the password. The password key is chosen again after the prompt and compared with the store's current key inside the write, so a key reset in between saves nothing. When the verb sealed a password to the key the store publishes, it prints the key id to check against the service log. It tests the connection with the new settings and saves only if the server answers. A failed test saves nothing. Windows and managed-identity servers need no password, and neither do changes to `name`, `monthlyCostUsd` or `enabled`.

The verb works while the service runs. The running service applies the change on its next config poll, within one sweep, with no restart.

Exit code `0` means the server was changed or already had the values. Exit code `1` means a change was not made. Causes include a bad argument, a server whose exact name is not found or matches more than one, a failed connection test, and a server that holds a remediation or hypothetical-index login, which refuses a change to how it is reached on every surface and says where to make it. Nothing is written in those cases. When `--set enabled=...` comes with other fields and the other fields save but `enabled` does not, whether the store refuses it or cannot be reached, the output lists what was saved, says `The settings above were saved`, and the exit code is `1`. The log lines the edit writes, which carry the driver's reason for a failed connection test and one audit line with the server id and the field names, print under the answer. No value and no password is in them. Every line the verb prints has characters that do not print written as `\uXXXX`, whatever a server name, a store answer or a driver message holds.

```
PerformanceMonitor.Darling.Service.exe --edit-server sql01 --set trustServerCertificate=true
```

At start-up the service compares the `servers` entry in `darling.json` with the registry for each enabled registered server. For each server that differs, it logs one WARN line, up to 10, then one line that names the rest. The line names each differing field with the file's value and the store's value, and gives the `--edit-server` command that applies the file's values. The command is the end of the line, and it runs as printed in the shell the line is for. On Linux that is bash or zsh. Each word that is not plain letters, digits and `. _ : / = -`, or that starts with `-`, `@` or `=`, is in single quotes, and the path of the service is quoted by the same rule. On Windows the line says `in PowerShell, run:`, and the command starts with `&` and the full path of `PerformanceMonitor.Darling.Service.exe` in single quotes, so it runs from any folder in Windows PowerShell 5.1 and in PowerShell 7, and cmd.exe refuses it at the first character. On both hosts the command ends with `--config` and the full path of the configuration file the service is using, quoted by the same rule, so the paste edits through that file on a host whose configuration is not at the default path. A path the rule cannot print gets no command. A Windows command is printed only when the path and every word hold nothing but letters, digits, `. _ : / = -`, spaces and backslashes, with no backslash at the end of a word. For any other value, such as a name with a quote, `&`, `%`, `$` or a letter outside ASCII, the line prints no command and says to run `--edit-server` by hand. On either host a value with a character that does not print gets no command. Each value in the line is in double quotes, a double quote inside a value is written as `\u0022`, and characters that do not print (control and format characters, private-use and unassigned code points) are written as `\uXXXX`. A field the verb does not set is named with where to change it, except `engine`: the engine of a registered server cannot be changed, so the line says to remove the server and add it again. When the file and the store match, there is no line. No password appears in it, because passwords are never compared.

On Linux, run it as the service's user: see [Service verbs on Linux](linux.md#service-verbs-on-linux).

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

### `--object-usage-compare`

```
PerformanceMonitor.Darling.Service.exe --object-usage-compare <server> <database> [days] [--config <path>]
```

The `get_object_usage` tool reads a chain that the service builds each hour. This verb checks that the chain gives the same answer as reading every snapshot. It reads the window both ways for one database, then prints `identical` with the time each read took, or each difference. `days` is how far back the window reaches. It is 90 if you leave it out. The verb only reads: it never builds the chain and never writes to the store, so a login that can read the store is enough.

The exit codes are the usual `0`, `1` and `2`, and two more. `3` means the two answers differ. `4` means the chain cannot answer that window yet, so there is nothing to compare. The tool reads every snapshot for such a window.

### `--grant-script`

```
PerformanceMonitor.Darling.Service.exe --grant-script <server-name> [--finops] [--azure] [--rds] [--column-stats-databases <db>]... [--config <path>]
PerformanceMonitor.Darling.Service.exe --grant-script --host <host> [--auth sql|integrated|windows|serviceprincipal|managedidentity] [--username <login>] [--finops] [--azure] [--rds] [--column-stats-databases <db>]...
```

The first form reads the server from the configuration. The second form is for a host that is not added yet. Both print T-SQL, and neither connects to a monitored server. The script is safe to run twice. `--azure` prints the Azure SQL Database form and a `*.database.windows.net` host implies it. `--rds` prints the Amazon RDS for SQL Server form, which a `*.rds.amazonaws.com` host implies. Run the RDS form as the RDS master user, see [AWS RDS for SQL Server](../permissions.md#aws-rds-for-sql-server). `--column-stats-databases` adds the optional database-level `SELECT` for the column_stats collector, in the databases you list and nowhere else. Give it once per database (`--column-stats-databases SalesDb --column-stats-databases "Sales, Archive"`): each value is one name taken whole, so a name with a comma or a space works when it is quoted, and a comma never splits a list. See [Column selectivity](../permissions.md#column-selectivity-per-database-select-optional).

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
