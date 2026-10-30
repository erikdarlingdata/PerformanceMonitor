# Troubleshooting

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

Find what you see in the table for your product. Each row gives the likely cause, the fix, and a link to the page that explains the fix. [Lite](#lite) is first and [Darling](#darling) is second.

## Lite

Lite writes its logs to `%LOCALAPPDATA%\PerformanceMonitorLite-Data\logs`, one `lite_<date>.log` file per day. Lite keeps each file for 7 days. In the sidebar, **View Log** opens today's file and **Open Log Folder** opens the folder. Lite also logs the success or failure of each collection to the `collection_log` table in DuckDB.

Verbosity is the `log_minimum_level` key in `settings.json`. It has no UI. See `Lite/config/settings.sample.json`. The value is one of `Trace`, `Debug`, `Information`, `Warning`, `Error`, `Critical` or `None`. It defaults to `Information`. Lite reads it at startup, so a change needs a restart.

Set it to `Debug` when you need the per-database collection timing lines. They look like `[server] collector [db] => N rows (sql:Nms, duckdb:Nms)`, one per database per collector per cycle. They are below the default because at that rate they bury the collection failures that you usually open the log for. The total that they add up to is in `duration_ms` in `collection_log`, which Lite records at every level.

### Lite symptoms

| What you see | Likely cause | Fix | More |
|---|---|---|---|
| Lite stops at start. The exit code is `0x80131506` and stderr shows `Fatal error. Your Windows doesn't fully support CET.` `Setup.exe` ends with "Install Partially Succeeded". | Windows is missing updates that .NET 10 needs on a CPU with CET. | Install all available Windows updates and restart. A stopgap exists. | [Lite stops at start with a CET message](#lite-stops-at-start-with-a-cet-message) below |
| No data after you connect. | The first collection cycle has not run. It takes 1 to 5 minutes. | Wait for one cycle. If it still shows nothing, read the log for connection errors. | [Lite guide](lite-guide.md) |
| A server does not connect. The error mentions "A network-related or instance-specific error". This text comes from the SQL Server client, not from Lite. | A firewall blocks the TCP port of the instance, or SQL Server is not listening on TCP. | Open the port in the firewall between the Lite machine and the server. The default port is 1433. If the port is different, write the host as `name,port`. Turn on TCP/IP for the instance in SQL Server Configuration Manager. | [Permissions](permissions.md#permissions) |
| An Azure SQL Database server does not connect. The error is 40615. | The server firewall did not allow the IP address of the Lite machine. | Add a server-level firewall rule, or a database-level rule in master. If a database-level rule in a user database allows the IP, set the Database for the server to that database. | [Permissions for Azure SQL Database](permissions.md#azure-sql-database-lite--darling) |
| A login or permission error on connect. | The login lacks a required permission. | Grant the permissions listed for your platform. For Azure SQL Database, use a contained database user with `VIEW DATABASE STATE`. | [Permissions](permissions.md#permissions) |
| A server that uses Entra ID (MFA) or Azure Device Code shows `Skipped - requires interactive authentication` or `Authentication cancelled by user`. The log says `interactive authentication cancelled by user`. | These two sign-ins are interactive. The background check never raises a sign-in window, and Lite stops asking after you decline or close one. A device-code window expires in about 3 minutes. | Double-click the server in the sidebar and sign in again. | [Authentication](authentication.md) |
| A server that uses Existing Azure Sign-In fails to connect. | That mode never asks for a sign-in. It uses the Azure identity that is already on the machine, and none is valid. | Run `az login` as the same Windows user that runs Lite, then connect again. | [Existing Azure Sign-In](authentication.md#existing-azure-sign-in-lite-only-activedirectorydefault) |
| Entra ID (MFA) fails with `0x80070520` or `unknown_broker_error`. | The Windows account broker cannot reach your logon session from a process that another account launched, such as an elevated Lite. | Use Azure Device Code for that server. | [Authentication](authentication.md) |
| The Query Store tab is empty. | Query Store is off for the target database. | Run `ALTER DATABASE [YourDB] SET QUERY_STORE = ON`. | [Permissions](permissions.md#permissions) |
| The blocked process reports are empty. | The blocked process threshold is still 0. Lite tries to set it to 5 seconds with `sp_configure`. On AWS RDS, `sp_configure` is not available. On Azure SQL Database, the threshold is fixed at 20 seconds. | On AWS RDS, set `blocked process threshold (s)` in an RDS Parameter Group. On other platforms, check that the login has the `ALTER SETTINGS` permission. | [Platform notes](platform-notes.md#aws-rds-parameter-group-configuration) |
| FinOps Index Analysis hangs, times out, or returns `Msg 229` on `sql_expression_dependencies`. | The login lacks the per-database grants for this tab. | Apply the full grant set for the databases that you analyze. It fixes both failures. | [FinOps Index Analysis grants](permissions.md#finops-index-analysis-per-database-grants) |
| A server that is reachable only over a named pipe does not connect. | Lite connects to a remote SQL Server over TCP only. It refuses a host that starts with `np:`, `lpc:` or `admin:`, or a path that starts with `\` or `/`. | Turn on TCP/IP for the instance in SQL Server Configuration Manager. If the port is not 1433, write the host as `name,port`. | [Permissions](permissions.md#permissions) |

### Lite stops at start with a CET message

Symptom: Lite stops at start. The exit code is `0x80131506` and stderr shows this message:

```text
Fatal error. Your Windows doesn't fully support CET. Please install all available Windows updates.
```

`Setup.exe` then ends with "Install Partially Succeeded" and gives no reason, because Velopack shows a fixed message when the app's install step fails. Velopack's velopack.log records the same exit code.

Cause: .NET 10 apps need current Windows updates to start on a CPU with CET (Control-flow Enforcement Technology, a hardware protection). Without them, Lite stops at start. We saw this on Windows Server 2022 build 20348.1366. The exit code is `0x80131506` and stderr shows the message quoted above.

Fix: install all available Windows updates and restart. If you cannot do that yet, there is a short-term workaround. Run it from an elevated PowerShell before `Setup.exe` or the first start. It turns off one protection (`UserShadowStack`, the hardware-enforced stack protection) for the Lite exe only:

```powershell
Set-ProcessMitigation -Name PerformanceMonitorLite.exe -Disable UserShadowStack
```

This weakens Lite's protection against stack-corruption attacks, so treat it as a stopgap. To undo it once Windows is patched, remove the values the first command wrote:

```powershell
Remove-ItemProperty -Path 'HKLM:\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Image File Execution Options\PerformanceMonitorLite.exe' -Name MitigationOptions, MitigationAuditOptions, EAFModules -ErrorAction SilentlyContinue
```

On Windows Server 2022 build 20348.5622, `Set-ProcessMitigation -Remove -Disable UserShadowStack` did not remove the setting (it returned `C000000D`). `Set-ProcessMitigation -Reset` failed too ("Invalid input parameters"). The registry way above worked. The `Set-ProcessMitigation -Disable` command writes three values (`MitigationOptions`, `MitigationAuditOptions`, `EAFModules`) under the exe's key in `Image File Execution Options`. The undo removes only those values and leaves any other value on that key alone. Restart the program afterwards. The first command is described in Microsoft's [Set-ProcessMitigation](https://learn.microsoft.com/en-us/powershell/module/processmitigations/set-processmitigation) reference.

## Darling

The service log is the first place to look. On Windows it is `%ProgramData%\PerformanceMonitorDarling\logs\darling-service_<yyyyMMdd>.log`. The installer shows the same lines while it waits for the first start, and the first start takes about two minutes. It is not a hang.

### Darling symptoms

| What you see | Likely cause | Fix | More |
|---|---|---|---|
| The service log has a critical line that starts `This service is installed somewhere its own account cannot read:`. Or the install is done but the service does not start. | The service runs as the virtual account `NT SERVICE\PerformanceMonitor Darling`. That account cannot read a folder under a user profile (`C:\Users\...`), a UNC path or a mapped drive. | Stop the service. Move the install to `C:\Program Files\PerformanceMonitorDarling`. Run `install-darling.ps1` again from the new folder. `darling.json`, the store and the credentials stay where they are. | [Install Darling, Step 3](darling/install.md#step-3-extract-to-a-machine-scoped-path) and [the install location](darling/install.md#the-install-location-has-to-be-machine-scoped) |
| The service log says `initdb failed (exit code ...)` for `C:\ProgramData\PerformanceMonitorDarling\pg`. An exit code of `-1073741515` (`0xC0000135`, `STATUS_DLL_NOT_FOUND`) comes with an empty `Output:`. | Windows stopped `initdb.exe` in the loader before it ran. Two causes are common. The Microsoft Visual C++ runtime files are missing from the PostgreSQL `bin` folder. Or the service account cannot read the install folder (the row above). | To tell the two apart, run `initdb.exe --version` from the PostgreSQL `bin` folder in an elevated prompt. If that fails too, the files are missing. Redeploy the package, or install the Microsoft Visual C++ 2015-2022 x64 redistributable. If it works, move the install as in the row above. | [Install Darling, Step 7](darling/install.md#step-7-wait-about-two-minutes) |
| The service log says `Managed Postgres bootstrap failed`. Or PostgreSQL does not start. | The bundled PostgreSQL could not be unpacked, initialized or started. The message names the failing step. | Read the PostgreSQL server log, `pg.log`. By default it is `C:\ProgramData\PerformanceMonitorDarling\pg.log`, beside the data folder `pg`. If `postgres.dataDirectory` is set, it is beside that folder. The service log quotes the tail of it. | [Managed Bundled PostgreSQL](darling/managed-postgresql.md#managed-bundled-postgresql) |
| The service log says `Cannot load configuration`, `Configuration problem: ...` or `Cannot reach or migrate the Postgres store`. The service is running but idles. | There is no `darling.json` at the path it tried, a field fails validation, or the store connection string or login is wrong. | Read the message, which names the path or the field. Fix the file and restart the service. A file that another process holds open is retried for two minutes first. | [Darling troubleshooting](darling/troubleshooting.md) and [Install Darling, Step 5](darling/install.md#step-5-pre-flight) |
| The service or the Viewer stops at start. The exit code is `0x80131506` and stderr shows `Your Windows doesn't fully support CET.` | Windows is missing updates that .NET 10 needs on a CPU with CET. `install-darling.ps1` recognizes the message in its pre-flight and stops. | Install all available Windows updates and restart. The operator guide has a stopgap for both exe files. | [The CET error](darling/install.md#windows-stops-at-start-the-cet-error) |
| A .NET host error says that it cannot find the framework `Microsoft.AspNetCore.App`, or that you must install .NET. This applies to the Linux tarball only. | The Windows zip carries its own .NET runtime, so Windows never shows this error. The Linux tarball is not self-contained. It needs the ASP.NET Core 10 runtime and the .NET 10 runtime. | Install both runtimes from the .NET 10 download page. The container image already has both. | [Run on Linux](darling-quick-start.md#run-on-linux-docker-compose-or-systemd) and [Prerequisites](darling/install.md#prerequisites) |
| `--test-connection` passes for a server that uses Windows authentication, but the service fails to connect. The output has `NOTE: this test ran as ..., the person at this console, not as the service.` | The command-line test runs as you. The service runs as its own account, `NT SERVICE\PerformanceMonitor Darling`, unless you changed it. | Start the service and run `--test-connection` again. It then asks the running service, and the output says `Tested by the running service`. If the service fails with a login error, run `CREATE LOGIN [NT SERVICE\PerformanceMonitor Darling] FROM WINDOWS;` and grant the permissions. | [Pre-flight in the operator guide](darling/install.md#validate-the-config-pre-flight) and [Permissions](permissions.md#permissions) |
| A server shows no data, and `PERMISSIONS` rows are in `collection_log`. | The login was denied reads for that collector (SQL errors 229, 297 or 300). | Grant the permission. The collector retries each cycle and recovers when the grant lands. | [Permissions on monitored servers](darling/monitored-servers.md#permissions-on-monitored-servers) |
| The service log says `Failed to ensure XE sessions`. Deadlock and blocked process data stay empty. | The login lacks `ALTER ANY EVENT SESSION`. | Grant the permission, or have an administrator create and start the sessions `PerformanceMonitor_Deadlock`, `PerformanceMonitor_BlockedProcess` and `PerformanceMonitor_Attention`. | [The Extended Events sessions](darling/monitored-servers.md#the-extended-events-sessions-the-service-creates) |
| The Recommendations tab shows no findings. | Analysis needs 24 hours of collected data for each server. A new install has none yet. | Wait one day. | [Install Darling](darling/install.md#things-that-will-bite-you) |
| The Viewer will not connect. | The Viewer read a different `darling.json` than the one you edited, or a value in it is wrong. | Read the **Configuration this viewer used** block in the failure window. It names the file and the rule that picked it. | [The Viewer will not connect](darling/troubleshooting.md) and [The WPF viewer](darling/viewer.md) |
| An MCP client cannot connect. | MCP is off by default. If the log says `Port 5152 is already in use`, another program holds the port. A client that gets 401 is missing the bearer token. | Turn MCP on in the Viewer settings or set `mcp.enabled: true` in `darling.json`. If the port is busy, change `mcp.port`. | [MCP server](mcp-server.md#darling) |
| A server that is reachable only over a named pipe does not connect. | The service connects to a remote SQL Server over TCP only. | Turn on TCP/IP for the instance in SQL Server Configuration Manager. If the port is not 1433, write the host as `name,port`. | [Permissions](permissions.md#permissions) |

### Send a bug report

Run the service with `--diagnostics-bundle <path>` on the store host, from an elevated prompt:

```powershell
PerformanceMonitor.Darling.Service.exe --diagnostics-bundle darling-bundle.json
```

The command writes one JSON file with the store's state, the collection health, slow reads, statement history and the recent service log. It replaces server, host, database and login names and IP addresses with aliases and leaves credentials out. It checks the finished file for every name that it knows and writes nothing if one remains. That check covers only the names it knows. Open the file and read it before you attach it to an issue.

The operator guide lists every option, including `--hours`, `--server` and `--alias-map`. See [Diagnostics Bundle](darling/cli.md#diagnostics-bundle).

### More Darling help

- [Darling troubleshooting](darling/troubleshooting.md) has the full list of service log lines and what each one means.
- [Things that will bite you](darling/install.md#things-that-will-bite-you) and [Known gaps](darling/install.md#known-gaps) cover install surprises.
- [Running Darling day to day](darling/admin.md#where-everything-is) lists where each file is.
- The Dashboard (Full edition) is in [deprecated/Dashboard/README.md](../deprecated/Dashboard/README.md).
