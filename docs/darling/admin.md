# Running Darling day to day

This page is for the person who keeps a Darling install running after the first week. Each heading below is one task, written as numbered steps. When you need to know what a setting means, follow the link to the [Darling operator guide](../../Darling/README.md).

## Read this first: `darling.json` is only a seed for the server list

The service reads the `servers` array in `darling.json` on the first start only. After that, the store holds the server list. If you edit the array later, nothing happens.

To change the list, use one of these:

- The Viewer: **Manage Servers**.
- The `add_servers`, `edit_server` and `remove_server` MCP tools. They need `mcp.allowWrites` and `mcp.allowServerAdmin`.
- The `--add-server` command line verb. It can add a server. It cannot remove one.

The web Admin page can edit a server that already exists. It cannot add, remove, enable or disable one. The page says so itself. For the full list of what the file still controls, see [`darling.json` is only a seed](install.md#darlingjson-is-only-a-seed).

Run the command line verbs on the service host, from the folder that holds `PerformanceMonitor.Darling.Service.exe`. If a verb reads a credential or a token, run it from an elevated PowerShell.

## Add a server

1. Make sure the monitoring login exists on the target server. To print the T-SQL that creates it, run `.\PerformanceMonitor.Darling.Service.exe --grant-script --host <host>`. See [Permissions on monitored servers](../../Darling/README.md#permissions-on-monitored-servers).
2. Add the server in one of these ways:
   - In the Viewer, click **Add Server** in the sidebar, fill in the dialog and save.
   - On the command line, for a Windows-authentication server, run `.\PerformanceMonitor.Darling.Service.exe --add-server --host sql01 --auth windows`.
   - On the command line, for a SQL login, run `.\PerformanceMonitor.Darling.Service.exe --add-server --host sql01 --auth sql --username darling_monitor`. The verb asks for the password and does not echo it. There is no password flag, on purpose.
   - For many servers, put a JSON array in `servers.json` and run `Get-Content servers.json | .\PerformanceMonitor.Darling.Service.exe --add-server`.
   - Through MCP, call `add_servers` with the same JSON array.
3. Read the verb's output. It tests the connection and prints one line per server. A server that cannot be reached is not added. Re-running a batch that is already added is not an error.
4. Wait for the service's next pass over the server list. Then check the server's **Collection Health** tab in the Viewer.

A Windows-authentication server is tested by the running service, as the account the service runs under, when the verb can reach the service. If it cannot, the verb tests the server as you and says so.

## Remove a server

There is no command line verb that removes a server. Use the Viewer or MCP.

1. Optional, and only while the service still monitors the server: run `.\PerformanceMonitor.Darling.Service.exe --drop-xe-sessions <server-name> --dry-run`. If the list is right, run it again without `--dry-run`. This also drops the shared sessions (the deadlock and blocked process sessions) that removal leaves alone. The shared sessions then stop capturing until the service reconnects. If another Darling install monitors the server, skip this step.
2. Remove the server in one of these ways:
   - In the Viewer, open **Manage Servers**, select the server and click **Delete**. Confirm the question.
   - Through MCP, call `remove_server` with the server's display name or storage name. The call has no confirm step.
3. Wait for the next config reload. The service stops collecting from the server within one pass.

What removal changes:

- **On the monitored server:** the service drops this install's own Extended Events sessions on it, with one attempt within 15 seconds. It does not drop the shared sessions, and it does not drop a session that another registration of this install keeps. If the attempt failed, run `.\PerformanceMonitor.Darling.Service.exe --drop-xe-sessions --print-sql` and run the statements it prints on the server. See [Drop the Extended Events sessions a removed server left behind](../../Darling/README.md#drop-the-extended-events-sessions-a-removed-server-left-behind---drop-xe-sessions).
- **In the store:** the server's definition is deleted. The history that Darling already collected stays.
- **The monitoring login:** removal does not touch it. If you no longer need it, drop it on the server yourself.

To stop collecting from a server and keep its definition, disable it instead. See [Pause and resume collection](#pause-and-resume-collection).

## Change a monitoring password

Which steps you follow depends on where the server entry came from.

For a server you added with the Viewer, `--add-server` or MCP:

1. Change the password on the monitored server.
2. Open the server for editing. In the Viewer, use **Manage Servers**, select the server and click **Edit**. On the web dashboard, open the **Admin** page, then the Servers tab, and click **Edit** on the row. Through MCP, call `edit_server`.
3. Type the new password and save.
4. Check the server's **Collection Health** tab after the next pass.

A saved password is sealed to the service's password key and to the connection it was saved with. If you also change the host, port, database, login or encryption setting, enter the password again in the same edit. See [The password key](../../Darling/README.md#security--least-privilege-roles).

For a server that still takes its password from `darling.json` (`encryptedPassword`):

1. Change the password on the monitored server.
2. On the service host, run `.\PerformanceMonitor.Darling.Service.exe --encrypt-password`. Type the new password when it asks. The verb prints a base64 blob.
3. Paste the blob into the server's `encryptedPassword` in `darling.json`. Edit the file as text. It is JSONC, so do not round-trip it through `ConvertFrom-Json`.
4. Restart the service.

The blob is bound to the machine. Make it on the machine that runs the service.

If you reset the password key with `--reset-password-key`, every password saved in the store has to be entered again. See [Make a new password key](../../Darling/README.md#make-a-new-password-key---reset-password-key).

## Rotate the web or MCP token

The token is a secret in `darling.json`. The `web.network` and `mcp.network` blocks are read from the file only, so a change needs a restart.

1. Make a new token. It must have at least 16 characters, at least 6 of them different. To make one, use the PowerShell one-liner under [MCP endpoint](../../Darling/README.md#mcp-endpoint-assistant-over-the-lan).
2. On the service host, run `.\PerformanceMonitor.Darling.Service.exe --encrypt-password`. Paste the token when it asks. The verb prints a blob.
3. In `darling.json`, replace the blob in `web.network.encryptedToken` (the dashboard) or `mcp.network.encryptedToken` (MCP).
4. Restart the service.
5. In an elevated PowerShell, run `.\PerformanceMonitor.Darling.Service.exe --print-web-token` or `--print-mcp-token`. Check that it prints the new token. This writes a live token to the screen, so do it where no one else can see it.
6. Give the new token to every client. Browsers sign in again on the dashboard's sign-in page. Each MCP client needs the new bearer header. The old token stops working at the restart.

`--configure-network` can also make a token for you. It generates 32 random characters.

## Pause and resume collection

To pause all collection:

1. In the Viewer, open **Settings**. Under **Data Collection**, click **Pause Collection**. Wait for the status line to change.
2. Or call `pause_collection` through MCP. It needs `mcp.allowWrites`. Then call `get_command_result` and `get_collection_state` to see that it took effect.

To resume, click **Resume Collection** in the same place, or call `resume_collection`.

A pause stops collection, alerting, analysis and purge for the whole service. There is no command line verb for a global pause. A Viewer opened read-only cannot pause.

To pause one server:

1. In the Viewer, open **Manage Servers**, right-click the server and click **Enable / Disable Collection**.
2. Or call `disable_server` through MCP, and `enable_server` to bring it back.

The server's definition and history stay.

To turn one collector off for the whole fleet, run `.\PerformanceMonitor.Darling.Service.exe --disable-collector <collector>`. Add `--server <name>` to change one server only. Use `--enable-collector` to turn it back on. If you turn off `long_query_completions`, the service drops its Extended Events session on the monitored servers.

## Upgrade

Do the steps from an elevated PowerShell on the service host.

1. Download the new zip. Take its SHA256 from the release page.
2. Extract it to a staging folder that only an administrator can write to, for example `C:\Program Files\PerformanceMonitorDarling-staging\<version>`. Do not use a folder made directly under `C:\`.
3. Run the script from the new copy. To check the hash, point `-Source` at the zip:
   `& "C:\Program Files\PerformanceMonitorDarling-staging\<version>\upgrade-darling.ps1" -Source C:\Users\you\Downloads\PerformanceMonitorDarling-<version>.zip -Sha256 <hash>`
   To skip the hash check, point `-Source` at the extracted folder.
4. The script stops the service and copies the old files to `_rollback_manual_<stamp>`. It lays the new build down, checks that `darling.json` did not change, and starts the service. If it fails, run it again. That is safe.
5. Work the checklist that the script prints about 10 to 15 minutes later. A running service does not prove that it collects.

The script refuses to run from the install folder itself. It never kills a process. If it names a process that blocks the upgrade, close that process and run it again. It keeps the newest three rollback backups. To clear old ones, run `& "C:\Program Files\PerformanceMonitorDarling\upgrade-darling.ps1" -PruneOnly`.

If you installed with the MSI, do not use this script. Run the newer MSI instead. It upgrades in place and keeps `darling.json` and the store.

For more, see [Upgrading an existing install](../../Darling/README.md#upgrading-an-existing-install) and [Rolling back to an older build](../../Darling/README.md#rolling-back-to-an-older-build).

## Uninstall

Run these from an elevated PowerShell, from the folder that holds `uninstall-darling.ps1`. On an MSI install the script refuses. Uninstall from **Settings, Apps** instead.

Before you start, know that the uninstall does not touch the monitored servers. If you want this install's Extended Events sessions gone from them, remove each server first. See [Remove a server](#remove-a-server).

Without `-PurgeData`:

1. Run `.\uninstall-darling.ps1`.
2. The script stops the service and waits up to 90 seconds. Then it deletes the service. It removes the scoped Windows Firewall rules that the installer made and the **Darling Viewer** shortcuts. It also removes any direct grant that the installer gave an account on `darling.json` and the store.
3. It leaves the program folder, `darling.json` and `C:\ProgramData\PerformanceMonitorDarling` in place. That folder holds the store with all collected history, the credential files and the logs.

Running `install-darling.ps1` again restores the install.

With `-PurgeData`:

1. Run `.\uninstall-darling.ps1 -PurgeData`.
2. The script prints what it will delete. Type `DELETE` to confirm. Any other answer keeps the data.
3. It then deletes `C:\ProgramData\PerformanceMonitorDarling`. This removes the store in that folder (all collected history), the generated store credentials and the service logs. You cannot undo this. If `postgres.dataDirectory` in `darling.json` names a folder outside `C:\ProgramData\PerformanceMonitorDarling`, the script leaves that store folder alone. Delete it yourself.

`-PurgeData` does not delete the program folder or `darling.json`. To remove them, delete them yourself. It also does not delete the Viewer's per-user files under `%APPDATA%\PerformanceMonitorDarling`.

For a script run that cannot answer a question, add `-ConfirmPurge` to `-PurgeData`. It deletes without the `DELETE` prompt.

## Where everything is

| What | Where |
|---|---|
| Config file | `darling.json`, beside `PerformanceMonitor.Darling.Service.exe`. The default install folder is `C:\Program Files\PerformanceMonitorDarling` |
| Service logs | `%ProgramData%\PerformanceMonitorDarling\logs\darling-service_<yyyyMMdd>.log`. The service also writes to the Windows Event Log, source `PerformanceMonitor Darling` |
| Store (the bundled PostgreSQL) | `%ProgramData%\PerformanceMonitorDarling\pg`, unless `postgres.dataDirectory` names another folder |
| Store credential files | `pg-credential.dpapi`, `pg-admin-credential.dpapi`, `pg-viewer-credential.dpapi` and `pg-mcp-credential.dpapi`, beside the store folder in `%ProgramData%\PerformanceMonitorDarling` |
| Password key | `password-key.dpapi`. See [Who can read `darling.json` and the credentials](../../Darling/README.md#security--least-privilege-roles) for who can open it |
| Log-hash key | `log-hash-key.dpapi`. A managed store on Windows keeps it beside `pg-credential.dpapi`. An own store (`postgres.managed` off) keeps it in a `darling-keys` folder beside `darling.json`. On Linux the file is `log-hash-key`, and under Docker Compose it is in the `darling-credentials` volume. The service uses the key to hash log text before it stores it. The key is kept outside the store, so someone who can read the store cannot test guesses against the hashes |
| Rollback backups | `_rollback_manual_<stamp>` folders in the install folder |
| Viewer log | `%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_<yyyyMMdd>.log` |
| Viewer preferences | A small JSON file in `%APPDATA%\PerformanceMonitorDarling` |

Back up `darling.json` and the password key file. Nothing else in the install folder has to travel with them. See [Install Darling](install.md) for the install procedure.
