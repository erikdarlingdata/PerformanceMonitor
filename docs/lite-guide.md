# Quick Start — Lite

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

1. Download **[`PerformanceMonitorLite-lite-Setup.exe`](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest)** — a self-contained build, so there is **no .NET runtime to install first**.
2. Run the installer — it installs to `%LocalAppData%\PerformanceMonitorLite`, adds **Start Menu** and **Desktop** shortcuts, and registers the app under **Apps & Features** so it shows up in Windows search and can be uninstalled normally. Auto-update is wired in. Your data goes in `%LocalAppData%\PerformanceMonitorLite-Data`, a separate folder the installer never touches.
3. Launch from the Start Menu or Desktop shortcut.
4. Click **+ Add Server**, enter connection details, test, save.
5. Double-click the server in the sidebar to connect.

On Windows 11 on ARM, the same downloads run under Windows' x64 emulation.

Data starts flowing within 1–5 minutes. That's it. No installation on your server, no Agent jobs, no sysadmin required.

For the first server, check three things in the **Add Server** dialog:

- Authentication: pick the mode that matches the login. [Authentication](authentication.md) describes each mode.
- Certificate: Encryption defaults to Mandatory, and Lite checks the server certificate. If the server uses a self-signed certificate, check **Trust server certificate (skip certificate validation)**, or install a certificate that the Lite machine trusts. An AWS RDS server does not use a self-signed certificate, and it fails the same check, because its certificate chains to an Amazon certificate authority that Windows does not trust. See [AWS RDS for SQL Server: the server certificate](platform-notes.md#aws-rds-for-sql-server-the-server-certificate).
- Grants: the login needs four server grants, not only `VIEW SERVER STATE`. [Permissions](permissions.md#permissions) has the grant block, and the Azure SQL Database form.

**Taking the portable ZIP (`PerformanceMonitorLite-<version>.zip`) instead?** It is a self-contained win-x64 build too, so there is **nothing to install first** — unzip it anywhere and run `PerformanceMonitorLite.exe`. A stock Windows Server with no .NET on it at all is fine. The release also lists `PerformanceMonitorLite-lite-Portable.zip`, which Velopack builds together with `Setup.exe`. It keeps the app in a `current` folder. This guide does not cover it.

`READ-ME-FIRST.txt` still ships in the ZIP beside `PerformanceMonitorLite.exe`, now saying there is nothing to install rather than listing downloads. Monitored SQL Servers need nothing installed on them either way.

Windows itself must have its updates installed. If Lite stops at start with a CET message, see [Lite stops at start with a CET message](troubleshooting.md#lite-stops-at-start-with-a-cet-message).

### Check the download

Each release has a `SHA256SUMS.txt` on the same [release page](https://github.com/erikdarlingdata/PerformanceMonitor/releases/latest). For Lite it lists the portable `PerformanceMonitorLite-<version>.zip`. A release has no line for `Setup.exe` in that file. Download the ZIP and `SHA256SUMS.txt` into one folder, and compare the hash before you extract the ZIP:

```powershell
$zip = 'PerformanceMonitorLite-<version>.zip'    # the file you downloaded
(Get-FileHash -Path $zip -Algorithm SHA256).Hash.ToLower()
Select-String -Path .\SHA256SUMS.txt -SimpleMatch -Pattern $zip
```

The two hex strings must match. If they differ, delete the ZIP and download it again.

### Upgrade Lite

**Running Lite with no window, or from a portable ZIP in a fixed folder?** The steps in this section use the window. Follow [Upgrade a headless Lite](#upgrade-a-headless-lite) instead.

**Upgrading from zip?** Click **Import Settings** then **Import Data** in the sidebar. Settings imports server connections, alert thresholds, SMTP config, and schedules. Data imports historical DuckDB + Parquet archives. **Auto-update users** (installed via Setup.exe) get updates automatically — no manual import needed.

**Import Settings** needs the folder that holds `config\servers.json`. **Import Data** needs the folder that holds `monitor.duckdb`. For a Lite older than 3.4.0, both are the same folder: the one you unzipped, or `%LOCALAPPDATA%\PerformanceMonitorLite`. From 3.4.0 on, `monitor.duckdb` is in `%LOCALAPPDATA%\PerformanceMonitorLite-Data`, so Import Data accepts that folder. The server list is in `%ProgramData%\PerformanceMonitorLite\config`, so Import Settings finds no `servers.json` in that data folder unless an older copy is left there. To move a current install to another PC, copy the folders instead, as in [Back up Lite and move it to another PC](#back-up-lite-and-move-it-to-another-pc).

#### Upgrading an install older than 3.4.0

Do not run `Setup.exe` over a Lite version older than 3.4.0. Before 3.4.0, Lite kept its DuckDB store, Parquet archive, logs, and `settings.json` inside `%LOCALAPPDATA%\PerformanceMonitorLite`. That folder is also the install folder, and `Setup.exe` deletes it before the new build starts. The history is then lost. Lite 3.4.0 and later keep that data in `%LOCALAPPDATA%\PerformanceMonitorLite-Data`, so `Setup.exe` does not touch it.

To find your version, click **About** in the left sidebar. The window shows `Version 3.x.y`.

If your version is older than 3.4.0, use one of these two ways:

1. Click **About**, then install the update from there. This upgrades in place and keeps your data.
2. Close Lite and copy `%LOCALAPPDATA%\PerformanceMonitorLite` to a safe place. Then run `Setup.exe`. `Setup.exe` deletes the original folder, so nothing is restored on its own. After Lite starts, click **Import Settings** and then **Import Data** in the sidebar, and point both at your copy.

After option 1, the first start of 3.4.0 or later moves the old data to the new folder. Your server list and your saved passwords are not in the install folder, so a `Setup.exe` upgrade does not delete them.

You can also extract the portable ZIP over your existing copy to upgrade in place. [Upgrade a headless Lite](#upgrade-a-headless-lite) has those steps, with a copy to go back to.

### Where Lite keeps its files

| What | Where |
|---|---|
| Collected data (`monitor.duckdb`) | `%LOCALAPPDATA%\PerformanceMonitorLite-Data` |
| Parquet archive | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\archive` |
| Logs (`lite_<date>.log`) | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\logs` |
| Settings (`settings.json`, `collection_schedule.json`, `ignored_wait_types.json`) | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config` |
| Server list (`servers.json`) and credential profiles (`profiles.json`) | `%ProgramData%\PerformanceMonitorLite\config`, shared by every Windows user on the machine |
| SQL and Entra passwords, the SMTP password, and webhook URLs | Windows Credential Manager, per Windows user |
| The installed app (`Setup.exe` install) | `%LOCALAPPDATA%\PerformanceMonitorLite` |

Lite keeps archived data for 3 months. This is a fixed value. `settings.json` has no retention key, and the Settings window has no retention control. Lite deletes an archive file when the date in its file name is more than 3 months back. A row can therefore stay a little longer than 3 months.

Lite keeps each log file for 7 days. To open the logs, click **View Log** or **Open Log Folder** in the sidebar.

### Run Lite headless

Lite can collect with no window, no tray icon and no signed-in user. A scheduled task starts it when the machine starts. Use this on a server that nobody signs in to.

Start Lite with `--headless`:

```
PerformanceMonitorLite.exe --headless
```

A headless Lite does these things as usual:

- It collects from every server, and it checks alerts.
- It sends alerts by email, Slack, Teams and webhook.
- It runs retention and archiving.
- It runs the MCP server, if you turned it on. See [Use MCP on a headless Lite](#use-mcp-on-a-headless-lite).
- It checks for updates a few seconds after it starts, and writes one line to the log for the result: no update, an update with its version, or a failed check with the reason. The `check_for_updates_on_startup` setting turns the check off, and Lite then logs one line that says so. It cannot show a balloon.

A headless Lite does not show a window, a tray icon, a balloon or a dialog. Each headless start writes these two lines to the log, at Information level:

- `Headless start: no window is shown and no tray icon is created.`
- `Headless: no tray icon is created.`

Lite writes no line for an alert balloon. With no tray icon, there is no balloon to skip. The other alert channels still send.

Lite also writes a line that starts with `Headless: skipped` when it skips something that it would show on a screen. It writes this line at Information level, and only the first time that it skips each of these in a run:

- The request from a second start to show the window.
- The Entra device-code sign-in prompt. Lite cancels the sign-in.
- The update balloon. Lite also writes a line that says a new version exists.
- The notice that the store was rebuilt. Lite also writes the notice as a Warning.
- The fatal-error dialog and the error dialog. The log holds the same error.
- The dialogs about a `settings.json` file or setting values that Lite could not read. The log holds the same text.
- The dialog about server settings that were not saved.
- The two prompts for an old Lite that runs as an administrator, during an upgrade. See [Upgrade a headless Lite](#upgrade-a-headless-lite).

A normal headless start can therefore write no `Headless: skipped` line. After a headless start, read the log to see what Lite skipped.

Only one Lite runs in a Windows session. If you start Lite a second time in that session, the second start exits. A headless Lite does not show a window when the second start asks it to.

If a headless Lite cannot open its database when it starts, for example because another process holds the file, it does not exit at once. It writes one line to the log for each failed attempt, with the error and the delay before the next attempt. The first delay is 10 seconds. Each delay is twice the one before, up to 5 minutes. Lite keeps trying for 30 minutes. During the wait, `--stop` still ends Lite, and a second start is refused as usual. While Lite is trying to open its database, `--stop` returns 1, so run it again. If the 30 minutes pass, Lite writes a line that says it is giving up after a number of attempts, and it exits with code 3.

Task Scheduler's **If the task fails, restart every** setting did not restart a run that exited with code 3. In a test, the task went to Ready with Last Result 3 and no second run started. So a Lite that gives up stays down until someone starts it again. To see that this happened, look for a **Last Result** of `3` on the task, and for the giving up line in the Lite log.

A start also fails with code 3 when the account is not allowed to create the lock that keeps Lite to one copy and no other Lite is running in the same Windows session. Lite writes the reason to the log.

#### Set up a dedicated Windows user

1. Make a Windows user for Lite. If the user has the "Log on as a batch job" right, a standard user is enough. The setting "Run whether the user is logged on or not" in the scheduled task logs the user on as a batch job. To give the right, open Local Security Policy. Go to Local Policies, then User Rights Assignment, then "Log on as a batch job", and add the user. You can also add the user to a group that has the right. On a stand-alone server, Windows gives the right to Administrators, Backup Operators and Performance Log Users by default. To give the right with no window, add the user to the Performance Log Users group from an elevated PowerShell: `Add-LocalGroupMember -Group 'Performance Log Users' -Member '<user>'`. In a test, that membership gave the right, and registering the scheduled task with a stored password did not. If a Group Policy sets this right, add the user there. Without the right, Windows refuses the logon. Task Scheduler says that the task is registered but may fail to start. It says that the batch logon privilege must be enabled for the task user. The logon then fails. Windows words this error as "Logon failure: the user has not been granted the requested logon type at this computer."
2. Install Lite for that user. Run `Setup.exe` as that user, which installs to the `%LOCALAPPDATA%\PerformanceMonitorLite` folder of the user. Or unzip the portable ZIP into any folder, for example `C:\PerformanceMonitorLite`, and give the user Read and execute on it. Lite does not write to its install folder while it runs. It reads the `config` folder beside the exe to seed the settings of the user. The user needs write access to the install folder only when that user unzips a newer ZIP over it, as in [Upgrade a headless Lite](#upgrade-a-headless-lite). Lite keeps its data, its logs and its settings in the `%LOCALAPPDATA%` of the user. A user who never signed in has no profile folder yet. Windows makes it the first time the scheduled task logs the user on. See [Where Lite keeps its files](#where-lite-keeps-its-files).
3. Give the user the servers and their passwords. To add them with no window, use `--add-server`: see [Add servers with `--add-server`](#add-servers-with---add-server), and [Add servers on a machine with no desktop sign-in](#add-servers-on-a-machine-with-no-desktop-sign-in) when nobody can sign in or use `runas`. To use the Lite window instead, sign in once as that user, and start Lite the normal way. The server list is shared by all users, but passwords are not. Lite keeps SQL and Entra passwords, the SMTP password and webhook URLs in the Windows Credential Manager of each user. This user has none yet. Open each server in **Edit Server**, enter the password again, and save. Settings are also kept for each user. Set up email, Slack, Teams and webhook alerts in **Settings** as this user. If nobody can sign in as this user, start Lite as this user from your own session instead. Run `runas /user:<name> "<path>\PerformanceMonitorLite.exe"`, and enter the password of the user when `runas` asks. Do the same steps in the Lite window that opens. Lite stores the passwords and the settings for the user that you named. Run this step in a session on the machine itself, signed in as that user or in a `runas /user:<user> cmd` window. Do not run it from a remote PowerShell session. A scheduled task can fail to read a password that was saved that way. When Lite cannot find a saved password for a server, it writes this warning to its log: "No saved password was found for server '<name>' under this Windows logon."
4. Close Lite. Sign out.

Do not use **Entra ID (MFA)** or **Device Code** sign-in for a headless Lite. Both need a person to answer a prompt each session. A headless Lite cancels a device-code sign-in and writes the reason to the log. Use a login that needs no prompt: Windows, SQL Server, Service Principal, Managed Identity or Existing Azure Sign-In. See [Authentication](authentication.md).

#### Start it with a scheduled task

Make a task in Task Scheduler with these settings:

| Setting | Value |
|---|---|
| Name | Any name. The examples in this guide use `Lite start` for this task and `Lite stop` for the stop task. |
| Run as | The dedicated user |
| Run whether the user is logged on or not | On |
| Run with highest privileges | Off. Lite does not need it, and the task ran with it off in a test. |
| Do not store password | Off. With it on, the task can read Lite's saved passwords only while the dedicated user is signed in. An "At startup" task runs before anyone signs in. |
| Trigger | At startup |
| Action | `PerformanceMonitorLite.exe` with the argument `--headless` |
| If the task fails, restart every | 1 minute, up to 3 times. This does not restart a Lite that gave up and exited with code 3. |
| Long-running limit | Turn off "Stop the task if it runs longer than" |

To make the task in PowerShell, run this in an elevated PowerShell. Do not use `schtasks /create /rp`: it puts the password on the command line. `Get-Credential` asks for the password at a prompt, so it is typed into no command line and kept in no file. A machine with no prompt can set `$user` and `$password` from a secret store instead.

```powershell
$cred = Get-Credential '<machine>\<dedicated user>'
$user = $cred.UserName
$password = $cred.GetNetworkCredential().Password

$settings = New-ScheduledTaskSettingsSet -RestartCount 3 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)
$start = New-ScheduledTaskAction -Execute '<path>\PerformanceMonitorLite.exe' -Argument '--headless'
Register-ScheduledTask -TaskName 'Lite start' -Action $start -Trigger (New-ScheduledTaskTrigger -AtStartup) -Settings $settings -User $user -Password $password -RunLevel Limited
```

With `-User` and `-Password`, the task runs whether the user is logged on or not, and Task Scheduler stores the password. `-RunLevel Limited` leaves "Run with highest privileges" off. A zero `-ExecutionTimeLimit` turns off the long-running limit.

Task Scheduler runs this task in session 0. Session 0 has no taskbar. Without `--headless`, the first tray balloon that fails for that reason writes one Warning to the log. Lite then skips tray balloons for the rest of the run. It does not write an error for each alert, and the other alert channels still send.

#### Stop it

Do not use **End** in Task Scheduler, and do not use `Stop-Process`. Both end Lite at once. Lite then runs no shutdown code. It does not close the database cleanly, and it does not release the MCP port.

Use `--stop` instead:

```
PerformanceMonitorLite.exe --stop
```

`--stop` asks the running Lite to shut down the same way that it does when you close the window. Lite stops collection, stops the MCP server and closes the database. `--stop` then waits up to 60 seconds for Lite to exit. It does not start Lite or open a window. Its exit code tells you what happened:

| Exit code | Meaning |
|---|---|
| 0 | Lite exited |
| 1 | No Lite was listening. Lite is not running, or it is still starting. A Lite that is still starting does not listen yet. Try again in a minute. |
| 2 | Lite had not exited after 60 seconds |

The request goes to the Lite in the same Windows session as `--stop`, and `--stop` waits only for a Lite in that session. Run `--stop` in session 0, as the dedicated user or as the SYSTEM account. In a test, `--stop` run as SYSTEM stopped the dedicated user's Lite and exited with code 0. A `--stop` that cannot reach a Lite, for example because it runs in another session, exits with code 1, as it does when no Lite runs. Make a second scheduled task for it. Give it the dedicated user, the same "Run whether the user is logged on or not" setting, and the action `PerformanceMonitorLite.exe` with the argument `--stop`. In PowerShell, with `$user` and `$password` set as in [Start it with a scheduled task](#start-it-with-a-scheduled-task):

```powershell
$stop = New-ScheduledTaskAction -Execute '<path>\PerformanceMonitorLite.exe' -Argument '--stop'
Register-ScheduledTask -TaskName 'Lite stop' -Action $stop -User $user -Password $password -RunLevel Limited
```

Run that task to stop Lite. For example:

```
schtasks /run /tn "Lite stop"
```

`schtasks /run` starts the task and returns at once. It does not wait for `--stop` to finish, and it does not show the exit code. Task Scheduler keeps the exit code as the last result of the task. To wait for `--stop` and read the exit code:

1. Run the task.
2. Run `schtasks /query /tn "Lite stop" /v /fo list` again and again until `Status` is not `Running`.
3. Read `Last Result` in the same output. Check that `Last Run Time` is later than the time that you ran the task. Then you do not read the result of an earlier run.

In PowerShell, the same steps are:

```
Start-ScheduledTask -TaskName 'Lite stop'
while ((Get-ScheduledTask -TaskName 'Lite stop').State -eq 'Running') { Start-Sleep -Seconds 2 }
Get-ScheduledTaskInfo -TaskName 'Lite stop' | Select-Object LastRunTime, LastTaskResult
```

A `Last Result` of 0 means that Lite exited. The other values are the same as in the table.

`--stop` writes what it did to the Lite log of the account that runs it, and only when that account already has a Lite logs folder. Each line has `[Stop]` in it. The lines say whether `--stop` asked a running Lite to stop, found none, or timed out. Run as the dedicated user, `--stop` writes them to the same logs folder as the Lite that it stops. Run as another account, such as SYSTEM, it writes no `[Stop]` line to the log of the dedicated user, and it writes none at all when that account has no Lite logs folder. The exit code is then the record of the stop. The headless Lite that receives the request writes `Headless instance asked to shut down.` to its own log, whichever account asked. That line does not name the account, because the request is a named event that carries none.

A hard end does not break the store. DuckDB replays its write-ahead log at the next start. The next start can take a little longer than usual.

#### Upgrade a headless Lite

The **About** button needs a window, so a headless Lite does not update itself. It logs when a new version exists. These steps are for a portable ZIP in a fixed folder that a scheduled task starts. Nothing of yours is in that folder. The server list is in `%ProgramData%\PerformanceMonitorLite\config`. The settings, the store, the archive and the logs are in `%LOCALAPPDATA%\PerformanceMonitorLite-Data` of the dedicated user. A new ZIP touches none of them. The `config` folder beside the exe holds the default `collection_schedule.json` and `ignored_wait_types.json`, which Lite copies into the data folder only when the file is missing there. To upgrade:

1. Run the stop task. Wait until it is no longer running, and check that its last result is 0. See [Stop it](#stop-it).
2. Copy the program folder to a rollback folder. Delete an older `<folder>.previous` first, or `Copy-Item` puts the new copy inside it:

   ```powershell
   Copy-Item -Path '<folder>' -Destination '<folder>.previous' -Recurse
   ```

3. Unzip the new portable ZIP over the folder, as an account that can write to it. Without `-Force`, `Expand-Archive` stops at the first file that already exists:

   ```powershell
   Expand-Archive -Path '<zip>' -DestinationPath '<folder>' -Force
   ```

4. Run the scheduled task that starts Lite with `--headless`.
5. Check which build runs. The start line in the log, `Starting PerformanceMonitorLite v<version>`, holds the whole version of the build. A build from before that change wrote only the numeric version, such as `v3.14.0.0`, so two nightlies of one version read alike. The product version of the exe holds the whole version too:

   ```powershell
   (Get-Item '<folder>\PerformanceMonitorLite.exe').VersionInfo.ProductVersion
   ```

   A release reads `3.14.0`. A nightly reads `3.14.0-nightly.<date>.<run number>`. The command reads the file on disk, so it names the running build only after step 4. Lite also writes one row for every start to `build-history.json` in the data folder, newest first. The `Build` field of the first row is the build of the latest start.

To upgrade an install that `Setup.exe` made, run the new `Setup.exe` as the dedicated user in place of steps 2 and 3. The rollback below is for the portable ZIP.

`-Force` replaces the files that the ZIP holds and leaves every other file where it is, so a file that only an older build had stays. Lite's own code loads nothing from a plugins or lib folder. To keep only the new build's files in the folder, run `Remove-Item -Path '<folder>\*' -Recurse -Force` after step 2 and before step 3, and keep the folder itself so that the dedicated user keeps its access to it. The copy from step 2 holds the old files, including a default file that you changed in `config`.

To roll back the program, run the stop task and check its last result, as in step 1. Then put the files from the copy back, and run the scheduled task that starts Lite:

```powershell
Remove-Item -Path '<folder>\*' -Recurse -Force
Copy-Item -Path '<folder>.previous\*' -Destination '<folder>' -Recurse
```

The data folder is not in the program folder, so the rollback leaves it as it is. A new build can raise the schema version of `monitor.duckdb` at its first start. A build that is older than the schema version of the store does not open it. It refuses, and it leaves the file unchanged. A row in `build-history.json` with a `SchemaAfter` above its `SchemaBefore` is a start that raised it. To keep a way back past such a start, stop Lite and copy `%LOCALAPPDATA%\PerformanceMonitorLite-Data` of the dedicated user before the first start of the new build, as in [Back up Lite and move it to another PC](#back-up-lite-and-move-it-to-another-pc). To roll back past it, put that copy back with the old program files. What Lite collected after the copy is not in it.

Suppose you start the new version with `--headless` while the old version still runs in the same session. The new version asks the old one to shut down, and then it takes over. A headless start never shows the "close it and continue?" dialog. If the old version runs as an administrator and the new one does not, the new one cannot close it. It writes the reason to the log and exits. Stop the old one first.

#### Where the logs are

A headless Lite writes the same files as a Lite with a window. The logs are in `%LOCALAPPDATA%\PerformanceMonitorLite-Data\logs` of the dedicated user. The files are `lite_<date>.log`. Lite keeps each file for 7 days. The data folder is `%LOCALAPPDATA%\PerformanceMonitorLite-Data` of the same user.

#### Use MCP on a headless Lite

A headless Lite has no Settings window. You turn MCP on in `settings.json`, and you read the token with a command.

For a full setup of a headless Lite that an AI client on another machine uses, see [Set up a headless Lite for an AI client on another machine](#set-up-a-headless-lite-for-an-ai-client-on-another-machine).

To turn MCP on, add these keys to the `settings.json` of the dedicated user. The file is in `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config`. Restart Lite after you change it.

| Key | Value | What it does |
|---|---|---|
| `mcp_enabled` | `true` | Turns the MCP server on. The default is `false`. |
| `mcp_port` | A port number | The port that the server listens on, on localhost. The default is `5151`. |
| `mcp_allow_tokenless_loopback` | `true` | Optional. The server then accepts requests with no token. Leave it out to keep the token. |

With `mcp_allow_tokenless_loopback` set to `true`, Lite does not read its stored token. Any program on the machine that can reach localhost can then call the read tools, whichever Windows user runs it. Lite does not serve the write tools or the investigation tools in this mode, because they need a token. We recommend that you leave the setting off.

With the setting off, Lite makes a token at its first start with MCP on. Lite keeps the token in the Windows Credential Manager of the user that runs Lite. Each request must carry it in an `Authorization: Bearer <token>` header. The dedicated user never signs in, so you read the token with `--print-mcp-token`. Run the command as the dedicated user, after Lite starts once as that user with MCP on:

```
PerformanceMonitorLite.exe --print-mcp-token
```

The command prints the token and a new line to standard output, and then it exits. It only reads. It never makes or changes a token, it does not start Lite, and it does not disturb a Lite that runs. It writes the token nowhere except standard output. The token is a live secret, so do not leave it in a shared window or in a log.

Lite has no console of its own. A command prompt or PowerShell can give the prompt back before Lite writes. To read the token as the dedicated user:

1. Open a command prompt as that user: `runas /user:<name> cmd`
2. In the new window, run `cmd /c "<path>\PerformanceMonitorLite.exe" --print-mcp-token`. The `cmd /c` makes the window wait for Lite. To keep the token in a file, add `> "%USERPROFILE%\mcp-token.txt"` at the end of the line. A file in the profile folder of the user can be read only by that user and by administrators. Do not write the token to a folder that other users can read, for example the folder that the window opens in. Delete the file after you copy the token.

In PowerShell, pipe the output to get it: `& "<path>\PerformanceMonitorLite.exe" --print-mcp-token | Out-String`.

If a policy turns on PowerShell module logging or transcription, PowerShell records the token in the event log or in the transcript folder. In that case, use the command prompt steps above.

The exit code says what happened:

| Code | Meaning | Standard output | Standard error |
|---|---|---|---|
| 0 | Lite printed the token. | The token | Nothing |
| 1 | No token is stored for this user. Lite made none. | Nothing | One line |
| 2 | The read of the Credential Manager entry failed, or the entry is empty. A token might exist. | Nothing | The reason |
| 3 | The command has nowhere to write: no console and no redirect. Lite did not read the token. | Nothing | Nothing |

For code 1, turn MCP on and start Lite as that user once. Lite then makes the token.

#### Add servers with `--add-server`

Add a server with `--add-server`. The Add Server window is the only other way to add one, and a dedicated user never signs in to open it. Run `--add-server` as the Windows user that runs Lite, because Lite keeps the password in the Credential Manager of that user. Run it in a session on the machine itself, signed in as that user or in a `runas /user:<user> cmd` window, or from a scheduled task that runs as that user: see [Add servers on a machine with no desktop sign-in](#add-servers-on-a-machine-with-no-desktop-sign-in). Do not run it from a remote PowerShell session. A scheduled task can fail to read a password that was saved that way. When Lite cannot find a saved password for a server, it writes this warning to its log: "No saved password was found for server '<name>' under this Windows logon." The command pipes a JSON array of servers to Lite on standard input:

```
type servers-to-add.json | PerformanceMonitorLite.exe --add-server
```

To keep the servers and their passwords off the disk, pipe the JSON from a secret store instead of a file. [Add servers on a machine with no desktop sign-in](#add-servers-on-a-machine-with-no-desktop-sign-in) shows how.

Each entry has `host` and can have `display_name`, `auth`, `username`, `password` and `database`. The value of `auth` is `windows`, `sql`, `serviceprincipal`, `managedidentity` or `entradefault`. For one server, you can give the fields as flags instead: `--host`, `--name`, `--auth`, `--username` and `--database`.

An entry can also set four connection settings, with the names and values that Darling's `add_servers` uses:

| Field | Flag | Value | If left out |
|---|---|---|---|
| `encrypt_mode` | `--encrypt-mode` | `Optional`, `Mandatory` or `Strict` | `Mandatory` |
| `trust_server_certificate` | `--trust-server-certificate` | `true` or `false` | `false` |
| `read_only_intent` | `--read-only-intent` | `true` or `false` | `false` |
| `multi_subnet_failover` | `--multi-subnet-failover` | `true` or `false` | `false` |

In JSON, the three switches take a JSON `true` or `false`, not text. As flags, a switch alone means `true`, and it also takes `true` or `false` as its value. An unknown `encrypt_mode` word, or a value that is not `true` or `false`, exits with code 1 and changes nothing.

A key in an entry that the command does not read is dropped, and the entry is still added. Right after the entry's line, the command prints `Not read, so not applied:` and each key name. When a key looks like one the command reads, the line says which: `name (did you mean display_name?)`, or `trustServerCertifcate (did you mean trust_server_certificate?)`. Key names match in any letter case, so `Host` is read. A key that starts with `// ` is a note and is never listed. Only key names are printed, never a value, so a password in the entry stays out of the output. The Darling-only keys `port`, `aws_role_arn`, `aws_external_id`, `query_store_replica`, `capture_plans` and `alert_delivery_mode_override` are not read here, so they are listed too. The `add_servers` MCP tool answers the same way, with an `ignored_keys` list of `{ "key", "did_you_mean" }` on the entry's row in `results`. An entry that is refused names its unread keys in the refusal text, because one of them can be the reason. An entry with no such key has no `ignored_keys`, and the answer's counters and status words are unchanged.

`trust_server_certificate` turns off the check of the server certificate. Use it only for a server that serves a self-signed certificate, such as a new install or a container, and only on a network that you trust. Without it, the connection test refuses such a server with `The certificate chain was issued by an authority that is not trusted.` An AWS RDS server fails the same test with the same message, because its certificate chains to an Amazon certificate authority that this machine does not trust. For RDS you can import that authority into the trusted roots of the machine instead. See [AWS RDS for SQL Server: the server certificate](platform-notes.md#aws-rds-for-sql-server-the-server-certificate). `encrypt_mode` `Strict` ignores `trust_server_certificate`: it needs a server that supports TDS 8.0 and a certificate that this machine trusts. A connection test of a `Strict` entry that fails in the transport (a reset connection or a certificate this machine does not trust) says so after the driver's text. An error the server sent, such as a login failure, and a failure before any connection is made, such as a secret that cannot be read, get no such line.

A setting that you give is recorded as confirmed for the Windows account that runs the command, as the Add Server window does. The next start of Lite does not ask about it. For a server that is already in the list, a setting that you leave out keeps its listed value. A listed `trust_server_certificate` or weaker `encrypt_mode` that this account never confirmed still refuses the update, unless you give that setting. The read-only intent is part of the identity of a server, so a run that leaves it out names the server without it.

A SQL login then needs its password as one line on standard input. There is no password flag, because a flag shows in the process list. There is no password prompt either, because a command prompt does not wait for Lite. Always pipe the input, so that the shell waits for Lite and you can read the exit code.

The command runs the same checks and the same connection test as the Add Server window. It writes `servers.json`, saves the password for the user that runs it, and records the host confirmation for that user. If a server is already in the list, the command replaces its saved password and adds no second entry.

If any entry fails, the command still tests every entry, lists each failure on standard error with its entry number and host, and exits with code 1. It adds and updates nothing, including the entries that passed, and its last line says so. Fix the failed entries and run the command again with all of them. When it succeeds, it exits with code 0 and prints the Windows account and the folders it wrote to.

Lite keeps the server list in memory and writes it back. If a Lite is running, `--add-server` exits with code 1 and changes nothing. Run `--stop`, run `--add-server`, and start Lite again. Lite collects from the new server from that start. A Microsoft Entra MFA or device code server needs a person to sign in, so add it in the window.

#### Add servers on a machine with no desktop sign-in

Use this when nobody can sign in to the Lite machine and `runas` is not an option, for example a machine that you reach only through a remote command channel. A remote PowerShell session is not enough, for the reason above. Run `--add-server` from a scheduled task that runs as the dedicated user, the way [Stop it](#stop-it) runs `--stop`. Windows logs the user on as a batch job for the task, so the user needs the batch logon right: see step 1 of [Set up a dedicated Windows user](#set-up-a-dedicated-windows-user). Lite saves the password in the Credential Manager of that user, and the scheduled task that starts Lite reads it from there. In a test, a Lite that started this way collected from every server that it was given.

Two things about the task:

- The task action is `cmd.exe`, not Lite. `--add-server` needs a pipe for its input and a place for its output. A task that runs `PerformanceMonitorLite.exe --add-server` directly has neither, so it exits with code 1 and writes nothing.
- The JSON comes from a secret store through a pipe, so no file of servers is ever on the disk. The example uses `Get-Secret` from the `Microsoft.PowerShell.SecretManagement` module. Replace it with the command that your secret store provides. It must print the JSON array of servers, as in the example above, and the dedicated user must be allowed to read that secret. The task writes the result of the run to a file in the profile folder of that user. The result lists the servers that were added and the account that the credentials were stored for. It never holds a password.

Do these steps in an elevated PowerShell on the Lite machine. `$user` and `$password` are the dedicated user and its password, set as in [Start it with a scheduled task](#start-it-with-a-scheduled-task).

1. Stop Lite if it runs, in every session and for every user. `--add-server` exits with code 1 and changes nothing while any Lite runs. See [Stop it](#stop-it).
2. Make the task, run it, and wait for it to end:

   ```powershell
   $arguments = @'
   /c powershell.exe -NoProfile -Command "Get-Secret -Name <secret name> -AsPlainText" | "<path>\PerformanceMonitorLite.exe" --add-server > "%USERPROFILE%\add-server-result.txt" 2>&1
   '@
   $action = New-ScheduledTaskAction -Execute 'cmd.exe' -Argument $arguments
   Register-ScheduledTask -TaskName 'Lite add servers' -Action $action -User $user -Password $password -RunLevel Limited
   Start-ScheduledTask -TaskName 'Lite add servers'
   while ((Get-ScheduledTask -TaskName 'Lite add servers').State -eq 'Running') { Start-Sleep -Seconds 2 }
   Get-ScheduledTaskInfo -TaskName 'Lite add servers' | Select-Object LastRunTime, LastTaskResult
   ```

3. Read the result. A `LastTaskResult` of 0 means that every entry was added. Read the file in the profile folder of the dedicated user, for example `Get-Content 'C:\Users\<user>\add-server-result.txt'`. When the run succeeds, it ends with the line `Credentials were stored for the Windows account <account>.` Check that the account is the dedicated user. When the run is refused, the file holds the reason.
4. Delete the result file and the task:

   ```powershell
   Remove-Item 'C:\Users\<user>\add-server-result.txt'
   Unregister-ScheduledTask -TaskName 'Lite add servers' -Confirm:$false
   ```

5. Run the scheduled task that starts Lite. Lite collects from the added servers from that start.

#### Set up a headless Lite for an AI client on another machine

This procedure sets up a headless Lite that an AI client on another machine reaches over MCP. It uses the parts above, in this order. Do the steps on the Lite machine, except step 7.

1. Download Lite. Use the install text at the top of this guide, [Quick Start](#quick-start--lite). Step 2 installs it for the dedicated user.
2. Make the dedicated user, install Lite as that user, and make the scheduled task. Follow steps 1 and 2 of [Set up a dedicated Windows user](#set-up-a-dedicated-windows-user) and then [Start it with a scheduled task](#start-it-with-a-scheduled-task). In step 3 of the user setup, `--add-server` replaces only adding the servers and entering their passwords, as step 4 of this procedure does. It does not set up email, Slack, Teams or webhook alerts, or their passwords and URLs. If you want alerts, do that part of step 3 as that user in **Settings**, or with `runas`. Then do step 4 of the user setup. If you do not want alerts, skip steps 3 and 4 of the user setup. Do not start the task yet. If the task already ran, stop Lite first. See [Stop it](#stop-it).
3. Turn MCP on for the dedicated user. Open a `cmd` window as that user: `runas /user:<user> cmd`. In that window, run these commands:
   ```
   mkdir "%LOCALAPPDATA%\PerformanceMonitorLite-Data\config"
   notepad "%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\settings.json"
   ```
   If `settings.json` is new, give it this content and save it:
   ```
   { "mcp_enabled": true }
   ```
   If the file exists, add the `mcp_enabled` key to it. The MCP port is `5151`. To use another port, add `mcp_port` with a number from 1024 to 65535. Leave `mcp_allow_tokenless_loopback` out. See [Use MCP on a headless Lite](#use-mcp-on-a-headless-lite) for the keys. Ignore the error that `mkdir` prints when the folder exists.
4. Add the servers with `--add-server`, in the same window, as the same user. With no desktop and no `runas`, use [Add servers on a machine with no desktop sign-in](#add-servers-on-a-machine-with-no-desktop-sign-in) for this step, and read the servers from a secret store, not from a file. Lite keeps the password in the Credential Manager of the user that runs the command. The dedicated user must run it, in a session on the machine itself, signed in as that user or in the `runas /user:<user> cmd` window. Do not run it from a remote PowerShell session. A scheduled task can fail to read a password that was saved that way. When Lite cannot find a saved password for a server, it writes this warning to its log: "No saved password was found for server '<name>' under this Windows logon." The command has no password prompt and no password flag. It reads the servers from standard input.
   1. Make a file of servers in the profile folder of that user. Only that user and administrators can read that folder. Run `notepad "%USERPROFILE%\servers-to-add.json"` and enter one object for each server. In a password, write `"` as `\"` and `\` as `\\`:
      ```
      [
        { "host": "<sql-host>", "display_name": "<name>", "auth": "sql", "username": "<sql-login>", "password": "<password>" }
      ]
      ```
      For a server that uses Windows authentication, leave out `auth`, `username` and `password`. The connection then uses the dedicated user. For all the fields, see [Add servers with `--add-server`](#add-servers-with---add-server).
   2. Make sure that no Lite runs, in any session and for any user. That includes a Lite window of your own. If a Lite runs, the command exits with code 1 and changes nothing.
   3. Run the command. The pipe makes the window wait for Lite, so the output shows in the window:
      ```
      type "%USERPROFILE%\servers-to-add.json" | "<path>\PerformanceMonitorLite.exe" --add-server
      ```
      For one server, you can give flags instead of the JSON file: `--host`, `--name`, `--auth`, `--username`, `--database` and the four settings above. Then put only the password, on one line, in a file in the same profile folder, and pipe that file: `type "%USERPROFILE%\password.txt" | "<path>\PerformanceMonitorLite.exe" --add-server --host <sql-host> --auth sql --username <sql-login>`. Do not type the password into the command with `echo`. In a `cmd` window, each side of a pipe runs as a command of its own. So the password shows in the process list and in the command history of the window. Delete the file when the command is done.
   4. Read the output. It ends with the line `Credentials were stored for the Windows account <account>.` Check that the account is the dedicated user. If it is another account, the password is stored for the wrong user. Run the command again as the dedicated user. If the window shows no output, add `> "%USERPROFILE%\add-server-result.txt" 2>&1` at the end of the command line and read that file. Lite never writes the password to the output.
   5. Delete the file of servers: `del "%USERPROFILE%\servers-to-add.json"`.
5. Start the scheduled task, for example with `schtasks /run /tn "<task name>"`. Lite collects from the added servers from this start, and it makes the MCP token. See [Start it with a scheduled task](#start-it-with-a-scheduled-task).
6. Read the token with `--print-mcp-token`, in a `cmd` window that you open as the dedicated user. Follow [Use MCP on a headless Lite](#use-mcp-on-a-headless-lite). Treat the token like a password. Do not leave it in a shared window, a chat or a log.
7. On the client machine, forward a local port to Lite with SSH. Use an account that can sign in to the Lite machine over SSH. The Lite machine must run an SSH server, and this guide does not cover how to set one up.
   ```
   ssh -L <local port>:127.0.0.1:<Lite MCP port> <user>@<host>
   ```
   Keep that SSH session open while the client works. Then point the client at the local port, and put the token in the client config. For Claude Code, the command is:
   ```
   claude mcp add --transport http --scope user sql-monitor http://localhost:<local port>/ --header "Authorization: Bearer <token>"
   ```
   The command puts the token on the command line. It stays in your shell history, and it shows in the process list while the command runs. PowerShell keeps its history in a file, at the path `(Get-PSReadLineOption).HistorySavePath`, so delete the line there if you ran it. To keep the token off the command line, use a `.mcp.json` file. Claude Code's documentation says a `headers` value there can hold `${NAME}`, which it replaces with the environment variable `NAME`, for example `"Authorization": "Bearer ${SQL_MONITOR_TOKEN}"`. Set that variable in the Windows environment variable settings, not with a typed command.
   The client needs the header `Authorization: Bearer <token>` on every request. [Lite with Copilot](mcp-server.md#lite-with-copilot) shows the VS Code entry. In it, use the URL `http://localhost:<local port>/`.
8. Keep the token, even with the SSH forward. The forward protects only the network hop between the two machines. A loopback port on the Lite machine is open to every program and every account on that machine. The token is the only control on that port, so leave `mcp_allow_tokenless_loopback` off.

### Lite Data Storage

All data is stored in `%LOCALAPPDATA%\PerformanceMonitorLite-Data\` — a different folder from the install directory (`%LOCALAPPDATA%\PerformanceMonitorLite\`), so neither an in-app update nor re-running `Setup.exe` can disturb it. Data from an older install is moved into the new folder automatically the first time this version starts.

- **Hot data** in DuckDB 1.5.6: non-blocking checkpoints and free block reuse. The hot store holds the data since the last reset. On a test install with six servers and the default collectors at 512 MB, that was about the last 2 hours. Each hour, Lite moves rows older than 7 days to Parquet, so a reset usually moves the data first. The hot store has a reset point. It is 512 MB unless `settings.json` sets `hot_store_reset_mb` (see [Hot store reset point](#hot-store-reset-point)). Lite resets the store when the data in it reaches the reset point. It also resets the store when the file reaches twice the reset point. A reset writes the collected data to Parquet and deletes the file. It then creates a new small file and puts back the settings, tags, mute rules and investigation records. Collection pauses for a short time while the reset runs. After the reset, Lite compacts the Parquet files into monthly archives, and collection continues during compaction.
- **Archive** to Parquet with ZSTD compression (~10x reduction) — automatic monthly compaction keeps file count low (~75 files vs thousands)
- **Retention**: 3-month calendar-month rolling window. Retention is fixed at 3 months and cannot be changed.
- **Memory limit**: DuckDB's memory limit is a setting (Settings, Dashboard Defaults, "DuckDB memory limit"). The default is 2 GB, and it can go from 1 GB up to 80% of memory. A change applies after a restart.
- Typical size: it depends on the number of servers, the number of collectors, and whether Query Store collection is on. The data in the hot store does not grow past about the reset point, which is 512 MB by default. Lite resets the store at that size. The hot file can be larger than its data, because DuckDB keeps freed blocks in the file. Lite resets the store when the file reaches twice the reset point, so the file does not grow past about 1 GB at the default. The Parquet archive grows with the 3 months of history that Lite keeps.

Compaction merges the per-run Parquet files into one file for each month. `--stop` does not wait for it to finish. Lite checks for the stop before it starts each group of files, and it waits up to 5 seconds for a merge that is already running. If that merge is still running at the end of the 5 seconds, Lite exits and the merge ends with it. In both cases a group of files is either fully merged or left as it was, so a read returns each row once. A merge writes to a `.tmp` file first, and a read never opens that file. The first archive pass after the next start runs about an hour after the start. It deletes a leftover `.tmp` file and runs the compaction again. Lite logs one line when compaction ends: complete, cancelled by shutdown, or failed.

For the full list of folders, see [Where Lite keeps its files](#where-lite-keeps-its-files). To remove a server or uninstall, see [Remove a server, uninstall, and delete the data](#remove-a-server-uninstall-and-delete-the-data). To back up Lite or move it to another PC, see [Back up Lite and move it to another PC](#back-up-lite-and-move-it-to-another-pc).

### Hot store reset point

`settings.json` has one key for the amount of data at which Lite archives all hot data and starts a fresh store:

| Key | Default | Notes |
|---|---|---|
| `hot_store_reset_mb` | `512` | The amount of data in the hot store, in MB, at which Lite resets it. The range is 128 to 2048. |

Lite counts the data inside `monitor.duckdb` (the used blocks, which **Store Health** also shows), not the length of the file. DuckDB keeps freed blocks in the file, so the file is longer than its data. On a test install the data was about 60% of the file length. If Lite cannot read the data size because the store is busy, it does not reset on the file length. It reads the data size again at the next check. Lite resets the store when the file reaches twice the reset point, whatever the data size, and that check does not need the data size.

Lite reads the key once at start, so a change applies after a restart. A value outside the range is changed to the nearest limit, and Lite writes one warning to the log. A value that is not a number is ignored and the default applies, with one warning in the log. **Store Health** shows the reset point that is in force and says whether it is the default or comes from the setting.

Raise the key when an install with many servers resets often. For example, a test install with six servers and the default collectors reset about every 2 hours. Each reset paused collection for 9 seconds on average, and the longest pause was 30 seconds. Parquet compaction then ran for 14 to 53 seconds.

A larger hot file has these costs:

- Disk: the hot file can grow to twice the reset point, so it uses up to that much disk space beside the Parquet archive. At the largest setting, 2048 MB, that is up to 4 GB.
- Insert speed: an earlier test found that inserts slowed down on a large hot file. A later test of 600-row batches into one table measured no change in insert time up to 2.3 GB. We recommend raising the key in steps, and checking the collection durations in the collection log after each step.
- Checkpoints: a checkpoint takes longer on a larger file. In the test it took about 150 ms at 560 MB and about 350 ms at 1.2 GB.
- Start time: Lite rebuilds the indexes of the hot store at every start, so a start takes longer with a larger file. The rebuild took about 0.6 seconds on a 402 MB file.
- Reset time: a reset exports more data, so each pause is longer. Resets are less frequent.

### Back up Lite and move it to another PC

To back up Lite, close Lite and copy two folders:

- `%LOCALAPPDATA%\PerformanceMonitorLite-Data`
- `%ProgramData%\PerformanceMonitorLite\config`

To move Lite to another PC:

1. Install Lite on the new PC, start it once, and close it.
2. Copy the two folders from the old PC over the same paths on the new PC.
3. Start Lite. It gives the copied data folder a new install id, because the old id belongs to the old machine.
4. Enter the SQL and Entra passwords again for each server. Windows Credential Manager passwords do not move with the folders. Servers that use Windows Authentication need nothing. If you use email alerts or webhooks, enter the SMTP password and webhook URLs again.

Do not use **Import Settings** for this move. It reads `config\servers.json` under the folder that you select. The data folder has no current server list, because the list is in `%ProgramData%`.

### Remove a server, uninstall, and delete the data

Do these steps in this order. Lite can only remove its Extended Events sessions from a server while Lite is still installed.

1. Right-click each server in the sidebar and select **Remove**. Lite then drops this install's Extended Events sessions on that server and deletes the stored password for that server. The two shared sessions, `PerformanceMonitor_Deadlock` and `PerformanceMonitor_BlockedProcess`, stay on the server. See [Removing a server](lite-extended-events.md#removing-a-server). To change a server instead, select **Edit** in the same menu.
2. Close Lite. If a minimized copy is in the system tray, open it and close it too.
3. Uninstall Lite from Apps & Features. For the portable ZIP, delete the folder that you unzipped.
4. To delete the history, delete `%LOCALAPPDATA%\PerformanceMonitorLite-Data`.
5. To delete the server list, delete `%ProgramData%\PerformanceMonitorLite\config`.
6. To delete the SMTP password and webhook URLs, open Windows Credential Manager. Delete the entries whose names start with `PerformanceMonitorLite_`.

Uninstalling does not delete the data folder or the server list. If you uninstall before you remove the servers, the Extended Events sessions stay on those servers. Lite has no command that drops them later, so see [Cleaning up by hand](lite-extended-events.md#cleaning-up-by-hand).

### Lite Configuration

| File | Location | Purpose |
|---|---|---|
| `servers.json` | `%ProgramData%\PerformanceMonitorLite\config\` (machine-wide) | Server connections, shared across all Windows users on the machine. Passwords stay per-user in Windows Credential Manager. Optional **Utility Database** per server for community procs installed outside master. |
| `settings.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | MCP server, startup behavior, alert thresholds, SMTP configuration, the [Extended Events ring buffer limit](#extended-events-ring-buffer-limit) |
| `collection_schedule.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | Per-collector enable/disable and frequency. A collector that runs once a day or less often can also have a `run_at` time, so it runs in a quiet hour. That time is 24-hour `HH:MM` on the monitored server's clock, and you set it in the schedule editor. Each server starts at that time plus its own spread of under 60 minutes, and a run can start up to 60 minutes after that slot, so a run can start nearly 2 hours after `run_at`. It runs only while Lite is open. A day is skipped only when Lite was closed from the slot until 60 minutes after it. |
| `ignored_wait_types.json` | `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\` (per-user) | 126 benign wait types excluded by default |

When a second Windows user on the same machine launches Lite, they see the shared `servers.json` immediately. SQL Auth and Entra MFA passwords are scoped to each user's own Credential Manager, so they'll be prompted once per server; Windows Auth works without any prompt.

Lite refuses to connect to an entry in `servers.json` until the Windows account that runs it has confirmed the host that the entry names, so an entry that you add by hand fails with `You have not connected to this server's host, <host>, from this Windows account yet.` and a `ServerHostNotConfirmedException` in the log. On a machine with no desktop, run `--add-server` as that account with the same host and database as the entry: it confirms the host, stores the password, and updates the listed entry without adding a second one (see [Add servers with `--add-server`](#add-servers-with---add-server)). `--add-server` refuses a listed entry that holds a Query Store replica, a utility database, a certificate that is trusted without a check, Optional encryption or a managed identity client id that the account has not confirmed, and it changes nothing. A run that gives the encryption mode or the certificate setting itself (`--encrypt-mode`, `--trust-server-certificate`) confirms that setting, and the other settings are confirmed in the Add Server window.

**Always On AG?** Enable **ReadOnlyIntent** in the connection settings to route Lite's monitoring queries to a readable secondary, keeping the primary clear. Enable **MultiSubnetFailover** for multi-subnet failover scenarios.

**Query Store from a replica.** Set **QS replica** (a host, `host,port` or listener name) on a SQL Server registration to read Query Store from a readable secondary instead of the primary. Empty, the default, reads the primary and changes nothing. Azure SQL Database never reads a replica. The replica host gets the same login and password as the server, so it must accept them. Collection health shows the newest Query Store run's `read_source` (for example `replica 12, primary 2`) in the Note column and the MCP `get_collection_health` row, only when a run has one.

### Extended Events ring buffer limit

`settings.json` has one key for the ring buffer of the deadlock, blocked-process and long-query Extended Events sessions Lite creates:

| Key | Default | Notes |
|---|---|---|
| `xe_ring_buffer_max_events` | `1000` | The `max_events_limit` of the session's ring buffer. A ring buffer holds at most this many events or 4 MB (`max_memory`, fixed at 4096 KB), whichever fills first. SQL Server's own default is 1000, and a session at its limit evicts its oldest events before a collection reads them. A larger value holds more events between collections. Clamped to 1000-10000: a number outside that range is replaced by the nearest bound, at any size a double can hold (a fraction is dropped first, so 4321.7 is 4321); a number past that, such as 1e999, and a value that is not a number are ignored and 1000 is used; and for an out-of-range value Lite logs a warning naming the key, the value given and the value used at every start while the file holds the out-of-range value. Lite reads the key only at start (not on a settings reload), so a change needs a restart, and then reaches a session only when that session is next created (see below). File only, with no control in the Settings window. Edit and restart. |

**Memory often fills first.** `max_memory` stays at 4096 KB whatever the setting. A deadlock event carries its whole deadlock graph XML, so with large graphs the deadlock session's buffer fills by memory before it holds 1,000 events, and a higher limit adds little there. The setting mostly helps the blocked-process and long-query sessions. When memory was the cap, the loss note says so.

**It applies when a session is created.** A session keeps the limit it was created with, so a changed value reaches only sessions created afterwards. How an existing session gets the new value:

- Long query: automatic. When a `long_query_completions` read finds this install's own session running below the setting, Lite recreates that session with the setting right after the read's rows are stored: it drops the session, creates it with the setting and starts it. On Azure SQL Database this is done per database, and only in the databases the long-query sessions are created in: a session in a database left off the server's list of databases that get Extended Events sessions (Manage Servers) is left alone. A recreate that completes is not repeated while Lite runs, for that session and that setting value, so an engine that keeps a lower limit is recreated once, not on every run; a restart or a new value tries once more. A failed attempt is logged as a warning, a missing session is created by the next cycle's check, and the recreate is tried again an hour later. Events that reach the session between that read and the recreate are lost. Turning the collector off and on again also recreates the session with the current value. Lite never drops or alters the shared deadlock and blocked-process sessions for this.
- Deadlock and blocked-process: drop the session with the statements under [Cleaning up by hand](lite-extended-events.md#cleaning-up-by-hand), using the shared names. Lite creates a missing session the next time its collector runs.

**The deadlock and blocked-process sessions are shared by name across installs, so when two installs with different settings watch one server, whichever created the session wins. The loss note shows the session's actual limit.**

**Where a loss shows.** When a collection finds that events were evicted from a ring buffer before the read, or that the session dropped events before they reached its targets, collection health shows a WARNING on the run that found the loss, with a note that says how many, over what window, and what to do about it. The WARNING clears on the next clean run, because collection health reads only a collector's newest run; the MCP window answers keep the record for the whole window they cover. The same note reaches the `ring_buffer_loss` key of the MCP tools `get_deadlocks`, `get_deadlock_trend`, `get_blocked_process_reports`, `get_blocking_trend`, `get_blocking_stats` and `get_long_query_completions` when the window they cover lost events (an empty answer carries it inside `hints`, and then does not call the window an all-clear). Read it like this:

- **Evicted** is the number of events that arrived and were pushed out of a full buffer before a collection read them. **Dropped** is a per-session number: events the session discarded from its own full buffers before they reached any target. For you they are the same loss. A larger event limit helps only with evicted events, so for dropped events the note advises collecting more often.
- "At least N" means the previous read was truncated, so some events it held were never counted.
- After a session restart the note says the loss cannot be counted, and it gives no number: a window whose only lossy runs were restarts has `evicted_events` and `dropped_events` null, not 0, and `unknown_spans` counts the restarts. The window of a note names the runs that found the loss; each loss happened in the time before its run. A restart is routine, so this alone does not raise the WARNING.
- The note advises raising `xe_ring_buffer_max_events` only when the event limit was the cap and the read was not truncated. When the buffer was full at its 4 MB memory cap, or the read was already truncated, a higher limit would not help: collect more often instead.
- When the setting is above the session's actual limit, the note says so: the session was created before the setting changed, or by another install. It also gives the steps above: this install's own long-query session is recreated with the setting once after its next read (see above), and a shared deadlock or blocked-process session takes the setting once it is dropped and Lite creates it again.
- A value that makes collection health report a truncated ring buffer is too high. SQL Server cuts a ring buffer's XML at about 4 MB when it is read, so events held past that cut are not read.
- Azure SQL Database registrations of the logical server (no database, or `master`) read deadlocks from a telemetry file and report no ring-buffer loss for them.
- `get_blocked_process_reports` and `get_blocking_trend` with a `database_name` filter carry the whole session's loss, not the filtered database's: the buffer holds every database's events, and the loss then has a `database_filter_note` saying so.

### Appearance

Lite ships three themes — **Dark** (the default), **Light** and **Cool Breeze** — picked from **Settings → Color theme**, applied live. Every surface in the app is painted from one palette of eighteen named colors per theme, and you can adjust twelve of them yourself (#3577), per theme, with a reset to the shipped values.

Open **Settings**, expand **Colors** beneath the theme drop-down, and you get one row per color for the theme currently showing: a swatch, the hex, a picker (hue / saturation / brightness sliders) and the **measured contrast ratio** against the surface that color renders on — page background against text, accent against the ink that sits on it, each status color against the page. Green is 4.50:1 or better (WCAG AA for text), amber is 3.00–4.49 (fine for a marker, hard as text), red is under 3.00. The readout is a measurement, not a gate: **Apply** always works. **Reset to default** returns the current theme to its shipped palette and leaves your other themes alone. **Open file** opens the JSON in your editor; anything you save there applies live too.

The file is `%LOCALAPPDATA%\PerformanceMonitorLite-Data\config\theme-overrides.json`, beside `settings.json`. It is keyed by theme, then by color, and holds only what you changed:

```json
{
  "Dark": {
    "AccentColor": "#3A7BD5",
    "AccentForegroundColor": "#FFFFFF"
  },
  "CoolBreeze": {
    "WarningColor": "#B45309"
  }
}
```

Values are `#RRGGBB` (`#AARRGGBB` is accepted). A key that is not one of the twelve, a value that is not a color, a theme that is not one of the three, or a file that is not JSON is skipped with a line in the log and never stops Lite from starting. Deleting the file, or a theme's block, is the same as Reset.

| Key | Group | What it paints |
|---|---|---|
| `BackgroundColor` | Backgrounds | The window and panel background most text sits on |
| `BackgroundLightColor` | Backgrounds | Cards, settings sections, buttons at rest, headers — a step lighter than the page |
| `BackgroundDarkColor` | Backgrounds | The sidebar, grid rows and other recessed areas — a step darker than the page |
| `ForegroundColor` | Text | Primary text: values, headers, labels |
| `ForegroundDimColor` | Text | Captions and secondary labels |
| `AccentColor` | Accent | The selected tab, the selected grid row, highlighted combo items, accent buttons, links |
| `AccentForegroundColor` | Accent | The ink for text and glyphs on an accent fill |
| `AlternatingRowColor` | Rows | Every other grid row |
| `SuccessColor` | Status | Healthy / OK markers and the Done row mark |
| `WarningColor` | Status | Warning markers and the To Do row mark |
| `ErrorColor` | Status | Error / critical markers and the Do Not Do row mark |
| `InfoColor` | Status | Informational markers |

The other six palette colors (`AccentHoverColor`, `AccentPressedColor`, `BackgroundLighterColor`, `ForegroundMutedColor`, `BorderColor`, `BorderLightColor`) are derived tones — a hover is the accent lightened a step, a border is the background lifted a step — and follow the theme author's values; they are not adjustable, and neither is any single control (the selected tab, one button): the knobs are the palette, and every surface built from it follows. A handful of component-specific brushes the themes paint with a fixed value (the plan viewer's panels, the time-range slicer, the deadlock graph) are likewise not derived from the palette and do not move.

### Lite Collectors

More than 40 collectors run on independent, configurable schedules (the long-running-query completion trace is opt-in and ships disabled):

| Collector | Default | Source |
|---|---|---|
| query_snapshots | 1 min | `sys.dm_exec_requests` + `sys.dm_exec_sessions` |
| blocked_process_report | 1 min | XE ring buffer session |
| waiting_tasks | 1 min | `sys.dm_os_waiting_tasks` |
| wait_stats | 1 min | `sys.dm_os_wait_stats` (deltas) |
| latch_stats | 1 min | `sys.dm_os_latch_stats` (deltas) |
| spinlock_stats | 1 min | `sys.dm_os_spinlock_stats` (deltas) |
| cpu_scheduler_stats | 1 min | `sys.dm_os_schedulers` runnable/blocked/queued task counts (not Azure SQL DB) |
| long_query_completions | 1 min (opt-in, ships off) | dedicated completion-trace XE (`rpc_completed`/`sql_batch_completed` over a duration threshold, plus `attention`) |
| attention | 1 min | dedicated `PerformanceMonitor_Attention` XE session (`attention`: queries a client cancelled or timed out, with the client application, host, database and statement; feeds the Client Timeouts alert) |
| query_stats | 1 min | `sys.dm_exec_query_stats` (deltas) |
| procedure_stats | 1 min | `sys.dm_exec_procedure_stats` (deltas) |
| cpu_utilization | 1 min | `sys.dm_os_ring_buffers` scheduler monitor |
| database_states | 1 min | `sys.databases` state per database — feeds the database offline/unhealthy alert (not Azure SQL DB) |
| file_io_stats | 1 min | `sys.dm_io_virtual_file_stats` (deltas) |
| memory_stats | 1 min | `sys.dm_os_sys_memory` + memory counters |
| memory_grant_stats | 1 min | `sys.dm_exec_query_memory_grants` |
| tempdb_stats | 1 min | `sys.dm_db_file_space_usage` + `tempdb.sys.database_files` (the ROWS files' growth ceiling) |
| perfmon_stats | 1 min | `sys.dm_os_performance_counters` (deltas) |
| deadlocks | 5 min | dedicated `PerformanceMonitor_Deadlock` XE session (`xml_deadlock_report`; `database_xml_deadlock_report` on Azure SQL DB) |
| dmv_blocking_snapshot | 1 min | `sys.dm_os_waiting_tasks` + `sys.dm_exec_*` (always-on blocking fallback when the blocked-process-report XE is unavailable) |
| ag_replica_states | 1 min | `sys.dm_hadr_availability_replica_states` + `sys.availability_replicas` (Availability Group replica health; zero rows without AGs, not Azure SQL DB) |
| ag_database_replica_states | 1 min | `sys.dm_hadr_database_replica_states` (per-database send/redo queues, rates, secondary lag; zero rows without AGs, not Azure SQL DB) |
| session_stats | 5 min | `sys.dm_exec_sessions` active session tracking |
| session_summary_stats | 5 min | `sys.dm_exec_sessions` top app/host/database summary |
| memory_clerks | 5 min | `sys.dm_os_memory_clerks` |
| memory_pressure_events | 5 min | `sys.dm_os_ring_buffers` RING_BUFFER_RESOURCE_MONITOR |
| query_store | 5 min | Query Store DMVs (per database) |
| plan_correction | 5 min | `sys.dm_db_tuning_recommendations` + `sys.database_automatic_tuning_options` (automatic plan correction: FORCE_LAST_GOOD_PLAN enablement plus the engine's live regression recommendations, with the regressed query's text resolved through Query Store; SQL Server 2017+, Azure SQL DB and Azure SQL Managed Instance) |
| plan_cache_stats | 5 min | `sys.dm_exec_cached_plans` (single-use vs reused plan-cache bloat) |
| system_health_events | 5 min | `system_health` XE ring buffer (not Azure SQL DB) |
| default_trace_events | 5 min | default trace via `sys.fn_trace_gettable` (not Azure SQL DB) |
| job_history | 5 min | `msdb.dbo.sysjobhistory` retained job-run history (not Azure SQL DB) |
| agent_status | 5 min | `sys.dm_server_services` + `msdb.dbo.sysjobschedules` (not Azure SQL DB / RDS) |
| running_jobs | 5 min | `msdb` job history with duration vs avg/p95 (not Azure SQL DB / AWS RDS) |
| database_size_stats | 1 hour | `sys.master_files` + `FILEPROPERTY` + `dm_os_volume_stats` |
| pvs_stats | 1 hour | `sys.dm_tran_persistent_version_store_stats` + `sys.databases` (ADR persistent version store size and cleanup state per database; SQL Server 2019+ only, always collected on Azure SQL DB) |
| query_store_health | 1 hour | `sys.database_query_store_options` per database (actual vs desired state, readonly_reason, storage used vs cap, cleanup thresholds, runtime-stats interval length; SQL Server 2016+, one row per database with OFF recorded explicitly) |
| server_properties | on connect | `SERVERPROPERTY()` hardware and licensing metadata |
| index_object_stats | Daily | `sys.dm_db_partition_stats` + `sys.dm_db_index_usage_stats` + `sys.dm_db_index_operational_stats` |
| server_config | On connect | `sys.configurations` |
| database_config | On connect | `sys.databases` |
| database_scoped_config | On connect | Database-scoped configurations |
| trace_flags | On connect | `DBCC TRACESTATUS` |

Darling runs these same collectors across a fleet of servers. It adds five more that Lite does not have:

- `query_store_forced_plans` reads each database's forced plans.
- `query_store_hints` reads each database's Query Store hints (SQL Server 2022 and later).
- `identity_capacity` reads how full each database's identity columns are.
- `database_protection` reads the last good CHECKDB and the last full and log backup of each database.
- `agent_alerts` reads the SQL Server Agent alerts and the operators they notify.

Darling also runs more than 25 PostgreSQL collectors. See the [Darling collector reference](../Darling/README.md).

The Extended Events sessions that Lite creates on a monitored server, and how to remove them, are on their own page: [The Extended Events Sessions Lite Creates](lite-extended-events.md).
