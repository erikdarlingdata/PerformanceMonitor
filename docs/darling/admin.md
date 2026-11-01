# Running Darling day to day

Audience: operators.

This page is for the person who keeps a Darling install running after the first week. Each heading below is one task, written as numbered steps. When you need to know what a setting means, follow the link to the [Darling operator guide](../../Darling/README.md).

## Read this first: `darling.json` is only a seed for the server list

The service reads the `servers` array in `darling.json` on the first start only. After that, the store holds the server list. If you edit the array later, nothing happens.

To change the list, use one of these:

- The Viewer: **Manage Servers**.
- The `add_servers`, `edit_server` and `remove_server` MCP tools. They need `mcp.allowWrites` and `mcp.allowServerAdmin`.
- The `--add-server` command line verb. It can add a server. It cannot remove one.

The web Admin page can edit a server that already exists. It cannot add, remove, enable or disable one. The page says so itself. For the full list of what the file still controls, see [`darling.json` is only a seed](install.md#darlingjson-is-only-a-seed).

The Admin page's edit and the `POST /api/servers` route save a server that uses Windows authentication only over a verified certificate. The route adds a server and has no page of its own. A verified certificate means `trust_server_certificate` false and `encrypt_mode` `Mandatory` or `Strict`. Setting `web.allowWindowsAuthOverUnverifiedTls` to true lifts this (default off), as `mcp.allowWindowsAuthOverUnverifiedTls` does for the MCP tools.

Run the command line verbs on the service host, from the folder that holds `PerformanceMonitor.Darling.Service.exe`. If a verb reads a credential or a token, run it from an elevated PowerShell.

## Add a server

1. Make sure the monitoring login exists on the target server. To print the T-SQL that creates it, run `.\PerformanceMonitor.Darling.Service.exe --grant-script --host <host>`. See [Permissions on monitored servers](monitored-servers.md#permissions-on-monitored-servers).
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

- **On the monitored server:** the service drops this install's own Extended Events sessions on it, with one attempt within 15 seconds. It does not drop the shared sessions, and it does not drop a session that another registration of this install keeps. If the attempt failed, run `.\PerformanceMonitor.Darling.Service.exe --drop-xe-sessions --print-sql` and run the statements it prints on the server. See [Drop the Extended Events sessions a removed server left behind](collector-settings.md#drop-the-extended-events-sessions-a-removed-server-left-behind---drop-xe-sessions).
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

A saved password is sealed to the service's password key and to the connection it was saved with. If you also change the host, port, database, login or encryption setting, enter the password again in the same edit. See [The password key](security.md#security--least-privilege-roles).

For a server that still takes its password from `darling.json` (`encryptedPassword`):

1. Change the password on the monitored server.
2. On the service host, run `.\PerformanceMonitor.Darling.Service.exe --encrypt-password`. Type the new password when it asks. The verb prints a base64 blob.
3. Paste the blob into the server's `encryptedPassword` in `darling.json`. Edit the file as text. It is JSONC, so do not round-trip it through `ConvertFrom-Json`.
4. Restart the service.

The blob is bound to the machine. Make it on the machine that runs the service.

If you reset the password key with `--reset-password-key`, every password saved in the store has to be entered again. See [Make a new password key](collector-settings.md#make-a-new-password-key---reset-password-key).

## Rotate the web or MCP token

The token is a secret in `darling.json`. The `web.network` and `mcp.network` blocks are read from the file only, so a change needs a restart.

1. Make a new token. It must have at least 16 characters, at least 6 of them different. To make one, use the PowerShell one-liner under [MCP endpoint](network-endpoints.md#mcp-endpoint-assistant-over-the-lan).
2. On the service host, run `.\PerformanceMonitor.Darling.Service.exe --encrypt-password`. Paste the token when it asks. The verb prints a blob.
3. In `darling.json`, replace the blob in `web.network.encryptedToken` (the dashboard) or `mcp.network.encryptedToken` (MCP).
4. Restart the service.
5. In an elevated PowerShell, run `.\PerformanceMonitor.Darling.Service.exe --print-web-token` or `--print-mcp-token`. Check that it prints the new token. This writes a live token to the screen, so do it where no one else can see it.
6. Give the new token to every client. Browsers sign in again on the dashboard's sign-in page. Each MCP client needs the new bearer header. The old token stops working at the restart.

`--configure-network` can also make a token for you. It generates 32 random characters.

## People, sign-ins and grants

Darling keeps a list of people. Each person has a name, the sign-ins that belong to them, and grants that say what they can do and where. Removing a server or a tag removes the grants on it, so adding the same server back does not bring them back. Nothing reads a grant yet, so adding one changes no power today. Administrators keep every power they have. For who can write the list and what is recorded, see [People, identities and grants](security.md).

A sign-in is one of two kinds:

- **Web sign-in**: the sign-in key of a person who signs in to the web dashboard through the identity provider.
- **Slack**: the Slack team id and user id, such as `T0123ABCD` and `U0123ABCD`.

A grant names what the person can do and where. Where is one server, one tag, or all servers. What is one of these:

- Ask the agent (reads only).
- Request Level 2 changes.
- Request server actions, with the server actions it covers.
- Approve server actions, with the server actions it covers.
- Undo agent changes.
- Approve or reject agent notes.
- Give report feedback.

### On the web dashboard

Open the **Admin** page, then the **People** tab. For an admin, the tab lists every person with their sign-ins and grants. Click **Open** on a row to see one person.

Only an admin who is signed in to the dashboard sees the list, and that includes a session signed in with the shared web token. The list shows each person's sign-in keys and Slack ids, so any other session sees only its own sign-in key and its **Copy** button. An admin can make changes only when the web's store login can make them. Otherwise the admin sees the list and a sentence that says why it cannot be changed.

To add a person, type the name and click **Add person**. In a person's details you can rename them, disable or enable them, add or remove a sign-in, and add or remove a grant. A grant cannot be edited. Remove it and add it again instead.

### Link a web sign-in to a person

Darling keeps no list of who signs in. So a person gives their sign-in key to an administrator:

1. The person signs in to the web dashboard through the identity provider and opens **Admin**, then **People**.
2. The tab shows **Your sign-in key** at the top. A person who is not an admin sees only that. The person clicks **Copy** and sends the key to an administrator.
3. The administrator opens the person's details, picks **Web sign-in**, pastes the key and clicks **Add sign-in**.

An administrator who links their own sign-in can click **Add my own sign-in** in the person's details instead.

The key is a hash of the sign-in that Darling works out when you sign in. It is not a password. Knowing it signs nobody in. The shared web token has no key, so a session signed in with it cannot be linked to a person, and the tab says so. A session from a build before sign-in keys has none either: open `/auth/logout` and sign in again.

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

Windows PowerShell 5.1 and PowerShell 7 both run the script. Add `-Check` to the command from step 3 to see whether the upgrade can run. It runs the checks that come before the stop, tries the backup-folder step on a throwaway folder, prints the PowerShell version, and changes nothing.

The script refuses to run from the install folder itself. It never kills a process. If it names a process that blocks the upgrade, close that process and run it again. It keeps the newest three rollback backups. To clear old ones, run `& "C:\Program Files\PerformanceMonitorDarling\upgrade-darling.ps1" -PruneOnly`.

The script also refuses a build made with a plain `dotnet publish` over a release install. Such a build carries no .NET runtime, and the old runtime files left in the folder stop the service from starting. The refusal comes before the service stops, so nothing changes. Use the release zip, or publish with `--runtime win-x64 --self-contained`.

If you installed with the MSI, do not use this script. Run the newer MSI instead. It upgrades in place and keeps `darling.json` and the store.

For more, see [Upgrading an existing install](#upgrading-an-existing-install) and [Rolling back to an older build](#rolling-back-to-an-older-build).

### Upgrading an existing install

`install-darling.ps1` registers a service; it does not lay a new build over an old one. That step — stop, back up, copy, start, verify — is `upgrade-darling.ps1`, shipped in the same zip.

> **`upgrade-darling.ps1` does not exist in 3.5.0 or earlier.** It was added after 3.5.0 was tagged, so a build up to and including 3.5.0 does not contain it and neither does its zip. If you upgrade from one of those, use the short manual procedure in [upgrading from a build that predates the script](#upgrading-from-a-build-that-predates-the-script) once. The steps in this section describe a script that those builds do not have.

In the examples below, replace `<version>` with the release number you downloaded. Extract the new zip to a **staging** folder only an administrator can write to, from an **elevated** PowerShell, and run *its* copy (#4043): the folder you run the script from is the trust root, and anyone who can write to it can replace the script itself, so a folder under `C:\Program Files\` is required, not a folder made directly under `C:\`. (Windows UAC's split token is not a security boundary here: trusting the installing admin's account also trusts that same admin's non-elevated session, since both are the same account.)

```powershell
Expand-Archive PerformanceMonitorDarling-<version>.zip -DestinationPath "C:\Program Files\PerformanceMonitorDarling-staging\<version>"
& "C:\Program Files\PerformanceMonitorDarling-staging\<version>\upgrade-darling.ps1" -Source "C:\Program Files\PerformanceMonitorDarling-staging\<version>"
```

To verify the zip's SHA256 as part of the same run, point `-Source` at the downloaded zip itself instead of the extracted folder, with the hash from the release page — the script copies that zip into its own protected staging folder, hashes the copy, and extracts from it:

```powershell
& "C:\Program Files\PerformanceMonitorDarling-staging\<version>\upgrade-darling.ps1" -Source C:\Users\you\Downloads\PerformanceMonitorDarling-<version>.zip -Sha256 <hash-from-the-release-page>
```

It resolves the install directory from the registered service, verifies the zip's SHA256 when you point it at one (checked against `-Sha256` or a `SHA256SUMS.txt` beside it, itself only trusted from a folder that passes the same writability check), backs the install root's files up to `_rollback_manual_<stamp>`, prunes the backups past the newest `-KeepRollbacks` (3), lays the new build down, confirms `darling.json` is byte-identical, and starts the service. Re-running after a failure is safe and is the intended recovery: a backup taken in the last `-BackupWindowMinutes` (60) is reused rather than replaced when the upgrade that took it did not finish, so a re-run cannot overwrite the good pre-upgrade copy with a copy of a half-upgraded tree. An upgrade that finished (`darling-install-manifest.txt` is newer than the backup) always gets a new backup, so two complete upgrades inside the hour each keep their own. A missing or unreadable manifest counts as not finished.

**It never kills a process**, and it checks for them twice. Before stopping anything it names processes running out of the install tree that a service stop will *not* close — your own `psql.exe`, a shell sitting in the folder, a Darling Viewer you left open — and refuses, costing nothing but a re-run. After the service is down it checks again with no exclusions; anything still there is usually a postmaster that outlived the stop, and that is exactly what must not be killed (the bundled PostgreSQL lives under `pg-runtime` and killing it takes the store down). Give it a few seconds and re-run.

**A rollback backup holds the install root's *files*, not `viewer\`, `wwwroot\`, `runtimes\` or `pg-runtime\`** — that is what keeps one to ~120 MB instead of ~1 GB. It matters in one case: if a copy dies partway, those subdirectories can be left mixed old-and-new, and a full revert is the *previous version's zip re-extracted* followed by the backup's files over the top. The script says so at the point of failure.

It **refuses to run from the install directory itself** — the copy would overwrite the script PowerShell is reading — which is why the staging folder above is not optional.

**Files the new build no longer ships.** The copy is an *overlay*: it writes what the new build ships and deletes nothing else, so a dependency that went away, an assembly that changed name, or a whole `runtimes\<rid>\lib\<tfm>\` subtree stranded by a target-framework move stays in the tree forever — and those are directories .NET probes for assemblies ([#2529](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2529)). It has happened: the Lite package dropped 44 shipped files across twelve consecutive releases, 43 of them in one step. After every successful copy the script writes `darling-install-manifest.txt` into the install root recording the files that copy laid down, and the next upgrade diffs its own payload against it and **names** whatever an earlier build shipped and this one does not. Add `-RemoveStaleFiles` to delete them rather than only list them — off by default for now, so you can see the answer on your own boxes before anything acts on it.

Everything it can name provably came out of one of our own zips, because the manifest is DERIVED from the payload rather than maintained by hand: `darling.json`, its `.bak-*` copies, the DPAPI credential blobs, the `_rollback_manual_*` backups and `pg-runtime\` were never in a payload, so they are never in the manifest and can never be nominated. When it cannot tell — no manifest yet, one it cannot parse, a source it cannot read — it removes nothing and says so on its own line. Deleting `darling-install-manifest.txt` is safe: the next upgrade reports that it cannot tell, removes nothing, and writes a fresh one.

**Backups pile up, and nothing used to remove them.** A dogfood box was found carrying 46 of them, 5.48 GB, the oldest three weeks old, with the service naming every one on every start ([#2525](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2525)). Retention above fixes new deploys; boxes that already have a backlog clear it with the installed copy, which needs no staging folder and does not stop the service:

```powershell
& "C:\Program Files\PerformanceMonitorDarling\upgrade-darling.ps1" -ListRollbacks   # show what would go
& "C:\Program Files\PerformanceMonitorDarling\upgrade-darling.ps1" -PruneOnly       # remove all but the newest 3
```

The service reports the set once per start with a count, a total and that command — informational while you are within retention, a warning past it. It never deletes one itself: it did not create them.

**The install is not verified when the service reaches Running.** That means the process started, not that it collects. The script prints a post-start checklist. About 10–15 minutes after the start, work it in the Viewer or the web page: every monitored server shows Online, Collectors shows OK with none failing, and the service log has a `Postgres store ready (schema v<N>, ...)` line from this start. N is the store version this build expects. If you connect to the store, `SELECT MAX(version) FROM darling_schema_version` returns the same N. Hold the upgrade unverified until every check passes.

#### Rolling back to an older build

A rollback restores the install files, not the store ([#5773](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5773)). The store keeps every migration the newer build applied, and the older build runs against it: migrations are additive, so it starts and collects. When it starts, the service logs one warning that names the migrations it does not know, for example: *"The store is at V183; this build knows up to V180, so V181-V183 are unknown to it."*

An older build ignores what those migrations added, so it can read some newer data differently. The list below names each migration that changes what an older build reads, and what to do before you roll back past it. A migration not listed here has no known effect on what an older build reads. What each migration added is in the ladder fixtures `Darling/Darling.Tests/Fixtures/migration-ladder-*.sql` and in `PgMigrations.cs`, and the release notes in `docs/changelog/` name the changes.

- **V181**: an older build reads a notification route scoped to a server or a tag ([#5750](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5750)) as fleet-wide. A route meant for one server then sends the alerts of every server. A scoped route for every metric (`*`) matches nothing, so its alerts go to the default destinations. Disable or delete every scoped route in the Notification Routes window before you roll back to a build older than V181.
- **V182**: an older build cannot write its hourly read-latency rows ([#5506](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5506)). V182 adds the window band to the table's key, and the older build's write names the old key, so each hour's write fails with a warning in the service log and that hour's read latency is not recorded. Collection and every other read are not affected. Nothing needs to be done before you roll back; recording resumes when a build at V182 or later runs again.
- **V200**: an older build does not know the excluded-volumes list ([#6098](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6098)), so it ignores the `excluded_volumes` column and the Volume Free Space alert evaluates every volume again. A volume you excluded then alerts once more. Nothing needs to be done before you roll back; the list is kept in the store and applies again when a build at V200 or later runs.
- **V204**: an older build does not know the folders, personal views and owner of the custom views (the `folder`, `personal`, `created_by` and `owner_key` columns) ([#6214](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6214)). It lists every view in one flat list by name, including each person's personal views, and lets anyone change or delete any view. Two views in different folders can share a name, and the older build then shows both under the same name. Nothing needs to be done before you roll back, but a personal view becomes visible and editable to everyone: delete any you do not want shared first. The older build never reads or writes `owner_key` (the stable key that decides who owns a personal view), so it also cannot hold a view you made personal to its owner: there is no database guard against that, by choice.
- **V205**: an older build does not know the expiry of a custom alert rule (the `expires_at_utc` column) ([#6235](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6235)). It ignores the column, so a rule that has passed its expiry is evaluated and fires again. Nothing needs to be done before you roll back, but disable or delete any rule you set an expiry on and no longer want firing; the expiry is kept in the store and applies again when a build that knows it runs.
- **V206**: an older build reads a collector schedule's enabled flag as a plain true or false and fails on a NULL ([#6227](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6227)). The schedule tools store NULL ("inherit") on a row that sets only a frequency or a retention, and `inherit: true` stores it on purpose. Before you roll back, give every NULL the value it inherits: the fleet-wide row's value for that collector, else the collector's shipped default (off for `long_query_completions`, on for every other collector). `UPDATE config.config_collector_schedules AS s SET enabled = COALESCE((SELECT f.enabled FROM config.config_collector_schedules AS f WHERE s.server_id IS NOT NULL AND f.server_id IS NULL AND lower(f.collector_name) = lower(s.collector_name) LIMIT 1), lower(s.collector_name) <> 'long_query_completions') WHERE s.enabled IS NULL;` The statement reads the rows as they were before it ran, so a server's row takes its fleet-wide row's value even when that row is set to false. Deleting those rows works too. If you skip this step the older build cannot read the store's configuration at all: every reload fails on the NULL, so it runs on darling.json alone, servers added after the store was seeded are not collected, and the store's paused flag, alert and notification settings and the MCP and web enable flags are ignored (an endpoint switched off in the store comes back up when darling.json still turns it on). The schedule tools' write grants on the `mcp` role are not removed by an older build's role provisioning either, so revoke them: `REVOKE INSERT, UPDATE ON config.config_collector_schedules FROM mcp; REVOKE INSERT, UPDATE, DELETE ON config.config_collector_run_times FROM mcp;`
- **V207**: an older build does not know the change watches ([#6234](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6234)). A change watch opens an investigation request marked `change_watch` (the `trigger_kind` column), and the older build's alert grouping does not check that column. While a newer and an older instance share one store, an alert on the older instance can join a change-watch request that is still open, within the usual grouping window, and its alert is then carried on that request instead of opening its own. The newer build never joins an alert to a change-watch request. Stop the newer instance before you let an older instance alert against the same store. The watch tables and the column stay in the store and apply again when a newer build runs.
- **V208**: an older build does not know a mute rule's start time ([#6231](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6231)), so it ignores the `starts_at_utc` column and applies a quiet window or mute rule that is set ahead of its start as soon as it is stored. Before you roll back past V208, clear or end any quiet window whose start is still ahead (`get_mute_rules` shows it as `state: upcoming`), or it mutes early on the older build.
- **V210**: an older build does not know the guard on the MCP tag writes ([#6281](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6281)). Rolling back leaves the guard's trigger and function in the store, where they keep refusing: the older build's tag tools then get a refusal naming the route when they would change which servers an agent route covers (and its remove_server answers an error, not a refusal, for a server mapped under such a route). Nothing is read differently, so nothing needs doing before rolling back; to lift the refusal, drop `trg_agent_route_tag_guard` on `config.server_tags` and `config.server_tag_map`.
- **V213**: an older build does not know the Query Store replica ([#6254](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6254)). Rolling back leaves `config_monitored_servers.query_store_replica` and `collect.collection_log.read_source` in place, and the older build ignores both: it reads Query Store from the monitored server itself, not from the replica, so the primary carries that load again, and it does not record which host a run read from. Before you roll back past V213, clear the replica on each server that has one (`edit_server` or the Edit Server dialog), or note that the primary is read again; nothing else needs doing first.
- **V216**: an older build does not know that the Client Timeouts alert is on by default ([#5736](https://github.com/erikdarlingdata/PerformanceMonitor/issues/5736)). Rolling back leaves `config_alert_settings.client_timeouts_enabled` as it is: the older build reads the same column, so the alert stays on until you switch it off in the Settings window. The older build's own default for a new store is off. Nothing needs doing first.
- **V219**: an older build does not know the Query Store self-statement keys ([#6750](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6750)). Rolling back leaves `collect.query_store_self_keys`, `collect.query_store_self_keys_state` and the two triggers on `collect.query_store_text` in place. The older build ignores the tables, and the triggers keep the keys current whenever it writes statement text, so when you roll forward again the Query Store regressions read still hides the monitor's own statement. Nothing needs to be done before you roll back.
- **V220**: an older build does not know the Job History statistics index ([#6798](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6798)). Rolling back leaves `idx_job_history_job_success` on `collect.job_history` in place. The older build's Job History read can still use it, and the collector keeps it current on every insert. Nothing needs doing before you roll back. Leave the index in place: the viewer recognizes a V220 store by it, so a store whose index was dropped answers 219 to the viewer while its schema version says 220, the viewer refuses it, and the migrator does not build the index again.
- **V221**: an older build does not know the Query Store hash-hourly table ([#6800](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6800)). Rolling back leaves `collect.query_store_hash_hourly`, the day tables `collect.query_store_hash_daily`, `collect.query_store_hash_daily_built` and `collect.query_store_hash_daily_failures`, and the `hash_built_seq` column of `collect.query_store_compose_stamp_built` in place. The older build neither reads nor fills them, so the tables stop growing and their hours and days go stale. Nothing to do before rolling back. If you roll forward again, the next hourly passes rebuild the stale hours, newest first, and the next cleanup removes the hash-hourly rows of any hour the older build retired without deleting them (it deletes by the retention floor, not by the hours it just retired)
- **V227**: an older build does not know a threshold override held for a server tag ([#6926](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6926)). Rolling back leaves the `scope_tag_id` column on `config.config_threshold_overrides` (and `server_id` nullable), and any tag override rows stay. An older build reads the whole overrides table when it loads, and a row with no server stops that read, so it applies **no** threshold override at all (server or database ones included) until the tag rows are gone. Before rolling back past V227, remove every tag override (the Viewer's Threshold Overrides window, the web Admin tab, or `set_alert_threshold_override` with `clear` and a tag), or run `DELETE FROM config.config_threshold_overrides WHERE server_id IS NULL;` as the owner.
- **V229**: an older build does not know the severity filter on a notification route ([#6928](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6928)). Rolling back leaves `config.config_notification_routes.severities` in place. The older build ignores it. A route set to `critical` or `warning` takes alerts of both severities again. A Warning alert then reaches the channels of a route that was meant for Critical alerts only. Before you roll back, run `get_notification_routes` and look at every route whose `severities` is not `both`. If its channels must not see the other severity, change the route's destinations or disable it. A route set to `both` is unchanged. If you roll forward again, the filters you set are still there.
- **V231**: an older build does not know that a mute rule can repeat on a schedule ([#7040](https://github.com/erikdarlingdata/PerformanceMonitor/issues/7040)). Rolling back leaves `config.config_mute_rules.recur_days`, `recur_start_time`, `recur_end_time` and `recur_time_zone` in place. The older service and Viewer ignore them, so a recurring rule mutes all day, from its start to its expiry, instead of only inside its windows. Upgrade the service before you create recurring rules. Before you roll back, delete or pause every rule that has a schedule. `get_mute_rules` lists them with a `schedule`. A rule with no schedule is unchanged. If you roll forward again, the schedules you set are still there.
- **V232**: an older build does not know the Daily Health Report ([#7041](https://github.com/erikdarlingdata/PerformanceMonitor/issues/7041)). Rolling back leaves `daily_report_enabled`, `daily_report_send_at`, `daily_report_time_zone` and `daily_report_title` on `config.config_alert_settings` in place. The older build ignores them and sends no daily report, so a report that was on stops going out. Nothing needs doing before you roll back. If you roll forward again the saved settings apply as they were.
- **V233**: an older build does not know the report notice setting on a notification route ([#6979](https://github.com/erikdarlingdata/PerformanceMonitor/issues/6979)). Rolling back leaves `config.config_notification_routes.report_notice` in place. The older build ignores it and posts no report notices, so a route that posted them stops. Nothing needs doing before you roll back. If you roll forward again the saved settings apply as they were.
- **V234**: an older build does not know the alert key on the alert history ([#7076](https://github.com/erikdarlingdata/PerformanceMonitor/issues/7076)). Rolling back leaves `config.config_alert_log.alert_key` in place. The older build ignores it and does not write it. Store Job Over Cadence, Retention Held, Raw Purge Over Horizon, Compose Rollup Hours Stuck and the compression, refresh and retention job alerts are then announced again after each service start while their condition stands. That is how they behaved before V234. Nothing needs doing before you roll back. If you roll forward again, the rows the older build wrote have no key. Each of those alerts that still stands is announced once more, then stays quiet across restarts.
- **V235**: an older build does not know the new blocking bars ([#7083](https://github.com/erikdarlingdata/PerformanceMonitor/issues/7083)). The rung moved `config.config_alert_settings.blocking_count_threshold` from 1 to 3, `blocking_wait_seconds_threshold` from 0 to 60 and `pg_blocking_count_threshold` from 1 to 3, each only where it still held the old default. Rolling back leaves those values in place. The older build reads them as plain numbers: it pages when 3 blocked events fall in the last hour, whatever their wait, and it runs the total blocked wait alert at 60 seconds. Nothing needs doing before you roll back. To get the old paging back, set the count bars to 1 and the wait bar to 0 first. If you roll forward again the saved values apply as they were.
- **V236**: an older build does not know what a mute rule does with a matching alert ([#7083](https://github.com/erikdarlingdata/PerformanceMonitor/issues/7083)). Rolling back leaves `config.config_mute_rules.action` in place. The older build ignores it, so a rule set to send alerts to the daily digest mutes those alerts instead: they are recorded as muted and reach nobody, not even the digest. Before you roll back, delete the digest rules or change them to mute rules on purpose (Manage Mute Rules, or the `update_mute_rule` tool with `action` set to `mute`).
- **V238**: an older build does not know action approval ([#7109](https://github.com/erikdarlingdata/PerformanceMonitor/issues/7109)). Rolling back leaves `config.config_notification_routes.action_approval`, `config.config_notification_routes.approval_single_person` and the `config.agent_action_proposals` table in place. The older build ignores them, so a Level 3 route set to need approval lets the agent run its server actions with no approval, and pending proposals are never decided. Before you roll back, lower every Level 3 route whose approval is required to Level 2 or below.

### Upgrading from a build that predates the script

This is a note for very old builds. `upgrade-darling.ps1` was added after 3.5.0, so a build from 3.5.0 or earlier needs this manual procedure once ([#2593](https://github.com/erikdarlingdata/PerformanceMonitor/issues/2593)). It is the same sequence the script automates.

Nothing that you must keep lives in the install directory. The managed store, the DPAPI credential blobs and the logs are under `C:\ProgramData\PerformanceMonitorDarling`. The only file that has to travel is `darling.json`. Renaming the install folder does not back up your data. For a data rollback point, snapshot the volume, or stop the service and copy the ProgramData folder.

1. Verify the download. Run `Get-FileHash <zip> -Algorithm SHA256` and compare it with `SHA256SUMS.txt` from the same release page. If you skip this step, you cannot recover from it.
2. Run `Stop-Service 'PerformanceMonitor Darling' -Force`. Poll until `Status` is `Stopped`. A stop request is not a stopped service.
3. Rename the current install folder (for example, add `_old`). This is your rollback.
4. Extract the new zip to the original folder name.
5. Copy `darling.json` from the renamed folder into the new one. Hash it on both sides and confirm the hashes match.
6. Run `.\install-darling.ps1` from the new folder. For an existing install, it re-points the service's binPath and keeps the config, the store and the credentials.
7. Start the service, then verify with the post-start checklist. Reaching `Running` means the process started, not that it collects.

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

After the store is deleted, `-PurgeData` also removes the local group `PerformanceMonitor Darling Operators`, with its members, but only when the install made it. The script judges that by the group's description: the installer gives the group it makes a fixed description, and a group that still has it is removed. A group an administrator made first has a different description, so the script leaves it and prints the command to remove it yourself (`net localgroup "PerformanceMonitor Darling Operators" /delete`). If you answer anything but `DELETE`, the store stays and so does the group. On a domain controller the script never looks up or removes a group. A plain uninstall without `-PurgeData` keeps the group, so a reinstall gets back the same members.

`-PurgeData` does not delete the program folder or `darling.json`. To remove them, delete them yourself. It also does not delete the Viewer's per-user files under `%APPDATA%\PerformanceMonitorDarling`.

For a script run that cannot answer a question, add `-ConfirmPurge` to `-PurgeData`. It deletes without the `DELETE` prompt.

## Where everything is

| What | Where |
|---|---|
| Config file | `darling.json`, beside `PerformanceMonitor.Darling.Service.exe`. The default install folder is `C:\Program Files\PerformanceMonitorDarling` |
| Service logs | `%ProgramData%\PerformanceMonitorDarling\logs\darling-service_<yyyyMMdd>.log`. The service also writes to the Windows Event Log, source `PerformanceMonitor Darling` |
| Store (the bundled PostgreSQL) | `%ProgramData%\PerformanceMonitorDarling\pg`, unless `postgres.dataDirectory` names another folder |
| Store credential files | `pg-credential.dpapi`, `pg-admin-credential.dpapi`, `pg-viewer-credential.dpapi`, `pg-web-credential.dpapi` and `pg-mcp-credential.dpapi`, beside the store folder in `%ProgramData%\PerformanceMonitorDarling` |
| Password key | `password-key.dpapi`. See [Who can read `darling.json` and the credentials](security.md#security--least-privilege-roles) for who can open it |
| Log-hash key | `log-hash-key.dpapi`. A managed store on Windows keeps it beside `pg-credential.dpapi`. An own store (`postgres.managed` off) keeps it in a `darling-keys` folder beside `darling.json`. On Linux the file is `log-hash-key`, and under Docker Compose it is in the `darling-credentials` volume. The service uses the key to hash log text before it stores it. The key is kept outside the store, so someone who can read the store cannot test guesses against the hashes |
| Rollback backups | `_rollback_manual_<stamp>` folders in the install folder |
| Viewer log | `%APPDATA%\PerformanceMonitorDarling\logs\darling-viewer_<yyyyMMdd>.log` |
| Viewer preferences | A small JSON file in `%APPDATA%\PerformanceMonitorDarling` |

Back up `darling.json` and the password key file. Nothing else in the install folder has to travel with them. See [Install Darling](install.md) for the install procedure.

## How It Runs (Reference)

Fixed cadences, hardcoded on purpose:

| What | Cadence |
|---|---|
| Collector sweep loop | Every 15 seconds (each collector runs when its own shared schedule is due — most every 1 minute, some every 5, sizes hourly, index stats daily) |
| Alert evaluation | Every 30 seconds per connected server (Lite's overview cadence) |
| Scheduled analysis | Every 30 minutes per server, 120-second budget, analyzing the last 4 hours; findings persist to `analysis_findings` and high-severity ones notify through the configured channels |
| Retention purge | First sweep after startup, then daily |
| Store background-job health (policy-job self-heal across compression, rollup refresh and retention; job cadence; Retention Held), the TimescaleDB availability re-probe, the retention coverage re-evaluation and the store-object convergence pass | First sweep after startup, then hourly at :30 past the minute; the availability re-probe runs first and outside the TimescaleDB gate, so a service that fell back to plain-PostgreSQL mode at startup recovers on its own (#3815); a held retention policy arms itself on the first hourly pass after its rollups cover raw, no restart needed (#3812); every pass writes one `Store job health:` line naming what it read, per family (#3816); the store-object convergence pass runs last and re-ensures every store object the start path builds — hypertables, compression policies, continuous aggregates and their refresh windows, the baseline relations, the store's statement statistics, the composer's covering indexes — so one ensure that failed at startup heals within the hour instead of at the next restart (on a store without TimescaleDB, the steps that do not need it run every hour too, #3913), and writes one `Store object convergence:` line naming what it changed and what failed even when that is nothing (#3817) |
| Reconnect attempts | Every 60 seconds while a server is unreachable |
