# Install Darling on Linux with systemd

Audience: operators.

This page takes you from a downloaded tarball to a service that collects data. It is for a Linux host that runs systemd, with a PostgreSQL server that you run yourself as the store. Follow it from top to bottom. The Docker Compose shape is in the [Darling quick start](../darling-quick-start.md#run-on-linux-docker-compose-or-systemd-1804). The Helm chart is in [Run Darling on Kubernetes](kubernetes.md). The Windows install is in [Install Darling](install.md).

This page describes a host install under systemd. It does not describe the tarball inside a container. For a container, use the compose distribution in the [Darling quick start](../darling-quick-start.md#run-on-linux-docker-compose-or-systemd-1804). A container base image such as `mcr.microsoft.com/dotnet/aspnet` sets `DOTNET_RUNNING_IN_CONTAINER=true`, and with that set the service keeps its keys in `/var/lib/darling/credentials`, not in `darling-keys` beside `darling.json`.

The steps were run on Ubuntu 24.04 with PostgreSQL 18 and TimescaleDB 2.32 on the same host as the service. On another distribution the package names and the PostgreSQL paths differ, and the rest is the same.

Every step ends with a proof point. If a proof point does not appear, stop at that step. The next step will not fix it.

## What is different from Windows

- The bundled store is Windows only. On Linux `postgres.managed` is `false`, and you create the database and its login.
- There is no DPAPI, so `--encrypt-password` is Windows only. A secret in `darling.json` is a `file:` or `env:` reference.
- The scripts are `install-darling.sh` and `upgrade-darling.sh`, and the tarball has the unit, `darling.service`. See [Install and upgrade with the scripts](#install-and-upgrade-with-the-scripts).
- The Viewer is a Windows app. A Linux host serves the [web dashboard](web-dashboard.md), or a Windows Viewer connects to the store over the network.
- Every command on the other pages is written `.\PerformanceMonitor.Darling.Service.exe --verb`. The Linux form is in [Service verbs on Linux](#service-verbs-on-linux).

## Install and upgrade with the scripts

The tarball has `install-darling.sh`, `upgrade-darling.sh`, `darling.service` and `darling.linux.sample.json` in its root ([#9688](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9688)). The scripts do steps 5 to 8 of this page and the upgrade, with the same paths, and they check their own work. The numbered steps stay the reference for what the scripts do, and for a host where you do them by hand.

**Install.** Do steps 1 to 4 first. Then extract the tarball into a new folder that only root can write, and run its script. The folder is not `/opt/darling`: the script puts the build there.

```bash
sudo mkdir /opt/darling-new
sudo tar -xzf PerformanceMonitorDarling-linux-x64-<version>.tar.gz -C /opt/darling-new --no-same-owner --no-same-permissions
sudo /opt/darling-new/install-darling.sh --source PerformanceMonitorDarling-linux-x64-<version>.tar.gz --sha256 <hash>
```

- The first run creates the `darling` user and the folders of step 5, puts the build in `/opt/darling`, installs the unit, and creates `/etc/darling/darling.json` from `darling.linux.sample.json` with two empty secret files beside it. It does not start the service, and it exits with code `2`.
- Edit the three files as [step 6](#step-6-write-darlingjson-and-the-secret-files) says, then run the script again. The second run runs the pre-flight of step 7 as the `darling` user, enables and starts the unit, and runs the health check.
- A config that is already in `/etc/darling` is kept, and so is a unit that is already installed. `--replace-unit` installs the tarball's unit over yours and keeps yours as `darling.service.previous`.
- `--check` runs the checks and changes nothing. `--source <tarball>` installs the build from the tarball, not from the folder the script runs from. The script copies the tarball first and checks the copy that it installs, against `--sha256`, or without it against the `SHA256SUMS-linux.txt` beside the tarball. Run the script again with the same arguments.
- A `SHA256SUMS-linux.txt` beside the tarball shows that the tarball is whole. It does not show where the tarball came from, because whoever can replace the tarball can replace the file beside it. `--sha256 <hash>`, with a hash that you took from the release page yourself, is the check that does. Both arguments are for a tarball: with a folder as the source the scripts refuse `--sha256` and `--skip-hash-check`. Without `--source` the source is the folder the script runs from, so that form has no hash check at all, and the blocks on this page pass `--source`.
- The script that runs is the one that you extracted, and it checks the hash only after it starts. Check the tarball first, with the same hash, before you extract it: `echo "<hash>  <tarball>" | sha256sum -c -` prints `<tarball>: OK`.
- The scripts run as root, so they refuse to run from a folder that root does not own or that its group or other users can write. The same goes for every folder above it up to `/`, for the script, for `darling-script-lib.sh` and for the `darling.service` and `darling.linux.sample.json` beside it. `/opt` and `/etc/systemd/system`, where the scripts put the build and the unit, must pass the same check with every folder above them. They refuse a `--source` folder like that too, and one that holds a file or a folder like that. The commands above make a folder that passes: `/opt` is root's on a stock host, and a folder in your home directory is not. `--no-same-permissions` lets root's umask set the modes, so a tarball that was built with group-writable files still extracts closed to group writes.
- The commands the scripts run as the `darling` user, such as the build's `--version` and `--test-connection`, get `PATH` and the `Environment=` lines of the installed unit and its drop-ins, and nothing from root's shell. A variable the build needs to start, such as `DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=1` on a host without ICU, belongs in the unit or in a drop-in. Before a first install there is no unit, so on such a host write the unit by hand first ([step 8](#step-8-write-the-unit-and-start-the-service)): the install keeps a unit that is already there. The lines of an `EnvironmentFile=` are not read.
- The service user gets only the folders it writes. `/usr/share/PerformanceMonitorDarling` stays root's, and the script takes it back when an older install gave it to `darling`. It refuses, and changes no owner, when `logs` or `darling-keys` is a symbolic link.

**Upgrade.** Read the release notes and back up the store first, as [Upgrade by hand](#upgrade-by-hand) says. The script does not read or change the store's PostgreSQL settings, so the compare of the store's settings file with the [Settings](#settings) block stays with you: it is step 3 of [Upgrade by hand](#upgrade-by-hand). Then extract the new tarball into a new folder and run the upgrade script that came out of it:

```bash
sudo rm -rf /opt/darling-new && sudo mkdir /opt/darling-new
sudo tar -xzf PerformanceMonitorDarling-linux-x64-<new-version>.tar.gz -C /opt/darling-new --no-same-owner --no-same-permissions
sudo /opt/darling-new/upgrade-darling.sh --check --source PerformanceMonitorDarling-linux-x64-<new-version>.tar.gz --sha256 <hash>
sudo /opt/darling-new/upgrade-darling.sh --source PerformanceMonitorDarling-linux-x64-<new-version>.tar.gz --sha256 <hash>
```

- It refuses, and changes nothing, while a process other than the service runs from `/opt/darling`, such as a `--backfill-rollups` that has not finished. That holds for any process under `/opt/darling/`, whichever account started it, and the message names its PID.
- It stops the service and checks that no process is left that runs from `/opt/darling`.
- It moves `/opt/darling` to `/opt/darling.previous`, the rollback copy, and moves the new build into `/opt/darling`. The rollback copy is a copy on new files that root owns, not the old files renamed, so an account that owned the old files or holds one open cannot change it. Before the copy, root takes every file of the installed build (owner root, no group or other write), and a link in it that points outside the folder stops the upgrade with nothing replaced. The new rollback copy is made before the one of an earlier upgrade is removed, so a copy that fails leaves that one in place, and for that moment the disk holds four builds. `--rollback` puts back a copy made the same way. `/etc/darling` is not touched, and the script checks that the config, the secret files and the keys are as they were. When that check cannot run, the script says so and does not say that they are as they were.
- It starts the service and runs the health check. When the start or the check fails, it puts the old build back, starts it, and exits with code `1`. The build that failed stays in `/opt/darling.failed`. Nothing uses that folder, and a later upgrade that succeeds reminds you to remove it. When the rollback cannot move a build, the script does not say "rolled back": it says that the rollback did not finish and why, and it leaves the service stopped with the old build still in `/opt/darling.previous`. When it cannot put back a unit that `--replace-unit` replaced, it says that too, leaves the service stopped with the old build in `/opt/darling`, and names the commands that finish the rollback.
- It warns, and changes nothing, when `/usr/share/PerformanceMonitorDarling` is not root's. An install by hand from an older version of this page gave that folder to `darling`. The warning names the folder, its owner and mode, and the fix: `sudo chown root:root /usr/share/PerformanceMonitorDarling && sudo chmod 755 /usr/share/PerformanceMonitorDarling`. `install-darling.sh` repairs the folder. The upgrade only names it, because a `chown` by root on a folder that the `darling` user owns follows whatever link that account puts there. `--check` prints the same warning.
- An upgrade to the version that is already installed says so and goes on.
- Each run that gets past the pre-flight leaves `/opt/darling/upgrade-run.json`, the record that `upgrade-darling.ps1` writes on Windows ([#9432](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9432)). The service reads it at its start and every 5 minutes and stores it in `collect.upgrade_runs`: `in_progress` when the new build starts, then `succeeded`, or `failed` with the first error. `--rollback` and `--check` write none. The service cannot remove a file from `/opt/darling`, which only root can write, so the record stays until the next upgrade replaces the folder. For a `failed` run, and for a run that started more than 7 days ago, the service logs one warning after each start that it could not remove the file, and the store keeps one row for the run.
- `--rollback` goes back to `/opt/darling.previous` by hand. It runs from the extracted copy, as the next bullet says. It uses up the rollback copy, as the automatic rollback after a failed start does. The build that was in `/opt/darling.previous` is then in `/opt/darling`, `/opt/darling.previous` is gone, and the build it replaced is in `/opt/darling.failed`, where an earlier build of that name is removed first. A second `--rollback` stops with nothing to go back to, and going forward again is a new upgrade from a tarball. The store is not rolled back: read [Rolling back to an older build](admin.md#rolling-back-to-an-older-build) first.
- `/opt/darling-new` is the folder that you extracted the new tarball into. The upgrade puts the new build in `/opt/darling` (from the tarball with `--source`, from the folder the script runs from without it) and leaves `/opt/darling-new` where it is. `--rollback` runs from that copy, `sudo /opt/darling-new/upgrade-darling.sh --rollback`, because the scripts refuse to run from `/opt/darling`. Remove the folder when you no longer want that rollback. The first line of the Upgrade block removes it and makes it again before the next extract, so an old copy never mixes with the new files.
- A unit that you wrote by hand is kept. Each upgrade warns about it twice: the unit is not the tarball's, and it sets no `DARLING_STOPPED_MARKER`. The unit of [step 8](#step-8-write-the-unit-and-start-the-service) has no sandbox lines and no stopped marker. With it the service runs without the unit's sandbox (described above). The health check then sees only whether the process stays up, and not whether a startup failure stopped collection. `--replace-unit` switches to the tarball's unit, which has both, and keeps yours as `darling.service.previous`. When that upgrade fails, the rollback puts yours back. `--rollback` leaves the unit as it is. `--check` shows both warnings, so you see them before the run.

**The health check.** With the web dashboard off there is no port to ask, so the scripts watch what systemd and the service give. For 30 seconds after the start (`--health-seconds` changes it, to 99999 at most), the unit must stay `active`, its main process must stay the same one, and the file `/run/darling/stopped` must not exist. The service keeps that file present while a startup failure has stopped collection for good, such as a `darling.json` that it rejects. Its process stays up in that state, so `active (running)` alone does not show it. The tarball's unit names the file in `DARLING_STOPPED_MARKER`. With a unit that does not, the scripts see only the first two signs and say so. A healthy start is not proof of collection: [step 9](#step-9-proof-that-it-collects) is.

**The unit's sandbox.** The tarball's `darling.service` is the unit of step 8 with `RestartSec=10`, `ProtectSystem=strict`, `NoNewPrivileges=yes`, `PrivateTmp=yes`, `PrivateDevices=yes`, `ProtectHome=yes`, `ProtectKernelTunables=yes`, `RestrictSUIDSGID=yes` and `LockPersonality=yes`. The folders the service can write that hold anything are `/etc/darling/darling-keys`, `/usr/share/PerformanceMonitorDarling/logs` and `/run/darling`. It can also write its own private `/tmp` and `/var/tmp`, and the folders that every account on the host can write and that these settings do not close: `/dev/shm`, `/dev/mqueue`, and a folder under `/run` that is open to all, such as `/run/lock` on Ubuntu. It sees no device, and `/home`, `/root` and `/run/user` are empty for it. If `darling.json` names another path for the service to write, pick one outside those three and add it with `sudo systemctl edit darling`:

```ini
[Service]
ReadWritePaths=/your/folder
```

## Step 1: Prerequisites

- A Linux x64 host with systemd, and `sudo` on it.
- PostgreSQL for the store. The minimum is PostgreSQL 16, and the service is developed and validated against PostgreSQL 18. TimescaleDB is optional. With it the service converts the collector tables to hypertables with compression. Without it the service runs in plain PostgreSQL mode. This page sets up PostgreSQL 18 with TimescaleDB.
- A store host sized for your estate. The [store sizing guide](../darling-store-sizing.md) gives the disk, RAM and vCPUs by server count.
- A monitored SQL Server and a login on it with the [monitoring grants](monitored-servers.md#permissions-on-monitored-servers). On Linux the login is a SQL login (`"auth": "sql"`).

## Step 2: Install the .NET runtime

The tarball is framework-dependent. The host needs the .NET 10 runtime and the ASP.NET Core 10 runtime. Ubuntu 24.04 has both in its own repositories, and they install `dotnet` at `/usr/bin/dotnet`, which is the path the unit uses. `Microsoft.Data.SqlClient` also needs `libgssapi-krb5-2`.

```bash
sudo apt-get update
sudo apt-get install -y dotnet-runtime-10.0 aspnetcore-runtime-10.0 libgssapi-krb5-2
dotnet --list-runtimes
```

On a distribution without these packages, take both runtimes from <https://dotnet.microsoft.com/download/dotnet/10.0>, and change the `dotnet` path in the unit if it is not `/usr/bin/dotnet`.

**Proof:** the list has a `Microsoft.AspNetCore.App 10.` line and a `Microsoft.NETCore.App 10.` line.

## Step 3: Set up the store

Do this step on the PostgreSQL host, as a user who can run `sudo -u postgres`.

### Install PostgreSQL 18 and TimescaleDB

Ubuntu's own repositories do not carry PostgreSQL 18 or TimescaleDB. PostgreSQL 18 comes from the PostgreSQL project's apt repository (PGDG), and TimescaleDB comes from the Timescale apt repository. The two projects keep the current setup commands on their own pages: [PostgreSQL on Ubuntu](https://www.postgresql.org/download/linux/ubuntu/) and [Install TimescaleDB on Linux](https://docs.timescale.com/self-hosted/latest/install/installation-linux/). At the time of writing they are:

```bash
sudo apt-get install -y postgresql-common
sudo /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh
echo "deb https://packagecloud.io/timescale/timescaledb/ubuntu/ $(lsb_release -c -s) main" | sudo tee /etc/apt/sources.list.d/timescaledb.list
wget --quiet -O - https://packagecloud.io/timescale/timescaledb/gpgkey | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/timescaledb.gpg
sudo apt-get update
sudo apt-get install -y postgresql-18 timescaledb-2-postgresql-18
```

**Proof:** `sudo -u postgres psql -c "SELECT version()"` answers with PostgreSQL 18.

### Settings

Put the settings in one file of your own, so a package upgrade does not overwrite them. On a Debian or Ubuntu package install that file goes in `/etc/postgresql/18/main/conf.d/`, for example `/etc/postgresql/18/main/conf.d/darling.conf`.

```ini
shared_preload_libraries = 'timescaledb,pg_stat_statements'
listen_addresses = 'localhost'
timezone = 'UTC'
random_page_cost = 1.1
timescaledb.max_background_workers = 128
max_worker_processes = 139
timescaledb.telemetry_level = off
pg_stat_statements.track_utility = off
```

Restart PostgreSQL, because the preload list and the worker settings are read only at a start:

```bash
sudo systemctl restart postgresql@18-main
```

This table says what each setting is for, and what the service does or logs without it. The managed Windows store sets all of them for itself. The service changes no server setting on a store that you bring.

| Setting | Value | Why |
|---|---|---|
| `shared_preload_libraries` | `'timescaledb,pg_stat_statements'` | TimescaleDB must be preloaded before its extension can be created. `pg_stat_statements` feeds the store's own statement statistics (`get_store_query_stats`). |
| `listen_addresses` | `'localhost'` | Enough when the service runs on the store host. Name another address only when the service or a Viewer connects from another machine, and then read [Opt-in Network Endpoints](network-endpoints.md). |
| `timezone` | `'UTC'` | The service pins its own connections to UTC. TimescaleDB's own jobs follow the server's time zone, so a window measured in days starts and ends off UTC midnight on any other zone. |
| `random_page_cost` | `1.1` | For SSD storage. With the default of 4 the planner can pick a sequential scan over a BRIN index for windows of 12 hours or more. |
| `timescaledb.max_background_workers` and `max_worker_processes` | `128` and `139` | The stock `max_worker_processes = 8` cannot run the compression, retention and refresh policies. This pair is a safe start for a new install. Re-size it after the first start, as [Size the workers after the first start](#size-the-workers-after-the-first-start) says. |
| `timescaledb.telemetry_level` | `off` | Stops TimescaleDB's own telemetry job. The managed store sets it. |
| `pg_stat_statements.track_utility` | `off` | With it on, utility statements are recorded with their literals, a password in a role statement among them. The service warns about it. |
| `timescaledb.enable_job_execution_logging` | `on` (optional) | Records successful policy runs in `timescaledb_information.job_history`. Without it only failed runs are recorded. |
| `logging_collector` and `log_directory` | leave as packaged | See [Store log capture](#store-log-capture). |

After the service starts, `--check-settings` prints the store host profile and a verdict for each sizing setting. See [Check Host/Store Sizing Settings](install.md#check-hoststore-sizing-settings).

### The owner role and the database

The service connects as one login that owns the store database. It does not need to be a superuser. `createuser` asks for the new role's password twice and does not echo it. Use a long random password, and keep it for step 6.

```bash
sudo -u postgres createuser --pwprompt darling
sudo -u postgres createdb --owner=darling --template=template0 --encoding=UTF8 --locale=C darling
```

The database must use the C collation, the same as the managed Windows store and the compose store. `--template=template0` is required with `--locale=C`, because a database made from the default template keeps the template's locale and the command fails. The [Darling Exporter](../../Exporter/README.md#the-store-collation) refuses a store with any other collation.

### Extensions and rights that need a superuser

The owner role cannot do these itself. Run them once as `postgres`, in the `darling` database, before the first start:

```bash
sudo -u postgres psql -X -d darling
```

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements SCHEMA public;
GRANT pg_monitor TO darling;
REVOKE SELECT ON public.pg_stat_statements FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.pg_stat_statements(boolean) FROM PUBLIC;
GRANT SELECT ON public.pg_stat_statements TO pg_read_all_stats, darling;
GRANT EXECUTE ON FUNCTION public.pg_stat_statements(boolean) TO pg_read_all_stats, darling;
REVOKE SELECT ON public.pg_stat_statements_info FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.pg_stat_statements_info() FROM PUBLIC;
GRANT SELECT ON public.pg_stat_statements_info TO pg_read_all_stats, darling;
GRANT EXECUTE ON FUNCTION public.pg_stat_statements_info() TO pg_read_all_stats, darling;
```

What each one is for:

- **`timescaledb`.** The service can create this extension itself, and a superuser can create it first. The owner role builds every continuous aggregate and its indexes with no grant on TimescaleDB's `_timescaledb_internal` schema.
- **`pg_stat_statements`.** This extension is not trusted, so only a superuser can create it. The service does not try, and it logs nothing while the extension is missing. Name `SCHEMA public`. After its first start the service sets the database `search_path` to `collect, config, public`, so a later `CREATE EXTENSION pg_stat_statements` without a schema lands in `collect`, and the service then warns that `PUBLIC can still read collect.pg_stat_statements`. The statements in [Security](security.md#security--least-privilege-roles) are written for `public`.
- **`pg_monitor`.** Without it the service logs `The store's role cannot read pg_wal's size (pg_ls_waldir)` and records the WAL figures without that size. The [store log capture](#store-log-capture) needs it too.
- **The `pg_stat_statements` revokes and grants.** They stop every login in the database from reading the statement statistics of every role. Without them the service warns `PUBLIC can still read public.pg_stat_statements` and names these statements. Only the owner of the extension or a superuser can run them.

A superuser login for the service also clears all of this, and [Security](security.md#security--least-privilege-roles) describes the store that way. The grants above are the smaller choice. When the owner role lacks a right the service uses, the service logs one warning at each start that begins `The store login "darling" is not a superuser and lacks privileges the service uses`. It says what each missing right costs and gives the statements that grant it, to run as a superuser in the `darling` database.

**Proof:** `sudo -u postgres psql -X -d darling -c "\dx"` lists `timescaledb` and `pg_stat_statements`, and `SHOW shared_preload_libraries` returns both names.

### The least-privilege roles, when the web dashboard or MCP is on

The tarball ships `provision-roles.sql`. It creates the `admin`, `viewer`, `web` and `mcp` logins on a store that you bring. You can skip it while the web dashboard and MCP are both off, which is the default. With either one on and no `postgres.webConnectionString` or `postgres.mcpConnectionString` set, that surface connects as the owner and the service warns about it. When to run the script, its placeholders and what it grants are in [Security](security.md#security--least-privilege-roles).

Custom alerts run on the `web` role, so they need the script and `postgres.webConnectionString`. With the setting unset, the service logs `Custom alert evaluator not started` and names the setting ([#9700](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9700)). With it set, the service signs in with that login once at start. If the store refuses it, the service logs a warning that names the setting, the login and the store's error, and the evaluator stays down until the next start ([#9760](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9760)). The built-in alerts are not affected.

Run the script on the store host as the `postgres` OS user. The `darling` owner from the step above is not a superuser and has no `CREATEROLE`, so the script stops at its guard (`is not a superuser and does not have CREATEROLE`) and changes nothing. Work on a copy, so that the four passwords never sit under `/opt/darling`, where the next [upgrade by hand](#upgrade-by-hand) moves them to `/opt/darling.previous`. The run needs all four placeholders replaced, `web` and `mcp` included, even when only the custom alert evaluator's `web` login is used. Make the copy in a folder that only you can read:

```bash
work=$(mktemp -d)
install -m 600 /opt/darling/provision-roles.sql "$work/provision-roles.sql"
```

Open the copy in an editor and replace every occurrence of `CHANGE_ME_ADMIN_PASSWORD`, `CHANGE_ME_VIEWER_PASSWORD`, `CHANGE_ME_WEB_PASSWORD` and `CHANGE_ME_MCP_PASSWORD` with a long random password. Each one sits in a check and in its `ALTER ROLE` line, so use a find-and-replace. Keep the `web` and `mcp` passwords in the files that `postgres.webConnectionString` and `postgres.mcpConnectionString` read as `file:` references, such as `file:/etc/darling/store_web` in the [Security example](security.md#security--least-privilege-roles). Then run the copy and delete it. The copy goes in on standard input, because the `postgres` user cannot read a file that only you can read. `-q` leaves out the command tags (`DO`, `SET`, `GRANT`), so the run prints only warnings and errors, and each line is worth reading.

```bash
sudo -u postgres psql -X -q -d darling -f - < "$work/provision-roles.sql"
rm -rf "$work"
```

**Proof:** `sudo -u postgres psql -X -d darling -At -c "SELECT rolname FROM pg_roles WHERE rolname IN ('admin', 'viewer', 'web', 'mcp') ORDER BY rolname"` lists all four. Then set `postgres.webConnectionString` in `darling.json`, and `postgres.mcpConnectionString` when MCP is on.

### Store log capture

The service reads the store's own PostgreSQL log into the store. A Debian or Ubuntu package logs to `/var/log/postgresql` with `logging_collector = off`, which is a healthy setup, and the service cannot read that. It logs one of these two warnings once, and repeats the same cause only at Debug until the cause changes:

- `Store log capture failed ... The store keeps no log files to read (logging_collector is off or the log folder is missing)`
- `Store log capture failed ... Reading the store's own log needs pg_monitor and EXECUTE on pg_read_binary_file`

Both are safe to leave. Collection from your monitored servers is not affected, and PostgreSQL's log is still in `/var/log/postgresql`. Capture needs `logging_collector = on`, a `log_directory` that is absolute or inside the data directory (the default, `log`, is inside), `pg_monitor` for the owner role (granted above), and this grant, run as `postgres` in the `darling` database:

```sql
GRANT EXECUTE ON FUNCTION pg_catalog.pg_read_binary_file(text, bigint, bigint) TO darling;
```

That lets the role read any file in the data directory and in an absolute `log_directory`, so grant it only when you want the capture. `pg_read_server_files` is not needed. A relative `log_directory` that leads out of the data directory, such as `../pglog`, is the one place PostgreSQL refuses the read without that role, and the warning then says to set an absolute path.

## Step 4: Download and verify

Each release on the [release page](https://github.com/erikdarlingdata/PerformanceMonitor/releases) has `PerformanceMonitorDarling-linux-x64-<version>.tar.gz` and `SHA256SUMS-linux.txt`. Download both into a new, empty folder for this release. Every release names its checksum file `SHA256SUMS-linux.txt`, so a download into the folder of an earlier release replaces that release's file, and its archive can no longer be checked. The checksum file lists the compose archive too, so tell `sha256sum` to skip the file that you did not download:

```bash
sha256sum -c --ignore-missing SHA256SUMS-linux.txt
```

**Proof:** `PerformanceMonitorDarling-linux-x64-<version>.tar.gz: OK`. Without `--ignore-missing` the command fails on the compose archive's line.

## Step 5: Extract, and create the user and folders

The tarball has no top-level folder, so make the folder first. Its files are owned by root ([#9701](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9701)). `--no-same-owner` keeps root the owner of everything that is extracted, whatever an archive records, and `--no-same-permissions` lets root's umask set the modes, so nothing extracts group-writable.

```bash
sudo mkdir -p /opt/darling
sudo tar -xzf PerformanceMonitorDarling-linux-x64-<version>.tar.gz -C /opt/darling --no-same-owner --no-same-permissions
```

Create the service user and the folders it writes. The unit runs as that user, and it cannot create folders under `/etc` or `/usr/share` itself.

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin darling
sudo mkdir -p /etc/darling/darling-keys
sudo mkdir -p /usr/share/PerformanceMonitorDarling/logs
sudo chown root:darling /etc/darling
sudo chmod 750 /etc/darling
sudo chown darling:darling /etc/darling/darling-keys
sudo chmod 700 /etc/darling/darling-keys
sudo chown darling:darling /usr/share/PerformanceMonitorDarling/logs
sudo chmod 700 /usr/share/PerformanceMonitorDarling/logs
```

Only `logs` goes to `darling`. Its parent, `/usr/share/PerformanceMonitorDarling`, stays root's, like `/etc/darling`: an account that owns a parent folder can swap the folder inside it for a link, and a later `chown` by root then lands on whatever the link names.

| Path | Owner and mode | What it holds |
|---|---|---|
| `/opt/darling` | root, as extracted | The service files. The service never writes here. |
| `/etc/darling` | `root:darling`, `750` | `darling.json` and the secret files. Other users on the host cannot list it. |
| `/etc/darling/darling-keys` | `darling:darling`, `700` | The `password-key` and `log-hash-key` files. The service makes them on its first start, mode `600`. It checks that the folder is owner-only. Back this folder up (see [Security](security.md)). |
| `/usr/share/PerformanceMonitorDarling/logs` | `darling:darling`, `700` | The file log. Without this folder the service still runs, writes no log file, and says `File logging is disabled` at start. |

`darling-keys` sits beside `darling.json`. If your config is not in `/etc/darling`, create the folder next to it.

**Proof:** `/opt/darling/PerformanceMonitor.Darling.Service --version` prints the version that you downloaded.

## Step 6: Write `darling.json` and the secret files

The `darling.sample.json` in the tarball is shaped for Windows: it starts with `"managed": true` and uses `encryptedPassword`. Do not copy it on Linux ([#9701](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9701)). Write a file like the one below, or copy `darling.linux.sample.json` from the tarball, which is the same file with comments.

Create the three files empty, with the right owner and mode, before anything secret goes in them:

```bash
sudo install -o darling -g darling -m 600 /dev/null /etc/darling/darling.json
sudo install -o darling -g darling -m 600 /dev/null /etc/darling/store_connection
sudo install -o darling -g darling -m 600 /dev/null /etc/darling/sql_password
```

Then edit each one as root, for example `sudo nano /etc/darling/darling.json`. Do not put a password on a command line, where the shell history keeps it.

`/etc/darling/darling.json`:

```json
{
  "postgres": {
    "managed": false,
    "connectionString": "file:/etc/darling/store_connection"
  },
  "servers": [
    {
      "host": "<sql-server>,1433",
      "name": "<display-name>",
      "auth": "sql",
      "username": "<monitoring-login>",
      "password": "file:/etc/darling/sql_password"
    }
  ],
  "web": { "enabled": false },
  "mcp": { "enabled": false }
}
```

`/etc/darling/store_connection` holds one line, the Npgsql connection string for the owner role from step 3:

```
Host=127.0.0.1;Port=5432;Username=darling;Password=<store-password>;Database=darling
```

`/etc/darling/sql_password` holds one line, the password of the monitoring login. The service trims the end of a `file:` secret, so a final newline does no harm.

Notes on the file:

- **Nothing secret goes in `darling.json`.** Every secret slot takes a `file:/path` or an `env:NAME` reference. A literal `password` works, and the service warns about it on every use. systemd's `LoadCredential=` pairs with `file:`.
- **Keep the secret files in `/etc/darling`, not in `darling-keys`.** The key folder is for the service's own keys.
- **`servers` is only a seed.** The service copies the list into the store on its first start, and later edits to the list in the file do nothing. Get each entry right before the first start. See [Changing a registered server](#changing-a-registered-server) and [`darling.json` is only a seed](install.md#darlingjson-is-only-a-seed).
- **A PostgreSQL target** sets `"engine": "postgres"` on its entry. See [PostgreSQL Targets](postgresql-targets.md#postgresql-targets).
- The file is JSONC, so `//` comments are allowed. Every field is in the [Configuration Reference](configuration.md).

**Proof:** `sudo ls -l /etc/darling` shows the three files as `-rw-------` and owned by `darling`.

## Step 7: Pre-flight

```bash
sudo runuser -u darling -- env DARLING_CONFIG=/etc/darling/darling.json dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll --test-connection
```

This validates the file and then connects to every configured server, as the service's own user. It exits with code `0` only when the config is valid and every server is reachable.

**Proof:**

```
Validating connectivity to <count> server(s)...
  [PASS] <display-name>: SQL major version <major>, <edition>, msdb access: yes
All servers reachable.
```

Two things to expect here:

- **A note about the registry on a new store.** Before the first start the store has no tables, so the command prints `NOTE: the store has no server registry yet, so validating darling.json's own server list instead.` That is expected on a store that has never been started ([#9695](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9695)).
- **A certificate failure names the certificate.** An untrusted certificate fails with `[FAIL] <display-name>: A connection was successfully established with the server, but then an error occurred during the pre-login handshake. (provider: TCP Provider, error: 35 - An internal exception was caught) The server's certificate chain is not trusted by this machine, so add the issuing CA to the system trust store, or set trustServerCertificate for this server knowing that it skips the check.` The SQL Server's certificate chains to a CA that is not in the Linux trust store. Amazon RDS for SQL Server is one such case. When the certificate was not issued to the name in `host`, the last sentence says that instead, or as well. The secure fix is to add the CA: put each CA certificate as its own `.crt` file under `/usr/local/share/ca-certificates/`, run `sudo update-ca-certificates`, and run the pre-flight again. The other fix is `"trustServerCertificate": true` on the server's entry, which keeps the connection encrypted and accepts any certificate. Choose before the first start, because `servers` is only a seed. For a server that is already registered, editing `darling.json` and restarting the service changes nothing, so use one of two ways out. Add the CA to the trust store as described. Or run `--edit-server sql01 --set trustServerCertificate=true`, or `--set encryptMode=<mode>`, in the form shown under [Changing a registered server](#changing-a-registered-server). The verb works while the service runs and needs no restart, and a server with a SQL login asks for its password again.

## Step 8: Write the unit and start the service

Save this as `/etc/systemd/system/darling.service`. The tarball has a `darling.service` in its root. It is the same unit, plus the sandbox that "The unit's sandbox" in the scripts section describes and a `DARLING_STOPPED_MARKER` line that the scripts' health check reads. Use it when you can: `sudo install -m 644 <folder you extracted to>/darling.service /etc/systemd/system/darling.service`. The unit below is the smaller one. A host that runs it gets a warning about the unit, and about the missing stopped marker, at each script upgrade.

```ini
[Unit]
Description=PerformanceMonitor Darling
After=network-online.target postgresql.service
Wants=network-online.target

[Service]
Type=notify
ExecStart=/usr/bin/dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll
Environment=DARLING_CONFIG=/etc/darling/darling.json
User=darling
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now darling
systemctl status darling
```

**Proof:** `Active: active (running)`. This proves only that the process started. Step 9 proves that it collects.

Know these limits of the unit ([#9702](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9702)):

- **The service tells systemd it is ready when its host starts, before the store is ready.** With `Type=notify` the service sends the ready signal itself, and it sends it at host start. `active (running)` is true while the service is still migrating the store or retrying its connection. The real readiness signal is the `Postgres store ready` line, followed by `PerformanceMonitor Darling collection loop started`.
- **`Restart=on-failure` restarts a process that exits.** It does nothing for a service that runs and has stopped collecting. Watch for that with the store check in step 9.
- **`After=postgresql.service` orders the unit after a local PostgreSQL only under that unit name.** That is the name on Debian and Ubuntu. Another name, such as `postgresql-16.service` from the PostgreSQL packages for RHEL, goes on the `After=` line instead. With the store on another host the line does nothing. In every case the service retries the store until PostgreSQL answers.

## Step 9: Proof that it collects

Read the file log, not the journal:

```bash
sudo tail -n 60 -f /usr/share/PerformanceMonitorDarling/logs/darling-service_$(date +%Y%m%d).log
```

**Proof:** these lines, at the default log level:

```
Loaded configuration from /etc/darling/darling.json: <count> server(s)
Postgres store ready (schema v<version>, <count> migration(s) applied)
TimescaleDB detected ...
TimescaleDB: <n>/<n> collector table(s) are hypertables
TimescaleDB: <n>/<n> continuous aggregate(s) ready (...)
PerformanceMonitor Darling collection loop started
[<display-name>] Connected (major <major>, engine <engine type>, edition <edition>, server_id <id>)
```

`Postgres store ready` means the store is migrated. `PerformanceMonitor Darling collection loop started` means ready. One `Connected` line for each server means the service reached it. On the hypertable line and on the continuous aggregate line, both numbers must be equal.

The lines that the Windows install page lists for the managed store (`Extracting the bundled Postgres runtime`, `Managed Postgres started`, `Creating the 'darling' database`, `Least-privilege roles ready`) never appear on a store that you bring. The per-collector `=> <count> rows (sql:<ms>ms, pg:<ms>ms)` lines are written at `Debug`, which the default level does not write. See [Logs](operations.md#logs) to turn them on.

With no more servers than `max_concurrent_sweeps` (4 by default), and no more than 8, the most connects that run at once, every server starts its first connect when the loop starts. A larger fleet spreads its first connects over 150 seconds, so that a restart does not start them all at once, and each server that waits says so once with `[<display-name>] first connect in about <n>s`. For a SQL Server target the `Connected` line names the engine type (`Box`, `AzureSqlDb` or `ManagedInstance`) and the edition in the words the pre-flight prints, such as `engine Box, edition Enterprise`. For a PostgreSQL target it names the major version and the engine, and no edition.

With the web dashboard and MCP off, the service opens no listening socket, so there is no port to probe. The proof that collection works is in the store. This counts the collector runs of the last 15 minutes for each server:

```bash
sudo -u postgres psql -X -d darling -c "SELECT server_id, status, count(*) FROM collect.collection_log WHERE collection_time > (now() AT TIME ZONE 'UTC') - interval '15 minutes' GROUP BY server_id, status ORDER BY server_id, status"
```

**Proof:** `SUCCESS` rows for every server. For a check that your own monitoring can run, use the one-row query in [Watching the service from outside](operations.md#watching-the-service-from-outside). It returns `true` while collection is current. The `heartbeat` setting on the same page pings a URL of yours instead.

### What a new install logs that is not a fault

These lines appear on a healthy new install ([#9695](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9695)):

- `Retention policy for <table> HELD PAUSED` or `RE-HELD`, for the query statistics tables and their rollups. The rollups cannot cover a day that has not closed, so a new store carries these holds for at least its first day. A held policy's purge is stopped, but while the store's oldest row is newer than the policy's horizon there is nothing to purge yet. The line says so and names no remedy: `HELD PAUSED` at `Information` when the service starts and at `Debug` on the hourly pass, `RE-HELD` at `Information`. Once the oldest row is past the horizon, a hold can cost history, and the line is a `WARN` that names the remedies, among them `--backfill-rollups`. `HELD PAUSED` then warns at each start and on the first hourly pass after the crossing.
- `Store log capture failed`, a `WARN` once per service start and then at `Debug` while the cause stays the same. See [Store log capture](#store-log-capture).
- `Rollup coverage floor: <n> rollup(s) hold no rows yet`, one `Information` line at start. Each of those rollups writes its own floor line at `Debug`.

### Size the workers after the first start

The worker formula in [Background workers](operations.md#background-workers-sizing-an-unmanaged-store-and-what-happens-if-you-dont) needs the store's hypertable and continuous aggregate counts, and they exist only after the first start creates the tables. That is why step 3 starts with `128` and `139`. Now read the real counts:

```bash
sudo -u postgres psql -X -d darling -c "SELECT count(*) AS hypertables FROM timescaledb_information.hypertables" -c "SELECT count(*) AS continuous_aggregates FROM timescaledb_information.continuous_aggregates"
```

Work the formula with those two numbers and the host's RAM. For host RAM, use the memory that the host always has. On a virtual machine with dynamic memory that is its minimum, not what it holds at the moment. If the result differs from the pair in `darling.conf`, change it, restart PostgreSQL, and then restart the service. Do this again after each upgrade, because a new build can add tables.

## Logs

- **The file log is the one to read.** It is `/usr/share/PerformanceMonitorDarling/logs/darling-service_yyyyMMdd.log`, one file for each day, kept 14 days, one line for each event. The date in the name is the host's local date, so a command that builds the name uses `date +%Y%m%d`, not `date -u`.
- **The journal is the noisy one.** `sudo journalctl -u darling` shows one line for each event, as `source[event id] message`, and the log level goes to the journal priority. To list only warnings and errors, run `sudo journalctl -u darling -p warning`. Without `sudo`, an account that is in neither the `adm` nor the `systemd-journal` group sees `-- No entries --` and a hint, which is the same output a clean service gives. Use the journal for what systemd itself reports: starts, stops and a process that exits.
- **The log files are mode `644` inside a `700` folder.** The service creates each file with the process umask. The folder's mode is what keeps other users out, so this is harmless. To make the files owner-only as well, add `UMask=0077` to the unit's `[Service]` section and restart the service.
- **The log level** is set with a standard .NET logging source. In the unit, that is a line such as `Environment=Logging__LogLevel__PerformanceMonitor.Darling.Service=Debug`. See [Logs](operations.md#logs).
- There is no Windows event log on Linux. The file log and the journal are the two surfaces.

## Service verbs on Linux

Run a verb as the service's user, with `DARLING_CONFIG` set, so that it reads the same config and can open the same keys and secret files:

```bash
sudo runuser -u darling -- env DARLING_CONFIG=/etc/darling/darling.json dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll <verb>
```

The apphost, `/opt/darling/PerformanceMonitor.Darling.Service <verb>`, is the same program and finds `dotnet` by itself. A message from the service that tells you to run a verb names it in this form, with the folder the service runs from. An example is `dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll --backfill-rollups` ([#9703](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9703)). The `--help` text names the same command, so on Linux you read the usage lines as printed ([#9751](https://github.com/erikdarlingdata/PerformanceMonitor/issues/9751)). An MCP answer names the verb without the folder, as in `dotnet PerformanceMonitor.Darling.Service.dll --backfill-rollups`. Run it from `/opt/darling`, or put the folder before the `.dll` name.

In this table `darling-verb` stands for the whole command above. The [command line reference](cli.md) has every verb with its exit codes.

| On the Windows pages | On Linux |
|---|---|
| `.\PerformanceMonitor.Darling.Service.exe --version` | `darling-verb --version` |
| `.\PerformanceMonitor.Darling.Service.exe --test-connection` or `--validate-config` | `darling-verb --test-connection` |
| `.\PerformanceMonitor.Darling.Service.exe --check-settings` | `darling-verb --check-settings` |
| `.\PerformanceMonitor.Darling.Service.exe --add-server` | `darling-verb --add-server`, with the arguments in [Add-server](cli.md#--add-server) |
| `.\PerformanceMonitor.Darling.Service.exe --edit-server` | `darling-verb --edit-server`, with the arguments in [Edit-server](cli.md#--edit-server) |
| `.\PerformanceMonitor.Darling.Service.exe --grant-script` | `darling-verb --grant-script` |
| `--enable-collector`, `--disable-collector`, `--set-collector-run-at`, `--set-collector-frequency`, `--set-collector-retention` | The same verbs after `darling-verb`. See [Collector settings](cli.md#collector-settings) |
| `.\PerformanceMonitor.Darling.Service.exe --backfill-rollups` | `darling-verb --backfill-rollups`. Add `--dry-run` to see the plan first |
| `.\PerformanceMonitor.Darling.Service.exe --reset-password-key` | `darling-verb --reset-password-key`, then `sudo systemctl restart darling` |
| `.\PerformanceMonitor.Darling.Service.exe --set-login-password --username <login>` | `<command that prints the password> \| darling-verb --set-login-password --username <login>`. Add `--dry-run` first. A sudo policy that records input (`log_input`) records the piped password: see [Set-login-password](cli.md#--set-login-password) |
| `.\PerformanceMonitor.Darling.Service.exe --diagnostics-bundle` | `darling-verb --diagnostics-bundle`. See [Diagnostics Bundle](cli.md#diagnostics-bundle) |
| `--add-alert-source`, `--rotate-alert-source`, `--remove-alert-source`, `--list-alert-sources` | The same verbs after `darling-verb` |
| `--encrypt-password` | Windows only. Use a `file:` or `env:` reference in `darling.json` |
| `--enable-web`, `--disable-web`, `--enable-mcp`, `--disable-mcp` | Windows only. See [The web dashboard and MCP](#the-web-dashboard-and-mcp) |
| `--print-web-token`, `--print-mcp-token` | Windows only. On Linux the token is in the file that your `file:` reference names |
| `--configure-network`, `--configure-firewall`, `--grant-cert-key-access` | Windows only. Edit the `network` blocks in `darling.json` by hand, and open ports with the host's own firewall |
| `--print-viewer-connection`, `--export-viewer-config` | Windows only, for the managed store. Write the Viewer's `darling.json` by hand, as [Manual configuration](viewer.md#manual-configuration-fallback) says |
| `Start-Service`, `Stop-Service` or `Restart-Service 'PerformanceMonitor Darling'` | `sudo systemctl start darling`, `stop` or `restart` |
| `Get-Service 'PerformanceMonitor Darling'` | `systemctl status darling` |
| `Get-Content <log> -Wait -Tail 40` | `sudo tail -n 40 -f /usr/share/PerformanceMonitorDarling/logs/darling-service_$(date +%Y%m%d).log` |
| `install-darling.ps1`, `Install-Darling.cmd`, the MSI | `install-darling.sh`, or steps 5 to 8 of this page |
| `upgrade-darling.ps1` | `upgrade-darling.sh`, or [Upgrade by hand](#upgrade-by-hand) |
| `uninstall-darling.ps1` | [Uninstall](#uninstall) |

## Changing a registered server

After the first start the server list lives in the store, and an edit to `servers` in `darling.json` does nothing for a server that is already registered. At start-up the service logs one WARN line for each enabled registered server whose entry in the file differs from the store. The line names each differing field with the file's value and the store's value, and gives the `--edit-server` command that applies the file's values, with each word that is not plain in single quotes.

- **To add a server**, use `--add-server` in its Linux form. It cannot change a server that is already registered.
- **To change a registered server**, for example its `trustServerCertificate` or its `encryptMode`, use `--edit-server` in its Linux form. It works while the service runs, and the service applies the change within one sweep with no restart. When the change affects how a server with a saved password is reached, the verb asks for that password again, at a hidden prompt or as one line from stdin when input is redirected. It tests the connection with the new settings and saves only if the server answers.
- **To change what `--edit-server` does not set**, such as a saved password, you need a write surface. They are the web dashboard's Admin page, the MCP `edit_server` tool (which needs `mcp.allowWrites`, `mcp.allowServerAdmin` and a token), and a Windows Viewer connected to this store. [Running Darling day to day](admin.md) has the steps for each surface.

For example, with the command from [Service verbs on Linux](#service-verbs-on-linux):

```bash
sudo runuser -u darling -- env DARLING_CONFIG=/etc/darling/darling.json dotnet /opt/darling/PerformanceMonitor.Darling.Service.dll --edit-server sql01 --set trustServerCertificate=true
```

[Edit-server](cli.md#--edit-server) lists the fields and the rules.

## The web dashboard and MCP

Both are off in the example. `--enable-web` and `--enable-mcp` are Windows only, so on Linux the switch is set in one of two places:

- **Before the first start**, set `web.enabled` or `mcp.enabled` to `true` in `darling.json`. The first start copies the value into the store.
- **After the first start**, the store holds the switch, in `config.config_service.web_enabled` and `mcp_enabled`, and the file's value is ignored. Change it there as the store owner, for example `UPDATE config.config_service SET web_enabled = true WHERE id = 1`. No restart is needed. The service reads the switch on each collection sweep, and it starts or stops the surface when the value changes. On the Docker Compose shape, run the statement through psql in the store container, as the [quick start](../darling-quick-start.md#run-on-linux-docker-compose-or-systemd-1804) shows for MCP.

The `network` block of each surface (`listen`, `allowFrom`, `token`) is read from the file at every start. Give the token as a `file:` reference. The settings are in [Web dashboard configuration](configuration-web.md) and [MCP configuration](configuration-mcp.md), and the exposure rules are in [Opt-in Network Endpoints](network-endpoints.md). Run `provision-roles.sql` before you turn either surface on (see [The least-privilege roles](#the-least-privilege-roles-when-the-web-dashboard-or-mcp-is-on)).

## Upgrade by hand

This section is for the systemd shape. For Docker Compose, see [Compose upgrade](admin.md#compose-upgrade).

`upgrade-darling.sh` does steps 5 and 6 below and checks the result: see [Install and upgrade with the scripts](#install-and-upgrade-with-the-scripts). Steps 1 to 4 stay with you, the compare of the store settings in step 3 among them, and so do steps 7 and 8. The config, the secrets and the keys are in `/etc/darling`, outside the service folder, so an upgrade replaces only `/opt/darling`.

1. Read the release notes, and [Rolling back to an older build](admin.md#rolling-back-to-an-older-build) for anything that an older build reads differently.
2. Back up the store, as in [Your own PostgreSQL](../darling-store-backup.md#your-own-postgresql). For a store on this host, the `postgres` OS user can run the dump without a password. This writes the dump to the current folder. Your shell makes the file, not `postgres`, and the dump holds every collected row, so the `umask 077` makes the file readable only by its owner and root:

   ```bash
   (umask 077; sudo -u postgres pg_dump --format=custom --no-owner --no-acl darling > darling-store.dump)
   ```

3. Compare the store settings with the block in [Settings](#settings), because a release can add a setting. The file is `/etc/postgresql/18/main/conf.d/darling.conf` on a Debian or Ubuntu package install. A store set up from an earlier quick start can lack `pg_stat_statements.track_utility = off` and `timescaledb.telemetry_level = off`, and the service then warns about `track_utility` at every start. Add what is missing and reload PostgreSQL. A reload is enough for these two. A change to the preload list or the worker settings needs a restart, as [Settings](#settings) says.

   ```bash
   sudo systemctl reload postgresql@18-main
   ```

4. Download and verify the new tarball, as in [step 4](#step-4-download-and-verify).
5. Replace the service files and keep the old ones as the rollback copy. An earlier upgrade can leave an older `/opt/darling.previous`, so the block removes it first. Without that, `mv` moves the folder inside the old copy and reports no error. With `-T`, `mv` stops with an error instead. The service stays stopped until step 6:

   ```bash
   sudo systemctl stop darling
   sudo rm -rf /opt/darling.previous
   sudo mv -T /opt/darling /opt/darling.previous
   sudo mkdir -p /opt/darling
   sudo tar -xzf PerformanceMonitorDarling-linux-x64-<new-version>.tar.gz -C /opt/darling --no-same-owner --no-same-permissions
   ```

6. Compare `/etc/systemd/system/darling.service` with the unit in step 8, because a release can change it. For example, 3.14.0 adds `Type=notify`, `postgresql.service` in `After=` and `Wants=network-online.target`. To compare them, save the unit from step 8 as `/tmp/darling.service.new`, then run the command below. No output means that the files are the same:

   ```bash
   diff /etc/systemd/system/darling.service /tmp/darling.service.new
   ```

   If the files differ, install the new unit over yours. Then reload systemd and start the service:

   ```bash
   sudo install -m 644 /tmp/darling.service.new /etc/systemd/system/darling.service
   sudo systemctl daemon-reload
   sudo systemctl start darling
   ```

7. Check the proof lines of step 9. `Postgres store ready (schema v<version>, <count> migration(s) applied)` shows the store's new schema version.
8. Read the worker counts again, as in [Size the workers after the first start](#size-the-workers-after-the-first-start).

Nothing checks that the old process is gone or rolls back for you. To go back, stop the unit, move `/opt/darling.previous` back to `/opt/darling` and start the unit. The store's migrations stay applied, so read the rollback list first. Remove `/opt/darling.previous` when you no longer need it.

## Uninstall

The uninstall does not touch the monitored servers. To remove this install's Extended Events sessions from them, remove each server first. See [Remove a server](admin.md#remove-a-server).

```bash
sudo systemctl disable --now darling
sudo rm /etc/systemd/system/darling.service
sudo systemctl daemon-reload
sudo rm -rf /opt/darling /opt/darling.previous
```

That leaves the store, the config, the keys and the logs. To remove them too:

```bash
sudo rm -rf /etc/darling /usr/share/PerformanceMonitorDarling
sudo userdel darling
sudo -u postgres dropdb darling
sudo -u postgres dropuser darling
```

`dropdb` deletes every collected row. Without the `darling-keys` folder, a saved password in a restored copy of the store cannot be opened again.

## Where everything is

| What | Where |
|---|---|
| Service files | `/opt/darling` |
| Config file | `/etc/darling/darling.json`, named by `DARLING_CONFIG` in the unit |
| Secret files | `/etc/darling/store_connection` and `/etc/darling/sql_password`, or wherever your `file:` references point |
| Password key and log-hash key | `/etc/darling/darling-keys/password-key` and `/etc/darling/darling-keys/log-hash-key` |
| Unit | `/etc/systemd/system/darling.service` |
| Service logs | `/usr/share/PerformanceMonitorDarling/logs/darling-service_yyyyMMdd.log`, and `sudo journalctl -u darling` |
| Store | Your PostgreSQL server's data directory, database `darling` |
| Store settings | `/etc/postgresql/18/main/conf.d/darling.conf` on a Debian or Ubuntu package install |
| Rollback copy | `/opt/darling.previous`, after an upgrade |

Back up `/etc/darling`, with its keys, and the store.

## Where to go next

- [The web dashboard](web-dashboard.md) and [MCP server](../mcp-server.md#darling): the two surfaces a Linux host serves.
- [Running Darling day to day](admin.md): add or remove a server, change a password, pause collection.
- [Operations](operations.md): the store, retention, the logs and watching the service from outside.
- [Darling troubleshooting](troubleshooting.md): every service log line and what it means.
