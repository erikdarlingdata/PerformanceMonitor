# Back up, restore and move the store

Audience: operators.

This guide shows how to back up a Darling store, restore it, and move it to another machine. It covers the three ways a store runs: the managed store on Windows, your own PostgreSQL, and the Linux compose stack. It ends with how much disk the store uses and how to change that.

The store is the PostgreSQL database named `darling`. Copying the database is not enough. The service also keeps files outside the database that the database cannot be opened or read without. Back those up with the database, and keep them apart from it where the section says so.

## Which recipe to use

| You want to | Managed store (Windows) | Your own PostgreSQL | Compose (Linux) |
|---|---|---|---|
| Keep a rollback point on the same machine | Copy the folders | `pg_dump` | Copy the volumes or `pg_dump` |
| Move to another machine | Dump and restore | `pg_dump` and `pg_restore` | `pg_dump` and `pg_restore` |

## The managed store on Windows

The Windows service is named `PerformanceMonitor Darling`. It runs a bundled PostgreSQL 18 with TimescaleDB, from the folder `pg-runtime` in the install folder. By default the store lives under `C:\ProgramData\PerformanceMonitorDarling`. The data directory is the `pg` folder in it, unless `postgres.dataDirectory` in `darling.json` names another one.

### What to keep

The folder that holds the data directory is the store root. Keep all of it. These are the files that matter.

| Item | Where | What it is |
|---|---|---|
| `pg\` | the store root | The database files |
| `pg-credential.dpapi` | the store root, beside `pg\` | The password of the store owner, `darling` |
| `pg-admin-credential.dpapi`, `pg-viewer-credential.dpapi`, `pg-web-credential.dpapi`, `pg-mcp-credential.dpapi` | the store root | The passwords of the four least-privilege roles |
| `password-key.dpapi` | the store root | The key that opens the SQL login passwords saved in the store |
| `log-hash-key.dpapi` | the store root | The key for the identities of stored log events |
| `server.crt` and `server.key` | the store root, only when the store has TLS on | The store's own certificate and key |
| `darling-managed.conf` | the store root | The settings the service manages. The service rewrites it on every start |
| `darling.json` | the install folder, beside `PerformanceMonitor.Darling.Service.exe` | The service configuration, and its `.bak-*` copies |
| The files named in `web.network.tls` and `mcp.network.tls` in `darling.json` | wherever those blocks point | The certificates for the web dashboard and the MCP server |

You do not need `pg-runtime` in the install folder. The service extracts it again from `pg-runtime.zip` when it is missing.

### A copy on the same machine

The credential files are protected with Windows DPAPI at machine scope. Only the machine that wrote them can read them. A copy of the store root on another machine cannot be opened, because the new machine cannot read `pg-credential.dpapi`. To move the store to another machine, use the dump and restore steps in the next section.

To make a rollback point:

1. Open an elevated PowerShell prompt.
2. Stop the service. The service stops the store when it started it.

   ```powershell
   Stop-Service 'PerformanceMonitor Darling' -Force
   (Get-Service 'PerformanceMonitor Darling').WaitForStatus('Stopped', [TimeSpan]::FromSeconds(120))
   ```

3. If a `postgres.exe` from the data directory still runs, the service did not start it. Stop it with `pg_ctl.exe -D "C:\ProgramData\PerformanceMonitorDarling\pg" stop -m fast`. The program is in `pg-runtime\pgsql\bin` in the install folder.
4. Copy the store root and `darling.json`. Use `/COPYALL` so that the owners and access lists travel with the files.

   ```powershell
   robocopy 'C:\ProgramData\PerformanceMonitorDarling' 'D:\darling-backup\store' /E /COPYALL /DCOPY:DAT
   Copy-Item 'C:\Program Files\PerformanceMonitorDarling\darling.json' 'D:\darling-backup\'
   ```

5. Start the service.

   ```powershell
   Start-Service 'PerformanceMonitor Darling'
   ```

To restore the copy, stop the service the same way, copy the folders back on the same machine, and start the service. We recommend that you restore with the same Darling build that made the copy.

Renaming or copying the install folder does not back up the data. The install folder holds none of it.

### Move the store to another machine

Move the data with `pg_dump` and `pg_restore`. These programs ship in the managed bundle, in `pg-runtime\pgsql\bin` in the install folder. You do not need to stop the service for the dump.

The new store must run the same TimescaleDB version as the old store. Install the same Darling build on the new machine. Then upgrade it after the restore. To read the version, run this in `psql` against each store:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';
```

On the old machine, make the dump. Run this in an elevated PowerShell prompt. The first lines read the store password from `pg-credential.dpapi`.

```powershell
Add-Type -AssemblyName System.Security
$entropy = [Text.Encoding]::UTF8.GetBytes('PerformanceMonitor.Darling.v1')
$blob = (Get-Content -Raw 'C:\ProgramData\PerformanceMonitorDarling\pg-credential.dpapi').Trim()
$env:PGPASSWORD = [Text.Encoding]::UTF8.GetString([Security.Cryptography.ProtectedData]::Unprotect([Convert]::FromBase64String($blob), $entropy, 'LocalMachine'))
$bin = 'C:\Program Files\PerformanceMonitorDarling\pg-runtime\pgsql\bin'
& "$bin\pg_dump.exe" -h 127.0.0.1 -p 5641 -U darling --format=custom --no-owner --no-acl --file D:\darling-store.dump darling
Remove-Item Env:PGPASSWORD
```

The port 5641 is the default of `postgres.port`. The flags `--no-owner` and `--no-acl` leave out the owners and grants. The service sets the roles and grants again on every start. `pg_dump` can print warnings about circular foreign-key constraints in the TimescaleDB catalog. In our test the dump restored with no errors.

On the new machine:

1. Install the same Darling build, start the service once, and stop it. The first start creates and migrates an empty store.
2. Copy `darling.json` from the old machine. The `encryptedPassword` values in it are protected for the old machine. Run `--encrypt-password` again on the new machine and paste the new values.
3. Read the new store password from the new machine's `pg-credential.dpapi` with the same lines as above. Then start the new store by hand.

   ```powershell
   & "$bin\pg_ctl.exe" -D 'C:\ProgramData\PerformanceMonitorDarling\pg' -w start
   ```

4. Replace the empty database with the dump. The `timescaledb_pre_restore` and `timescaledb_post_restore` steps are required. Without them the restore fails with an error such as `could not find hypertable with id 1`.

   ```powershell
   & "$bin\dropdb.exe" -h 127.0.0.1 -p 5641 -U darling darling
   & "$bin\createdb.exe" -h 127.0.0.1 -p 5641 -U darling darling
   & "$bin\psql.exe" -h 127.0.0.1 -p 5641 -U darling -d darling -c "CREATE EXTENSION timescaledb" -c "SELECT timescaledb_pre_restore()"
   & "$bin\pg_restore.exe" -h 127.0.0.1 -p 5641 -U darling --dbname=darling --exit-on-error D:\darling-store.dump
   & "$bin\psql.exe" -h 127.0.0.1 -p 5641 -U darling -d darling -c "SELECT timescaledb_post_restore()"
   ```

5. Stop the store you started, then start the service. If you leave the store running, the service uses it but never stops it.

   ```powershell
   & "$bin\pg_ctl.exe" -D 'C:\ProgramData\PerformanceMonitorDarling\pg' -m fast -w stop
   Start-Service 'PerformanceMonitor Darling'
   ```

6. Enter the saved SQL login passwords again. The key that opens them, `password-key.dpapi`, cannot move between machines. The service reports the key as mismatched or missing. Run `PerformanceMonitor.Darling.Service.exe --reset-password-key`, restart the service, and enter each password again in the Viewer.
7. Run `PerformanceMonitor.Darling.Service.exe --check-settings`. It prints the store size and the TimescaleDB version.

Restore into the same PostgreSQL major version or a newer one. The managed bundle is PostgreSQL 18. A dump from a compose store (PostgreSQL 17) restores into it. A dump from the managed store does not restore into the compose store.

## Your own PostgreSQL

If the service connects to a PostgreSQL that you run (`postgres.managed` is `false`), you own the database backup. Use your normal backup tool, or `pg_dump`. The managed bundle does not apply here, so use the `pg_dump` and `pg_restore` of your own PostgreSQL install. They must be the same major version as the server or newer.

Dump the `darling` database:

```
pg_dump -h <host> -p <port> -U <owner> --format=custom --no-owner --no-acl --file darling-store.dump darling
```

Restore it into an empty database. If the store has the TimescaleDB extension (`\dx` lists `timescaledb`), you need the two TimescaleDB steps. They must be in this order.

```
createdb -h <host> -p <port> -U <owner> darling
psql -h <host> -p <port> -U <owner> -d darling -c "CREATE EXTENSION timescaledb" -c "SELECT timescaledb_pre_restore()"
pg_restore -h <host> -p <port> -U <owner> --dbname=darling --exit-on-error darling-store.dump
psql -h <host> -p <port> -U <owner> -d darling -c "SELECT timescaledb_post_restore()"
```

If the store has no TimescaleDB, skip the `psql` lines and run `createdb` and `pg_restore` only. The new TimescaleDB version must match the old one.

Run `Darling/tools/provision-roles.sql` again on the restored database, as the database owner. The dump left out the grants, and this script sets them for the admin, viewer and mcp roles.

Also keep these files. The database dump does not contain them.

| Item | Where | What it is |
|---|---|---|
| `darling.json` and its `.bak-*` copies | beside `PerformanceMonitor.Darling.Service.exe`, or the path you set with `--config` or `DARLING_CONFIG` | The service configuration |
| `darling-keys\` | beside `darling.json` | The key for saved SQL login passwords (`password-key`, or `password-key.dpapi` on Windows) and the log-hash key |
| The certificate files | the paths in `web.network.tls` and `mcp.network.tls` | PFX bundles, or PEM certificate and key pairs |

A store restored without `darling-keys\` cannot open its saved SQL login passwords. The service reports the key as missing. Back up `darling-keys\` after the first start and again after every `--reset-password-key`. Keep that backup apart from the store dump, so that one stolen backup does not hold both the passwords and their key.

On Windows, `password-key.dpapi` and every `encryptedPassword` in `darling.json` are protected for one machine. On another machine, run `--reset-password-key` and `--encrypt-password` again. On Linux, `password-key` is a plain owner-only file and moves with the folder. Keep its mode at `0600` and its owner the service account.

## The Linux compose stack

The compose file is `Darling/compose/docker-compose.yml`. Its project name is `performancemonitor-darling`, so Docker names the two volumes `performancemonitor-darling_darling-store` and `performancemonitor-darling_darling-credentials`. Run `docker volume ls` to confirm the names on your host.

| Item | Where | What it is |
|---|---|---|
| Volume `darling-store` | `/var/lib/postgresql/data` in the `store` container | The database files |
| Volume `darling-credentials` | `/var/lib/darling/credentials` in the `darling` container | The role passwords, `password-key` and `log-hash-key` |
| `darling.json` | the compose folder | The service configuration |
| `secrets/` | the compose folder | The store password, the connection string, the web and MCP tokens, the SQL login password and any TLS files |

Back up the credentials volume apart from the store volume, and keep the backups in different places.

To make a rollback point on the same host, copy both volumes while the stack is stopped:

```
docker compose stop
docker run --rm -v performancemonitor-darling_darling-store:/data:ro -v "$PWD":/backup alpine tar czf /backup/darling-store.tgz -C /data .
docker run --rm -v performancemonitor-darling_darling-credentials:/data:ro -v "$PWD":/backup alpine tar czf /backup/darling-credentials.tgz -C /data .
docker compose start
```

To move to another host, use a dump. The store container has `pg_dump` and `pg_restore`. On the old host:

```
docker compose exec -T store pg_dump -U darling --format=custom --no-owner --no-acl darling > darling-store.dump
```

On the new host, copy the compose folder (`darling.json` and `secrets/`), start only the store, and restore into its empty `darling` database:

```
docker compose up -d store
docker compose exec -T store psql -U darling -d darling -c "CREATE EXTENSION IF NOT EXISTS timescaledb" -c "SELECT timescaledb_pre_restore()"
docker compose exec -T store pg_restore -U darling -d darling --exit-on-error < darling-store.dump
docker compose exec -T store psql -U darling -d darling -c "SELECT timescaledb_post_restore()"
```

Then restore the credentials volume from its backup and start the service. The service sets the role grants again on its first start.

```
docker run --rm -v performancemonitor-darling_darling-credentials:/data -v "$PWD":/backup alpine tar xzf /backup/darling-credentials.tgz -C /data
docker compose up -d
```

The store container runs PostgreSQL 17 (`timescale/timescaledb:latest-pg17`). Do not restore a dump from a newer major version into it.

## How much disk will it use

The size of the store depends on the number of monitored servers and the collectors you run on them. The [store sizing guide](darling-store-sizing.md) gives measured sizes and host sizes by server count. To see the size of your store now, run `--check-settings`, or run this in `psql` as the store owner.

```sql
SELECT pg_size_pretty(pg_database_size(current_database())) AS store_size;
```

This query lists the ten largest tables. It counts a TimescaleDB table with all of its chunks.

```sql
SELECT table_schema, table_name, pg_size_pretty(size_bytes) AS size
FROM (
    SELECT hypertable_schema AS table_schema, hypertable_name AS table_name,
           hypertable_size(format('%I.%I', hypertable_schema, hypertable_name)) AS size_bytes
    FROM timescaledb_information.hypertables
    UNION ALL
    SELECT n.nspname, c.relname, pg_total_relation_size(c.oid)
    FROM pg_class AS c
    JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE c.relkind = 'r'
      AND n.nspname IN ('collect', 'config')
      AND NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables AS h
                      WHERE h.hypertable_schema = n.nspname AND h.hypertable_name = c.relname)
) AS t
ORDER BY size_bytes DESC
LIMIT 10;
```

On a store without TimescaleDB, the view `timescaledb_information.hypertables` does not exist. Remove the first part of the `UNION ALL` there.

### How long the store keeps each collector

A purge runs on the first sweep after the service starts, and then once a day. With TimescaleDB, the purge drops whole chunks. Without it, the purge uses `DELETE`. These are the defaults.

| Days kept | Collectors |
|---|---|
| 7 | `query_snapshots`, `waiting_tasks`, `running_jobs`, `agent_status` |
| 14 | `pg_plan_capture` |
| 30 | Every other collector, 53 in all. Examples are `wait_stats`, `query_stats`, `query_store`, `cpu_utilization`, `deadlocks`, `pg_wait_stats` and `pg_log_events` |
| 90 | `database_size_stats`, `index_object_stats`, `database_protection`, `identity_capacity`, `pvs_stats`, `pg_wraparound_stats`, `pg_deadlocks`, `pg_replication_slots`, `pg_autovacuum_stats`, `pg_index_usage_stats`, `pg_table_bloat_stats`, `pg_index_bloat` |
| 365 | `server_properties`, `job_history`, `pg_server_config`, `pg_plan_capture_readiness`, `pg_extension_availability`, `pg_column_stats`, `pg_database_size_stats` |

Some data follows its own horizon, not a collector's.

| Data | Days kept |
|---|---|
| Raw `query_stats`, `procedure_stats` and `query_store_stats`, with TimescaleDB | 4, then the hourly and daily rollups hold the history |
| Query Store interval tables `collect.query_store_interval_wide` and `collect.query_store_interval_latest` | 10 and 15 |
| `analysis_findings` and finished rows of `config.config_command` | 30 |
| `collection_log` | 60 |
| Hourly history rollups, and `config_alert_log` | 90 |
| `collect.plan_force_actions` | 365 |
| Daily history rollups | No limit |

The [store sizing guide](darling-store-sizing.md#the-retention-horizons-behind-these-numbers) lists the rollup tiers and the Query Store tables in more detail.

### Change the retention

Retention is not a setting in `darling.json`. It is the column `retention_days` of the table `config.config_collector_schedules`, one value for each collector. A row that is not there means the default above. Change it in one of three ways.

- The command line: `PerformanceMonitor.Darling.Service.exe --set-collector-retention <collector> <days|default> [--server <server>]`. The days are 1 to 3650. The word `default` removes the override. Retention is fleet-wide: the purge reads one value for each collector, so a number with `--server` is refused. `--server` with `default` removes a per-server value that an older build stored.
- The Collector Schedules window in the Viewer.
- The MCP tool `set_collector_retention`, when `mcp.allowWrites` is on.

A shorter retention frees disk on the next daily purge. A longer retention grows the store by the extra days of that collector.
