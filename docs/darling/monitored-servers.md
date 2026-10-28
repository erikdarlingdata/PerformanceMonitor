# Darling and your monitored servers

Audience: operators.

This page says what the Darling service does on each SQL Server it monitors, which Extended Events sessions it creates, and which permissions it needs. It moved here from the Darling operator guide. The grant block itself is in [Permissions](../permissions.md#lite--darling-on-premises).

## What the Service Does on Monitored Servers

On each successful connect, the service:

1. **Probes the server** — one query against `sys.dm_os_sys_info` / `SERVERPROPERTY()` for version, engine edition (box / Managed Instance / Azure SQL DB), AWS RDS detection, and msdb access. It is the same detection query Lite runs, so both editions classify a server identically.
2. **Ensures three Extended Events sessions** (created if missing, started if stopped; a ring buffer of at most 4 MB each, no files written on the server):
   - `PerformanceMonitor_Deadlock` — `xml_deadlock_report`, server-scoped on on-prem/Managed Instance/RDS; `database_xml_deadlock_report`, database-scoped on Azure SQL Database.
   - `PerformanceMonitor_BlockedProcess` — `blocked_process_report`, server-scoped (database-scoped on Azure SQL Database).
   - `PerformanceMonitor_Attention` — `attention`, the event a client sends when a query times out or is cancelled (#5640). Server-scoped, or database-scoped on Azure SQL Database. It has an `event_counter` target (the exact count) and a 1 MB ring buffer (the detail of recent events: application, host, database, session and statement text).
3. **Bootstraps the blocked-process threshold** — if `blocked process threshold (s)` is `0`, the service sets it to `5` via `sp_configure`. On AWS RDS `sp_configure` is unavailable; the attempt is tolerated and logged, and you set the threshold through an RDS Parameter Group instead (Azure SQL Database has a fixed 20-second threshold).
4. **Runs the on-connect config snapshots once** (`server_config`, `database_config`, `database_scoped_config`, `trace_flags`, `server_properties`), then runs all scheduled collectors on the shared default cadences.

Every failure in steps 2–3 is tolerated and logged: the deadlock/blocked-process collectors simply read zero rows until the sessions exist (and blocked-process reports only start arriving once the threshold is set). Monitoring queries connect with a 15-second connect budget and an application name of `PerformanceMonitorDarling`; connection encryption fails closed to `Mandatory` when the configured mode is unrecognized.

## The Extended Events Sessions the Service Creates

Each Darling install has an eight-character id. The service makes it at start and keeps it in `config.config_install_id`, a store table that holds one row. The id appears in the name of each session that belongs to this install alone. Two installs that monitor one server never share such a session, so one install cannot drop or stop the other's.

### The sessions

- `PerformanceMonitor_Darling_<id>_LongQueryCompletions` is this install's long-query trace. The service creates it while the opt-in `long_query_completions` collector is on. It is created with `STARTUP_STATE = OFF`, so it stays stopped after a server restart. Each check starts it again when it finds it stopped.
- `PerformanceMonitor_Deadlock`, `PerformanceMonitor_BlockedProcess` and `PerformanceMonitor_Attention` keep their shared names, because Lite and other installs read them too. The service never drops them. It checks them at connect and then once an hour. It creates a missing one and starts a stopped one, so a deliberate stop lasts less than an hour.
- On Azure SQL Database, a shared session can stay unusable after a start. The service then creates `PerformanceMonitor_Darling_<id>_Deadlock`, `PerformanceMonitor_Darling_<id>_BlockedProcess` or `PerformanceMonitor_Darling_<id>_Attention` in that database, with `STARTUP_STATE = OFF`, and reads that one. When the shared session works again, the service switches back and drops its own fallback.

### The old shared session

Earlier versions shared one session, `PerformanceMonitor_LongQueryCompletions`, between all installs. An upgraded install drops it once for each registration and database. It then records that drop and never touches the session again. If an older install creates it again later, the service logs one Information line per start and leaves the session alone. To remove it, see [Drop the Extended Events sessions a removed server left behind](collector-settings.md#drop-the-extended-events-sessions-a-removed-server-left-behind---drop-xe-sessions).

### Removing a server

Removing a server drops this install's sessions on it. These are the long-query session and its own deadlock and blocked-process fallbacks. The service makes one attempt, within 15 seconds. A failed drop is logged and never stops the removal.

The service leaves a session that another registration of this install keeps. On premises, it also leaves the session when it cannot tell whether another registration of this install shares the instance. That happens when the instance name of this server is not known. The service logs the reason. The two shared sessions stay on the server. The [`--drop-xe-sessions`](collector-settings.md#drop-the-extended-events-sessions-a-removed-server-left-behind---drop-xe-sessions) section shows how to drop them.

### Registrations that share an instance

On premises, a registration with the trace off leaves the long-query session in place when another registration of this install has the trace on. Both registrations must point at the same instance. The service compares the last known `@@SERVERNAME` of each one.

### Read-only intent on Azure SQL Database

A session cannot be created over a read-only connection. For a registration with read-only intent, the service creates the session definition over a connection without the intent. The definition replicates to the read-only replica. The service then starts the session over the registration's own connection. A drop stops the session on the replica first, then drops it on the primary. Microsoft describes the method in [Monitor read-only replicas with Extended Events](https://learn.microsoft.com/en-us/azure/azure-sql/database/read-scale-out). On Hyperscale with several high-availability replicas, a read-only connection lands on one replica that the app cannot choose, so the stop reaches only that one. Microsoft Learn describes no way to address a single high-availability replica; it says the read-intent workload is distributed arbitrarily across them ([Connect to an HA replica](https://learn.microsoft.com/en-us/azure/azure-sql/database/service-tier-hyperscale-replicas#connect-to-an-ha-replica)).

A Managed Instance registration with read-only intent needs the trace started on the primary first. The service does not do that, so the registration gets the read-only message (error 3906). A registration without the intent can land on a read-only database, such as a geo-secondary. It gets one clear message, and the service retries the create every hour.

### Azure SQL Database limits

Microsoft Learn lists these caps under "Resource governance" on [Extended Events in Azure SQL](https://learn.microsoft.com/en-us/azure/azure-sql/database/xevent-db-diff-from-svr). A database holds at most 100 started sessions. An elastic pool holds at most 100 database-scoped sessions. Session memory is capped at 128 MB per database and 512 MB per pool. In a dense pool, a start can fail below 100 sessions.

Each install uses one long-query session per database while the trace is on. It adds up to two fallbacks while a shared session is unusable. A failed create or start on Azure SQL Database carries a sentence that names these caps.

### Known limits

- An older Darling with the trace on loses capture once for each upgraded install, until it reconnects.
- An older Lite creates the old session again within one collection cycle.
- Two registrations of one instance in one install drop the old session twice.
- A Darling server that you point at a new host keeps its server id. The service never drops the old session on the new host.
- A lost record costs one more drop. This happens when you remove a server and add it again.
- On RDS Multi-AZ, the drop reaches the primary only. The copy on the standby stays stopped after a failover, unless an older install starts it.
- On Hyperscale with several high-availability replicas, the stop reaches only the replica that a read-only connection lands on. A copy of the session that runs on another replica is not stopped. Microsoft Learn describes no way to address one high-availability replica: [Connect to an HA replica](https://learn.microsoft.com/en-us/azure/azure-sql/database/service-tier-hyperscale-replicas#connect-to-an-ha-replica).
- On Azure SQL Database, two registrations of one database with different logins share one session name, as before.

### Clones

A physical clone keeps the install id. A VM snapshot is a physical clone, and so is a store copied with `pg_basebackup`. The clone and the original then use the same session names on every server both monitor. To give the clone its own id, stop the service, delete the row, and start the service. The service makes a new id at start.

```sql
DELETE FROM config.config_install_id;
```

A managed major upgrade of the store keeps the id. The upgrade gives the store a new cluster identifier and a higher major version, and keeps the OIDs of the store's database and of the install id table. The row records both OIDs and the major version it last saw. When the cluster identifier changed, both OIDs still match, and the major version is higher than the row's, the service keeps the id and writes the new cluster identifier and major version on the row (one Information line in the log names both cluster identifiers). A changed cluster identifier at the same or a lower major version gets a new id, so a copy of the row into a fresh install of the same version does not share the id. A copy into an install of a higher version looks like an upgrade and keeps the id: delete the row there, as for a clone.

A store moved by dump and restore, or by logical replication, is a different store. It gets a new id on its own, because a restore assigns new OIDs. The sessions of the old id stay until you drop them by hand; `--drop-xe-sessions` lists them.

## Permissions on Monitored Servers

Darling needs the **same target-server grants as Lite**, so the copy-paste block lives in one place for both: **[Permissions in docs/permissions.md](../permissions.md#lite--darling-on-premises)** — `VIEW SERVER STATE`, `CONNECT ANY DATABASE`, `VIEW ANY DEFINITION`, `ALTER ANY EVENT SESSION`, and the optional `ALTER TRACE`, `ALTER SETTINGS`, and msdb job-table grants, verified live against SQL Server 2025 with a scratch login carrying exactly them ([#1823](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1823)). That block is authoritative; this section is the Darling-specific reading of it. Keeping one list instead of two is deliberate — a second copy is how the old one went stale.

**The one Darling-specific line:** for `"auth": "integrated"` the grants go to the Windows account **the service runs as**, so use `CREATE LOGIN [DOMAIN\svc-account] FROM WINDOWS;` in place of the block's `CREATE LOGIN ... WITH PASSWORD`. Everything after it is unchanged. See [Run the service as a domain account or gMSA](install.md#run-the-service-as-a-domain-account-or-gmsa) for which account that actually is — it is not the one you ran `--test-connection` as.

What each grant buys you, and what breaks without it:

| Grant | Why | If missing |
|---|---|---|
| `VIEW SERVER STATE` | All DMV collectors (wait stats, query stats, memory, CPU, file I/O, sessions, etc.) and the connect probe | Collection fails — this one is required |
| `ALTER ANY EVENT SESSION` | Create/start the three shared XE sessions (deadlock, blocked-process and attention) | Logged; deadlock, blocked-process and client-timeout collectors read zero rows (an admin can pre-create the sessions instead) |
| `CONNECT ANY DATABASE` | The per-database collectors (`database_scoped_config`, `query_store_health`, `index_object_stats`, `database_size_stats`, `query_store_stats`, `query_store_forced_plans`) enter each database via `EXECUTE [db].sys.sp_executesql` | Databases the login cannot enter are skipped; without the grant that is every user database |
| `VIEW ANY DEFINITION` | Catalog-view row visibility everywhere: `sys.tables` / `sys.indexes` / `sys.objects` for the index and object collectors, `sys.dm_db_partition_stats`, and the AG catalog views (`sys.availability_groups`, `sys.availability_replicas`) | **Silently zero rows** — catalog views hide rows rather than erroring, so missing objects look exactly like empty databases, and a real AG cluster looks identical to a server with no AGs |
| `ALTER SETTINGS` | The `sp_configure` blocked-process-threshold bootstrap | Logged; set the threshold yourself (or via RDS Parameter Group) |
| `ALTER TRACE` | The `default_trace_events` collector — `sys.traces` / `fn_trace_gettable` accept nothing less | `PERMISSIONS` skip in collection health; the default-trace tab stays empty |
| msdb job-table `SELECT`s + `agent_datetime` `EXECUTE` | `running_jobs` / `job_history` / `agent_status` collectors and the failed/long-running-job alerts — all direct table reads; `SQLAgentReaderRole` alone leaves every one failing with error 229 | Skipped gracefully — logged as a permissions skip, alerts return no jobs |
| `DBCC TRACESTATUS` permission | `trace_flags` snapshot | Degrades to zero rows with a warning |

The msdb grants live inside a system database SQL Server setup can rewrite — re-check them after a CU or version upgrade.

**Amazon RDS for SQL Server:** RDS has no sysadmin and refuses `CONNECT ANY DATABASE`, `ALTER SETTINGS` and five of the msdb grants to its master user, so the block above cannot be run there. `--grant-script` (and the Add Server dialog) print an RDS form for a `*.rds.amazonaws.com` host, or with `--rds`: it runs as the master user, leaves those grants out, and creates a user for the login in each current user database. A database created later needs the same `CREATE USER`. See [AWS RDS for SQL Server](../permissions.md#aws-rds-for-sql-server) for the measured results and what each missing grant costs.

**Azure SQL Database:** connect to the one database you monitor (set the server entry's `"database"`), using a contained user with `VIEW DATABASE STATE`, `VIEW DEFINITION` and `ALTER ANY DATABASE EVENT SESSION`, matching the product's existing Azure guidance. The third grant lets the monitor create its Extended Events sessions, which are database-scoped there. A contained user cannot read the tempdb DMVs: `tempdb_stats` shows `NO_PERMISSIONS`, and only a server login in `##MS_ServerStateReader##` fixes that. SQL Agent collectors are skipped automatically.

Collectors that hit a permission error (SQL errors 229/297/300, plus 8189 from `sys.traces`) log a `PERMISSIONS` row in `collection_log` and retry on their next scheduled run — one denied collector never stops the rest.

### Which collectors run on which platform

Every collector declares its own applicability in code (`AppliesTo(CollectorTargetInfo)`), so this is not a hand-maintained per-collector list — the SQL Server collectors fall into seven groups, and a collector outside its supported platform is **skipped before it runs**, not failed and logged every cycle.

| Runs on | Collectors | Gate |
|---|---|---|
| Everything | wait stats, CPU utilization, memory (stats/clerks/grants), file I/O, tempdb, latches, spinlocks, plan cache, session summary, plus blocking, deadlocks, blocked-process reports, DMV blocking snapshots, perfmon, query snapshots, procedure stats, index/object stats, long-query completions, client timeouts (`attention`), database config/scoped-config/size, server properties, session stats, waiting tasks | no gate |
| On-prem, Managed Instance, RDS — **not** Azure SQL DB | CPU scheduler stats, default trace events, memory pressure events, server config, system health events, trace flags, database states, Availability Group replica states, Availability Group database replica states | `!IsAzureSqlDb` |
| On-prem and Managed Instance, needs msdb | job history | `!IsAzureSqlDb && HasMsdbAccess` |
| On-prem and Managed Instance, needs msdb — **not** RDS | agent status, running jobs | `!IsAzureSqlDb && !IsAwsRds && HasMsdbAccess` |
| SQL Server 2016+ (or any Azure flavour) | query stats, Query Store stats, Query Store health, Query Store forced plans | `SqlMajorVersion >= 13 \|\| IsAzureSqlDb \|\| IsAzureManagedInstance` |
| SQL Server 2017+ (or any Azure flavour) | plan correction (automatic tuning recommendations) | `SqlMajorVersion >= 14 \|\| IsAzureSqlDb \|\| IsAzureManagedInstance` |
| SQL Server 2019+ (or any Azure flavour) | persistent version store stats | `SqlMajorVersion >= 15 \|\| IsAzureSqlDb \|\| IsAzureManagedInstance` |

Notes:

- **Azure SQL DB** is the most restricted target: the nine `!IsAzureSqlDb` collectors above, and job history, read server-scoped DMVs or on-disk artifacts that do not exist there, and the SQL Agent collectors have no Agent to read. Nothing about that is a permission problem, so it is not reported as one.
- **AWS RDS** blocks direct `msdb` job reads specifically; the rest of the SQL Agent surface is unaffected.
- **`HasMsdbAccess`** is probed per server at connect and is exactly `HAS_DBACCESS('msdb')` — *any* access to msdb, not a specific role or table grant. Losing msdb access later moves those collectors from running to skipped without an error storm. A login that can enter msdb but lacks `SELECT` on the job tables passes this probe and is caught one layer down as a `PERMISSIONS` skip instead.
- An unknown version (`SqlMajorVersion == 0`, i.e. detection has not completed yet) is treated as capable rather than skipped, so a collector is never silently dropped because a probe was slow.

If a tab or column is empty and you expect data, check **Collection Health**: a collector skipped for platform reasons shows no runs at all, whereas one denied by permissions logs `PERMISSIONS` and is classified `NO_PERMISSIONS`. Those are different problems with different fixes — the first is expected on that platform, the second is a grant to add from the table above.
