# The Extended Events Sessions Lite Creates

Audience: operators.

Part of the [Lite guide](lite-guide.md) and the [Performance Monitor documentation](../README.md#documentation).

Each Lite install has an eight-character id. Lite makes it at the first start and keeps it in `install-id.json`, in the data folder `%LOCALAPPDATA%\PerformanceMonitorLite-Data\`. The id appears in the name of each session that belongs to this install alone. Two installs that monitor one server never share such a session, so one install cannot drop or stop the other's.

If Lite cannot read `install-id.json` (another program holds it open, or the read fails), it leaves the file as it is and uses no id until it can read it. If it cannot save a new id, it does not use that id either. With no id, Lite creates, starts and drops no session of its own, and the long-query trace records why as a fault. The next collection cycle tries again, at most once a minute. The log says it once as a Warning with the error, then at Debug until the id is read. A file that holds something other than an id record still gets a new id.

## The sessions

- `PerformanceMonitor_Lite_<id>_LongQueryCompletions` is this install's long-query trace. Lite creates it while the opt-in `long_query_completions` collector is on. It is created with `STARTUP_STATE = OFF`, so it stays stopped after a server restart. Each check starts it again when it finds it stopped.
- `PerformanceMonitor_Deadlock` and `PerformanceMonitor_BlockedProcess` keep their shared names, because Darling and other installs read them too. Lite never drops them. It checks each one whenever its collector runs. It creates a missing one and starts a stopped one, so a deliberate stop does not last.
- `PerformanceMonitor_Attention` is the client-timeout session that the `attention` collector reads. It also keeps its shared name, and Lite never drops it. Lite checks it while the `attention` collector is on. It creates a missing one with `STARTUP_STATE = ON` and starts a stopped one. On Azure SQL Database, Lite creates one in every monitored database. If a session of that name has an older definition, Lite drops it and creates it again once for each run of the app.
- On Azure SQL Database, a shared session can stay unusable after a start. Lite then creates `PerformanceMonitor_Lite_<id>_Deadlock`, `PerformanceMonitor_Lite_<id>_BlockedProcess` or `PerformanceMonitor_Lite_<id>_Attention` in that database, with `STARTUP_STATE = OFF`, and reads that one. When the shared session works again, Lite switches back and drops its own fallback.

## The old shared session

Earlier versions shared one session, `PerformanceMonitor_LongQueryCompletions`, between all installs. An upgraded install drops it once for each registration and database. It then records that drop and never touches the session again. If an older install creates it again later, Lite logs one Information line per start and leaves the session alone. To remove it, run the statements under "Cleaning up by hand" below.

## Removing a server

Removing a server drops this install's sessions on it. These are the long-query session and its own deadlock, blocked-process and client-timeout fallbacks. Lite makes one attempt, within 15 seconds. A failed drop is logged and never stops the removal.

Lite leaves a session that another registration of this install keeps. On premises, it also leaves the session when it cannot tell whether another registration of this install shares the instance. That happens when the instance name of this server is not known. Lite logs the reason. The three shared sessions stay on the server.

## Registrations that share an instance

On premises, a registration with the trace off leaves the long-query session in place when another registration of this install has the trace on. Both registrations must point at the same instance. Lite compares the last known `@@SERVERNAME` of each one.

## Read-only intent on Azure SQL Database

A session cannot be created over a read-only connection. For a registration with read-only intent, Lite creates the session definition over a connection without the intent. The definition replicates to the read-only replica. Lite then starts the session over the registration's own connection. A drop stops the session on the replica first, then drops it on the primary. Microsoft describes the method in [Monitor read-only replicas with Extended Events](https://learn.microsoft.com/en-us/azure/azure-sql/database/read-scale-out). On Hyperscale with several high-availability replicas, a read-only connection lands on one replica that the app cannot choose, so the stop reaches only that one. Microsoft Learn describes no way to address a single high-availability replica; it says the read-intent workload is distributed arbitrarily across them ([Connect to an HA replica](https://learn.microsoft.com/en-us/azure/azure-sql/database/service-tier-hyperscale-replicas#connect-to-an-ha-replica)).

A Managed Instance registration with read-only intent needs the trace started on the primary first. Lite does not do that, so the registration gets the read-only message (error 3906). A registration without the intent can land on a read-only database, such as a geo-secondary. It gets one clear message, and Lite retries the create every hour.

## Azure SQL Database limits

Microsoft Learn lists these caps under "Resource governance" on [Extended Events in Azure SQL](https://learn.microsoft.com/en-us/azure/azure-sql/database/xevent-db-diff-from-svr). A database holds at most 100 started sessions. An elastic pool holds at most 100 database-scoped sessions. Session memory is capped at 128 MB per database and 512 MB per pool. In a dense pool, a start can fail below 100 sessions.

Each install uses one long-query session per database while the trace is on. It adds up to three fallbacks while a shared session is unusable. A failed create or start on Azure SQL Database carries a sentence that names these caps.

## Known limits

- An older Darling with the trace on loses capture once for each upgraded install, until it reconnects.
- An older Lite creates the old session again within one collection cycle.
- Two registrations of one instance in one install drop the old session twice.
- A lost record costs one more drop. This happens when you recreate the Lite data folder.
- On RDS Multi-AZ, the drop reaches the primary only. The copy on the standby stays stopped after a failover, unless an older install starts it.
- On Hyperscale with several high-availability replicas, the stop reaches only the replica that a read-only connection lands on. A copy of the session that runs on another replica is not stopped. Microsoft Learn describes no way to address one high-availability replica: [Connect to an HA replica](https://learn.microsoft.com/en-us/azure/azure-sql/database/service-tier-hyperscale-replicas#connect-to-an-ha-replica).
- On Azure SQL Database, two registrations of one database with different logins share one session name, as before.

## Clones

A clone of the whole machine, such as a VM snapshot, keeps the machine name and the Windows user. It also keeps the install id. The clone and the original then use the same session names on every server both monitor.

To give the clone its own id, delete `install-id.json` and restart Lite. Lite makes a new id at start. A data folder that you copy to another machine gets a new id on its own. So does a data folder that another Windows user opens. Import Settings does not carry the file.

## Cleaning up by hand

Lite has no command that drops sessions, so run the statements below yourself. Replace `<id>` with the `id` value in `install-id.json`. Each statement checks that the session exists first, so a run that finds nothing changes nothing.

The login needs the permission that [Microsoft Learn's `DROP EVENT SESSION` page](https://learn.microsoft.com/en-us/sql/t-sql/statements/drop-event-session-transact-sql) requires. On SQL Server and Azure SQL Managed Instance that is `DROP ANY EVENT SESSION` (SQL Server 2022 and later) or `ALTER ANY EVENT SESSION`. On Azure SQL Database it is `DROP ANY DATABASE EVENT SESSION`, in each database.

Server scope (SQL Server, Azure SQL Managed Instance and AWS RDS). The first statement drops the old shared session. The second drops this install's own session. Lite makes the deadlock, blocked-process and client-timeout fallbacks in Azure SQL Database only, so a server has none to drop.

```sql
IF EXISTS (SELECT 1/0 FROM sys.server_event_sessions AS ses WHERE ses.name = N'PerformanceMonitor_LongQueryCompletions')
    DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON SERVER;

IF EXISTS (SELECT 1/0 FROM sys.server_event_sessions AS ses WHERE ses.name = N'PerformanceMonitor_Lite_<id>_LongQueryCompletions')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_LongQueryCompletions] ON SERVER;
```

Database scope (Azure SQL Database). Run these five statements in each monitored database.

```sql
IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_LongQueryCompletions')
    DROP EVENT SESSION [PerformanceMonitor_LongQueryCompletions] ON DATABASE;

IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_Lite_<id>_LongQueryCompletions')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_LongQueryCompletions] ON DATABASE;

IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_Lite_<id>_Deadlock')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_Deadlock] ON DATABASE;

IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_Lite_<id>_BlockedProcess')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_BlockedProcess] ON DATABASE;

IF EXISTS (SELECT 1/0 FROM sys.database_event_sessions AS des WHERE des.name = N'PerformanceMonitor_Lite_<id>_Attention')
    DROP EVENT SESSION [PerformanceMonitor_Lite_<id>_Attention] ON DATABASE;
```

If the registration used read-only intent, first stop the session over a read-only connection with `ALTER EVENT SESSION [name] ON DATABASE STATE = STOP;`. Then run the drop over a connection without the intent, which reaches the primary. On Hyperscale with several high-availability replicas, the stop reaches only the replica that a read-only connection lands on. A copy of the session that runs on another replica is not stopped. Microsoft Learn describes no way to address one high-availability replica: [Connect to an HA replica](https://learn.microsoft.com/en-us/azure/azure-sql/database/service-tier-hyperscale-replicas#connect-to-an-ha-replica).

The shared `PerformanceMonitor_Deadlock`, `PerformanceMonitor_BlockedProcess` and `PerformanceMonitor_Attention` sessions need the same statements with those names. Run them only when no other install monitors the server, because every install that monitors it uses them.
