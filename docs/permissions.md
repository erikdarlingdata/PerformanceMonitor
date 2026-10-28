# Permissions

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

### Lite / Darling (On-Premises)

Nothing is installed on the target server. Below is the full least-privilege grant set, verified live against SQL Server 2025 with a scratch login ([#1823](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1823)). **This block is the authoritative copy** — Lite and Darling need the identical grants, so the Darling operator guide points here rather than keeping a second list that can drift out of step:

```sql
USE [master];

/* Create the login first. Skip this statement if the login already exists. For Windows
   authentication use CREATE LOGIN [DOMAIN\account] FROM WINDOWS; instead. In Darling with integrated
   authentication, the account is the one the service runs as (see the Darling operator guide). */
CREATE LOGIN [YourLogin] WITH PASSWORD = N'<strong password>';

/* Server-scoped DMVs - the required core. Also implies VIEW DATABASE STATE in every database. */
GRANT VIEW SERVER STATE TO [YourLogin];

/* Enter every current and future database, with no per-database users to maintain. The
   per-database collectors (database scoped config, index/object stats, database size,
   Query Store) skip databases the login cannot enter. */
GRANT CONNECT ANY DATABASE TO [YourLogin];

/* Catalog visibility. Catalog views enforce permissions by HIDING ROWS, not by erroring:
   without this, sys.tables / sys.indexes (the index and object collectors), sys.identity_columns
   (the identity column capacity collector) and the AG catalog views return zero rows that look exactly like "no data" (see the AG note below). */
GRANT VIEW ANY DEFINITION TO [YourLogin];

/* The deadlock, blocked-process and attention Extended Events sessions, and the opt-in long-query trace session.
   A login without this shows PERMISSIONS for the collector instead of an error. */
GRANT ALTER ANY EVENT SESSION TO [YourLogin];

/* Optional: the default-trace collector reads sys.traces, which requires ALTER TRACE -
   VIEW SERVER STATE does not cover it. Note ALTER TRACE is not read-only (it also permits
   creating and altering traces, and implies SHOWPLAN in every database); withhold it and
   the collector records a PERMISSIONS skip instead. */
GRANT ALTER TRACE TO [YourLogin];

/* Optional: lets the app bootstrap 'blocked process threshold (s)' to 5 when it is still 0.
   sp_configure + RECONFIGURE require ALTER SETTINGS; without it the bootstrap is logged and
   skipped, blocked process reports stay empty, and you set the threshold yourself (or via an
   RDS Parameter Group). Blocking stays visible either way through the DMV blocking snapshot. */
GRANT ALTER SETTINGS TO [YourLogin];

/* Optional: SQL Agent job monitoring + failed-job alerts. Direct table grants, deliberately
   NOT SQLAgentReaderRole: that role gates the sp_help_job* procedures, which this product
   never calls, and grants NO SELECT on the tables the collectors actually read - with only
   the role, every Agent collector fails with error 229. */
USE [msdb];
CREATE USER [YourLogin] FOR LOGIN [YourLogin];
GRANT SELECT ON dbo.sysjobs         TO [YourLogin];
GRANT SELECT ON dbo.sysjobsteps     TO [YourLogin];  -- names the Agent job and step on a Long-Running Query alert
GRANT SELECT ON dbo.sysjobactivity  TO [YourLogin];
GRANT SELECT ON dbo.sysjobhistory   TO [YourLogin];
GRANT SELECT ON dbo.sysjobschedules TO [YourLogin];
GRANT SELECT ON dbo.syscategories   TO [YourLogin];
GRANT SELECT ON dbo.syssessions     TO [YourLogin];
GRANT EXECUTE ON dbo.agent_datetime TO [YourLogin];
```

Two operational notes. The msdb grants live inside a system database that SQL Server setup can rewrite — re-check them after a CU or version upgrade. And if a security review rejects `CONNECT ANY DATABASE`, the documented fallback is a real user per database (the same shape as the [FinOps loop below](#finops-index-analysis-per-database-grants), minus the `sql_expression_dependencies` grant) — at the cost of not covering databases created later.

**Availability Groups need a second grant.** The `ag_replica_states` / `ag_database_replica_states` collectors read the AG *catalog views* (`sys.availability_groups`, `sys.availability_replicas`) alongside the `sys.dm_hadr_*` DMVs. The DMVs are covered by `VIEW SERVER STATE`, but [the catalog views require `VIEW ANY DEFINITION`](https://learn.microsoft.com/en-us/sql/database-engine/availability-groups/windows/monitor-availability-groups-transact-sql) — and catalog views enforce that by *hiding rows*, not by raising an error. So on a real AG cluster a login with only `VIEW SERVER STATE` gets zero rows, which looks exactly like a server with no availability groups. If your AG dashboards are empty, this grant is why.

Darling can print this script for you. `PerformanceMonitor.Darling.Service.exe --grant-script <server-name>` prints the login and the grants below for a server that Darling monitors. `--grant-script --host <host>` does the same for a host that you did not add yet. In the Viewer, the Add Server dialog has a **Copy grant script** button that copies the same text.

On the machine that runs the service, the Copy grant script button and `--grant-script` read the account the service runs as. On any other machine they cannot read that. The script then names a placeholder account (`DOMAIN\your-service-account`) and says so. Run `--grant-script` on the service's machine for the real account, or pass `--service-account <account>` to name it yourself.

The principal is the SQL login that the server entry uses. With Windows authentication, it is the account that the service runs as:

- For a SQL Server on the same machine, it is `NT SERVICE\PerformanceMonitor Darling`.
- For a remote SQL Server, it is the machine account (`DOMAIN\MACHINE$`).

`--finops` adds the FinOps per-database grants. `--azure` prints the Azure SQL Database form, and a `name.database.windows.net` host does too. An Azure SQL Managed Instance host has a zone label before `database.windows.net`, so it gets the on-premises form. The script has no password, only a placeholder. You can run it a second time with no harm.

The connection test also names each of the four required grants that the login does not have: `VIEW SERVER STATE`, `CONNECT ANY DATABASE`, `VIEW ANY DEFINITION` and `ALTER ANY EVENT SESSION`. Beside a successful test, it shows a warning with the statement that adds each missing grant. With Windows authentication, only the service's own test does this check: the Viewer, the MCP tools and `test_connect`. `--test-connection` runs as the console user, so it skips the check.

Darling uses the same target-server grants. Its bundled PostgreSQL store and service account are covered in the [Darling operator guide](../Darling/README.md). Darling can also monitor PostgreSQL servers, which need different grants (see [PostgreSQL targets](#postgresql-targets-darling)), and it has two optional extra logins (see [Optional Darling logins](#optional-darling-logins)). The deprecated Full edition's install/least-privilege grants are in [deprecated/Dashboard/README.md](../deprecated/Dashboard/README.md).

### FinOps Index Analysis (per-database grants)

Applies to **all editions**. The FinOps Index Analysis tab runs `sp_IndexCleanup` against each user database you ask it to inspect, executing as your app login. The server-level grants above (`VIEW SERVER STATE` and friends) are *not* sufficient on their own — the login also needs a user mapping in every user database it will analyze, plus `VIEW DATABASE STATE`, `VIEW DEFINITION`, and `SELECT` on `sys.sql_expression_dependencies` in each.

The third grant is the easy one to miss: by default only members of `db_owner` have `SELECT` on `sys.sql_expression_dependencies`, and `VIEW DEFINITION` does not include it. `sp_IndexCleanup` queries that catalog view (via three-part name to the target database) when checking for computed columns and check constraints that reference UDFs, so the failure only surfaces on databases that actually have those — which is why a smoke-test database may pass and a real workload database fails with `Msg 229`.

For each target user database:

```sql
USE [YourTargetDatabase];
CREATE USER [YourLogin] FOR LOGIN [YourLogin];
GRANT VIEW DATABASE STATE                       TO [YourLogin];
GRANT VIEW DEFINITION                           TO [YourLogin];
GRANT SELECT ON sys.sql_expression_dependencies TO [YourLogin];
```

Or apply it to every online, writable user database with a cursor over `sys.databases`. It skips single-user databases and databases your login cannot open. If a database still fails, the script prints a `Skipped` message for it and goes on to the next. Set `@login` to your login's name at the top, and double any `'` in the name inside `N'...'`. Each database receives the name as a parameter, so a bracket or a question mark in the name needs no other escaping:

```sql
DECLARE
    @login sysname = N'YourLogin',
    @database_name sysname,
    @procedure nvarchar(600),
    @sql nvarchar(max) = N'
DECLARE
    @statement nvarchar(max) = N'''';

IF NOT EXISTS
(
    SELECT
        1/0
    FROM sys.database_principals AS dp
    WHERE dp.name = @login
)
BEGIN
    SET @statement += N''CREATE USER '' + QUOTENAME(@login) + N'' FOR LOGIN '' + QUOTENAME(@login) + N'';'';
END;

SET @statement += N''GRANT VIEW DATABASE STATE TO '' + QUOTENAME(@login) + N'';'';
SET @statement += N''GRANT VIEW DEFINITION TO '' + QUOTENAME(@login) + N'';'';
SET @statement += N''GRANT SELECT ON sys.sql_expression_dependencies TO '' + QUOTENAME(@login) + N'';'';

EXECUTE sys.sp_executesql
    @statement;';

DECLARE
    database_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT
        d.name
    FROM sys.databases AS d
    WHERE d.database_id > 4
    AND   d.state = 0
    AND   DATABASEPROPERTYEX(d.name, N'Updateability') = N'READ_WRITE'
    AND   d.user_access <> 1
    AND   HAS_DBACCESS(d.name) = 1
    ORDER BY
        d.database_id;

OPEN database_cursor;

FETCH NEXT
FROM database_cursor
INTO @database_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    PRINT N'Granting in ' + QUOTENAME(@database_name);
    SET @procedure = QUOTENAME(@database_name) + N'.sys.sp_executesql';
    BEGIN TRY
        EXECUTE @procedure
            @sql,
            N'@login sysname',
            @login = @login;
    END TRY
    BEGIN CATCH
        PRINT N'Skipped ' + QUOTENAME(@database_name) + N': ' + ERROR_MESSAGE();
    END CATCH;

    FETCH NEXT
    FROM database_cursor
    INTO @database_name;
END;

CLOSE database_cursor;
DEALLOCATE database_cursor;
```

**Symptoms if missing.** Two distinct failure modes depending on which grant is absent:

- *No user mapping in the target database* — `sp_IndexCleanup` can hang at 100% CPU with no waits and never return, instead of failing fast with `Msg 916`. It's a SQL Server engine bug where a permission check at execute time is misclassified as "this plan needs to be recompiled," producing an infinite recompile loop. Reproduces on SQL Server 2016 SP3 through 2025 CU4.
- *User is mapped with `VIEW DATABASE STATE` + `VIEW DEFINITION` but no `SELECT` on `sys.sql_expression_dependencies`* — fails fast with `Msg 229, Level 14, State 5: The SELECT permission was denied on the object 'sql_expression_dependencies'` the moment a database with a UDF-bound computed column or check constraint is reached.

Adding all three grants eliminates both. See issue [#915](https://github.com/erikdarlingdata/PerformanceMonitor/issues/915) for the full diagnosis.

### Azure SQL Database (Lite / Darling)

Azure SQL Database doesn't support server-level logins. Create a **contained database user** directly on the target database:

```sql
-- Connect to your target database (not master)
CREATE USER [YourLogin] WITH PASSWORD = 'YourStrongPassword';
GRANT VIEW DATABASE STATE TO [YourLogin];
GRANT VIEW DEFINITION     TO [YourLogin];
GRANT ALTER ANY DATABASE EVENT SESSION TO [YourLogin];
```

The third grant, `ALTER ANY DATABASE EVENT SESSION`, lets the monitor create its Extended Events sessions in the database. Without it, the Extended Events collectors have no session to read.

The contained user cannot read the tempdb space DMVs, and no grant changes that. The `tempdb_stats` collector is refused on every run, and Collection Health shows `NO_PERMISSIONS` for it. To collect tempdb data, create a server login in `master`, add it to the `##MS_ServerStateReader##` server role, and map it into the database. A contained user cannot be put in that role.

For [Managed Identity or Service Principal](authentication.md#authentication) authentication, create the contained user from the identity's display name instead of a password — an Azure AD admin must already be configured on the logical server:

```sql
-- Connect to your target database (not master)
CREATE USER [your-managed-identity-or-app-registration-name] FROM EXTERNAL PROVIDER;
GRANT VIEW DATABASE STATE TO [your-managed-identity-or-app-registration-name];
GRANT VIEW DEFINITION     TO [your-managed-identity-or-app-registration-name];
GRANT ALTER ANY DATABASE EVENT SESSION TO [your-managed-identity-or-app-registration-name];
```

For a large fleet, grant an Entra **group** instead of provisioning each identity individually where your architecture allows it. SQL Agent and msdb are not available on Azure SQL Database — those collectors are skipped automatically.

> `VIEW DEFINITION` is the database-scoped form of the catalog-visibility grant explained above: without it, `sys.tables` / `sys.indexes` return zero rows to the index and object collectors — silently, not as an error. `VIEW DATABASE STATE` is what lets the `sys.dm_os_*` DMVs return server hardware inventory (CPU count, physical memory, socket and core topology) and memory metrics on each monitored database. If the contained user lacks it, edition, version, and storage still resolve from permission-free scalars, so the FinOps **Server Inventory** grid keeps the server's row and shows a non-alarming "Hardware Note" that hardware inventory is unavailable, instead of dropping the entire row (#1535).

### Azure SQL Managed Instance

Works like on-premises. Use server-level logins with `VIEW SERVER STATE`. SQL Agent is available.

### AWS RDS for SQL Server

For ongoing collection, `VIEW SERVER STATE` and msdb access work the same as on-premises, but `sp_configure` is not available (use RDS Parameter Groups instead — see [Platform Notes](platform-notes.md#platform-notes)).

### PostgreSQL targets (Darling)

Only Darling monitors PostgreSQL servers. Lite does not. The SQL Server grants above do not apply to a PostgreSQL target. A PostgreSQL target needs these:

- `pg_monitor` on the monitoring role.
- `pg_read_all_data` on PostgreSQL 14 and later.
- `pg_read_server_files` and `EXECUTE` on `pg_read_file`, on self-hosted targets only. Three collectors read the server log with them.
- The `pg_stat_statements` extension, for the `pg_statement_stats` collector.

The exact statements are in the Darling operator guide, in [Permissions on a PostgreSQL target](darling/postgresql-targets.md#permissions-on-a-postgresql-target).

### Optional Darling logins

Darling can use two more logins, each with its own grants. Neither is part of the grant block above. The monitoring login stays read-only.

- The remediation login runs the changes that an operator or an agent starts. These are: force or unforce a Query Store plan, evict a cached plan, and kill a session. Its grants are in the Darling operator guide, in [Configuration Reference](darling/configuration.md#servers-array-at-least-one-entry), under "Least privilege for the remediation credential".
- The "Test an index" login is for PostgreSQL targets only. The check never runs under the monitoring login. The setup is in [Testing an index on PostgreSQL](darling/configuration.md#testing-an-index-on-postgresql).
