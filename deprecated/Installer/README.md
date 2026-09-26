# Performance Monitor CLI Installer (Full Edition)

> **The "Full" edition — a `PerformanceMonitor` database installed on the target SQL Server with T-SQL collectors running via SQL Agent — is deprecated.** As of v3.3.0 it is no longer included in release assets, but it is supported for existing users and remains buildable from the repo. New deployments should use **[Lite](../Lite/README.md)** (portable desktop app, nothing installed on the server) or **[Darling](../Darling/README.md)** (headless service + viewer). This CLI installer remains for those who still run the Full edition. See the [root README](../README.md) for the current editions.

Self-contained console application that installs the `PerformanceMonitor` database, collector stored procedures, reporting views, and SQL Agent jobs on a target SQL Server instance. The executable bundles the .NET 10.0 runtime — nothing to install on the machine running it.

Run it from the repository root (the directory containing `install/`); the installer searches for `install/` from the current directory.

## Install

Windows Authentication:

```
PerformanceMonitorInstaller.exe YourServerName
```

SQL Authentication:

```
PerformanceMonitorInstaller.exe YourServerName sa YourPassword
```

Entra ID (MFA) Authentication:

```
PerformanceMonitorInstaller.exe YourServerName --entra user@domain.com
```

Entra managed identity / service principal (non-interactive, for Azure SQL Managed Instance or other AAD-enabled targets):

```
PerformanceMonitorInstaller.exe YourMI.database.windows.net --managed-identity
PerformanceMonitorInstaller.exe YourMI.database.windows.net --managed-identity=MI_CLIENT_ID
PerformanceMonitorInstaller.exe YourMI.database.windows.net --service-principal APP_CLIENT_ID
```

Bare `--managed-identity` uses the system-assigned identity; pass a client id (`--managed-identity=ID` or `--managed-identity ID`) for a user-assigned one. `--service-principal` reads the client secret from the `PM_AZURE_CLIENT_SECRET` environment variable (never the command line).

Clean reinstall (drops existing database and all collected data):

```
PerformanceMonitorInstaller.exe YourServerName --reinstall
PerformanceMonitorInstaller.exe YourServerName sa YourPassword --reinstall
```

Custom data/log file locations (applied only when the database is first created):

```
PerformanceMonitorInstaller.exe YourServerName --data-path D:\SQLData --log-path E:\SQLLogs
```

Uninstall (removes database, Agent jobs, and XE sessions):

```
PerformanceMonitorInstaller.exe YourServerName --uninstall
PerformanceMonitorInstaller.exe YourServerName sa YourPassword --uninstall
```

The installer automatically tests the connection, checks the SQL Server version (2016+ required), executes SQL scripts, downloads community dependencies, creates SQL Agent jobs, and runs initial data collection. You can also install directly from the Dashboard's Add Server dialog.

**Air-gapped environments?** Place pre-downloaded community scripts (`sp_WhoIsActive.sql`, `DarlingData.sql`, `Install-All-Scripts.sql`) in a `community/` directory next to the installer. The installer uses local files when present and falls back to GitHub downloads otherwise.

## CLI Installer Options

| Option | Description |
|---|---|
| `SERVER` | SQL Server instance name (positional, required) |
| `USERNAME PASSWORD` | SQL Authentication credentials (positional, optional) |
| `--entra EMAIL` | Microsoft Entra ID interactive authentication (MFA) |
| `--managed-identity[=CLIENT_ID]` | Microsoft Entra managed identity (system-assigned, or user-assigned via `CLIENT_ID`) |
| `--service-principal CLIENT_ID` | Microsoft Entra service principal (client secret via `PM_AZURE_CLIENT_SECRET`) |
| `--reinstall` | Drop existing database and perform clean install |
| `--uninstall` | Remove database, Agent jobs, and XE sessions |
| `--reset-schedule` | Reset collection schedule to recommended defaults |
| `--preserve-jobs` | Keep existing SQL Agent job schedules during upgrade |
| `--encrypt=optional\|mandatory\|strict` | Connection encryption level (default: mandatory) |
| `--trust-cert` | Trust server certificate without validation (default: require valid cert) |
| `--data-path DIR` | Server-side directory for the data (`.mdf`) file (used only on first install) |
| `--log-path DIR` | Server-side directory for the log (`.ldf`) file (used only on first install) |
| `--help` | Show usage information and exit |

> **Custom file locations:** `--data-path` / `--log-path` set where SQL Server places the PerformanceMonitor data and log files. They take effect **only when the database is first created** — if the database already exists they are ignored. Either flag may be supplied independently; an omitted one falls back to the instance default (`SERVERPROPERTY('InstanceDefaultDataPath')` / `InstanceDefaultLogPath`). The directory is a path **on the SQL Server host** and must already exist, with the SQL Server service account holding write permission. Both `--data-path D:\SQLData` and `--data-path=D:\SQLData` forms are accepted; quote paths containing spaces. Not applicable to Azure SQL Managed Instance, which always uses its managed file layout.

**Environment variables:** Set `PM_SQL_PASSWORD` to avoid passing the SQL Auth password on the command line, and `PM_AZURE_CLIENT_SECRET` to supply the `--service-principal` client secret.

## Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Invalid arguments |
| `2` | Connection failed |
| `3` | Critical file failed (scripts 01–03) |
| `4` | Partial installation (non-critical failures) |
| `5` | Version check failed (SQL Server 2014 or earlier) |
| `6` | SQL files not found |
| `7` | Uninstall failed |
| `8` | Upgrade script failed |

## Post-Installation

1. Ensure SQL Server Agent is running — the collection job executes every minute.
2. Verify installation:

```sql
SELECT * FROM PerformanceMonitor.config.current_version;

SELECT TOP (20) *
FROM PerformanceMonitor.config.collection_log
ORDER BY collection_time DESC;
```

3. Install the Dashboard to view the data — see [`Dashboard/README.md`](../Dashboard/README.md).

For what gets installed, data retention, managed-platform behavior, tabs, troubleshooting, and permissions, see [`Dashboard/README.md`](../Dashboard/README.md).
