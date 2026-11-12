# Install Scripts

> **The Full Edition is deprecated.** These scripts belong to it. No release since v3.3.0 includes the Full Edition, but it is still supported for existing users. New deployments use [Lite](../Lite/README.md) or [Darling](../Darling/README.md).

SQL scripts that create and configure the PerformanceMonitor database for the Full Edition. The CLI installer and the Add Server dialog of the Full Dashboard execute these scripts in order against the target SQL Server instance. The source of both apps is frozen in [`deprecated/dashboard-installer-source.zip`](../deprecated/README.md).

Scripts create the database, collection tables, configuration tables, stored procedures for each collector, SQL Agent jobs, reporting views, and data retention logic. Each script is idempotent — tables use `IF NOT EXISTS` guards and procedures use `CREATE OR ALTER`, so re-running the installer on an existing installation is safe and preserves collected data.

Tested on SQL Server 2016, 2017, 2019, 2022, and 2025.
