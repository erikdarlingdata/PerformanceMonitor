# Install Scripts

SQL scripts that create and configure the PerformanceMonitor database for the Full Edition. The CLI installer (`Installer/`) and Dashboard's Add Server dialog execute these scripts in order against the target SQL Server instance.

Scripts create the database, collection tables, configuration tables, stored procedures for each collector, SQL Agent jobs, reporting views, and data retention logic. Each script is idempotent — tables use `IF NOT EXISTS` guards and procedures use `CREATE OR ALTER`, so re-running the installer on an existing installation is safe and preserves collected data.

Tested on SQL Server 2016, 2017, 2019, 2022, and 2025.
