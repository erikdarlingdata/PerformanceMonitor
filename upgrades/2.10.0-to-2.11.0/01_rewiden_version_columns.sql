/*
Re-run installation_history version-column widening from the 2.4.0-to-2.5.0 upgrade.

Scope (release-notes guidance — keep this quiet):
    Servers that crossed 2.4 -> 2.5 BEFORE PR #828's fix shipped in v2.7.0 ran the
    widen script as a no-op (it executed against master, not PerformanceMonitor)
    and then advanced their installer_version past 2.5. The upgrade filter only
    selects folders where ToVersion > currentVersion, so the now-fixed script is
    never re-applied on those servers and their sql_server_version /
    sql_server_edition columns stay at nvarchar(255) -- which truncates SQL 2022+
    @@VERSION strings (~260 chars) on every install attempt.

    This is a silent remediation for that long tail. The body is identical to
    upgrades/2.4.0-to-2.5.0/01_widen_version_columns.sql and is idempotent: the
    IF EXISTS guard checks max_length = 510 (nvarchar(255) in bytes) so servers
    that are already widened, or that ran the fixed 2.4->2.5 upgrade cleanly,
    skip both ALTERs.

    For changelog purposes this is a one-line "fix": "Re-run installation_history
    column widening for servers that hit the no-op pre-v2.7.0 upgrade (#828)."
    Do not promote it as a new feature.
*/

SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;
SET IMPLICIT_TRANSACTIONS OFF;
SET STATISTICS TIME, IO OFF;
GO

USE PerformanceMonitor;
GO

IF EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'config.installation_history')
    AND   c.name = N'sql_server_version'
    AND   c.max_length = 510 /* nvarchar(255) = 510 bytes */
)
BEGIN
    ALTER TABLE config.installation_history
        ALTER COLUMN sql_server_version nvarchar(512) NOT NULL;

    PRINT 'Widened config.installation_history.sql_server_version to nvarchar(512)';
END;

IF EXISTS
(
    SELECT
        1/0
    FROM sys.columns AS c
    WHERE c.object_id = OBJECT_ID(N'config.installation_history')
    AND   c.name = N'sql_server_edition'
    AND   c.max_length = 510
)
BEGIN
    ALTER TABLE config.installation_history
        ALTER COLUMN sql_server_edition nvarchar(512) NOT NULL;

    PRINT 'Widened config.installation_history.sql_server_edition to nvarchar(512)';
END;
