/*
Widen sql_server_version and sql_server_edition columns in config.installation_history
Some @@VERSION strings exceed 255 characters (#712)
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
