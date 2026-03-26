/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from 2.4.0 to 2.5.0
Widen config.installation_history.sql_server_version from nvarchar(255) to nvarchar(512).
Idempotent: safe to run multiple times.
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

IF OBJECT_ID(N'config.installation_history', N'U') IS NOT NULL
BEGIN
    IF EXISTS
    (
        SELECT
            1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = N'config'
        AND   TABLE_NAME = N'installation_history'
        AND   COLUMN_NAME = N'sql_server_version'
        AND   DATA_TYPE = N'nvarchar'
        AND   CHARACTER_MAXIMUM_LENGTH BETWEEN 1 AND 511
    )
    BEGIN
        ALTER TABLE
            config.installation_history
        ALTER COLUMN
            sql_server_version nvarchar(512) NOT NULL;

        PRINT 'Widened config.installation_history.sql_server_version to nvarchar(512).';
    END
    ELSE
    BEGIN
        PRINT 'config.installation_history.sql_server_version already nvarchar(512) or wider; skipping.';
    END;
END
ELSE
BEGIN
    PRINT 'Table config.installation_history does not exist; skipping.';
END;
GO
