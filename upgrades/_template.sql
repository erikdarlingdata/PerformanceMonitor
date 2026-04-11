/*
Copyright 2026 Darling Data, LLC
https://www.erikdarling.com/

Upgrade from X.Y.Z to X.Y.Z
<describe what this upgrade does and why>
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

/* upgrade logic here — must be idempotent */
