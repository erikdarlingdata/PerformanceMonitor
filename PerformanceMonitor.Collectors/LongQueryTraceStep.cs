/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// One open-and-act step of the long-query trace's ensure: what runs on one connection. Lite and Darling name the same
/// steps, so a test can see which connection string each one opens, with or without read-only intent (#4961).
/// </summary>
public enum LongQueryTraceStep
{
    /// <summary>
    /// The registration's own connection creates the session when it is missing and starts it when it is stopped. Every
    /// registration on a server-scoped engine, and a registration without read-only intent on Azure SQL Database.
    /// </summary>
    CreateAndStart,

    /// <summary>
    /// Azure SQL Database, a registration with read-only intent: a connection without read-only intent creates the
    /// session's definition when it is missing, and does not start it there. A session cannot be created on a read-only
    /// replica, and the definition replicates from the primary.
    /// </summary>
    CreateDefinition,

    /// <summary>
    /// Azure SQL Database, a registration with read-only intent: the registration's own read-only connection starts the
    /// session. Run state is per replica, so the session runs only where it was started.
    /// </summary>
    Start,
}
