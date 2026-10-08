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

    /// <summary>
    /// Azure SQL Database, a registration with read-only intent: the registration's own read-only connection reads whether the
    /// session's definition is visible on its replica and whether the session runs there. The ensure opens a connection
    /// without read-only intent only when this finds the definition missing (#4961).
    /// </summary>
    Check,

    /// <summary>
    /// Azure SQL Database, a registration with read-only intent: the registration's own read-only connection stops the
    /// session, when it is running there. A session that runs on a read-only replica is stopped over a connection to that
    /// replica, and only then dropped over a connection to the primary (#4961).
    /// </summary>
    Stop,

    /// <summary>
    /// Drops the session. One drop over the registration's own connection for a registration without read-only intent and on
    /// every server-scoped engine. For a registration with read-only intent on Azure SQL Database, the drop of the
    /// definition over a connection without read-only intent, after <see cref="Stop"/> (#4961).
    /// </summary>
    Drop,
}

/// <summary>
/// What a registration with read-only intent finds on its own replica for the long-query session in one Azure SQL Database
/// database (#4961): whether the definition is visible there, and whether the session runs there.
/// </summary>
/// <param name="DefinitionExists">The session's definition is in <c>sys.database_event_sessions</c> on the replica.</param>
/// <param name="Running">The session runs on the replica (<c>sys.dm_xe_database_sessions</c>).</param>
public readonly record struct LongQueryTraceReplicaState(bool DefinitionExists, bool Running);
