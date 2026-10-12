/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The long-query session that versions before #4961 shared between installs. Each upgraded install drops it once per
/// registration and database, then records the drop in collector_state so it never touches that session again.
/// </summary>
public static class LegacyLongQuerySession
{
    /// <summary>The collector name the drop record is kept under in collector_state.</summary>
    public const string StateCollector = "long_query_completions";

    /// <summary>The start of the drop record's state key. The database name follows it, empty on server scope.</summary>
    public const string StateKeyPrefix = "legacy_session_dropped:";

    /// <summary>The drop record's state key for one database. Server scope passes an empty name.</summary>
    public static string StateKey(string database) => StateKeyPrefix + database;
}
