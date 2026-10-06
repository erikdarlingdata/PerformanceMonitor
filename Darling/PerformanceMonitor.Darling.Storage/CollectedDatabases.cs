/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #5245: the databases the store has collected for one server, the list both pickers offer. The desktop viewer's
/// Excluded Databases picker and the web viewer's database filter (<c>GET /api/server-databases</c>) both run
/// <see cref="NamesSql"/>, so the two lists cannot drift. It lives here, in the project both of them reference,
/// rather than in either one.
/// </summary>
public static class CollectedDatabases
{
    /// <summary>
    /// DISTINCT user-database names the store has collected for one server, from either the database
    /// config snapshot or the per-file size stats (UNION dedupes across both), system databases removed,
    /// ordered by name. $1 server_id.
    ///
    /// <para><b>Each arm is DISTINCT on its own.</b> The query reads every retained row of both views for the server,
    /// and a bare <c>UNION</c> of them makes the planner sort the whole input (about 2.2 million
    /// <c>v_database_size_stats</c> rows for 500 databases at the hourly, 90-day default: a 23 MB external merge sort,
    /// a 4.4 s median on a rig). Deduplicating inside each arm lets it hash-aggregate the rows down to the few hundred
    /// names before the union (0.26 s median, same rows). Bounding both arms to the retention window instead was
    /// measured and gains almost nothing at the worst case (3.4 s), because that window IS the table. #5245.</para>
    /// </summary>
    public const string NamesSql = """
        SELECT database_name
        FROM (
            SELECT DISTINCT database_name FROM v_database_config WHERE server_id = $1
            UNION
            SELECT DISTINCT database_name FROM v_database_size_stats WHERE server_id = $1
        ) AS d
        WHERE database_name IS NOT NULL
        AND   database_name NOT IN ('master', 'model', 'msdb', 'tempdb')
        ORDER BY database_name
        """;
}
