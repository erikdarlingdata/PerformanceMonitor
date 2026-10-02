/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The collection-log note for a per-database run whose database list came back empty (#4961).
///
/// <para><b>What it fixes.</b> A per-database run with no database to read recorded SUCCESS with 0 rows and no
/// note, which reads as "nothing ran" rather than "nothing was left to read". Two things empty the list: a
/// logical server whose every user database is monitored as its own server (the read leaves those to their own
/// registrations, <see cref="ICollectorDefinition{TRow}.SkipsSeparatelyMonitoredDatabases"/>), and a server
/// whose every database is excluded. The status stays SUCCESS, since nothing failed; the note says why nothing
/// was read.</para>
///
/// <para><b>Wording.</b> Neither note carries the empty-enumeration marker. The health bands add "target has
/// user databases" to a note that does, and that would be wrong here: the databases exist, they are read
/// somewhere else or by choice not read at all. A server whose every run carries one of these notes reads
/// "note (all N runs)" with no qualifier.</para>
///
/// <para>Shared by Lite and Darling so the two products cannot word the same situation differently.</para>
/// </summary>
public static class EmptyDatabaseListNote
{
    /// <summary>Every database the server lists is monitored as its own server, so this registration reads none.</summary>
    public const string EverySeparatelyMonitored = "no database read: every user database is monitored as its own server";

    /// <summary>Every database on the server is excluded, so there is none to read.</summary>
    public const string EveryExcluded = "no database read: every database is excluded";

    /// <summary>
    /// The note for one per-database run, or null when the run has nothing to explain.
    ///
    /// <para>Null when at least one database was read, and null when the list is empty for a reason the caller
    /// cannot name: no exclusions are configured, or a database scope narrowed the list (the scope may be what
    /// emptied it, and a note that blames the exclusions would then be wrong).</para>
    /// </summary>
    /// <param name="listed">The databases the server listed, after its exclusions and any scope, before the separately monitored ones were left out. A read that never opens <c>master</c> leaves it out of this count too, so a list of <c>master</c> alone is empty.</param>
    /// <param name="read">The databases the run reads: <paramref name="listed"/> less the separately monitored ones.</param>
    /// <param name="skipsSeparatelyMonitored">Whether this collector leaves out the databases monitored as their own servers.</param>
    /// <param name="exclusionsConfigured">Whether the server has any excluded database.</param>
    /// <param name="databaseScoped">Whether this collector is scoped to named databases.</param>
    public static string? For(int listed, int read, bool skipsSeparatelyMonitored, bool exclusionsConfigured, bool databaseScoped)
    {
        if (read > 0)
        {
            return null;
        }

        if (skipsSeparatelyMonitored && listed > 0)
        {
            return EverySeparatelyMonitored;
        }

        return listed == 0 && exclusionsConfigured && !databaseScoped ? EveryExcluded : null;
    }
}
