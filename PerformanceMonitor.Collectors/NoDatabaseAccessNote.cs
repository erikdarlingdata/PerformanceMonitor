// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The shared answer to "an online user database the monitoring login cannot enter" (#6889). Every per-database list query
/// screens with <c>HAS_DBACCESS(d.name) = 1</c>, which keeps the 916 error out of the cycle but also drops the database without
/// a word: a Query Store read then comes back as an ordinary empty result. These pieces let each list query report those
/// databases through the existing probe-failure channel (the enumeration's second result set, or a payload collector's trailing
/// one) and let <see cref="EnumeratedCollectorDriver.BuildNote(bool, IReadOnlyList{EnumerationProbeFailure}, bool)"/> turn them
/// into ONE sentence on the run's SUCCESS row. <see cref="CollectorRuntimePrecondition.HealthyEmptyMessage"/> already refuses to
/// call an empty read a true negative when the newest SUCCESS run carries a note, so no new store table is needed.
/// </summary>
public static class NoDatabaseAccessNote
{
    /// <summary>The probe-failure <c>error_text</c> that marks a no-access row. The note builder partitions on it.</summary>
    public const string ProbeErrorText = "the monitoring login has no user in this database";

    /// <summary>Appended to a list query's database scope for the normal list: databases the login can enter.</summary>
    public const string AccessiblePredicate = "AND   HAS_DBACCESS(d.name) = 1";

    /// <summary>
    /// Appended to the same scope for the no-access probe. Online user databases only: the system databases and
    /// <c>rdsadmin</c> (which no customer login enters on Amazon RDS) never count.
    /// </summary>
    public const string InaccessiblePredicate =
        "AND   HAS_DBACCESS(d.name) = 0 AND d.database_id > 4 AND d.database_id < 32761 AND d.name <> N'rdsadmin'";

    /// <summary>The projection of the no-access probe: the probe-failure contract's (name, error_text) pair.</summary>
    public const string ProbeProjection = "SELECT /* PerformanceMonitorLite */ d.name, N'" + ProbeErrorText + "'";

    /// <summary>
    /// For a plain list query: the opening of the statement that records every candidate database with its access answer
    /// once, in the collector's OWN scope (its WHERE, minus the access screen), so the normal list and the no-access probe
    /// can never disagree about which databases are in scope.
    /// </summary>
    public const string ScopeProjection = @"DECLARE
    @scope TABLE (name sysname, database_id integer, has_access bit);

INSERT @scope (name, database_id, has_access)
SELECT /* PerformanceMonitorLite */
    d.name,
    d.database_id,
    HAS_DBACCESS(d.name)";

    /// <summary>For a plain list query: the two result sets after <see cref="ScopeProjection"/>'s statement. The list, then the probe-failure rows.</summary>
    public const string ScopeResults = @"

SELECT /* PerformanceMonitorLite */
    s.name
FROM @scope AS s
WHERE s.has_access = 1
ORDER BY
    s.name;

SELECT /* PerformanceMonitorLite */
    s.name,
    N'" + ProbeErrorText + @"'
FROM @scope AS s
WHERE s.has_access = 0
AND   s.database_id > 4
AND   s.database_id < 32761
AND   s.name <> N'rdsadmin'
ORDER BY
    s.name;";

    /// <summary>
    /// For a cursor loop over <c>@db</c> (cursor <c>db_check</c>) that fills <c>@probe_failures</c>: the top of the loop body.
    /// The cursor no longer screens with HAS_DBACCESS, so the databases it skips are the ones reported.
    /// </summary>
    public const string LoopGuardDbCheck = @"
    IF HAS_DBACCESS(@db) = 0
    BEGIN
        IF DB_ID(@db) > 4 AND DB_ID(@db) < 32761 AND @db <> N'rdsadmin'
        BEGIN
            INSERT @probe_failures (name, error_text)
            VALUES (@db, N'" + ProbeErrorText + @"');
        END;

        FETCH NEXT
        FROM db_check
        INTO @db;

        CONTINUE;
    END;
";

    /// <summary>The same guard for the cursor <c>db_cursor</c> over <c>@db_name</c> (database_size_stats).</summary>
    public const string LoopGuardDbCursor = @"
    IF HAS_DBACCESS(@db_name) = 0
    BEGIN
        IF DB_ID(@db_name) > 4 AND DB_ID(@db_name) < 32761 AND @db_name <> N'rdsadmin'
        BEGIN
            INSERT @probe_failures (name, error_text)
            VALUES (@db_name, N'" + ProbeErrorText + @"');
        END;

        FETCH NEXT FROM db_cursor INTO @db_name;

        CONTINUE;
    END;
";

    /// <summary>The most database names the sentence spells out.</summary>
    public const int MaxNamed = 10;

    /// <summary>
    /// The single sentence for the databases that failed <see cref="InaccessiblePredicate"/>, or null when none did.
    /// On Amazon RDS it adds the statement that fixes it, because RDS refuses <c>CONNECT ANY DATABASE</c> and a user in each
    /// database is the only way in.
    /// </summary>
    public static string? Sentence(IReadOnlyList<string> databases, bool awsRds)
    {
        ArgumentNullException.ThrowIfNull(databases);
        if (databases.Count == 0)
        {
            return null;
        }

        var named = string.Join(", ", databases.Take(MaxNamed));
        if (databases.Count > MaxNamed)
        {
            named += string.Create(CultureInfo.InvariantCulture, $", and {databases.Count - MaxNamed} more");
        }

        var text = string.Create(
            CultureInfo.InvariantCulture,
            $"{databases.Count} online user database(s) not collected: the monitoring login has no user in {named}, so nothing was collected from them");

        return awsRds
            ? text + ". Amazon RDS has no CONNECT ANY DATABASE, so create a user in each: CREATE USER [login] FOR LOGIN [login]"
            : text;
    }
}
