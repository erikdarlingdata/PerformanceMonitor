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

    /// <summary>
    /// The first half of the one rule "an online user database, not a vendor database", written after an id expression:
    /// <c>d.database_id BETWEEN 5 AND 32760</c> leaves out the system databases and the reserved range above them. A
    /// <c>BETWEEN</c> so the expression (<c>DB_ID(@db)</c> in a loop guard) is written once. Every site that asks the question
    /// composes this with <see cref="NotVendorControlled"/> instead of retyping it, so the two vendor names cannot drift (#6889).
    /// </summary>
    public const string UserDatabaseIds = "BETWEEN 5 AND 32760";

    /// <summary>
    /// The second half of the rule, written after a name expression: not <c>rdsadmin</c> and not <c>gcloud_cloudsqladmin</c>,
    /// the names in <see cref="DatabaseScopeFilter.VendorControlledDatabaseNames"/>. No customer login enters either, and the
    /// lists that skip them (#1565) must not report them as databases the login cannot enter.
    /// </summary>
    public const string NotVendorControlled = "NOT IN (" + DatabaseScopeFilter.VendorControlledDatabaseNames + ")";

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
AND   s.database_id " + UserDatabaseIds + @"
AND   s.name " + NotVendorControlled + @"
ORDER BY
    s.name;";

    /// <summary>
    /// For a cursor loop over <c>@db</c> (cursor <c>db_check</c>) that fills <c>@probe_failures</c>: the top of the loop body.
    /// The cursor no longer screens with HAS_DBACCESS, so the databases it skips are the ones reported.
    /// </summary>
    public const string LoopGuardDbCheck = @"
    IF HAS_DBACCESS(@db) = 0
    BEGIN
        IF DB_ID(@db) " + UserDatabaseIds + @" AND @db " + NotVendorControlled + @"
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
        IF DB_ID(@db_name) " + UserDatabaseIds + @" AND @db_name " + NotVendorControlled + @"
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
    /// The names the note and the app-log line spell out: at most <see cref="MaxNamed"/>, then a count of the rest.
    /// One builder, so the SUCCESS-row sentence and the log line name the same databases the same way.
    /// </summary>
    public static string NamedList(IReadOnlyList<string> databases)
    {
        ArgumentNullException.ThrowIfNull(databases);
        var named = string.Join(", ", databases.Take(MaxNamed));
        if (databases.Count > MaxNamed)
        {
            named += string.Create(CultureInfo.InvariantCulture, $", and {databases.Count - MaxNamed} more");
        }

        return named;
    }

    /// <summary>
    /// The ONE app-log line for a run's no-access databases (Information, not Warning). The note on the SUCCESS row already
    /// carries the story, and a least-privilege login with no user in many databases is a posture that does not change, so
    /// writing a Warning per database per collector per cycle would bury the failures worth reading (#1823, #6889).
    /// </summary>
    public const string NoAccessLogTemplate =
        "{Collector} on '{Server}': {Count} online user database(s) not collected, the monitoring login has no user in {Databases}";

    /// <summary>
    /// Splits a run's probe failures into the no-access databases (their names, in order) and every other failure. The
    /// hosts log the first group as one Information line and keep the per-item Warning loop for the second.
    /// </summary>
    public static (List<string> NoAccess, List<EnumerationProbeFailure> Other) Partition(IReadOnlyList<EnumerationProbeFailure> probeFailures)
    {
        ArgumentNullException.ThrowIfNull(probeFailures);
        var noAccess = new List<string>();
        var other = new List<EnumerationProbeFailure>();
        foreach (var failure in probeFailures)
        {
            if (string.Equals(failure.Error, ProbeErrorText, StringComparison.Ordinal))
            {
                noAccess.Add(failure.Item);
            }
            else
            {
                other.Add(failure);
            }
        }

        return (noAccess, other);
    }

    /// <summary>
    /// The single sentence for the online user databases the login cannot enter (the rule <see cref="UserDatabaseIds"/> and <see cref="NotVendorControlled"/> state), or null when none did.
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

        var named = NamedList(databases);

        var text = string.Create(
            CultureInfo.InvariantCulture,
            $"{databases.Count} online user database(s) not collected: the monitoring login has no user in {named}, so nothing was collected from them");

        return awsRds
            ? text + ". Amazon RDS has no CONNECT ANY DATABASE, so create a user in each: CREATE USER [login] FOR LOGIN [login]"
            : text;
    }
}
