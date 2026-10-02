/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Shared gate for <c>blocked_process_report</c> and <c>deadlocks</c> (#4200): both read a DEDICATED XE
/// ring-buffer session -- one event type each -- and cast+shred its <c>target_data</c> on every cycle, even
/// though a healthy estate finds nothing new almost every time. Measured on a production fleet (43 servers,
/// 6 h): 0.02% of blocked_process_report runs and 1.35% of deadlocks runs stored a row; the rest paid the
/// cast+shred to store nothing, about 4 hours a day of SQL time on the monitored servers for that one
/// estate.
///
/// <para><b>The signal.</b> <c>sys.dm_xe_session_targets.execution_count</c> (its Azure SQL DB
/// database-scoped twin, <c>sys.dm_xe_database_session_targets</c>) counts events the session has
/// delivered to the target. Because each of these two sessions carries exactly one event type (verified
/// against <c>DarlingXeSessions.cs</c>: one <c>ADD EVENT</c> per session), that counter is an exact "did
/// anything arrive since the last run?" signal, and reading it costs one integer with no XML materialized.
/// Both collectors read it in <c>BuildQuery</c>, BEFORE the cast, in the same batch, and skip the cast+shred
/// entirely when it has not moved.</para>
///
/// <para><b>The three shapes that always shred</b> -- the conservative default, the same rule
/// <see cref="QueryStoreOpenIntervalState"/> uses for its own stale-or-absent stamp: the counter is
/// unreadable this cycle (NULL -- the session or target is missing, mid-provisioning, or the DMV join found
/// no row); there is no stored prior (first run, a restarted host, or a store that lost the row); or the
/// counter no longer AGREES with the stored prior, whether it grew (the ordinary case) or fell (the session
/// restarted or was recreated, so the ring buffer's contents no longer correspond to what the prior was
/// measured against). Only an exact match skips -- every other case runs the same full read the collector
/// always ran before this gate existed, so the rows stored are identical either way.</para>
/// </summary>
public static class XeShredGate
{
    /// <summary>
    /// The state key for a server-scoped session. Azure SQL DB's per-database sessions each get their own
    /// key via <see cref="KeyFor"/> -- one execution_count per database, because each database's
    /// database-scoped session dispatches independently (the same reason
    /// <c>PerDatabaseWatermarkColumn</c> exists on both collectors).
    /// </summary>
    public const string StateKey = "xe_execution_count";

    /// <summary>
    /// The state key for one database's session (Azure SQL DB), or the plain server-scoped
    /// <see cref="StateKey"/> when <paramref name="databaseName"/> is null.
    /// </summary>
    public static string KeyFor(string? databaseName) =>
        databaseName is null ? StateKey : StateKey + ":" + databaseName;

    /// <summary>
    /// The stored prior for this server (or, on Azure SQL DB, for <paramref name="databaseName"/>), or null
    /// when there is none. An absent key, a blank value and a value that does not parse as a bigint all
    /// read as "no prior" -- what a first run, a restarted host and a corrupt row all look like -- and all
    /// three take the conservative full-shred path in <see cref="ShouldShred"/>.
    /// </summary>
    public static long? ReadLast(IReadOnlyDictionary<string, string> state, string? databaseName)
    {
        if (state.TryGetValue(KeyFor(databaseName), out var raw)
            && long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
        {
            return value;
        }

        return null;
    }

    /// <summary>
    /// The pure decision, unit-testable without a server: shred unless BOTH the current and the prior
    /// execution_count are known and EQUAL. Anything else -- an unreadable current count, no prior, a
    /// count that grew, or one that fell -- shreds, which is the same full read every one of these
    /// collectors always ran before this gate existed. The actual gate lives server-side (an IF in the
    /// collector's own T-SQL, evaluated on the SAME values this method would be given), because the whole
    /// point is to skip the cast+shred without a round trip; this mirrors that IF for a test that needs no
    /// SQL Server.
    /// </summary>
    public static bool ShouldShred(long? currentExecutionCount, long? lastExecutionCount) =>
        currentExecutionCount is null
        || lastExecutionCount is null
        || currentExecutionCount.Value != lastExecutionCount.Value;

    /// <summary>Renders a current count for <see cref="CollectorContext.PendingState"/>.</summary>
    public static string ToStateValue(long executionCount) =>
        executionCount.ToString(CultureInfo.InvariantCulture);
}
