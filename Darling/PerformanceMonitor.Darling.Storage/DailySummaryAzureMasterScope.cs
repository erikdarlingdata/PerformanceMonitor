/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #4925: the daily summary for an Azure SQL Database master target that has separately monitored databases.
/// A master's blocked-process reports and deadlocks include events from the sibling databases, which are
/// monitored (and counted) as their own targets, so the master's day counted them twice. This scopes the two
/// event CTEs of <see cref="DailySummarySql.RangeSql"/> the way the alert sweep and the fleet card scope them,
/// without touching the frozen statement.
///
/// <para>The scope is a pair of <c>FILTER</c> clauses, not <c>WHERE</c> clauses, so the day spine and the
/// signal-presence count stay exactly as they were: a day whose only events belonged to a sibling still exists
/// and still counts its sources as present. Parameters <c>$1</c>-<c>$4</c> keep their meaning and the list of
/// database names is <c>$5</c> (a <c>text[]</c>). The DMV blocking CTE is left alone: a master's DMV sees only
/// its own sessions, so its fallback count is already the master's.</para>
///
/// <para>Deadlocks whose row names an own database count in SQL. A row stamped NULL or <c>master</c> (or with a
/// sibling's name) may be a sibling-only graph, so those go through <see cref="GraphDeadlocksByDayAsync"/>,
/// which applies the every-process rule: a graph counts unless every process in it ran in a listed database.</para>
/// </summary>
public static class DailySummaryAzureMasterScope
{
    // The keep predicate for blocked-process reports: the alert collector's own (PgFactCollector.BlockingSqlSkippingSeparate), with the list at $5.
    private const string BprKeepPredicate =
        "(database_name IS NULL OR NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($5::text[]) x)))";

    // The deadlock rows that cannot be all-in: a named database that is not master and not a listed one
    // (PgFactCollector.DeadlockOutsideCountSql), with the list at $5.
    private const string DeadlockOutsidePredicate =
        "database_name IS NOT NULL AND lower(database_name) <> 'master' AND NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($5::text[]) x))";

    // The two CTE select lines exactly as they appear in DailySummarySql.RangeSql. One line each, so the
    // replace does not depend on the source file's line endings.
    private const string DeadlockSelectLine = "SELECT date_trunc('day', deadlock_time) AS d, COUNT(*) AS c";
    private const string BprSelectLine = "SELECT date_trunc('day', event_time) AS d, COUNT(*) AS c, MAX(wait_time_ms) AS max_wait_ms";

    /// <summary>The graphs the every-process rule still has to decide, one row per stored deadlock: the row's
    /// database is unknown, is master, or is a listed one. $1 server_id, $2/$3 the half-open window as
    /// <see cref="DailySummarySql.RangeSql"/> reads it, $4 the event-window floor for $2, $5 the list.</summary>
    public const string DeadlockGraphsByDaySql = @"
SELECT date_trunc('day', deadlock_time) AS d, deadlock_graph_xml
FROM v_deadlocks
WHERE server_id = $1 AND deadlock_time >= $2 AND deadlock_time < $3
AND   collection_time >= $4
AND   (database_name IS NULL OR lower(database_name) = 'master' OR lower(database_name) = ANY(SELECT lower(x) FROM unnest($5::text[]) x))";

    /// <summary>
    /// <paramref name="routedSql"/> (any tier's <see cref="DailySummarySql.RangeSql"/> text) with the blocked-process
    /// and deadlock counts scoped to the master's own events. Throws <see cref="InvalidOperationException"/> when
    /// either CTE is not found, so editing <see cref="DailySummarySql.RangeSql"/> without updating this fails loudly.
    /// The caller binds the list as <c>$5</c>.
    /// </summary>
    public static string Scope(string routedSql)
    {
        ArgumentNullException.ThrowIfNull(routedSql);

        var scoped = ReplaceOnce(routedSql, DeadlockSelectLine,
            "SELECT date_trunc('day', deadlock_time) AS d, COUNT(*) FILTER (WHERE " + DeadlockOutsidePredicate + ") AS c");
        scoped = ReplaceOnce(scoped, BprSelectLine,
            "SELECT date_trunc('day', event_time) AS d, COUNT(*) FILTER (WHERE " + BprKeepPredicate + ") AS c, "
            + "MAX(wait_time_ms) FILTER (WHERE " + BprKeepPredicate + ") AS max_wait_ms");
        return scoped;
    }

    private static string ReplaceOnce(string sql, string fragment, string replacement)
    {
        var at = sql.IndexOf(fragment, StringComparison.Ordinal);
        if (at < 0 || sql.IndexOf(fragment, at + fragment.Length, StringComparison.Ordinal) >= 0)
        {
            throw new InvalidOperationException(
                "Daily-summary master scoping found no single '" + fragment + "' to replace: DailySummarySql.RangeSql has drifted from DailySummaryAzureMasterScope (#4925).");
        }

        return sql.Remove(at, fragment.Length).Insert(at, replacement);
    }

    /// <summary>
    /// The deadlocks per UTC day that the every-process rule keeps from the rows <see cref="Scope"/> leaves to the
    /// graph check: one for each stored graph in [<paramref name="start"/>, <paramref name="end"/>) that is not
    /// wholly inside <paramref name="separate"/>. Add each day's number to the scoped statement's deadlock count.
    /// </summary>
    public static async Task<Dictionary<DateTime, long>> GraphDeadlocksByDayAsync(
        NpgsqlDataSource postgres, int serverId, DateTime start, DateTime end,
        IReadOnlyList<string> separate, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(separate);

        var byDay = new Dictionary<DateTime, long>();
        await using var command = postgres.CreateCommand(DeadlockGraphsByDaySql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(EventWindowFloor.For(start), DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = separate.ToArray() });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var xml = reader.IsDBNull(1) ? null : reader.GetString(1);
            if (DeadlockGraphDatabases.AllIn(xml, separate)) continue;

            var day = reader.GetDateTime(0).Date;
            byDay[day] = byDay.GetValueOrDefault(day) + 1;
        }

        return byDay;
    }

    /// <summary>The cache-key part for a list: empty when there is none, otherwise the names lowercased, sorted
    /// and joined, so two reads with different lists never share a cached block.</summary>
    public static string CacheScopeKey(IReadOnlyList<string>? list)
        => list is null || list.Count == 0
            ? ""
            : string.Join('\u001f', list.Select(name => name.ToLowerInvariant()).Distinct().OrderBy(name => name, StringComparer.Ordinal));
}
