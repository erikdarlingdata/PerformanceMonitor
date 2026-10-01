using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// The SQL and counting shared by the blocking and deadlock facts and the anomaly spike detectors for an
/// Azure SQL Database master target: events of databases monitored as their own targets are skipped.
/// A null or empty list produces no predicate and no parameters, so the SQL is byte-identical to the unscoped form.
/// </summary>
internal static class SeparatelyMonitoredScope
{
    /// <summary>
    /// A predicate fragment (leading AND) that skips rows whose <c>database_name</c> is in the list,
    /// case-insensitively; a NULL database passes. Parameters start at <paramref name="firstParameter"/>.
    /// </summary>
    public static string BprFilter(IReadOnlyList<string>? databases, int firstParameter)
    {
        if (databases is not { Count: > 0 }) return string.Empty;
        var names = string.Join(", ", databases.Select((_, i) => "lower($" + (firstParameter + i) + ")"));
        return " AND (database_name IS NULL OR lower(database_name) NOT IN (" + names + "))";
    }

    public static void AddParameters(DbCommand command, IReadOnlyList<string>? databases)
    {
        if (databases is not { Count: > 0 }) return;
        foreach (var d in databases)
            command.Parameters.Add(new DuckDBParameter { Value = d });
    }

    /// <summary>
    /// The SQL term for a deadlock row that is provably not wholly inside the list: the row's database (the
    /// event's database on the telemetry arm) is set, is not the connection's own master (which can be a
    /// fallback stamp, so those rows go to the graph check), and is not in the list.
    /// </summary>
    public static string DeadlockOutsideSql(IReadOnlyList<string> databases, int firstParameter)
    {
        var names = string.Join(", ", databases.Select((_, i) => "lower($" + (firstParameter + i) + ")"));
        return "database_name IS NOT NULL AND lower(database_name) <> 'master' AND lower(database_name) NOT IN (" + names + ")";
    }

    /// <summary>
    /// Counts the window's deadlocks that are NOT wholly inside the list (the engine's every-process rule,
    /// <see cref="DeadlockGraphDatabases.AllIn"/>). A deadlock with no graph is counted. A row database
    /// that is set, not master and not in the list already proves the deadlock is not wholly inside, so those
    /// are counted in SQL and only the remaining deadlocks have their graph read and parsed.
    /// </summary>
    public static async Task<long> CountDeadlocksAsync(
        DuckDBConnection connection, int serverId, DateTime start, DateTime end, bool inclusiveEnd,
        IReadOnlyList<string> databases, CancellationToken token)
    {
        using var command = connection.CreateCommand();
        return await CountDeadlocksAsync(command, serverId, start, end, inclusiveEnd, databases, token);
    }

    /// <summary>The same count on a connection held under the store's read lock (the overview card's).</summary>
    public static async Task<long> CountDeadlocksAsync(
        LockedConnection connection, int serverId, DateTime start, DateTime end, bool inclusiveEnd,
        IReadOnlyList<string> databases, CancellationToken token)
    {
        using var command = connection.CreateCommand();
        return await CountDeadlocksAsync(command, serverId, start, end, inclusiveEnd, databases, token);
    }

    private static async Task<long> CountDeadlocksAsync(
        DuckDBCommand command, int serverId, DateTime start, DateTime end, bool inclusiveEnd,
        IReadOnlyList<string> databases, CancellationToken token)
    {
        var window = "server_id = $1 AND deadlock_time >= $2 AND deadlock_time " + (inclusiveEnd ? "<=" : "<") + " $3";
        var outside = DeadlockOutsideSql(databases, 4);
        command.CommandText = "SELECT CASE WHEN " + outside + " THEN NULL ELSE deadlock_graph_xml END, "
            + "CASE WHEN " + outside + " THEN 1 ELSE 0 END FROM " + StoredEventCopies.Deadlocks(window) + " AS dl";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = start });
        command.Parameters.Add(new DuckDBParameter { Value = end });
        AddParameters(command, databases);
        long count = 0;
        using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            if (Convert.ToInt32(reader.GetValue(1)) == 1) { count++; continue; }
            var xml = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (!DeadlockGraphDatabases.AllIn(xml, databases)) count++;
        }
        return count;
    }
}
