using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

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
        return "AND (database_name IS NULL OR lower(database_name) NOT IN (" + names + "))";
    }

    public static void AddParameters(DbCommand command, IReadOnlyList<string>? databases)
    {
        if (databases is not { Count: > 0 }) return;
        foreach (var d in databases)
            command.Parameters.Add(new DuckDBParameter { Value = d });
    }

    /// <summary>
    /// Counts the window's deadlocks that are NOT wholly inside the list (the engine's every-process rule,
    /// <see cref="DeadlockGraphDatabases.AllIn"/>). A deadlock with no graph is counted.
    /// </summary>
    public static async Task<long> CountDeadlocksAsync(
        DuckDBConnection connection, int serverId, DateTime start, DateTime end, bool inclusiveEnd,
        IReadOnlyList<string> databases, CancellationToken token)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT deadlock_graph_xml FROM v_deadlocks WHERE server_id = $1 AND collection_time >= $2 AND collection_time "
            + (inclusiveEnd ? "<=" : "<") + " $3";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = start });
        command.Parameters.Add(new DuckDBParameter { Value = end });
        long count = 0;
        using var reader = await command.ExecuteReaderAsync(token);
        while (await reader.ReadAsync(token))
        {
            var xml = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (!DeadlockGraphDatabases.AllIn(xml, databases)) count++;
        }
        return count;
    }
}
