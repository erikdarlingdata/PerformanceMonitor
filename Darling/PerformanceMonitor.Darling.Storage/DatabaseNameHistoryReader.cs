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
/// #5373: reads the database_id to database_name history a severe error is resolved against, so the Darling MCP
/// tool and the desktop viewer's Severe Errors tab (both reference this project) resolve an id to the name it carried
/// at the error's own time, with the one query. SQL Server reuses the id of a dropped database, so the server's
/// LATEST map names the wrong database for an old error. The rule itself lives in <see cref="DatabaseNameHistory"/>.
/// </summary>
public static class DatabaseNameHistoryReader
{
    /// <summary>
    /// The id-to-name change points, from the collected size snapshots (<c>database_size_stats</c> is the only
    /// collected table carrying BOTH database_id and database_name for every online DB), limited to the errors' own
    /// time range, in one query.
    /// <para><c>floor_snap</c> is the newest snapshot at or before the first error (a backward walk of the
    /// <c>(server_id, collection_time)</c> index that stops at its first row), <c>snap</c> is every snapshot from
    /// there to the last error, and <c>runs</c> keeps the rows where an id's name differs from its previous snapshot.
    /// The last arm covers an id with NO snapshot in that range: the oldest snapshot after the last error (stops at
    /// its first match per id, and runs only for the ids the range missed).</para>
    /// $1 server_id, $2 first error time, $3 last error time, $4 the int[] of database ids the errors carry.
    /// </summary>
    public const string Sql = """
        WITH floor_snap AS (
            SELECT COALESCE(MAX(collection_time), $2) AS floor_time
            FROM v_database_size_stats
            WHERE server_id = $1
            AND   collection_time <= $2
        ),
        snap AS (
            SELECT DISTINCT database_id, database_name, collection_time
            FROM v_database_size_stats
            WHERE server_id = $1
            AND   collection_time >= (SELECT floor_time FROM floor_snap)
            AND   collection_time <= $3
            AND   database_id = ANY($4)
            AND   database_name IS NOT NULL
        ),
        runs AS (
            SELECT
                database_id,
                database_name,
                collection_time,
                LAG(database_name) OVER (PARTITION BY database_id ORDER BY collection_time) AS previous_name
            FROM snap
        )
        SELECT database_id, database_name, collection_time
        FROM runs
        WHERE previous_name IS DISTINCT FROM database_name
        UNION ALL
        SELECT u.database_id, after_range.database_name, after_range.collection_time
        FROM unnest($4) AS u(database_id)
        CROSS JOIN LATERAL (
            SELECT d.database_name, d.collection_time
            FROM v_database_size_stats AS d
            WHERE d.server_id = $1
            AND   d.collection_time > $3
            AND   d.database_id = u.database_id
            AND   d.database_name IS NOT NULL
            ORDER BY d.collection_time
            LIMIT 1
        ) AS after_range
        WHERE NOT EXISTS (SELECT 1 FROM snap AS s WHERE s.database_id = u.database_id)
        """;

    /// <summary>
    /// Loads the history for the severe errors whose database ids are <paramref name="databaseIds"/> and whose
    /// times are <paramref name="times"/>. No real id to resolve, or no error time, returns
    /// <see cref="DatabaseNameHistory.Empty"/> without touching the store.
    /// </summary>
    public static async Task<DatabaseNameHistory> ReadAsync(
        NpgsqlDataSource postgres, int serverId, IEnumerable<int> databaseIds, IEnumerable<DateTime?> times,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var ids = databaseIds.Where(id => id != 0).Distinct().ToArray();
        if (ids.Length == 0 || DatabaseNameHistory.RangeOf(times) is not { } range)
            return DatabaseNameHistory.Empty;

        var changes = new List<DatabaseNameHistory.Change>();
        await using var command = postgres.CreateCommand(Sql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(range.Min, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(range.Max, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = ids });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.IsDBNull(0) || reader.IsDBNull(1) || reader.IsDBNull(2))
                continue;
            changes.Add(new DatabaseNameHistory.Change(reader.GetInt32(0), reader.GetString(1), reader.GetDateTime(2)));
        }

        return new DatabaseNameHistory(changes);
    }
}
