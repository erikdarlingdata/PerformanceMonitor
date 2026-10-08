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
    /// there to the last error, and <c>runs</c> keeps the rows where an id's name differs from its previous snapshot
    /// (ties on one instant break by name, so the result is fixed).</para>
    /// <para>The floor snapshot is server-wide, so an id missing from it (an offline, suspect or excluded database is
    /// not collected) gets <c>before_floor</c>: its newest row before the floor, for every such id in ONE pass. The
    /// last arm covers an id with NO row up to the last error: its oldest row after it, also one pass over only the
    /// ids still unfound.</para>
    /// <para>Both extra arms are bounded by <see cref="DatabaseNameHistory.LookbackDays"/> on the time index
    /// (<c>before_floor</c> back from the floor, <c>after_range</c> forward from the last error): an id that is never
    /// collected (an excluded database, id 32767) must not make every refresh scan the whole history. When one id has two
    /// names at one instant, the larger name in ordinal order wins in every arm.</para>
    /// $1 server_id, $2 first error time, $3 last error time, $4 the int[] of database ids the errors carry,
    /// $5 the look-back interval.
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
                LAG(database_name) OVER (PARTITION BY database_id ORDER BY collection_time, database_name COLLATE "C") AS previous_name
            FROM snap
        ),
        missing AS (
            SELECT u.database_id
            FROM unnest($4) AS u(database_id)
            WHERE NOT EXISTS
                  (
                      SELECT 1 FROM snap AS s
                      WHERE s.database_id = u.database_id
                      AND   s.collection_time = (SELECT floor_time FROM floor_snap)
                  )
        ),
        before_floor AS (
            SELECT DISTINCT ON (d.database_id) d.database_id, d.database_name, d.collection_time
            FROM v_database_size_stats AS d
            WHERE EXISTS (SELECT 1 FROM missing)
            AND   d.server_id = $1
            AND   d.collection_time < (SELECT floor_time FROM floor_snap)
            AND   d.collection_time >= (SELECT floor_time FROM floor_snap) - $5::interval
            AND   d.database_id IN (SELECT m.database_id FROM missing AS m)
            AND   d.database_name IS NOT NULL
            ORDER BY d.database_id, d.collection_time DESC, d.database_name COLLATE "C" DESC
        ),
        unfound AS (
            SELECT m.database_id
            FROM missing AS m
            WHERE NOT EXISTS (SELECT 1 FROM before_floor AS b WHERE b.database_id = m.database_id)
            AND   NOT EXISTS (SELECT 1 FROM snap AS s WHERE s.database_id = m.database_id)
        ),
        after_range AS (
            SELECT DISTINCT ON (d.database_id) d.database_id, d.database_name, d.collection_time
            FROM v_database_size_stats AS d
            WHERE EXISTS (SELECT 1 FROM unfound)
            AND   d.server_id = $1
            AND   d.collection_time > $3
            AND   d.collection_time <= $3 + $5::interval
            AND   d.database_id IN (SELECT f.database_id FROM unfound AS f)
            AND   d.database_name IS NOT NULL
            ORDER BY d.database_id, d.collection_time ASC, d.database_name COLLATE "C" DESC
        )
        SELECT database_id, database_name, collection_time
        FROM runs
        WHERE previous_name IS DISTINCT FROM database_name
        UNION ALL
        SELECT database_id, database_name, collection_time FROM before_floor
        UNION ALL
        SELECT database_id, database_name, collection_time FROM after_range
        """;

    /// <summary>
    /// The newest row per requested id, for an error with no time (it is named by its id's newest name). Read only
    /// when such an error is shown, in one pass over the last <see cref="DatabaseNameHistory.LookbackDays"/> days.
    /// $1 server_id, $2 the int[] of database ids, $3 the oldest time to look at.
    /// </summary>
    public const string NewestSql = """
        SELECT DISTINCT ON (database_id) database_id, database_name, collection_time
        FROM v_database_size_stats
        WHERE server_id = $1
        AND   database_id = ANY($2)
        AND   collection_time >= $3
        AND   database_name IS NOT NULL
        ORDER BY database_id, collection_time DESC, database_name COLLATE "C" DESC
        """;

    /// <summary>
    /// Loads the history for the severe errors with the given (database id, time) pairs. No real id to resolve returns
    /// <see cref="DatabaseNameHistory.Empty"/> without touching the store. An error with no time is resolved to its
    /// id's newest name, which takes one more read, made only when such an error is shown.
    /// </summary>
    public static async Task<DatabaseNameHistory> ReadAsync(
        NpgsqlDataSource postgres, int serverId, IEnumerable<(int? DatabaseId, DateTime? EventTime)> errors,
        int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var plan = DatabaseNameHistory.Plan(errors);
        if (plan.Ids.Length == 0)
            return DatabaseNameHistory.Empty;

        var changes = new List<DatabaseNameHistory.Change>();
        if (plan.Range is { } range)
        {
            await using var command = postgres.CreateCommand(Sql);
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(range.Min, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(range.Max, DateTimeKind.Unspecified) });
            command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = plan.Ids });
            command.Parameters.Add(new NpgsqlParameter<TimeSpan> { TypedValue = DatabaseNameHistory.LookbackWindow });
            await ReadChangesAsync(command, changes, cancellationToken);
        }

        if (plan.NeedsNewest)
        {
            await using var newest = postgres.CreateCommand(NewestSql);
            newest.CommandTimeout = commandTimeoutSeconds;
            newest.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            newest.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = plan.Ids });
            newest.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(DateTime.UtcNow - DatabaseNameHistory.LookbackWindow, DateTimeKind.Unspecified) });
            await ReadChangesAsync(newest, changes, cancellationToken);
        }

        return new DatabaseNameHistory(changes);
    }

    private static async Task ReadChangesAsync(NpgsqlCommand command, List<DatabaseNameHistory.Change> changes, CancellationToken cancellationToken)
    {
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.IsDBNull(0) || reader.IsDBNull(1) || reader.IsDBNull(2))
                continue;
            changes.Add(new DatabaseNameHistory.Change(reader.GetInt32(0), reader.GetString(1), reader.GetDateTime(2)));
        }
    }
}
