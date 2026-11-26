/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The two statements that give the rows of a table their missing liveness rows one keyset slice at a time: the Query
/// Store text's seen table (<see cref="QueryStoreTextSeen.AdoptSlice"/>) and the two payload dimensions' stamp tables
/// (<see cref="PayloadDimensionSeen.AdoptSlice"/>). A source row is without a liveness row only when it was written with
/// the insert trigger off (a restore that disables triggers, a session in replica mode), and a prune that reads the
/// liveness table alone would keep such a row for good.
///
/// <para><b>Why slices.</b> The liveness table's foreign key cascades, so its keys are a subset of the source table's, and
/// finding the missing ones means comparing the two primary keys. Compared whole, that is a read of both indexes from
/// end to end, which grows with the table. A slice is the keys after a cursor up to the Nth key in key order: the first
/// statement (<see cref="UpperKeySql"/>) finds that Nth key with a read of the source table's primary key alone, and the
/// second (<see cref="InsertSql"/>) carries the range on both primary keys, so each side of the comparison starts at the
/// cursor and ends at the upper key.</para>
///
/// <para>The key columns are compared as a row in primary key order, which a multi-column primary key reads as one
/// range. The caller carries the upper key of one slice as the cursor of the next.</para>
/// </summary>
public sealed class LivenessAdoptSlice
{
    private readonly string _sourceTable;
    private readonly IReadOnlyList<string> _keyColumns;
    private readonly string _lastSeenColumn;

    /// <param name="sourceTable">The table whose rows need a liveness row, schema-qualified.</param>
    /// <param name="livenessTable">The liveness table, schema-qualified.</param>
    /// <param name="keyColumns">The primary key columns of both tables, in primary key order.</param>
    /// <param name="lastSeenColumn">The stamp column, of the same name in both tables.</param>
    public LivenessAdoptSlice(string sourceTable, string livenessTable, IReadOnlyList<string> keyColumns, string lastSeenColumn)
    {
        ArgumentException.ThrowIfNullOrEmpty(sourceTable);
        ArgumentException.ThrowIfNullOrEmpty(livenessTable);
        ArgumentNullException.ThrowIfNull(keyColumns);
        ArgumentException.ThrowIfNullOrEmpty(lastSeenColumn);
        if (keyColumns.Count == 0)
        {
            throw new ArgumentException("A liveness table has at least one key column.", nameof(keyColumns));
        }

        _sourceTable = sourceTable;
        LivenessTable = livenessTable;
        _keyColumns = keyColumns;
        _lastSeenColumn = lastSeenColumn;
    }

    /// <summary>The liveness table, schema-qualified.</summary>
    public string LivenessTable { get; }

    /// <summary>How many columns the key has, which is how many values a cursor and an upper key hold.</summary>
    public int KeyColumnCount => _keyColumns.Count;

    /// <summary>
    /// Finds where the next slice ends: one row holding the last of the next <c>N</c> keys after the cursor in key order,
    /// then how many keys that was. No row when no key is left after the cursor. Fewer keys than <c>N</c> means the slice
    /// reaches the end of the keys. It reads the source table's primary key only, from the cursor on, and stops after
    /// <c>N</c> keys.
    ///
    /// <para>Binds: the cursor's key values when <paramref name="hasCursor"/>, then <c>N</c>.</para>
    /// </summary>
    public string UpperKeySql(bool hasCursor)
    {
        var limit = "$" + ((hasCursor ? KeyColumnCount : 0) + 1).ToString(CultureInfo.InvariantCulture);
        var after = hasCursor ? $"\n        WHERE {KeyRow("d")} > {BindRow(1)}" : string.Empty;
        return $@"
SELECT {KeyList("s")}, s.keys
FROM (
    SELECT {KeyList("k")}, count(*) OVER () AS keys
    FROM (
        SELECT {KeyList("d")}
        FROM {_sourceTable} AS d{after}
        ORDER BY {KeyList("d")}
        LIMIT {limit}
    ) AS k
) AS s
ORDER BY {string.Join(", ", _keyColumns.Select(column => "s." + column + " DESC"))}
LIMIT 1";
    }

    /// <summary>
    /// Writes the liveness row of every source row of one slice that has none, stamped with the source row's own stamp. The
    /// slice is the keys after the cursor up to and including the upper key, and the range is on the source table's key and
    /// on the liveness table's key, so the comparison reads that range of each primary key and no more. The keys are compared
    /// first, key against key, and only a missing key reads its source row: the <c>LATERAL ... OFFSET 0</c> keeps that read a
    /// primary key probe per missing key, where a plain join leaves the planner free to read the slice's source rows, or the
    /// source table, whole. <c>ON CONFLICT DO NOTHING</c> keeps the liveness row a concurrent insert's trigger wrote first.
    ///
    /// <para>Binds: <c>$1</c> the retention pass's cutoff, which every purge statement is bound with and which is named only
    /// so the bind matches the statement, then the cursor's key values when <paramref name="hasCursor"/>, then the upper
    /// key's values.</para>
    /// </summary>
    public string InsertSql(bool hasCursor)
    {
        var upper = BindRow((hasCursor ? KeyColumnCount : 0) + 2);
        string Range(string alias) =>
            (hasCursor ? $"{KeyRow(alias)} > {BindRow(2)} AND " : string.Empty) + $"{KeyRow(alias)} <= {upper}";

        return $@"
INSERT INTO {LivenessTable} ({string.Join(", ", _keyColumns)}, {_lastSeenColumn})
SELECT {KeyList("x")}, x.{_lastSeenColumn}
FROM (
    SELECT {KeyList("k")}
    FROM {_sourceTable} AS k
    WHERE $1::timestamp IS NOT NULL
    AND   {Range("k")}
    AND   NOT EXISTS (
        SELECT 1 FROM {LivenessTable} AS s
        WHERE {KeyMatch("s", "k")}
        AND   {Range("s")})
    OFFSET 0
) AS missing
CROSS JOIN LATERAL (
    SELECT {KeyList("t")}, t.{_lastSeenColumn}
    FROM {_sourceTable} AS t
    WHERE {KeyMatch("t", "missing")}
    OFFSET 0
) AS x
ORDER BY {KeyList("x")}
ON CONFLICT ({string.Join(", ", _keyColumns)}) DO NOTHING";
    }

    private string KeyList(string alias) => string.Join(", ", _keyColumns.Select(column => alias + "." + column));

    private string KeyRow(string alias) => "(" + KeyList(alias) + ")";

    private string KeyMatch(string left, string right) =>
        string.Join(" AND ", _keyColumns.Select(column => $"{left}.{column} = {right}.{column}"));

    private string BindRow(int firstOrdinal) =>
        "(" + string.Join(", ", Enumerable.Range(firstOrdinal, KeyColumnCount).Select(ordinal => "$" + ordinal.ToString(CultureInfo.InvariantCulture))) + ")";
}
