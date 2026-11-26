/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// The one read of <c>v_index_object_stats</c> behind <c>get_object_usage</c> (#8784), shared by Lite and Darling so the statement, its column
/// order and the mapper that reads it cannot drift apart. Both stores expose the view under the same name with the same columns, and the
/// statement uses only what DuckDB and PostgreSQL both accept (positional <c>$n</c> parameters, <c>interval</c> literals, <c>COALESCE</c>).
/// </summary>
public static class ObjectUsageWindowRead
{
    /// <summary>
    /// How far before the window start the baseline snapshot may lie. The collector runs daily, so the newest snapshot at or before the window
    /// start is normally less than a day before it; a week allows for a stalled collector without letting the baseline read walk every older
    /// chunk. With none that near, the first snapshot inside the window is the baseline and the reply's covered days say so.
    /// </summary>
    public const int BaselineReachDays = 8;

    /// <summary>
    /// The 17 columns every read of <c>v_index_object_stats</c> for <see cref="Map"/> selects, in the order <see cref="Map"/> reads them, from
    /// a table aliased <c>s</c>. The one definition: <see cref="Sql"/>, Darling's boundary-row read and Darling's chain builder all build their
    /// statements from it, so the list and the mapper cannot drift apart. Change this list and <see cref="Map"/> together.
    /// </summary>
    public const string Columns = """
        s.collection_time,
        s.sqlserver_start_time,
        s.answering_server_name,
        s.object_id,
        s.index_id,
        s.schema_name,
        s.table_name,
        COALESCE(s.is_indexed_view, false),
        COALESCE(s.user_seeks, 0),
        COALESCE(s.user_scans, 0),
        COALESCE(s.user_lookups, 0),
        COALESCE(s.user_updates, 0),
        s.last_user_seek,
        s.last_user_scan,
        s.last_user_lookup,
        s.last_user_update,
        s.is_memory_optimized
        """;

    /// <summary>
    /// The baseline CTE: <c>baseline.collection_time</c> is the newest snapshot of one database at or before the window start, no more than
    /// <see cref="BaselineReachDays"/> before it, or NULL. $1 server_id, $2 database_name, $3 the window start. The one definition: <see cref="Sql"/>
    /// and Darling's bounds read both start from it, so the persisted path and the full fold always agree on the baseline.
    /// </summary>
    public static readonly string BaselineCte = $"""
        WITH baseline AS
        (
            SELECT max(b.collection_time) AS collection_time
            FROM v_index_object_stats AS b
            WHERE b.server_id = $1
            AND   b.database_name = $2
            AND   b.collection_time <= $3
            AND   b.collection_time > $3 - interval '{BaselineReachDays} days'
        )
        """;

    /// <summary>
    /// The window's snapshots for one database, oldest first, from the newest snapshot at or before the window start (the baseline) onward.
    /// $1 server_id, $2 database_name, $3 the window start (naive UTC, the clock collection_time is stored in). The baseline lookup is bounded
    /// below by <see cref="BaselineReachDays"/> so Darling's (server_id, collection_time DESC) index answers it from the chunks near the start.
    /// The SELECT list is <see cref="Columns"/>, the contract <see cref="Map"/> reads: change <see cref="Columns"/> and <see cref="Map"/> together.
    /// Darling's full fold reads it. Its rows come in <c>collection_time</c> order only: Darling's chunks give that order as they are read, and an
    /// order by <c>collection_id</c> inside a snapshot is a sort of every row of the window.
    /// </summary>
    public static readonly string Sql = WindowSql("s.collection_time");

    /// <summary>
    /// <see cref="Sql"/> with the rows of one snapshot in <c>collection_id</c> order, the order Lite's chain builder reads them in. Lite's full
    /// fold reads it, so when a snapshot's rows disagree (a retried database, a rename between two reads of one run) the first non-null node
    /// name and the last row's identity are the same on both of Lite's paths, whatever order DuckDB scans the hot table and the archive in.
    /// </summary>
    public static readonly string SqlInCollectionIdOrder = WindowSql("s.collection_time,\n    s.collection_id");

    private static string WindowSql(string orderBy) => BaselineCte + $"""


        SELECT
        {Columns}
        FROM v_index_object_stats AS s
        WHERE s.server_id = $1
        AND   s.database_name = $2
        AND   s.collection_time >= COALESCE((SELECT collection_time FROM baseline), $3)
        ORDER BY
            {orderBy}
        """;

    /// <summary>Maps one row of <see cref="Sql"/> (17 columns, in the order of <see cref="Columns"/>). The integer columns are converted rather than read by
    /// their exact type, because the two stores do not declare them the same width. <c>is_memory_optimized</c> is read as stored: a row from
    /// before the column was collected holds NULL, which is "not known" and not "no".</summary>
    public static ObjectUsageSample Map(DbDataReader reader)
    {
        ArgumentNullException.ThrowIfNull(reader);

        DateTime? T(int i) => reader.IsDBNull(i) ? null : reader.GetDateTime(i);
        int I(int i) => Convert.ToInt32(reader.GetValue(i), CultureInfo.InvariantCulture);
        long L(int i) => Convert.ToInt64(reader.GetValue(i), CultureInfo.InvariantCulture);
        return new ObjectUsageSample(
            reader.GetDateTime(0),
            T(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            I(3),
            I(4),
            reader.IsDBNull(5) ? "" : reader.GetString(5),
            reader.IsDBNull(6) ? "" : reader.GetString(6),
            reader.GetBoolean(7),
            L(8),
            L(9),
            L(10),
            L(11),
            T(12),
            T(13),
            T(14),
            T(15),
            reader.IsDBNull(16) ? null : reader.GetBoolean(16));
    }
}

/// <summary>
/// What one <c>get_object_usage</c> request read, kept for a few minutes (#8784). The read walks every snapshot of the window, and a caller
/// pages through the reply with <c>next_offset</c>: without this each page would repeat the whole read. Keeping the evidence also gives every
/// page of one walk the same window and the same rows, so paging neither skips nor repeats an object when a collection lands in between.
///
/// <para>The key is built by the caller AFTER it has resolved and authorised the server, from the resolved ids, so an entry is only ever
/// returned to a request that was allowed the same servers. The evidence is a day-grain summary of data collected daily, so a few minutes of
/// age changes nothing a reader could act on. Small and bounded: the oldest entry goes when a new one would exceed the cap.</para>
/// </summary>
public sealed class ObjectUsageEvidenceCache
{
    /// <summary>How long an entry is served.</summary>
    public static readonly TimeSpan DefaultLifetime = TimeSpan.FromMinutes(5);

    /// <summary>The most entries kept.</summary>
    public const int MaxEntries = 8;

    private readonly Dictionary<string, (long StoredAtMs, ObjectUsageEvidence Evidence)> _entries = new(StringComparer.Ordinal);
    private readonly object _gate = new();
    private readonly TimeSpan _lifetime;
    private readonly Func<long> _clockMs;

    /// <summary>Creates a cache.</summary>
    /// <param name="lifetime">How long an entry is served; <see cref="DefaultLifetime"/> when null.</param>
    /// <param name="clockMs">A monotonic millisecond clock, for a test; the process tick count when null.</param>
    public ObjectUsageEvidenceCache(TimeSpan? lifetime = null, Func<long>? clockMs = null)
    {
        _lifetime = lifetime ?? DefaultLifetime;
        _clockMs = clockMs ?? (() => Environment.TickCount64);
    }

    /// <summary>The key of one request: the resolved server ids in the order read, the database (exact) and the window length.</summary>
    public static string Key(IEnumerable<int> serverIds, string databaseName, int daysBack) =>
        string.Join(",", serverIds) + "|" + daysBack.ToString(CultureInfo.InvariantCulture) + "|" + databaseName;

    /// <summary>Returns the entry for <paramref name="key"/> when one is still fresh.</summary>
    public bool TryGet(string key, out ObjectUsageEvidence evidence)
    {
        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var entry))
            {
                if (_clockMs() - entry.StoredAtMs < _lifetime.TotalMilliseconds)
                {
                    evidence = entry.Evidence;
                    return true;
                }

                _entries.Remove(key);
            }
        }

        evidence = null!;
        return false;
    }

    /// <summary>Stores an entry, dropping the oldest when the cache is full.</summary>
    public void Set(string key, ObjectUsageEvidence evidence)
    {
        ArgumentNullException.ThrowIfNull(evidence);

        lock (_gate)
        {
            if (!_entries.ContainsKey(key) && _entries.Count >= MaxEntries)
            {
                string? oldest = null;
                var oldestAt = long.MaxValue;
                foreach (var (k, v) in _entries)
                {
                    if (v.StoredAtMs < oldestAt)
                    {
                        oldestAt = v.StoredAtMs;
                        oldest = k;
                    }
                }

                if (oldest is not null)
                {
                    _entries.Remove(oldest);
                }
            }

            _entries[key] = (_clockMs(), evidence);
        }
    }
}

/// <summary>The evidence of one <c>get_object_usage</c> request: the window it was read for and each side's reads.</summary>
/// <param name="WindowStartUtc">The window start the reads used.</param>
/// <param name="ReadAtUtc">When the evidence was read.</param>
/// <param name="Sides">The sides, the asked-for registration first.</param>
public sealed record ObjectUsageEvidence(DateTime WindowStartUtc, DateTime ReadAtUtc, IReadOnlyList<ObjectUsageSide> Sides);
