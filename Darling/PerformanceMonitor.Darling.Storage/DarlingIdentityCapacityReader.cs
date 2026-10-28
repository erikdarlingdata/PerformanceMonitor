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
using System.Numerics;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// One identity column above 10 percent used, in its database's newest scan inside the window (#6875), with the SAME column's oldest stored
/// sample in the window beside it for the rate. <see cref="FirstValue"/> and <see cref="FirstAt"/> equal the newest sample when the window
/// holds only one for this column.
/// </summary>
public sealed record IdentityColumnFact(
    string DatabaseName,
    string? SchemaName,
    string? TableName,
    string? ColumnName,
    string? DataType,
    int? TypePrecision,
    BigInteger? LastValue,
    BigInteger? SeedValue,
    BigInteger? IncrementValue,
    double? StoredPercent,
    DateTime CapturedAt,
    BigInteger? FirstValue,
    DateTime? FirstAt)
{
    /// <summary>
    /// The capacity from the stored value, seed and increment. Null when the value was not stored: the collector stores NULL for a value
    /// past what a CLR decimal holds (about 7.9e28) while it keeps <see cref="StoredPercent"/> exact, and a kept row is always above 10
    /// percent, so a NULL value here is that case and never "nothing used".
    /// </summary>
    public IdentityCapacity? Capacity =>
        LastValue is null || SeedValue is null || IncrementValue is null
            ? null
            : IdentityCapacityMath.Calculate(DataType, TypePrecision ?? 0, SeedValue.Value, IncrementValue.Value, LastValue);

    /// <summary>Percent used: the exact figure from <see cref="Capacity"/>, else the one the collector stored.</summary>
    public double? PercentUsed => Capacity?.PercentUsed ?? StoredPercent;

    /// <summary>Inserts left before the engine raises an arithmetic overflow: null when the value was not stored.</summary>
    public BigInteger? ValuesRemaining => Capacity?.Remaining;

    /// <summary>ok, warning (70 percent) or critical (90 percent); null when no percent is known.</summary>
    public string? Band => PercentUsed is double percent ? IdentityCapacityMath.Band(percent) : null;

    /// <summary>The rate and the days left, null with one sample or a value that did not move toward the end (see <see cref="IdentityCapacityMath.EstimateExhaustion"/>).</summary>
    public IdentityExhaustion? Exhaustion =>
        SeedValue is null || IncrementValue is null || FirstAt is null
            ? null
            : IdentityCapacityMath.EstimateExhaustion(
                DataType, TypePrecision ?? 0, SeedValue.Value, IncrementValue.Value, FirstValue, FirstAt.Value, LastValue, CapturedAt);
}

/// <summary>A database with a scan marker in the window: it was scanned, whether or not it has a column above 10 percent.</summary>
/// <param name="DatabaseName">The database.</param>
/// <param name="ScannedAt">Its newest marker in the window (UTC).</param>
public sealed record IdentityScannedDatabase(string DatabaseName, DateTime ScannedAt);

/// <summary>
/// An online database of the server that has no scan marker in the window: the pass budget's rotation has not reached it yet, or it is
/// one the collector never scans (a snapshot, a readable secondary), which the database config capture cannot tell apart. It is never
/// "clean".
/// </summary>
/// <param name="DatabaseName">The database.</param>
/// <param name="LastScannedAt">Its newest marker before the window (UTC), null when the store holds none.</param>
public sealed record IdentityNotYetScannedDatabase(string DatabaseName, DateTime? LastScannedAt);

/// <summary>What <see cref="DarlingIdentityCapacityReader.ReadAsync"/> found.</summary>
/// <param name="Columns">Every column above 10 percent in each scanned database's newest scan, fullest first. Empty when every scanned database is clean.</param>
/// <param name="Databases">The databases with a marker in the window, by name.</param>
/// <param name="NotYetScanned">The server's online databases from its newest database config capture that have no marker in the window, by name, each with the time of its last scan when an older one exists. Empty when the server has no config capture.</param>
public sealed record IdentityCapacityRead(
    IReadOnlyList<IdentityColumnFact> Columns,
    IReadOnlyList<IdentityScannedDatabase> Databases,
    IReadOnlyList<IdentityNotYetScannedDatabase>? NotYetScanned = null)
{
    /// <summary>The databases with no marker in the window, never null.</summary>
    public IReadOnlyList<IdentityNotYetScannedDatabase> Unscanned => NotYetScanned ?? Array.Empty<IdentityNotYetScannedDatabase>();

    /// <summary>At least one database was scanned in the window.</summary>
    public bool HasScan => Databases.Count > 0;
}

/// <summary>
/// #6875: identity column capacity in place, from <c>collect.identity_capacity</c> (V222): per scanned database one all-NULL marker row
/// (<c>column_name</c> NULL) plus the top 20 columns above 10 percent, all stamped with the pass's one <c>collection_time</c>. A database is
/// judged by its own newest marker in the window, so a database the pass budget did not reach is not judged against another's scan. The
/// marker is what separates "scanned, nothing above 10 percent" (a marker and no column) from "not scanned" (no marker). The rate compares
/// each column's newest value with its OWN oldest sample in the window, so a column that crossed 10 percent mid-window still gets the
/// samples it has. Values are read as text (<c>numeric(38,0)</c> is wider than a CLR decimal). Bounded by <c>server_id</c> and the window on
/// <c>collection_time</c> (<c>idx_identity_capacity_time</c>).
/// </summary>
public static class DarlingIdentityCapacityReader
{
    /// <summary>
    /// Each scanned database with its newest marker, then that scan's columns beside the same column's oldest sample in the window. A
    /// database with a marker and no column comes back as one row with NULL column fields. $1 server, $2 start, $3 end, $4 database filter.
    /// </summary>
    public static string ScanSql(DatabaseFilter databases) => $@"
WITH win AS (
    SELECT collection_time, database_name, schema_name, table_name, column_name, data_type, type_precision,
           last_value::text AS last_value, seed_value::text AS seed_value, increment_value::text AS increment_value, percent_used
    FROM collect.identity_capacity
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3{databases.Clause("database_name", 4)}),
scan AS (
    SELECT database_name, MAX(collection_time) AS scanned_at FROM win WHERE column_name IS NULL GROUP BY database_name)
SELECT s.database_name, s.scanned_at, c.schema_name, c.table_name, c.column_name, c.data_type, c.type_precision,
       c.last_value, c.seed_value, c.increment_value, c.percent_used, f.collection_time, f.last_value
FROM scan AS s
LEFT JOIN win AS c ON c.database_name = s.database_name AND c.collection_time = s.scanned_at AND c.column_name IS NOT NULL
LEFT JOIN LATERAL (
    SELECT w.collection_time, w.last_value FROM win AS w
    WHERE w.database_name = c.database_name AND w.schema_name = c.schema_name AND w.table_name = c.table_name AND w.column_name = c.column_name
    ORDER BY w.collection_time LIMIT 1) AS f ON c.column_name IS NOT NULL
ORDER BY c.percent_used DESC NULLS LAST, s.database_name, c.schema_name, c.table_name, c.column_name";

    /// <summary>
    /// The server's online user databases in its newest database config capture at or before the window's end, each with its newest
    /// marker at or before the end. The reader drops the ones that have a marker in the window. $1 server, $2 end, $3 database filter,
    /// $4 the names the collector never scans.
    /// </summary>
    public static string ListedDatabasesSql(DatabaseFilter databases) => $@"
SELECT d.database_name,
       (SELECT MAX(i.collection_time) FROM collect.identity_capacity AS i
        WHERE i.server_id = $1 AND i.database_name = d.database_name AND i.column_name IS NULL AND i.collection_time <= $2)
FROM (
    SELECT DISTINCT database_name FROM v_database_config
    WHERE server_id = $1
    AND   capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND capture_time <= $2)
    AND   state_desc = 'ONLINE'
    AND   database_name IS NOT NULL
    AND   database_name <> ALL($4){databases.Clause("database_name", 3)}) AS d
ORDER BY d.database_name";

    /// <summary>Reads the newest scan of every database with a marker in the window <paramref name="start"/> to <paramref name="end"/> (UTC).</summary>
    public static async Task<IdentityCapacityRead> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, DateTime start, DateTime end,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        await using var command = postgres.CreateCommand(ScanSql(databases));
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        command.Parameters.Add(databases.Parameter());

        var scanned = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        var columns = new List<IdentityColumnFact>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var database = reader.GetString(0);
            var scannedAt = reader.GetDateTime(1);
            scanned[database] = scannedAt;
            if (reader.IsDBNull(4))
                continue; // the marker alone: scanned, no column above 10 percent

            string? Text(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
            BigInteger? Big(int i) => reader.IsDBNull(i) ? null : BigInteger.Parse(reader.GetString(i), CultureInfo.InvariantCulture);
            columns.Add(new IdentityColumnFact(
                database, Text(2), Text(3), Text(4), Text(5), reader.IsDBNull(6) ? null : reader.GetInt32(6),
                Big(7), Big(8), Big(9), reader.IsDBNull(10) ? null : reader.GetDouble(10), scannedAt,
                Big(12), reader.IsDBNull(11) ? null : reader.GetDateTime(11)));
        }

        await reader.CloseAsync();

        // A database the rotation has not reached has no marker, so the scan answer alone would leave it out and the server could look
        // clean. The server's database list is its newest database config capture (every online database, once per capture).
        var notYetScanned = new List<IdentityNotYetScannedDatabase>();
        await using (var listing = postgres.CreateCommand(ListedDatabasesSql(databases)))
        {
            listing.CommandTimeout = commandTimeoutSeconds;
            listing.Parameters.AddWithValue(serverId);
            listing.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
            listing.Parameters.Add(databases.Parameter());
            listing.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = IdentityCapacityCollector.SkippedDatabaseNames.ToArray() });
            await using var names = await listing.ExecuteReaderAsync(cancellationToken);
            while (await names.ReadAsync(cancellationToken))
            {
                var name = names.GetString(0);
                if (!scanned.ContainsKey(name))
                    notYetScanned.Add(new IdentityNotYetScannedDatabase(name, names.IsDBNull(1) ? null : names.GetDateTime(1)));
            }
        }

        return new IdentityCapacityRead(
            columns.OrderByDescending(c => c.PercentUsed ?? -1)
                .ThenBy(c => c.DatabaseName, StringComparer.Ordinal).ThenBy(c => c.SchemaName, StringComparer.Ordinal)
                .ThenBy(c => c.TableName, StringComparer.Ordinal).ThenBy(c => c.ColumnName, StringComparer.Ordinal).ToList(),
            scanned.OrderBy(d => d.Key, StringComparer.Ordinal).Select(d => new IdentityScannedDatabase(d.Key, d.Value)).ToList(),
            notYetScanned);
    }
}
