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
/// An online database of the server that has no scan marker in the window and that the collector did not skip on purpose: the pass
/// budget's rotation has not reached it yet. It is never "clean".
/// </summary>
/// <param name="DatabaseName">The database.</param>
/// <param name="LastScannedAt">Its newest marker before the window (UTC), null when the store holds none.</param>
public sealed record IdentityNotYetScannedDatabase(string DatabaseName, DateTime? LastScannedAt);

/// <summary>
/// A database with no scan marker in the window that the collector skipped on purpose and said why (a readable secondary, a snapshot, a
/// database the monitoring login cannot enter). Never "clean" and never "not yet scanned": waiting does not change it.
/// </summary>
/// <param name="DatabaseName">The database.</param>
/// <param name="Reason">The reason word the collector stored (<see cref="IdentityCapacityCollector.SkipSecondary"/>, <see cref="IdentityCapacityCollector.SkipSnapshot"/>, <see cref="IdentityCapacityCollector.SkipNoAccess"/>).</param>
/// <param name="LastScannedAt">Its newest marker before the window (UTC), null when the store holds none.</param>
public sealed record IdentitySkippedDatabase(string DatabaseName, string Reason, DateTime? LastScannedAt)
{
    /// <summary>"not scanned: readable secondary" and its siblings, the reason word itself for one this build does not know.</summary>
    public string Text => IdentityCapacityCollector.SkipReasonText(Reason) ?? "not scanned: " + Reason;
}

/// <summary>What <see cref="DarlingIdentityCapacityReader.ReadAsync"/> found.</summary>
/// <param name="Columns">Every column above 10 percent in each scanned database's newest scan, fullest first. Empty when every scanned database is clean.</param>
/// <param name="Databases">The databases with a marker in the window, by name.</param>
/// <param name="NotYetScanned">The server's online databases from its newest database config capture that have no marker in the window, by name, each with the time of its last scan when an older one exists. Empty when the server has no config capture.</param>
public sealed record IdentityCapacityRead(
    IReadOnlyList<IdentityColumnFact> Columns,
    IReadOnlyList<IdentityScannedDatabase> Databases,
    IReadOnlyList<IdentityNotYetScannedDatabase>? NotYetScanned = null,
    IReadOnlyList<IdentitySkippedDatabase>? SkippedOnPurpose = null)
{
    /// <summary>The databases the collector skipped on purpose with a stored reason, never null.</summary>
    public IReadOnlyList<IdentitySkippedDatabase> Skipped => SkippedOnPurpose ?? Array.Empty<IdentitySkippedDatabase>();

    /// <summary>The databases with no marker in the window, never null.</summary>
    public IReadOnlyList<IdentityNotYetScannedDatabase> Unscanned => NotYetScanned ?? Array.Empty<IdentityNotYetScannedDatabase>();

    /// <summary>At least one database was scanned in the window.</summary>
    public bool HasScan => Databases.Count > 0;
}

/// <summary>
/// #6875: identity column capacity in place, from <c>collect.identity_capacity</c> (V224): per scanned database one all-NULL marker row
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
    ///
    /// <para>No CTE is read more than once: PostgreSQL materializes one that is, and the correlated lookup per column then rescanned the
    /// tuplestore with no index (#6875). The newest marker per database is a <c>DISTINCT ON</c> over the partial marker index, the
    /// columns of that scan are joined from the base table (the window bound on the join is implied by the marker's time, and it is what
    /// lets the planner use <c>idx_identity_capacity_time</c> instead of a seq scan), and the oldest sample per column is one
    /// <c>DISTINCT ON</c> pass over the window instead of a probe per column. Same rows as the old shape: on a throwaway store with 400
    /// databases and 28 scans in the window, 3,340 ms before and 175 ms after.</para>
    /// </summary>
    public static string ScanSql(DatabaseFilter databases) => $@"
SELECT n.database_name, n.scanned_at, c.schema_name, c.table_name, c.column_name, c.data_type, c.type_precision,
       c.last_value::text, c.seed_value::text, c.increment_value::text, c.percent_used, f.collection_time, f.last_value
FROM (
    SELECT DISTINCT ON (m.database_name) m.database_name, m.collection_time AS scanned_at
    FROM collect.identity_capacity AS m
    WHERE m.server_id = $1 AND m.collection_time >= $2 AND m.collection_time <= $3
    AND   m.column_name IS NULL AND m.skip_reason IS NULL{databases.Clause("m.database_name", 4)}
    ORDER BY m.database_name, m.collection_time DESC) AS n
LEFT JOIN collect.identity_capacity AS c
  ON c.server_id = $1 AND c.collection_time = n.scanned_at AND c.collection_time >= $2 AND c.collection_time <= $3
  AND c.database_name = n.database_name AND c.column_name IS NOT NULL
LEFT JOIN (
    SELECT DISTINCT ON (w.database_name, w.schema_name, w.table_name, w.column_name)
           w.database_name, w.schema_name, w.table_name, w.column_name, w.collection_time, w.last_value::text AS last_value
    FROM collect.identity_capacity AS w
    WHERE w.server_id = $1 AND w.collection_time >= $2 AND w.collection_time <= $3 AND w.column_name IS NOT NULL{databases.Clause("w.database_name", 4)}
    ORDER BY w.database_name, w.schema_name, w.table_name, w.column_name, w.collection_time) AS f
  ON f.database_name = c.database_name AND f.schema_name = c.schema_name AND f.table_name = c.table_name AND f.column_name = c.column_name
ORDER BY c.percent_used DESC NULLS LAST, n.database_name, c.schema_name, c.table_name, c.column_name";

    /// <summary>
    /// The server's online user databases in its newest database config capture at or before the window's end, each with its newest
    /// marker at or before the end, and the reason the collector stored for skipping it on purpose in the window (newest wins). The reader
    /// drops the ones that have a marker in the window. A skip row has <c>column_name</c> NULL too, so a marker read says
    /// <c>skip_reason IS NULL</c>. $1 server, $2 end, $3 database filter, $4 the names the collector never scans, $5 window start.
    ///
    /// <para>A database the collector's own database scope (#3477) leaves out is not listed either: the collector's enumeration never reaches
    /// it, so it would read "not yet scanned" forever. The scope is read from the same row the collector's context is built from
    /// (<c>config.config_collector_schedules.databases</c> for <c>identity_capacity</c>, the server's own row first, then the fleet row,
    /// NULL falls through and an empty array means no scope) and compared without case, so the reader errs toward listing a database. The
    /// server-level exclusion needs no clause: <c>v_database_config</c> never holds a database it excludes.</para>
    /// </summary>
    public static string ListedDatabasesSql(DatabaseFilter databases) => $@"
SELECT d.database_name,
       (SELECT MAX(i.collection_time) FROM collect.identity_capacity AS i
        WHERE i.server_id = $1 AND i.database_name = d.database_name AND i.column_name IS NULL AND i.skip_reason IS NULL AND i.collection_time <= $2),
       (SELECT i.skip_reason FROM collect.identity_capacity AS i
        WHERE i.server_id = $1 AND i.database_name = d.database_name AND i.column_name IS NULL AND i.skip_reason IS NOT NULL
        AND   i.collection_time >= $5 AND i.collection_time <= $2
        ORDER BY i.collection_time DESC LIMIT 1)
FROM (
    SELECT DISTINCT v.database_name FROM v_database_config AS v
    CROSS JOIN (
        SELECT COALESCE(array_agg(lower(btrim(s.name, E' \t\r\n'))) FILTER (WHERE btrim(s.name, E' \t\r\n') <> ''), ARRAY[]::text[]) AS names
        FROM unnest(COALESCE(
            (SELECT cs.databases FROM config.config_collector_schedules AS cs WHERE cs.collector_name = 'identity_capacity' AND cs.server_id = $1),
            (SELECT cs.databases FROM config.config_collector_schedules AS cs WHERE cs.collector_name = 'identity_capacity' AND cs.server_id IS NULL),
            ARRAY[]::text[])) AS s(name)) AS scope
    WHERE v.server_id = $1
    AND   v.capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1 AND capture_time <= $2)
    AND   v.state_desc = 'ONLINE'
    AND   v.database_name IS NOT NULL
    AND   v.database_name <> ALL($4){databases.Clause("v.database_name", 3)}
    AND   (cardinality(scope.names) = 0 OR lower(v.database_name) = ANY(scope.names))) AS d
ORDER BY d.database_name";

    /// <summary>
    /// The newest scan's columns of every database with a marker in the window, and nothing else (#6875): the read behind the self-alert
    /// sweep (every 30 seconds per server) and the Viewer Overview card, which judge the columns and never look at the databases waiting or
    /// skipped. One statement, <see cref="ScanSql"/>; <see cref="IdentityCapacityRead.NotYetScanned"/> and
    /// <see cref="IdentityCapacityRead.SkippedOnPurpose"/> are null, so a caller that needs them uses <see cref="ReadAsync"/>.
    /// </summary>
    public static async Task<IdentityCapacityRead> ReadNewestScansAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, DateTime start, DateTime end,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var (scanned, columns) = await ReadScansAsync(postgres, serverId, databases, start, end, commandTimeoutSeconds, cancellationToken);
        return Assemble(scanned, columns, null, null);
    }

    /// <summary>Reads the newest scan of every database with a marker in the window <paramref name="start"/> to <paramref name="end"/> (UTC), and the databases still waiting or skipped on purpose.</summary>
    public static async Task<IdentityCapacityRead> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, DateTime start, DateTime end,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var (scanned, columns) = await ReadScansAsync(postgres, serverId, databases, start, end, commandTimeoutSeconds, cancellationToken);

        // A database the rotation has not reached has no marker, so the scan answer alone would leave it out and the server could look
        // clean. The server's database list is its newest database config capture (every online database, once per capture).
        var notYetScanned = new List<IdentityNotYetScannedDatabase>();
        var skipped = new List<IdentitySkippedDatabase>();
        await using (var listing = postgres.CreateCommand(ListedDatabasesSql(databases)))
        {
            listing.CommandTimeout = commandTimeoutSeconds;
            listing.Parameters.AddWithValue(serverId);
            listing.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
            listing.Parameters.Add(databases.Parameter());
            listing.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = IdentityCapacityCollector.SkippedDatabaseNames.ToArray() });
            listing.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
            await using var names = await listing.ExecuteReaderAsync(cancellationToken);
            while (await names.ReadAsync(cancellationToken))
            {
                var name = names.GetString(0);
                if (scanned.ContainsKey(name))
                    continue;

                var lastScannedAt = names.IsDBNull(1) ? null : (DateTime?)names.GetDateTime(1);
                if (names.IsDBNull(2))
                    notYetScanned.Add(new IdentityNotYetScannedDatabase(name, lastScannedAt));
                else
                    skipped.Add(new IdentitySkippedDatabase(name, names.GetString(2), lastScannedAt));
            }
        }

        return Assemble(scanned, columns, notYetScanned, skipped);
    }

    private static async Task<(Dictionary<string, DateTime> Scanned, List<IdentityColumnFact> Columns)> ReadScansAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, DateTime start, DateTime end,
        int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
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

        return (scanned, columns);
    }

    private static IdentityCapacityRead Assemble(
        Dictionary<string, DateTime> scanned, List<IdentityColumnFact> columns,
        List<IdentityNotYetScannedDatabase>? notYetScanned, List<IdentitySkippedDatabase>? skipped) =>
        new(
            columns.OrderByDescending(c => c.PercentUsed ?? -1)
                .ThenBy(c => c.DatabaseName, StringComparer.Ordinal).ThenBy(c => c.SchemaName, StringComparer.Ordinal)
                .ThenBy(c => c.TableName, StringComparer.Ordinal).ThenBy(c => c.ColumnName, StringComparer.Ordinal).ToList(),
            scanned.OrderBy(d => d.Key, StringComparer.Ordinal).Select(d => new IdentityScannedDatabase(d.Key, d.Value)).ToList(),
            notYetScanned,
            skipped);
}
