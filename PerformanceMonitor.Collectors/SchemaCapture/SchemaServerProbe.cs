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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #7643: what the server says about one user database, for the work planner. <see cref="AgGroupId"/>, <see cref="AgName"/> and
/// <see cref="IsPrimaryReplica"/> are null for a database outside an availability group. <see cref="RecoveryForkGuid"/> changes when
/// the database is restored or recovers onto a new fork, so a changed value means the stored history no longer describes it.
/// </summary>
public sealed record SchemaDatabaseProbe(
    string Name, int DatabaseId, string StateDesc, bool IsReadOnly, bool IsInStandby, int? SourceDatabaseId, bool HasAccess,
    Guid? RecoveryForkGuid, Guid? AgGroupId, string? AgName, bool? IsPrimaryReplica);

/// <summary>#7643: the statements and reader of the per-pass database probe.</summary>
public static class SchemaServerProbe
{
    /// <summary>
    /// On premises, one batch: every user database (id above 4, below the resource-database range), with its state, snapshot source,
    /// access, recovery fork and availability group. The group join is the local replica's row, as in <see cref="QueryStoreCollector"/>.
    /// A database of a distributed availability group has a local row in its own group and one in the distributed group, so the batch
    /// returns both with <c>ag_is_distributed</c> and <see cref="ReadAsync"/> keeps one row per database (<see cref="OneRowPerDatabase"/>).
    /// </summary>
    public static string BuildSql() => @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    name = d.name,
    database_id = d.database_id,
    state_desc = d.state_desc,
    is_read_only = d.is_read_only,
    is_in_standby = d.is_in_standby,
    source_database_id = d.source_database_id,
    has_access = CONVERT(bit, ISNULL(HAS_DBACCESS(d.name), 0)),
    recovery_fork_guid = rs.recovery_fork_guid,
    ag_group_id = drs.group_id,
    ag_name = ag.name,
    is_primary_replica = drs.is_primary_replica,
    ag_is_distributed = ag.is_distributed
FROM sys.databases AS d
LEFT JOIN sys.database_recovery_status AS rs
  ON rs.database_id = d.database_id
LEFT JOIN sys.dm_hadr_database_replica_states AS drs
  ON drs.database_id = d.database_id
 AND drs.is_local = 1
LEFT JOIN sys.availability_groups AS ag
  ON ag.group_id = drs.group_id
WHERE d.database_id > 4
AND   d.database_id < 32761
ORDER BY
    d.database_id
OPTION(RECOMPILE);";

    /// <summary>
    /// Azure SQL Database: the same columns for the current database. There is no availability group or recovery fork to read there
    /// (the fork is null, which the planner reads as unchanged), and the database is a primary.
    /// </summary>
    public static string BuildAzureSql() => @"
SELECT /* PerformanceMonitorLite */
    name = d.name,
    database_id = d.database_id,
    state_desc = d.state_desc,
    is_read_only = d.is_read_only,
    is_in_standby = d.is_in_standby,
    source_database_id = d.source_database_id,
    has_access = CONVERT(bit, 1),
    recovery_fork_guid = CONVERT(uniqueidentifier, NULL),
    ag_group_id = CONVERT(uniqueidentifier, NULL),
    ag_name = CONVERT(sysname, NULL),
    is_primary_replica = CONVERT(bit, NULL),
    ag_is_distributed = CONVERT(bit, NULL)
FROM sys.databases AS d
WHERE d.name = DB_NAME()
OPTION(RECOMPILE);";

    public static async Task<IReadOnlyList<SchemaDatabaseProbe>> ReadAsync(DbDataReader reader, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(reader);
        var rows = new List<(SchemaDatabaseProbe, bool)>();
        while (await reader.ReadAsync(ct))
        {
            rows.Add((new SchemaDatabaseProbe(
                reader.GetString(0),
                reader.GetInt32(1),
                reader.GetString(2),
                reader.GetBoolean(3),
                reader.GetBoolean(4),
                reader.IsDBNull(5) ? null : Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture),
                reader.GetBoolean(6),
                reader.IsDBNull(7) ? null : reader.GetGuid(7),
                reader.IsDBNull(8) ? null : reader.GetGuid(8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetBoolean(10)),
                !reader.IsDBNull(11) && reader.GetBoolean(11)));
        }

        return OneRowPerDatabase(rows);
    }

    /// <summary>
    /// One probe per database id, in the order first seen. Where a database has more than one row (a distributed availability group),
    /// the row of a group that is not distributed wins: that is the group the database's own replicas belong to.
    /// </summary>
    public static IReadOnlyList<SchemaDatabaseProbe> OneRowPerDatabase(IEnumerable<(SchemaDatabaseProbe Probe, bool IsDistributed)> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        var order = new List<int>();
        var chosen = new Dictionary<int, (SchemaDatabaseProbe Probe, bool IsDistributed)>();
        foreach (var row in rows)
        {
            if (!chosen.TryGetValue(row.Probe.DatabaseId, out var kept))
            {
                order.Add(row.Probe.DatabaseId);
                chosen[row.Probe.DatabaseId] = row;
            }
            else if (kept.IsDistributed && !row.IsDistributed)
            {
                chosen[row.Probe.DatabaseId] = row;
            }
        }

        return order.Select(id => chosen[id].Probe).ToList();
    }
}
