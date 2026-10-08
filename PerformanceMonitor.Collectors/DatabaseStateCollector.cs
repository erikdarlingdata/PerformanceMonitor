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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Per-database state (<c>sys.databases.state_desc</c>) captured as a TIME SERIES, unlike the
/// load-time <see cref="DatabaseConfigCollector"/> snapshot: a database going OFFLINE / SUSPECT /
/// RECOVERY_PENDING / RESTORING after a server is added would never show up in the config snapshot
/// (frequency 0), so the "database offline / unhealthy state" alert needs its own cheap periodic
/// feed. Selects the handful of columns that alert needs — name, id, <c>state_desc</c>, and
/// <c>is_in_standby</c> (a read-only log-shipping secondary sits permanently in RESTORING and must
/// be distinguishable from a genuinely stuck restore) — against in-memory catalog metadata, so on
/// every server this is a few-row read regardless of database count.
///
/// <para>Deliberately UNFILTERED at collection time (no excluded-database splice and no
/// system-database cutoff): a SUSPECT <c>msdb</c> or <c>model</c> is exactly the kind of thing this
/// alert exists to catch, and the excluded-database list is an ALERT concern the engine re-applies
/// client-side (mirroring how blocking/deadlock/long-query reads exclude databases downstream, not
/// in the collected table). The whole-fleet state history stays available to the viewers.</para>
///
/// <para>Uses the default time-series prefix columns (<c>collection_id</c> / <c>collection_time</c>)
/// rather than the config snapshots' <c>config_id</c> / <c>capture_time</c>, because each cycle is a
/// point-in-time observation, not a slowly-changing configuration row. Not collected on Azure SQL DB
/// (see <see cref="AppliesTo"/>), so this single server-scoped query never runs there.</para>
/// </summary>
public sealed class DatabaseStateCollector : CollectorDefinitionBase<DatabaseStateCollector.Row>
{
    public static DatabaseStateCollector Instance { get; } = new();

    private DatabaseStateCollector()
    {
    }

    public readonly record struct Row(
        string DatabaseName,
        int DatabaseId,
        string? StateDesc,
        bool IsInStandby);

    public override string Name => "database_states";

    public override string TargetTable => "database_states";

    /// <summary>
    /// Not collected on Azure SQL DB (the same <c>!IsAzureSqlDb</c> gate the AG / system_health collectors
    /// use): there <c>sys.databases</c> exposes only <c>master</c> + the connected database, whose state is
    /// Azure-managed and effectively always ONLINE, so there is no per-database state to deviate from.
    /// Azure Managed Instance behaves like a full instance (complete <c>sys.databases</c>) and is NOT gated.
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsAzureSqlDb;

    public override CollectorQuery BuildQuery(CollectorContext context)
    {
        var query = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT /* PerformanceMonitorLite */
    database_name = d.name,
    database_id = d.database_id,
    state_desc = d.state_desc,
    is_in_standby = d.is_in_standby
FROM sys.databases AS d
ORDER BY d.name
OPTION(RECOMPILE);";

        return new CollectorQuery(query);
    }

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("database_id", CollectorColumnType.Integer),
        new CollectorColumn("state_desc", CollectorColumnType.Varchar),
        new CollectorColumn("is_in_standby", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.GetString(0),
                Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.DatabaseName)
            .Value(row.DatabaseId)
            .Value(row.StateDesc)
            .Value(row.IsInStandby);
    }
}
