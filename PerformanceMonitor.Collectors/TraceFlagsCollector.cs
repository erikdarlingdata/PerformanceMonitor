/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Active trace flags via DBCC TRACESTATUS(-1) staged through a #temp table (on-load only).
/// Extracted verbatim from Lite's RemoteCollectorService.ServerConfig.cs. The caller wraps the
/// run in a permission-tolerant catch (DBCC may be denied) — that policy stays host-side so a
/// permissions failure degrades to zero rows with a warning, exactly as before.
/// </summary>
public sealed class TraceFlagsCollector : CollectorDefinitionBase<TraceFlagsCollector.Row>
{
    public static TraceFlagsCollector Instance { get; } = new();

    private TraceFlagsCollector()
    {
    }

    public readonly record struct Row(int TraceFlag, bool Status, bool IsGlobal, bool IsSession);

    private const string QueryText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

CREATE TABLE
    #trace_flags
(
    trace_flag integer NOT NULL,
    status bit NOT NULL,
    is_global bit NOT NULL,
    is_session bit NOT NULL
);

INSERT
    #trace_flags
(
    trace_flag,
    status,
    is_global,
    is_session
)
EXECUTE(N'DBCC TRACESTATUS(-1) WITH NO_INFOMSGS;');

SELECT /* PerformanceMonitorLite */
    tf.trace_flag,
    tf.status,
    tf.is_global,
    tf.is_session
FROM #trace_flags AS tf
ORDER BY tf.trace_flag
OPTION(RECOMPILE);";

    public override string Name => "trace_flags";

    public override string TargetTable => "trace_flags";

    /// <summary>The config snapshots' prefix is config_id/capture_time in Lite's schema; Darling mirrors it.</summary>
    public override string PrefixIdColumnName => "config_id";

    public override string PrefixTimeColumnName => "capture_time";

    /// <summary>
    /// NOT Azure SQL Database (edition 5): <c>DBCC TRACESTATUS</c> is not supported there. Azure SQL
    /// Managed Instance (edition 8) and on-prem both support it, so they collect. Gated here in the shared
    /// AppliesTo so Lite and Darling skip identically — before this the gate lived only in Lite's
    /// IsCollectorSupported, and Darling relied on <c>RunTraceFlagsTolerantAsync</c> swallowing the error to
    /// a 0-row SUCCESS (this makes the skip explicit, matching Lite's SKIPPED record).
    /// </summary>
    public override bool AppliesTo(CollectorTargetInfo target) => !target.IsAzureSqlDb;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("trace_flag", CollectorColumnType.Integer),
        new CollectorColumn("status", CollectorColumnType.Boolean),
        new CollectorColumn("is_global", CollectorColumnType.Boolean),
        new CollectorColumn("is_session", CollectorColumnType.Boolean),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.GetInt32(0),
                reader.GetBoolean(1),
                reader.GetBoolean(2),
                reader.GetBoolean(3)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        writer
            .Value(row.TraceFlag)   /* trace_flag INTEGER */
            .Value(row.Status)      /* status BOOLEAN */
            .Value(row.IsGlobal)    /* is_global BOOLEAN */
            .Value(row.IsSession);  /* is_session BOOLEAN */
    }
}
