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
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Schema tracking (#7643): the catalog definition of <c>schema_objects</c>, the Darling-only collector that keeps versioned table and view
/// definitions in the store. Its own table, <c>schema_capture_passes</c>, is the pass log: one row per database per pass, saying what was
/// read and written, so "nothing changed" and "nothing was read" are not the same stored answer. The definitions themselves go to the
/// <c>schema_*</c> version tables, which the store writer owns.
///
/// <para>The pass runs through <c>DarlingSchemaCapture</c>, not through the host's one-query path. <see cref="BuildQuery"/> is the
/// per-pass database probe, so the definition is complete for anything that reads it, and <see cref="ReadAsync"/> is not used.</para>
///
/// <para>A plain table on every store (<see cref="StaysPlainTable"/>).</para>
/// </summary>
public sealed class SchemaObjectsCollector : CollectorDefinitionBase<SchemaObjectsCollector.PassRow>
{
    public static SchemaObjectsCollector Instance { get; } = new();

    private SchemaObjectsCollector()
    {
    }

    /// <summary>One database's pass: what kind it was, why, and what it read and wrote.</summary>
    public sealed record PassRow(
        string DatabaseName,
        string HistoryKey,
        string PassKind,
        string? Reason,
        int EventsRead,
        int ObjectsRead,
        int VersionsWritten,
        int DropsWritten,
        bool Complete,
        string? Note);

    public override string Name => "schema_objects";

    public override string TargetTable => "schema_capture_passes";

    /// <summary>A plain table with a time-sliced DELETE: one row per database per pass.</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin (ruling 10-21: new collection is Darling-only).</summary>
    public override bool IsDarlingOnly => true;

    /// <summary>The session can be created on every target but an Amazon RDS Web or Express edition.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => CollectorTargetInfo.XeSessionSupported(target);

    public override CollectorQuery BuildQuery(CollectorContext context) => new(SchemaServerProbe.BuildSql());

    public override ValueTask<List<PassRow>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken) =>
        throw new NotSupportedException("schema_objects runs through DarlingSchemaCapture");

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("history_key", CollectorColumnType.Varchar),
        new CollectorColumn("pass_kind", CollectorColumnType.Varchar),
        new CollectorColumn("reason", CollectorColumnType.Varchar),
        new CollectorColumn("events_read", CollectorColumnType.Integer),
        new CollectorColumn("objects_read", CollectorColumnType.Integer),
        new CollectorColumn("versions_written", CollectorColumnType.Integer),
        new CollectorColumn("drops_written", CollectorColumnType.Integer),
        new CollectorColumn("complete", CollectorColumnType.Boolean),
        new CollectorColumn("note", CollectorColumnType.Varchar),
    };

    public override void WritePayload(PassRow row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.DatabaseName)        /* database_name VARCHAR */
            .Value(row.HistoryKey)          /* history_key VARCHAR */
            .Value(row.PassKind)            /* pass_kind VARCHAR */
            .Value(row.Reason)              /* reason VARCHAR */
            .Value(row.EventsRead)          /* events_read INTEGER */
            .Value(row.ObjectsRead)         /* objects_read INTEGER */
            .Value(row.VersionsWritten)     /* versions_written INTEGER */
            .Value(row.DropsWritten)        /* drops_written INTEGER */
            .Value(row.Complete)            /* complete BOOLEAN */
            .Value(row.Note);               /* note VARCHAR */
    }
}
