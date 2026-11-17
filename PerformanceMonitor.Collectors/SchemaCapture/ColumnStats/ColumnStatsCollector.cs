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
/// Column selectivity (#8798): the catalog definition of <c>column_stats</c>, the Darling-only collector that keeps the density
/// vectors of each table's statistics in the store. Its own table, <c>column_stats_runs</c>, is the run log: one row per database per
/// run, saying what was checked, read, skipped and why, so "nothing changed" and "nothing was read" are not the same stored answer.
/// The statistics themselves go to <c>collect.column_stats</c>, which the store owns.
///
/// <para>The run goes through <c>DarlingColumnStatsCapture</c>, not through the host's one-query path. <see cref="BuildQuery"/> is the
/// schema-tracking database probe, so the definition is complete for anything that reads it, and <see cref="ReadAsync"/> is not used.</para>
///
/// <para>A plain table on every store (<see cref="StaysPlainTable"/>). SQL Server and Azure SQL Database targets only (the default
/// <see cref="CollectorDefinitionBase{TRow}.TargetEngine"/>).</para>
/// </summary>
public sealed class ColumnStatsCollector : CollectorDefinitionBase<ColumnStatsCollector.RunRow>
{
    public static ColumnStatsCollector Instance { get; } = new();

    private ColumnStatsCollector()
    {
    }

    /// <summary>One database's run: what kind it was, what it checked, read, skipped and deleted, and whether it finished.</summary>
    public sealed record RunRow(
        string DatabaseName,
        string HistoryKey,
        string Kind,
        int Checked,
        int Read,
        int Deleted,
        int SkippedFiltered,
        int SkippedMemoryOptimized,
        int SkippedXmlSpatialColumnstore,
        int SkippedNeverUpdated,
        int SkippedLocked,
        int SkippedOverCap,
        int Errors,
        string? SampleError,
        string? Refused,
        bool Complete,
        string? Note);

    public override string Name => "column_stats";

    public override string TargetTable => "column_stats_runs";

    /// <summary>A plain table with a time-sliced DELETE: one row per database per run.</summary>
    public override bool StaysPlainTable => true;

    /// <summary>Darling stores and reads it; Lite has no twin (ruling 10-21: new collection is Darling-only).</summary>
    public override bool IsDarlingOnly => true;

    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(SchemaServerProbe.BuildSql());

    public override ValueTask<List<RunRow>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken) =>
        throw new NotSupportedException("column_stats runs through DarlingColumnStatsCapture");

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("history_key", CollectorColumnType.Varchar),
        new CollectorColumn("kind", CollectorColumnType.Varchar),
        new CollectorColumn("checked", CollectorColumnType.Integer),
        new CollectorColumn("read", CollectorColumnType.Integer),
        new CollectorColumn("deleted", CollectorColumnType.Integer),
        new CollectorColumn("skipped_filtered", CollectorColumnType.Integer),
        new CollectorColumn("skipped_memory_optimized", CollectorColumnType.Integer),
        new CollectorColumn("skipped_xml_spatial_columnstore", CollectorColumnType.Integer),
        new CollectorColumn("skipped_never_updated", CollectorColumnType.Integer),
        new CollectorColumn("skipped_locked", CollectorColumnType.Integer),
        new CollectorColumn("skipped_over_cap", CollectorColumnType.Integer),
        new CollectorColumn("errors", CollectorColumnType.Integer),
        new CollectorColumn("sample_error", CollectorColumnType.Varchar),
        new CollectorColumn("refused", CollectorColumnType.Varchar),
        new CollectorColumn("complete", CollectorColumnType.Boolean),
        new CollectorColumn("note", CollectorColumnType.Varchar),
    };

    public override void WritePayload(RunRow row, ICollectorRowWriter writer, CollectorContext context)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(writer);

        writer
            .Value(row.DatabaseName)                    /* database_name VARCHAR */
            .Value(row.HistoryKey)                      /* history_key VARCHAR */
            .Value(row.Kind)                            /* kind VARCHAR: baseline|incremental|run_now */
            .Value(row.Checked)                         /* checked INTEGER */
            .Value(row.Read)                            /* read INTEGER */
            .Value(row.Deleted)                         /* deleted INTEGER */
            .Value(row.SkippedFiltered)                 /* skipped_filtered INTEGER */
            .Value(row.SkippedMemoryOptimized)          /* skipped_memory_optimized INTEGER */
            .Value(row.SkippedXmlSpatialColumnstore)    /* skipped_xml_spatial_columnstore INTEGER */
            .Value(row.SkippedNeverUpdated)             /* skipped_never_updated INTEGER */
            .Value(row.SkippedLocked)                   /* skipped_locked INTEGER */
            .Value(row.SkippedOverCap)                  /* skipped_over_cap INTEGER */
            .Value(row.Errors)                          /* errors INTEGER */
            .Value(row.SampleError)                     /* sample_error VARCHAR */
            .Value(row.Refused)                         /* refused VARCHAR: null|no_select|tf9485 */
            .Value(row.Complete)                        /* complete BOOLEAN */
            .Value(row.Note);                           /* note VARCHAR */
    }
}
