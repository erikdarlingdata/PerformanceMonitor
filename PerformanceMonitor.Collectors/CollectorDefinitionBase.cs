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
/// Convenience base for collector definitions: the optional contract members default to the
/// common case (no watermark, applies everywhere, single connection, no supplemental query),
/// so a definition overrides only what actually differs. Existing definitions state their
/// choices explicitly via overrides — that explicitness is deliberate.
/// </summary>
public abstract class CollectorDefinitionBase<TRow> : ICollectorDefinition<TRow>
{
    public abstract string Name { get; }

    public abstract string TargetTable { get; }

    public virtual bool IncludesCollectionId => true;

    public virtual string PrefixIdColumnName => "collection_id";

    public virtual string PrefixTimeColumnName => "collection_time";

    public virtual int? CommandTimeoutSecondsOverride => null;

    public abstract IReadOnlyList<CollectorColumn> PayloadColumns { get; }

    public virtual string? WatermarkColumn => null;

    public virtual string? NumericWatermarkColumn => null;

    public virtual string? PerDatabaseWatermarkColumn => null;

    public virtual IReadOnlyList<string> StateKeys => System.Array.Empty<string>();

    public virtual int? PerItemRowCountWarnThreshold => null;

    public virtual int? PerItemTextByteBudget => null;

    public virtual TimeSpan? PerItemWallClockBudget => null;

    public virtual CollectorTargetEngine TargetEngine => CollectorTargetEngine.SqlServer;

    /// <summary>
    /// Target-shape gating WITHIN this definition's engine — hosting flavour, version floors, msdb
    /// reach. Overrides do not need to consider <see cref="TargetEngine"/>: the engine check is
    /// composed on top by <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>,
    /// which is what the runners call, so a T-SQL definition can never reach a non-SQL-Server target
    /// even though this returns true by default.
    /// </summary>
    public virtual bool AppliesTo(CollectorTargetInfo target) => true;

    public virtual bool YieldsOnLockTimeout => false;

    public virtual bool RunsPerDatabase(CollectorTargetInfo target) => false;

    public abstract CollectorQuery BuildQuery(CollectorContext context);

    public virtual CollectorQuery? BuildSupplementalQuery(CollectorContext context) => null;

    public virtual ValueTask ApplySupplementalAsync(List<TRow> rows, DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
        => ValueTask.CompletedTask;

    public virtual bool EmitsProbeFailures => false;

    public virtual CollectorQuery? BuildEnumerationQuery(CollectorContext context) => null;

    public virtual CollectorQuery? BuildEnumerationProbe(CollectorContext context) => null;

    public virtual CollectorQuery BuildPerItemQuery(string item, CollectorContext context)
        => throw new System.NotSupportedException($"{Name} does not enumerate items.");

    public virtual ValueTask ReadItemAsync(string item, DbDataReader reader, List<TRow> rows, CollectorContext context, CancellationToken cancellationToken)
        => throw new System.NotSupportedException($"{Name} does not enumerate items.");

    public abstract ValueTask<List<TRow>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken);

    public abstract void WritePayload(TRow row, ICollectorRowWriter writer, CollectorContext context);
}
