/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>The index half of the Store Health tab: the index rows and the partition coverage rows, read together (the coverage read is
/// a few milliseconds beside the index read).</summary>
public sealed record StoreIndexHealth(
    IReadOnlyList<DarlingStoreTableHealthReader.StoreIndexHealthRow> Indexes,
    IReadOnlyList<DarlingStoreTableHealthReader.StoreIndexCoverageRow> Coverage);

/// <summary>
/// The reads the Store Health tab makes (#8787): the store's own tables and indexes, whether now is a safe time for maintenance, and a
/// measured page occupancy for one plain table. Each calls the Storage reader the web page and the MCP tools call, over the Viewer's own
/// store connection, so the three surfaces show the same figures. Read only: nothing here writes to the store. The tables and indexes
/// reads are separate calls on purpose (about 1.5 s and 1 s on a busy machine), so the tab can show the tables as soon as theirs returns.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>One row per store table, rewrite candidates first (the reader's own order).</summary>
    public Task<List<DarlingStoreTableHealthReader.StoreTableHealthRow>> GetStoreTableHealthAsync(CancellationToken cancellationToken = default) =>
        DarlingStoreTableHealthReader.GetAsync(_dataSource, cancellationToken);

    /// <summary>One row per store index, and the leaf index coverage of each partitioned parent.</summary>
    public async Task<StoreIndexHealth> GetStoreIndexHealthAsync(CancellationToken cancellationToken = default)
    {
        var indexes = await DarlingStoreTableHealthReader.GetIndexesAsync(_dataSource, cancellationToken);
        var coverage = await DarlingStoreTableHealthReader.GetIndexCoverageAsync(_dataSource, cancellationToken);
        return new StoreIndexHealth(indexes, coverage);
    }

    /// <summary>The "safe for maintenance now" verdict with its reasons.</summary>
    public Task<DarlingStoreMaintenanceReadinessReader.StoreMaintenanceReadiness> GetStoreMaintenanceReadinessAsync(CancellationToken cancellationToken = default) =>
        DarlingStoreMaintenanceReadinessReader.GetAsync(_dataSource, cancellationToken: cancellationToken);

    /// <summary>Measures one table, named schema-qualified (<c>collect.store_metrics</c>). A refused or slow table comes back as a result
    /// with a sentence, never as an exception.</summary>
    public Task<DarlingStoreTableMeasureReader.StoreTableMeasureResult> MeasureStoreTableAsync(string qualifiedName, CancellationToken cancellationToken = default) =>
        DarlingStoreTableMeasureReader.MeasureAsync(_dataSource, qualifiedName, cancellationToken);
}
