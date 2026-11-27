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
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>The index half of the Store Health tab: the index rows and the partition coverage rows, read together (the coverage read is
/// a few milliseconds beside the index read). When only the coverage read runs past its limit, the index rows stand, <see cref="Coverage"/>
/// is empty and <see cref="CoverageLeftOutNote"/> says the coverage was left out. <see cref="ReadAtUtc"/> is when the store answered the
/// older of the reads behind these rows (the two are held separately, #8924); null when a caller built the rows itself.</summary>
public sealed record StoreIndexHealth(
    IReadOnlyList<DarlingStoreTableHealthReader.StoreIndexHealthRow> Indexes,
    IReadOnlyList<DarlingStoreTableHealthReader.StoreIndexCoverageRow> Coverage,
    string? CoverageLeftOutNote = null,
    DateTime? ReadAtUtc = null);

/// <summary>
/// #9374: Store Health's first open from the store's last measured relation sizes: the tables rows and when the sizes were measured. The
/// tables read says the store answered it now (<c>ReadAtUtc</c> is the call's time): only the sizes are from <see cref="MeasuredAtUtc"/>.
/// Rows of this read are never put in the held cache. There are no index rows: the live indexes read runs beside this read and costs about as
/// much, so reading them here too would double the store's index work and show nothing sooner.
/// </summary>
public sealed record StoreLastSizesHealth(
    StoreHealthRead<IReadOnlyList<DarlingStoreTableHealthReader.StoreTableHealthRow>> Tables,
    DateTime MeasuredAtUtc);

/// <summary>
/// The reads the Store Health tab makes (#8787): the store's own tables and indexes, whether now is a safe time for maintenance, and a
/// measured page occupancy for one plain table. Each calls the Storage reader the web page and the MCP tools call, over the Viewer's own
/// store connection, so the three surfaces show the same figures. Read only: nothing here writes to the store. The tables and indexes
/// reads are separate calls on purpose (about 1.5 s and 1 s on a busy machine), so the tab can show the tables as soon as theirs returns.
///
/// <para>#8924: the tables and indexes reads go through the store's <see cref="DarlingStoreTableHealthCache"/>, which holds each for
/// <see cref="DarlingStoreTableHealthCache.Lifetime"/>. The tab's refresh timer fires every 30 seconds while the tab is open, and each
/// statement sizes every relation of the store, so a refresh inside the lifetime is answered from the held rows and sends the store
/// nothing. <c>fresh</c> is the tab's Refresh button: it drops the held rows and reads the store. Each answer carries the time the store
/// gave it, which the tab shows.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>#9224: the store's held reads, with a refresh that fails behind a held part written to the Viewer log (the held part stays on
    /// screen with its own read time). The Viewer never warms: most sessions never open Store Health.</summary>
    private DarlingStoreTableHealthCache HeldStoreHealth()
    {
        var held = DarlingStoreTableHealthCache.For(_dataSource);
        held.OnReadFailed ??= static (part, ex) =>
            ViewerLogger.Warn("StoreHealth", $"The store table health read of {part} failed ({ex.GetType().Name}); the next refresh after the lifetime tries again.");
        return held;
    }

    /// <summary>One row per store table, behind on autovacuum first (the reader's own order), with the time the store answered.</summary>
    public Task<StoreHealthRead<IReadOnlyList<DarlingStoreTableHealthReader.StoreTableHealthRow>>> GetStoreTableHealthAsync(
        bool fresh = false, CancellationToken cancellationToken = default) =>
        HeldStoreHealth().GetTablesAsync(_dataSource, fresh, cancellationToken);

    /// <summary>The store's autovacuum settings, the workers running and the cancels the store log holds in the last day, held like the
    /// tables read. A read past its limit throws <see cref="StoreTableHealthTimeoutException"/>.</summary>
    public Task<StoreHealthRead<DarlingStoreTableHealthReader.StoreAutovacuumSettings>> GetStoreAutovacuumSettingsAsync(
        bool fresh = false, CancellationToken cancellationToken = default) =>
        HeldStoreHealth().GetAutovacuumSettingsAsync(_dataSource, fresh, cancellationToken);

    /// <summary>One row per store index, and the leaf index coverage of each partitioned parent.</summary>
    public Task<StoreIndexHealth> GetStoreIndexHealthAsync(bool fresh = false, CancellationToken cancellationToken = default) =>
        ReadStoreIndexHealthAsync(HeldStoreHealth(), _dataSource, fresh, cancellationToken);

    /// <summary>The index half of the tab from <paramref name="held"/>, the cache of <paramref name="store"/>: the index rows, then the
    /// coverage rows. A test hands in a cache over reads that count their calls.</summary>
    internal static async Task<StoreIndexHealth> ReadStoreIndexHealthAsync(
        DarlingStoreTableHealthCache held, NpgsqlDataSource store, bool fresh, CancellationToken cancellationToken)
    {
        var indexes = await held.GetIndexesAsync(store, fresh, cancellationToken);
        try
        {
            var coverage = await held.GetIndexCoverageAsync(store, fresh, cancellationToken);
            var older = coverage.ReadAtUtc < indexes.ReadAtUtc ? coverage.ReadAtUtc : indexes.ReadAtUtc;
            return new StoreIndexHealth(indexes.Rows, coverage.Rows, ReadAtUtc: older);
        }
        catch (StoreTableHealthTimeoutException ex)
        {
            return new StoreIndexHealth(indexes.Rows, Array.Empty<DarlingStoreTableHealthReader.StoreIndexCoverageRow>(), ex.Message, indexes.ReadAtUtc);
        }
    }

    /// <summary>#9374: whether the store's held reads already hold a tables read. The tab shows the last measured sizes only when they do not.</summary>
    public bool HoldsStoreTables => HeldStoreHealth().HoldsTables;

    /// <summary>#9374: the tables rows from the store's last measured relation sizes, or null when there are none to show (a store
    /// behind the sizes table, an empty table, or a read of them that failed), so the caller reads live as it always did. A failure other than
    /// the caller's cancel is written to the Viewer log at Debug and answered with null, so a store or role that cannot read the sizes tables
    /// costs the tab nothing. The rows are not held: the held reads stay the live ones.</summary>
    public Task<StoreLastSizesHealth?> GetStoreLastSizesHealthAsync(CancellationToken cancellationToken = default) =>
        ReadLastSizesHealthAsync(_dataSource, StoreRelationSizesStore.ReadLastSizesAsync, DateTime.UtcNow, cancellationToken);

    /// <summary>The read behind <see cref="GetStoreLastSizesHealthAsync"/> over <paramref name="readLastSizes"/>, as of <paramref name="asOfUtc"/>.
    /// Only the tables read runs from the pass: the live indexes read runs beside this one, so the indexes are not read here.</summary>
    internal static async Task<StoreLastSizesHealth?> ReadLastSizesHealthAsync(
        NpgsqlDataSource store,
        Func<NpgsqlDataSource, CancellationToken, Task<MeasuredSizePass?>> readLastSizes,
        DateTime asOfUtc,
        CancellationToken cancellationToken)
    {
        try
        {
            var measured = await readLastSizes(store, cancellationToken);
            if (measured is null)
            {
                return null;
            }

            var tables = await DarlingStoreTableHealthReader.GetAsync(store, asOfUtc, measured.Pass, cancellationToken);
            return new StoreLastSizesHealth(
                new StoreHealthRead<IReadOnlyList<DarlingStoreTableHealthReader.StoreTableHealthRow>>(tables, asOfUtc),
                measured.MeasuredAtUtc);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            ViewerLogger.Debug("StoreHealth", $"The store's last measured relation sizes could not be read ({ex.GetType().Name}); the live read runs instead.");
            return null;
        }
    }

    /// <summary>The "safe for maintenance now" verdict with its reasons.</summary>
    public Task<DarlingStoreMaintenanceReadinessReader.StoreMaintenanceReadiness> GetStoreMaintenanceReadinessAsync(CancellationToken cancellationToken = default) =>
        DarlingStoreMaintenanceReadinessReader.GetAsync(_dataSource, cancellationToken: cancellationToken);

    /// <summary>Measures one table, named schema-qualified (<c>collect.store_metrics</c>). A refused or slow table comes back as a result
    /// with a sentence, never as an exception.</summary>
    public Task<DarlingStoreTableMeasureReader.StoreTableMeasureResult> MeasureStoreTableAsync(string qualifiedName, CancellationToken cancellationToken = default) =>
        DarlingStoreTableMeasureReader.MeasureAsync(_dataSource, qualifiedName, cancellationToken);
}
