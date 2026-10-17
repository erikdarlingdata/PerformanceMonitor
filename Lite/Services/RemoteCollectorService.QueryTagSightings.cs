/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /* Static: the definition runner is a static method, and the map pass is the only flusher. */
    private static readonly QueryTagSightingBuffer _queryTagSightings = new();

    /* Tests swap in their own buffer so a parallel class's collector run cannot flush their rows into another database. */
    private QueryTagSightingBuffer? _queryTagSightingsOverride;

    internal QueryTagSightingBuffer QueryTagSightingBufferForTests
    {
        get => _queryTagSightingsOverride ?? _queryTagSightings;
        set => _queryTagSightingsOverride = value;
    }

    /* The static batch writer cannot see the instance's store; the last constructed service names it for the overflow flush. */
    private static DuckDbInitializer? s_queryTagFlushDb;

    /// <summary>
    /// #5990: the map step after a collection round. Rebuilds the tag map when the rules are ahead of it, otherwise
    /// extends it for the keys sighted since the last pass. Failure-isolated like the sightings write.
    /// </summary>
    internal async Task RunQueryTagMapPassAsync(CancellationToken cancellationToken)
    {
        try
        {
            /* The cursors live in memory, per server, and only a pass that committed moves them (the pass updates the
               dictionary after its commit). A fresh process starts from the epoch. */
            /* The buffered sightings reach the store in one upsert, before the map reads them. */
            var buffer = QueryTagSightingBufferForTests;
            buffer.Flush(_duckDb, cancellationToken: cancellationToken);
            if (buffer.SchemaHeldBack(_duckDb))
            {
                return;
            }

            var result = await new LocalDataService(_duckDb).BuildQueryTagMapAsync(_queryTagMapCursors);
            if (result.KeysResolved > 0)
            {
                AppLogger.Debug("QueryTags", $"Tag map {(result.Rebuilt ? "rebuilt" : "extended")}: {result.KeysResolved} keys resolved, {result.KeysTagged} tagged");
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            AppLogger.Warn("QueryTags", $"The query-tag map pass failed; the next pass retries: {ex.Message}");
        }
    }

    /// <summary>How long the shutdown flush waits for the write lock. The window closing waits only five seconds for the
    /// collection loop, so the flush stays inside that.</summary>
    internal static readonly TimeSpan ShutdownFlushLimit = TimeSpan.FromSeconds(3);

    /// <summary>
    /// #5990: the shutdown drain. A round cancelled before its map pass never flushes, so the loop calls this once it has
    /// stopped. Writes nothing when the limit passes before the lock is free; the sightings are derived data.
    /// </summary>
    internal void FlushQueryTagSightingsAtShutdown(CancellationToken cancellationToken = default)
    {
        try
        {
            var buffer = QueryTagSightingBufferForTests;
            buffer.Flush(_duckDb, ShutdownFlushLimit, cancellationToken);
            if (buffer.Count > 0)
            {
                AppLogger.Warn("QueryTags", $"The shutdown query-tag sightings flush did not finish; {buffer.Count} buffered sightings are lost (derived data, re-sighted after the next start)");
            }
        }
        catch (Exception ex)
        {
            AppLogger.Warn("QueryTags", $"The shutdown query-tag sightings flush failed: {ex.Message}");
        }
    }

    private readonly Dictionary<int, QueryTagMapCursor> _queryTagMapCursors = new();
}
