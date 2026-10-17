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

namespace PerformanceMonitorLite.Services;

public partial class RemoteCollectorService
{
    /* Static: the definition runner is a static method, and the map pass is the only flusher. */
    private static readonly QueryTagSightingBuffer _queryTagSightings = new();

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
            using (var flushConnection = _duckDb.CreateConnection())
            {
                flushConnection.Open();
                _queryTagSightings.Flush(flushConnection);
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

    private readonly Dictionary<int, QueryTagMapCursor> _queryTagMapCursors = new();
}
