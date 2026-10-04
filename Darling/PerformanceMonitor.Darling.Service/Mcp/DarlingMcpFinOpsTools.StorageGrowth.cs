/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Service.Mcp;

public sealed partial class DarlingMcpFinOpsTools
{
    internal const string StorageGrowthView = "storage_growth";

    internal const string StorageGrowthViewLine =
        "storage_growth: database sizes, growth, fastest-growing tables.";

    internal const string StorageGrowthViewGuide =
        "storage_growth answers one level at a time, picked by parameters, beside server, view, level and hours_back; hours_back other than 24 is refused, because the window is a fixed 30 days (the desktop's 7/30/90-day picker is not offered). No database_name: level databases, the section databases (status, message, database_count, truncated, rows) with up to 50 rows, ordered by growth_30d_mb descending (null last), then growth_7d_mb descending (null last), then database_name; database_count is the count before the cap. Rows are database_name, current_size_mb, size_7d_ago_mb, size_30d_ago_mb, growth_7d_mb, growth_30d_mb, daily_growth_rate_mb (2 places), growth_pct_30d (1 place), has_sibling_row, has_log_service_file and note (the desktop's note text, null when none); the past sizes, growth, rate and percent are null when the database has no snapshot at that age, never 0. database_name: level objects, the section database (status, message, row: that database's row, or status empty when it is not in the latest snapshot) and the section objects (status, message, window_days 30, object_count, truncated, days, rows). limit is the object top-N here, 1-20, default 10, and is refused above 20; at the other levels any limit other than the default is refused. Rows are in the desktop's growth order (growth over the 30 days descending with null counted as 0, then schema.table ordinal) and carry object_name ('schema.table'), schema_name, table_name, reserved_mb, used_mb (1 place), total_rows, index_count, growth_mb (1 place, null with no earlier sample), growth_pct (1 place), daily_growth_rate_mb (2 places) and cells. days lists each UTC day with a sample, ascending, as yyyy-MM-ddTHH:mm:ss.fffffffZ; cells has one [mb, band] per day, aligned to days: mb is the table's reserved MB that day (1 place), band an integer 0-7 from log1p(mb) scaled across all the cells from the smallest positive mb (band 0) to the largest (band 7; all bands 7 when every cell is equal); a day with no sample or 0 MB is [null, null]. The heatmap's table set can differ from the grid's near the cut, as on the desktop: the summary read ranks a table with no growth figure last, the daily-series read ranks it as 0 growth, so a table can sit in the grid with an empty heatmap row. database_name plus object_name ('schema.table', exactly as a row of the objects level, from the 20 fastest growers): level indexes, the section database and the section indexes (status, index_count, truncated, rows) with up to 30 rows ordered by index_id then index_name; rows are database_name, schema_name, table_name, index_name, index_type_desc, index_id, reserved_mb (1 place), total_rows, user_seeks, user_scans, user_lookups, total_reads, user_updates, last_user_access_server_local, and classification (Unused, Write-only or Active). last_user_access_server_local is the monitored server's own clock, not UTC, printed yyyy-MM-ddTHH:mm:ss with no Z, null when none. object_name without database_name, or an object_name not among those objects, is refused. Each section's status is ok, empty or not_collected; the whole answer is not_collected only when every section is gated for the server's engine, and empty only when every section is empty and none is gated. Times are UTC and end in Z except last_user_access_server_local.";

    /// <summary>The fixed ceiling on the database list.</summary>
    internal const int MaxStorageGrowthDatabases = 50;

    /// <summary>The fixed ceiling on the index list of one table.</summary>
    internal const int MaxStorageGrowthIndexes = 30;

    /// <summary>The most objects the objects level returns, and the set <c>object_name</c> is resolved against.</summary>
    internal const int MaxStorageGrowthObjects = 20;

    /// <summary>The heatmap's fixed window, the desktop's default.</summary>
    internal const int StorageGrowthWindowDays = 30;

    /// <summary>How many discrete bands the heatmap cells fall into.</summary>
    internal const int StorageGrowthBandCount = 8;

    /// <summary>The database ordering: 30-day growth descending (null last), then 7-day growth the same way, then name, so the order is total.</summary>
    internal static List<StorageGrowthDto> OrderStorageGrowthDatabases(IEnumerable<StorageGrowthDto> rows) =>
        rows.OrderBy(r => r.Growth30dMb is null)
            .ThenByDescending(r => r.Growth30dMb ?? 0m)
            .ThenBy(r => r.Growth7dMb is null)
            .ThenByDescending(r => r.Growth7dMb ?? 0m)
            .ThenBy(r => r.DatabaseName, StringComparer.Ordinal)
            .ToList();

    /// <summary>The index ordering: index id, then name.</summary>
    internal static List<IndexUsageDto> OrderStorageGrowthIndexes(IEnumerable<IndexUsageDto> rows) =>
        rows.OrderBy(r => r.IndexId)
            .ThenBy(r => r.IndexName, StringComparer.Ordinal)
            .ToList();

    private static decimal? RoundGrowth(decimal? value, int places) =>
        value is decimal v ? Math.Round(v, places, MidpointRounding.AwayFromZero) : null;

    /// <summary>One database's row; the note is the desktop's text.</summary>
    internal static object StorageGrowthDatabaseRow(StorageGrowthDto r) => new
    {
        database_name = r.DatabaseName,
        current_size_mb = RoundGrowth(r.CurrentSizeMb, 2),
        size_7d_ago_mb = RoundGrowth(r.Size7dAgoMb, 2),
        size_30d_ago_mb = RoundGrowth(r.Size30dAgoMb, 2),
        growth_7d_mb = RoundGrowth(r.Growth7dMb, 2),
        growth_30d_mb = RoundGrowth(r.Growth30dMb, 2),
        daily_growth_rate_mb = RoundGrowth(r.DailyGrowthRateMb, 2),
        growth_pct_30d = RoundGrowth(r.GrowthPct30d, 1),
        has_sibling_row = r.HasSiblingRow,
        has_log_service_file = r.HasLogServiceFile,
        note = AzureSiblingDatabaseSize.StorageGrowthNote(r.HasLogServiceFile, r.HasSiblingRow),
    };

    /// <summary>The key an object is known by: <c>schema.table</c>.</summary>
    internal static string StorageGrowthObjectKey(ObjectSizeGrowthDto o) => $"{o.SchemaName}.{o.TableName}";

    /// <summary>
    /// The objects in the desktop's order (<see cref="FinOpsHeatmapBuilder.RankTopGrowers"/>: growth descending with a
    /// missing growth counted as 0, then key ordinal), cut to <paramref name="limit"/>; the second value says whether
    /// more were left out.
    /// </summary>
    internal static (List<ObjectSizeGrowthDto> Ranked, bool Truncated) RankStorageGrowthObjects(IReadOnlyCollection<ObjectSizeGrowthDto> objects, int limit)
    {
        var byKey = objects.GroupBy(StorageGrowthObjectKey, StringComparer.Ordinal).ToDictionary(g => g.Key, g => g.First(), StringComparer.Ordinal);
        var keys = FinOpsHeatmapBuilder.RankTopGrowers(
            objects.Select(o => (StorageGrowthObjectKey(o), (double)(o.Growth30dMb ?? 0m))), objects.Count);
        var distinct = keys.Distinct(StringComparer.Ordinal).ToList();
        return (distinct.Take(limit).Select(k => byKey[k]).ToList(), distinct.Count > limit);
    }

    /// <summary>
    /// The ranked objects' rows with their heatmap cells, built the viewer's way: rank, then pivot the day samples
    /// (rows bottom to top, so the biggest grower is the last matrix row), then band the matrix. Days are the
    /// distinct sample days, ascending.
    /// </summary>
    internal static (List<string> Days, List<object> Rows) StorageGrowthObjectRows(
        IReadOnlyList<ObjectSizeGrowthDto> ranked, IEnumerable<FinOpsObjectDaySample> samples)
    {
        var keysTopFirst = ranked.Select(StorageGrowthObjectKey).ToList();
        var shown = keysTopFirst.ToHashSet(StringComparer.Ordinal);
        var matrix = FinOpsHeatmapBuilder.BuildMatrix(Enumerable.Reverse(keysTopFirst).ToList(), samples.Where(x => shown.Contains(x.ObjectKey)));
        var bands = FinOpsHeatmapBuilder.MatrixLogBands(matrix, StorageGrowthBandCount);
        var days = matrix.Days.Select(McpHelpers.FormatEffectiveStart).ToList();
        var rows = new List<object>();
        for (var i = 0; i < ranked.Count; i++)
        {
            var o = ranked[i];
            var matrixRow = ranked.Count - 1 - i;
            var cells = new List<object?[]>();
            for (var c = 0; c < matrix.Days.Length; c++)
            {
                var mb = matrix.Intensities[matrixRow, c];
                cells.Add(mb > 0 ? [Math.Round((decimal)mb, 1, MidpointRounding.AwayFromZero), bands[matrixRow, c]] : [null, null]);
            }
            rows.Add(new
            {
                object_name = keysTopFirst[i],
                schema_name = o.SchemaName,
                table_name = o.TableName,
                reserved_mb = RoundGrowth(o.CurrentReservedMb, 1),
                used_mb = RoundGrowth(o.CurrentUsedMb, 1),
                total_rows = o.TotalRows,
                index_count = o.IndexCount,
                growth_mb = RoundGrowth(o.Growth30dMb, 1),
                growth_pct = RoundGrowth(o.GrowthPct30d, 1),
                daily_growth_rate_mb = RoundGrowth(o.DailyGrowthRateMb, 2),
                cells,
            });
        }
        return (days, rows);
    }

    /// <summary>One index's row: every field of the read, plus the SQL's classification. The access time is the server's own clock.</summary>
    internal static object StorageGrowthIndexRow(IndexUsageDto r) => new
    {
        database_name = r.DatabaseName,
        schema_name = r.SchemaName,
        table_name = r.TableName,
        index_name = r.IndexName,
        index_type_desc = r.IndexTypeDesc,
        index_id = r.IndexId,
        reserved_mb = RoundGrowth(r.ReservedMb, 1),
        total_rows = r.TotalRows,
        user_seeks = r.UserSeeks,
        user_scans = r.UserScans,
        user_lookups = r.UserLookups,
        total_reads = r.TotalReads,
        user_updates = r.UserUpdates,
        /* The monitored server's own clock, read verbatim: no Z, no shifting. */
        last_user_access_server_local = r.LastUserAccess?.ToString("yyyy-MM-dd'T'HH:mm:ss", CultureInfo.InvariantCulture),
        classification = r.Classification,
    };

    internal static string BuildStorageGrowthDatabasesPayload(string server, int hoursBack, List<StorageGrowthDto> databases)
    {
        var ordered = OrderStorageGrowthDatabases(databases);
        return JsonSerializer.Serialize(new
        {
            server,
            view = StorageGrowthView,
            level = "databases",
            hours_back = hoursBack,
            databases = new
            {
                status = "ok",
                message = (string?)null,
                database_count = ordered.Count,
                truncated = ordered.Count > MaxStorageGrowthDatabases,
                rows = ordered.Take(MaxStorageGrowthDatabases).Select(StorageGrowthDatabaseRow).ToList(),
            },
        }, McpHelpers.JsonOptions);
    }

    internal static string BuildStorageGrowthObjectsPayload(
        string server, int hoursBack, object database, IReadOnlyList<ObjectSizeGrowthDto> ranked, bool truncated,
        IEnumerable<FinOpsObjectDaySample> samples, string? objectGate)
    {
        var (days, rows) = StorageGrowthObjectRows(ranked, samples);
        return JsonSerializer.Serialize(new
        {
            server,
            view = StorageGrowthView,
            level = "objects",
            hours_back = hoursBack,
            database,
            objects = new
            {
                status = SectionStatus(objectGate, ranked.Count),
                message = objectGate == null ? null : NotCollectedMessage(objectGate),
                window_days = StorageGrowthWindowDays,
                object_count = ranked.Count,
                truncated,
                days,
                rows,
            },
        }, McpHelpers.JsonOptions);
    }

    internal static string BuildStorageGrowthIndexesPayload(
        string server, int hoursBack, object database, List<IndexUsageDto> indexes, string? indexGate)
    {
        var ordered = OrderStorageGrowthIndexes(indexes);
        return JsonSerializer.Serialize(new
        {
            server,
            view = StorageGrowthView,
            level = "indexes",
            hours_back = hoursBack,
            database,
            indexes = new
            {
                status = SectionStatus(indexGate, indexes.Count),
                message = indexGate == null ? null : NotCollectedMessage(indexGate),
                index_count = ordered.Count,
                truncated = ordered.Count > MaxStorageGrowthIndexes,
                rows = ordered.Take(MaxStorageGrowthIndexes).Select(StorageGrowthIndexRow).ToList(),
            },
        }, McpHelpers.JsonOptions);
    }

    internal static object StorageGrowthDatabaseSection(List<StorageGrowthDto> rows, string databaseName, string? gate)
    {
        var row = rows.FirstOrDefault(r => string.Equals(r.DatabaseName, databaseName, StringComparison.Ordinal));
        return new
        {
            status = SectionStatus(gate, row == null ? 0 : 1),
            message = gate == null ? null : NotCollectedMessage(gate),
            row = row == null ? null : StorageGrowthDatabaseRow(row),
        };
    }

    private static async Task<string> ReadStorageGrowthAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, int hoursBack, int limit,
        string? databaseName, string? objectName, CancellationToken ct)
    {
        if (hoursBack != 24)
            return McpHelpers.Refusal("hours_back",
                $"Invalid hours_back value '{hoursBack}': view {StorageGrowthView} reads a fixed {StorageGrowthWindowDays} days; hours_back does not apply. Omit it or pass 24.");
        if (databaseName == null && objectName != null)
            return McpHelpers.Refusal("object_name", $"object_name needs database_name: {objectName} is a table in one database. Pass database_name too, or omit object_name.");
        var objectsLevel = databaseName != null && objectName == null;
        if (objectsLevel && limit > MaxStorageGrowthObjects)
            return McpHelpers.Refusal("limit", $"Invalid limit value '{limit}': the objects level of view {StorageGrowthView} returns at most {MaxStorageGrowthObjects} objects.");
        if (!objectsLevel && limit != DefaultLimit)
            return McpHelpers.Refusal("limit",
                $"limit applies only to the objects level (database_name without object_name) of view {StorageGrowthView}; omit it for the {(databaseName == null ? "databases" : "indexes")} level.");

        var now = DateTime.UtcNow;
        var timeout = McpCommandDeadlines.ReadSeconds;
        var databases = await DarlingFinOpsStorageGrowthReader.GetStorageGrowthAsync(postgres, resolved.ServerId, now, timeout, ct);
        var dbGate = databases.Count == 0 ? await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "database_size_stats", ct) : null;

        if (databaseName == null)
        {
            if (dbGate != null) return dbGate;
            if (IsBareEmpty([databases.Count], [dbGate]))
                return McpHelpers.Status("empty", "No database size snapshot was found for this server, so there is no storage growth to show.");
            return BuildStorageGrowthDatabasesPayload(resolved.ServerName, hoursBack, databases);
        }

        var windowStart = DateTime.SpecifyKind(now.AddDays(-StorageGrowthWindowDays), DateTimeKind.Unspecified);
        /* One more than the cut, so the cut is known; the indexes level resolves against the largest set. */
        var topN = (objectsLevel ? limit : MaxStorageGrowthObjects) + 1;
        var (objects, samples) = await DarlingFinOpsStorageGrowthReader.GetObjectGrowthHeatmapDataAsync(
            postgres, resolved.ServerId, databaseName, windowStart, StorageGrowthWindowDays, topN, timeout, ct);
        var objectGate = objects.Count == 0 ? await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "index_object_stats", ct) : null;

        if (dbGate != null && objectGate != null) return dbGate;
        if (IsBareEmpty([databases.Count, objects.Count], [dbGate, objectGate]))
            return McpHelpers.Status("empty", "No database size snapshot or object size data was found for this server, so there is no storage growth to show.");

        var (ranked, truncated) = RankStorageGrowthObjects(objects, objectsLevel ? limit : MaxStorageGrowthObjects);
        var database = StorageGrowthDatabaseSection(databases, databaseName, dbGate);

        if (objectsLevel)
        {
            return BuildStorageGrowthObjectsPayload(resolved.ServerName, hoursBack, database, ranked, truncated, samples, objectGate);
        }

        var match = ranked.FirstOrDefault(o => string.Equals(StorageGrowthObjectKey(o), objectName, StringComparison.Ordinal));
        if (match == null)
            return McpHelpers.Refusal("object_name",
                $"object_name '{objectName}' is not among the {MaxStorageGrowthObjects} fastest-growing objects of database '{databaseName}'. Read the objects level (database_name only) and pass an object_name from its rows.");

        var indexes = await DarlingFinOpsStorageGrowthReader.GetObjectIndexDetailAsync(
            postgres, resolved.ServerId, databaseName, match.SchemaName, match.TableName, timeout, ct);
        var indexGate = indexes.Count == 0 ? await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "index_object_stats", ct) : null;
        return BuildStorageGrowthIndexesPayload(resolved.ServerName, hoursBack, database, indexes, indexGate);
    }
}
