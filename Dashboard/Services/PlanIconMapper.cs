using System;
using System.Collections.Generic;
using System.Windows.Media.Imaging;

namespace PerformanceMonitorDashboard.Services;

public static class PlanIconMapper
{
    private static readonly Dictionary<string, BitmapImage> _iconCache = new();

    // PhysicalOp (with spaces) → icon filename (without .png)
    private static readonly Dictionary<string, string> IconMap = new(StringComparer.OrdinalIgnoreCase)
    {
        // Join operators
        ["Adaptive Join"] = "adaptive_join",
        ["Hash Match"] = "hash_match",
        ["Merge Join"] = "merge_join",
        ["Nested Loops"] = "nested_loops",

        // Index operations
        ["Clustered Index Delete"] = "clustered_index_delete",
        ["Clustered Index Insert"] = "clustered_index_insert",
        ["Clustered Index Merge"] = "clustered_index_merge",
        ["Clustered Index Scan"] = "clustered_index_scan",
        ["Clustered Index Seek"] = "clustered_index_seek",
        ["Clustered Index Update"] = "clustered_index_update",
        ["Clustered Update"] = "clustered_update",
        ["Index Delete"] = "index_delete",
        ["Index Insert"] = "index_insert",
        ["Index Scan"] = "index_scan",
        ["Index Seek"] = "index_seek",
        ["Index Spool"] = "index_spool",
        ["Index Update"] = "index_update",

        // Columnstore
        ["Columnstore Index Delete"] = "columnstore_index_delete",
        ["Columnstore Index Insert"] = "columnstore_index_insert",
        ["Columnstore Index Merge"] = "columnstore_index_merge",
        ["Columnstore Index Scan"] = "columnstore_index_scan",
        ["Columnstore Index Update"] = "columnstore_index_update",

        // Scan operators
        ["Table Scan"] = "table_scan",
        ["Constant Scan"] = "constant_scan",
        ["Deleted Scan"] = "deleted_scan",
        ["Inserted Scan"] = "inserted_scan",
        ["Log Row Scan"] = "log_row_scan",
        ["Parameter Table Scan"] = "parameter_table_scan",

        // Table DML
        ["Table Delete"] = "table_delete",
        ["Table Insert"] = "table_insert",
        ["Table Merge"] = "table_merge",
        ["Table Update"] = "table_update",

        // Lookup
        ["Key Lookup"] = "bookmark_lookup",
        ["RID Lookup"] = "rid_lookup",
        ["Bookmark Lookup"] = "bookmark_lookup",

        // Aggregation
        ["Stream Aggregate"] = "stream_aggregate",
        ["Window Aggregate"] = "window_aggregate",
        ["Group By Aggregate"] = "group_by_aggregate",

        // Scalar / compute
        ["Compute Scalar"] = "compute_scalar",
        ["Filter"] = "filter",
        ["Assert"] = "assert",

        // Sort / top
        ["Sort"] = "sort",
        ["Top"] = "top",

        // Spool
        ["Table Spool"] = "table_spool",
        ["Row Count Spool"] = "row_count_spool",
        ["Window Spool"] = "table_spool",
        ["Eager Spool"] = "table_spool",
        ["Lazy Spool"] = "table_spool",
        ["Spool"] = "spool",

        // Set operations
        ["Concatenation"] = "concatenation",
        ["Union"] = "union",
        ["Union All"] = "union_all",

        // Parallelism
        ["Parallelism"] = "parallelism",
        ["Distribute Streams"] = "parallelism",
        ["Gather Streams"] = "parallelism",
        ["Repartition Streams"] = "parallelism",

        // Remote
        ["Remote Query"] = "remote_query",
        ["Remote Scan"] = "remote_scan",
        ["Remote Index Scan"] = "remote_index_scan",
        ["Remote Index Seek"] = "remote_index_seek",
        ["Remote Insert"] = "remote_insert",
        ["Remote Delete"] = "remote_delete",
        ["Remote Update"] = "remote_update",

        // Miscellaneous
        ["Bitmap"] = "bitmap",
        ["Batch Hash Table Build"] = "batch_hash_table_build",
        ["Collapse"] = "collapse",
        ["Distinct"] = "sort",
        ["Foreign Key References Check"] = "foreign_key_references_check",
        ["Merge Interval"] = "merge_interval",
        ["Print"] = "print",
        ["Rank"] = "rank",
        ["Segment"] = "segment",
        ["Sequence"] = "sequence",
        ["Sequence Project"] = "sequence_project",
        ["Split"] = "split",
        ["Switch"] = "switch",
        ["Table-valued function"] = "table_valued_function",
        ["UDX"] = "udx",
        ["Predict"] = "predict",
        ["Apply"] = "apply",

        // PDW / distributed
        ["Broadcast"] = "broadcast",
        ["Compute To Control Node"] = "compute_to_control_node",
        ["Const Table Get"] = "const_table_get",
        ["Control To Compute Nodes"] = "control_to_compute_nodes",
        ["External Broadcast"] = "external_broadcast",
        ["External Export"] = "external_export",
        ["External Local Streaming"] = "external_local_streaming",
        ["External Round Robin"] = "external_round_robin",
        ["External Shuffle"] = "external_shuffle",
        ["Get"] = "get",
        ["Group By Apply"] = "apply",
        ["Join"] = "join",
        ["Project"] = "project",
        ["Shuffle"] = "shuffle",
        ["Single Source Round Robin"] = "single_source_round_robin",
        ["Single Source Shuffle"] = "single_source_shuffle",
        ["Trim"] = "trim",

        // Cursor operators
        ["Fetch Query"] = "fetch_query",
        ["Population Query"] = "population_query",
        ["Refresh Query"] = "refresh_query",

        // Legacy / Shiloh
        ["Result"] = "result",
        ["Aggregate"] = "aggregate",
        ["Assign"] = "assign",
        ["Arithmetic Expression"] = "arithmetic_expression",
        ["Convert"] = "convert",
        ["Declare"] = "declare",
        ["Delete"] = "delete",
        ["Dynamic"] = "dynamic",
        ["Hash Match Root"] = "hash_match_root",
        ["Hash Match Team"] = "hash_match_team",
        ["If"] = "if",
        ["Insert"] = "insert",
        ["Intrinsic"] = "intrinsic",
        ["Keyset"] = "keyset",
        ["Locate"] = "locate",
        ["Set Function"] = "set_function",
        ["Snapshot"] = "snapshot",
        ["TSQL"] = "sql",
        ["Update"] = "update",

        // Catch-all
        ["Cursor Catch All"] = "cursor_catch_all",
        ["Language Construct Catch All"] = "language_construct_catch_all",
    };

    public static string GetIconName(string physicalOp)
    {
        if (IconMap.TryGetValue(physicalOp, out var iconName))
            return iconName;

        // Try removing spaces as fallback
        var normalized = physicalOp.Replace(" ", "_").ToLowerInvariant();
        return normalized;
    }

    public static BitmapImage? GetIcon(string iconName)
    {
        if (_iconCache.TryGetValue(iconName, out var cached))
            return cached;

        try
        {
            var uri = new Uri($"pack://application:,,,/Resources/PlanIcons/{iconName}.png", UriKind.Absolute);
            var bitmap = new BitmapImage();
            bitmap.BeginInit();
            bitmap.UriSource = uri;
            bitmap.CacheOption = BitmapCacheOption.OnLoad;
            bitmap.DecodePixelWidth = 32;
            bitmap.EndInit();
            bitmap.Freeze();
            _iconCache[iconName] = bitmap;
            return bitmap;
        }
        catch
        {
            // Try fallback icon
            if (iconName != "iterator_catch_all")
                return GetIcon("iterator_catch_all");
            return null;
        }
    }
}
