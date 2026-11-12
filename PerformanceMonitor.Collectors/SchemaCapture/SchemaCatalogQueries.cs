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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #7643: the T-SQL that reads one database's user tables and views for the schema capture, as a batch that runs inside that database.
/// The caller wraps it in <c>EXECUTE [db].sys.sp_executesql</c>, quote-doubled, as IndexObjectStatsCollector.BuildPerItemQuery does;
/// on Azure it runs the batch directly on the database connection. Result sets, in order: (1) one row, <c>has_view_definition</c> and
/// <c>object_count</c>; (2) objects; (3) columns; (4) indexes; (5) constraints; (6) triggers. Sets 3 to 6 start with the object_id.
/// Lists (key, included, foreign key and referenced columns) are comma-joined with <c>\</c> and <c>,</c> escaped by a backslash, built
/// with FOR XML PATH because STRING_AGG needs SQL Server 2017 and the floor here is 2016.
/// </summary>
public static class SchemaCatalogQueries
{
    /// <summary>A full read returns at most this many objects, in object_id order; one more is observed so overflow is seen, not inferred.</summary>
    public const int MaxObjectsPerRead = 10000;

    /// <summary>A targeted read inlines at most this many ids; more is the caller's cue to do a full read.</summary>
    public const int MaxTargetedIds = 1000;

    public static string BuildFullRead() => BuildFullRead(MaxObjectsPerRead);

    /* The cap is a parameter only so a test can reach the over-cap branch without ten thousand tables. */
    internal static string BuildFullRead(int maxObjects)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxObjects, 1);
        var take = (maxObjects + 1).ToString(CultureInfo.InvariantCulture);
        var cap = maxObjects.ToString(CultureInfo.InvariantCulture);

        return Prologue
            + "INSERT #pm_schema_ids (object_id)\r\n"
            + "SELECT TOP (" + take + ") o.object_id\r\n"
            + "FROM sys.objects AS o\r\n"
            + "WHERE o.type IN ('U', 'V') AND o.is_ms_shipped = 0\r\n"
            + "ORDER BY o.object_id;\r\n"
            + "DECLARE @object_count int = (SELECT COUNT(*) FROM #pm_schema_ids);\r\n"
            + "IF @object_count > " + cap + "\r\n"
            + "BEGIN\r\n"
            + "    DELETE FROM #pm_schema_ids WHERE object_id = (SELECT MAX(object_id) FROM #pm_schema_ids);\r\n"
            + "END;\r\n"
            + ResultSets;
    }

    public static string BuildTargetedRead(IReadOnlyCollection<int> objectIds)
    {
        ArgumentNullException.ThrowIfNull(objectIds);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(objectIds.Count, MaxTargetedIds);

        var list = string.Join(",", objectIds.Distinct().Order().Select(i => i.ToString(CultureInfo.InvariantCulture)));
        var filter = list.Length == 0 ? "1 = 0" : "o.object_id IN (" + list + ")";

        return Prologue
            + "INSERT #pm_schema_ids (object_id)\r\n"
            + "SELECT o.object_id\r\n"
            + "FROM sys.objects AS o\r\n"
            + "WHERE o.type IN ('U', 'V') AND o.is_ms_shipped = 0 AND " + filter + ";\r\n"
            + "DECLARE @object_count int = (SELECT COUNT(*) FROM #pm_schema_ids);\r\n"
            + ResultSets;
    }

    /* LOCK_TIMEOUT as the other per-database catalog collectors set it (IdentityCapacityCollector, DatabaseProtectionCollector): DDL left
       open in a transaction, or an offline rebuild of a large index, fails the read in five seconds with error 1222 instead of holding it
       for the whole database budget, and the failed read is queued for the next pass. Never READ UNCOMMITTED: that would record an
       uncommitted ALTER that may roll back, and no event would ever correct that version. */
    private const string Prologue =
        "SET NOCOUNT ON; /* PerformanceMonitorLite */\r\n"
        + "SET LOCK_TIMEOUT 5000;\r\n"
        + "IF OBJECT_ID(N'tempdb..#pm_schema_ids') IS NOT NULL DROP TABLE #pm_schema_ids;\r\n"
        + "CREATE TABLE #pm_schema_ids (object_id int NOT NULL PRIMARY KEY);\r\n";

    /* STUFF over a FOR XML PATH string: the delimiter is a comma, a name's own backslash and comma are escaped first, and .value('.')
       turns the XML entities back into the characters. A descending index key is written "Name DESC". */
    private const string ResultSets = """
SELECT
    CONVERT(bit, ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'VIEW DEFINITION'), 0)) AS has_view_definition,
    @object_count AS object_count;

SELECT
    o.object_id,
    SCHEMA_NAME(o.schema_id) AS schema_name,
    o.name,
    o.type,
    o.modify_date,
    t.temporal_type_desc,
    hs.name + N'.' + ht.name AS history_table,
    t.is_memory_optimized,
    t.durability_desc,
    p.partition_scheme,
    p.partition_column,
    t.lock_escalation_desc,
    sm.is_schema_bound,
    CASE WHEN o.type = 'V'
         THEN CONVERT(bit, CASE WHEN EXISTS (SELECT 1 FROM sys.indexes AS vi WHERE vi.object_id = o.object_id AND vi.index_id = 1) THEN 1 ELSE 0 END)
    END AS is_indexed,
    sm.definition AS view_definition
FROM #pm_schema_ids AS i
JOIN sys.objects AS o ON o.object_id = i.object_id
LEFT JOIN sys.tables AS t ON t.object_id = o.object_id
LEFT JOIN sys.tables AS ht ON ht.object_id = t.history_table_id
LEFT JOIN sys.schemas AS hs ON hs.schema_id = ht.schema_id
LEFT JOIN sys.sql_modules AS sm ON sm.object_id = o.object_id AND o.type = 'V'
OUTER APPLY
(
    SELECT TOP (1)
        ps.name AS partition_scheme,
        (
            SELECT TOP (1) pc.name
            FROM sys.index_columns AS ic
            JOIN sys.columns AS pc ON pc.object_id = ic.object_id AND pc.column_id = ic.column_id
            WHERE ic.object_id = x.object_id AND ic.index_id = x.index_id AND ic.partition_ordinal > 0
            ORDER BY ic.partition_ordinal
        ) AS partition_column
    FROM sys.indexes AS x
    JOIN sys.partition_schemes AS ps ON ps.data_space_id = x.data_space_id
    WHERE x.object_id = o.object_id AND x.index_id IN (0, 1)
) AS p
ORDER BY o.object_id;

SELECT
    c.object_id,
    c.column_id,
    c.name,
    ty.name AS type_name,
    CONVERT(int, c.max_length) AS max_length,
    CONVERT(int, c.precision) AS precision,
    CONVERT(int, c.scale) AS scale,
    c.is_nullable,
    CONVERT(decimal(38, 0), idc.seed_value) AS identity_seed,
    CONVERT(decimal(38, 0), idc.increment_value) AS identity_increment,
    cc.definition AS computed_definition,
    cc.is_persisted,
    dc.name AS default_name,
    dc.definition AS default_definition,
    c.collation_name,
    c.is_sparse,
    c.is_rowguidcol
FROM #pm_schema_ids AS i
JOIN sys.columns AS c ON c.object_id = i.object_id
JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
LEFT JOIN sys.identity_columns AS idc ON idc.object_id = c.object_id AND idc.column_id = c.column_id
LEFT JOIN sys.computed_columns AS cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
LEFT JOIN sys.default_constraints AS dc ON dc.object_id = c.default_object_id
ORDER BY c.object_id, c.column_id;

SELECT
    x.object_id,
    x.index_id,
    x.name,
    x.type_desc,
    x.is_unique,
    x.is_primary_key,
    x.is_unique_constraint,
    STUFF((
        SELECT N',' + REPLACE(REPLACE(kc.name, N'\', N'\\'), N',', N'\,') + CASE WHEN ic.is_descending_key = 1 THEN N' DESC' ELSE N'' END
        FROM sys.index_columns AS ic
        JOIN sys.columns AS kc ON kc.object_id = ic.object_id AND kc.column_id = ic.column_id
        WHERE ic.object_id = x.object_id AND ic.index_id = x.index_id AND ic.is_included_column = 0 AND ic.key_ordinal > 0
        ORDER BY ic.key_ordinal
        FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 1, N'') AS key_columns,
    STUFF((
        SELECT N',' + REPLACE(REPLACE(kc.name, N'\', N'\\'), N',', N'\,')
        FROM sys.index_columns AS ic
        JOIN sys.columns AS kc ON kc.object_id = ic.object_id AND kc.column_id = ic.column_id
        WHERE ic.object_id = x.object_id AND ic.index_id = x.index_id AND ic.is_included_column = 1
        ORDER BY ic.column_id
        FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 1, N'') AS included_columns,
    x.filter_definition,
    (
        SELECT CASE WHEN MIN(pt.data_compression_desc) = MAX(pt.data_compression_desc) THEN MIN(pt.data_compression_desc) ELSE N'MIXED' END
        FROM sys.partitions AS pt
        WHERE pt.object_id = x.object_id AND pt.index_id = x.index_id
    ) AS data_compression,
    x.is_disabled
FROM #pm_schema_ids AS i
JOIN sys.indexes AS x ON x.object_id = i.object_id
WHERE x.is_hypothetical = 0
ORDER BY x.object_id, x.index_id;

SELECT k.object_id, k.kind, k.name, k.definition, k.fk_columns, k.referenced_schema, k.referenced_table, k.referenced_columns,
       k.delete_action, k.update_action, k.is_disabled, k.is_not_trusted, k.is_system_named
FROM
(
    SELECT kc.parent_object_id AS object_id,
           CONVERT(nchar(1), CASE kc.type WHEN 'PK' THEN N'P' ELSE N'U' END) AS kind,
           kc.name,
           CONVERT(nvarchar(max), NULL) AS definition,
           CONVERT(nvarchar(max), NULL) AS fk_columns,
           CONVERT(nvarchar(128), NULL) AS referenced_schema,
           CONVERT(nvarchar(128), NULL) AS referenced_table,
           CONVERT(nvarchar(max), NULL) AS referenced_columns,
           CONVERT(nvarchar(60), NULL) AS delete_action,
           CONVERT(nvarchar(60), NULL) AS update_action,
           CONVERT(bit, 0) AS is_disabled,
           CONVERT(bit, 0) AS is_not_trusted,
           kc.is_system_named
    FROM #pm_schema_ids AS i
    JOIN sys.key_constraints AS kc ON kc.parent_object_id = i.object_id
    UNION ALL
    SELECT fk.parent_object_id,
           N'F',
           fk.name,
           NULL,
           STUFF((
               SELECT N',' + REPLACE(REPLACE(pc.name, N'\', N'\\'), N',', N'\,')
               FROM sys.foreign_key_columns AS fkc
               JOIN sys.columns AS pc ON pc.object_id = fkc.parent_object_id AND pc.column_id = fkc.parent_column_id
               WHERE fkc.constraint_object_id = fk.object_id
               ORDER BY fkc.constraint_column_id
               FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 1, N''),
           SCHEMA_NAME(ro.schema_id),
           ro.name,
           STUFF((
               SELECT N',' + REPLACE(REPLACE(rc.name, N'\', N'\\'), N',', N'\,')
               FROM sys.foreign_key_columns AS fkc
               JOIN sys.columns AS rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
               WHERE fkc.constraint_object_id = fk.object_id
               ORDER BY fkc.constraint_column_id
               FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 1, N''),
           fk.delete_referential_action_desc,
           fk.update_referential_action_desc,
           fk.is_disabled,
           fk.is_not_trusted,
           fk.is_system_named
    FROM #pm_schema_ids AS i
    JOIN sys.foreign_keys AS fk ON fk.parent_object_id = i.object_id
    JOIN sys.objects AS ro ON ro.object_id = fk.referenced_object_id
    UNION ALL
    SELECT ck.parent_object_id, N'C', ck.name, ck.definition, NULL, NULL, NULL, NULL, NULL, NULL,
           ck.is_disabled, ck.is_not_trusted, ck.is_system_named
    FROM #pm_schema_ids AS i
    JOIN sys.check_constraints AS ck ON ck.parent_object_id = i.object_id
    UNION ALL
    SELECT dc.parent_object_id, N'D', dc.name, dc.definition, NULL, NULL, NULL, NULL, NULL, NULL,
           CONVERT(bit, 0), CONVERT(bit, 0), dc.is_system_named
    FROM #pm_schema_ids AS i
    JOIN sys.default_constraints AS dc ON dc.parent_object_id = i.object_id
) AS k
ORDER BY k.object_id, k.kind, k.name;

SELECT
    tr.parent_id AS object_id,
    tr.name,
    tr.is_instead_of_trigger,
    STUFF((
        SELECT N',' + te.type_desc
        FROM sys.trigger_events AS te
        WHERE te.object_id = tr.object_id
        ORDER BY te.type_desc
        FOR XML PATH(N''), TYPE).value(N'.', N'nvarchar(max)'), 1, 1, N'') AS events
FROM #pm_schema_ids AS i
JOIN sys.triggers AS tr ON tr.parent_id = i.object_id AND tr.parent_class = 1
ORDER BY tr.parent_id, tr.name;

DROP TABLE #pm_schema_ids;
""";
}
