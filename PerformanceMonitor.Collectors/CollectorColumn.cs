/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Logical column types for collector payload columns — engine-neutral names each host maps to
/// its own storage types (Lite/DuckDB and Darling/Postgres both have direct equivalents).
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1720:Identifier contains type name",
    Justification = "Engine-neutral storage-type vocabulary by design. Integer/Double/Decimal name the logical column types the hosts map, not .NET types.")]
public enum CollectorColumnType
{
    BigInt,
    Integer,
    SmallInt,
    Varchar,
    Timestamp,
    Double,
    Decimal,
    Boolean,
}

/// <summary>
/// One payload column a collector definition emits, in emission order. The host writes its
/// standard prefix columns first; these describe everything after the prefix.
/// </summary>
public sealed class CollectorColumn
{
    public CollectorColumn(string name, CollectorColumnType type)
    {
        Name = name;
        Type = type;
    }

    /// <summary>
    /// Decimal column with explicit precision/scale, mirroring the column's declared type in
    /// Lite's DuckDB schema (e.g. DECIMAL(18,2) memory MBs, DECIMAL(5,2) percent_complete) so
    /// Darling's generated Postgres DDL matches column-for-column.
    /// </summary>
    public CollectorColumn(string name, CollectorColumnType type, int precision, int scale)
        : this(name, type)
    {
        Precision = precision;
        Scale = scale;
    }

    public string Name { get; }

    public CollectorColumnType Type { get; }

    /// <summary>Declared precision for <see cref="CollectorColumnType.Decimal"/> columns; 0 otherwise.</summary>
    public int Precision { get; }

    /// <summary>Declared scale for <see cref="CollectorColumnType.Decimal"/> columns; 0 otherwise.</summary>
    public int Scale { get; }
}
