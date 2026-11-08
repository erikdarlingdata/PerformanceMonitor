/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/* #7643: one table or view as read from a database's catalog: the object, its table or view properties, and its columns, indexes,
   constraints and triggers. The child records hold exactly the columns of the store's schema_columns, schema_indexes, schema_constraints
   and schema_triggers. Column lists (key, included, foreign key, referenced) are names in catalog order; a descending key column is
   written "Name DESC" by the reader so the direction is part of the definition. */

/// <summary>A table or view with everything the definition hash covers. <see cref="Type"/> is 'U' for a table and 'V' for a view.</summary>
public sealed record SchemaObjectDefinition(
    int ObjectId,
    string Schema,
    string Name,
    char Type,
    DateTime? ModifyDate,
    SchemaTableProperties? Table,
    SchemaViewProperties? View,
    IReadOnlyList<SchemaColumn> Columns,
    IReadOnlyList<SchemaIndex> Indexes,
    IReadOnlyList<SchemaConstraint> Constraints,
    IReadOnlyList<SchemaTrigger> Triggers);

public sealed record SchemaTableProperties(
    string? TemporalType,
    string? HistoryTable,
    bool? IsMemoryOptimized,
    string? Durability,
    string? PartitionScheme,
    string? PartitionColumn,
    string? LockEscalation);

public sealed record SchemaViewProperties(
    bool? IsSchemaBound,
    bool? IsIndexed,
    string? ViewDefinition);

public sealed record SchemaColumn(
    int Ordinal,
    string Name,
    string TypeName,
    int? MaxLength,
    int? Precision,
    int? Scale,
    bool IsNullable,
    decimal? IdentitySeed,
    decimal? IdentityIncrement,
    string? ComputedDefinition,
    bool? IsPersisted,
    string? DefaultName,
    string? DefaultDefinition,
    string? CollationName,
    bool? IsSparse,
    bool? IsRowGuidCol);

/// <summary><see cref="IndexId"/> is the catalog's index_id, kept for the store and not part of the hash. A heap has a null name.</summary>
public sealed record SchemaIndex(
    int IndexId,
    string? Name,
    string TypeDesc,
    bool IsUnique,
    bool IsPrimaryKey,
    bool IsUniqueConstraint,
    IReadOnlyList<string> KeyColumns,
    IReadOnlyList<string> IncludedColumns,
    string? FilterDefinition,
    string? DataCompression,
    bool IsDisabled);

/// <summary><see cref="Kind"/> is 'P' primary key, 'U' unique, 'F' foreign key, 'C' check, 'D' default.</summary>
public sealed record SchemaConstraint(
    char Kind,
    string Name,
    string? Definition,
    IReadOnlyList<string> FkColumns,
    string? ReferencedSchema,
    string? ReferencedTable,
    IReadOnlyList<string> ReferencedColumns,
    string? DeleteAction,
    string? UpdateAction,
    bool IsDisabled,
    bool IsNotTrusted,
    bool IsSystemNamed);

public sealed record SchemaTrigger(
    string Name,
    bool IsInsteadOf,
    string Events);
