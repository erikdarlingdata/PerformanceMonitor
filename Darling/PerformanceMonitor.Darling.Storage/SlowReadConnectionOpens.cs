/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// V321 (#9923): <c>collect.slow_reads</c> gains <c>connection_opens</c>, how many physical connection opens the read's own statements
/// waited on, and <c>connection_open_ms</c>, the sum of those opens in milliseconds. A physical open is not a statement, so
/// <c>statement_count</c> counts the statements the read ran and these two columns count the opens beside it. Both are nullable
/// <c>integer</c> with no default: a row from a read that did not capture statements, or written before this rung, has no count, and a
/// NULL says so where a 0 would claim a read that opened no connection.
///
/// <para><b>An older build ignores the columns.</b> Its writer names its own column list and its reads name theirs, so it neither fills
/// nor reads them. A row an older build writes has NULL in both, and its <c>statement_count</c> still includes the opens it saw as
/// statements.</para>
/// </summary>
public static class SlowReadConnectionOpens
{
    /// <summary>
    /// The store schema version that adds the two columns. The one place the number is written: the migration, the schema version
    /// constant and the viewer's version map read it, so renumbering the rung is a change to this line.
    /// </summary>
    public const int Version = 321;

    /// <summary>The DDL of the rung. The table is plain and small, so <c>ADD COLUMN</c> holds its lock for an instant. Every statement is idempotent.</summary>
    public const string AddColumnsSql = """
ALTER TABLE collect.slow_reads
    ADD COLUMN IF NOT EXISTS connection_opens integer,
    ADD COLUMN IF NOT EXISTS connection_open_ms integer;
""";
}
