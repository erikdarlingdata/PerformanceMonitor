/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The SQL half of the trend bucketing contract (#3897) that the store's own readers need — the MCP reads in the
/// service and the PostgreSQL trend reads here build their <c>date_bin</c> from it. The sizing rules (the width
/// ladder, the point budget, the refusals) live in <c>PerformanceMonitor.Common.TrendBuckets</c>, which this
/// assembly cannot see; <c>TrendBucketSqlTests</c> pins <see cref="OriginSql"/> equal to <c>TrendBuckets.OriginSql</c>,
/// which Lite's DuckDB reads use, so both SKUs bin the same rows into the same buckets at any width.
/// </summary>
public static class TrendBucketSql
{
    /// <summary>
    /// The origin every bucketing read aligns to. A midnight, so every ladder width (each divides a day) lands on
    /// round clock times; and an explicit one, because PostgreSQL's <c>date_bin</c> requires it and DuckDB's
    /// <c>time_bucket</c> otherwise defaults to 2000-01-03, which bins a width that does not divide that offset
    /// differently.
    /// </summary>
    public const string OriginSql = "TIMESTAMP '2000-01-01 00:00:00'";
}
