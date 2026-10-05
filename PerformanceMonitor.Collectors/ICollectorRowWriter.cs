/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Positional row writer a collector definition emits its payload through. Hosts adapt this onto
/// their storage engine's bulk writer (Lite: DuckDB appender row; Darling: Npgsql binary COPY row).
/// Column ORDER is the contract — it must match the definition's <c>PayloadColumns</c> exactly,
/// and hosts write their standard prefix (collection id/time, server identity) before handing the
/// row to the definition.
/// </summary>
public interface ICollectorRowWriter
{
    ICollectorRowWriter Value(string? value);
    ICollectorRowWriter Value(long value);
    ICollectorRowWriter Value(long? value);
    ICollectorRowWriter Value(int value);
    ICollectorRowWriter Value(int? value);
    ICollectorRowWriter Value(short value);
    ICollectorRowWriter Value(short? value);
    ICollectorRowWriter Value(double value);
    ICollectorRowWriter Value(double? value);
    ICollectorRowWriter Value(decimal value);
    ICollectorRowWriter Value(decimal? value);
    ICollectorRowWriter Value(bool value);
    ICollectorRowWriter Value(bool? value);
    ICollectorRowWriter Value(DateTime value);
    ICollectorRowWriter Value(DateTime? value);
    ICollectorRowWriter NullValue();

    /// <summary>
    /// Writes a payload column that may already be held by the store under a content digest. A host whose
    /// store keeps payloads by digest can write <paramref name="knownDigest"/> alone when
    /// <paramref name="content"/> is null; every other host writes the content, which is this default.
    /// Exactly one payload column is consumed either way.
    /// </summary>
    ICollectorRowWriter PayloadOrDigest(string? content, string? knownDigest) => Value(content);
}
