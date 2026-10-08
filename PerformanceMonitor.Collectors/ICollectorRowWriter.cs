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
    /// Writes a payload column that may already be in the store under a known digest (#5158). A writer that
    /// can store a digest without the content overrides this; the default ignores the digest and writes the
    /// content, so a host that does not (Lite) behaves exactly as it did with <see cref="Value(string?)"/>.
    /// </summary>
    /// <param name="content">The payload text, or null when the row carries none.</param>
    /// <param name="knownDigest">The digest the store already holds for this content, or null.</param>
    ICollectorRowWriter PayloadOrDigest(string? content, string? knownDigest) => Value(content);
}
