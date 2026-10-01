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

/// <summary>
/// Opt-in contract for a collector definition whose batch can return a row the store already holds under
/// the SAME exact identity, where the row has no numeric watermark or natural key to prevent it. The
/// deadlocks collector is the case: on an Azure SQL Database registered at master, a database's own
/// ring-buffer item and the server's telemetry read can both return one deadlock. The host reads the
/// batch's own identities back from the store ONCE, before the write, and calls
/// <see cref="DropAlreadyStored"/>.
///
/// <para>An identity is the event time (whole microseconds) plus the FULL graph text, compared ordinally:
/// no hash, so a collision can never drop a row. A row with no time or no graph has no identity and is
/// never dropped. If two reads serialize the same deadlock differently, the identities differ and both
/// rows are kept.</para>
/// </summary>
public interface IStoredIdentityDedupedCollector<TRow>
{
    /// <summary>
    /// One row's identity, or null when the row has none (a null time, or a null or empty graph).
    /// </summary>
    (DateTime Time, string Graph)? GetIdentity(TRow row);

    /// <summary>
    /// Pure: the rows of <paramref name="rows"/> whose <see cref="GetIdentity"/> is null, or is neither a
    /// member of <paramref name="stored"/> nor already carried by an earlier row of the same list, in the
    /// same relative order. Mutates neither argument.
    /// </summary>
    List<TRow> DropAlreadyStored(List<TRow> rows, IReadOnlySet<(DateTime Time, string Graph)> stored);
}
