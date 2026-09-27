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
/// Opt-in contract (#4487) for a collector definition whose batch can re-deliver rows the store already
/// holds under the SAME natural key as an earlier batch. job_history's failback lookback is the case: a
/// target that fails over away from an epoch and back re-reads up to
/// <see cref="JobHistoryCollector.FailbackLookbackDays"/> days of that epoch's own already-stored rows, so
/// the numeric watermark alone can no longer prevent a duplicate. The host reads the batch's own natural
/// keys back from the store ONCE, before the write, and calls <see cref="DropAlreadyStored"/> so an honest
/// re-read of an already-closed epoch is not duplicated. job_history is the only collector that implements
/// this today; no other collector's write path changes.
/// </summary>
public interface INaturalKeyDedupedCollector<TRow>
{
    /// <summary>
    /// The natural key's own column names, in the exact order <see cref="GetNaturalKey"/> returns them —
    /// NOT including server_id, which the host's stored-key read already scopes to. Documentation and pin
    /// value only; the host's SQL names these columns itself.
    /// </summary>
    IReadOnlyList<string> NaturalKeyColumns { get; }

    /// <summary>Extracts one row's natural key (excluding server_id).</summary>
    (long InstanceId, string JobId, int StepId, DateTime RunDateTime) GetNaturalKey(TRow row);

    /// <summary>
    /// Pure: the rows of <paramref name="rows"/> whose <see cref="GetNaturalKey"/> is NOT a member of
    /// <paramref name="storedKeys"/>, in the same relative order. Mutates neither argument.
    /// </summary>
    List<TRow> DropAlreadyStored(
        List<TRow> rows,
        IReadOnlySet<(long InstanceId, string JobId, int StepId, DateTime RunDateTime)> storedKeys);
}
