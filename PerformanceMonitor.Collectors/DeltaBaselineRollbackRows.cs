/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The rows of a delta collector, carrying a snapshot of the baselines its run advances (the #7419 shape, shared). A host
/// that ends the run before the rows are stored calls <see cref="RollBackBaselines"/>, so the next run's delta covers both
/// intervals. Keys are captured before their first delta call of the run: all at once at the end of the read for a family
/// whose <c>WritePayload</c> advances them (<see cref="DeltaBaselineRollbackRows.Capture{TRow}"/>), or one by one through
/// <see cref="CaptureKey"/> for a family that differences each row as the reader yields it. The snapshot restores only what
/// the run itself wrote: a row the failed write never reached has not advanced and is left alone, and so is a baseline
/// anyone else replaced or cleared since. A key with no baseline before the run goes back to unseen.
/// </summary>
public sealed class DeltaBaselineRollbackRows<TRow> : List<TRow>, IBaselineRollback
{
    private readonly CollectorContext _context;
    private readonly IReadOnlyList<string> _groups;
    private readonly HashSet<string> _captured = new(StringComparer.Ordinal);
    private List<IDeltaBaselineSnapshot>? _snapshots = new();

    /// <param name="groups">The delta groups the family's <c>CalculateDelta*</c> calls advance.</param>
    public DeltaBaselineRollbackRows(CollectorContext context, IReadOnlyList<string> groups)
    {
        _context = context;
        _groups = groups;
    }

    /// <summary>Captures <paramref name="key"/>'s baselines unless this run already has. Call before its first delta call.</summary>
    public void CaptureKey(string key)
    {
        if (_captured.Add(key))
        {
            Snapshot(new[] { key });
        }
    }

    /// <summary>Captures every key of <paramref name="keys"/> this run has not captured yet, in one snapshot.</summary>
    public void CaptureKeys(IEnumerable<string> keys)
    {
        var fresh = keys.Where(_captured.Add).ToList();
        if (fresh.Count > 0)
        {
            Snapshot(fresh);
        }
    }

    private void Snapshot(IReadOnlyCollection<string> keys)
    {
        var snapshot = _context.Deltas.CaptureBaselines(_context.ServerId, _groups, keys, _context.CollectionTime);
        if (snapshot is not null)
        {
            _snapshots?.Add(snapshot);
        }
    }

    /// <summary>Restores every snapshot, latest first, so the earliest capture of a group's pass window is what stands.</summary>
    public void RollBackBaselines()
    {
        var snapshots = _snapshots;
        _snapshots = null;
        if (snapshots is null)
        {
            return;
        }

        for (var i = snapshots.Count - 1; i >= 0; i--)
        {
            snapshots[i].Restore();
        }
    }
}

/// <summary>Builds a <see cref="DeltaBaselineRollbackRows{TRow}"/> at the end of a delta collector's read.</summary>
public static class DeltaBaselineRollbackRows
{
    /// <summary>
    /// Wraps <paramref name="rows"/> with a snapshot of the baselines <paramref name="groups"/> hold for each row's delta key,
    /// as they stand before the write advances them. <paramref name="deltaKey"/> is the same derivation the family's
    /// <c>CalculateDelta*</c> calls use.
    /// </summary>
    public static List<TRow> Capture<TRow>(List<TRow> rows, CollectorContext context, IReadOnlyList<string> groups, Func<TRow, string> deltaKey)
    {
        var wrapped = new DeltaBaselineRollbackRows<TRow>(context, groups);
        wrapped.CaptureKeys(rows.Select(deltaKey));
        wrapped.AddRange(rows);
        return wrapped;
    }
}
