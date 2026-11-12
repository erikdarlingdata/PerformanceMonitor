/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #8146: the measurements of one item, held until the host lands or drops the item (<see cref="CollectorContext.StagedItemMeasurements"/>).
/// Same label rules as <see cref="CollectorContext.Measure"/>, checked when a value is staged, so a bad label fails where it is
/// written. Landing keeps each method's meaning: <see cref="Measure"/> adds to the run's count, <see cref="MeasureMax"/> keeps the
/// larger value.
/// </summary>
public sealed class StagedMeasurements
{
    private readonly List<StagedMeasurement> _entries = new();

    /// <summary>Stages a count, added to the run's count of <paramref name="label"/> when the item lands.</summary>
    public void Measure(string label, long value)
    {
        CollectorContext.ValidateMeasurementLabel(label);
        _entries.Add(new StagedMeasurement(label, value, KeepLargest: false));
    }

    /// <summary>Stages a span or a level, compared with the run's value of <paramref name="label"/> when the item lands.</summary>
    public void MeasureMax(string label, long value)
    {
        CollectorContext.ValidateMeasurementLabel(label);
        _entries.Add(new StagedMeasurement(label, value, KeepLargest: true));
    }

    internal IReadOnlyList<StagedMeasurement> Entries => _entries;

    internal void Clear() => _entries.Clear();
}

internal readonly record struct StagedMeasurement(string Label, long Value, bool KeepLargest);
